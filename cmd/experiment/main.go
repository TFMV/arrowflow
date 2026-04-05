package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/TFMV/arrowflow/internal/metrics"
	"github.com/TFMV/arrowflow/internal/producer"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ba "github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
)

func main() {
	mode := flag.String("mode", "direct", "Experiment mode: direct, stress")
	duration := flag.Duration("duration", 30*time.Second, "Experiment duration")
	rate := flag.Int("rate", 10000, "Messages per second")
	workers := flag.Int("workers", runtime.NumCPU(), "Worker goroutines")
	batchSize := flag.Int("batch-size", 1000, "Batch size")
	enableHyper := flag.Bool("hyper", true, "Enable HyperType")
	maxRate := flag.Int("max-rate", 100000, "Max stress rate")
	sizeDist := flag.String("size-dist", "heavy-tail", "Payload size distribution")
	denorm := flag.Bool("denorm", true, "Enable denormalized Arrow output")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch *mode {
	case "stress":
		runStress(ctx, *duration, *workers, *batchSize, *enableHyper, *sizeDist, *denorm, *maxRate)
	default:
		result, err := runDirect(ctx, directConfig{
			Rate:        *rate,
			Workers:     *workers,
			Duration:    *duration,
			BatchSize:   *batchSize,
			EnableHyper: *enableHyper,
			SizeDist:    *sizeDist,
			Denorm:      *denorm,
		})
		if err != nil {
			log.Fatal(err)
		}
		printResult(result)
	}
}

type directConfig struct {
	Rate        int
	Workers     int
	Duration    time.Duration
	BatchSize   int
	EnableHyper bool
	SizeDist    string
	Denorm      bool
	Corpus      *producer.RawCorpus
}

type result struct {
	duration time.Duration
	messages int64
	bytes    int64
	errors   int64
	report   metrics.TelemetryReport
}

func runDirect(parent context.Context, cfg directConfig) (*result, error) {
	corpusPlan := cfg.Corpus
	preparedHere := false
	if corpusPlan == nil {
		var err error
		corpusPlan, err = producer.PrepareRawCorpus(producer.WireConfig{
			SizeDist:      cfg.SizeDist,
			SchemaVersion: "1.0.0",
		}, prepareCorpusOptions(cfg.Rate, cfg.Duration))
		if err != nil {
			return nil, err
		}
		preparedHere = true
	}
	if preparedHere {
		logPreparedCorpus(corpusPlan)
	}
	corpus := corpusPlan.Messages

	base, ht, err := newTranscoder(cfg.EnableHyper, cfg.Denorm)
	if err != nil {
		return nil, err
	}
	if err := warmTranscoder(base, ht, cfg.Denorm, corpus); err != nil {
		base.Release()
		return nil, err
	}

	transcoders, err := cloneTranscoders(base, cfg.Workers)
	if err != nil {
		base.Release()
		return nil, err
	}
	defer releaseTranscoders(transcoders)
	shards := shardCorpus(corpus, cfg.Workers)

	ctx, cancel := context.WithTimeout(parent, cfg.Duration)
	defer cancel()

	metrics.Reset()

	var totalMessages atomic.Int64
	var totalBytes atomic.Int64
	var errorsCount atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for workerID, tc := range transcoders {
		wg.Add(1)
		go func(worker int, tc *ba.Transcoder) {
			defer wg.Done()
			shard := shardForWorker(shards, worker, corpus)
			cursor := 0
			pending := 0

			process := func(raw []byte) bool {
				startConsume := time.Now()
				var appendErr error
				if cfg.Denorm {
					appendErr = tc.AppendDenormRaw(raw)
				} else {
					appendErr = tc.AppendRaw(raw)
				}
				if appendErr != nil {
					errorsCount.Add(1)
					return false
				}

				metrics.RecordLatency("consume", startConsume)
				metrics.RecordConsumedThroughput(int64(len(raw)), 1)
				totalMessages.Add(1)
				totalBytes.Add(int64(len(raw)))
				pending++
				if pending >= cfg.BatchSize {
					recordBatch(tc, cfg.Denorm, pending)
					pending = 0
				}
				return true
			}

			runRateLimitedLoop(ctx, rateShare(cfg.Rate, worker, cfg.Workers), func() bool {
				return process(nextRaw(shard, &cursor))
			})

			if pending > 0 {
				recordBatch(tc, cfg.Denorm, pending)
			}
		}(workerID, tc)
	}

	wg.Wait()
	return &result{
		duration: time.Since(start),
		messages: totalMessages.Load(),
		bytes:    totalBytes.Load(),
		errors:   errorsCount.Load(),
		report:   metrics.GenerateTelemetryReport(),
	}, nil
}

func runStress(ctx context.Context, duration time.Duration, workers int, batchSize int, hyper bool, sizeDist string, denorm bool, maxRate int) {
	corpusPlan, err := producer.PrepareRawCorpus(producer.WireConfig{
		SizeDist:      sizeDist,
		SchemaVersion: "1.0.0",
	}, prepareCorpusOptions(maxRate, duration))
	if err != nil {
		log.Fatal(err)
	}
	logPreparedCorpus(corpusPlan)

	rates := []int{1000, 5000, 10000, 25000, 50000, 75000, 100000}
	for _, rate := range rates {
		if rate > maxRate {
			continue
		}
		result, err := runDirect(ctx, directConfig{
			Rate:        rate,
			Workers:     workers,
			Duration:    duration,
			BatchSize:   batchSize,
			EnableHyper: hyper,
			SizeDist:    sizeDist,
			Denorm:      denorm,
			Corpus:      corpusPlan,
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Stress Results:\n")
		fmt.Printf("  Rate Input: %d\n", rate)
		printResult(result)
	}
}

func printResult(result *result) {
	fmt.Printf("Experiment Results:\n")
	fmt.Printf("  Duration: %v\n", result.duration)
	fmt.Printf("  Messages: %d\n", result.messages)
	fmt.Printf("  Rate: %.2f msg/s\n", float64(result.messages)/result.duration.Seconds())
	result.report.PrintSummary()
	log.Printf("Errors: %d", result.errors)
}

func newTranscoder(enableHyper, denorm bool) (*ba.Transcoder, *ba.HyperType, error) {
	protoPath := findProtoPath()
	protoDir := filepath.Dir(protoPath)
	protoFile := filepath.Base(protoPath)

	fd, err := ba.CompileProtoToFileDescriptor(protoFile, []string{protoDir})
	if err != nil {
		return nil, nil, err
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return nil, nil, err
	}

	var ht *ba.HyperType
	var opts []ba.Option
	if enableHyper {
		ht = ba.NewHyperType(md, ba.WithAutoRecompile(0, 1.0))
		opts = append(opts, ba.WithHyperType(ht))
	}
	if denorm {
		opts = append(opts, ba.WithDenormalizerPlan(
			pbpath.PlanPath("schema_version"),
			pbpath.PlanPath("event_timestamp"),
			pbpath.PlanPath("user.user_id"),
			pbpath.PlanPath("session.session_id"),
			pbpath.PlanPath("tracing.trace_id"),
			pbpath.PlanPath("payload.event_type"),
			pbpath.PlanPath("metrics[*].name"),
			pbpath.PlanPath("tags[*].key"),
		))
	}
	tc, err := ba.New(md, memory.DefaultAllocator, opts...)
	if err != nil {
		return nil, nil, err
	}
	return tc, ht, nil
}

func recordBatch(tc *ba.Transcoder, denorm bool, inputMessages int) {
	startBatch := time.Now()
	var rec arrow.RecordBatch
	if denorm {
		rec = tc.NewDenormalizerRecordBatch()
	} else {
		rec = tc.NewRecordBatch()
	}
	metrics.RecordLatency("batch_output", startBatch)
	defer rec.Release()

	if rec.NumRows() == 0 {
		return
	}
	var sizeBytes uint64
	for _, col := range rec.Columns() {
		sizeBytes += col.Data().SizeInBytes()
	}
	metrics.RecordBatchMetrics(int(rec.NumRows()), int(rec.NumCols()), int64(sizeBytes), inputMessages)
}

func warmTranscoder(tc *ba.Transcoder, ht *ba.HyperType, denorm bool, corpus [][]byte) error {
	if tc == nil || len(corpus) == 0 {
		return nil
	}

	for _, raw := range corpus {
		var err error
		if denorm {
			err = tc.AppendDenormRaw(raw)
		} else {
			err = tc.AppendRaw(raw)
		}
		if err != nil {
			return err
		}
	}

	var rec arrow.RecordBatch
	if denorm {
		rec = tc.NewDenormalizerRecordBatch()
	} else {
		rec = tc.NewRecordBatch()
	}
	rec.Release()

	if ht != nil {
		return ht.Recompile()
	}
	return nil
}

func cloneTranscoders(base *ba.Transcoder, workers int) ([]*ba.Transcoder, error) {
	if workers <= 0 {
		workers = 1
	}

	transcoders := make([]*ba.Transcoder, 0, workers)
	transcoders = append(transcoders, base)
	for i := 1; i < workers; i++ {
		clone, err := base.Clone(memory.NewGoAllocator())
		if err != nil {
			for _, tc := range transcoders[1:] {
				tc.Release()
			}
			return nil, err
		}
		transcoders = append(transcoders, clone)
	}
	return transcoders, nil
}

func releaseTranscoders(transcoders []*ba.Transcoder) {
	for _, tc := range transcoders {
		if tc != nil {
			tc.Release()
		}
	}
}

func prepareCorpusOptions(rate int, duration time.Duration) producer.CorpusOptions {
	return producer.CorpusOptions{
		TargetMessages: desiredCorpusMessages(rate, duration),
		PilotMessages:  1024,
		MaxMessages:    65536,
		MaxBytes:       256 << 20,
	}
}

func desiredCorpusMessages(rate int, duration time.Duration) int {
	if rate > 0 && duration > 0 {
		target := int(float64(rate)*duration.Seconds() + 0.5)
		if target > 0 {
			return target
		}
	}
	return 32768
}

func logPreparedCorpus(corpus *producer.RawCorpus) {
	if corpus == nil {
		return
	}
	log.Printf(
		"Prepared corpus: %d messages replayable (%d desired, %.2f MB total, avg %d bytes)",
		len(corpus.Messages),
		corpus.DesiredMessages,
		float64(corpus.TotalBytes)/1024/1024,
		corpus.AvgBytes,
	)
}

func shardCorpus(corpus [][]byte, workers int) [][][]byte {
	if workers <= 0 {
		workers = 1
	}
	shards := make([][][]byte, workers)
	for i, raw := range corpus {
		shards[i%workers] = append(shards[i%workers], raw)
	}
	return shards
}

func shardForWorker(shards [][][]byte, worker int, fallback [][]byte) [][]byte {
	if worker >= 0 && worker < len(shards) && len(shards[worker]) > 0 {
		return shards[worker]
	}
	return fallback
}

func nextRaw(shard [][]byte, cursor *int) []byte {
	raw := shard[*cursor%len(shard)]
	*cursor = *cursor + 1
	return raw
}

func runRateLimitedLoop(ctx context.Context, rate int, fn func() bool) {
	if rate <= 0 {
		for ctx.Err() == nil {
			if !fn() && ctx.Err() != nil {
				return
			}
		}
		return
	}

	last := time.Now()
	budget := 0.0
	for {
		if ctx.Err() != nil {
			return
		}

		now := time.Now()
		budget += float64(rate) * now.Sub(last).Seconds()
		last = now

		if budget < 1 {
			sleepFor := time.Duration(((1 - budget) / float64(rate)) * float64(time.Second))
			if sleepFor <= 0 {
				sleepFor = 50 * time.Microsecond
			}
			if sleepFor > time.Millisecond {
				sleepFor = time.Millisecond
			}
			timer := time.NewTimer(sleepFor)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}

		toRun := int(budget)
		budget -= float64(toRun)
		for i := 0; i < toRun; i++ {
			if ctx.Err() != nil {
				return
			}
			if !fn() && ctx.Err() != nil {
				return
			}
		}
	}
}

func rateShare(total, worker, workers int) int {
	if workers <= 1 {
		return total
	}
	share := total / workers
	if worker < total%workers {
		share++
	}
	return share
}

func findProtoPath() string {
	searchPaths := []string{
		"./internal/schemas/event.proto",
		"internal/schemas/event.proto",
		"../internal/schemas/event.proto",
	}
	for _, path := range searchPaths {
		if _, err := os.Stat(path); err == nil {
			absPath, _ := filepath.Abs(path)
			return absPath
		}
	}
	cwd, _ := os.Getwd()
	return filepath.Join(cwd, "internal/schemas/event.proto")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
