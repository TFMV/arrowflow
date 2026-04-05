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

	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/metrics"
	"github.com/TFMV/arrowflow/internal/producer"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ba "github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
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
}

type result struct {
	duration time.Duration
	messages int64
	bytes    int64
	errors   int64
	report   metrics.TelemetryReport
}

func runDirect(parent context.Context, cfg directConfig) (*result, error) {
	metrics.Reset()

	base, err := newTranscoder(cfg.EnableHyper, cfg.Denorm)
	if err != nil {
		return nil, err
	}
	defer base.Release()

	transcoders := []*ba.Transcoder{base}
	for i := 1; i < cfg.Workers; i++ {
		clone, cloneErr := base.Clone(memory.NewGoAllocator())
		if cloneErr != nil {
			return nil, cloneErr
		}
		transcoders = append(transcoders, clone)
	}
	defer func() {
		for _, tc := range transcoders[1:] {
			tc.Release()
		}
	}()

	ctx, cancel := context.WithTimeout(parent, cfg.Duration)
	defer cancel()

	var totalMessages atomic.Int64
	var totalBytes atomic.Int64
	var errorsCount atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for _, tc := range transcoders {
		wg.Add(1)
		go func(tc *ba.Transcoder) {
			defer wg.Done()
			wp := producer.NewWireProducer(nil, config.Load(), producer.WireConfig{SizeDist: cfg.SizeDist})
			ticker := time.NewTicker(perWorkerInterval(cfg.Rate, cfg.Workers))
			defer ticker.Stop()

			pending := 0
			for {
				select {
				case <-ctx.Done():
					if pending > 0 {
						recordBatch(tc, cfg.Denorm, pending)
					}
					return
				case <-ticker.C:
					raw, marshalErr := proto.Marshal(wp.GenerateWireMessage())
					if marshalErr != nil {
						errorsCount.Add(1)
						continue
					}

					startConsume := time.Now()
					var appendErr error
					if cfg.Denorm {
						appendErr = tc.AppendDenormRaw(raw)
					} else {
						appendErr = tc.AppendRaw(raw)
					}
					if appendErr != nil {
						errorsCount.Add(1)
						continue
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
				}
			}
		}(tc)
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

func newTranscoder(enableHyper, denorm bool) (*ba.Transcoder, error) {
	protoPath := findProtoPath()
	protoDir := filepath.Dir(protoPath)
	protoFile := filepath.Base(protoPath)

	fd, err := ba.CompileProtoToFileDescriptor(protoFile, []string{protoDir})
	if err != nil {
		return nil, err
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return nil, err
	}

	var opts []ba.Option
	if enableHyper {
		opts = append(opts, ba.WithHyperType(ba.NewHyperType(md, ba.WithAutoRecompile(100_000, 0.01))))
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
	return ba.New(md, memory.DefaultAllocator, opts...)
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

func perWorkerInterval(rate, workers int) time.Duration {
	if rate <= 0 {
		return time.Microsecond
	}
	perWorker := rate / max(workers, 1)
	if perWorker <= 0 {
		perWorker = 1
	}
	interval := time.Second / time.Duration(perWorker)
	if interval <= 0 {
		return time.Microsecond
	}
	return interval
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
