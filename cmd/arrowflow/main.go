package main

import (
	"context"
	"errors"
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

	"github.com/TFMV/arrowflow/internal/chaos"
	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/consumer"
	"github.com/TFMV/arrowflow/internal/metrics"
	"github.com/TFMV/arrowflow/internal/producer"
	"github.com/TFMV/arrowflow/internal/stream"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ba "github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
)

type ExperimentResult struct {
	Mode       string
	Duration   time.Duration
	Messages   int64
	Bytes      int64
	Errors     int64
	Report     metrics.TelemetryReport
	RatePerSec float64
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	metrics.Reset()
	stopTelemetry := metrics.StartTelemetryLoop(time.Second, "telemetry.json")

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "producer":
		err = runProducer(ctx, args)
	case "consumer":
		err = runConsumer(ctx, args)
	case "benchmark":
		err = runBenchmark(ctx, args)
	case "experiment":
		err = runExperiment(ctx, args)
	case "chaos":
		err = runChaos(ctx, args)
	default:
		usage()
		os.Exit(1)
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("Error: %v", err)
	}

	stopTelemetry()
	report := metrics.GenerateTelemetryReport()
	writeTelemetry(report)
	report.PrintSummary()
}

func usage() {
	fmt.Printf("ArrowFlow - Streaming Protobuf -> Arrow Pipeline\n\n")
	fmt.Printf("Usage: arrowflow <command> [options]\n\n")
	fmt.Printf("Commands:\n")
	fmt.Printf("  producer    Run streaming producer\n")
	fmt.Printf("  consumer    Run streaming consumer\n")
	fmt.Printf("  benchmark   Run direct protobuf->Arrow benchmark\n")
	fmt.Printf("  experiment  Run direct/stream experiment modes\n")
	fmt.Printf("  chaos       Run chaos injection experiment\n")
}

func runProducer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("producer", flag.ExitOnError)
	rate := fs.Int("rate", 1000, "Messages per second")
	mode := fs.String("mode", "steady", "Mode: steady, burst, sinusoidal")
	sizeDist := fs.String("size-dist", "heavy-tail", "Size: small, medium, large, heavy-tail")
	burstFactor := fs.Int("burst-factor", 50, "Burst multiplier")
	schemaVer := fs.String("schema-version", "1.0.0", "Schema version")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := config.Load()
	p, err := stream.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer p.Close()

	prod := producer.NewWireProducer(p, cfg, producer.WireConfig{
		Mode:          *mode,
		Rate:          *rate,
		BurstFactor:   *burstFactor,
		BurstDuration: 5 * time.Second,
		SizeDist:      *sizeDist,
		SchemaVersion: *schemaVer,
	})

	log.Printf("Producer: rate=%d, mode=%s, size=%s", *rate, *mode, *sizeDist)
	err = prod.Run(ctx)
	msgs, bytes, errs, bursts := prod.Stats()
	log.Printf("Produced: messages=%d bytes=%d errors=%d bursts=%d", msgs, bytes, errs, bursts)
	return err
}

func runConsumer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("consumer", flag.ExitOnError)
	workers := fs.Int("workers", runtime.NumCPU(), "Worker goroutines")
	batchSize := fs.Int("batch-size", 1000, "Batch size")
	mode := fs.String("mode", "denorm", "Mode: nested, denorm")
	hyper := fs.Bool("hypertype", true, "Enable HyperType")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := config.Load()
	cfg.ConsumerPullWorkers = max(1, *workers)
	cfg.ConsumerPullBatchSize = min(max(1, *batchSize), 512)
	s, err := stream.NewConsumer(cfg)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer s.Close()

	cons, err := consumer.NewWireConsumer(s, cfg, consumer.ConsumerConfig{
		Workers:     *workers,
		OutputMode:  *mode,
		BatchSize:   *batchSize,
		EnableHyper: *hyper,
		DenormPaths: consumer.DefaultConsumerConfig.DenormPaths,
	})
	if err != nil {
		return err
	}
	defer cons.Close()

	log.Printf("Consumer: workers=%d batch=%d mode=%s hyper=%v", *workers, *batchSize, *mode, *hyper)
	if err := cons.Run(ctx); err != nil {
		return err
	}
	msgs, bytes, errs, rows, batches := cons.Stats()
	log.Printf("Consumed: messages=%d bytes=%d errors=%d rows=%d batches=%d", msgs, bytes, errs, rows, batches)
	return nil
}

func runBenchmark(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	messages := fs.Int("messages", 100000, "Total messages")
	size := fs.Int("size", 4096, "Payload size")
	workers := fs.Int("workers", runtime.NumCPU(), "Workers")
	batchSize := fs.Int("batch-size", 1000, "Batch flush size")
	hyper := fs.Bool("hyper", true, "Enable HyperType")
	denorm := fs.Bool("denorm", true, "Use denormalized Arrow output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	result, err := runDirectPipeline(ctx, directPipelineConfig{
		Mode:        "benchmark",
		Rate:        0,
		Workers:     *workers,
		Duration:    0,
		Messages:    *messages,
		BatchSize:   *batchSize,
		SizeDist:    "large",
		EnableHyper: *hyper,
		Denorm:      *denorm,
		FixedSize:   *size,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Results:\n")
	fmt.Printf("  Messages: %d\n", result.Messages)
	fmt.Printf("  Time: %v\n", result.Duration)
	fmt.Printf("  Rate: %.2f msg/s\n", result.RatePerSec)
	fmt.Printf("  Throughput: %.2f MB/s\n", float64(result.Bytes)/result.Duration.Seconds()/1024/1024)
	return nil
}

func runExperiment(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("experiment", flag.ExitOnError)
	mode := fs.String("mode", "direct", "Mode: direct, stress, stream")
	rate := fs.Int("rate", 10000, "Messages per second")
	workers := fs.Int("workers", runtime.NumCPU(), "Workers")
	duration := fs.Duration("duration", 30*time.Second, "Duration")
	hyper := fs.Bool("hyper", true, "Enable HyperType")
	batchSize := fs.Int("batch-size", 1000, "Batch size")
	sizeDist := fs.String("size-dist", "heavy-tail", "Size distribution")
	denorm := fs.Bool("denorm", true, "Enable denormalization")
	maxRate := fs.Int("max-rate", 100000, "Maximum stress rate")
	if err := fs.Parse(args); err != nil {
		return err
	}

	switch *mode {
	case "stream":
		result, err := runStreamExperiment(ctx, streamExperimentConfig{
			Rate:        *rate,
			Workers:     *workers,
			Duration:    *duration,
			BatchSize:   *batchSize,
			SizeDist:    *sizeDist,
			EnableHyper: *hyper,
			Denorm:      *denorm,
		})
		if err != nil {
			return err
		}
		printExperimentResult(result)
		return nil
	case "stress":
		return runStressCollapse(ctx, stressConfig{
			Workers:     *workers,
			Duration:    *duration,
			BatchSize:   *batchSize,
			SizeDist:    *sizeDist,
			EnableHyper: *hyper,
			Denorm:      *denorm,
			MaxRate:     *maxRate,
		})
	default:
		result, err := runDirectPipeline(ctx, directPipelineConfig{
			Mode:        "direct",
			Rate:        *rate,
			Workers:     *workers,
			Duration:    *duration,
			Messages:    0,
			BatchSize:   *batchSize,
			SizeDist:    *sizeDist,
			EnableHyper: *hyper,
			Denorm:      *denorm,
		})
		if err != nil {
			return err
		}
		printExperimentResult(result)
		return nil
	}
}

func runChaos(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	rate := fs.Int("rate", 1000, "Base messages per second")
	mode := fs.String("mode", "steady", "Mode: steady, burst")
	burstFactor := fs.Int("burst-factor", 50, "Burst multiplier")
	sizeDist := fs.String("size-dist", "heavy-tail", "Size distribution")
	chaosBurst := fs.Bool("chaos-burst", true, "Enable burst spikes")
	chaosSchema := fs.Bool("chaos-schema", true, "Enable schema evolution")
	chaosHotPartition := fs.Bool("chaos-hot-partition", true, "Enable hot partition skew")
	chaosSizeShock := fs.Bool("chaos-size-shock", true, "Enable size shocks")
	hyper := fs.Bool("hyper", true, "Enable HyperType")
	workers := fs.Int("workers", runtime.NumCPU(), "Workers")
	duration := fs.Duration("duration", 30*time.Second, "Chaos test duration")
	if err := fs.Parse(args); err != nil {
		return err
	}

	injector := chaos.NewInjector(chaos.Config{
		BurstEnabled:         *chaosBurst,
		BurstInterval:        10 * time.Second,
		BurstFactorMin:       10,
		BurstFactorMax:       100,
		BurstProbability:     0.2,
		SchemaEvolution:      *chaosSchema,
		SchemaVersions:       []string{"1.0.0", "1.1.0", "2.0.0"},
		SchemaSwapInterval:   30 * time.Second,
		HotPartitionEnabled:  *chaosHotPartition,
		HotPartitionRatio:    0.2,
		HotPartitionCount:    3,
		SizeShockEnabled:     *chaosSizeShock,
		SizeShockInterval:    15 * time.Second,
		SizeShockProbability: 0.1,
	})

	result, err := runStreamExperiment(ctx, streamExperimentConfig{
		Rate:        *rate,
		Workers:     *workers,
		Duration:    *duration,
		BatchSize:   1000,
		SizeDist:    *sizeDist,
		EnableHyper: *hyper,
		Denorm:      true,
		Mode:        *mode,
		BurstFactor: *burstFactor,
		Injector:    injector,
	})
	if err != nil {
		return err
	}
	inj, bursts, swaps, shocks := injector.Stats()
	fmt.Printf("Chaos Results:\n")
	fmt.Printf("  Mode: %s\n", *mode)
	fmt.Printf("  Duration: %v\n", result.Duration)
	fmt.Printf("  Messages (Consumed): %d\n", result.Messages)
	fmt.Printf("  Injections: %d\n", inj)
	fmt.Printf("  Bursts: %d\n", bursts)
	fmt.Printf("  Schema Swaps: %d\n", swaps)
	fmt.Printf("  Size Shocks: %d\n", shocks)
	return nil
}

type directPipelineConfig struct {
	Mode        string
	Rate        int
	Workers     int
	Duration    time.Duration
	Messages    int
	BatchSize   int
	SizeDist    string
	EnableHyper bool
	Denorm      bool
	FixedSize   int
	Corpus      *producer.RawCorpus
}

func runDirectPipeline(parent context.Context, cfg directPipelineConfig) (*ExperimentResult, error) {
	corpusPlan := cfg.Corpus
	preparedHere := false
	if corpusPlan == nil {
		var err error
		corpusPlan, err = producer.PrepareRawCorpus(producer.WireConfig{
			SizeDist:      cfg.SizeDist,
			SchemaVersion: "1.0.0",
		}, prepareCorpusOptions(cfg.Messages, cfg.Rate, cfg.Duration, cfg.FixedSize))
		if err != nil {
			return nil, err
		}
		preparedHere = true
	}
	if preparedHere {
		logPreparedCorpus(cfg.Mode, corpusPlan)
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

	runCtx := parent
	cancel := func() {}
	if cfg.Duration > 0 {
		runCtx, cancel = context.WithTimeout(parent, cfg.Duration)
	}
	defer cancel()

	metrics.Reset()

	var produced atomic.Int64
	var producedBytes atomic.Int64
	var workerErrors atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for workerID := 0; workerID < cfg.Workers; workerID++ {
		wg.Add(1)
		go func(tc *ba.Transcoder, workerIndex int) {
			defer wg.Done()
			shard := shardForWorker(shards, workerIndex, corpus)
			pending := 0
			cursor := 0

			process := func(raw []byte) bool {
				startConsume := time.Now()
				var appendErr error
				if cfg.Denorm {
					appendErr = tc.AppendDenormRaw(raw)
				} else {
					appendErr = tc.AppendRaw(raw)
				}
				if appendErr != nil {
					workerErrors.Add(1)
					return false
				}
				metrics.RecordLatency("consume", startConsume)
				metrics.RecordConsumedThroughput(int64(len(raw)), 1)
				produced.Add(1)
				producedBytes.Add(int64(len(raw)))
				pending++
				if pending >= cfg.BatchSize {
					recordDirectBatch(tc, cfg.Denorm, pending)
					pending = 0
				}
				return true
			}

			if cfg.Messages > 0 {
				target := rateShare(cfg.Messages, workerIndex, cfg.Workers)
				for processed := 0; processed < target; processed++ {
					process(nextRaw(shard, &cursor))
				}
			} else {
				runRateLimitedLoop(runCtx, rateShare(cfg.Rate, workerIndex, cfg.Workers), func() bool {
					return process(nextRaw(shard, &cursor))
				})
				if pending > 0 {
					recordDirectBatch(tc, cfg.Denorm, pending)
				}
				return
			}

			if pending > 0 {
				recordDirectBatch(tc, cfg.Denorm, pending)
			}
		}(transcoders[workerID], workerID)
	}

	wg.Wait()
	elapsed := time.Since(start)
	if cfg.Duration > 0 {
		elapsed = cfg.Duration
	}

	report := metrics.GenerateTelemetryReport()
	return &ExperimentResult{
		Mode:       cfg.Mode,
		Duration:   elapsed,
		Messages:   produced.Load(),
		Bytes:      producedBytes.Load(),
		Errors:     workerErrors.Load(),
		Report:     report,
		RatePerSec: float64(produced.Load()) / elapsed.Seconds(),
	}, nil
}

type streamExperimentConfig struct {
	Rate        int
	Workers     int
	Duration    time.Duration
	BatchSize   int
	SizeDist    string
	EnableHyper bool
	Denorm      bool
	Mode        string
	BurstFactor int
	Injector    *chaos.Injector
}

func runStreamExperiment(parent context.Context, cfg streamExperimentConfig) (*ExperimentResult, error) {
	appCfg := config.Load()
	runID := time.Now().UnixNano()
	appCfg.StreamName = fmt.Sprintf("ARROWFLOW_EXP_%d", runID)
	appCfg.Topic = fmt.Sprintf("arrowflow.test.%d", runID)
	appCfg.ConsumerGroup = fmt.Sprintf("arrowflow-exp-%d", runID)
	appCfg.ConsumerStartAtNew = false
	appCfg.ConsumerPullWorkers = max(1, cfg.Workers)
	appCfg.ConsumerPullBatchSize = min(max(1, cfg.BatchSize), 512)

	pub, err := stream.NewProducer(appCfg)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}
	defer pub.Close()

	sub, err := stream.NewConsumer(appCfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	defer sub.Close()

	wireConsumer, err := consumer.NewWireConsumer(sub, appCfg, consumer.ConsumerConfig{
		Workers:     cfg.Workers,
		OutputMode:  outputMode(cfg.Denorm),
		BatchSize:   cfg.BatchSize,
		EnableHyper: cfg.EnableHyper,
		DenormPaths: consumer.DefaultConsumerConfig.DenormPaths,
	})
	if err != nil {
		return nil, err
	}
	defer wireConsumer.Close()

	var corpus [][]byte
	if cfg.Injector == nil {
		corpusPlan, prepErr := producer.PrepareRawCorpus(producer.WireConfig{
			SizeDist:      cfg.SizeDist,
			SchemaVersion: "1.0.0",
		}, prepareCorpusOptions(0, cfg.Rate, cfg.Duration, 0))
		if prepErr != nil {
			return nil, prepErr
		}
		logPreparedCorpus("stream", corpusPlan)
		corpus = corpusPlan.Messages
		if err := wireConsumer.Warmup(corpus); err != nil {
			return nil, err
		}
	}

	metrics.Reset()

	consumeCtx, cancelConsumer := context.WithCancel(parent)
	defer cancelConsumer()

	consumeErrCh := make(chan error, 1)
	go func() {
		consumeErrCh <- wireConsumer.Run(consumeCtx)
	}()

	if cfg.Injector != nil {
		cfg.Injector.Start(consumeCtx)
	}

	produceCtx, cancelProduce := context.WithTimeout(parent, cfg.Duration)
	defer cancelProduce()

	producerWorkers := max(1, cfg.Workers)
	shards := shardCorpus(corpus, producerWorkers)
	start := time.Now()
	var produced atomic.Int64
	var producedBytes atomic.Int64
	errCh := make(chan error, producerWorkers)
	var wg sync.WaitGroup

	for workerID := 0; workerID < producerWorkers; workerID++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			wp := producer.NewWireProducer(pub, appCfg, producer.WireConfig{
				Mode:          cfg.Mode,
				Rate:          rateShare(cfg.Rate, worker, producerWorkers),
				BurstFactor:   max(cfg.BurstFactor, 1),
				BurstDuration: 5 * time.Second,
				SizeDist:      cfg.SizeDist,
				SchemaVersion: "1.0.0",
			})
			if cfg.Injector != nil {
				wp.AttachInjector(cfg.Injector)
			}

			shard := shardForWorker(shards, worker, corpus)
			cursor := 0
			publishOne := func() bool {
				var raw []byte
				if cfg.Injector != nil {
					rawMsg, rawErr := wp.GenerateRawMessage(0)
					if rawErr != nil {
						return false
					}
					raw = rawMsg
				} else {
					raw = nextRaw(shard, &cursor)
				}

				startPublish := time.Now()
				if err := pub.Publish(produceCtx, appCfg.Topic, &stream.Msg{Payload: raw}); err != nil {
					if produceCtx.Err() != nil {
						return false
					}
					select {
					case errCh <- err:
					default:
					}
					cancelProduce()
					return false
				}
				metrics.RecordLatency("produce", startPublish)
				metrics.RecordProducedThroughput(int64(len(raw)), 1)
				produced.Add(1)
				producedBytes.Add(int64(len(raw)))
				return true
			}

			baseRate := rateShare(cfg.Rate, worker, producerWorkers)
			if baseRate <= 0 {
				for produceCtx.Err() == nil {
					if !publishOne() && produceCtx.Err() != nil {
						return
					}
				}
				return
			}

			runRateLimitedLoop(produceCtx, baseRate, publishOne)
		}(workerID)
	}

	wg.Wait()
	select {
	case produceErr := <-errCh:
		cancelConsumer()
		return nil, produceErr
	default:
	}

	produceElapsed := time.Since(start)
	waitForDrain(wireConsumer, produced.Load(), maxDuration(10*time.Second, cfg.Duration))
	cancelConsumer()
	if consumeErr := <-consumeErrCh; consumeErr != nil {
		return nil, consumeErr
	}

	msgs, bytes, errs, _, _ := wireConsumer.Stats()
	report := metrics.GenerateTelemetryReport()
	return &ExperimentResult{
		Mode:       "stream",
		Duration:   produceElapsed,
		Messages:   msgs,
		Bytes:      bytes,
		Errors:     errs,
		Report:     report,
		RatePerSec: float64(msgs) / produceElapsed.Seconds(),
	}, nil
}

type stressConfig struct {
	Workers     int
	Duration    time.Duration
	BatchSize   int
	SizeDist    string
	EnableHyper bool
	Denorm      bool
	MaxRate     int
}

func runStressCollapse(ctx context.Context, cfg stressConfig) error {
	corpusPlan, err := producer.PrepareRawCorpus(producer.WireConfig{
		SizeDist:      cfg.SizeDist,
		SchemaVersion: "1.0.0",
	}, prepareCorpusOptions(0, cfg.MaxRate, cfg.Duration, 0))
	if err != nil {
		return err
	}
	logPreparedCorpus("stress", corpusPlan)

	rates := []int{1000, 5000, 10000, 25000, 50000, 75000, 100000}
	for _, rate := range rates {
		if rate > cfg.MaxRate {
			continue
		}
		result, err := runDirectPipeline(ctx, directPipelineConfig{
			Mode:        "stress",
			Rate:        rate,
			Workers:     cfg.Workers,
			Duration:    cfg.Duration,
			BatchSize:   cfg.BatchSize,
			SizeDist:    cfg.SizeDist,
			EnableHyper: cfg.EnableHyper,
			Denorm:      cfg.Denorm,
			Corpus:      corpusPlan,
		})
		if err != nil {
			return err
		}
		fmt.Printf("Stress Results:\n")
		fmt.Printf("  Rate Input: %d\n", rate)
		printExperimentResult(result)
	}
	return nil
}

func printExperimentResult(result *ExperimentResult) {
	fmt.Printf("Experiment Results:\n")
	fmt.Printf("  Mode: %s\n", result.Mode)
	fmt.Printf("  Duration: %v\n", result.Duration)
	fmt.Printf("  Messages: %d\n", result.Messages)
	fmt.Printf("  Rate: %.2f msg/s\n", result.RatePerSec)
}

func writeTelemetry(report metrics.TelemetryReport) {
	data, err := report.ToJSON()
	if err != nil {
		return
	}
	_ = os.WriteFile("telemetry.json", data, 0o644)
}

func newTranscoder(enableHyper, denorm bool) (*ba.Transcoder, *ba.HyperType, error) {
	protoPath := findProtoPath()
	protoDir := filepath.Dir(protoPath)
	protoFile := filepath.Base(protoPath)

	fd, err := ba.CompileProtoToFileDescriptor(protoFile, []string{protoDir})
	if err != nil {
		return nil, nil, fmt.Errorf("compile proto: %w", err)
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return nil, nil, fmt.Errorf("get descriptor: %w", err)
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

func prepareCorpusOptions(messages, rate int, duration time.Duration, fixedSize int) producer.CorpusOptions {
	if messages > 0 {
		return producer.CorpusOptions{
			TargetMessages: messages,
			FixedSize:      fixedSize,
			PilotMessages:  1024,
		}
	}

	return producer.CorpusOptions{
		TargetMessages: desiredCorpusMessages(messages, rate, duration),
		FixedSize:      fixedSize,
		PilotMessages:  1024,
		MaxMessages:    65536,
		MaxBytes:       256 << 20,
	}
}

func desiredCorpusMessages(messages, rate int, duration time.Duration) int {
	if messages > 0 {
		return messages
	}
	if rate > 0 && duration > 0 {
		target := int(float64(rate)*duration.Seconds() + 0.5)
		if target > 0 {
			return target
		}
	}
	return 32768
}

func logPreparedCorpus(mode string, corpus *producer.RawCorpus) {
	if corpus == nil {
		return
	}
	log.Printf(
		"Prepared corpus for %s: %d messages replayable (%d desired, %.2f MB total, avg %d bytes)",
		mode,
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
	if len(corpus) == 0 {
		return shards
	}
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
	if len(shard) == 0 {
		return nil
	}
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

func recordDirectBatch(tc *ba.Transcoder, denorm bool, inputMessages int) {
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
	metrics.RecordBatchMetrics(int(rec.NumRows()), int(rec.NumCols()), recordBatchSize(rec), inputMessages)
}

func recordBatchSize(rec arrow.RecordBatch) int64 {
	var total uint64
	for _, col := range rec.Columns() {
		total += col.Data().SizeInBytes()
	}
	return int64(total)
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

func outputMode(denorm bool) string {
	if denorm {
		return "denorm"
	}
	return "nested"
}

func waitForDrain(cons *consumer.WireConsumer, produced int64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs, _, _, _, _ := cons.Stats()
		report := metrics.GenerateTelemetryReport()
		if msgs >= produced && report.Streaming.ConsumerLag == 0 && report.Streaming.BufferDepth == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
