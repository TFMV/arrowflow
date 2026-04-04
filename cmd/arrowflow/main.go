package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
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
	"github.com/TFMV/arrowflow/internal/metrics"
	"github.com/TFMV/arrowflow/internal/producer"
	"github.com/TFMV/arrowflow/internal/schemas/pb"
	"github.com/TFMV/arrowflow/internal/stream"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ba "github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
)

func main() {
	flag.Usage = func() {
		fmt.Printf("ArrowFlow - Streaming Protobuf → Arrow Pipeline\n\n")
		fmt.Printf("Usage: arrowflow <command> [options]\n\n")
		fmt.Printf("Commands:\n")
		fmt.Printf("  producer    Run streaming producer\n")
		fmt.Printf("  consumer    Run streaming consumer\n")
		fmt.Printf("  benchmark   Run throughput benchmark\n")
		fmt.Printf("  experiment  Run experiment modes\n")
		fmt.Printf("  chaos       Run chaos injection producer\n\n")
		fmt.Printf("Examples:\n")
		fmt.Printf("  arrowflow producer --rate 100000 --mode burst\n")
		fmt.Printf("  arrowflow consumer --batch-size 1024 --hypertype\n")
		fmt.Printf("  arrowflow benchmark --messages 1000000 --mode stress\n")
		fmt.Printf("  arrowflow experiment --mode direct --rate 50000\n")
		fmt.Printf("  arrowflow chaos --rate 10000 --chaos-burst\n\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	cmd := flag.Arg(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received signal, shutting down...")
		cancel()
	}()

	metrics.StartTelemetryLoop(5*time.Second, "telemetry.json")

	var err error
	switch cmd {
	case "producer":
		err = runProducer(ctx)
	case "consumer":
		err = runConsumer(ctx)
	case "benchmark":
		err = runBenchmark(ctx)
	case "experiment":
		err = runExperiment(ctx)
	case "chaos":
		err = runChaos(ctx)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		flag.Usage()
		os.Exit(1)
	}

	if err != nil && err != context.Canceled {
		log.Fatalf("Error: %v", err)
	}

	report := metrics.GenerateTelemetryReport()
	report.PrintSummary()
}

func runProducer(ctx context.Context) error {
	rate := flag.Int("rate", 1000, "Messages per second")
	mode := flag.String("mode", "steady", "Mode: steady, burst, sinusoidal")
	sizeDist := flag.String("size-dist", "heavy-tail", "Size: small, medium, large, heavy-tail")
	burstFactor := flag.Int("burst-factor", 50, "Burst multiplier")
	schemaVer := flag.String("schema-version", "1.0.0", "Schema version")
	flag.Parse()

	cfg := config.Load()

	p, err := stream.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer p.Close()

	wc := producer.WireConfig{
		Mode:          *mode,
		Rate:          *rate,
		BurstFactor:   *burstFactor,
		BurstDuration: 5 * time.Second,
		SizeDist:      *sizeDist,
		SchemaVersion: *schemaVer,
	}

	prod := producer.NewWireProducer(p, cfg, wc)
	log.Printf("Producer: rate=%d, mode=%s, size=%s", *rate, *mode, *sizeDist)

	err = prod.Run(ctx)
	msgs, bytes, _, _ := prod.Stats()
	log.Printf("Produced: %d messages, %d bytes", msgs, bytes)
	return err
}

func runConsumer(ctx context.Context) error {
	workers := flag.Int("workers", runtime.NumCPU(), "Worker goroutines")
	batchSize := flag.Int("batch-size", 1000, "Batch size")
	mode := flag.String("mode", "denorm", "Mode: nested, denorm")
	hyper := flag.Bool("hypertype", true, "Enable HyperType")
	flag.Parse()

	cfg := config.Load()

	s, err := stream.NewConsumer(cfg)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer s.Close()

	protoPath := findProtoPath()
	protoDir := filepath.Dir(protoPath)
	protoFile := filepath.Base(protoPath)

	fd, err := ba.CompileProtoToFileDescriptor(protoFile, []string{protoDir})
	if err != nil {
		return fmt.Errorf("compile proto: %w", err)
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return fmt.Errorf("get descriptor: %w", err)
	}

	var ht *ba.HyperType
	if *hyper {
		ht = ba.NewHyperType(md, ba.WithAutoRecompile(100_000, 0.01))
	}

	paths := []pbpath.PlanPathSpec{
		pbpath.PlanPath("schema_version"),
		pbpath.PlanPath("event_timestamp"),
		pbpath.PlanPath("user.user_id"),
		pbpath.PlanPath("session.session_id"),
		pbpath.PlanPath("tracing.trace_id"),
		pbpath.PlanPath("payload.event_type"),
	}

	tc, err := ba.New(md, memory.DefaultAllocator,
		ba.WithHyperType(ht),
		ba.WithDenormalizerPlan(paths...),
	)
	if err != nil {
		return fmt.Errorf("create transcoder: %w", err)
	}
	defer tc.Release()

	log.Printf("Consumer: workers=%d, batch=%d, mode=%s, hyper=%v", *workers, *batchSize, *mode, *hyper)

	var totalMsgs int64
	err = s.Consume(ctx, cfg.Topic, func(msg *stream.Msg) error {
		ts := time.Now()
		if err := tc.AppendDenormRaw(msg.Payload); err != nil {
			return err
		}
		metrics.RecordLatency("consume", ts)
		atomic.AddInt64(&totalMsgs, 1)

		if totalMsgs%int64(*batchSize) == 0 {
			startBatch := time.Now()
			rec := tc.NewDenormalizerRecordBatch()
			metrics.RecordLatency("batch_output", startBatch)
			rec.Release()
		}
		return nil
	})

	log.Printf("Consumed: %d messages", atomic.LoadInt64(&totalMsgs))
	return err
}

func runBenchmark(ctx context.Context) error {
	messages := flag.Int("messages", 100000, "Total messages")
	size := flag.Int("size", 4096, "Message size")
	mode := flag.String("mode", "steady", "Mode: steady, stress")
	flag.Parse()

	fd, err := ba.CompileProtoToFileDescriptor("./internal/schemas/event.proto", []string{"."})
	if err != nil {
		return fmt.Errorf("compile proto: %w", err)
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return fmt.Errorf("get descriptor: %w", err)
	}

	ht := ba.NewHyperType(md)
	paths := []pbpath.PlanPathSpec{pbpath.PlanPath("user.user_id"), pbpath.PlanPath("payload.event_type")}

	tc, err := ba.New(md, memory.DefaultAllocator, ba.WithHyperType(ht), ba.WithDenormalizerPlan(paths...))
	if err != nil {
		return fmt.Errorf("create transcoder: %w", err)
	}
	defer tc.Release()

	log.Printf("Benchmark: messages=%d, size=%d, mode=%s", *messages, *size, *mode)

	start := time.Now()
	total := 0

	for i := 0; i < *messages; i++ {
		msg := &pb.Event{
			SchemaVersion: "1.0.0",
			User:          &pb.Event_UserContext{UserId: fmt.Sprintf("user-%d", i)},
			Payload:       &pb.Event_Payload{EventType: "test", RawData: make([]byte, *size)},
		}
		raw, _ := proto.Marshal(msg)
		tc.AppendDenormRaw(raw)
		total++
	}

	elapsed := time.Since(start)
	rate := float64(total) / elapsed.Seconds()
	mbps := float64(total**size) / elapsed.Seconds() / 1024 / 1024

	fmt.Printf("Results:\n")
	fmt.Printf("  Messages: %d\n", total)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Rate: %.2f msg/s\n", rate)
	fmt.Printf("  Throughput: %.2f MB/s\n", mbps)
	return nil
}

func runExperiment(ctx context.Context) error {
	// Create a custom flag set to properly parse experiment-specific flags
	fs := flag.NewFlagSet("experiment", flag.ExitOnError)
	mode := fs.String("mode", "direct", "Mode: direct, stress, stream")
	rate := fs.Int("rate", 10000, "Messages per second")
	workers := fs.Int("workers", runtime.NumCPU(), "Workers")
	duration := fs.Duration("duration", 30*time.Second, "Duration")
	hyper := fs.Bool("hyper", true, "Enable HyperType")
	batchSize := fs.Int("batch-size", 1000, "Batch size for Arrow records")
	sizeDist := fs.String("size-dist", "heavy-tail", "Size distribution (small, medium, large, heavy-tail)")
	denorm := fs.Bool("denorm", true, "Enable denormalization (vs nested)")

	// Get unparsed args (skip the command name)
	args := os.Args[2:]
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Printf("[DEBUG] mode=%s, rate=%d, workers=%d, duration=%v, hyper=%v\n", *mode, *rate, *workers, *duration, *hyper)

	// Find project root (where internal/schemas exists)
	protoPath := findProtoPath()
	protoDir := filepath.Dir(protoPath)
	protoFile := filepath.Base(protoPath)

	fd, err := ba.CompileProtoToFileDescriptor(protoFile, []string{protoDir})
	if err != nil {
		return fmt.Errorf("compile proto: %w", err)
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return fmt.Errorf("get descriptor: %w", err)
	}

	paths := []pbpath.PlanPathSpec{
		pbpath.PlanPath("schema_version"),
		pbpath.PlanPath("event_timestamp"),
		pbpath.PlanPath("user.user_id"),
		pbpath.PlanPath("session.session_id"),
	}

	log.Printf("Experiment: mode=%s, rate=%d, workers=%d, duration=%v", *mode, *rate, *workers, *duration)

	var totalMsgs int64
	var totalBytes int64

	ratePerWorker := *rate / *workers
	interval := time.Second / time.Duration(ratePerWorker)
	if interval == 0 {
		interval = time.Microsecond
	}

	start := time.Now()
	deadline := start.Add(*duration)

	if *mode == "stream" {
		return runStreamExperiment(ctx, *rate, *duration, *workers, *hyper, *batchSize, *sizeDist, *denorm)
	}

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			// Initialize WireProducer for this worker to generate data
			cfg := config.Load()
			wp := producer.NewWireProducer(nil, cfg, producer.WireConfig{
				SizeDist: *sizeDist,
			})

			htWorker := ba.NewHyperType(md)
			var opts []ba.Option
			opts = append(opts, ba.WithHyperType(htWorker))
			if *denorm {
				opts = append(opts, ba.WithDenormalizerPlan(paths...))
			}

			tc, err := ba.New(md, memory.DefaultAllocator, opts...)
			if err != nil {
				log.Printf("worker %d: create transcoder: %v", workerID, err)
				return
			}
			defer tc.Release()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if time.Now().After(deadline) {
						return
					}
					
					evt := wp.GenerateWireMessage()
					raw, _ := proto.Marshal(evt)
					
					start := time.Now()
					if *denorm {
						tc.AppendDenormRaw(raw)
					} else {
						tc.AppendRaw(raw)
					}
					metrics.RecordLatency("consume", start)
					atomic.AddInt64(&totalMsgs, 1)
					atomic.AddInt64(&totalBytes, int64(len(raw)))
				}
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(start)
	rateCalc := float64(totalMsgs) / elapsed.Seconds()

	fmt.Printf("Experiment Results:\n")
	fmt.Printf("  Mode: %s\n", *mode)
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Printf("  Messages: %d\n", totalMsgs)
	fmt.Printf("  Rate: %.2f msg/s\n", rateCalc)
	return nil
}

func runStreamExperiment(ctx context.Context, rate int, duration time.Duration, workers int, hyper bool, batchSize int, sizeDist string, denorm bool) error {
	log.Printf("Running Stream Experiment (NATS): rate=%d, duration=%v, workers=%d, batch=%d, dist=%s, denorm=%v", rate, duration, workers, batchSize, sizeDist, denorm)
	cfg := config.Load()

	p, err := stream.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer p.Close()

	s, err := stream.NewConsumer(cfg)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer s.Close()

	protoPath := findProtoPath()
	protoDir := filepath.Dir(protoPath)
	protoFile := filepath.Base(protoPath)

	fd, err := ba.CompileProtoToFileDescriptor(protoFile, []string{protoDir})
	if err != nil {
		return err
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return err
	}

	var ht *ba.HyperType
	if hyper {
		// HyperType benefits from higher recompile threshold for large batches
		ht = ba.NewHyperType(md, ba.WithAutoRecompile(100_000, 0.01))
	}

	paths := []pbpath.PlanPathSpec{
		pbpath.PlanPath("schema_version"),
		pbpath.PlanPath("event_timestamp"),
		pbpath.PlanPath("user.user_id"),
		pbpath.PlanPath("session.session_id"),
	}

	var opts []ba.Option
	opts = append(opts, ba.WithHyperType(ht))
	if denorm {
		opts = append(opts, ba.WithDenormalizerPlan(paths...))
	}

	tc, err := ba.New(md, memory.DefaultAllocator, opts...)
	if err != nil {
		return err
	}
	defer tc.Release()

	var totalMsgs int64
	var wg sync.WaitGroup

	// Consumer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Consume(ctx, cfg.Topic, func(msg *stream.Msg) error {
			start := time.Now()
			var err error
			if denorm {
				err = tc.AppendDenormRaw(msg.Payload)
			} else {
				err = tc.AppendRaw(msg.Payload)
			}
			if err != nil {
				return err
			}
			metrics.RecordLatency("consume", start)
			metrics.RecordThroughput(int64(len(msg.Payload)), 1)
			msgCount := atomic.AddInt64(&totalMsgs, 1)

			if msgCount%int64(batchSize) == 0 {
				startBatch := time.Now()
				var rec arrow.Record
				if denorm {
					rec = tc.NewDenormalizerRecordBatch()
				} else {
					rec = tc.NewRecordBatch()
				}
				
				// Record detailed metrics
				metrics.RecordBatchMetrics(int(rec.NumRows()), int(rec.NumCols()), 0, int(rec.NumRows()))
				metrics.RecordLatency("batch_output", startBatch)
				rec.Release()
			}
			return nil
		})
	}()

	// Producer loop
	producerRate := rate
	interval := time.Second / time.Duration(producerRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	deadline := start.Add(duration)

	// Initialize WireProducer to generate realistic data based on sizeDist
	wp := producer.NewWireProducer(nil, cfg, producer.WireConfig{
		SizeDist: sizeDist,
	})

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			if time.Now().After(deadline) {
				break loop
			}
			evt := wp.GenerateWireMessage()
			raw, _ := proto.Marshal(evt)
			
			publishStart := time.Now()
			p.Publish(ctx, cfg.Topic, &stream.Msg{Payload: raw})
			metrics.RecordLatency("produce", publishStart)
		}
	}

	time.Sleep(2 * time.Second) // Let consumer catch up
	
	elapsed := time.Since(start)
	rateCalc := float64(atomic.LoadInt64(&totalMsgs)) / elapsed.Seconds()

	fmt.Printf("Experiment Results (NATS):\n")
	fmt.Printf("  Mode: stream\n")
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Printf("  Messages: %d\n", atomic.LoadInt64(&totalMsgs))
	fmt.Printf("  Rate: %.2f msg/s\n", rateCalc)

	return nil
}

func runChaos(ctx context.Context) error {
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

	args := os.Args[2:]
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	cfg := config.Load()

	p, err := stream.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer p.Close()

	s, err := stream.NewConsumer(cfg)
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}
	defer s.Close()

	protoPath := findProtoPath()
	protoDir := filepath.Dir(protoPath)
	protoFile := filepath.Base(protoPath)

	fd, err := ba.CompileProtoToFileDescriptor(protoFile, []string{protoDir})
	if err != nil {
		return err
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return err
	}

	var ht *ba.HyperType
	if *hyper {
		ht = ba.NewHyperType(md, ba.WithAutoRecompile(100_000, 0.01))
	}

	paths := []pbpath.PlanPathSpec{
		pbpath.PlanPath("schema_version"),
		pbpath.PlanPath("event_timestamp"),
		pbpath.PlanPath("user.user_id"),
		pbpath.PlanPath("session.session_id"),
	}

	tc, err := ba.New(md, memory.DefaultAllocator, ba.WithHyperType(ht), ba.WithDenormalizerPlan(paths...))
	if err != nil {
		return err
	}
	defer tc.Release()

	injector := chaos.NewInjector(chaos.Config{
		BurstEnabled:        *chaosBurst,
		BurstInterval:       10 * time.Second,
		BurstFactorMin:      10,
		BurstFactorMax:      100,
		SchemaEvolution:     *chaosSchema,
		SchemaVersions:      []string{"1.0.0", "1.1.0", "2.0.0"},
		SchemaSwapInterval:  30 * time.Second,
		HotPartitionEnabled: *chaosHotPartition,
		HotPartitionRatio:   0.2,
		HotPartitionCount:   3,
		SizeShockEnabled:    *chaosSizeShock,
		SizeShockInterval:   15 * time.Second,
	})
	injector.Start(ctx)

	var totalMsgsConsumed int64
	var wg sync.WaitGroup

	// Background consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Consume(ctx, cfg.Topic, func(msg *stream.Msg) error {
			start := time.Now()
			if err := tc.AppendDenormRaw(msg.Payload); err != nil {
				return err
			}
			metrics.RecordLatency("consume", start)
			atomic.AddInt64(&totalMsgsConsumed, 1)

			if atomic.LoadInt64(&totalMsgsConsumed)%1000 == 0 {
				startBatch := time.Now()
				rec := tc.NewDenormalizerRecordBatch()
				metrics.RecordLatency("batch_output", startBatch)
				rec.Release()
			}
			return nil
		})
	}()

	wc := producer.WireConfig{
		Mode:          *mode,
		Rate:          *rate,
		BurstFactor:   *burstFactor,
		BurstDuration: 5 * time.Second,
		SizeDist:      *sizeDist,
		SchemaVersion: "1.0.0",
	}

	prod := producer.NewWireProducer(p, cfg, wc)
	log.Printf("Chaos Producer: rate=%d, mode=%s, workers=%d, hyper=%v", *rate, *mode, *workers, *hyper)

	err = prod.Run(ctx)

	time.Sleep(2 * time.Second) // Let consumer catch up

	msgs, bytes, _, _ := prod.Stats()
	inj, bursts, swaps, _ := injector.Stats()

	fmt.Printf("Chaos Results:\n")
	fmt.Printf("  Messages (Produced): %d\n", msgs)
	fmt.Printf("  Messages (Consumed): %d\n", atomic.LoadInt64(&totalMsgsConsumed))
	fmt.Printf("  Bytes: %d\n", bytes)
	fmt.Printf("  Injections: %d\n", inj)
	fmt.Printf("  Bursts: %d\n", bursts)
	fmt.Printf("  Schema Swaps: %d\n", swaps)
	return nil
}

func findProtoPath() string {
	// Check common locations relative to binary and current dir
	searchPaths := []string{
		"./internal/schemas/event.proto",
		"internal/schemas/event.proto",
		"../internal/schemas/event.proto",
		"cmd/arrowflow/internal/schemas/event.proto",
	}

	// Also check absolute paths from known project locations
	exePath, err := os.Executable()
	if err == nil {
		exeDir := filepath.Dir(exePath)
		searchPaths = append(searchPaths,
			filepath.Join(exeDir, "internal/schemas/event.proto"),
			filepath.Join(exeDir, "..", "internal/schemas/event.proto"),
			filepath.Join(exeDir, "..", "..", "internal/schemas/event.proto"),
		)
	}

	for _, p := range searchPaths {
		if _, err := os.Stat(p); err == nil {
			absPath, _ := filepath.Abs(p)
			log.Printf("Found proto at: %s", absPath)
			return absPath
		}
	}

	// Fallback - try current working dir
	cwd, _ := os.Getwd()
	fallback := filepath.Join(cwd, "internal/schemas/event.proto")
	log.Printf("Using fallback proto path: %s", fallback)
	return fallback
}

func init() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
}
