package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/TFMV/arrowflow/internal/chaos"
	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/metrics"
	"github.com/TFMV/arrowflow/internal/producer"
	"github.com/TFMV/arrowflow/internal/schemas/pb"
	"github.com/TFMV/arrowflow/internal/stream"

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

	if err := prod.Run(ctx); err != nil {
		return err
	}

	msgs, bytes, _, _ := prod.Stats()
	log.Printf("Produced: %d messages, %d bytes", msgs, bytes)
	return nil
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

	fd, err := ba.CompileProtoToFileDescriptor("./internal/schemas/event.proto", []string{"."})
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
			rec := tc.NewDenormalizerRecordBatch()
			rec.Release()
		}
		return nil
	})

	log.Printf("Consumed: %d messages", totalMsgs)
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
	mode := flag.String("mode", "direct", "Mode: direct, stress")
	rate := flag.Int("rate", 10000, "Messages per second")
	workers := flag.Int("workers", runtime.NumCPU(), "Workers")
	duration := flag.Duration("duration", 30*time.Second, "Duration")
	hyper := flag.Bool("hyper", true, "Enable HyperType")
	flag.Parse()

	fd, err := ba.CompileProtoToFileDescriptor("./internal/schemas/event.proto", []string{"."})
	if err != nil {
		return fmt.Errorf("compile proto: %w", err)
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return fmt.Errorf("get descriptor: %w", err)
	}

	var ht *ba.HyperType
	if *hyper {
		ht = ba.NewHyperType(md)
	}

	paths := []pbpath.PlanPathSpec{
		pbpath.PlanPath("schema_version"),
		pbpath.PlanPath("event_timestamp"),
		pbpath.PlanPath("user.user_id"),
		pbpath.PlanPath("session.session_id"),
	}

	tc, err := ba.New(md, memory.DefaultAllocator, ba.WithHyperType(ht), ba.WithDenormalizerPlan(paths...))
	if err != nil {
		return fmt.Errorf("create transcoder: %w", err)
	}
	defer tc.Release()

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

	for i := 0; i < *workers; i++ {
		go func() {
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
					msg := &pb.Event{
						SchemaVersion: "1.0.0",
						User:          &pb.Event_UserContext{UserId: fmt.Sprintf("user-%d", rand.Int())},
						Session:       &pb.Event_SessionContext{SessionId: fmt.Sprintf("sess-%d", rand.Int())},
					}
					raw, _ := proto.Marshal(msg)
					ts := time.Now()
					tc.AppendDenormRaw(raw)
					metrics.RecordLatency("consume", ts)
					atomic.AddInt64(&totalMsgs, 1)
					atomic.AddInt64(&totalBytes, int64(len(raw)))
				}
			}
		}()
	}

	time.Sleep(*duration)

	elapsed := time.Since(start)
	rateCalc := float64(totalMsgs) / elapsed.Seconds()

	fmt.Printf("Experiment Results:\n")
	fmt.Printf("  Mode: %s\n", *mode)
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Printf("  Messages: %d\n", totalMsgs)
	fmt.Printf("  Rate: %.2f msg/s\n", rateCalc)
	return nil
}

func runChaos(ctx context.Context) error {
	rate := flag.Int("rate", 1000, "Base messages per second")
	mode := flag.String("mode", "steady", "Mode: steady, burst")
	burstFactor := flag.Int("burst-factor", 50, "Burst multiplier")
	sizeDist := flag.String("size-dist", "heavy-tail", "Size distribution")
	chaosBurst := flag.Bool("chaos-burst", true, "Enable burst spikes")
	chaosSchema := flag.Bool("chaos-schema", true, "Enable schema evolution")
	chaosHotPartition := flag.Bool("chaos-hot-partition", true, "Enable hot partition skew")
	chaosSizeShock := flag.Bool("chaos-size-shock", true, "Enable size shocks")
	flag.Parse()

	cfg := config.Load()

	p, err := stream.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer p.Close()

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

	wc := producer.WireConfig{
		Mode:          *mode,
		Rate:          *rate,
		BurstFactor:   *burstFactor,
		BurstDuration: 5 * time.Second,
		SizeDist:      *sizeDist,
		SchemaVersion: "1.0.0",
	}

	prod := producer.NewWireProducer(p, cfg, wc)
	log.Printf("Chaos Producer: rate=%d, mode=%s, chaos=enabled", *rate, *mode)

	if err := prod.Run(ctx); err != nil {
		return err
	}

	msgs, bytes, _, _ := prod.Stats()
	inj, bursts, swaps, _ := injector.Stats()

	fmt.Printf("Chaos Results:\n")
	fmt.Printf("  Messages: %d\n", msgs)
	fmt.Printf("  Bytes: %d\n", bytes)
	fmt.Printf("  Injections: %d\n", inj)
	fmt.Printf("  Bursts: %d\n", bursts)
	fmt.Printf("  Schema Swaps: %d\n", swaps)
	return nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
}
