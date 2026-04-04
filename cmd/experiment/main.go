package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/TFMV/arrowflow/internal/metrics"
	"github.com/TFMV/arrowflow/internal/schemas/pb"

	"github.com/apache/arrow-go/v18/arrow/memory"
	ba "github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
)

var (
	mode        = flag.String("mode", "direct", "Experiment mode: direct, stress")
	duration    = flag.Duration("duration", 30*time.Second, "Experiment duration")
	rate        = flag.Int("rate", 10000, "Messages per second")
	workers     = flag.Int("workers", runtime.NumCPU(), "Worker goroutines")
	batchSize   = flag.Int("batch-size", 1000, "Batch size")
	enableHyper = flag.Bool("hyper", true, "Enable HyperType")
	maxRate     = flag.Int("max-rate", 100000, "Max stress rate")
)

func main() {
	flag.Parse()

	log.Printf("Starting ArrowFlow Experiment: %s", *mode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received signal, shutting down...")
		cancel()
	}()

	var result *ExperimentResult
	switch *mode {
	case "stress":
		result = runStressCollapse(ctx)
	case "direct":
		fallthrough
	default:
		result = runDirectIngestion(ctx)
	}

	report := metrics.GenerateTelemetryReport()
	report.PrintSummary()

	log.Printf("=== Experiment Results ===")
	log.Printf("Mode: %s", result.Mode)
	log.Printf("Duration: %.2fs", result.Duration.Seconds())
	log.Printf("Throughput: %.2f msg/s (%.2f MB/s)",
		result.Throughput.MessagesPerSec, result.Throughput.BytesPerSec/1024/1024)
	log.Printf("Errors: %d", result.Errors)

	if result.DegradationPoint != nil {
		log.Printf("=== Degradation Point ===")
		log.Printf("Throughput Ceiling: %.2f msg/s", result.DegradationPoint.ThroughputCeiling)
		log.Printf("Latency Divergence: %.2f ms", result.DegradationPoint.LatencyDivergence)
	}
}

type ExperimentResult struct {
	Mode             string
	Duration         time.Duration
	Throughput       ThroughputResult
	Latency          LatencyResult
	Memory           MemoryResult
	Errors           int64
	DegradationPoint *DegradationPoint
}

type ThroughputResult struct {
	MessagesPerSec float64
	BytesPerSec    float64
	TotalMessages  int64
	TotalBytes     int64
}

type LatencyResult struct {
	P50  float64
	P95  float64
	P99  float64
	P999 float64
}

type MemoryResult struct {
	PeakHeapMB float64
	AvgHeapMB  float64
	GCPauseAvg float64
	TotalGCs   uint32
}

type DegradationPoint struct {
	ThroughputCeiling float64
	LatencyDivergence float64
	GCThrashOnset     bool
}

func runDirectIngestion(ctx context.Context) *ExperimentResult {
	log.Println("Running: Direct Ingestion (No Broker)")

	fd, err := ba.CompileProtoToFileDescriptor("./internal/schemas/event.proto", []string{"."})
	if err != nil {
		log.Fatalf("Compile proto: %v", err)
	}
	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		log.Fatalf("Get descriptor: %v", err)
	}

	var ht *ba.HyperType
	if *enableHyper {
		ht = ba.NewHyperType(md, ba.WithAutoRecompile(100000, 0.01))
	}

	paths := []pbpath.PlanPathSpec{
		pbpath.PlanPath("schema_version"),
		pbpath.PlanPath("event_timestamp"),
		pbpath.PlanPath("user.user_id"),
		pbpath.PlanPath("session.session_id"),
		pbpath.PlanPath("tracing.trace_id"),
		pbpath.PlanPath("payload.event_type"),
	}

	tc, err := ba.New(md, memory.DefaultAllocator, ba.WithHyperType(ht), ba.WithDenormalizerPlan(paths...))
	if err != nil {
		log.Fatalf("Create transcoder: %v", err)
	}
	defer tc.Release()

	var wg sync.WaitGroup
	var totalMessages int64
	var totalBytes int64
	var errors int64

	ratePerWorker := *rate / *workers
	interval := time.Second / time.Duration(ratePerWorker)
	if interval == 0 {
		interval = time.Microsecond
	}

	start := time.Now()
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			var localMsgs int64
			var localBytes int64

			for {
				select {
				case <-ctx.Done():
					atomic.AddInt64(&totalMessages, localMsgs)
					atomic.AddInt64(&totalBytes, localBytes)
					return
				case <-ticker.C:
					msg := generateTestEvent()
					raw, _ := proto.Marshal(msg)

					ts := time.Now()
					if err := tc.AppendDenormRaw(raw); err != nil {
						atomic.AddInt64(&errors, 1)
						continue
					}
					metrics.RecordLatency("consume", ts)

					localMsgs++
					localBytes += int64(len(raw))

					if localMsgs%int64(*batchSize) == 0 {
						rec := tc.NewDenormalizerRecordBatch()
						rec.Release()
					}
				}
			}
		}()
	}

	time.Sleep(*duration)
	wg.Wait()

	elapsed := time.Since(start)
	report := metrics.GenerateTelemetryReport()

	return &ExperimentResult{
		Mode:     "direct_ingestion",
		Duration: elapsed,
		Throughput: ThroughputResult{
			MessagesPerSec: float64(totalMessages) / elapsed.Seconds(),
			BytesPerSec:    float64(totalBytes) / elapsed.Seconds(),
			TotalMessages:  totalMessages,
			TotalBytes:     totalBytes,
		},
		Latency: LatencyResult{
			P50:  report.Latency.Consume.Mean,
			P95:  report.Latency.Consume.P95,
			P99:  report.Latency.Consume.P99,
			P999: report.Latency.Consume.P999,
		},
		Memory: MemoryResult{
			PeakHeapMB: report.Memory.HeapAllocMB,
		},
		Errors: errors,
	}
}

func runStressCollapse(ctx context.Context) *ExperimentResult {
	log.Println("Running: Stress Collapse Mode")

	rates := []int{1000, 5000, 10000, 25000, 50000, 75000, 100000}
	if *maxRate > 0 {
		var filtered []int
		for _, r := range rates {
			if r <= *maxRate {
				filtered = append(filtered, r)
			}
		}
		rates = filtered
	}

	var peakThroughput float64
	var degPoint *DegradationPoint
	var prevP99 float64

	for _, r := range rates {
		log.Printf("Testing rate: %d msg/s", r)
		rate = &r

		result := runDirectIngestion(ctx)

		if result.Throughput.MessagesPerSec > peakThroughput {
			peakThroughput = result.Throughput.MessagesPerSec
		}

		if prevP99 > 0 && result.Latency.P99 > prevP99*2 {
			degPoint = &DegradationPoint{
				ThroughputCeiling: peakThroughput,
				LatencyDivergence: result.Latency.P99,
			}
			log.Printf("Degradation detected at %d msg/s!", r)
			break
		}
		prevP99 = result.Latency.P99
	}

	return &ExperimentResult{
		Mode:             "stress_collapse",
		Duration:         0,
		Throughput:       ThroughputResult{MessagesPerSec: peakThroughput},
		DegradationPoint: degPoint,
	}
}

func generateTestEvent() *pb.Event {
	return &pb.Event{
		SchemaVersion:  "1.0.0",
		EventTimestamp: time.Now().UnixMilli(),
		User: &pb.Event_UserContext{
			UserId:    randUUID(),
			SessionId: randUUID(),
		},
		Session: &pb.Event_SessionContext{
			SessionId: randUUID(),
		},
		Tracing: &pb.Event_TracingContext{
			TraceId: randUUID(),
		},
		Payload: &pb.Event_Payload{
			EventType: "test",
			RawData:   make([]byte, rand.Intn(5000)+100),
		},
	}
}

func randUUID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 36)
	for i := range b {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			b[i] = '-'
		} else {
			b[i] = chars[rand.Intn(len(chars))]
		}
	}
	return string(b)
}
