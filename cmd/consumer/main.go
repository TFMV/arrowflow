package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/consumer"
	"github.com/TFMV/arrowflow/internal/stream"
)

func main() {
	workers := flag.Int("workers", runtime.NumCPU(), "Number of worker goroutines")
	outputMode := flag.String("mode", "denorm", "Output mode: nested, denorm")
	batchSize := flag.Int("batch-size", 1000, "Batch size before flushing")
	enableHyper := flag.Bool("hyper", true, "Enable HyperType for faster parsing")
	flag.Parse()

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	s, err := stream.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer s.Close()

	cc := consumer.ConsumerConfig{
		Workers:     *workers,
		OutputMode:  *outputMode,
		BatchSize:   *batchSize,
		EnableHyper: *enableHyper,
		DenormPaths: []string{
			"schema_version",
			"event_timestamp",
			"user.user_id",
			"session.session_id",
			"tracing.trace_id",
			"payload.event_type",
			"enrichment.geo.country",
			"enrichment.geo.city",
		},
	}

	cons, err := consumer.NewWireConsumer(s, cfg, cc)
	if err != nil {
		log.Fatalf("Failed to create wire consumer: %v", err)
	}
	defer cons.Close()

	log.Printf("Starting wire consumer: workers=%d, mode=%s, batch=%d, hyper=%v",
		*workers, *outputMode, *batchSize, *enableHyper)

	if err := cons.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Consumer error: %v", err)
	}

	msgs, bytes, errors, rows, batches := cons.Stats()
	log.Printf("Stats: messages=%d, bytes=%d, errors=%d, rows=%d, batches=%d",
		msgs, bytes, errors, rows, batches)
}
