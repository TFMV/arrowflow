package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/stream"
)

var (
	messages = flag.Int("messages", 100000, "Total messages to produce")
	size     = flag.Int("size", 4096, "Message size in bytes")
	parallel = flag.Int("parallel", runtime.NumCPU(), "Parallel producers")
)

func main() {
	flag.Parse()

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := stream.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer p.Close()

	topic := cfg.Topic

	log.Printf("Benchmarking: messages=%d, size=%d, parallel=%d", *messages, *size, *parallel)

	payload := make([]byte, *size)

	start := time.Now()
	total := 0

	for i := 0; i < *messages; i++ {
		msg := &stream.Msg{
			Payload:   payload,
			Timestamp: time.Now().UnixMilli(),
		}
		if err := p.Publish(ctx, topic, msg); err != nil {
			log.Printf("Publish error: %v", err)
			break
		}
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
}
