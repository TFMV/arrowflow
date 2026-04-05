package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/stream"
)

func main() {
	messages := flag.Int("messages", 100000, "Total messages to publish")
	size := flag.Int("size", 4096, "Message size in bytes")
	parallel := flag.Int("parallel", runtime.NumCPU(), "Parallel publishers")
	flag.Parse()

	cfg := config.Load()
	p, err := stream.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer p.Close()

	payload := make([]byte, *size)
	perWorker := *messages / max(*parallel, 1)
	remainder := *messages % max(*parallel, 1)

	ctx := context.Background()
	start := time.Now()
	var total atomic.Int64
	var wg sync.WaitGroup
	for workerID := 0; workerID < *parallel; workerID++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			count := perWorker
			if worker < remainder {
				count++
			}
			for i := 0; i < count; i++ {
				if err := p.Publish(ctx, cfg.Topic, &stream.Msg{Payload: payload}); err != nil {
					log.Printf("Publish error: %v", err)
					return
				}
				total.Add(1)
			}
		}(workerID)
	}
	wg.Wait()

	elapsed := time.Since(start)
	msgs := total.Load()
	mbps := float64(msgs*int64(*size)) / elapsed.Seconds() / 1024 / 1024

	fmt.Printf("Results:\n")
	fmt.Printf("  Messages: %d\n", msgs)
	fmt.Printf("  Time: %v\n", elapsed)
	fmt.Printf("  Rate: %.2f msg/s\n", float64(msgs)/elapsed.Seconds())
	fmt.Printf("  Throughput: %.2f MB/s\n", mbps)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
