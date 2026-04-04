package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/producer"
	"github.com/TFMV/arrowflow/internal/stream"
)

func main() {
	mode := flag.String("mode", "steady", "Production mode: steady, burst, sinusoidal")
	rate := flag.Int("rate", 1000, "Messages per second")
	burstFactor := flag.Int("burst-factor", 50, "Burst multiplier")
	sizeDist := flag.String("size-dist", "heavy-tail", "Size distribution: small, medium, large, heavy-tail")
	schemaVersion := flag.String("schema-version", "1.0.0", "Schema version")
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

	p, err := stream.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer p.Close()

	wc := producer.WireConfig{
		Mode:          *mode,
		Rate:          *rate,
		BurstFactor:   *burstFactor,
		BurstDuration: 5 * time.Second,
		SizeDist:      *sizeDist,
		SchemaVersion: *schemaVersion,
	}

	prod := producer.NewWireProducer(p, cfg, wc)

	log.Printf("Starting wire producer: mode=%s, rate=%d, size-dist=%s, schema=%s",
		*mode, *rate, *sizeDist, *schemaVersion)

	if err := prod.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Producer error: %v", err)
	}

	msgs, bytes, errors, bursts := prod.Stats()
	log.Printf("Stats: messages=%d, bytes=%d, errors=%d, bursts=%d", msgs, bytes, errors, bursts)
}
