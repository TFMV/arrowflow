package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/arrowflow/internal/chaos"
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

	chaosBurst := flag.Bool("chaos-burst", true, "Enable burst spikes")
	chaosSchema := flag.Bool("chaos-schema", true, "Enable schema evolution mid-stream")
	chaosHotPartition := flag.Bool("chaos-hot-partition", true, "Enable hot partition skew")
	chaosSizeShock := flag.Bool("chaos-size-shock", true, "Enable random size shocks")
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

	injector := chaos.NewInjector(chaos.Config{
		BurstEnabled:     *chaosBurst,
		BurstInterval:    10 * time.Second,
		BurstFactorMin:   10,
		BurstFactorMax:   100,
		BurstProbability: 0.2,

		SchemaEvolution:    *chaosSchema,
		SchemaVersions:     []string{"1.0.0", "1.1.0", "2.0.0"},
		SchemaSwapInterval: 30 * time.Second,

		HotPartitionEnabled: *chaosHotPartition,
		HotPartitionRatio:   0.2,
		HotPartitionCount:   3,

		SizeShockEnabled:     *chaosSizeShock,
		SizeShockInterval:    15 * time.Second,
		SizeShockProbability: 0.1,
	})

	injector.Start(ctx)

	wc := producer.WireConfig{
		Mode:          *mode,
		Rate:          *rate,
		BurstFactor:   *burstFactor,
		BurstDuration: 5 * time.Second,
		SizeDist:      *sizeDist,
		SchemaVersion: *schemaVersion,
	}

	prod := producer.NewWireProducer(p, cfg, wc)

	log.Printf("Starting chaos wire producer: mode=%s, rate=%d, chaos=%v",
		*mode, *rate, *chaosBurst || *chaosSchema || *chaosHotPartition || *chaosSizeShock)

	if err := prod.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Producer error: %v", err)
	}

	msgs, bytes, errors, _ := prod.Stats()
	inj, bursts, schemaSwaps, sizeShocks := injector.Stats()

	log.Printf("Stats: messages=%d, bytes=%d, errors=%d", msgs, bytes, errors)
	log.Printf("Chaos: injections=%d, bursts=%d, schema_swaps=%d, size_shocks=%d", inj, bursts, schemaSwaps, sizeShocks)
}
