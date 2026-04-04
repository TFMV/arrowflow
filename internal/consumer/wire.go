package consumer

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/metrics"
	"github.com/TFMV/arrowflow/internal/schemas/pb"
	"github.com/TFMV/arrowflow/internal/stream"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ba "github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
)

type OutputMode int

const (
	OutputModeNested OutputMode = iota
	OutputModeDenorm
)

type WireConsumer struct {
	subscriber stream.Subscriber
	cfg        *config.Config
	topic      string

	workers     int
	outputMode  OutputMode
	batchSize   int
	enableHyper bool

	ht          *ba.HyperType
	transcoders []*ba.Transcoder

	stats struct {
		messages atomic.Int64
		bytes    atomic.Int64
		errors   atomic.Int64
		rows     atomic.Int64
		batches  atomic.Int64
	}

	inputChan chan []byte
	ctx       context.Context
	cancel    context.CancelFunc
}

type ConsumerConfig struct {
	Workers     int
	OutputMode  string
	BatchSize   int
	EnableHyper bool
	DenormPaths []string
}

var DefaultConsumerConfig = ConsumerConfig{
	Workers:     runtime.NumCPU(),
	OutputMode:  "denorm",
	BatchSize:   1000,
	EnableHyper: true,
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

func NewWireConsumer(s stream.Subscriber, cfg *config.Config, cc ConsumerConfig) (*WireConsumer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	protoPath := "./internal/schemas/event.proto"
	fd, err := ba.CompileProtoToFileDescriptor(protoPath, []string{"."})
	if err != nil {
		return nil, fmt.Errorf("compile proto: %w", err)
	}

	md, err := ba.GetMessageDescriptorByName(fd, "Event")
	if err != nil {
		return nil, fmt.Errorf("get message descriptor: %w", err)
	}

	var ht *ba.HyperType
	if cc.EnableHyper {
		ht = ba.NewHyperType(md, ba.WithAutoRecompile(100_000, 0.01))
	}

	opts := []ba.Option{
		ba.WithHyperType(ht),
	}

	if cc.OutputMode == "denorm" {
		paths := make([]pbpath.PlanPathSpec, len(cc.DenormPaths))
		for i, p := range cc.DenormPaths {
			paths[i] = pbpath.PlanPath(p)
		}
		opts = append(opts, ba.WithDenormalizerPlan(paths...))
	}

	tc, err := ba.New(md, memory.DefaultAllocator, opts...)
	if err != nil {
		return nil, fmt.Errorf("create transcoder: %w", err)
	}

	transcoders := []*ba.Transcoder{tc}
	for i := 1; i < cc.Workers; i++ {
		clone, err := tc.Clone(memory.NewGoAllocator())
		if err != nil {
			return nil, fmt.Errorf("clone transcoder: %w", err)
		}
		transcoders = append(transcoders, clone)
	}

	return &WireConsumer{
		subscriber:  s,
		cfg:         cfg,
		topic:       cfg.Topic,
		workers:     cc.Workers,
		outputMode:  parseOutputMode(cc.OutputMode),
		batchSize:   cc.BatchSize,
		enableHyper: cc.EnableHyper,
		ht:          ht,
		transcoders: transcoders,
		inputChan:   make(chan []byte, cc.BatchSize*cc.Workers),
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

func parseOutputMode(m string) OutputMode {
	switch m {
	case "nested":
		return OutputModeNested
	default:
		return OutputModeDenorm
	}
}

func (wc *WireConsumer) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for i := 0; i < wc.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			wc.processWorker(workerID)
		}(i)
	}

	err := wc.subscriber.Consume(ctx, wc.topic, func(msg *stream.Msg) error {
		select {
		case <-wc.ctx.Done():
			return wc.ctx.Err()
		default:
			wc.inputChan <- msg.Payload
			wc.stats.bytes.Add(int64(len(msg.Payload)))
			wc.stats.messages.Add(1)
			return nil
		}
	})

	close(wc.inputChan)
	wg.Wait()

	if wc.ht != nil && wc.ht.Profile() != nil {
		wc.ht.RecompileAsync()
	}

	return err
}

func (wc *WireConsumer) processWorker(workerID int) {
	tc := wc.transcoders[workerID]
	if tc == nil {
		return
	}
	defer tc.Release()

	var batchCount int
	for raw := range wc.inputChan {
		var err error
		switch wc.outputMode {
		case OutputModeDenorm:
			err = tc.AppendDenormRaw(raw)
		default:
			err = tc.AppendRaw(raw)
		}

		if err != nil {
			wc.stats.errors.Add(1)
			continue
		}

		if tc.NewRecordBatch().NumRows() >= int64(wc.batchSize) {
			var rec arrow.RecordBatch
			switch wc.outputMode {
			case OutputModeDenorm:
				rec = tc.NewDenormalizerRecordBatch()
			default:
				rec = tc.NewRecordBatch()
			}

			if rec.NumRows() > 0 {
				metrics.BytesConsumed.Add(float64(rec.NumRows() * rec.NumCols() * 100))
				metrics.MessagesConsumed.Add(float64(rec.NumRows()))
				wc.stats.rows.Add(rec.NumRows())
				wc.stats.batches.Add(1)
				batchCount++
			}
			rec.Release()
		}
	}

	finalBatch := func() arrow.RecordBatch {
		switch wc.outputMode {
		case OutputModeDenorm:
			return tc.NewDenormalizerRecordBatch()
		default:
			return tc.NewRecordBatch()
		}
	}()

	if finalBatch.NumRows() > 0 {
		metrics.BytesConsumed.Add(float64(finalBatch.NumRows() * finalBatch.NumCols() * 100))
		metrics.MessagesConsumed.Add(float64(finalBatch.NumRows()))
		wc.stats.rows.Add(finalBatch.NumRows())
		wc.stats.batches.Add(1)
	}
	finalBatch.Release()

	log.Printf("Worker %d: processed %d batches", workerID, batchCount)
}

func (wc *WireConsumer) Stats() (messages, bytes, errors, rows, batches int64) {
	return wc.stats.messages.Load(),
		wc.stats.bytes.Load(),
		wc.stats.errors.Load(),
		wc.stats.rows.Load(),
		wc.stats.batches.Load()
}

func (wc *WireConsumer) Close() error {
	wc.cancel()
	for _, tc := range wc.transcoders {
		if tc != nil {
			tc.Release()
		}
	}
	return wc.subscriber.Close()
}

type LegacyConsumer struct {
	subscriber stream.Subscriber
	cfg        *config.Config
	topic      string

	totalMessages int
	totalBytes    int
}

func NewLegacyConsumer(s stream.Subscriber, cfg *config.Config) *LegacyConsumer {
	return &LegacyConsumer{
		subscriber: s,
		cfg:        cfg,
		topic:      cfg.Topic,
	}
}

func (c *LegacyConsumer) Run(ctx context.Context) error {
	return c.subscriber.Consume(ctx, c.topic, func(msg *stream.Msg) error {
		var evt pb.Event
		if err := proto.Unmarshal(msg.Payload, &evt); err != nil {
			return err
		}

		c.totalMessages++
		c.totalBytes += len(msg.Payload)

		metrics.MessagesConsumed.Inc()
		metrics.BytesConsumed.Add(float64(len(msg.Payload)))

		return nil
	})
}

func (c *LegacyConsumer) TotalMessages() int {
	return c.totalMessages
}

func (c *LegacyConsumer) TotalBytes() int {
	return c.totalBytes
}
