package stream

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func NewProducer(cfg *config.Config) (Publisher, error) {
	return NewNATSProducer(cfg)
}

func NewConsumer(cfg *config.Config) (Subscriber, error) {
	return NewNATSConsumer(cfg)
}

func connectNATS(cfg *config.Config, name string) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.RetryOnFailedConnect(true),
		nats.Timeout(5 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("NATS disconnected (%s): %v", name, err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS reconnected (%s) to %v", name, nc.ConnectedUrl())
		}),
	}

	return nats.Connect(cfg.NATSURL, opts...)
}

type natsProducer struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

func NewNATSProducer(cfg *config.Config) (Publisher, error) {
	nc, err := connectNATS(cfg, "ArrowFlow Producer")
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := ensureStream(ctx, js, cfg); err != nil {
		nc.Close()
		return nil, err
	}

	return &natsProducer{conn: nc, js: js}, nil
}

func (p *natsProducer) Publish(ctx context.Context, topic string, msg *Msg) error {
	if p.js == nil {
		return nats.ErrNoServers
	}
	_, err := p.js.Publish(ctx, topic, msg.Payload)
	return err
}

func (p *natsProducer) Close() error {
	p.conn.Close()
	return nil
}

type natsConsumer struct {
	conn *nats.Conn
	js   jetstream.JetStream
	conf *config.Config
}

func NewNATSConsumer(cfg *config.Config) (Subscriber, error) {
	nc, err := connectNATS(cfg, "ArrowFlow Consumer")
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &natsConsumer{conn: nc, js: js, conf: cfg}, nil
}

func (c *natsConsumer) Consume(ctx context.Context, topic string, handler func(msg *Msg) error) error {
	if err := ensureStream(ctx, c.js, c.conf); err != nil {
		return err
	}

	stream, err := c.js.Stream(ctx, c.conf.StreamName)
	if err != nil {
		return err
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:           c.conf.ConsumerGroup,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           c.conf.ConsumerAckWait,
		MaxAckPending:     c.conf.ConsumerMaxAckPending,
		FilterSubject:     topic,
		MaxWaiting:        max(2, c.conf.ConsumerPullWorkers*2),
		MaxRequestBatch:   max(1, c.conf.ConsumerPullBatchSize),
		MaxRequestExpires: fetchMaxWait(c.conf),
	}
	if c.conf.ConsumerStartAtNew {
		consumerCfg.DeliverPolicy = jetstream.DeliverNewPolicy
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return err
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go pollConsumerMetrics(consumeCtx, cons)

	fetchWorkers := max(1, c.conf.ConsumerPullWorkers)
	errCh := make(chan error, fetchWorkers)
	var wg sync.WaitGroup
	for worker := 0; worker < fetchWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.pullLoop(consumeCtx, cons, max(1, c.conf.ConsumerPullBatchSize), fetchMaxWait(c.conf), handler, errCh)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-consumeCtx.Done():
		<-done
		return nil
	case err := <-errCh:
		cancel()
		<-done
		return err
	}
}

func (c *natsConsumer) Close() error {
	c.conn.Close()
	return nil
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg *config.Config) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.StreamName,
		Subjects:  []string{cfg.Topic},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   streamStorage(cfg),
		Replicas:  cfg.StreamReplicas,
		MaxBytes:  cfg.StreamMaxBytes,
	})
	return err
}

func pollConsumerMetrics(ctx context.Context, cons jetstream.Consumer) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	record := func() {
		status, err := cons.Info(ctx)
		if err != nil {
			return
		}
		metrics.SetConsumerLag(int64(status.NumPending))
		metrics.SetBufferDepth(int64(status.NumAckPending))
	}

	record()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			record()
		}
	}
}

func (c *natsConsumer) pullLoop(ctx context.Context, cons jetstream.Consumer, batchSize int, maxWait time.Duration, handler func(msg *Msg) error, errCh chan<- error) {
	for {
		if ctx.Err() != nil {
			return
		}

		batch, err := cons.Fetch(batchSize, jetstream.FetchMaxWait(maxWait))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			sendConsumeErr(errCh, err)
			return
		}

		for jsMsg := range batch.Messages() {
			msg := wrapJetStreamMessage(jsMsg)
			if err := handler(msg); err != nil && !msg.Settled() {
				if nakErr := msg.Nak(); nakErr != nil {
					log.Printf("NATS negative ack failed: %v", nakErr)
				}
			}
			if ctx.Err() != nil {
				return
			}
		}

		if batchErr := batch.Error(); batchErr != nil &&
			!errors.Is(batchErr, jetstream.ErrNoMessages) &&
			!errors.Is(batchErr, nats.ErrTimeout) {
			if ctx.Err() != nil {
				return
			}
			sendConsumeErr(errCh, batchErr)
			return
		}
	}
}

func wrapJetStreamMessage(jsMsg jetstream.Msg) *Msg {
	headers := make(map[string][]byte, len(jsMsg.Headers()))
	for key, values := range jsMsg.Headers() {
		if len(values) == 0 {
			continue
		}
		headers[key] = []byte(values[0])
	}

	return &Msg{
		Payload:   append([]byte(nil), jsMsg.Data()...),
		Subject:   jsMsg.Subject(),
		Timestamp: time.Now().UnixNano(),
		Headers:   headers,
		ackFn:     jsMsg.Ack,
		nakFn:     jsMsg.Nak,
		termFn:    jsMsg.Term,
	}
}

func sendConsumeErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func fetchMaxWait(cfg *config.Config) time.Duration {
	if cfg.ConsumerFetchMaxWait > 0 {
		return cfg.ConsumerFetchMaxWait
	}
	return 250 * time.Millisecond
}

func streamStorage(cfg *config.Config) jetstream.StorageType {
	if cfg.StreamStorage == "memory" {
		return jetstream.MemoryStorage
	}
	return jetstream.FileStorage
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
