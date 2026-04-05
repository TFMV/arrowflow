package stream

import (
	"context"
	"log"
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
		nats.MaxReconnects(-1),            // Infinite reconnects for stability
		nats.ReconnectWait(2 * time.Second), // Wait 2s between attempts
		nats.RetryOnFailedConnect(true),   // Wait for NATS to be available
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
		log.Printf("Warning: JetStream not available: %v", err)
		// Try without JetStream - will use direct publish
		return &natsProducer{conn: nc, js: nil}, nil
	}

	// Try to create stream if it doesn't exist
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamName := "ARROWFLOW"
	topic := cfg.Topic

	// Check if stream exists
	if _, err := js.Stream(ctx, streamName); err != nil {
		// Stream doesn't exist, create it
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{
			Name:      streamName,
			Subjects:  []string{topic},
			Retention: jetstream.LimitsPolicy,
			Storage:   jetstream.MemoryStorage,
			MaxBytes:  1024 * 1024 * 100, // 100MB
		})
		if err != nil {
			log.Printf("Warning: could not create NATS stream: %v", err)
			// Continue without JetStream - will use direct publish
			return &natsProducer{conn: nc, js: nil}, nil
		}
		log.Printf("Created NATS stream: %s with subject: %s", streamName, topic)
	}

	return &natsProducer{conn: nc, js: js}, nil
}

func (p *natsProducer) Publish(ctx context.Context, topic string, msg *Msg) error {
	// Try JetStream first, fall back to direct publish
	if p.js != nil {
		_, err := p.js.Publish(ctx, topic, msg.Payload)
		if err == nil {
			return nil
		}
		// If JetStream fails, try direct publish
		log.Printf("JetStream publish failed: %v, trying direct publish", err)
	}

	// Fallback: direct nats publish (no JetStream)
	if p.conn != nil {
		return p.conn.Publish(topic, msg.Payload)
	}

	return nil
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
	streamName := "ARROWFLOW"
	stream, err := c.js.Stream(ctx, streamName)
	if err != nil {
		// Fallback: try to create it if it doesn't exist
		stream, err = c.js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     streamName,
			Subjects: []string{c.conf.Topic},
		})
		if err != nil {
			return err
		}
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   c.conf.ConsumerGroup,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	// Start metrics polling
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		// Initial check
		if status, err := cons.Info(ctx); err == nil {
			metrics.SetConsumerLag(int64(status.NumPending))
			metrics.SetBufferDepth(int64(status.NumAckPending))
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				status, err := cons.Info(ctx)
				if err == nil {
					metrics.SetConsumerLag(int64(status.NumPending))
					metrics.SetBufferDepth(int64(status.NumAckPending))
				}
			}
		}
	}()

	iter, err := cons.Consume(func(msg jetstream.Msg) {
		m := &Msg{
			Payload:   msg.Data(),
			Timestamp: time.Now().UnixNano(), // Use nanoseconds for consistency
		}
		if err := handler(m); err != nil {
			msg.Term() // Serious error, stop retrying this message
		} else {
			msg.Ack()
		}
	})
	if err != nil {
		return err
	}

	// Wait for context cancellation
	<-ctx.Done()
	iter.Stop()
	return ctx.Err()
}

func (c *natsConsumer) Close() error {
	c.conn.Close()
	return nil
}
