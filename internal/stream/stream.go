package stream

import (
	"context"
	"time"

	"github.com/TFMV/arrowflow/internal/config"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func NewProducer(cfg *config.Config) (Publisher, error) {
	return NewNATSProducer(cfg)
}

func NewConsumer(cfg *config.Config) (Subscriber, error) {
	return NewNATSConsumer(cfg)
}

type natsProducer struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

func NewNATSProducer(cfg *config.Config) (Publisher, error) {
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &natsProducer{conn: nc, js: js}, nil
}

func (p *natsProducer) Publish(ctx context.Context, topic string, msg *Msg) error {
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
	nc, err := nats.Connect(cfg.NATSURL)
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
	stream, err := c.js.Stream(ctx, c.conf.Topic)
	if err != nil {
		stream, err = c.js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     c.conf.Topic,
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

	msgs, err := cons.Fetch(10)
	if err != nil {
		return err
	}

	for msg := range msgs.Messages() {
		m := &Msg{
			Payload:   msg.Data(),
			Timestamp: time.Now().UnixMilli(),
		}
		if err := handler(m); err != nil {
			msg.Nak()
		} else {
			msg.Ack()
		}
	}

	return msgs.Error()
}

func (c *natsConsumer) Close() error {
	c.conn.Close()
	return nil
}
