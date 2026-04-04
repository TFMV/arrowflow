package stream

import (
	"context"
)

type Msg struct {
	Key       []byte
	Payload   []byte
	Timestamp int64
	Headers   map[string][]byte
}

type Publisher interface {
	Publish(ctx context.Context, topic string, msg *Msg) error
	Close() error
}

type Subscriber interface {
	Consume(ctx context.Context, topic string, handler func(msg *Msg) error) error
	Close() error
}
