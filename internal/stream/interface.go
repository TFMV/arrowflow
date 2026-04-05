package stream

import (
	"context"
	"errors"
	"sync"
)

type Msg struct {
	Key       []byte
	Payload   []byte
	Subject   string
	Timestamp int64
	Headers   map[string][]byte

	mu      sync.Mutex
	settled bool
	ackFn   func() error
	nakFn   func() error
	termFn  func() error
}

var ErrNoSettlementHandler = errors.New("stream: settlement handler not available")

func (m *Msg) Ack() error {
	return m.settle(m.ackFn)
}

func (m *Msg) Nak() error {
	return m.settle(m.nakFn)
}

func (m *Msg) Term() error {
	return m.settle(m.termFn)
}

func (m *Msg) Settled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settled
}

func (m *Msg) settle(fn func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.settled {
		return nil
	}
	if fn == nil {
		return ErrNoSettlementHandler
	}
	if err := fn(); err != nil {
		return err
	}
	m.settled = true
	return nil
}

type Publisher interface {
	Publish(ctx context.Context, topic string, msg *Msg) error
	Close() error
}

type Subscriber interface {
	Consume(ctx context.Context, topic string, handler func(msg *Msg) error) error
	Close() error
}
