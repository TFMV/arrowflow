package chaos

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

type Injector struct {
	cfg Config

	active      atomic.Bool
	mu          sync.RWMutex
	schemaVer   string
	burstFactor int
	targetRate  int

	stats struct {
		injections  atomic.Int64
		bursts      atomic.Int64
		schemaSwaps atomic.Int64
		sizeShocks  atomic.Int64
	}
}

type Config struct {
	BurstEnabled     bool
	BurstInterval    time.Duration
	BurstFactorMin   int
	BurstFactorMax   int
	BurstProbability float32

	SchemaEvolution    bool
	SchemaVersions     []string
	SchemaSwapInterval time.Duration

	HotPartitionEnabled bool
	HotPartitionRatio   float32
	HotPartitionCount   int

	LagInjectionEnabled bool
	LagDelayRange       time.Duration

	SizeShockEnabled     bool
	SizeShockInterval    time.Duration
	SizeShockProbability float32
}

var DefaultConfig = Config{
	BurstEnabled:     true,
	BurstInterval:    10 * time.Second,
	BurstFactorMin:   10,
	BurstFactorMax:   100,
	BurstProbability: 0.2,

	SchemaEvolution:    true,
	SchemaVersions:     []string{"1.0.0", "1.1.0", "2.0.0"},
	SchemaSwapInterval: 30 * time.Second,

	HotPartitionEnabled: true,
	HotPartitionRatio:   0.2,
	HotPartitionCount:   3,

	LagInjectionEnabled: false,
	LagDelayRange:       100 * time.Millisecond,

	SizeShockEnabled:     true,
	SizeShockInterval:    15 * time.Second,
	SizeShockProbability: 0.1,
}

func NewInjector(cfg Config) *Injector {
	if len(cfg.SchemaVersions) == 0 {
		cfg.SchemaVersions = []string{"1.0.0"}
	}
	return &Injector{
		cfg:        cfg,
		schemaVer:  cfg.SchemaVersions[0],
		targetRate: 1,
	}
}

func (c *Injector) Start(ctx context.Context) {
	c.active.Store(true)

	if c.cfg.BurstEnabled {
		go c.burstLoop(ctx)
	}

	if c.cfg.SchemaEvolution {
		go c.schemaSwapLoop(ctx)
	}

	if c.cfg.SizeShockEnabled {
		go c.sizeShockLoop(ctx)
	}
}

func (c *Injector) Stop() {
	c.active.Store(false)
}

func (c *Injector) burstLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.BurstInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !shouldFloat(c.cfg.BurstProbability) {
				continue
			}

			factor := randInt(c.cfg.BurstFactorMin, c.cfg.BurstFactorMax)
			c.burstFactor = factor
			c.stats.bursts.Add(1)
			c.stats.injections.Add(1)

			go func() {
				time.Sleep(time.Duration(factor/10) * time.Second)
				c.burstFactor = 0
			}()
		}
	}
}

func (c *Injector) schemaSwapLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.SchemaSwapInterval)
	defer ticker.Stop()

	idx := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idx = (idx + 1) % len(c.cfg.SchemaVersions)
			c.mu.Lock()
			c.schemaVer = c.cfg.SchemaVersions[idx]
			c.mu.Unlock()
			c.stats.schemaSwaps.Add(1)
			c.stats.injections.Add(1)
		}
	}
}

func (c *Injector) sizeShockLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.SizeShockInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if shouldFloat(c.cfg.SizeShockProbability) {
				c.stats.sizeShocks.Add(1)
				c.stats.injections.Add(1)
			}
		}
	}
}

func (c *Injector) ApplyRate(baseRate int) int {
	if c.burstFactor > 0 {
		return baseRate * c.burstFactor
	}
	return baseRate
}

func (c *Injector) GetSchemaVersion() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.schemaVer
}

func (c *Injector) GenerateKey() string {
	if !c.cfg.HotPartitionEnabled {
		return randomKey(16)
	}

	if shouldFloat(c.cfg.HotPartitionRatio) {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(c.cfg.HotPartitionCount)))
		return "hot-" + string('0'+byte(n.Int64()))
	}
	return "cold-" + randomKey(16)
}

func (c *Injector) ApplyLag(ctx context.Context) {
	if !c.cfg.LagInjectionEnabled {
		return
	}
	delay := c.cfg.LagDelayRange
	time.Sleep(time.Duration(randInt(0, int(delay.Milliseconds()))) * time.Millisecond)
}

func (c *Injector) ShouldApplySizeShock() bool {
	return c.stats.sizeShocks.Load()%2 == 1
}

func (c *Injector) Stats() (injections, bursts, schemaSwaps, sizeShocks int64) {
	return c.stats.injections.Load(),
		c.stats.bursts.Load(),
		c.stats.schemaSwaps.Load(),
		c.stats.sizeShocks.Load()
}

func shouldFloat(prob float32) bool {
	n, _ := rand.Int(rand.Reader, big.NewInt(10000))
	return int(n.Int64()) < int(prob*10000)
}

func randInt(min, max int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	return int(n.Int64()) + min
}

func randomKey(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		b[i] = chars[n.Int64()]
	}
	return string(b)
}

type LatencyInjector struct {
	enabled    bool
	delayRange time.Duration
	injectors  int64
}

func NewLatencyInjector(delayRange time.Duration) *LatencyInjector {
	return &LatencyInjector{
		delayRange: delayRange,
	}
}

func (l *LatencyInjector) Enable() {
	l.enabled = true
}

func (l *LatencyInjector) Disable() {
	l.enabled = false
}

func (l *LatencyInjector) Inject() {
	if !l.enabled {
		return
	}
	atomic.AddInt64(&l.injectors, 1)
	delay := time.Duration(randInt(0, int(l.delayRange.Milliseconds()))) * time.Millisecond
	time.Sleep(delay)
}

func (l *LatencyInjector) Count() int64 {
	return atomic.LoadInt64(&l.injectors)
}

type ErrorInjector struct {
	enabled    bool
	errorRate  float32
	errors     int64
	mu         sync.Mutex
	errorTypes []errorType
}

type errorType struct {
	name string
	fn   func() error
}

func NewErrorInjector(errorRate float32) *ErrorInjector {
	return &ErrorInjector{
		errorRate: errorRate,
		errorTypes: []errorType{
			{name: "timeout", fn: func() error { return context.DeadlineExceeded }},
			{name: "cancelled", fn: func() error { return context.Canceled }},
			{name: "temporarily_unavailable", fn: func() error { return ErrTemporarilyUnavailable }},
		},
	}
}

var ErrTemporarilyUnavailable = &temporarilyUnavailable{}

type temporarilyUnavailable struct{}

func (e *temporarilyUnavailable) Error() string {
	return "temporarily unavailable"
}

func (e *ErrorInjector) Enable() {
	e.enabled = true
}

func (e *ErrorInjector) Disable() {
	e.enabled = false
}

func (e *ErrorInjector) Inject() error {
	if !e.enabled {
		return nil
	}

	if !shouldFloat(e.errorRate) {
		return nil
	}

	atomic.AddInt64(&e.errors, 1)

	idx := randInt(0, len(e.errorTypes))
	return e.errorTypes[idx].fn()
}

func (e *ErrorInjector) ErrorCount() int64 {
	return atomic.LoadInt64(&e.errors)
}

type PartitionSkewInjector struct {
	enabled          bool
	targetPartitions []int
	skewFactor       float32
}

func NewPartitionSkewInjector(partitions []int, skewFactor float32) *PartitionSkewInjector {
	return &PartitionSkewInjector{
		targetPartitions: partitions,
		skewFactor:       skewFactor,
	}
}

func (p *PartitionSkewInjector) Inject(key string) string {
	if !p.enabled {
		return key
	}

	if shouldFloat(p.skewFactor) && len(p.targetPartitions) > 0 {
		idx := randInt(0, len(p.targetPartitions))
		return "partition-" + string(rune('0'+p.targetPartitions[idx]))
	}
	return key
}
