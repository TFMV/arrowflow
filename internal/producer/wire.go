package producer

import (
	"context"
	"log"
	mrand "math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TFMV/arrowflow/internal/chaos"
	"github.com/TFMV/arrowflow/internal/config"
	"github.com/TFMV/arrowflow/internal/metrics"
	pb "github.com/TFMV/arrowflow/internal/schemas/pb"
	"github.com/TFMV/arrowflow/internal/stream"
	"google.golang.org/protobuf/proto"
)

var rngPool = sync.Pool{
	New: func() any {
		return mrand.New(mrand.NewSource(time.Now().UnixNano()))
	},
}

type Mode int

const (
	ModeSteady Mode = iota
	ModeBurst
	ModeSinusoidal
)

type SizeDistribution int

const (
	SizeUniform SizeDistribution = iota
	SizeSmall
	SizeMedium
	SizeLarge
	SizeHeavyTail
)

type WireProducer struct {
	publisher stream.Publisher
	cfg       *config.Config
	topic     string

	mode          Mode
	sizeDist      SizeDistribution
	rate          int
	burstFactor   int
	burstDuration time.Duration
	schemaVersion string
	injector      *chaos.Injector

	stats struct {
		messages atomic.Int64
		bytes    atomic.Int64
		errors   atomic.Int64
		bursts   atomic.Int64
	}

	ctx    context.Context
	cancel context.CancelFunc
}

type WireConfig struct {
	Mode          string
	Rate          int
	BurstFactor   int
	BurstDuration time.Duration
	SizeDist      string
	SchemaVersion string
	HotPartition  float32
}

var DefaultWireConfig = WireConfig{
	Mode:          "steady",
	Rate:          1000,
	BurstFactor:   50,
	BurstDuration: 5 * time.Second,
	SizeDist:      "heavy-tail",
	SchemaVersion: "1.0.0",
	HotPartition:  0.2,
}

func NewWireProducer(p stream.Publisher, cfg *config.Config, wc WireConfig) *WireProducer {
	ctx, cancel := context.WithCancel(context.Background())
	topic := "arrowflow-events"
	if cfg != nil {
		topic = cfg.Topic
	}
	return &WireProducer{
		publisher:     p,
		cfg:           cfg,
		topic:         topic,
		mode:          parseMode(wc.Mode),
		sizeDist:      parseSizeDist(wc.SizeDist),
		rate:          wc.Rate,
		burstFactor:   wc.BurstFactor,
		burstDuration: wc.BurstDuration,
		schemaVersion: wc.SchemaVersion,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func parseMode(m string) Mode {
	switch m {
	case "burst":
		return ModeBurst
	case "sinusoidal":
		return ModeSinusoidal
	default:
		return ModeSteady
	}
}

func parseSizeDist(s string) SizeDistribution {
	switch s {
	case "small":
		return SizeSmall
	case "medium":
		return SizeMedium
	case "large":
		return SizeLarge
	case "heavy-tail":
		return SizeHeavyTail
	default:
		return SizeUniform
	}
}

func (wp *WireProducer) Run(ctx context.Context) error {
	switch wp.mode {
	case ModeBurst:
		return wp.runBurst(ctx)
	case ModeSinusoidal:
		return wp.runSinusoidal(ctx)
	default:
		return wp.runSteady(ctx)
	}
}

func (wp *WireProducer) AttachInjector(injector *chaos.Injector) {
	wp.injector = injector
}

func (wp *WireProducer) runSteady(ctx context.Context) error {
	interval := time.Second / time.Duration(wp.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := wp.emitBatch(ctx, wp.effectiveBatchCount(1)); err != nil {
				return err
			}
		}
	}
}

func (wp *WireProducer) runBurst(ctx context.Context) error {
	ticker := time.NewTicker(wp.burstDuration)
	defer ticker.Stop()

	steadyInterval := time.Second / time.Duration(wp.rate)
	steadyTicker := time.NewTicker(steadyInterval)
	defer steadyTicker.Stop()

	bursting := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			bursting = !bursting
			if bursting {
				wp.stats.bursts.Add(1)
			}
		default:
			if bursting {
				burstInterval := time.Second / time.Duration(wp.rate*wp.burstFactor)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(burstInterval):
					if err := wp.emitBatch(ctx, wp.effectiveBatchCount(wp.burstFactor)); err != nil {
						return err
					}
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-steadyTicker.C:
					if err := wp.emitBatch(ctx, wp.effectiveBatchCount(1)); err != nil {
						return err
					}
				}
			}
		}
	}
}

func (wp *WireProducer) runSinusoidal(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var phase float64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			phase += 0.01
			factor := 1.0 + float64(wp.burstFactor)*0.1*(1+sinus(phase))
			rate := int(float64(wp.rate) * factor)

			if rate > wp.rate*wp.burstFactor {
				wp.stats.bursts.Add(1)
			}

			for i := 0; i < rate/100; i++ {
				if err := wp.emitBatch(ctx, wp.effectiveBatchCount(1)); err != nil {
					return err
				}
			}
		}
	}
}

func sinus(x float64) float64 {
	return (sinApprox(x*6.28318) + sinApprox(x*12.56636)) / 2
}

func sinApprox(x float64) float64 {
	x = x - 6.28318*float64(int(x/6.28318))
	if x > 3.14159 {
		x -= 6.28318
	}
	return x - (x*x*x)/6 + (x*x*x*x*x)/120
}

func (wp *WireProducer) emitBatch(ctx context.Context, count int) error {
	for i := 0; i < count; i++ {
		msg := wp.GenerateWireMessage()
		if wp.injector != nil {
			msg.SchemaVersion = wp.injector.GetSchemaVersion()
		}
		wireBytes, err := proto.Marshal(msg)
		if err != nil {
			wp.stats.errors.Add(1)
			continue
		}

		start := time.Now()
		if wp.injector != nil {
			wp.injector.ApplyLag(ctx)
		}
		err = wp.publisher.Publish(ctx, wp.topic, &stream.Msg{
			Key:     []byte(wp.generateKey()),
			Payload: wireBytes,
		})
		if err != nil {
			wp.stats.errors.Add(1)
			log.Printf("[PRODUCER] Publish error: %v", err)
			continue
		}

		metrics.RecordLatency("produce", start)
		metrics.RecordProducedThroughput(int64(len(wireBytes)), 1)
		wp.stats.messages.Add(1)
		wp.stats.bytes.Add(int64(len(wireBytes)))
	}
	return nil
}

func (wp *WireProducer) GenerateWireMessage() *pb.Event {
	cfg := stressConfig
	switch wp.sizeDist {
	case SizeSmall:
		cfg.LargePayloadProb = 0
		cfg.NumMetrics = 1
		cfg.NumTags = 2
		cfg.MaxPageViews = 1
		cfg.MaxClicks = 1
		cfg.MaxErrors = 0
	case SizeMedium:
		cfg.LargePayloadProb = 0.05
		cfg.NumMetrics = 3
		cfg.NumTags = 5
		cfg.MaxPageViews = 3
		cfg.MaxClicks = 5
		cfg.MaxErrors = 1
	case SizeLarge:
		cfg.LargePayloadProb = 0.8
		cfg.NumMetrics = 10
		cfg.NumTags = 20
		cfg.MaxPageViews = 10
		cfg.MaxClicks = 20
		cfg.MaxErrors = 5
	case SizeHeavyTail:
		cfg.LargePayloadProb = 0.15
		cfg.NumMetrics = randInt(1, 15)
		cfg.NumTags = randInt(5, 30)
		cfg.MaxPageViews = randInt(0, 15)
		cfg.MaxClicks = randInt(0, 25)
		cfg.MaxErrors = randInt(0, 10)
	default:
		cfg.LargePayloadProb = 0.1
	}

	if wp.injector != nil && wp.injector.ShouldApplySizeShock() {
		cfg.LargePayloadProb = 1
		cfg.NumMetrics = maxInt(cfg.NumMetrics, 20)
		cfg.NumTags = maxInt(cfg.NumTags, 40)
	}

	evt := genEvent(cfg)
	if wp.injector != nil {
		evt.SchemaVersion = wp.injector.GetSchemaVersion()
	} else {
		evt.SchemaVersion = wp.schemaVersion
	}
	return evt
}

var stressConfig = StressConfig{
	IncludeUser:       0.7,
	IncludeSession:    0.6,
	IncludeTracing:    0.5,
	IncludeEnrichment: 0.4,
	IncludePayload:    0.8,
	IncludeMetrics:    0.5,
	LargePayloadProb:  0.1,
	NumMetrics:        5,
	NumTags:           10,
	MaxPageViews:      10,
	MaxClicks:         20,
	MaxErrors:         5,
}

type StressConfig struct {
	IncludeUser       float32
	IncludeSession    float32
	IncludeTracing    float32
	IncludeEnrichment float32
	IncludePayload    float32
	IncludeMetrics    float32
	LargePayloadProb  float32
	NumMetrics        int
	NumTags           int
	MaxPageViews      int
	MaxClicks         int
	MaxErrors         int
}

func genEvent(cfg StressConfig) *pb.Event {
	evt := &pb.Event{
		SchemaVersion:  "1.0.0",
		EventTimestamp: time.Now().UnixMilli(),
	}

	if should(cfg.IncludeUser) {
		evt.User = genUserContext()
	}
	if should(cfg.IncludeSession) {
		evt.Session = genSessionContext(cfg)
	}
	if should(cfg.IncludeTracing) {
		evt.Tracing = genTracingContext()
	}
	if should(cfg.IncludeEnrichment) {
		evt.Enrichment = genEnrichment()
	}
	if should(cfg.IncludePayload) {
		evt.Payload = genPayload(cfg)
	}
	if should(cfg.IncludeMetrics) {
		evt.Metrics = genMetrics(cfg.NumMetrics)
	}
	evt.Tags = genTags(cfg.NumTags)

	return evt
}

func should(prob float32) bool {
	rng := rngPool.Get().(*mrand.Rand)
	defer rngPool.Put(rng)
	return rng.Intn(10000) < int(prob*10000)
}

func randInt(min, max int) int {
	if max <= min {
		return min
	}
	rng := rngPool.Get().(*mrand.Rand)
	defer rngPool.Put(rng)
	return rng.Intn(max-min) + min
}

func randInt64(min, max int64) int64 {
	if max <= min {
		return min
	}
	rng := rngPool.Get().(*mrand.Rand)
	defer rngPool.Put(rng)
	return rng.Int63n(max-min) + min
}

func randFloat(min, max float64) float64 {
	f := float64(randInt(0, 10000)) / 10000.0
	return min + f*(max-min)
}

func randString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	rng := rngPool.Get().(*mrand.Rand)
	defer rngPool.Put(rng)
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rng.Intn(len(chars))]
	}
	return string(b)
}

func randUUID() string {
	return randString(8) + "-" + randString(4) + "-" + randString(4) + "-" + randString(4) + "-" + randString(12)
}

func genUserContext() *pb.Event_UserContext {
	profile := &pb.Event_UserContext_Profile{
		Email:    randString(20) + "@example.com",
		Username: randString(12),
		Roles:    []string{"user", "premium"},
		Preferences: map[string]string{
			"theme": "dark", "language": "en",
		},
		Account: &pb.Event_UserContext_Profile_Account{
			AccountId:   randUUID(),
			Tier:        "premium",
			CreatedAt:   randInt64(1600000000000, 1700000000000),
			Permissions: []string{"read", "write"},
			Billing: &pb.Event_UserContext_Profile_Account_Billing{
				PaymentMethod: "card",
				Currency:      "USD",
				Invoices: []*pb.Event_UserContext_Profile_Account_Billing_Invoice{
					{
						InvoiceId: randUUID(),
						Amount:    randInt64(1000, 100000),
						Status:    "paid",
					},
				},
			},
		},
	}
	return &pb.Event_UserContext{
		UserId:    randUUID(),
		SessionId: randUUID(),
		DeviceId:  randUUID(),
		IpAddress: "192.168.1." + randString(3),
		Attributes: map[string]string{
			"region": "us-east-1", "tenant": "default",
		},
		Profile: profile,
	}
}

func genSessionContext(cfg StressConfig) *pb.Event_SessionContext {
	nav := &pb.Event_SessionContext_Navigation{}
	nav.PageViews = make([]*pb.Event_SessionContext_Navigation_PageView, randInt(0, cfg.MaxPageViews))
	for i := range nav.PageViews {
		nav.PageViews[i] = &pb.Event_SessionContext_Navigation_PageView{
			Url:        "https://example.com/page/" + randString(8),
			Title:      "Page " + randString(10),
			Timestamp:  randInt64(1700000000000, 1800000000000),
			DurationMs: int32(randInt(100, 60000)),
		}
	}
	nav.Clicks = make([]*pb.Event_SessionContext_Navigation_ClickEvent, randInt(0, cfg.MaxClicks))
	for i := range nav.Clicks {
		nav.Clicks[i] = &pb.Event_SessionContext_Navigation_ClickEvent{
			ElementId:   "elem-" + randString(6),
			ElementType: "button",
			X:           int32(randInt(0, 1920)),
			Y:           int32(randInt(0, 1080)),
			Timestamp:   randInt64(1700000000000, 1800000000000),
		}
	}
	return &pb.Event_SessionContext{
		SessionId:    randUUID(),
		StartedAt:    randInt64(1700000000000, 1800000000000),
		LastActivity: time.Now().UnixMilli(),
		EntryUrl:     "https://example.com",
		Navigation:   nav,
	}
}

func genTracingContext() *pb.Event_TracingContext {
	return &pb.Event_TracingContext{
		TraceId:       randUUID(),
		SpanId:        randString(16),
		ParentSpanId:  randString(16),
		ServiceName:   "arrowflow-ingest",
		OperationName: "process_event",
		StartTime:     randInt64(1700000000000, 1800000000000),
		DurationNs:    randInt64(1000, 10000000),
		Tags:          map[string]string{"version": "1.0.0", "env": "production"},
	}
}

func genEnrichment() *pb.Event_Enrichment {
	return &pb.Event_Enrichment{
		Geo: &pb.Event_Enrichment_Geolocation{
			Country:   "US",
			Region:    "California",
			City:      "San Francisco",
			Timezone:  "America/Los_Angeles",
			Latitude:  randFloat(37.0, 38.0),
			Longitude: randFloat(-122.5, -122.0),
			Isp:       "Comcast",
		},
		Ua: &pb.Event_Enrichment_UserAgentInfo{
			Browser:    "Chrome",
			Os:         "Mac OS X",
			DeviceType: "desktop",
			IsMobile:   false,
			IsBot:      false,
		},
		Device: &pb.Event_Enrichment_DeviceInfo{
			DeviceId:   randUUID(),
			DeviceType: "desktop",
			Os:         "Mac OS X",
		},
		AbTest: &pb.Event_Enrichment_ABTest{
			ExperimentId: randUUID(),
			VariantId:    "variant_a",
		},
		Fraud: &pb.Event_Enrichment_FraudDetection{
			RiskScore: int32(randInt(0, 100)),
			RiskLevel: "low",
		},
	}
}

func genPayload(cfg StressConfig) *pb.Event_Payload {
	size := 1024
	if should(cfg.LargePayloadProb) {
		size = randInt(10000, 65536)
	}
	return &pb.Event_Payload{
		EventType: "click",
		RawData:   make([]byte, size),
		JsonBlob:  `{"data": "` + randString(size/2) + `"}`,
		Metadata:  map[string]string{"format": "json"},
	}
}

func genMetrics(count int) []*pb.Event_Metric {
	metrics := make([]*pb.Event_Metric, count)
	for i := 0; i < count; i++ {
		metrics[i] = &pb.Event_Metric{
			Name:      "metric_" + randString(8),
			Value:     randFloat(0, 1000),
			Unit:      "ms",
			Timestamp: time.Now().UnixMilli(),
		}
	}
	return metrics
}

func genTags(count int) []*pb.Event_Tag {
	tags := make([]*pb.Event_Tag, count)
	for i := 0; i < count; i++ {
		tags[i] = &pb.Event_Tag{
			Key:   "tag_" + randString(6),
			Value: randString(12),
		}
	}
	return tags
}

func generateKey() string {
	const hotPartitions = 3
	rng := rngPool.Get().(*mrand.Rand)
	defer rngPool.Put(rng)
	if rng.Intn(100) < 20 {
		return "hot-" + strconv.Itoa(rng.Intn(hotPartitions))
	}
	return "cold-" + randString(16)
}

func (wp *WireProducer) SwitchSchemaVersion(version string) {
	wp.schemaVersion = version
}

func (wp *WireProducer) Stats() (messages, bytes, errors, bursts int64) {
	return wp.stats.messages.Load(),
		wp.stats.bytes.Load(),
		wp.stats.errors.Load(),
		wp.stats.bursts.Load()
}

func (wp *WireProducer) Close() error {
	wp.cancel()
	return wp.publisher.Close()
}

func (wp *WireProducer) effectiveBatchCount(base int) int {
	if wp.injector == nil {
		return base
	}
	effectiveRate := wp.injector.ApplyRate(wp.rate)
	if wp.rate <= 0 || effectiveRate <= wp.rate {
		return base
	}
	factor := effectiveRate / wp.rate
	if factor < 1 {
		factor = 1
	}
	return base * factor
}

func (wp *WireProducer) generateKey() string {
	if wp.injector != nil {
		return wp.injector.GenerateKey()
	}
	return generateKey()
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
