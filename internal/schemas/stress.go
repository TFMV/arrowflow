package schemas

import (
	"crypto/rand"
	"math/big"
	"time"

	pb "github.com/TFMV/arrowflow/internal/schemas/pb"
)

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

var DefaultStressConfig = StressConfig{
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

func GenerateEvent(cfg StressConfig) *pb.Event {
	evt := &pb.Event{
		SchemaVersion:  "1.0.0",
		EventTimestamp: time.Now().UnixMilli(),
	}

	if should(cfg.IncludeUser) {
		evt.User = generateUserContext()
	}
	if should(cfg.IncludeSession) {
		evt.Session = generateSessionContext(cfg)
	}
	if should(cfg.IncludeTracing) {
		evt.Tracing = generateTracingContext()
	}
	if should(cfg.IncludeEnrichment) {
		evt.Enrichment = generateEnrichment()
	}
	if should(cfg.IncludePayload) {
		evt.Payload = generatePayload(cfg)
	}
	if should(cfg.IncludeMetrics) {
		evt.Metrics = generateMetrics(cfg.NumMetrics)
	}
	evt.Tags = generateTags(cfg.NumTags)

	return evt
}

func should(prob float32) bool {
	n, _ := rand.Int(rand.Reader, big.NewInt(10000))
	return int(n.Int64()) < int(prob*10000)
}

func randString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		b[i] = chars[n.Int64()]
	}
	return string(b)
}

func randUUID() string {
	return randString(8) + "-" + randString(4) + "-" + randString(4) + "-" + randString(4) + "-" + randString(12)
}

func randInt(min, max int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	return int(n.Int64()) + min
}

func randInt64(min, max int64) int64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(max-min))
	return n.Int64() + min
}

func randFloat(min, max float64) float64 {
	f := float64(randInt(0, 10000)) / 10000.0
	return min + f*(max-min)
}

func generateUserContext() *pb.Event_UserContext {
	profile := &pb.Event_UserContext_Profile{
		Email:    randString(20) + "@example.com",
		Username: randString(12),
		Roles:    []string{"user", "premium"},
		Preferences: map[string]string{
			"theme":         "dark",
			"language":      "en",
			"notifications": "enabled",
		},
		Account: &pb.Event_UserContext_Profile_Account{
			AccountId:   randUUID(),
			Tier:        "premium",
			CreatedAt:   randInt64(1600000000000, 1700000000000),
			Permissions: []string{"read", "write", "delete"},
			Metadata:    map[string]string{"source": "web"},
			Billing: &pb.Event_UserContext_Profile_Account_Billing{
				PaymentMethod: "card",
				Currency:      "USD",
				Invoices: []*pb.Event_UserContext_Profile_Account_Billing_Invoice{
					{
						InvoiceId: randUUID(),
						Amount:    randInt64(1000, 100000),
						Status:    "paid",
						Items: []*pb.Event_UserContext_Profile_Account_Billing_Invoice_LineItem{
							{Sku: "SKU-" + randString(8), Description: "Premium subscription", Quantity: 1, UnitPrice: 999},
						},
					},
				},
			},
		},
	}

	return &pb.Event_UserContext{
		UserId:    randUUID(),
		SessionId: randUUID(),
		DeviceId:  randUUID(),
		IpAddress: "192.168." + randString(3),
		UserAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
		Attributes: map[string]string{
			"region":  "us-east-1",
			"tenant":  "default",
			"version": "2.1.0",
		},
		Profile: profile,
	}
}

func generateSessionContext(cfg StressConfig) *pb.Event_SessionContext {
	nav := &pb.Event_SessionContext_Navigation{}

	numPV := randInt(0, cfg.MaxPageViews)
	nav.PageViews = make([]*pb.Event_SessionContext_Navigation_PageView, numPV)
	for i := 0; i < numPV; i++ {
		nav.PageViews[i] = &pb.Event_SessionContext_Navigation_PageView{
			Url:        "https://example.com/page/" + randString(8),
			Title:      "Page " + randString(10),
			Timestamp:  randInt64(1700000000000, 1800000000000),
			DurationMs: int32(randInt(100, 60000)),
			Referrer:   "https://referrer.example.com",
			Params:     map[string]string{"ref": randString(4), "utm_campaign": randString(6)},
		}
	}

	numClicks := randInt(0, cfg.MaxClicks)
	nav.Clicks = make([]*pb.Event_SessionContext_Navigation_ClickEvent, numClicks)
	for i := 0; i < numClicks; i++ {
		nav.Clicks[i] = &pb.Event_SessionContext_Navigation_ClickEvent{
			ElementId:   "elem-" + randString(6),
			ElementType: "button",
			Xpath:       "//div[" + randString(3) + "]/button",
			X:           int32(randInt(0, 1920)),
			Y:           int32(randInt(0, 1080)),
			Timestamp:   randInt64(1700000000000, 1800000000000),
			Data:        map[string]string{"action": randString(8)},
		}
	}

	numErrors := randInt(0, cfg.MaxErrors)
	nav.Errors = make([]*pb.Event_SessionContext_Navigation_ErrorEvent, numErrors)
	for i := 0; i < numErrors; i++ {
		nav.Errors[i] = &pb.Event_SessionContext_Navigation_ErrorEvent{
			ErrorType:  "TypeError",
			Message:    "Cannot read property '" + randString(8) + "' of undefined",
			StackTrace: "at " + randString(12) + " (" + randString(8) + ".js:" + randString(2) + ")\n    at " + randString(10),
			Timestamp:  randInt64(1700000000000, 1800000000000),
			Context:    map[string]string{"line": randString(3), "column": randString(2)},
		}
	}

	return &pb.Event_SessionContext{
		SessionId:    randUUID(),
		StartedAt:    randInt64(1700000000000, 1800000000000),
		LastActivity: time.Now().UnixMilli(),
		EntryUrl:     "https://example.com",
		Referrers:    []string{"https://google.com", "https://twitter.com"},
		SessionData:  map[string]string{"cart_id": randUUID(), "checkout_step": "2"},
		Navigation:   nav,
	}
}

func generateTracingContext() *pb.Event_TracingContext {
	numAnnots := randInt(1, 5)
	annotations := make([]*pb.Event_TracingContext_SpanAnnotation, numAnnots)
	for i := 0; i < numAnnots; i++ {
		annotations[i] = &pb.Event_TracingContext_SpanAnnotation{
			Key:       randString(8),
			Value:     randString(16),
			Timestamp: randInt64(1700000000000, 1800000000000),
		}
	}

	return &pb.Event_TracingContext{
		TraceId:       randUUID(),
		SpanId:        randString(16),
		ParentSpanId:  randString(16),
		ServiceName:   "arrowflow-ingest",
		OperationName: "process_event",
		StartTime:     randInt64(1700000000000, 1800000000000),
		DurationNs:    randInt64(1000, 10000000),
		Tags:          map[string]string{"version": "1.0.0", "env": "production"},
		Annotations:   annotations,
	}
}

func generateEnrichment() *pb.Event_Enrichment {
	return &pb.Event_Enrichment{
		Geo: &pb.Event_Enrichment_Geolocation{
			Country:   "US",
			Region:    "California",
			City:      "San Francisco",
			Timezone:  "America/Los_Angeles",
			Latitude:  randFloat(37.0, 38.0),
			Longitude: randFloat(-122.5, -122.0),
			Isp:       "Comcast",
			Asn:       "AS7922",
			Extra:     map[string]string{"accuracy": "10m"},
		},
		Ua: &pb.Event_Enrichment_UserAgentInfo{
			Browser:        "Chrome",
			BrowserVersion: "120.0.0",
			Os:             "Mac OS X",
			OsVersion:      "10.15.7",
			DeviceType:     "desktop",
			DeviceBrand:    "Apple",
			DeviceModel:    "MacBook Pro",
			IsMobile:       false,
			IsBot:          false,
			RawHeaders:     map[string]string{"accept": "text/html"},
		},
		Device: &pb.Event_Enrichment_DeviceInfo{
			DeviceId:         randUUID(),
			DeviceType:       "desktop",
			Os:               "Mac OS X",
			OsVersion:        "10.15.7",
			ScreenResolution: "2560x1600",
			ScreenWidth:      2560,
			ScreenHeight:     1600,
			ColorDepth:       24,
			IsTablet:         false,
			IsMobile:         false,
			Capabilities:     map[string]string{"webgl": "true"},
		},
		Server: &pb.Event_Enrichment_ServerInfo{
			ServerName:        "ingest-01",
			Datacenter:        "us-east-1a",
			Region:            "us-east-1",
			Cluster:           "primary",
			Tags:              map[string]string{"tier": "ingest"},
			AvailableServices: []string{"kafka", "redis", "postgres"},
		},
		AbTest: &pb.Event_Enrichment_ABTest{
			ExperimentId:      randUUID(),
			VariantId:         "variant_a",
			ExperimentName:    "checkout_flow_optimization",
			VariantName:       "control",
			TrafficAllocation: randFloat(0, 100),
			Parameters:        map[string]string{"button_color": "blue"},
		},
		Fraud: &pb.Event_Enrichment_FraudDetection{
			RiskScore: int32(randInt(0, 100)),
			RiskLevel: "low",
			Flags:     []string{"trusted_device"},
			Signals:   map[string]string{"velocity": "normal"},
		},
	}
}

func generatePayload(cfg StressConfig) *pb.Event_Payload {
	size := 1024
	if should(cfg.LargePayloadProb) {
		size = randInt(10000, 65536)
	}

	return &pb.Event_Payload{
		EventType: "click",
		RawData:   make([]byte, size),
		JsonBlob:  `{"data": "` + randString(size/2) + `"}`,
		Metadata:  map[string]string{"format": "json", "encoding": "utf-8"},
		CustomFields: []*pb.Event_Payload_CustomField{
			{Name: "field1", Value: randString(20), Type: pb.Event_Payload_CustomField_STRING},
			{Name: "field2", Value: randString(30), Type: pb.Event_Payload_CustomField_JSON},
		},
	}
}

func generateMetrics(count int) []*pb.Event_Metric {
	metrics := make([]*pb.Event_Metric, count)
	for i := 0; i < count; i++ {
		metrics[i] = &pb.Event_Metric{
			Name:      "metric_" + randString(8),
			Value:     randFloat(0, 1000),
			Unit:      "ms",
			Timestamp: time.Now().UnixMilli(),
			Labels:    map[string]string{"host": "server-01"},
		}
	}
	return metrics
}

func generateTags(count int) []*pb.Event_Tag {
	tags := make([]*pb.Event_Tag, count)
	for i := 0; i < count; i++ {
		tags[i] = &pb.Event_Tag{
			Key:   "tag_" + randString(6),
			Value: randString(12),
		}
	}
	return tags
}
