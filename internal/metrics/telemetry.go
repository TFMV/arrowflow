package metrics

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	PrometheusPort int
)

type Timer struct {
	start time.Time
}

func StartTimer() *Timer {
	return &Timer{start: time.Now()}
}

func (t *Timer) Stop() time.Duration {
	return time.Since(t.start)
}

type Histogram struct {
	sum   atomic.Int64
	count atomic.Int64
	min   atomic.Int64
	max   atomic.Int64
	vals  []float64
	mu    sync.Mutex
	size  int
}

func NewHistogram(size int) *Histogram {
	return &Histogram{vals: make([]float64, 0, size), size: size}
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.vals = append(h.vals, v)
	h.sum.Add(int64(v * 1000000000))
	h.count.Add(1)

	oldMin := h.min.Load()
	if oldMin == 0 || int64(v*1000000000) < oldMin {
		h.min.Store(int64(v * 1000000000))
	}
	oldMax := h.max.Load()
	if int64(v*1000000000) > oldMax {
		h.max.Store(int64(v * 1000000000))
	}

	if len(h.vals) > h.size {
		copy(h.vals[:len(h.vals)-1], h.vals[1:])
		h.vals = h.vals[:len(h.vals)-1]
	}
}

func (h *Histogram) Stats() (count, min, max, mean, p50, p95, p99, p999 float64) {
	count = float64(h.count.Load())
	if count == 0 {
		return
	}

	min = float64(h.min.Load()) / 1000000000
	max = float64(h.max.Load()) / 1000000000
	mean = float64(h.sum.Load()) / 1000000000 / count

	if len(h.vals) > 0 {
		sorted := make([]float64, len(h.vals))
		copy(sorted, h.vals)
		quickSort(sorted)
		p50 = sorted[int(0.50*float64(len(sorted)))]
		p95 = sorted[int(0.95*float64(len(sorted)))]
		p99 = sorted[int(0.99*float64(len(sorted)))]
		if len(sorted) > 1000 {
			p999 = sorted[int(0.999*float64(len(sorted)))]
		} else {
			p999 = p99
		}
	}

	return
}

func quickSort(a []float64) {
	if len(a) < 2 {
		return
	}
	left, right := 0, len(a)-1
	pivot := len(a) / 2
	a[pivot], a[right] = a[right], a[pivot]
	for i := range a {
		if a[i] < a[right] {
			a[i], a[left] = a[left], a[i]
			left++
		}
	}
	a[left], a[right] = a[right], a[left]
	quickSort(a[:left])
	quickSort(a[left+1:])
}

type Gauge struct {
	val  int64
	peak int64
}

func (g *Gauge) Set(v float64) {
	iv := int64(v)
	atomic.StoreInt64(&g.val, iv)
	for {
		p := atomic.LoadInt64(&g.peak)
		if iv <= p {
			break
		}
		if atomic.CompareAndSwapInt64(&g.peak, p, iv) {
			break
		}
	}
}

func (g *Gauge) Add(v float64) {
	newVal := atomic.AddInt64(&g.val, int64(v))
	for {
		p := atomic.LoadInt64(&g.peak)
		if newVal <= p {
			break
		}
		if atomic.CompareAndSwapInt64(&g.peak, p, newVal) {
			break
		}
	}
}

func (g *Gauge) Get() float64 {
	return float64(atomic.LoadInt64(&g.val))
}

func (g *Gauge) GetPeak() float64 {
	return float64(atomic.LoadInt64(&g.peak))
}

var consumerLag Gauge
var bufferDepth Gauge

type Throughput struct {
	bytes    int64
	messages int64
	start    time.Time
}

func NewThroughput() *Throughput {
	return &Throughput{start: time.Now()}
}

func (t *Throughput) Add(bytes, messages int64) {
	atomic.AddInt64(&t.bytes, bytes)
	atomic.AddInt64(&t.messages, messages)
}

func (t *Throughput) Rate() (msgRate, byteRate float64) {
	elapsed := time.Since(t.start).Seconds()
	if elapsed == 0 {
		return
	}
	msgRate = float64(atomic.LoadInt64(&t.messages)) / elapsed
	byteRate = float64(atomic.LoadInt64(&t.bytes)) / elapsed
	return
}

func (t *Throughput) Totals() (messages, bytes int64) {
	return atomic.LoadInt64(&t.messages), atomic.LoadInt64(&t.bytes)
}

func (t *Throughput) Reset() {
	atomic.StoreInt64(&t.bytes, 0)
	atomic.StoreInt64(&t.messages, 0)
}

var (
	globalLatency    = NewLatencyDistribution()
	globalThroughput = NewThroughput()

	ProduceLatencyMs       prometheus.Histogram
	ConsumeLatencyMs       prometheus.Histogram
	BatchOutputLatencyMs   prometheus.Histogram
	MessagesProduced       prometheus.Counter
	MessagesConsumed       prometheus.Counter
	BytesProduced          prometheus.Counter
	BytesConsumed          prometheus.Counter
	ProducerThroughput     prometheus.Gauge
	ConsumerThroughput     prometheus.Gauge
	RecordBatchSize        prometheus.Histogram
	RecordBatchRows        prometheus.Histogram
	ColumnExpansionFactor  prometheus.Histogram
	DenormFanoutMultiplier prometheus.Histogram
	HeapGrowthMB           prometheus.Gauge
	GCPauseMs              prometheus.Histogram
	ErrorsTotal            prometheus.Counter
)

type LatencyDistribution struct {
	mu        sync.Mutex
	histogram map[string]*Histogram
}

func NewLatencyDistribution() *LatencyDistribution {
	return &LatencyDistribution{histogram: make(map[string]*Histogram)}
}

func (ld *LatencyDistribution) Observe(name string, seconds float64) {
	ld.mu.Lock()
	h, ok := ld.histogram[name]
	if !ok {
		h = NewHistogram(10000)
		ld.histogram[name] = h
	}
	h.Observe(seconds)
	ld.mu.Unlock()
}

func (ld *LatencyDistribution) Stats(name string) (count, min, max, mean, p50, p95, p99, p999 float64) {
	ld.mu.Lock()
	h, ok := ld.histogram[name]
	if !ok {
		ld.mu.Unlock()
		return
	}
	count, min, max, mean, p50, p95, p99, p999 = h.Stats()
	ld.mu.Unlock()
	return
}

func init() {
	rand.Seed(time.Now().UnixNano())

	ProduceLatencyMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "arrowflow_produce_latency_ms",
		Help:    "Time from producer to broker",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000},
	})

	ConsumeLatencyMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "arrowflow_consume_latency_ms",
		Help:    "Time from broker to consumer",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000},
	})

	BatchOutputLatencyMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "arrowflow_batch_output_latency_ms",
		Help:    "Time to produce Arrow batch",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 25, 50, 100, 250, 500, 1000},
	})

	MessagesProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "arrowflow_messages_produced_total",
		Help: "Total messages produced",
	})

	MessagesConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "arrowflow_messages_consumed_total",
		Help: "Total messages consumed",
	})

	BytesProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "arrowflow_bytes_produced_total",
		Help: "Total bytes produced",
	})

	BytesConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "arrowflow_bytes_consumed_total",
		Help: "Total bytes consumed",
	})

	ProducerThroughput = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "arrowflow_producer_throughput_msg_s",
		Help: "Producer messages per second",
	})

	ConsumerThroughput = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "arrowflow_consumer_throughput_msg_s",
		Help: "Consumer messages per second",
	})

	RecordBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "arrowflow_record_batch_size_bytes",
		Help:    "RecordBatch size in bytes",
		Buckets: []float64{1024, 4096, 16384, 65536, 262144, 1048576, 4194304},
	})

	RecordBatchRows = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "arrowflow_record_batch_rows",
		Help:    "Number of rows per RecordBatch",
		Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000, 50000},
	})

	ColumnExpansionFactor = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "arrowflow_column_expansion_factor",
		Help:    "Ratio of Arrow columns to proto fields",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10},
	})

	DenormFanoutMultiplier = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "arrowflow_denorm_fanout_multiplier",
		Help:    "Rows output per input message from denorm fan-out",
		Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 500},
	})

	HeapGrowthMB = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "arrowflow_heap_growth_mb",
		Help: "Heap growth in MB",
	})

	GCPauseMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "arrowflow_gc_pause_ms",
		Help:    "GC pause time in milliseconds",
		Buckets: []float64{0.1, 0.5, 1, 5, 10, 25, 50, 100, 250},
	})

	ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "arrowflow_errors_total",
		Help: "Total errors",
	})

	prometheus.MustRegister(
		ProduceLatencyMs, ConsumeLatencyMs, BatchOutputLatencyMs,
		MessagesProduced, MessagesConsumed, BytesProduced, BytesConsumed,
		ProducerThroughput, ConsumerThroughput,
		RecordBatchSize, RecordBatchRows, ColumnExpansionFactor, DenormFanoutMultiplier,
		HeapGrowthMB, GCPauseMs, ErrorsTotal,
	)
}

func RecordLatency(name string, start time.Time) {
	duration := time.Since(start).Nanoseconds()
	globalLatency.Observe(name, float64(duration)/1000000000)

	switch name {
	case "produce":
		ProduceLatencyMs.Observe(float64(duration) / 1000000)
	case "consume":
		ConsumeLatencyMs.Observe(float64(duration) / 1000000)
	case "batch_output":
		BatchOutputLatencyMs.Observe(float64(duration) / 1000000)
	}
}

func RecordThroughput(bytes, messages int64) {
	globalThroughput.Add(bytes, messages)
	MessagesProduced.Add(float64(messages))
	BytesProduced.Add(float64(bytes))

	msgRate, byteRate := globalThroughput.Rate()
	ProducerThroughput.Set(msgRate)
	ConsumerThroughput.Set(byteRate)
}

func RecordBatchMetrics(rows, cols int, sizeBytes int64, denormRows int) {
	RecordBatchSize.Observe(float64(sizeBytes))
	RecordBatchRows.Observe(float64(rows))

	// Record to internal Stats for GenerateTelemetryReport
	globalLatency.Observe("batch_size_bytes", float64(sizeBytes))
	globalLatency.Observe("batch_rows", float64(rows))

	if cols > 0 && rows > 0 {
		expansion := float64(cols) / float64(rows)
		ColumnExpansionFactor.Observe(expansion)
		globalLatency.Observe("column_expansion", expansion)
	}

	if denormRows > 0 {
		fanout := float64(denormRows) / float64(rows)
		DenormFanoutMultiplier.Observe(fanout)
		globalLatency.Observe("denorm_fanout", fanout)
	}
}

func RecordMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	heapUsedMB := float64(m.HeapAlloc) / 1024 / 1024
	HeapGrowthMB.Set(heapUsedMB)
}

var lastGCPauseNs uint64

func RecordGCStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	if m.NumGC > 0 {
		pauseNs := m.PauseNs[(m.NumGC+255)%256]
		if lastGCPauseNs > 0 && pauseNs != lastGCPauseNs {
			pauseMs := float64(pauseNs) / 1000000
			GCPauseMs.Observe(pauseMs)
		}
		lastGCPauseNs = pauseNs
	}
}

func RecordError() {
	ErrorsTotal.Inc()
}

func SetConsumerLag(lag int64) {
	consumerLag.Set(float64(lag))
}

func SetBufferDepth(depth int64) {
	bufferDepth.Set(float64(depth))
}

type TelemetryReport struct {
	Timestamp  time.Time       `json:"timestamp"`
	Throughput ThroughputStats `json:"throughput"`
	Latency    LatencyStats    `json:"latency"`
	Memory     MemoryStats     `json:"memory"`
	Arrow      ArrowStats      `json:"arrow"`
	Streaming  StreamingStats  `json:"streaming"`
}

type ThroughputStats struct {
	MessagesPerSec float64 `json:"messages_per_sec"`
	BytesPerSec    float64 `json:"bytes_per_sec"`
	TotalMessages  int64   `json:"total_messages"`
	TotalBytes     int64   `json:"total_bytes"`
}

type LatencyStats struct {
	Produce     LatencyPercentiles `json:"produce"`
	Consume     LatencyPercentiles `json:"consume"`
	BatchOutput LatencyPercentiles `json:"batch_output"`
}

type LatencyPercentiles struct {
	Count float64 `json:"count"`
	Mean  float64 `json:"mean_ns"`
	P50   float64 `json:"p50_ns"`
	P95   float64 `json:"p95_ns"`
	P99   float64 `json:"p99_ns"`
	P999  float64 `json:"p999_ns"`
}

type MemoryStats struct {
	HeapAllocMB   float64 `json:"heap_alloc_mb"`
	HeapInUseMB   float64 `json:"heap_in_use_mb"`
	GCs           uint32  `json:"gc_count"`
	GCPauseLastMs float64 `json:"gc_pause_last_ms"`
}

type ArrowStats struct {
	AvgBatchSizeBytes  float64 `json:"avg_batch_size_bytes"`
	AvgBatchRows       float64 `json:"avg_batch_rows"`
	AvgColumnExpansion float64 `json:"avg_column_expansion"`
	AvgDenormFanout    float64 `json:"avg_denorm_fanout"`
}

type StreamingStats struct {
	ConsumerLag     int64 `json:"consumer_lag"`
	PeakConsumerLag int64 `json:"peak_consumer_lag"`
	BufferDepth     int64 `json:"buffer_depth"`
	PeakBufferDepth int64 `json:"peak_buffer_depth"`
}

func GenerateTelemetryReport() TelemetryReport {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	msgRate, byteRate := globalThroughput.Rate()
	totalMsgs, totalBytes := globalThroughput.Totals()

	report := TelemetryReport{
		Timestamp: time.Now(),
		Throughput: ThroughputStats{
			MessagesPerSec: msgRate,
			BytesPerSec:    byteRate,
			TotalMessages:  totalMsgs,
			TotalBytes:     totalBytes,
		},
		Latency: LatencyStats{
			Produce:     getLatencyStats("produce"),
			Consume:     getLatencyStats("consume"),
			BatchOutput: getLatencyStats("batch_output"),
		},
		Memory: MemoryStats{
			HeapAllocMB:   float64(m.HeapAlloc) / 1024 / 1024,
			HeapInUseMB:   float64(m.HeapInuse) / 1024 / 1024,
			GCs:           m.NumGC,
			GCPauseLastMs: float64(m.PauseNs[(m.NumGC+255)%256]) / 1000000,
		},
		Arrow: ArrowStats{
			AvgBatchSizeBytes:  getMean("batch_size_bytes"),
			AvgBatchRows:       getMean("batch_rows"),
			AvgColumnExpansion: getMean("column_expansion"),
			AvgDenormFanout:    getMean("denorm_fanout"),
		},
		Streaming: StreamingStats{
			ConsumerLag:     int64(consumerLag.Get()),
			PeakConsumerLag: int64(consumerLag.GetPeak()),
			BufferDepth:     int64(bufferDepth.Get()),
			PeakBufferDepth: int64(bufferDepth.GetPeak()),
		},
	}

	return report
}

func getLatencyStats(name string) LatencyPercentiles {
	count, _, _, mean, p50, p95, p99, p999 := globalLatency.Stats(name)
	return LatencyPercentiles{
		Count: count,
		Mean:  mean * 1000000000,
		P50:   p50 * 1000000000,
		P95:   p95 * 1000000000,
		P99:   p99 * 1000000000,
		P999:  p999 * 1000000000,
	}
}

func getMean(name string) float64 {
	_, _, _, mean, _, _, _, _ := globalLatency.Stats(name)
	return mean
}

func (r TelemetryReport) ToJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func (r TelemetryReport) PrintSummary() {
	fmt.Printf("=== ArrowFlow Telemetry Report ===\n")
	fmt.Printf("Timestamp: %s\n\n", r.Timestamp.Format(time.RFC3339))

	fmt.Printf("Throughput:\n")
	fmt.Printf("  Messages/sec: %.2f\n", r.Throughput.MessagesPerSec)
	fmt.Printf("  Bytes/sec:    %.2f MB/s\n", r.Throughput.BytesPerSec/1024/1024)
	fmt.Printf("  Total:        %d messages, %.2f MB\n\n", r.Throughput.TotalMessages, float64(r.Throughput.TotalBytes)/1024/1024)

	fmt.Printf("Latency (ns):\n")
	fmt.Printf("  Produce:      mean=%.0f, p50=%.0f, p95=%.0f, p99=%.0f, p99.9=%.0f\n",
		r.Latency.Produce.Mean, r.Latency.Produce.P50, r.Latency.Produce.P95, r.Latency.Produce.P99, r.Latency.Produce.P999)
	fmt.Printf("  Consume:      mean=%.0f, p50=%.0f, p95=%.0f, p99=%.0f, p99.9=%.0f\n",
		r.Latency.Consume.Mean, r.Latency.Consume.P50, r.Latency.Consume.P95, r.Latency.Consume.P99, r.Latency.Consume.P999)
	fmt.Printf("  Batch Output: mean=%.0f, p50=%.0f, p95=%.0f, p99=%.0f, p99.9=%.0f\n\n",
		r.Latency.BatchOutput.Mean, r.Latency.BatchOutput.P50, r.Latency.BatchOutput.P95, r.Latency.BatchOutput.P99, r.Latency.BatchOutput.P999)

	fmt.Printf("Memory:\n")
	fmt.Printf("  Heap Alloc:  %.2f MB\n", r.Memory.HeapAllocMB)
	fmt.Printf("  Heap In Use: %.2f MB\n", r.Memory.HeapInUseMB)
	fmt.Printf("  GCs:         %d\n", r.Memory.GCs)
	fmt.Printf("  GC Pause:    %.2f ms\n\n", r.Memory.GCPauseLastMs)

	fmt.Printf("Arrow Integration:\n")
	fmt.Printf("  Avg Batch:     %.0f rows (%.2f KB)\n", r.Arrow.AvgBatchRows, r.Arrow.AvgBatchSizeBytes/1024)
	fmt.Printf("  Expansion:     %.2f cols/row\n", r.Arrow.AvgColumnExpansion)
	fmt.Printf("  Denorm Fanout: %.2f multiplier\n\n", r.Arrow.AvgDenormFanout)

	fmt.Printf("Streaming:\n")
	fmt.Printf("  Consumer Lag: %d (Peak: %d)\n", r.Streaming.ConsumerLag, r.Streaming.PeakConsumerLag)
	fmt.Printf("  Buffer Depth: %d (Peak: %d)\n", r.Streaming.BufferDepth, r.Streaming.PeakBufferDepth)
}

func StartTelemetryLoop(interval time.Duration, outputFile string) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			RecordMemoryStats()
			RecordGCStats()

			if outputFile != "" {
				report := GenerateTelemetryReport()
				data, _ := report.ToJSON()
				os.WriteFile(outputFile, data, 0644)
			}
		}
	}()
}

func StartPrometheus(port int) {
	if port <= 0 {
		port = 9090
	}

	addr := fmt.Sprintf(":%d", port)
	go func() {
		log.Printf("Prometheus metrics server starting on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("Prometheus server error: %v", err)
		}
	}()
}

var _ = promhttp.Handler
