# ArrowFlow: Streaming Protobuf → Arrow Pipeline

A scientific evaluation harness for measuring bufarrowlib under realistic streaming pressure. Designed to find phase transitions, crossover points, and stability boundaries in the ingestion pipeline.

## Quick Start

```bash
# Build all binaries
make build

# Verify CLI
./bin/arrowflow -h
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ARROWFLOW PIPELINE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Producer                Broker              Consumer           Arrow     │
│  ───────                ──────              ────────           ──────     │
│                                                                             │
│  ┌─────────┐      ┌─────────────┐      ┌─────────────┐    ┌─────────────┐ │
│  │ Wire    │─────▶│  NATS/      │─────▶│ bufarrowlib │────▶│ RecordBatch │ │
│  │ Bytes   │      │  Kafka      │      │ Decoder     │    │ Output      │ │
│  └─────────┘      └─────────────┘      └─────────────┘    └─────────────┘ │
│       │                                      │                         │
│       ▼                                      ▼                         │
│  ┌─────────┐                        ┌─────────────┐                  │
│  │ Chaos   │                        │ HyperType   │                  │
│  │ Injector│                        │ (optional)  │                  │
│  └─────────┘                        └─────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## CLI Usage

```bash
# Producer - generates raw protobuf wire bytes
./bin/arrowflow producer --rate 100000 --mode burst

# Consumer - decodes via bufarrowlib with optional HyperType
./bin/arrowflow consumer --batch-size 1024 --hypertype

# Benchmark - throughput stress test
./bin/arrowflow benchmark --mode stress --duration 10m

# Experiment - scientific analysis modes
./bin/arrowflow experiment --mode direct --rate 50000

# Chaos - failure injection
./bin/arrowflow chaos --rate 10000 --chaos-burst
```

---

# SCIENTIFIC EXPERIMENTS

This section contains reproducible experiments to answer the critical analysis questions.

## Prerequisites

```bash
# Start NATS server WITH JetStream enabled (required for streaming)
docker run -d -p 4222:4222 --name nats nats:latest -js

# Or enable JetStream in an existing container
# docker exec nats nats-server -js

# Verify JetStream is available
docker exec nats nats-cli -s localhost:4222 jetstream streams ls

# Set environment variables
export NATS_URL="nats://localhost:4222"
export TOPIC="arrowflow.test"
```

---

## Experiment 1: Find Denorm Phase Transition Point

**Goal**: Identify where denormalized row expansion grows faster than input rate.

### Method
Run at increasing input rates with denorm enabled, measure:
- Input message rate
- Output row rate (rows = messages × fan-out multiplier)
- Memory allocation rate

### Reproduction

Run the provided script:

```bash
./scripts/denorm-phase-transition.sh
```

Or run manually using background processes:

```bash
for RATE in 1000 5000 10000 25000 50000 75000 100000; do
  echo "Testing rate: $RATE"
  ./bin/arrowflow experiment --mode direct --rate $RATE --duration 20s --workers 8 &
  PID=$!
  sleep 25
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
done
```

### Expected Observations
- **Linear regime**: Output rows grow linearly with input (fan-out ~1-2x)
- **Transition point**: When repeated fields expand, output rate accelerates
- **Explosion point**: Output rate >> input rate, memory grows superlinearly

---

## Experiment 2: HyperType Crossover Threshold

**Goal**: Find where HyperType optimization overhead exceeds benefit.

### Method
Compare performance with and without HyperType at different:
- Message sizes
- Schema complexities
- Input rates

### Reproduction

```bash
./scripts/hypertype-crossover.sh
```

Or run manually:

```bash
# Without HyperType
./bin/arrowflow experiment --mode direct --rate 10000 --duration 20s --hyper=false &
PID=$!
sleep 25
kill $PID 2>/dev/null || true
wait $PID 2>/dev/null || true

# With HyperType
./bin/arrowflow experiment --mode direct --rate 10000 --duration 20s --hyper=true &
PID=$!
sleep 25
kill $PID 2>/dev/null || true
wait $PID 2>/dev/null || true
```

### Expected Results
- **Small messages**: HyperType overhead may exceed benefit (compilation cost > parse savings)
- **Large messages**: HyperType provides clear benefit
- **High rate**: HyperType PGO recompilation may show staged improvements

---

## Experiment 3: GC Phase Shifts by Batch Size

**Goal**: Find batch sizes where GC behavior changes dramatically.

### Reproduction

```bash
./scripts/gc-phase-shifts.sh
```

Or run manually:

```bash
for BATCH in 100 500 1000 5000 10000; do
  echo "Testing batch: $BATCH"
  ./bin/arrowflow consumer --batch-size $BATCH --workers 4 &
  PID=$!
  sleep 15
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
done
```

### Expected Signatures
| Batch Size | Behavior | Dominated By |
|------------|----------|--------------|
| 1-100 | High GC frequency | CPU/GC |
| 100-1000 | Balanced | - |
| 10000+ | GC spikes | Memory/GC collapse |

---

## Experiment 4: Denorm Structural Explosion

**Goal**: Detect threshold where denorm becomes unstable.

### Reproduction

```bash
./scripts/denorm-explosion.sh
```

Or run manually:

```bash
for RATE in 10000 20000 30000; do
  echo "Testing rate: $RATE"
  ./bin/arrowflow experiment --mode direct --rate $RATE --duration 15s &
  PID=$!
  sleep 20
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
done
```

### Warning Signs
- Fan-out multiplier becomes variable
- Latency distribution becomes bimodal (two peaks)
- Cache miss rate spikes
- Memory grows non-linearly

---

## Run All Experiments

```bash
./scripts/run-all-experiments.sh
```

This will:
1. Run baseline with HyperType
2. Run baseline without HyperType  
3. Sweep batch sizes (100, 500, 1000, 5000, 10000)
4. Run stress collapse test
5. Run chaos injection
6. Sweep rates (10K, 25K, 50K, 75K, 100K)

Results are saved to `experiment-results/`

---

## Interpreting Results

### Key Questions to Answer

| Question | How to Find Answer |
|----------|-------------------|
| Where does throughput saturate? | Run stress test, find plateau |
| What dominates cost? | Compare baseline vs denorm vs hyper |
| When does HyperType fail? | Compare with/without at varying sizes |
| Optimal batch size? | Sweep batch sizes, find GC minimum |
| Denorm linear or phase-transition? | Measure fan-out ratio vs input rate |
| Stability boundary? | Chaos injection + stress collapse |

### Phase Transition Signatures

**Denorm Phase Transition**:
```
Linear:    rows/sec ≈ messages/sec × fan-out (constant)
Transition: rows/sec begins accelerating  
Explosion: rows/sec >> messages/sec (unstable)
```

**HyperType Crossover**:
```
Beneficial: hypertype_time < baseline_time
Break-even: hypertype_time ≈ baseline_time  
Harmful:    hypertype_time > baseline_time (compilation overhead)
```

**GC Phase Shift**:
```
Smooth:    pause_time grows linearly with batch_size
Jump:      pause_time spikes at specific batch thresholds
Collapse:  pause_time dominates latency at large batches
```

---

## Configuration Reference

### Environment Variables

```bash
# Stream configuration
export STREAM_BACKEND="nats"           # nats or kafka
export NATS_URL="nats://localhost:4222"
export TOPIC="arrowflow.test"
export CONSUMER_GROUP="arrowflow-group"

# Producer configuration  
export BATCH_SIZE=1000
export BUFFER_SIZE_BYTES=1048576
```

### Metric Output

Results are written to:
- `telemetry.json` - Real-time metrics in JSON format
- Console - Live summary printed on exit
- Prometheus - Available on `:9090/metrics` (if enabled)
- CSV files - In `results/` directory for each experiment

---

## Troubleshooting

### "nats: no response from stream" / High produce latency
**Cause**: NATS JetStream is not enabled or streams are not configured.

**Solution**:
```bash
# Restart NATS with JetStream enabled
docker rm -f nats
docker run -d -p 4222:4222 --name nats nats:latest -js

# Create stream manually if needed
docker exec nats nats-cli -s localhost:4222 \
  stream add ARROWFLOW --subjects arrowflow.test --storage memory
```

### High Latency
- Reduce batch size
- Disable HyperType (for small messages)
- Enable more workers

### GC Thrashing
- Increase batch size
- Reduce worker count
- Enable HyperType (reduces allocation churn)

### Memory Explosion
- Check for denorm fan-out explosion
- Reduce repeated field cardinality in test data
- Enable batch limits
