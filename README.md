# ArrowFlow: Streaming Protobuf → Arrow Pipeline

A scientific evaluation harness for measuring `bufarrowlib` under realistic streaming pressure. Designed to identify **phase transitions**, **crossover points**, and **stability boundaries** in the ingestion pipeline.

## Quick Start

```bash
# Build all binaries
make build

# Start NATS server with JetStream enabled
docker run -d -p 4222:4222 --name nats nats:latest -js

# Run the full automated evaluation suite
./scripts/run-all-experiments.sh
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ARROWFLOW PIPELINE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Producer                Broker              Consumer           Arrow       │
│  ───────                ──────              ────────           ──────       │
│                                                                             │
│  ┌─────────┐      ┌─────────────┐      ┌─────────────┐    ┌─────────────┐   │
│  │ Wire    │─────▶│  NATS/      │─────▶│ bufarrowlib │───▶│ RecordBatch │   │
│  │ Bytes   │      │  Kafka      │      │ Decoder     │    │ Output      │   │
│  └─────────┘      └─────────────┘      └─────────────┘    └─────────────┘   │
│       │                                      │                              │
│       ▼                                      ▼                              │
│  ┌─────────┐                        ┌─────────────┐                         │
│  │ Chaos   │                        │ HyperType   │                         │
│  │ Injector│                        │ (optional)  │                         │
│  └─────────┘                        └─────────────┘                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## CLI Usage

ArrowFlow provides several specialized subcommands for performance characterization:

```bash
# Experiment - multi-dimensional scientific analysis
./bin/arrowflow experiment \
  --mode stream \
  --rate 50000 \
  --duration 20s \
  --batch-size 1000 \
  --size-dist heavy-tail \
  --denorm=true \
  --hyper=true

# Chaos - failure and skew injection
./bin/arrowflow chaos \
  --rate 10000 \
  --duration 1m \
  --chaos-burst \
  --chaos-schema \
  --chaos-hot-partition \
  --chaos-size-shock
```

### Key Flags:
| Flag | Description | Values |
| :--- | :--- | :--- |
| `--mode` | Execution mode | `stream` (NATS), `direct` (In-process) |
| `--size-dist` | Message size profile | `small`, `medium`, `large`, `heavy-tail` |
| `--denorm` | Structural mode | `true` (Flat Arrow), `false` (Nested Arrow) |
| `--batch-size` | Buffering threshold | Rows before Arrow emission (e.g., 100, 1000, 10000) |
| `--hyper` | Enable HyperType | JIT-compiled Protobuf decoding |

---

# SCIENTIFIC EVALUATION

ArrowFlow is built to answer six fundamental questions about the ingestion pipeline. These are automated in the `./scripts/run-all-experiments.sh` suite.

## The 6-Phase Evaluation Suite

### Phase 1: HyperType Crossover Point
**Goal**: Identify where HyperType's JIT overhead outweighs its benefits.
- **Method**: Compares baseline vs HyperType across `small`, `medium`, `large`, and `heavy-tail` distributions.
- **Outcome**: Documents the payload size crossover where JIT parsing becomes 4x+ faster than standard decoding.

### Phase 2: Batch Size & GC Optimization
**Goal**: Find the "sweet spot" for row buffering.
- **Method**: Sweeps batch sizes from 100 to 10,000 rows.
- **Metric**: Monitors `GC Count` vs `Consumption Latency`.

### Phase 3: Structural Cost (Denorm vs Nested)
**Goal**: Quantify the cost of flat Arrow denormalization.
- **Method**: Compares `denorm=true` vs `denorm=false` at high rates.
- **Metric**: Uses `Expansion Factor` and `Fan-out Multiplier` to measure row inflation.

### Phase 4: Throughput Saturation (Finding the Plateau)
**Goal**: Identify the hardware/environment bottleneck.
- **Method**: Rate sweep from 10,000 to 200,000 msg/s.
- **Metric**: Finds where `Observed Rate` plateaus and `Consumer Lag` spikes.

### Phase 5: Worker Scaling Efficiency
**Goal**: Measure parallelism scaling limits.
- **Method**: Scales processing workers from 2 to 16.

### Phase 6: Chaos & Stability Boundary
**Goal**: Test system recovery under adverse conditions.
- **Method**: Sustained 1-minute run with bursts, schema evolution, and partition skew enabled.

---

## Interpreting Telemetry

Hardened telemetry provides three new specialized sections for scientific analysis:

### 1. Arrow Integration Metrics
Found in the `Arrow Integration` section of the report and `results.csv`:
- **Expansion**: Measures column complexity (cols/row).
- **Denorm Fan-out**: The ratio of input messages to output rows (multi-row expansion).
- **Avg Batch**: Actual rows and KB per emitted RecordBatch.

### 2. Streaming Performance
- **Consumer Lag**: Messages pending in the broker.
- **Buffer Depth**: Messages pending in the internal transcoder buffer.
- **Peak Metrics**: Evaluates stability boundaries by tracking the highest recorded lag/depth.

### 3. Latency Characterization
Standardized to **nanoseconds (ns)** across all reports:
- **Produce**: Network/Broker entry latency.
- **Consume**: Ingestion and Transcoding latency.
- **Batch Output**: Arrow record serialization and emission time.

## Evaluation Results
All results are automatically aggregated into:
- `results/all-experiments/results.csv`: A machine-readable matrix of all phases.
- `results/all-experiments/*.txt`: Human-readable summaries for individual trials.

---

## Troubleshooting

### "nats: no response from stream"
Ensure NATS is started with `-js` (JetStream enabled).

### Metrics show 0 for consumption
Ensure you are using `--mode stream` if testing with the NATS broker. For internal stress tests, use `--mode direct`.

### High GC overhead
Increase `--batch-size` or enable `--hyper=true` to reduce allocation frequency.
