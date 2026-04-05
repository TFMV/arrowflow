# ArrowFlow Analysis Results

## Executive Summary

This run used the corrected ArrowFlow harness, a real 3-node JetStream cluster with `Replicas=3` and file storage, and a direct corpus benchmark path that warms HyperType before the clock starts. The system is substantially more trustworthy than the earlier research-only version because the benchmark path now acks after Arrow flush, isolates worker state correctly, uses real denorm fanout, parallelizes JetStream pulls, and honors command flags correctly.

The measured bottleneck is now clearer:
- the direct Arrow/`bufarrowlib` path is fast enough that the broker becomes the visible ceiling
- the replicated broker path dominates end-to-end STREAM throughput once parsing gets cheap enough
- aggressive batch sizes mostly trade memory and buffer depth for little or no throughput gain

## Environment

- Host: single machine Docker cluster
- Broker: 3-node NATS JetStream
- Stream storage: file
- Replication: 3
- Date: April 5, 2026
- Result matrix: `results/all-experiments/results.csv`

## Key Findings

### 1. The direct Protobuf -> Arrow path is much faster than the STREAM ceiling

One direct corpus benchmark pass:

- `100000` messages
- fixed payload size `1024`
- `8` workers
- `batch=1000`
- HyperType enabled
- denorm enabled
- result: `91417.93 msg/s`
- throughput: `237.73 MB/s`
- mean consume latency: `82.8 us`

One heavy-tail direct run:

- `49999.20 msg/s` sustained for `10s`
- `444.37 MB/s`
- `35.8 us` mean consume latency
- `6.4 us` mean batch-output latency

That separation matters. ArrowFlow is no longer measuring a mysterious `bufarrowlib` ceiling in the low thousands. The direct path is fast; the replicated broker path is what flattens first.

### 2. HyperType is consistently beneficial for consume latency

Mean consume-latency speedup versus `--hyper=false`:

| Distribution | HyperType Mean | Baseline Mean | Speedup |
| :--- | :--- | :--- | :--- |
| Small | 8.9 us | 24.2 us | 2.71x |
| Medium | 14.5 us | 36.0 us | 2.49x |
| Large | 113.3 us | 158.0 us | 1.39x |
| Heavy-tail | 38.8 us | 73.5 us | 1.90x |

Observed throughput did not increase in every STREAM distribution. That is expected once the run becomes broker-bound: lower consumer-side latency does not guarantee higher end-to-end throughput when replicated admission is the bottleneck.

### 3. Batch size `500` was the best operating point in this run

For heavy-tail STREAM mode at `50000 msg/s` offered load:

| Batch | Observed Rate | Mean Consume | Heap |
| :--- | :--- | :--- | :--- |
| 100 | 4753.43 msg/s | 38.3 us | 37.76 MB |
| 500 | 4838.98 msg/s | 35.8 us | 242.63 MB |
| 1000 | 4318.51 msg/s | 39.9 us | 426.72 MB |
| 5000 | 3131.43 msg/s | 53.4 us | 1944.95 MB |
| 10000 | 3837.94 msg/s | 65.1 us | 2149.75 MB |

Large batches drastically increased heap usage and internal buffer depth without producing better throughput.

### 4. Denormalized Arrow output still wins on this schema

At `50000 msg/s`, `8 workers`, `batch=1000`:

| Mode | Observed Rate | Mean Consume | Fanout |
| :--- | :--- | :--- | :--- |
| Nested | 4492.68 msg/s | 46.1 us | 1.00x |
| Denorm | 4639.97 msg/s | 36.2 us | 71.79x |

This result is specific to the current denorm plan and schema shape. It does not mean denormalization is free; it means the current `bufarrowlib` denorm path is still cheaper than the nested path for this event structure, even while producing a very large row fanout.

### 5. Throughput ceiling is broker-path dominated

Saturation sweep:

| Offered Rate | Observed Rate | Mean Consume | Mean Produce |
| :--- | :--- | :--- | :--- |
| 10000 | 4437.00 msg/s | 36.6 us | 1801.4 us |
| 50000 | 3835.44 msg/s | 40.6 us | 2084.3 us |
| 100000 | 4418.79 msg/s | 37.4 us | 1808.7 us |
| 150000 | 4680.39 msg/s | 39.7 us | 1707.6 us |
| 200000 | 4661.59 msg/s | 37.4 us | 1714.7 us |

The ceiling moved materially once the harness bugs were fixed, but the dominant cost is still in broker admission and replication rather than Arrow append. The useful summary is not “`200000` offered beats `10000` offered.” It is that the replicated STREAM path plateaus around `4.7k msg/s` while the direct Arrow path is an order of magnitude faster.

### 6. Worker scaling is visible now, but still not linear

At `50000 msg/s` offered load:

| Workers | Observed Rate | Mean Consume | Heap |
| :--- | :--- | :--- | :--- |
| 2 | 2540.71 msg/s | 31.9 us | 184.60 MB |
| 4 | 3249.53 msg/s | 38.2 us | 206.36 MB |
| 8 | 4247.79 msg/s | 38.6 us | 421.85 MB |
| 16 | 4692.71 msg/s | 47.0 us | 527.59 MB |

More workers now help because the broker fetch path is no longer serialized through a single callback. The scaling is still sublinear, and the extra concurrency eventually shifts cost into publish latency and memory pressure.

### 7. Chaos mode stressed memory and buffering more than lag

Chaos run summary:
- Consumed messages: `87418`
- Duration: `20.0s`
- Produce mean latency: `1.79 ms`
- Consume mean latency: `34.6 us`
- Heap alloc: `287.25 MB`
- Peak buffer depth: `7875`
- Peak consumer lag: `108`

That profile suggests the consumer-side in-process backlog was the first pressure surface, not broker backlog.

## Integrity Notes

The old invalid conclusions are no longer applicable. Specifically:
- STREAM mode no longer silently acks before Arrow processing.
- The benchmark CLI now honors subcommand flags.
- The direct benchmark path now uses immutable raw corpora and explicit HyperType warmup/recompile before timing.
- Denorm fanout numbers come from repeated-field plans instead of scalar-only plans.
- The cluster path is replicated JetStream, not single-replica in-memory fallback.
- Broker fetch is parallelized with pull workers instead of a single callback funnel.
- Final CSV values were regenerated from the final command reports, which are the stable end-of-run measurements.

## Overall Read

ArrowFlow is now a credible research and systems-validation harness for this workload. It is still not a substitute for multi-host production testing, but it now exercises the right failure domains:
- replicated broker writes
- worker isolation
- shared HyperType with worker-local transcoders
- Arrow record lifecycle correctness
- meaningful denorm fanout
- deterministic benchmark reporting
