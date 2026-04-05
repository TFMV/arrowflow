# ArrowFlow Analysis Results

## Executive Summary

This run used the corrected ArrowFlow harness and a real 3-node JetStream cluster with `Replicas=3` and file storage. The system is substantially more trustworthy than the earlier research-only version because the benchmark path now acks after Arrow flush, isolates worker state, uses real denorm fanout, and honors command flags correctly.

The measured bottleneck is now clearer:
- the Arrow/`bufarrowlib` path is fast enough to benefit from HyperType
- the replicated broker path still dominates end-to-end throughput at higher offered rates
- aggressive batch sizes mostly trade memory and buffer depth for little throughput gain

## Environment

- Host: single machine Docker cluster
- Broker: 3-node NATS JetStream
- Stream storage: file
- Replication: 3
- Date: April 4, 2026
- Result matrix: `results/all-experiments/results.csv`

## Key Findings

### 1. HyperType is consistently beneficial

Mean consume-latency speedup versus `--hyper=false`:

| Distribution | HyperType Mean | Baseline Mean | Speedup |
| :--- | :--- | :--- | :--- |
| Small | 9.1 us | 26.9 us | 2.95x |
| Medium | 19.0 us | 37.2 us | 1.95x |
| Large | 78.7 us | 119.5 us | 1.52x |
| Heavy-tail | 37.5 us | 75.8 us | 2.02x |

HyperType also improved observed throughput in every distribution in this run.

### 2. Batch size `1000` remains the best operating point

For heavy-tail STREAM mode at `50000 msg/s` offered load:

| Batch | Observed Rate | Mean Consume | Heap |
| :--- | :--- | :--- | :--- |
| 100 | 2253.99 msg/s | 41.5 us | 38.45 MB |
| 500 | 2142.19 msg/s | 40.2 us | 241.77 MB |
| 1000 | 2402.62 msg/s | 36.5 us | 223.98 MB |
| 5000 | 2068.75 msg/s | 48.7 us | 1343.04 MB |
| 10000 | 2199.06 msg/s | 53.5 us | 1477.80 MB |

Large batches drastically increased heap usage and internal buffer depth without producing better throughput.

### 3. Denormalized Arrow output wins on this schema

At `50000 msg/s`, `8 workers`, `batch=1000`:

| Mode | Observed Rate | Mean Consume | Fanout |
| :--- | :--- | :--- | :--- |
| Nested | 1998.80 msg/s | 52.8 us | 1.00x |
| Denorm | 2932.68 msg/s | 32.3 us | 72.37x |

This result is specific to the current denorm plan and schema shape. It does not mean denormalization is free; it means the current `bufarrowlib` denorm path is still cheaper than the nested path for this event structure, even while producing a very large row fanout.

### 4. Throughput ceiling is broker-path dominated

Saturation sweep:

| Offered Rate | Observed Rate | Mean Consume | Mean Produce |
| :--- | :--- | :--- | :--- |
| 10000 | 1383.99 msg/s | 45.0 us | 680.8 us |
| 50000 | 1589.45 msg/s | 66.3 us | 1204.9 us |
| 100000 | 2994.99 msg/s | 43.0 us | 1625.7 us |
| 150000 | 3511.83 msg/s | 41.2 us | 1950.1 us |
| 200000 | 5404.24 msg/s | 32.5 us | 1445.7 us |

The ceiling moved materially once the harness bugs were fixed, but the dominant cost is still in broker admission and replication rather than Arrow append.

### 5. Worker scaling is not linear

At `50000 msg/s` offered load:

| Workers | Observed Rate | Mean Consume | Heap |
| :--- | :--- | :--- | :--- |
| 2 | 2120.17 msg/s | 36.2 us | 115.32 MB |
| 4 | 2057.39 msg/s | 39.6 us | 198.02 MB |
| 8 | 1759.39 msg/s | 52.7 us | 321.36 MB |
| 16 | 2133.66 msg/s | 51.5 us | 928.94 MB |

More workers increase isolation but do not buy linear throughput in this environment. The broker path and batching behavior dominate first.

### 6. Chaos mode stressed memory and buffering, not broker lag

Chaos run summary:
- Consumed messages: `28892`
- Duration: `20.0s`
- Produce mean latency: `655.4 us`
- Consume mean latency: `38.7 us`
- Heap alloc: `365.48 MB`
- Peak buffer depth: `7817`
- Peak consumer lag: `0`

That profile suggests the consumer-side in-process backlog was the first pressure surface, not broker backlog.

## Integrity Notes

The old invalid conclusions are no longer applicable. Specifically:
- STREAM mode no longer silently acks before Arrow processing.
- The benchmark CLI now honors subcommand flags.
- Denorm fanout numbers come from repeated-field plans instead of scalar-only plans.
- The cluster path is replicated JetStream, not single-replica in-memory fallback.
- Final CSV values were regenerated from the final command reports, which are the stable end-of-run measurements.

## Overall Read

ArrowFlow is now a credible research and systems-validation harness for this workload. It is still not a substitute for multi-host production testing, but it now exercises the right failure domains:
- replicated broker writes
- worker isolation
- Arrow record lifecycle correctness
- meaningful denorm fanout
- deterministic benchmark reporting
