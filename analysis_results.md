# ArrowFlow Analysis Results

## Executive Summary

This run used the corpus-first ArrowFlow harness, a real 3-node JetStream cluster with `Replicas=3` and file storage, and a direct replay path that warms HyperType before the timer starts. The important result is that the local Protobuf -> Arrow path is now clearly faster than the replicated broker path.

- Every timed mode now prepares a bounded raw-wire corpus before measurement.
- `direct` and `stress` replay that corpus in process.
- `stream` replays the same corpus through JetStream instead of generating payloads while timing.
- The direct replay path is now well above `100k msg/s`.
- The replicated STREAM ceiling is still in the low thousands on this single-host 3-node cluster.

## Environment

- Host: single machine Docker cluster
- Broker: 3-node NATS JetStream
- Stream storage: file
- Replication: 3
- Date: April 5, 2026
- STREAM matrix: `results/all-experiments/results.csv`

## Key Findings

### 1. The direct `bufarrowlib` path is no longer ambiguous

Best direct replay runs on a prebuilt `100000`-message fixed corpus:

- Nested output, `8` workers, `batch=2000`: `182999.32 msg/s`, `476.36 MB/s`
- Denorm output, `8` workers, `batch=2000`: `113801.76 msg/s`, `296.62 MB/s`

One heavy-tail direct replay run at a fixed rate also stayed clean:

- `50000.70 msg/s` for `10s`
- `445.96 MB/s`
- `89.7 us` mean consume latency
- `8.9 us` mean batch-output latency

That means the low STREAM numbers are not a `bufarrowlib` ceiling. The local replay path is fast enough that the broker path is now the obvious bottleneck.

### 2. HyperType still helps the local consume path

Mean consume-latency speedup versus `--hyper=false` in the STREAM crossover runs:

- Small: `1.43x` (`20.6 us` -> `14.4 us`)
- Medium: `1.61x` (`34.7 us` -> `21.6 us`)
- Large: `1.52x` (`160.3 us` -> `105.5 us`)
- Heavy-tail: `1.53x` (`73.1 us` -> `47.8 us`)

What changed is the interpretation. In this fully replicated STREAM run, those latency wins did not translate into higher end-to-end throughput. Throughput was slightly lower with HyperType in each crossover pair, which is exactly what a broker-bound run looks like: consumer CPU gets cheaper, but the long pole is elsewhere.

### 3. Batch size `100` was the best STREAM operating point in this run

Heavy-tail STREAM mode at `50000 msg/s` offered load:

- `batch=100`: `5419.90 msg/s`, `45.2 us` mean consume latency, `473.48 MB` heap
- `batch=500`: `4936.59 msg/s`, `47.0 us`, `689.71 MB`
- `batch=1000`: `4750.48 msg/s`, `49.2 us`, `856.66 MB`
- `batch=5000`: `3761.00 msg/s`, `69.5 us`, `1666.72 MB`
- `batch=10000`: `3416.62 msg/s`, `76.3 us`, `2425.05 MB`

Large batches bought more heap pressure and deeper in-process queues, not better throughput.

### 4. Nested won the end-to-end STREAM comparison, but denorm stayed locally efficient

At `50000 msg/s`, `8` workers, `batch=1000`:

- Nested: `5189.99 msg/s`, `52.2 us` mean consume latency
- Denorm: `4803.75 msg/s`, `47.9 us` mean consume latency, `72.36x` fanout

This is a good example of the bottleneck shift. Denorm is still cheaper inside the consumer on this schema, but once the whole path is broker-limited the end-to-end throughput winner can still be nested output.

### 5. The STREAM ceiling is broker-path dominated

Saturation sweep:

- `10000` offered -> `4933.54 msg/s`
- `50000` offered -> `4778.72 msg/s`
- `100000` offered -> `4628.22 msg/s`
- `150000` offered -> `4395.80 msg/s`
- `200000` offered -> `5889.87 msg/s`

The best observed point was the `200000 msg/s` offered run:

- `5889.87 msg/s` observed
- `43.4 us` mean consume latency
- `1.36 ms` mean produce latency

The useful conclusion is not that `200000` offered is magically optimal. It is that the replicated STREAM path still tops out around `5k-6k msg/s` while the direct replay path is an order of magnitude faster.

### 6. Worker scaling improved, then flattened

At `50000 msg/s` offered load:

- `2 workers`: `3047.43 msg/s`
- `4 workers`: `3997.27 msg/s`
- `8 workers`: `4806.68 msg/s`
- `16 workers`: `4590.07 msg/s`

Moving to parallel pull workers helped compared with the earlier single-callback funnel. But scaling is still sublinear, and the extra concurrency starts showing up as memory pressure and publish latency before it turns into more throughput.

### 7. Chaos mode still bends the in-process pipeline before the broker

Chaos run summary:

- Consumed messages: `89893`
- Duration: `20.0s`
- Produce mean latency: `1.74 ms`
- Consume mean latency: `48.2 us`
- Heap alloc: `238.38 MB`
- Peak buffer depth: `7821`
- Peak consumer lag: `298`

That profile still points at local backlog and batch formation as the first pressure surface in this environment.

## Integrity Notes

The benchmark conclusions now rest on a much cleaner harness:

- all timed modes use prebuilt raw corpora
- HyperType warmup and `Recompile()` happen before timed replay
- worker state is isolated correctly
- consumer ack happens only after Arrow batch flush
- STREAM fetch uses parallel pull workers
- the cluster path is real replicated JetStream
- per-message Prometheus bookkeeping is no longer in the timed hot path
- final CSV rows are extracted from the final command summaries

## Overall Read

ArrowFlow is now a credible single-host systems harness for this workload. The local `bufarrowlib` path is clearly capable of `100k+ msg/s` on replay, and the current STREAM ceiling is dominated by replicated broker coordination rather than Protobuf -> Arrow transformation cost.
