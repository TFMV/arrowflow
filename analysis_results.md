# Performance Analysis: ArrowFlow Evaluation on Mac M4

This report summarizes the performance characterization and scientific findings of the ArrowFlow pipeline, evaluating `bufarrowlib` and `HyperType` efficiencies under various streaming pressures.

## TLDR; Executive Summary
- **HyperType Dominance**: HyperType (JIT decoding) consistently provides a **4x to 6x latency reduction** across all message distributions.
- **Throughput Plateau**: The current execution environment (Mac M4 + NATS JetStream) plateaus at **~1,900 msg/sec**, regardless of worker scaling or input rate.
- **Optimal Configuration**: A batch size of **1,000 rows** represents the "sweet spot," balancing GC pressure (~1.1k cycles) with stable consumption latency (~6.7μs).
- **Denormalization Efficiency**: Flattening structural data (denorm) is unexpectedly **more efficient** for `bufarrowlib` than nested processing in this schema, yielding a **72% reduction** in mean consumption latency.

---

## Key Results & Findings

### 1. The HyperType Advantage
The evaluation compared standard decoding (`ht_false`) against HyperType JIT decoding (`ht_true`) across four message size distributions:

| Distribution | Baseline Latency (mean) | HyperType Latency (mean) | Improvement |
| :--- | :--- | :--- | :--- |
| **Small** | 12,897 ns | 2,913 ns | **4.4x** |
| **Medium** | 17,484 ns | 3,642 ns | **4.8x** |
| **Large** | 45,635 ns | 10,691 ns | **4.2x** |
| **Heavy-Tail** | 31,628 ns | 5,015 ns | **6.3x** |

**Finding**: HyperType is not just for large messages. The 4.4x speedup on `small` payloads suggests the efficiency of compiled field access on the Mac M4 far outweighs the JIT context overhead even at minimal message sizes.

### 2. Throughput Saturation & Scaling Indicators
Testing input rates from 10,000 to 200,000 messages/sec revealed a consistent throughput ceiling:

- **Observed Plateau**: All tests beyond the saturation point converged to **1,850 - 1,950 msg/s**.
- **Worker Scaling**: Increasing workers from 2 to 16 yielded **0% improvement** in total throughput, remaining fixed at ~1,900 msg/s.
- **Analysis**: This indicates a single-threaded bottleneck, likely in the NATS consumer fetch mechanism or the synchronous RecordBatch emission path within the harness.

### 3. Structural Efficiency: Nested vs. Denormalized
We compared processing raw nested events against denormalizing them into flat Arrow rows:

- **Nested Latency**: 19,362 ns (Mean Consume)
- **Denorm Latency**: 5,376 ns (Mean Consume)
- **Finding**: Denormalization into flat rows is significantly faster (**72% lower latency**) for `bufarrowlib`. The denormalization plan in `bufarrowlib` appears highly optimized relative to generic recursive traversal of nested Protobuf structures.

### 4. Batch Size & GC Behavior
Sweeping batch sizes from 100 to 10,000 rows provided insights into memory pressure:

- **GC Behavior**: GC counts remained relatively stable (~1,100 - 1,200) across all batch sizes, but showed a slight downward trend at the highest batch settings (10k rows), suggesting better allocation amortisation.
- **Consumption Latency**: Latency remained stable (~5.4μs - 6.7μs) across the range, with a slight increase at the 10,000-row mark.

---

## Implementation & bufarrowlib Constraints

The following constraints were documented during the evaluation process (`experiment-log.md`):

> [!IMPORTANT]
> **Concurrency Safety**: `Transcoder` and `HyperType` contexts are **NOT thread-safe**. Concurrent access to the same context will cause segmentation violations or internal state corruption. **Each worker must use its own isolated Transcoder and HyperType instances.**

> [!NOTE]
> **Precision Requirements**: Standard millisecond benchmarks are insufficient for `bufarrowlib`. All performance evaluations must use **nanosecond resolution** to capture the 2μs - 15μs hot-path efficiencies accurately.

> [!TIP]
> **Transport Overhead**: Broker settings significantly impact observed performance. Low-latency evaluation requires a pre-configured NATS JetStream server; otherwise, `Produce` latencies can spike to ~500ms due to unbuffered direct NATS fallback logic.

---
- **Hardware Profile**: Mac M4 (Apple Silicon)
- **Broker Config**: NATS JetStream (Memory Storage Fallback)
- **Evaluation Date**: April 4, 2026
