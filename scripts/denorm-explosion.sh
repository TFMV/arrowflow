#!/bin/bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

RESULTS_DIR="$ARROWFLOW_ROOT/results/denorm-explosion"
CSV_FILE="$RESULTS_DIR/results.csv"

mkdir -p "$RESULTS_DIR"
ensure_binary

echo "=== Finding Denorm Explosion Point ==="
echo "Testing at increasing throughput to detect structural instability"
echo "Binary: $BIN"
echo

echo "rate,workers,rate_observed,messages,duration_sec,latency_mean_ns,latency_p50_ns,latency_p95_ns,latency_p99_ns,latency_p999_ns,memory_heap_mb,memory_heap_inuse_mb,memory_gc_count,memory_gc_pause_ms,avg_fanout" >"$CSV_FILE"

for rate in 5000 10000 20000 30000 50000 75000 100000; do
  for workers in 4 8; do
    echo "--- Rate: $rate msg/s, Workers: $workers ---"
    output_file="$RESULTS_DIR/rate-${rate}-workers-${workers}.txt"

    run_arrowflow "$output_file" experiment --mode direct --rate "$rate" --duration 15s --workers "$workers" --denorm=true

    rate_observed="$(summary_value "$output_file" "Rate" | awk '{print $1}')"
    messages="$(summary_value "$output_file" "Messages")"
    duration_out="$(duration_to_seconds "$(summary_value "$output_file" "Duration")")"
    lat_mean="$(latency_value "$output_file" "Consume" "mean")"
    lat_p50="$(latency_value "$output_file" "Consume" "p50")"
    lat_p95="$(latency_value "$output_file" "Consume" "p95")"
    lat_p99="$(latency_value "$output_file" "Consume" "p99")"
    lat_p999="$(latency_value "$output_file" "Consume" "p99.9")"
    heap_alloc="$(scalar_value "$output_file" "Heap Alloc")"
    heap_inuse="$(scalar_value "$output_file" "Heap In Use")"
    gc_count="$(scalar_value "$output_file" "GCs")"
    gc_pause="$(scalar_value "$output_file" "GC Pause")"
    fanout="$(scalar_value "$output_file" "Denorm Fanout")"

    echo "$rate,$workers,$rate_observed,$messages,$duration_out,$lat_mean,$lat_p50,$lat_p95,$lat_p99,$lat_p999,$heap_alloc,$heap_inuse,$gc_count,$gc_pause,$fanout" >>"$CSV_FILE"
    echo "  Observed: $rate_observed msg/s"
    echo "  Fanout: $fanout"
    echo
  done
done

echo "Results: $CSV_FILE"
