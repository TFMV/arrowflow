#!/bin/bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

RESULTS_DIR="$ARROWFLOW_ROOT/results/gc-phase-shifts"
CSV_FILE="$RESULTS_DIR/results.csv"

mkdir -p "$RESULTS_DIR"
ensure_binary

echo "=== Finding GC Phase Transitions ==="
echo "Sweeping batch sizes to find GC behavior changes"
echo "Binary: $BIN"
echo

echo "batch_size,rate_observed,messages,duration_sec,latency_mean_ns,latency_p50_ns,latency_p95_ns,latency_p99_ns,memory_heap_mb,memory_heap_inuse_mb,memory_gc_count,memory_gc_pause_ms" >"$CSV_FILE"

for batch in 100 500 1000 2500 5000 10000; do
  echo "--- Batch size: $batch ---"
  output_file="$RESULTS_DIR/batch-${batch}.txt"

  run_arrowflow "$output_file" experiment --mode direct --rate 10000 --duration 15s --workers 4 --batch-size "$batch"

  rate_observed="$(summary_value "$output_file" "Rate" | awk '{print $1}')"
  messages="$(summary_value "$output_file" "Messages")"
  duration_out="$(duration_to_seconds "$(summary_value "$output_file" "Duration")")"
  lat_mean="$(latency_value "$output_file" "Consume" "mean")"
  lat_p50="$(latency_value "$output_file" "Consume" "p50")"
  lat_p95="$(latency_value "$output_file" "Consume" "p95")"
  lat_p99="$(latency_value "$output_file" "Consume" "p99")"
  heap_alloc="$(scalar_value "$output_file" "Heap Alloc")"
  heap_inuse="$(scalar_value "$output_file" "Heap In Use")"
  gc_count="$(scalar_value "$output_file" "GCs")"
  gc_pause="$(scalar_value "$output_file" "GC Pause")"

  echo "$batch,$rate_observed,$messages,$duration_out,$lat_mean,$lat_p50,$lat_p95,$lat_p99,$heap_alloc,$heap_inuse,$gc_count,$gc_pause" >>"$CSV_FILE"
  echo "  Rate: ${rate_observed} msg/s"
  echo "  Messages: $messages"
  echo "  Latency mean/p99: ${lat_mean} / ${lat_p99} ns"
  echo "  Heap: ${heap_alloc} MB"
  echo "  GCs: $gc_count"
  echo
done

echo "Results: $CSV_FILE"
