#!/bin/bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

RESULTS_DIR="$ARROWFLOW_ROOT/results/denorm-phase-transition"
CSV_FILE="$RESULTS_DIR/results.csv"

mkdir -p "$RESULTS_DIR"
ensure_binary

echo "=== Finding Denorm Phase Transition ==="
echo "This experiment measures where fan-out becomes nonlinear"
echo "Binary: $BIN"
echo

echo "rate_input,rate_observed,messages,duration_sec,latency_consume_mean_ns,latency_consume_p50_ns,latency_consume_p95_ns,latency_consume_p99_ns,latency_consume_p999_ns,memory_heap_mb,memory_gc_count,avg_fanout" >"$CSV_FILE"

for rate in 1000 5000 10000 25000 50000 75000 100000; do
  echo "--- Testing rate: $rate msg/s ---"
  output_file="$RESULTS_DIR/rate-${rate}.txt"

  run_arrowflow "$output_file" experiment --mode direct --rate "$rate" --duration 20s --workers 8 --denorm=true

  rate_observed="$(summary_value "$output_file" "Rate" | awk '{print $1}')"
  messages="$(summary_value "$output_file" "Messages")"
  duration_out="$(duration_to_seconds "$(summary_value "$output_file" "Duration")")"
  lat_mean="$(latency_value "$output_file" "Consume" "mean")"
  lat_p50="$(latency_value "$output_file" "Consume" "p50")"
  lat_p95="$(latency_value "$output_file" "Consume" "p95")"
  lat_p99="$(latency_value "$output_file" "Consume" "p99")"
  lat_p999="$(latency_value "$output_file" "Consume" "p99.9")"
  heap_alloc="$(scalar_value "$output_file" "Heap Alloc")"
  gc_count="$(scalar_value "$output_file" "GCs")"
  fanout="$(scalar_value "$output_file" "Denorm Fanout")"

  echo "$rate,$rate_observed,$messages,$duration_out,$lat_mean,$lat_p50,$lat_p95,$lat_p99,$lat_p999,$heap_alloc,$gc_count,$fanout" >>"$CSV_FILE"
  echo "  Observed rate: $rate_observed msg/s"
  echo "  Fanout: $fanout"
  echo
done

echo "Results: $CSV_FILE"
