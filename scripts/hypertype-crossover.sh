#!/bin/bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

RESULTS_DIR="$ARROWFLOW_ROOT/results/hypertype-crossover"
CSV_FILE="$RESULTS_DIR/results.csv"

mkdir -p "$RESULTS_DIR"
ensure_binary

echo "=== Finding HyperType Crossover Point ==="
echo "Comparing decode performance with and without HyperType"
echo "Binary: $BIN"
echo

echo "rate,hyper_enabled,rate_observed,messages,duration_sec,latency_mean_ns,latency_p50_ns,latency_p95_ns,latency_p99_ns,latency_p999_ns,memory_heap_mb,memory_gc_count" >"$CSV_FILE"

for rate in 5000 10000 25000 50000 100000; do
  for hyper in false true; do
    echo "--- Rate: $rate msg/s, HyperType: $hyper ---"
    output_file="$RESULTS_DIR/${hyper}-${rate}.txt"

    run_arrowflow "$output_file" experiment --mode direct --rate "$rate" --duration 15s --workers 4 --hyper="$hyper"

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

    echo "$rate,$hyper,$rate_observed,$messages,$duration_out,$lat_mean,$lat_p50,$lat_p95,$lat_p99,$lat_p999,$heap_alloc,$gc_count" >>"$CSV_FILE"
    echo "  Observed: $rate_observed msg/s"
    echo "  Mean latency: $lat_mean ns"
    echo
  done
done

echo "Results: $CSV_FILE"
