#!/bin/bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

RESULTS_DIR="$ARROWFLOW_ROOT/results/all-experiments"
CSV_FILE="$RESULTS_DIR/results.csv"

mkdir -p "$RESULTS_DIR"
ensure_binary

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                 ArrowFlow Evaluation Suite                 ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo
echo "Binary: $BIN"
echo "Build: $(go version | awk '{print $1, $2}')"
echo "Date: $(date)"
echo

echo ">>> Restarting 3-node NATS JetStream cluster"
compose_cmd down -v --remove-orphans >/dev/null 2>&1 || true
compose_cmd up -d

wait_for_http 8222
wait_for_http 8223
wait_for_http 8224

for ((i = 1; i <= 60; i++)); do
  if docker exec nats-box nats --server nats://nats-1:4222 stream ls >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

export NATS_URL="nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"
export STREAM_REPLICAS=3
export STREAM_STORAGE=file
export CONSUMER_ACK_WAIT=60s
export CONSUMER_MAX_ACK_PENDING=65536

echo ">>> Using NATS seed list: $NATS_URL"
echo

echo "experiment,rate_input,hyper,workers,batch_size,size_dist,denorm,rate_observed,messages,duration_sec,latency_consume_mean_ns,latency_consume_p99_ns,latency_produce_mean_ns,latency_batch_mean_ns,memory_heap_mb,memory_gc_count,peak_lag,peak_buffer,avg_batch_rows,avg_batch_kb,avg_expansion_columns,avg_fanout" >"$CSV_FILE"

run_experiment() {
  local name="$1"
  local rate="$2"
  local hyper="$3"
  local duration="$4"
  local workers="$5"
  local batch_size="${6:-1000}"
  local size_dist="${7:-heavy-tail}"
  local denorm="${8:-true}"
  local output_file="$RESULTS_DIR/${name}.txt"

  echo ">>> Running: $name"
  run_arrowflow "$output_file" experiment \
    --mode stream \
    --rate "$rate" \
    --duration "$duration" \
    --workers "$workers" \
    --hyper="$hyper" \
    --batch-size="$batch_size" \
    --size-dist="$size_dist" \
    --denorm="$denorm"

  local rate_observed messages duration_out
  local lat_mean lat_p99 lat_prod_mean lat_batch_mean
  local heap gc peak_lag peak_buffer avg_rows avg_kb expansion fanout

  rate_observed="$(summary_value "$output_file" "Rate" | awk '{print $1}')"
  messages="$(summary_value "$output_file" "Messages")"
  duration_out="$(duration_to_seconds "$(summary_value "$output_file" "Duration")")"

  lat_mean="$(latency_value "$output_file" "Consume" "mean")"
  lat_p99="$(latency_value "$output_file" "Consume" "p99")"
  lat_prod_mean="$(latency_value "$output_file" "Produce" "mean")"
  lat_batch_mean="$(latency_value "$output_file" "Batch Output" "mean")"
  heap="$(scalar_value "$output_file" "Heap Alloc")"
  gc="$(scalar_value "$output_file" "GCs")"
  peak_lag="$(peak_value "$output_file" "Consumer Lag")"
  peak_buffer="$(peak_value "$output_file" "Buffer Depth")"
  avg_rows="$(avg_batch_rows "$output_file")"
  avg_kb="$(avg_batch_kb "$output_file")"
  expansion="$(scalar_value "$output_file" "Expansion")"
  fanout="$(scalar_value "$output_file" "Denorm Fanout")"

  echo "$name,$rate,$hyper,$workers,$batch_size,$size_dist,$denorm,$rate_observed,$messages,$duration_out,$lat_mean,$lat_p99,$lat_prod_mean,$lat_batch_mean,$heap,$gc,$peak_lag,$peak_buffer,$avg_rows,$avg_kb,$expansion,$fanout" >>"$CSV_FILE"
  echo "    Observed=${rate_observed} msg/s | Consume mean=${lat_mean} ns | Fanout=${fanout}"
}

echo "--- Phase 1 ---"
for dist in small medium large heavy-tail; do
  run_experiment "crossover_${dist}_ht_true" 20000 true 15s 8 1000 "$dist" true
  run_experiment "crossover_${dist}_ht_false" 20000 false 15s 8 1000 "$dist" true
done

echo "--- Phase 2 ---"
for batch in 100 500 1000 5000 10000; do
  run_experiment "batch_${batch}" 50000 true 15s 8 "$batch" heavy-tail true
done

echo "--- Phase 3 ---"
run_experiment "struct_nested" 50000 true 20s 8 1000 heavy-tail false
run_experiment "struct_denorm" 50000 true 20s 8 1000 heavy-tail true

echo "--- Phase 4 ---"
for rate in 10000 50000 100000 150000 200000; do
  run_experiment "saturation_${rate}" "$rate" true 15s 8 1000 heavy-tail true
done

echo "--- Phase 5 ---"
for workers in 2 4 8 16; do
  run_experiment "scale_${workers}" 50000 true 15s "$workers" 1000 heavy-tail true
done

echo "--- Phase 6 (chaos) ---"
run_arrowflow "$RESULTS_DIR/chaos_extended.txt" chaos \
  --rate 25000 \
  --duration 20s \
  --mode burst \
  --chaos-burst \
  --chaos-schema \
  --chaos-hot-partition \
  --chaos-size-shock \
  --workers 8 \
  --hyper=true

echo
echo "Results: $CSV_FILE"
echo
echo "All experiments completed."
