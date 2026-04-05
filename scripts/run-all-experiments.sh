#!/bin/bash
# run-all-experiments.sh
# ArrowFlow deterministic evaluation suite (JetStream-safe version)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROWFLOW_ROOT="$(dirname "$SCRIPT_DIR")"
BIN="$ARROWFLOW_ROOT/bin/arrowflow"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                 ArrowFlow Evaluation Suite                 ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

echo "Binary: $BIN"
echo "Build: $(go version | awk '{print $1, $2}')"
echo "Date: $(date)"
echo ""

RESULTS_DIR="$ARROWFLOW_ROOT/results/all-experiments"
mkdir -p "$RESULTS_DIR"

CSV_FILE="$RESULTS_DIR/results.csv"

# ------------------------------------------------------------
# Infrastructure
# ------------------------------------------------------------

echo ">>> Initializing 3-node NATS Cluster (Docker Compose)..."
docker-compose down -v >/dev/null 2>&1 || true
docker-compose up -d --quiet-pull

# ------------------------------------------------------------
# JetStream-safe readiness gate
# ------------------------------------------------------------

wait_for_nats() {
  local name=$1
  local retries=90

  echo ">>> Waiting for $name to become JetStream-ready..."

  for ((i=1; i<=retries; i++)); do

    # L2: server alive
    if ! curl -s "http://localhost:8222/varz" >/dev/null 2>&1; then
      sleep 1
      continue
    fi

    # L3: JetStream actually usable (hard validation)
    if docker exec nats-box nats stream ls \
      --server nats://nats-1:4222 >/dev/null 2>&1; then

      echo ">>> $name is JetStream-ready"
      return 0
    fi

    sleep 1
  done

  echo "ERROR: $name failed JetStream readiness check"
  exit 1
}

# Wait for cluster control plane
wait_for_nats "NATS Cluster"

# ------------------------------------------------------------
# Cluster verification (stable leader gate)
# ------------------------------------------------------------

echo ">>> Waiting for stable JetStream cluster leader..."

for i in {1..60}; do
  INFO=$(docker exec nats-box nats cluster info 2>/dev/null || true)

  if echo "$INFO" | grep -q "Leader" && echo "$INFO" | grep -q "Healthy"; then
    echo ">>> Cluster stable"
    break
  fi

  echo ">>> waiting for leader election..."
  sleep 1
done

sleep 2

# ------------------------------------------------------------
# Environment
# ------------------------------------------------------------

export NATS_URL="nats://localhost:4222,nats://localhost:4223,nats://localhost:4224"

echo ">>> Using NATS seed list: $NATS_URL"
echo ""

# ------------------------------------------------------------
# CSV header
# ------------------------------------------------------------

echo "experiment,rate_input,hyper,workers,batch_size,size_dist,denorm,rate_observed,messages,duration_sec,latency_consume_mean_ns,latency_consume_p99_ns,latency_produce_mean_ns,latency_batch_mean_ns,memory_heap_mb,memory_gc_count,peak_lag,peak_buffer,avg_batch_rows,avg_batch_kb,avg_expansion,avg_fanout" > "$CSV_FILE"

# ------------------------------------------------------------
# Experiment runner
# ------------------------------------------------------------

run_experiment() {
  local name="$1"
  local rate="$2"
  local hyper="$3"
  local duration="$4"
  local workers="$5"
  local batch_size="${6:-1000}"
  local size_dist="${7:-heavy-tail}"
  local denorm="${8:-true}"

  echo ">>> Running: $name"

  rm -f "$ARROWFLOW_ROOT/telemetry.json"

  OUTPUT=$("$BIN" experiment --mode stream \
    --rate "$rate" \
    --duration "$duration" \
    --workers "$workers" \
    --hyper="$hyper" \
    --batch-size="$batch_size" \
    --size-dist="$size_dist" \
    --denorm="$denorm" 2>&1) || true

  echo "$OUTPUT" > "$RESULTS_DIR/${name}.txt"

  RATE_OBSERVED=$(echo "$OUTPUT" | grep -E "Rate:" | awk '{print $2}')
  MESSAGES=$(echo "$OUTPUT" | grep -E "Messages:" | awk '{print $2}')
  DURATION_OUT=$(echo "$OUTPUT" | grep -E "Duration:" | awk '{print $2}' | tr -d 's')

  LAT_MEAN=$(echo "$OUTPUT" | grep "Consume:" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P99=$(echo "$OUTPUT" | grep "Consume:" | awk -F'p99=' '{print $2}' | awk -F',' '{print $1}')
  LAT_BATCH_MEAN=$(echo "$OUTPUT" | grep "Batch Output:" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}')
  LAT_PROD_MEAN=$(echo "$OUTPUT" | grep "Produce:" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}')

  HEAP=$(echo "$OUTPUT" | grep "Heap Alloc:" | awk '{print $3}' | tr -d 'MB')
  GC=$(echo "$OUTPUT" | grep "GCs:" | awk '{print $2}')

  PEAK_LAG=$(echo "$OUTPUT" | grep "Consumer Lag:" | awk -F'Peak: ' '{print $2}')
  PEAK_BUFFER=$(echo "$OUTPUT" | grep "Buffer Depth:" | awk -F'Peak: ' '{print $2}')

  AVG_ROWS=$(echo "$OUTPUT" | grep "Avg Batch:" | awk '{print $3}')
  AVG_KB=$(echo "$OUTPUT" | grep "Avg Batch:" | awk -F'(' '{print $2}' | awk '{print $1}')
  EXPANSION=$(echo "$OUTPUT" | grep "Expansion:" | awk '{print $2}')
  FANOUT=$(echo "$OUTPUT" | grep "Denorm Fanout:" | awk '{print $3}')

  # defaults
  RATE_OBSERVED=${RATE_OBSERVED:-0}
  MESSAGES=${MESSAGES:-0}
  DURATION_OUT=${DURATION_OUT:-0}
  LAT_MEAN=${LAT_MEAN:-0}
  LAT_P99=${LAT_P99:-0}
  LAT_BATCH_MEAN=${LAT_BATCH_MEAN:-0}
  LAT_PROD_MEAN=${LAT_PROD_MEAN:-0}
  HEAP=${HEAP:-0}
  GC=${GC:-0}
  PEAK_LAG=${PEAK_LAG:-0}
  PEAK_BUFFER=${PEAK_BUFFER:-0}
  AVG_ROWS=${AVG_ROWS:-0}
  AVG_KB=${AVG_KB:-0}
  EXPANSION=${EXPANSION:-0}
  FANOUT=${FANOUT:-0}

  echo "$name,$rate,$hyper,$workers,$batch_size,$size_dist,$denorm,$RATE_OBSERVED,$MESSAGES,$DURATION_OUT,$LAT_MEAN,$LAT_P99,$LAT_PROD_MEAN,$LAT_BATCH_MEAN,$HEAP,$GC,$PEAK_LAG,$PEAK_BUFFER,$AVG_ROWS,$AVG_KB,$EXPANSION,$FANOUT" >> "$CSV_FILE"

  echo "    Observed=$RATE_OBSERVED msg/s | Lat=$LAT_MEAN ns | Fanout=$FANOUT"
}

# ------------------------------------------------------------
# Experiments
# ------------------------------------------------------------

echo "--- Phase 1 ---"
for DIST in small medium large heavy-tail; do
  run_experiment "crossover_${DIST}_ht_true" 20000 true 15s 8 1000 "$DIST" true
  run_experiment "crossover_${DIST}_ht_false" 20000 false 15s 8 1000 "$DIST" true
done

echo "--- Phase 2 ---"
for BATCH in 100 500 1000 5000 10000; do
  run_experiment "batch_${BATCH}" 50000 true 15s 8 "$BATCH" heavy-tail true
done

echo "--- Phase 3 ---"
run_experiment "struct_nested" 50000 true 20s 8 1000 heavy-tail false
run_experiment "struct_denorm" 50000 true 20s 8 1000 heavy-tail true

echo "--- Phase 4 ---"
for RATE in 10000 50000 100000 150000 200000; do
  run_experiment "saturation_${RATE}" "$RATE" true 15s 8 1000 heavy-tail true
done

echo "--- Phase 5 ---"
for WORKERS in 2 4 8 16; do
  run_experiment "scale_${WORKERS}" 50000 true 15s "$WORKERS" 1000 heavy-tail true
done

echo "--- Phase 6 (chaos) ---"
"$BIN" chaos --rate 25000 --duration 20s --mode burst --chaos-burst --chaos-schema --workers 8 --hyper=true \
  > "$RESULTS_DIR/chaos_extended.txt" 2>&1 || true

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------

echo ""
echo "Results: $CSV_FILE"
echo ""

echo "All done."
