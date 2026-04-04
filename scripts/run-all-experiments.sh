#!/bin/bash
# run-all-experiments.sh
# Complete evaluation suite for ArrowFlow scientific analysis

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
# Header
echo "experiment,rate_input,hyper,workers,batch_size,size_dist,denorm,rate_observed,messages,duration_sec,latency_consume_mean_ns,latency_consume_p99_ns,latency_produce_mean_ns,latency_batch_mean_ns,memory_heap_mb,memory_gc_count,peak_lag,peak_buffer,avg_batch_rows,avg_batch_kb,avg_expansion,avg_fanout" > "$CSV_FILE"

run_experiment() {
  local name="$1"
  local rate="$2"
  local hyper="$3"
  local duration="$4"
  local workers="$5"
  local batch_size="${6:-1000}"
  local size_dist="${7:-heavy-tail}"
  local denorm="${8:-true}"
  
  echo ">>> Running: $name (rate=$rate, hyper=$hyper, workers=$workers, batch=$batch_size, dist=$size_dist, denorm=$denorm)"
  
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
  
  # Base metrics
  RATE_OBSERVED=$(echo "$OUTPUT" | grep -E "^\s*Rate:" | awk '{print $2}' | tr -d ' ')
  MESSAGES=$(echo "$OUTPUT" | grep -E "^\s*Messages:" | awk '{print $2}' | tr -d ' ')
  DURATION_OUT=$(echo "$OUTPUT" | grep -E "^\s*Duration:" | awk '{print $2}' | tr -d 's')
  
  LAT_MEAN=$(echo "$OUTPUT" | grep "Consume:" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  LAT_P99=$(echo "$OUTPUT" | grep "Consume:" | awk -F'p99=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  LAT_BATCH_MEAN=$(echo "$OUTPUT" | grep "Batch Output:" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  LAT_PROD_MEAN=$(echo "$OUTPUT" | grep "Produce:" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  
  HEAP=$(echo "$OUTPUT" | grep "Heap Alloc:" | awk '{print $3}' | tr -d 'MB')
  GC=$(echo "$OUTPUT" | grep "GCs:" | awk '{print $2}' | tr -d ' ')
  
  PEAK_LAG=$(echo "$OUTPUT" | grep "Consumer Lag:" | awk -F'Peak: ' '{print $2}' | tr -d ')' | tr -d ' ')
  PEAK_BUFFER=$(echo "$OUTPUT" | grep "Buffer Depth:" | awk -F'Peak: ' '{print $2}' | tr -d ')' | tr -d ' ')
  
  # Arrow Integration Metrics
  AVG_ROWS=$(echo "$OUTPUT" | grep "Avg Batch:" | awk '{print $3}' | tr -d ' ')
  AVG_KB=$(echo "$OUTPUT" | grep "Avg Batch:" | awk -F'(' '{print $2}' | awk '{print $1}' | tr -d ' ')
  EXPANSION=$(echo "$OUTPUT" | grep "Expansion:" | awk '{print $2}' | tr -d ' ')
  FANOUT=$(echo "$OUTPUT" | grep "Denorm Fanout:" | awk '{print $3}' | tr -d ' ')

  # Defaults for missing data
  RATE_OBSERVED=${RATE_OBSERVED:-0}; MESSAGES=${MESSAGES:-0}; DURATION_OUT=${DURATION_OUT:-0}
  LAT_MEAN=${LAT_MEAN:-0}; LAT_P99=${LAT_P99:-0}; LAT_BATCH_MEAN=${LAT_BATCH_MEAN:-0}; LAT_PROD_MEAN=${LAT_PROD_MEAN:-0}
  HEAP=${HEAP:-0}; GC=${GC:-0}; PEAK_LAG=${PEAK_LAG:-0}; PEAK_BUFFER=${PEAK_BUFFER:-0}
  AVG_ROWS=${AVG_ROWS:-0}; AVG_KB=${AVG_KB:-0}; EXPANSION=${EXPANSION:-0}; FANOUT=${FANOUT:-0}
  
  echo "$name,$rate,$hyper,$workers,$batch_size,$size_dist,$denorm,$RATE_OBSERVED,$MESSAGES,$DURATION_OUT,$LAT_MEAN,$LAT_P99,$LAT_PROD_MEAN,$LAT_BATCH_MEAN,$HEAP,$GC,$PEAK_LAG,$PEAK_BUFFER,$AVG_ROWS,$AVG_KB,$EXPANSION,$FANOUT" >> "$CSV_FILE"
  
  echo "    Observed: $RATE_OBSERVED msg/s, Cons Lat: $LAT_MEAN ns, Fan-out: $FANOUT"
}

# --- Phase 1: Baseline & Crossovers ---
echo "--- Phase 1: Baseline & HyperType Crossover ---"
for DIST in small medium large heavy-tail; do
  run_experiment "crossover_${DIST}_ht_true" 20000 true 15s 8 1000 "$DIST" true
  run_experiment "crossover_${DIST}_ht_false" 20000 false 15s 8 1000 "$DIST" true
done

# --- Phase 2: Batch Size Optimization ---
echo ""
echo "--- Phase 2: Batch Size Optimization ---"
for BATCH in 100 500 1000 5000 10000; do
  run_experiment "batch_size_${BATCH}" 50000 true 15s 8 "$BATCH" heavy-tail true
done

# --- Phase 3: Structural Cost (Nested vs Denorm) ---
echo ""
echo "--- Phase 3: Structural Cost (Nested vs Denorm) ---"
run_experiment "struct_nested" 50000 true 20s 8 1000 heavy-tail false
run_experiment "struct_denorm" 50000 true 20s 8 1000 heavy-tail true

# --- Phase 4: Throughput Saturation (Finding the Plateau) ---
echo ""
echo "--- Phase 4: Rate Sweep (Saturation Analysis) ---"
for RATE in 10000 50000 100000 150000 200000; do
  run_experiment "saturation_${RATE}" "$RATE" true 15s 8 1000 heavy-tail true
done

# --- Phase 5: Worker Scaling ---
echo ""
echo "--- Phase 5: Worker Scaling ---"
for WORKERS in 2 4 8 16; do
  run_experiment "scaling_workers_${WORKERS}" 50000 true 15s "$WORKERS" 1000 heavy-tail true
done

# --- Phase 6: Chaos & Stability ---
echo ""
echo ">>> Running extended chaos injection test..."
(
  "$BIN" chaos --rate 25000 --duration 20s --mode burst --chaos-burst --chaos-schema --workers 8 --hyper=true 2>&1
) > "$RESULTS_DIR/chaos_extended.txt" 2>&1 || true

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Analysis Summary                                        ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Results: $CSV_FILE"
echo ""
echo "=== HyperType Benefit by Message Size ==="
grep "crossover" "$CSV_FILE" | awk -F',' '{print $1,$6,$3,$11}' | sort -k2
echo ""
echo "=== Batch Size vs GC Pause ==="
grep "batch_size" "$CSV_FILE" | awk -F',' '{print "Batch:",$5,"GCs:",$16,"Latency:",$11}'
echo ""
echo "=== Denorm vs Nested Efficiency ==="
grep "struct_" "$CSV_FILE" | awk -F',' '{print $1,"Observed Rate:",$8,"Expansion:",$21}'
echo ""
echo "=== Peak Throughput Plateaus ==="
grep "saturation" "$CSV_FILE" | awk -F',' '{print "Input:",$2,"Observed:",$8,"Lag:",$17}'
echo ""

echo "All results saved to: $RESULTS_DIR/"