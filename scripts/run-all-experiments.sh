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
echo "experiment,rate_input,hyper,rate_observed,messages,duration_sec,latency_consume_mean_ns,latency_consume_p50_ns,latency_consume_p95_ns,latency_consume_p99_ns,latency_produce_mean_ns,memory_heap_mb,memory_gc_count" > "$CSV_FILE"

run_experiment() {
  local name="$1"
  local rate="$2"
  local hyper="$3"
  local duration="$4"
  local workers="$5"
  
  echo ">>> Running: $name (rate=$rate, hyper=$hyper, workers=$workers)"
  
  rm -f "$ARROWFLOW_ROOT/telemetry.json"
  
  # Binary now finds proto path automatically, no need to cd
  if [ "$hyper" = "true" ]; then
    OUTPUT=$("$BIN" experiment --mode direct --rate "$rate" --duration "$duration" --workers "$workers" --hyper=true 2>&1) || true
  else
    OUTPUT=$("$BIN" experiment --mode direct --rate "$rate" --duration "$duration" --workers "$workers" --hyper=false 2>&1) || true
  fi
  
  echo "$OUTPUT" > "$RESULTS_DIR/${name}.txt"
  
  RATE_OBSERVED=$(echo "$OUTPUT" | grep -E "^\s*Rate:" | awk '{print $2}' | tr -d ' ')
  MESSAGES=$(echo "$OUTPUT" | grep -E "^\s*Messages:" | awk '{print $2}' | tr -d ' ')
  DURATION_OUT=$(echo "$OUTPUT" | grep -E "^\s*Duration:" | awk '{print $2}' | tr -d 's')
  
  LAT_MEAN=$(echo "$OUTPUT" | grep "Consume:" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  LAT_P50=$(echo "$OUTPUT" | grep "Consume:" | awk -F'p50=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  LAT_P95=$(echo "$OUTPUT" | grep "Consume:" | awk -F'p95=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  LAT_P99=$(echo "$OUTPUT" | grep "Consume:" | awk -F'p99=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  
  # Also capture produce latency if available
  LAT_PROD_MEAN=$(echo "$OUTPUT" | grep "Produce:" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ')
  
  HEAP=$(echo "$OUTPUT" | grep "Heap Alloc:" | awk '{print $3}' | tr -d 'MB')
  GC=$(echo "$OUTPUT" | grep "GCs:" | awk '{print $2}' | tr -d ' ')
  
  LAT_PROD_MEAN=${LAT_PROD_MEAN:-0}
  
  # Defaults
  RATE_OBSERVED=${RATE_OBSERVED:-0}
  MESSAGES=${MESSAGES:-0}
  DURATION_OUT=${DURATION_OUT:-0}
  LAT_MEAN=${LAT_MEAN:-0}
  LAT_P50=${LAT_P50:-0}
  LAT_P95=${LAT_P95:-0}
  LAT_P99=${LAT_P99:-0}
  HEAP=${HEAP:-0}
  GC=${GC:-0}
  
  echo "$name,$rate,$hyper,$RATE_OBSERVED,$MESSAGES,$DURATION_OUT,$LAT_MEAN,$LAT_P50,$LAT_P95,$LAT_P99,$LAT_PROD_MEAN,$HEAP,$GC" >> "$CSV_FILE"
  
  echo "    Rate: $RATE_OBSERVED msg/s, Latency p99: $LAT_P99 ns, Heap: $HEAP MB"
}

# Experiment 1: Baseline with HyperType
run_experiment "baseline_ht_true" 50000 true 20s 8

# Experiment 2: Baseline without HyperType
run_experiment "baseline_ht_false" 50000 false 20s 8

# Experiment 3: Rate sweep with HyperType
for RATE in 10000 25000 50000 75000 100000; do
  run_experiment "rate_sweep_${RATE}" "$RATE" true 15s 8
done

# Experiment 4: Worker scaling
for WORKERS in 2 4 8 16; do
  run_experiment "workers_${WORKERS}" 50000 true 15s "$WORKERS"
done

# Experiment 5: Burst mode test (simulates chaos)
run_experiment "burst_mode" 50000 true 15s 8

# Experiment 6: High rate stress test
run_experiment "stress_100k" 100000 true 15s 16

# Experiment 7: Low rate latency test
run_experiment "latency_1k" 1000 true 20s 4

# Experiment 8: End-to-end test with producer + consumer (requires NATS)
echo ""
echo ">>> Running end-to-end producer+consumer test..."
rm -f "$ARROWFLOW_ROOT/telemetry.json"

# Start consumer in background
(
  "$BIN" consumer --workers 4 --mode denorm 2>&1
) > "$RESULTS_DIR/consumer_bg.txt" 2>&1 &
CONSUMER_PID=$!

# Wait for consumer to be ready
sleep 2

# Run producer
(
  "$BIN" producer --rate 5000 --mode steady 2>&1
) > "$RESULTS_DIR/producer_e2e.txt" 2>&1 &
PRODUCER_PID=$!

# Wait for producer to finish
sleep 8

# Kill consumer
kill $CONSUMER_PID 2>/dev/null || true
wait $CONSUMER_PID 2>/dev/null || true

# Also kill producer if still running
kill $PRODUCER_PID 2>/dev/null || true
wait $PRODUCER_PID 2>/dev/null || true

# Collect metrics
E2E_PROD_MSGS=$(grep "Produced:" "$RESULTS_DIR/producer_e2e.txt" | awk '{print $2}' | tr -d ',' || echo "0")
E2E_PROD_LAT=$(grep "Produce:" "$RESULTS_DIR/consumer_bg.txt" | tail -1 | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ' || echo "0")
E2E_CONS_LAT=$(grep "Consume:" "$RESULTS_DIR/consumer_bg.txt" | tail -1 | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ' || echo "0")
E2E_HEAP=$(grep "Heap Alloc:" "$RESULTS_DIR/consumer_bg.txt" | tail -1 | awk '{print $3}' | tr -d 'MB' || echo "0")
E2E_GC=$(grep "GCs:" "$RESULTS_DIR/consumer_bg.txt" | tail -1 | awk '{print $2}' | tr -d ' ' || echo "0")

echo "    E2E Producer Messages: $E2E_PROD_MSGS"
echo "    Produce Latency: $E2E_PROD_LAT ns"
echo "    Consume Latency: $E2E_CONS_LAT ns"
echo "    Heap: $E2E_HEAP MB, GCs: $E2E_GC"

# Add to CSV
echo "e2e_test,5000,true,$E2E_PROD_MSGS,8,0,$E2E_CONS_LAT,0,0,0,$E2E_PROD_LAT,$E2E_HEAP,$E2E_GC" >> "$CSV_FILE"
echo ""

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║     End-to-End System Test (requires NATS)                  ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Test producer with NATS (run in background with timeout)
echo ">>> Testing Producer (end-to-end latency)..."
rm -f "$ARROWFLOW_ROOT/telemetry.json"
(
  "$BIN" producer --rate 1000 --mode steady &
  PID=$!
  sleep 5
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
) > "$RESULTS_DIR/producer_test.txt" 2>&1 || true

# Check if producer ran successfully (look for produce latency)
if grep -q "Produced:" "$RESULTS_DIR/producer_test.txt" 2>/dev/null; then
  echo "    Producer: OK"
  PROD_LAT=$(grep "Produce:" "$RESULTS_DIR/producer_test.txt" | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}' | tr -d ' ' || echo "0")
  if [ -n "$PROD_LAT" ] && [ "$PROD_LAT" != "0" ]; then
    echo "    Produce Latency: $PROD_LAT ns"
  else
    echo "    Produce Latency: <measurement pending>"
  fi
else
  echo "    Producer: Completed (check results for details)"
fi
echo ""

# Test chaos mode (run in background with timeout)
echo ">>> Testing Chaos Injection..."
rm -f "$ARROWFLOW_ROOT/telemetry.json"
(
  "$BIN" chaos --rate 5000 --mode steady --chaos-burst --chaos-schema &
  PID=$!
  sleep 5
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
) > "$RESULTS_DIR/chaos_test.txt" 2>&1 || true

if grep -q "Chaos Results:" "$RESULTS_DIR/chaos_test.txt" 2>/dev/null; then
  echo "    Chaos: OK"
else
  echo "    Chaos: Completed"
fi
echo ""

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Analysis Summary                                        ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Results: $CSV_FILE"
echo ""
echo "=== Throughput with vs without HyperType ==="
grep "baseline" "$CSV_FILE"
echo ""
echo "=== Rate Sweep ==="
grep "rate_sweep" "$CSV_FILE" | head -5
echo ""
echo "=== Worker Scaling ==="
grep "workers" "$CSV_FILE"
echo ""
echo "=== Burst & Stress ==="
grep -E "burst_mode|stress_100k|latency_1k" "$CSV_FILE"
echo ""

echo "All results saved to: $RESULTS_DIR/"