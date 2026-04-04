#!/bin/bash
# hypertype-crossover.sh
# Find where HyperType optimization overhead exceeds benefit

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROWFLOW_ROOT="$(dirname "$SCRIPT_DIR")"
BIN="$ARROWFLOW_ROOT/bin/arrowflow"

echo "=== Finding HyperType Crossover Point ==="
echo "Comparing decode performance with and without HyperType"
echo "Binary: $BIN"
echo ""

mkdir -p "$ARROWFLOW_ROOT/results/hypertype-crossover"

CSV_FILE="$ARROWFLOW_ROOT/results/hypertype-crossover/results.csv"
echo "rate,hyper_enabled,rate_observed,messages,duration_sec,latency_mean_ns,latency_p50_ns,latency_p95_ns,latency_p99_ns,latency_p999_ns,memory_heap_mb,memory_gc_count" > "$CSV_FILE"

for RATE in 5000 10000 25000 50000 100000; do
  echo "--- Rate: $RATE msg/s ---"
  
  # Test WITHOUT HyperType (hyper=false)
  echo "  Running WITHOUT HyperType..."
  rm -f "$ARROWFLOW_ROOT/telemetry.json"
  
  OUTPUT_NO_HT=$("$BIN" experiment --mode direct --rate "$RATE" --duration 15s --workers 4 --hyper=false 2>&1) || true
  echo "$OUTPUT_NO_HT" > "$ARROWFLOW_ROOT/results/hypertype-crossover/baseline-${RATE}.txt"
  
  RATE_NO_HT=$(echo "$OUTPUT_NO_HT" | grep -E "^\s*Rate:" | awk '{print $2}' | tr -d ' ')
  MSGS_NO_HT=$(echo "$OUTPUT_NO_HT" | grep -E "^\s*Messages:" | awk '{print $2}' | tr -d ' ')
  DUR_NO_HT=$(echo "$OUTPUT_NO_HT" | grep -E "^\s*Duration:" | awk '{print $2}' | tr -d 's')
  LAT_MEAN_NO_HT=$(echo "$OUTPUT_NO_HT" | grep -A1 "Consume:" | tail -1 | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P50_NO_HT=$(echo "$OUTPUT_NO_HT" | grep -A1 "Consume:" | tail -1 | awk -F'p50=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P95_NO_HT=$(echo "$OUTPUT_NO_HT" | grep -A1 "Consume:" | tail -1 | awk -F'p95=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P99_NO_HT=$(echo "$OUTPUT_NO_HT" | grep -A1 "Consume:" | tail -1 | awk -F'p99=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P999_NO_HT=$(echo "$OUTPUT_NO_HT" | grep -A1 "Consume:" | tail -1 | awk -F'p99.9=' '{print $2}' | tr -d ' ')
  HEAP_NO_HT=$(echo "$OUTPUT_NO_HT" | grep "Heap Alloc:" | awk '{print $3}' | tr -d 'MB')
  GC_NO_HT=$(echo "$OUTPUT_NO_HT" | grep "GCs:" | awk '{print $2}' | tr -d ' ')
  
  # Defaults
  RATE_NO_HT=${RATE_NO_HT:-0}
  MSGS_NO_HT=${MSGS_NO_HT:-0}
  DUR_NO_HT=${DUR_NO_HT:-0}
  LAT_MEAN_NO_HT=${LAT_MEAN_NO_HT:-0}
  LAT_P50_NO_HT=${LAT_P50_NO_HT:-0}
  LAT_P95_NO_HT=${LAT_P95_NO_HT:-0}
  LAT_P99_NO_HT=${LAT_P99_NO_HT:-0}
  LAT_P999_NO_HT=${LAT_P999_NO_HT:-0}
  HEAP_NO_HT=${HEAP_NO_HT:-0}
  GC_NO_HT=${GC_NO_HT:-0}
  
  echo "    Without HT: rate=$RATE_NO_HT, latency_mean=$LAT_MEAN_NO_HT ns"
  
  # Test WITH HyperType (hyper=true)
  echo "  Running WITH HyperType..."
  rm -f "$ARROWFLOW_ROOT/telemetry.json"
  
  OUTPUT_HT=$("$BIN" experiment --mode direct --rate "$RATE" --duration 15s --workers 4 --hyper=true 2>&1) || true
  echo "$OUTPUT_HT" > "$ARROWFLOW_ROOT/results/hypertype-crossover/hypertype-${RATE}.txt"
  
  RATE_HT=$(echo "$OUTPUT_HT" | grep -E "^\s*Rate:" | awk '{print $2}' | tr -d ' ')
  MSGS_HT=$(echo "$OUTPUT_HT" | grep -E "^\s*Messages:" | awk '{print $2}' | tr -d ' ')
  DUR_HT=$(echo "$OUTPUT_HT" | grep -E "^\s*Duration:" | awk '{print $2}' | tr -d 's')
  LAT_MEAN_HT=$(echo "$OUTPUT_HT" | grep -A1 "Consume:" | tail -1 | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P50_HT=$(echo "$OUTPUT_HT" | grep -A1 "Consume:" | tail -1 | awk -F'p50=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P95_HT=$(echo "$OUTPUT_HT" | grep -A1 "Consume:" | tail -1 | awk -F'p95=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P99_HT=$(echo "$OUTPUT_HT" | grep -A1 "Consume:" | tail -1 | awk -F'p99=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P999_HT=$(echo "$OUTPUT_HT" | grep -A1 "Consume:" | tail -1 | awk -F'p99.9=' '{print $2}' | tr -d ' ')
  HEAP_HT=$(echo "$OUTPUT_HT" | grep "Heap Alloc:" | awk '{print $3}' | tr -d 'MB')
  GC_HT=$(echo "$OUTPUT_HT" | grep "GCs:" | awk '{print $2}' | tr -d ' ')
  
  # Defaults
  RATE_HT=${RATE_HT:-0}
  MSGS_HT=${MSGS_HT:-0}
  DUR_HT=${DUR_HT:-0}
  LAT_MEAN_HT=${LAT_MEAN_HT:-0}
  LAT_P50_HT=${LAT_P50_HT:-0}
  LAT_P95_HT=${LAT_P95_HT:-0}
  LAT_P99_HT=${LAT_P99_HT:-0}
  LAT_P999_HT=${LAT_P999_HT:-0}
  HEAP_HT=${HEAP_HT:-0}
  GC_HT=${GC_HT:-0}
  
  echo "    With HT:    rate=$RATE_HT, latency_mean=$LAT_MEAN_HT ns"
  
  # Append both to CSV
  echo "$RATE,false,$RATE_NO_HT,$MSGS_NO_HT,$DUR_NO_HT,$LAT_MEAN_NO_HT,$LAT_P50_NO_HT,$LAT_P95_NO_HT,$LAT_P99_NO_HT,$LAT_P999_NO_HT,$HEAP_NO_HT,$GC_NO_HT" >> "$CSV_FILE"
  echo "$RATE,true,$RATE_HT,$MSGS_HT,$DUR_HT,$LAT_MEAN_HT,$LAT_P50_HT,$LAT_P95_HT,$LAT_P99_HT,$LAT_P999_HT,$HEAP_HT,$GC_HT" >> "$CSV_FILE"
  
  echo ""
done

echo "=== Crossover Analysis ==="
echo "Results: $CSV_FILE"
echo ""
echo "Interpretation:"
echo "  - At low rates: Without HT may be faster (lower overhead)"
echo "  - At high rates: With HT should be faster (better memory layout)"
echo "  - Crossover: rate where both have similar latency"
echo ""
echo "=== Results Summary ==="
cat "$CSV_FILE"