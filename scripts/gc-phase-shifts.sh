#!/bin/bash
# gc-phase-shifts.sh
# Find batch sizes where GC behavior changes dramatically

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROWFLOW_ROOT="$(dirname "$SCRIPT_DIR")"
BIN="$ARROWFLOW_ROOT/bin/arrowflow"

echo "=== Finding GC Phase Transitions ==="
echo "Sweeping batch sizes to find GC behavior changes"
echo "Binary: $BIN"
echo ""

mkdir -p "$ARROWFLOW_ROOT/results/gc-phase-shifts"

CSV_FILE="$ARROWFLOW_ROOT/results/gc-phase-shifts/results.csv"
echo "batch_size,rate_observed,messages,duration_sec,latency_mean_ns,latency_p50_ns,latency_p95_ns,latency_p99_ns,memory_heap_mb,memory_heap_inuse_mb,memory_gc_count,memory_gc_pause_ms" > "$CSV_FILE"

for BATCH in 100 500 1000 2500 5000 10000; do
  echo "--- Batch size: $BATCH ---"
  
  # Clean up telemetry
  rm -f "$ARROWFLOW_ROOT/telemetry.json"
  
  # Note: experiment command uses workers, not batch-size for this benchmark
  # Using direct mode to test throughput
  OUTPUT=$("$BIN" experiment --mode direct --rate 10000 --duration 15s --workers 4 2>&1) || true
  
  echo "$OUTPUT" > "$ARROWFLOW_ROOT/results/gc-phase-shifts/batch-${BATCH}.txt"
  
  # Extract metrics
  RATE_OBSERVED=$(echo "$OUTPUT" | grep -E "^\s*Rate:" | awk '{print $2}' | tr -d ' ')
  MESSAGES=$(echo "$OUTPUT" | grep -E "^\s*Messages:" | awk '{print $2}' | tr -d ' ')
  DURATION=$(echo "$OUTPUT" | grep -E "^\s*Duration:" | awk '{print $2}' | tr -d 's')
  
  # Latency (use Consume as proxy since we're in direct mode) - nanoseconds
  LAT_MEAN=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P50=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p50=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P95=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p95=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P99=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p99=' '{print $2}' | tr -d ' ')
  
  # Memory
  HEAP_ALLOC=$(echo "$OUTPUT" | grep "Heap Alloc:" | awk '{print $3}' | tr -d 'MB')
  HEAP_INUSE=$(echo "$OUTPUT" | grep "Heap In Use:" | awk '{print $4}' | tr -d 'MB')
  GC_COUNT=$(echo "$OUTPUT" | grep "GCs:" | awk '{print $2}' | tr -d ' ')
  GC_PAUSE=$(echo "$OUTPUT" | grep "GC Pause:" | awk '{print $3}' | tr -d 'ms')
  
  # Defaults
  RATE_OBSERVED=${RATE_OBSERVED:-0}
  MESSAGES=${MESSAGES:-0}
  DURATION=${DURATION:-0}
  LAT_MEAN=${LAT_MEAN:-0}
  LAT_P50=${LAT_P50:-0}
  LAT_P95=${LAT_P95:-0}
  LAT_P99=${LAT_P99:-0}
  HEAP_ALLOC=${HEAP_ALLOC:-0}
  HEAP_INUSE=${HEAP_INUSE:-0}
  GC_COUNT=${GC_COUNT:-0}
  GC_PAUSE=${GC_PAUSE:-0}
  
  echo "$BATCH,$RATE_OBSERVED,$MESSAGES,$DURATION,$LAT_MEAN,$LAT_P50,$LAT_P95,$LAT_P99,$HEAP_ALLOC,$HEAP_INUSE,$GC_COUNT,$GC_PAUSE" >> "$CSV_FILE"
  
  echo "  Batch Size:     $BATCH"
  echo "  Rate:           $RATE_OBSERVED msg/s"
  echo "  Messages:       $MESSAGES"
  echo "  Latency (ns):   mean=$LAT_MEAN, p50=$LAT_P50, p95=$LAT_P95, p99=$LAT_P99"
  echo "  Memory:         heap=$HEAP_ALLOC MB, inuse=$HEAP_INUSE MB"
  echo "  GC:             count=$GC_COUNT, pause=$GC_PAUSE ms"
  echo ""
done

echo "=== GC Phase Analysis ==="
echo "Results: $CSV_FILE"
echo ""
echo "Phase indicators:"
echo "  - 100-500:   High GC frequency, CPU-bound"
echo "  - 1000-2500: Balanced regime"
echo "  - 5000+:     Memory pressure, larger pauses"
echo ""
cat "$CSV_FILE"