#!/bin/bash
# denorm-phase-transition.sh
# Find where denormalized row expansion grows faster than input rate

set -e

ARROWFLOW_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ARROWFLOW_ROOT/bin/arrowflow"
RESULTS_DIR="$ARROWFLOW_ROOT/results/denorm-phase-transition"

echo "=== Finding Denorm Phase Transition ==="
echo "This experiment measures where fan-out becomes nonlinear"
echo "Binary: $BIN"
echo ""

mkdir -p "$RESULTS_DIR"

# CSV header for structured results
CSV_FILE="$RESULTS_DIR/results.csv"
echo "rate_input,rate_observed,messages,duration_sec,latency_consume_mean_ns,latency_consume_p50_ns,latency_consume_p95_ns,latency_consume_p99_ns,latency_consume_p999_ns,memory_heap_mb,memory_gc_count" > "$CSV_FILE"

for RATE in 1000 5000 10000 25000 50000 75000 100000; do
  echo "--- Testing rate: $RATE msg/s ---"
  
  # Clean up any previous telemetry
  rm -f "$ARROWFLOW_ROOT/telemetry.json"
  
  # Run experiment synchronously (no &)
  # Add explicit --mode direct to ensure proper flag parsing
  OUTPUT=$("$BIN" experiment --mode direct --rate "$RATE" --duration 20s --workers 8 2>&1) || true
  
  # Save full output
  echo "$OUTPUT" > "$RESULTS_DIR/rate-${RATE}.txt"
  
  # Extract metrics with higher accuracy
  RATE_OBSERVED=$(echo "$OUTPUT" | grep -E "^\s*Rate:" | awk '{print $2}' | tr -d ' ')
  MESSAGES=$(echo "$OUTPUT" | grep -E "^\s*Messages:" | awk '{print $2}' | tr -d ' ')
  DURATION=$(echo "$OUTPUT" | grep -E "^\s*Duration:" | awk '{print $2}' | tr -d 's')
  
  # Latency metrics (Consume) - in nanoseconds
  LAT_MEAN=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P50=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p50=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P95=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p95=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P99=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p99=' '{print $2}' | awk -F',' '{print $1}')
  LAT_P999=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p99.9=' '{print $2}' | tr -d ' ')
  
  # Memory metrics
  HEAP_ALLOC=$(echo "$OUTPUT" | grep "Heap Alloc:" | awk '{print $3}' | tr -d 'MB')
  GC_COUNT=$(echo "$OUTPUT" | grep "GCs:" | awk '{print $2}' | tr -d ' ')
  
  # Throughput (if available in telemetry)
  if [ -f "$ARROWFLOW_ROOT/telemetry.json" ]; then
    THROUGHPUT_MSGS=$(grep -o '"messages_per_sec":[0-9.]*' "$ARROWFLOW_ROOT/telemetry.json" | head -1 | cut -d':' -f2)
    THROUGHPUT_BYTES=$(grep -o '"bytes_per_sec":[0-9.]*' "$ARROWFLOW_ROOT/telemetry.json" | head -1 | cut -d':' -f2)
  else
    THROUGHPUT_MSGS="0"
    THROUGHPUT_BYTES="0"
  fi
  
  # Default empty values if extraction failed
  RATE_OBSERVED=${RATE_OBSERVED:-0}
  MESSAGES=${MESSAGES:-0}
  DURATION=${DURATION:-0}
  LAT_MEAN=${LAT_MEAN:-0}
  LAT_P50=${LAT_P50:-0}
  LAT_P95=${LAT_P95:-0}
  LAT_P99=${LAT_P99:-0}
  LAT_P999=${LAT_P999:-0}
  HEAP_ALLOC=${HEAP_ALLOC:-0}
  GC_COUNT=${GC_COUNT:-0}
  
  # Append to CSV
  echo "$RATE,$RATE_OBSERVED,$MESSAGES,$DURATION,$LAT_MEAN,$LAT_P50,$LAT_P95,$LAT_P99,$LAT_P999,$HEAP_ALLOC,$GC_COUNT" >> "$CSV_FILE"
  
  # Pretty print
  echo "  Input Rate:      $RATE msg/s"
  echo "  Observed Rate:   $RATE_OBSERVED msg/s"
  echo "  Messages:        $MESSAGES"
  echo "  Duration:        ${DURATION}s"
  echo "  Latency (ns):    mean=$LAT_MEAN, p50=$LAT_P50, p95=$LAT_P95, p99=$LAT_P99, p99.9=$LAT_P999"
  echo "  Memory Heap:     ${HEAP_ALLOC} MB"
  echo "  GC Count:        $GC_COUNT"
  echo ""
done

echo "=== Phase Transition Analysis ==="
echo "Results saved to: $CSV_FILE"
echo ""
echo "Phase Transition Indicators:"
echo "  - Linear regime: output_rate ≈ input_rate (fan-out = 1)"
echo "  - Transition: output_rate begins accelerating"  
echo "  - Explosion: output_rate >> input_rate (structural denorm explosion)"
echo ""

# Display CSV summary
echo "=== Results Summary ==="
cat "$CSV_FILE"