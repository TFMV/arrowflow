#!/bin/bash
# denorm-explosion.sh
# Find where denorm fan-out becomes unstable at high complexity

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROWFLOW_ROOT="$(dirname "$SCRIPT_DIR")"
BIN="$ARROWFLOW_ROOT/bin/arrowflow"

echo "=== Finding Denorm Explosion Point ==="
echo "Testing at increasing throughput to detect structural instability"
echo "Binary: $BIN"
echo ""

mkdir -p "$ARROWFLOW_ROOT/results/denorm-explosion"

CSV_FILE="$ARROWFLOW_ROOT/results/denorm-explosion/results.csv"
echo "rate,workers,rate_observed,messages,duration_sec,latency_mean_ns,latency_p50_ns,latency_p95_ns,latency_p99_ns,latency_p999_ns,memory_heap_mb,memory_heap_inuse_mb,memory_gc_count,memory_gc_pause_ms" > "$CSV_FILE"

for RATE in 5000 10000 20000 30000 50000 75000 100000; do
  for WORKERS in 4 8; do
    echo "--- Rate: $RATE msg/s, Workers: $WORKERS ---"
    
    rm -f "$ARROWFLOW_ROOT/telemetry.json"
    
    OUTPUT=$("$BIN" experiment --mode direct --rate "$RATE" --duration 15s --workers "$WORKERS" 2>&1) || true
    
    echo "$OUTPUT" > "$ARROWFLOW_ROOT/results/denorm-explosion/rate-${RATE}-workers-${WORKERS}.txt"
    
    # Extract metrics
    RATE_OBSERVED=$(echo "$OUTPUT" | grep -E "^\s*Rate:" | awk '{print $2}' | tr -d ' ')
    MESSAGES=$(echo "$OUTPUT" | grep -E "^\s*Messages:" | awk '{print $2}' | tr -d ' ')
    DURATION=$(echo "$OUTPUT" | grep -E "^\s*Duration:" | awk '{print $2}' | tr -d 's')
    
    LAT_MEAN=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'mean=' '{print $2}' | awk -F',' '{print $1}')
    LAT_P50=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p50=' '{print $2}' | awk -F',' '{print $1}')
    LAT_P95=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p95=' '{print $2}' | awk -F',' '{print $1}')
    LAT_P99=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p99=' '{print $2}' | awk -F',' '{print $1}')
    LAT_P999=$(echo "$OUTPUT" | grep -A1 "Consume:" | tail -1 | awk -F'p99.9=' '{print $2}' | tr -d ' ')
    
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
    LAT_P999=${LAT_P999:-0}
    HEAP_ALLOC=${HEAP_ALLOC:-0}
    HEAP_INUSE=${HEAP_INUSE:-0}
    GC_COUNT=${GC_COUNT:-0}
    GC_PAUSE=${GC_PAUSE:-0}
    
    echo "$RATE,$WORKERS,$RATE_OBSERVED,$MESSAGES,$DURATION,$LAT_MEAN,$LAT_P50,$LAT_P95,$LAT_P99,$LAT_P999,$HEAP_ALLOC,$HEAP_INUSE,$GC_COUNT,$GC_PAUSE" >> "$CSV_FILE"
    
    echo "  Rate:       $RATE_OBSERVED msg/s"
    echo "  Messages:   $MESSAGES"
    echo "  Latency:    mean=$LAT_MEAN, p99=$LAT_P99, p99.9=$LAT_P999 ns"
    echo "  Memory:     heap=$HEAP_ALLOC MB, inuse=$HEAP_INUSE MB"
    echo "  GC:         count=$GC_COUNT, pause=$GC_PAUSE ms"
    echo ""
  done
done

echo "=== Explosion Detection ==="
echo "Results: $CSV_FILE"
echo ""
echo "Warning signs to watch for:"
echo "  - Latency p99.9 >> p99 (tail inflation)"
echo "  - Memory growth > linearly with rate"
echo "  - GC count spikes unexpectedly"
echo ""
echo "=== Summary ==="
cat "$CSV_FILE"