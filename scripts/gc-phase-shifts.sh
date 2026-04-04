#!/bin/bash
# gc-phase-shifts.sh
# Find batch sizes where GC behavior changes dramatically

set -e

echo "=== Finding GC Phase Transitions ==="
echo "Sweeping batch sizes to find GC behavior changes"
echo ""

RESULTS_DIR="results/gc-phase-shifts"
mkdir -p $RESULTS_DIR

# Enable GC tracing
export GODEBUG=gctrace=1

for BATCH in 100 500 1000 5000 10000 50000; do
  echo "--- Batch size: $BATCH ---"
  
  LOG_FILE="$RESULTS_DIR/batch-${BATCH}.log"
  
  # Run with GC tracing
  timeout 20s ./bin/arrowflow consumer \
    --batch-size $BATCH \
    --workers 4 2>&1 | tee "$LOG_FILE" &
  
  PID=$!
  
  # Wait for some data
  sleep 10
  
  # Get heap stats
  if [ -f telemetry.json ]; then
    HEAP=$(cat telemetry.json | grep -o '"heap_alloc_mb":[0-9.]*' | tail -1 || echo "N/A")
    GCS=$(cat telemetry.json | grep -o '"gc_count":[0-9]*' | tail -1 || echo "N/A")
    echo "  Heap: $HEAP, GCs: $GCS"
  fi
  
  # Let it run a bit more
  sleep 5
  
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
  
  echo ""
done

echo "=== GC Phase Analysis ==="
echo "Look for batch size thresholds where:"
echo "  - GC frequency jumps (gctrace output)"
echo "  - Allocation churn spikes"  
echo "  - Pause times become non-smooth"
echo ""
echo "Expected transitions:"
echo "  - 1-100: CPU-bound (high GC frequency)"
echo "  - 100-1000: Balanced"
echo "  - 10000+: Memory/GC collapse"
echo ""

echo "Results saved to: $RESULTS_DIR/"
