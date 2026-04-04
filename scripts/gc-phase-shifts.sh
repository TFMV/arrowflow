#!/bin/bash
# gc-phase-shifts.sh
# Find batch sizes where GC behavior changes dramatically

echo "=== Finding GC Phase Transitions ==="
echo "Sweeping batch sizes to find GC behavior changes"
echo ""

RESULTS_DIR="results/gc-phase-shifts"
mkdir -p $RESULTS_DIR

for BATCH in 100 500 1000 5000 10000; do
  echo "--- Batch size: $BATCH ---"
  
  LOG_FILE="$RESULTS_DIR/batch-${BATCH}.log"
  
  ./bin/arrowflow consumer \
    --batch-size $BATCH \
    --workers 4 2>&1 | tee "$LOG_FILE" &
  
  PID=$!
  
  # Wait for data
  sleep 15
  
  # Get heap stats
  if [ -f telemetry.json ]; then
    HEAP=$(grep -o '"heap_alloc_mb":[0-9.]*' telemetry.json | tail -1 || echo "N/A")
    GCS=$(grep -o '"gc_count":[0-9]*' telemetry.json | tail -1 || echo "N/A")
    echo "  Heap: $HEAP, GCs: $GCS"
  fi
  
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
  
  echo ""
done

echo "=== GC Phase Analysis ==="
echo "Look for batch size thresholds where:"
echo "  - GC frequency jumps"
echo "  - Allocation churn spikes"  
echo "  - Pause times become non-smooth"
echo ""
echo "Expected transitions:"
echo "  - 1-100: CPU-bound (high GC frequency)"
echo "  - 100-1000: Balanced"
echo "  - 10000+: Memory/GC collapse"
echo ""

echo "Results saved to: $RESULTS_DIR/"
