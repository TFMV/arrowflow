#!/bin/bash
# denorm-explosion.sh
# Find where denorm fan-out becomes unstable

echo "=== Finding Denorm Explosion Point ==="
echo "Testing with increasing repeated field cardinality"
echo ""

RESULTS_DIR="results/denorm-explosion"
mkdir -p $RESULTS_DIR

# Test at high rate with varying complexity
for RATE in 10000 20000 30000; do
  echo "--- Rate: $RATE msg/s ---"
  
  LOG_FILE="$RESULTS_DIR/rate-${RATE}.txt"
  
  ./bin/arrowflow experiment \
    --mode direct \
    --rate $RATE \
    --duration 15s 2>&1 | tee "$LOG_FILE" &
  
  PID=$!
  sleep 20
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
  
  echo ""
done

echo "=== Explosion Detection ==="
echo "Warning signs to look for:"
echo "  - Fan-out multiplier becomes variable"
echo "  - Latency distribution becomes bimodal"
echo "  - Cache miss rate spikes"
echo "  - Memory grows non-linearly"
echo ""

echo "Results saved to: $RESULTS_DIR/"
