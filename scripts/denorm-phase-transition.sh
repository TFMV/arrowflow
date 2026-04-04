#!/bin/bash
# denorm-phase-transition.sh
# Find where denormalized row expansion grows faster than input rate

echo "=== Finding Denorm Phase Transition ==="
echo "This experiment measures where fan-out becomes nonlinear"
echo ""

RESULTS_DIR="results/denorm-phase-transition"
mkdir -p $RESULTS_DIR

for RATE in 1000 5000 10000 25000 50000 75000 100000; do
  echo "--- Testing rate: $RATE msg/s ---"
  
  RESULT_FILE="$RESULTS_DIR/rate-${RATE}.txt"
  
  ./bin/arrowflow experiment \
    --mode direct \
    --rate $RATE \
    --duration 20s \
    --workers 8 2>&1 | tee "$RESULT_FILE" &
  
  PID=$!
  
  # Wait for duration + buffer
  sleep 25
  
  # Kill if still running
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
  
  # Extract key metrics
  RATE_OBSERVED=$(grep "Rate:" "$RESULT_FILE" | awk '{print $2}' || echo "0")
  MESSAGES=$(grep "Messages:" "$RESULT_FILE" | awk '{print $2}' || echo "0")
  
  echo "  Input Rate: $RATE msg/s"
  echo "  Observed Rate: $RATE_OBSERVED msg/s"
  echo "  Total Messages: $MESSAGES"
  echo ""
  
  # Check telemetry for memory behavior
  if [ -f telemetry.json ]; then
    HEAP=$(grep -o '"heap_alloc_mb":[0-9.]*' telemetry.json | tail -1 || echo "N/A")
    echo "  Memory: $HEAP"
  fi
  echo ""
done

echo "=== Phase Transition Analysis ==="
echo "Compare output rates vs input rates:"
echo "  - Linear regime: output_rate ≈ input_rate x fan-out (constant)"
echo "  - Transition: output_rate begins accelerating"
echo "  - Explosion: output_rate >> input_rate (unstable)"
echo ""

echo "Results saved to: $RESULTS_DIR/"
