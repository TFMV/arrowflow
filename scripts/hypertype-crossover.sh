#!/bin/bash
# hypertype-crossover.sh
# Find where HyperType optimization overhead exceeds benefit

echo "=== Finding HyperType Crossover Point ==="
echo "Comparing decode performance with and without HyperType"
echo ""

RESULTS_DIR="results/hypertype-crossover"
mkdir -p $RESULTS_DIR

# Test at different rates
for RATE in 10000 25000 50000; do
  echo "--- Rate: ${RATE} msg/s ---"
  
  # Without HyperType (baseline)
  echo "  Running WITHOUT HyperType..."
  LOG_FILE="$RESULTS_DIR/baseline-${RATE}.txt"
  
  ./bin/arrowflow experiment \
    --mode direct \
    --rate $RATE \
    --duration 20s \
    --hyper=false 2>&1 | tee "$LOG_FILE" &
  
  PID=$!
  sleep 25
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
  
  # With HyperType enabled
  echo "  Running WITH HyperType..."
  LOG_FILE="$RESULTS_DIR/hypertype-${RATE}.txt"
  
  ./bin/arrowflow experiment \
    --mode direct \
    --rate $RATE \
    --duration 20s \
    --hyper=true 2>&1 | tee "$LOG_FILE" &
  
  PID=$!
  sleep 25
  kill $PID 2>/dev/null || true
  wait $PID 2>/dev/null || true
  
  echo ""
done

echo "=== Crossover Analysis ==="
echo "Compare latency across different rates:"
echo "  - Small rates: HyperType overhead may exceed benefit"
echo "  - Large rates: HyperType provides clear benefit"  
echo "  - Break-even: Look for crossing point in latency data"
echo ""

echo "Results saved to: $RESULTS_DIR/"
