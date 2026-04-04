#!/bin/bash
# hypertype-crossover.sh
# Find where HyperType optimization overhead exceeds benefit

set -e

echo "=== Finding HyperType Crossover Point ==="
echo "Comparing decode performance with and without HyperType"
echo ""

RESULTS_DIR="results/hypertype-crossover"
mkdir -p $RESULTS_DIR

# Test at different message sizes
for SIZE in 256 1024 4096 16384 65536; do
  echo "--- Message size: ${SIZE} bytes ---"
  
  # Without HyperType (baseline)
  echo "  Running WITHOUT HyperType..."
  timeout 30s ./bin/arrowflow experiment \
    --mode direct \
    --rate 10000 \
    --duration 20s \
    --hyper=false 2>&1 | tee "$RESULTS_DIR/baseline-${SIZE}.txt"
  
  # With HyperType enabled
  echo "  Running WITH HyperType..."
  timeout 30s ./bin/arrowflow experiment \
    --mode direct \
    --rate 10000 \
    --duration 20s \
    --hyper=true 2>&1 | tee "$RESULTS_DIR/hypertype-${SIZE}.txt"
  
  # Extract and compare latencies
  echo ""
done

echo "=== Crossover Analysis ==="
echo "Compare latency across different message sizes:"
echo "  - Small messages (<1KB): HyperType overhead may exceed benefit"
echo "  - Large messages (>10KB): HyperType provides clear benefit"  
echo "  - Break-even: Look for crossing point in latency data"
echo ""

echo "Results saved to: $RESULTS_DIR/"
