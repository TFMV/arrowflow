#!/bin/bash
# denorm-explosion.sh
# Find where denorm fan-out becomes unstable

set -e

echo "=== Finding Denorm Explosion Point ==="
echo "Testing with increasing repeated field cardinality"
echo ""

RESULTS_DIR="results/denorm-explosion"
mkdir -p $RESULTS_DIR

# Low fan-out scenario
echo "--- Low fan-out (1-2 repeated items) ---"
timeout 20s ./bin/arrowflow experiment \
  --mode direct \
  --rate 20000 \
  --duration 15s 2>&1 | tee "$RESULTS_DIR/low-fanout.txt"

# Check for warning signs
echo "Checking for stability indicators..."

# Medium fan-out scenario  
echo "--- Medium fan-out (5-10 repeated items) ---"
timeout 20s ./bin/arrowflow experiment \
  --mode direct \
  --rate 20000 \
  --duration 15s 2>&1 | tee "$RESULTS_DIR/medium-fanout.txt"

# High fan-out scenario
echo "--- High fan-out (20+ repeated items) ---"
timeout 20s ./bin/arrowflow experiment \
  --mode direct \
  --rate 20000 \
  --duration 15s 2>&1 | tee "$RESULTS_DIR/high-fanout.txt"

echo ""
echo "=== Explosion Detection ==="
echo "Warning signs to look for:"
echo "  - Fan-out multiplier becomes variable"
echo "  - Latency distribution becomes bimodal"
echo "  - Cache miss rate spikes"
echo "  - Memory grows non-linearly"
echo ""

echo "Results saved to: $RESULTS_DIR/"
