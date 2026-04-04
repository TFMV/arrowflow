#!/bin/bash
# run-all-experiments.sh
# Complete evaluation suite for ArrowFlow scientific analysis

set -e

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     ArrowFlow Scientific Evaluation Suite                 ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create results directory
RESULTS_DIR="experiment-results"
mkdir -p "$RESULTS_DIR"

echo "Build: $(go version | awk '{print $1, $2}')"
echo "Date: $(date)"
echo ""

# Experiment 1: Baseline throughput (with HyperType)
echo ">>> [1/6] Baseline throughput measurement (with HyperType)..."
timeout 60s ./bin/arrowflow experiment \
  --mode direct \
  --rate 50000 \
  --duration 30s \
  --hyper=true > "$RESULTS_DIR/baseline-with-hypertype.txt" 2>&1 || true
echo "    Done"

# Experiment 2: Baseline throughput (without HyperType)
echo ">>> [2/6] Baseline throughput measurement (without HyperType)..."
timeout 60s ./bin/arrowflow experiment \
  --mode direct \
  --rate 50000 \
  --duration 30s \
  --hyper=false > "$RESULTS_DIR/baseline-no-hypertype.txt" 2>&1 || true
echo "    Done"

# Experiment 3: Batch size sweep
echo ">>> [3/6] Batch size sweep..."
for BATCH in 100 500 1000 5000 10000; do
  echo "    Testing batch=$BATCH..."
  timeout 15s ./bin/arrowflow consumer --batch-size "$BATCH" 2>&1 | head -10 > "$RESULTS_DIR/batch-${BATCH}.txt" || true
done
echo "    Done"

# Experiment 4: Stress collapse
echo ">>> [4/6] Stress collapse test..."
timeout 120s ./bin/arrowflow experiment \
  --mode stress \
  --max-rate 100000 > "$RESULTS_DIR/stress-collapse.txt" 2>&1 || true
echo "    Done"

# Experiment 5: Chaos injection
echo ">>> [5/6] Chaos injection..."
timeout 60s ./bin/arrowflow chaos \
  --rate 10000 \
  --chaos-burst \
  --chaos-schema > "$RESULTS_DIR/chaos-injection.txt" 2>&1 || true
echo "    Done"

# Experiment 6: Direct ingestion at various rates
echo ">>> [6/6] Rate sweep (finding throughput ceiling)..."
for RATE in 10000 25000 50000 75000 100000; do
  echo "    Testing rate=$RATE..."
  timeout 20s ./bin/arrowflow experiment \
    --mode direct \
    --rate "$RATE" \
    --duration 15s > "$RESULTS_DIR/rate-${RATE}.txt" 2>&1 || true
done
echo "    Done"

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Generating Analysis Report                           ║"
echo "╚════════════════════════════════════════════════════════════╝"

# Extract and summarize key metrics
echo ""
echo "=== Throughput Comparison ==="
echo "With HyperType:"
grep "Rate:" "$RESULTS_DIR/baseline-with-hypertype.txt" 2>/dev/null || echo "  No data"
echo "Without HyperType:"
grep "Rate:" "$RESULTS_DIR/baseline-no-hypertype.txt" 2>/dev/null || echo "  No data"

echo ""
echo "=== Throughput Ceiling ==="
for RATE in 10000 25000 50000 75000 100000; do
  RATE_OBSERVED=$(grep "Rate:" "$RESULTS_DIR/rate-${RATE}.txt" 2>/dev/null | awk '{print $2}' || echo "0")
  echo "  Rate $RATE -> Observed: $RATE_OBSERVED msg/s"
done

echo ""
echo "=== Stress Collapse Point ==="
grep "Degradation" "$RESULTS_DIR/stress-collapse.txt" 2>/dev/null || echo "  No degradation detected"
grep "Throughput Ceiling" "$RESULTS_DIR/stress-collapse.txt" 2>/dev/null || echo "  No ceiling detected"

echo ""
echo "=== Chaos Results ==="
grep "Messages:" "$RESULTS_DIR/chaos-injection.txt" 2>/dev/null || echo "  No data"
grep "Injections:" "$RESULTS_DIR/chaos-injection.txt" 2>/dev/null || echo "  No data"

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║     Key Questions Answered                               ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "1. Where does throughput saturate?"
echo "   → Check rate sweep output above"
echo ""
echo "2. What dominates cost: parsing, batching, or fan-out?"
echo "   → Compare with/without HyperType and batch sweep"
echo ""
echo "3. When does HyperType stop being beneficial?"
echo "   → Compare baseline results above"
echo ""
echo "4. Which batch sizes maximize throughput before GC collapse?"
echo "   → Check batch sweep results"
echo ""
echo "5. Does denorm behave linearly or phase-transition?"
echo "   → Run denorm-phase-transition.sh separately"
echo ""
echo "6. Where is the system's true stability boundary?"
echo "   → Check stress-collapse and chaos results"
echo ""

echo "Results saved to: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"

echo ""
echo "For detailed analysis, run individual scripts:"
echo "  ./scripts/denorm-phase-transition.sh"
echo "  ./scripts/hypertype-crossover.sh"
echo "  ./scripts/gc-phase-shifts.sh"
echo "  ./scripts/denorm-explosion.sh"
