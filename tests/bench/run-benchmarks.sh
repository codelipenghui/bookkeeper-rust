#!/bin/bash
#
# Comprehensive benchmark suite for Rust BookKeeper bookie.
# Runs all benchmark suites and collects results.
#
# Usage:
#   bash tests/bench/run-benchmarks.sh                    # All suites against local bookie
#   bash tests/bench/run-benchmarks.sh --bookie 10.0.0.1:3181  # Specific bookie
#   bash tests/bench/run-benchmarks.sh --suite 1a         # Specific suite only
#   bash tests/bench/run-benchmarks.sh --release           # Build in release mode first
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmark-results/$(date +%Y%m%d-%H%M%S)"
BOOKIE="127.0.0.1:3181"
SUITE="all"
BUILD_RELEASE=false
BK_BIN="$PROJECT_ROOT/target/debug/bk"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --bookie) BOOKIE="$2"; shift 2 ;;
        --suite) SUITE="$2"; shift 2 ;;
        --release) BUILD_RELEASE=true; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"

if $BUILD_RELEASE; then
    echo "=== Building release binary ==="
    cd "$PROJECT_ROOT"
    cargo build --release -p bookkeeper-cli 2>&1 | tail -3
    BK_BIN="$PROJECT_ROOT/target/release/bk"
fi

echo "=== Benchmark Configuration ==="
echo "  Bookie:  $BOOKIE"
echo "  Suite:   $SUITE"
echo "  Binary:  $BK_BIN"
echo "  Results: $RESULTS_DIR"
echo ""

# ---------------------------------------------------------------------------
# Suite 1A: Entry Size Sweep
# ---------------------------------------------------------------------------
run_suite_1a() {
    echo "=== Suite 1A: Entry Size Sweep ==="
    for SIZE in 128 512 1024 4096 16384 65536; do
        echo "--- Entry size: ${SIZE}B ---"
        "$BK_BIN" bench-bookie --bookie "$BOOKIE" \
            --entry-size "$SIZE" \
            --warmup 1000 \
            --latency-count 5000 \
            --throughput-count 50000 \
            --max-outstanding 1000 \
            2>&1 | tee "$RESULTS_DIR/1a-size-${SIZE}B.txt"
    done
}

# ---------------------------------------------------------------------------
# Suite 1B: Concurrency Sweep
# ---------------------------------------------------------------------------
run_suite_1b() {
    echo "=== Suite 1B: Concurrency Sweep ==="
    for OUTSTANDING in 1 10 100 1000 5000 10000; do
        echo "--- Max outstanding: $OUTSTANDING ---"
        "$BK_BIN" bench-bookie --bookie "$BOOKIE" \
            --entry-size 1024 \
            --warmup 1000 \
            --latency-count 2000 \
            --throughput-count 50000 \
            --max-outstanding "$OUTSTANDING" \
            2>&1 | tee "$RESULTS_DIR/1b-outstanding-${OUTSTANDING}.txt"
    done
}

# ---------------------------------------------------------------------------
# Suite 1C: Digest Comparison
# ---------------------------------------------------------------------------
run_suite_1c() {
    echo "=== Suite 1C: Digest Type Comparison ==="
    for DIGEST in crc32c crc32 hmac dummy; do
        echo "--- Digest: $DIGEST ---"
        "$BK_BIN" bench-bookie --bookie "$BOOKIE" \
            --entry-size 1024 \
            --warmup 1000 \
            --latency-count 5000 \
            --throughput-count 50000 \
            --max-outstanding 1000 \
            --digest "$DIGEST" \
            2>&1 | tee "$RESULTS_DIR/1c-digest-${DIGEST}.txt"
    done
}

# ---------------------------------------------------------------------------
# Suite 1D: Sustained Load
# ---------------------------------------------------------------------------
run_suite_1d() {
    echo "=== Suite 1D: Sustained Load ==="
    for DUR in 60 300; do
        echo "--- Duration: ${DUR}s ---"
        "$BK_BIN" bench-bookie --bookie "$BOOKIE" \
            --entry-size 1024 \
            --warmup 5000 \
            --latency-count 5000 \
            --throughput-count 0 \
            --max-outstanding 1000 \
            --duration "$DUR" \
            2>&1 | tee "$RESULTS_DIR/1d-sustained-${DUR}s.txt"
    done
}

# ---------------------------------------------------------------------------
# Suite 3: Mixed Read/Write
# ---------------------------------------------------------------------------
run_suite_3() {
    echo "=== Suite 3: Mixed Read/Write ==="
    for READ_PCT in 0 30 50 80 100; do
        echo "--- Read: ${READ_PCT}% ---"
        "$BK_BIN" bench-bookie --bookie "$BOOKIE" \
            --entry-size 1024 \
            --warmup 10000 \
            --latency-count 5000 \
            --throughput-count 50000 \
            --max-outstanding 1000 \
            --read-pct "$READ_PCT" \
            2>&1 | tee "$RESULTS_DIR/3-mixed-read${READ_PCT}pct.txt"
    done
}

# ---------------------------------------------------------------------------
# Suite 5: Criterion Microbenchmarks
# ---------------------------------------------------------------------------
run_suite_5() {
    echo "=== Suite 5: Criterion Microbenchmarks ==="
    cd "$PROJECT_ROOT"

    echo "--- Protocol benchmarks ---"
    cargo bench -p bookkeeper-protocol --bench protocol_bench 2>&1 \
        | tee "$RESULTS_DIR/5-protocol-bench.txt"

    echo "--- Storage benchmarks ---"
    cargo bench -p bookkeeper-storage --bench storage_bench 2>&1 \
        | tee "$RESULTS_DIR/5-storage-bench.txt"
}

# ---------------------------------------------------------------------------
# Run selected suites
# ---------------------------------------------------------------------------
case "$SUITE" in
    all)
        run_suite_1a
        run_suite_1b
        run_suite_1c
        run_suite_1d
        run_suite_3
        run_suite_5
        ;;
    1a) run_suite_1a ;;
    1b) run_suite_1b ;;
    1c) run_suite_1c ;;
    1d) run_suite_1d ;;
    3)  run_suite_3 ;;
    5)  run_suite_5 ;;
    *)  echo "Unknown suite: $SUITE (options: all, 1a, 1b, 1c, 1d, 3, 5)"; exit 1 ;;
esac

echo ""
echo "=== All benchmarks complete ==="
echo "Results saved to: $RESULTS_DIR"
echo ""
echo "Files:"
ls -la "$RESULTS_DIR/"
