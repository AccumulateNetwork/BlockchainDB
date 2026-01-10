#!/bin/bash
# Overnight batch performance testing script
# Runs benchmarks on all perf/* branches and collects results

set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_BRANCH="2-performance"
OUTDIR="/home/paul/bdb-batch-$(date +%Y%m%d-%H%M%S)"
TEST_SIZE="${TEST_SIZE:-200M}"  # 10M, 100M, or 200M
LONG_RUN="${LONG_RUN:-true}"    # Run 500M degradation test

# Map test size to test name
case "$TEST_SIZE" in
    10M)  TEST_NAME="TestBDB_10M" ;;
    100M) TEST_NAME="TestBDB_100M" ;;
    200M) TEST_NAME="TestBDB_200M" ;;
    *)    echo "Invalid TEST_SIZE: $TEST_SIZE"; exit 1 ;;
esac

cd "$REPO_ROOT"

# Create output directories
mkdir -p "$OUTDIR/results" "$OUTDIR/profiles" "$OUTDIR/long-run"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$OUTDIR/run.log"
}

log "=== BDB Batch Performance Testing ==="
log "Output directory: $OUTDIR"
log "Test size: $TEST_SIZE ($TEST_NAME)"
log "Long-run test: $LONG_RUN"
log ""

# Phase 0: Run durability tests first (on baseline branch)
log "=== Phase 0: Durability Tests ==="
mkdir -p "$OUTDIR/durability"

git checkout "$BASE_BRANCH"

log "Running graceful restart test..."
if go test -run "^TestDurability_GracefulRestart$" -v \
    ./database/dkv/... > "$OUTDIR/durability/graceful-restart.txt" 2>&1; then
    log "  PASS: Graceful restart"
else
    log "  FAIL: Graceful restart - check $OUTDIR/durability/graceful-restart.txt"
fi

log "Running crash recovery test..."
if go test -run "^TestDurability_CrashRecovery$" -v \
    ./database/dkv/... > "$OUTDIR/durability/crash-recovery.txt" 2>&1; then
    log "  PASS: Crash recovery"
else
    log "  FAIL: Crash recovery - check $OUTDIR/durability/crash-recovery.txt"
fi

log "Running write-flush-verify test..."
if go test -run "^TestDurability_WriteFlushVerify$" -v \
    ./database/dkv/... > "$OUTDIR/durability/write-flush-verify.txt" 2>&1; then
    log "  PASS: Write-flush-verify"
else
    log "  FAIL: Write-flush-verify - check $OUTDIR/durability/write-flush-verify.txt"
fi

log ""
log "=== Phase 1: Configuration Experiments ==="

# Initialize summary CSV
echo "branch,commit,time_sec,entries,tx_per_sec,gc_count,heap_mb,status" > "$OUTDIR/summary.csv"

# Get list of perf branches
BRANCHES=$(git branch -a | grep -E "perf/" | sed 's/.*perf\//perf\//' | sort -u)

log "Branches to test:"
for b in $BRANCHES; do
    log "  - $b"
done
log ""

run_benchmark() {
    local branch="$1"
    local test_name="$2"
    local output_prefix="$3"

    log "--- Testing $branch with $test_name ---"

    # Checkout branch
    git checkout "$branch" 2>/dev/null || {
        log "ERROR: Failed to checkout $branch"
        return 1
    }

    local commit=$(git rev-parse --short HEAD)
    log "Commit: $commit"

    # Clean any previous test artifacts
    rm -f /tmp/bdb_benchmark.log

    # Run benchmark with CPU profiling
    local start_time=$(date +%s)

    if go test -run "^${test_name}$" -v \
        -cpuprofile="$OUTDIR/profiles/${output_prefix}-cpu.prof" \
        ./database/dkv/... > "$OUTDIR/results/${output_prefix}.txt" 2>&1; then
        local status="pass"
    else
        local status="fail"
        log "WARNING: Benchmark failed for $branch"
    fi

    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    # Copy benchmark log if exists
    if [[ -f /tmp/bdb_benchmark.log ]]; then
        cp /tmp/bdb_benchmark.log "$OUTDIR/results/${output_prefix}-metrics.log"
    fi

    # Extract metrics from benchmark log (last line is final stats)
    local metrics_file="/tmp/bdb_benchmark.log"
    if [[ -f "$metrics_file" ]]; then
        local last_line=$(tail -1 "$metrics_file")
        local entries=$(echo "$last_line" | jq -r '.entries // 0')
        local gc_count=$(echo "$last_line" | jq -r '.num_gc // 0')
        local heap_bytes=$(echo "$last_line" | jq -r '.heap_bytes // 0')
        local heap_mb=$(echo "scale=2; $heap_bytes / 1048576" | bc)
        local tx_per_sec=$(echo "scale=0; $entries / $elapsed" | bc)
    else
        local entries=0
        local gc_count=0
        local heap_mb=0
        local tx_per_sec=0
    fi

    # Append to summary
    echo "$branch,$commit,$elapsed,$entries,$tx_per_sec,$gc_count,$heap_mb,$status" >> "$OUTDIR/summary.csv"

    log "Completed in ${elapsed}s - ${tx_per_sec} tx/sec, ${gc_count} GC, ${heap_mb}MB heap"
    log ""
}

# Run benchmarks on all branches
for branch in $BRANCHES; do
    output_prefix=$(echo "$branch" | tr '/' '-')
    run_benchmark "$branch" "$TEST_NAME" "$output_prefix"
done

# Run long-run degradation test if enabled
if [[ "$LONG_RUN" == "true" ]]; then
    log "=== Starting Long-Run Degradation Test (500M) ==="

    # Use baseline or best-performing branch
    git checkout "$BASE_BRANCH"

    # Run 500M test with detailed metrics
    rm -f /tmp/bdb_benchmark.log

    local start_time=$(date +%s)

    if go test -run "^TestBDB_500M$" -v \
        -cpuprofile="$OUTDIR/long-run/cpu.prof" \
        ./database/dkv/... > "$OUTDIR/long-run/output.txt" 2>&1; then
        log "Long-run test completed"
    else
        log "Long-run test failed (may not have 500M test defined)"
    fi

    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    # Copy metrics log
    if [[ -f /tmp/bdb_benchmark.log ]]; then
        cp /tmp/bdb_benchmark.log "$OUTDIR/long-run/metrics.jsonl"
    fi

    log "Long-run completed in ${elapsed}s"
fi

# Return to base branch
git checkout "$BASE_BRANCH"

# Generate summary report
log ""
log "=== RESULTS SUMMARY ==="
log ""
column -t -s',' "$OUTDIR/summary.csv" | tee -a "$OUTDIR/run.log"

log ""
log "=== Testing Complete ==="
log "Results: $OUTDIR"
log "Summary: $OUTDIR/summary.csv"
log "Profiles: $OUTDIR/profiles/"

# Find best performer
best=$(tail -n +2 "$OUTDIR/summary.csv" | sort -t',' -k5 -rn | head -1)
best_branch=$(echo "$best" | cut -d',' -f1)
best_txps=$(echo "$best" | cut -d',' -f5)
log ""
log "Best performer: $best_branch with $best_txps tx/sec"
