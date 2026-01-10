#!/bin/bash
# Create performance experiment branches with different configurations
# Each branch modifies kv_shard.go constants

set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KV_SHARD="$REPO_ROOT/database/kv/kv_shard.go"
BLOCKCHAIN_STORE="$REPO_ROOT/database/dkv/blockchain_store.go"
BASE_BRANCH="2-performance"

cd "$REPO_ROOT"

# Ensure we start from clean state
git checkout "$BASE_BRANCH"
git pull origin "$BASE_BRANCH" 2>/dev/null || true

create_branch() {
    local branch_name="$1"
    local channels="$2"
    local buffer="$3"
    local bins="$4"
    local description="$5"

    echo "=== Creating $branch_name ==="
    echo "  Channels: $channels, Buffer: $buffer, Bins: $bins"

    # Create branch from base
    git checkout "$BASE_BRANCH"
    git checkout -B "$branch_name"

    # Modify NumChannelGroups
    sed -i "s/NumChannelGroups = [0-9]*/NumChannelGroups = $channels/" "$KV_SHARD"

    # Modify WriteChannelSize
    sed -i "s/WriteChannelSize = [0-9]*/WriteChannelSize = $buffer/" "$KV_SHARD"

    # Modify bin count in NewBlockchainStore (blockchain_store.go line 43)
    sed -i "s/NewKVShard(directory, [0-9]*, 200_000_000)/NewKVShard(directory, $bins, 200_000_000)/" "$BLOCKCHAIN_STORE"

    # Verify changes compile
    if ! go build ./database/kv ./database/dkv 2>/dev/null; then
        echo "ERROR: Build failed for $branch_name"
        git checkout "$BASE_BRANCH"
        return 1
    fi

    # Commit
    git add -A
    git commit -m "perf: $description

Config: channels=$channels, buffer=$buffer, bins=$bins
Branch for automated performance testing.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"

    echo "  Created and committed $branch_name"
}

echo "Creating performance experiment branches..."
echo "Base branch: $BASE_BRANCH"
echo ""

# Channel experiments (bins=64, buffer=20000)
create_branch "perf/2-channels" 2 20000 64 "2 channel groups for less coordination overhead"
create_branch "perf/4-channels" 4 20000 64 "4 channel groups (baseline)"
create_branch "perf/8-channels" 8 20000 64 "8 channel groups for more parallelism"
create_branch "perf/16-channels" 16 20000 64 "16 channel groups approaching original design"

# Bucket experiments (channels=4, buffer=20000)
create_branch "perf/buckets-32" 4 20000 32 "32 bins per shard (more overflow)"
create_branch "perf/buckets-64" 4 20000 64 "64 bins per shard (baseline)"
create_branch "perf/buckets-128" 4 20000 128 "128 bins per shard (less overflow)"
create_branch "perf/buckets-256" 4 20000 256 "256 bins per shard (minimal overflow)"

# Buffer experiments (channels=4, bins=64)
create_branch "perf/buffer-5k" 4 5000 64 "5k buffer for tighter backpressure"
create_branch "perf/buffer-20k" 4 20000 64 "20k buffer (baseline)"
create_branch "perf/buffer-50k" 4 50000 64 "50k buffer for more buffering"

# Combined experiments
create_branch "perf/optimistic" 8 50000 256 "Max parallelism config"
create_branch "perf/conservative" 2 10000 128 "Minimal overhead config"

# Return to base branch
git checkout "$BASE_BRANCH"

echo ""
echo "=== All branches created ==="
git branch -a | grep "perf/"
