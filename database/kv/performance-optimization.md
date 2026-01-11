# KVShard Performance Optimization Guide

This document captures the optimization theories, processes, and findings from performance tuning the KVShard async write architecture.

**Last Updated**: 2026-01-11
**Branch**: `2-performance`
**Commit**: `6156ffc`

## Architecture Overview

KVShard is a 1024-shard key-value store optimized for blockchain data:
- **1024 shards**: Determined by first 2 bytes of key hash, enables parallel I/O
- **20-byte internal keys**: Truncated from 32-byte external keys (7.9% disk savings)
- **Lock-free bloom filter**: Mmap'd with atomic bit operations, crash recovery
- **Two storage tiers**:
  - DynaKV: In-memory for mutable data (accounts, state)
  - PermKV: Disk-based for immutable data (transactions, blocks)
- **Async writes**: 8 channel groups with background workers, deferred bloom add

## Optimization History

### Initial Architecture (256 channels, 256 workers)
- One channel and one worker goroutine per shard
- Results: 14.06s for 10M entries
- Problem: 27.87% CPU time spent in `runtime.selectgo` (channel coordination)

### Attempt 1: Single Channel with NumCPU Workers
- Hypothesis: Reduce goroutines to reduce coordination overhead
- Implementation: 1 channel, NumCPU (24) workers
- Results: 16.84s (20% slower)
- Problem: Single channel became a contention bottleneck (30% selectgo)

### Attempt 2: Single Channel with 2x NumCPU Workers
- Hypothesis: More workers would increase throughput
- Implementation: 1 channel, 48 workers
- Results: 18.5s (worse)
- Problem: More workers competing for single channel increased contention

### Final Solution: Sharded Channels (8 groups)
- Hypothesis: Balance parallelism with reduced coordination
- Implementation: 8 channel groups, NumCPU/8 workers per group
- Results: 14.38s (equivalent to original, but with 1024 shards)
- selectgo overhead: 3.05% (down from 27.87%)
- Goroutines: ~28 (down from 256, 89% reduction)

### 20-Byte Internal Keys (2026-01)
- Hypothesis: 32-byte keys waste storage; 20 bytes sufficient for collision resistance
- Implementation: External API accepts [32]byte, internally truncated to [20]byte
- Results: Entry size 48→36 bytes, ~6GB savings at 500M entries
- Collision resistance: 2^80 birthday bound (sufficient for blockchain)

### Lock-Free Bloom Filter (2026-01)
- Hypothesis: Mutex on bloom add is unnecessary (bit-OR is idempotent)
- Implementation: Removed mutex, atomic bit operations
- Results: Eliminated lock contention on hot write path

### Deferred Bloom Add (2026-01)
- Hypothesis: Bloom add on caller blocks hot path unnecessarily
- Implementation: Moved bloom add from PutPermAsync to background worker
- Results: Faster caller return, parallelized bloom operations
- Note: Reads check pendingCache before bloom to handle race

## Key Findings

### Channel Contention vs Parallelism Tradeoff
- Too many channels (256): High coordination overhead from select statements
- Too few channels (1): Contention bottleneck as workers compete
- Sweet spot (8): Balances load distribution with reduced coordination

### Why 8 Channel Groups Works (1024 shards)
1. Shards 0-127 → Channel 0
2. Shards 128-255 → Channel 1
3. Shards 256-383 → Channel 2
4. Shards 384-511 → Channel 3
5. Shards 512-639 → Channel 4
6. Shards 640-767 → Channel 5
7. Shards 768-895 → Channel 6
8. Shards 896-1023 → Channel 7

This provides:
- Enough parallelism to saturate disk I/O
- Reduced select statement complexity
- Natural load balancing (SHA256 keys distribute evenly)

## Allocation Optimizations

### Bloom Filter Hash Functions
**Before**: Used `fnv.New32a()` which allocates interface values
```go
// BAD: Allocates on every call
h := fnv.New32a()
h.Write(key)
return h.Sum32()
```

**After**: Inline FNV computation (zero allocations)
```go
// GOOD: Zero allocations
h1 := uint32(2166136261)
for _, b := range key {
    h1 ^= uint32(b)
    h1 *= 16777619
}
```

### Benchmark Data Generation
**Before**: `fmt.Sprintf` for key generation (allocates strings)
```go
// BAD: String allocation + conversion
key := []byte(fmt.Sprintf("account:%012d", idx))
```

**After**: `strconv.AppendUint` with pre-allocated buffer
```go
// GOOD: Append to existing buffer
buf := make([]byte, 0, 20)
buf = append(buf, "account:"...)
buf = strconv.AppendUint(buf, idx, 10)
```

### Hash Buffer Pooling
**Before**: Allocate buffer every call
```go
// BAD: Allocation per hash
data := make([]byte, 16)
```

**After**: sync.Pool for buffer reuse
```go
// GOOD: Pool reuse
var hashBufPool = sync.Pool{
    New: func() any { return make([]byte, 16) },
}
data := hashBufPool.Get().([]byte)
defer hashBufPool.Put(data)
```

## Profiling Methodology

### CPU Profiling
```bash
go test -run '^TestBDB_10M$' -v -cpuprofile=/tmp/cpu.prof
go tool pprof -top /tmp/cpu.prof
```

Key metrics to watch:
- `runtime.selectgo`: Channel select overhead
- `runtime.futex`: Lock contention
- `runtime.lock2/unlock2`: Mutex operations
- `syscall.Syscall6`: I/O operations (expected to be high)

### Memory/GC Analysis
Check benchmark log JSON for:
- `num_gc`: Total GC cycles
- `gc_pause_ms`: Total GC pause time
- `heap_bytes`: Current heap size
- `num_goroutine`: Active goroutines

GC overhead formula:
```
GC overhead % = (gc_pause_ms / total_time_ms) * 100
```

For 10M entries, expect:
- BDB: ~150 GC cycles, ~15ms total pause (~0.1% overhead)
- GC is NOT a significant bottleneck for this workload

## Performance Benchmarks

### 100M Deterministic Benchmark (2026-01-11)

Both databases receive identical data: 1 account + 100 txs per batch.

| Metric | BadgerDB | BlockchainDB | Ratio |
|--------|----------|--------------|-------|
| Total Entries @ 3min | 1.3M | 99.9M | **77x** |
| Entries/second | 7,141 | 554,944 | **78x** |
| Avg Batch Write | 14.1ms | 121µs | **117x** |
| Disk Usage | 214 MB | 22.9 GB | - |

### 500M Benchmarks (Prior Results)

| Database | Time | Entries/sec | Disk Usage |
|----------|------|-------------|------------|
| BlockchainDB | ~15 min | 555K | 75.8 GB |
| LevelDB | ~40 min | 208K | 52.1 GB |
| BadgerDB | ~8+ hours | 17K | 89.2 GB |

### 10M Historical Benchmarks

| Database | Time | Notes |
|----------|------|-------|
| BDB (8 channels, 1024 shards) | ~14s | Current optimal |
| BDB (4 channels, 256 shards) | 14.38s | Previous optimal |
| BDB (256 channels) | 14.06s | Original, high selectgo |
| BDB (1 channel) | 16.84s | Too much contention |

## Tuning Parameters

### WriteChannelSize (current: 20000)
Buffer size per channel group. Larger = more memory, smoother throughput.
- Too small: Writers block waiting for workers
- Too large: Memory waste, delayed backpressure

### NumChannelGroups (current: 8)
Number of independent channel groups.
- Increase if: CPU utilization is low, I/O could be more parallel
- Decrease if: selectgo overhead increases

### NumShards (current: 1024)
Number of storage shards. Tested up to 2048.
- More shards = better parallelism, more file handles
- 1024 with 256 bins: 17% faster than 64 bins

### BinCount (current: 256)
Buckets per shard for key distribution.
- More bins = less overflow, more files per shard
- 256 bins optimal based on benchmarks

### InternalKeySize (current: 20)
Bytes stored per key (truncated from 32-byte external keys).
- Saves 12 bytes per entry (25% reduction)
- Collision resistance: 2^80 birthday bound

### workersPerGroup (current: NumCPU/8, min 2)
Workers per channel group.
- Total workers = NumChannelGroups × workersPerGroup
- Should roughly match available I/O parallelism

## Implemented Optimizations

1. ~~**Lock-free bloom filter**~~: Atomic bit operations instead of mutex ✅
2. ~~**Deferred bloom add**~~: Moved to async workers ✅
3. ~~**20-byte internal keys**~~: 7.9% disk savings ✅
4. **Sharded channels**: 8 groups with balanced workers ✅

## Future Optimization Opportunities

1. **Parallel hash/route goroutines**: Hash in goroutine before channel send
2. **Batch API**: Ordered execution for dependent transactions
3. **Multi-process IPC**: Unix socket front-end for external processes
4. **Direct I/O**: Bypass OS cache for predictable latency
5. **NUMA-aware sharding**: Pin shards to specific CPU cores

## Debugging Tips

### High selectgo overhead (>10%)
- Too many channels competing in select
- Solution: Reduce channel count, use sharded approach

### High futex/lock2 (>20%)
- Lock contention on shared resources
- Check: Bloom filter mutex, cache mutexes, shard locks

### Low CPU utilization with slow performance
- I/O bound or blocking on channels
- Check: Disk throughput, channel buffer sizes

### Memory growth over time
- Possible leak in pending cache or pooled buffers
- Check: pendingCache map sizes, sync.Pool effectiveness
