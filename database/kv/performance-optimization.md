# KVShard Performance Optimization Guide

This document captures the optimization theories, processes, and findings from performance tuning the KVShard async write architecture.

## Architecture Overview

KVShard is a 256-shard key-value store optimized for blockchain data:
- **256 shards**: Determined by first byte of key hash, enables parallel I/O
- **Bloom filter**: Mmap'd for fast negative lookups with crash recovery
- **Two storage tiers**:
  - DynaKV: In-memory for mutable data (accounts, state)
  - PermKV: Disk-based for immutable data (transactions, blocks)
- **Async writes**: Background workers process permanent writes via channels

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

### Final Solution: Sharded Channels (4 groups)
- Hypothesis: Balance parallelism with reduced coordination
- Implementation: 4 channel groups, NumCPU/4 workers per group
- Results: 14.38s (equivalent to original)
- selectgo overhead: 3.05% (down from 27.87%)
- Goroutines: 28 (down from 256, 89% reduction)

## Key Findings

### Channel Contention vs Parallelism Tradeoff
- Too many channels (256): High coordination overhead from select statements
- Too few channels (1): Contention bottleneck as workers compete
- Sweet spot (4): Balances load distribution with reduced coordination

### Why 4 Channel Groups Works
1. Shards 0-63 → Channel 0
2. Shards 64-127 → Channel 1
3. Shards 128-191 → Channel 2
4. Shards 192-255 → Channel 3

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

## Performance Benchmarks (10M entries)

| Database | Time | Notes |
|----------|------|-------|
| BDB (4 channels) | 14.38s | Optimized architecture |
| BDB (256 channels) | 14.06s | Original, high selectgo |
| BDB (1 channel) | 16.84s | Too much contention |
| Badger | ~22s | LSM-tree overhead |
| LevelDB | ~25s | Single-threaded compaction |

## Tuning Parameters

### WriteChannelSize (current: 20000)
Buffer size per channel group. Larger = more memory, smoother throughput.
- Too small: Writers block waiting for workers
- Too large: Memory waste, delayed backpressure

### NumChannelGroups (current: 4)
Number of independent channel groups.
- Increase if: CPU utilization is low, I/O could be more parallel
- Decrease if: selectgo overhead increases

### workersPerGroup (current: NumCPU/4, min 2)
Workers per channel group.
- Total workers = NumChannelGroups × workersPerGroup
- Should roughly match available I/O parallelism

## Future Optimization Opportunities

1. **Batch writes within workers**: Accumulate multiple writes before disk flush
2. **Lock-free bloom filter**: Atomic bit operations instead of mutex
3. **Direct I/O**: Bypass OS cache for predictable latency
4. **Adaptive channel count**: Dynamically adjust based on load
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
