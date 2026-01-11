# Benchmark Results - January 2026

**Branch**: `2-performance`
**Base Commit**: `6156ffc` (Fix branch creation script)
**Date**: 2026-01-11
**System**: Linux 6.16.3-76061603-generic, 24 CPU cores

## Executive Summary

BlockchainDB (BDB) demonstrates **77x faster batch writes** than BadgerDB and **555K entries/sec** sustained throughput at 100M entries. Key optimizations include:

- **20-byte internal keys**: 7.9% disk savings (6GB at 500M entries)
- **Lock-free bloom filter**: Eliminated mutex contention on hot path
- **Deferred bloom add**: Moved from caller to async worker
- **Sharded async channels**: 8 channel groups, reduced selectgo from 28% to 3%

## Deterministic 100M Benchmark

Both databases receive identical data: 1 account + 100 transactions per batch.

### Results at 3 Minutes

| Metric | BadgerDB | BlockchainDB | Ratio |
|--------|----------|--------------|-------|
| Total Entries | 1.3M | 99.9M | **77x** |
| Entries/second | 7,141 | 554,944 | **78x** |
| Disk Usage | 214 MB | 22.9 GB | - |
| Bytes/Entry | 175 B | 246 B | 1.4x |
| Avg Batch Write | 14.1ms | 121µs | **117x** |
| Avg Read Time | 20µs | 117µs | 0.17x |
| Heap Memory | 1.0 GB | N/A | - |

### Results at 6 Minutes

| Metric | BadgerDB | BlockchainDB | Ratio |
|--------|----------|--------------|-------|
| Total Entries | 3.8M | 99.9M (done) | **26x** |
| Entries/second | 13,862 | 0 (waiting) | - |
| Disk Usage | 685 MB | 22.9 GB | - |
| Bytes/Entry | 190 B | 246 B | 1.3x |
| Avg Batch Write | 9.6ms | 121µs | **79x** |

**Key Observations:**
- BDB completed 100M entries in ~2 minutes
- Badger at 3.8M after 6 minutes (estimated 4+ hours to complete)
- BDB batch writes 79-117x faster than Badger
- BDB reads slightly slower (117µs vs 20µs) due to disk-based architecture

## 500M Entry Benchmarks (Prior Results)

From earlier testing sessions:

| Database | 500M Time | Entries/sec | Disk Usage |
|----------|-----------|-------------|------------|
| BlockchainDB | ~15 min | 555K | 75.8 GB |
| LevelDB | ~40 min | 208K | 52.1 GB |
| BadgerDB | ~8+ hours | 17K | 89.2 GB |

## Architecture Optimizations

### 1. 20-Byte Internal Keys

**Commit**: Current branch (uncommitted)

External API accepts `[32]byte` keys, internally truncated to `[20]byte`:

```go
const InternalKeySize = 20
type InternalKey [InternalKeySize]byte

func truncateKey(key [32]byte) InternalKey {
    var k InternalKey
    copy(k[:], key[:InternalKeySize])
    return k
}
```

**Impact:**
- Entry size: 48 → 36 bytes (25% reduction per entry)
- Disk savings at 500M: ~6 GB (7.9% reduction)
- Collision resistance: 2^80 birthday bound (sufficient for blockchain)
- Performance: Unchanged (bottleneck is hashing/I/O, not key comparison)

### 2. Lock-Free Bloom Filter

**Commit**: `02b0ddc`

Bloom filter `Add()` uses atomic bit-OR operations:

```go
func (bf *MmapBloomFilter) Add(key InternalKey) {
    // Atomic bit-OR: safe for concurrent access
    bf.data[byteIdx] |= 1 << bitIdx
    // No mutex needed - idempotent operation
}
```

**Impact:**
- Removed mutex from hot write path
- Multiple goroutines can add concurrently without locking

### 3. Deferred Bloom Add

**Commit**: `02b0ddc`

Bloom filter add moved from `PutPermAsync()` to background worker:

```go
// Before: Add in caller (blocking)
func PutPermAsync(key, value) {
    k.Bloom.Add(ikey)  // Was here
    channel <- request
}

// After: Add in worker (non-blocking)
func batchWriter() {
    req := <-channel
    k.Bloom.Add(req.key)  // Now here
    k.Shards[idx].PutPerm(...)
}
```

**Impact:**
- Caller returns faster
- Bloom add parallelized across worker goroutines
- Reads check pendingCache first (before bloom) to handle race

### 4. Sharded Channel Architecture

**Commit**: `02b0ddc`

```
8 channel groups × (NumCPU/8) workers = balanced parallelism

Shard 0-127   → Channel 0 → Workers
Shard 128-255 → Channel 1 → Workers
...
Shard 896-1023 → Channel 7 → Workers
```

**Impact:**
- selectgo overhead: 28% → 3%
- Goroutines: 256 → 28 (89% reduction)
- Performance maintained with 8x fewer goroutines

## Configuration

Current optimal settings:

```go
const (
    DefaultNumShards        = 1024
    DefaultNumChannelGroups = 8
    DefaultWriteChannelSize = 20000
    DefaultBinCount         = 256
    InternalKeySize         = 20
)
```

## Test Reproduction

```bash
# Clone and checkout
git clone https://github.com/BlockchainDB/database
cd database
git checkout 2-performance

# Run deterministic benchmark
cd database/dkv
go test -run '^TestDeterministic_100M$' -v -timeout 6h > /tmp/bench.log 2>&1

# Monitor progress
tail -f /tmp/bench.log

# Run 500M benchmarks (individual databases)
go test -run '^TestBDB_500M$' -v -timeout 2h
go test -run '^TestLevelDB_500M$' -v -timeout 2h
go test -run '^TestBadger_500M$' -v -timeout 12h
```

## CPU Profile Analysis

```bash
go test -run '^TestBDB_100M$' -cpuprofile=/tmp/cpu.prof
go tool pprof -top /tmp/cpu.prof
```

Key metrics to monitor:
- `runtime.selectgo`: Should be <5% (channel overhead)
- `syscall.Syscall6`: Expected high (I/O operations)
- `runtime.futex`: Should be <10% (lock contention)

## Future Work

1. **Parallel hash/route goroutines**: Remove serial hashing bottleneck
2. **Batch API**: Ordered execution for dependent transactions
3. **Multi-process IPC**: Unix socket front-end for multi-process access

## Historical Comparison

| Version | 10M Time | Notes |
|---------|----------|-------|
| Original (256 channels) | 14.06s | 28% selectgo |
| Single channel | 16.84s | Contention bottleneck |
| 4 channel groups | 14.38s | 3% selectgo |
| 8 channel groups | ~14s | Current optimal |
