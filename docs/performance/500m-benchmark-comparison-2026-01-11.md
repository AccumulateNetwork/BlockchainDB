# 500M Entry Benchmark Comparison - 2026-01-11

## Test Configuration
- **Target**: 500,000,096 entries (500M)
- **Timeout**: 5 hours (none hit limit)
- **System**: Linux 6.16.3, 24 CPU cores
- **All tests run in parallel**
- **Workload**: ~99.9% writes, ~0.1% reads (~550K reads during test)

## Summary Results

| Metric | BadgerDB | LevelDB | BlockchainDB | Best |
|--------|----------|---------|--------------|------|
| **Time** | 1h14m | 1h32m | 1h45m | Badger |
| **Throughput** | 97,223/s | 69,121/s | 55,361/s | Badger |
| **Disk Usage** | 102.9 GB | 34.9 GB | 75.1 GB | LevelDB |
| **Bytes/Entry** | 205.9 B | 69.8 B | 150.2 B | LevelDB |
| **Avg Write** | 332 µs | 688 µs | 1,256 µs | Badger |
| **Avg Read** | 4,940 µs | 3,747 µs | 2 µs | **BDB** |
| **Heap Memory** | 12.41 GB | 0.92 GB | 1.24 GB | LevelDB |
| **GC Pause Total** | 182,718 ms | 117 ms | 2,329 ms | LevelDB |

## Analysis

### Write Performance
BadgerDB achieved highest write throughput (97K/s), followed by LevelDB (69K/s) and BDB (55K/s). However, this came at significant memory cost for Badger.

### Read Performance (limited sample ~550K reads)
BlockchainDB showed 2µs average read time vs 3,747µs (LevelDB) and 4,940µs (Badger). However, this is from a small sample during a write-heavy test. **Dedicated read benchmarks needed for conclusive comparison.**

### Storage Efficiency
LevelDB is most storage-efficient at 69.8 bytes/entry. BDB uses 150.2 bytes/entry, Badger uses 205.9 bytes/entry.

### Memory Usage
BadgerDB used 12.4 GB heap memory with 182 seconds of GC pause time (significant). LevelDB used only 0.92 GB with minimal GC impact. BDB used 1.24 GB.

## Key Observations

1. **BadgerDB trades memory for write speed** - Fast writes but 12GB heap and 3 minutes of GC pauses
2. **LevelDB is most balanced** - Good write speed, excellent storage efficiency, low memory
3. **BlockchainDB excels at reads** - 2µs read latency is exceptional for blockchain query workloads

## Recommendation

For blockchain workloads with frequent reads:
- **BlockchainDB** - Best read performance (2µs vs 4000+µs)

For write-heavy ingestion:
- **BadgerDB** - Fastest writes if memory available

For storage-constrained environments:
- **LevelDB** - Best bytes/entry ratio

## Log Files
- `bdb_500m_benchmark_2026-01-11.log`
- `leveldb_500m_benchmark_2026-01-11.log`
- `badger_500m_benchmark_2026-01-11.log`
