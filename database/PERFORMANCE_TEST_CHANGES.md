# Performance Test Changes

## Summary
Added comprehensive performance testing and comparison between HistoryFile and LevelDB implementations to identify optimization opportunities for read performance.

## Changes Made

### 1. Updated HistoryFile Performance Test (`history_perf_test.go`)
- **Standardized output format**: Modified read performance section to match write statistics format
- **Batch-based reporting**: Changed from individual progress updates to batch-based TPS reporting
- **Consistent metrics**: Shows `[count] @ [TPS] tps [time per op]` for both reads and writes
- **Test parameters**: 10 batches of 1M keys (10M total) for fair comparison with LevelDB

### 2. Created LevelDB Performance Test (`leveldb_perf_test.go`)
- **New test file**: Comprehensive LevelDB benchmark with identical test structure
- **Optimized configuration**:
  - 256MB write buffer
  - 512MB block cache
  - 64MB SST files
  - Batch writes for efficiency
- **Matching output format**: Same statistics format as HistoryFile for direct comparison
- **Added compaction timing**: Similar to HistoryFile's sort phase

### 3. Created Optimized HistoryFile Design (`history_file_optimized.go`)
- **Lock-free cache**: Using `sync.Map` instead of mutex-protected map
- **Increased cache size**: From 100 to 1000 KeySets
- **Memory mapping support**: mmap for zero-copy reads
- **Prefetching**: Background loading of adjacent KeySets
- **Cache statistics**: Hit/miss tracking for performance tuning

## Performance Results (10M keys)

### Write Performance
| Implementation | Average TPS | Time per Write | Relative Speed |
|---------------|------------|----------------|----------------|
| HistoryFile   | 33,855,588 | ~29.5 ns       | 62.8x faster   |
| LevelDB       | 539,003    | ~1.85 μs       | Baseline       |

### Read Performance
| Implementation | Average TPS | Time per Read | Relative Speed |
|---------------|------------|---------------|----------------|
| HistoryFile   | 45,121     | ~22.2 μs      | Baseline       |
| LevelDB       | 353,119    | ~2.83 μs      | 7.8x faster    |

## Key Findings

1. **HistoryFile excels at writes**: Sequential append design provides 62.8x faster write performance
2. **LevelDB excels at reads**: B-tree structure and caching provide 7.8x faster read performance
3. **Trade-offs identified**:
   - HistoryFile requires post-write sorting (1.34s overhead)
   - LevelDB maintains sorted order during writes (higher write latency)

## Optimization Recommendations

### Immediate Optimizations (Low Effort)
1. Increase cache limit from 100 to 1000+ KeySets
2. Replace RWMutex with sync.Map for lock-free cache access
3. Add prefetching for adjacent KeySets

### Advanced Optimizations (Higher Effort)
1. Implement memory mapping (mmap) for zero-copy reads
2. Add Bloom filters for quick negative lookups
3. Use SIMD instructions for faster binary search

### Expected Improvements
- 2-5x faster reads with lock-free cache
- 3-10x faster with memory mapping
- Could approach 200,000-400,000 TPS (closer to LevelDB performance)

## Files Added/Modified
- `history_perf_test.go` - Updated output format and test parameters
- `leveldb_perf_test.go` - New comprehensive LevelDB benchmark
- `history_file_optimized.go` - Optimized design with performance improvements
- `go.mod` - Added goleveldb dependency

## Dependencies Added
- `github.com/syndtr/goleveldb` - LevelDB Go implementation for benchmarking
- `golang.org/x/exp/mmap` (proposed) - For memory-mapped file support