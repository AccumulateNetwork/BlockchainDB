# Large Fan-Out Test Results

## Test Configuration
- Dataset: 20,000 entries with truly random SHA-256 hashes
- Fan-out values tested: 256, 1024, 2048
- Read tests: 2,000 sequential and 2,000 random reads

## Performance Results

### Write Performance
| Fan-Out | Write Rate | Latency per Write | Relative Performance |
|---------|------------|-------------------|---------------------|
| 256     | 741/sec    | 1.35 ms          | Baseline (100%)     |
| 1024    | 737/sec    | 1.36 ms          | 99.5%              |
| 2048    | 725/sec    | 1.38 ms          | 97.8%              |

**Finding:** Write performance slightly degrades with larger fan-outs, but the difference is minimal (<3%).

### Sequential Read Performance
| Fan-Out | Read Rate    | Latency per Read | Relative Performance |
|---------|--------------|------------------|---------------------|
| 256     | 5,447,884/sec| 0.183 µs        | Baseline (100%)     |
| 1024    | 4,995,654/sec| 0.200 µs        | 91.7%              |
| 2048    | 4,775,390/sec| 0.209 µs        | 87.7%              |

**Finding:** Sequential reads are fastest with fan-out 256, with performance degrading as fan-out increases.

### Random Read Performance
| Fan-Out | Read Rate    | Latency per Read | Relative Performance |
|---------|--------------|------------------|---------------------|
| 256     | 2,395,118/sec| 0.417 µs        | 94.7%              |
| 1024    | 2,527,937/sec| 0.396 µs        | Baseline (100%)     |
| 2048    | 2,179,079/sec| 0.459 µs        | 86.2%              |

**Finding:** Fan-out 1024 shows the best random read performance, outperforming both 256 and 2048.

## Memory Usage Analysis

| Fan-Out | Memory per Level | Memory per Entry | Memory Efficiency |
|---------|------------------|------------------|-------------------|
| 256     | 4 MB            | 209 bytes        | Best              |
| 1024    | 16 MB           | 838 bytes        | Moderate          |
| 2048    | 32 MB           | 1,677 bytes      | Poor              |

**Finding:** Memory usage grows linearly with fan-out, with 2048 using 8x more memory than 256.

## Tree Structure Analysis

| Fan-Out | Bits per Level | Max Tree Depth | Entries per Bin (avg) |
|---------|----------------|----------------|----------------------|
| 256     | 8              | 32 levels      | 78                   |
| 1024    | 10             | 26 levels      | 19                   |
| 2048    | 11             | 23 levels      | 9                    |

**Finding:** With 20K entries, all configurations stayed at 1 level. Larger fan-outs spread entries more thinly across bins.

## Key Insights

### 1. Write Performance
- All fan-out values show similar write performance (~725-741 writes/sec)
- The overhead of managing more bins in larger fan-outs has minimal impact
- Write performance is dominated by WAL and buffer management, not fan-out

### 2. Read Performance Trade-offs
- **Sequential reads:** Smaller fan-outs (256) perform better due to better cache locality
- **Random reads:** Medium fan-out (1024) is optimal, balancing bin count and search efficiency
- Fan-out 2048 shows degraded performance in both sequential and random reads

### 3. Memory Overhead
- Fan-out 2048 uses 8x more memory than 256 (32MB vs 4MB per level)
- Memory per entry increases dramatically: 209 bytes (256) → 1,677 bytes (2048)
- The memory overhead makes 2048 impractical for most use cases

### 4. Scalability Considerations
For larger datasets (1M+ entries):
- Fan-out 256: Would create deeper trees (2-3 levels)
- Fan-out 1024: Better balance, fewer levels
- Fan-out 2048: Minimal depth benefit but huge memory cost

## Recommendations

### ❌ Fan-Out 2048: Not Recommended
- **Cons:**
  - 8x memory usage of fan-out 256
  - Worst sequential read performance
  - No significant benefits over 1024
  - Excessive memory per entry (1.6KB)

### ⚖️ Fan-Out 1024: Specialized Use Cases
- **Pros:**
  - Best random read performance
  - Shallower trees for very large datasets
  - Good for workloads with heavy random access
- **Cons:**
  - 4x memory usage of fan-out 256
  - Worse sequential read performance
  - More complex bit manipulation (10 bits)

### ✅ Fan-Out 256: Recommended Default
- **Pros:**
  - Best write performance
  - Best sequential read performance
  - Byte-aligned (8 bits) - simplest implementation
  - Most memory efficient
  - Good balance for most workloads
- **Cons:**
  - Slightly worse random read performance than 1024

## Conclusion

The testing confirms that **fan-out 256 remains the optimal choice** for most use cases:
1. It provides the best overall performance
2. Uses memory efficiently
3. Has byte-aligned operations
4. Scales well to large datasets

Fan-out 1024 could be considered for specialized workloads with:
- Heavy random access patterns
- Very large datasets (100M+ entries)
- Systems with ample memory

Fan-out 2048 should be avoided due to excessive memory usage with no meaningful performance benefits.

## Test Command
```bash
go test -run TestLargeFanOut -v -timeout 180s
```

## Test Dataset Characteristics
- 20,000 unique entries
- Keys: SHA-256 hashes of random data (crypto/rand)
- Values: DBBKey with sequential offsets
- Truly random distribution across hash space