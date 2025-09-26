# Append-Only Read Strategies

## The Challenge
When using append-only writes for speed, reads become more complex since data isn't sorted or indexed during write. Here are the strategies to handle this:

## Strategy 1: Sort After Write Phase (Batch Processing)
**Best for: Bulk loading, blockchain sync, ETL pipelines**

```go
// Write Phase - append only, unsorted
for batch := range batches {
    historyFile.AddKeys(batch)  // Fast append
}

// Sort Phase - one-time cost
historyFile.SortAllBins()  // Sort each bin in place

// Read Phase - binary search on sorted data
value := historyFile.Get(key)  // O(log n) lookup
```

**Pros:**
- Writes are maximally fast
- Reads after sorting are O(log n)
- Simple implementation

**Cons:**
- Can't read during write phase
- Requires explicit sort phase

## Strategy 2: In-Memory Index with Periodic Merge
**Best for: Mixed read/write workloads**

```go
type AppendOnlyWithIndex struct {
    // Append-only file for durability
    dataFile *os.File

    // In-memory sorted index
    memIndex *btree.BTree  // or skip list

    // When memory gets full, merge to disk
    diskIndices []*SortedSegment
}

func (a *AppendOnlyWithIndex) Write(key, value) {
    // Append to file (durable)
    a.dataFile.Append(key, value)

    // Add to memory index (fast reads)
    a.memIndex.Insert(key, fileOffset)

    // Periodically flush memory to sorted disk segment
    if a.memIndex.Size() > threshold {
        a.flushToDisk()
    }
}

func (a *AppendOnlyWithIndex) Read(key) {
    // Check memory first (fast)
    if val := a.memIndex.Get(key); val != nil {
        return val
    }

    // Check disk segments (sorted, so binary search)
    for _, segment := range a.diskIndices {
        if val := segment.BinarySearch(key); val != nil {
            return val
        }
    }
}
```

## Strategy 3: Log-Structured Merge Tree (LSM)
**Best for: High-performance databases**

```
Level 0: [MemTable] - In-memory sorted tree
           ↓ (flush when full)
Level 1: [SST-1] [SST-2] [SST-3] - Small sorted files
           ↓ (compact when many files)
Level 2: [BigSST-1] [BigSST-2] - Larger sorted files
           ↓
Level 3: [HugeSST-1] - Even larger sorted files
```

Reads check each level, writes always go to memory first.

## Strategy 4: Bloom Filters for Negative Lookups
**Best for: Reducing unnecessary disk reads**

```go
type OptimizedAppendOnly struct {
    bloomFilter *bloom.Filter  // Probabilistic membership test
    dataFile    *os.File
}

func (o *OptimizedAppendOnly) Read(key) {
    // Check bloom filter first (in-memory, instant)
    if !o.bloomFilter.MightContain(key) {
        return nil  // Definitely not present, no disk read!
    }

    // Maybe present, need to check disk
    return o.searchDisk(key)
}
```

## Strategy 5: Hybrid Approach (What We Should Use)
**Best for: Blockchain database with distinct phases**

```go
type HybridHistoryFile struct {
    // During sync: append-only for speed
    appendFile *os.File

    // After each batch: quick in-memory index
    recentKeys map[[32]byte]uint64  // key -> offset

    // After sync: sorted segments
    sortedSegments []*SortedSegment

    // Optimization: bloom filter
    bloom *BloomFilter
}

// Phase 1: Fast writes during blockchain sync
func (h *HybridHistoryFile) FastWrite(keys []byte) {
    offset := h.appendFile.Append(keys)

    // Update in-memory index for recent keys
    for _, key := range keys {
        h.recentKeys[key] = offset
        h.bloom.Add(key)
    }
}

// Phase 2: Optimize for reads after sync
func (h *HybridHistoryFile) OptimizeForReads() {
    // Sort and index the append-only file
    h.sortedSegments = h.createSortedSegments()

    // Clear memory index (now have sorted segments)
    h.recentKeys = nil
}

// Adaptive reads based on current phase
func (h *HybridHistoryFile) Read(key [32]byte) (*Value, error) {
    // Check bloom filter first
    if !h.bloom.MightContain(key) {
        return nil, NotFound
    }

    // During sync: check memory index
    if h.recentKeys != nil {
        if offset, ok := h.recentKeys[key]; ok {
            return h.readAt(offset)
        }
    }

    // After sync: binary search sorted segments
    if h.sortedSegments != nil {
        return h.binarySearchSegments(key)
    }

    // Fallback: linear scan (should rarely happen)
    return h.linearScan(key)
}
```

## Practical Implementation for HistoryFile

For the blockchain database, I recommend:

1. **During Initial Sync (Write-Heavy)**
   - Use pure append-only writes
   - Maintain small in-memory index of recent keys
   - Defer sorting until sync completes

2. **After Sync (Read-Heavy)**
   - Sort each bin file once
   - Create position index: `key -> file offset`
   - Use binary search for all reads

3. **For New Blocks (Mixed Workload)**
   - Append new keys to separate "recent" file
   - Periodically merge and sort into main files
   - Check recent file first, then main files

Example implementation:

```go
func (hf *HistoryFileOptimized) Get(key [32]byte) (*DBBKey, error) {
    binIndex := hf.Index(key)

    // 1. Check if bin is sorted
    if hf.binSorted[binIndex] {
        // Binary search - O(log n)
        return hf.binarySearchBin(binIndex, key)
    }

    // 2. Check memory cache for recent writes
    if val, ok := hf.recentWrites[key]; ok {
        return val, nil
    }

    // 3. Fall back to linear scan
    // (only during initial sync)
    return hf.linearScanBin(binIndex, key)
}

func (hf *HistoryFileOptimized) binarySearchBin(binIndex int, key [32]byte) (*DBBKey, error) {
    file := hf.binFiles[binIndex]
    size := hf.binSizes[binIndex]

    // Binary search the sorted file
    left := int64(0)
    right := size / DBKeyFullSize - 1

    for left <= right {
        mid := (left + right) / 2
        offset := mid * DBKeyFullSize

        var entry [DBKeyFullSize]byte
        file.ReadAt(entry[:], offset)

        cmp := bytes.Compare(entry[:32], key[:])
        if cmp == 0 {
            // Found it!
            dbKey := new(DBBKey)
            dbKey.Unmarshal(entry[32:])
            return dbKey, nil
        } else if cmp < 0 {
            left = mid + 1
        } else {
            right = mid - 1
        }
    }

    return nil, NotFound
}
```

## Performance Comparison

| Strategy | Write Speed | Read Speed | Memory Usage | Complexity |
|----------|------------|------------|--------------|------------|
| Read-Modify-Write | O(n²) | O(log n) | High | Simple |
| Pure Append | O(1) | O(n) | Low | Simple |
| Append + Sort | O(1) write, O(n log n) sort | O(log n) | Low | Simple |
| Append + Memory Index | O(1) | O(log n) | Medium | Medium |
| LSM Tree | O(1) | O(log n × levels) | Medium | Complex |
| Hybrid | O(1) | O(log n) | Low-Medium | Medium |

## Recommendation

For the blockchain database, use the **Hybrid Approach**:
1. Append-only during bulk sync (maximum write speed)
2. Sort bins after sync completes
3. Binary search for reads after sorting
4. Keep recent writes in memory index for mixed workloads

This gives you:
- 10x+ faster writes during sync
- O(log n) reads after optimization
- Simple, maintainable code
- Good performance in all phases