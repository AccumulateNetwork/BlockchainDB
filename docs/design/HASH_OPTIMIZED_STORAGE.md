# Hash-Optimized Storage Design

## Key Insights

1. **Caching is useless** - Random hash access means ~0% cache hit rate
2. **Sparse indices don't work** - Hash space is too large (2^256)
3. **Every read is a cold read** - Must optimize for single disk seeks

## Recommended Solution: Hash-Partitioned Dense Index

### Architecture

```
256 Partitions (by first byte of hash)
├── Partition 0x00/
│   ├── index.dat   (dense index: key→offset)
│   └── data.dat    (actual key-value pairs)
├── Partition 0x01/
│   ├── index.dat
│   └── data.dat
...
└── Partition 0xFF/
    ├── index.dat
    └── data.dat
```

### Why This Works

1. **Natural distribution** - Cryptographic hashes uniformly distribute across partitions
2. **Smaller indices** - Each partition index is 1/256th the size
3. **Parallel operations** - Different partitions can be accessed concurrently
4. **Single seek reads** - Dense index gives exact offset

### Implementation

```go
type HashPartitionedDB struct {
    partitions [256]*Partition
}

type Partition struct {
    indexFile *os.File
    dataFile  *os.File
    index     []IndexEntry  // Kept in memory if possible
    mutex     sync.RWMutex  // Per-partition locking
}

type IndexEntry struct {
    Key    [32]byte
    Offset int64
    Size   uint32
}
```

### Write Path (Append-Only)

```go
func (p *Partition) Append(key [32]byte, value []byte) error {
    p.mutex.Lock()
    defer p.mutex.Unlock()

    // Get current end of data file
    offset, _ := p.dataFile.Seek(0, io.SeekEnd)

    // Write to data file
    entry := append(key[:], value...)
    p.dataFile.Write(entry)

    // Add to index
    p.index = append(p.index, IndexEntry{
        Key:    key,
        Offset: offset,
        Size:   uint32(len(value)),
    })

    // Persist index entry
    p.indexFile.Write(marshalIndexEntry(key, offset, len(value)))

    return nil
}
```

### Read Path (One Seek)

```go
func (db *HashPartitionedDB) Get(key [32]byte) ([]byte, error) {
    // Select partition by first byte
    partition := db.partitions[key[0]]

    partition.mutex.RLock()
    defer partition.mutex.RUnlock()

    // Binary search the dense index (in memory)
    idx := sort.Search(len(partition.index), func(i int) bool {
        return bytes.Compare(partition.index[i].Key[:], key[:]) >= 0
    })

    if idx >= len(partition.index) || partition.index[idx].Key != key {
        return nil, ErrNotFound
    }

    // Single disk read at exact offset
    entry := partition.index[idx]
    buf := make([]byte, entry.Size)
    partition.dataFile.ReadAt(buf, entry.Offset)

    return buf, nil
}
```

### Memory Requirements

For 100 million keys:
```
Per index entry: 32 (key) + 8 (offset) + 4 (size) = 44 bytes
Total index: 100M × 44 bytes = 4.4GB

Per partition: 4.4GB / 256 = 17.2MB
```

This fits comfortably in memory for most systems.

### Optimization: Tiered Storage

For even larger datasets:

```go
type TieredPartition struct {
    // Tier 1: Recent writes (fully in memory)
    memTable *MemTable

    // Tier 2: Recent index (in memory)
    recentIndex []IndexEntry

    // Tier 3: Historical index (memory-mapped)
    historicalIndex []byte  // mmap'd file

    // Data always on disk
    dataFile *os.File
}

func (tp *TieredPartition) Get(key [32]byte) ([]byte, error) {
    // Check memory table first
    if val, ok := tp.memTable.Get(key); ok {
        return val, nil
    }

    // Check recent index
    if idx := tp.searchRecentIndex(key); idx >= 0 {
        return tp.readData(tp.recentIndex[idx])
    }

    // Check historical index (memory-mapped, OS handles paging)
    if idx := tp.searchHistoricalIndex(key); idx >= 0 {
        return tp.readData(tp.parseIndexEntry(idx))
    }

    return nil, ErrNotFound
}
```

### No-Cache Performance Analysis

| Operation | Current System | Hash-Partitioned |
|-----------|---------------|------------------|
| Write | O(n) - rewrite KeySet | O(1) - append only |
| Read | O(log n) × num_keysets + cache miss | O(log n/256) + 1 seek |
| Memory | O(n) for cache + data | O(n/256) for index only |
| Disk seeks | Multiple | Exactly 1 |

### Parallel Operations

```go
func (db *HashPartitionedDB) BatchGet(keys [][32]byte) []Result {
    results := make([]Result, len(keys))
    var wg sync.WaitGroup

    for i, key := range keys {
        wg.Add(1)
        go func(idx int, k [32]byte) {
            defer wg.Done()
            val, err := db.Get(k)
            results[idx] = Result{val, err}
        }(i, key)
    }

    wg.Wait()
    return results
}
```

Different partitions can be accessed in parallel with no lock contention.

### Compaction Strategy

Since it's append-only and immutable:

```go
func (p *Partition) Compact() error {
    // Only needed if you want to reclaim space from duplicates
    // (shouldn't happen with hash(value) = key)

    seen := make(map[[32]byte]bool)
    newIndex := []IndexEntry{}

    // Scan backwards (keep newest)
    for i := len(p.index) - 1; i >= 0; i-- {
        if !seen[p.index[i].Key] {
            seen[p.index[i].Key] = true
            newIndex = append(newIndex, p.index[i])
        }
    }

    // Reverse to restore order
    sort.Slice(newIndex, func(i, j int) bool {
        return bytes.Compare(newIndex[i].Key[:], newIndex[j].Key[:]) < 0
    })

    p.index = newIndex
    return p.rewriteFiles()
}
```

### Benefits Over Current System

1. **No cache needed** - Accepts random access pattern
2. **Predictable performance** - Always 1 disk seek
3. **No degradation** - O(1) writes forever
4. **Parallel friendly** - 256-way parallelism
5. **Simple implementation** - Just arrays and binary search

### Summary

For hash-based keys with random access:
- **Forget caching** - It won't help
- **Use dense indices** - Every key needs an entry
- **Partition by hash prefix** - Divide and conquer
- **Keep indices in memory** - Or memory-map them
- **Optimize for one disk seek** - That's the best you can do

This design accepts the reality of random hash access and optimizes for it, rather than fighting it with useless caches.