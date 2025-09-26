# KV Layer Performance Fix - Implementation Guide

## Problem Statement
The KV layer uses **O(n) linear scanning** for key lookups, causing 14x performance degradation at 2M keys. The history layer already solved this with a hybrid sorted/unsorted approach achieving sustained 3.56M writes/sec.

## Quick Fix (1 Day) - Add Binary Search

### File: `database/storage/kfile.go`

**Current Problem** (line 413):
```go
// Linear scan - O(n) - TERRIBLE!
for len(keys) >= DBKeyFullSize {
    if [32]byte(keys) == Key {
        // Found
    }
    keys = keys[DBKeyFullSize:]
}
```

**Immediate Fix**:
```go
// Add to KFile struct
type KFile struct {
    // ... existing fields ...
    sectionSorted []bool  // NEW: Track if each section is sorted
}

// Replace kGet method
func (k *KFile) kGet(Key [32]byte) (*DBBKey, error) {
    index := k.OffsetIndex(Key[:])
    start := k.Offsets[index]
    end := k.Offsets[index+1]  // or k.File.EOD for last

    if start == end {
        return nil, errors.New("not found")
    }

    keys := make([]byte, end-start)
    if err := k.File.ReadAt(start, keys); err != nil {
        return nil, err
    }

    // NEW: Ensure section is sorted
    if !k.sectionSorted[index] {
        k.sortKeysInPlace(keys)
        k.sectionSorted[index] = true
        // Optionally write back sorted keys
        k.File.WriteAt(start, keys)
    }

    // NEW: Binary search
    numKeys := len(keys) / DBKeyFullSize
    idx := sort.Search(numKeys, func(i int) bool {
        offset := i * DBKeyFullSize
        return bytes.Compare(keys[offset:offset+32], Key[:]) >= 0
    })

    if idx < numKeys {
        offset := idx * DBKeyFullSize
        if bytes.Equal(keys[offset:offset+32], Key[:]) {
            var dbKey DBBKey
            dbKey.Unmarshal(keys[offset:])
            return &dbKey, nil
        }
    }

    return nil, errors.New("not found")
}

// Helper to sort keys in place
func (k *KFile) sortKeysInPlace(keys []byte) {
    numKeys := len(keys) / DBKeyFullSize
    entries := make([][]byte, numKeys)

    for i := 0; i < numKeys; i++ {
        start := i * DBKeyFullSize
        entries[i] = keys[start : start+DBKeyFullSize]
    }

    sort.Slice(entries, func(i, j int) bool {
        return bytes.Compare(entries[i][:32], entries[j][:32]) < 0
    })

    for i, entry := range entries {
        copy(keys[i*DBKeyFullSize:], entry)
    }
}
```

**Expected Improvement**: 100-1000x for sections with >1000 keys

## Full Solution (1 Week) - Port Hybrid Approach

### Step 1: Create Hybrid Section Structure

```go
// New file: database/storage/hybrid_section.go
package blockchainDB

import (
    "bytes"
    "sort"
    "sync"
)

type HybridSection struct {
    index    int
    file     *BFile

    // Sorted data (on disk)
    sortedOffset int64
    sortedCount  int32

    // Unsorted buffer (in memory)
    unsortedKeys   []byte
    unsortedCount  int32
    maxUnsorted    int32

    // Memory index for O(1) lookup
    memIndex map[[32]byte]*DBBKey

    mu sync.RWMutex
}

func NewHybridSection(index int, file *BFile) *HybridSection {
    return &HybridSection{
        index:       index,
        file:        file,
        memIndex:    make(map[[32]byte]*DBBKey),
        maxUnsorted: 1000,  // Trigger sort after 1000 keys
    }
}

func (hs *HybridSection) Put(key [32]byte, value *DBBKey) error {
    hs.mu.Lock()
    defer hs.mu.Unlock()

    // Add to memory
    keyBytes := value.Bytes(key)
    hs.unsortedKeys = append(hs.unsortedKeys, keyBytes...)
    hs.memIndex[key] = value
    hs.unsortedCount++

    // Check if we need to trigger background sort
    if hs.unsortedCount >= hs.maxUnsorted {
        return hs.triggerSort()
    }

    return nil
}

func (hs *HybridSection) Get(key [32]byte) (*DBBKey, error) {
    hs.mu.RLock()

    // Check memory index first - O(1)
    if val, ok := hs.memIndex[key]; ok {
        hs.mu.RUnlock()
        return val, nil
    }

    // Check sorted section - O(log n)
    if hs.sortedCount > 0 {
        hs.mu.RUnlock()
        return hs.binarySearchSorted(key)
    }

    hs.mu.RUnlock()
    return nil, errors.New("not found")
}

func (hs *HybridSection) binarySearchSorted(key [32]byte) (*DBBKey, error) {
    // Read sorted section
    sortedSize := int64(hs.sortedCount) * DBKeyFullSize
    sortedData := make([]byte, sortedSize)

    if err := hs.file.ReadAt(hs.sortedOffset, sortedData); err != nil {
        return nil, err
    }

    // Binary search
    numKeys := int(hs.sortedCount)
    idx := sort.Search(numKeys, func(i int) bool {
        offset := i * DBKeyFullSize
        return bytes.Compare(sortedData[offset:offset+32], key[:]) >= 0
    })

    if idx < numKeys {
        offset := idx * DBKeyFullSize
        if bytes.Equal(sortedData[offset:offset+32], key[:]) {
            var dbKey DBBKey
            dbKey.Unmarshal(sortedData[offset:])
            return &dbKey, nil
        }
    }

    return nil, errors.New("not found")
}

func (hs *HybridSection) triggerSort() error {
    // This would queue the section for background sorting
    // For now, do it synchronously
    return hs.sortAndMerge()
}

func (hs *HybridSection) sortAndMerge() error {
    // Read existing sorted data
    var allKeys []byte
    if hs.sortedCount > 0 {
        sortedSize := int64(hs.sortedCount) * DBKeyFullSize
        sortedData := make([]byte, sortedSize)
        if err := hs.file.ReadAt(hs.sortedOffset, sortedData); err != nil {
            return err
        }
        allKeys = sortedData
    }

    // Merge with unsorted
    allKeys = append(allKeys, hs.unsortedKeys...)

    // Sort all keys
    hs.sortKeysInPlace(allKeys)

    // Write back to disk
    newOffset, err := hs.file.Offset()
    if err != nil {
        return err
    }

    if _, err := hs.file.Write(allKeys); err != nil {
        return err
    }

    // Update metadata
    hs.sortedOffset = newOffset
    hs.sortedCount = int32(len(allKeys) / DBKeyFullSize)

    // Clear unsorted buffer
    hs.unsortedKeys = nil
    hs.unsortedCount = 0

    // Keep recent keys in memory index for fast access
    // (optional optimization)

    return nil
}

func (hs *HybridSection) sortKeysInPlace(keys []byte) {
    // Same as before - sort keys in place
}
```

### Step 2: Update KFile to Use Hybrid Sections

```go
// Modify database/storage/kfile.go

type KFile struct {
    // ... existing fields ...

    // NEW: Replace simple offsets with hybrid sections
    hybridSections []*HybridSection

    // NEW: Background sorting
    sortQueue   chan int
    sortWorkers sync.WaitGroup
    stopSignal  chan struct{}
}

func NewKFile(...) (*KFile, error) {
    // ... existing initialization ...

    // Initialize hybrid sections
    kFile.hybridSections = make([]*HybridSection, offsetCnt)
    for i := uint64(0); i < offsetCnt; i++ {
        kFile.hybridSections[i] = NewHybridSection(int(i), kFile.File)
    }

    // Start background sorter
    kFile.sortQueue = make(chan int, offsetCnt)
    kFile.stopSignal = make(chan struct{})
    kFile.startBackgroundSorter()

    return kFile, nil
}

func (k *KFile) Put(Key [32]byte, dbBKey *DBBKey) error {
    // Use hybrid section
    index := k.OffsetIndex(Key[:])
    return k.hybridSections[index].Put(Key, dbBKey)
}

func (k *KFile) Get(Key [32]byte) (*DBBKey, error) {
    // Check cache first
    if value, ok := k.Cache[Key]; ok {
        return value, nil
    }

    // Use hybrid section
    index := k.OffsetIndex(Key[:])
    return k.hybridSections[index].Get(Key)
}

func (k *KFile) startBackgroundSorter() {
    k.sortWorkers.Add(1)
    go func() {
        defer k.sortWorkers.Done()

        for {
            select {
            case idx := <-k.sortQueue:
                // Sort the section
                k.hybridSections[idx].sortAndMerge()

            case <-k.stopSignal:
                return
            }
        }
    }()
}
```

### Step 3: Add Write Buffering

```go
// Add to KV struct in database/kv/kv.go

type KV struct {
    // ... existing fields ...

    // NEW: Write buffer
    writeBuffer *WriteBuffer
}

type WriteBuffer struct {
    entries []WriteEntry
    size    int
    maxSize int
    kv      *KV
    mu      sync.Mutex
}

type WriteEntry struct {
    Key   [32]byte
    Value []byte
}

func (wb *WriteBuffer) Add(key [32]byte, value []byte) error {
    wb.mu.Lock()
    defer wb.mu.Unlock()

    wb.entries = append(wb.entries, WriteEntry{key, value})
    wb.size += len(value) + 32

    if wb.size >= wb.maxSize {
        return wb.flush()
    }

    return nil
}

func (wb *WriteBuffer) flush() error {
    if len(wb.entries) == 0 {
        return nil
    }

    // Batch write all entries
    for _, entry := range wb.entries {
        if err := wb.kv.directPut(entry.Key, entry.Value); err != nil {
            return err
        }
    }

    // Clear buffer
    wb.entries = wb.entries[:0]
    wb.size = 0

    return nil
}

// Modified Put method
func (k *KV) Put(key [32]byte, value []byte) error {
    return k.writeBuffer.Add(key, value)
}
```

## Performance Testing

Create a benchmark to verify improvements:

```go
// database/benchmarks/kv_performance_test.go

func BenchmarkKVOperations(b *testing.B) {
    scenarios := []struct {
        name string
        keys int
    }{
        {"10K", 10_000},
        {"100K", 100_000},
        {"1M", 1_000_000},
    }

    for _, sc := range scenarios {
        b.Run(sc.name, func(b *testing.B) {
            kv, _ := NewKV(...)

            // Write test
            b.Run("Write", func(b *testing.B) {
                b.ResetTimer()
                for i := 0; i < b.N && i < sc.keys; i++ {
                    key := generateKey(i)
                    kv.Put(key, []byte("value"))
                }
                b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
            })

            // Read test
            b.Run("Read", func(b *testing.B) {
                b.ResetTimer()
                for i := 0; i < b.N && i < sc.keys; i++ {
                    key := generateKey(i % sc.keys)
                    kv.Get(key)
                }
                b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
            })
        })
    }
}
```

## Expected Results

| Metric | Current | With Binary Search | With Full Hybrid |
|--------|---------|-------------------|------------------|
| Write @ 10K | 1.8M/sec | 1.8M/sec | 3.5M/sec |
| Write @ 1M | 128K/sec | 150K/sec | 3.5M/sec |
| Read @ 10K | 500K/sec | 2M/sec | 2M/sec |
| Read @ 1M | 50K/sec | 500K/sec | 1M/sec |
| Memory Usage | Grows linearly | Same | Bounded by maxUnsorted |

## Migration Path

1. **Day 1**: Implement binary search in kfile.go - Immediate improvement
2. **Day 2-3**: Create HybridSection structure and tests
3. **Day 4-5**: Integrate HybridSection into KFile
4. **Day 6**: Add write buffering
5. **Day 7**: Performance testing and tuning

## Testing Checklist

- [ ] Unit tests for binary search
- [ ] Unit tests for HybridSection
- [ ] Integration tests for modified KFile
- [ ] Benchmark showing no degradation at 2M+ keys
- [ ] Concurrent read/write tests
- [ ] Memory usage tests

## Success Criteria

1. **No performance degradation** - Maintain consistent ops/sec up to 10M keys
2. **Read performance** - Minimum 500K reads/sec at any scale
3. **Write performance** - Minimum 1M writes/sec sustained
4. **Memory bounded** - Memory usage does not grow unbounded with data size

## Code Location Reference

Files to modify:
- `database/storage/kfile.go` - Add hybrid sections
- `database/kv/kv.go` - Add write buffering
- `database/storage/bfile.go` - Ensure async I/O support

Working reference implementation:
- `database/history/history_file.go` - Lines 26-90 (HybridLeaf structure)
- `database/history/history_file.go` - Lines 200-250 (Put/Get methods)

The solution is already proven - just needs to be ported from history to KV layer.