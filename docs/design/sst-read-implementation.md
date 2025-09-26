# SST (Sorted String Table) Read Implementation

## SST File Format

### 1. File Structure
```
[Header: 64 bytes]
  Magic:      8 bytes  "SSTABLE\0"
  Version:    4 bytes
  NumEntries: 4 bytes
  MinKey:     32 bytes (first key in file)
  MaxKey:     32 bytes (last key in file)

[Index Block: Variable size, typically 4KB]
  Sparse index with every Nth key and its offset
  Format: [Key(32B)][BlockOffset(8B)][BlockSize(4B)]...

[Data Blocks: 4KB each]
  Sorted key-value pairs
  Format: [Key(32B)][ValueLen(4B)][Value(variable)]...

[Bloom Filter: Optional, ~10 bits per key]
  Probabilistic structure for quick negative lookups

[Footer: 32 bytes]
  IndexOffset:  8 bytes
  IndexSize:    4 bytes
  BloomOffset:  8 bytes
  BloomSize:    4 bytes
  Checksum:     8 bytes
```

## Read Implementation

### Core Reader Structure
```go
type SSTReader struct {
    file        *os.File
    header      SSTHeader
    index       *IndexBlock
    bloomFilter *BloomFilter
    cache       *BlockCache  // LRU cache for hot blocks
}

type SSTHeader struct {
    Magic       [8]byte
    Version     uint32
    NumEntries  uint32
    MinKey      [32]byte
    MaxKey      [32]byte
}

type IndexBlock struct {
    entries []IndexEntry
}

type IndexEntry struct {
    Key         [32]byte
    BlockOffset int64
    BlockSize   uint32
}
```

### Read Path - Step by Step

#### Step 1: Open and Load Metadata
```go
func OpenSST(filename string) (*SSTReader, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }

    reader := &SSTReader{
        file:  file,
        cache: NewBlockCache(100), // Cache 100 blocks
    }

    // Read header (64 bytes)
    headerBuf := make([]byte, 64)
    if _, err := file.Read(headerBuf); err != nil {
        return nil, err
    }
    reader.header = parseHeader(headerBuf)

    // Read footer to find index location
    file.Seek(-32, io.SeekEnd)
    footerBuf := make([]byte, 32)
    file.Read(footerBuf)

    indexOffset := binary.BigEndian.Uint64(footerBuf[0:8])
    indexSize := binary.BigEndian.Uint32(footerBuf[8:12])

    // Load index into memory (small, typically <100KB)
    file.Seek(int64(indexOffset), io.SeekStart)
    indexBuf := make([]byte, indexSize)
    file.Read(indexBuf)
    reader.index = parseIndex(indexBuf)

    // Optional: Load bloom filter
    bloomOffset := binary.BigEndian.Uint64(footerBuf[12:20])
    if bloomOffset > 0 {
        reader.loadBloomFilter(bloomOffset)
    }

    return reader, nil
}
```

#### Step 2: Binary Search for Key
```go
func (r *SSTReader) Get(key [32]byte) ([]byte, error) {
    // Quick bounds check
    if bytes.Compare(key[:], r.header.MinKey[:]) < 0 ||
       bytes.Compare(key[:], r.header.MaxKey[:]) > 0 {
        return nil, ErrNotFound
    }

    // Optional: Check bloom filter (fast negative lookup)
    if r.bloomFilter != nil && !r.bloomFilter.MayContain(key) {
        return nil, ErrNotFound
    }

    // Binary search the sparse index to find the right block
    blockIdx := r.findBlock(key)
    if blockIdx < 0 {
        return nil, ErrNotFound
    }

    // Load the block (possibly from cache)
    block := r.loadBlock(blockIdx)

    // Binary search within the block
    return block.binarySearch(key)
}

func (r *SSTReader) findBlock(key [32]byte) int {
    left, right := 0, len(r.index.entries)-1

    for left <= right {
        mid := (left + right) / 2
        cmp := bytes.Compare(key[:], r.index.entries[mid].Key[:])

        if cmp == 0 {
            return mid  // Exact match in index
        } else if cmp < 0 {
            right = mid - 1
        } else {
            left = mid + 1
        }
    }

    // Key would be in block 'right' (if it exists)
    if right >= 0 && right < len(r.index.entries) {
        return right
    }
    return -1
}
```

#### Step 3: Load and Search Block
```go
func (r *SSTReader) loadBlock(idx int) *DataBlock {
    entry := r.index.entries[idx]

    // Check cache first
    if block := r.cache.Get(entry.BlockOffset); block != nil {
        return block
    }

    // Read from disk
    buf := make([]byte, entry.BlockSize)
    r.file.ReadAt(buf, entry.BlockOffset)

    block := parseDataBlock(buf)
    r.cache.Put(entry.BlockOffset, block)
    return block
}

type DataBlock struct {
    entries []BlockEntry
}

type BlockEntry struct {
    Key   [32]byte
    Value []byte
}

func (b *DataBlock) binarySearch(key [32]byte) ([]byte, error) {
    left, right := 0, len(b.entries)-1

    for left <= right {
        mid := (left + right) / 2
        cmp := bytes.Compare(key[:], b.entries[mid].Key[:])

        if cmp == 0 {
            return b.entries[mid].Value, nil
        } else if cmp < 0 {
            right = mid - 1
        } else {
            left = mid + 1
        }
    }

    return nil, ErrNotFound
}
```

## Read Optimizations

### 1. Bloom Filter for Fast Negatives
```go
type BloomFilter struct {
    bits     []byte
    numHash  int
}

func (bf *BloomFilter) MayContain(key [32]byte) bool {
    h1, h2 := hash1(key), hash2(key)

    for i := 0; i < bf.numHash; i++ {
        pos := (h1 + i*h2) % (len(bf.bits) * 8)
        if !bf.getBit(pos) {
            return false  // Definitely not present
        }
    }
    return true  // Maybe present
}
```

### 2. Block Cache for Hot Data
```go
type BlockCache struct {
    cache *lru.Cache
    hits  uint64
    miss  uint64
}

func (c *BlockCache) Get(offset int64) *DataBlock {
    if val, ok := c.cache.Get(offset); ok {
        atomic.AddUint64(&c.hits, 1)
        return val.(*DataBlock)
    }
    atomic.AddUint64(&c.miss, 1)
    return nil
}
```

### 3. Parallel Searches Across Segments
```go
func (db *Database) Get(key [32]byte) ([]byte, error) {
    // Check write buffer first
    if val, found := db.writeBuffer.Get(key); found {
        return val, nil
    }

    // Search segments in parallel
    resultChan := make(chan searchResult, len(db.segments))

    for _, segment := range db.segments {
        go func(sst *SSTReader) {
            val, err := sst.Get(key)
            resultChan <- searchResult{val, err}
        }(segment)
    }

    // Return first successful result
    for i := 0; i < len(db.segments); i++ {
        result := <-resultChan
        if result.err == nil {
            return result.val, nil
        }
    }

    return nil, ErrNotFound
}
```

### 4. Memory-Mapped Files for Zero-Copy Reads
```go
func OpenSSTMapped(filename string) (*SSTReader, error) {
    file, _ := os.Open(filename)
    stat, _ := file.Stat()

    // Memory map the entire file
    data, err := syscall.Mmap(
        int(file.Fd()),
        0,
        int(stat.Size()),
        syscall.PROT_READ,
        syscall.MAP_SHARED,
    )

    return &SSTReader{
        data: data,  // Direct memory access, no copying
    }, nil
}

// Reading is just pointer arithmetic
func (r *SSTReader) readAt(offset int64, size int) []byte {
    return r.data[offset : offset+int64(size)]
}
```

## Performance Characteristics

### Single SST Read
- **Best case** (cached): ~100ns
- **Average case** (1 disk seek): ~10µs
- **Worst case** (cold read): ~100µs

### Multiple Segments
With 10 segments:
- **With bloom filters**: Check 1-2 segments average
- **Without bloom filters**: Check all 10 segments
- **With parallel search**: Latency of slowest segment

### Optimizations Impact
| Optimization | Improvement | Use Case |
|--------------|------------|----------|
| Bloom Filter | 10-100x for negatives | Keys not in DB |
| Block Cache | 100x for hot data | Repeated reads |
| Parallel Search | Nx speedup | Multiple segments |
| Memory Mapping | 2-5x | Large files |

## Example: Complete Read Path

```go
// User calls Get
value, err := db.Get(key)

// 1. Check write buffer (100ns)
if found in memory:
    return immediately

// 2. Check segments newest to oldest
for each segment:
    // 2a. Bounds check (5ns)
    if key outside [minKey, maxKey]:
        skip

    // 2b. Bloom filter check (50ns)
    if bloom says "definitely not here":
        skip

    // 2c. Binary search index (200ns)
    find block containing key

    // 2d. Load block (10µs if not cached)
    read 4KB from disk

    // 2e. Binary search block (500ns)
    find exact key-value pair

    // 2f. Return value
    if found:
        return value

return "not found"
```

## Total Read Time
- **Hot path** (in cache): ~1µs
- **Warm path** (bloom filter hit): ~10µs
- **Cold path** (disk read): ~100µs
- **Not found** (with bloom): ~500ns per segment

This is 100-1000x faster than the current HistoryFile implementation!