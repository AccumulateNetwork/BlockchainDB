# DKV Locality-Based Value Storage Design

## Core Insight

Instead of organizing value files by hash (which randomly distributes related data), organize them by URL tree structure to achieve:
- **Locality of access**: Related pages stored together
- **Better caching**: OS page cache more effective
- **Efficient compaction**: Can compact one domain at a time
- **Improved performance**: Sequential disk access for related items

## Current Problem

```go
// Hash-based storage scatters related data
hash("example.com/page1") = 0x3f... → valueFile_017.dat @ offset 12345
hash("example.com/page2") = 0x8b... → valueFile_092.dat @ offset 67890
hash("example.com/page3") = 0x1a... → valueFile_003.dat @ offset 54321

// Reading all example.com pages requires random I/O across 3 files
```

## Proposed Design

### 1. Three-Layer Architecture

```go
type LocalityDKV struct {
    // Layer 1: Hash index (small, in memory)
    hashIndex  *TreeDKV  // hash → ValuePointer

    // Layer 2: URL index (sorted, on SSD)
    urlIndex   *TreeDKV  // url → hash

    // Layer 3: Value storage (organized by URL hierarchy)
    valueStore *HierarchicalValueStore
}

type ValuePointer struct {
    SegmentID  uint32  // Which segment file
    Offset     uint64  // Offset within segment
    Size       uint32  // Size of value
    Generation uint32  // For garbage collection
}
```

### 2. Hierarchical Value Storage

```
values/
├── segment_map.json          # Segment metadata
├── com/
│   ├── example/
│   │   ├── segment_0001.val  # All example.com data together
│   │   ├── segment_0002.val  # When 0001 fills up
│   │   └── index.json         # Local index for this domain
│   ├── google/
│   │   └── segment_0001.val
│   └── facebook/
│       └── segment_0001.val
├── org/
│   └── wikipedia/
│       ├── segment_0001.val
│       ├── segment_0002.val
│       └── segment_0003.val  # Wikipedia needs more segments
└── _misc/
    └── segment_0001.val      # Non-standard URLs
```

### 3. Segment Management

```go
type Segment struct {
    Path       string           // e.g., "com/example/segment_0001.val"
    DomainPath string          // e.g., "com.example"
    SegmentNum uint32          // Sequential within domain
    Size       uint64          // Current size
    MaxSize    uint64          // When to create new segment (e.g., 64MB)
    WritePos   uint64          // Next write position

    // Locality tracking
    MinURL     string          // First URL in segment
    MaxURL     string          // Last URL in segment
    URLCount   uint32          // Number of URLs

    // Garbage collection
    LiveBytes  uint64          // Actual live data
    DeadBytes  uint64          // Deleted/updated data
    Generation uint32          // Increment on compaction
}

type SegmentManager struct {
    segments    map[string]*Segment  // SegmentID → Segment
    domainMap   map[string][]string  // Domain → [SegmentIDs]
    activeSegs  map[string]string    // Domain → active SegmentID
    mu          sync.RWMutex
}
```

### 4. Write Path

```go
func (dkv *LocalityDKV) Put(url string, value []byte) error {
    // 1. Calculate hash
    hash := sha256(url)

    // 2. Determine segment based on URL structure
    domain := extractDomain(url)  // e.g., "com.example"
    segment := dkv.getOrCreateSegment(domain)

    // 3. Append to segment file
    pointer := segment.Append(value)

    // 4. Update indices
    dkv.hashIndex.Put(hash, pointer.Serialize())
    dkv.urlIndex.Put(url, hash)

    // 5. Track in segment metadata
    segment.UpdateBounds(url)

    return nil
}

func (sm *SegmentManager) getOrCreateSegment(domain string) *Segment {
    // Check active segment for domain
    if segID := sm.activeSegs[domain]; segID != "" {
        seg := sm.segments[segID]
        if seg.Size < seg.MaxSize {
            return seg
        }
    }

    // Create new segment
    return sm.createNewSegment(domain)
}
```

### 5. Read Path

```go
func (dkv *LocalityDKV) Get(key interface{}) ([]byte, error) {
    var hash [32]byte

    switch k := key.(type) {
    case string:  // URL lookup
        // First get hash from URL index
        hashBytes, _ := dkv.urlIndex.Get(k)
        copy(hash[:], hashBytes)

    case [32]byte:  // Direct hash lookup
        hash = k
    }

    // Get pointer from hash index
    pointerBytes, found := dkv.hashIndex.Get(hash)
    if !found {
        return nil, NotFound
    }

    pointer := ParsePointer(pointerBytes)

    // Read from segment file
    return dkv.valueStore.Read(pointer)
}
```

### 6. Locality-Aware Operations

```go
// Read multiple URLs from same domain - FAST!
func (dkv *LocalityDKV) GetDomainPages(domain string) []Page {
    urls := dkv.urlIndex.Range(domain, domain + "~")

    // All these URLs likely in same segment file
    // OS can read-ahead effectively
    // CPU cache stays warm

    pages := make([]Page, 0, len(urls))
    for _, url := range urls {
        data, _ := dkv.Get(url)  // Sequential reads from same file!
        pages = append(pages, Page{URL: url, Data: data})
    }
    return pages
}
```

### 7. Compaction Strategy

```go
type CompactionStrategy struct {
    // Per-domain compaction (much more efficient!)
    MinDeadRatio   float64  // Compact when 30% dead
    MinSegmentAge  Duration // Don't compact segments < 1 hour old
}

func (dkv *LocalityDKV) CompactDomain(domain string) error {
    segments := dkv.segmentManager.GetDomainSegments(domain)

    // Read all live data (sequential from same files!)
    liveData := []KV{}
    for _, seg := range segments {
        entries := seg.ReadAll()
        for _, e := range entries {
            if dkv.isLive(e.Hash) {
                liveData = append(liveData, e)
            }
        }
    }

    // Write to new segment (sequential writes!)
    newSeg := dkv.segmentManager.CreateCompactedSegment(domain)
    for _, kv := range liveData {
        pointer := newSeg.Append(kv.Value)
        dkv.hashIndex.Put(kv.Hash, pointer)
    }

    // Atomic switch
    dkv.segmentManager.ReplaceSegments(domain, segments, newSeg)

    return nil
}
```

## Key Benefits

### 1. Cache Efficiency
```go
// Reading related pages
example.com/page1  → segment_com_example_0001.val @ 0
example.com/page2  → segment_com_example_0001.val @ 4096
example.com/page3  → segment_com_example_0001.val @ 8192

// OS reads ahead, all pages in cache after first read!
```

### 2. Compaction Efficiency
- Compact one domain at a time
- No need to scan entire value space
- Can prioritize hot domains

### 3. Deletion Efficiency
```go
// Delete entire domain
func (dkv *LocalityDKV) DeleteDomain(domain string) error {
    // Remove segments
    segments := dkv.segmentManager.GetDomainSegments(domain)
    for _, seg := range segments {
        os.Remove(seg.Path)
    }

    // Clean indices
    urls := dkv.urlIndex.Range(domain, domain + "~")
    for _, url := range urls {
        hash := sha256(url)
        dkv.hashIndex.Delete(hash)
        dkv.urlIndex.Delete(url)
    }
}
```

### 4. Backup/Archive Efficiency
```bash
# Can backup specific domains
tar -czf example.com.tar.gz values/com/example/

# Can move cold domains to slower storage
mv values/com/oldsite/ /mnt/cold-storage/
```

## Challenges to Address

### 1. Hash Lookups Need Indirection
```go
// Before: hash → value (1 lookup)
// After:  hash → pointer → value (2 lookups)

// Mitigation: Keep hot pointers in memory cache
type PointerCache struct {
    cache *lru.Cache  // hash → ValuePointer
}
```

### 2. Variable Segment Sizes
```go
// Some domains huge, others tiny
// Solution: Dynamic segment sizing

func (sm *SegmentManager) calculateSegmentSize(domain string) uint64 {
    switch {
    case strings.Contains(domain, "wikipedia"):
        return 1 * GB  // Large segments for huge sites
    case sm.isPopularDomain(domain):
        return 256 * MB
    default:
        return 64 * MB  // Default size
    }
}
```

### 3. URL Pattern Variations
```go
// Not all keys are URLs
type KeyClassifier struct {
    patterns []Pattern
}

func (kc *KeyClassifier) Classify(key string) string {
    // URLs: organize by domain
    if url, err := url.Parse(key); err == nil {
        return domainToPath(url.Host)
    }

    // Paths: organize by directory
    if strings.HasPrefix(key, "/") {
        return pathToSegment(key)
    }

    // Timestamps: organize by date
    if ts, err := parseTimestamp(key); err == nil {
        return timeToSegment(ts)
    }

    // Default: misc bucket
    return "_misc"
}
```

### 4. Migration from Hash-Based Storage
```go
func MigrateToLocalityStorage(old *DKV, new *LocalityDKV) error {
    iter := old.urlIndex.NewIterator()

    batch := []KV{}
    currentDomain := ""

    for iter.Next() {
        url := iter.Key()
        hash := iter.Value()

        domain := extractDomain(url)
        if domain != currentDomain {
            // Process batch for previous domain
            new.BatchPutDomain(currentDomain, batch)
            batch = []KV{}
            currentDomain = domain
        }

        value := old.GetByHash(hash)
        batch = append(batch, KV{URL: url, Value: value})
    }

    // Final batch
    new.BatchPutDomain(currentDomain, batch)
}
```

## Performance Projections

### Sequential vs Random I/O
```
Random (Hash-based):
- 100 pages from example.com
- 100 random seeks across value space
- ~10ms per seek = 1000ms total

Sequential (URL-based):
- 100 pages from example.com
- 1 seek + sequential reads
- ~10ms + 1ms = 11ms total

Improvement: ~100x for domain-scoped operations
```

### Cache Hit Rates
```
Hash-based:
- Random distribution
- Cache hit rate: ~10% (depends on total cache size)

URL-based:
- Locality of reference
- Cache hit rate: ~80% (hot domains stay cached)

Improvement: 8x cache efficiency
```

## Implementation Phases

### Phase 1: Basic Structure
- Implement SegmentManager
- URL-to-segment mapping
- Basic append/read operations

### Phase 2: Indices Integration
- Update hash index to use pointers
- Maintain consistency between indices
- Pointer cache implementation

### Phase 3: Compaction
- Per-domain compaction
- Dead space tracking
- Generation-based GC

### Phase 4: Advanced Features
- Dynamic segment sizing
- Tiered storage support
- Domain-level operations

## Questions for Review

1. **Segment size strategy**: Fixed vs dynamic vs adaptive?
2. **Directory depth**: How deep should domain paths go?
3. **Non-URL keys**: How to handle arbitrary keys?
4. **Consistency**: How to ensure atomic updates across indices?
5. **Recovery**: How to handle partial writes/crashes?
6. **Migration**: Support live migration from hash-based?