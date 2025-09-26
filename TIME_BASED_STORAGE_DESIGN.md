# Time-Based Value Storage Architecture

## Core Concept

Replace the current KV approach with:
1. **Time-based value files** (12-hour blocks) for immutable blockchain data
2. **Key index** pointing to (file_id, offset, length)
3. **Sharded dynamic storage** by key ranges for mutable state
4. **Full iteration support** for database reconstruction

## Architecture Overview

```
BlockchainDB/
├── values/                     # Time-based value storage
│   ├── 2024/
│   │   ├── 01/                # January
│   │   │   ├── v_20240101_00.dat  # Jan 1, 00:00-12:00
│   │   │   ├── v_20240101_12.dat  # Jan 1, 12:00-24:00
│   │   │   └── ...
│   │   └── 12/                # December
│   └── metadata/
│       └── value_files.json   # Registry of all value files
│
├── keys/                       # Key indices
│   ├── primary.idx            # Main key -> (file, offset, length) index
│   ├── bloom.filter           # Bloom filter for existence checks
│   └── checkpoints/           # Periodic index snapshots
│
└── dynamic/                    # Sharded mutable state
    ├── shard_00/              # Keys 0x00...
    ├── shard_01/              # Keys 0x01...
    └── config.json            # Sharding configuration
```

## 1. Time-Based Value Files

### File Format
```go
// ValueFile represents a 12-hour block of immutable values
type ValueFile struct {
    Header    ValueFileHeader
    Entries   []ValueEntry
    Index     ValueFileIndex  // For fast iteration
}

type ValueFileHeader struct {
    Magic       [4]byte    // "VBLK"
    Version     uint32
    StartTime   int64      // Unix timestamp of first entry
    EndTime     int64      // Unix timestamp of last entry
    EntryCount  uint32     // Number of values
    IndexOffset uint64     // Where the index section starts
    Checksum    [32]byte   // SHA256 of all entries
}

type ValueEntry struct {
    Timestamp  int64      // When this value was written
    Length     uint32     // Length of value data
    Key        [32]byte   // Hash of the value (for verification)
    Value      []byte     // Actual value data
}

type ValueFileIndex struct {
    // For efficient iteration and rebuilding
    KeyOffsets map[[32]byte]uint64  // Key -> file offset
    TimeIndex  []TimeEntry          // Sorted by timestamp
}

type TimeEntry struct {
    Timestamp int64
    Offset    uint64
    Key       [32]byte
}
```

### Implementation
```go
package blockchainDB

import (
    "encoding/binary"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
)

type TimeBasedValueStore struct {
    baseDir        string
    currentFile    *ValueFileWriter
    fileMutex      sync.RWMutex
    rotateInterval time.Duration

    // File registry
    fileRegistry   map[string]*ValueFileInfo  // filename -> info
    timeIndex      []TimeRange                // Sorted time ranges
}

type ValueFileWriter struct {
    file       *os.File
    header     ValueFileHeader
    entries    []ValueEntry
    keyIndex   map[[32]byte]uint64
    timeIndex  []TimeEntry
    mutex      sync.Mutex
}

type TimeRange struct {
    Filename   string
    StartTime  int64
    EndTime    int64
}

func NewTimeBasedValueStore(baseDir string) (*TimeBasedValueStore, error) {
    store := &TimeBasedValueStore{
        baseDir:        baseDir,
        rotateInterval: 12 * time.Hour,
        fileRegistry:   make(map[string]*ValueFileInfo),
    }

    // Start rotation goroutine
    go store.rotationWorker()

    return store, nil
}

func (s *TimeBasedValueStore) Put(key [32]byte, value []byte) error {
    s.fileMutex.RLock()
    writer := s.currentFile
    s.fileMutex.RUnlock()

    if writer == nil {
        s.rotateFile()
        writer = s.currentFile
    }

    return writer.Append(key, value)
}

func (w *ValueFileWriter) Append(key [32]byte, value []byte) error {
    w.mutex.Lock()
    defer w.mutex.Unlock()

    entry := ValueEntry{
        Timestamp: time.Now().Unix(),
        Length:    uint32(len(value)),
        Key:       key,
        Value:     value,
    }

    // Write to file
    offset := w.getCurrentOffset()
    if err := w.writeEntry(entry); err != nil {
        return err
    }

    // Update indices
    w.keyIndex[key] = offset
    w.timeIndex = append(w.timeIndex, TimeEntry{
        Timestamp: entry.Timestamp,
        Offset:    offset,
        Key:       key,
    })

    w.header.EntryCount++
    return nil
}

func (s *TimeBasedValueStore) rotationWorker() {
    ticker := time.NewTicker(s.rotateInterval)
    defer ticker.Stop()

    for range ticker.C {
        s.rotateFile()
    }
}

func (s *TimeBasedValueStore) rotateFile() error {
    s.fileMutex.Lock()
    defer s.fileMutex.Unlock()

    // Close current file
    if s.currentFile != nil {
        s.currentFile.Close()
    }

    // Create new file with timestamp
    now := time.Now()
    filename := s.generateFilename(now)

    newFile, err := s.createValueFile(filename)
    if err != nil {
        return err
    }

    s.currentFile = newFile

    // Update registry
    s.fileRegistry[filename] = &ValueFileInfo{
        Filename:  filename,
        StartTime: now.Unix(),
        EndTime:   now.Add(s.rotateInterval).Unix(),
    }

    return nil
}

func (s *TimeBasedValueStore) generateFilename(t time.Time) string {
    // Format: values/2024/01/v_20240101_00.dat (for 00:00-12:00)
    // Format: values/2024/01/v_20240101_12.dat (for 12:00-24:00)
    hour := 0
    if t.Hour() >= 12 {
        hour = 12
    }

    return filepath.Join(
        s.baseDir,
        "values",
        fmt.Sprintf("%04d", t.Year()),
        fmt.Sprintf("%02d", t.Month()),
        fmt.Sprintf("v_%04d%02d%02d_%02d.dat",
            t.Year(), t.Month(), t.Day(), hour),
    )
}
```

## 2. Key Index Structure

```go
// KeyIndex maps keys to their location in value files
type KeyIndex struct {
    indexFile     *os.File
    bloomFilter   *BloomFilter
    memIndex      map[[32]byte]*KeyLocation  // Recent keys in memory
    diskIndex     *BTree                      // Persistent B-tree on disk

    mu sync.RWMutex
}

type KeyLocation struct {
    FileID    uint32   // Which value file (by sequence number)
    Offset    uint64   // Offset within that file
    Length    uint32   // Length of value
    Timestamp int64    // When it was written
}

func (ki *KeyIndex) Put(key [32]byte, loc KeyLocation) error {
    ki.mu.Lock()
    defer ki.mu.Unlock()

    // Update bloom filter
    ki.bloomFilter.Add(key)

    // Update memory index
    ki.memIndex[key] = &loc

    // Schedule disk write
    return ki.scheduleFlush()
}

func (ki *KeyIndex) Get(key [32]byte) (*KeyLocation, error) {
    // Check bloom filter first
    if !ki.bloomFilter.Test(key) {
        return nil, ErrNotFound
    }

    ki.mu.RLock()
    defer ki.mu.RUnlock()

    // Check memory index
    if loc, ok := ki.memIndex[key]; ok {
        return loc, nil
    }

    // Check disk index
    return ki.diskIndex.Get(key)
}
```

## 3. Iteration and Rebuild Support

```go
// ValueFileIterator allows iteration over a value file
type ValueFileIterator struct {
    file      *os.File
    header    ValueFileHeader
    current   int
    timeIndex []TimeEntry
}

func (s *TimeBasedValueStore) IterateTimeRange(start, end int64) <-chan ValueEntry {
    ch := make(chan ValueEntry, 100)

    go func() {
        defer close(ch)

        // Find relevant files
        files := s.findFilesInRange(start, end)

        for _, filename := range files {
            iter, err := s.openIterator(filename)
            if err != nil {
                continue
            }

            for iter.Next() {
                entry := iter.Value()
                if entry.Timestamp >= start && entry.Timestamp <= end {
                    ch <- entry
                }
            }

            iter.Close()
        }
    }()

    return ch
}

// RebuildKeyIndex reconstructs the entire key index from value files
func (s *TimeBasedValueStore) RebuildKeyIndex() error {
    newIndex := NewKeyIndex()

    // Iterate through all value files in chronological order
    for _, timeRange := range s.timeIndex {
        iter, err := s.openIterator(timeRange.Filename)
        if err != nil {
            return err
        }

        fileID := s.getFileID(timeRange.Filename)

        for iter.Next() {
            entry := iter.Value()

            // Verify key matches hash of value
            computedKey := sha256.Sum256(entry.Value)
            if computedKey != entry.Key {
                return fmt.Errorf("key mismatch in %s", timeRange.Filename)
            }

            // Add to new index
            newIndex.Put(entry.Key, KeyLocation{
                FileID:    fileID,
                Offset:    iter.Offset(),
                Length:    entry.Length,
                Timestamp: entry.Timestamp,
            })
        }

        iter.Close()
    }

    // Atomic swap
    s.keyIndex = newIndex
    return nil
}
```

## 4. Dynamic Storage Sharding

```go
// DynamicStorage handles mutable state with key-based sharding
type DynamicStorage struct {
    shards     []*Shard
    numShards  int
    shardBits  int  // How many bits of key to use for sharding
}

type Shard struct {
    id         int
    directory  string
    kvStore    *CompactableKV  // Can be compressed
    keyRange   KeyRange
    mu         sync.RWMutex
}

type KeyRange struct {
    Start [32]byte
    End   [32]byte
}

func NewDynamicStorage(baseDir string, numShards int) *DynamicStorage {
    ds := &DynamicStorage{
        numShards: numShards,
        shardBits: int(math.Log2(float64(numShards))),
        shards:    make([]*Shard, numShards),
    }

    for i := 0; i < numShards; i++ {
        ds.shards[i] = &Shard{
            id:        i,
            directory: filepath.Join(baseDir, fmt.Sprintf("shard_%02x", i)),
            keyRange:  ds.calculateKeyRange(i),
            kvStore:   NewCompactableKV(),
        }
    }

    return ds
}

func (ds *DynamicStorage) getShardIndex(key [32]byte) int {
    // Use first N bits of key for shard selection
    return int(key[0]) % ds.numShards
}

func (ds *DynamicStorage) Put(key [32]byte, value []byte) error {
    shard := ds.shards[ds.getShardIndex(key)]
    return shard.Put(key, value)
}

func (s *Shard) Compact() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // Create new compacted file
    newFile := s.directory + ".compact"

    // Write all current key-values sorted
    err := s.kvStore.WriteCompacted(newFile)
    if err != nil {
        return err
    }

    // Atomic swap
    return s.kvStore.SwapToCompacted(newFile)
}
```

## 5. System Integration

```go
// BlockchainDB combines time-based and dynamic storage
type BlockchainDB struct {
    // Immutable blockchain data (content-addressed)
    timeStore    *TimeBasedValueStore
    keyIndex     *KeyIndex

    // Mutable state data
    dynamicStore *DynamicStorage

    // Configuration
    config       Config
}

type Config struct {
    DataDir           string
    ValueRotateHours  int      // Default: 12
    DynamicShards     int      // Default: 256
    CompactInterval   Duration // How often to compact dynamic shards
}

func (db *BlockchainDB) Put(key [32]byte, value []byte, mutable bool) error {
    if mutable {
        // Mutable state goes to dynamic storage
        return db.dynamicStore.Put(key, value)
    } else {
        // Immutable blockchain data goes to time-based storage
        err := db.timeStore.Put(key, value)
        if err != nil {
            return err
        }

        // Update key index
        loc := KeyLocation{
            FileID:    db.timeStore.getCurrentFileID(),
            Offset:    db.timeStore.getLastOffset(),
            Length:    uint32(len(value)),
            Timestamp: time.Now().Unix(),
        }

        return db.keyIndex.Put(key, loc)
    }
}

func (db *BlockchainDB) Get(key [32]byte) ([]byte, error) {
    // Check dynamic storage first (more recent)
    if value, err := db.dynamicStore.Get(key); err == nil {
        return value, nil
    }

    // Check time-based storage
    loc, err := db.keyIndex.Get(key)
    if err != nil {
        return nil, err
    }

    return db.timeStore.ReadAt(loc.FileID, loc.Offset, loc.Length)
}
```

## Advantages

### 1. Time-Based Organization
- **730 files/year** - manageable and archiveable
- **Natural chronological ordering** - perfect for blockchain
- **Easy archival** - old files can be moved to cold storage
- **Parallel access** - different files can be read simultaneously

### 2. Rebuild Capability
- **Self-contained values** - each file has all data needed
- **Key verification** - hash(value) = key for blockchain data
- **Index reconstruction** - can rebuild from value files alone
- **Disaster recovery** - only value files needed for full restore

### 3. Efficient Sharding
- **Key-based sharding** - predictable distribution
- **Independent compaction** - each shard compacts separately
- **Parallel operations** - different shards accessed concurrently
- **Scalable** - can increase shards as needed

### 4. Performance Benefits
- **Sequential writes** - append-only to current file
- **No key-value coupling** - keys and values stored separately
- **Efficient iteration** - time-ordered traversal
- **Reduced I/O** - batch operations per file

## Migration Path

### Phase 1: Implement Time-Based Storage
1. Create ValueFile format and writer
2. Implement 12-hour rotation
3. Add iteration support

### Phase 2: Key Index
1. Implement in-memory index with periodic flush
2. Add bloom filter
3. Create rebuild functionality

### Phase 3: Dynamic Sharding
1. Implement key-based shard selection
2. Add compaction per shard
3. Optimize for concurrent access

### Phase 4: Integration
1. Combine both storage systems
2. Add routing logic
3. Implement migration tools

## File Formats

### Value File Naming
```
v_YYYYMMDD_HH.dat
- v_20240101_00.dat (Jan 1, 2024, 00:00-12:00)
- v_20240101_12.dat (Jan 1, 2024, 12:00-24:00)
```

### Shard Naming
```
shard_XX/
- shard_00/ (keys starting with 0x00)
- shard_ff/ (keys starting with 0xff)
```

## Metrics to Track

1. **Files per time period** - Should be ~60-61 per month
2. **Average file size** - For capacity planning
3. **Compression ratio** - For dynamic shards
4. **Rebuild time** - Key index reconstruction speed
5. **Shard balance** - Key distribution across shards

This architecture provides clear separation of concerns, efficient access patterns, and full rebuild capability while maintaining the blockchain requirement that keys are hashes of values.