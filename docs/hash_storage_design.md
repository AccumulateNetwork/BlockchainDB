# Hash-Based Storage Design for BlockchainDB

## Executive Summary

This document outlines the design for implementing LevelDB-compatible APIs while organizing all database entries by their content hash in persistent storage. This approach provides deduplication, integrity verification, and efficient content-addressed storage ideal for blockchain applications.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    LevelDB Compatible API                   │
│  Put(key,value) | Get(key) | Delete(key) | Iterator | Batch │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Key-Hash Mapping Layer                   │
│        Maintains bidirectional key ↔ hash mappings          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Hash-Based Storage Engine                  │
│     ┌──────────┐    ┌──────────┐    ┌──────────┐            │
│     │   Hash   │    │  Value   │    │  Index   │            │
│     │   Index  │───▶│  Store   │───▶│  Cache   │            │
│     └──────────┘    └──────────┘    └──────────┘            │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Hash-Based Value Store

```go
// Primary storage interface for hash-organized data
type HashStore struct {
    // Content-addressed storage
    values    map[[32]byte][]byte  // Hash -> Value mapping
    refCount  map[[32]byte]int     // Reference counting for deduplication

    // Persistence layer
    vFile     *BFile               // Value file
    hIndex    *HistoryFile         // Hash index with offsets
}

// Store value by its hash
func (h *HashStore) StoreByHash(value []byte) ([32]byte, error) {
    hash := sha256.Sum256(value)

    // Check if value already exists (deduplication)
    if h.refCount[hash] > 0 {
        h.refCount[hash]++
        return hash, nil
    }

    // Store new value
    offset, err := h.vFile.Write(value)
    if err != nil {
        return [32]byte{}, err
    }

    // Update index
    h.hIndex.Put(hash, offset, len(value))
    h.refCount[hash] = 1

    return hash, nil
}
```

### 2. Key-Hash Mapping Layer

```go
// Maintains mapping between user keys and content hashes
type KeyHashMapper struct {
    // Primary mappings
    keyToHash map[string][32]byte  // User key -> Content hash
    hashToKeys map[[32]byte][]string // Content hash -> List of keys

    // Persistence
    kFile *KFile  // Key index file

    // Optimization
    bloom *BloomFilter  // Quick existence checks
}

// Map a key to a hash
func (m *KeyHashMapper) MapKey(key []byte, hash [32]byte) error {
    keyStr := string(key)

    // Update mappings
    oldHash, exists := m.keyToHash[keyStr]
    if exists {
        // Remove old mapping
        m.removeKeyFromHash(keyStr, oldHash)
    }

    m.keyToHash[keyStr] = hash
    m.hashToKeys[hash] = append(m.hashToKeys[hash], keyStr)

    // Update bloom filter
    m.bloom.Add(key)

    // Persist mapping
    return m.kFile.PutMapping(key, hash)
}
```

### 3. LevelDB Compatible Interface

```go
// Full LevelDB-compatible database
type LevelDBCompat struct {
    hashStore *HashStore
    mapper    *KeyHashMapper

    // Transaction support
    currentBatch *WriteBatch

    // Snapshot support
    snapshots []*Snapshot
}

// Put operation - LevelDB compatible
func (db *LevelDBCompat) Put(key, value []byte) error {
    // Store value by hash
    hash, err := db.hashStore.StoreByHash(value)
    if err != nil {
        return err
    }

    // Map key to hash
    return db.mapper.MapKey(key, hash)
}

// Get operation - LevelDB compatible
func (db *LevelDBCompat) Get(key []byte) ([]byte, error) {
    // Look up hash for key
    hash, exists := db.mapper.GetHash(key)
    if !exists {
        return nil, ErrNotFound
    }

    // Retrieve value by hash
    return db.hashStore.GetByHash(hash)
}

// Delete operation - LevelDB compatible
func (db *LevelDBCompat) Delete(key []byte) error {
    // Get hash for key
    hash, exists := db.mapper.GetHash(key)
    if !exists {
        return nil // LevelDB ignores missing keys on delete
    }

    // Remove key mapping
    db.mapper.RemoveKey(key)

    // Decrease reference count
    db.hashStore.DecRef(hash)

    return nil
}
```

## Storage Layout

### Directory Structure
```
blockchaindb/
├── keys/
│   ├── mappings.kf     # Key-to-hash mappings
│   ├── bloom.bf        # Bloom filter for key existence
│   └── index.idx       # Sorted key index for iteration
├── values/
│   ├── content.vf      # Actual value data
│   ├── hashes.hf       # Hash-to-offset index
│   └── refs.rf         # Reference counts
├── snapshots/
│   └── snap_*.dat      # Snapshot files
└── manifest.json       # Database metadata
```

### File Formats

#### Hash Index File (hashes.hf)
```
[Header: 64 bytes]
  - Magic: "HASHIDX1" (8 bytes)
  - Version: uint32
  - Entry count: uint64
  - Checksum: [32]byte

[Entries: 48 bytes each]
  - Hash: [32]byte
  - Offset: uint64 (in content.vf)
  - Length: uint64
```

#### Key Mapping File (mappings.kf)
```
[Header: 64 bytes]
  - Magic: "KEYMAP01" (8 bytes)
  - Version: uint32
  - Entry count: uint64

[Entries: Variable length]
  - Key length: uint32
  - Key: []byte
  - Hash: [32]byte
```

## Iterator Implementation

```go
type HashIterator struct {
    db        *LevelDBCompat
    keys      []string  // Sorted key list
    position  int
    snapshot  *Snapshot // Optional snapshot
}

func (it *HashIterator) Seek(target []byte) {
    // Binary search in sorted keys
    it.position = sort.Search(len(it.keys), func(i int) bool {
        return bytes.Compare([]byte(it.keys[i]), target) >= 0
    })
}

func (it *HashIterator) Next() {
    if it.Valid() {
        it.position++
    }
}

func (it *HashIterator) Key() []byte {
    if !it.Valid() {
        return nil
    }
    return []byte(it.keys[it.position])
}

func (it *HashIterator) Value() []byte {
    if !it.Valid() {
        return nil
    }
    key := it.Key()
    value, _ := it.db.Get(key)
    return value
}
```

## Batch Operations

```go
type WriteBatch struct {
    ops []BatchOp
}

type BatchOp struct {
    Type  OpType // Put or Delete
    Key   []byte
    Value []byte // Only for Put
}

func (db *LevelDBCompat) Write(batch *WriteBatch) error {
    // Pre-compute all hashes
    hashes := make([][32]byte, 0, len(batch.ops))
    for _, op := range batch.ops {
        if op.Type == OpPut {
            hash := sha256.Sum256(op.Value)
            hashes = append(hashes, hash)
        }
    }

    // Atomic write
    tx := db.BeginTransaction()
    defer tx.Rollback()

    for i, op := range batch.ops {
        switch op.Type {
        case OpPut:
            if err := tx.PutWithHash(op.Key, op.Value, hashes[i]); err != nil {
                return err
            }
        case OpDelete:
            if err := tx.Delete(op.Key); err != nil {
                return err
            }
        }
    }

    return tx.Commit()
}
```

## Performance Optimizations

### 1. Caching Strategy
- **Hash Cache**: LRU cache of recent hash lookups
- **Value Cache**: Frequently accessed values kept in memory
- **Mapping Cache**: Hot key-to-hash mappings

### 2. Bloom Filters
- Quick negative lookups for non-existent keys
- Reduces disk I/O for Get operations

### 3. Batch Processing
- Group writes to minimize disk syncs
- Parallel hash computation for batch operations

### 4. Compression
- Values compressed before storage
- Compression algorithm selected based on value size

## Migration Strategy

### Phase 1: Wrapper Implementation
```go
// Wrap existing BlockchainDB with LevelDB interface
type LevelDBWrapper struct {
    kv2 *KV2
}

func (w *LevelDBWrapper) Put(key, value []byte) error {
    var k [32]byte
    copy(k[:], key[:32])
    return w.kv2.Put(k, value)
}
```

### Phase 2: Native Hash Storage
- Implement HashStore component
- Add key-hash mapping layer
- Maintain backward compatibility

### Phase 3: Full LevelDB Parity
- Complete iterator implementation
- Add snapshot isolation
- Implement all LevelDB options

## Benefits

1. **Deduplication**: Identical values stored once
2. **Integrity**: Automatic verification via hashes
3. **Efficiency**: Content-addressed storage
4. **Immutability**: Perfect for blockchain data
5. **Compatibility**: Full LevelDB API support

## Testing Plan

### Unit Tests
- Test each component in isolation
- Verify hash computation and storage
- Test key-hash mapping operations

### Integration Tests
- Test full LevelDB API compliance
- Verify transaction atomicity
- Test snapshot isolation

### Performance Tests
- Benchmark vs standard LevelDB
- Measure deduplication savings
- Test iterator performance

## Implementation Timeline

- **Week 1-2**: Implement HashStore component
- **Week 3-4**: Build KeyHashMapper layer
- **Week 5-6**: Add LevelDB compatible interface
- **Week 7-8**: Implement iterators and batch operations
- **Week 9-10**: Add snapshot support
- **Week 11-12**: Testing and optimization

## Conclusion

This hash-based storage design provides a robust foundation for implementing LevelDB-compatible APIs while leveraging content-addressed storage for improved efficiency, deduplication, and integrity verification. The design maintains full compatibility with existing LevelDB applications while providing blockchain-optimized storage underneath.