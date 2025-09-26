# Implementation Plan: LevelDB API Compatibility for BlockchainDB

## Executive Summary

This document outlines the practical implementation plan to add LevelDB API compatibility to the existing BlockchainDB while maintaining its hash-based storage architecture.

## Current State Analysis

### Existing BlockchainDB Strengths
- ✅ Efficient hash-based storage with fixed [32]byte keys
- ✅ Two-layer architecture (Permanent/Dynamic)
- ✅ Sharding support
- ✅ History tracking
- ✅ Basic view/snapshot functionality

### Critical Gaps for LevelDB Compatibility
1. **No variable-length key support** (LevelDB uses []byte, we use [32]byte)
2. **No Delete operation**
3. **No Iterator/range queries**
4. **No batch operations**
5. **No explicit snapshot management**

## Proposed Architecture

```
┌──────────────────────────────────────────────────┐
│          LevelDB API Compatibility Layer          │
│                 (New Component)                   │
├──────────────────────────────────────────────────┤
│ • Variable key → Fixed key mapping                │
│ • Delete operation with tombstones                │
│ • Iterator implementation                         │
│ • Batch operation support                         │
│ • Snapshot management                             │
└──────────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────┐
│         Existing BlockchainDB Components          │
├──────────────────────────────────────────────────┤
│ • KV2 (Two-layer storage)                         │
│ • KVShard (Sharding)                              │
│ • KFile/BFile (Storage)                           │
│ • HistoryFile (Versioning)                        │
└──────────────────────────────────────────────────┘
```

## Implementation Phases

### Phase 1: Minimal Viable Compatibility (Week 1)

Create `leveldb_wrapper.go`:

```go
package blockchainDB

import (
    "crypto/sha256"
    "errors"
)

// LevelDB wraps BlockchainDB with LevelDB-compatible API
type LevelDB struct {
    db        *KV2
    keyMap    map[string][32]byte  // Maps variable keys to fixed keys
    deleted   map[string]bool      // Tombstones for deleted keys
    sortedKeys []string             // For iteration
}

// Open creates a LevelDB-compatible database
func OpenLevelDB(path string) (*LevelDB, error) {
    kv2, err := OpenKV2(path)
    if err != nil {
        return nil, err
    }

    return &LevelDB{
        db:        kv2,
        keyMap:    make(map[string][32]byte),
        deleted:   make(map[string]bool),
        sortedKeys: make([]string, 0),
    }, nil
}

// Put stores a key-value pair (LevelDB compatible)
func (l *LevelDB) Put(key, value []byte) error {
    // Convert variable-length key to fixed [32]byte
    fixedKey := l.makeFixedKey(key)

    // Store mapping
    keyStr := string(key)
    l.keyMap[keyStr] = fixedKey
    l.deleted[keyStr] = false

    // Update sorted keys for iteration
    l.updateSortedKeys(keyStr)

    // Store in underlying DB
    _, err := l.db.Put(fixedKey, value)
    return err
}

// Get retrieves a value (LevelDB compatible)
func (l *LevelDB) Get(key []byte) ([]byte, error) {
    keyStr := string(key)

    // Check if deleted
    if l.deleted[keyStr] {
        return nil, errors.New("not found")
    }

    // Get fixed key
    fixedKey, exists := l.keyMap[keyStr]
    if !exists {
        return nil, errors.New("not found")
    }

    // Retrieve from underlying DB
    return l.db.Get(fixedKey)
}

// Delete removes a key (LevelDB compatible)
func (l *LevelDB) Delete(key []byte) error {
    keyStr := string(key)
    l.deleted[keyStr] = true
    return nil
}

func (l *LevelDB) makeFixedKey(key []byte) [32]byte {
    if len(key) <= 32 {
        var fixed [32]byte
        copy(fixed[:], key)
        return fixed
    }
    return sha256.Sum256(key)
}
```

### Phase 2: Iterator Support (Week 2)

Add to `leveldb_wrapper.go`:

```go
// Iterator provides ordered traversal
type Iterator struct {
    db       *LevelDB
    keys     []string
    position int
}

// NewIterator creates an iterator
func (l *LevelDB) NewIterator() *Iterator {
    return &Iterator{
        db:       l,
        keys:     l.sortedKeys,
        position: 0,
    }
}

// Seek positions the iterator
func (it *Iterator) Seek(key []byte) {
    target := string(key)
    // Binary search for position
    it.position = sort.SearchStrings(it.keys, target)
}

// Next moves to next key
func (it *Iterator) Next() {
    if it.Valid() {
        it.position++
    }
}

// Key returns current key
func (it *Iterator) Key() []byte {
    if !it.Valid() {
        return nil
    }
    return []byte(it.keys[it.position])
}

// Value returns current value
func (it *Iterator) Value() []byte {
    if !it.Valid() {
        return nil
    }
    val, _ := it.db.Get(it.Key())
    return val
}

// Valid checks if position is valid
func (it *Iterator) Valid() bool {
    return it.position >= 0 && it.position < len(it.keys)
}
```

### Phase 3: Batch Operations (Week 3)

```go
// WriteBatch accumulates operations
type WriteBatch struct {
    ops []operation
}

type operation struct {
    typ   opType
    key   []byte
    value []byte
}

type opType int

const (
    opPut opType = iota
    opDelete
)

// NewWriteBatch creates a batch
func NewWriteBatch() *WriteBatch {
    return &WriteBatch{}
}

// Put adds a put to the batch
func (b *WriteBatch) Put(key, value []byte) {
    b.ops = append(b.ops, operation{
        typ:   opPut,
        key:   key,
        value: value,
    })
}

// Delete adds a delete to the batch
func (b *WriteBatch) Delete(key []byte) {
    b.ops = append(b.ops, operation{
        typ: opDelete,
        key: key,
    })
}

// Write executes the batch atomically
func (l *LevelDB) Write(batch *WriteBatch) error {
    // TODO: Add transaction support for atomicity
    for _, op := range batch.ops {
        switch op.typ {
        case opPut:
            if err := l.Put(op.key, op.value); err != nil {
                return err
            }
        case opDelete:
            if err := l.Delete(op.key); err != nil {
                return err
            }
        }
    }
    return nil
}
```

### Phase 4: Persistence Layer (Week 4)

Add persistent storage for key mappings and tombstones:

```go
// PersistentLevelDB adds persistence for mappings
type PersistentLevelDB struct {
    *LevelDB
    mappingFile string
    deleteFile  string
}

// SaveMappings persists key mappings to disk
func (p *PersistentLevelDB) SaveMappings() error {
    // Store keyMap to mappingFile
    // Store deleted to deleteFile
    // Implementation details...
}

// LoadMappings loads key mappings from disk
func (p *PersistentLevelDB) LoadMappings() error {
    // Load keyMap from mappingFile
    // Load deleted from deleteFile
    // Rebuild sortedKeys
    // Implementation details...
}
```

## Testing Strategy

### Unit Tests
1. Test each LevelDB API method
2. Test key conversion (variable to fixed)
3. Test iterator operations
4. Test batch atomicity

### Compatibility Tests
```go
func TestLevelDBCompatibility(t *testing.T) {
    db := OpenLevelDB("/tmp/test")

    // Test standard LevelDB operations
    db.Put([]byte("key1"), []byte("value1"))
    val, _ := db.Get([]byte("key1"))
    assert.Equal(t, []byte("value1"), val)

    // Test iteration
    iter := db.NewIterator()
    for iter.Seek([]byte("key")); iter.Valid(); iter.Next() {
        // Process key/value
    }

    // Test batch
    batch := NewWriteBatch()
    batch.Put([]byte("k1"), []byte("v1"))
    batch.Put([]byte("k2"), []byte("v2"))
    batch.Delete([]byte("k1"))
    db.Write(batch)
}
```

### Performance Benchmarks
- Compare with standard LevelDB
- Measure overhead of key mapping
- Test iterator performance
- Benchmark batch operations

## Migration Path

### For Existing BlockchainDB Users
1. No changes needed - existing APIs remain
2. Can optionally adopt LevelDB API

### For LevelDB Users
1. Replace `leveldb.Open()` with `blockchainDB.OpenLevelDB()`
2. All other code remains the same
3. Get benefits of hash-based storage

## Risk Mitigation

### Performance Risks
- **Risk**: Key mapping overhead
- **Mitigation**: Use in-memory cache, lazy loading

### Compatibility Risks
- **Risk**: Subtle LevelDB behavior differences
- **Mitigation**: Extensive testing against LevelDB test suite

### Storage Risks
- **Risk**: Increased storage for mappings
- **Mitigation**: Compress mapping data, periodic cleanup

## Success Metrics

1. **Functional**: Pass LevelDB compatibility test suite
2. **Performance**: < 10% overhead vs native BlockchainDB
3. **Storage**: < 5% additional storage for mappings
4. **Adoption**: Successfully run existing LevelDB applications

## Timeline

| Week | Deliverable | Success Criteria |
|------|-------------|-----------------|
| 1 | Basic wrapper with Put/Get/Delete | Unit tests pass |
| 2 | Iterator implementation | Range queries work |
| 3 | Batch operations | Atomic batches work |
| 4 | Persistence layer | Survives restart |
| 5 | Performance optimization | Meets benchmarks |
| 6 | Documentation & examples | Ready for users |

## Conclusion

This implementation plan provides a practical path to add LevelDB compatibility to BlockchainDB without modifying the core architecture. The wrapper approach allows us to:

1. Maintain backward compatibility
2. Preserve hash-based storage benefits
3. Support LevelDB applications
4. Enable gradual migration

The modular design ensures we can implement and test incrementally, reducing risk and allowing for early feedback.