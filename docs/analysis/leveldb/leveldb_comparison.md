# LevelDB API vs BlockchainDB Comparison

## Overview

This document compares the standard LevelDB API with the BlockchainDB implementation, outlining the path to support all LevelDB APIs while organizing entries by their hash in persistent storage.

## LevelDB Core APIs

### 1. Basic Operations

| LevelDB API | Description | BlockchainDB Current | BlockchainDB Target |
|-------------|-------------|---------------------|-------------------|
| `Put(key, value)` | Write a key-value pair | ✅ `KV.Put()`, `KV2.Put()` | Add batch support |
| `Get(key)` | Read value for a key | ✅ `KV.Get()`, `KV2.Get()` | Maintain compatibility |
| `Delete(key)` | Remove a key-value pair | ❌ Not implemented | Add deletion support |
| `Write(batch)` | Atomic batch operations | ❌ Not implemented | Add batch operations |

### 2. Iteration APIs

| LevelDB API | Description | BlockchainDB Current | BlockchainDB Target |
|-------------|-------------|---------------------|-------------------|
| `NewIterator()` | Create iterator over DB | ❌ Not implemented | Add iterator support |
| `Iterator.Seek(key)` | Position at first key >= target | ❌ Not implemented | Add seek functionality |
| `Iterator.Next()` | Move to next entry | ❌ Not implemented | Implement traversal |
| `Iterator.Prev()` | Move to previous entry | ❌ Not implemented | Implement reverse traversal |
| `Iterator.Key()` | Get current key | ❌ Not implemented | Return current position key |
| `Iterator.Value()` | Get current value | ❌ Not implemented | Return current position value |
| `Iterator.Valid()` | Check if position is valid | ❌ Not implemented | Add validity check |

### 3. Snapshot APIs

| LevelDB API | Description | BlockchainDB Current | BlockchainDB Target |
|-------------|-------------|---------------------|-------------------|
| `GetSnapshot()` | Create consistent read view | ✅ Partial via ViewKV | Full snapshot support |
| `ReleaseSnapshot()` | Release snapshot | ✅ Via ViewKV.Close() | Maintain compatibility |
| `GetApproximateSizes()` | Estimate space usage | ❌ Not implemented | Add size estimation |

### 4. Database Management

| LevelDB API | Description | BlockchainDB Current | BlockchainDB Target |
|-------------|-------------|---------------------|-------------------|
| `Open(options, path)` | Open database | ✅ `OpenKV()`, `OpenKV2()` | Add options support |
| `Close()` | Close database | ✅ `KV.Close()` | Maintain compatibility |
| `DestroyDB(path)` | Delete entire database | ✅ Via `os.RemoveAll()` | Add dedicated method |
| `RepairDB(path)` | Attempt to repair corrupted DB | ❌ Not implemented | Add repair functionality |

### 5. Advanced Features

| LevelDB API | Description | BlockchainDB Current | BlockchainDB Target |
|-------------|-------------|---------------------|-------------------|
| `GetProperty(property)` | Get DB statistics | ❌ Not implemented | Add statistics |
| `CompactRange(start, limit)` | Manual compaction | ✅ Partial via Compress() | Full range compaction |
| `WriteBatch` operations | Batch Put/Delete | ❌ Not implemented | Add batch class |

## BlockchainDB Unique Features

### Current Implementation

1. **Two-Layer Architecture (KV2)**
   - PermKV: Immutable storage with history
   - DynaKV: Mutable storage without history
   - Smart separation of content-addressed and state storage

2. **Hash-Based Organization**
   - KFile: Stores keys with offset/length pointers
   - BFile: Stores actual values
   - HistoryFile: Maintains version history

3. **Sharding Support (KVShard)**
   - Automatic sharding across multiple KV2 instances
   - Hash-based shard selection

### Proposed Hash-Based Storage Design

```go
// Proposed interface to support LevelDB API with hash-based storage
type HashDB interface {
    // LevelDB Compatible Operations
    Put(key []byte, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    Write(batch WriteBatch) error

    // Iterator Support
    NewIterator(options *ReadOptions) Iterator

    // Snapshot Support
    GetSnapshot() *Snapshot
    ReleaseSnapshot(snap *Snapshot)

    // Hash-Based Storage (Internal)
    putByHash(hash [32]byte, value []byte) error
    getByHash(hash [32]byte) ([]byte, error)

    // Content-Addressed Storage
    PutContent(value []byte) ([32]byte, error) // Returns hash as key
    GetContent(hash [32]byte) ([]byte, error)
}
```

## Implementation Plan

### Phase 1: Core API Compatibility
1. Add Delete operation
2. Implement WriteBatch for atomic operations
3. Add basic iterator support

### Phase 2: Hash-Based Reorganization
1. Implement content-addressed storage layer
2. Add hash-based indexing
3. Maintain key-to-hash mapping for LevelDB compatibility

### Phase 3: Advanced Features
1. Add snapshot isolation
2. Implement range queries
3. Add statistics and monitoring

### Phase 4: Optimization
1. Implement efficient range compaction
2. Add bloom filters for faster lookups
3. Optimize iterator performance

## Key Design Decisions

### Hash-Based Storage Benefits
1. **Deduplication**: Identical values stored once
2. **Integrity**: Built-in verification via hashes
3. **Efficiency**: Content-addressed storage reduces redundancy
4. **Immutability**: Perfect for blockchain use cases

### Compatibility Layer
- Maintain key→hash mapping for LevelDB API
- Transparent hash computation for values
- Support both key-based and hash-based access

### Storage Organization
```
BlockchainDB/
├── keys/           # Key-to-hash mappings
│   ├── index.dat   # Key index file
│   └── bloom.dat   # Bloom filter for existence checks
├── values/         # Hash-organized value storage
│   ├── 00/         # First two hex chars of hash
│   │   └── *.dat   # Value files
│   └── ff/
└── meta/           # Metadata and configuration
    ├── manifest    # DB version and schema
    └── stats.json  # Statistics
```

## Migration Path

1. **Phase 1**: Wrapper around existing BlockchainDB
2. **Phase 2**: Native implementation with backward compatibility
3. **Phase 3**: Full LevelDB API parity with hash-based storage

## Testing Strategy

1. **Unit Tests**: Test each LevelDB API method
2. **Compatibility Tests**: Verify LevelDB behavior parity
3. **Performance Tests**: Benchmark against standard LevelDB
4. **Integration Tests**: Test with existing LevelDB applications

## Next Steps

1. Implement missing LevelDB APIs (Delete, Iterator, WriteBatch)
2. Design key-to-hash mapping layer
3. Create compatibility wrapper
4. Benchmark performance vs standard LevelDB