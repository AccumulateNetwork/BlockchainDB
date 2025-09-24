# LevelDB Feature Design for BlockchainDB

## Core Insight: Key Type Separation

BlockchainDB recognizes that blockchain systems use two fundamentally different key types:
1. **Hash Keys** - Cryptographic hashes (blocks, transactions, states)
2. **Dynamic Keys** - Application-defined keys (indices, metadata, counters)

These require completely different storage strategies for optimal performance.

## 1. Hash Key Storage (90% of blockchain data)

### Characteristics
- Keys are cryptographic hashes (32-64 bytes)
- Random distribution, no meaningful order
- No range queries needed
- No iteration needed (except full scan for maintenance)
- Immutable once written
- Primary operations: Get, Put, Delete (rare)

### Design
```go
type HashStorage struct {
    // Direct file offset mapping
    index    map[[32]byte]FileOffset  // In-memory hash -> offset
    dataFile *AppendOnlyFile          // Append-only data file

    // No sorting, no B-tree, no LSM tree needed
}
```

### Implementation Strategy
- **Storage**: Append-only log with in-memory hash index
- **Writes**: Append to file, update index
- **Reads**: O(1) index lookup → direct file read
- **Deletes**: Mark tombstone in index (lazy deletion)
- **No Compaction**: Since no sorting, just periodic garbage collection

### Benefits Over LevelDB for Hashes
- No sorting overhead
- No compaction CPU cost
- Direct O(1) access
- Simpler implementation
- Better cache locality

## 2. Dynamic Key Storage (10% of blockchain data)

### Characteristics
- User-defined keys (strings, integers, composites)
- Order matters for range queries
- Iteration needed
- Relatively small dataset
- Frequent updates

### Design
```go
type DynamicKeyStorage struct {
    tree     *btree.BTree           // In-memory B-tree for small datasets
    memtable *MemTable              // Write buffer
    sstables []*SSTable             // For larger datasets

    // Full LevelDB-like features here
}
```

### LevelDB Features Supported (Dynamic Keys Only)
- **Iterators**: Forward/reverse iteration
- **Range Queries**: Seek, Next, Prev
- **Ordered Scans**: Key prefix matching
- **Batch Writes**: Atomic multi-key updates
- **Snapshots**: Point-in-time views
- **Compaction**: Background optimization

## 3. Unified API Design

```go
type BlockchainDB struct {
    hashStore    *HashStorage      // For hash keys
    dynamicStore *DynamicKeyStorage // For dynamic keys
}

// Key type detection
func (db *BlockchainDB) Put(key, value []byte) error {
    if isHashKey(key) {
        return db.hashStore.Put(key, value)
    }
    return db.dynamicStore.Put(key, value)
}

// Iterator only for dynamic keys
func (db *BlockchainDB) NewIterator(prefix []byte) Iterator {
    if isHashKey(prefix) {
        return &NoOpIterator{} // Or error
    }
    return db.dynamicStore.NewIterator(prefix)
}
```

## 4. Batch Operations

```go
type Batch struct {
    hashOps    []HashOperation     // Simple put/delete
    dynamicOps []DynamicOperation  // Can include range deletes
}

func (b *Batch) Put(key, value []byte) {
    if isHashKey(key) {
        b.hashOps = append(b.hashOps, HashOperation{Put, key, value})
    } else {
        b.dynamicOps = append(b.dynamicOps, DynamicOperation{Put, key, value})
    }
}

func (b *Batch) Delete(key []byte) {
    // Similar logic
}

func (b *Batch) DeleteRange(start, end []byte) {
    // Only for dynamic keys
    if isHashKey(start) {
        panic("Range delete not supported for hash keys")
    }
    b.dynamicOps = append(b.dynamicOps, DynamicOperation{DeleteRange, start, end})
}
```

## 5. Snapshot/MVCC Support

```go
type Snapshot struct {
    hashVersion    uint64  // Version for hash storage
    dynamicVersion uint64  // Version for dynamic storage
}

// Hash storage: Simple versioning
type HashEntry struct {
    Value     []byte
    Version   uint64
    Deleted   bool
}

// Dynamic storage: Full MVCC with timestamps
type DynamicEntry struct {
    Key       []byte
    Value     []byte
    Timestamp uint64
    Next      *DynamicEntry // Version chain
}
```

## 6. Migration Path from LevelDB

```go
// Compatibility layer
type LevelDBCompat struct {
    db *BlockchainDB
}

func (l *LevelDBCompat) Get(key []byte) ([]byte, error) {
    return l.db.Get(key)
}

func (l *LevelDBCompat) NewIterator() Iterator {
    // Returns composite iterator:
    // - No-op for hash keys
    // - Full iterator for dynamic keys
    return &CompositeIterator{
        dynamic: l.db.dynamicStore.NewIterator(nil),
    }
}
```

## 7. Performance Characteristics

### Hash Keys (Blocks, Transactions)
- Write: O(1) - Append + index update
- Read: O(1) - Index lookup + single disk read
- Delete: O(1) - Mark in index
- Space: Minimal overhead (no sorting structures)

### Dynamic Keys (Indices, Metadata)
- Write: O(log n) - Tree insertion
- Read: O(log n) - Tree lookup
- Range Query: O(log n + k) - Tree traversal
- Space: Standard B-tree/LSM overhead

## 8. Configuration

```go
type Config struct {
    // Hash storage config
    HashIndexSize     int  // Pre-allocated index size
    HashFileSize      int  // Max file size before rotation

    // Dynamic storage config
    MemTableSize      int  // When to flush to SSTable
    MaxOpenFiles      int  // File handle limit
    BlockCacheSize    int  // LRU cache size
    CompressionType   int  // Snappy, LZ4, none

    // Key type detection
    HashKeyPrefixes   [][]byte // Identify hash keys by prefix
    HashKeyLength     int      // Fixed length for hash keys (32, 64)
}
```

## 9. Implementation Priorities

1. **Phase 1**: Hash storage with simple index
2. **Phase 2**: Basic dynamic key B-tree
3. **Phase 3**: Batch operations
4. **Phase 4**: Snapshots for hash storage
5. **Phase 5**: Full MVCC for dynamic storage
6. **Phase 6**: SSTable implementation for large dynamic datasets

## Summary

This design achieves LevelDB compatibility while optimizing for blockchain workloads:
- **Hash keys** get simplified, faster storage without unnecessary sorting
- **Dynamic keys** get full LevelDB features where they're actually useful
- Clean separation allows optimal implementation for each use case
- Migration path preserves API compatibility