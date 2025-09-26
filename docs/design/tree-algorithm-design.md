# Tree-Based Algorithm Design for BlockchainDB

## Recommended Solution: B+ Tree with Write-Ahead Buffer

### Why B+ Tree is Optimal for BlockchainDB

1. **Disk-Optimized**: Designed for databases with block-based I/O
2. **Sorted Order Maintained**: Keys always sorted, enabling binary search within nodes
3. **Bulk Loading Efficient**: Can build tree bottom-up for initial data
4. **Range Queries**: Leaf nodes linked for sequential access
5. **Predictable Performance**: Consistent O(log_B n) operations

### Proposed Architecture

```
┌─────────────────────────────────────┐
│        Write-Ahead Buffer           │ ← In-memory sorted buffer
│         (Red-Black Tree)            │   Flushes to B+ Tree periodically
└─────────────┬───────────────────────┘
              ↓
┌─────────────────────────────────────┐
│           B+ Tree Root              │ ← Cached in memory
│         [K1, K2, ..., Kn]          │
└────┬────────┬────────┬─────────────┘
     ↓        ↓        ↓
┌────────┐ ┌────────┐ ┌────────┐
│ Node 1 │ │ Node 2 │ │ Node 3 │     ← Internal nodes (cached)
└──┬──┬──┘ └──┬──┬──┘ └──┬──┬──┘
   ↓  ↓       ↓  ↓       ↓  ↓
┌────┐┌────┐┌────┐┌────┐┌────┐┌────┐
│Leaf││Leaf││Leaf││Leaf││Leaf││Leaf│ ← Leaf nodes (on disk)
│←───→│←───→│←───→│←───→│←───→│     ← Linked for sequential access
└────┘└────┘└────┘└────┘└────┘└────┘
```

### Implementation Strategy

#### Phase 1: Write-Ahead Buffer
```go
type WriteBuffer struct {
    tree     *rbtree.Tree  // Red-black tree for sorting
    size     int          // Current size in bytes
    maxSize  int          // Flush threshold (e.g., 4MB)
    mutex    sync.RWMutex
}

// Batch incoming writes
func (wb *WriteBuffer) Put(key [32]byte, value *DBBKey) error {
    wb.mutex.Lock()
    defer wb.mutex.Unlock()

    wb.tree.Insert(key, value)
    wb.size += 48  // Size of entry

    if wb.size >= wb.maxSize {
        return wb.flush()  // Merge into B+ tree
    }
    return nil
}
```

#### Phase 2: B+ Tree Structure
```go
type BPlusTree struct {
    root      *Node
    order     int    // Branching factor (e.g., 256)
    height    int
    numKeys   uint64
    file      *os.File
    cache     *NodeCache  // LRU cache for internal nodes
}

type Node struct {
    isLeaf   bool
    keys     [][32]byte    // Sorted keys
    values   []*DBBKey     // Only in leaf nodes
    children []int64       // File offsets to child nodes
    next     int64         // Next leaf (for sequential access)
    offset   int64         // This node's file offset
}
```

### Key Algorithms

#### 1. Insertion with Minimal I/O
```go
func (tree *BPlusTree) Insert(key [32]byte, value *DBBKey) error {
    // Step 1: Find leaf node (mostly from cache)
    leaf := tree.findLeaf(key)

    // Step 2: Insert into leaf
    if leaf.hasSpace() {
        leaf.insertSorted(key, value)
        return tree.writePage(leaf)  // Single page write
    }

    // Step 3: Split if necessary (rare)
    newLeaf := leaf.split()
    tree.updateParent(leaf, newLeaf)
    return nil
}
```

#### 2. Bulk Loading for Initial Data
```go
func (tree *BPlusTree) BulkLoad(entries []Entry) error {
    // Sort all entries first
    sort.Slice(entries, func(i, j int) bool {
        return bytes.Compare(entries[i].Key[:], entries[j].Key[:]) < 0
    })

    // Build leaves first
    leaves := make([]*Node, 0)
    current := tree.newLeafNode()

    for _, entry := range entries {
        if current.isFull() {
            leaves = append(leaves, current)
            current = tree.newLeafNode()
        }
        current.insert(entry.Key, entry.Value)
    }

    // Build internal nodes bottom-up
    tree.buildInternalNodes(leaves)
    return nil
}
```

#### 3. Optimized Search
```go
func (tree *BPlusTree) Get(key [32]byte) (*DBBKey, error) {
    node := tree.root

    // Traverse internal nodes (cached)
    for !node.isLeaf {
        idx := node.binarySearch(key)
        childOffset := node.children[idx]
        node = tree.cache.GetOrLoad(childOffset)
    }

    // Search in leaf node
    idx := node.binarySearch(key)
    if idx < len(node.keys) && bytes.Equal(node.keys[idx][:], key[:]) {
        return node.values[idx], nil
    }

    return nil, ErrNotFound
}
```

### Performance Optimizations

#### 1. Page Size Alignment
```go
const PageSize = 4096  // Align with OS page size
const KeysPerNode = (PageSize - HeaderSize) / EntrySize  // ~80 keys
```

#### 2. Write Combining
```go
type WriteBatch struct {
    operations []WriteOp
    tree       *BPlusTree
}

func (wb *WriteBatch) Apply() error {
    // Sort operations by key
    sort.Slice(wb.operations, ...)

    // Apply in sorted order (minimizes node splits)
    for _, op := range wb.operations {
        wb.tree.Insert(op.Key, op.Value)
    }

    return wb.tree.Sync()  // Single fsync
}
```

#### 3. Compression for Leaf Nodes
```go
func (node *Node) compress() []byte {
    // Delta encoding for sequential keys
    // Prefix compression for similar keys
    // Snappy compression for values
}
```

### Migration Path from Current System

#### Step 1: Add B+ Tree Alongside Current System
```go
type HybridStore struct {
    legacy  *HistoryFile  // Current implementation
    btree   *BPlusTree    // New implementation
    migrate bool          // Flag to enable migration
}
```

#### Step 2: Gradual Migration
```go
func (hs *HybridStore) Put(key [32]byte, value *DBBKey) error {
    if hs.migrate {
        // Write to both during migration
        hs.btree.Insert(key, value)
    }
    return hs.legacy.AddKeys(...)
}

func (hs *HybridStore) Get(key [32]byte) (*DBBKey, error) {
    if hs.migrate {
        // Try B+ tree first
        if val, err := hs.btree.Get(key); err == nil {
            return val, nil
        }
    }
    return hs.legacy.Get(key)
}
```

### Expected Performance Improvements

| Operation | Current | B+ Tree | Improvement |
|-----------|---------|---------|-------------|
| Write (small dataset) | 25M TPS | 20M TPS | -20% (buffer overhead) |
| Write (large dataset) | 128K TPS | 5M TPS | **39x** |
| Read (cached) | 1.5M RPS | 10M RPS | **6.7x** |
| Read (disk) | 46K RPS | 200K RPS | **4.3x** |
| Memory usage | O(n) | O(log n) | **Logarithmic** |
| Write amplification | O(n) | O(1) | **Constant** |

### Alternative: LSM Tree for Write-Heavy Workloads

If writes significantly outnumber reads:

```go
type LSMTree struct {
    memTable   *SkipList     // In-memory writes
    immutable  *SkipList     // Being flushed
    levels     []SSTables    // Sorted String Tables
    manifest   *Manifest     // Track file metadata
}

// Write path - always O(1)
func (lsm *LSMTree) Put(key, value []byte) error {
    return lsm.memTable.Insert(key, value)  // Memory only
}

// Background compaction
func (lsm *LSMTree) compact() {
    // Merge overlapping SSTables
    // Maintain sorted order per level
    // Delete obsolete values
}
```

### Recommended Implementation Order

1. **Week 1**: Implement Write-Ahead Buffer with RB-tree
2. **Week 2**: Basic B+ tree with insert/search
3. **Week 3**: Node caching and bulk loading
4. **Week 4**: Migration framework and testing
5. **Week 5**: Performance tuning and benchmarks

### Conclusion

The B+ Tree with Write-Ahead Buffer provides:
- **Consistent O(log n) performance** at any scale
- **Efficient disk I/O** with page-aligned nodes
- **Write buffering** to batch operations
- **Cache-friendly** internal node structure
- **Proven architecture** used by major databases

This will eliminate the current O(n) degradation and provide predictable, scalable performance.