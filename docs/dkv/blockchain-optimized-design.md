# Blockchain-Optimized DKV Design

## The Dual-Tree Architecture

For blockchain storage, use two specialized trees:

### 1. Hash Tree (Primary - 90% of data)
- **Purpose**: Content-addressed blockchain data
- **Keys**: SHA-256 hashes
- **Values**: Blocks, transactions, state, merkle proofs
- **Optimized for**: Fast hash lookups, deduplication, proof verification

### 2. URL/Index Tree (Secondary - 10% of data)
- **Purpose**: Metadata, ordering, and range queries
- **Keys**: Structured strings (URLs, paths, timestamps)
- **Values**: References to hash tree (32-byte hashes)
- **Optimized for**: Iteration, range queries, human-readable access

## Why This Works for Blockchains

### Blockchain Data Characteristics

```go
// Most blockchain data is content-addressed
type BlockchainData struct {
    // 90% - Accessed by hash
    Blocks       map[Hash]Block           // Block by hash
    Transactions map[Hash]Transaction     // Tx by hash
    State        map[Hash]StateNode       // Merkle nodes
    Receipts     map[Hash]Receipt         // Tx receipts

    // 10% - Needs ordering/iteration
    BlockIndex   map[uint64]Hash          // Block number → hash
    TimeIndex    map[time.Time][]Hash     // Timestamp → tx list
    AccountIndex map[Address]Hash         // Account → state
}
```

### Access Patterns

| Operation | Frequency | Tree Used | Performance |
|-----------|-----------|-----------|-------------|
| Get block by hash | Very High | Hash Tree | O(log n) |
| Get tx by hash | Very High | Hash Tree | O(log n) |
| Verify merkle proof | High | Hash Tree | O(log n) |
| Get block by number | Medium | URL Tree → Hash Tree | O(log n) × 2 |
| List recent blocks | Low | URL Tree iteration | O(k) |
| Account history | Low | URL Tree range query | O(k log n) |

## Implementation

### Using Current DualIndexDKV

```go
store := NewDualIndexDKV(dir)

// Store block (most common operation)
blockHash := sha256(blockData)
store.PutWithKey(fmt.Sprintf("block:%010d", blockNum), blockData)
// Internally: hashTree[blockHash] = blockData
//            urlTree["block:0000000001"] = blockHash

// Fast hash lookup (primary use case)
block, _ := store.GetByHash(blockHash)  // Direct to hash tree

// Range query (occasional)
store.IterateByKey(func(key string, value []byte) error {
    if strings.HasPrefix(key, "block:0000001") {
        // Process blocks 10000-19999
    }
})
```

### Optimized Version

```go
type BlockchainDKV struct {
    hashTree  *TreeDKV  // Primary: all blockchain data
    indexTree *TreeDKV  // Secondary: just indices
}

func (b *BlockchainDKV) PutBlock(block Block) error {
    hash := block.Hash()
    data := block.Serialize()

    // Primary storage (full data)
    b.hashTree.Put(hash, data)

    // Index entries (just references)
    b.indexTree.Put(makeKey("block:num", block.Number), hash[:])
    b.indexTree.Put(makeKey("block:time", block.Timestamp), hash[:])

    return nil
}

func (b *BlockchainDKV) GetBlockByHash(hash [32]byte) (Block, error) {
    // Single lookup in hash tree
    data, found, err := b.hashTree.Get(hash)
    return DeserializeBlock(data), err
}

func (b *BlockchainDKV) GetBlockByNumber(num uint64) (Block, error) {
    // First: get hash from index
    hashBytes, found, _ := b.indexTree.Get(makeKey("block:num", num))

    // Second: get block from hash tree
    var hash [32]byte
    copy(hash[:], hashBytes)
    return b.GetBlockByHash(hash)
}

func (b *BlockchainDKV) GetBlockRange(start, end uint64) []Block {
    startKey := makeKey("block:num", start)
    endKey := makeKey("block:num", end)

    // Iterate through index
    iter := b.indexTree.Range(startKey, endKey)

    blocks := []Block{}
    for _, kv := range iter {
        var hash [32]byte
        copy(hash[:], kv.Value)
        block, _ := b.GetBlockByHash(hash)
        blocks = append(blocks, block)
    }

    return blocks
}
```

## Storage Overhead Analysis

### Single Hash Tree
- **Size**: 100% of data
- **Iteration**: Random/meaningless
- **Range queries**: Impossible

### Dual Tree (Our Approach)
- **Hash Tree**: 100% of data
- **URL Tree**: ~5-10% overhead (just 32-byte hashes + keys)
- **Total**: ~110% of single tree
- **Benefit**: Full ordering and range queries

### Example with 1TB Blockchain

```
Single Hash Tree:
- Data: 1TB
- Total: 1TB
- Can't iterate meaningfully

Dual Tree:
- Hash Tree: 1TB (all block/tx data)
- URL Tree: 50GB (indices and references)
- Total: 1.05TB
- Full iteration and range query support

Worth it? Absolutely!
```

## Optimizations

### 1. Lazy Index Building
```go
// Only index what you need
if needBlockNumberIndex {
    indexTree.Put(blockNumKey, hash)
}
// Skip indices you don't use
```

### 2. Batched Index Updates
```go
// Batch index writes for better performance
indexBatch := []KVPair{}
for _, block := range blocks {
    indexBatch = append(indexBatch, KVPair{
        Key: makeBlockNumberKey(block.Number),
        Value: block.Hash(),
    })
}
indexTree.BatchPut(indexBatch)
```

### 3. Selective Indexing
```go
// Only index recent data in URL tree
if block.Number > currentHeight - 10000 {
    indexTree.Put(key, hash)  // Recent blocks
} else {
    // Old blocks only in hash tree
}
```

## Comparison with Other Blockchain Storage

| System | Approach | Trade-off |
|--------|----------|-----------|
| **Bitcoin Core** | LevelDB by block height | No hash index (slower hash lookups) |
| **Ethereum (Geth)** | Separate ancient/fresh DBs | Complex, migration overhead |
| **IPFS** | Pure content-addressed | No ordering without external index |
| **Our Dual-Tree** | Hash + URL trees | 10% overhead for full capabilities |

## Conclusion

The dual-tree approach is optimal for blockchains because:

1. **99% of lookups are by hash** → Hash tree handles these at O(log n)
2. **1% need ordering** → URL tree provides this with minimal overhead
3. **Storage overhead is minimal** → URL tree only stores 32-byte references
4. **Best of both worlds** → Content addressing + meaningful iteration

This is exactly what blockchain storage needs: fast hash lookups for consensus/verification, with optional ordering for queries/analytics.