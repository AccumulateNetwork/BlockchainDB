# DKV API Reference

## Core API (Production-Ready)

### TreeDKV

```go
import "github.com/BlockchainDB/database/dkv"
```

#### Create/Open
```go
store, err := dkv.NewTreeDKV("./data")
defer store.Close()
```

#### Basic Operations
```go
// Put
var key [32]byte = sha256.Sum256([]byte("my-key"))
err := store.Put(key, []byte("my-value"))

// Get
value, found, err := store.Get(key)
if !found {
    // Key doesn't exist
}

// Delete
err := store.Delete(key)  // Marks as deleted (tombstone)
```

#### Iteration
```go
// Full iteration (hash order - essentially random)
iter := store.NewIterator()
defer iter.Close()

for iter.Next() {
    key := iter.Key()      // [32]byte
    value := iter.Value()  // []byte
}

// Range iteration (hash range - not meaningful for URLs)
iter := store.NewIteratorWithOptions(&dkv.IteratorOptions{
    Start: startHash,
    End:   endHash,
})
```

#### Batch Operations
```go
pairs := []dkv.KVPair{
    {Key: hash1, Value: []byte("value1")},
    {Key: hash2, Value: []byte("value2")},
}
err := store.BatchPut(pairs)

// Range query
results, err := store.Range(startHash, endHash)
```

### Types

```go
type KVPair struct {
    Key   [32]byte
    Value []byte
}

type IteratorOptions struct {
    Start [32]byte  // Inclusive
    End   [32]byte  // Inclusive
}
```

## Experimental APIs

These compile but aren't fully tested:

### DualIndexDKV

Maintains both hash and original key ordering.

```go
dual, err := dkv.NewDualIndexDKV("./data")

// Store with original key
err := dual.PutWithKey("https://example.com", []byte("content"))

// Get by hash (fast)
hash := sha256.Sum256([]byte("https://example.com"))
value, found, err := dual.GetByHash(hash)

// Get by key (slower, two lookups)
value, found, err := dual.GetByKey("https://example.com")

// Iterate by original key order (meaningful)
err := dual.IterateByKey(func(url string, value []byte) error {
    fmt.Printf("%s\n", url)
    return nil
})
```

### URLStore

Wrapper with metadata support.

```go
store, err := dkv.NewURLStore("./data")

// Store URL with metadata
err := store.PutURL("https://example.com", data, "namespace")

// Get with metadata
data, metadata, err := store.GetURL("https://example.com")
// metadata.Domain, metadata.Path, metadata.Timestamp

// Iterate by domain
store.IterateDomain("example.com", func(url string, data []byte) error {
    return nil
})
```

### ShardedDKV

Distributes across multiple trees.

```go
sharded, err := dkv.NewShardedDKV("./data", 16)  // 16 shards

// Automatically routes to correct shard
sharded.Put(hash, value)
value, found, err := sharded.Get(hash)

// Merged iteration across all shards
iter := sharded.NewIterator()
```

### PrefixRouter

Routes by key prefix.

```go
router, err := dkv.NewPrefixRouter("./data", 2)  // 2-byte prefix

// Routes to different subtrees based on first 2 bytes
router.Put(hash, value)
```

## Bloom Filters

Used internally by TreeDKV for fast negative lookups.

```go
// Standalone usage (usually not needed)
bf := dkv.NewBloomFilter(expectedItems, falsePositiveRate)
bf.Add(key)
if bf.MayContain(key) {
    // Might exist (check disk)
}
```

## Constants

```go
const (
    maxKeysPerLeaf = 100000           // Keys per SSTable
    maxLevels      = 7                // Tree depth
    memTableSize   = 4 * 1024 * 1024  // 4MB flush threshold
)
```

## Error Handling

```go
value, found, err := store.Get(key)
if err != nil {
    // I/O error, corruption, etc.
}
if !found {
    // Key doesn't exist (not an error)
}
```

## Thread Safety

- ✅ TreeDKV - Safe for concurrent use
- ❌ Iterator - Use one per goroutine
- ✅ ShardedDKV - Safe for concurrent use

## Performance Notes

- Batch writes for better throughput
- Bloom filters eliminate ~99% of negative lookups
- Compaction runs automatically in background
- Iterator holds references to all levels (memory usage)

## Example: URL Storage

Since keys are hashes and iteration is random, you need metadata:

```go
// Option 1: Store URL in value
type Record struct {
    URL  string
    Data []byte
}

value, _ := json.Marshal(Record{URL: url, Data: data})
store.Put(sha256(url), value)

// Option 2: Use DualIndexDKV for ordered iteration
dual.PutWithKey(url, data)
dual.IterateByKey(func(url string, data []byte) error {
    // URLs in order
})

// Option 3: Accept random iteration
iter := store.NewIterator()
for iter.Next() {
    // Random order, need to decode value to get URL
}
```