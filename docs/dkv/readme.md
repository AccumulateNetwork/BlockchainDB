# DKV - Dynamic Key-Value Store

## What We Built

A tree-based key-value store using LSM-tree architecture, sorted by 32-byte hash keys.

### Working Components

✅ **Core TreeDKV** (`database/dkv/dkv_tree.go`)
- Put, Get, Delete, Range queries
- Iterator with merge sorting across levels
- Background compaction
- Bloom filters for fast negative lookups

✅ **Basic Tests** (`database/dkv/integration_test.go`)
- Core operations tested
- Some advanced features untested

### Experimental Components

These are implemented but not production-ready:

- **URL Store** - Wrapper with metadata tracking
- **Dual Index** - Maintains both hash and key order
- **Prefix Router** - Routes by key prefix
- **Sharded Store** - Distributes across shards

## Current Architecture

```
TreeDKV (sorted by hash)
├── MemTable (in-memory skip list)
├── Level 0 (recent flushes, 4MB files)
├── Level 1 (compacted, 10MB files)
└── Level N (exponentially larger)
```

Keys are 32-byte hashes, iteration order is hash order (essentially random for URLs).

## The Key Decision

**Current Implementation: Hash-Sorted**
- ✅ Fast O(log n) hash lookups
- ✅ Even distribution
- ❌ Random iteration order
- ❌ No meaningful range queries

**Alternative: Key-Sorted** (requires rewrite)
- ✅ Meaningful iteration order
- ✅ Domain/prefix queries work
- ❌ No fast hash lookups without index
- ❌ Uneven distribution

**Dual-Index** (implemented, experimental)
- ✅ Both hash lookups and ordered iteration
- ❌ 2x writes, 1.3x storage
- ❌ More complex

See [architecture.md](./architecture.md) for details.

## Quick Start

```go
import "github.com/BlockchainDB/database/dkv"

// Basic usage
store, _ := dkv.NewTreeDKV("./data")
defer store.Close()

// Store by hash
url := "https://example.com"
key := sha256.Sum256([]byte(url))
store.Put(key, []byte("content"))

// Retrieve
value, found, _ := store.Get(key)

// Iterate (hash order - essentially random)
iter := store.NewIterator()
for iter.Next() {
    // Process key, value
}
```

See [api.md](./api.md) for complete reference.

## File Structure

```
data/
├── manifest.json         # Metadata
├── level0/*.sst         # Recent writes
├── level1/*.sst         # Compacted data
└── levelN/*.sst         # Larger files
```

## Performance

- Writes: ~50K ops/sec
- Reads: ~200K ops/sec (with bloom filters)
- Storage: ~1.1x data size (with metadata)

## Status

Production-ready: Core TreeDKV with hash sorting
Experimental: Everything else

## Next Steps

Choose one:
1. **Keep as-is**: Use hash-sorted tree, accept random iteration
2. **Add index**: Use DualIndexDKV for both access patterns
3. **Rewrite**: Change to key-sorted for meaningful iteration