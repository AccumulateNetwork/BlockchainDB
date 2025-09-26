# DKV Architecture

## The Fundamental Choice

In a sorted tree, you pick ONE sort order. This determines everything.

### Option 1: Sort by Hash (Current Implementation)

```go
key1 := sha256("https://apple.com")    // 0x8b1c...
key2 := sha256("https://banana.com")   // 0x3f2a...
key3 := sha256("https://cherry.com")   // 0x1a5d...

// Tree order: 0x1a5d < 0x3f2a < 0x8b1c
// Result: cherry, banana, apple (meaningless)
```

**Use when:**
- You need content-addressed storage
- Hash lookups are primary
- Iteration order doesn't matter

### Option 2: Sort by Original Key

```go
key1 := "https://apple.com"
key2 := "https://banana.com"
key3 := "https://cherry.com"

// Tree order: apple < banana < cherry
// Result: Natural alphabetical order
```

**Use when:**
- You need meaningful iteration
- Range queries matter (e.g., all URLs from domain)
- Can tolerate slower hash lookups

### Option 3: Dual Index

Maintain TWO trees:

```
Hash Tree: hash → value (for fast lookups)
Key Tree: url → hash (for ordered iteration)
```

**Trade-offs:**
- Every write updates both trees
- ~30% more storage for index
- Consistency complexity

## Implementation Impact

### Hash-Sorted (Current)
```go
store.Put(sha256(url), data)          // O(log n)
store.Get(sha256(url))                 // O(log n)
store.Range(hash1, hash2)              // Useless
for iter.Next() { }                   // Random order
```

### Key-Sorted (Alternative)
```go
store.Put(url, data)                   // O(log n)
store.Get(url)                         // O(log n)
store.GetByHash(hash)                  // O(n) without index
store.Range("a.com", "z.com")          // Meaningful!
for iter.Next() { }                    // Alphabetical
```

### Dual-Index (Experimental)
```go
dual.PutWithKey(url, data)             // O(log n) × 2
dual.GetByHash(hash)                   // O(log n)
dual.GetByKey(url)                     // O(log n) × 2
dual.IterateByKey(func(url, data) {}) // Ordered by URL
dual.IterateByHash(func(hash, data) {}) // Ordered by hash
```

## Which Should You Use?

```
Need hash lookups only?           → Hash-sorted
Need ordered iteration only?      → Key-sorted
Need both?                        → Dual-index or pick one
Building a blockchain?            → Hash-sorted
Building a web crawler?           → Key-sorted or dual
Building a cache?                 → Hash-sorted
```

## Migration Cost

Changing sort order requires full rebuild:

```go
// Cannot change in place!
oldTree := OpenTreeByHash()
newTree := OpenTreeByKey()

for oldTree.Next() {
    key := RecoverOriginalKey() // Need metadata!
    newTree.Put(key, value)
}
```

**Warning:** If you don't store original keys, you can't migrate from hash to key sorting.

## Current State

- ✅ Hash-sorted TreeDKV is production-ready
- 🔧 Dual-index is implemented but experimental
- ❌ Key-sorted would require rewrite of comparator