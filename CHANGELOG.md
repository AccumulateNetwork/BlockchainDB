# Changelog

## 2026-01-11 - Performance Optimizations & 20-Byte Internal Keys

**Branch**: `2-performance`
**Commit**: `6156ffc`

### Added
- **20-byte internal keys**: External API accepts [32]byte, internally truncated to [20]byte
  - 25% reduction in entry size (48→36 bytes)
  - 7.9% disk savings (~6GB at 500M entries)
  - 2^80 birthday bound collision resistance
- **Lock-free bloom filter**: Atomic bit-OR operations, no mutex on hot path
- **Deferred bloom add**: Moved from PutPermAsync to background workers
- **Comprehensive benchmark documentation**: `docs/performance/benchmark-results-2026-01.md`

### Changed
- **NumChannelGroups**: 4 → 8 (better parallelism with 1024 shards)
- **NumShards**: 256 → 1024 (17% faster with 256 bins)
- **selectgo overhead**: 28% → 3% CPU time

### Performance Results (100M Deterministic Benchmark)
| Metric | BadgerDB | BlockchainDB | Ratio |
|--------|----------|--------------|-------|
| Entries @ 3min | 1.3M | 99.9M | **77x** |
| Entries/second | 7,141 | 554,944 | **78x** |
| Batch Write Time | 14.1ms | 121µs | **117x** |

### Technical Details
- `InternalKey`: New [20]byte type for internal storage
- `truncateKey()`: Converts [32]byte external key to InternalKey
- Entry format: 20-byte key + 8-byte offset + 8-byte length = 36 bytes
- Bloom filter: Lock-free with pendingCache check before bloom on reads

---

## 2024-01-26 - DKV Implementation & Documentation Cleanup

### Added
- **DKV (Dynamic Key-Value Store)** - Complete LSM-tree implementation
  - `database/dkv/dkv_tree.go` - Core tree-based storage with sorted hash keys
  - `database/dkv/bloom.go` - Bloom filters for fast negative lookups
  - `database/dkv/dual_index.go` - Experimental dual-index for both hash and key ordering
  - `database/dkv/url_store.go` - Experimental URL storage with metadata
  - `database/dkv/prefix_router.go` - Experimental routing strategies
  - `database/dkv/integration_test.go` - Comprehensive tests

### Changed
- **Documentation Reorganization**
  - All documentation moved to `/docs` directory (except root README.md)
  - All CAPS filenames renamed to lowercase with hyphens
  - Consolidated DKV documentation from 40KB across 9 files to 10KB across 3 focused files
  - Removed outdated and redundant documentation

### Technical Details

#### DKV Architecture
- LSM-tree with multiple levels and background compaction
- Sorted by 32-byte hash keys (SHA-256)
- MemTable (4MB) → Level 0 → Level N compaction strategy
- Bloom filters reduce disk reads by ~99%
- Performance: ~50K writes/sec, ~200K reads/sec

#### Key Trade-off Documented
- **Current**: Hash-sorted for fast O(log n) lookups, but random iteration order
- **Alternative**: Key-sorted for meaningful iteration, but slower hash lookups
- **Dual-index**: Both capabilities, but 2x writes and 1.3x storage

### File Structure
```
database/dkv/           # Core implementation (production-ready)
docs/dkv/              # Focused documentation
  ├── readme.md        # Overview and status
  ├── architecture.md  # Design decisions
  └── api.md          # Usage guide
```

### Status
- **Production-ready**: Core TreeDKV with hash sorting
- **Experimental**: DualIndex, URLStore, PrefixRouter, ShardedDKV