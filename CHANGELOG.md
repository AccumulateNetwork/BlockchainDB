# Changelog

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