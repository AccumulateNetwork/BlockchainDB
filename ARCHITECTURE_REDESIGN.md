# BlockchainDB Architecture Redesign

## Summary

Moving from a traditional KV approach to a **time-based value storage** with **key-based sharding** for dynamic data. This design aligns perfectly with blockchain requirements where data is immutable and content-addressed.

## Core Design Principles

### 1. Time-Based Value Storage (Immutable Data)
- **12-hour blocks**: ~730 files per year
- **Self-contained files**: Each file can rebuild the database
- **Content-addressed**: Key = SHA256(value)
- **Full iteration support**: For verification and rebuilding

### 2. Key-Based Dynamic Sharding (Mutable State)
- **Sharded by key prefix**: Predictable routing
- **Independent compaction**: Each shard manages itself
- **Efficient compression**: Localized data patterns

## Implementation Structure

```
BlockchainDB/
├── database/
│   ├── timeseries/       # NEW: Time-based storage
│   │   ├── value_file.go     # Individual value file management
│   │   ├── time_store.go     # Time-based rotation and management
│   │   └── key_index.go      # Key to file/offset mapping
│   │
│   ├── sharded/          # NEW: Sharded dynamic storage
│   │   ├── shard.go          # Individual shard management
│   │   ├── router.go         # Key-based routing
│   │   └── compactor.go      # Shard compaction
│   │
│   └── (existing dirs)
│
└── data/                 # Runtime data organization
    ├── values/           # Time-based immutable values
    │   └── 2024/01/v_20240101_00.dat
    │
    ├── keys/             # Key indices
    │   └── primary.idx
    │
    └── shards/           # Dynamic state shards
        └── shard_00/
```

## Key Components Created

### 1. ValueFile (`value_file.go`)
- **Binary format** with magic bytes and version
- **Entry structure**: timestamp + length + key + value
- **Built-in index** for fast lookups
- **Iteration support** for rebuilding
- **Integrity verification** via SHA256

### 2. TimeStore (`time_store.go`)
- **Automatic rotation** every 12 hours
- **Time-range queries** for historical data
- **File registry** tracking all value files
- **Rebuild capability** from value files alone

### 3. KeyIndex (`key_index.go`)
- **Maps keys → (file, offset, length)**
- **Bloom filter** for fast non-existence checks
- **Memory cache** for recent keys
- **Write buffering** to reduce I/O

## Benefits Over Current KV Approach

### Performance
| Aspect | Current KV | Time-Based Design |
|--------|-----------|-------------------|
| Write Speed | Degrades O(n²) | Constant O(1) append |
| Read Speed | O(n) linear scan | O(1) with index |
| File Count | Grows unbounded | ~730/year predictable |
| Rebuild Time | Requires key DB | Values self-contained |
| Archival | Difficult | Natural by time period |

### Operational
1. **Natural archival**: Old time files → cold storage
2. **Parallel access**: Different files accessed concurrently
3. **Incremental backup**: Only new files need backing up
4. **Disaster recovery**: Value files contain everything

### Blockchain-Specific
1. **Content addressing**: Key = hash(value) enforced
2. **Immutability**: Append-only time files
3. **Verification**: Can verify all data integrity
4. **Chain replay**: Natural chronological order

## Migration Strategy

### Phase 1: Build New Components (Week 1)
- [x] Implement ValueFile format
- [x] Create TimeStore with rotation
- [x] Build KeyIndex structure
- [ ] Add sharded dynamic storage

### Phase 2: Integration (Week 2)
- [ ] Router for immutable vs mutable data
- [ ] Migration tools from old KV format
- [ ] Benchmarking and optimization

### Phase 3: Deployment (Week 3)
- [ ] Parallel run with existing system
- [ ] Data migration
- [ ] Cutover to new system

## Usage Example

```go
// Initialize
db := NewBlockchainDB(BlockchainConfig{
    DataDir:          "/data/blockchain",
    ValueRotateHours: 12,
    DynamicShards:    256,
})

// Write immutable blockchain data
key := sha256.Sum256(blockData)
err := db.PutImmutable(key, blockData)

// Write mutable state
stateKey := sha256.Sum256([]byte("account:0x123"))
err := db.PutMutable(stateKey, accountData)

// Read (automatically routes to correct storage)
value, err := db.Get(key)

// Time-range iteration
for entry := range db.IterateTimeRange(startTime, endTime) {
    // Process historical data
}

// Rebuild index from values
err := db.RebuildIndex()
```

## Performance Expectations

### Write Performance
- **Immutable**: 5M+ ops/sec (simple append)
- **Mutable**: 1M+ ops/sec (sharded writes)
- **No degradation** over time

### Read Performance
- **Indexed**: O(1) lookup ~1M ops/sec
- **Time range**: Efficient file-level filtering
- **Rebuild**: ~500K entries/sec scanning

### Storage Efficiency
- **Compression**: Natural by time period
- **Deduplication**: Not needed (content-addressed)
- **Archival**: Old files easily compressed/moved

## Comparison with Failed KV Approach

| Issue | KV Problem | Time-Based Solution |
|-------|------------|-------------------|
| Linear scanning | O(n) in sections | Index provides O(1) |
| File growth | Single growing file | Bounded 12-hour files |
| Rebuild capability | Requires key DB | Self-contained values |
| Write amplification | Read-modify-write | Pure append |
| Memory pressure | Unbounded cache | Time-bounded working set |

## Next Steps

1. **Complete sharded storage** implementation
2. **Build integration layer** combining both systems
3. **Create migration tools** from existing KV
4. **Benchmark against** current implementation
5. **Deploy in parallel** for validation

## Conclusion

This time-based architecture with key sharding:
- **Solves all performance issues** identified in KV layer
- **Aligns perfectly** with blockchain data patterns
- **Provides operational benefits** for production systems
- **Enables efficient** archival and recovery

The key insight: **Stop fighting the data's natural structure**. Blockchain data is time-ordered and immutable - organize it that way.