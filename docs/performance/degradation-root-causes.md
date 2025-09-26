# Exact Locations of Performance Degradation

## Critical Bottlenecks Found

### 1. HistoryFile.UpdateKeySet() - PRIMARY CULPRIT
**Location**: `history_file.go:303-351`

**Problem**: Every write triggers massive overhead:
```go
// Line 324: Allocates NEW buffer for ENTIRE KeySet on EVERY update
buffer := make([]byte, NewLength)

// Line 327: Reads ENTIRE KeySet from disk
hf.File.ReadAt(buffer[:CurrentLength], int64(keySet.Start))

// Line 333: Writes ENTIRE KeySet back to disk
hf.File.WriteAt(buffer[:NewLength], int64(offset))

// Line 349: Sorts ALL KeySets after EVERY update!
hf.OffsetSort()
```

**Impact**:
- Adding 1 key to a 1MB KeySet = Read 1MB + Write 1MB + Sort all KeySets
- This explains the 80% degradation as KeySets grow

### 2. HistoryFile.AddKeys() - SECONDARY BOTTLENECK
**Location**: `history_file.go:170-206`

**Problem**: Multiple disk writes per AddKeys call:
```go
// Line 201: Updates KeySet (triggers full read/write)
hf.UpdateKeySet(index, keyList[startOff:endOff])

// Line 205: Writes ENTIRE header after EVERY AddKeys
hf.File.WriteAt(hf.Marshal(), 0)
```

**Impact**:
- Every batch write = Multiple UpdateKeySet calls + Header write
- Header grows with KeySets, becomes larger over time

### 3. KV2.Put() - DOUBLE LOOKUP PENALTY
**Location**: `kv_2.go:156-178`

**Problem**: Every Put does 2 lookups before writing:
```go
// Line 158: First lookup in DynaKV
if value2, err2 := k.DynaKV.Get(key); err2 == nil {

// Line 166: Second lookup in PermKV
if value2, err2 := k.PermKV.Get(key); err2 == nil {
```

**Impact**:
- New keys require 2 failed lookups before write
- As dataset grows, lookup cost increases
- No bloom filter to avoid unnecessary lookups

### 4. Memory Allocation Storm
**Location**: Multiple locations

**Problems**:
1. `history_file.go:221` - Creates entire entry array for sorting:
   ```go
   entries := make([]entry, numEntries)
   ```

2. `history_file.go:251` - Allocates buffer for each KeySet during sort:
   ```go
   buffer := make([]byte, keysLen)
   ```

3. `history_file.go:377` - Allocates buffer for each uncached read:
   ```go
   buffer = make([]byte, keysLen)
   ```

**Impact**:
- GC pressure increases with dataset size
- Memory fragmentation from constant allocations
- Cache thrashing as working set exceeds available memory

### 5. OffsetSort() - O(n log n) on EVERY Write
**Location**: `history_file.go:276-292`

**Problem**: Sorts ALL KeySets after every update:
```go
func (hf *HistoryFile) OffsetSort() {
    slices.SortFunc(hf.KeySetOffset, func(a, b *KeySet) int {
        // Sorts entire KeySetOffset array
    })
}
```

**Called from**: `UpdateKeySet()` line 349

**Impact**:
- With 1000 KeySets, every write triggers O(1000 log 1000) sort
- Completely unnecessary - KeySets rarely move

### 6. Cache Invalidation Issues
**Location**: `history_file.go:345-347, 266-268`

**Problem**: Cache cleared too aggressively:
```go
// After sorting ALL KeySets:
hf.keySetCache = make(map[int][]byte)  // Clears entire cache!

// After updating one KeySet:
delete(hf.keySetCache, index)  // Only this should happen
```

**Impact**:
- Cache misses spike after bulk operations
- Rebuilding cache causes allocation storm

## Write Path Analysis

For a single Put operation to KV2:
1. **KV2.Put()** → 2 Get operations (DynaKV, PermKV)
2. **KV.Put()** → Calls kFile.Put()
3. **kFile.Put()** → Calls HistoryFile.AddKeys()
4. **HistoryFile.AddKeys()** → Calls UpdateKeySet()
5. **UpdateKeySet()** →
   - Allocates new buffer (size of entire KeySet)
   - Reads entire KeySet from disk
   - Appends new key
   - Writes entire KeySet back
   - Calls OffsetSort() on ALL KeySets
   - Invalidates cache
6. **AddKeys()** → Writes entire header

**Total for 1 key write**:
- 2 failed lookups
- 1 full KeySet read (potentially MBs)
- 1 full KeySet write (potentially MBs)
- 1 sort of all KeySets
- 1 header write
- Multiple memory allocations

## Why Performance Degrades Over Time

1. **KeySet Growth**: As KeySets grow from 48 bytes to MBs, UpdateKeySet becomes catastrophic
2. **Header Growth**: Header size increases with KeySet count
3. **Sort Complexity**: More KeySets = longer sort time on every write
4. **Cache Misses**: Larger working set exceeds cache capacity
5. **GC Pressure**: Constant large allocations trigger frequent GC

## The Smoking Gun

The **UpdateKeySet()** function is the primary culprit. It:
- Reads/writes entire KeySet for single key addition
- Triggers unnecessary sort of all KeySets
- Allocates massive buffers repeatedly
- Has O(n) complexity where n is KeySet size

This explains why:
- Performance drops 80% as KeySets grow
- Write speed varies wildly (depends on KeySet being updated)
- Memory usage explodes over time

## Fix Priority

1. **URGENT**: Fix UpdateKeySet() to append-only writes
2. **URGENT**: Remove OffsetSort() from write path
3. **HIGH**: Add bloom filters to avoid lookups
4. **HIGH**: Implement write buffering
5. **MEDIUM**: Fix cache invalidation logic
6. **MEDIUM**: Use memory pools for allocations