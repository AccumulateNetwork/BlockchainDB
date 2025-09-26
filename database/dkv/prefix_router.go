package dkv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
)

// PrefixRouter manages file organization based on key prefixes
// This is particularly useful for hash keys to distribute them evenly
type PrefixRouter struct {
	directory    string
	prefixDepth  int // Number of bytes to use for prefix (1-4)
	buckets      map[string]*PrefixBucket
	mu           sync.RWMutex
}

// PrefixBucket represents a collection of files for a specific prefix range
type PrefixBucket struct {
	prefix      string
	minKey      [32]byte
	maxKey      [32]byte
	tree        *TreeDKV
	entryCount  int64
	sizeBytes   int64
}

// NewPrefixRouter creates a new prefix-based router
func NewPrefixRouter(directory string, prefixDepth int) (*PrefixRouter, error) {
	if prefixDepth < 1 || prefixDepth > 4 {
		prefixDepth = 2 // Default to 2 bytes (65536 possible buckets)
	}

	pr := &PrefixRouter{
		directory:   directory,
		prefixDepth: prefixDepth,
		buckets:     make(map[string]*PrefixBucket),
	}

	// Initialize buckets based on existing files
	if err := pr.initializeBuckets(); err != nil {
		return nil, err
	}

	return pr, nil
}

// Put routes a key to the appropriate bucket based on its prefix
func (pr *PrefixRouter) Put(key [32]byte, value []byte) error {
	prefix := pr.getPrefix(key)

	pr.mu.Lock()
	bucket, exists := pr.buckets[prefix]
	if !exists {
		// Create new bucket
		bucket = pr.createBucket(prefix)
		pr.buckets[prefix] = bucket
	}
	pr.mu.Unlock()

	// Update bucket stats
	bucket.entryCount++
	bucket.sizeBytes += int64(32 + len(value))

	// Route to appropriate tree
	return bucket.tree.Put(key, value)
}

// Get retrieves a value by routing to the correct bucket
func (pr *PrefixRouter) Get(key [32]byte) ([]byte, bool, error) {
	prefix := pr.getPrefix(key)

	pr.mu.RLock()
	bucket, exists := pr.buckets[prefix]
	pr.mu.RUnlock()

	if !exists {
		return nil, false, nil
	}

	return bucket.tree.Get(key)
}

// getPrefix extracts the routing prefix from a key
func (pr *PrefixRouter) getPrefix(key [32]byte) string {
	return hex.EncodeToString(key[:pr.prefixDepth])
}

// createBucket creates a new bucket for a specific prefix
func (pr *PrefixRouter) createBucket(prefix string) *PrefixBucket {
	bucketDir := filepath.Join(pr.directory, "prefix_"+prefix)
	tree, _ := NewTreeDKV(bucketDir)

	// Calculate theoretical min/max keys for this prefix
	minKey, maxKey := pr.calculateKeyRange(prefix)

	return &PrefixBucket{
		prefix: prefix,
		minKey: minKey,
		maxKey: maxKey,
		tree:   tree,
	}
}

// calculateKeyRange determines the min and max keys for a prefix
func (pr *PrefixRouter) calculateKeyRange(prefix string) ([32]byte, [32]byte) {
	prefixBytes, _ := hex.DecodeString(prefix)

	var minKey, maxKey [32]byte
	copy(minKey[:], prefixBytes)
	copy(maxKey[:], prefixBytes)

	// Fill remaining bytes
	for i := pr.prefixDepth; i < 32; i++ {
		minKey[i] = 0x00
		maxKey[i] = 0xFF
	}

	return minKey, maxKey
}

// initializeBuckets loads existing buckets from disk
func (pr *PrefixRouter) initializeBuckets() error {
	// Scan directory for existing prefix buckets
	// Implementation would check for existing prefix_* directories
	return nil
}

// HierarchicalRouter provides multi-level routing for better organization
type HierarchicalRouter struct {
	levels      []RouterLevel
	defaultTree *TreeDKV
}

// RouterLevel represents a level in the hierarchy
type RouterLevel struct {
	name        string
	keyExtractor func([32]byte) string
	routers     map[string]*TreeDKV
	mu          sync.RWMutex
}

// NewHierarchicalRouter creates a multi-level routing system
func NewHierarchicalRouter(directory string) (*HierarchicalRouter, error) {
	defaultTree, err := NewTreeDKV(filepath.Join(directory, "default"))
	if err != nil {
		return nil, err
	}

	hr := &HierarchicalRouter{
		levels:      make([]RouterLevel, 0),
		defaultTree: defaultTree,
	}

	// Add default levels
	hr.AddLevel("prefix2", func(key [32]byte) string {
		return hex.EncodeToString(key[:2])
	})

	hr.AddLevel("prefix4", func(key [32]byte) string {
		return hex.EncodeToString(key[:4])
	})

	return hr, nil
}

// AddLevel adds a routing level to the hierarchy
func (hr *HierarchicalRouter) AddLevel(name string, extractor func([32]byte) string) {
	hr.levels = append(hr.levels, RouterLevel{
		name:         name,
		keyExtractor: extractor,
		routers:      make(map[string]*TreeDKV),
	})
}

// Route determines the best tree for a given key
func (hr *HierarchicalRouter) Route(key [32]byte) *TreeDKV {
	// Try each level in order
	for _, level := range hr.levels {
		routeKey := level.keyExtractor(key)

		level.mu.RLock()
		tree, exists := level.routers[routeKey]
		level.mu.RUnlock()

		if exists {
			return tree
		}
	}

	// Fall back to default tree
	return hr.defaultTree
}

// ShardedDKV distributes keys across multiple trees using consistent hashing
type ShardedDKV struct {
	shards      []*TreeDKV
	numShards   int
	shardMask   uint32
	directory   string
}

// NewShardedDKV creates a sharded DKV store
func NewShardedDKV(directory string, numShards int) (*ShardedDKV, error) {
	// Ensure numShards is a power of 2
	actualShards := 1
	for actualShards < numShards {
		actualShards *= 2
	}

	sd := &ShardedDKV{
		shards:    make([]*TreeDKV, actualShards),
		numShards: actualShards,
		shardMask: uint32(actualShards - 1),
		directory: directory,
	}

	// Initialize shards
	for i := 0; i < actualShards; i++ {
		shardDir := filepath.Join(directory, fmt.Sprintf("shard_%04d", i))
		tree, err := NewTreeDKV(shardDir)
		if err != nil {
			return nil, err
		}
		sd.shards[i] = tree
	}

	return sd, nil
}

// getShard determines which shard a key belongs to
func (sd *ShardedDKV) getShard(key [32]byte) *TreeDKV {
	// Use first 4 bytes of key as hash
	hash := uint32(key[0])<<24 | uint32(key[1])<<16 | uint32(key[2])<<8 | uint32(key[3])
	shardIdx := hash & sd.shardMask
	return sd.shards[shardIdx]
}

// Put stores a key-value pair in the appropriate shard
func (sd *ShardedDKV) Put(key [32]byte, value []byte) error {
	return sd.getShard(key).Put(key, value)
}

// Get retrieves a value from the appropriate shard
func (sd *ShardedDKV) Get(key [32]byte) ([]byte, bool, error) {
	return sd.getShard(key).Get(key)
}

// Delete removes a key from the appropriate shard
func (sd *ShardedDKV) Delete(key [32]byte) error {
	return sd.getShard(key).Delete(key)
}

// NewIterator creates a merged iterator across all shards
func (sd *ShardedDKV) NewIterator() *MergedIterator {
	var iters []*Iterator
	for _, shard := range sd.shards {
		iters = append(iters, shard.NewIterator())
	}
	return NewMergedIterator(iters)
}

// MergedIterator combines multiple iterators into one sorted stream
type MergedIterator struct {
	iterators   []*Iterator
	heap        *IteratorHeap
	current     KVPair
}

// NewMergedIterator creates a merged iterator
func NewMergedIterator(iters []*Iterator) *MergedIterator {
	mi := &MergedIterator{
		iterators: iters,
		heap:      NewIteratorHeap(),
	}

	// Initialize heap
	for i, iter := range iters {
		if iter.Next() {
			mi.heap.Push(&HeapEntry{
				Key:    iter.Key(),
				Value:  iter.Value(),
				Source: i,
			})
		}
	}

	return mi
}

// Next advances to the next key-value pair
func (mi *MergedIterator) Next() bool {
	if mi.heap.Len() == 0 {
		return false
	}

	entry := mi.heap.Pop()
	mi.current = KVPair{
		Key:   entry.Key,
		Value: entry.Value,
	}

	// Advance source iterator
	if mi.iterators[entry.Source].Next() {
		mi.heap.Push(&HeapEntry{
			Key:    mi.iterators[entry.Source].Key(),
			Value:  mi.iterators[entry.Source].Value(),
			Source: entry.Source,
		})
	}

	return true
}

// Key returns the current key
func (mi *MergedIterator) Key() [32]byte {
	return mi.current.Key
}

// Value returns the current value
func (mi *MergedIterator) Value() []byte {
	return mi.current.Value
}

// Close closes all underlying iterators
func (mi *MergedIterator) Close() error {
	for _, iter := range mi.iterators {
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

// AdaptiveRouter dynamically adjusts routing based on access patterns
type AdaptiveRouter struct {
	hotKeys     map[[32]byte]*HotKeyInfo
	coldStorage *TreeDKV
	hotStorage  *TreeDKV
	accessCount sync.Map
	mu          sync.RWMutex
}

// HotKeyInfo tracks access patterns for frequently accessed keys
type HotKeyInfo struct {
	key         [32]byte
	accessCount int64
	lastAccess  int64
	size        int64
}

// NewAdaptiveRouter creates a router that adapts to access patterns
func NewAdaptiveRouter(directory string) (*AdaptiveRouter, error) {
	coldTree, err := NewTreeDKV(filepath.Join(directory, "cold"))
	if err != nil {
		return nil, err
	}

	hotTree, err := NewTreeDKV(filepath.Join(directory, "hot"))
	if err != nil {
		return nil, err
	}

	return &AdaptiveRouter{
		hotKeys:     make(map[[32]byte]*HotKeyInfo),
		coldStorage: coldTree,
		hotStorage:  hotTree,
	}, nil
}

// Get retrieves a value and updates access statistics
func (ar *AdaptiveRouter) Get(key [32]byte) ([]byte, bool, error) {
	// Update access count
	count, _ := ar.accessCount.LoadOrStore(key, int64(0))
	newCount := count.(int64) + 1
	ar.accessCount.Store(key, newCount)

	// Check if key should be promoted to hot storage
	if newCount > 10 {
		ar.promoteToHot(key)
	}

	// Try hot storage first
	if value, found, err := ar.hotStorage.Get(key); found {
		return value, true, err
	}

	// Fall back to cold storage
	return ar.coldStorage.Get(key)
}

// promoteToHot moves a key from cold to hot storage
func (ar *AdaptiveRouter) promoteToHot(key [32]byte) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	// Check if already in hot storage
	if _, exists := ar.hotKeys[key]; exists {
		return
	}

	// Move from cold to hot
	if value, found, _ := ar.coldStorage.Get(key); found {
		ar.hotStorage.Put(key, value)
		ar.coldStorage.Delete(key)

		ar.hotKeys[key] = &HotKeyInfo{
			key:         key,
			accessCount: 10,
			size:        int64(len(value)),
		}
	}
}

// RangePartitioner divides the key space into ranges
type RangePartitioner struct {
	ranges      []KeyRange
	trees       map[string]*TreeDKV
	directory   string
}

// KeyRange defines a range of keys
type KeyRange struct {
	name   string
	minKey [32]byte
	maxKey [32]byte
	tree   *TreeDKV
}

// NewRangePartitioner creates a range-based partitioner
func NewRangePartitioner(directory string) (*RangePartitioner, error) {
	rp := &RangePartitioner{
		directory: directory,
		trees:     make(map[string]*TreeDKV),
	}

	// Create default ranges (can be customized)
	rp.createDefaultRanges()

	return rp, nil
}

// createDefaultRanges sets up default key ranges
func (rp *RangePartitioner) createDefaultRanges() error {
	// Create 16 ranges based on first byte
	for i := 0; i < 16; i++ {
		rangeName := fmt.Sprintf("range_%02x", i)

		var minKey, maxKey [32]byte
		minKey[0] = byte(i * 16)
		maxKey[0] = byte((i+1)*16 - 1)
		for j := 1; j < 32; j++ {
			minKey[j] = 0x00
			maxKey[j] = 0xFF
		}

		tree, err := NewTreeDKV(filepath.Join(rp.directory, rangeName))
		if err != nil {
			return err
		}

		rp.ranges = append(rp.ranges, KeyRange{
			name:   rangeName,
			minKey: minKey,
			maxKey: maxKey,
			tree:   tree,
		})
	}

	return nil
}

// findRange locates the appropriate range for a key
func (rp *RangePartitioner) findRange(key [32]byte) *TreeDKV {
	for _, r := range rp.ranges {
		if bytes.Compare(key[:], r.minKey[:]) >= 0 &&
		   bytes.Compare(key[:], r.maxKey[:]) <= 0 {
			return r.tree
		}
	}
	// Should not happen with proper range setup
	return rp.ranges[0].tree
}

// Put stores a key in the appropriate range
func (rp *RangePartitioner) Put(key [32]byte, value []byte) error {
	return rp.findRange(key).Put(key, value)
}

// Get retrieves a value from the appropriate range
func (rp *RangePartitioner) Get(key [32]byte) ([]byte, bool, error) {
	return rp.findRange(key).Get(key)
}

// SplitRange splits a range that has grown too large
func (rp *RangePartitioner) SplitRange(rangeIdx int) error {
	if rangeIdx >= len(rp.ranges) {
		return fmt.Errorf("invalid range index")
	}

	oldRange := rp.ranges[rangeIdx]

	// Calculate midpoint
	var midKey [32]byte
	for i := 0; i < 32; i++ {
		midKey[i] = (oldRange.minKey[i] + oldRange.maxKey[i]) / 2
	}

	// Create two new ranges
	leftRange := KeyRange{
		name:   oldRange.name + "_left",
		minKey: oldRange.minKey,
		maxKey: midKey,
	}

	rightRange := KeyRange{
		name:   oldRange.name + "_right",
		minKey: midKey,
		maxKey: oldRange.maxKey,
	}

	// Create new trees
	leftTree, _ := NewTreeDKV(filepath.Join(rp.directory, leftRange.name))
	rightTree, _ := NewTreeDKV(filepath.Join(rp.directory, rightRange.name))

	leftRange.tree = leftTree
	rightRange.tree = rightTree

	// Migrate data from old range
	iter := oldRange.tree.NewIterator()
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if bytes.Compare(key[:], midKey[:]) < 0 {
			leftTree.Put(key, value)
		} else {
			rightTree.Put(key, value)
		}
	}

	// Replace old range with new ranges
	newRanges := make([]KeyRange, 0, len(rp.ranges)+1)
	for i, r := range rp.ranges {
		if i == rangeIdx {
			newRanges = append(newRanges, leftRange, rightRange)
		} else {
			newRanges = append(newRanges, r)
		}
	}
	rp.ranges = newRanges

	// Clean up old range
	oldRange.tree.Close()

	return nil
}