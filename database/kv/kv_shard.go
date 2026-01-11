package kv

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
)

// Default configuration - optimal for most systems based on 200M benchmarks
// Testing showed 1024 shards with 256 bins is 17% faster than 64 bins
const (
	DefaultNumShards        = 1024
	DefaultNumChannelGroups = 8
	DefaultWriteChannelSize = 20000
	DefaultBinCount         = 256
)

// KVShardConfig holds tunable parameters stored in the database.
type KVShardConfig struct {
	NumShards        int `json:"num_shards"`
	NumChannelGroups int `json:"num_channel_groups"`
	WriteChannelSize int `json:"write_channel_size"`
	BinCount         int `json:"bin_count"`
	ExpectedItems    int `json:"expected_items"`
}

// DefaultConfig returns optimal defaults.
func DefaultConfig(expectedItems int) KVShardConfig {
	return KVShardConfig{
		NumShards:        DefaultNumShards,
		NumChannelGroups: DefaultNumChannelGroups,
		WriteChannelSize: DefaultWriteChannelSize,
		BinCount:         DefaultBinCount,
		ExpectedItems:    expectedItems,
	}
}

// InternalKeySize is the number of bytes stored internally (160 bits).
// External API accepts [32]byte, but we only store the first 20 bytes.
// This provides collision resistance up to ~2^80 entries (birthday bound)
// and is secure against mining attacks below ~2^80 hash operations.
// Saves 12 bytes per entry (6GB at 500M entries).
const InternalKeySize = 20

// InternalKey is the truncated key type used for internal storage.
type InternalKey [InternalKeySize]byte

// truncateKey converts a 32-byte external key to a 20-byte internal key.
func truncateKey(key [32]byte) InternalKey {
	var k InternalKey
	copy(k[:], key[:InternalKeySize])
	return k
}

// writeRequest represents an async write operation
type writeRequest struct {
	shardIdx int
	key      InternalKey
	value    []byte
}

// KVShard distributes key-value pairs across multiple KV2 shards.
// Configuration is stored in config.json within the database directory.
//
// Key features:
//   - Configurable shard count (default 1024, tested up to 2048)
//   - Single mmap'd bloom filter for fast negative lookups with crash recovery
//   - Each shard has independent DynaKV (in-memory) and PermKV (disk-based)
//   - Async writes via sharded channels with configurable workers
//   - Write-through cache for pending async writes
type KVShard struct {
	Directory string
	Config    KVShardConfig
	Shards    []*KV2
	Bloom     *MmapBloomFilter

	// Async write infrastructure
	writeChans      []chan writeRequest
	writerWg        sync.WaitGroup
	stopWriters     chan struct{}
	pendingWrites   atomic.Int64
	workersPerGroup int

	// Write-through cache for pending async writes
	pendingCache []map[InternalKey][]byte
	cacheMu      []sync.RWMutex
}

// ShardIndex determines which shard a key belongs to for a given shard count.
// Uses first 2 bytes of key for distribution up to 65536 shards.
func ShardIndex(key []byte, numShards int) int {
	if len(key) == 0 {
		return 0
	}
	if numShards <= 256 {
		return int(key[0]) % numShards
	}
	// Use first 2 bytes for larger shard counts
	idx := int(key[0])<<8 | int(key[1])
	return idx % numShards
}

// ShardDir returns the directory path for a specific shard.
func (k *KVShard) ShardDir(index int) string {
	dirname := fmt.Sprintf("shard_%04d", index)
	return filepath.Join(k.Directory, dirname)
}

// saveConfig writes the configuration to the database directory.
func (k *KVShard) saveConfig() error {
	configPath := filepath.Join(k.Directory, "config.json")
	data, err := json.MarshalIndent(k.Config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath, data, 0644)
}

// loadConfig reads the configuration from the database directory.
func loadConfig(directory string) (KVShardConfig, error) {
	configPath := filepath.Join(directory, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		return KVShardConfig{}, err
	}
	var config KVShardConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return KVShardConfig{}, err
	}
	return config, nil
}

// NewKVShard creates a new sharded key-value store with the given configuration.
// The configuration is saved to config.json in the database directory.
func NewKVShard(directory string, config KVShardConfig) (*KVShard, error) {
	if err := os.RemoveAll(directory); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	// Create mmap'd bloom filter
	bloom, _, err := NewMmapBloomFilter(directory, config.ExpectedItems, 0.01)
	if err != nil {
		return nil, fmt.Errorf("failed to create bloom filter: %w", err)
	}

	// Workers per group: distribute NumCPU across channel groups
	workersPerGroup := runtime.NumCPU() / config.NumChannelGroups
	if workersPerGroup < 2 {
		workersPerGroup = 2
	}

	kvs := &KVShard{
		Directory:       directory,
		Config:          config,
		Bloom:           bloom,
		stopWriters:     make(chan struct{}),
		workersPerGroup: workersPerGroup,
		Shards:          make([]*KV2, config.NumShards),
		writeChans:      make([]chan writeRequest, config.NumChannelGroups),
		pendingCache:    make([]map[InternalKey][]byte, config.NumShards),
		cacheMu:         make([]sync.RWMutex, config.NumShards),
	}

	// Initialize sharded channels
	for i := 0; i < config.NumChannelGroups; i++ {
		kvs.writeChans[i] = make(chan writeRequest, config.WriteChannelSize)
	}

	// Initialize pending caches
	for i := 0; i < config.NumShards; i++ {
		kvs.pendingCache[i] = make(map[InternalKey][]byte)
	}

	// Create all shards
	for i := 0; i < config.NumShards; i++ {
		shardDir := kvs.ShardDir(i)
		var err error
		if kvs.Shards[i], err = NewKV2(shardDir, int32(config.BinCount)); err != nil {
			// Cleanup on failure
			for j := 0; j < i; j++ {
				kvs.Shards[j].Close()
			}
			return nil, err
		}
	}

	// Save configuration
	if err := kvs.saveConfig(); err != nil {
		return nil, fmt.Errorf("failed to save config: %w", err)
	}

	// Start async writers
	kvs.startAsyncWriters()

	return kvs, nil
}

// OpenKVShard opens an existing sharded key-value store.
// Configuration is loaded from config.json in the database directory.
// If the bloom filter's dirty flag is set (crash recovery), it writes the
// recovered bloom filter to the database before continuing.
func OpenKVShard(directory string) (*KVShard, error) {
	// Load configuration from database
	config, err := loadConfig(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Workers per group: distribute NumCPU across channel groups
	workersPerGroup := runtime.NumCPU() / config.NumChannelGroups
	if workersPerGroup < 2 {
		workersPerGroup = 2
	}

	kvs := &KVShard{
		Directory:       directory,
		Config:          config,
		stopWriters:     make(chan struct{}),
		workersPerGroup: workersPerGroup,
		Shards:          make([]*KV2, config.NumShards),
		writeChans:      make([]chan writeRequest, config.NumChannelGroups),
		pendingCache:    make([]map[InternalKey][]byte, config.NumShards),
		cacheMu:         make([]sync.RWMutex, config.NumShards),
	}

	// Initialize sharded channels
	for i := 0; i < config.NumChannelGroups; i++ {
		kvs.writeChans[i] = make(chan writeRequest, config.WriteChannelSize)
	}

	// Initialize pending caches
	for i := 0; i < config.NumShards; i++ {
		kvs.pendingCache[i] = make(map[InternalKey][]byte)
	}

	// Open mmap'd bloom filter (handles crash recovery)
	bloom, needsRecovery, err := NewMmapBloomFilter(directory, config.ExpectedItems, 0.01)
	if err != nil {
		return nil, fmt.Errorf("failed to open bloom filter: %w", err)
	}
	kvs.Bloom = bloom

	// If dirty (crash recovery), write recovered bloom filter to database
	if needsRecovery {
		if err := bloom.WriteToDatabase(directory); err != nil {
			return nil, fmt.Errorf("failed to write recovered bloom filter: %w", err)
		}
	}

	// Open all shards
	for i := 0; i < config.NumShards; i++ {
		shardDir := kvs.ShardDir(i)
		var err error
		if kvs.Shards[i], err = OpenKV2(shardDir); err != nil {
			// Cleanup on failure
			for j := 0; j < i; j++ {
				kvs.Shards[j].Close()
			}
			return nil, err
		}
	}

	// Start async writers
	kvs.startAsyncWriters()

	return kvs, nil
}

// PutDyna stores a value in the DynaKV tier (in-memory).
// Use for data that may change (accounts, state).
func (k *KVShard) PutDyna(key [32]byte, value []byte) error {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)

	// Add to bloom filter (lock-free operation)
	k.Bloom.Add(ikey)

	_, err := k.Shards[index].PutDyna(ikey, value)
	return err
}

// PutPerm stores a value in the PermKV tier (disk-based).
// Use for data that won't change (transactions, blocks).
// This is synchronous - waits for write to complete.
func (k *KVShard) PutPerm(key [32]byte, value []byte) error {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)

	// Add to bloom filter (lock-free operation)
	k.Bloom.Add(ikey)

	_, err := k.Shards[index].PutPerm(ikey, value)
	return err
}

// PutPermAsync stores a value in the PermKV tier asynchronously.
// Returns immediately after queueing the write. Use Flush() to ensure
// all pending writes are complete. Ideal for transaction data.
//
// The value is added to a write-through cache so reads can find it
// immediately without flushing. The cache entry is removed once the
// background writer persists the value to disk.
//
// Bloom filter add is deferred to the background worker to minimize
// latency on the hot path. Reads check cache first to handle this.
func (k *KVShard) PutPermAsync(key [32]byte, value []byte) {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)

	// Make a copy of value since caller may reuse the buffer
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// Add to write-through cache (so reads can find it immediately)
	// This must happen BEFORE queueing since bloom add is now deferred
	k.cacheMu[index].Lock()
	k.pendingCache[index][ikey] = valueCopy
	k.cacheMu[index].Unlock()

	// Route to channel group based on shard index
	// This distributes load across channels, reducing contention
	shardsPerGroup := k.Config.NumShards / k.Config.NumChannelGroups
	groupIdx := index / shardsPerGroup
	k.pendingWrites.Add(1)
	k.writeChans[groupIdx] <- writeRequest{shardIdx: index, key: ikey, value: valueCopy}
}

// startAsyncWriters initializes and starts background writer goroutines.
// Each channel group gets workersPerGroup workers.
func (k *KVShard) startAsyncWriters() {
	for g := 0; g < k.Config.NumChannelGroups; g++ {
		for w := 0; w < k.workersPerGroup; w++ {
			k.writerWg.Add(1)
			go k.batchWriter(g)
		}
	}
}

// batchWriter is a background goroutine that processes writes from a specific channel group.
// Multiple workers share each channel group, reducing per-channel contention.
// Bloom filter add is done here (after channel read) to minimize hot-path latency.
func (k *KVShard) batchWriter(groupIdx int) {
	defer k.writerWg.Done()

	ch := k.writeChans[groupIdx]

	// Process writes as they come
	for {
		select {
		case req := <-ch:
			// Add to bloom filter (lock-free, done here to reduce hot-path latency)
			k.Bloom.Add(req.key)

			// Process the write
			k.Shards[req.shardIdx].PutPerm(req.key, req.value)

			// Remove from pending cache after write completes
			k.cacheMu[req.shardIdx].Lock()
			delete(k.pendingCache[req.shardIdx], req.key)
			k.cacheMu[req.shardIdx].Unlock()

			k.pendingWrites.Add(-1)

		case <-k.stopWriters:
			// Drain remaining writes before exiting
			for {
				select {
				case req := <-ch:
					k.Bloom.Add(req.key)
					k.Shards[req.shardIdx].PutPerm(req.key, req.value)
					k.cacheMu[req.shardIdx].Lock()
					delete(k.pendingCache[req.shardIdx], req.key)
					k.cacheMu[req.shardIdx].Unlock()
					k.pendingWrites.Add(-1)
				default:
					return
				}
			}
		}
	}
}

// Flush waits for all pending async writes to complete.
func (k *KVShard) Flush() {
	// Spin-wait for all pending writes to complete
	for k.pendingWrites.Load() > 0 {
		runtime.Gosched()
	}
}

// PendingWrites returns the total number of pending async writes.
func (k *KVShard) PendingWrites() int64 {
	return k.pendingWrites.Load()
}

// Put stores a value using automatic tier detection.
// New keys go to PermKV; changed values migrate to DynaKV.
func (k *KVShard) Put(key [32]byte, value []byte) error {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)

	// Add to bloom filter (lock-free operation)
	k.Bloom.Add(ikey)

	_, err := k.Shards[index].Put(ikey, value)
	return err
}

// GetDyna retrieves a value from DynaKV only.
func (k *KVShard) GetDyna(key [32]byte) ([]byte, error) {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)
	return k.Shards[index].GetDyna(ikey)
}

// DeleteDyna deletes a key from DynaKV.
func (k *KVShard) DeleteDyna(key [32]byte) error {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)
	return k.Shards[index].DynaKV.Delete(ikey)
}

// GetPerm retrieves a value from PermKV only.
// Checks the write-through cache first for pending async writes,
// then falls back to disk. No flush is needed.
func (k *KVShard) GetPerm(key [32]byte) ([]byte, error) {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)

	// Check write-through cache first (pending async writes)
	// Must check before bloom since bloom add is deferred to async workers
	k.cacheMu[index].RLock()
	if value, ok := k.pendingCache[index][ikey]; ok {
		// Make a copy since cache entry may be removed
		result := make([]byte, len(value))
		copy(result, value)
		k.cacheMu[index].RUnlock()
		return result, nil
	}
	k.cacheMu[index].RUnlock()

	// Fast bloom filter check (lock-free operation)
	if !k.Bloom.MayContain(ikey) {
		return nil, fmt.Errorf("key not found (bloom filter)")
	}

	// Not in cache, read from disk
	return k.Shards[index].GetPerm(ikey)
}

// Get retrieves a value, checking DynaKV first, then cache, then PermKV.
func (k *KVShard) Get(key [32]byte) ([]byte, error) {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)
	shard := k.Shards[index]

	// Fast path: Check DynaKV first (in-memory)
	if value, err := shard.GetDyna(ikey); err == nil {
		return value, nil
	}

	// Check write-through cache (pending async writes)
	// Must check before bloom since bloom add is deferred to async workers
	k.cacheMu[index].RLock()
	if value, ok := k.pendingCache[index][ikey]; ok {
		result := make([]byte, len(value))
		copy(result, value)
		k.cacheMu[index].RUnlock()
		return result, nil
	}
	k.cacheMu[index].RUnlock()

	// Bloom filter check before disk lookup (lock-free operation)
	if !k.Bloom.MayContain(ikey) {
		return nil, fmt.Errorf("key not found")
	}

	// Slow path: Check PermKV (disk-based)
	return shard.GetPerm(ikey)
}

// Has checks if a key exists (fast bloom filter check).
func (k *KVShard) Has(key [32]byte) bool {
	exists, _ := k.HasWithStats(key)
	return exists
}

// HasWithStats checks if a key exists and returns bloom filter statistics.
// Returns (exists, bloomFilterHit) where bloomFilterHit is true if the
// bloom filter definitively said the key doesn't exist (no disk access needed).
func (k *KVShard) HasWithStats(key [32]byte) (exists bool, bloomHit bool) {
	ikey := truncateKey(key)
	index := ShardIndex(key[:], k.Config.NumShards)

	// Check write-through cache first (pending async writes)
	// Must check before bloom since bloom add is deferred to async workers
	k.cacheMu[index].RLock()
	_, inCache := k.pendingCache[index][ikey]
	k.cacheMu[index].RUnlock()
	if inCache {
		return true, false
	}

	// Bloom filter check (lock-free operation)
	if !k.Bloom.MayContain(ikey) {
		// Bloom filter says definitely not present
		return false, true
	}

	// Bloom said "maybe present", verify in actual store
	return k.Shards[index].Has(ikey), false
}

// ForEachDyna iterates over all DynaKV keys with the given prefix across all shards.
func (k *KVShard) ForEachDyna(prefix []byte, fn func(key, value []byte) error) error {
	for _, shard := range k.Shards {
		if err := shard.ForEachDyna(prefix, fn); err != nil {
			return err
		}
	}
	return nil
}

// Compress compacts all shards.
func (k *KVShard) Compress() error {
	for _, shard := range k.Shards {
		if err := shard.Compress(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all shards and performs controlled shutdown of the bloom filter.
// The bloom filter is marked clean and written to the database.
func (k *KVShard) Close() error {
	// Stop async writers and wait for them to finish
	close(k.stopWriters)
	k.writerWg.Wait()

	// Close bloom filter (marks clean, writes to database, unmaps)
	if err := k.Bloom.Close(k.Directory); err != nil {
		return err
	}

	// Close all shards
	for _, shard := range k.Shards {
		if err := shard.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Stats returns statistics about all shards.
func (k *KVShard) Stats() KVShardStats {
	stats := KVShardStats{
		ShardStats: make([]KV2Stats, k.Config.NumShards),
	}

	for i, shard := range k.Shards {
		stats.ShardStats[i] = shard.Stats()
		stats.TotalDynaEntries += stats.ShardStats[i].DynaEntries
		stats.TotalPermWrites += stats.ShardStats[i].PermWrites
	}

	return stats
}

// KVShardStats contains statistics for all shards.
type KVShardStats struct {
	TotalDynaEntries uint64
	TotalPermWrites  uint64
	ShardStats       []KV2Stats
}

// BloomFilter implements a space-efficient probabilistic data structure.
type BloomFilter struct {
	bits      []byte
	size      uint32
	hashCount uint32
}

// NewBloomFilter creates a bloom filter with optimal parameters.
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	m := optimalBitSize(expectedItems, falsePositiveRate)
	k := optimalHashCount(expectedItems, m)

	return &BloomFilter{
		bits:      make([]byte, (m+7)/8),
		size:      uint32(m),
		hashCount: uint32(k),
	}
}

// NewBloomFilterFromBytes reconstructs a bloom filter from serialized bytes.
func NewBloomFilterFromBytes(data []byte) *BloomFilter {
	if len(data) < 8 {
		return NewBloomFilter(1000000, 0.01)
	}

	size := binary.BigEndian.Uint32(data[0:4])
	hashCount := binary.BigEndian.Uint32(data[4:8])

	bf := &BloomFilter{
		bits:      make([]byte, len(data)-8),
		size:      size,
		hashCount: hashCount,
	}
	copy(bf.bits, data[8:])

	return bf
}

// Add inserts a key into the bloom filter.
func (bf *BloomFilter) Add(key InternalKey) {
	h1, h2 := bf.hash(key[:])

	for i := uint32(0); i < bf.hashCount; i++ {
		pos := (h1 + i*h2) % bf.size
		byteIdx := pos / 8
		bitIdx := pos % 8

		if byteIdx < uint32(len(bf.bits)) {
			bf.bits[byteIdx] |= 1 << bitIdx
		}
	}
}

// MayContain checks if a key might be in the set.
func (bf *BloomFilter) MayContain(key InternalKey) bool {
	h1, h2 := bf.hash(key[:])

	for i := uint32(0); i < bf.hashCount; i++ {
		pos := (h1 + i*h2) % bf.size
		byteIdx := pos / 8
		bitIdx := pos % 8

		if byteIdx >= uint32(len(bf.bits)) {
			return false
		}

		if bf.bits[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}

	return true
}

// Serialize converts the bloom filter to bytes for storage.
func (bf *BloomFilter) Serialize() []byte {
	result := make([]byte, 8+len(bf.bits))

	binary.BigEndian.PutUint32(result[0:4], bf.size)
	binary.BigEndian.PutUint32(result[4:8], bf.hashCount)
	copy(result[8:], bf.bits)

	return result
}

// hash computes two hash values for double hashing using inline FNV.
// Zero allocations - FNV-1a and FNV-1 computed directly.
func (bf *BloomFilter) hash(key []byte) (uint32, uint32) {
	// FNV-1a
	h1 := uint32(2166136261)
	for _, b := range key {
		h1 ^= uint32(b)
		h1 *= 16777619
	}

	// FNV-1 (different order of xor/multiply)
	h2 := uint32(2166136261)
	for _, b := range key {
		h2 *= 16777619
		h2 ^= uint32(b)
	}

	return h1, h2
}

// optimalBitSize calculates optimal bit array size.
func optimalBitSize(n int, p float64) int {
	m := -float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	return int(math.Ceil(m))
}

// optimalHashCount calculates optimal number of hash functions.
func optimalHashCount(n int, m int) int {
	k := float64(m) / float64(n) * math.Ln2
	return int(math.Max(1, math.Round(k)))
}

// MmapBloomFilter provides a memory-mapped bloom filter with crash recovery.
//
// File format:
//   [header: 20 bytes]
//     - magic: 4 bytes ("BLFM")
//     - version: 4 bytes
//     - size (m): 4 bytes
//     - hashCount (k): 4 bytes
//     - dirty flag: 1 byte (1=in-use, 0=clean)
//     - reserved: 3 bytes
//   [bits: variable]
//
// On crash, the dirty flag remains 1, signaling recovery is needed.
// Recovery reads the mmap file (which has latest state) and writes to database.
type MmapBloomFilter struct {
	path      string
	file      *os.File
	data      []byte // mmap'd data
	size      uint32
	hashCount uint32
	mu        sync.RWMutex
}

const (
	mmapBloomMagic      = "BLFM"
	mmapBloomVersion    = 1
	mmapBloomHeaderSize = 20 // 4 + 4 + 4 + 4 + 1 + 3
)

// NewMmapBloomFilter creates a new memory-mapped bloom filter.
// If the mmap file exists and is dirty (crash recovery), returns needsRecovery=true.
func NewMmapBloomFilter(directory string, expectedItems int, falsePositiveRate float64) (*MmapBloomFilter, bool, error) {
	mmapPath := filepath.Join(directory, "bloom.mmap")

	// Check if mmap file exists
	if info, err := os.Stat(mmapPath); err == nil && info.Size() >= mmapBloomHeaderSize {
		// File exists - check if dirty (crash recovery needed)
		return openMmapBloomFilter(mmapPath)
	}

	// Create new mmap bloom filter
	return createMmapBloomFilter(mmapPath, expectedItems, falsePositiveRate)
}

// createMmapBloomFilter creates a new mmap bloom filter file.
func createMmapBloomFilter(path string, expectedItems int, falsePositiveRate float64) (*MmapBloomFilter, bool, error) {
	m := optimalBitSize(expectedItems, falsePositiveRate)
	k := optimalHashCount(expectedItems, m)
	bitsSize := (m + 7) / 8
	fileSize := mmapBloomHeaderSize + bitsSize

	// Create file
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, false, err
	}

	// Truncate to required size
	if err := file.Truncate(int64(fileSize)); err != nil {
		file.Close()
		return nil, false, err
	}

	// Memory map
	data, err := syscall.Mmap(int(file.Fd()), 0, fileSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, false, err
	}

	// Write header
	copy(data[0:4], mmapBloomMagic)
	binary.BigEndian.PutUint32(data[4:8], mmapBloomVersion)
	binary.BigEndian.PutUint32(data[8:12], uint32(m))
	binary.BigEndian.PutUint32(data[12:16], uint32(k))
	data[16] = 1 // dirty = in-use

	// Sync to disk
	file.Sync()

	return &MmapBloomFilter{
		path:      path,
		file:      file,
		data:      data,
		size:      uint32(m),
		hashCount: uint32(k),
	}, false, nil
}

// openMmapBloomFilter opens an existing mmap bloom filter.
// Returns needsRecovery=true if the dirty flag is set.
func openMmapBloomFilter(path string) (*MmapBloomFilter, bool, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, false, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, false, err
	}

	// Memory map
	data, err := syscall.Mmap(int(file.Fd()), 0, int(info.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, false, err
	}

	// Validate header
	if string(data[0:4]) != mmapBloomMagic {
		syscall.Munmap(data)
		file.Close()
		return nil, false, fmt.Errorf("invalid mmap bloom filter magic")
	}

	version := binary.BigEndian.Uint32(data[4:8])
	if version != mmapBloomVersion {
		syscall.Munmap(data)
		file.Close()
		return nil, false, fmt.Errorf("unsupported mmap bloom filter version: %d", version)
	}

	size := binary.BigEndian.Uint32(data[8:12])
	hashCount := binary.BigEndian.Uint32(data[12:16])
	dirty := data[16] == 1

	// If dirty, need recovery (but filter is usable - mmap has latest state)
	needsRecovery := dirty

	// Mark as in-use (write directly to file for durability)
	data[16] = 1
	file.WriteAt([]byte{1}, 16)
	file.Sync()

	return &MmapBloomFilter{
		path:      path,
		file:      file,
		data:      data,
		size:      size,
		hashCount: hashCount,
	}, needsRecovery, nil
}

// Add inserts a key into the bloom filter.
// Changes are immediately visible in the mmap'd file.
// Lock-free: bit-OR operations are idempotent and safe for concurrent access.
func (bf *MmapBloomFilter) Add(key InternalKey) {
	h1, h2 := bf.hash(key[:])

	for i := uint32(0); i < bf.hashCount; i++ {
		pos := (h1 + i*h2) % bf.size
		byteIdx := mmapBloomHeaderSize + int(pos/8)
		bitIdx := pos % 8

		if byteIdx < len(bf.data) {
			// Atomic bit-OR: safe for concurrent access
			// Even if two goroutines set the same bit simultaneously, result is correct
			bf.data[byteIdx] |= 1 << bitIdx
		}
	}
	// No explicit sync needed - mmap with MAP_SHARED will persist
}

// MayContain checks if a key might be in the set.
// Lock-free: reading bits while other goroutines write is safe.
// Worst case: might miss a very recently added key (acceptable for bloom).
func (bf *MmapBloomFilter) MayContain(key InternalKey) bool {
	h1, h2 := bf.hash(key[:])

	for i := uint32(0); i < bf.hashCount; i++ {
		pos := (h1 + i*h2) % bf.size
		byteIdx := mmapBloomHeaderSize + int(pos/8)
		bitIdx := pos % 8

		if byteIdx >= len(bf.data) {
			return false
		}

		if bf.data[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}

	return true
}

// Serialize returns the bloom filter data for writing to the database.
// Format matches BloomFilter.Serialize() for compatibility.
func (bf *MmapBloomFilter) Serialize() []byte {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	bitsStart := mmapBloomHeaderSize
	bits := bf.data[bitsStart:]

	result := make([]byte, 8+len(bits))
	binary.BigEndian.PutUint32(result[0:4], bf.size)
	binary.BigEndian.PutUint32(result[4:8], bf.hashCount)
	copy(result[8:], bits)

	return result
}

// WriteToDatabase saves the bloom filter to the database file.
// Called during controlled shutdown and crash recovery.
func (bf *MmapBloomFilter) WriteToDatabase(directory string) error {
	bloomPath := filepath.Join(directory, "bloom.dat")
	return os.WriteFile(bloomPath, bf.Serialize(), 0644)
}

// MarkClean sets the dirty flag to 0, indicating clean shutdown.
func (bf *MmapBloomFilter) MarkClean() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	bf.data[16] = 0
	bf.file.WriteAt([]byte{0}, 16)
	bf.file.Sync()
}

// Close performs controlled shutdown:
// 1. Marks the mmap file as clean
// 2. Writes bloom filter to database
// 3. Unmaps and closes file
func (bf *MmapBloomFilter) Close(directory string) error {
	// Mark clean before writing to database
	bf.MarkClean()

	// Write to database
	if err := bf.WriteToDatabase(directory); err != nil {
		return err
	}

	// Unmap
	if bf.data != nil {
		if err := syscall.Munmap(bf.data); err != nil {
			return err
		}
		bf.data = nil
	}

	// Close file
	if bf.file != nil {
		return bf.file.Close()
	}

	return nil
}

// hash computes two hash values for double hashing using inline FNV.
// Zero allocations - FNV-1a and FNV-1 computed directly.
func (bf *MmapBloomFilter) hash(key []byte) (uint32, uint32) {
	// FNV-1a
	h1 := uint32(2166136261)
	for _, b := range key {
		h1 ^= uint32(b)
		h1 *= 16777619
	}

	// FNV-1 (different order of xor/multiply)
	h2 := uint32(2166136261)
	for _, b := range key {
		h2 *= 16777619
		h2 ^= uint32(b)
	}

	return h1, h2
}
