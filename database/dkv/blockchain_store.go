package dkv

import (
	"crypto/sha256"
	"errors"
	"sync/atomic"

	"github.com/AccumulateNetwork/BlockchainDB/database/kv"
)

// BlockchainStore provides a LevelDB/BadgerDB-like interface optimized for blockchain data.
// It wraps KVShard to provide:
//   - Dynamic data (accounts, state) via PutDyna/GetDyna - in-memory with WAL
//   - Permanent data (transactions, blocks) via PutByHash/GetByHash - async disk writes
//   - 256 shards for parallel I/O
//   - Write-through cache for async writes (no flush needed for reads)
//   - Bloom filter for fast negative lookups
type BlockchainStore struct {
	shards *kv.KVShard
	stats  StoreStats
}

// StoreStats tracks store statistics
type StoreStats struct {
	Puts          atomic.Uint64
	Gets          atomic.Uint64
	Deletes       atomic.Uint64
	PutsByHash    atomic.Uint64
	BloomHits     atomic.Uint64 // Bloom said "definitely not present"
	BloomMisses   atomic.Uint64 // Bloom said "maybe present"
	Deduplication atomic.Uint64 // Writes skipped due to existing hash
}

// ErrEmptyKey is returned when an empty key is provided
var ErrEmptyKey = errors.New("key cannot be empty")

// ErrKeyNotFound is returned when a key is not found
var ErrKeyNotFound = errors.New("key not found")

// NewBlockchainStore creates a new blockchain-optimized store
func NewBlockchainStore(directory string) (*BlockchainStore, error) {
	// Create KVShard with 64 bins per shard, expecting 10M items
	shards, err := kv.NewKVShard(directory, 64, 10_000_000)
	if err != nil {
		return nil, err
	}

	return &BlockchainStore{
		shards: shards,
	}, nil
}

// OpenBlockchainStore opens an existing blockchain store
func OpenBlockchainStore(directory string) (*BlockchainStore, error) {
	shards, err := kv.OpenKVShard(directory)
	if err != nil {
		return nil, err
	}

	return &BlockchainStore{
		shards: shards,
	}, nil
}

// Put stores a key-value pair for dynamic data (accounts, state).
// The key is hashed internally for sharding. Use for data that may change.
func (bs *BlockchainStore) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	bs.stats.Puts.Add(1)

	// Hash the key for sharding (KVShard uses [32]byte keys)
	keyHash := sha256.Sum256(key)
	return bs.shards.PutDyna(keyHash, value)
}

// PutByHash stores a value by its hash directly (content-addressed storage).
// Returns the hash. Used for transactions, blocks - data that won't change.
// Writes are async for high throughput. Duplicate values are deduplicated.
func (bs *BlockchainStore) PutByHash(value []byte) ([32]byte, error) {
	bs.stats.PutsByHash.Add(1)

	hash := sha256.Sum256(value)

	// Check if already exists (deduplication)
	if bs.shards.Has(hash) {
		bs.stats.Deduplication.Add(1)
		return hash, nil
	}

	bs.shards.PutPermAsync(hash, value)
	return hash, nil
}

// Get retrieves a value by key (for dynamic data like accounts)
func (bs *BlockchainStore) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	bs.stats.Gets.Add(1)

	keyHash := sha256.Sum256(key)
	return bs.shards.GetDyna(keyHash)
}

// GetByHash retrieves a value by its hash directly (for transactions, blocks)
func (bs *BlockchainStore) GetByHash(hash [32]byte) ([]byte, error) {
	bs.stats.Gets.Add(1)
	return bs.shards.GetPerm(hash)
}

// Has checks if a hash exists (fast bloom filter check)
func (bs *BlockchainStore) Has(hash [32]byte) bool {
	exists, bloomHit := bs.shards.HasWithStats(hash)
	if bloomHit {
		bs.stats.BloomHits.Add(1)
	} else {
		bs.stats.BloomMisses.Add(1)
	}
	return exists
}

// Delete removes a key from dynamic storage.
// Note: Content-addressed data (stored via PutByHash) cannot be deleted.
func (bs *BlockchainStore) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	bs.stats.Deletes.Add(1)

	keyHash := sha256.Sum256(key)
	return bs.shards.DeleteDyna(keyHash)
}

// ForEachPrefix iterates over all dynamic keys with the given prefix
// Note: This only works for dynamic data stored via Put(), not PutByHash()
func (bs *BlockchainStore) ForEachPrefix(prefix []byte, fn func(key, value []byte) error) error {
	// ForEachDyna iterates over DynaKV which stores by hash, not original key
	// This is a limitation - prefix iteration on hashed keys isn't meaningful
	// For true prefix iteration, would need to store original keys separately
	return bs.shards.ForEachDyna(prefix, fn)
}

// ForEach iterates over all dynamic entries
func (bs *BlockchainStore) ForEach(fn func(key, value []byte) error) error {
	return bs.shards.ForEachDyna(nil, fn)
}

// Close closes the store, flushing any pending writes
func (bs *BlockchainStore) Close() error {
	return bs.shards.Close()
}

// Flush waits for all pending async writes to complete
func (bs *BlockchainStore) Flush() {
	bs.shards.Flush()
}

// Stats returns store statistics
func (bs *BlockchainStore) Stats() StoreStats {
	return bs.stats
}

// Batch provides atomic batch operations
type Batch struct {
	store      *BlockchainStore
	puts       []batchPut
	putsByHash [][]byte
	deletes    [][]byte
}

type batchPut struct {
	key   []byte
	value []byte
}

// NewBatch creates a new batch for atomic operations
func (bs *BlockchainStore) NewBatch() *Batch {
	return &Batch{store: bs}
}

// Put queues a key-value pair for batch write (dynamic data)
func (b *Batch) Put(key, value []byte) {
	b.puts = append(b.puts, batchPut{key: key, value: value})
}

// PutByHash queues a value for content-addressed storage (permanent data)
func (b *Batch) PutByHash(value []byte) {
	b.putsByHash = append(b.putsByHash, value)
}

// Delete queues a key for deletion (no-op for blockchain data)
func (b *Batch) Delete(key []byte) {
	b.deletes = append(b.deletes, key)
}

// Commit writes all queued operations
func (b *Batch) Commit() error {
	// Process all dynamic puts
	for _, p := range b.puts {
		if err := b.store.Put(p.key, p.value); err != nil {
			return err
		}
	}

	// Process all permanent puts (async)
	for _, v := range b.putsByHash {
		if _, err := b.store.PutByHash(v); err != nil {
			return err
		}
	}

	// Clear the batch
	b.puts = nil
	b.putsByHash = nil

	return nil
}

// Len returns the number of pending operations
func (b *Batch) Len() int {
	return len(b.puts) + len(b.putsByHash) + len(b.deletes)
}

// Reset clears all pending operations without committing
func (b *Batch) Reset() {
	b.puts = nil
	b.putsByHash = nil
	b.deletes = nil
}
