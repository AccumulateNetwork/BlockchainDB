package blockchainDB

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"sort"
	"sync"
)

var (
	ErrNotFound     = errors.New("key not found")
	ErrClosed       = errors.New("database closed")
	ErrInvalidBatch = errors.New("invalid batch operation")
)

// OpType represents the type of batch operation
type OpType int

const (
	OpPut OpType = iota
	OpDelete
)

// BatchOp represents a single operation in a batch
type BatchOp struct {
	Type  OpType
	Key   []byte
	Value []byte
}

// WriteBatch accumulates database operations for atomic execution
type WriteBatch struct {
	ops []BatchOp
	mu  sync.Mutex
}

// NewWriteBatch creates a new batch for atomic operations
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		ops: make([]BatchOp, 0),
	}
}

// Put adds a put operation to the batch
func (wb *WriteBatch) Put(key, value []byte) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	wb.ops = append(wb.ops, BatchOp{
		Type:  OpPut,
		Key:   append([]byte{}, key...),
		Value: append([]byte{}, value...),
	})
}

// Delete adds a delete operation to the batch
func (wb *WriteBatch) Delete(key []byte) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	wb.ops = append(wb.ops, BatchOp{
		Type: OpDelete,
		Key:  append([]byte{}, key...),
	})
}

// Clear removes all operations from the batch
func (wb *WriteBatch) Clear() {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	wb.ops = wb.ops[:0]
}

// HashDB provides LevelDB-compatible API with hash-based storage
type HashDB struct {
	// Storage layers
	hashStore *HashStore
	mapper    *KeyHashMapper

	// Options
	directory string
	readOnly  bool

	// State management
	mu     sync.RWMutex
	closed bool

	// Statistics
	stats DBStats
}

// DBStats tracks database statistics
type DBStats struct {
	Puts          uint64
	Gets          uint64
	Deletes       uint64
	Hits          uint64
	Misses        uint64
	BytesWritten  uint64
	BytesRead     uint64
	Deduplicates  uint64
}

// HashStore manages content-addressed value storage
type HashStore struct {
	kv2      *KV2                   // Underlying storage
	refCount map[[32]byte]uint64    // Reference counting
	cache    map[[32]byte][]byte    // Value cache
	mu       sync.RWMutex
}

// NewHashStore creates a new hash-based value store
func NewHashStore(directory string) (*HashStore, error) {
	kv2, err := NewKV2(directory, 1024, 100000, 50)
	if err != nil {
		return nil, err
	}

	return &HashStore{
		kv2:      kv2,
		refCount: make(map[[32]byte]uint64),
		cache:    make(map[[32]byte][]byte),
	}, nil
}

// StoreByHash stores a value and returns its hash
func (hs *HashStore) StoreByHash(value []byte) ([32]byte, error) {
	hash := sha256.Sum256(value)

	hs.mu.Lock()
	defer hs.mu.Unlock()

	// Check if already stored (deduplication)
	if hs.refCount[hash] > 0 {
		hs.refCount[hash]++
		return hash, nil
	}

	// Store in underlying KV2
	if _, err := hs.kv2.PutPerm(hash, value); err != nil {
		return [32]byte{}, err
	}

	hs.refCount[hash] = 1
	hs.cache[hash] = value

	return hash, nil
}

// GetByHash retrieves a value by its hash
func (hs *HashStore) GetByHash(hash [32]byte) ([]byte, error) {
	hs.mu.RLock()

	// Check cache first
	if value, ok := hs.cache[hash]; ok {
		hs.mu.RUnlock()
		return value, nil
	}
	hs.mu.RUnlock()

	// Fetch from storage
	value, err := hs.kv2.GetPerm(hash)
	if err != nil {
		return nil, err
	}

	// Update cache
	hs.mu.Lock()
	hs.cache[hash] = value
	hs.mu.Unlock()

	return value, nil
}

// DecRef decreases reference count, potentially removing value
func (hs *HashStore) DecRef(hash [32]byte) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if count, exists := hs.refCount[hash]; exists && count > 0 {
		hs.refCount[hash]--
		if hs.refCount[hash] == 0 {
			delete(hs.refCount, hash)
			delete(hs.cache, hash)
			// Note: We don't actually delete from storage for immutability
		}
	}
}

// KeyHashMapper maintains key-to-hash mappings
type KeyHashMapper struct {
	keyToHash  map[string][32]byte
	hashToKeys map[[32]byte][]string
	sortedKeys []string
	kv2        *KV2
	mu         sync.RWMutex
}

// NewKeyHashMapper creates a new key-hash mapper
func NewKeyHashMapper(directory string) (*KeyHashMapper, error) {
	kv2, err := NewKV2(directory, 1024, 100000, 50)
	if err != nil {
		return nil, err
	}

	return &KeyHashMapper{
		keyToHash:  make(map[string][32]byte),
		hashToKeys: make(map[[32]byte][]string),
		sortedKeys: make([]string, 0),
		kv2:        kv2,
	}, nil
}

// MapKey creates or updates a key-to-hash mapping
func (km *KeyHashMapper) MapKey(key []byte, hash [32]byte) error {
	km.mu.Lock()
	defer km.mu.Unlock()

	keyStr := string(key)

	// Remove old mapping if exists
	if oldHash, exists := km.keyToHash[keyStr]; exists {
		km.removeKeyFromHashUnsafe(keyStr, oldHash)
	}

	// Add new mapping
	km.keyToHash[keyStr] = hash
	km.hashToKeys[hash] = append(km.hashToKeys[hash], keyStr)

	// Update sorted keys for iteration
	if !km.keyExists(keyStr) {
		km.sortedKeys = append(km.sortedKeys, keyStr)
		sort.Strings(km.sortedKeys)
	}

	// Persist mapping (store key->hash in DynaKV)
	var keyHash [32]byte
	copy(keyHash[:], sha256.Sum256(key)[:])
	_, err := km.kv2.PutDyna(keyHash, hash[:])
	return err
}

// GetHash returns the hash for a given key
func (km *KeyHashMapper) GetHash(key []byte) ([32]byte, bool) {
	km.mu.RLock()
	defer km.mu.RUnlock()

	hash, exists := km.keyToHash[string(key)]
	return hash, exists
}

// RemoveKey removes a key mapping
func (km *KeyHashMapper) RemoveKey(key []byte) {
	km.mu.Lock()
	defer km.mu.Unlock()

	keyStr := string(key)
	if hash, exists := km.keyToHash[keyStr]; exists {
		km.removeKeyFromHashUnsafe(keyStr, hash)
		delete(km.keyToHash, keyStr)

		// Remove from sorted keys
		idx := sort.SearchStrings(km.sortedKeys, keyStr)
		if idx < len(km.sortedKeys) && km.sortedKeys[idx] == keyStr {
			km.sortedKeys = append(km.sortedKeys[:idx], km.sortedKeys[idx+1:]...)
		}
	}
}

func (km *KeyHashMapper) removeKeyFromHashUnsafe(key string, hash [32]byte) {
	if keys, exists := km.hashToKeys[hash]; exists {
		for i, k := range keys {
			if k == key {
				km.hashToKeys[hash] = append(keys[:i], keys[i+1:]...)
				break
			}
		}
		if len(km.hashToKeys[hash]) == 0 {
			delete(km.hashToKeys, hash)
		}
	}
}

func (km *KeyHashMapper) keyExists(key string) bool {
	idx := sort.SearchStrings(km.sortedKeys, key)
	return idx < len(km.sortedKeys) && km.sortedKeys[idx] == key
}

// NewHashDB creates a new LevelDB-compatible database with hash-based storage
func NewHashDB(directory string) (*HashDB, error) {
	hashStore, err := NewHashStore(directory + "/hashes")
	if err != nil {
		return nil, err
	}

	mapper, err := NewKeyHashMapper(directory + "/keys")
	if err != nil {
		return nil, err
	}

	return &HashDB{
		hashStore: hashStore,
		mapper:    mapper,
		directory: directory,
		closed:    false,
	}, nil
}

// Put stores a key-value pair
func (db *HashDB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Store value by hash
	hash, err := db.hashStore.StoreByHash(value)
	if err != nil {
		return err
	}

	// Map key to hash
	if err := db.mapper.MapKey(key, hash); err != nil {
		return err
	}

	// Update statistics
	db.stats.Puts++
	db.stats.BytesWritten += uint64(len(value))

	return nil
}

// Get retrieves a value by key
func (db *HashDB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	// Look up hash for key
	hash, exists := db.mapper.GetHash(key)
	if !exists {
		db.stats.Misses++
		return nil, ErrNotFound
	}

	// Retrieve value by hash
	value, err := db.hashStore.GetByHash(hash)
	if err != nil {
		db.stats.Misses++
		return nil, err
	}

	// Update statistics
	db.stats.Gets++
	db.stats.Hits++
	db.stats.BytesRead += uint64(len(value))

	return value, nil
}

// Delete removes a key-value pair
func (db *HashDB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Get hash for key
	hash, exists := db.mapper.GetHash(key)
	if !exists {
		return nil // LevelDB ignores missing keys
	}

	// Remove key mapping
	db.mapper.RemoveKey(key)

	// Decrease reference count
	db.hashStore.DecRef(hash)

	// Update statistics
	db.stats.Deletes++

	return nil
}

// Write executes a batch of operations atomically
func (db *HashDB) Write(batch *WriteBatch) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	// Execute all operations
	for _, op := range batch.ops {
		switch op.Type {
		case OpPut:
			if err := db.putUnsafe(op.Key, op.Value); err != nil {
				return err
			}
		case OpDelete:
			if err := db.deleteUnsafe(op.Key); err != nil {
				return err
			}
		default:
			return ErrInvalidBatch
		}
	}

	return nil
}

func (db *HashDB) putUnsafe(key, value []byte) error {
	hash, err := db.hashStore.StoreByHash(value)
	if err != nil {
		return err
	}

	if err := db.mapper.MapKey(key, hash); err != nil {
		return err
	}

	db.stats.Puts++
	db.stats.BytesWritten += uint64(len(value))
	return nil
}

func (db *HashDB) deleteUnsafe(key []byte) error {
	hash, exists := db.mapper.GetHash(key)
	if !exists {
		return nil
	}

	db.mapper.RemoveKey(key)
	db.hashStore.DecRef(hash)
	db.stats.Deletes++
	return nil
}

// Close closes the database
func (db *HashDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	db.closed = true

	// Close underlying storage
	if err := db.hashStore.kv2.Close(); err != nil {
		return err
	}

	if err := db.mapper.kv2.Close(); err != nil {
		return err
	}

	return nil
}

// GetStats returns database statistics
func (db *HashDB) GetStats() DBStats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.stats
}

// Iterator provides iteration over database entries
type Iterator struct {
	db       *HashDB
	keys     []string
	position int
}

// NewIterator creates a new iterator
func (db *HashDB) NewIterator() *Iterator {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return &Iterator{
		db:       db,
		keys:     append([]string{}, db.mapper.sortedKeys...),
		position: 0,
	}
}

// Seek positions the iterator at the first key >= target
func (it *Iterator) Seek(target []byte) {
	it.position = sort.SearchStrings(it.keys, string(target))
}

// SeekToFirst positions at the first key
func (it *Iterator) SeekToFirst() {
	it.position = 0
}

// SeekToLast positions at the last key
func (it *Iterator) SeekToLast() {
	it.position = len(it.keys) - 1
}

// Next moves to the next entry
func (it *Iterator) Next() {
	if it.Valid() {
		it.position++
	}
}

// Prev moves to the previous entry
func (it *Iterator) Prev() {
	if it.position > 0 {
		it.position--
	}
}

// Valid checks if the iterator position is valid
func (it *Iterator) Valid() bool {
	return it.position >= 0 && it.position < len(it.keys)
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return []byte(it.keys[it.position])
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	value, _ := it.db.Get(it.Key())
	return value
}

// Close releases iterator resources
func (it *Iterator) Close() error {
	it.keys = nil
	return nil
}