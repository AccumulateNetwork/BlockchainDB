package dkv

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
)

// DualIndexDKV maintains two separate trees:
// 1. Primary tree sorted by hash for O(log n) hash lookups
// 2. Secondary tree sorted by original key for ordered iteration
type DualIndexDKV struct {
	directory string

	// Primary index: hash -> value
	hashTree    *TreeDKV

	// Secondary index: original_key -> hash
	keyTree     *TreeDKV

	// Optional: in-memory cache for hot lookups
	cache       map[[32]byte][]byte
	cacheMu     sync.RWMutex

	// Metadata storage
	metaTree    *TreeDKV
}

// NewDualIndexDKV creates a store with both hash and key sorting
func NewDualIndexDKV(directory string) (*DualIndexDKV, error) {
	hashTree, err := NewTreeDKV(filepath.Join(directory, "by_hash"))
	if err != nil {
		return nil, err
	}

	keyTree, err := NewTreeDKV(filepath.Join(directory, "by_key"))
	if err != nil {
		return nil, err
	}

	metaTree, err := NewTreeDKV(filepath.Join(directory, "metadata"))
	if err != nil {
		return nil, err
	}

	return &DualIndexDKV{
		directory: directory,
		hashTree:  hashTree,
		keyTree:   keyTree,
		metaTree:  metaTree,
		cache:     make(map[[32]byte][]byte),
	}, nil
}

// PutWithKey stores value with both hash and original key indexing
func (d *DualIndexDKV) PutWithKey(originalKey string, value []byte) error {
	// Generate hash
	hash := sha256.Sum256([]byte(originalKey))

	// Store in hash tree (primary storage)
	if err := d.hashTree.Put(hash, value); err != nil {
		return err
	}

	// Store hash reference in key tree (secondary index)
	// This allows us to iterate by original key order
	if err := d.keyTree.Put(stringToKey(originalKey), hash[:]); err != nil {
		return err
	}

	// Store metadata mapping
	meta := KeyMetadata{
		OriginalKey: originalKey,
		Hash:        hash,
		Size:        len(value),
	}
	metaBytes, _ := json.Marshal(meta)
	if err := d.metaTree.Put(hash, metaBytes); err != nil {
		return err
	}

	// Update cache
	d.cacheMu.Lock()
	d.cache[hash] = value
	d.cacheMu.Unlock()

	return nil
}

// GetByHash retrieves value by hash (fast O(log n))
func (d *DualIndexDKV) GetByHash(hash [32]byte) ([]byte, bool, error) {
	// Check cache first
	d.cacheMu.RLock()
	if val, ok := d.cache[hash]; ok {
		d.cacheMu.RUnlock()
		return val, true, nil
	}
	d.cacheMu.RUnlock()

	return d.hashTree.Get(hash)
}

// GetByKey retrieves value by original key (requires two lookups)
func (d *DualIndexDKV) GetByKey(originalKey string) ([]byte, bool, error) {
	// First, get hash from key tree
	hashBytes, found, err := d.keyTree.Get(stringToKey(originalKey))
	if err != nil || !found {
		return nil, false, err
	}

	// Convert to hash
	var hash [32]byte
	copy(hash[:], hashBytes)

	// Then get value from hash tree
	return d.GetByHash(hash)
}

// IterateByKey iterates in original key order (not hash order)
func (d *DualIndexDKV) IterateByKey(fn func(key string, value []byte) error) error {
	iter := d.keyTree.NewIterator()
	defer iter.Close()

	for iter.Next() {
		// Get original key from the key tree key
		originalKey := keyToString(iter.Key())

		// Get hash reference
		hashBytes := iter.Value()
		var hash [32]byte
		copy(hash[:], hashBytes)

		// Get actual value from hash tree
		value, found, err := d.hashTree.Get(hash)
		if err != nil || !found {
			continue
		}

		if err := fn(originalKey, value); err != nil {
			return err
		}
	}

	return nil
}

// IterateByHash iterates in hash order
func (d *DualIndexDKV) IterateByHash(fn func(hash [32]byte, value []byte) error) error {
	iter := d.hashTree.NewIterator()
	defer iter.Close()

	for iter.Next() {
		if err := fn(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}

	return nil
}

// RangeByKey returns values for keys in lexicographic range
func (d *DualIndexDKV) RangeByKey(startKey, endKey string) ([]KeyValue, error) {
	var results []KeyValue

	iter := d.keyTree.NewIteratorWithOptions(&IteratorOptions{
		Start: stringToKey(startKey),
		End:   stringToKey(endKey),
	})
	defer iter.Close()

	for iter.Next() {
		originalKey := keyToString(iter.Key())
		hashBytes := iter.Value()

		var hash [32]byte
		copy(hash[:], hashBytes)

		value, found, _ := d.hashTree.Get(hash)
		if found {
			results = append(results, KeyValue{
				Key:   originalKey,
				Hash:  hash,
				Value: value,
			})
		}
	}

	return results, nil
}

// KeyValue represents a key-value pair with both forms
type KeyValue struct {
	Key   string
	Hash  [32]byte
	Value []byte
}

// KeyMetadata stores the mapping between original key and hash
type KeyMetadata struct {
	OriginalKey string   `json:"original_key"`
	Hash        [32]byte `json:"hash"`
	Size        int      `json:"size"`
}

// Helper functions to convert strings to fixed-size keys
func stringToKey(s string) [32]byte {
	// For short strings, pad with zeros
	// For long strings, use hash
	if len(s) <= 32 {
		var key [32]byte
		copy(key[:], []byte(s))
		return key
	}
	return sha256.Sum256([]byte(s))
}

func keyToString(key [32]byte) string {
	// Find null terminator
	end := bytes.IndexByte(key[:], 0)
	if end == -1 {
		end = 32
	}
	return string(key[:end])
}

// MultiIndexDKV supports multiple custom sort orders
type MultiIndexDKV struct {
	directory    string
	indices      map[string]*TreeDKV
	primaryTree  *TreeDKV
	indexFuncs   map[string]IndexFunc
	mu           sync.RWMutex
}

// IndexFunc generates an index key from data
type IndexFunc func(originalKey string, value []byte) [32]byte

// NewMultiIndexDKV creates a store with multiple indices
func NewMultiIndexDKV(directory string) (*MultiIndexDKV, error) {
	primaryTree, err := NewTreeDKV(filepath.Join(directory, "primary"))
	if err != nil {
		return nil, err
	}

	return &MultiIndexDKV{
		directory:   directory,
		indices:     make(map[string]*TreeDKV),
		primaryTree: primaryTree,
		indexFuncs:  make(map[string]IndexFunc),
	}, nil
}

// AddIndex creates a new sorted index with custom key generation
func (m *MultiIndexDKV) AddIndex(name string, indexFunc IndexFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tree, err := NewTreeDKV(filepath.Join(m.directory, "index_"+name))
	if err != nil {
		return err
	}

	m.indices[name] = tree
	m.indexFuncs[name] = indexFunc

	// Rebuild index from existing data
	iter := m.primaryTree.NewIterator()
	defer iter.Close()

	for iter.Next() {
		hash := iter.Key()
		value := iter.Value()

		// Generate index key
		indexKey := indexFunc("", value)

		// Store hash reference in index
		tree.Put(indexKey, hash[:])
	}

	return nil
}

// Put stores value in primary tree and all indices
func (m *MultiIndexDKV) Put(originalKey string, value []byte) error {
	hash := sha256.Sum256([]byte(originalKey))

	// Store in primary tree
	if err := m.primaryTree.Put(hash, value); err != nil {
		return err
	}

	// Update all indices
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, indexFunc := range m.indexFuncs {
		indexKey := indexFunc(originalKey, value)
		if err := m.indices[name].Put(indexKey, hash[:]); err != nil {
			return err
		}
	}

	return nil
}

// IterateByIndex iterates using a specific index order
func (m *MultiIndexDKV) IterateByIndex(indexName string, fn func(value []byte) error) error {
	m.mu.RLock()
	indexTree, exists := m.indices[indexName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("index %s not found", indexName)
	}

	iter := indexTree.NewIterator()
	defer iter.Close()

	for iter.Next() {
		// Get hash from index
		hashBytes := iter.Value()
		var hash [32]byte
		copy(hash[:], hashBytes)

		// Get value from primary tree
		value, found, err := m.primaryTree.Get(hash)
		if err != nil || !found {
			continue
		}

		if err := fn(value); err != nil {
			return err
		}
	}

	return nil
}

// Example index functions
func CreateTimestampIndex() IndexFunc {
	return func(key string, value []byte) [32]byte {
		// Extract timestamp from value and create sortable key
		// This is a simplified example
		var result [32]byte
		// Would parse timestamp from value and encode it
		return result
	}
}

func CreateSizeIndex() IndexFunc {
	return func(key string, value []byte) [32]byte {
		// Create key based on size for size-ordered iteration
		var result [32]byte
		size := uint64(len(value))
		// Encode size in big-endian for correct sort order
		result[0] = byte(size >> 56)
		result[1] = byte(size >> 48)
		result[2] = byte(size >> 40)
		result[3] = byte(size >> 32)
		result[4] = byte(size >> 24)
		result[5] = byte(size >> 16)
		result[6] = byte(size >> 8)
		result[7] = byte(size)
		return result
	}
}

func CreateReverseIndex() IndexFunc {
	return func(key string, value []byte) [32]byte {
		// Create reverse key for reverse order iteration
		reversed := []byte(key)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		return sha256.Sum256(reversed)
	}
}