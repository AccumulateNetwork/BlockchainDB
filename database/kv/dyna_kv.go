package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// DynaKV is a memory-optimized key-value store for dynamic (frequently changing) data.
// All values are cached in memory for fast reads, with a write-ahead log (WAL) for durability.
//
// Key features:
//   - In-memory cache for O(1) reads
//   - Append-only WAL for durability
//   - Sorted keys for efficient prefix iteration
//   - Periodic compaction to reclaim WAL space
type DynaKV struct {
	directory string

	// In-memory cache: key → value
	cache map[string][]byte
	mu    sync.RWMutex

	// Write-ahead log for durability
	walFile   *os.File
	walOffset int64

	// Sorted keys for prefix iteration
	sortedKeys []string
	keysDirty  bool

	// Statistics
	puts    uint64
	gets    uint64
	deletes uint64
}

// WAL entry format:
// [op: 1 byte][keyLen: 2 bytes][key: keyLen bytes][valueLen: 4 bytes][value: valueLen bytes]
const (
	walOpPut    byte = 1
	walOpDelete byte = 2
)

// NewDynaKV creates a new DynaKV store in the given directory.
func NewDynaKV(directory string) (*DynaKV, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(directory, "wal.dat")
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	d := &DynaKV{
		directory:  directory,
		cache:      make(map[string][]byte),
		walFile:    walFile,
		sortedKeys: make([]string, 0),
	}

	// Load existing WAL entries into cache
	if err := d.loadWAL(); err != nil {
		walFile.Close()
		return nil, err
	}

	return d, nil
}

// OpenDynaKV opens an existing DynaKV store.
func OpenDynaKV(directory string) (*DynaKV, error) {
	return NewDynaKV(directory) // Same logic - loads from WAL
}

// loadWAL reads the WAL file and populates the in-memory cache.
func (d *DynaKV) loadWAL() error {
	info, err := d.walFile.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		return nil // Empty WAL
	}

	// Read entire WAL (small enough to fit in memory)
	data := make([]byte, info.Size())
	if _, err := d.walFile.ReadAt(data, 0); err != nil {
		return err
	}

	offset := 0
	for offset < len(data) {
		if offset+7 > len(data) { // Minimum entry: op(1) + keyLen(2) + key(0) + valueLen(4)
			break // Truncated entry, stop
		}

		op := data[offset]
		keyLen := binary.BigEndian.Uint16(data[offset+1 : offset+3])
		offset += 3

		if offset+int(keyLen)+4 > len(data) {
			break // Truncated
		}

		key := string(data[offset : offset+int(keyLen)])
		offset += int(keyLen)

		valueLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		switch op {
		case walOpPut:
			if offset+int(valueLen) > len(data) {
				break // Truncated
			}
			value := make([]byte, valueLen)
			copy(value, data[offset:offset+int(valueLen)])
			offset += int(valueLen)
			d.cache[key] = value
		case walOpDelete:
			delete(d.cache, key)
		}
	}

	d.walOffset = info.Size()

	// Build sorted keys
	d.rebuildSortedKeys()

	return nil
}

// rebuildSortedKeys rebuilds the sorted key list from the cache.
func (d *DynaKV) rebuildSortedKeys() {
	d.sortedKeys = make([]string, 0, len(d.cache))
	for k := range d.cache {
		d.sortedKeys = append(d.sortedKeys, k)
	}
	sort.Strings(d.sortedKeys)
	d.keysDirty = false
}

// Put stores a key-value pair.
func (d *DynaKV) Put(key [32]byte, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	keyStr := string(key[:])

	// Write to WAL first for durability
	if err := d.writeWALEntry(walOpPut, keyStr, value); err != nil {
		return err
	}

	// Update cache
	_, existed := d.cache[keyStr]
	d.cache[keyStr] = value
	d.puts++

	// Update sorted keys
	if !existed {
		d.sortedKeys = append(d.sortedKeys, keyStr)
		d.keysDirty = true
	}

	return nil
}

// PutBytes stores a key-value pair with variable-length key.
func (d *DynaKV) PutBytes(key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	keyStr := string(key)

	// Write to WAL first for durability
	if err := d.writeWALEntry(walOpPut, keyStr, value); err != nil {
		return err
	}

	// Update cache
	_, existed := d.cache[keyStr]
	d.cache[keyStr] = value
	d.puts++

	// Update sorted keys
	if !existed {
		d.sortedKeys = append(d.sortedKeys, keyStr)
		d.keysDirty = true
	}

	return nil
}

// writeWALEntry writes an entry to the WAL file.
func (d *DynaKV) writeWALEntry(op byte, key string, value []byte) error {
	// Format: [op: 1][keyLen: 2][key: n][valueLen: 4][value: m]
	entrySize := 1 + 2 + len(key) + 4 + len(value)
	entry := make([]byte, entrySize)

	entry[0] = op
	binary.BigEndian.PutUint16(entry[1:3], uint16(len(key)))
	copy(entry[3:3+len(key)], key)
	binary.BigEndian.PutUint32(entry[3+len(key):7+len(key)], uint32(len(value)))
	copy(entry[7+len(key):], value)

	n, err := d.walFile.Write(entry)
	if err != nil {
		return err
	}
	d.walOffset += int64(n)

	return nil
}

// Get retrieves a value by key.
func (d *DynaKV) Get(key [32]byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	d.gets++

	value, exists := d.cache[string(key[:])]
	if !exists {
		return nil, errors.New("key not found")
	}

	// Return a copy to prevent mutation
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// GetBytes retrieves a value by variable-length key.
func (d *DynaKV) GetBytes(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	d.gets++

	value, exists := d.cache[string(key)]
	if !exists {
		return nil, errors.New("key not found")
	}

	// Return a copy to prevent mutation
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Has checks if a key exists.
func (d *DynaKV) Has(key [32]byte) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	_, exists := d.cache[string(key[:])]
	return exists
}

// Delete removes a key.
func (d *DynaKV) Delete(key [32]byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	keyStr := string(key[:])

	if _, exists := d.cache[keyStr]; !exists {
		return nil // Already deleted
	}

	// Write delete to WAL
	if err := d.writeWALEntry(walOpDelete, keyStr, nil); err != nil {
		return err
	}

	delete(d.cache, keyStr)
	d.deletes++
	d.keysDirty = true // Need to rebuild sorted keys

	return nil
}

// ForEachPrefix iterates over all keys with the given prefix in sorted order.
func (d *DynaKV) ForEachPrefix(prefix []byte, fn func(key, value []byte) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Sort keys if dirty
	if d.keysDirty {
		sort.Strings(d.sortedKeys)
		d.keysDirty = false
	}

	// Binary search to find start of prefix range
	prefixStr := string(prefix)
	start := sort.SearchStrings(d.sortedKeys, prefixStr)

	// Iterate until prefix no longer matches
	for i := start; i < len(d.sortedKeys); i++ {
		key := d.sortedKeys[i]
		if !bytes.HasPrefix([]byte(key), prefix) {
			break
		}
		value := d.cache[key]
		if err := fn([]byte(key), value); err != nil {
			return err
		}
	}

	return nil
}

// ForEach iterates over all keys in sorted order.
func (d *DynaKV) ForEach(fn func(key, value []byte) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Sort keys if dirty
	if d.keysDirty {
		sort.Strings(d.sortedKeys)
		d.keysDirty = false
	}

	for _, key := range d.sortedKeys {
		value := d.cache[key]
		if err := fn([]byte(key), value); err != nil {
			return err
		}
	}

	return nil
}

// Compact rewrites the WAL to remove deleted entries and duplicates.
func (d *DynaKV) Compact() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Create new WAL file
	newWalPath := filepath.Join(d.directory, "wal_new.dat")
	newWal, err := os.OpenFile(newWalPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	// Write all current cache entries to new WAL
	for key, value := range d.cache {
		entrySize := 1 + 2 + len(key) + 4 + len(value)
		entry := make([]byte, entrySize)
		entry[0] = walOpPut
		binary.BigEndian.PutUint16(entry[1:3], uint16(len(key)))
		copy(entry[3:3+len(key)], key)
		binary.BigEndian.PutUint32(entry[3+len(key):7+len(key)], uint32(len(value)))
		copy(entry[7+len(key):], value)

		if _, err := newWal.Write(entry); err != nil {
			newWal.Close()
			os.Remove(newWalPath)
			return err
		}
	}

	// Close old WAL
	oldWalPath := filepath.Join(d.directory, "wal.dat")
	d.walFile.Close()

	// Atomically replace old WAL with new
	if err := os.Rename(newWalPath, oldWalPath); err != nil {
		return err
	}

	// Reopen WAL
	d.walFile, err = os.OpenFile(oldWalPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	info, _ := d.walFile.Stat()
	d.walOffset = info.Size()

	return nil
}

// Close closes the DynaKV store.
func (d *DynaKV) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.walFile != nil {
		if err := d.walFile.Sync(); err != nil {
			return err
		}
		return d.walFile.Close()
	}
	return nil
}

// Stats returns statistics about the store.
func (d *DynaKV) Stats() map[string]uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return map[string]uint64{
		"entries": uint64(len(d.cache)),
		"puts":    d.puts,
		"gets":    d.gets,
		"deletes": d.deletes,
	}
}

// Size returns the number of entries in the cache.
func (d *DynaKV) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.cache)
}

// WALSize returns the current WAL file size in bytes.
func (d *DynaKV) WALSize() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.walOffset
}
