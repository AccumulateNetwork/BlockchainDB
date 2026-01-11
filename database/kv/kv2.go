package kv

import (
	"bytes"
	"os"
	"path/filepath"
)

const (
	PermDirName = "perm"
	DynaDirName = "dyna"
)

// KV2 maintains two layers of key-value pairs with different characteristics:
//
// 1. PermKV (Permanent KV): Disk-based storage using BucketStore
//   - Optimized for write-once data (transactions, blocks)
//   - Content-addressed storage where keys are derived from values
//   - Data is immutable once written
//   - Hash-bucket based for efficient lookups without sorting
//
// 2. DynaKV (Dynamic KV): Memory-based storage using DynaKV
//   - Optimized for frequently changing data (accounts, state)
//   - Values cached in memory for fast reads
//   - WAL for durability
//
// This design efficiently separates immutable data from mutable data,
// which is a common pattern in blockchain-style databases.
type KV2 struct {
	Directory string       // Directory where PermKV and DynaKV are stored
	PermKV    *BucketStore // Permanent data - bucket-based disk storage
	DynaKV    *DynaKV      // Dynamic data - memory-based with WAL
	DWrites   int          // Number of writes to DynaKV since last compress
	PWrites   int          // Number of writes to PermKV
}

// NewKV2 creates a new two-tier key-value store.
//
// Parameters:
//   - directory: Base directory for the store
//   - bucketCount: Number of buckets for PermKV (default 1024)
func NewKV2(directory string, bucketCount int32) (*KV2, error) {
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	kv2 := &KV2{
		Directory: directory,
	}

	// Create PermKV (bucket-based disk storage)
	permDir := filepath.Join(directory, PermDirName)
	var err error
	if kv2.PermKV, err = NewBucketStore(permDir, uint32(bucketCount), 0); err != nil {
		return nil, err
	}

	// Create DynaKV (memory-based)
	dynaDir := filepath.Join(directory, DynaDirName)
	if kv2.DynaKV, err = NewDynaKV(dynaDir); err != nil {
		kv2.PermKV.Close()
		return nil, err
	}

	return kv2, nil
}

// OpenKV2 opens an existing two-tier key-value store.
func OpenKV2(directory string) (*KV2, error) {
	kv2 := &KV2{
		Directory: directory,
	}

	// Open PermKV
	permDir := filepath.Join(directory, PermDirName)
	var err error
	if kv2.PermKV, err = OpenBucketStore(permDir); err != nil {
		return nil, err
	}

	// Open DynaKV
	dynaDir := filepath.Join(directory, DynaDirName)
	if kv2.DynaKV, err = OpenDynaKV(dynaDir); err != nil {
		kv2.PermKV.Close()
		return nil, err
	}

	return kv2, nil
}

// GetDyna retrieves a value from DynaKV only.
// Use when you know the key is dynamic data.
func (k *KV2) GetDyna(key InternalKey) ([]byte, error) {
	return k.DynaKV.Get(key)
}

// GetPerm retrieves a value from PermKV only.
// Use when you know the key is permanent data.
func (k *KV2) GetPerm(key InternalKey) ([]byte, error) {
	return k.PermKV.Get(key)
}

// Get retrieves a value, checking DynaKV first then PermKV.
// This is the general-purpose get that handles both data types.
func (k *KV2) Get(key InternalKey) ([]byte, error) {
	// Check DynaKV first (fast, in-memory)
	if value, err := k.DynaKV.Get(key); err == nil {
		return value, nil
	}

	// Fall back to PermKV (disk-based)
	return k.PermKV.Get(key)
}

// PutDyna stores a value in DynaKV.
// Use for data that may change (accounts, state).
func (k *KV2) PutDyna(key InternalKey, value []byte) (writes int, err error) {
	k.DWrites++
	err = k.DynaKV.Put(key, value)
	return k.DWrites, err
}

// PutPerm stores a value in PermKV.
// Use for data that won't change (transactions, blocks).
func (k *KV2) PutPerm(key InternalKey, value []byte) (writes int, err error) {
	k.PWrites++
	err = k.PermKV.Put(key, value)
	return k.DWrites, err
}

// Put stores a value using automatic tier detection.
//
// Logic:
//  1. If key exists in DynaKV with same value → no-op
//  2. If key exists in DynaKV with different value → update DynaKV
//  3. If key exists in PermKV with same value → no-op
//  4. If key exists in PermKV with different value → store in DynaKV (key "migrates")
//  5. If key doesn't exist → store in PermKV (default for new keys)
func (k *KV2) Put(key InternalKey, value []byte) (writes int, err error) {
	// Check if key exists in DynaKV
	if existingValue, err := k.DynaKV.Get(key); err == nil {
		if bytes.Equal(value, existingValue) {
			return k.DWrites, nil // No change needed
		}
		// Value changed, update DynaKV
		k.DWrites++
		err = k.DynaKV.Put(key, value)
		return k.DWrites, err
	}

	// Check if key exists in PermKV
	if existingValue, err := k.PermKV.Get(key); err == nil {
		if bytes.Equal(value, existingValue) {
			return k.DWrites, nil // No change needed
		}
		// Permanent value changed - migrate to DynaKV
		k.DWrites++
		err = k.DynaKV.Put(key, value)
		return k.DWrites, err
	}

	// New key - default to PermKV
	k.PWrites++
	err = k.PermKV.Put(key, value)
	return k.DWrites, err
}

// Has checks if a key exists in either tier.
func (k *KV2) Has(key InternalKey) bool {
	if k.DynaKV.Has(key) {
		return true
	}
	// Check PermKV
	return k.PermKV.Has(key)
}

// HasDyna checks if a key exists in DynaKV.
func (k *KV2) HasDyna(key InternalKey) bool {
	return k.DynaKV.Has(key)
}

// ForEachDyna iterates over all keys in DynaKV with the given prefix.
func (k *KV2) ForEachDyna(prefix []byte, fn func(key, value []byte) error) error {
	return k.DynaKV.ForEachPrefix(prefix, fn)
}

// Compress compacts the DynaKV WAL to reclaim space.
// Only DynaKV needs compression since PermKV data doesn't change.
func (k *KV2) Compress() error {
	if err := k.DynaKV.Compact(); err != nil {
		return err
	}
	k.DWrites = 0
	k.PWrites = 0
	return nil
}

// Close closes both stores.
func (k *KV2) Close() error {
	if err := k.DynaKV.Close(); err != nil {
		return err
	}
	return k.PermKV.Close()
}

// Stats returns statistics about both stores.
func (k *KV2) Stats() KV2Stats {
	dynaStats := k.DynaKV.Stats()
	permStats := k.PermKV.Stats()

	return KV2Stats{
		DynaEntries: dynaStats["entries"],
		DynaPuts:    dynaStats["puts"],
		DynaGets:    dynaStats["gets"],
		DynaWALSize: uint64(k.DynaKV.WALSize()),
		PermEntries: permStats.EntryCount,
		PermWrites:  uint64(k.PWrites),
	}
}

// KV2Stats contains statistics for both tiers.
type KV2Stats struct {
	// DynaKV stats
	DynaEntries uint64
	DynaPuts    uint64
	DynaGets    uint64
	DynaWALSize uint64

	// PermKV stats
	PermEntries uint64
	PermWrites  uint64
}
