package dkv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// File naming constants
	dkvIndexPrefix = "dkv_index_"
	dkvValuePrefix = "dkv_values_"

	// Hash-based organization
	numHashBuckets = 256 // Number of hash buckets (using first byte of hash)

	// Compaction thresholds
	compactThreshold = 0.5      // Compact when 50% of space is wasted
	minCompactSize   = 100 * 1024 // Don't compact files smaller than 100KB (reduced for testing)

	// Entry size
	entryHeaderSize = 48 // 32-byte key + 8-byte offset + 8-byte length
)

// DKV (Dynamic Key-Value) is optimized for mutable data with hash-based organization
// Values are organized by hash prefix for better distribution and deduplication
type DKV struct {
	Directory string

	// Hash-based value files (organized by first byte of key hash)
	// Each bucket handles keys with the same hash prefix
	hashBuckets []*HashBucket

	// Global statistics
	totalReads    atomic.Int64
	totalWrites   atomic.Int64
	totalDeletes  atomic.Int64
	totalCompacts atomic.Int64

	// Compaction control
	compactRunning atomic.Bool
	lastCompact    time.Time

	mu sync.RWMutex
}

// HashBucket manages values for keys with the same hash prefix
type HashBucket struct {
	id         int    // Bucket ID (0-255)
	directory  string // Directory for this bucket's files

	// Current value file for this bucket
	valueFile     *os.File
	valueFilePath string
	valueOffset   atomic.Int64 // Current write position

	// In-memory index for this bucket
	index      map[[32]byte]*DKVEntry
	indexMutex sync.RWMutex

	// Pending writes buffer (for batching and sorting)
	pendingWrites []*DKVEntry
	pendingMutex  sync.Mutex

	// Statistics for this bucket
	totalSize   atomic.Int64
	wastedSpace atomic.Int64
	entryCount  atomic.Int64
}

// DKVEntry represents a key-value entry
type DKVEntry struct {
	Key         [32]byte
	ValueOffset uint64
	ValueLength uint64
	Timestamp   int64
	Version     uint32
	Deleted     bool
}

// NewDKV creates a new dynamic key-value store with hash-based organization
func NewDKV(directory string) (*DKV, error) {
	// Create main directory
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	dkv := &DKV{
		Directory:   directory,
		hashBuckets: make([]*HashBucket, numHashBuckets),
		lastCompact: time.Now(),
	}

	// Initialize hash buckets
	for i := 0; i < numHashBuckets; i++ {
		bucket, err := newHashBucket(i, directory)
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket %d: %w", i, err)
		}
		dkv.hashBuckets[i] = bucket
	}

	// Load existing indices
	if err := dkv.loadIndices(); err != nil {
		return nil, err
	}

	return dkv, nil
}

// newHashBucket creates a new hash bucket
func newHashBucket(id int, parentDir string) (*HashBucket, error) {
	// Create bucket directory
	bucketDir := filepath.Join(parentDir, fmt.Sprintf("bucket_%02x", id))
	if err := os.MkdirAll(bucketDir, os.ModePerm); err != nil {
		return nil, err
	}

	bucket := &HashBucket{
		id:            id,
		directory:     bucketDir,
		index:         make(map[[32]byte]*DKVEntry),
		pendingWrites: make([]*DKVEntry, 0, 1000),
	}

	// Open or create value file
	valueFilePath := filepath.Join(bucketDir, fmt.Sprintf("%s%02x.dat", dkvValuePrefix, id))
	var err error
	bucket.valueFile, err = os.OpenFile(valueFilePath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	bucket.valueFilePath = valueFilePath

	// Get current file size
	info, err := bucket.valueFile.Stat()
	if err != nil {
		return nil, err
	}
	bucket.valueOffset.Store(info.Size())
	bucket.totalSize.Store(info.Size())

	return bucket, nil
}

// getBucket returns the hash bucket for a given key
func (dkv *DKV) getBucket(key [32]byte) *HashBucket {
	// Use first byte of key as bucket index
	bucketIdx := int(key[0])
	return dkv.hashBuckets[bucketIdx]
}

// Put stores a key-value pair, replacing any existing value
func (dkv *DKV) Put(key [32]byte, value []byte) error {
	dkv.totalWrites.Add(1)

	bucket := dkv.getBucket(key)

	// Create entry
	entry := &DKVEntry{
		Key:         key,
		ValueLength: uint64(len(value)),
		Timestamp:   time.Now().UnixNano(),
		Version:     1,
		Deleted:     false,
	}

	// Check if key already exists
	bucket.indexMutex.RLock()
	if existing, exists := bucket.index[key]; exists {
		entry.Version = existing.Version + 1
		// Mark old space as wasted
		bucket.wastedSpace.Add(int64(existing.ValueLength))
	}
	bucket.indexMutex.RUnlock()

	// Add to pending writes for batching
	bucket.pendingMutex.Lock()
	entry.ValueOffset = uint64(bucket.valueOffset.Load())
	bucket.pendingWrites = append(bucket.pendingWrites, entry)

	// Write value to file
	if _, err := bucket.valueFile.Write(value); err != nil {
		bucket.pendingMutex.Unlock()
		return err
	}

	bucket.valueOffset.Add(int64(len(value)))
	bucket.totalSize.Add(int64(len(value)))

	// If we have enough pending writes, flush them
	shouldFlush := len(bucket.pendingWrites) >= 100
	bucket.pendingMutex.Unlock()

	if shouldFlush {
		return dkv.flushBucket(bucket)
	}

	// Update in-memory index immediately for consistency
	bucket.indexMutex.Lock()
	bucket.index[key] = entry
	bucket.indexMutex.Unlock()

	return nil
}

// Get retrieves a value by key
func (dkv *DKV) Get(key [32]byte) ([]byte, bool, error) {
	dkv.totalReads.Add(1)

	bucket := dkv.getBucket(key)

	// Check in-memory index
	bucket.indexMutex.RLock()
	entry, exists := bucket.index[key]
	bucket.indexMutex.RUnlock()

	if !exists || entry.Deleted {
		return nil, false, nil
	}

	// Read value from file
	value := make([]byte, entry.ValueLength)
	_, err := bucket.valueFile.ReadAt(value, int64(entry.ValueOffset))
	if err != nil {
		return nil, false, err
	}

	return value, true, nil
}

// Delete marks a key as deleted
func (dkv *DKV) Delete(key [32]byte) error {
	dkv.totalDeletes.Add(1)

	bucket := dkv.getBucket(key)

	bucket.indexMutex.Lock()
	defer bucket.indexMutex.Unlock()

	if entry, exists := bucket.index[key]; exists {
		entry.Deleted = true
		entry.Timestamp = time.Now().UnixNano()
		bucket.wastedSpace.Add(int64(entry.ValueLength))
		return nil
	}

	return errors.New("key not found")
}

// flushBucket sorts and deduplicates pending writes, then updates the index
func (dkv *DKV) flushBucket(bucket *HashBucket) error {
	bucket.pendingMutex.Lock()
	if len(bucket.pendingWrites) == 0 {
		bucket.pendingMutex.Unlock()
		return nil
	}

	// Copy pending writes and clear buffer
	writes := make([]*DKVEntry, len(bucket.pendingWrites))
	copy(writes, bucket.pendingWrites)
	bucket.pendingWrites = bucket.pendingWrites[:0]
	bucket.pendingMutex.Unlock()

	// Sort by key for better locality
	sort.Slice(writes, func(i, j int) bool {
		return bytes.Compare(writes[i].Key[:], writes[j].Key[:]) < 0
	})

	// Deduplicate - keep only the latest version of each key
	deduped := make([]*DKVEntry, 0, len(writes))
	seen := make(map[[32]byte]int)

	for _, entry := range writes {
		if idx, exists := seen[entry.Key]; exists {
			// Replace with newer version
			deduped[idx] = entry
		} else {
			seen[entry.Key] = len(deduped)
			deduped = append(deduped, entry)
		}
	}

	// Update index with deduplicated entries
	bucket.indexMutex.Lock()
	for _, entry := range deduped {
		bucket.index[entry.Key] = entry
		bucket.entryCount.Store(int64(len(bucket.index)))
	}
	bucket.indexMutex.Unlock()

	// Check if compaction is needed
	if dkv.shouldCompactBucket(bucket) {
		go dkv.compactBucket(bucket)
	}

	return nil
}

// shouldCompactBucket checks if a bucket needs compaction
func (dkv *DKV) shouldCompactBucket(bucket *HashBucket) bool {
	totalSize := bucket.totalSize.Load()
	wastedSpace := bucket.wastedSpace.Load()

	if totalSize < minCompactSize {
		return false // Too small to compact
	}

	wastedRatio := float64(wastedSpace) / float64(totalSize)
	return wastedRatio > compactThreshold
}

// compactBucket performs compaction on a single bucket
func (dkv *DKV) compactBucket(bucket *HashBucket) error {
	// Avoid concurrent compactions
	if !dkv.compactRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer dkv.compactRunning.Store(false)

	bucket.indexMutex.Lock()
	defer bucket.indexMutex.Unlock()

	// Create temporary file for compacted data
	tempPath := bucket.valueFilePath + ".compact"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	// Collect all valid entries and sort by key
	validEntries := make([]*DKVEntry, 0, len(bucket.index))
	for _, entry := range bucket.index {
		if !entry.Deleted {
			validEntries = append(validEntries, entry)
		}
	}

	// Sort entries by key for better locality
	sort.Slice(validEntries, func(i, j int) bool {
		return bytes.Compare(validEntries[i].Key[:], validEntries[j].Key[:]) < 0
	})

	// Write sorted, deduplicated data to temp file
	newOffset := int64(0)
	for _, entry := range validEntries {
		// Read old value
		value := make([]byte, entry.ValueLength)
		if _, err := bucket.valueFile.ReadAt(value, int64(entry.ValueOffset)); err != nil {
			return err
		}

		// Write to new file
		if _, err := tempFile.Write(value); err != nil {
			return err
		}

		// Update entry with new offset
		entry.ValueOffset = uint64(newOffset)
		newOffset += int64(entry.ValueLength)
	}

	// Sync temp file
	if err := tempFile.Sync(); err != nil {
		return err
	}
	tempFile.Close()

	// Close old file
	bucket.valueFile.Close()

	// Rename temp file to replace old file
	if err := os.Rename(tempPath, bucket.valueFilePath); err != nil {
		return err
	}

	// Reopen the compacted file
	bucket.valueFile, err = os.OpenFile(bucket.valueFilePath,
		os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// Update statistics
	bucket.valueOffset.Store(newOffset)
	bucket.totalSize.Store(newOffset)
	bucket.wastedSpace.Store(0)
	dkv.totalCompacts.Add(1)

	return nil
}

// loadIndices loads existing indices from disk
func (dkv *DKV) loadIndices() error {
	for _, bucket := range dkv.hashBuckets {
		indexPath := filepath.Join(bucket.directory, fmt.Sprintf("%s%02x.idx", dkvIndexPrefix, bucket.id))

		// Check if index file exists
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			continue // No existing index
		}

		// Read index file
		data, err := os.ReadFile(indexPath)
		if err != nil {
			return err
		}

		// Parse index entries
		offset := 0
		for offset < len(data) {
			if len(data[offset:]) < entryHeaderSize {
				break
			}

			entry := &DKVEntry{}
			copy(entry.Key[:], data[offset:offset+32])
			entry.ValueOffset = binary.BigEndian.Uint64(data[offset+32:])
			entry.ValueLength = binary.BigEndian.Uint64(data[offset+40:])

			bucket.index[entry.Key] = entry
			offset += entryHeaderSize
		}

		bucket.entryCount.Store(int64(len(bucket.index)))
	}

	return nil
}

// SaveIndices saves all indices to disk
func (dkv *DKV) SaveIndices() error {
	for _, bucket := range dkv.hashBuckets {
		if err := dkv.saveBucketIndex(bucket); err != nil {
			return err
		}
	}
	return nil
}

// saveBucketIndex saves a single bucket's index to disk
func (dkv *DKV) saveBucketIndex(bucket *HashBucket) error {
	bucket.indexMutex.RLock()
	defer bucket.indexMutex.RUnlock()

	// Prepare sorted index data
	entries := make([]*DKVEntry, 0, len(bucket.index))
	for _, entry := range bucket.index {
		if !entry.Deleted {
			entries = append(entries, entry)
		}
	}

	// Sort by key
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key[:], entries[j].Key[:]) < 0
	})

	// Marshal to bytes
	data := make([]byte, 0, len(entries)*entryHeaderSize)
	for _, entry := range entries {
		entryData := make([]byte, entryHeaderSize)
		copy(entryData[:32], entry.Key[:])
		binary.BigEndian.PutUint64(entryData[32:], entry.ValueOffset)
		binary.BigEndian.PutUint64(entryData[40:], entry.ValueLength)
		data = append(data, entryData...)
	}

	// Write to index file
	indexPath := filepath.Join(bucket.directory, fmt.Sprintf("%s%02x.idx", dkvIndexPrefix, bucket.id))
	return os.WriteFile(indexPath, data, 0644)
}

// Close closes the DKV store
func (dkv *DKV) Close() error {
	// Flush all pending writes
	for _, bucket := range dkv.hashBuckets {
		if err := dkv.flushBucket(bucket); err != nil {
			return err
		}
	}

	// Save indices
	if err := dkv.SaveIndices(); err != nil {
		return err
	}

	// Close all value files
	for _, bucket := range dkv.hashBuckets {
		if err := bucket.valueFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Stats returns statistics about the DKV store
func (dkv *DKV) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"total_reads":    dkv.totalReads.Load(),
		"total_writes":   dkv.totalWrites.Load(),
		"total_deletes":  dkv.totalDeletes.Load(),
		"total_compacts": dkv.totalCompacts.Load(),
		"buckets":        make([]map[string]interface{}, numHashBuckets),
	}

	for i, bucket := range dkv.hashBuckets {
		bucketStats := map[string]interface{}{
			"id":           bucket.id,
			"entries":      bucket.entryCount.Load(),
			"total_size":   bucket.totalSize.Load(),
			"wasted_space": bucket.wastedSpace.Load(),
		}
		stats["buckets"].([]map[string]interface{})[i] = bucketStats
	}

	return stats
}

// Compact forces compaction of all buckets that need it
func (dkv *DKV) Compact() error {
	for _, bucket := range dkv.hashBuckets {
		if dkv.shouldCompactBucket(bucket) {
			if err := dkv.compactBucket(bucket); err != nil {
				return err
			}
		}
	}
	return nil
}