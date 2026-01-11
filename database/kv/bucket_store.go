package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// BucketStore is a hash-bucket based key-value store for immutable data.
// Keys are hashed to buckets. Buckets grow dynamically as needed.
//
// File layout:
//   - Header (4KB): magic, version, bucket_count, entry_count
//   - Bucket index: array of BucketMeta (offset, size, count per bucket)
//   - Buckets: contiguous regions holding key entries
//   - Growth area: new bucket locations when originals overflow
//
// Entry format (48 bytes):
//   - Key: 32 bytes
//   - Value offset: 8 bytes (pointer into value file)
//   - Value length: 8 bytes
type BucketStore struct {
	directory string

	indexFile  *os.File // Bucket index and metadata
	valueFile  *os.File // Append-only value storage
	valueOffset int64   // Current write position in value file

	bucketCount  uint32
	bucketSize   uint32       // Initial bucket size in bytes
	buckets      []BucketMeta // In-memory bucket index

	entryCount   uint64

	mu sync.RWMutex
}

// BucketMeta describes a bucket's location and state
type BucketMeta struct {
	Offset   int64  // Offset in index file
	Size     uint32 // Allocated size in bytes
	Count    uint32 // Number of entries
	Capacity uint32 // Max entries that fit (Size / EntrySize)
}

const (
	bucketMagic       = "BKTS"
	bucketVersion     = 1
	bucketHeaderSize  = 4096
	bucketMetaSize    = 24 // 8 + 4 + 4 + 4 + padding
	bucketEntrySize   = 36 // 20 key + 8 offset + 8 length
	defaultBucketSize = 64 * 1024 // 64KB per bucket initially
	defaultBucketCount = 1024     // 1024 buckets
)

// NewBucketStore creates a new bucket-based store
func NewBucketStore(directory string, bucketCount, bucketSize uint32) (*BucketStore, error) {
	if bucketCount == 0 {
		bucketCount = defaultBucketCount
	}
	if bucketSize == 0 {
		bucketSize = defaultBucketSize
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	bs := &BucketStore{
		directory:   directory,
		bucketCount: bucketCount,
		bucketSize:  bucketSize,
		buckets:     make([]BucketMeta, bucketCount),
	}

	// Create index file
	indexPath := filepath.Join(directory, "buckets.idx")
	var err error
	bs.indexFile, err = os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Create value file
	valuePath := filepath.Join(directory, "values.dat")
	bs.valueFile, err = os.OpenFile(valuePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		bs.indexFile.Close()
		return nil, err
	}

	// Initialize file layout
	if err := bs.initializeLayout(); err != nil {
		bs.Close()
		return nil, err
	}

	return bs, nil
}

// OpenBucketStore opens an existing bucket store
func OpenBucketStore(directory string) (*BucketStore, error) {
	bs := &BucketStore{
		directory: directory,
	}

	// Open index file
	indexPath := filepath.Join(directory, "buckets.idx")
	var err error
	bs.indexFile, err = os.OpenFile(indexPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Open value file
	valuePath := filepath.Join(directory, "values.dat")
	bs.valueFile, err = os.OpenFile(valuePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		bs.indexFile.Close()
		return nil, err
	}

	// Read header and bucket index
	if err := bs.loadHeader(); err != nil {
		bs.Close()
		return nil, err
	}

	// Get value file offset
	info, _ := bs.valueFile.Stat()
	bs.valueOffset = info.Size()

	return bs, nil
}

// initializeLayout creates the initial file structure
func (bs *BucketStore) initializeLayout() error {
	// Calculate layout
	indexSize := bucketHeaderSize + int64(bs.bucketCount)*bucketMetaSize
	bucketsStart := indexSize

	// Total file size: header + index + all buckets
	totalSize := bucketsStart + int64(bs.bucketCount)*int64(bs.bucketSize)

	// Extend file
	if err := bs.indexFile.Truncate(totalSize); err != nil {
		return err
	}

	// Write header
	header := make([]byte, bucketHeaderSize)
	copy(header[0:4], bucketMagic)
	binary.BigEndian.PutUint32(header[4:8], bucketVersion)
	binary.BigEndian.PutUint32(header[8:12], bs.bucketCount)
	binary.BigEndian.PutUint32(header[12:16], bs.bucketSize)
	binary.BigEndian.PutUint64(header[16:24], 0) // entry count

	if _, err := bs.indexFile.WriteAt(header, 0); err != nil {
		return err
	}

	// Initialize bucket metadata
	capacity := bs.bucketSize / bucketEntrySize
	for i := uint32(0); i < bs.bucketCount; i++ {
		bs.buckets[i] = BucketMeta{
			Offset:   bucketsStart + int64(i)*int64(bs.bucketSize),
			Size:     bs.bucketSize,
			Count:    0,
			Capacity: capacity,
		}
	}

	// Write bucket index
	return bs.writeBucketIndex()
}

// loadHeader reads the header and bucket index from disk
func (bs *BucketStore) loadHeader() error {
	header := make([]byte, bucketHeaderSize)
	if _, err := bs.indexFile.ReadAt(header, 0); err != nil {
		return err
	}

	if string(header[0:4]) != bucketMagic {
		return errors.New("invalid bucket store magic")
	}

	version := binary.BigEndian.Uint32(header[4:8])
	if version != bucketVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	bs.bucketCount = binary.BigEndian.Uint32(header[8:12])
	bs.bucketSize = binary.BigEndian.Uint32(header[12:16])
	bs.entryCount = binary.BigEndian.Uint64(header[16:24])

	// Read bucket index
	bs.buckets = make([]BucketMeta, bs.bucketCount)
	indexData := make([]byte, bs.bucketCount*bucketMetaSize)
	if _, err := bs.indexFile.ReadAt(indexData, bucketHeaderSize); err != nil {
		return err
	}

	for i := uint32(0); i < bs.bucketCount; i++ {
		offset := i * bucketMetaSize
		bs.buckets[i] = BucketMeta{
			Offset:   int64(binary.BigEndian.Uint64(indexData[offset : offset+8])),
			Size:     binary.BigEndian.Uint32(indexData[offset+8 : offset+12]),
			Count:    binary.BigEndian.Uint32(indexData[offset+12 : offset+16]),
			Capacity: binary.BigEndian.Uint32(indexData[offset+16 : offset+20]),
		}
	}

	return nil
}

// writeBucketIndex writes the bucket index to disk
func (bs *BucketStore) writeBucketIndex() error {
	indexData := make([]byte, bs.bucketCount*bucketMetaSize)

	for i := uint32(0); i < bs.bucketCount; i++ {
		offset := i * bucketMetaSize
		binary.BigEndian.PutUint64(indexData[offset:offset+8], uint64(bs.buckets[i].Offset))
		binary.BigEndian.PutUint32(indexData[offset+8:offset+12], bs.buckets[i].Size)
		binary.BigEndian.PutUint32(indexData[offset+12:offset+16], bs.buckets[i].Count)
		binary.BigEndian.PutUint32(indexData[offset+16:offset+20], bs.buckets[i].Capacity)
	}

	_, err := bs.indexFile.WriteAt(indexData, bucketHeaderSize)
	return err
}

// updateHeader writes the entry count to the header
func (bs *BucketStore) updateHeader() error {
	countBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(countBytes, bs.entryCount)
	_, err := bs.indexFile.WriteAt(countBytes, 16)
	return err
}

// bucketIndex returns which bucket a key belongs to
func (bs *BucketStore) bucketIndex(key InternalKey) uint32 {
	// Use first 4 bytes of key as hash
	h := binary.BigEndian.Uint32(key[0:4])
	return h % bs.bucketCount
}

// Put stores a key-value pair
func (bs *BucketStore) Put(key InternalKey, value []byte) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// Write value to value file
	valueOffset := bs.valueOffset
	if _, err := bs.valueFile.Write(value); err != nil {
		return err
	}
	bs.valueOffset += int64(len(value))

	// Create entry
	entry := make([]byte, bucketEntrySize)
	copy(entry[0:InternalKeySize], key[:])
	binary.BigEndian.PutUint64(entry[InternalKeySize:InternalKeySize+8], uint64(valueOffset))
	binary.BigEndian.PutUint64(entry[InternalKeySize+8:InternalKeySize+16], uint64(len(value)))

	// Find bucket
	idx := bs.bucketIndex(key)
	bucket := &bs.buckets[idx]

	// Check if bucket needs to grow
	if bucket.Count >= bucket.Capacity {
		if err := bs.growBucket(idx); err != nil {
			return err
		}
	}

	// Write entry to bucket
	entryOffset := bucket.Offset + int64(bucket.Count)*bucketEntrySize
	if _, err := bs.indexFile.WriteAt(entry, entryOffset); err != nil {
		return err
	}

	bucket.Count++
	bs.entryCount++

	return nil
}

// growBucket relocates a bucket to the end of file.
// The vacated space is given to the previous bucket, so no space is wasted.
//
// Before: | B0 (64KB) | B1 (64KB) | B2 (64KB) |
// B1 overflows, moves to end:
// After:  | B0 (128KB)           | B2 (64KB) | B1' (new, 128KB) |
//           ^-- B0 absorbs B1's old space
func (bs *BucketStore) growBucket(idx uint32) error {
	bucket := &bs.buckets[idx]
	oldSize := bucket.Size

	// Read existing entries
	oldData := make([]byte, bucket.Count*uint32(bucketEntrySize))
	if _, err := bs.indexFile.ReadAt(oldData, bucket.Offset); err != nil {
		return err
	}

	// Allocate new space at end of file (2x current size for growth room)
	info, err := bs.indexFile.Stat()
	if err != nil {
		return err
	}
	newOffset := info.Size()
	newSize := bucket.Size * 2
	newCapacity := newSize / bucketEntrySize

	// Extend file
	if err := bs.indexFile.Truncate(newOffset + int64(newSize)); err != nil {
		return err
	}

	// Copy entries to new location
	if _, err := bs.indexFile.WriteAt(oldData, newOffset); err != nil {
		return err
	}

	// Update this bucket's metadata
	bucket.Offset = newOffset
	bucket.Size = newSize
	bucket.Capacity = newCapacity

	// Give the vacated space to the previous bucket (if exists)
	if idx > 0 {
		prevBucket := &bs.buckets[idx-1]
		prevBucket.Size += oldSize
		prevBucket.Capacity = prevBucket.Size / bucketEntrySize
		if err := bs.writeBucketMeta(idx - 1); err != nil {
			return err
		}
	}

	// Persist this bucket's new location
	return bs.writeBucketMeta(idx)
}

// writeBucketMeta writes a single bucket's metadata
func (bs *BucketStore) writeBucketMeta(idx uint32) error {
	bucket := &bs.buckets[idx]
	data := make([]byte, bucketMetaSize)
	binary.BigEndian.PutUint64(data[0:8], uint64(bucket.Offset))
	binary.BigEndian.PutUint32(data[8:12], bucket.Size)
	binary.BigEndian.PutUint32(data[12:16], bucket.Count)
	binary.BigEndian.PutUint32(data[16:20], bucket.Capacity)

	offset := bucketHeaderSize + int64(idx)*bucketMetaSize
	_, err := bs.indexFile.WriteAt(data, offset)
	return err
}

// Get retrieves a value by key
func (bs *BucketStore) Get(key InternalKey) ([]byte, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	idx := bs.bucketIndex(key)
	bucket := &bs.buckets[idx]

	if bucket.Count == 0 {
		return nil, errors.New("key not found")
	}

	// Read bucket entries
	bucketData := make([]byte, bucket.Count*uint32(bucketEntrySize))
	if _, err := bs.indexFile.ReadAt(bucketData, bucket.Offset); err != nil {
		return nil, err
	}

	// Scan for key
	for i := uint32(0); i < bucket.Count; i++ {
		offset := i * bucketEntrySize
		var entryKey InternalKey
		copy(entryKey[:], bucketData[offset:offset+InternalKeySize])

		if entryKey == key {
			valueOffset := binary.BigEndian.Uint64(bucketData[offset+InternalKeySize : offset+InternalKeySize+8])
			valueLength := binary.BigEndian.Uint64(bucketData[offset+InternalKeySize+8 : offset+InternalKeySize+16])

			// Read value
			value := make([]byte, valueLength)
			if _, err := bs.valueFile.ReadAt(value, int64(valueOffset)); err != nil {
				return nil, err
			}
			return value, nil
		}
	}

	return nil, errors.New("key not found")
}

// Has checks if a key exists
func (bs *BucketStore) Has(key InternalKey) bool {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	idx := bs.bucketIndex(key)
	bucket := &bs.buckets[idx]

	if bucket.Count == 0 {
		return false
	}

	// Read bucket entries
	bucketData := make([]byte, bucket.Count*uint32(bucketEntrySize))
	if _, err := bs.indexFile.ReadAt(bucketData, bucket.Offset); err != nil {
		return false
	}

	// Scan for key
	for i := uint32(0); i < bucket.Count; i++ {
		offset := i * bucketEntrySize
		var entryKey InternalKey
		copy(entryKey[:], bucketData[offset:offset+InternalKeySize])

		if entryKey == key {
			return true
		}
	}

	return false
}

// Close closes the store
func (bs *BucketStore) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// Update header with final entry count
	if err := bs.updateHeader(); err != nil {
		return err
	}

	// Write final bucket index
	if err := bs.writeBucketIndex(); err != nil {
		return err
	}

	// Sync and close files
	bs.indexFile.Sync()
	bs.valueFile.Sync()

	if err := bs.indexFile.Close(); err != nil {
		return err
	}
	return bs.valueFile.Close()
}

// Stats returns store statistics
func (bs *BucketStore) Stats() BucketStoreStats {
	bs.mu.RLock()
	defer bs.mu.RUnlock()

	var totalBucketBytes int64
	var maxBucketCount uint32
	var emptyBuckets uint32

	for _, b := range bs.buckets {
		totalBucketBytes += int64(b.Size)
		if b.Count > maxBucketCount {
			maxBucketCount = b.Count
		}
		if b.Count == 0 {
			emptyBuckets++
		}
	}

	return BucketStoreStats{
		EntryCount:      bs.entryCount,
		BucketCount:     bs.bucketCount,
		TotalBucketBytes: totalBucketBytes,
		MaxBucketEntries: maxBucketCount,
		EmptyBuckets:    emptyBuckets,
		AvgEntriesPerBucket: float64(bs.entryCount) / float64(bs.bucketCount),
	}
}

// BucketStoreStats contains store statistics
type BucketStoreStats struct {
	EntryCount          uint64
	BucketCount         uint32
	TotalBucketBytes    int64
	MaxBucketEntries    uint32
	EmptyBuckets        uint32
	AvgEntriesPerBucket float64
}
