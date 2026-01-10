package kv

import (
	"github.com/AccumulateNetwork/BlockchainDB/database/storage"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	KVKeyEntrySize   = 48   // 32 byte key + 8 byte offset + 8 byte length
	KVHeaderSize     = 1024 // Space for header at start of file
	BinMetaSize      = 20   // 8 byte offset + 8 byte size + 4 byte count per bin
	valueFilename    = "values.dat"
	valueTmpFilename = "values_tmp.dat"
	kvFilename       = "kv_index.dat"
	binMetaFilename  = "bin_meta.dat"
)

// KV implements a high-performance key-value store using the hybrid sorted/unsorted approach
// Originally from HistoryFile, now the main KV implementation
// - Writes are O(1) append operations to memory
// - Reads check memory first (O(1)), then sorted disk (O(log n))
// - Background task automatically sorts data for optimal read performance
type KV struct {
	// Directories and files
	Directory   string
	ValueFile   *storage.BFile   // Append-only value storage
	IndexFile   *os.File // Key index file

	// Configuration
	BinCount    int32  // Number of bins for key distribution
	HeaderSizeKV uint64 // Size of header in bytes

	// Bin management using hybrid approach
	bins []*HybridBin // Hybrid bins for key management

	// Caching
	keyCache     map[[32]byte]*KeyLocation
	maxCacheSize int
	cacheHits    atomic.Int64
	cacheMisses  atomic.Int64

	// Hybrid configuration
	maxUnsortedEntries int // Max entries before triggering background sort
	sortBatchSize      int // Number of bins to sort per background cycle

	// Background sorting
	sortQueue  chan int
	stopSignal chan struct{}
	sortWg     sync.WaitGroup

	// Global statistics
	totalReads  atomic.Int64
	totalWrites atomic.Int64
	totalSorts  atomic.Int64

	// Mutex for thread safety
	mu sync.RWMutex
}

// KeyLocation describes where a value is stored
type KeyLocation struct {
	Offset uint64
	Length uint64
}

// HybridBin represents a single bin with hybrid sorted/unsorted storage
type HybridBin struct {
	binIndex int
	file     *os.File // Reference to index file

	// Sorted section (on disk)
	sortedOffset int64 // Where sorted data starts in file
	sortedSize   int64 // Size of sorted section
	sortedCount  int32 // Number of sorted entries

	// Unsorted section (in memory + will be persisted)
	unsortedBuffer []byte // In-memory buffer for recent writes
	unsortedCount  int32  // Number of unsorted entries

	// Memory index for fast lookups
	memIndex map[[32]byte]*KeyLocation // key -> location for O(1) lookup

	// Statistics
	reads  atomic.Int64
	writes atomic.Int64
	sorts  atomic.Int64

	mu sync.RWMutex
}

// NewKV creates a new key-value store with hybrid storage
func NewKV(directory string, binCount int32) (*KV, error) {
	if binCount <= 0 || binCount > 10240 {
		return nil, fmt.Errorf("bin count must be between 1 and 10240, got %d", binCount)
	}

	// Create directory
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	kv := &KV{
		Directory:          directory,
		BinCount:           binCount,
		HeaderSizeKV:       KVHeaderSize,
		keyCache:           make(map[[32]byte]*KeyLocation),
		maxCacheSize:       10000,
		maxUnsortedEntries: 100, // Lower threshold for memory efficiency
		sortBatchSize:      10,
		sortQueue:          make(chan int, binCount),
		stopSignal:         make(chan struct{}),
	}

	// Create value file
	valueFilePath := filepath.Join(directory, valueFilename)
	var err error
	if kv.ValueFile, err = storage.NewBFile(valueFilePath); err != nil {
		return nil, err
	}

	// Create index file
	indexFilePath := filepath.Join(directory, kvFilename)
	if kv.IndexFile, err = os.Create(indexFilePath); err != nil {
		return nil, err
	}

	// Initialize bins
	kv.bins = make([]*HybridBin, binCount)
	for i := int32(0); i < binCount; i++ {
		kv.bins[i] = &HybridBin{
			binIndex:       int(i),
			file:           kv.IndexFile,
			memIndex:       make(map[[32]byte]*KeyLocation),
			unsortedBuffer: make([]byte, 0, 100*KVKeyEntrySize),
		}
	}

	// Write header
	if err := kv.writeHeader(); err != nil {
		return nil, err
	}

	// Start background sorter
	kv.startBackgroundSorter()

	return kv, nil
}

// OpenKV opens an existing key-value store
func OpenKV(directory string) (*KV, error) {
	kv := &KV{
		Directory:          directory,
		keyCache:           make(map[[32]byte]*KeyLocation),
		maxCacheSize:       10000,
		maxUnsortedEntries: 100, // Lower threshold for memory efficiency
		sortBatchSize:      10,
		sortQueue:          make(chan int, 1024),
		stopSignal:         make(chan struct{}),
	}

	// Open value file
	valueFilePath := filepath.Join(directory, valueFilename)
	var err error
	if kv.ValueFile, err = storage.OpenBFile(valueFilePath); err != nil {
		return nil, err
	}

	// Open index file
	indexFilePath := filepath.Join(directory, kvFilename)
	if kv.IndexFile, err = os.OpenFile(indexFilePath, os.O_RDWR, 0644); err != nil {
		return nil, err
	}

	// Read header and initialize
	if err := kv.readHeader(); err != nil {
		return nil, err
	}

	// Initialize bins
	kv.bins = make([]*HybridBin, kv.BinCount)
	for i := int32(0); i < kv.BinCount; i++ {
		kv.bins[i] = &HybridBin{
			binIndex:       int(i),
			file:           kv.IndexFile,
			memIndex:       make(map[[32]byte]*KeyLocation),
			unsortedBuffer: make([]byte, 0, 100*KVKeyEntrySize),
		}
	}

	// Load bin metadata first (sets sortedOffset, sortedSize, sortedCount)
	if err := kv.loadBinMetadata(); err != nil {
		return nil, err
	}

	// Now load bin data into memIndex
	for i := int32(0); i < kv.BinCount; i++ {
		if err := kv.loadBin(kv.bins[i]); err != nil {
			return nil, err
		}
	}

	// Start background sorter
	kv.startBackgroundSorter()

	return kv, nil
}

// Put stores a key-value pair
func (kv *KV) Put(key [32]byte, value []byte) error {
	kv.totalWrites.Add(1)

	// Get offset for value
	offset, err := kv.ValueFile.Offset()
	if err != nil {
		return err
	}

	// Write value to value file
	if _, err := kv.ValueFile.Write(value); err != nil {
		return err
	}

	// Create key location
	location := &KeyLocation{
		Offset: offset,
		Length: uint64(len(value)),
	}

	// Determine bin
	binIndex := kv.getBinIndex(key)
	bin := kv.bins[binIndex]

	// Add to bin
	return bin.Put(key, location, kv.sortQueue)
}

// Get retrieves a value by key
func (kv *KV) Get(key [32]byte) ([]byte, error) {
	kv.totalReads.Add(1)

	// Check cache first
	kv.mu.RLock()
	if loc, ok := kv.keyCache[key]; ok {
		kv.mu.RUnlock()
		kv.cacheHits.Add(1)
		return kv.readValue(loc)
	}
	kv.mu.RUnlock()
	kv.cacheMisses.Add(1)

	// Determine bin
	binIndex := kv.getBinIndex(key)
	bin := kv.bins[binIndex]

	// Get from bin
	location, err := bin.Get(key)
	if err != nil {
		return nil, err
	}

	// Update cache
	kv.updateCache(key, location)

	// Read value
	return kv.readValue(location)
}

// Put adds a key to the bin
func (bin *HybridBin) Put(key [32]byte, location *KeyLocation, sortQueue chan<- int) error {
	bin.mu.Lock()
	defer bin.mu.Unlock()

	bin.writes.Add(1)

	// Add to memory index
	bin.memIndex[key] = location

	// Add to unsorted buffer
	keyEntry := make([]byte, KVKeyEntrySize)
	copy(keyEntry[:32], key[:])
	binary.BigEndian.PutUint64(keyEntry[32:40], location.Offset)
	binary.BigEndian.PutUint64(keyEntry[40:48], location.Length)

	bin.unsortedBuffer = append(bin.unsortedBuffer, keyEntry...)
	bin.unsortedCount++

	// Check if we need to trigger background sort
	// Sort frequently to keep memIndex small and avoid memory bloat
	if bin.unsortedCount >= int32(100) { // maxUnsortedEntries - lower threshold for memory efficiency
		select {
		case sortQueue <- bin.binIndex:
			// Queued for sorting
		default:
			// Queue is full, will be sorted later
		}
	}

	return nil
}

// Get retrieves a key from the bin
func (bin *HybridBin) Get(key [32]byte) (*KeyLocation, error) {
	bin.mu.RLock()
	bin.reads.Add(1)

	// Check memory index first (O(1))
	if loc, ok := bin.memIndex[key]; ok {
		bin.mu.RUnlock()
		return loc, nil
	}

	// Check if we have sorted data
	if bin.sortedCount > 0 {
		bin.mu.RUnlock()
		// Binary search in sorted section
		return bin.binarySearch(key)
	}

	bin.mu.RUnlock()
	return nil, errors.New("key not found")
}

// binarySearch performs binary search in the sorted section
func (bin *HybridBin) binarySearch(key [32]byte) (*KeyLocation, error) {
	bin.mu.RLock()
	defer bin.mu.RUnlock()

	if bin.sortedCount == 0 {
		return nil, errors.New("no sorted data")
	}

	// Read sorted section
	sortedData := make([]byte, bin.sortedSize)
	if _, err := bin.file.ReadAt(sortedData, bin.sortedOffset); err != nil {
		return nil, err
	}

	// Binary search
	numEntries := int(bin.sortedSize / KVKeyEntrySize)
	left, right := 0, numEntries-1

	for left <= right {
		mid := (left + right) / 2
		offset := mid * KVKeyEntrySize

		var midKey [32]byte
		copy(midKey[:], sortedData[offset:offset+32])

		cmp := bytes.Compare(midKey[:], key[:])
		if cmp == 0 {
			// Found it
			loc := &KeyLocation{
				Offset: binary.BigEndian.Uint64(sortedData[offset+32 : offset+40]),
				Length: binary.BigEndian.Uint64(sortedData[offset+40 : offset+48]),
			}
			return loc, nil
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil, errors.New("key not found")
}

// getBinIndex determines which bin a key belongs to
func (kv *KV) getBinIndex(key [32]byte) int {
	// Use first bytes of key to determine bin
	hash := binary.BigEndian.Uint32(key[:4])
	return int(hash % uint32(kv.BinCount))
}

// readValue reads a value from the value file
func (kv *KV) readValue(loc *KeyLocation) ([]byte, error) {
	value := make([]byte, loc.Length)
	if err := kv.ValueFile.ReadAt(loc.Offset, value); err != nil {
		return nil, err
	}
	return value, nil
}

// updateCache updates the key cache
func (kv *KV) updateCache(key [32]byte, location *KeyLocation) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Evict if cache is full
	if len(kv.keyCache) >= kv.maxCacheSize {
		// Simple eviction: remove a random entry
		for k := range kv.keyCache {
			delete(kv.keyCache, k)
			break
		}
	}

	kv.keyCache[key] = location
}

// startBackgroundSorter starts the background sorting goroutine
func (kv *KV) startBackgroundSorter() {
	kv.sortWg.Add(1)
	go func() {
		defer kv.sortWg.Done()

		for {
			select {
			case binIndex := <-kv.sortQueue:
				// Sort this bin
				kv.sortBin(binIndex)

			case <-kv.stopSignal:
				return
			}
		}
	}()
}

// sortBin sorts the unsorted entries in a bin
// Uses a per-bin sorted file that gets replaced to avoid write amplification
func (kv *KV) sortBin(binIndex int) {
	bin := kv.bins[binIndex]

	bin.mu.Lock()
	defer bin.mu.Unlock()

	if bin.unsortedCount == 0 {
		return
	}

	bin.sorts.Add(1)
	kv.totalSorts.Add(1)

	// Combine sorted and unsorted data
	var allEntries [][]byte

	// Load existing sorted entries from per-bin file
	if bin.sortedCount > 0 {
		binSortedPath := filepath.Join(kv.Directory, fmt.Sprintf("bin_%04d.idx", binIndex))
		sortedData, err := os.ReadFile(binSortedPath)
		if err == nil {
			numSorted := len(sortedData) / KVKeyEntrySize
			for i := 0; i < numSorted; i++ {
				offset := i * KVKeyEntrySize
				entry := make([]byte, KVKeyEntrySize)
				copy(entry, sortedData[offset:offset+KVKeyEntrySize])
				allEntries = append(allEntries, entry)
			}
		}
	}

	// Add unsorted entries
	numUnsorted := int(bin.unsortedCount)
	for i := 0; i < numUnsorted; i++ {
		offset := i * KVKeyEntrySize
		entry := make([]byte, KVKeyEntrySize)
		copy(entry, bin.unsortedBuffer[offset:offset+KVKeyEntrySize])
		allEntries = append(allEntries, entry)
	}

	// Sort all entries
	sort.Slice(allEntries, func(i, j int) bool {
		return bytes.Compare(allEntries[i][:32], allEntries[j][:32]) < 0
	})

	// Write sorted data to per-bin file (replaces old file - no write amplification)
	sortedData := make([]byte, 0, len(allEntries)*KVKeyEntrySize)
	for _, entry := range allEntries {
		sortedData = append(sortedData, entry...)
	}

	binSortedPath := filepath.Join(kv.Directory, fmt.Sprintf("bin_%04d.idx", binIndex))
	if err := os.WriteFile(binSortedPath, sortedData, 0644); err != nil {
		return
	}

	// Update bin metadata (no longer using shared file offsets)
	bin.sortedSize = int64(len(sortedData))
	bin.sortedCount = int32(len(allEntries))

	// Clear unsorted buffer AND memIndex
	// memIndex only holds unsorted entries - once sorted to disk, we use binary search
	bin.unsortedBuffer = bin.unsortedBuffer[:0]
	bin.unsortedCount = 0
	bin.memIndex = make(map[[32]byte]*KeyLocation) // Clear to free memory
}

// writeHeader writes the KV header to the index file
func (kv *KV) writeHeader() error {
	header := make([]byte, KVHeaderSize)

	// Magic bytes
	copy(header[0:4], []byte("KVDB"))

	// Version
	binary.BigEndian.PutUint32(header[4:8], 1)

	// Bin count
	binary.BigEndian.PutUint32(header[8:12], uint32(kv.BinCount))

	// Write to file
	_, err := kv.IndexFile.WriteAt(header, 0)
	return err
}

// readHeader reads the KV header from the index file
func (kv *KV) readHeader() error {
	header := make([]byte, KVHeaderSize)

	if _, err := kv.IndexFile.ReadAt(header, 0); err != nil {
		return err
	}

	// Check magic bytes
	if string(header[0:4]) != "KVDB" {
		return errors.New("invalid KV file format")
	}

	// Check version
	version := binary.BigEndian.Uint32(header[4:8])
	if version != 1 {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Read bin count
	kv.BinCount = int32(binary.BigEndian.Uint32(header[8:12]))

	return nil
}

// loadBin loads existing data for a bin from disk
func (kv *KV) loadBin(bin *HybridBin) error {
	// Bin metadata is loaded separately via loadBinMetadata
	// memIndex only holds unsorted entries (recent writes), not all entries
	// For sorted data, we use binary search on disk to avoid massive memory usage
	// With 100M entries, loading all keys would consume ~10GB of heap
	return nil
}

// loadBinMetadata loads bin metadata from the metadata file
func (kv *KV) loadBinMetadata() error {
	metaPath := filepath.Join(kv.Directory, binMetaFilename)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No metadata file yet, bins start empty
		}
		return err
	}

	// Validate size
	expectedSize := int(kv.BinCount) * BinMetaSize
	if len(data) < expectedSize {
		return nil // Metadata file too small, treat as empty
	}

	// Load each bin's metadata
	for i := int32(0); i < kv.BinCount; i++ {
		offset := int(i) * BinMetaSize
		bin := kv.bins[i]
		bin.sortedOffset = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		bin.sortedSize = int64(binary.BigEndian.Uint64(data[offset+8 : offset+16]))
		bin.sortedCount = int32(binary.BigEndian.Uint32(data[offset+16 : offset+20]))
	}

	return nil
}

// saveBinMetadata saves bin metadata to the metadata file
func (kv *KV) saveBinMetadata() error {
	data := make([]byte, int(kv.BinCount)*BinMetaSize)

	for i, bin := range kv.bins {
		offset := i * BinMetaSize
		bin.mu.RLock()
		binary.BigEndian.PutUint64(data[offset:offset+8], uint64(bin.sortedOffset))
		binary.BigEndian.PutUint64(data[offset+8:offset+16], uint64(bin.sortedSize))
		binary.BigEndian.PutUint32(data[offset+16:offset+20], uint32(bin.sortedCount))
		bin.mu.RUnlock()
	}

	metaPath := filepath.Join(kv.Directory, binMetaFilename)
	return os.WriteFile(metaPath, data, 0644)
}

// Close closes the KV store
func (kv *KV) Close() error {
	// Stop background sorter
	close(kv.stopSignal)
	kv.sortWg.Wait()

	// Flush all unsorted data
	for i := range kv.bins {
		kv.sortBin(i)
	}

	// Save bin metadata for persistence
	if err := kv.saveBinMetadata(); err != nil {
		return err
	}

	// Close files
	if err := kv.ValueFile.Close(); err != nil {
		return err
	}

	if err := kv.IndexFile.Close(); err != nil {
		return err
	}

	return nil
}

// Stats returns statistics about the KV store
func (kv *KV) Stats() KVStats {
	stats := KVStats{
		TotalReads:  kv.totalReads.Load(),
		TotalWrites: kv.totalWrites.Load(),
		TotalSorts:  kv.totalSorts.Load(),
		CacheHits:   kv.cacheHits.Load(),
		CacheMisses: kv.cacheMisses.Load(),
		BinStats:    make([]BinStats, kv.BinCount),
	}

	for i, bin := range kv.bins {
		bin.mu.RLock()
		stats.BinStats[i] = BinStats{
			BinIndex:      i,
			SortedCount:   bin.sortedCount,
			UnsortedCount: bin.unsortedCount,
			MemoryKeys:    len(bin.memIndex),
			Reads:         bin.reads.Load(),
			Writes:        bin.writes.Load(),
			Sorts:         bin.sorts.Load(),
		}
		bin.mu.RUnlock()
	}

	return stats
}

// KVStats contains statistics about the KV store
type KVStats struct {
	TotalReads  int64
	TotalWrites int64
	TotalSorts  int64
	CacheHits   int64
	CacheMisses int64
	BinStats    []BinStats
}

// BinStats contains statistics about a single bin
type BinStats struct {
	BinIndex      int
	SortedCount   int32
	UnsortedCount int32
	MemoryKeys    int
	Reads         int64
	Writes        int64
	Sorts         int64
}