package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	WriteBufferSize = 4 * 1024 * 1024  // 4MB write buffer
	BinCacheSize    = 16 * 1024        // 16KB per bin cache
	MaxBinFileSize  = 4 * 1024 * 1024  // 4MB triggers fan-out
	NumBins         = 256               // Number of bins per level
	WALSegmentSize  = 1024 * 1024      // 1MB WAL segments
)

// MultiHashTable implements a multi-level hash table with buffered merging
type MultiHashTable struct {
	directory    string
	writeBuffer  *WriteBuffer
	levels       []*TreeLevel
	wal          *WAL
	stats        *Statistics
	shutdownCh   chan struct{}
	wg           sync.WaitGroup
	mu           sync.RWMutex
}

// TreeLevel represents one level in the hash tree
type TreeLevel struct {
	depth int
	bins  [NumBins]*Bin
}

// Bin represents a single bin at a tree level
type Bin struct {
	id       string           // e.g., "0A", "0A3F"
	level    int
	index    byte
	cache    *BinCache
	file     string           // Path to sorted data file
	tempFile string           // Path to temp file during merge
	size     int64
	version  uint64           // Version for copy-on-write
	mu       sync.RWMutex
}

// BinCache holds entries in memory before merging to disk
type BinCache struct {
	entries []HashEntry
	size    int64
	maxSize int64
	sorted  bool
}

// HashEntry represents a key-value pair
type HashEntry struct {
	Key   [32]byte
	Value DBBKey
}

// WriteBuffer accumulates writes before distribution
type WriteBuffer struct {
	entries []HashEntry
	size    int64
	maxSize int64
	mu      sync.Mutex
}

// WAL provides write-ahead logging for crash recovery
type WAL struct {
	directory   string
	currentFile *os.File
	sequence    uint64
	mu          sync.Mutex
}

// Statistics tracks performance metrics
type Statistics struct {
	writes      uint64
	reads       uint64
	merges      uint64
	fanOuts     uint64
	bytesWritten uint64
	bytesRead    uint64
}

// NewMultiHashTable creates a new multi-level hash table
func NewMultiHashTable(directory string) (*MultiHashTable, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	mht := &MultiHashTable{
		directory:   directory,
		writeBuffer: NewWriteBuffer(WriteBufferSize),
		levels:      make([]*TreeLevel, 0),
		stats:       &Statistics{},
		shutdownCh:  make(chan struct{}),
	}

	// Initialize WAL
	walDir := filepath.Join(directory, "wal")
	wal, err := NewWAL(walDir)
	if err != nil {
		return nil, err
	}
	mht.wal = wal

	// Initialize first level
	mht.levels = append(mht.levels, NewTreeLevel(0, directory))

	// Recover from WAL if needed
	if err := mht.recover(); err != nil {
		return nil, fmt.Errorf("recovery failed: %v", err)
	}

	// Start background workers
	mht.startBackgroundWorkers()

	return mht, nil
}

// Put adds a key-value pair to the hash table
func (mht *MultiHashTable) Put(key [32]byte, value DBBKey) error {
	// Log to WAL first
	if err := mht.wal.LogPut(key, value); err != nil {
		return err
	}

	// Add to write buffer
	mht.writeBuffer.mu.Lock()
	mht.writeBuffer.entries = append(mht.writeBuffer.entries, HashEntry{key, value})
	mht.writeBuffer.size += 48 // Size of entry

	// Check if buffer needs flushing
	if mht.writeBuffer.size >= mht.writeBuffer.maxSize {
		entries := mht.writeBuffer.entries
		mht.writeBuffer.entries = make([]HashEntry, 0, 1000)
		mht.writeBuffer.size = 0
		mht.writeBuffer.mu.Unlock()

		// Distribute entries asynchronously
		mht.wg.Add(1)
		go mht.distributeEntries(entries)
	} else {
		mht.writeBuffer.mu.Unlock()
	}

	atomic.AddUint64(&mht.stats.writes, 1)
	return nil
}

// Get retrieves a value by key
func (mht *MultiHashTable) Get(key [32]byte) (*DBBKey, error) {
	atomic.AddUint64(&mht.stats.reads, 1)

	// Check write buffer first
	if value := mht.writeBuffer.Get(key); value != nil {
		return value, nil
	}

	// Search through levels
	for _, level := range mht.levels {
		binIndex := mht.getBinIndex(key, level.depth)
		bin := level.bins[binIndex]

		if bin == nil {
			continue
		}

		// Check bin's cache
		bin.mu.RLock()
		if value := bin.cache.Get(key); value != nil {
			bin.mu.RUnlock()
			return value, nil
		}

		// Check bin's file (non-blocking read)
		if bin.file != "" && fileExists(bin.file) {
			value, err := mht.searchBinFile(bin.file, key, bin.version)
			bin.mu.RUnlock()
			if err == nil {
				return value, nil
			}
		} else {
			bin.mu.RUnlock()
		}
	}

	return nil, fmt.Errorf("key not found")
}

// distributeEntries distributes entries from write buffer to bins
func (mht *MultiHashTable) distributeEntries(entries []HashEntry) {
	defer mht.wg.Done()

	// Group entries by first-level bin
	groups := make(map[byte][]HashEntry)
	for _, entry := range entries {
		binIndex := entry.Key[0]
		groups[binIndex] = append(groups[binIndex], entry)
	}

	// Process each group
	var wg sync.WaitGroup
	for binIndex, binEntries := range groups {
		wg.Add(1)
		go func(idx byte, entries []HashEntry) {
			defer wg.Done()
			mht.addToBin(0, idx, entries)
		}(binIndex, binEntries)
	}
	wg.Wait()

	// Mark WAL segment as distributed
	mht.wal.MarkDistributed()
}

// addToBin adds entries to a specific bin
func (mht *MultiHashTable) addToBin(levelDepth int, binIndex byte, entries []HashEntry) {
	if levelDepth >= len(mht.levels) {
		mht.mu.Lock()
		if levelDepth >= len(mht.levels) {
			mht.levels = append(mht.levels, NewTreeLevel(levelDepth, mht.directory))
		}
		mht.mu.Unlock()
	}

	bin := mht.levels[levelDepth].bins[binIndex]

	bin.mu.Lock()
	defer bin.mu.Unlock()

	// Add entries to cache
	bin.cache.entries = append(bin.cache.entries, entries...)
	bin.cache.size += int64(len(entries) * 48)
	bin.cache.sorted = false

	// Check if cache needs merging
	if bin.cache.size >= bin.cache.maxSize {
		mht.mergeBin(bin)
	}
}

// mergeBin merges a bin's cache with its file (copy-on-write)
func (mht *MultiHashTable) mergeBin(bin *Bin) error {
	atomic.AddUint64(&mht.stats.merges, 1)

	// Sort cache entries
	bin.cache.Sort()

	// Create temp file for merge
	tempFile := fmt.Sprintf("%s.tmp.%d", bin.file, time.Now().UnixNano())

	// If bin file exists, merge with it
	if fileExists(bin.file) {
		// Read existing entries
		existing, err := mht.readBinFile(bin.file)
		if err != nil {
			return err
		}

		// Merge cache with existing (both sorted)
		merged := mergeSortedEntries(bin.cache.entries, existing)

		// Check if we need to fan out
		if int64(len(merged)*48) > MaxBinFileSize {
			atomic.AddUint64(&mht.stats.fanOuts, 1)
			return mht.fanOutBin(bin, merged)
		}

		// Write merged data to temp file
		if err := mht.writeBinFile(tempFile, merged); err != nil {
			return err
		}
	} else {
		// No existing file, just write cache
		if err := mht.writeBinFile(tempFile, bin.cache.entries); err != nil {
			return err
		}
	}

	// Atomic rename (makes new version visible)
	oldVersion := bin.version
	bin.version++
	if err := os.Rename(tempFile, bin.file); err != nil {
		return err
	}

	// Clear cache
	bin.cache.Clear()

	// Clean up old version after grace period
	mht.wg.Add(1)
	go mht.cleanupOldVersion(bin.file, oldVersion)

	return nil
}

// fanOutBin distributes a bin's entries to next level
func (mht *MultiHashTable) fanOutBin(bin *Bin, entries []HashEntry) error {
	nextLevel := bin.level + 1

	// Ensure next level exists
	if nextLevel >= len(mht.levels) {
		mht.mu.Lock()
		if nextLevel >= len(mht.levels) {
			mht.levels = append(mht.levels, NewTreeLevel(nextLevel, mht.directory))
		}
		mht.mu.Unlock()
	}

	// Group entries by next-level bin
	groups := make(map[byte][]HashEntry)
	for _, entry := range entries {
		nextBinIndex := entry.Key[nextLevel]
		groups[nextBinIndex] = append(groups[nextBinIndex], entry)
	}

	// Distribute to next level bins
	for binIndex, binEntries := range groups {
		mht.addToBin(nextLevel, binIndex, binEntries)
	}

	// Mark original bin as fanned out
	os.Remove(bin.file)
	bin.file = ""
	bin.size = 0

	return nil
}

// Helper functions

func NewWriteBuffer(maxSize int64) *WriteBuffer {
	return &WriteBuffer{
		entries: make([]HashEntry, 0, 1000),
		maxSize: maxSize,
	}
}

func (wb *WriteBuffer) Get(key [32]byte) *DBBKey {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	for _, entry := range wb.entries {
		if entry.Key == key {
			return &entry.Value
		}
	}
	return nil
}

func NewTreeLevel(depth int, directory string) *TreeLevel {
	level := &TreeLevel{depth: depth}
	for i := 0; i < NumBins; i++ {
		binID := fmt.Sprintf("L%d_%02X", depth, i)
		level.bins[i] = &Bin{
			id:    binID,
			level: depth,
			index: byte(i),
			cache: &BinCache{
				entries: make([]HashEntry, 0),
				maxSize: BinCacheSize,
			},
			file: filepath.Join(directory, binID+".bin"),
		}
	}
	return level
}

func (bc *BinCache) Get(key [32]byte) *DBBKey {
	// Binary search if sorted
	if bc.sorted {
		idx := sort.Search(len(bc.entries), func(i int) bool {
			return bytes.Compare(bc.entries[i].Key[:], key[:]) >= 0
		})
		if idx < len(bc.entries) && bc.entries[idx].Key == key {
			return &bc.entries[idx].Value
		}
	} else {
		// Linear search if not sorted
		for _, entry := range bc.entries {
			if entry.Key == key {
				return &entry.Value
			}
		}
	}
	return nil
}

func (bc *BinCache) Sort() {
	if !bc.sorted {
		sort.Slice(bc.entries, func(i, j int) bool {
			return bytes.Compare(bc.entries[i].Key[:], bc.entries[j].Key[:]) < 0
		})
		bc.sorted = true
	}
}

func (bc *BinCache) Clear() {
	bc.entries = bc.entries[:0]
	bc.size = 0
	bc.sorted = false
}

func (mht *MultiHashTable) getBinIndex(key [32]byte, depth int) byte {
	if depth < len(key) {
		return key[depth]
	}
	return 0
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func mergeSortedEntries(a, b []HashEntry) []HashEntry {
	result := make([]HashEntry, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		cmp := bytes.Compare(a[i].Key[:], b[j].Key[:])
		if cmp < 0 {
			result = append(result, a[i])
			i++
		} else if cmp > 0 {
			result = append(result, b[j])
			j++
		} else {
			// Duplicate key, keep newer (from cache)
			result = append(result, a[i])
			i++
			j++
		}
	}

	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// Binary file operations

func (mht *MultiHashTable) writeBinFile(path string, entries []HashEntry) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	header := make([]byte, 16)
	binary.BigEndian.PutUint64(header[0:8], uint64(len(entries)))
	binary.BigEndian.PutUint64(header[8:16], uint64(time.Now().Unix()))
	if _, err := file.Write(header); err != nil {
		return err
	}

	// Write entries
	for _, entry := range entries {
		if _, err := file.Write(entry.Key[:]); err != nil {
			return err
		}
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], entry.Value.Offset)
		binary.BigEndian.PutUint64(data[8:16], entry.Value.Length)
		if _, err := file.Write(data); err != nil {
			return err
		}
	}

	return file.Sync()
}

func (mht *MultiHashTable) readBinFile(path string) ([]HashEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read header
	header := make([]byte, 16)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, err
	}

	count := binary.BigEndian.Uint64(header[0:8])
	entries := make([]HashEntry, count)

	// Read entries
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(file, entries[i].Key[:]); err != nil {
			return nil, err
		}
		data := make([]byte, 16)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, err
		}
		entries[i].Value.Offset = binary.BigEndian.Uint64(data[0:8])
		entries[i].Value.Length = binary.BigEndian.Uint64(data[8:16])
	}

	return entries, nil
}

func (mht *MultiHashTable) searchBinFile(path string, key [32]byte, version uint64) (*DBBKey, error) {
	// This uses the versioned file to avoid blocking during merges
	entries, err := mht.readBinFile(path)
	if err != nil {
		return nil, err
	}

	// Binary search
	idx := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].Key[:], key[:]) >= 0
	})

	if idx < len(entries) && entries[idx].Key == key {
		return &entries[idx].Value, nil
	}

	return nil, fmt.Errorf("not found")
}

// Background workers

func (mht *MultiHashTable) startBackgroundWorkers() {
	// Periodic flush of write buffer
	mht.wg.Add(1)
	go func() {
		defer mht.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				mht.flushWriteBuffer()
			case <-mht.shutdownCh:
				return
			}
		}
	}()
}

func (mht *MultiHashTable) flushWriteBuffer() {
	mht.writeBuffer.mu.Lock()
	if len(mht.writeBuffer.entries) == 0 {
		mht.writeBuffer.mu.Unlock()
		return
	}

	entries := mht.writeBuffer.entries
	mht.writeBuffer.entries = make([]HashEntry, 0, 1000)
	mht.writeBuffer.size = 0
	mht.writeBuffer.mu.Unlock()

	mht.wg.Add(1)
	go mht.distributeEntries(entries)
}

func (mht *MultiHashTable) cleanupOldVersion(file string, oldVersion uint64) {
	defer mht.wg.Done()

	// Wait for grace period to allow ongoing reads to complete
	time.Sleep(5 * time.Second)

	// Old version files would be cleaned up here
	// In production, we'd track readers and clean up when safe
}

// Shutdown gracefully shuts down the hash table
func (mht *MultiHashTable) Shutdown() error {
	close(mht.shutdownCh)

	// Flush any remaining entries
	mht.flushWriteBuffer()

	// Wait for all background operations
	mht.wg.Wait()

	// Close WAL
	return mht.wal.Close()
}

// recover is implemented in multi_hash_table_wal.go