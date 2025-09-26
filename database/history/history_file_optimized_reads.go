package history

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/AccumulateNetwork/BlockchainDB/database/utils"
)

// HistoryFileOptimized uses append-only writes with optimized reads
type HistoryFileOptimized struct {
	Directory string
	NumBins   int

	// File handles
	binFiles []*os.File
	binSizes []int64

	// Optimization state
	binSorted []bool // Is this bin sorted?

	// Memory cache for recent writes (before sorting)
	recentWrites      map[[32]byte]uint64 // key -> file offset
	recentWritesLimit int

	// Statistics
	totalWrites   int64
	totalReads    int64
	linearScans   int64
	binarySearches int64

	mu sync.RWMutex
}

// NewHistoryFileOptimized creates an optimized history file
func NewHistoryFileOptimized(numBins int, directory string) (*HistoryFileOptimized, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	hf := &HistoryFileOptimized{
		Directory:         directory,
		NumBins:          numBins,
		binFiles:         make([]*os.File, numBins),
		binSizes:         make([]int64, numBins),
		binSorted:        make([]bool, numBins),
		recentWrites:     make(map[[32]byte]uint64),
		recentWritesLimit: 10000, // Keep last 10K keys in memory
	}

	// Open bin files
	for i := 0; i < numBins; i++ {
		filename := filepath.Join(directory, fmt.Sprintf("bin_%06d.dat", i))
		// Note: We don't use O_APPEND because we need WriteAt for sorting
		// We'll seek to end manually when appending
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			hf.Close()
			return nil, err
		}

		stat, err := file.Stat()
		if err != nil {
			hf.Close()
			return nil, err
		}

		hf.binFiles[i] = file
		hf.binSizes[i] = stat.Size()

		// Check if bin is already sorted (from previous run)
		if stat.Size() > 0 {
			hf.binSorted[i] = hf.checkIfSorted(i)
		}
	}

	return hf, nil
}

// Index returns the bin index for a key
func (hf *HistoryFileOptimized) Index(key [32]byte) int {
	index := binary.BigEndian.Uint32(key[:4])
	return int(index % uint32(hf.NumBins))
}

// AddKeys adds keys using append-only writes
func (hf *HistoryFileOptimized) AddKeys(keyList []byte) error {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	// When we add new keys, bins become unsorted
	binsTouched := make(map[int]bool)

	// Process and append keys
	for i := 0; i < len(keyList); i += utils.DBKeyFullSize {
		key := [32]byte{}
		copy(key[:], keyList[i:i+32])
		binIndex := hf.Index(key)

		// Mark bin as unsorted
		if hf.binSorted[binIndex] {
			hf.binSorted[binIndex] = false
			binsTouched[binIndex] = true
		}

		// Record file offset for recent keys
		offset := hf.binSizes[binIndex]
		hf.recentWrites[key] = uint64(offset)

		// Write to bin file (append at end)
		n, err := hf.binFiles[binIndex].WriteAt(keyList[i:i+utils.DBKeyFullSize], offset)
		if err != nil {
			return err
		}
		hf.binSizes[binIndex] += int64(n)
	}

	// Trim memory cache if too large
	if len(hf.recentWrites) > hf.recentWritesLimit {
		// Simple strategy: clear cache (could be smarter with LRU)
		hf.recentWrites = make(map[[32]byte]uint64)
	}

	hf.totalWrites++
	return nil
}

// Get retrieves a key using the most efficient method available
func (hf *HistoryFileOptimized) Get(key [32]byte) (*utils.DBBKey, error) {
	binIndex := hf.Index(key)

	hf.mu.RLock()
	defer hf.mu.RUnlock()

	hf.totalReads++

	// 1. Check memory cache first (O(1))
	if offset, ok := hf.recentWrites[key]; ok {
		return hf.readAtOffset(binIndex, offset)
	}

	// 2. Use binary search if bin is sorted (O(log n))
	if hf.binSorted[binIndex] {
		hf.binarySearches++
		return hf.binarySearchBin(binIndex, key)
	}

	// 3. Fall back to linear scan (O(n))
	hf.linearScans++
	return hf.linearScanBin(binIndex, key)
}

// OptimizeForReads sorts all bins for efficient reading
func (hf *HistoryFileOptimized) OptimizeForReads() error {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	fmt.Println("Optimizing bins for fast reads...")
	startTime := time.Now()

	for i, file := range hf.binFiles {
		if hf.binSorted[i] {
			continue // Already sorted
		}

		size := hf.binSizes[i]
		if size == 0 {
			hf.binSorted[i] = true
			continue
		}

		// Read entire bin
		buffer := make([]byte, size)
		_, err := file.ReadAt(buffer, 0)
		if err != nil {
			return err
		}

		// Sort the buffer
		sortKeyBufferOptimized(buffer)

		// Write back sorted
		_, err = file.WriteAt(buffer, 0)
		if err != nil {
			return err
		}

		file.Sync()
		hf.binSorted[i] = true
	}

	// Clear memory cache since everything is now sorted
	hf.recentWrites = make(map[[32]byte]uint64)

	fmt.Printf("Optimization complete in %.2fs\n", time.Since(startTime).Seconds())
	return nil
}

// binarySearchBin performs binary search on a sorted bin
func (hf *HistoryFileOptimized) binarySearchBin(binIndex int, key [32]byte) (*utils.DBBKey, error) {
	file := hf.binFiles[binIndex]
	size := hf.binSizes[binIndex]

	if size == 0 {
		return nil, fmt.Errorf("key not found")
	}

	numEntries := size / utils.DBKeyFullSize
	left := int64(0)
	right := numEntries - 1

	var entry [utils.DBKeyFullSize]byte

	for left <= right {
		mid := (left + right) / 2
		offset := mid * utils.DBKeyFullSize

		_, err := file.ReadAt(entry[:], offset)
		if err != nil {
			return nil, err
		}

		cmp := bytes.Compare(entry[:32], key[:])
		if cmp == 0 {
			// Found it!
			dbKey := new(utils.DBBKey)
			dbKey.Unmarshal(entry[32:])
			return dbKey, nil
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil, fmt.Errorf("key not found")
}

// linearScanBin performs linear search on an unsorted bin
func (hf *HistoryFileOptimized) linearScanBin(binIndex int, key [32]byte) (*utils.DBBKey, error) {
	file := hf.binFiles[binIndex]
	size := hf.binSizes[binIndex]

	if size == 0 {
		return nil, fmt.Errorf("key not found")
	}

	// Read entire bin (could optimize with chunked reading)
	buffer := make([]byte, size)
	_, err := file.ReadAt(buffer, 0)
	if err != nil {
		return nil, err
	}

	// Linear search
	for i := 0; i < len(buffer); i += utils.DBKeyFullSize {
		if bytes.Equal(buffer[i:i+32], key[:]) {
			dbKey := new(utils.DBBKey)
			dbKey.Unmarshal(buffer[i+32 : i+utils.DBKeyFullSize])
			return dbKey, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// readAtOffset reads a key at a known offset
func (hf *HistoryFileOptimized) readAtOffset(binIndex int, offset uint64) (*utils.DBBKey, error) {
	var entry [utils.DBKeyFullSize]byte
	_, err := hf.binFiles[binIndex].ReadAt(entry[:], int64(offset))
	if err != nil {
		return nil, err
	}

	dbKey := new(utils.DBBKey)
	dbKey.Unmarshal(entry[32:])
	return dbKey, nil
}

// checkIfSorted checks if a bin file is sorted
func (hf *HistoryFileOptimized) checkIfSorted(binIndex int) bool {
	size := hf.binSizes[binIndex]
	if size <= utils.DBKeyFullSize {
		return true // Single entry or empty is sorted
	}

	file := hf.binFiles[binIndex]
	var prev, curr [32]byte

	// Check first two entries
	file.ReadAt(prev[:], 0)
	file.ReadAt(curr[:], utils.DBKeyFullSize)

	if bytes.Compare(prev[:], curr[:]) > 0 {
		return false
	}

	// Sample check: read every 100th entry
	for offset := int64(utils.DBKeyFullSize * 100); offset < size; offset += utils.DBKeyFullSize * 100 {
		copy(prev[:], curr[:])
		file.ReadAt(curr[:], offset)
		if bytes.Compare(prev[:], curr[:]) > 0 {
			return false
		}
	}

	return true
}

// Close closes all files
func (hf *HistoryFileOptimized) Close() error {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	for i, file := range hf.binFiles {
		if file != nil {
			file.Close()
			hf.binFiles[i] = nil
		}
	}
	return nil
}

// Stats returns performance statistics
func (hf *HistoryFileOptimized) Stats() string {
	hf.mu.RLock()
	defer hf.mu.RUnlock()

	sortedBins := 0
	totalSize := int64(0)

	for i, sorted := range hf.binSorted {
		if sorted {
			sortedBins++
		}
		totalSize += hf.binSizes[i]
	}

	readEfficiency := float64(0)
	if hf.totalReads > 0 {
		readEfficiency = float64(hf.binarySearches) / float64(hf.totalReads) * 100
	}

	return fmt.Sprintf(
		"Bins: %d (%d sorted), Size: %d MB, Writes: %d, Reads: %d (%.1f%% binary search, %d linear scans)",
		hf.NumBins, sortedBins, totalSize/(1024*1024), hf.totalWrites, hf.totalReads,
		readEfficiency, hf.linearScans)
}

// sortKeyBufferOptimized sorts entries in place
func sortKeyBufferOptimized(buffer []byte) {
	numEntries := len(buffer) / utils.DBKeyFullSize
	if numEntries <= 1 {
		return
	}

	// In-place quicksort for better memory efficiency
	quickSortKeys(buffer, 0, numEntries-1)
}

// quickSortKeys performs in-place quicksort on key entries
func quickSortKeys(buffer []byte, low, high int) {
	if low < high {
		pi := partition(buffer, low, high)
		quickSortKeys(buffer, low, pi-1)
		quickSortKeys(buffer, pi+1, high)
	}
}

func partition(buffer []byte, low, high int) int {
	// Use last element as pivot
	pivotOffset := high * utils.DBKeyFullSize
	pivot := buffer[pivotOffset : pivotOffset+32]

	i := low - 1

	for j := low; j < high; j++ {
		jOffset := j * utils.DBKeyFullSize
		if bytes.Compare(buffer[jOffset:jOffset+32], pivot) < 0 {
			i++
			// Swap entries i and j
			swapEntries(buffer, i, j)
		}
	}

	// Swap pivot to correct position
	swapEntries(buffer, i+1, high)
	return i + 1
}

func swapEntries(buffer []byte, i, j int) {
	if i == j {
		return
	}

	iOffset := i * utils.DBKeyFullSize
	jOffset := j * utils.DBKeyFullSize

	// Swap the entries
	var temp [utils.DBKeyFullSize]byte
	copy(temp[:], buffer[iOffset:iOffset+utils.DBKeyFullSize])
	copy(buffer[iOffset:], buffer[jOffset:jOffset+utils.DBKeyFullSize])
	copy(buffer[jOffset:], temp[:])
}