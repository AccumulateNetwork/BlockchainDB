package history

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AccumulateNetwork/BlockchainDB/database/utils"
)

// HybridLeafNode represents a single bin/leaf in the history file
type HybridLeafNode struct {
	// File handling
	file     *os.File
	filePath string

	// Sorted section (on disk)
	sortedSize   int64  // Size of sorted section in file
	sortedCount  int32  // Number of sorted entries
	isSorted     bool   // Is the sorted section valid?

	// Unsorted section (in memory + on disk)
	unsortedBuffer []byte          // In-memory buffer for recent writes
	unsortedCount  int32          // Number of unsorted entries
	unsortedOffset int64          // Where unsorted section starts in file

	// Memory index for fast lookups
	memIndex map[[32]byte]int // key -> position in unsortedBuffer

	// Statistics
	reads  int64
	writes int64
	sorts  int64

	mu sync.RWMutex
}

// HistoryFileHybrid implements the hybrid sorted/unsorted approach
type HistoryFileHybrid struct {
	Directory string
	NumBins   int

	// Leaf nodes (one per bin)
	leaves []*HybridLeafNode

	// Configuration
	maxUnsortedEntries int // Max entries before triggering background sort
	sortBatchSize      int // Number of bins to sort per background cycle

	// Background sorting
	sortQueue    chan int       // Queue of bins needing sorting
	stopSignal   chan struct{} // Signal to stop background worker
	sortWg       sync.WaitGroup

	// Global statistics
	totalReads  atomic.Int64
	totalWrites atomic.Int64
	totalSorts  atomic.Int64
}

// NewHistoryFileHybrid creates a new hybrid history file
func NewHistoryFileHybrid(numBins int, directory string) (*HistoryFileHybrid, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	hf := &HistoryFileHybrid{
		Directory:          directory,
		NumBins:           numBins,
		leaves:            make([]*HybridLeafNode, numBins),
		maxUnsortedEntries: 1000,  // Trigger sort after 1000 unsorted entries
		sortBatchSize:     10,      // Sort 10 bins at a time
		sortQueue:         make(chan int, numBins),
		stopSignal:        make(chan struct{}),
	}

	// Initialize leaf nodes
	for i := 0; i < numBins; i++ {
		leaf, err := hf.createLeafNode(i)
		if err != nil {
			hf.Close()
			return nil, err
		}
		hf.leaves[i] = leaf
	}

	// Start background sorting worker
	hf.sortWg.Add(1)
	go hf.backgroundSorter()

	return hf, nil
}

// createLeafNode creates and initializes a leaf node
func (hf *HistoryFileHybrid) createLeafNode(binIndex int) (*HybridLeafNode, error) {
	filename := filepath.Join(hf.Directory, fmt.Sprintf("bin_%06d.dat", binIndex))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	leaf := &HybridLeafNode{
		file:           file,
		filePath:      filename,
		unsortedBuffer: make([]byte, 0, hf.maxUnsortedEntries*utils.DBKeyFullSize),
		memIndex:      make(map[[32]byte]int),
	}

	// Check if file has existing sorted data
	if stat.Size() > 0 {
		// Read header to determine sorted/unsorted sections
		var header [16]byte
		file.ReadAt(header[:], 0)

		leaf.sortedCount = int32(binary.BigEndian.Uint32(header[0:4]))
		leaf.sortedSize = int64(binary.BigEndian.Uint32(header[4:8]))
		leaf.unsortedCount = int32(binary.BigEndian.Uint32(header[8:12]))
		leaf.unsortedOffset = int64(binary.BigEndian.Uint32(header[12:16]))

		if leaf.sortedCount > 0 {
			leaf.isSorted = true
		}

		// Load unsorted section into memory
		if leaf.unsortedCount > 0 {
			unsortedSize := int64(leaf.unsortedCount) * utils.DBKeyFullSize
			leaf.unsortedBuffer = make([]byte, unsortedSize)
			file.ReadAt(leaf.unsortedBuffer, leaf.unsortedOffset)

			// Rebuild memory index
			for i := int32(0); i < leaf.unsortedCount; i++ {
				offset := i * int32(utils.DBKeyFullSize)
				var key [32]byte
				copy(key[:], leaf.unsortedBuffer[offset:offset+32])
				leaf.memIndex[key] = int(offset)
			}
		}
	}

	return leaf, nil
}

// Index returns the bin index for a key
func (hf *HistoryFileHybrid) Index(key [32]byte) int {
	index := binary.BigEndian.Uint32(key[:4])
	return int(index % uint32(hf.NumBins))
}

// AddKeys adds keys to the history file
func (hf *HistoryFileHybrid) AddKeys(keyList []byte) error {
	if len(keyList)%utils.DBKeyFullSize != 0 {
		return fmt.Errorf("keyList length must be multiple of %d", utils.DBKeyFullSize)
	}

	// Group keys by bin
	binKeys := make(map[int][]byte)
	for i := 0; i < len(keyList); i += utils.DBKeyFullSize {
		key := [32]byte{}
		copy(key[:], keyList[i:i+32])
		binIndex := hf.Index(key)

		if binKeys[binIndex] == nil {
			binKeys[binIndex] = make([]byte, 0, utils.DBKeyFullSize)
		}
		binKeys[binIndex] = append(binKeys[binIndex], keyList[i:i+utils.DBKeyFullSize]...)
	}

	// Add to each bin
	for binIndex, data := range binKeys {
		leaf := hf.leaves[binIndex]
		if err := leaf.addKeys(data); err != nil {
			return err
		}

		// Check if this leaf needs sorting
		if int(leaf.unsortedCount) >= hf.maxUnsortedEntries {
			select {
			case hf.sortQueue <- binIndex:
				// Queued for sorting
			default:
				// Queue full, will be sorted later
			}
		}
	}

	hf.totalWrites.Add(1)
	return nil
}

// addKeys adds keys to a leaf node (keeps them unsorted in memory)
func (leaf *HybridLeafNode) addKeys(keyList []byte) error {
	leaf.mu.Lock()
	defer leaf.mu.Unlock()

	for i := 0; i < len(keyList); i += utils.DBKeyFullSize {
		// Add to memory buffer
		offset := len(leaf.unsortedBuffer)
		leaf.unsortedBuffer = append(leaf.unsortedBuffer, keyList[i:i+utils.DBKeyFullSize]...)

		// Update memory index
		var key [32]byte
		copy(key[:], keyList[i:i+32])
		leaf.memIndex[key] = offset

		leaf.unsortedCount++
		leaf.writes++
	}

	return nil
}

// Get retrieves a key from the history file
func (hf *HistoryFileHybrid) Get(key [32]byte) (*utils.DBBKey, error) {
	binIndex := hf.Index(key)
	leaf := hf.leaves[binIndex]

	hf.totalReads.Add(1)
	return leaf.get(key)
}

// get retrieves a key from a leaf node
func (leaf *HybridLeafNode) get(key [32]byte) (*utils.DBBKey, error) {
	leaf.mu.RLock()
	defer leaf.mu.RUnlock()

	leaf.reads++

	// 1. Check unsorted memory buffer first (O(1) with index)
	if offset, ok := leaf.memIndex[key]; ok {
		dbKey := new(utils.DBBKey)
		dbKey.Unmarshal(leaf.unsortedBuffer[offset+32 : offset+utils.DBKeyFullSize])
		return dbKey, nil
	}

	// 2. Binary search sorted section if it exists
	if leaf.isSorted && leaf.sortedCount > 0 {
		return leaf.binarySearchSorted(key)
	}

	return nil, fmt.Errorf("key not found")
}

// binarySearchSorted performs binary search on the sorted section
func (leaf *HybridLeafNode) binarySearchSorted(key [32]byte) (*utils.DBBKey, error) {
	numEntries := leaf.sortedCount
	left := int32(0)
	right := numEntries - 1

	var entry [utils.DBKeyFullSize]byte
	headerSize := int64(16) // Size of header with counts/offsets

	for left <= right {
		mid := (left + right) / 2
		offset := headerSize + int64(mid)*utils.DBKeyFullSize

		_, err := leaf.file.ReadAt(entry[:], offset)
		if err != nil {
			return nil, err
		}

		cmp := bytes.Compare(entry[:32], key[:])
		if cmp == 0 {
			dbKey := new(utils.DBBKey)
			dbKey.Unmarshal(entry[32:])
			return dbKey, nil
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil, fmt.Errorf("key not found in sorted section")
}

// backgroundSorter continuously sorts leaves that need sorting
func (hf *HistoryFileHybrid) backgroundSorter() {
	defer hf.sortWg.Done()

	ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
	defer ticker.Stop()

	binsToSort := make([]int, 0, hf.sortBatchSize)

	for {
		select {
		case <-hf.stopSignal:
			return

		case binIndex := <-hf.sortQueue:
			// Collect bins to sort in batch
			binsToSort = append(binsToSort, binIndex)

			// Sort when we have a batch or queue is empty
			if len(binsToSort) >= hf.sortBatchSize {
				hf.sortBatch(binsToSort)
				binsToSort = binsToSort[:0]
			}

		case <-ticker.C:
			// Periodic check - sort any pending bins
			if len(binsToSort) > 0 {
				hf.sortBatch(binsToSort)
				binsToSort = binsToSort[:0]
			}

			// Also check for bins that need sorting but weren't queued
			hf.checkAndQueueUnsortedBins()
		}
	}
}

// sortBatch sorts a batch of bins
func (hf *HistoryFileHybrid) sortBatch(binIndices []int) {
	for _, binIndex := range binIndices {
		leaf := hf.leaves[binIndex]
		if err := leaf.sortAndFlush(); err != nil {
			fmt.Printf("Error sorting bin %d: %v\n", binIndex, err)
		} else {
			hf.totalSorts.Add(1)
		}
	}
}

// sortAndFlush sorts unsorted entries and merges with sorted section
func (leaf *HybridLeafNode) sortAndFlush() error {
	leaf.mu.Lock()
	defer leaf.mu.Unlock()

	if leaf.unsortedCount == 0 {
		return nil // Nothing to sort
	}

	// Read existing sorted section if it exists
	var allEntries []byte
	headerSize := int64(16)

	if leaf.isSorted && leaf.sortedCount > 0 {
		sortedSize := int64(leaf.sortedCount) * utils.DBKeyFullSize
		sortedData := make([]byte, sortedSize)
		_, err := leaf.file.ReadAt(sortedData, headerSize)
		if err != nil {
			return err
		}
		allEntries = sortedData
	}

	// Append unsorted entries
	allEntries = append(allEntries, leaf.unsortedBuffer...)

	// Sort all entries
	sortKeyBufferHybrid(allEntries)

	// Write header
	var header [16]byte
	totalCount := leaf.sortedCount + leaf.unsortedCount
	binary.BigEndian.PutUint32(header[0:4], uint32(totalCount))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(allEntries)))
	binary.BigEndian.PutUint32(header[8:12], 0)  // No unsorted entries after sort
	binary.BigEndian.PutUint32(header[12:16], 0) // No unsorted offset

	// Write everything back
	leaf.file.WriteAt(header[:], 0)
	leaf.file.WriteAt(allEntries, headerSize)
	leaf.file.Sync()

	// Update leaf state
	leaf.sortedCount = totalCount
	leaf.sortedSize = int64(len(allEntries))
	leaf.isSorted = true
	leaf.unsortedCount = 0
	leaf.unsortedBuffer = leaf.unsortedBuffer[:0]
	leaf.memIndex = make(map[[32]byte]int)
	leaf.sorts++

	return nil
}

// checkAndQueueUnsortedBins checks all bins and queues those needing sorting
func (hf *HistoryFileHybrid) checkAndQueueUnsortedBins() {
	for i, leaf := range hf.leaves {
		leaf.mu.RLock()
		needsSort := int(leaf.unsortedCount) >= hf.maxUnsortedEntries/2
		leaf.mu.RUnlock()

		if needsSort {
			select {
			case hf.sortQueue <- i:
			default:
			}
		}
	}
}

// FlushAll forces sorting of all unsorted data
func (hf *HistoryFileHybrid) FlushAll() error {
	fmt.Println("Flushing all unsorted entries...")
	startTime := time.Now()

	for i, leaf := range hf.leaves {
		if leaf.unsortedCount > 0 {
			if err := leaf.sortAndFlush(); err != nil {
				return fmt.Errorf("failed to flush bin %d: %w", i, err)
			}
		}
	}

	fmt.Printf("Flush complete in %.2fs\n", time.Since(startTime).Seconds())
	return nil
}

// Close closes the history file and stops background workers
func (hf *HistoryFileHybrid) Close() error {
	// Stop background sorter
	close(hf.stopSignal)
	hf.sortWg.Wait()

	// Flush all remaining unsorted data
	hf.FlushAll()

	// Close all files
	for _, leaf := range hf.leaves {
		if leaf != nil && leaf.file != nil {
			leaf.file.Close()
		}
	}

	return nil
}

// Stats returns statistics about the history file
func (hf *HistoryFileHybrid) Stats() string {
	totalSorted := int64(0)
	totalUnsorted := int64(0)
	maxUnsorted := int32(0)

	for _, leaf := range hf.leaves {
		leaf.mu.RLock()
		totalSorted += int64(leaf.sortedCount)
		totalUnsorted += int64(leaf.unsortedCount)
		if leaf.unsortedCount > maxUnsorted {
			maxUnsorted = leaf.unsortedCount
		}
		leaf.mu.RUnlock()
	}

	return fmt.Sprintf(
		"Bins: %d, Keys: %d (sorted: %d, unsorted: %d), Max unsorted/bin: %d, Reads: %d, Writes: %d, Sorts: %d",
		hf.NumBins, totalSorted+totalUnsorted, totalSorted, totalUnsorted, maxUnsorted,
		hf.totalReads.Load(), hf.totalWrites.Load(), hf.totalSorts.Load())
}

// sortKeyBufferHybrid sorts entries in the buffer
func sortKeyBufferHybrid(buffer []byte) {
	numEntries := len(buffer) / utils.DBKeyFullSize
	if numEntries <= 1 {
		return
	}

	// QuickSort for efficiency
	quickSortHybrid(buffer, 0, numEntries-1)
}

func quickSortHybrid(buffer []byte, low, high int) {
	if low < high {
		pi := partitionHybrid(buffer, low, high)
		quickSortHybrid(buffer, low, pi-1)
		quickSortHybrid(buffer, pi+1, high)
	}
}

func partitionHybrid(buffer []byte, low, high int) int {
	pivotOffset := high * utils.DBKeyFullSize
	pivot := buffer[pivotOffset : pivotOffset+32]

	i := low - 1
	for j := low; j < high; j++ {
		jOffset := j * utils.DBKeyFullSize
		if bytes.Compare(buffer[jOffset:jOffset+32], pivot) < 0 {
			i++
			swapEntriesHybrid(buffer, i, j)
		}
	}
	swapEntriesHybrid(buffer, i+1, high)
	return i + 1
}

func swapEntriesHybrid(buffer []byte, i, j int) {
	if i == j {
		return
	}
	iOffset := i * utils.DBKeyFullSize
	jOffset := j * utils.DBKeyFullSize
	var temp [utils.DBKeyFullSize]byte
	copy(temp[:], buffer[iOffset:iOffset+utils.DBKeyFullSize])
	copy(buffer[iOffset:], buffer[jOffset:jOffset+utils.DBKeyFullSize])
	copy(buffer[jOffset:], temp[:])
}