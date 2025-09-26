package history

import (
	"github.com/AccumulateNetwork/BlockchainDB/database/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	KeySetSize     = 16
	HeaderLocation = 1024 // Space for header at start of file
)

var HistoryFilename = "history_000000.dat" // Use var instead of const to avoid conflict

// HistoryFile implements a hybrid sorted/unsorted storage approach for optimal performance
// - Writes are O(1) append operations to memory
// - Reads check memory first (O(1)), then sorted disk (O(log n))
// - Background task automatically sorts data for optimal read performance
type HistoryFile struct {
	// File and directory info
	Directory string
	Filename  string
	File      *os.File

	// Configuration (from original)
	OffsetCnt   int32  // Number of bins/indexes
	HeaderSize  uint64 // Size of header in bytes

	// Bin management - now using hybrid leaf nodes
	KeySets      []*KeySet      // For compatibility with original interface
	KeySetOffset []*KeySet      // For compatibility
	leaves       []*HybridLeaf  // New: Hybrid leaf nodes for each bin

	// Caching (from original)
	keySetCache   map[int][]byte
	maxCacheSize  int
	cacheAccesses int64
	cacheMisses   int64

	// Hybrid configuration
	maxUnsortedEntries int  // Max entries before triggering background sort
	sortBatchSize      int  // Number of bins to sort per background cycle

	// Background sorting
	sortQueue    chan int       // Queue of bins needing sorting
	stopSignal   chan struct{}  // Signal to stop background worker
	sortWg       sync.WaitGroup

	// Global statistics
	totalReads  atomic.Int64
	totalWrites atomic.Int64
	totalSorts  atomic.Int64

	// Mutex for thread safety
	mu sync.RWMutex
}

// HybridLeaf represents a single bin with hybrid sorted/unsorted storage
type HybridLeaf struct {
	binIndex int
	file     *os.File  // Reference to main file

	// Sorted section (on disk)
	sortedOffset int64  // Where sorted data starts in file
	sortedSize   int64  // Size of sorted section
	sortedCount  int32  // Number of sorted entries

	// Unsorted section (in memory + will be persisted)
	unsortedBuffer []byte           // In-memory buffer for recent writes
	unsortedCount  int32           // Number of unsorted entries

	// Memory index for fast lookups
	memIndex map[[32]byte]*utils.DBBKey  // key -> utils.DBBKey for O(1) lookup

	// Statistics
	reads  int64
	writes int64
	sorts  int64

	mu sync.RWMutex
}

// KeySet maintains compatibility with original interface
type KeySet struct {
	Start       uint64
	End         uint64
	Index       uint64
	OffsetIndex uint64
}

func (keySet KeySet) Bytes() []byte {
	b := make([]byte, 0, KeySetSize)
	b = binary.BigEndian.AppendUint64(b, keySet.Start)
	b = binary.BigEndian.AppendUint64(b, keySet.End)
	return b
}

func (keySet *KeySet) Unmarshal(b []byte) {
	keySet.Start = binary.BigEndian.Uint64(b)
	keySet.End = binary.BigEndian.Uint64(b[8:])
}

// NewHistoryFile creates a new history file with hybrid storage
func NewHistoryFile(OffsetCnt uint64, Directory string) (historyFile *HistoryFile, err error) {
	if OffsetCnt < 0 || OffsetCnt > 102400 {
		return nil, fmt.Errorf("index must be less than or equal to 10240, received %d", OffsetCnt)
	}

	hf := new(HistoryFile)
	hf.Directory = Directory
	os.Mkdir(Directory, os.ModePerm)

	hf.Filename = filepath.Join(Directory, HistoryFilename)
	if hf.File, err = os.Create(hf.Filename); err != nil {
		return nil, err
	}

	// Initialize configuration
	hf.OffsetCnt = int32(OffsetCnt)
	hf.HeaderSize = 4 + KeySetSize*OffsetCnt
	hf.KeySets = make([]*KeySet, OffsetCnt)
	hf.KeySetOffset = make([]*KeySet, OffsetCnt)
	hf.keySetCache = make(map[int][]byte)
	hf.maxCacheSize = 100

	// Hybrid configuration
	hf.maxUnsortedEntries = 1000  // Trigger sort after 1000 unsorted entries
	hf.sortBatchSize = 10         // Sort 10 bins at a time
	hf.sortQueue = make(chan int, OffsetCnt)
	hf.stopSignal = make(chan struct{})

	// Initialize leaf nodes
	hf.leaves = make([]*HybridLeaf, OffsetCnt)
	for i := uint64(0); i < OffsetCnt; i++ {
		// Initialize KeySet for compatibility
		keySet := new(KeySet)
		keySet.Start = HeaderLocation + i*KeySetSize
		keySet.End = keySet.Start
		keySet.Index = i
		keySet.OffsetIndex = i
		hf.KeySets[i] = keySet
		hf.KeySetOffset[i] = keySet

		// Create hybrid leaf
		leaf := &HybridLeaf{
			binIndex:       int(i),
			file:          hf.File,
			sortedOffset:  int64(HeaderLocation + OffsetCnt*KeySetSize + i*1024*1024), // Reserve 1MB per bin initially
			unsortedBuffer: make([]byte, 0, hf.maxUnsortedEntries*utils.DBKeyFullSize),
			memIndex:      make(map[[32]byte]*utils.DBBKey),
		}
		hf.leaves[i] = leaf
	}

	// Start background sorting worker
	hf.sortWg.Add(1)
	go hf.backgroundSorter()

	// Write initial header
	if _, err = hf.File.WriteAt(hf.Marshal(), 0); err != nil {
		return nil, err
	}

	return hf, nil
}

// LoadHistoryFile loads an existing history file
func LoadHistoryFile(Directory string) (historyFile *HistoryFile, err error) {
	hf := new(HistoryFile)
	hf.Directory = Directory

	hf.Filename = filepath.Join(Directory, HistoryFilename)
	if hf.File, err = os.OpenFile(hf.Filename, os.O_RDWR, 0644); err != nil {
		return nil, err
	}

	// Read header
	header := make([]byte, 4)
	if _, err = hf.File.ReadAt(header, 0); err != nil {
		return nil, err
	}

	offsetCnt := binary.BigEndian.Uint32(header)
	hf.OffsetCnt = int32(offsetCnt)
	hf.HeaderSize = 4 + KeySetSize*uint64(offsetCnt)
	hf.KeySets = make([]*KeySet, offsetCnt)
	hf.KeySetOffset = make([]*KeySet, offsetCnt)
	hf.keySetCache = make(map[int][]byte)
	hf.maxCacheSize = 100

	// Hybrid configuration
	hf.maxUnsortedEntries = 1000
	hf.sortBatchSize = 10
	hf.sortQueue = make(chan int, offsetCnt)
	hf.stopSignal = make(chan struct{})

	// Load KeySets and create leaf nodes
	hf.leaves = make([]*HybridLeaf, offsetCnt)
	for i := uint32(0); i < offsetCnt; i++ {
		keySet := new(KeySet)
		keySetData := make([]byte, KeySetSize)
		if _, err = hf.File.ReadAt(keySetData, int64(4+i*uint32(KeySetSize))); err != nil {
			return nil, err
		}
		keySet.Unmarshal(keySetData)
		keySet.Index = uint64(i)
		keySet.OffsetIndex = uint64(i)
		hf.KeySets[i] = keySet
		hf.KeySetOffset[i] = keySet

		// Create hybrid leaf
		leaf := &HybridLeaf{
			binIndex:       int(i),
			file:          hf.File,
			sortedOffset:  int64(HeaderLocation + offsetCnt*uint32(KeySetSize) + i*1024*1024),
			unsortedBuffer: make([]byte, 0, hf.maxUnsortedEntries*utils.DBKeyFullSize),
			memIndex:      make(map[[32]byte]*utils.DBBKey),
		}

		// Load existing data if any
		if keySet.End > keySet.Start {
			size := keySet.End - keySet.Start
			leaf.sortedSize = int64(size)
			leaf.sortedCount = int32(size / uint64(utils.DBKeyFullSize))
		}

		hf.leaves[i] = leaf
	}

	// Start background sorting worker
	hf.sortWg.Add(1)
	go hf.backgroundSorter()

	return hf, nil
}

// Close closes the history file and stops background workers
func (hf *HistoryFile) Close() error {
	if hf.stopSignal != nil {
		close(hf.stopSignal)
		hf.sortWg.Wait()
	}

	// Flush all remaining unsorted data
	hf.FlushAll()

	// Close file
	if hf.File != nil {
		return hf.File.Close()
	}
	return nil
}

// Index returns the bin index for a key
func (hf *HistoryFile) Index(key [32]byte) int {
	// Use first 4 bytes for indexing (same as original)
	index := binary.BigEndian.Uint32(key[:4])
	return int(index % uint32(hf.OffsetCnt))
}

// AddKeys adds keys using the hybrid approach - O(1) append to memory
func (hf *HistoryFile) AddKeys(keyList []byte) (err error) {
	if len(keyList) == 0 {
		return nil
	}

	if len(keyList)%utils.DBKeyFullSize != 0 {
		return fmt.Errorf("keyList is the wrong length")
	}

	hf.mu.Lock()
	defer hf.mu.Unlock()

	// Group keys by bin and ensure they're sorted within each bin
	currentBin := -1
	binStart := 0

	for i := 0; i < len(keyList); i += utils.DBKeyFullSize {
		binIndex := hf.Index([32]byte(keyList[i:i+32]))

		if currentBin == -1 {
			currentBin = binIndex
		} else if binIndex != currentBin {
			// Process previous bin's keys
			if err := hf.addKeysToLeaf(currentBin, keyList[binStart:i]); err != nil {
				return err
			}

			// Check if bin needs sorting
			if int(hf.leaves[currentBin].unsortedCount) >= hf.maxUnsortedEntries {
				select {
				case hf.sortQueue <- currentBin:
				default:
				}
			}

			currentBin = binIndex
			binStart = i
		} else if binIndex < currentBin {
			return errors.New("keyList is not sorted")
		}
	}

	// Process last bin's keys
	if err := hf.addKeysToLeaf(currentBin, keyList[binStart:]); err != nil {
		return err
	}

	// Check if last bin needs sorting
	if int(hf.leaves[currentBin].unsortedCount) >= hf.maxUnsortedEntries {
		select {
		case hf.sortQueue <- currentBin:
		default:
		}
	}

	hf.totalWrites.Add(1)

	// Update header
	_, err = hf.File.WriteAt(hf.Marshal(), 0)
	return err
}

// addKeysToLeaf adds keys to a specific leaf node (keeps them unsorted in memory)
func (hf *HistoryFile) addKeysToLeaf(binIndex int, keyList []byte) error {
	leaf := hf.leaves[binIndex]
	leaf.mu.Lock()
	defer leaf.mu.Unlock()

	for i := 0; i < len(keyList); i += utils.DBKeyFullSize {
		// Parse the key entry
		var dbKeyFull utils.DBBKeyFull
		copy(dbKeyFull.Key[:], keyList[i:i+32])
		dbKeyFull.DBBKey.Unmarshal(keyList[i+32:i+utils.DBKeyFullSize])

		// Add to memory buffer
		leaf.unsortedBuffer = append(leaf.unsortedBuffer, keyList[i:i+utils.DBKeyFullSize]...)

		// Update memory index for O(1) lookups
		leaf.memIndex[dbKeyFull.Key] = &dbKeyFull.DBBKey
		leaf.unsortedCount++
		leaf.writes++
	}

	// Update KeySet for compatibility
	hf.KeySets[binIndex].End = hf.KeySets[binIndex].Start + uint64(leaf.sortedSize) + uint64(len(leaf.unsortedBuffer))

	return nil
}

// Get retrieves a key using the hybrid approach
func (hf *HistoryFile) Get(key [32]byte) (*utils.DBBKey, error) {
	binIndex := hf.Index(key)
	leaf := hf.leaves[binIndex]

	hf.totalReads.Add(1)

	leaf.mu.RLock()
	defer leaf.mu.RUnlock()

	leaf.reads++

	// 1. Check unsorted memory buffer first (O(1) with index)
	if dbKey, ok := leaf.memIndex[key]; ok {
		return dbKey, nil
	}

	// 2. Check cache
	hf.cacheAccesses++
	if cachedData, ok := hf.keySetCache[binIndex]; ok {
		// Search in cached data
		for i := 0; i < len(cachedData); i += utils.DBKeyFullSize {
			if bytes.Equal(cachedData[i:i+32], key[:]) {
				dbKey := new(utils.DBBKey)
				dbKey.Unmarshal(cachedData[i+32:i+utils.DBKeyFullSize])
				return dbKey, nil
			}
		}
	}
	hf.cacheMisses++

	// 3. Binary search sorted section if it exists
	if leaf.sortedCount > 0 {
		return hf.binarySearchSorted(leaf, key)
	}

	return nil, fmt.Errorf("key not found")
}

// binarySearchSorted performs binary search on the sorted section
func (hf *HistoryFile) binarySearchSorted(leaf *HybridLeaf, key [32]byte) (*utils.DBBKey, error) {
	numEntries := leaf.sortedCount
	left := int32(0)
	right := numEntries - 1

	var entry [utils.DBKeyFullSize]byte

	for left <= right {
		mid := (left + right) / 2
		offset := leaf.sortedOffset + int64(mid)*utils.DBKeyFullSize

		_, err := hf.File.ReadAt(entry[:], offset)
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
func (hf *HistoryFile) backgroundSorter() {
	defer hf.sortWg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	binsToSort := make([]int, 0, hf.sortBatchSize)

	for {
		select {
		case <-hf.stopSignal:
			return

		case binIndex := <-hf.sortQueue:
			binsToSort = append(binsToSort, binIndex)

			if len(binsToSort) >= hf.sortBatchSize {
				hf.sortBatch(binsToSort)
				binsToSort = binsToSort[:0]
			}

		case <-ticker.C:
			if len(binsToSort) > 0 {
				hf.sortBatch(binsToSort)
				binsToSort = binsToSort[:0]
			}

			// Check for bins that need sorting
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
	}
}

// sortBatch sorts a batch of bins
func (hf *HistoryFile) sortBatch(binIndices []int) {
	for _, binIndex := range binIndices {
		leaf := hf.leaves[binIndex]
		if err := hf.sortAndFlushLeaf(leaf); err != nil {
			fmt.Printf("Error sorting bin %d: %v\n", binIndex, err)
		} else {
			hf.totalSorts.Add(1)
		}
	}
}

// sortAndFlushLeaf sorts unsorted entries and merges with sorted section
func (hf *HistoryFile) sortAndFlushLeaf(leaf *HybridLeaf) error {
	leaf.mu.Lock()
	defer leaf.mu.Unlock()

	if leaf.unsortedCount == 0 {
		return nil
	}

	// Read existing sorted section if it exists
	var allEntries []byte

	if leaf.sortedCount > 0 {
		sortedData := make([]byte, leaf.sortedSize)
		_, err := hf.File.ReadAt(sortedData, leaf.sortedOffset)
		if err != nil {
			return err
		}
		allEntries = sortedData
	}

	// Append unsorted entries
	allEntries = append(allEntries, leaf.unsortedBuffer...)

	// Sort all entries
	hf.sortKeyBuffer(allEntries)

	// Write back to file
	_, err := hf.File.WriteAt(allEntries, leaf.sortedOffset)
	if err != nil {
		return err
	}

	// Update leaf state
	leaf.sortedCount += leaf.unsortedCount
	leaf.sortedSize = int64(len(allEntries))
	leaf.unsortedCount = 0
	leaf.unsortedBuffer = leaf.unsortedBuffer[:0]
	leaf.memIndex = make(map[[32]byte]*utils.DBBKey)
	leaf.sorts++

	// Update KeySet
	keySet := hf.KeySets[leaf.binIndex]
	keySet.Start = uint64(leaf.sortedOffset)
	keySet.End = keySet.Start + uint64(leaf.sortedSize)

	// Clear cache for this bin
	delete(hf.keySetCache, leaf.binIndex)

	return nil
}

// SortAllKeySets sorts all bins for optimal read performance
func (hf *HistoryFile) SortAllKeySets() error {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	for i, leaf := range hf.leaves {
		if leaf.unsortedCount > 0 || leaf.sortedCount == 0 {
			// Read all data for this bin
			keySet := hf.KeySets[i]
			if keySet.End <= keySet.Start {
				continue
			}

			size := keySet.End - keySet.Start
			buffer := make([]byte, size)

			// Read sorted portion if exists
			if leaf.sortedCount > 0 {
				_, err := hf.File.ReadAt(buffer[:leaf.sortedSize], leaf.sortedOffset)
				if err != nil {
					return err
				}
			}

			// Add unsorted portion
			if leaf.unsortedCount > 0 {
				copy(buffer[leaf.sortedSize:], leaf.unsortedBuffer)
			}

			// Sort
			hf.sortKeyBuffer(buffer)

			// Write back
			_, err := hf.File.WriteAt(buffer, leaf.sortedOffset)
			if err != nil {
				return err
			}

			// Update state
			leaf.sortedCount = int32(len(buffer) / utils.DBKeyFullSize)
			leaf.sortedSize = int64(len(buffer))
			leaf.unsortedCount = 0
			leaf.unsortedBuffer = leaf.unsortedBuffer[:0]
			leaf.memIndex = make(map[[32]byte]*utils.DBBKey)
		}
	}

	return nil
}

// FlushAll forces sorting of all unsorted data
func (hf *HistoryFile) FlushAll() error {
	for _, leaf := range hf.leaves {
		if leaf.unsortedCount > 0 {
			if err := hf.sortAndFlushLeaf(leaf); err != nil {
				return err
			}
		}
	}
	return nil
}

// sortKeyBuffer sorts entries in the buffer by their 32-byte key
func (hf *HistoryFile) sortKeyBuffer(buffer []byte) {
	numEntries := len(buffer) / utils.DBKeyFullSize
	if numEntries <= 1 {
		return
	}

	// Create a slice of entries for sorting
	type entry struct {
		data [utils.DBKeyFullSize]byte
	}
	entries := make([]entry, numEntries)

	// Copy buffer data into entries
	for i := 0; i < numEntries; i++ {
		copy(entries[i].data[:], buffer[i*utils.DBKeyFullSize:(i+1)*utils.DBKeyFullSize])
	}

	// Sort entries by the first 32 bytes (the key)
	slices.SortFunc(entries, func(a, b entry) int {
		return bytes.Compare(a.data[:32], b.data[:32])
	})

	// Copy sorted entries back to buffer
	for i, e := range entries {
		copy(buffer[i*utils.DBKeyFullSize:], e.data[:])
	}
}

// Marshal serializes the header
func (hf *HistoryFile) Marshal() []byte {
	data := make([]byte, hf.HeaderSize)
	binary.BigEndian.PutUint32(data, uint32(hf.OffsetCnt))

	for i, keySet := range hf.KeySets {
		offset := 4 + i*KeySetSize
		copy(data[offset:], keySet.Bytes())
	}

	return data
}

// UpdateKeySet - compatibility method (now just delegates to AddKeys)
func (hf *HistoryFile) UpdateKeySet(index int, keyList []byte) error {
	// In the hybrid approach, this is handled by addKeysToLeaf
	// which is called from AddKeys
	return nil
}

// FindKey - compatibility method
func (hf *HistoryFile) FindKey(keySet KeySet, key [32]byte) (*utils.DBBKey, error) {
	return hf.Get(key)
}

// Unmarshal deserializes the header (compatibility method)
func (hf *HistoryFile) Unmarshal(data []byte) {
	hf.OffsetCnt = int32(binary.BigEndian.Uint32(data))
	// Initialize the slices with the correct size
	hf.KeySets = make([]*KeySet, hf.OffsetCnt)
	hf.KeySetOffset = make([]*KeySet, hf.OffsetCnt)
	hf.leaves = make([]*HybridLeaf, hf.OffsetCnt)

	data = data[4:]
	for i := uint64(0); i < uint64(hf.OffsetCnt); i++ {
		ks := new(KeySet)
		ks.OffsetIndex = i
		ks.Unmarshal(data)
		hf.KeySets[i] = ks
		hf.KeySetOffset[i] = ks

		// Create hybrid leaf
		leaf := &HybridLeaf{
			binIndex:       int(i),
			file:          hf.File,
			sortedOffset:  int64(HeaderLocation + uint64(hf.OffsetCnt)*KeySetSize + i*1024*1024),
			unsortedBuffer: make([]byte, 0, 1000*utils.DBKeyFullSize),
			memIndex:      make(map[[32]byte]*utils.DBBKey),
		}
		hf.leaves[i] = leaf

		data = data[KeySetSize:] // Advance to next KeySet
	}
	hf.OffsetSort()
}

// OffsetSort sorts the offset array (compatibility method)
func (hf *HistoryFile) OffsetSort() {
	ret := 0
	slices.SortFunc(hf.KeySetOffset, func(a, b *KeySet) int {
		switch {
		case a.End < b.End:
			ret = -1
		case a.End > b.End:
			ret = 1
		default:
			ret = 0
		}
		return ret
	})
	for i, ks := range hf.KeySetOffset {
		ks.OffsetIndex = uint64(i)
	}
}

// Stats returns statistics about the history file
func (hf *HistoryFile) Stats() string {
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

	cacheHitRate := float64(0)
	if hf.cacheAccesses > 0 {
		cacheHitRate = float64(hf.cacheAccesses-hf.cacheMisses) / float64(hf.cacheAccesses) * 100
	}

	return fmt.Sprintf(
		"Bins: %d, Keys: %d (sorted: %d, unsorted: %d), Max unsorted/bin: %d, "+
			"Reads: %d, Writes: %d, Sorts: %d, Cache hit rate: %.1f%%",
		hf.OffsetCnt, totalSorted+totalUnsorted, totalSorted, totalUnsorted, maxUnsorted,
		hf.totalReads.Load(), hf.totalWrites.Load(), hf.totalSorts.Load(), cacheHitRate)
}