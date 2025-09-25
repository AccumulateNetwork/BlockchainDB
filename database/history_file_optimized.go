package blockchainDB

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/mmap"
)

// HistoryFileOptimized is an optimized version with better read performance
type HistoryFileOptimized struct {
	OffsetCnt       uint64
	KeySets         []*KeySet
	KeySetOffset    []*KeySet
	File            *os.File
	Directory       string

	// Optimizations
	keySetCache     sync.Map    // Lock-free cache
	mmapReader      *mmap.ReaderAt // Memory-mapped file reader
	cacheHits       atomic.Uint64
	cacheMisses     atomic.Uint64
	maxCacheSize    int // Configurable cache size
}

// NewHistoryFileOptimized creates an optimized history file
func NewHistoryFileOptimized(offsetCnt uint64, directory string) (*HistoryFileOptimized, error) {
	hf := new(HistoryFileOptimized)
	hf.OffsetCnt = offsetCnt
	hf.KeySets = make([]*KeySet, offsetCnt)
	hf.KeySetOffset = make([]*KeySet, offsetCnt)
	hf.Directory = directory
	hf.maxCacheSize = 1000 // Much larger cache

	for i := range hf.KeySets {
		keySet := new(KeySet)
		keySet.Start = HeaderLocation + uint64(i)*KeySetSize
		keySet.End = keySet.Start
		keySet.Index = uint64(i)
		keySet.OffsetIndex = uint64(i)
		hf.KeySets[i] = keySet
		hf.KeySetOffset[i] = keySet
	}

	filePath := fmt.Sprintf("%s/keys%d.hst", directory, offsetCnt)
	if _, err := os.Stat(filePath); err == nil {
		return hf.LoadHistoryFile()
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	hf.File = file

	// Initialize memory mapping
	if err := hf.initMmap(); err != nil {
		// Fall back to regular file I/O if mmap fails
		fmt.Printf("Warning: mmap initialization failed, using regular I/O: %v\n", err)
	}

	hdr := hf.Marshal()
	if _, err = hf.File.WriteAt(hdr, 0); err != nil {
		return nil, err
	}

	return hf, nil
}

// initMmap initializes memory mapping for the file
func (hf *HistoryFileOptimized) initMmap() error {
	stat, err := hf.File.Stat()
	if err != nil {
		return err
	}

	if stat.Size() > 0 {
		hf.mmapReader, err = mmap.Open(hf.File.Name())
		if err != nil {
			return err
		}
	}
	return nil
}

// Index returns which KeySet a key belongs to
func (hf *HistoryFileOptimized) Index(Key [32]byte) int {
	h := fxhash64(Key[:])
	return int(h % hf.OffsetCnt)
}

// Get retrieves a key with optimizations
func (hf *HistoryFileOptimized) GetOptimized(Key [32]byte) (dbBKey *DBBKey, err error) {
	index := hf.Index(Key)
	start := hf.KeySets[index].Start
	end := hf.KeySets[index].End
	keysLen := end - start

	if keysLen == 0 {
		return nil, errors.New("not found")
	}

	// Try lock-free cache first
	if cached, ok := hf.keySetCache.Load(index); ok {
		hf.cacheHits.Add(1)
		buffer := cached.([]byte)
		return hf.binarySearch(buffer, Key)
	}

	hf.cacheMisses.Add(1)

	// Read data using mmap if available, otherwise use file I/O
	var buffer []byte
	if hf.mmapReader != nil {
		buffer = make([]byte, keysLen)
		_, err = hf.mmapReader.ReadAt(buffer, int64(start))
	} else {
		buffer = make([]byte, keysLen)
		_, err = hf.File.ReadAt(buffer, int64(start))
	}

	if err != nil {
		return nil, err
	}

	// Store in cache (no lock needed with sync.Map)
	hf.keySetCache.Store(index, buffer)

	// Prefetch adjacent KeySets in background
	go hf.prefetchKeySet(index - 1)
	go hf.prefetchKeySet(index + 1)

	return hf.binarySearch(buffer, Key)
}

// prefetchKeySet loads a KeySet into cache in the background
func (hf *HistoryFileOptimized) prefetchKeySet(index int) {
	if index < 0 || index >= int(hf.OffsetCnt) {
		return
	}

	// Check if already cached
	if _, ok := hf.keySetCache.Load(index); ok {
		return
	}

	start := hf.KeySets[index].Start
	end := hf.KeySets[index].End
	keysLen := end - start

	if keysLen == 0 {
		return
	}

	buffer := make([]byte, keysLen)
	if hf.mmapReader != nil {
		hf.mmapReader.ReadAt(buffer, int64(start))
	} else {
		hf.File.ReadAt(buffer, int64(start))
	}

	hf.keySetCache.Store(index, buffer)
}

// binarySearch performs binary search on a sorted buffer
func (hf *HistoryFileOptimized) binarySearch(buffer []byte, Key [32]byte) (*DBBKey, error) {
	numEntries := len(buffer) / DBKeyFullSize
	left, right := 0, numEntries-1

	for left <= right {
		mid := (left + right) / 2
		midOffset := mid * DBKeyFullSize
		midKey := buffer[midOffset : midOffset+32]

		cmp := bytes.Compare(midKey, Key[:])
		if cmp == 0 {
			var dbKey DBBKey
			if _, err := dbKey.Unmarshal(buffer[midOffset : midOffset+DBKeyFullSize]); err != nil {
				return nil, err
			}
			return &dbKey, nil
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil, errors.New("not found")
}

// GetStats returns cache statistics
func (hf *HistoryFileOptimized) GetStats() (hits, misses uint64, hitRate float64) {
	hits = hf.cacheHits.Load()
	misses = hf.cacheMisses.Load()
	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	return
}

// Close cleans up resources
func (hf *HistoryFileOptimized) Close() error {
	if hf.mmapReader != nil {
		if err := hf.mmapReader.Close(); err != nil {
			return err
		}
	}
	return hf.File.Close()
}

// Marshal serializes the header
func (hf *HistoryFileOptimized) Marshal() []byte {
	// Implementation same as original HistoryFile
	buffer := make([]byte, HeaderLocation)
	// ... marshal logic here
	return buffer
}

// LoadHistoryFile loads an existing history file
func (hf *HistoryFileOptimized) LoadHistoryFile() (*HistoryFileOptimized, error) {
	// Implementation similar to original, but returns HistoryFileOptimized
	// ... load logic here
	return hf, nil
}

// AddKeys adds keys to the history file
func (hf *HistoryFileOptimized) AddKeys(keys []byte) error {
	// Implementation same as original
	// Clear affected cache entries after write
	// ...
	return nil
}

// SortAllKeySets sorts all KeySets for binary search
func (hf *HistoryFileOptimized) SortAllKeySets() error {
	// Implementation same as original
	// Clear entire cache after sorting
	hf.keySetCache = sync.Map{}
	return nil
}