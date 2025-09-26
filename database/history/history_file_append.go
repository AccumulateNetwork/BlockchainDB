package history

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/AccumulateNetwork/BlockchainDB/database/utils"
)

// HistoryFileAppend is an optimized version that uses append-only writes
// to avoid the read-modify-write pattern that causes performance degradation
type HistoryFileAppend struct {
	Directory string
	NumBins   int

	// File handles for each bin - kept open for performance
	binFiles []*os.File
	binSizes []int64 // Current size of each bin file

	// Mutex for thread safety
	mu sync.RWMutex

	// Stats
	totalWrites int64
	totalKeys   int64
}

// NewHistoryFileAppend creates a new append-optimized history file
func NewHistoryFileAppend(numBins int, directory string) (*HistoryFileAppend, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	hf := &HistoryFileAppend{
		Directory: directory,
		NumBins:   numBins,
		binFiles:  make([]*os.File, numBins),
		binSizes:  make([]int64, numBins),
	}

	// Open all bin files in append mode
	for i := 0; i < numBins; i++ {
		filename := filepath.Join(directory, fmt.Sprintf("bin_%06d.dat", i))
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			// Clean up already opened files
			for j := 0; j < i; j++ {
				hf.binFiles[j].Close()
			}
			return nil, fmt.Errorf("failed to open bin %d: %w", i, err)
		}

		// Get current file size
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			for j := 0; j < i; j++ {
				hf.binFiles[j].Close()
			}
			return nil, err
		}

		hf.binFiles[i] = file
		hf.binSizes[i] = stat.Size()
	}

	return hf, nil
}

// Index returns the bin index for a key
func (hf *HistoryFileAppend) Index(key [32]byte) int {
	// Use first 4 bytes for indexing (same as original)
	index := binary.BigEndian.Uint32(key[:4])
	return int(index % uint32(hf.NumBins))
}

// AddKeys adds a batch of keys using append-only writes
func (hf *HistoryFileAppend) AddKeys(keyList []byte) error {
	if len(keyList) == 0 {
		return nil
	}

	if len(keyList)%utils.DBKeyFullSize != 0 {
		return fmt.Errorf("keyList length must be multiple of %d", utils.DBKeyFullSize)
	}

	hf.mu.Lock()
	defer hf.mu.Unlock()

	// Group keys by bin
	type binData struct {
		binIndex int
		data     []byte
	}

	bins := make(map[int][]byte)
	currentBin := -1
	var currentData []byte

	// Process keys and group by bin
	for i := 0; i < len(keyList); i += utils.DBKeyFullSize {
		key := [32]byte{}
		copy(key[:], keyList[i:i+32])
		binIndex := hf.Index(key)

		if binIndex != currentBin {
			if currentBin >= 0 && len(currentData) > 0 {
				if existing, ok := bins[currentBin]; ok {
					bins[currentBin] = append(existing, currentData...)
				} else {
					bins[currentBin] = currentData
				}
			}
			currentBin = binIndex
			currentData = make([]byte, 0, utils.DBKeyFullSize)
		}

		currentData = append(currentData, keyList[i:i+utils.DBKeyFullSize]...)
	}

	// Add last bin
	if currentBin >= 0 && len(currentData) > 0 {
		if existing, ok := bins[currentBin]; ok {
			bins[currentBin] = append(existing, currentData...)
		} else {
			bins[currentBin] = currentData
		}
	}

	// Write to each bin file (append only!)
	for binIndex, data := range bins {
		// Just append to the file - no reading required!
		n, err := hf.binFiles[binIndex].Write(data)
		if err != nil {
			return fmt.Errorf("failed to write to bin %d: %w", binIndex, err)
		}

		hf.binSizes[binIndex] += int64(n)
		hf.totalKeys += int64(len(data) / utils.DBKeyFullSize)
	}

	hf.totalWrites++
	return nil
}

// Get retrieves a key from the history file
func (hf *HistoryFileAppend) Get(key [32]byte) (*utils.DBBKey, error) {
	binIndex := hf.Index(key)

	hf.mu.RLock()
	file := hf.binFiles[binIndex]
	size := hf.binSizes[binIndex]
	hf.mu.RUnlock()

	if size == 0 {
		return nil, fmt.Errorf("key not found")
	}

	// Read the entire bin (could optimize with indexing later)
	buffer := make([]byte, size)
	_, err := file.ReadAt(buffer, 0)
	if err != nil {
		return nil, err
	}

	// Linear search (could optimize with binary search after sorting)
	for i := 0; i < len(buffer); i += utils.DBKeyFullSize {
		if string(buffer[i:i+32]) == string(key[:]) {
			dbKey := new(utils.DBBKey)
			dbKey.Unmarshal(buffer[i+32 : i+utils.DBKeyFullSize])
			return dbKey, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// SortAllBins sorts all bins for efficient binary search
func (hf *HistoryFileAppend) SortAllBins() error {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	for i, file := range hf.binFiles {
		size := hf.binSizes[i]
		if size == 0 {
			continue
		}

		// Read entire bin
		buffer := make([]byte, size)
		_, err := file.ReadAt(buffer, 0)
		if err != nil {
			return err
		}

		// Sort the buffer
		sortKeyBufferAppend(buffer)

		// Write back sorted
		_, err = file.WriteAt(buffer, 0)
		if err != nil {
			return err
		}

		// Sync to disk
		file.Sync()
	}

	return nil
}

// Close closes all bin files
func (hf *HistoryFileAppend) Close() error {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	var firstErr error
	for i, file := range hf.binFiles {
		if file != nil {
			if err := file.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("failed to close bin %d: %w", i, err)
			}
			hf.binFiles[i] = nil
		}
	}

	return firstErr
}

// Stats returns statistics about the history file
func (hf *HistoryFileAppend) Stats() string {
	hf.mu.RLock()
	defer hf.mu.RUnlock()

	totalSize := int64(0)
	maxBinSize := int64(0)
	nonEmptyBins := 0

	for _, size := range hf.binSizes {
		totalSize += size
		if size > 0 {
			nonEmptyBins++
		}
		if size > maxBinSize {
			maxBinSize = size
		}
	}

	avgBinSize := int64(0)
	if nonEmptyBins > 0 {
		avgBinSize = totalSize / int64(nonEmptyBins)
	}

	return fmt.Sprintf("Bins: %d (non-empty: %d), Keys: %d, Writes: %d, Total: %d MB, Max bin: %d KB, Avg bin: %d KB",
		hf.NumBins, nonEmptyBins, hf.totalKeys, hf.totalWrites,
		totalSize/(1024*1024), maxBinSize/1024, avgBinSize/1024)
}

// sortKeyBufferAppend sorts entries in the buffer by their 32-byte key
func sortKeyBufferAppend(buffer []byte) {
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