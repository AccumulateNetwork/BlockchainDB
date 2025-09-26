package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dkvFilename    = "dkv_index.dat"
	dkvValueFile   = "dkv_values.dat"
	compactThreshold = 0.5 // Compact when 50% of space is wasted
)

// DKV (Dynamic Key-Value) is optimized for mutable data
// Unlike KV which is optimized for immutable/append-only data,
// DKV supports efficient updates and deletes with periodic compaction
type DKV struct {
	Directory   string
	ValueFile   *os.File // Current value file
	IndexFile   *os.File // Key index file

	// Sharding by key prefix for better concurrency
	shards []*DKVShard
	numShards int

	// Compaction management
	wastedSpace     atomic.Int64
	totalSpace      atomic.Int64
	compactRunning  atomic.Bool
	lastCompact     time.Time

	// Statistics
	totalReads   atomic.Int64
	totalWrites  atomic.Int64
	totalDeletes atomic.Int64
	totalCompacts atomic.Int64

	mu sync.RWMutex
}

// DKVShard represents a shard of the dynamic KV store
type DKVShard struct {
	id       int
	index    map[[32]byte]*DKVEntry // In-memory index
	deleted  map[[32]byte]bool       // Tombstones for deleted keys

	// Write-ahead log for crash recovery
	wal      *os.File
	walPath  string

	mu sync.RWMutex
}

// DKVEntry represents a mutable key-value entry
type DKVEntry struct {
	Key       [32]byte
	ValueOffset uint64
	ValueLength uint64
	Timestamp   int64
	Version     uint32  // Version number for updates
	Deleted     bool    // Tombstone marker
}

// NewDKV creates a new dynamic key-value store
func NewDKV(directory string, numShards int) (*DKV, error) {
	if numShards <= 0 || numShards > 256 {
		numShards = 16 // Default to 16 shards
	}

	// Create directory
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	dkv := &DKV{
		Directory:   directory,
		numShards:   numShards,
		shards:      make([]*DKVShard, numShards),
		lastCompact: time.Now(),
	}

	// Create value file
	valueFilePath := filepath.Join(directory, dkvValueFile)
	var err error
	if dkv.ValueFile, err = os.OpenFile(valueFilePath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
		return nil, err
	}

	// Create index file
	indexFilePath := filepath.Join(directory, dkvFilename)
	if dkv.IndexFile, err = os.Create(indexFilePath); err != nil {
		return nil, err
	}

	// Initialize shards
	for i := 0; i < numShards; i++ {
		shard := &DKVShard{
			id:      i,
			index:   make(map[[32]byte]*DKVEntry),
			deleted: make(map[[32]byte]bool),
			walPath: filepath.Join(directory, fmt.Sprintf("dkv_wal_%d.log", i)),
		}

		// Create WAL for each shard
		if shard.wal, err = os.Create(shard.walPath); err != nil {
			return nil, err
		}

		dkv.shards[i] = shard
	}

	// Write header
	if err := dkv.writeHeader(); err != nil {
		return nil, err
	}

	return dkv, nil
}

// OpenDKV opens an existing dynamic key-value store
func OpenDKV(directory string) (*DKV, error) {
	dkv := &DKV{
		Directory: directory,
	}

	// Open value file
	valueFilePath := filepath.Join(directory, dkvValueFile)
	var err error
	if dkv.ValueFile, err = os.OpenFile(valueFilePath,
		os.O_RDWR|os.O_APPEND, 0644); err != nil {
		return nil, err
	}

	// Open index file
	indexFilePath := filepath.Join(directory, dkvFilename)
	if dkv.IndexFile, err = os.OpenFile(indexFilePath,
		os.O_RDWR, 0644); err != nil {
		return nil, err
	}

	// Read header
	if err := dkv.readHeader(); err != nil {
		return nil, err
	}

	// Initialize shards
	dkv.shards = make([]*DKVShard, dkv.numShards)
	for i := 0; i < dkv.numShards; i++ {
		shard := &DKVShard{
			id:      i,
			index:   make(map[[32]byte]*DKVEntry),
			deleted: make(map[[32]byte]bool),
			walPath: filepath.Join(directory, fmt.Sprintf("dkv_wal_%d.log", i)),
		}

		// Open WAL
		if shard.wal, err = os.OpenFile(shard.walPath,
			os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); err != nil {
			return nil, err
		}

		// Replay WAL to rebuild index
		if err := shard.replayWAL(); err != nil {
			return nil, err
		}

		dkv.shards[i] = shard
	}

	// Load index from file
	if err := dkv.loadIndex(); err != nil {
		return nil, err
	}

	return dkv, nil
}

// Put stores or updates a key-value pair
func (dkv *DKV) Put(key [32]byte, value []byte) error {
	dkv.totalWrites.Add(1)

	// Get shard
	shardID := dkv.getShardID(key)
	shard := dkv.shards[shardID]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if key exists for update
	var oldEntry *DKVEntry
	if existing, exists := shard.index[key]; exists {
		oldEntry = existing
		// Mark old space as wasted
		dkv.wastedSpace.Add(int64(existing.ValueLength))
	}

	// Get current file position
	fileInfo, err := dkv.ValueFile.Stat()
	if err != nil {
		return err
	}
	offset := uint64(fileInfo.Size())

	// Write value to file
	if _, err := dkv.ValueFile.Write(value); err != nil {
		return err
	}

	// Create new entry
	entry := &DKVEntry{
		Key:         key,
		ValueOffset: offset,
		ValueLength: uint64(len(value)),
		Timestamp:   time.Now().Unix(),
		Version:     1,
		Deleted:     false,
	}

	if oldEntry != nil {
		entry.Version = oldEntry.Version + 1
	}

	// Update index
	shard.index[key] = entry
	delete(shard.deleted, key)

	// Write to WAL
	if err := shard.writeWAL(entry); err != nil {
		return err
	}

	// Update space tracking
	dkv.totalSpace.Add(int64(len(value)))

	// Check if compaction needed
	dkv.maybeCompact()

	return nil
}

// Get retrieves a value by key
func (dkv *DKV) Get(key [32]byte) ([]byte, error) {
	dkv.totalReads.Add(1)

	// Get shard
	shardID := dkv.getShardID(key)
	shard := dkv.shards[shardID]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Check if deleted
	if shard.deleted[key] {
		return nil, errors.New("key not found (deleted)")
	}

	// Get from index
	entry, exists := shard.index[key]
	if !exists {
		return nil, errors.New("key not found")
	}

	// Read value from file
	value := make([]byte, entry.ValueLength)
	if _, err := dkv.ValueFile.ReadAt(value, int64(entry.ValueOffset)); err != nil {
		return nil, err
	}

	return value, nil
}

// Delete marks a key as deleted
func (dkv *DKV) Delete(key [32]byte) error {
	dkv.totalDeletes.Add(1)

	// Get shard
	shardID := dkv.getShardID(key)
	shard := dkv.shards[shardID]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if exists
	entry, exists := shard.index[key]
	if !exists {
		return errors.New("key not found")
	}

	// Mark as deleted
	shard.deleted[key] = true
	entry.Deleted = true

	// Mark space as wasted
	dkv.wastedSpace.Add(int64(entry.ValueLength))

	// Write tombstone to WAL
	if err := shard.writeWAL(entry); err != nil {
		return err
	}

	// Trigger compaction if needed
	dkv.maybeCompact()

	return nil
}

// Update atomically updates a value if it exists
func (dkv *DKV) Update(key [32]byte, updateFunc func([]byte) ([]byte, error)) error {
	// Get shard
	shardID := dkv.getShardID(key)
	shard := dkv.shards[shardID]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Get current value
	entry, exists := shard.index[key]
	if !exists {
		return errors.New("key not found")
	}

	// Read current value
	oldValue := make([]byte, entry.ValueLength)
	if _, err := dkv.ValueFile.ReadAt(oldValue, int64(entry.ValueOffset)); err != nil {
		return err
	}

	// Apply update function
	newValue, err := updateFunc(oldValue)
	if err != nil {
		return err
	}

	// If value unchanged, no-op
	if bytes.Equal(oldValue, newValue) {
		return nil
	}

	// Mark old space as wasted
	dkv.wastedSpace.Add(int64(entry.ValueLength))

	// Write new value
	fileInfo, _ := dkv.ValueFile.Stat()
	offset := uint64(fileInfo.Size())

	if _, err := dkv.ValueFile.Write(newValue); err != nil {
		return err
	}

	// Update entry
	entry.ValueOffset = offset
	entry.ValueLength = uint64(len(newValue))
	entry.Timestamp = time.Now().Unix()
	entry.Version++

	// Write to WAL
	if err := shard.writeWAL(entry); err != nil {
		return err
	}

	// Update space tracking
	dkv.totalSpace.Add(int64(len(newValue)))

	return nil
}

// Compact performs compaction to reclaim wasted space
func (dkv *DKV) Compact() error {
	if !dkv.compactRunning.CompareAndSwap(false, true) {
		return errors.New("compaction already running")
	}
	defer dkv.compactRunning.Store(false)

	dkv.totalCompacts.Add(1)

	// Create new value file
	newValuePath := filepath.Join(dkv.Directory, "dkv_values_new.dat")
	newValueFile, err := os.Create(newValuePath)
	if err != nil {
		return err
	}

	// Rewrite all active values
	var newOffset uint64
	for _, shard := range dkv.shards {
		shard.mu.Lock()

		for key, entry := range shard.index {
			if shard.deleted[key] || entry.Deleted {
				continue
			}

			// Read old value
			value := make([]byte, entry.ValueLength)
			if _, err := dkv.ValueFile.ReadAt(value, int64(entry.ValueOffset)); err != nil {
				shard.mu.Unlock()
				return err
			}

			// Write to new file
			if _, err := newValueFile.Write(value); err != nil {
				shard.mu.Unlock()
				return err
			}

			// Update entry
			entry.ValueOffset = newOffset
			newOffset += uint64(len(value))
		}

		// Clear deleted entries
		shard.deleted = make(map[[32]byte]bool)

		shard.mu.Unlock()
	}

	// Close files
	newValueFile.Close()
	dkv.ValueFile.Close()

	// Atomic replace
	oldPath := filepath.Join(dkv.Directory, dkvValueFile)
	os.Remove(oldPath)
	os.Rename(newValuePath, oldPath)

	// Reopen value file
	dkv.ValueFile, err = os.OpenFile(oldPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// Reset space tracking
	dkv.wastedSpace.Store(0)
	dkv.totalSpace.Store(int64(newOffset))
	dkv.lastCompact = time.Now()

	return nil
}

// maybeCompact checks if compaction should run
func (dkv *DKV) maybeCompact() {
	totalSpace := dkv.totalSpace.Load()
	wastedSpace := dkv.wastedSpace.Load()

	if totalSpace > 0 {
		wasteRatio := float64(wastedSpace) / float64(totalSpace)
		if wasteRatio > compactThreshold {
			// Run compaction in background
			go dkv.Compact()
		}
	}
}

// getShardID determines which shard a key belongs to
func (dkv *DKV) getShardID(key [32]byte) int {
	return int(key[0]) % dkv.numShards
}

// writeWAL writes an entry to the write-ahead log
func (shard *DKVShard) writeWAL(entry *DKVEntry) error {
	// Simple binary format: [key][offset][length][timestamp][version][deleted]
	data := make([]byte, 32+8+8+8+4+1)
	copy(data[0:32], entry.Key[:])
	binary.BigEndian.PutUint64(data[32:40], entry.ValueOffset)
	binary.BigEndian.PutUint64(data[40:48], entry.ValueLength)
	binary.BigEndian.PutUint64(data[48:56], uint64(entry.Timestamp))
	binary.BigEndian.PutUint32(data[56:60], entry.Version)
	if entry.Deleted {
		data[60] = 1
	}

	_, err := shard.wal.Write(data)
	return err
}

// replayWAL replays the write-ahead log to rebuild the index
func (shard *DKVShard) replayWAL() error {
	// Seek to beginning
	if _, err := shard.wal.Seek(0, 0); err != nil {
		return err
	}

	entrySize := 32 + 8 + 8 + 8 + 4 + 1
	data := make([]byte, entrySize)

	for {
		n, err := shard.wal.Read(data)
		if err != nil || n != entrySize {
			break
		}

		entry := &DKVEntry{}
		copy(entry.Key[:], data[0:32])
		entry.ValueOffset = binary.BigEndian.Uint64(data[32:40])
		entry.ValueLength = binary.BigEndian.Uint64(data[40:48])
		entry.Timestamp = int64(binary.BigEndian.Uint64(data[48:56]))
		entry.Version = binary.BigEndian.Uint32(data[56:60])
		entry.Deleted = data[60] == 1

		if entry.Deleted {
			shard.deleted[entry.Key] = true
		} else {
			shard.index[entry.Key] = entry
		}
	}

	// Seek to end for appending
	_, err := shard.wal.Seek(0, 2)
	return err
}

// writeHeader writes the DKV header
func (dkv *DKV) writeHeader() error {
	header := make([]byte, 1024)

	// Magic bytes
	copy(header[0:4], []byte("DKVB"))

	// Version
	binary.BigEndian.PutUint32(header[4:8], 1)

	// Number of shards
	binary.BigEndian.PutUint32(header[8:12], uint32(dkv.numShards))

	// Write to file
	_, err := dkv.IndexFile.WriteAt(header, 0)
	return err
}

// readHeader reads the DKV header
func (dkv *DKV) readHeader() error {
	header := make([]byte, 1024)

	if _, err := dkv.IndexFile.ReadAt(header, 0); err != nil {
		return err
	}

	// Check magic bytes
	if string(header[0:4]) != "DKVB" {
		return errors.New("invalid DKV file format")
	}

	// Check version
	version := binary.BigEndian.Uint32(header[4:8])
	if version != 1 {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Read number of shards
	dkv.numShards = int(binary.BigEndian.Uint32(header[8:12]))

	return nil
}

// loadIndex loads the index from disk
func (dkv *DKV) loadIndex() error {
	// This would load persisted index data
	// For now, rely on WAL replay
	return nil
}

// Close closes the DKV store
func (dkv *DKV) Close() error {
	// Close all shard WALs
	for _, shard := range dkv.shards {
		if shard.wal != nil {
			shard.wal.Close()
		}
	}

	// Close main files
	if dkv.ValueFile != nil {
		dkv.ValueFile.Close()
	}

	if dkv.IndexFile != nil {
		dkv.IndexFile.Close()
	}

	return nil
}

// Stats returns statistics about the DKV store
func (dkv *DKV) Stats() DKVStats {
	stats := DKVStats{
		TotalReads:    dkv.totalReads.Load(),
		TotalWrites:   dkv.totalWrites.Load(),
		TotalDeletes:  dkv.totalDeletes.Load(),
		TotalCompacts: dkv.totalCompacts.Load(),
		TotalSpace:    dkv.totalSpace.Load(),
		WastedSpace:   dkv.wastedSpace.Load(),
		NumShards:     dkv.numShards,
		LastCompact:   dkv.lastCompact,
	}

	// Calculate waste ratio
	if stats.TotalSpace > 0 {
		stats.WasteRatio = float64(stats.WastedSpace) / float64(stats.TotalSpace)
	}

	// Collect per-shard stats
	for _, shard := range dkv.shards {
		shard.mu.RLock()
		stats.TotalKeys += len(shard.index)
		stats.DeletedKeys += len(shard.deleted)
		shard.mu.RUnlock()
	}

	return stats
}

// DKVStats contains statistics about the DKV store
type DKVStats struct {
	TotalReads    int64
	TotalWrites   int64
	TotalDeletes  int64
	TotalCompacts int64
	TotalKeys     int
	DeletedKeys   int
	TotalSpace    int64
	WastedSpace   int64
	WasteRatio    float64
	NumShards     int
	LastCompact   time.Time
}