package timeseries

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

// KeyIndex maps keys to their locations in value files
type KeyIndex struct {
	indexFile   *os.File
	bloomFilter *BloomFilter
	memIndex    map[[32]byte]*KeyLocation // Recent keys in memory
	diskIndex   map[[32]byte]*KeyLocation // Persistent index

	// Write buffering
	writeBuffer []*KeyIndexEntry
	bufferSize  int
	maxBuffer   int

	mu sync.RWMutex
}

// KeyLocation describes where a value is stored
type KeyLocation struct {
	Filename  string // Which value file
	Offset    uint64 // Offset within that file
	Length    uint32 // Length of value
	Timestamp int64  // When it was written
}

// KeyIndexEntry is a buffered write entry
type KeyIndexEntry struct {
	Key      [32]byte
	Location KeyLocation
}

// BloomFilter provides fast existence checks
type BloomFilter struct {
	bits   []byte
	size   uint64
	hashes uint32
}

// NewKeyIndex creates a new key index
func NewKeyIndex() *KeyIndex {
	return &KeyIndex{
		bloomFilter: NewBloomFilterWithSize(10 * 1024 * 1024), // 10MB
		memIndex:    make(map[[32]byte]*KeyLocation),
		diskIndex:   make(map[[32]byte]*KeyLocation),
		writeBuffer: make([]*KeyIndexEntry, 0, 1000),
		maxBuffer:   1000,
	}
}

// OpenKeyIndex opens an existing key index
func OpenKeyIndex(filename string) (*KeyIndex, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	ki := &KeyIndex{
		indexFile:   file,
		bloomFilter: NewBloomFilterWithSize(10 * 1024 * 1024),
		memIndex:    make(map[[32]byte]*KeyLocation),
		diskIndex:   make(map[[32]byte]*KeyLocation),
		writeBuffer: make([]*KeyIndexEntry, 0, 1000),
		maxBuffer:   1000,
	}

	// Load existing index
	if err := ki.load(); err != nil {
		file.Close()
		return nil, err
	}

	return ki, nil
}

// Put adds a key-location mapping to the index
func (ki *KeyIndex) Put(key [32]byte, location KeyLocation) error {
	ki.mu.Lock()
	defer ki.mu.Unlock()

	// Add to bloom filter
	ki.bloomFilter.Add(key)

	// Add to memory index
	ki.memIndex[key] = &location

	// Buffer the write
	ki.writeBuffer = append(ki.writeBuffer, &KeyIndexEntry{
		Key:      key,
		Location: location,
	})
	ki.bufferSize++

	// Flush if buffer is full
	if ki.bufferSize >= ki.maxBuffer {
		return ki.flushBuffer()
	}

	return nil
}

// Get retrieves the location for a key
func (ki *KeyIndex) Get(key [32]byte) (*KeyLocation, error) {
	// Check bloom filter first
	if !ki.bloomFilter.Test(key) {
		return nil, errors.New("key not found")
	}

	ki.mu.RLock()
	defer ki.mu.RUnlock()

	// Check memory index
	if loc, ok := ki.memIndex[key]; ok {
		return loc, nil
	}

	// Check disk index
	if loc, ok := ki.diskIndex[key]; ok {
		return loc, nil
	}

	return nil, errors.New("key not found")
}

// Delete removes a key from the index
func (ki *KeyIndex) Delete(key [32]byte) error {
	ki.mu.Lock()
	defer ki.mu.Unlock()

	delete(ki.memIndex, key)
	delete(ki.diskIndex, key)

	// Note: Can't remove from bloom filter (it's append-only)
	// This means bloom filter may have false positives for deleted keys

	return nil
}

// flushBuffer writes buffered entries to disk
func (ki *KeyIndex) flushBuffer() error {
	if len(ki.writeBuffer) == 0 {
		return nil
	}

	// Write each entry to disk
	for _, entry := range ki.writeBuffer {
		if err := ki.writeEntry(entry); err != nil {
			return err
		}

		// Move from memory to disk index
		ki.diskIndex[entry.Key] = &entry.Location
	}

	// Clear buffer
	ki.writeBuffer = ki.writeBuffer[:0]
	ki.bufferSize = 0

	// Sync to disk
	if ki.indexFile != nil {
		return ki.indexFile.Sync()
	}

	return nil
}

// writeEntry writes a single index entry to disk
func (ki *KeyIndex) writeEntry(entry *KeyIndexEntry) error {
	if ki.indexFile == nil {
		return nil // Memory-only mode
	}

	// Write key
	if _, err := ki.indexFile.Write(entry.Key[:]); err != nil {
		return err
	}

	// Write filename length and filename
	filenameBytes := []byte(entry.Location.Filename)
	if err := binary.Write(ki.indexFile, binary.BigEndian, uint32(len(filenameBytes))); err != nil {
		return err
	}
	if _, err := ki.indexFile.Write(filenameBytes); err != nil {
		return err
	}

	// Write offset, length, timestamp
	if err := binary.Write(ki.indexFile, binary.BigEndian, entry.Location.Offset); err != nil {
		return err
	}
	if err := binary.Write(ki.indexFile, binary.BigEndian, entry.Location.Length); err != nil {
		return err
	}
	if err := binary.Write(ki.indexFile, binary.BigEndian, entry.Location.Timestamp); err != nil {
		return err
	}

	return nil
}

// load reads the index from disk
func (ki *KeyIndex) load() error {
	if ki.indexFile == nil {
		return nil
	}

	// Seek to beginning
	if _, err := ki.indexFile.Seek(0, 0); err != nil {
		return err
	}

	for {
		var key [32]byte

		// Read key
		if _, err := ki.indexFile.Read(key[:]); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		// Read filename length
		var filenameLen uint32
		if err := binary.Read(ki.indexFile, binary.BigEndian, &filenameLen); err != nil {
			return err
		}

		// Read filename
		filenameBytes := make([]byte, filenameLen)
		if _, err := ki.indexFile.Read(filenameBytes); err != nil {
			return err
		}

		location := KeyLocation{
			Filename: string(filenameBytes),
		}

		// Read offset, length, timestamp
		if err := binary.Read(ki.indexFile, binary.BigEndian, &location.Offset); err != nil {
			return err
		}
		if err := binary.Read(ki.indexFile, binary.BigEndian, &location.Length); err != nil {
			return err
		}
		if err := binary.Read(ki.indexFile, binary.BigEndian, &location.Timestamp); err != nil {
			return err
		}

		// Add to disk index and bloom filter
		ki.diskIndex[key] = &location
		ki.bloomFilter.Add(key)
	}

	// Seek to end for future writes
	_, err := ki.indexFile.Seek(0, 2)
	return err
}

// Close flushes pending writes and closes the index
func (ki *KeyIndex) Close() error {
	ki.mu.Lock()
	defer ki.mu.Unlock()

	// Flush any pending writes
	if err := ki.flushBuffer(); err != nil {
		return err
	}

	// Close file
	if ki.indexFile != nil {
		return ki.indexFile.Close()
	}

	return nil
}

// Stats returns statistics about the index
func (ki *KeyIndex) Stats() KeyIndexStats {
	ki.mu.RLock()
	defer ki.mu.RUnlock()

	return KeyIndexStats{
		MemoryKeys: len(ki.memIndex),
		DiskKeys:   len(ki.diskIndex),
		TotalKeys:  len(ki.memIndex) + len(ki.diskIndex),
		BufferSize: ki.bufferSize,
	}
}

// KeyIndexStats contains index statistics
type KeyIndexStats struct {
	MemoryKeys int
	DiskKeys   int
	TotalKeys  int
	BufferSize int
}

// NewBloomFilterWithSize creates a bloom filter with specified size in bytes
func NewBloomFilterWithSize(sizeBytes int) *BloomFilter {
	return &BloomFilter{
		bits:   make([]byte, sizeBytes),
		size:   uint64(sizeBytes * 8), // Convert to bits
		hashes: 3,
	}
}

// Add adds a key to the bloom filter
func (bf *BloomFilter) Add(key [32]byte) {
	h1, h2 := bf.hash(key)

	for i := uint32(0); i < bf.hashes; i++ {
		pos := (h1 + uint64(i)*h2) % bf.size
		bytePos := pos / 8
		bitPos := pos % 8
		bf.bits[bytePos] |= 1 << bitPos
	}
}

// Test checks if a key might be in the set
func (bf *BloomFilter) Test(key [32]byte) bool {
	h1, h2 := bf.hash(key)

	for i := uint32(0); i < bf.hashes; i++ {
		pos := (h1 + uint64(i)*h2) % bf.size
		bytePos := pos / 8
		bitPos := pos % 8

		if bf.bits[bytePos]&(1<<bitPos) == 0 {
			return false
		}
	}

	return true
}

// hash generates two hash values from a key using FNV-1a
func (bf *BloomFilter) hash(key [32]byte) (uint64, uint64) {
	// FNV-1a hash
	const (
		prime  = 0x100000001b3
		offset = 0xcbf29ce484222325
	)

	h1 := uint64(offset)
	h2 := uint64(offset)

	// First hash
	for i := 0; i < 16; i++ {
		h1 ^= uint64(key[i])
		h1 *= prime
	}

	// Second hash
	for i := 16; i < 32; i++ {
		h2 ^= uint64(key[i])
		h2 *= prime
	}

	return h1, h2
}