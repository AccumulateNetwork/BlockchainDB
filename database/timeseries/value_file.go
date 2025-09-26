package timeseries

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	MagicBytes = "VBLK"
	Version    = 1
	HeaderSize = 64 // Fixed header size
)

var (
	ErrInvalidMagic   = errors.New("invalid magic bytes")
	ErrVersionMismatch = errors.New("version mismatch")
	ErrCorruptedFile  = errors.New("file corrupted")
)

// ValueFile represents a time-based file containing values
type ValueFile struct {
	filename  string
	file      *os.File
	header    *ValueFileHeader
	index     *ValueIndex
	mu        sync.RWMutex
	readonly  bool
}

// ValueFileHeader contains metadata about the value file
type ValueFileHeader struct {
	Magic       [4]byte  // "VBLK"
	Version     uint32   // File format version
	StartTime   int64    // Unix timestamp of first entry
	EndTime     int64    // Unix timestamp of last entry
	EntryCount  uint32   // Number of values
	DataOffset  uint64   // Where data starts (after header)
	IndexOffset uint64   // Where index starts
	Checksum    [32]byte // SHA256 of all entries
}

// ValueEntry represents a single value with metadata
type ValueEntry struct {
	Timestamp int64    // When this value was written
	Length    uint32   // Length of value data
	Key       [32]byte // Hash of the value (for verification)
	Value     []byte   // Actual value data
}

// ValueIndex provides fast lookups within a value file
type ValueIndex struct {
	keyOffsets map[[32]byte]uint64 // Key -> file offset
	timeIndex  []TimeEntry         // Sorted by timestamp
	mu         sync.RWMutex
}

// TimeEntry maps timestamp to file location
type TimeEntry struct {
	Timestamp int64
	Offset    uint64
	Key       [32]byte
}

// NewValueFile creates a new value file for writing
func NewValueFile(filename string, startTime int64) (*ValueFile, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	vf := &ValueFile{
		filename: filename,
		file:     file,
		header: &ValueFileHeader{
			Magic:      [4]byte{'V', 'B', 'L', 'K'},
			Version:    Version,
			StartTime:  startTime,
			DataOffset: HeaderSize,
		},
		index: &ValueIndex{
			keyOffsets: make(map[[32]byte]uint64),
			timeIndex:  make([]TimeEntry, 0),
		},
		readonly: false,
	}

	// Write initial header (will be updated on close)
	if err := vf.writeHeader(); err != nil {
		file.Close()
		os.Remove(filename)
		return nil, err
	}

	// Seek to data start position
	if _, err := file.Seek(HeaderSize, 0); err != nil {
		file.Close()
		os.Remove(filename)
		return nil, err
	}

	return vf, nil
}

// OpenValueFile opens an existing value file for reading
func OpenValueFile(filename string) (*ValueFile, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	vf := &ValueFile{
		filename: filename,
		file:     file,
		header:   &ValueFileHeader{},
		readonly: true,
	}

	// Read and validate header
	if err := vf.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	// Load index if it exists
	if vf.header.IndexOffset > 0 {
		if err := vf.loadIndex(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return vf, nil
}

// Append adds a new value to the file
func (vf *ValueFile) Append(value []byte) ([32]byte, error) {
	if vf.readonly {
		return [32]byte{}, errors.New("file is read-only")
	}

	vf.mu.Lock()
	defer vf.mu.Unlock()

	// Calculate key as hash of value
	key := sha256.Sum256(value)

	// Get current file position
	offset, err := vf.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return [32]byte{}, err
	}

	// Create entry
	entry := ValueEntry{
		Timestamp: time.Now().Unix(),
		Length:    uint32(len(value)),
		Key:       key,
		Value:     value,
	}

	// Write entry
	if err := vf.writeEntry(&entry); err != nil {
		return [32]byte{}, err
	}

	// Update index
	vf.index.keyOffsets[key] = uint64(offset)
	vf.index.timeIndex = append(vf.index.timeIndex, TimeEntry{
		Timestamp: entry.Timestamp,
		Offset:    uint64(offset),
		Key:       key,
	})

	// Update header
	vf.header.EntryCount++
	vf.header.EndTime = entry.Timestamp

	return key, nil
}

// Get retrieves a value by key
func (vf *ValueFile) Get(key [32]byte) ([]byte, error) {
	vf.mu.RLock()
	defer vf.mu.RUnlock()

	offset, exists := vf.index.keyOffsets[key]
	if !exists {
		return nil, errors.New("key not found")
	}

	// Seek to entry
	if _, err := vf.file.Seek(int64(offset), 0); err != nil {
		return nil, err
	}

	// Read entry
	entry, err := vf.readEntry()
	if err != nil {
		return nil, err
	}

	// Verify key matches
	if entry.Key != key {
		return nil, ErrCorruptedFile
	}

	// Verify value integrity
	computedKey := sha256.Sum256(entry.Value)
	if computedKey != key {
		return nil, errors.New("value integrity check failed")
	}

	return entry.Value, nil
}

// Iterate returns a channel of all entries in the file
func (vf *ValueFile) Iterate() <-chan ValueEntry {
	ch := make(chan ValueEntry, 100)

	go func() {
		defer close(ch)

		vf.mu.RLock()
		defer vf.mu.RUnlock()

		// Seek to data start
		if _, err := vf.file.Seek(int64(vf.header.DataOffset), 0); err != nil {
			return
		}

		for i := uint32(0); i < vf.header.EntryCount; i++ {
			entry, err := vf.readEntry()
			if err != nil {
				return
			}
			ch <- *entry
		}
	}()

	return ch
}

// IterateTimeRange returns entries within a time range
func (vf *ValueFile) IterateTimeRange(start, end int64) <-chan ValueEntry {
	ch := make(chan ValueEntry, 100)

	go func() {
		defer close(ch)

		vf.mu.RLock()
		defer vf.mu.RUnlock()

		// Use time index for efficient range query
		for _, te := range vf.index.timeIndex {
			if te.Timestamp < start {
				continue
			}
			if te.Timestamp > end {
				break
			}

			// Seek and read entry
			if _, err := vf.file.Seek(int64(te.Offset), 0); err != nil {
				return
			}

			entry, err := vf.readEntry()
			if err != nil {
				return
			}

			ch <- *entry
		}
	}()

	return ch
}

// Close finalizes and closes the value file
func (vf *ValueFile) Close() error {
	vf.mu.Lock()
	defer vf.mu.Unlock()

	if !vf.readonly {
		// Write index to file
		if err := vf.writeIndex(); err != nil {
			return err
		}

		// Update and write final header
		if err := vf.writeHeader(); err != nil {
			return err
		}
	}

	return vf.file.Close()
}

// writeEntry writes a single entry to the file
func (vf *ValueFile) writeEntry(entry *ValueEntry) error {
	// Write timestamp
	if err := binary.Write(vf.file, binary.BigEndian, entry.Timestamp); err != nil {
		return err
	}

	// Write length
	if err := binary.Write(vf.file, binary.BigEndian, entry.Length); err != nil {
		return err
	}

	// Write key
	if _, err := vf.file.Write(entry.Key[:]); err != nil {
		return err
	}

	// Write value
	if _, err := vf.file.Write(entry.Value); err != nil {
		return err
	}

	return nil
}

// readEntry reads a single entry from the file
func (vf *ValueFile) readEntry() (*ValueEntry, error) {
	entry := &ValueEntry{}

	// Read timestamp
	if err := binary.Read(vf.file, binary.BigEndian, &entry.Timestamp); err != nil {
		return nil, err
	}

	// Read length
	if err := binary.Read(vf.file, binary.BigEndian, &entry.Length); err != nil {
		return nil, err
	}

	// Read key
	if _, err := vf.file.Read(entry.Key[:]); err != nil {
		return nil, err
	}

	// Read value
	entry.Value = make([]byte, entry.Length)
	if _, err := vf.file.Read(entry.Value); err != nil {
		return nil, err
	}

	return entry, nil
}

// writeHeader writes the file header
func (vf *ValueFile) writeHeader() error {
	// Seek to beginning
	if _, err := vf.file.Seek(0, 0); err != nil {
		return err
	}

	// Write magic bytes
	if _, err := vf.file.Write(vf.header.Magic[:]); err != nil {
		return err
	}

	// Write version
	if err := binary.Write(vf.file, binary.BigEndian, vf.header.Version); err != nil {
		return err
	}

	// Write timestamps
	if err := binary.Write(vf.file, binary.BigEndian, vf.header.StartTime); err != nil {
		return err
	}
	if err := binary.Write(vf.file, binary.BigEndian, vf.header.EndTime); err != nil {
		return err
	}

	// Write counts and offsets
	if err := binary.Write(vf.file, binary.BigEndian, vf.header.EntryCount); err != nil {
		return err
	}
	if err := binary.Write(vf.file, binary.BigEndian, vf.header.DataOffset); err != nil {
		return err
	}
	if err := binary.Write(vf.file, binary.BigEndian, vf.header.IndexOffset); err != nil {
		return err
	}

	// Write checksum
	if _, err := vf.file.Write(vf.header.Checksum[:]); err != nil {
		return err
	}

	return nil
}

// readHeader reads and validates the file header
func (vf *ValueFile) readHeader() error {
	// Seek to beginning
	if _, err := vf.file.Seek(0, 0); err != nil {
		return err
	}

	// Read magic bytes
	if _, err := vf.file.Read(vf.header.Magic[:]); err != nil {
		return err
	}

	// Validate magic
	if string(vf.header.Magic[:]) != MagicBytes {
		return ErrInvalidMagic
	}

	// Read version
	if err := binary.Read(vf.file, binary.BigEndian, &vf.header.Version); err != nil {
		return err
	}

	// Check version compatibility
	if vf.header.Version != Version {
		return ErrVersionMismatch
	}

	// Read timestamps
	if err := binary.Read(vf.file, binary.BigEndian, &vf.header.StartTime); err != nil {
		return err
	}
	if err := binary.Read(vf.file, binary.BigEndian, &vf.header.EndTime); err != nil {
		return err
	}

	// Read counts and offsets
	if err := binary.Read(vf.file, binary.BigEndian, &vf.header.EntryCount); err != nil {
		return err
	}
	if err := binary.Read(vf.file, binary.BigEndian, &vf.header.DataOffset); err != nil {
		return err
	}
	if err := binary.Read(vf.file, binary.BigEndian, &vf.header.IndexOffset); err != nil {
		return err
	}

	// Read checksum
	if _, err := vf.file.Read(vf.header.Checksum[:]); err != nil {
		return err
	}

	return nil
}

// writeIndex writes the index section to the file
func (vf *ValueFile) writeIndex() error {
	// Get current position (will be index offset)
	offset, err := vf.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	vf.header.IndexOffset = uint64(offset)

	// Write number of index entries
	if err := binary.Write(vf.file, binary.BigEndian, uint32(len(vf.index.keyOffsets))); err != nil {
		return err
	}

	// Write key-offset pairs
	for key, offset := range vf.index.keyOffsets {
		if _, err := vf.file.Write(key[:]); err != nil {
			return err
		}
		if err := binary.Write(vf.file, binary.BigEndian, offset); err != nil {
			return err
		}
	}

	// Write time index
	if err := binary.Write(vf.file, binary.BigEndian, uint32(len(vf.index.timeIndex))); err != nil {
		return err
	}

	for _, te := range vf.index.timeIndex {
		if err := binary.Write(vf.file, binary.BigEndian, te.Timestamp); err != nil {
			return err
		}
		if err := binary.Write(vf.file, binary.BigEndian, te.Offset); err != nil {
			return err
		}
		if _, err := vf.file.Write(te.Key[:]); err != nil {
			return err
		}
	}

	return nil
}

// loadIndex reads the index from the file
func (vf *ValueFile) loadIndex() error {
	// Seek to index offset
	if _, err := vf.file.Seek(int64(vf.header.IndexOffset), 0); err != nil {
		return err
	}

	vf.index = &ValueIndex{
		keyOffsets: make(map[[32]byte]uint64),
		timeIndex:  make([]TimeEntry, 0),
	}

	// Read number of key-offset entries
	var keyCount uint32
	if err := binary.Read(vf.file, binary.BigEndian, &keyCount); err != nil {
		return err
	}

	// Read key-offset pairs
	for i := uint32(0); i < keyCount; i++ {
		var key [32]byte
		var offset uint64

		if _, err := vf.file.Read(key[:]); err != nil {
			return err
		}
		if err := binary.Read(vf.file, binary.BigEndian, &offset); err != nil {
			return err
		}

		vf.index.keyOffsets[key] = offset
	}

	// Read time index count
	var timeCount uint32
	if err := binary.Read(vf.file, binary.BigEndian, &timeCount); err != nil {
		return err
	}

	// Read time index entries
	for i := uint32(0); i < timeCount; i++ {
		te := TimeEntry{}

		if err := binary.Read(vf.file, binary.BigEndian, &te.Timestamp); err != nil {
			return err
		}
		if err := binary.Read(vf.file, binary.BigEndian, &te.Offset); err != nil {
			return err
		}
		if _, err := vf.file.Read(te.Key[:]); err != nil {
			return err
		}

		vf.index.timeIndex = append(vf.index.timeIndex, te)
	}

	return nil
}

// Stats returns statistics about the value file
func (vf *ValueFile) Stats() ValueFileStats {
	vf.mu.RLock()
	defer vf.mu.RUnlock()

	info, _ := vf.file.Stat()

	return ValueFileStats{
		Filename:   vf.filename,
		FileSize:   info.Size(),
		EntryCount: vf.header.EntryCount,
		StartTime:  time.Unix(vf.header.StartTime, 0),
		EndTime:    time.Unix(vf.header.EndTime, 0),
		Duration:   time.Duration(vf.header.EndTime-vf.header.StartTime) * time.Second,
	}
}

// ValueFileStats contains statistics about a value file
type ValueFileStats struct {
	Filename   string
	FileSize   int64
	EntryCount uint32
	StartTime  time.Time
	EndTime    time.Time
	Duration   time.Duration
}

// Verify checks the integrity of all entries in the file
func (vf *ValueFile) Verify() error {
	vf.mu.RLock()
	defer vf.mu.RUnlock()

	hasher := sha256.New()
	errorCount := 0

	// Verify each entry
	for entry := range vf.Iterate() {
		// Check that key matches hash of value
		computedKey := sha256.Sum256(entry.Value)
		if computedKey != entry.Key {
			errorCount++
			fmt.Printf("Integrity error at timestamp %d: key mismatch\n", entry.Timestamp)
		}

		// Add to running hash
		hasher.Write(entry.Key[:])
		hasher.Write(entry.Value)
	}

	if errorCount > 0 {
		return fmt.Errorf("found %d integrity errors", errorCount)
	}

	return nil
}