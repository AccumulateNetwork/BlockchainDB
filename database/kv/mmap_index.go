package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sort"
	"syscall"
)

// MmapIndex provides memory-mapped access to a sorted key index file.
// This enables O(log n) lookups without loading the entire index into memory.
//
// File format:
//   [header: 16 bytes]
//     - magic: 4 bytes ("MMIX")
//     - version: 4 bytes
//     - entry_count: 8 bytes
//   [entries: sorted by key]
//     - key: 32 bytes
//     - offset: 8 bytes
//     - length: 8 bytes
//   Total: 48 bytes per entry (same as KVKeyEntrySize)
type MmapIndex struct {
	path       string
	file       *os.File
	data       []byte // mmap'd data
	entryCount int64
	dataOffset int64 // where entries start (after header)
}

const (
	mmapMagic       = "MMIX"
	mmapVersion     = 1
	mmapHeaderSize  = 16
	mmapEntrySize   = 48 // 32 byte key + 8 byte offset + 8 byte length
)

// NewMmapIndex creates a new memory-mapped index file.
func NewMmapIndex(path string) (*MmapIndex, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Write header
	header := make([]byte, mmapHeaderSize)
	copy(header[0:4], mmapMagic)
	binary.BigEndian.PutUint32(header[4:8], mmapVersion)
	binary.BigEndian.PutUint64(header[8:16], 0) // entry count = 0

	if _, err := file.Write(header); err != nil {
		file.Close()
		return nil, err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return nil, err
	}

	return &MmapIndex{
		path:       path,
		file:       file,
		entryCount: 0,
		dataOffset: mmapHeaderSize,
	}, nil
}

// OpenMmapIndex opens an existing memory-mapped index file.
func OpenMmapIndex(path string) (*MmapIndex, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Get file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() < mmapHeaderSize {
		file.Close()
		return nil, errors.New("mmap index file too small")
	}

	// Memory map the file
	data, err := syscall.Mmap(int(file.Fd()), 0, int(info.Size()),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Validate header
	if string(data[0:4]) != mmapMagic {
		syscall.Munmap(data)
		file.Close()
		return nil, errors.New("invalid mmap index magic")
	}

	version := binary.BigEndian.Uint32(data[4:8])
	if version != mmapVersion {
		syscall.Munmap(data)
		file.Close()
		return nil, errors.New("unsupported mmap index version")
	}

	entryCount := binary.BigEndian.Uint64(data[8:16])

	return &MmapIndex{
		path:       path,
		file:       file,
		data:       data,
		entryCount: int64(entryCount),
		dataOffset: mmapHeaderSize,
	}, nil
}

// Get performs a binary search for a key in the mmap'd index.
// Returns the KeyLocation or an error if not found.
func (m *MmapIndex) Get(key [32]byte) (*KeyLocation, error) {
	if m.data == nil || m.entryCount == 0 {
		return nil, errors.New("key not found")
	}

	left, right := int64(0), m.entryCount-1

	for left <= right {
		mid := (left + right) / 2
		entryOffset := m.dataOffset + mid*mmapEntrySize

		var midKey [32]byte
		copy(midKey[:], m.data[entryOffset:entryOffset+32])

		cmp := bytes.Compare(midKey[:], key[:])
		if cmp == 0 {
			// Found it
			return &KeyLocation{
				Offset: binary.BigEndian.Uint64(m.data[entryOffset+32 : entryOffset+40]),
				Length: binary.BigEndian.Uint64(m.data[entryOffset+40 : entryOffset+48]),
			}, nil
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return nil, errors.New("key not found")
}

// Has checks if a key exists in the index.
func (m *MmapIndex) Has(key [32]byte) bool {
	_, err := m.Get(key)
	return err == nil
}

// EntryCount returns the number of entries in the index.
func (m *MmapIndex) EntryCount() int64 {
	return m.entryCount
}

// Build creates or rebuilds the mmap index from a slice of entries.
// Entries are sorted by key before writing.
func (m *MmapIndex) Build(entries []IndexEntry) error {
	// Sort entries by key
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key[:], entries[j].Key[:]) < 0
	})

	// Calculate new file size
	newSize := mmapHeaderSize + int64(len(entries))*mmapEntrySize

	// Unmap existing data if any
	if m.data != nil {
		if err := syscall.Munmap(m.data); err != nil {
			return err
		}
		m.data = nil
	}

	// Truncate file to new size
	if err := m.file.Truncate(newSize); err != nil {
		return err
	}

	// Seek to beginning
	if _, err := m.file.Seek(0, 0); err != nil {
		return err
	}

	// Write header
	header := make([]byte, mmapHeaderSize)
	copy(header[0:4], mmapMagic)
	binary.BigEndian.PutUint32(header[4:8], mmapVersion)
	binary.BigEndian.PutUint64(header[8:16], uint64(len(entries)))

	if _, err := m.file.Write(header); err != nil {
		return err
	}

	// Write entries
	for _, entry := range entries {
		entryData := make([]byte, mmapEntrySize)
		copy(entryData[0:32], entry.Key[:])
		binary.BigEndian.PutUint64(entryData[32:40], entry.Offset)
		binary.BigEndian.PutUint64(entryData[40:48], entry.Length)

		if _, err := m.file.Write(entryData); err != nil {
			return err
		}
	}

	if err := m.file.Sync(); err != nil {
		return err
	}

	// Re-mmap the file
	m.data, _ = syscall.Mmap(int(m.file.Fd()), 0, int(newSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	m.entryCount = int64(len(entries))

	return nil
}

// Append adds a new entry to the index.
// Note: This invalidates sorted order - call Rebuild() after batch appends.
func (m *MmapIndex) Append(key [32]byte, offset, length uint64) error {
	// Calculate new entry position
	entryOffset := mmapHeaderSize + m.entryCount*mmapEntrySize
	newSize := entryOffset + mmapEntrySize

	// Unmap if mapped
	if m.data != nil {
		if err := syscall.Munmap(m.data); err != nil {
			return err
		}
		m.data = nil
	}

	// Extend file
	if err := m.file.Truncate(newSize); err != nil {
		return err
	}

	// Write entry
	entryData := make([]byte, mmapEntrySize)
	copy(entryData[0:32], key[:])
	binary.BigEndian.PutUint64(entryData[32:40], offset)
	binary.BigEndian.PutUint64(entryData[40:48], length)

	if _, err := m.file.WriteAt(entryData, entryOffset); err != nil {
		return err
	}

	// Update entry count in header
	countData := make([]byte, 8)
	binary.BigEndian.PutUint64(countData, uint64(m.entryCount+1))
	if _, err := m.file.WriteAt(countData, 8); err != nil {
		return err
	}

	m.entryCount++

	// Re-mmap
	m.data, _ = syscall.Mmap(int(m.file.Fd()), 0, int(newSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	return nil
}

// Close closes the mmap index.
func (m *MmapIndex) Close() error {
	if m.data != nil {
		if err := syscall.Munmap(m.data); err != nil {
			return err
		}
		m.data = nil
	}
	if m.file != nil {
		return m.file.Close()
	}
	return nil
}

// IndexEntry represents a single entry in the index.
type IndexEntry struct {
	Key    [32]byte
	Offset uint64
	Length uint64
}

// ForEach iterates over all entries in the index in sorted order.
func (m *MmapIndex) ForEach(fn func(key [32]byte, offset, length uint64) error) error {
	if m.data == nil {
		return nil
	}

	for i := int64(0); i < m.entryCount; i++ {
		entryOffset := m.dataOffset + i*mmapEntrySize

		var key [32]byte
		copy(key[:], m.data[entryOffset:entryOffset+32])
		offset := binary.BigEndian.Uint64(m.data[entryOffset+32 : entryOffset+40])
		length := binary.BigEndian.Uint64(m.data[entryOffset+40 : entryOffset+48])

		if err := fn(key, offset, length); err != nil {
			return err
		}
	}

	return nil
}

// Size returns the number of bytes used by the index file.
func (m *MmapIndex) Size() int64 {
	if m.file == nil {
		return 0
	}
	info, err := m.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
