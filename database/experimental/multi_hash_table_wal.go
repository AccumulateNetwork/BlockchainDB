package blockchainDB

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// WAL operation types
const (
	OpPut        byte = 0x01
	OpDistributed byte = 0x02
	OpMergeStart  byte = 0x03
	OpMergeEnd    byte = 0x04
	OpCheckpoint  byte = 0x05
)

// WALEntry represents a single WAL entry
type WALEntry struct {
	Sequence  uint64
	Timestamp int64
	OpType    byte
	Key       [32]byte
	Value     DBBKey
}

// NewWAL creates a new write-ahead log
func NewWAL(directory string) (*WAL, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	wal := &WAL{
		directory: directory,
	}

	// Find the latest WAL file or create a new one
	if err := wal.openNextSegment(); err != nil {
		return nil, err
	}

	return wal, nil
}

// LogPut logs a Put operation
func (wal *WAL) LogPut(key [32]byte, value DBBKey) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entry := WALEntry{
		Sequence:  wal.sequence,
		Timestamp: time.Now().UnixNano(),
		OpType:    OpPut,
		Key:       key,
		Value:     value,
	}

	if err := wal.writeEntry(entry); err != nil {
		return err
	}

	wal.sequence++

	// Rotate log if needed
	if wal.currentSize() > WALSegmentSize {
		return wal.rotate()
	}

	return nil
}

// MarkDistributed marks entries as distributed to bins
func (wal *WAL) MarkDistributed() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entry := WALEntry{
		Sequence:  wal.sequence,
		Timestamp: time.Now().UnixNano(),
		OpType:    OpDistributed,
	}

	if err := wal.writeEntry(entry); err != nil {
		return err
	}

	wal.sequence++
	return nil
}

// LogMergeStart logs the start of a merge operation
func (wal *WAL) LogMergeStart(binID string) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entry := WALEntry{
		Sequence:  wal.sequence,
		Timestamp: time.Now().UnixNano(),
		OpType:    OpMergeStart,
		// Store binID in first bytes of Key
	}
	copy(entry.Key[:], []byte(binID))

	if err := wal.writeEntry(entry); err != nil {
		return err
	}

	wal.sequence++
	return nil
}

// LogMergeEnd logs the completion of a merge operation
func (wal *WAL) LogMergeEnd(binID string) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entry := WALEntry{
		Sequence:  wal.sequence,
		Timestamp: time.Now().UnixNano(),
		OpType:    OpMergeEnd,
	}
	copy(entry.Key[:], []byte(binID))

	if err := wal.writeEntry(entry); err != nil {
		return err
	}

	wal.sequence++
	return nil
}

// writeEntry writes a single entry to the WAL
func (wal *WAL) writeEntry(entry WALEntry) error {
	// Format: [Length:4][Sequence:8][Timestamp:8][OpType:1][Key:32][Value:16][Checksum:4]

	buf := make([]byte, 73) // Total size

	// Write fields
	binary.BigEndian.PutUint32(buf[0:4], 69)  // Length (excluding length field itself)
	binary.BigEndian.PutUint64(buf[4:12], entry.Sequence)
	binary.BigEndian.PutUint64(buf[12:20], uint64(entry.Timestamp))
	buf[20] = entry.OpType
	copy(buf[21:53], entry.Key[:])
	binary.BigEndian.PutUint64(buf[53:61], entry.Value.Offset)
	binary.BigEndian.PutUint64(buf[61:69], entry.Value.Length)

	// Calculate and add checksum
	checksum := crc32(buf[:69])
	binary.BigEndian.PutUint32(buf[69:73], checksum)

	// Write to file
	if _, err := wal.currentFile.Write(buf); err != nil {
		return err
	}

	// Sync for durability
	// DISABLED FOR TESTING - sync after every write kills performance
	// return wal.currentFile.Sync()
	return nil
}

// openNextSegment opens the next WAL segment file
func (wal *WAL) openNextSegment() error {
	// Find the highest numbered segment
	files, err := filepath.Glob(filepath.Join(wal.directory, "wal_*.log"))
	if err != nil {
		return err
	}

	var maxSeq uint64
	for _, file := range files {
		var seq uint64
		base := filepath.Base(file)
		if _, err := fmt.Sscanf(base, "wal_%d.log", &seq); err == nil {
			if seq > maxSeq {
				maxSeq = seq
			}
		}
	}

	// Open the next segment
	nextFile := filepath.Join(wal.directory, fmt.Sprintf("wal_%06d.log", maxSeq+1))
	file, err := os.OpenFile(nextFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	wal.currentFile = file
	return nil
}

// rotate closes current segment and opens a new one
func (wal *WAL) rotate() error {
	if wal.currentFile != nil {
		if err := wal.currentFile.Close(); err != nil {
			return err
		}
	}
	return wal.openNextSegment()
}

// currentSize returns the current size of the WAL file
func (wal *WAL) currentSize() int64 {
	if wal.currentFile == nil {
		return 0
	}
	info, err := wal.currentFile.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// Close closes the WAL
func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.currentFile != nil {
		return wal.currentFile.Close()
	}
	return nil
}

// Replay replays WAL entries for recovery
func (wal *WAL) Replay(handler func(entry WALEntry) error) error {
	files, err := filepath.Glob(filepath.Join(wal.directory, "wal_*.log"))
	if err != nil {
		return err
	}

	// Process files in order
	for _, file := range files {
		if err := wal.replayFile(file, handler); err != nil {
			return fmt.Errorf("replay %s: %v", file, err)
		}
	}

	return nil
}

// replayFile replays entries from a single WAL file
func (wal *WAL) replayFile(path string, handler func(WALEntry) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		// Read entry length
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		length := binary.BigEndian.Uint32(lenBuf)
		if length != 69 {
			return fmt.Errorf("invalid entry length: %d", length)
		}

		// Read the rest of the entry
		entryBuf := make([]byte, length+4) // Include checksum
		copy(entryBuf, lenBuf)
		if _, err := io.ReadFull(reader, entryBuf[4:]); err != nil {
			return err
		}

		// Verify checksum
		checksum := binary.BigEndian.Uint32(entryBuf[69:73])
		calculated := crc32(entryBuf[:69])
		if checksum != calculated {
			return fmt.Errorf("checksum mismatch: %x != %x", checksum, calculated)
		}

		// Parse entry
		entry := WALEntry{
			Sequence:  binary.BigEndian.Uint64(entryBuf[4:12]),
			Timestamp: int64(binary.BigEndian.Uint64(entryBuf[12:20])),
			OpType:    entryBuf[20],
		}
		copy(entry.Key[:], entryBuf[21:53])
		entry.Value.Offset = binary.BigEndian.Uint64(entryBuf[53:61])
		entry.Value.Length = binary.BigEndian.Uint64(entryBuf[61:69])

		// Process entry
		if err := handler(entry); err != nil {
			return err
		}

		// Update sequence
		if entry.Sequence >= wal.sequence {
			wal.sequence = entry.Sequence + 1
		}
	}
}

// CleanupOldSegments removes WAL segments that have been fully processed
func (wal *WAL) CleanupOldSegments(beforeSequence uint64) error {
	files, err := filepath.Glob(filepath.Join(wal.directory, "wal_*.log"))
	if err != nil {
		return err
	}

	for _, file := range files {
		// Check if all entries in this file are before the sequence
		if shouldDelete, err := wal.checkSegmentForDeletion(file, beforeSequence); err != nil {
			return err
		} else if shouldDelete {
			if err := os.Remove(file); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkSegmentForDeletion checks if a segment can be deleted
func (wal *WAL) checkSegmentForDeletion(path string, beforeSequence uint64) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()

	// Read just the headers to check sequences
	reader := bufio.NewReader(file)

	for {
		// Read entry header
		headerBuf := make([]byte, 20)
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			if err == io.EOF {
				return true, nil // All entries are old
			}
			return false, err
		}

		// Check sequence
		sequence := binary.BigEndian.Uint64(headerBuf[4:12])
		if sequence >= beforeSequence {
			return false, nil // Found a new entry
		}

		// Skip rest of entry
		if _, err := reader.Discard(53); err != nil { // Skip rest of entry
			return false, err
		}
	}
}

// Simple CRC32 implementation for checksums
func crc32(data []byte) uint32 {
	var crc uint32 = 0xFFFFFFFF

	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xEDB88320
			} else {
				crc >>= 1
			}
		}
	}

	return ^crc
}

// RecoveryState tracks recovery progress
type RecoveryState struct {
	LastDistributed uint64
	PendingEntries  []HashEntry
	InProgressMerges map[string]bool
}

// Recover replays the WAL to restore state after a crash
func (mht *MultiHashTable) recover() error {
	state := &RecoveryState{
		PendingEntries:   make([]HashEntry, 0),
		InProgressMerges: make(map[string]bool),
	}

	// Replay WAL entries
	err := mht.wal.Replay(func(entry WALEntry) error {
		switch entry.OpType {
		case OpPut:
			// Add to pending entries
			state.PendingEntries = append(state.PendingEntries, HashEntry{
				Key:   entry.Key,
				Value: entry.Value,
			})

		case OpDistributed:
			// Mark entries as distributed
			state.LastDistributed = entry.Sequence
			state.PendingEntries = state.PendingEntries[:0]

		case OpMergeStart:
			// Track in-progress merge
			binID := string(entry.Key[:])
			state.InProgressMerges[binID] = true

		case OpMergeEnd:
			// Merge completed
			binID := string(entry.Key[:])
			delete(state.InProgressMerges, binID)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Re-distribute any pending entries
	if len(state.PendingEntries) > 0 {
		mht.wg.Add(1)
		go mht.distributeEntries(state.PendingEntries)
	}

	// Clean up any incomplete merges
	for binID := range state.InProgressMerges {
		// Remove temp files from incomplete merges
		tempFiles, _ := filepath.Glob(filepath.Join(mht.directory, binID+".tmp.*"))
		for _, tempFile := range tempFiles {
			os.Remove(tempFile)
		}
	}

	// Clean up old WAL segments
	if state.LastDistributed > 0 {
		return mht.wal.CleanupOldSegments(state.LastDistributed)
	}

	return nil
}