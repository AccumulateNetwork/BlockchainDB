package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
)

const (
	historyFilename = "history.dat"
	KeySetSize      = 16
)

// KeySet
// The starting offset and ending offset for each KeySet
// Start points to the next entry in the KeySet. End points
// to the entry after the last entry in the KeySet.
//
// If Start == End then the KeySet is empty.
type KeySet struct {
	OffsetIndex uint64 // Offset Order (enables KeySet Index -> Offset Index)
	KeySetIndex uint64 // KeySet Order (enables Offset Index -> KeySet Index)
	Start       uint64 // offset to the start of KeySet
	End         uint64 // offset to the first entry after the KeySet
}

func (ks *KeySet) Marshal() []byte {
	var buff [KeySetSize]byte
	binary.BigEndian.PutUint64(buff[:], ks.Start)
	binary.BigEndian.PutUint64(buff[8:], ks.End)
	return buff[:]
}

func (ks *KeySet) Unmarshal(buff []byte) {
	ks.Start = binary.BigEndian.Uint64(buff)
	ks.End = binary.BigEndian.Uint64(buff[8:])
}

// HistoryFile
// Holds Large sets of Keys, and generally provides slower access to values
// The Header are the Marshaled entries in the struct.  Following the Header
// in the History file are all the DBBKey entries.  Writes to the HistoryFile
// are not buffered.
//
// Performance Optimizations:
// - No longer maintains its own Bloom filter, reducing memory usage
// - Uses local buffers instead of shared buffers to reduce memory growth
// - Synchronous history pushing to prevent memory buildup
// - Bloom filter checking is now handled by the parent KFile

type HistoryFile struct {
	// Not marshaled
	Mutex        sync.Mutex // Stops access to History during a reorg
	Directory    string     // Path to the file
	Filename     string     // Computed; directory + filename
	HeaderSize   uint64     // Computed based of IndexCnt
	File         *os.File   // Path to the History File
	KeySetOffset []*KeySet  // Offsets around key sets, in file offset order
	// Marshaled
	OffsetCnt int32     // Count of offsets to key sets
	KeySets   []*KeySet // Offsets around key sets, in key index order
}

// NewHistoryFile
// Creates and initializes a HistoryFile.  If one already exists, it is replaced with
// a fresh, new, empty HistoryFile
func NewHistoryFile(OffsetCnt uint64, Directory string) (historyFile *HistoryFile, err error) {
	if OffsetCnt < 0 || OffsetCnt > 102400 {
		return nil, fmt.Errorf("index must be less than or equal to 10240, received %d", OffsetCnt)
	}
	hf := new(HistoryFile)
	hf.Directory = Directory
	os.Mkdir(Directory, os.ModePerm)

	hf.Filename = filepath.Join(Directory, historyFilename)
	if hf.File, err = os.Create(hf.Filename); err != nil {
		return nil, err
	}
	hf.OffsetCnt = int32(OffsetCnt)
	hf.HeaderSize = 4 + KeySetSize*OffsetCnt
	hf.KeySets = make([]*KeySet, OffsetCnt)
	hf.KeySetOffset = make([]*KeySet, OffsetCnt)

	// Bloom filter is now managed by KFile, not HistoryFile

	for i := uint64(0); i < OffsetCnt; i++ {
		ks := new(KeySet)
		ks.OffsetIndex = i               // To begin with, KeySets are in the same order by Index
		ks.KeySetIndex = i               //    and by offset
		ks.Start = uint64(hf.HeaderSize) // Fewer special cases if empty KeySets are
		ks.End = uint64(hf.HeaderSize)   //   initialized to empty at end of the header
		hf.KeySets[i] = ks               // Sorted by KeySet Index numbers
		hf.KeySetOffset[i] = ks          // Sorted by Memory address
	}
	if _, err = hf.File.WriteAt(hf.Marshal(), 0); err != nil { // Write out the header to the HistoryFile
		return nil, err
	}

	return hf, nil
}

// OpenHistoryFile
// Opens an existing HistoryFile and loads its header (the KeySet offsets).
func OpenHistoryFile(Directory string) (historyFile *HistoryFile, err error) {
	hf := new(HistoryFile)
	hf.Directory = Directory
	hf.Filename = filepath.Join(Directory, historyFilename)
	if hf.File, err = os.OpenFile(hf.Filename, os.O_RDWR, 0644); err != nil {
		return nil, err
	}

	// The first 4 bytes hold the OffsetCnt, which sizes the header
	var cntBuff [4]byte
	if _, err = hf.File.ReadAt(cntBuff[:], 0); err != nil {
		return nil, err
	}
	offsetCnt := binary.BigEndian.Uint32(cntBuff[:])
	hf.OffsetCnt = int32(offsetCnt)
	hf.HeaderSize = 4 + KeySetSize*uint64(offsetCnt)

	header := make([]byte, hf.HeaderSize)
	if _, err = hf.File.ReadAt(header, 0); err != nil {
		return nil, err
	}
	hf.Unmarshal(header)
	return hf, nil
}

// EOF
// Return the last offset in the HistoryFile
func (hf *HistoryFile) EOF() uint64 {
	return hf.KeySetOffset[hf.OffsetCnt-1].End
}

// Marshal
// Only marshals the header, which is written to the// Marshal
// Only marshals the header, which is written to the front of the History File
func (hf *HistoryFile) Marshal() []byte {
	buff := make([]byte, hf.HeaderSize)
	binary.BigEndian.PutUint32(buff, uint32(hf.OffsetCnt))
	b := buff[4:]
	for i := 0; i < int(hf.OffsetCnt); i++ {
		copy(b, hf.KeySets[i].Marshal())
		b = b[KeySetSize:]
	}
	return buff
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// min function is now used by KFile as well

// Unmarshal
// Unmarshals the header.
func (hf *HistoryFile) Unmarshal(data []byte) {
	hf.OffsetCnt = int32(binary.BigEndian.Uint32(data))
	data = data[4:]
	// Allocate the KeySet slices if needed (e.g. when opening an existing
	// HistoryFile rather than creating a new one)
	if len(hf.KeySets) != int(hf.OffsetCnt) {
		hf.KeySets = make([]*KeySet, hf.OffsetCnt)
		hf.KeySetOffset = make([]*KeySet, hf.OffsetCnt)
	}
	for i := uint64(0); i < uint64(hf.OffsetCnt); i++ {
		ks := new(KeySet)
		ks.OffsetIndex = i
		ks.KeySetIndex = i
		ks.Unmarshal(data)
		data = data[KeySetSize:] // Advance to the next KeySet entry
		hf.KeySets[i] = ks
		hf.KeySetOffset[i] = ks
	}
	hf.OffsetSort()
}

// Index
// Compute the index into the KeySets for this key
func (hf *HistoryFile) Index(key [32]byte) int {
	return int(binary.BigEndian.Uint32(key[IndexOffsets:]) % uint32(hf.OffsetCnt))
}

// AddKeys
// Take a buffer of Keys, sort them into bins, and add them to the History file.
// Assumes the keyList is already sorted into bins internally.
func (hf *HistoryFile) AddKeys(keyList []byte) (err error) {
	if len(keyList) == 0 {
		return nil // There was nothing to update
	}

	if len(keyList)%DBKeyFullSize != 0 {
		return fmt.Errorf("keyList is the wrong length")
	}

	// Bloom filter is now managed by KFile, not HistoryFile

	index := hf.Index([32]byte(keyList))
	var kIndex int
	var startOff, endOff uint64 // Start and end of entries in the same KeySet

	for keyPtr := keyList; len(keyPtr) > 0; keyPtr = keyPtr[DBKeyFullSize:] {
		kIndex = hf.Index([32]byte(keyPtr))
		switch {
		case kIndex == index: // Key is part of this KeySet (of index)
			endOff += DBKeyFullSize //Guess the end to avoid an end case
		case kIndex < index:
			return errors.New("keyList is not sorted")
		default:
			if err := hf.UpdateKeySet(index, keyList[startOff:endOff]); err != nil {
				return err
			}
			index = kIndex // The new index will be the next index (kIndex)
			startOff = endOff
			endOff += DBKeyFullSize
		}
	}
	if err = hf.UpdateKeySet(index, keyList[startOff:endOff]); err != nil {
		fmt.Printf("Update End %d to %d\n", startOff, endOff)
		return err
	}
	if _, err = hf.File.WriteAt(hf.Marshal(), 0); err != nil {
		return err
	}
	// Sync so the caller can safely discard its copy of these keys
	// (PushHistory resets the kfile once AddKeys returns)
	return hf.File.Sync()
}

// OffsetSort
// Sort the indexes by HistoryFile Offsets; Sort by the end, because
// empty keySets can have the same Start as one keySet...
func (hf *HistoryFile) OffsetSort() {
	ret := 0
	slices.SortFunc(hf.KeySetOffset, func(a, b *KeySet) int {
		switch {
		case a.End < b.End:
			ret = -1
		case a.End == b.End:
			ret = 0
		default:
			ret = 1
		}
		return ret
	})
	for i, keySet := range hf.KeySetOffset {
		keySet.OffsetIndex = uint64(i)
	}
}

// recordSort
// Sorts a buffer of 48-byte key records in place by their 32-byte key
type recordSort []byte

func (r recordSort) Len() int { return len(r) / DBKeyFullSize }
func (r recordSort) Less(i, j int) bool {
	return bytes.Compare(r[i*DBKeyFullSize:i*DBKeyFullSize+32], r[j*DBKeyFullSize:j*DBKeyFullSize+32]) < 0
}
func (r recordSort) Swap(i, j int) {
	var tmp [DBKeyFullSize]byte
	copy(tmp[:], r[i*DBKeyFullSize:])
	copy(r[i*DBKeyFullSize:(i+1)*DBKeyFullSize], r[j*DBKeyFullSize:])
	copy(r[j*DBKeyFullSize:(j+1)*DBKeyFullSize], tmp[:])
}

// mergeRecords
// Merges two key-sorted record buffers into one sorted buffer.  Records
// with equal keys are deduplicated; the record from incoming wins.
// (Duplicates arise when a crash between a history push and the kfile
// reset leaves keys in both places; the next push re-sends them.)
func mergeRecords(existing, incoming []byte) []byte {
	merged := make([]byte, 0, len(existing)+len(incoming))
	for len(existing) > 0 && len(incoming) > 0 {
		switch bytes.Compare(existing[:32], incoming[:32]) {
		case -1:
			merged = append(merged, existing[:DBKeyFullSize]...)
			existing = existing[DBKeyFullSize:]
		case 1:
			merged = append(merged, incoming[:DBKeyFullSize]...)
			incoming = incoming[DBKeyFullSize:]
		default: // Same key: the incoming record wins
			merged = append(merged, incoming[:DBKeyFullSize]...)
			existing = existing[DBKeyFullSize:]
			incoming = incoming[DBKeyFullSize:]
		}
	}
	merged = append(merged, existing...)
	merged = append(merged, incoming...)
	return merged
}

// UpdateKeySet
// Add the given entries to the KeySet at the given index and update
// the History File.
//
// The records in a KeySet are kept sorted by key (Get relies on this to
// binary search).  The incoming records are sorted, then merged with the
// KeySet's existing records; the merged KeySet is written wherever it
// first fits (which may mean relocating it).
func (hf *HistoryFile) UpdateKeySet(index int, keyList []byte) (err error) {

	if len(keyList) == 0 { // Ignore nil lists
		return nil
	}

	// Sort the incoming records by key.  Sorting is in place; callers do
	// not reuse the buffer.
	sort.Sort(recordSort(keyList))

	keySet := hf.KeySets[index]
	CurrentLength := keySet.End - keySet.Start

	// Read the existing (sorted) records and merge the new ones in
	existing := make([]byte, CurrentLength)
	if _, err = hf.File.ReadAt(existing, int64(keySet.Start)); err != nil {
		return err
	}
	merged := mergeRecords(existing, keyList)
	NewLength := uint64(len(merged))

	// First fit: find the first gap the merged KeySet fits in
	offset := uint64(hf.HeaderSize)
	iAfter := 0
	for iAfter = 0; iAfter < int(hf.OffsetCnt); iAfter++ {
		if hf.KeySetOffset[iAfter].Start-offset >= NewLength {
			break
		}
		offset = hf.KeySetOffset[iAfter].End
	}

	if _, err = hf.File.WriteAt(merged, int64(offset)); err != nil {
		return err
	}

	// Update position of keySet in the HistoryFile
	keySet.Start = offset
	keySet.End = offset + NewLength

	hf.OffsetSort() // Ensure all the offset sorting is correct.

	return nil
}

// Get
// Get the value for a given DBKeyFull.  The value returned
// is free for the user to use (i.e. not part of a buffer used
// by the BFile)
func (hf *HistoryFile) Get(Key [32]byte) (dbBKey *DBBKey, err error) {

	// The header reflects what is on disk.  Points keys to the section where it is.
	index := hf.Index(Key)
	start := hf.KeySets[index].Start // The index is where the section starts
	end := hf.KeySets[index].End
	numRecords := int64(end-start) / DBKeyFullSize

	if numRecords == 0 { //                  If the start is the end, the section is empty
		return nil, errors.New("not found")
	}

	// The records in a KeySet are sorted by key (see UpdateKeySet), so
	// binary search, reading one 48-byte record per probe rather than the
	// entire KeySet.  Once the remaining range is small, read it in one
	// gulp and finish the search in memory to save syscalls.
	const inMemRecords = 64 // 3KB; the range read in one gulp

	var record [DBKeyFullSize]byte
	lo, hi := int64(0), numRecords-1
	for hi-lo+1 > inMemRecords {
		mid := (lo + hi) / 2
		if _, err = hf.File.ReadAt(record[:], int64(start)+mid*DBKeyFullSize); err != nil {
			return nil, err
		}
		switch bytes.Compare(Key[:], record[:32]) {
		case 0:
			var dbKey DBBKey
			if _, err := dbKey.Unmarshal(record[:]); err != nil {
				return nil, err
			}
			return &dbKey, nil
		case -1:
			hi = mid - 1
		default:
			lo = mid + 1
		}
	}

	// Read the remaining range and binary search it in memory
	var gulp [inMemRecords * DBKeyFullSize]byte
	n := hi - lo + 1
	buff := gulp[:n*DBKeyFullSize]
	if _, err = hf.File.ReadAt(buff, int64(start)+lo*DBKeyFullSize); err != nil {
		return nil, err
	}
	i, j := int64(0), n-1
	for i <= j {
		mid := (i + j) / 2
		rec := buff[mid*DBKeyFullSize:]
		switch bytes.Compare(Key[:], rec[:32]) {
		case 0:
			var dbKey DBBKey
			if _, err := dbKey.Unmarshal(rec[:DBKeyFullSize]); err != nil {
				return nil, err
			}
			return &dbKey, nil
		case -1:
			j = mid - 1
		default:
			i = mid + 1
		}
	}
	return nil, errors.New("not found")
}
