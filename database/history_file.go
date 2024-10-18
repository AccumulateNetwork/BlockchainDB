package blockchainDB

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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
	OffsetIndex int    // Offset Order (enables KeySet Index -> Offset Index)
	KeySetIndex int    // KeySet Order (enables Offset Index -> KeySet Index)
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

type HistoryFile struct {
	// Not marshaled
	Mutex        sync.Mutex // Stops access to History during a reorg
	Directory    string     // Path to the file
	Filename     string     // Computed; directory + filename
	HeaderSize   int        // Computed based of IndexCnt
	File         *os.File   // Path to the History File
	KeySetOffset []*KeySet  // Offsets around key sets, in file offset order
	Buffer       []byte     // A reusable buffer for updating HistoryFile
	// Marshaled
	OffsetCnt int32     // Count of offsets to key sets
	KeySets   []*KeySet // Offsets around key sets, in key index order
}

// NewHistoryFile
// Creates and initializes a HistoryFile.  If one already exists, it is replaced with
// a fresh, new, empty HistoryFile
func NewHistoryFile(OffsetCnt int, Directory string) (historyFile *HistoryFile, err error) {
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
	for i := 0; i < OffsetCnt; i++ {
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

// EOF
// Return the last offset in the HistoryFile
func (hf *HistoryFile) EOF() uint64 {
	return hf.KeySetOffset[hf.OffsetCnt-1].End
}

// Marshal
// Only marshals the header, which is written to the front of the History File
func (hf *HistoryFile) Marshal() []byte {
	buff := make([]byte, hf.HeaderSize)
	binary.BigEndian.PutUint32(buff, uint32(hf.OffsetCnt))
	b := buff[4:]
	for i := 0; i < int(hf.OffsetCnt); i++ {
		copy(b, hf.KeySets[i].Marshal())
		b = b[16:]
	}
	return buff
}

// Unmarshal
// Unmarshals the header.
func (hf *HistoryFile) Unmarshal(data []byte) {
	hf.OffsetCnt = int32(binary.BigEndian.Uint32(data))
	data = data[4:]
	for i := 0; i < int(hf.OffsetCnt); i++ {
		ks := new(KeySet)
		ks.OffsetIndex = i
		ks.Unmarshal(data)
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
	_, err = hf.File.WriteAt(hf.Marshal(), 0)
	return err
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
		keySet.OffsetIndex = i
	}
}

// UpdateKeySet
// Add the given entries to the KeySet at the given index and update
// the History File
//
// # If the new keys fit where the KeySet is, just add them to the HistoryFile
//
// If a KeySet does not fit where it is in the HistoryFile,
// Update it's start and end to where it can fit, and update the
// KeySets offsets, and the HistoryFile. Mem
func (hf *HistoryFile) UpdateKeySet(index int, keyList []byte) (err error) {

	if len(keyList) == 0 { // Ignore nil lists
		return nil
	}

	keySet := hf.KeySets[index]
	CurrentLength := keySet.End - keySet.Start
	NewLength := CurrentLength + uint64(len(keyList))

	offset := uint64(hf.HeaderSize)
	iAfter := 0
	for iAfter = 0; iAfter < int(hf.OffsetCnt); iAfter++ {
		if hf.KeySetOffset[iAfter].Start-offset >= NewLength {
			break
		}
		offset = hf.KeySetOffset[iAfter].End
	}

	// Make sure Buffer is big enough.  The Buffer only grows
	if len(hf.Buffer) < int(NewLength) {
		hf.Buffer = make([]byte, NewLength+1024*10)
	}

	// If we have to move the keySet, read in the keySet
	if _, err = hf.File.ReadAt(hf.Buffer[:CurrentLength], int64(keySet.Start)); err != nil {
		return err
	}
	// And tack on the keyList
	copy(hf.Buffer[CurrentLength:NewLength], keyList)
	if _, err = hf.File.WriteAt(hf.Buffer[:NewLength], int64(offset)); err != nil {
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
	keysLen := end - start

	if keysLen == 0 { //                     If the start is the end, the section is empty
		return nil, errors.New("not found") // TODO use buffer...
	}

	if len(hf.Buffer) < int(keysLen) {
		hf.Buffer = make([]byte, end-start+10240) // Grow the buffer by 10k if needed
	}
	keys := hf.Buffer[:keysLen]

	if _, err = hf.File.ReadAt(keys, int64(start)); err != nil { // Read the section
		return nil, err
	}

	var dbKey DBBKey                 //          Search the keys by unmarshaling each key as we search
	for len(keys) >= DBKeyFullSize { //          Search all DBBKey entries, note they are not sorted.
		if [32]byte(keys) == Key {
			if _, err := dbKey.Unmarshal(keys[:DBKeyFullSize]); err != nil {
				return nil, err
			}
			return &dbKey, nil
		}
		keys = keys[DBKeyFullSize:] //       Move to the next DBBKey
	}
	return nil, errors.New("not found")
}
