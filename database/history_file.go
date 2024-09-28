package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
)

const (
	historyFilename = "history.dat"
)

// KeySet
// The starting offset and ending offset for each KeySet
// Start points to the next entry in the KeySet. End points
// to the entry after the last entry in the KeySet.
//
// If Start == End then the KeySet is empty.
type KeySet struct {
	Index int    // Not marshalled
	Start uint64 // offset to the start of KeySet
	End   uint64 // offset to the first entry after the KeySet
}

func (ks *KeySet) Marshal() []byte {
	var buff [16]byte
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
	BFile        *BFile     // Path to the History File
	KeySetOffset []*KeySet  // Offsets around key sets, in file offset order

	// Marshaled
	OffsetCnt int32     // Count of offsets to key sets
	KeySetIdx []*KeySet // Offsets around key sets, in key index order
}

// NewHistoryFile
// Creates and initializes a HistoryFile.  If one already exists, it is replaced with
// a fresh, new, empty HistoryFile
func NewHistoryFile(IndexCnt int, Directory string) (historyFile *HistoryFile, err error) {
	if IndexCnt < 0 || IndexCnt > 10240 {
		return nil, fmt.Errorf("index must be less than or equal to 10240, received %d", IndexCnt)
	}
	hf := new(HistoryFile)
	hf.Directory = Directory
	hf.Filename = filepath.Join(Directory, historyFilename)
	hf.OffsetCnt = int32(IndexCnt)
	hf.HeaderSize = 4 + 8*IndexCnt
	hf.KeySetIdx = make([]*KeySet, IndexCnt)
	hf.KeySetOffset = make([]*KeySet, IndexCnt)
	for i := 0; i < IndexCnt; i++ {
		ks := new(KeySet)
		ks.Index = i
		ks.Start = uint64(hf.HeaderSize)
		ks.End = uint64(hf.HeaderSize)
		hf.KeySetIdx[i] = ks    // Sorted by KeySet Index numbers
		hf.KeySetOffset[i] = ks // Sorted by Memory address
	}
	return hf, nil
}

// Marshal
// Only marshals the header, which is written to the front of the History File
func (hf *HistoryFile) Marshal() []byte {
	buff := make([]byte, hf.HeaderSize)
	binary.BigEndian.PutUint32(buff, uint32(hf.OffsetCnt))
	b := buff[4:]
	for i := 0; i < int(hf.OffsetCnt); i++ {
		copy(b, hf.KeySetIdx[i].Marshal())
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
		ks.Index = i
		ks.Unmarshal(data)
		hf.KeySetIdx[i] = ks
		hf.KeySetOffset[i] = ks
	}
	// Sort KeySetOffset entries by where their file offsets
	sort.Slice(hf.KeySetOffset, func(i, j int) bool {
		return hf.KeySetOffset[i].Start < hf.KeySetOffset[j].Start
	})
}

// Index
// Compute the index into the KeySets for this key
func (hf *HistoryFile) Index(key [32]byte) int {
	return int(binary.BigEndian.Uint32(key[IndexOffsets:]) % uint32(hf.OffsetCnt))
}

// AddKeys
// Take a buffer of Keys, sort them into bins, and add them to the History file
func (hf *HistoryFile) AddKeys(keyList []byte) (err error) {
	if len(keyList)%DBKeySize != 0 {
		return fmt.Errorf("keyList is the wrong length")
	}
	keyLen := len(keyList)
	start := 0
	for j := 0; j < int(hf.OffsetCnt); j++ {
		for i := 0; i < keyLen; i += DBKeySize {
			start = i
		}
	}
	return nil
}
