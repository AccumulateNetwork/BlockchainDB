package blockchainDB

import (
	"encoding/binary"
	"io"
	"os"
)

const ValuePad = 1024 * 64                 // Byte Padding for Values
const KeyPad = 1000 * KeyEntrySize         // Byte Padding for Key Sections
const KeySets = 2000                       // # of Key Sections
const KeyEntrySize = 32 + 8 + 8            // Size of eacy Key Entry
const DSSize = 8 * 3                       // DataSection Size
const HeaderSize = DSSize*KeySets + DSSize // Header Size in bytes

// DataSection
// Section of stuff within the BFile.  There is a DataSection for
// Values, and a DataSection for each of the KeySets
type DataSection struct {
	Start uint64 // Byte Offset in the BFile to start of the DataSection
	Used  uint64 // Byte Offset to the end of the used DataSection
	End   uint64 // Byte Offset to end of the allocated for the DataSection
}

// BFHeader
// Holds all the offsets into the BFile
type BFHeader struct {
	Values  DataSection          // The Data Section for Values
	KeySets [KeySets]DataSection // Each of the BFile Key Sections
}

// BFile2
// Key value file, where all the keys are divided into KeySections so that
// when reading a key value out of the file allows for a key to be pulled
// out of a far smaller KeySection than the entire key set would require
type BFile2 struct {
	BFHeader                  // Header of the BFile2
	Filename string           // Fully qualified filename
	File     *os.File         // File pointer to the BFile
	Buffer   [BufferSize]byte // Write Buffer for Values
}

// KeyEntry
// Entry into a DataSection used to hold key offsets
type KeyEntry struct {
	Key    [32]byte // The Key
	Offset uint64   // Offset to the value in the value DataSection
	Length uint64   // Length of the value
}

// AddUint64
// A helper function for marshalling a uint64 value into a buffer
// Take a buffer, an offset, and a value, store the value and return offset+8
func AddUint64(buff []byte, offset int, v uint64) int {
	binary.BigEndian.PutUint64(buff[offset:], v)
	return offset + 8
}

// GetUint64
// Pull a uint64 value out of a slice at a given offset
func GetUint64(buff []byte, offset int) uint64 {
	return binary.BigEndian.Uint64(buff[offset:])
}

// Marshal
// Marshal a KeySection into a slice
func (e *DataSection) Marshal(offset int, KSBytes []byte) {
	offset = AddUint64(KSBytes, offset, e.Start)
	offset = AddUint64(KSBytes, offset, e.Used)
	_ = AddUint64(KSBytes, offset, e.End)
}

// Marshal
// Marshal the header into a slice
func (h *BFHeader) Marshal(offset int, HBytes []byte) {
	h.Values.Marshal(offset, HBytes)
	for _, e := range h.KeySets {
		e.Marshal(offset, HBytes)
		offset += DSSize
	}
}

// NewBFile2
// Create a new, empty BFile2
func NewBFile2(filename string) (bf *BFile2, err error) {
	// Create a BFile2 struct and create the file
	bf = new(BFile2)
	if bf.File, err = os.Create(filename); err != nil {
		return nil, err
	}

	// Initialize all of the offsets to all the DataSections in the BFile2
	bf.Values.Start = HeaderSize
	bf.Values.Used = HeaderSize
	bf.Values.End = ValuePad + HeaderSize
	offset := bf.Values.End

	for i, _ := range bf.KeySets {
		bf.KeySets[i].Start = offset
		bf.KeySets[i].Used = offset
		bf.KeySets[i].End = offset + KeyPad
		offset += KeyPad
	}

	// Write out the header to disk, and pad out the BFile2 to disk

	bf.UpdateHeader() // Write out the header
	bf.File.Seek(HeaderSize, io.SeekStart)
	var buff [ValuePad]byte
	bf.File.Write(buff[:])
	for range bf.KeySets {
		bf.File.Write(buff[KeyPad:])
	}

	return bf, nil
}

// UpdateHeader()
// flush the current Header for the BFile2 to disk
func (b *BFile2) UpdateHeader() error {
	buff := make([]byte, HeaderSize)
	b.BFHeader.Marshal(0, buff)
	b.File.Seek(0, io.SeekStart)
	_, err := b.File.Write(buff)
	return err
}
