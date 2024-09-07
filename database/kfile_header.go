package blockchainDB

import "encoding/binary"

const NumOffsets = 8
const HeaderSize = NumOffsets*8 + 8 // Offset to key sections plus an end of keys value

// Offset table for all the indexes in the KFile
// EndOfList is necessary because when we close the KFile, the list
// of keys will often be smaller.  So the file will have stuff past
// the key list we need to ignore.  This way, we have an offset to
// the end of valid keys.
type Header struct {
	Offsets   [NumOffsets]uint64 // List of offsets
	EndOfList uint64             // Offset marking end of the last Key section
}

// Index
// Returns the index of into the header for the given key
func (h *Header) Index(key []byte) uint16 {
	return binary.BigEndian.Uint16(key) % NumOffsets
}

// Marshal
// Convert the Header to bytes
func (h *Header) Marshal() []byte {
	buffer := make([]byte, HeaderSize)
	offset := 0
	for _, v := range h.Offsets {
		binary.BigEndian.PutUint64(buffer[offset:], v)
		offset += 8
	}
	binary.BigEndian.PutUint64(buffer[offset:], h.EndOfList) // EndOfList
	return buffer[:]
}

// Unmarshal
// Convert bytes to a header
func (h *Header) Unmarshal(data []byte) {
	for i := range h.Offsets {
		h.Offsets[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	h.EndOfList = uint64(binary.BigEndian.Uint64(data[HeaderSize-8:]))
}

// Init
// Initiate a header to its default value
// for an empty BFile
func (h *Header) Init() *Header {
	for i := range h.Offsets {
		h.Offsets[i] = HeaderSize
	}
	h.EndOfList = HeaderSize
	return h
}
