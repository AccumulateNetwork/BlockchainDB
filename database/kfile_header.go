package blockchainDB

import "encoding/binary"

const NumOffsets = 8
const HeaderSize = NumOffsets * 8 // Offset to values + 1024 offsets to keys

// Offset table for all the indexes in the KFile
type Header struct {
	Offsets [NumOffsets]uint64 //
}

// Index
// Returns the index of into the header for the given key
func (h *Header) Index(key [32]byte) uint16 {
	return binary.BigEndian.Uint16(key[:]) % NumOffsets
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
	return buffer[:]
}

// Unmarshal
// Convert bytes to a header
func (h *Header) Unmarshal(data []byte) {
	for i := range h.Offsets {
		h.Offsets[i] = binary.BigEndian.Uint64(data[i*8:])
	}
}

// NewHeader
// Create a new Header with all the initial offsets and entries created
// for an empty BFile
func NewHeader() Header {
	h := new(Header) // If other fields are added to the header, this
	return *h        // function is where that will be done.
}
