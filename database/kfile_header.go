package blockchainDB

import "encoding/binary"

const NumOffsets = 8
const HeaderSize = NumOffsets*8 + 8 // Offset to key sections plus an end of keys value

// Offset table for all the indexes in the KFile
type Header struct {
	Offsets   [NumOffsets]uint64 // List of offsets
	EndOfList uint64             // Always zero
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
	binary.BigEndian.PutUint64(buffer[offset:], 0) // EndOfList
	return buffer[:]
}

// Unmarshal
// Convert bytes to a header
func (h *Header) Unmarshal(data []byte) {
	for i := range h.Offsets {
		if v := binary.BigEndian.Uint64(data[i*8:]); v == 0 {
			break
		} else {
			h.Offsets[i] = v
		}
	}
}

// NewHeader
// Create a new Header with all the initial offsets and entries created
// for an empty BFile
func NewHeader() Header {
	h := new(Header) // If other fields are added to the header, this
	return *h        // function is where that will be done.
}
