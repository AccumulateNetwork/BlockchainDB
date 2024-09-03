package blockchainDB

import "encoding/binary"

const NumOffsets = 8
const HeaderSize = 8+NumOffsets * 8 // Offset to values + 1024 offsets to keys

type Header struct {
	EOD     uint64             // End of the values written, where keys begin
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
	binary.BigEndian.PutUint64(buffer[:],h.EOD)
	for _, v := range h.Offsets {
		offset += 8
		binary.BigEndian.PutUint64(buffer[offset:], v)
	}
	return buffer[:]
}

// Unmarshal
// Convert bytes to a header
func (h *Header) Unmarshal(data []byte) {
	h.EOD = binary.BigEndian.Uint64(data[:])
	for i := range h.Offsets {
		h.Offsets[i] = binary.BigEndian.Uint64(data[8+i*8:])
	}
}

// NewHeader
// Create a new Header with all the initial offsets and entries created
// for an empty BFile
func NewHeader() Header {
	h := new(Header)
	h.EOD = HeaderSize
	for i := range h.Offsets {
		h.Offsets[i] = HeaderSize
	}
	return *h
}
