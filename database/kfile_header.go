package blockchainDB

import "encoding/binary"

const IndexOffsets = 0 // byte index to a 16 bit int used to define keysets in a KFile
const IndexShards = 4  // byte index to a 16 bit int used to define a shard for a key

// Offset table for all the indexes in the KFile
// EndOfList is necessary because when we close the KFile, the list
// of keys will often be smaller.  So the file will have stuff past
// the key list we need to ignore.  This way, we have an offset to
// the end of valid keys.
type Header struct {
	Height     int64    // Height of the kFile
	OffsetsCnt uint32   // Number of bins in the Offset Table
	HeaderSize uint32   // Length of the header
	Offsets    []uint64 // List of offsets
	EndOfList  uint64   // Offset marking end of the last Key section
}

// OffsetIndex
// Returns the index of into the header for the given key
func (h *Header) OffsetIndex(key []byte) int {
	return int(binary.BigEndian.Uint32(key[IndexOffsets:]) % h.OffsetsCnt)
}

// ShardIndex
// Returns the index of into the header for the given key
func ShardIndex(key []byte) int {
	return int(binary.BigEndian.Uint32(key[IndexShards:]) % NumShards)
}

// Marshal
// Convert the Header to bytes
func (h *Header) Marshal() []byte {
	buffer := make([]byte, h.HeaderSize)
	offset := 0
	binary.BigEndian.PutUint64(buffer[offset:], uint64(h.Height))
	offset += 4
	binary.BigEndian.PutUint32(buffer[offset:], h.OffsetsCnt)
	offset += 4
	binary.BigEndian.PutUint32(buffer[offset:], h.HeaderSize)
	offset += 4
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
	h.Height = int64(binary.BigEndian.Uint64(data))
	data = data[4:]
	h.OffsetsCnt = binary.BigEndian.Uint32(data)
	data = data[4:]
	h.HeaderSize = binary.BigEndian.Uint32(data)
	data = data[4:]
	for i := range h.Offsets {
		h.Offsets[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	h.EndOfList = uint64(binary.BigEndian.Uint64(data[h.HeaderSize-8:]))
}

// Init
// Initiate a header to its default value
// for an empty BFile
func (h *Header) Init(Height int, OffsetsCnt int) *Header {
	h.Height = int64(Height)
	h.OffsetsCnt = uint32(OffsetsCnt)
	h.HeaderSize = uint32(OffsetsCnt)*8 + 8 + 4 + 4 + 8
	h.Offsets = make([]uint64, OffsetsCnt)
	for i := range h.Offsets {
		h.Offsets[i] = uint64(h.HeaderSize)
	}
	h.EndOfList = uint64(h.HeaderSize)
	return h
}
