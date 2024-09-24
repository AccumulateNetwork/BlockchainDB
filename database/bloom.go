package blockchainDB

import "encoding/binary"

type Bloom struct {
	SizeOfMap float64
	NumBytes  uint64
	Map       []byte
}

// NewBloom
// Create a Bloom Filter of the given size in MB
func NewBloom(size float64) *Bloom {
	if size < 0 {
		panic("bloom filters cannot have a negative size")
	}
	bloom := new(Bloom)
	bloom.SizeOfMap = size
	bloom.NumBytes = uint64(1024 * 1024 * bloom.SizeOfMap)
	bloom.Map = make([]byte, bloom.NumBytes)
	return bloom
}

func (b *Bloom) ByteMask(key [32]byte) (Index uint64, BitMask byte) {
	v := binary.BigEndian.Uint64(key[:])
	v = v % (b.NumBytes << 8)
	Index = v >> 8
	BitIndex := v & 0xFF
	switch { //                 This switch statement avoids doing a Log 2 calculation
	case BitIndex < 2: //       We could also do a shift and test; less code but slower
		BitMask = 0b_0000_0001
	case BitIndex < 4:
		BitMask = 0b_0000_0010
	case BitIndex < 8:
		BitMask = 0b_0000_0100
	case BitIndex < 16:
		BitMask = 0b_0000_1000
	case BitIndex < 32:
		BitMask = 0b_0001_0000
	case BitIndex < 64:
		BitMask = 0b_0010_0000
	case BitIndex < 128:
		BitMask = 0b_0100_0000
	default:
		BitMask = 0b_1000_0000
	}
	return Index, BitMask
}

// Test
// Test to see if an Address might be in the Database
// If Test returns false, the Address cannot be in the DB.
// If True, it might be, but you gotta check.
func (b *Bloom) Test(key [32]byte) bool {
	index, bitMask := b.ByteMask(key)
	return b.Map[index]&bitMask > 0
}

// Set
// Set a bit in the Bloom Filter, because a Key is being added to
// the DB.
func (b *Bloom) Set(key [32]byte) {
	index, bitMask := b.ByteMask(key)
	b.Map[index] = b.Map[index] | bitMask
}
