package blockchainDB

import "encoding/binary"

type Bloom struct {
	SizeOfMap float64
	NumBytes  uint64
	Map       []byte
	K         int     // Number of hash functions
}

// NewBloom
// Create a Bloom Filter of the given size in MB
func NewBloom(size float64) *Bloom {
	return NewBloomFilter(size, 3) // Default to 3 hash functions
}

// NewBloomFilter
// Create a Bloom Filter of the given size in MB with k hash functions
func NewBloomFilter(size float64, k int) *Bloom {
	if size < 0 {
		panic("bloom filters cannot have a negative size")
	}
	if k < 1 {
		panic("bloom filters must have at least one hash function")
	}
	bloom := new(Bloom)
	bloom.SizeOfMap = size
	bloom.NumBytes = uint64(1024 * 1024 * bloom.SizeOfMap)
	bloom.Map = make([]byte, bloom.NumBytes)
	bloom.K = k
	return bloom
}

// ByteMask generates an index and bitmask for a specific hash function
func (b *Bloom) ByteMask(key [32]byte, hashNum int) (Index uint64, BitMask byte) {
	// Since the key is a SHA-256 hash, we can simply use different byte ranges
	// Each 8-byte segment provides enough entropy for a good hash function
	offset := (hashNum * 8) % 24 // Use different 8-byte chunks, wrapping if needed
	
	// Extract an 8-byte value from the key
	v := binary.BigEndian.Uint64(key[offset:])
	
	// Modulo to fit in our bitmap
	v = v % (b.NumBytes << 8)
	
	// Split into byte index and bit index
	Index = v >> 8
	BitIndex := v & 0xFF
	
	// Convert bit index to a bitmask
	BitMask = 1 << (BitIndex % 8)
	
	return Index, BitMask
}

// Test
// Test to see if an Address might be in the Database
// If Test returns false, the Address cannot be in the DB.
// If True, it might be, but you gotta check.
func (b *Bloom) Test(key [32]byte) bool {
	// All bits must be set for all hash functions to return true
	for i := 0; i < b.K; i++ {
		index, bitMask := b.ByteMask(key, i)
		if b.Map[index]&bitMask == 0 {
			return false
		}
	}
	return true
}

// Set
// Set a bit in the Bloom Filter, because a Key is being added to
// the DB.
func (b *Bloom) Set(key [32]byte) {
	// Set bits for all hash functions
	for i := 0; i < b.K; i++ {
		index, bitMask := b.ByteMask(key, i)
		b.Map[index] = b.Map[index] | bitMask
	}
}
