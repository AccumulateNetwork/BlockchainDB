package blockchainDB

import "encoding/binary"

type Bloom struct {
	SizeOfMap float64
	NumBytes  uint64
	Map       []byte
	K         int    // Number of hash functions
	Capacity  uint64 // Number of keys the filter is sized for
	Count     uint64 // Number of keys added (duplicates counted; corrected on rebuild)
}

// BloomBitsPerKey
// Bits per key the filter is sized with.  At K=3 hash functions,
// 12 bits/key gives roughly a 1% false positive rate at capacity:
// p = (1 - e^(-3/12))^3 ~ 0.011
const BloomBitsPerKey = 12

// minBloomBytes keeps tiny filters from thrashing on rebuilds (4KB ~ 2,700 keys)
const minBloomBytes = 4096

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
	bloom.Capacity = bloom.NumBytes * 8 / BloomBitsPerKey
	return bloom
}

// NewBloomSizedForKeys
// Create a Bloom Filter sized for the given number of keys with k hash
// functions.  Sizing is BloomBitsPerKey bits per key (~1% false
// positives at capacity with k=3); past capacity the false positive
// rate degrades gracefully.
func NewBloomSizedForKeys(nKeys uint64, k int) *Bloom {
	return newBloomSized(nKeys, BloomBitsPerKey, k)
}

// newBloomSized
// Sized constructor with explicit bits per key (BloomSet uses more
// bits per key on later layers to bound the aggregate false positive
// rate).
func newBloomSized(nKeys, bitsPerKey uint64, k int) *Bloom {
	if k < 1 {
		panic("bloom filters must have at least one hash function")
	}
	bloom := new(Bloom)
	bloom.NumBytes = nKeys * bitsPerKey / 8
	if bloom.NumBytes < minBloomBytes {
		bloom.NumBytes = minBloomBytes
	}
	bloom.SizeOfMap = float64(bloom.NumBytes) / (1024 * 1024)
	bloom.Map = make([]byte, bloom.NumBytes)
	bloom.K = k
	bloom.Capacity = bloom.NumBytes * 8 / bitsPerKey
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
	b.Count++
	// Set bits for all hash functions
	for i := 0; i < b.K; i++ {
		index, bitMask := b.ByteMask(key, i)
		b.Map[index] = b.Map[index] | bitMask
	}
}
