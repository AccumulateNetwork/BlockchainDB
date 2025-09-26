package dkv

import (
	"hash"
	"hash/fnv"
	"math"
)

// BloomFilter implements a space-efficient probabilistic data structure
// for fast negative lookups in SSTables
type BloomFilter struct {
	bits      []byte
	size      uint32
	hashCount uint32
}

// NewBloomFilter creates a bloom filter with optimal parameters
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal bit size
	m := optimalBitSize(expectedItems, falsePositiveRate)
	// Calculate optimal number of hash functions
	k := optimalHashCount(expectedItems, m)

	return &BloomFilter{
		bits:      make([]byte, (m+7)/8),
		size:      uint32(m),
		hashCount: uint32(k),
	}
}

// NewBloomFilterFromBytes reconstructs a bloom filter from serialized bytes
func NewBloomFilterFromBytes(data []byte) *BloomFilter {
	if len(data) < 8 {
		return nil
	}

	size := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	hashCount := uint32(data[4])<<24 | uint32(data[5])<<16 | uint32(data[6])<<8 | uint32(data[7])

	return &BloomFilter{
		bits:      data[8:],
		size:      size,
		hashCount: hashCount,
	}
}

// Add inserts a key into the bloom filter
func (bf *BloomFilter) Add(key [32]byte) {
	h1, h2 := bf.hash(key[:])

	for i := uint32(0); i < bf.hashCount; i++ {
		pos := (h1 + i*h2) % bf.size
		byteIdx := pos / 8
		bitIdx := pos % 8

		if byteIdx < uint32(len(bf.bits)) {
			bf.bits[byteIdx] |= 1 << bitIdx
		}
	}
}

// MayContain checks if a key might be in the set
func (bf *BloomFilter) MayContain(key [32]byte) bool {
	h1, h2 := bf.hash(key[:])

	for i := uint32(0); i < bf.hashCount; i++ {
		pos := (h1 + i*h2) % bf.size
		byteIdx := pos / 8
		bitIdx := pos % 8

		if byteIdx >= uint32(len(bf.bits)) {
			return false
		}

		if bf.bits[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}

	return true
}

// Serialize converts the bloom filter to bytes for storage
func (bf *BloomFilter) Serialize() []byte {
	result := make([]byte, 8+len(bf.bits))

	// Store size (4 bytes)
	result[0] = byte(bf.size >> 24)
	result[1] = byte(bf.size >> 16)
	result[2] = byte(bf.size >> 8)
	result[3] = byte(bf.size)

	// Store hash count (4 bytes)
	result[4] = byte(bf.hashCount >> 24)
	result[5] = byte(bf.hashCount >> 16)
	result[6] = byte(bf.hashCount >> 8)
	result[7] = byte(bf.hashCount)

	// Store bits
	copy(result[8:], bf.bits)

	return result
}

// hash generates two hash values using FNV-1a
func (bf *BloomFilter) hash(data []byte) (uint32, uint32) {
	h := fnv.New32a()
	h.Write(data)
	h1 := h.Sum32()

	h.Reset()
	h.Write(data)
	h.Write([]byte{0xFF}) // Salt for second hash
	h2 := h.Sum32()

	return h1, h2
}

// optimalBitSize calculates the optimal number of bits
func optimalBitSize(n int, p float64) int {
	if n == 0 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01 // Default to 1% false positive rate
	}
	m := -float64(n) * math.Log(p) / (math.Log(2) * math.Log(2))
	return int(math.Ceil(m))
}

// optimalHashCount calculates the optimal number of hash functions
func optimalHashCount(n, m int) int {
	if n == 0 {
		n = 1
	}
	if m == 0 {
		m = 1
	}
	k := float64(m) / float64(n) * math.Log(2)
	return int(math.Ceil(k))
}

// MultiLevelBloomFilter combines multiple bloom filters for different levels
type MultiLevelBloomFilter struct {
	levels []*BloomFilter
}

// NewMultiLevelBloomFilter creates a multi-level bloom filter
func NewMultiLevelBloomFilter(numLevels int) *MultiLevelBloomFilter {
	return &MultiLevelBloomFilter{
		levels: make([]*BloomFilter, numLevels),
	}
}

// SetLevel sets the bloom filter for a specific level
func (mlbf *MultiLevelBloomFilter) SetLevel(level int, bf *BloomFilter) {
	if level < len(mlbf.levels) {
		mlbf.levels[level] = bf
	}
}

// MayContain checks all levels for potential key existence
func (mlbf *MultiLevelBloomFilter) MayContain(level int, key [32]byte) bool {
	if level >= len(mlbf.levels) || mlbf.levels[level] == nil {
		return true // Conservative: assume it might exist if no filter
	}
	return mlbf.levels[level].MayContain(key)
}

// CountingBloomFilter supports deletion by using counters instead of bits
type CountingBloomFilter struct {
	counters  []uint8
	size      uint32
	hashCount uint32
}

// NewCountingBloomFilter creates a counting bloom filter
func NewCountingBloomFilter(expectedItems int, falsePositiveRate float64) *CountingBloomFilter {
	m := optimalBitSize(expectedItems, falsePositiveRate)
	k := optimalHashCount(expectedItems, m)

	return &CountingBloomFilter{
		counters:  make([]uint8, m),
		size:      uint32(m),
		hashCount: uint32(k),
	}
}

// Add increments counters for the key
func (cbf *CountingBloomFilter) Add(key [32]byte) {
	h := fnv.New32a()
	h.Write(key[:])
	h1 := h.Sum32()

	h.Reset()
	h.Write(key[:])
	h.Write([]byte{0xFF})
	h2 := h.Sum32()

	for i := uint32(0); i < cbf.hashCount; i++ {
		pos := (h1 + i*h2) % cbf.size
		if cbf.counters[pos] < 255 { // Prevent overflow
			cbf.counters[pos]++
		}
	}
}

// Remove decrements counters for the key
func (cbf *CountingBloomFilter) Remove(key [32]byte) {
	h := fnv.New32a()
	h.Write(key[:])
	h1 := h.Sum32()

	h.Reset()
	h.Write(key[:])
	h.Write([]byte{0xFF})
	h2 := h.Sum32()

	for i := uint32(0); i < cbf.hashCount; i++ {
		pos := (h1 + i*h2) % cbf.size
		if cbf.counters[pos] > 0 {
			cbf.counters[pos]--
		}
	}
}

// MayContain checks if a key might be in the set
func (cbf *CountingBloomFilter) MayContain(key [32]byte) bool {
	h := fnv.New32a()
	h.Write(key[:])
	h1 := h.Sum32()

	h.Reset()
	h.Write(key[:])
	h.Write([]byte{0xFF})
	h2 := h.Sum32()

	for i := uint32(0); i < cbf.hashCount; i++ {
		pos := (h1 + i*h2) % cbf.size
		if cbf.counters[pos] == 0 {
			return false
		}
	}

	return true
}

// ScalableBloomFilter dynamically grows to maintain target false positive rate
type ScalableBloomFilter struct {
	filters           []*BloomFilter
	expectedItems     int
	itemsAdded        int
	falsePositiveRate float64
	scaleFactor       int
}

// NewScalableBloomFilter creates a scalable bloom filter
func NewScalableBloomFilter(expectedItems int, falsePositiveRate float64) *ScalableBloomFilter {
	return &ScalableBloomFilter{
		filters:           []*BloomFilter{NewBloomFilter(expectedItems, falsePositiveRate)},
		expectedItems:     expectedItems,
		falsePositiveRate: falsePositiveRate,
		scaleFactor:       2,
	}
}

// Add inserts a key, growing the filter if needed
func (sbf *ScalableBloomFilter) Add(key [32]byte) {
	sbf.itemsAdded++

	// Check if we need a new filter
	if sbf.itemsAdded > sbf.expectedItems*len(sbf.filters) {
		newSize := sbf.expectedItems * sbf.scaleFactor
		newFilter := NewBloomFilter(newSize, sbf.falsePositiveRate)
		sbf.filters = append(sbf.filters, newFilter)
	}

	// Add to the most recent filter
	sbf.filters[len(sbf.filters)-1].Add(key)
}

// MayContain checks all filters for potential key existence
func (sbf *ScalableBloomFilter) MayContain(key [32]byte) bool {
	for _, filter := range sbf.filters {
		if filter.MayContain(key) {
			return true
		}
	}
	return false
}

// XorFilter provides better space efficiency than bloom filters
// with similar performance characteristics
type XorFilter struct {
	fingerprints []uint16
	blockLength  uint32
	seed         uint32
}

// NewXorFilter creates an xor filter from a set of keys
func NewXorFilter(keys [][32]byte) *XorFilter {
	// Simplified implementation - in production would use
	// proper xor filter construction algorithm
	return &XorFilter{
		fingerprints: make([]uint16, len(keys)*125/100), // 1.25x size
		blockLength:  3,
		seed:         42,
	}
}

// Contains checks if a key is in the set
func (xf *XorFilter) Contains(key [32]byte) bool {
	// Simplified implementation
	h := fnv.New32a()
	h.Write(key[:])
	h.Write([]byte{byte(xf.seed)})
	idx := h.Sum32() % uint32(len(xf.fingerprints))

	h.Reset()
	h.Write(key[:])
	fp := uint16(h.Sum32())

	return xf.fingerprints[idx] == fp
}

// CuckooFilter provides deletion support with good performance
type CuckooFilter struct {
	buckets      [][]uint16
	bucketSize   int
	numBuckets   int
	fingerprints int
}

// NewCuckooFilter creates a cuckoo filter
func NewCuckooFilter(expectedItems int) *CuckooFilter {
	numBuckets := expectedItems / 4
	if numBuckets < 1 {
		numBuckets = 1
	}

	cf := &CuckooFilter{
		buckets:    make([][]uint16, numBuckets),
		bucketSize: 4,
		numBuckets: numBuckets,
	}

	for i := range cf.buckets {
		cf.buckets[i] = make([]uint16, 0, cf.bucketSize)
	}

	return cf
}

// Add inserts a key into the cuckoo filter
func (cf *CuckooFilter) Add(key [32]byte) bool {
	fp := cf.fingerprint(key)
	i1 := cf.hash1(key)
	i2 := cf.altIndex(i1, fp)

	// Try to insert in first bucket
	if len(cf.buckets[i1]) < cf.bucketSize {
		cf.buckets[i1] = append(cf.buckets[i1], fp)
		cf.fingerprints++
		return true
	}

	// Try to insert in second bucket
	if len(cf.buckets[i2]) < cf.bucketSize {
		cf.buckets[i2] = append(cf.buckets[i2], fp)
		cf.fingerprints++
		return true
	}

	// Need to relocate existing items (simplified - full implementation
	// would do proper cuckoo hashing with maximum relocations)
	return false
}

// Contains checks if a key might be in the filter
func (cf *CuckooFilter) Contains(key [32]byte) bool {
	fp := cf.fingerprint(key)
	i1 := cf.hash1(key)
	i2 := cf.altIndex(i1, fp)

	// Check both possible buckets
	for _, f := range cf.buckets[i1] {
		if f == fp {
			return true
		}
	}

	for _, f := range cf.buckets[i2] {
		if f == fp {
			return true
		}
	}

	return false
}

// Delete removes a key from the filter
func (cf *CuckooFilter) Delete(key [32]byte) bool {
	fp := cf.fingerprint(key)
	i1 := cf.hash1(key)
	i2 := cf.altIndex(i1, fp)

	// Try to remove from first bucket
	for j, f := range cf.buckets[i1] {
		if f == fp {
			cf.buckets[i1] = append(cf.buckets[i1][:j], cf.buckets[i1][j+1:]...)
			cf.fingerprints--
			return true
		}
	}

	// Try to remove from second bucket
	for j, f := range cf.buckets[i2] {
		if f == fp {
			cf.buckets[i2] = append(cf.buckets[i2][:j], cf.buckets[i2][j+1:]...)
			cf.fingerprints--
			return true
		}
	}

	return false
}

func (cf *CuckooFilter) fingerprint(key [32]byte) uint16 {
	h := fnv.New32a()
	h.Write(key[:])
	return uint16(h.Sum32())
}

func (cf *CuckooFilter) hash1(key [32]byte) int {
	h := fnv.New32a()
	h.Write(key[:])
	return int(h.Sum32() % uint32(cf.numBuckets))
}

func (cf *CuckooFilter) altIndex(i int, fp uint16) int {
	h := fnv.New32a()
	h.Write([]byte{byte(fp >> 8), byte(fp)})
	return (i ^ int(h.Sum32())) % cf.numBuckets
}