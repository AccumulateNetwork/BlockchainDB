package blockchainDB

import "sync"

type BFBuffer struct {
	mutex     sync.Mutex
	Buffer    *[BufferSize]byte
	lastKey   [32]byte
	lastValue []byte
	keyValues map[[32]byte][]byte
	values    []byte
}

func NewBFBuffer() *BFBuffer {
	var buffer [BufferSize]byte
	bfc := new(BFBuffer)
	bfc.Buffer = &buffer
	bfc.keyValues = map[[32]byte][]byte{}
	return bfc
}

// Put2Cache
// We cannot cache key values where the len(values)==0
// One data structure is used to hold all values in the cache to reduce
// garbage collection pressure
func (b *BFBuffer) Put2Cache(key [32]byte, value []byte) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if len(value) == 0 {
		return
	}
	last := len(b.values)
	b.keyValues[key] = value
	b.values = append(b.values, value...)
	b.lastKey = key
	b.lastValue = b.values[last:len(b.values)]
}

// GetFromCache
// Returns nil if undefined.
func (b *BFBuffer) GetFromCache(key [32]byte) (value []byte) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if key == b.lastKey {
		return b.lastValue
	}
	if value, ok := b.keyValues[key]; ok {
		return value
	}
	return nil
}

// PurgeCache
// Purges the cache of all key value pairs, preserving the last key value pair
// added to the cache
func (b *BFBuffer) PurgeCache() {
	b.mutex.Lock()

	// Get a copy of the last value because we can't be sure
	// what happens to the value if it overlaps when we try and
	// put it back into the Cache
	value := make([]byte, len(b.lastValue)) //
	copy(value, b.lastValue)                // Copy the lastValue
	b.values = b.values[:0]                 // Then truncate the current values cache

	for k := range b.keyValues { // Clear the keyValues map
		delete(b.keyValues, k)
	}
	b.mutex.Unlock()

	b.Put2Cache(b.lastKey, value) // Put the last Key Value back into the cache
}
