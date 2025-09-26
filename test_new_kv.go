package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Simplified KV implementation to demonstrate the concept works
type SimpleKV struct {
	bins      []*HybridBin
	binCount  int
	stats     Stats
}

type HybridBin struct {
	sorted    []KeyEntry  // Sorted entries
	unsorted  []KeyEntry  // Recent unsorted entries
	memIndex  map[[32]byte]uint64
	mu        sync.RWMutex
}

type KeyEntry struct {
	Key    [32]byte
	Offset uint64
}

type Stats struct {
	writes atomic.Int64
	reads  atomic.Int64
	sorts  atomic.Int64
}

func NewSimpleKV(binCount int) *SimpleKV {
	kv := &SimpleKV{
		binCount: binCount,
		bins:     make([]*HybridBin, binCount),
	}

	for i := 0; i < binCount; i++ {
		kv.bins[i] = &HybridBin{
			memIndex: make(map[[32]byte]uint64),
		}
	}

	return kv
}

func (kv *SimpleKV) Put(key [32]byte, offset uint64) {
	kv.stats.writes.Add(1)

	binIdx := int(binary.BigEndian.Uint32(key[:4])) % kv.binCount
	bin := kv.bins[binIdx]

	bin.mu.Lock()
	defer bin.mu.Unlock()

	// Add to unsorted and memory index
	bin.unsorted = append(bin.unsorted, KeyEntry{key, offset})
	bin.memIndex[key] = offset

	// Sort if too many unsorted
	if len(bin.unsorted) >= 1000 {
		kv.sortBin(bin)
	}
}

func (kv *SimpleKV) Get(key [32]byte) (uint64, bool) {
	kv.stats.reads.Add(1)

	binIdx := int(binary.BigEndian.Uint32(key[:4])) % kv.binCount
	bin := kv.bins[binIdx]

	bin.mu.RLock()
	defer bin.mu.RUnlock()

	// Check memory index first (O(1))
	if offset, ok := bin.memIndex[key]; ok {
		return offset, true
	}

	// Binary search sorted section (O(log n))
	idx := sort.Search(len(bin.sorted), func(i int) bool {
		return bytes.Compare(bin.sorted[i].Key[:], key[:]) >= 0
	})

	if idx < len(bin.sorted) && bin.sorted[idx].Key == key {
		return bin.sorted[idx].Offset, true
	}

	return 0, false
}

func (kv *SimpleKV) sortBin(bin *HybridBin) {
	kv.stats.sorts.Add(1)

	// Merge sorted and unsorted
	all := append(bin.sorted, bin.unsorted...)

	// Sort all
	sort.Slice(all, func(i, j int) bool {
		return bytes.Compare(all[i].Key[:], all[j].Key[:]) < 0
	})

	// Replace
	bin.sorted = all
	bin.unsorted = nil
}

func (kv *SimpleKV) PrintStats() {
	fmt.Printf("Stats: Writes=%d, Reads=%d, Sorts=%d\n",
		kv.stats.writes.Load(),
		kv.stats.reads.Load(),
		kv.stats.sorts.Load())
}

func main() {
	fmt.Println("Testing new KV design with hybrid sorted/unsorted approach")
	fmt.Println("============================================================")

	kv := NewSimpleKV(256) // 256 bins

	// Generate test data
	numKeys := 100_000
	keys := make([][32]byte, numKeys)

	fmt.Printf("\n1. Writing %d keys...\n", numKeys)
	start := time.Now()

	for i := 0; i < numKeys; i++ {
		data := []byte(fmt.Sprintf("key-%d", i))
		keys[i] = sha256.Sum256(data)
		kv.Put(keys[i], uint64(i*100))

		if (i+1) % 10000 == 0 {
			fmt.Printf("   Written %d keys\n", i+1)
		}
	}

	writeTime := time.Since(start)
	writesPerSec := float64(numKeys) / writeTime.Seconds()
	fmt.Printf("   Write time: %v (%.0f keys/sec)\n", writeTime, writesPerSec)

	// Read all keys
	fmt.Printf("\n2. Reading %d keys...\n", numKeys)
	start = time.Now()
	found := 0

	for i := 0; i < numKeys; i++ {
		if offset, ok := kv.Get(keys[i]); ok {
			found++
			if offset != uint64(i*100) {
				fmt.Printf("   ERROR: Wrong offset for key %d\n", i)
			}
		}

		if (i+1) % 10000 == 0 {
			fmt.Printf("   Read %d keys\n", i+1)
		}
	}

	readTime := time.Since(start)
	readsPerSec := float64(numKeys) / readTime.Seconds()
	fmt.Printf("   Read time: %v (%.0f keys/sec)\n", readTime, readsPerSec)
	fmt.Printf("   Found: %d/%d\n", found, numKeys)

	// Print final stats
	fmt.Printf("\n3. Final Statistics:\n")
	kv.PrintStats()

	// Performance analysis
	fmt.Printf("\n4. Performance Analysis:\n")
	fmt.Printf("   Write Performance: %.2f M keys/sec\n", writesPerSec/1_000_000)
	fmt.Printf("   Read Performance:  %.2f M keys/sec\n", readsPerSec/1_000_000)
	fmt.Printf("   Background Sorts:  %d\n", kv.stats.sorts.Load())

	if writesPerSec > 1_000_000 {
		fmt.Println("\n✅ SUCCESS: Achieved > 1M writes/sec!")
	}
	if readsPerSec > 500_000 {
		fmt.Println("✅ SUCCESS: Achieved > 500K reads/sec!")
	}
}