package blockchainDB

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

func TestHistoryPerformance(t *testing.T) {
	directory := "/tmp/HistoryPerf"
	os.RemoveAll(directory)

	const numBatches = 10  // Smaller test for quick verification
	const batchSize = 1_000_000

	fr := NewFastRandom([]byte{1, 2})
	hf, err := NewHistoryFile(2000, directory)
	assert.NoError(t, err, "failed to create directory")

	// Pre-generate all keys
	allKeys := make([][32]byte, numBatches*batchSize)
	for i := range allKeys {
		allKeys[i] = fr.NextHash()
	}

	// Pre-sort keys by KeySet index
	type keyIndex struct {
		key   [32]byte
		index int
		hfIdx int
	}
	keyIndices := make([]keyIndex, numBatches*batchSize)
	for i, key := range allKeys {
		keyIndices[i] = keyIndex{
			key:   key,
			index: i,
			hfIdx: hf.Index(key),
		}
	}

	// Sort by KeySet index
	sort.Slice(keyIndices, func(i, j int) bool {
		return keyIndices[i].hfIdx < keyIndices[j].hfIdx
	})

	// Group sorted keys into batches
	sortedBatches := make([][][32]byte, numBatches)
	for i := 0; i < numBatches; i++ {
		sortedBatches[i] = make([][32]byte, batchSize)
		for j := 0; j < batchSize; j++ {
			sortedBatches[i][j] = keyIndices[i*batchSize+j].key
		}
	}

	// Write performance test
	fmt.Println("=== WRITE PERFORMANCE ===")
	var keyList = make([]DBBKeyFull, batchSize)
	start := time.Now()
	var cnt int

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		batchStart := time.Now()
		for i := uint64(0); i < batchSize; i++ {
			keyList[i].Key = sortedBatches[batchIdx][i]
			keyList[i].Length = uint64(0x1111 * (cnt + 1))
			keyList[i].Offset = uint64(0x1010 * (cnt + 1))
			cnt++
		}

		// Pack keys into buffer
		buff := make([]byte, DBKeyFullSize*batchSize)
		offset := 0
		for _, DBFull := range keyList {
			copy(buff[offset:], DBFull.DBBKey.Bytes(DBFull.Key))
			offset += DBKeyFullSize
		}

		// Add keys
		err = hf.AddKeys(buff)
		assert.NoError(t, err, "AddKeys failed")

		tps := float64(batchSize) / time.Since(batchStart).Seconds()
		comma := humanize.Comma(int64(cnt))
		fmt.Printf("%12s txs @ %12.2f tps %12s per write\n", comma, tps, ComputeTimePerOp(tps))
	}

	totalWriteTime := time.Since(start)
	avgWriteTPS := float64(numBatches*batchSize) / totalWriteTime.Seconds()
	fmt.Printf("Write complete: %v total, %.0f avg TPS\n\n", totalWriteTime, avgWriteTPS)

	// Sort for binary search
	fmt.Println("=== SORTING ===")
	sortStart := time.Now()
	err = hf.SortAllKeySets()
	assert.NoError(t, err, "SortAllKeySets failed")
	fmt.Printf("Sort complete: %v\n\n", time.Since(sortStart))

	// Read performance test
	fmt.Println("=== READ PERFORMANCE ===")
	fr.Reset()
	cnt = 0
	var dbFull DBBKeyFull
	readStart := time.Now()
	found := 0
	notFound := 0

	// Test reading first 100k keys
	testKeys := 100000
	for i := 0; i < testKeys; i++ {
		k := fr.NextHash()
		dbFull.Key = k
		dbFull.Length = uint64(0x1111 * (cnt + 1))
		dbFull.Offset = uint64(0x1010 * (cnt + 1))
		cnt++

		_, err := hf.Get(dbFull.Key)
		if err == nil {
			found++
		} else {
			notFound++
		}

		// Print progress every 10k keys
		if (i+1)%10000 == 0 {
			elapsed := time.Since(readStart)
			readsPerSec := float64(i+1) / elapsed.Seconds()
			fmt.Printf("  Read %d keys in %v @ %.0f reads/sec\n", i+1, elapsed, readsPerSec)
		}
	}

	totalReadTime := time.Since(readStart)
	avgReadRate := float64(testKeys) / totalReadTime.Seconds()
	fmt.Printf("\nRead complete: %d keys in %v\n", testKeys, totalReadTime)
	fmt.Printf("  Found: %d, Not Found: %d\n", found, notFound)
	fmt.Printf("  Average: %.0f reads/sec, %.2fµs per read\n", avgReadRate, totalReadTime.Seconds()*1e6/float64(testKeys))
}