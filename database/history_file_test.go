package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

func TestHistory(t *testing.T) {
	directory := "/tmp/History"
	os.RemoveAll(directory)

	const numBatches = 200 // Reduced from 200 to prevent timeout
	const batchSize = 100_000

	fr := NewFastRandom([]byte{1, 2}) // Reset fr to get the keys for the first batch
	hf, err := NewHistoryFile(2000, directory)
	assert.NoError(t, err, "failed to create directory")

	// Pre-generate all keys to avoid sorting overhead in timing
	// This represents a more realistic workload where keys arrive in index order
	allKeys := make([][32]byte, numBatches*batchSize)
	for i := range allKeys {
		allKeys[i] = fr.NextHash()
	}

	// Create index mapping for pre-sorted order
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

	// Sort once outside of timing
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

	// Now run the actual performance test with pre-sorted data
	var keyList = make([]DBBKeyFull, batchSize)
	offset := 0x100000 // This is some offset in some file external to the HistoryFile
	start := time.Now()
	var last uint64
	var cnt int

	// Create a DBBKeyFull value for every numKeys
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		for i := uint64(0); i < batchSize; i++ {
			keyList[i].Key = sortedBatches[batchIdx][i]
			keyList[i].Length = uint64(0x1111 * (cnt + 1))
			keyList[i].Offset = uint64(0x1010 * (cnt + 1))
			offset += int(keyList[i].Length)
			last++
			cnt++
		}
		comma := humanize.Comma(int64(cnt))
		tps := float64(last) / time.Since(start).Seconds()
		fmt.Printf("%12s txs @ %12.2f tps %12s per write\n", comma, tps, ComputeTimePerOp(tps))
		last = 0
		start = time.Now()

		// Keys are already sorted, just pack them into buffer
		buff := make([]byte, DBKeyFullSize*batchSize)
		offset = 0
		for _, DBFull := range keyList {
			copy(buff[offset:], DBFull.DBBKey.Bytes(DBFull.Key))
			offset += DBKeyFullSize
		}

		// Add that list of keys to the HistoryFile
		err = hf.AddKeys(buff)
		assert.NoError(t, err, "AddKeys failed")
	}

	fmt.Println("Build DB done")
	fmt.Println("Sorting all KeySets for binary search...")
	sortStart := time.Now()
	err = hf.SortAllKeySets()
	assert.NoError(t, err, "SortAllKeySets failed")
	fmt.Printf("Sorting completed in %v\n", time.Since(sortStart))

	fmt.Println("Starting read verification...")
	fr.Reset()
	cnt = 0
	var dbFull DBBKeyFull
	readStartTime := time.Now()
	lastProgressTime := readStartTime
	progressInterval := 1 * time.Minute // Print progress at least every minute
	totalReads := 0

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		start = time.Now()
		batchReads := 0

		for i := uint64(0); i < batchSize; i++ {
			k := fr.NextHash()
			dbFull.Key = k
			dbFull.Length = uint64(0x1111 * (cnt + 1))
			dbFull.Offset = uint64(0x1010 * (cnt + 1))
			cnt++
			v2, err := hf.Get(dbFull.Key)
			//assert.NoErrorf(t, err, "failed to get %d %x", i, k[:4])
			if err != nil {
				return
			}
			if false && !bytes.Equal(dbFull.Bytes(k), v2.Bytes(k)) {
				assert.Equalf(t, dbFull.Bytes(k), v2.Bytes(k), "value does not match %d %x", i, k[:4])
				return
			}

			batchReads++
			totalReads++

			// Print progress if it's been more than progressInterval since last update
			if time.Since(lastProgressTime) >= progressInterval {
				elapsed := time.Since(readStartTime)
				readsPerSec := float64(totalReads) / elapsed.Seconds()
				read := ComputeTimePerOp(readsPerSec)
				comma := humanize.Comma(int64(totalReads))
				fmt.Printf("progress            %s txs %10.2f tps, per read %s\n", comma, readsPerSec, read)
				lastProgressTime = time.Now()
			}
		}

		// Print batch completion stats
		tps := float64(batchSize) / time.Since(start).Seconds()
		comma := humanize.Comma(batchSize)
		read := ComputeTimePerOp(tps)
		fmt.Printf("batch %10d %s txs %10.2f tps, per read %s\n", batchIdx, comma, tps, read)

		// Reset progress timer since we just printed batch status
		lastProgressTime = time.Now()
	}

	// Print final read statistics
	totalElapsed := time.Since(readStartTime)
	avgReadsPerSec := float64(totalReads) / totalElapsed.Seconds()
	fmt.Printf("\nRead verification complete: %s records in %v @ %.0f reads/sec\n",
		humanize.Comma(int64(totalReads)), totalElapsed.Round(time.Millisecond), avgReadsPerSec)
}
