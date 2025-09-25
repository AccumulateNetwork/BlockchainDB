package blockchainDB

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkHistoryFileRead(b *testing.B) {
	directory := "/tmp/HistoryBench"
	os.RemoveAll(directory)

	const numKeys = 1_000_000

	fr := NewFastRandom([]byte{1, 2})
	hf, err := NewHistoryFile(2000, directory)
	assert.NoError(b, err, "failed to create directory")

	// Generate and store keys
	allKeys := make([][32]byte, numKeys)
	keyList := make([]DBBKeyFull, numKeys)

	// Generate all keys
	for i := range allKeys {
		allKeys[i] = fr.NextHash()
	}

	// Simply use the keys in order
	for i := 0; i < numKeys; i++ {
		keyList[i].Key = allKeys[i]
		keyList[i].Length = uint64(0x1111 * (i + 1))
		keyList[i].Offset = uint64(0x1010 * (i + 1))
	}

	// Pack keys into buffer
	buff := make([]byte, DBKeyFullSize*numKeys)
	offset := 0
	for _, DBFull := range keyList {
		copy(buff[offset:], DBFull.DBBKey.Bytes(DBFull.Key))
		offset += DBKeyFullSize
	}

	// Add keys
	err = hf.AddKeys(buff)
	assert.NoError(b, err, "AddKeys failed")

	// Sort keys for binary search
	fmt.Println("Sorting KeySets...")
	sortStart := time.Now()
	err = hf.SortAllKeySets()
	assert.NoError(b, err, "SortAllKeySets failed")
	fmt.Printf("Sort completed in %v\n", time.Since(sortStart))

	// Reset for reading
	fr.Reset()

	// Benchmark reads
	b.ResetTimer()

	var dbFull DBBKeyFull
	for i := 0; i < b.N; i++ {
		k := fr.NextHash()
		dbFull.Key = k
		_, err := hf.Get(dbFull.Key)
		if err != nil {
			// Key not found is OK in benchmark
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/sec")
}