package blockchainDB

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHistoryQuick(t *testing.T) {
	directory := "/tmp/HistoryQuick"
	os.RemoveAll(directory)

	const numKeys = 10000 // Small number for quick test

	fr := NewFastRandom([]byte{1, 2})
	hf, err := NewHistoryFile(100, directory) // Smaller number of KeySets
	assert.NoError(t, err, "failed to create directory")

	// Generate and add keys
	allKeys := make([][32]byte, numKeys)
	keyList := make([]DBBKeyFull, numKeys)

	for i := range allKeys {
		allKeys[i] = fr.NextHash()
		keyList[i].Key = allKeys[i]
		keyList[i].Length = uint64(0x1111)
		keyList[i].Offset = uint64(0x1010 * (i + 1))
	}

	// Group by KeySet index
	keysByIndex := make(map[int][]DBBKeyFull)
	for _, kf := range keyList {
		idx := hf.Index(kf.Key)
		keysByIndex[idx] = append(keysByIndex[idx], kf)
	}

	// Add keys by KeySet
	for idx := 0; idx < 100; idx++ {
		keys, ok := keysByIndex[idx]
		if !ok || len(keys) == 0 {
			continue
		}

		buff := make([]byte, DBKeyFullSize*len(keys))
		offset := 0
		for _, DBFull := range keys {
			copy(buff[offset:], DBFull.DBBKey.Bytes(DBFull.Key))
			offset += DBKeyFullSize
		}

		err = hf.AddKeys(buff)
		assert.NoError(t, err, "AddKeys failed")
	}

	// Sort for binary search
	fmt.Println("Sorting KeySets...")
	sortStart := time.Now()
	err = hf.SortAllKeySets()
	assert.NoError(t, err, "SortAllKeySets failed")
	fmt.Printf("Sort completed in %v\n", time.Since(sortStart))

	// Test reads
	fmt.Println("Testing reads...")
	readStart := time.Now()
	found := 0
	notFound := 0

	for _, key := range allKeys[:100] { // Test first 100 keys
		_, err := hf.Get(key)
		if err == nil {
			found++
		} else {
			notFound++
		}
	}

	readTime := time.Since(readStart)
	fmt.Printf("Read 100 keys in %v (found: %d, not found: %d)\n", readTime, found, notFound)
	fmt.Printf("Average read time: %v per key\n", readTime/100)

	// Analyze KeySet distribution
	fmt.Println("\nKeySet distribution:")
	for idx := 0; idx < 10; idx++ { // Show first 10 KeySets
		if keys, ok := keysByIndex[idx]; ok {
			fmt.Printf("  KeySet %d: %d keys\n", idx, len(keys))
		}
	}
}