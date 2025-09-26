package blockchainDB

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBinaryTreeDebug(t *testing.T) {
	directory := "/tmp/BinaryTreeDebug"
	os.RemoveAll(directory)

	fmt.Println("=== BINARY TREE DEBUG TEST ===")

	bts, err := NewBinaryTreeStorage(directory)
	assert.NoError(t, err)
	defer bts.Close()

	// Write just 10 keys
	fr := NewFastRandom([]byte{1, 2, 3})

	fmt.Println("Writing 10 keys...")
	for i := 0; i < 10; i++ {
		key := fr.NextHash()
		offset := uint64((i + 1) * 100)  // Start from 100 instead of 0
		length := uint64((i + 1) * 10)   // Start from 10 instead of 0

		fmt.Printf("Key %d: %x... (offset=%d, length=%d)\n", i, key[:4], offset, length)
		err := bts.AddKey(key, offset, length)
		assert.NoError(t, err)
	}

	// Force flush
	select {
	case bts.flushSignal <- struct{}{}:
	default:
	}
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("\nStats after write: %s\n", bts.Stats())

	// Try to read the same keys
	fmt.Println("\nReading back keys...")
	fr2 := NewFastRandom([]byte{1, 2, 3}) // Same seed

	for i := 0; i < 10; i++ {
		key := fr2.NextHash()
		offset, length, err := bts.Get(key)

		if err == nil {
			fmt.Printf("Key %d: FOUND (offset=%d, length=%d)\n", i, offset, length)
		} else {
			fmt.Printf("Key %d: NOT FOUND (%v)\n", i, err)
		}
	}

	// Check the leaf file directly
	fmt.Println("\nChecking leaf file contents...")
	files, _ := os.ReadDir(directory)
	for _, file := range files {
		fmt.Printf("File: %s\n", file.Name())

		// Read the file
		path := fmt.Sprintf("%s/%s", directory, file.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		// Parse header
		keyCount := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
		fmt.Printf("  Key count in header: %d\n", keyCount)
		fmt.Printf("  File size: %d bytes (header: 64, entries: %d)\n", len(data), len(data)-64)

		if keyCount > 0 && len(data) > 64 {
			// Show first key
			firstKey := data[64:96]
			fmt.Printf("  First key: %x...\n", firstKey[:4])
		}
	}
}