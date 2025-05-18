package blockchainDB

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

// TestBuildBigKV2 runs a test that directly uses KV2 without sharding
// This helps isolate memory usage with just a single KV2 instance
func TestBuildBigKV2(t *testing.T) {
	// Create memory profile file
	f, err := os.Create("kv2_memprofile.out")
	if err != nil {
		t.Fatal("could not create memory profile: ", err)
	}
	defer f.Close()

	// Setup test directory
	dir := "/tmp/BigKV2DB"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0777)

	// Create a single KV2 instance (no sharding)
	// Parameters: directory, offsetsCnt, keyLimit, MaxCachedBlocks
	kv2, err := NewKV2(dir, 1024, 1024*10, 100)
	if err != nil {
		t.Fatal(err)
	}

	// Run for 1 minute
	startTime := time.Now()
	endTime := startTime.Add(1 * time.Minute)
	count := 0

	// Loop until 1 minute has passed
	for time.Now().Before(endTime) {
		// Create a key and value
		keyStr := fmt.Sprintf("key%d", count)
		value := fmt.Sprintf("value%d", count)
		
		// Create a 32-byte key
		var key [32]byte
		// Copy the string bytes into the fixed-size array
		copy(key[:], []byte(keyStr))
		
		// Put the key-value pair
		writes, err := kv2.Put(key, []byte(value))
		if err != nil {
			t.Fatal(err)
		}
		
		// Print additional info every 100,000 entries
		if count%100000 == 0 {
			fmt.Printf("Writes since last compress: %d\n", writes)
		}
		
		count++
		
		// Print progress every 10,000 entries
		if count%10000 == 0 {
			elapsed := time.Since(startTime)
			fmt.Printf("Added %d entries in %v (%.2f entries/sec)\n", 
				count, elapsed, float64(count)/elapsed.Seconds())
		}
	}

	// Final stats
	elapsed := time.Since(startTime)
	fmt.Printf("\nFinal: Added %d entries in %v (%.2f entries/sec)\n", 
		count, elapsed, float64(count)/elapsed.Seconds())

	// Write memory profile
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal("could not write memory profile: ", err)
	}
}
