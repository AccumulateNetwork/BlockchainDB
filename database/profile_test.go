package blockchainDB

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

// TestBuildBigFor3Minutes runs a modified version of TestBuildBig that only runs for 3 minutes
func TestBuildBigFor3Minutes(t *testing.T) {
	// Create memory profile file
	f, err := os.Create("memprofile.out")
	if err != nil {
		t.Fatal("could not create memory profile: ", err)
	}
	defer f.Close()

	// Setup similar to TestBuildBig
	dir := "/tmp/BigDB"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0777)

	// Create a KVShard
	kvShard, err := NewKVShard(dir, 1024, 1024*10, 100) // offsetsCnt, keyLimit, MaxCachedBlocks
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
		err = kvShard.Put(key, []byte(value))
		if err != nil {
			t.Fatal(err)
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
