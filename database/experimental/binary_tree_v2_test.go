package blockchainDB

import (
	"fmt"
	"os"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

func TestBinaryTreeV2Split(t *testing.T) {
	directory := "/tmp/BinaryTreeV2Split"
	os.RemoveAll(directory)

	// Use smaller split size for testing
	const testSplitSize = 4 * 1024 // 4KB for faster testing

	fmt.Println("=== BINARY TREE V2 SPLIT TEST ===")
	fmt.Printf("Split size: %s\n", humanize.Bytes(uint64(LeafSplitSize)))
	fmt.Printf("Split depth: %d levels (creates %d leaves)\n", SplitDepth, 1<<SplitDepth)

	bts, err := NewBinaryTreeStorageV2(directory)
	assert.NoError(t, err)
	defer bts.Close()

	// Generate enough keys to trigger splits
	// Each key entry is 48 bytes, so ~1400 keys per 64KB
	const keysPerSplit = LeafSplitSize / KeyEntrySizeV2
	const totalKeys = keysPerSplit * 3 // Enough for multiple splits

	fmt.Printf("\nGenerating %s keys (expecting ~%d splits)\n",
		humanize.Comma(int64(totalKeys)), totalKeys/keysPerSplit)

	fr := NewFastRandom([]byte{1, 2, 3})
	startTime := time.Now()

	for i := 0; i < totalKeys; i++ {
		key := fr.NextHash()
		err := bts.AddKey(key, uint64((i+1)*100), uint64((i+1)*10))
		assert.NoError(t, err)

		if (i+1)%(keysPerSplit/2) == 0 {
			// Force flush to trigger splits
			select {
			case bts.flushSignal <- struct{}{}:
			default:
			}
			time.Sleep(150 * time.Millisecond) // Wait for flush

			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			fmt.Printf("Progress: %d keys, %.0f keys/sec, %s\n",
				i+1, rate, bts.Stats())
		}
	}

	// Final flush
	select {
	case bts.flushSignal <- struct{}{}:
	default:
	}
	time.Sleep(200 * time.Millisecond)

	totalTime := time.Since(startTime)
	fmt.Printf("\nWrite complete: %s keys in %.2fs = %.0f keys/sec\n",
		humanize.Comma(int64(totalKeys)), totalTime.Seconds(),
		float64(totalKeys)/totalTime.Seconds())
	fmt.Printf("Final stats: %s\n", bts.Stats())

	// Check files created
	files, _ := os.ReadDir(directory)
	fmt.Printf("\nLeaf files created: %d\n", len(files))

	// Analyze distribution
	fmt.Println("\nLeaf file sizes:")
	for _, file := range files {
		info, _ := file.Info()
		if info.Size() > HeaderSize {
			keyCount := (info.Size() - HeaderSize) / KeyEntrySizeV2
			fmt.Printf("  %s: %s (%d keys)\n",
				file.Name(), humanize.Bytes(uint64(info.Size())), keyCount)
		}
	}
}

func TestBinaryTreeV2Performance(t *testing.T) {
	directory := "/tmp/BinaryTreeV2Perf"
	os.RemoveAll(directory)

	const totalKeys = 1_000_000 // 1M keys

	fmt.Println("=== BINARY TREE V2 PERFORMANCE TEST ===")
	fmt.Printf("Total keys: %s\n", humanize.Comma(int64(totalKeys)))
	fmt.Printf("Split at: %s\n", humanize.Bytes(uint64(LeafSplitSize)))
	fmt.Printf("Split depth: %d levels\n", SplitDepth)

	bts, err := NewBinaryTreeStorageV2(directory)
	assert.NoError(t, err)
	defer bts.Close()

	fr := NewFastRandom([]byte{42})
	startTime := time.Now()

	for i := 0; i < totalKeys; i++ {
		key := fr.NextHash()
		err := bts.AddKey(key, uint64((i+1)*100), uint64((i+1)*10))
		assert.NoError(t, err)

		if (i+1)%100000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			fmt.Printf("Progress: %d keys, %.0f keys/sec, %s\n",
				i+1, rate, bts.Stats())
		}
	}

	// Final flush
	select {
	case bts.flushSignal <- struct{}{}:
	default:
	}
	time.Sleep(200 * time.Millisecond)

	totalTime := time.Since(startTime)
	fmt.Printf("\nWrite complete: %s keys in %.2fs = %.0f keys/sec\n",
		humanize.Comma(int64(totalKeys)), totalTime.Seconds(),
		float64(totalKeys)/totalTime.Seconds())

	finalStats := bts.Stats()
	fmt.Printf("Final stats: %s\n", finalStats)

	// Expected leaves calculation
	avgKeysPerLeaf := LeafSplitSize / KeyEntrySizeV2
	expectedLeaves := (totalKeys + avgKeysPerLeaf - 1) / avgKeysPerLeaf
	fmt.Printf("\nExpected leaves: ~%d (if perfectly balanced)\n", expectedLeaves)
	fmt.Printf("Actual leaves: %d\n", bts.TotalLeaves.Load())
	fmt.Printf("Splits performed: %d\n", bts.TotalSplits.Load())

	// Test some reads
	fmt.Println("\n=== READ TEST ===")
	fr2 := NewFastRandom([]byte{42}) // Same seed
	readStart := time.Now()
	found := 0

	for i := 0; i < 1000; i++ {
		key := fr2.NextHash()
		_, _, err := bts.Get(key)
		if err == nil {
			found++
		}
	}

	readTime := time.Since(readStart)
	usPerRead := readTime.Microseconds() / 1000
	fmt.Printf("Read 1000 keys in %.3fs = %.0f reads/sec (found: %d) - %d μs/read\n",
		readTime.Seconds(), 1000/readTime.Seconds(), found, usPerRead)
}