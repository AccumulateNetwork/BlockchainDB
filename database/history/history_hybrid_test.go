package history

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestHybridHistoryFile demonstrates the hybrid sorted/unsorted approach
func TestHybridHistoryFile(t *testing.T) {
	fmt.Println("=== HYBRID HISTORY FILE TEST ===")
	fmt.Println("Combines fast writes (unsorted in memory) with fast reads (sorted on disk)")
	fmt.Println("Background task automatically sorts when threshold reached")
	fmt.Println()

	const (
		totalKeys = 500_000
		batchSize = 10_000
		numBins   = 256
	)

	dir := "/tmp/HybridHistory"
	os.RemoveAll(dir)

	hf, err := NewHistoryFileHybrid(numBins, dir)
	assert.NoError(t, err)
	defer hf.Close()

	// Phase 1: Write keys and observe background sorting
	fmt.Println("=== PHASE 1: WRITES WITH AUTOMATIC BACKGROUND SORTING ===")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Max unsorted entries per bin: %d\n", hf.maxUnsortedEntries)
	fmt.Printf("  Background sort batch size:    %d bins\n", hf.sortBatchSize)
	fmt.Printf("  Total keys to write:           %d\n", totalKeys)
	fmt.Println()

	pipeline := NewKeyPipeline(totalKeys, batchSize, 5)
	pipeline.Start()

	var allKeys [][32]byte
	writeStart := time.Now()
	lastSorts := int64(0)

	for i := 0; i < pipeline.TotalBatches; i++ {
		batch, ok := pipeline.GetBatch()
		if !ok {
			break
		}

		// Build buffer and track keys
		buffer := make([]byte, 0, len(batch.Keys)*utils.DBKeyFullSize)
		for j, key := range batch.Keys {
			allKeys = append(allKeys, key)

			var dbKey utils.DBBKeyFull
			dbKey.Key = key
			dbKey.Offset = uint64(i*batchSize+j) * 1024
			dbKey.Length = uint64(256)
			buffer = append(buffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
		}

		err := hf.AddKeys(buffer)
		assert.NoError(t, err)

		// Report progress and background sorting activity
		if (i+1)%5 == 0 {
			currentSorts := hf.totalSorts.Load()
			newSorts := currentSorts - lastSorts
			lastSorts = currentSorts

			elapsed := time.Since(writeStart)
			rate := float64((i+1)*batchSize) / elapsed.Seconds()

			fmt.Printf("Batch %2d: %s | %.0f keys/sec | Background sorts: %d\n",
				i+1, hf.Stats(), rate, newSorts)
		}
	}

	pipeline.Stop()
	writeTime := time.Since(writeStart)

	fmt.Printf("\nWrite complete: %.2fs for %d keys (%.0f keys/sec)\n",
		writeTime.Seconds(), totalKeys, float64(totalKeys)/writeTime.Seconds())
	fmt.Printf("Final state: %s\n\n", hf.Stats())

	// Let background sorter catch up
	fmt.Println("Waiting for background sorter to complete...")
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("After background sorting: %s\n\n", hf.Stats())

	// Phase 2: Read performance test
	fmt.Println("=== PHASE 2: READ PERFORMANCE TEST ===")
	fmt.Println("Reads check: 1) Memory buffer (O(1)), 2) Sorted disk (O(log n))")
	fmt.Println()

	readTests := 10_000
	readStart := time.Now()
	found := 0

	for i := 0; i < readTests; i++ {
		key := allKeys[i*50] // Sample every 50th key
		_, err := hf.Get(key)
		if err == nil {
			found++
		}

		if (i+1)%2000 == 0 {
			elapsed := time.Since(readStart)
			rate := float64(i+1) / elapsed.Seconds()
			avgUs := elapsed.Microseconds() / int64(i+1)
			fmt.Printf("  Read %5d keys: %.0f reads/sec (%d μs/read avg)\n",
				i+1, rate, avgUs)
		}
	}

	readTime := time.Since(readStart)
	fmt.Printf("\nRead performance: %.2fs for %d reads (%.0f reads/sec)\n",
		readTime.Seconds(), readTests, float64(readTests)/readTime.Seconds())
	fmt.Printf("Found: %d/%d keys\n\n", found, readTests)

	// Phase 3: Mixed workload
	fmt.Println("=== PHASE 3: MIXED WORKLOAD TEST ===")
	fmt.Println("Simultaneous reads and writes with background sorting")
	fmt.Println()

	var reads atomic.Int64
	var writes atomic.Int64
	stopMixed := make(chan struct{})

	// Reader goroutine
	go func() {
		for {
			select {
			case <-stopMixed:
				return
			default:
				key := allKeys[reads.Load()%int64(len(allKeys))]
				hf.Get(key)
				reads.Add(1)
			}
		}
	}()

	// Writer goroutine
	go func() {
		pipeline := NewKeyPipeline(10_000, 100, 2)
		pipeline.Start()
		defer pipeline.Stop()

		for {
			select {
			case <-stopMixed:
				return
			default:
				batch, ok := pipeline.GetBatch()
				if !ok {
					return
				}

				buffer := make([]byte, 0, len(batch.Keys)*utils.DBKeyFullSize)
				for _, key := range batch.Keys {
					var dbKey utils.DBBKeyFull
					dbKey.Key = key
					dbKey.Offset = uint64(writes.Load()) * 1024
					dbKey.Length = uint64(256)
					buffer = append(buffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
				}

				hf.AddKeys(buffer)
				writes.Add(int64(len(batch.Keys)))
			}
		}
	}()

	// Run mixed workload for 2 seconds
	mixedStart := time.Now()
	time.Sleep(2 * time.Second)
	close(stopMixed)
	time.Sleep(100 * time.Millisecond) // Let goroutines finish

	mixedTime := time.Since(mixedStart)
	totalReads := reads.Load()
	totalWrites := writes.Load()

	fmt.Printf("Mixed workload results (%.1fs):\n", mixedTime.Seconds())
	fmt.Printf("  Reads:  %d (%.0f reads/sec)\n", totalReads, float64(totalReads)/mixedTime.Seconds())
	fmt.Printf("  Writes: %d (%.0f writes/sec)\n", totalWrites, float64(totalWrites)/mixedTime.Seconds())
	fmt.Printf("  Final state: %s\n\n", hf.Stats())

	// Phase 4: Flush and final read test
	fmt.Println("=== PHASE 4: FLUSH ALL AND FINAL READ TEST ===")

	err = hf.FlushAll()
	assert.NoError(t, err)

	fmt.Printf("After flush: %s\n\n", hf.Stats())

	// Final read test - everything should be sorted now
	fmt.Println("Final read test (all data sorted):")
	finalReadStart := time.Now()
	found = 0

	for i := 0; i < 1000; i++ {
		key := allKeys[i*100]
		_, err := hf.Get(key)
		if err == nil {
			found++
		}
	}

	finalReadTime := time.Since(finalReadStart)
	fmt.Printf("  1000 reads in %.3fs (%.0f reads/sec, %d μs/read avg)\n",
		finalReadTime.Seconds(),
		1000/finalReadTime.Seconds(),
		finalReadTime.Microseconds()/1000)
	fmt.Printf("  Found: %d/1000 keys\n", found)
}

// TestHybridSortingThresholds tests different sorting thresholds
func TestHybridSortingThresholds(t *testing.T) {
	fmt.Println("=== HYBRID SORTING THRESHOLD ANALYSIS ===")
	fmt.Println("Tests how different unsorted thresholds affect performance")
	fmt.Println()

	thresholds := []int{100, 500, 1000, 5000}
	results := make(map[int]struct {
		writeTime time.Duration
		readTime  time.Duration
		sorts     int64
	})

	const testKeys = 100_000

	for _, threshold := range thresholds {
		fmt.Printf("\n--- Testing with threshold = %d unsorted entries ---\n", threshold)

		dir := fmt.Sprintf("/tmp/HybridThreshold%d", threshold)
		os.RemoveAll(dir)

		hf, err := NewHistoryFileHybrid(64, dir)
		assert.NoError(t, err)
		hf.maxUnsortedEntries = threshold

		// Write test
		pipeline := NewKeyPipeline(testKeys, 1000, 5)
		pipeline.Start()

		var testKeySet [][32]byte
		writeStart := time.Now()

		for i := 0; i < pipeline.TotalBatches; i++ {
			batch, ok := pipeline.GetBatch()
			if !ok {
				break
			}

			buffer := make([]byte, 0, len(batch.Keys)*utils.DBKeyFullSize)
			for j, key := range batch.Keys {
				if j == 0 { // Save first key of each batch for reading
					testKeySet = append(testKeySet, key)
				}

				var dbKey utils.DBBKeyFull
				dbKey.Key = key
				dbKey.Offset = uint64(i*1000+j) * 1024
				dbKey.Length = uint64(256)
				buffer = append(buffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
			}

			hf.AddKeys(buffer)
		}

		pipeline.Stop()
		writeTime := time.Since(writeStart)

		// Let background sorter work
		time.Sleep(200 * time.Millisecond)

		// Read test
		readStart := time.Now()
		found := 0

		for _, key := range testKeySet {
			_, err := hf.Get(key)
			if err == nil {
				found++
			}
		}

		readTime := time.Since(readStart)

		results[threshold] = struct {
			writeTime time.Duration
			readTime  time.Duration
			sorts     int64
		}{
			writeTime: writeTime,
			readTime:  readTime,
			sorts:     hf.totalSorts.Load(),
		}

		fmt.Printf("  Write: %.3fs | Read: %.3fs | Background sorts: %d\n",
			writeTime.Seconds(), readTime.Seconds(), hf.totalSorts.Load())
		fmt.Printf("  Final: %s\n", hf.Stats())

		hf.Close()
	}

	// Analysis
	fmt.Println("\n=== THRESHOLD COMPARISON ===")
	fmt.Printf("%-10s | %-12s | %-12s | %-15s | %-10s\n",
		"Threshold", "Write (ms)", "Read (ms)", "Sorts", "Total (ms)")
	fmt.Printf("-----------|--------------|--------------|-----------------|------------\n")

	for _, threshold := range thresholds {
		r := results[threshold]
		totalMs := (r.writeTime + r.readTime).Milliseconds()
		fmt.Printf("%-10d | %-12d | %-12d | %-15d | %-10d\n",
			threshold,
			r.writeTime.Milliseconds(),
			r.readTime.Milliseconds(),
			r.sorts,
			totalMs)
	}

	fmt.Println("\nInsights:")
	fmt.Println("- Smaller thresholds: More frequent sorts, but smaller sort operations")
	fmt.Println("- Larger thresholds: Fewer sorts, but each sort is more expensive")
	fmt.Println("- Sweet spot depends on read/write ratio and latency requirements")
}