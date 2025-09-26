package history

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestReadStrategies demonstrates different read strategies for append-only storage
func TestReadStrategies(t *testing.T) {
	fmt.Println("=== READ STRATEGY COMPARISON ===")
	fmt.Println("Comparing read performance with different optimization levels")
	fmt.Println()

	const (
		totalKeys = 100_000
		batchSize = 10_000
		numBins   = 256
		readTests = 10_000
	)

	dir := "/tmp/ReadStrategies"
	os.RemoveAll(dir)

	hf, err := NewHistoryFileOptimized(numBins, dir)
	assert.NoError(t, err)
	defer hf.Close()

	// Generate and write test data
	fmt.Println("=== WRITE PHASE ===")
	pipeline := NewKeyPipeline(totalKeys, batchSize, 5)
	pipeline.Start()

	var allKeys [][32]byte
	writeStart := time.Now()

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
	}

	pipeline.Stop()
	writeTime := time.Since(writeStart)

	fmt.Printf("Wrote %d keys in %.2fs (%.0f keys/sec)\n",
		totalKeys, writeTime.Seconds(), float64(totalKeys)/writeTime.Seconds())
	fmt.Printf("Status: %s\n\n", hf.Stats())

	// Test 1: Reads BEFORE optimization (linear scan + memory cache)
	fmt.Println("=== TEST 1: READS WITHOUT OPTIMIZATION ===")
	fmt.Println("Strategy: Memory cache for recent keys + linear scan for others")

	readStart := time.Now()
	found := 0
	notFound := 0

	// Read a mix of recent (in cache) and old keys
	for i := 0; i < readTests; i++ {
		key := allKeys[i*10] // Sample every 10th key

		_, err := hf.Get(key)

		if err == nil {
			found++
		} else {
			notFound++
		}

		// Show progress
		if i > 0 && i%1000 == 0 {
			avgTime := time.Since(readStart) / time.Duration(i)
			fmt.Printf("  Read %d keys: avg %d µs/read, found %d/%d\n",
				i, avgTime.Microseconds(), found, i)
		}
	}

	unoptimizedTime := time.Since(readStart)
	fmt.Printf("\nUnoptimized reads: %.2fs for %d reads (%.0f reads/sec)\n",
		unoptimizedTime.Seconds(), readTests, float64(readTests)/unoptimizedTime.Seconds())
	fmt.Printf("Stats: %s\n\n", hf.Stats())

	// Test 2: Optimize bins (sort them)
	fmt.Println("=== OPTIMIZING FOR READS ===")
	optimizeStart := time.Now()
	err = hf.OptimizeForReads()
	assert.NoError(t, err)
	optimizeTime := time.Since(optimizeStart)
	fmt.Printf("Optimization took %.2fs\n\n", optimizeTime.Seconds())

	// Test 3: Reads AFTER optimization (binary search)
	fmt.Println("=== TEST 2: READS WITH OPTIMIZATION ===")
	fmt.Println("Strategy: Binary search on sorted bins")

	readStart = time.Now()
	found = 0
	notFound = 0

	for i := 0; i < readTests; i++ {
		key := allKeys[i*10] // Same keys as before

		_, err := hf.Get(key)

		if err == nil {
			found++
		} else {
			notFound++
		}

		// Show progress
		if i > 0 && i%1000 == 0 {
			avgTime := time.Since(readStart) / time.Duration(i)
			fmt.Printf("  Read %d keys: avg %d µs/read, found %d/%d\n",
				i, avgTime.Microseconds(), found, i)
		}
	}

	optimizedTime := time.Since(readStart)
	fmt.Printf("\nOptimized reads: %.2fs for %d reads (%.0f reads/sec)\n",
		optimizedTime.Seconds(), readTests, float64(readTests)/optimizedTime.Seconds())
	fmt.Printf("Stats: %s\n\n", hf.Stats())

	// Test 4: Mixed workload after optimization
	fmt.Println("=== TEST 3: MIXED WORKLOAD (Reads + New Writes) ===")

	mixedStart := time.Now()
	reads := 0
	writes := 0

	// Add some new keys (bins become unsorted again)
	newPipeline := NewKeyPipeline(1000, 100, 2)
	newPipeline.Start()

	for i := 0; i < 10; i++ {
		batch, ok := newPipeline.GetBatch()
		if !ok {
			break
		}

		buffer := make([]byte, 0, len(batch.Keys)*utils.DBKeyFullSize)
		for _, key := range batch.Keys {
			var dbKey utils.DBBKeyFull
			dbKey.Key = key
			dbKey.Offset = uint64(totalKeys+writes) * 1024
			dbKey.Length = uint64(256)
			buffer = append(buffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
			writes++
		}

		hf.AddKeys(buffer)

		// Do some reads between writes
		for j := 0; j < 100; j++ {
			key := allKeys[j]
			hf.Get(key)
			reads++
		}
	}

	newPipeline.Stop()
	mixedTime := time.Since(mixedStart)

	fmt.Printf("Mixed workload: %d reads + %d writes in %.2fs\n",
		reads, writes, mixedTime.Seconds())
	fmt.Printf("Stats: %s\n\n", hf.Stats())

	// Summary
	fmt.Println("=== PERFORMANCE SUMMARY ===")
	fmt.Printf("Write speed:              %.0f keys/sec\n", float64(totalKeys)/writeTime.Seconds())
	fmt.Printf("Read speed (unoptimized): %.0f reads/sec (linear scan)\n", float64(readTests)/unoptimizedTime.Seconds())
	fmt.Printf("Read speed (optimized):   %.0f reads/sec (binary search)\n", float64(readTests)/optimizedTime.Seconds())
	fmt.Printf("Optimization time:        %.2fs (one-time cost)\n", optimizeTime.Seconds())

	speedup := unoptimizedTime.Seconds() / optimizedTime.Seconds()
	fmt.Printf("\nRead speedup after optimization: %.1fx faster\n", speedup)

	// Calculate break-even point
	savedPerRead := (unoptimizedTime.Seconds() - optimizedTime.Seconds()) / float64(readTests)
	breakEvenReads := int(optimizeTime.Seconds() / savedPerRead)
	fmt.Printf("Break-even point: %d reads (optimization pays off after this many reads)\n", breakEvenReads)
}

// TestReadStrategiesProgression shows how performance changes with data size
func TestReadStrategiesProgression(t *testing.T) {
	fmt.Println("=== READ PERFORMANCE VS DATA SIZE ===")
	fmt.Println("Shows how read strategies scale with increasing data")
	fmt.Println()

	sizes := []int{1000, 10000, 100000}
	results := []struct {
		size          int
		linearTime    time.Duration
		binaryTime    time.Duration
		optimizeTime  time.Duration
	}{}

	for _, size := range sizes {
		fmt.Printf("\n--- Testing with %d keys ---\n", size)

		dir := fmt.Sprintf("/tmp/ReadScale%d", size)
		os.RemoveAll(dir)

		hf, err := NewHistoryFileOptimized(64, dir) // Fewer bins to show scaling
		assert.NoError(t, err)

		// Write test data
		pipeline := NewKeyPipeline(size, 1000, 2)
		pipeline.Start()

		var testKeys [][32]byte
		for i := 0; i < pipeline.TotalBatches; i++ {
			batch, ok := pipeline.GetBatch()
			if !ok {
				break
			}

			buffer := make([]byte, 0, len(batch.Keys)*utils.DBKeyFullSize)
			for j, key := range batch.Keys {
				if j%100 == 0 { // Sample some test keys
					testKeys = append(testKeys, key)
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

		// Test linear scan
		linearStart := time.Now()
		for _, key := range testKeys[:minInt(100, len(testKeys))] {
			hf.Get(key)
		}
		linearTime := time.Since(linearStart)

		// Optimize
		optimizeStart := time.Now()
		hf.OptimizeForReads()
		optimizeTime := time.Since(optimizeStart)

		// Test binary search
		binaryStart := time.Now()
		for _, key := range testKeys[:minInt(100, len(testKeys))] {
			hf.Get(key)
		}
		binaryTime := time.Since(binaryStart)

		results = append(results, struct {
			size          int
			linearTime    time.Duration
			binaryTime    time.Duration
			optimizeTime  time.Duration
		}{size, linearTime, binaryTime, optimizeTime})

		fmt.Printf("  Linear scan:    %.3fs for 100 reads\n", linearTime.Seconds())
		fmt.Printf("  Binary search:  %.3fs for 100 reads (%.1fx faster)\n",
			binaryTime.Seconds(), linearTime.Seconds()/binaryTime.Seconds())
		fmt.Printf("  Optimize time:  %.3fs\n", optimizeTime.Seconds())

		hf.Close()
	}

	// Show scaling
	fmt.Println("\n=== SCALING ANALYSIS ===")
	fmt.Printf("%-10s | %-15s | %-15s | %-10s\n", "Size", "Linear (ms)", "Binary (ms)", "Speedup")
	fmt.Printf("-----------|-----------------|-----------------|----------\n")

	for _, r := range results {
		linearMs := r.linearTime.Seconds() * 1000
		binaryMs := r.binaryTime.Seconds() * 1000
		speedup := linearMs / binaryMs

		fmt.Printf("%-10d | %-15.2f | %-15.2f | %.1fx\n",
			r.size, linearMs, binaryMs, speedup)
	}

	fmt.Println("\nConclusion:")
	fmt.Println("- Linear scan: O(n) - time increases linearly with data size")
	fmt.Println("- Binary search: O(log n) - time increases logarithmically")
	fmt.Println("- Speedup increases with data size (more benefit for larger datasets)")
}

// minInt is defined in performance_comparison_test.go