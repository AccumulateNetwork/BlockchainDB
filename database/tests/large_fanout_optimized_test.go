package blockchainDB

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestLargeFanOutOptimized uses batch-based key generation to reduce memory
func TestLargeFanOutOptimized(t *testing.T) {
	// Test configurations
	fanOuts := []int{256, 1024, 2048}
	totalEntries := 100_000 // 100K entries - realistic with fsync overhead
	batchSize := 10_000      // Smaller batches for 100K total

	t.Logf("=== OPTIMIZED LARGE FAN-OUT TEST WITH %d ENTRIES ===\n", totalEntries)

	// Get initial memory stats
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)

	type TestResult struct {
		FanOut         int
		WriteTime      time.Duration
		WriteRate      float64
		SortTime       time.Duration
		ReadTime       time.Duration
		ReadRate       float64
		RandomReadTime time.Duration
		RandomReadRate float64
		MemoryUsed     uint64
		Found          int
		RandomFound    int
	}

	results := make([]TestResult, 0)

	// Test each fan-out value
	for _, fanOut := range fanOuts {
		t.Logf("\n=== Testing Fan-Out %d ===", fanOut)

		dir := fmt.Sprintf("/tmp/large_fanout_opt_%d", fanOut)
		os.RemoveAll(dir)

		// Get memory before test
		var startMem runtime.MemStats
		runtime.ReadMemStats(&startMem)

		cht, err := NewConfigurableHashTable(dir, fanOut)
		if err != nil {
			t.Fatalf("Failed to create table with fan-out %d: %v", fanOut, err)
		}

		// === OPTIMIZED WRITE TEST ===
		t.Logf("Writing %d entries in batches of %d...", totalEntries, batchSize)
		writeStart := time.Now()

		// Use deterministic fast key generator
		keyGen := NewFastRandom([]byte{1, 2, 3, 4})
		numBatches := totalEntries / batchSize

		for batch := 0; batch < numBatches; batch++ {
			batchStart := time.Now()

			// Generate and write one batch at a time
			for i := 0; i < batchSize; i++ {
				key := keyGen.NextHash()
				value := DBBKey{
					Offset: uint64(batch*batchSize + i),
					Length: 100,
				}

				if err := cht.Put(key, value); err != nil {
					t.Fatalf("Put failed at batch %d, item %d: %v", batch, i, err)
				}
			}

			batchTime := time.Since(batchStart)
			batchRate := float64(batchSize) / batchTime.Seconds()

			// Report every 10 batches
			if (batch+1) % 10 == 0 {
				t.Logf("  Batch %d/%d: %.0f entries/sec", batch+1, numBatches, batchRate)
			}
		}

		writeTime := time.Since(writeStart)
		writeRate := float64(totalEntries) / writeTime.Seconds()
		t.Logf("Write complete: %v (%.0f entries/sec)", writeTime, writeRate)

		// Force flush and measure sort time
		t.Log("Sorting/Flushing buffers...")
		sortStart := time.Now()
		cht.flushWriteBuffer()
		time.Sleep(500 * time.Millisecond) // Brief wait for background operations
		sortTime := time.Since(sortStart)
		t.Logf("Sort/Flush complete: %v", sortTime)

		// === SEQUENTIAL READ TEST (regenerate keys) ===
		t.Log("Testing sequential reads (regenerating keys)...")
		readSamples := 10000
		readStart := time.Now()
		found := 0

		// Reset key generator to start from beginning
		keyGen.Reset()

		for i := 0; i < readSamples; i++ {
			key := keyGen.NextHash()
			expectedOffset := uint64(i)

			if retrieved, err := cht.Get(key); err == nil {
				if retrieved.Offset == expectedOffset {
					found++
				}
			}
		}

		readTime := time.Since(readStart)
		readRate := float64(found) / readTime.Seconds()
		t.Logf("Sequential reads: %d/%d found in %v (%.0f reads/sec)",
			found, readSamples, readTime, readRate)

		// === RANDOM READ TEST ===
		t.Log("Testing random reads...")
		randomReadStart := time.Now()
		randomFound := 0

		// Create new generator with different seed for random access
		randomGen := NewFastRandom([]byte{5, 6, 7, 8})

		for i := 0; i < readSamples; i++ {
			// Generate random key from the dataset
			skipCount := int(time.Now().UnixNano()) % totalEntries
			randomGen.Reset()

			// Skip to random position
			for j := 0; j < skipCount; j++ {
				randomGen.NextHash()
			}

			key := randomGen.NextHash()
			expectedOffset := uint64(skipCount)

			if retrieved, err := cht.Get(key); err == nil {
				if retrieved.Offset == expectedOffset {
					randomFound++
				}
			}
		}

		randomReadTime := time.Since(randomReadStart)
		randomReadRate := float64(randomFound) / randomReadTime.Seconds()
		t.Logf("Random reads: %d/%d found in %v (%.0f reads/sec)",
			randomFound, readSamples, randomReadTime, randomReadRate)

		// Get memory after test
		var endMem runtime.MemStats
		runtime.ReadMemStats(&endMem)
		memoryUsed := endMem.Alloc - startMem.Alloc

		// Store results
		result := TestResult{
			FanOut:         fanOut,
			WriteTime:      writeTime,
			WriteRate:      writeRate,
			SortTime:       sortTime,
			ReadTime:       readTime,
			ReadRate:       readRate,
			RandomReadTime: randomReadTime,
			RandomReadRate: randomReadRate,
			MemoryUsed:     memoryUsed,
			Found:          found,
			RandomFound:    randomFound,
		}
		results = append(results, result)

		// Cleanup
		cht.Shutdown()
		os.RemoveAll(dir)

		// Force GC between tests
		runtime.GC()
		time.Sleep(1 * time.Second)
	}

	// === PRINT COMPARISON TABLE ===
	t.Logf("\n=== PERFORMANCE COMPARISON (%d ENTRIES) ===", totalEntries)
	t.Log("FanOut | Write/s  | Sort    | SeqRead/s | RandRead/s | Memory")
	t.Log("-------|----------|---------|-----------|------------|--------")

	for _, r := range results {
		t.Logf("%6d | %8.0f | %7.1fs | %9.0f | %10.0f | %s",
			r.FanOut,
			r.WriteRate,
			r.SortTime.Seconds(),
			r.ReadRate,
			r.RandomReadRate,
			formatBytesOpt(r.MemoryUsed))
	}

	// === ANALYSIS ===
	t.Log("\n=== ANALYSIS ===")

	// Memory efficiency
	t.Log("\nMemory Efficiency (vs storing all keys):")
	fullKeyMemory := uint64(totalEntries * 32) // 32 bytes per key
	for _, r := range results {
		savings := float64(fullKeyMemory-r.MemoryUsed) / float64(fullKeyMemory) * 100
		t.Logf("  Fan-out %d: %s used (%.1f%% savings vs %s)",
			r.FanOut, formatBytesOpt(r.MemoryUsed), savings, formatBytesOpt(fullKeyMemory))
	}

	// Find best configuration
	var bestWrite, bestRead TestResult
	for _, r := range results {
		if r.WriteRate > bestWrite.WriteRate {
			bestWrite = r
		}
		if r.ReadRate > bestRead.ReadRate {
			bestRead = r
		}
	}

	t.Log("\n=== WINNERS ===")
	t.Logf("Best Write Performance: Fan-out %d (%.0f writes/sec)", bestWrite.FanOut, bestWrite.WriteRate)
	t.Logf("Best Read Performance:  Fan-out %d (%.0f reads/sec)", bestRead.FanOut, bestRead.ReadRate)
}

// TestBatchKeyGeneration demonstrates batch-based key generation
func TestBatchKeyGeneration(t *testing.T) {
	t.Log("=== BATCH KEY GENERATION BENCHMARK ===")

	sizes := []int{1_000_000, 10_000_000, 100_000_000}

	for _, size := range sizes {
		t.Logf("\nTesting %d keys:", size)

		// Test 1: All keys in memory (old approach)
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		start := time.Now()
		keys := make([][32]byte, size)
		gen := NewFastRandom([]byte{1, 2})
		for i := range keys {
			keys[i] = gen.NextHash()
		}
		oldTime := time.Since(start)

		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		oldMemory := m2.Alloc - m1.Alloc

		keys = nil // Free memory
		runtime.GC()

		// Test 2: Batch generation (new approach)
		var m3 runtime.MemStats
		runtime.ReadMemStats(&m3)

		batchSize := 1_000_000
		start = time.Now()
		gen.Reset()

		for batch := 0; batch < size/batchSize; batch++ {
			// Process one batch at a time
			for i := 0; i < batchSize; i++ {
				_ = gen.NextHash() // Generate and use immediately
			}
		}
		newTime := time.Since(start)

		var m4 runtime.MemStats
		runtime.ReadMemStats(&m4)
		newMemory := m4.Alloc - m3.Alloc

		t.Logf("  Old approach: %v, Memory: %s", oldTime, formatBytesOpt(oldMemory))
		t.Logf("  New approach: %v, Memory: %s", newTime, formatBytesOpt(newMemory))
		t.Logf("  Speedup: %.2fx, Memory savings: %.1f%%",
			float64(oldTime)/float64(newTime),
			float64(oldMemory-newMemory)/float64(oldMemory)*100)
	}
}

// TestKeyRegeneration verifies that keys can be regenerated identically
func TestKeyRegeneration(t *testing.T) {
	t.Log("=== KEY REGENERATION VERIFICATION ===")

	gen := NewFastRandom([]byte{1, 2, 3, 4})

	// Generate first batch
	batch1 := make([][32]byte, 1000)
	for i := range batch1 {
		batch1[i] = gen.NextHash()
	}

	// Reset and regenerate
	gen.Reset()
	batch2 := make([][32]byte, 1000)
	for i := range batch2 {
		batch2[i] = gen.NextHash()
	}

	// Verify they match
	mismatches := 0
	for i := range batch1 {
		if batch1[i] != batch2[i] {
			mismatches++
		}
	}

	if mismatches > 0 {
		t.Errorf("Key regeneration failed: %d mismatches out of %d keys", mismatches, len(batch1))
	} else {
		t.Log("✓ All 1000 keys regenerated identically")
	}

	// Test partial regeneration (skip to position)
	gen.Reset()
	// Skip first 500
	for i := 0; i < 500; i++ {
		gen.NextHash()
	}
	// Should match batch1[500]
	key500 := gen.NextHash()
	if key500 != batch1[500] {
		t.Error("Partial regeneration failed at position 500")
	} else {
		t.Log("✓ Partial regeneration successful")
	}
}

// formatBytesOpt formats bytes in human readable form (optimized version)
func formatBytesOpt(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2fGB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2fMB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1fKB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

// TestLargeFanOutConcurrentOptimized tests concurrent access with batch generation
func TestLargeFanOutConcurrentOptimized(t *testing.T) {
	fanOuts := []int{256, 1024, 2048}
	totalEntries := 10_000_000 // 10M for reasonable test time
	numGoroutines := 20

	t.Log("=== CONCURRENT ACCESS TEST (OPTIMIZED) ===")

	for _, fanOut := range fanOuts {
		t.Logf("\nTesting fan-out %d with %d goroutines...", fanOut, numGoroutines)

		dir := fmt.Sprintf("/tmp/concurrent_fanout_opt_%d", fanOut)
		os.RemoveAll(dir)

		cht, err := NewConfigurableHashTable(dir, fanOut)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		var wg sync.WaitGroup
		entriesPerGoroutine := totalEntries / numGoroutines

		// Concurrent writes with batch generation
		writeStart := time.Now()
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Each goroutine gets its own generator with unique seed
				gen := NewFastRandom([]byte{byte(id), byte(id + 1), byte(id + 2), byte(id + 3)})

				for i := 0; i < entriesPerGoroutine; i++ {
					key := gen.NextHash()
					value := DBBKey{
						Offset: uint64(id*entriesPerGoroutine + i),
						Length: 100,
					}

					if err := cht.Put(key, value); err != nil {
						t.Errorf("Put failed: %v", err)
					}
				}
			}(g)
		}

		wg.Wait()
		writeTime := time.Since(writeStart)
		writeRate := float64(totalEntries) / writeTime.Seconds()

		t.Logf("  Fan-out %d: %.0f concurrent writes/sec", fanOut, writeRate)

		// Memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("  Memory in use: %s", formatBytesOpt(m.Alloc))

		cht.Shutdown()
		os.RemoveAll(dir)
	}
}