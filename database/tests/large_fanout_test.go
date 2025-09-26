package blockchainDB

import (
	"crypto/sha256"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestLargeFanOut tests fan-out values 1024 and 2048 with 1M entries
func TestLargeFanOut(t *testing.T) {
	// Test configurations
	fanOuts := []int{256, 1024, 2048} // Include 256 for comparison
	numEntries := 1_000_000         // 1M entries (reduced from 200M for faster testing)

	t.Logf("=== LARGE FAN-OUT TEST WITH %d ENTRIES ===\n", numEntries)

	// Get initial memory stats
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)

	type TestResult struct {
		FanOut         int
		WriteTime      time.Duration
		WriteRate      float64
		ReadTime       time.Duration
		ReadRate       float64
		RandomReadTime time.Duration
		RandomReadRate float64
		Merges         uint64
		FanOuts        uint64
		Levels         int
		TreeDepth      int
		MemoryUsed     uint64
		CacheMemory    int
		BitsPerLevel   int
		Found          int
		RandomFound    int
	}

	results := make([]TestResult, 0)

	// Pre-generate keys using FastRandom (much faster than crypto/rand)
	t.Logf("Generating %d keys using FastRandom...", numEntries)
	keys := make([][32]byte, numEntries)
	values := make([]DBBKey, numEntries)

	// Use FastRandom for much faster key generation
	fr := NewFastRandom([]byte{1, 2, 3, 4})

	for i := 0; i < numEntries; i++ {
		keys[i] = fr.NextHash()
		values[i] = DBBKey{
			Offset: uint64(i * 100),
			Length: 100,
		}

		if i > 0 && i%1000000 == 0 {
			t.Logf("  Generated %d keys...", i)
		}
	}
	t.Log("Key generation complete")

	// Test each fan-out value
	for _, fanOut := range fanOuts {
		t.Logf("\n=== Testing Fan-Out %d ===", fanOut)

		dir := fmt.Sprintf("/tmp/large_fanout_%d", fanOut)
		os.RemoveAll(dir)

		// Get memory before test
		var startMem runtime.MemStats
		runtime.ReadMemStats(&startMem)

		cht, err := NewConfigurableHashTable(dir, fanOut)
		if err != nil {
			t.Fatalf("Failed to create table with fan-out %d: %v", fanOut, err)
		}

		// Calculate bits per level
		bitsPerLevel := 0
		temp := fanOut - 1
		for temp > 0 {
			bitsPerLevel++
			temp >>= 1
		}

		// === WRITE TEST ===
		t.Logf("Writing %d entries...", numEntries)
		writeStart := time.Now()

		for i := 0; i < numEntries; i++ {
			if err := cht.Put(keys[i], values[i]); err != nil {
				t.Fatalf("Put failed at %d: %v", i, err)
			}
		}

		writeTime := time.Since(writeStart)
		writeRate := float64(numEntries) / writeTime.Seconds()
		t.Logf("Write complete: %v (%.0f entries/sec)", writeTime, writeRate)

		// Force flush and wait for merges
		t.Log("Flushing buffers...")
		cht.flushWriteBuffer()
		time.Sleep(1 * time.Second)

		// === SEQUENTIAL READ TEST ===
		t.Log("Testing sequential reads (first 2K entries)...")
		readSamples := 2000
		readStart := time.Now()
		found := 0

		for i := 0; i < readSamples; i++ {
			if retrieved, err := cht.Get(keys[i]); err == nil {
				if retrieved.Offset == values[i].Offset {
					found++
				}
			}
		}

		readTime := time.Since(readStart)
		readRate := float64(found) / readTime.Seconds()
		t.Logf("Sequential reads: %d/%d found in %v (%.0f reads/sec)",
			found, readSamples, readTime, readRate)

		// === RANDOM READ TEST ===
		t.Log("Testing random reads (2K random entries)...")
		randomReadStart := time.Now()
		randomFound := 0

		for i := 0; i < readSamples; i++ {
			// Pick a random key from our dataset
			idx := int(time.Now().UnixNano()) % numEntries
			if retrieved, err := cht.Get(keys[idx]); err == nil {
				if retrieved.Offset == values[idx].Offset {
					randomFound++
				}
			}
		}

		randomReadTime := time.Since(randomReadStart)
		randomReadRate := float64(randomFound) / randomReadTime.Seconds()
		t.Logf("Random reads: %d/%d found in %v (%.0f reads/sec)",
			randomFound, readSamples, randomReadTime, randomReadRate)

		// Get statistics
		stats := cht.GetStatistics()

		// Get memory after test
		var endMem runtime.MemStats
		runtime.ReadMemStats(&endMem)
		memoryUsed := endMem.Alloc - startMem.Alloc

		// Store results
		result := TestResult{
			FanOut:         fanOut,
			WriteTime:      writeTime,
			WriteRate:      writeRate,
			ReadTime:       readTime,
			ReadRate:       readRate,
			RandomReadTime: randomReadTime,
			RandomReadRate: randomReadRate,
			Merges:         stats["total_merges"].(uint64),
			FanOuts:        stats["total_fanouts"].(uint64),
			Levels:         stats["num_levels"].(int),
			TreeDepth:      stats["num_levels"].(int),
			MemoryUsed:     memoryUsed,
			CacheMemory:    stats["total_cache_memory"].(int),
			BitsPerLevel:   bitsPerLevel,
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
	t.Log("\n=== PERFORMANCE COMPARISON (1M ENTRIES) ===")
	t.Log("FanOut | Bits | Levels | Write/s | SeqRead/s | RandRead/s | Merges | FanOuts | Memory")
	t.Log("-------|------|--------|---------|-----------|------------|--------|---------|--------")

	for _, r := range results {
		t.Logf("%6d | %4d | %6d | %7.0f | %9.0f | %10.0f | %6d | %7d | %s",
			r.FanOut,
			r.BitsPerLevel,
			r.Levels,
			r.WriteRate,
			r.ReadRate,
			r.RandomReadRate,
			r.Merges,
			r.FanOuts,
			formatBytesLarge(r.CacheMemory))
	}

	// === ANALYSIS ===
	t.Log("\n=== ANALYSIS ===")

	// Write performance analysis
	t.Log("\nWrite Performance:")
	for _, r := range results {
		t.Logf("  Fan-out %d: %.0f writes/sec (%.2f ms per write)",
			r.FanOut, r.WriteRate, float64(r.WriteTime.Milliseconds())/float64(numEntries))
	}

	// Read performance analysis
	t.Log("\nRead Performance (Sequential):")
	for _, r := range results {
		t.Logf("  Fan-out %d: %.0f reads/sec (%.3f µs per read)",
			r.FanOut, r.ReadRate, float64(r.ReadTime.Microseconds())/float64(r.Found))
	}

	t.Log("\nRead Performance (Random):")
	for _, r := range results {
		t.Logf("  Fan-out %d: %.0f reads/sec (%.3f µs per read)",
			r.FanOut, r.RandomReadRate, float64(r.RandomReadTime.Microseconds())/float64(r.RandomFound))
	}

	// Tree structure analysis
	t.Log("\nTree Structure:")
	for _, r := range results {
		avgEntriesPerBin := numEntries / r.FanOut
		t.Logf("  Fan-out %d: %d levels, ~%d entries per L1 bin",
			r.FanOut, r.Levels, avgEntriesPerBin)
	}

	// Memory analysis
	t.Log("\nMemory Usage:")
	for _, r := range results {
		memPerEntry := r.CacheMemory / numEntries
		t.Logf("  Fan-out %d: %s total cache (%d bytes per entry)",
			r.FanOut, formatBytesLarge(r.CacheMemory), memPerEntry)
	}

	// Find best configuration
	var bestWrite, bestSeqRead, bestRandRead TestResult
	for _, r := range results {
		if r.WriteRate > bestWrite.WriteRate {
			bestWrite = r
		}
		if r.ReadRate > bestSeqRead.ReadRate {
			bestSeqRead = r
		}
		if r.RandomReadRate > bestRandRead.RandomReadRate {
			bestRandRead = r
		}
	}

	t.Log("\n=== WINNERS ===")
	t.Logf("Best Write Performance: Fan-out %d (%.0f writes/sec)", bestWrite.FanOut, bestWrite.WriteRate)
	t.Logf("Best Sequential Read:   Fan-out %d (%.0f reads/sec)", bestSeqRead.FanOut, bestSeqRead.ReadRate)
	t.Logf("Best Random Read:       Fan-out %d (%.0f reads/sec)", bestRandRead.FanOut, bestRandRead.RandomReadRate)
}

// TestLargeFanOutConcurrent tests concurrent access with large fan-outs
func TestLargeFanOutConcurrent(t *testing.T) {
	fanOuts := []int{256, 1024, 2048}
	numEntries := 100000 // 100K for faster test
	numGoroutines := 20

	t.Log("=== CONCURRENT ACCESS TEST ===")

	for _, fanOut := range fanOuts {
		t.Logf("\nTesting fan-out %d with %d goroutines...", fanOut, numGoroutines)

		dir := fmt.Sprintf("/tmp/concurrent_fanout_%d", fanOut)
		os.RemoveAll(dir)

		cht, err := NewConfigurableHashTable(dir, fanOut)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		var wg sync.WaitGroup
		entriesPerGoroutine := numEntries / numGoroutines

		// Concurrent writes
		writeStart := time.Now()
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for i := 0; i < entriesPerGoroutine; i++ {
					data := fmt.Sprintf("g%d_e%d", id, i)
					hash := sha256.Sum256([]byte(data))
					value := DBBKey{
						Offset: uint64(id*100000 + i),
						Length: 100,
					}

					if err := cht.Put(hash, value); err != nil {
						t.Errorf("Put failed: %v", err)
					}
				}
			}(g)
		}

		wg.Wait()
		writeTime := time.Since(writeStart)
		writeRate := float64(numEntries) / writeTime.Seconds()

		t.Logf("  Fan-out %d: %.0f concurrent writes/sec", fanOut, writeRate)

		cht.Shutdown()
		os.RemoveAll(dir)
	}
}

// formatBytesLarge formats bytes in human readable form
func formatBytesLarge(bytes int) string {
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
