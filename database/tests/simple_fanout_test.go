package blockchainDB

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestSimpleFanOutComparison does a quick comparison of fan-out values
func TestSimpleFanOutComparison(t *testing.T) {
	fanOuts := []int{16, 64, 256}
	numEntries := 5000

	t.Log("=== FAN-OUT COMPARISON ===")
	t.Log("Testing with", numEntries, "entries")
	t.Log("")

	type Result struct {
		FanOut        int
		WriteRate     float64
		ReadRate      float64
		WriteTime     time.Duration
		ReadTime      time.Duration
		Merges        uint64
		Levels        int
		BitsPerLevel  int
		MaxDepth      int
	}

	results := make([]Result, 0)

	for _, fanOut := range fanOuts {
		t.Logf("Testing fan-out %d...", fanOut)

		dir := fmt.Sprintf("/tmp/simple_fanout_%d", fanOut)
		os.RemoveAll(dir)

		cht, err := NewConfigurableHashTable(dir, fanOut)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Calculate bits per level
		bitsPerLevel := 0
		temp := fanOut - 1
		for temp > 0 {
			bitsPerLevel++
			temp >>= 1
		}

		// Generate test data
		keys := make([][32]byte, numEntries)
		for i := 0; i < numEntries; i++ {
			data := fmt.Sprintf("entry_%d", i)
			keys[i] = sha256.Sum256([]byte(data))
		}

		// Write test
		writeStart := time.Now()
		for i := 0; i < numEntries; i++ {
			value := DBBKey{Offset: uint64(i), Length: 100}
			if err := cht.Put(keys[i], value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		writeTime := time.Since(writeStart)

		// Flush
		cht.flushWriteBuffer()
		time.Sleep(1 * time.Second)

		// Read test
		readStart := time.Now()
		found := 0
		readSamples := 1000
		for i := 0; i < readSamples; i++ {
			if _, err := cht.Get(keys[i%numEntries]); err == nil {
				found++
			}
		}
		readTime := time.Since(readStart)

		stats := cht.GetStatistics()

		result := Result{
			FanOut:       fanOut,
			WriteRate:    float64(numEntries) / writeTime.Seconds(),
			ReadRate:     float64(found) / readTime.Seconds(),
			WriteTime:    writeTime,
			ReadTime:     readTime,
			Merges:       stats["total_merges"].(uint64),
			Levels:       stats["num_levels"].(int),
			BitsPerLevel: bitsPerLevel,
			MaxDepth:     256 / bitsPerLevel, // Max depth with 256-bit hash
		}

		results = append(results, result)

		cht.Shutdown()
		os.RemoveAll(dir)
	}

	// Print comparison table
	t.Log("\n=== RESULTS TABLE ===")
	t.Log("FanOut | Bits/Level | Max Depth | Write/s | Read/s | Write Time | Merges | Levels")
	t.Log("-------|------------|-----------|---------|--------|------------|--------|--------")

	for _, r := range results {
		t.Logf("%6d | %10d | %9d | %7.0f | %6.0f | %10v | %6d | %6d",
			r.FanOut, r.BitsPerLevel, r.MaxDepth,
			r.WriteRate, r.ReadRate, r.WriteTime,
			r.Merges, r.Levels)
	}

	// Analysis
	t.Log("\n=== ANALYSIS ===")

	// Memory usage
	t.Logf("\nMemory Usage (per level):")
	for _, fanOut := range fanOuts {
		cachePerLevel := fanOut * 16 * 1024 // 16KB per bin
		t.Logf("  Fan-out %d: %d bins × 16KB = %.1f MB per level",
			fanOut, fanOut, float64(cachePerLevel)/(1024*1024))
	}

	// Parallelism
	t.Logf("\nParallelism Potential:")
	for _, fanOut := range fanOuts {
		t.Logf("  Fan-out %d: %d-way parallel operations possible", fanOut, fanOut)
	}

	// Tree depth analysis
	t.Logf("\nTree Depth for Different Data Sizes:")
	dataSizes := []int{1000, 10000, 100000, 1000000}
	for _, size := range dataSizes {
		t.Logf("  %d entries:", size)
		for _, fanOut := range fanOuts {
			// Estimate depth based on average bin size
			avgPerBin := size / fanOut
			depth := 1
			for avgPerBin > 1000 { // Assuming 1000 entries per bin before fan-out
				avgPerBin /= fanOut
				depth++
			}
			t.Logf("    Fan-out %d: ~%d levels", fanOut, depth)
		}
	}

	// Recommendation
	t.Log("\n=== RECOMMENDATION ===")
	var bestFanOut int
	var bestReason string

	// Simple scoring: balance between performance and memory
	bestScore := 0.0
	for _, r := range results {
		// Score based on write rate, read rate, and inverse of merges
		score := r.WriteRate/1000 + r.ReadRate/10000 - float64(r.Merges)/10
		if score > bestScore {
			bestScore = score
			bestFanOut = r.FanOut
		}
	}

	switch bestFanOut {
	case 16:
		bestReason = "Low memory usage, good for small datasets"
	case 64:
		bestReason = "Balanced memory and performance"
	case 256:
		bestReason = "Maximum parallelism, good for large datasets"
	}

	t.Logf("Recommended fan-out: %d", bestFanOut)
	t.Logf("Reason: %s", bestReason)
}

// TestFanOutTheory explains the theoretical implications
func TestFanOutTheory(t *testing.T) {
	t.Log("=== FAN-OUT THEORETICAL ANALYSIS ===")
	t.Log("")

	fanOuts := []int{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}

	t.Log("Fan-Out | Bits/Level | Max Levels | Bins/Level | Memory/Level | I/O Fanout")
	t.Log("--------|------------|------------|------------|--------------|------------")

	for _, fanOut := range fanOuts {
		bitsPerLevel := 0
		temp := fanOut - 1
		for temp > 0 {
			bitsPerLevel++
			temp >>= 1
		}

		maxLevels := 256 / bitsPerLevel // SHA-256 has 256 bits
		if 256%bitsPerLevel > 0 {
			maxLevels++
		}

		memoryPerLevel := fanOut * 16 // 16KB per bin
		ioFanout := fanOut              // Number of parallel I/O operations

		t.Logf("%7d | %10d | %10d | %10d | %11dKB | %10d",
			fanOut, bitsPerLevel, maxLevels, fanOut, memoryPerLevel, ioFanout)
	}

	t.Log("\n=== IMPLICATIONS ===")
	t.Log("")

	t.Log("Small Fan-Out (2-16):")
	t.Log("  ✓ Low memory usage per level")
	t.Log("  ✓ Deep trees possible (good for huge datasets)")
	t.Log("  ✗ More levels = more disk seeks for reads")
	t.Log("  ✗ Limited parallelism")
	t.Log("")

	t.Log("Medium Fan-Out (64-256):")
	t.Log("  ✓ Good balance of depth and width")
	t.Log("  ✓ Reasonable memory usage")
	t.Log("  ✓ Good parallelism")
	t.Log("  ✓ Byte-aligned for 256 (easy bit manipulation)")
	t.Log("")

	t.Log("Large Fan-Out (512-1024):")
	t.Log("  ✓ Maximum parallelism")
	t.Log("  ✓ Shallow trees (fewer disk seeks)")
	t.Log("  ✗ High memory usage per level")
	t.Log("  ✗ More complex bit extraction")
	t.Log("  ✗ Diminishing returns on parallelism")
	t.Log("")

	t.Log("RECOMMENDATION: 64 or 256")
	t.Log("  64: Best general-purpose balance")
	t.Log("  256: Best for systems with ample memory and need for maximum parallelism")
}