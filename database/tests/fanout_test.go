package blockchainDB

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// TestFanOutComparison tests different fan-out values
func TestFanOutComparison(t *testing.T) {
	fanOuts := []int{16, 64, 256, 1024}
	numEntries := 50000

	results := make(map[int]map[string]interface{})

	for _, fanOut := range fanOuts {
		t.Run(fmt.Sprintf("FanOut_%d", fanOut), func(t *testing.T) {
			dir := fmt.Sprintf("/tmp/fanout_test_%d", fanOut)
			os.RemoveAll(dir)

			cht, err := NewConfigurableHashTable(dir, fanOut)
			if err != nil {
				t.Fatalf("Failed to create table with fan-out %d: %v", fanOut, err)
			}
			defer cht.Shutdown()

			// Generate test data
			keys := make([][32]byte, numEntries)
			values := make([]DBBKey, numEntries)
			for i := 0; i < numEntries; i++ {
				data := fmt.Sprintf("test_entry_%d_%d", fanOut, i)
				keys[i] = sha256.Sum256([]byte(data))
				values[i] = DBBKey{
					Offset: uint64(i * 100),
					Length: uint64(len(data)),
				}
			}

			// Measure write performance
			writeStart := time.Now()
			for i := 0; i < numEntries; i++ {
				if err := cht.Put(keys[i], values[i]); err != nil {
					t.Fatalf("Put failed: %v", err)
				}
			}
			writeTime := time.Since(writeStart)

			// Force flush
			cht.flushWriteBuffer()
			time.Sleep(2 * time.Second)

			// Measure read performance
			readSamples := minInt(1000, numEntries/10)
			readStart := time.Now()
			found := 0
			for i := 0; i < readSamples; i++ {
				idx := rand.Intn(numEntries)
				if _, err := cht.Get(keys[idx]); err == nil {
					found++
				}
			}
			readTime := time.Since(readStart)

			// Get statistics
			stats := cht.GetStatistics()
			results[fanOut] = stats

			// Add test results
			stats["write_total_time"] = writeTime
			stats["write_rate"] = float64(numEntries) / writeTime.Seconds()
			stats["read_total_time"] = readTime
			stats["read_rate"] = float64(readSamples) / readTime.Seconds()
			stats["read_hit_rate"] = float64(found) / float64(readSamples)

			t.Logf("Fan-out %d results:", fanOut)
			t.Logf("  Write rate: %.0f/sec", stats["write_rate"])
			t.Logf("  Read rate: %.0f/sec", stats["read_rate"])
			t.Logf("  Merges: %d", stats["total_merges"])
			t.Logf("  Fan-outs: %d", stats["total_fanouts"])
			t.Logf("  Levels: %d", stats["num_levels"])
			t.Logf("  Memory: %d bytes", stats["total_cache_memory"])

			os.RemoveAll(dir) // Clean up
		})
	}

	// Summary comparison
	t.Log("\n=== SUMMARY COMPARISON ===")
	t.Log("Fan-out | Write Rate | Read Rate | Merges | Levels | Memory")
	t.Log("--------|------------|-----------|--------|--------|--------")
	for _, fanOut := range fanOuts {
		stats := results[fanOut]
		t.Logf("%7d | %10.0f | %9.0f | %6d | %6d | %s",
			fanOut,
			stats["write_rate"],
			stats["read_rate"],
			stats["total_merges"],
			stats["num_levels"],
			formatBytes(stats["total_cache_memory"].(int)))
	}
}

// TestFanOutScaling tests how each fan-out scales with data size
func TestFanOutScaling(t *testing.T) {
	fanOuts := []int{16, 256}
	scales := []int{1000, 10000, 50000}

	for _, fanOut := range fanOuts {
		t.Run(fmt.Sprintf("FanOut_%d", fanOut), func(t *testing.T) {
			dir := fmt.Sprintf("/tmp/fanout_scaling_%d", fanOut)
			os.RemoveAll(dir)

			cht, err := NewConfigurableHashTable(dir, fanOut)
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}
			defer cht.Shutdown()

			var previousRate float64

			for _, scale := range scales {
				// Generate keys for this scale
				keys := make([][32]byte, scale)
				for i := 0; i < scale; i++ {
					data := fmt.Sprintf("scale_%d_entry_%d", scale, i)
					keys[i] = sha256.Sum256([]byte(data))
				}

				// Measure write performance
				start := time.Now()
				for i := 0; i < scale; i++ {
					value := DBBKey{Offset: uint64(i), Length: 100}
					if err := cht.Put(keys[i], value); err != nil {
						t.Fatalf("Put failed: %v", err)
					}
				}
				elapsed := time.Since(start)

				writeRate := float64(scale) / elapsed.Seconds()

				// Calculate degradation
				var degradation float64
				if previousRate > 0 {
					degradation = (previousRate - writeRate) / previousRate * 100
				}
				previousRate = writeRate

				t.Logf("  Scale %7d: %.0f writes/sec (degradation: %.1f%%)",
					scale, writeRate, degradation)
			}

			stats := cht.GetStatistics()
			t.Logf("  Final stats: Merges=%d, FanOuts=%d, Levels=%d",
				stats["total_merges"], stats["total_fanouts"], stats["num_levels"])

			os.RemoveAll(dir)
		})
	}
}

// TestFanOutConcurrency tests concurrent access with different fan-outs
func TestFanOutConcurrency(t *testing.T) {
	fanOuts := []int{16, 64, 256}
	numGoroutines := 10
	entriesPerGoroutine := 1000

	for _, fanOut := range fanOuts {
		t.Run(fmt.Sprintf("FanOut_%d", fanOut), func(t *testing.T) {
			dir := fmt.Sprintf("/tmp/fanout_concurrent_%d", fanOut)
			os.RemoveAll(dir)

			cht, err := NewConfigurableHashTable(dir, fanOut)
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}
			defer cht.Shutdown()

			var wg sync.WaitGroup
			start := time.Now()

			// Concurrent writers
			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					for i := 0; i < entriesPerGoroutine; i++ {
						data := fmt.Sprintf("goroutine_%d_entry_%d", id, i)
						hash := sha256.Sum256([]byte(data))
						value := DBBKey{
							Offset: uint64(id*10000 + i),
							Length: uint64(len(data)),
						}

						if err := cht.Put(hash, value); err != nil {
							t.Errorf("Put failed: %v", err)
						}
					}
				}(g)
			}

			wg.Wait()
			elapsed := time.Since(start)

			totalEntries := numGoroutines * entriesPerGoroutine
			rate := float64(totalEntries) / elapsed.Seconds()

			stats := cht.GetStatistics()
			t.Logf("Fan-out %d concurrent results:", fanOut)
			t.Logf("  Total time: %v", elapsed)
			t.Logf("  Write rate: %.0f/sec", rate)
			t.Logf("  Merges: %d", stats["total_merges"])
			t.Logf("  Levels: %d", stats["num_levels"])

			os.RemoveAll(dir)
		})
	}
}

// TestOptimalFanOut runs a comprehensive test to find optimal fan-out
func TestOptimalFanOut(t *testing.T) {
	// Test more fan-out values
	fanOuts := []int{8, 16, 32, 64, 128, 256, 512}
	numEntries := 5000

	type Result struct {
		FanOut     int
		WriteRate  float64
		ReadRate   float64
		Merges     int
		FanOuts    int
		Levels     int
		Memory     int
		Score      float64 // Combined score
	}

	results := make([]Result, 0, len(fanOuts))

	for _, fanOut := range fanOuts {
		dir := fmt.Sprintf("/tmp/optimal_fanout_%d", fanOut)
		os.RemoveAll(dir)

		cht, err := NewConfigurableHashTable(dir, fanOut)
		if err != nil {
			t.Logf("Skipping fan-out %d: %v", fanOut, err)
			continue
		}

		// Generate test data
		keys := make([][32]byte, numEntries)
		for i := 0; i < numEntries; i++ {
			data := make([]byte, 32)
			rand.Read(data)
			keys[i] = sha256.Sum256(data)
		}

		// Write test
		writeStart := time.Now()
		for i := 0; i < numEntries; i++ {
			value := DBBKey{Offset: uint64(i), Length: 100}
			cht.Put(keys[i], value)
		}
		writeTime := time.Since(writeStart)
		writeRate := float64(numEntries) / writeTime.Seconds()

		// Flush and wait
		cht.flushWriteBuffer()
		time.Sleep(1 * time.Second)

		// Read test
		readSamples := 1000
		readStart := time.Now()
		found := 0
		for i := 0; i < readSamples; i++ {
			if _, err := cht.Get(keys[rand.Intn(numEntries)]); err == nil {
				found++
			}
		}
		readTime := time.Since(readStart)
		readRate := float64(found) / readTime.Seconds()

		stats := cht.GetStatistics()

		result := Result{
			FanOut:    fanOut,
			WriteRate: writeRate,
			ReadRate:  readRate,
			Merges:    int(stats["total_merges"].(uint64)),
			FanOuts:   int(stats["total_fanouts"].(uint64)),
			Levels:    stats["num_levels"].(int),
			Memory:    stats["total_cache_memory"].(int),
		}

		// Calculate composite score (higher is better)
		// Balance between performance and resource usage
		result.Score = (writeRate / 1000) + (readRate / 10000) -
			float64(result.Merges)/100 - float64(result.Memory)/1000000

		results = append(results, result)

		cht.Shutdown()
		os.RemoveAll(dir)
	}

	// Print results table
	t.Log("\n=== OPTIMAL FAN-OUT ANALYSIS ===")
	t.Log("FanOut | Write/s | Read/s | Merges | Levels | Memory  | Score")
	t.Log("-------|---------|--------|--------|--------|---------|-------")

	var bestResult Result
	bestScore := -999999.0

	for _, r := range results {
		t.Logf("%6d | %7.0f | %6.0f | %6d | %6d | %7s | %6.2f",
			r.FanOut, r.WriteRate, r.ReadRate, r.Merges,
			r.Levels, formatBytes(r.Memory), r.Score)

		if r.Score > bestScore {
			bestScore = r.Score
			bestResult = r
		}
	}

	t.Logf("\n🏆 OPTIMAL FAN-OUT: %d", bestResult.FanOut)
	t.Logf("   Best score: %.2f", bestResult.Score)
	t.Logf("   Write rate: %.0f/sec", bestResult.WriteRate)
	t.Logf("   Read rate: %.0f/sec", bestResult.ReadRate)
}

// Helper function to format bytes
func formatBytes(bytes int) string {
	const (
		KB = 1024
		MB = KB * 1024
	)
	switch {
	case bytes >= MB:
		return fmt.Sprintf("%.1fMB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1fKB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}