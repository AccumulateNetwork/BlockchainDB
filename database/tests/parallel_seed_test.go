package blockchainDB

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestParallelSeedBasedPerformance uses many goroutines for key generation
func TestParallelSeedBasedPerformance(t *testing.T) {
	fanOuts := []int{256, 1024, 2048}
	totalEntries := 1_000_000  // 1M entries for testing
	batchSize := 10_000          // 10k per batch = 1000 batches
	numWorkers := 200            // 200 goroutines for parallel generation

	t.Logf("=== PARALLEL SEED-BASED TEST (%d entries, %d workers) ===", totalEntries, numWorkers)
	t.Logf("Running on %d CPU cores", runtime.NumCPU())

	kg := NewKeyGenerator(batchSize)
	numBatches := totalEntries / batchSize

	for _, fanOut := range fanOuts {
		t.Logf("\n=== Testing Fan-Out %d ===", fanOut)

		dir := fmt.Sprintf("/tmp/parallel_seed_%d", fanOut)
		os.RemoveAll(dir)

		cht, err := NewConfigurableHashTable(dir, fanOut)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// PARALLEL WRITE TEST
		t.Logf("Starting write test: %d entries with %d workers", totalEntries, numWorkers)
		writeStart := time.Now()
		writtenCount := int64(0)
		errorCount := int64(0)

		// Progress reporter
		stopProgress := make(chan bool)
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond) // Update every 100ms
			defer ticker.Stop()
			lastCount := int64(0)

			for {
				select {
				case <-ticker.C:
					current := atomic.LoadInt64(&writtenCount)
					errors := atomic.LoadInt64(&errorCount)
					elapsed := time.Since(writeStart)

					if elapsed.Seconds() < 0.01 {
						continue
					}

					// Calculate rates
					rate := float64(current) / elapsed.Seconds()
					recentRate := float64(current - lastCount) / 0.5 // per 500ms
					percent := float64(current) * 100 / float64(totalEntries)

					// ETA calculation
					remaining := totalEntries - int(current)
					eta := time.Duration(0)
					if rate > 0 {
						eta = time.Duration(float64(remaining) / rate * float64(time.Second))
					}

					t.Logf("  WRITE: %8d/%d (%.1f%%) | Rate: %.0f/s (recent: %.0f/s) | Errors: %d | ETA: %v",
						current, totalEntries, percent, rate, recentRate, errors, eta)

					lastCount = current

				case <-stopProgress:
					return
				}
			}
		}()

		// Create work channel for batch numbers
		batchChan := make(chan int, numBatches)
		go func() {
			for i := 0; i < numBatches; i++ {
				batchChan <- i
			}
			close(batchChan)
		}()

		// Launch worker goroutines
		var wg sync.WaitGroup
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				// Each worker processes batches from the channel
				for batchNum := range batchChan {
					// Generate this batch's keys
					keys := kg.GenerateBatch(batchNum)

					// Write them
					for i, key := range keys {
						value := DBBKey{
							Offset: uint64(batchNum*batchSize + i),
							Length: 100,
						}
						if err := cht.Put(key, value); err != nil {
							atomic.AddInt64(&errorCount, 1)
							if atomic.LoadInt64(&errorCount) < 10 {
								t.Errorf("Worker %d: Put failed: %v", workerID, err)
							}
						} else {
							atomic.AddInt64(&writtenCount, 1)
						}
					}
				}
			}(w)
		}

		// Wait for all workers to complete
		wg.Wait()
		close(stopProgress)
		time.Sleep(100 * time.Millisecond) // Allow final progress report

		writeTime := time.Since(writeStart)
		finalCount := atomic.LoadInt64(&writtenCount)
		finalErrors := atomic.LoadInt64(&errorCount)
		writeRate := float64(finalCount) / writeTime.Seconds()

		t.Logf("Write complete: %d entries in %v (%.0f entries/sec), %d errors",
			finalCount, writeTime, writeRate, finalErrors)

		// Flush
		cht.flushWriteBuffer()
		time.Sleep(100 * time.Millisecond)

		// PARALLEL READ TEST
		t.Log("Testing parallel reads...")

		testRegions := []struct {
			name  string
			start int
			count int
		}{
			{"Early", 0, 10000},
			{"Middle", totalEntries/2, 10000},
			{"Late", totalEntries - 10000, 10000},
		}

		for _, region := range testRegions {
			readStart := time.Now()

			// Create work for parallel reads
			readWork := make(chan int, region.count)
			for i := 0; i < region.count; i++ {
				readWork <- region.start + i
			}
			close(readWork)

			var readWg sync.WaitGroup
			foundCount := make(chan int, numWorkers)

			// Launch reader workers
			for w := 0; w < numWorkers; w++ {
				readWg.Add(1)
				go func() {
					defer readWg.Done()
					localFound := 0

					for index := range readWork {
						key := kg.GenerateKey(index)
						if retrieved, err := cht.Get(key); err == nil {
							if retrieved.Offset == uint64(index) {
								localFound++
							}
						}
					}

					foundCount <- localFound
				}()
			}

			readWg.Wait()
			close(foundCount)

			// Sum up found
			totalFound := 0
			for found := range foundCount {
				totalFound += found
			}

			readTime := time.Since(readStart)
			readRate := float64(totalFound) / readTime.Seconds()
			t.Logf("  %s region: %d/%d found in %v (%.0f reads/sec)",
				region.name, totalFound, region.count, readTime, readRate)
		}

		// Memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("  Memory in use: %.2f MB", float64(m.Alloc)/1024/1024)

		cht.Shutdown()
		os.RemoveAll(dir)
	}
}

// TestParallelKeyGeneration benchmarks parallel key generation
func TestParallelKeyGeneration(t *testing.T) {
	t.Log("=== PARALLEL KEY GENERATION BENCHMARK ===")
	t.Logf("CPU cores: %d", runtime.NumCPU())

	totalKeys := 10_000_000
	batchSize := 10_000
	numBatches := totalKeys / batchSize

	workerCounts := []int{1, 10, 50, 100, 200}

	for _, numWorkers := range workerCounts {
		start := time.Now()

		// Create work channel
		batchChan := make(chan int, numBatches)
		for i := 0; i < numBatches; i++ {
			batchChan <- i
		}
		close(batchChan)

		var wg sync.WaitGroup
		kg := NewKeyGenerator(batchSize)

		// Launch workers
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for batchNum := range batchChan {
					// Generate batch (this simulates the work)
					_ = kg.GenerateBatch(batchNum)
				}
			}()
		}

		wg.Wait()
		elapsed := time.Since(start)
		keysPerSec := float64(totalKeys) / elapsed.Seconds()

		t.Logf("%3d workers: %v (%.0f keys/sec)", numWorkers, elapsed, keysPerSec)
	}
}

// TestConcurrentWrites tests that concurrent writes work correctly
func TestConcurrentWrites(t *testing.T) {
	t.Log("=== CONCURRENT WRITE CORRECTNESS TEST ===")

	dir := "/tmp/concurrent_test"
	os.RemoveAll(dir)

	cht, err := NewConfigurableHashTable(dir, 256)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer cht.Shutdown()
	defer os.RemoveAll(dir)

	numWorkers := 100
	keysPerWorker := 1000
	kg := NewKeyGenerator(keysPerWorker)

	var wg sync.WaitGroup

	// Each worker writes its own batch
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			keys := kg.GenerateBatch(workerID)
			for i, key := range keys {
				value := DBBKey{
					Offset: uint64(workerID*keysPerWorker + i),
					Length: 100,
				}
				if err := cht.Put(key, value); err != nil {
					t.Errorf("Worker %d: Put failed: %v", workerID, err)
				}
			}
		}(w)
	}

	wg.Wait()

	// Flush and verify
	cht.flushWriteBuffer()
	time.Sleep(100 * time.Millisecond)

	// Verify all keys are readable
	errors := 0
	for w := 0; w < numWorkers; w++ {
		keys := kg.GenerateBatch(w)
		for i, key := range keys {
			expectedOffset := uint64(w*keysPerWorker + i)
			if retrieved, err := cht.Get(key); err != nil {
				errors++
				if errors < 10 { // Only report first few errors
					t.Errorf("Failed to get key from batch %d, index %d", w, i)
				}
			} else if retrieved.Offset != expectedOffset {
				errors++
				if errors < 10 {
					t.Errorf("Wrong offset for batch %d, index %d: got %d, want %d",
						w, i, retrieved.Offset, expectedOffset)
				}
			}
		}
	}

	totalKeys := numWorkers * keysPerWorker
	t.Logf("Verified %d keys with %d errors", totalKeys, errors)
}