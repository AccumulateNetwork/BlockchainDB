package blockchainDB

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestKeyGenOptimization finds the optimal parameters for key generation
func TestKeyGenOptimization(t *testing.T) {
	t.Logf("=== KEY GENERATION OPTIMIZATION ===")
	t.Logf("Machine: %d CPU cores", runtime.NumCPU())

	totalKeys := 10_000_000
	batchSizes := []int{1_000, 10_000, 100_000}
	workerCounts := []int{1, 10, 24, 48, 100, 200, 400}

	results := make(map[string]float64)

	for _, batchSize := range batchSizes {
		numBatches := totalKeys / batchSize

		for _, numWorkers := range workerCounts {
			// Set GOMAXPROCS to use all cores
			runtime.GOMAXPROCS(runtime.NumCPU())

			start := time.Now()
			keysGenerated := int64(0)

			// Create work channel
			work := make(chan int, numBatches)
			for i := 0; i < numBatches; i++ {
				work <- i
			}
			close(work)

			// Launch workers
			var wg sync.WaitGroup
			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					for batchNum := range work {
						// Generate keys using batch number as seed
						seed := []byte{byte(batchNum), byte(batchNum >> 8), byte(batchNum >> 16), byte(batchNum >> 24)}
						fr := NewFastRandom(seed)

						for i := 0; i < batchSize; i++ {
							_ = fr.NextHash()
							atomic.AddInt64(&keysGenerated, 1)
						}
					}
				}(w)
			}

			wg.Wait()
			elapsed := time.Since(start)
			keysPerSec := float64(keysGenerated) / elapsed.Seconds()

			key := fmt.Sprintf("batch_%d_workers_%d", batchSize, numWorkers)
			results[key] = keysPerSec

			t.Logf("Batch %6d, Workers %3d: %8.3fs = %10.0f keys/sec",
				batchSize, numWorkers, elapsed.Seconds(), keysPerSec)
		}
		t.Log("---")
	}

	// Find best configuration
	var bestConfig string
	var bestRate float64
	for config, rate := range results {
		if rate > bestRate {
			bestRate = rate
			bestConfig = config
		}
	}

	t.Logf("\n=== BEST CONFIGURATION ===")
	t.Logf("%s: %.0f keys/sec", bestConfig, bestRate)
}

// TestKeyGenWithProgress shows progress during generation
func TestKeyGenWithProgress(t *testing.T) {
	t.Log("=== KEY GENERATION WITH PROGRESS ===")

	totalKeys := 100_000_000 // 100M keys
	batchSize := 10_000
	numWorkers := 100
	numBatches := totalKeys / batchSize

	t.Logf("Generating %d keys with %d workers (batch size %d)",
		totalKeys, numWorkers, batchSize)

	start := time.Now()
	keysGenerated := int64(0)
	lastReported := int64(0)

	// Progress reporting goroutine - report every 100ms for better visibility
	done := make(chan bool)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				current := atomic.LoadInt64(&keysGenerated)
				if current == lastReported && current < int64(totalKeys) {
					continue // Skip if no progress
				}
				lastReported = current

				elapsed := time.Since(start)
				if elapsed.Seconds() < 0.001 {
					continue // Avoid division by zero
				}

				rate := float64(current) / elapsed.Seconds()
				percent := float64(current) * 100 / float64(totalKeys)

				if current < int64(totalKeys) {
					remaining := float64(int64(totalKeys)-current) / rate
					eta := time.Duration(remaining * float64(time.Second))
					t.Logf("Progress: %10d/%d (%.1f%%) @ %.0f keys/sec, ETA: %v",
						current, totalKeys, percent, rate, eta)
				}

			case <-done:
				return
			}
		}
	}()

	// Create work channel
	work := make(chan int, numBatches)
	for i := 0; i < numBatches; i++ {
		work <- i
	}
	close(work)

	// Launch workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batchNum := range work {
				// Generate keys using batch number as seed
				seed := []byte{byte(batchNum), byte(batchNum >> 8), byte(batchNum >> 16), byte(batchNum >> 24)}
				fr := NewFastRandom(seed)

				for i := 0; i < batchSize; i++ {
					_ = fr.NextHash()
					atomic.AddInt64(&keysGenerated, 1)
				}
			}
		}(w)
	}

	wg.Wait()
	close(done)

	// Allow final progress update
	time.Sleep(150 * time.Millisecond)

	elapsed := time.Since(start)
	finalRate := float64(totalKeys) / elapsed.Seconds()
	finalCount := atomic.LoadInt64(&keysGenerated)

	t.Logf("\n=== COMPLETE ===")
	t.Logf("Generated %d keys in %v", finalCount, elapsed)
	t.Logf("Average rate: %.0f keys/sec", finalRate)
	t.Logf("Time per key: %.2f ns", elapsed.Seconds() * 1e9 / float64(totalKeys))

	if finalCount != int64(totalKeys) {
		t.Errorf("ERROR: Expected %d keys, got %d", totalKeys, finalCount)
	}
}

// TestKeyGenBottleneck tests where the bottleneck is
func TestKeyGenBottleneck(t *testing.T) {
	t.Log("=== BOTTLENECK ANALYSIS ===")

	numKeys := 1_000_000

	// Test 1: Pure key generation speed (single thread)
	start := time.Now()
	fr := NewFastRandom([]byte{1, 2, 3, 4})
	for i := 0; i < numKeys; i++ {
		_ = fr.NextHash()
	}
	singleThreadTime := time.Since(start)
	singleThreadRate := float64(numKeys) / singleThreadTime.Seconds()

	t.Logf("Single thread generation: %v = %.0f keys/sec",
		singleThreadTime, singleThreadRate)

	// Test 2: Channel overhead
	start = time.Now()
	ch := make(chan [32]byte, 1000)

	// Producer
	go func() {
		fr := NewFastRandom([]byte{1, 2, 3, 4})
		for i := 0; i < numKeys; i++ {
			ch <- fr.NextHash()
		}
		close(ch)
	}()

	// Consumer
	count := 0
	for range ch {
		count++
	}

	channelTime := time.Since(start)
	channelRate := float64(numKeys) / channelTime.Seconds()

	t.Logf("With channel overhead: %v = %.0f keys/sec (%.1fx slower)",
		channelTime, channelRate, channelTime.Seconds()/singleThreadTime.Seconds())

	// Test 3: Atomic counter overhead
	start = time.Now()
	counter := int64(0)
	fr = NewFastRandom([]byte{1, 2, 3, 4})
	for i := 0; i < numKeys; i++ {
		_ = fr.NextHash()
		atomic.AddInt64(&counter, 1)
	}
	atomicTime := time.Since(start)
	atomicRate := float64(numKeys) / atomicTime.Seconds()

	t.Logf("With atomic counter: %v = %.0f keys/sec (%.1fx slower)",
		atomicTime, atomicRate, atomicTime.Seconds()/singleThreadTime.Seconds())

	// Test 4: Memory allocation overhead
	start = time.Now()
	keys := make([][32]byte, numKeys)
	fr = NewFastRandom([]byte{1, 2, 3, 4})
	for i := 0; i < numKeys; i++ {
		keys[i] = fr.NextHash()
	}
	allocTime := time.Since(start)
	allocRate := float64(numKeys) / allocTime.Seconds()

	t.Logf("With allocation: %v = %.0f keys/sec (%.1fx slower)",
		allocTime, allocRate, allocTime.Seconds()/singleThreadTime.Seconds())

	t.Log("\n=== SUMMARY ===")
	t.Logf("Pure generation: %.0f keys/sec (baseline)", singleThreadRate)
	t.Logf("Best approach: minimize synchronization and allocation")
}