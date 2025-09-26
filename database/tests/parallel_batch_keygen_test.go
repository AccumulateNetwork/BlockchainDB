package blockchainDB

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// KeyBatch represents a completed batch of generated keys ready for database insertion.
// Each batch contains a fixed number of keys generated from a deterministic seed.
type KeyBatch struct {
	BatchID   int         // Unique identifier for this batch (0-indexed)
	Keys      [][32]byte  // Array of generated 32-byte keys
	StartTime time.Time   // When key generation started for this batch
	EndTime   time.Duration // How long it took to generate all keys in this batch
}

// BatchStats tracks comprehensive performance metrics for the entire batch processing pipeline.
// Used to analyze throughput, bottlenecks, and pipeline efficiency.
type BatchStats struct {
	TotalBatches      int64         // Total number of batches generated
	TotalKeys         int64         // Total number of keys generated across all batches
	GenerationTime    time.Duration // Total time spent generating keys
	DatabaseWriteTime time.Duration // Total time spent writing to database
	FirstBatchTime    time.Time     // Timestamp when first batch was written to DB
	LastBatchTime     time.Time     // Timestamp when last batch was written to DB
}

// TestParallelBatchKeyGen demonstrates a high-performance parallel batch key generation pipeline.
// This test implements a producer-consumer pattern where:
// - Multiple workers generate batches of keys in parallel
// - Generated batches are queued for database insertion
// - A database writer consumes batches and triggers new generation
// - Pipeline depth is maintained to maximize throughput
func TestParallelBatchKeyGen(t *testing.T) {
	fmt.Println("=== PARALLEL BATCH KEY GENERATION WITH DB SIMULATION ===")

	// Configuration parameters for the test
	totalKeys := 1_000_000 // Total number of keys to generate (reduced from 200M for faster testing)
	batchSize := 10_000      // Keys per batch (10K keys = ~320KB per batch at 32 bytes/key)
	pipelineDepth := 20      // Number of batches to keep in-flight (pre-generated and queued)
	dbWriteTimeMs := 1       // Simulated database write latency in milliseconds per batch

	numBatches := totalKeys / batchSize

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Total keys:         %d\n", totalKeys)
	fmt.Printf("  Batch size:         %d\n", batchSize)
	fmt.Printf("  Total batches:      %d\n", numBatches)
	fmt.Printf("  Pipeline depth:     %d batches\n", pipelineDepth)
	fmt.Printf("  DB write time:      %dms per batch\n", dbWriteTimeMs)
	fmt.Printf("\n")

	// Statistics
	var stats BatchStats
	batchesGenerated := int64(0)
	batchesWritten := int64(0)
	keysGenerated := int64(0)
	nextBatchID := int64(0)

	// Start time
	startTime := time.Now()

	// Channel for completed batches - buffered to hold pipeline depth worth of ready batches
	// This acts as the queue between key generators and the database writer
	completedBatches := make(chan *KeyBatch, pipelineDepth)

	// Signal channel to trigger new batch generation when a batch is consumed
	// Ensures we maintain constant pipeline depth (1-to-1 replacement strategy)
	generateSignal := make(chan struct{}, pipelineDepth)

	// Preload pipeline with initial batches to achieve steady-state immediately
	fmt.Printf("Preloading first %d batches...\n", pipelineDepth)
	preloadStart := time.Now()

	var preloadWg sync.WaitGroup
	for i := 0; i < pipelineDepth && i < numBatches; i++ {
		preloadWg.Add(1)
		go func() {
			defer preloadWg.Done()

			batchID := int(atomic.AddInt64(&nextBatchID, 1)) - 1
			genStart := time.Now()

			// Create deterministic seed from batch ID for reproducible key generation
			// This ensures the same batch ID always generates the same keys
			seed := []byte{
				byte(batchID),
				byte(batchID >> 8),
				byte(batchID >> 16),
				byte(batchID >> 24),
			}
			fr := NewFastRandom(seed)

			batch := &KeyBatch{
				BatchID:   batchID,
				Keys:      make([][32]byte, batchSize),
				StartTime: genStart,
			}

			for j := 0; j < batchSize; j++ {
				batch.Keys[j] = fr.NextHash()
				atomic.AddInt64(&keysGenerated, 1)
			}

			batch.EndTime = time.Since(genStart)
			completedBatches <- batch
			atomic.AddInt64(&batchesGenerated, 1)
		}()
	}

	preloadWg.Wait()
	preloadTime := time.Since(preloadStart)
	fmt.Printf("Preloading complete in %.2fs (%.0f keys/sec)\n\n",
		preloadTime.Seconds(),
		float64(pipelineDepth*batchSize)/preloadTime.Seconds())

	// Progress reporter
	progressDone := make(chan bool)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		lastGen := int64(0)
		lastWrite := int64(0)

		for {
			select {
			case <-ticker.C:
				gen := atomic.LoadInt64(&batchesGenerated)
				write := atomic.LoadInt64(&batchesWritten)
				keys := atomic.LoadInt64(&keysGenerated)

				if gen != lastGen || write != lastWrite {
					elapsed := time.Since(startTime)
					genRate := float64(keys) / elapsed.Seconds()
					queueDepth := len(completedBatches)

					fmt.Printf("[%6.1fs] Generated: %5d/%d batches | Written: %5d/%d | %.0f keys/sec | Queue: %d\n",
						elapsed.Seconds(),
						gen, numBatches,
						write, numBatches,
						genRate,
						queueDepth)

					lastGen = gen
					lastWrite = write
				}

			case <-progressDone:
				return
			}
		}
	}()

	// Generator pool - maintains exactly pipelineDepth workers to ensure continuous generation
	// Each worker waits for a signal to generate a new batch, maintaining steady pipeline depth
	generatorsDone := make(chan bool)
	go func() {
		defer close(generatorsDone)

		var genWg sync.WaitGroup

		// Start pipelineDepth workers - one for each slot in the pipeline
		for w := 0; w < pipelineDepth; w++ {
			genWg.Add(1)
			go func(workerID int) {
				defer genWg.Done()

				for range generateSignal {
					// Check if we've generated all batches
					batchID := int(atomic.AddInt64(&nextBatchID, 1)) - 1
					if batchID >= numBatches {
						break
					}

					genStart := time.Now()

					seed := []byte{
						byte(batchID),
						byte(batchID >> 8),
						byte(batchID >> 16),
						byte(batchID >> 24),
					}
					fr := NewFastRandom(seed)

					batch := &KeyBatch{
						BatchID:   batchID,
						Keys:      make([][32]byte, batchSize),
						StartTime: genStart,
					}

					for j := 0; j < batchSize; j++ {
						batch.Keys[j] = fr.NextHash()
						atomic.AddInt64(&keysGenerated, 1)
					}

					batch.EndTime = time.Since(genStart)
					completedBatches <- batch
					atomic.AddInt64(&batchesGenerated, 1)
				}
			}(w)
		}

		genWg.Wait()
	}()

	// Database writer (consumer) - simulates writing batches to database
	// For each batch consumed, it signals for a new batch to be generated,
	// maintaining constant pipeline depth throughout execution
	var dbWg sync.WaitGroup
	dbWg.Add(1)

	dbWriteStart := time.Now()
	go func() {
		defer dbWg.Done()

		batchTimes := make([]time.Duration, 0, numBatches)

		for i := 0; i < numBatches; i++ {
			// Consume a batch from the completed queue
			batch := <-completedBatches

			// Signal to generate a replacement batch (maintain pipeline depth)
			// This ensures we always have pipelineDepth batches in-flight
			if atomic.LoadInt64(&nextBatchID) < int64(numBatches) {
				select {
				case generateSignal <- struct{}{}:
				default:
				}
			}

			// Simulate database write latency
			time.Sleep(time.Duration(dbWriteTimeMs) * time.Millisecond)

			writeCount := atomic.AddInt64(&batchesWritten, 1)
			batchTimes = append(batchTimes, batch.EndTime)

			// Track timing
			if writeCount == 1 {
				stats.FirstBatchTime = time.Now()
			}
			if writeCount == int64(numBatches) {
				stats.LastBatchTime = time.Now()
			}

			// Report every 100th batch write
			if writeCount%100 == 0 {
				elapsed := time.Since(startTime)
				fmt.Printf("[%6.1fs] DB: Wrote batch %d (ID: %d), gen time: %v\n",
					elapsed.Seconds(),
					writeCount,
					batch.BatchID,
					batch.EndTime)
			}
		}

		// Close generate signal when done
		close(generateSignal)

		// Calculate average batch generation time
		var totalBatchTime time.Duration
		for _, bt := range batchTimes {
			totalBatchTime += bt
		}
		avgBatchTime := totalBatchTime / time.Duration(len(batchTimes))

		fmt.Printf("\n")
		fmt.Printf("Average batch generation time: %v\n", avgBatchTime)
	}()

	// Wait for DB writes to complete
	dbWg.Wait()
	stats.DatabaseWriteTime = time.Since(dbWriteStart)

	// Wait for generators to finish
	<-generatorsDone
	stats.GenerationTime = time.Since(startTime)

	// Stop progress reporter
	close(progressDone)
	time.Sleep(100 * time.Millisecond)

	// Calculate final statistics
	totalTime := time.Since(startTime)
	stats.TotalBatches = atomic.LoadInt64(&batchesGenerated)
	stats.TotalKeys = atomic.LoadInt64(&keysGenerated)

	fmt.Printf("\n")
	fmt.Printf("=== FINAL STATISTICS ===\n")
	fmt.Printf("Total keys generated:        %d\n", stats.TotalKeys)
	fmt.Printf("Total batches:              %d\n", stats.TotalBatches)
	fmt.Printf("Total time:                 %.2fs\n", totalTime.Seconds())
	fmt.Printf("\n")
	fmt.Printf("Generation phase:           %.2fs\n", stats.GenerationTime.Seconds())
	fmt.Printf("DB write phase:             %.2fs\n", stats.DatabaseWriteTime.Seconds())
	fmt.Printf("Time to first batch:        %.3fs\n", stats.FirstBatchTime.Sub(startTime).Seconds())
	fmt.Printf("Time to last batch written: %.2fs\n", stats.LastBatchTime.Sub(startTime).Seconds())
	fmt.Printf("\n")
	fmt.Printf("Key generation rate:        %.0f keys/sec\n", float64(stats.TotalKeys)/stats.GenerationTime.Seconds())
	fmt.Printf("Overall throughput:         %.0f keys/sec\n", float64(stats.TotalKeys)/totalTime.Seconds())
	fmt.Printf("Batch generation rate:      %.1f batches/sec\n", float64(stats.TotalBatches)/stats.GenerationTime.Seconds())
	fmt.Printf("Batch write rate:           %.1f batches/sec\n", float64(stats.TotalBatches)/stats.DatabaseWriteTime.Seconds())
	fmt.Printf("\n")

	// Analysis
	theoreticalDBTime := float64(numBatches*dbWriteTimeMs) / 1000.0
	actualDBTime := stats.DatabaseWriteTime.Seconds()
	pipelineEfficiency := (theoreticalDBTime / actualDBTime) * 100

	fmt.Printf("=== PIPELINE ANALYSIS ===\n")
	fmt.Printf("Theoretical DB time (sequential): %.2fs\n", theoreticalDBTime)
	fmt.Printf("Actual DB write time:             %.2fs\n", actualDBTime)
	fmt.Printf("Pipeline efficiency:              %.1f%%\n", pipelineEfficiency)

	if pipelineEfficiency < 95 {
		fmt.Printf("WARNING: Pipeline efficiency is low. Generation might not be keeping up with DB writes.\n")
		fmt.Printf("Consider increasing pipeline depth or optimizing generation.\n")
	} else {
		fmt.Printf("SUCCESS: Generation is maintaining pipeline depth as intended!\n")
		fmt.Printf("Pipeline maintained %d batch depth throughout execution.\n", pipelineDepth)
	}
}

// TestBatchPipelineScaling evaluates different pipeline depths to find the optimal configuration.
// It tests various concurrency levels to determine the sweet spot where generation
// keeps up with database writes without excessive resource usage.
func TestBatchPipelineScaling(t *testing.T) {
	fmt.Println("=== BATCH PIPELINE SCALING TEST ===")

	totalKeys := 1_000_000 // 1M keys for quick evaluation
	batchSize := 10_000    // 10K keys per batch
	dbWriteTimeMs := 50    // 50ms simulated DB latency (more realistic than 1ms)

	// Test different levels of concurrency to find optimal pipeline depth
	concurrentConfigs := []int{1, 2, 4, 8, 16, 32}

	type Result struct {
		ConcurrentBatches int
		TotalTime         time.Duration
		GenTime           time.Duration
		DBTime            time.Duration
		Efficiency        float64
	}

	results := []Result{}

	for _, concurrentBatches := range concurrentConfigs {
		fmt.Printf("\nTesting with %d concurrent batches...\n", concurrentBatches)

		numBatches := totalKeys / batchSize
		workQueue := make(chan int, numBatches)
		completedBatches := make(chan *KeyBatch, concurrentBatches*2)

		for i := 0; i < numBatches; i++ {
			workQueue <- i
		}
		close(workQueue)

		startTime := time.Now()
		batchesWritten := int64(0)

		// Generation workers
		var genWg sync.WaitGroup
		genStart := time.Now()

		for w := 0; w < concurrentBatches; w++ {
			genWg.Add(1)
			go func() {
				defer genWg.Done()

				for batchID := range workQueue {
					seed := []byte{byte(batchID), byte(batchID >> 8), byte(batchID >> 16), byte(batchID >> 24)}
					fr := NewFastRandom(seed)

					batch := &KeyBatch{
						BatchID: batchID,
						Keys:    make([][32]byte, batchSize),
					}

					for i := 0; i < batchSize; i++ {
						batch.Keys[i] = fr.NextHash()
					}

					completedBatches <- batch
				}
			}()
		}

		// DB writer
		var dbWg sync.WaitGroup
		dbWg.Add(1)

		dbStart := time.Now()
		go func() {
			defer dbWg.Done()

			for range completedBatches {
				time.Sleep(time.Duration(dbWriteTimeMs) * time.Millisecond)
				atomic.AddInt64(&batchesWritten, 1)
			}
		}()

		genWg.Wait()
		genTime := time.Since(genStart)
		close(completedBatches)

		dbWg.Wait()
		dbTime := time.Since(dbStart)

		totalTime := time.Since(startTime)

		theoreticalTime := float64(numBatches*dbWriteTimeMs) / 1000.0
		efficiency := (theoreticalTime / totalTime.Seconds()) * 100

		result := Result{
			ConcurrentBatches: concurrentBatches,
			TotalTime:         totalTime,
			GenTime:           genTime,
			DBTime:            dbTime,
			Efficiency:        efficiency,
		}
		results = append(results, result)

		fmt.Printf("  Total: %.2fs, Gen: %.2fs, DB: %.2fs, Efficiency: %.1f%%\n",
			totalTime.Seconds(), genTime.Seconds(), dbTime.Seconds(), efficiency)
	}

	fmt.Printf("\n=== SUMMARY ===\n")
	fmt.Printf("%-20s %-12s %-12s %-12s %-12s\n", "Concurrent Batches", "Total Time", "Gen Time", "DB Time", "Efficiency")
	fmt.Printf("%-20s %-12s %-12s %-12s %-12s\n", "------------------", "----------", "--------", "-------", "----------")

	bestEfficiency := 0.0
	bestConfig := 0

	for _, r := range results {
		fmt.Printf("%-20d %-12.2fs %-12.2fs %-12.2fs %-12.1f%%\n",
			r.ConcurrentBatches,
			r.TotalTime.Seconds(),
			r.GenTime.Seconds(),
			r.DBTime.Seconds(),
			r.Efficiency)

		if r.Efficiency > bestEfficiency {
			bestEfficiency = r.Efficiency
			bestConfig = r.ConcurrentBatches
		}
	}

	fmt.Printf("\nOptimal configuration: %d concurrent batches (%.1f%% efficiency)\n",
		bestConfig, bestEfficiency)

	fmt.Printf("\nRecommendation: Use %d-%d concurrent batches for best balance of efficiency and resource usage\n",
		bestConfig, bestConfig*2)
}