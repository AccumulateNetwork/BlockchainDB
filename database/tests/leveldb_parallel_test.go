package blockchainDB

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// TestLevelDBParallelBatch tests LevelDB with parallel batch key generation
func TestLevelDBParallelBatch(t *testing.T) {
	directory := "/tmp/LevelDBParallel"
	os.RemoveAll(directory)

	// Configuration
	const totalKeys = 10_000_000  // 10M keys total
	const batchSize = 100_000     // 100k keys per batch
	const pipelineDepth = 20      // Maintain 20 batches in pipeline
	const numBatches = totalKeys / batchSize

	fmt.Println("=== LEVELDB PARALLEL BATCH TEST ===")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Total keys:     %s\n", humanize.Comma(totalKeys))
	fmt.Printf("  Batch size:     %s\n", humanize.Comma(batchSize))
	fmt.Printf("  Total batches:  %d\n", numBatches)
	fmt.Printf("  Pipeline depth: %d\n", pipelineDepth)
	fmt.Printf("\n")

	// Open LevelDB with optimized settings
	opts := &opt.Options{
		WriteBuffer:         256 * 1024 * 1024, // 256MB write buffer
		BlockCacheCapacity:  512 * 1024 * 1024, // 512MB block cache
		CompactionTableSize: 64 * 1024 * 1024,  // 64MB SST files
		CompactionTotalSize: 640 * 1024 * 1024, // 640MB level size
	}

	db, err := leveldb.OpenFile(directory, opts)
	assert.NoError(t, err, "failed to open LevelDB")
	defer db.Close()

	// Statistics
	keysGenerated := int64(0)
	batchesGenerated := int64(0)
	batchesWritten := int64(0)
	nextBatchID := int64(0)

	// Batch structure for pipeline
	type KeyBatch struct {
		BatchID int
		Keys    [][32]byte
		Values  [][]byte
		GenTime time.Duration
	}

	// Channels for pipeline
	completedBatches := make(chan *KeyBatch, pipelineDepth)

	// Start time
	startTime := time.Now()

	// Progress reporter
	progressDone := make(chan bool)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		lastGen := int64(0)
		lastWrite := int64(0)

		for {
			select {
			case <-ticker.C:
				gen := atomic.LoadInt64(&batchesGenerated)
				written := atomic.LoadInt64(&batchesWritten)
				keys := atomic.LoadInt64(&keysGenerated)
				elapsed := time.Since(startTime)

				if gen != lastGen || written != lastWrite {
					keysPerSec := float64(keys) / elapsed.Seconds()
					queueDepth := len(completedBatches)

					fmt.Printf("[%6.1fs] Generated: %3d/%d | Written: %3d/%d | %.0f keys/sec | Queue: %d\n",
						elapsed.Seconds(), gen, numBatches, written, numBatches,
						keysPerSec, queueDepth)

					lastGen = gen
					lastWrite = written
				}

			case <-progressDone:
				return
			}
		}
	}()

	// Preload first batches
	fmt.Printf("Preloading first %d batches...\n", pipelineDepth)
	preloadStart := time.Now()

	var preloadWg sync.WaitGroup
	for i := 0; i < pipelineDepth && i < numBatches; i++ {
		preloadWg.Add(1)
		go func() {
			defer preloadWg.Done()

			batchID := int(atomic.AddInt64(&nextBatchID, 1)) - 1
			genStart := time.Now()

			// Generate keys with deterministic seed
			seed := []byte{
				byte(batchID),
				byte(batchID >> 8),
				byte(batchID >> 16),
				byte(batchID >> 24),
			}
			fr := NewFastRandom(seed)

			batch := &KeyBatch{
				BatchID: batchID,
				Keys:    make([][32]byte, batchSize),
				Values:  make([][]byte, batchSize),
			}

			for i := 0; i < batchSize; i++ {
				batch.Keys[i] = fr.NextHash()

				// Create value (offset and length like in original test)
				value := make([]byte, 16)
				offset := uint64(0x1010 * (batchID*batchSize + i + 1))
				length := uint64(0x1111 * (batchID*batchSize + i + 1))
				for j := 0; j < 8; j++ {
					value[j] = byte(offset >> (8 * j))
					value[j+8] = byte(length >> (8 * j))
				}
				batch.Values[i] = value

				atomic.AddInt64(&keysGenerated, 1)
			}

			batch.GenTime = time.Since(genStart)
			completedBatches <- batch
			atomic.AddInt64(&batchesGenerated, 1)
		}()
	}

	preloadWg.Wait()
	preloadTime := time.Since(preloadStart)
	fmt.Printf("Preloading complete in %.2fs (%.0f keys/sec)\n\n",
		preloadTime.Seconds(),
		float64(pipelineDepth*batchSize)/preloadTime.Seconds())

	// Signal channel for batch consumption
	generateSignal := make(chan struct{}, pipelineDepth)

	// Key generation workers (parallel) - maintain pipeline depth
	generatorsDone := make(chan bool)
	go func() {
		defer close(generatorsDone)
		var genWg sync.WaitGroup

		// Start pipeline depth worth of generators
		for w := 0; w < pipelineDepth; w++ {
			genWg.Add(1)
			go func(workerID int) {
				defer genWg.Done()

				for range generateSignal {
					batchID := int(atomic.AddInt64(&nextBatchID, 1)) - 1
					if batchID >= numBatches {
						break
					}

					genStart := time.Now()

					// Generate keys with deterministic seed
					seed := []byte{
						byte(batchID),
						byte(batchID >> 8),
						byte(batchID >> 16),
						byte(batchID >> 24),
					}
					fr := NewFastRandom(seed)

					batch := &KeyBatch{
						BatchID: batchID,
						Keys:    make([][32]byte, batchSize),
						Values:  make([][]byte, batchSize),
					}

					for i := 0; i < batchSize; i++ {
						batch.Keys[i] = fr.NextHash()

						// Create value
						value := make([]byte, 16)
						offset := uint64(0x1010 * (batchID*batchSize + i + 1))
						length := uint64(0x1111 * (batchID*batchSize + i + 1))
						for j := 0; j < 8; j++ {
							value[j] = byte(offset >> (8 * j))
							value[j+8] = byte(length >> (8 * j))
						}
						batch.Values[i] = value

						atomic.AddInt64(&keysGenerated, 1)
					}

					batch.GenTime = time.Since(genStart)
					completedBatches <- batch
					atomic.AddInt64(&batchesGenerated, 1)
				}
			}(w)
		}

		genWg.Wait()
	}()

	// Database writer (sequential) - triggers new batch generation
	writeStart := time.Now()
	var writeWg sync.WaitGroup
	writeWg.Add(1)

	go func() {
		defer writeWg.Done()

		writeOpts := &opt.WriteOptions{
			Sync: false, // Don't sync every batch for better performance
		}

		for i := 0; i < numBatches; i++ {
			// Get batch from queue
			batch := <-completedBatches

			// Signal to generate a replacement batch
			if atomic.LoadInt64(&nextBatchID) < int64(numBatches) {
				select {
				case generateSignal <- struct{}{}:
				default:
				}
			}

			// Write batch to LevelDB
			batchWriteStart := time.Now()
			leveldbBatch := new(leveldb.Batch)

			for j := 0; j < batchSize; j++ {
				leveldbBatch.Put(batch.Keys[j][:], batch.Values[j])
			}

			err := db.Write(leveldbBatch, writeOpts)
			assert.NoError(t, err, "LevelDB write failed")
			writeTime := time.Since(batchWriteStart)

			writeCount := atomic.AddInt64(&batchesWritten, 1)

			// Report every 10th batch
			if writeCount%10 == 0 {
				tps := float64(batchSize) / writeTime.Seconds()
				fmt.Printf("Batch %3d: Gen %6.2fms, Write %6.2fms = %.0f TPS\n",
					batch.BatchID,
					batch.GenTime.Seconds()*1000,
					writeTime.Seconds()*1000,
					tps)
			}
		}

		close(generateSignal)
	}()

	// Wait for writing to complete
	writeWg.Wait()
	writeTime := time.Since(writeStart)

	// Wait for generators to finish
	<-generatorsDone
	close(progressDone)
	time.Sleep(100 * time.Millisecond)

	totalTime := time.Since(startTime)

	// Print final statistics
	fmt.Printf("\n=== FINAL WRITE STATISTICS ===\n")
	fmt.Printf("Total keys written:   %s\n", humanize.Comma(int64(batchesWritten)*batchSize))
	fmt.Printf("Total time:          %.2fs\n", totalTime.Seconds())
	fmt.Printf("Write phase time:    %.2fs\n", writeTime.Seconds())
	fmt.Printf("Overall throughput:  %.0f keys/sec\n", float64(totalKeys)/totalTime.Seconds())
	fmt.Printf("Write throughput:    %.0f keys/sec\n", float64(totalKeys)/writeTime.Seconds())

	// Force compaction
	fmt.Println("\n=== COMPACTING ===")
	compactStart := time.Now()
	err = db.CompactRange(util.Range{})
	assert.NoError(t, err, "Compaction failed")
	fmt.Printf("Compaction complete: %v\n", time.Since(compactStart))

	// Test reads with parallel generation
	fmt.Println("\n=== READ PERFORMANCE TEST ===")

	// Reset for reading
	fr := NewFastRandom([]byte{1, 2})

	testBatches := 10
	readBatchSize := 10000
	totalReads := testBatches * readBatchSize

	readStart := time.Now()
	found := 0
	notFound := 0

	for batch := 0; batch < testBatches; batch++ {
		batchStart := time.Now()
		batchFound := 0
		batchNotFound := 0

		for i := 0; i < readBatchSize; i++ {
			k := fr.NextHash()

			_, err := db.Get(k[:], nil)
			if err == nil {
				batchFound++
				found++
			} else {
				batchNotFound++
				notFound++
			}
		}

		tps := float64(readBatchSize) / time.Since(batchStart).Seconds()
		fmt.Printf("Batch %2d: %5d reads @ %12.0f TPS (found: %d, not found: %d)\n",
			batch+1, readBatchSize, tps, batchFound, batchNotFound)
	}

	totalReadTime := time.Since(readStart)
	avgReadTPS := float64(totalReads) / totalReadTime.Seconds()
	fmt.Printf("\nRead complete: %v total, %.0f avg TPS\n", totalReadTime, avgReadTPS)
	fmt.Printf("Found: %d, Not found: %d\n", found, notFound)

	// Print database statistics
	fmt.Println("\n=== LEVELDB STATISTICS ===")
	stats, err := db.GetProperty("leveldb.stats")
	if err == nil {
		fmt.Println(stats)
	}
}