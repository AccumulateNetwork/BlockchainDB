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
)

// TestHierarchicalStorageParallel tests the new hierarchical storage with parallel batch writes
func TestHierarchicalStorageParallel(t *testing.T) {
	directory := "/tmp/HierarchicalStorage"
	os.RemoveAll(directory)

	// Configuration - same as HistoryFile test for comparison
	const totalKeys = 1_000_000 // 1M keys total (reduced from 200M for faster testing)
	const batchSize = 100_000     // 100k keys per batch
	const pipelineDepth = 20      // Maintain 20 batches in pipeline
	const numBatches = totalKeys / batchSize

	fmt.Println("=== HIERARCHICAL STORAGE PARALLEL BATCH TEST ===")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Total keys:      %s\n", humanize.Comma(totalKeys))
	fmt.Printf("  Batch size:      %s\n", humanize.Comma(batchSize))
	fmt.Printf("  Total batches:   %d\n", numBatches)
	fmt.Printf("  Pipeline depth:  %d\n", pipelineDepth)
	fmt.Printf("  Leaf capacity:   %s\n", humanize.Comma(LeafCapacity))
	fmt.Printf("  Expected leaves: ~%d\n", totalKeys/LeafCapacity)
	fmt.Printf("\n")

	// Create HierarchicalStorage
	hs, err := NewHierarchicalStorage(directory)
	assert.NoError(t, err, "failed to create HierarchicalStorage")
	defer hs.Close()

	// Statistics
	keysGenerated := int64(0)
	batchesGenerated := int64(0)
	batchesWritten := int64(0)
	nextBatchID := int64(0)

	// Batch structure for pipeline
	type KeyBatch struct {
		BatchID  int
		Keys     [][32]byte
		Buffer   []DBBKeyFull
		GenTime  time.Duration
		PrepTime time.Duration
	}

	// Channels for pipeline
	generatedBatches := make(chan *KeyBatch, pipelineDepth)
	preparedBatches := make(chan *KeyBatch, pipelineDepth)

	// Start time
	startTime := time.Now()

	// Progress reporter
	progressDone := make(chan bool)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				gen := atomic.LoadInt64(&batchesGenerated)
				written := atomic.LoadInt64(&batchesWritten)
				keys := atomic.LoadInt64(&keysGenerated)
				elapsed := time.Since(startTime)

				keysPerSec := float64(keys) / elapsed.Seconds()
				genQueue := len(generatedBatches)
				prepQueue := len(preparedBatches)

				fmt.Printf("[%6.1fs] Generated: %4d/%d | Written: %4d/%d | %.0f keys/sec | GenQ: %d PrepQ: %d | %s\n",
					elapsed.Seconds(), gen, numBatches, written, numBatches,
					keysPerSec, genQueue, prepQueue, hs.Stats())

			case <-progressDone:
				return
			}
		}
	}()

	// Key generation workers (parallel)
	generatorsDone := make(chan bool)
	go func() {
		defer close(generatorsDone)
		var genWg sync.WaitGroup

		// Start pipeline depth worth of generators
		for w := 0; w < pipelineDepth; w++ {
			genWg.Add(1)
			go func(workerID int) {
				defer genWg.Done()

				for {
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
					}

					for i := 0; i < batchSize; i++ {
						batch.Keys[i] = fr.NextHash()
						atomic.AddInt64(&keysGenerated, 1)
					}

					batch.GenTime = time.Since(genStart)
					atomic.AddInt64(&batchesGenerated, 1)

					generatedBatches <- batch
				}
			}(w)
		}

		genWg.Wait()
		close(generatedBatches)
	}()

	// Preparation workers (parallel) - prepare data for hierarchical storage
	preparersDone := make(chan bool)
	go func() {
		defer close(preparersDone)
		var prepWg sync.WaitGroup

		// Use half pipeline depth for preparers
		numPreparers := pipelineDepth / 2
		if numPreparers < 1 {
			numPreparers = 1
		}

		for w := 0; w < numPreparers; w++ {
			prepWg.Add(1)
			go func() {
				defer prepWg.Done()

				for batch := range generatedBatches {
					prepStart := time.Now()

					// Prepare DBBKeyFull entries
					batch.Buffer = make([]DBBKeyFull, batchSize)
					for i, key := range batch.Keys {
						batch.Buffer[i] = DBBKeyFull{
							Key: key,
							DBBKey: DBBKey{
								Offset: uint64(0x1010 * (batch.BatchID*batchSize + i + 1)),
								Length: uint64(0x1111 * (batch.BatchID*batchSize + i + 1)),
							},
						}
					}

					batch.PrepTime = time.Since(prepStart)
					preparedBatches <- batch
				}
			}()
		}

		prepWg.Wait()
		close(preparedBatches)
	}()

	// Database writer (sequential)
	writerDone := make(chan bool)
	writeStart := time.Now()

	go func() {
		defer close(writerDone)

		for batch := range preparedBatches {
			// Write to HierarchicalStorage
			writeStart := time.Now()

			for _, entry := range batch.Buffer {
				err := hs.AddKey(entry.Key, entry.DBBKey.Offset, entry.DBBKey.Length)
				assert.NoError(t, err, "AddKey failed")
			}

			writeTime := time.Since(writeStart)
			writeCount := atomic.AddInt64(&batchesWritten, 1)

			// Report every 10th batch
			if writeCount%10 == 0 {
				tps := float64(batchSize) / writeTime.Seconds()
				fmt.Printf("Batch %4d: Gen %6.2fms, Prep %6.2fms, Write %6.2fms = %.0f TPS\n",
					batch.BatchID,
					batch.GenTime.Seconds()*1000,
					batch.PrepTime.Seconds()*1000,
					writeTime.Seconds()*1000,
					tps)
			}
		}
	}()

	// Wait for pipeline to complete
	<-generatorsDone
	<-preparersDone
	<-writerDone
	close(progressDone)

	totalTime := time.Since(startTime)
	writeTime := time.Since(writeStart)

	// Print final statistics
	fmt.Printf("\n=== FINAL STATISTICS ===\n")
	fmt.Printf("Total keys written:   %s\n", humanize.Comma(int64(batchesWritten)*batchSize))
	fmt.Printf("Total time:          %.2fs\n", totalTime.Seconds())
	fmt.Printf("Write phase time:    %.2fs\n", writeTime.Seconds())
	fmt.Printf("Overall throughput:  %.0f keys/sec\n", float64(totalKeys)/totalTime.Seconds())
	fmt.Printf("Write throughput:    %.0f keys/sec\n", float64(totalKeys)/writeTime.Seconds())
	fmt.Printf("\nStorage Stats: %s\n", hs.Stats())

	// Test reads
	fmt.Println("\n=== READ PERFORMANCE TEST ===")
	fr := NewFastRandom([]byte{1, 2})

	testBatches := 10
	readBatchSize := 10000

	for batch := 0; batch < testBatches; batch++ {
		batchStart := time.Now()
		found := 0
		notFound := 0

		for i := 0; i < readBatchSize; i++ {
			k := fr.NextHash()
			_, _, err := hs.Get(k)
			if err == nil {
				found++
			} else {
				notFound++
			}
		}

		tps := float64(readBatchSize) / time.Since(batchStart).Seconds()
		fmt.Printf("Batch %2d: %5d reads @ %12.0f TPS (found: %d)\n",
			batch+1, readBatchSize, tps, found)
	}
}

// TestHierarchicalStorageSmall tests the hierarchical storage with a smaller dataset
func TestHierarchicalStorageSmall(t *testing.T) {
	directory := "/tmp/HierarchicalStorageSmall"
	os.RemoveAll(directory)

	const totalKeys = 1_000_000 // 1M keys for quick test
	const batchSize = 10_000

	fmt.Println("=== HIERARCHICAL STORAGE SMALL TEST ===")
	fmt.Printf("Total keys: %s\n", humanize.Comma(totalKeys))
	fmt.Printf("Batch size: %s\n", humanize.Comma(batchSize))
	fmt.Printf("Leaf capacity: %s\n", humanize.Comma(LeafCapacity))

	hs, err := NewHierarchicalStorage(directory)
	assert.NoError(t, err, "failed to create HierarchicalStorage")
	defer hs.Close()

	// Generate and write keys
	fr := NewFastRandom([]byte{1, 2, 3})
	startTime := time.Now()

	for i := 0; i < totalKeys; i++ {
		key := fr.NextHash()
		err := hs.AddKey(key, uint64(i*100), uint64(i*10))
		assert.NoError(t, err, "AddKey failed")

		if (i+1)%100000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			fmt.Printf("Progress: %d keys written, %.0f keys/sec, %s\n",
				i+1, rate, hs.Stats())
		}
	}

	totalTime := time.Since(startTime)
	fmt.Printf("\nWrite complete: %s keys in %.2fs = %.0f keys/sec\n",
		humanize.Comma(totalKeys), totalTime.Seconds(), float64(totalKeys)/totalTime.Seconds())
	fmt.Printf("Final stats: %s\n", hs.Stats())

	// Test reads
	fmt.Println("\n=== READ TEST ===")
	fr2 := NewFastRandom([]byte{1, 2, 3}) // Same seed to find some keys
	found := 0
	notFound := 0
	readStart := time.Now()

	for i := 0; i < 10000; i++ {
		key := fr2.NextHash()
		_, _, err := hs.Get(key)
		if err == nil {
			found++
		} else {
			notFound++
		}
	}

	readTime := time.Since(readStart)
	fmt.Printf("Read 10000 keys in %.2fs = %.0f reads/sec (found: %d, not found: %d)\n",
		readTime.Seconds(), 10000/readTime.Seconds(), found, notFound)
}