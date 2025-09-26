package history

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

// TestHistoryParallelBatch tests HistoryFile with parallel batch key generation
func TestHistoryParallelBatch(t *testing.T) {
	directory := "/tmp/HistoryParallel"
	os.RemoveAll(directory)

	// Configuration
	const totalKeys = 1_000_000 // 1M keys total (reduced from 200M for faster testing)
	const batchSize = 100_000     // 100k keys per batch
	const pipelineDepth = 20      // Maintain 20 batches in pipeline
	const numBatches = totalKeys / batchSize

	fmt.Println("=== HISTORYFILE PARALLEL BATCH TEST ===")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Total keys:     %s\n", humanize.Comma(totalKeys))
	fmt.Printf("  Batch size:     %s\n", humanize.Comma(batchSize))
	fmt.Printf("  Total batches:  %d\n", numBatches)
	fmt.Printf("  Pipeline depth: %d\n", pipelineDepth)
	fmt.Printf("\n")

	// Create HistoryFile with more bins for better distribution
	// Use power of 2 for efficient modulo operation (bitwise AND)
	// With 200M keys, using 8192 bins (2^13) means ~24K keys per bin on average
	// This should reduce contention and improve performance vs 2000 bins (~100K keys per bin)
	const numBins = 8192 // 2^13
	hf, err := NewHistoryFile(numBins, directory)
	assert.NoError(t, err, "failed to create HistoryFile")

	fmt.Printf("  Bins:           %s (2^13)\n", humanize.Comma(numBins))
	fmt.Printf("  Keys per bin:   ~%s (average)\n", humanize.Comma(totalKeys/numBins))

	// Statistics
	keysGenerated := int64(0)
	batchesGenerated := int64(0)
	batchesWritten := int64(0)
	nextBatchID := int64(0)

	// Batch structure for pipeline
	type KeyBatch struct {
		BatchID    int
		Keys       [][32]byte
		SortedKeys [][32]byte
		Buffer     []byte
		GenTime    time.Duration
		SortTime   time.Duration
	}

	// Channels for pipeline
	generatedBatches := make(chan *KeyBatch, pipelineDepth)
	sortedBatches := make(chan *KeyBatch, pipelineDepth)

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
				sortQueue := len(sortedBatches)

				fmt.Printf("[%6.1fs] Generated: %3d/%d | Written: %3d/%d | %.0f keys/sec | GenQ: %d SortQ: %d\n",
					elapsed.Seconds(), gen, numBatches, written, numBatches,
					keysPerSec, genQueue, sortQueue)

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

	// Sorting workers (parallel) - sort batches for HistoryFile index
	sortersDone := make(chan bool)
	go func() {
		defer close(sortersDone)
		var sortWg sync.WaitGroup

		// Use half pipeline depth for sorters
		numSorters := pipelineDepth / 2
		if numSorters < 1 {
			numSorters = 1
		}

		for w := 0; w < numSorters; w++ {
			sortWg.Add(1)
			go func() {
				defer sortWg.Done()

				for batch := range generatedBatches {
					sortStart := time.Now()

					// Create key indices for sorting
					type keyIndex struct {
						key   [32]byte
						hfIdx int
					}

					indices := make([]keyIndex, batchSize)
					for i, key := range batch.Keys {
						indices[i] = keyIndex{
							key:   key,
							hfIdx: hf.Index(key),
						}
					}

					// Sort by HistoryFile index
					sort.Slice(indices, func(i, j int) bool {
						return indices[i].hfIdx < indices[j].hfIdx
					})

					// Extract sorted keys and create buffer
					batch.SortedKeys = make([][32]byte, batchSize)
					batch.Buffer = make([]byte, utils.DBKeyFullSize*batchSize)
					bufOffset := 0

					for i, idx := range indices {
						batch.SortedKeys[i] = idx.key

						// Pack into buffer (simulating utils.DBBKey format)
						var dbKey utils.DBBKeyFull
						dbKey.Key = idx.key
						dbKey.Offset = uint64(0x1010 * (batch.BatchID*batchSize + i + 1))
						dbKey.Length = uint64(0x1111 * (batch.BatchID*batchSize + i + 1))

						copy(batch.Buffer[bufOffset:], dbKey.DBBKey.Bytes(dbKey.Key))
						bufOffset += utils.DBKeyFullSize
					}

					batch.SortTime = time.Since(sortStart)
					sortedBatches <- batch
				}
			}()
		}

		sortWg.Wait()
		close(sortedBatches)
	}()

	// Database writer (sequential)
	writerDone := make(chan bool)
	writeStart := time.Now()

	go func() {
		defer close(writerDone)

		for batch := range sortedBatches {
			// Write to HistoryFile
			writeStart := time.Now()
			err := hf.AddKeys(batch.Buffer)
			assert.NoError(t, err, "AddKeys failed")
			writeTime := time.Since(writeStart)

			writeCount := atomic.AddInt64(&batchesWritten, 1)

			// Report every 10th batch
			if writeCount%10 == 0 {
				tps := float64(batchSize) / writeTime.Seconds()
				usPerWrite := float64(writeTime.Microseconds()) / float64(batchSize)
				fmt.Printf("Batch %3d: Gen %6.2fms, Sort %6.2fms, Write %6.2fms = %.0f TPS (%.1f μs/write)\n",
					batch.BatchID,
					batch.GenTime.Seconds()*1000,
					batch.SortTime.Seconds()*1000,
					writeTime.Seconds()*1000,
					tps,
					usPerWrite)
			}
		}
	}()

	// Wait for pipeline to complete
	<-generatorsDone
	<-sortersDone
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

	// Sort all KeySets for binary search
	fmt.Println("\nSorting all KeySets for binary search...")
	sortStart := time.Now()
	err = hf.SortAllKeySets()
	assert.NoError(t, err, "SortAllKeySets failed")
	fmt.Printf("Sort complete: %v\n", time.Since(sortStart))

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
			// Use Get method if FindKey doesn't exist
			_, e := hf.Get(k)
			if e == nil {
				found++
			} else {
				notFound++
			}
		}

		readTime := time.Since(batchStart)
		tps := float64(readBatchSize) / readTime.Seconds()
		usPerRead := readTime.Microseconds() / int64(readBatchSize)
		fmt.Printf("Batch %2d: %5d reads @ %12.0f TPS (found: %d) - %d μs/read\n",
			batch+1, readBatchSize, tps, found, usPerRead)
	}
}
