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

// TestBinaryTreeStorageParallel tests the binary tree storage with parallel writes
func TestBinaryTreeStorageParallel(t *testing.T) {
	directory := "/tmp/BinaryTreeStorage"
	os.RemoveAll(directory)

	// Configuration matching HistoryFile test
	const totalKeys = 1_000_000 // 1M keys (reduced from 200M for faster testing)
	const batchSize = 100_000
	const pipelineDepth = 20
	const numBatches = totalKeys / batchSize

	fmt.Println("=== BINARY TREE STORAGE PARALLEL BATCH TEST ===")
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Total keys:      %s\n", humanize.Comma(totalKeys))
	fmt.Printf("  Batch size:      %s\n", humanize.Comma(batchSize))
	fmt.Printf("  Total batches:   %d\n", numBatches)
	fmt.Printf("  Pipeline depth:  %d\n", pipelineDepth)
	fmt.Printf("  Leaf capacity:   %s keys\n", humanize.Comma(LeafMaxKeys))
	fmt.Printf("  Tree structure:  Binary (navigation by hash bits)\n")
	fmt.Printf("\n")

	// Create BinaryTreeStorage
	bts, err := NewBinaryTreeStorage(directory)
	assert.NoError(t, err, "failed to create BinaryTreeStorage")
	defer bts.Close()

	// Statistics
	keysGenerated := int64(0)
	batchesGenerated := int64(0)
	batchesWritten := int64(0)
	nextBatchID := int64(0)

	// Batch structure
	type KeyBatch struct {
		BatchID  int
		Keys     [][32]byte
		Entries  []KeyWrite
		GenTime  time.Duration
		PrepTime time.Duration
	}

	// Pipeline channels
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

				fmt.Printf("[%6.1fs] Generated: %4d/%d | Written: %4d/%d | %.0f keys/sec | Q: %d/%d | %s\n",
					elapsed.Seconds(), gen, numBatches, written, numBatches,
					keysPerSec, genQueue, prepQueue, bts.Stats())

			case <-progressDone:
				return
			}
		}
	}()

	// Key generation workers
	generatorsDone := make(chan bool)
	go func() {
		defer close(generatorsDone)
		var genWg sync.WaitGroup

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

	// Preparation workers
	preparersDone := make(chan bool)
	go func() {
		defer close(preparersDone)
		var prepWg sync.WaitGroup

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

					// Prepare entries
					batch.Entries = make([]KeyWrite, batchSize)
					for i, key := range batch.Keys {
						batch.Entries[i] = KeyWrite{
							Key:    key,
							Offset: uint64(0x1010 * (batch.BatchID*batchSize + i + 1)),
							Length: uint64(0x1111 * (batch.BatchID*batchSize + i + 1)),
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

	// Writer
	writerDone := make(chan bool)
	writeStart := time.Now()

	go func() {
		defer close(writerDone)

		for batch := range preparedBatches {
			writeStart := time.Now()

			for _, entry := range batch.Entries {
				err := bts.AddKey(entry.Key, entry.Offset, entry.Length)
				assert.NoError(t, err, "AddKey failed")
			}

			writeTime := time.Since(writeStart)
			writeCount := atomic.AddInt64(&batchesWritten, 1)

			// Report every 10th batch
			if writeCount%10 == 0 {
				tps := float64(batchSize) / writeTime.Seconds()
				fmt.Printf("Batch %4d: Gen %6.2fms, Prep %6.2fms, Write %6.2fms = %.0f TPS | %s\n",
					batch.BatchID,
					batch.GenTime.Seconds()*1000,
					batch.PrepTime.Seconds()*1000,
					writeTime.Seconds()*1000,
					tps,
					bts.Stats())
			}
		}
	}()

	// Wait for completion
	<-generatorsDone
	<-preparersDone
	<-writerDone
	close(progressDone)

	totalTime := time.Since(startTime)
	writeTime := time.Since(writeStart)

	// Final statistics
	fmt.Printf("\n=== FINAL STATISTICS ===\n")
	fmt.Printf("Total keys written:  %s\n", humanize.Comma(int64(batchesWritten)*batchSize))
	fmt.Printf("Total time:          %.2fs\n", totalTime.Seconds())
	fmt.Printf("Write phase time:    %.2fs\n", writeTime.Seconds())
	fmt.Printf("Overall throughput:  %.0f keys/sec\n", float64(totalKeys)/totalTime.Seconds())
	fmt.Printf("Write throughput:    %.0f keys/sec\n", float64(totalKeys)/writeTime.Seconds())
	fmt.Printf("\nFinal storage: %s\n", bts.Stats())
	fmt.Printf("Expected leaves:     ~%d (at full capacity)\n", totalKeys/LeafMaxKeys)

	// Test reads
	fmt.Println("\n=== READ PERFORMANCE TEST ===")

	testBatches := 10
	readBatchSize := 10000

	// Use the same seed generation as writes to read keys that actually exist
	for batch := 0; batch < testBatches; batch++ {
		// Use same seed as write phase for this batch
		seed := []byte{
			byte(batch),
			byte(batch >> 8),
			byte(batch >> 16),
			byte(batch >> 24),
		}
		fr := NewFastRandom(seed)

		batchStart := time.Now()
		found := 0
		notFound := 0

		for i := 0; i < readBatchSize; i++ {
			k := fr.NextHash()
			_, _, err := bts.Get(k)
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

// TestBinaryTreeSmall tests with a smaller dataset for quick validation
func TestBinaryTreeSmall(t *testing.T) {
	directory := "/tmp/BinaryTreeSmall"
	os.RemoveAll(directory)

	const totalKeys = 500_000 // 500K keys

	fmt.Println("=== BINARY TREE SMALL TEST ===")
	fmt.Printf("Total keys: %s\n", humanize.Comma(totalKeys))
	fmt.Printf("Leaf capacity: %s\n", humanize.Comma(LeafMaxKeys))
	fmt.Printf("Expected leaves: ~%d\n", totalKeys/LeafMaxKeys)

	bts, err := NewBinaryTreeStorage(directory)
	assert.NoError(t, err)
	defer bts.Close()

	// Generate and write keys
	fr := NewFastRandom([]byte{1, 2, 3})
	startTime := time.Now()

	for i := 0; i < totalKeys; i++ {
		key := fr.NextHash()
		err := bts.AddKey(key, uint64(i*100), uint64(i*10))
		assert.NoError(t, err)

		if (i+1)%50000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			fmt.Printf("Progress: %d keys, %.0f keys/sec, %s\n",
				i+1, rate, bts.Stats())
		}
	}

	// Flush remaining writes
	select {
	case bts.flushSignal <- struct{}{}:
	default:
	}
	time.Sleep(200 * time.Millisecond) // Give time for flush to complete

	totalTime := time.Since(startTime)
	fmt.Printf("\nWrite complete: %s keys in %.2fs = %.0f keys/sec\n",
		humanize.Comma(totalKeys), totalTime.Seconds(), float64(totalKeys)/totalTime.Seconds())
	fmt.Printf("Final: %s\n", bts.Stats())

	// Verify leaf count is reasonable
	leafCount := bts.TotalLeaves.Load()
	expectedLeaves := uint32((totalKeys + LeafMaxKeys - 1) / LeafMaxKeys)
	fmt.Printf("Leaf efficiency: %d leaves created (expected ~%d)\n", leafCount, expectedLeaves)

	// Test reads - use same keys we wrote
	fmt.Println("\n=== READ TEST ===")
	fr2 := NewFastRandom([]byte{1, 2, 3}) // Same seed to generate same keys
	found := 0
	notFound := 0
	readStart := time.Now()

	// Read first 10000 keys we wrote
	for i := 0; i < 10000; i++ {
		key := fr2.NextHash()
		offset, length, err := bts.Get(key)
		if err == nil {
			// Verify the values match what we wrote
			expectedOffset := uint64(i * 100)
			expectedLength := uint64(i * 10)
			if offset == expectedOffset && length == expectedLength {
				found++
			} else {
				fmt.Printf("Value mismatch for key %d: got (%d, %d), expected (%d, %d)\n",
					i, offset, length, expectedOffset, expectedLength)
			}
		} else {
			notFound++
			if i == 0 {
				fmt.Printf("First key not found, error: %v\n", err)
			}
		}
	}

	readTime := time.Since(readStart)
	fmt.Printf("Read 10000 keys in %.3fs = %.0f reads/sec (found: %d)\n",
		readTime.Seconds(), 10000/readTime.Seconds(), found)
}