package history

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

// TestHistoryWithPipeline demonstrates the cleanest way to test HistoryFile
// using the reusable KeyPipeline utility
func TestHistoryWithPipeline(t *testing.T) {
	directory := "/tmp/HistoryPipeline"
	os.RemoveAll(directory)

	fmt.Println("=== HISTORY FILE TEST WITH KEY PIPELINE ===")
	fmt.Println("Using reusable pipeline for clean, efficient testing")
	fmt.Println()

	// Configuration
	const (
		totalKeys     = 10_000_000 // 10M keys
		batchSize     = 100_000    // 100K per batch
		pipelineDepth = 20         // Maintain 20 batches in-flight
		numBins       = 8192       // 2^13 bins
	)

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Total keys:     %s\n", humanize.Comma(int64(totalKeys)))
	fmt.Printf("  Batch size:     %s\n", humanize.Comma(int64(batchSize)))
	fmt.Printf("  Pipeline depth: %d\n", pipelineDepth)
	fmt.Printf("  Bins:           %d (2^13)\n", numBins)
	fmt.Printf("\n")

	// Create HistoryFile
	hf, err := NewHistoryFile(numBins, directory)
	assert.NoError(t, err, "failed to create HistoryFile")

	// Create and start the key pipeline
	pipeline := NewKeyPipeline(totalKeys, batchSize, pipelineDepth)
	pipeline.SortForHistory = true
	pipeline.Start()
	defer pipeline.Stop()

	// Write Phase
	fmt.Println("=== WRITE PHASE ===")
	startWrite := time.Now()
	batchesWritten := 0

	for batchesWritten < pipeline.TotalBatches {
		batch, ok := pipeline.GetBatch()
		if !ok {
			break
		}

		// Sort for HistoryFile
		batch.SortForHistoryFile(hf)

		// Write to history
		writeStart := time.Now()
		err := hf.AddKeys(batch.Buffer)
		assert.NoError(t, err, "AddKeys failed")
		writeTime := time.Since(writeStart)

		batchesWritten++

		// Report progress
		if batchesWritten%10 == 0 {
			stats := pipeline.Stats()
			tps := float64(batchSize) / writeTime.Seconds()
			usPerWrite := writeTime.Microseconds() / int64(batchSize)

			fmt.Printf("Batch %3d: Write %6.2fms = %.0f TPS (%d μs/write) | Pipeline: %s\n",
				batchesWritten,
				writeTime.Seconds()*1000,
				tps,
				usPerWrite,
				stats.String())
		}
	}

	writeTime := time.Since(startWrite)
	fmt.Printf("\nWrite complete: %s keys in %.2fs = %.0f keys/sec\n",
		humanize.Comma(int64(totalKeys)),
		writeTime.Seconds(),
		float64(totalKeys)/writeTime.Seconds())

	// Sort phase
	fmt.Println("\n=== SORT PHASE ===")
	sortStart := time.Now()
	err = hf.SortAllKeySets()
	assert.NoError(t, err, "SortAllKeySets failed")
	sortTime := time.Since(sortStart)
	fmt.Printf("Sort complete in %.2fs\n", sortTime.Seconds())

	// Read test with new pipeline for verification
	fmt.Println("\n=== READ VERIFICATION ===")
	testReadsWithPipeline(t, hf)

	// Statistics
	fmt.Println("\n=== STATISTICS ===")
	finalStats := pipeline.Stats()
	fmt.Printf("Pipeline generation rate: %.0f keys/sec\n", finalStats.KeysPerSecond)
	fmt.Printf("Database write rate:      %.0f keys/sec\n", float64(totalKeys)/writeTime.Seconds())
	fmt.Printf("Pipeline efficiency:      %.1f%%\n",
		(float64(totalKeys)/writeTime.Seconds())/finalStats.KeysPerSecond*100)
}

// testReadsWithPipeline uses a small pipeline to verify reads
func testReadsWithPipeline(t *testing.T, hf *HistoryFile) {
	// Create small pipeline for read testing
	readPipeline := NewKeyPipeline(100_000, 10_000, 5)
	readPipeline.Start()
	defer readPipeline.Stop()

	totalReads := 0
	found := 0
	notFound := 0
	startRead := time.Now()

	// Read first 10 batches
	for i := 0; i < 10; i++ {
		batch, ok := readPipeline.GetBatch()
		if !ok {
			break
		}

		batchStart := time.Now()
		batchFound := 0

		for _, key := range batch.Keys {
			_, err := hf.Get(key)
			if err == nil {
				found++
				batchFound++
			} else {
				notFound++
			}
			totalReads++
		}

		batchTime := time.Since(batchStart)
		readsPerSec := float64(len(batch.Keys)) / batchTime.Seconds()
		usPerRead := batchTime.Microseconds() / int64(len(batch.Keys))

		fmt.Printf("Read batch %2d: %d keys @ %.0f reads/sec (%d μs/read), found: %d/%d\n",
			i+1, len(batch.Keys), readsPerSec, usPerRead, batchFound, len(batch.Keys))
	}

	totalTime := time.Since(startRead)
	fmt.Printf("\nTotal: %d reads in %.2fs = %.0f reads/sec (found: %d, not found: %d)\n",
		totalReads, totalTime.Seconds(), float64(totalReads)/totalTime.Seconds(),
		found, notFound)
}

// TestPipelinePerformance tests the pipeline itself
func TestPipelinePerformance(t *testing.T) {
	fmt.Println("=== KEY PIPELINE PERFORMANCE TEST ===")

	configurations := []struct {
		name          string
		totalKeys     int
		batchSize     int
		pipelineDepth int
	}{
		{"Small batches", 1_000_000, 1_000, 10},
		{"Medium batches", 1_000_000, 10_000, 20},
		{"Large batches", 1_000_000, 100_000, 5},
		{"Deep pipeline", 1_000_000, 10_000, 50},
		{"Shallow pipeline", 1_000_000, 10_000, 5},
	}

	for _, config := range configurations {
		fmt.Printf("\n--- %s ---\n", config.name)
		fmt.Printf("Total keys: %s, Batch size: %s, Pipeline depth: %d\n",
			humanize.Comma(int64(config.totalKeys)),
			humanize.Comma(int64(config.batchSize)),
			config.pipelineDepth)

		pipeline := NewKeyPipeline(config.totalKeys, config.batchSize, config.pipelineDepth)
		pipeline.Start()

		start := time.Now()
		batchesConsumed := 0

		// Consume all batches
		for batchesConsumed < pipeline.TotalBatches {
			_, ok := pipeline.GetBatchWithTimeout(100 * time.Millisecond)
			if !ok {
				fmt.Println("Timeout waiting for batch")
				break
			}

			// Simulate work (would normally process the batch here)
			time.Sleep(1 * time.Millisecond)
			batchesConsumed++

			if batchesConsumed%(pipeline.TotalBatches/10) == 0 {
				stats := pipeline.Stats()
				elapsed := time.Since(start)
				fmt.Printf("  Progress: %d/%d batches, %.0f keys/sec, queue: %d\n",
					batchesConsumed, pipeline.TotalBatches,
					float64(batchesConsumed*config.batchSize)/elapsed.Seconds(),
					stats.QueueDepth)
			}
		}

		pipeline.Stop()

		elapsed := time.Since(start)
		throughput := float64(config.totalKeys) / elapsed.Seconds()
		fmt.Printf("  Completed: %.2fs, %.0f keys/sec\n", elapsed.Seconds(), throughput)
	}
}

// TestPipelinedBinaryTree tests the binary tree storage with pipeline
func TestPipelinedBinaryTree(t *testing.T) {
	directory := "/tmp/BinaryTreePipelined"
	os.RemoveAll(directory)

	fmt.Println("=== BINARY TREE WITH KEY PIPELINE ===")

	const (
		totalKeys     = 1_000_000
		batchSize     = 10_000
		pipelineDepth = 20
	)

	// Create binary tree storage
	bts, err := NewBinaryTreeStorageV2(directory)
	assert.NoError(t, err)
	defer bts.Close()

	// Create pipeline
	pipeline := NewKeyPipeline(totalKeys, batchSize, pipelineDepth)
	pipeline.Start()
	defer pipeline.Stop()

	fmt.Printf("Writing %s keys with pipeline...\n", humanize.Comma(int64(totalKeys)))
	startTime := time.Now()
	keysWritten := atomic.Int64{}

	// Consume and write batches
	for i := 0; i < pipeline.TotalBatches; i++ {
		batch, ok := pipeline.GetBatch()
		if !ok {
			break
		}

		// Write keys to binary tree
		for j, key := range batch.Keys {
			offset := uint64(batch.BatchID*batchSize+j) * 1024
			length := uint64(256)
			err := bts.AddKey(key, offset, length)
			assert.NoError(t, err)
			keysWritten.Add(1)
		}

		if (i+1)%10 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(keysWritten.Load()) / elapsed.Seconds()
			fmt.Printf("  Progress: %d batches, %.0f keys/sec, %s\n",
				i+1, rate, bts.Stats())
		}
	}

	// Final flush
	select {
	case bts.flushSignal <- struct{}{}:
	default:
	}
	time.Sleep(200 * time.Millisecond)

	totalTime := time.Since(startTime)
	fmt.Printf("\nWrite complete: %s keys in %.2fs = %.0f keys/sec\n",
		humanize.Comma(keysWritten.Load()),
		totalTime.Seconds(),
		float64(keysWritten.Load())/totalTime.Seconds())
	fmt.Printf("Final: %s\n", bts.Stats())
}