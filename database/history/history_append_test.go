package history

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AccumulateNetwork/BlockchainDB/database/utils"
	"github.com/stretchr/testify/assert"
)

// TestHistoryAppendPerformance compares the original vs append-only implementations
func TestHistoryAppendPerformance(t *testing.T) {
	fmt.Println("=== HISTORY FILE PERFORMANCE COMPARISON ===")
	fmt.Println("Comparing original (read-modify-write) vs append-only implementations")
	fmt.Println()

	const (
		totalKeys = 1_000_000 // 1M keys for comparison
		batchSize = 10_000    // 10K per batch
		numBins   = 256       // Small number of bins to show the problem clearly
	)

	// Test 1: Original HistoryFile
	fmt.Println("1. ORIGINAL HISTORYFILE (read-modify-write pattern):")
	fmt.Println("   Problem: Each write reads ALL existing keys, appends new ones, writes ALL back")
	fmt.Println()

	dirOriginal := "/tmp/HistoryOriginal"
	os.RemoveAll(dirOriginal)

	hfOriginal, err := NewHistoryFile(numBins, dirOriginal)
	assert.NoError(t, err)

	pipeline1 := utils.NewKeyPipeline(totalKeys, batchSize, 5)
	pipeline1.SortForHistory = true
	pipeline1.Start()

	startOriginal := time.Now()
	batchTimes := []time.Duration{}

	for i := 0; i < pipeline1.TotalBatches; i++ {
		batch, ok := pipeline1.GetBatch()
		if !ok {
			break
		}

		batch.SortForHistoryFile(hfOriginal)

		writeStart := time.Now()
		err := hfOriginal.AddKeys(batch.Buffer)
		assert.NoError(t, err)
		writeTime := time.Since(writeStart)
		batchTimes = append(batchTimes, writeTime)

		if (i+1)%10 == 0 {
			avgTime := time.Duration(0)
			for j := i - 9; j <= i; j++ {
				avgTime += batchTimes[j]
			}
			avgTime /= 10

			tps := float64(batchSize) / avgTime.Seconds()
			usPerWrite := avgTime.Microseconds() / int64(batchSize)

			fmt.Printf("   Batch %3d: Last 10 avg: %6.2fms = %.0f TPS (%d μs/write)\n",
				i+1, avgTime.Seconds()*1000, tps, usPerWrite)
		}
	}

	pipeline1.Stop()
	timeOriginal := time.Since(startOriginal)

	fmt.Printf("\n   Total time: %.2fs\n", timeOriginal.Seconds())
	fmt.Printf("   Throughput: %.0f keys/sec\n", float64(totalKeys)/timeOriginal.Seconds())
	fmt.Printf("   Avg batch:  %.2fms\n", timeOriginal.Seconds()*1000/float64(pipeline1.TotalBatches))

	// Test 2: Append-only HistoryFile
	fmt.Println("\n2. APPEND-ONLY HISTORYFILE (just append new keys):")
	fmt.Println("   Solution: Each write just appends new keys to the bin file")
	fmt.Println()

	dirAppend := "/tmp/HistoryAppend"
	os.RemoveAll(dirAppend)

	hfAppend, err := NewHistoryFileAppend(numBins, dirAppend)
	assert.NoError(t, err)
	defer hfAppend.Close()

	pipeline2 := utils.NewKeyPipeline(totalKeys, batchSize, 5)
	pipeline2.SortForHistory = true
	pipeline2.Start()

	startAppend := time.Now()
	batchTimesAppend := []time.Duration{}

	for i := 0; i < pipeline2.TotalBatches; i++ {
		batch, ok := pipeline2.GetBatch()
		if !ok {
			break
		}

		batch.SortForHistoryFile(hfOriginal) // Use same sorting

		writeStart := time.Now()
		err := hfAppend.AddKeys(batch.Buffer)
		assert.NoError(t, err)
		writeTime := time.Since(writeStart)
		batchTimesAppend = append(batchTimesAppend, writeTime)

		if (i+1)%10 == 0 {
			avgTime := time.Duration(0)
			for j := i - 9; j <= i; j++ {
				avgTime += batchTimesAppend[j]
			}
			avgTime /= 10

			tps := float64(batchSize) / avgTime.Seconds()
			usPerWrite := avgTime.Microseconds() / int64(batchSize)

			fmt.Printf("   Batch %3d: Last 10 avg: %6.2fms = %.0f TPS (%d μs/write) - %s\n",
				i+1, avgTime.Seconds()*1000, tps, usPerWrite, hfAppend.Stats())
		}
	}

	pipeline2.Stop()
	timeAppend := time.Since(startAppend)

	fmt.Printf("\n   Total time: %.2fs\n", timeAppend.Seconds())
	fmt.Printf("   Throughput: %.0f keys/sec\n", float64(totalKeys)/timeAppend.Seconds())
	fmt.Printf("   Avg batch:  %.2fms\n", timeAppend.Seconds()*1000/float64(pipeline2.TotalBatches))

	// Comparison
	fmt.Println("\n=== PERFORMANCE COMPARISON ===")
	fmt.Printf("Original time:    %.2fs (%.0f keys/sec)\n",
		timeOriginal.Seconds(), float64(totalKeys)/timeOriginal.Seconds())
	fmt.Printf("Append-only time: %.2fs (%.0f keys/sec)\n",
		timeAppend.Seconds(), float64(totalKeys)/timeAppend.Seconds())

	speedup := timeOriginal.Seconds() / timeAppend.Seconds()
	fmt.Printf("\nSpeedup: %.1fx faster with append-only approach\n", speedup)

	// Show how batch times change over time
	fmt.Println("\n=== BATCH TIME PROGRESSION ===")
	fmt.Println("Shows how performance degrades with original approach:")
	fmt.Println()
	fmt.Printf("Batch    | Original     | Append-Only  | Difference\n")
	fmt.Printf("---------|--------------|--------------|------------\n")

	for i := 9; i < len(batchTimes); i += 10 {
		origMs := batchTimes[i].Seconds() * 1000
		appendMs := batchTimesAppend[i].Seconds() * 1000
		diff := origMs - appendMs
		fmt.Printf("Batch %3d| %8.2f ms | %8.2f ms | %+7.2f ms\n",
			i+1, origMs, appendMs, diff)
	}

	// Analyze degradation
	firstTenAvg := time.Duration(0)
	lastTenAvg := time.Duration(0)
	for i := 0; i < 10; i++ {
		firstTenAvg += batchTimes[i]
		lastTenAvg += batchTimes[len(batchTimes)-10+i]
	}
	firstTenAvg /= 10
	lastTenAvg /= 10

	fmt.Printf("\nOriginal degradation:\n")
	fmt.Printf("  First 10 batches avg: %.2fms\n", firstTenAvg.Seconds()*1000)
	fmt.Printf("  Last 10 batches avg:  %.2fms\n", lastTenAvg.Seconds()*1000)
	fmt.Printf("  Degradation factor:   %.1fx slower\n", float64(lastTenAvg)/float64(firstTenAvg))

	firstTenAvgAppend := time.Duration(0)
	lastTenAvgAppend := time.Duration(0)
	for i := 0; i < 10; i++ {
		firstTenAvgAppend += batchTimesAppend[i]
		lastTenAvgAppend += batchTimesAppend[len(batchTimesAppend)-10+i]
	}
	firstTenAvgAppend /= 10
	lastTenAvgAppend /= 10

	fmt.Printf("\nAppend-only consistency:\n")
	fmt.Printf("  First 10 batches avg: %.2fms\n", firstTenAvgAppend.Seconds()*1000)
	fmt.Printf("  Last 10 batches avg:  %.2fms\n", lastTenAvgAppend.Seconds()*1000)
	fmt.Printf("  Degradation factor:   %.1fx\n", float64(lastTenAvgAppend)/float64(firstTenAvgAppend))
}

// TestAppendOnlyCorrectness verifies the append-only implementation works correctly
func TestAppendOnlyCorrectness(t *testing.T) {
	fmt.Println("=== APPEND-ONLY CORRECTNESS TEST ===")

	dir := "/tmp/AppendCorrectness"
	os.RemoveAll(dir)

	hf, err := NewHistoryFileAppend(256, dir)
	assert.NoError(t, err)
	defer hf.Close()

	// Write some test keys
	const testKeys = 10000
	pipeline := utils.NewKeyPipeline(testKeys, 1000, 2)
	pipeline.Start()

	writtenKeys := make([][32]byte, 0, testKeys)

	fmt.Println("Writing test keys...")
	for i := 0; i < pipeline.TotalBatches; i++ {
		batch, ok := pipeline.GetBatch()
		if !ok {
			break
		}

		// Build buffer
		buffer := make([]byte, 0, len(batch.Keys)*utils.DBKeyFullSize)
		for j, key := range batch.Keys {
			writtenKeys = append(writtenKeys, key)

			var dbKey utils.DBBKeyFull
			dbKey.Key = key
			dbKey.Offset = uint64(i*1000+j) * 1024
			dbKey.Length = uint64(256)
			buffer = append(buffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
		}

		err := hf.AddKeys(buffer)
		assert.NoError(t, err)
	}

	pipeline.Stop()

	fmt.Printf("Wrote %d keys\n", len(writtenKeys))
	fmt.Println("Sorting bins for efficient lookup...")

	err = hf.SortAllBins()
	assert.NoError(t, err)

	// Test reading back a sample of keys
	fmt.Println("Testing reads...")
	found := 0
	notFound := 0

	for i := 0; i < 100; i++ {
		key := writtenKeys[i*100] // Sample every 100th key
		_, err := hf.Get(key)
		if err == nil {
			found++
		} else {
			notFound++
		}
	}

	fmt.Printf("Read test: %d found, %d not found\n", found, notFound)
	assert.Equal(t, 100, found, "All sampled keys should be found")

	fmt.Println("Stats:", hf.Stats())
	fmt.Println("Correctness test passed!")
}