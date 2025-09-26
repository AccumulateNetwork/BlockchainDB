package history

import (
	"fmt"
	"os"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

// TestHistoryMetricsDemo demonstrates the improved metrics output in a quick test
func TestHistoryMetricsDemo(t *testing.T) {
	directory := "/tmp/HistoryMetricsDemo"
	os.RemoveAll(directory)

	fmt.Println("=== HISTORY FILE METRICS DEMONSTRATION ===")
	fmt.Println("This test demonstrates clear operation latency metrics")
	fmt.Println()

	// Small configuration for quick demo
	const (
		totalKeys    = 100_000 // 100K keys for quick demo
		batchSize    = 10_000  // 10K per batch
		numBins      = 256     // Small number of bins for demo
		readTestKeys = 10_000  // Number of keys to test reading
	)

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Total keys:      %s\n", humanize.Comma(int64(totalKeys)))
	fmt.Printf("  Batch size:      %s\n", humanize.Comma(int64(batchSize)))
	fmt.Printf("  Bins:            %d\n", numBins)
	fmt.Printf("  Read test size:  %s keys\n", humanize.Comma(int64(readTestKeys)))
	fmt.Printf("\n")

	// Create HistoryFile
	hf, err := NewHistoryFile(numBins, directory)
	assert.NoError(t, err, "failed to create HistoryFile")

	// Create key pipeline for clean generation
	pipeline := NewKeyPipeline(totalKeys, batchSize, 5)
	pipeline.SortForHistory = true
	pipeline.Start()
	defer pipeline.Stop()

	fmt.Println("=== WRITE PHASE WITH DETAILED METRICS ===")
	fmt.Println("Showing microseconds per write operation")
	fmt.Println()

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

		// Detailed metrics per batch
		tps := float64(batchSize) / writeTime.Seconds()
		usPerWrite := writeTime.Microseconds() / int64(batchSize)

		fmt.Printf("Batch %2d: Write took %6.2fms = %.0f TPS (%d μs/write)\n",
			batchesWritten,
			writeTime.Seconds()*1000,
			tps,
			usPerWrite)
	}

	writeTime := time.Since(startWrite)
	fmt.Printf("\nWrite Summary:\n")
	fmt.Printf("  Total keys:        %s\n", humanize.Comma(int64(totalKeys)))
	fmt.Printf("  Total time:        %.3fs\n", writeTime.Seconds())
	fmt.Printf("  Write throughput:  %.0f keys/sec\n", float64(totalKeys)/writeTime.Seconds())
	fmt.Printf("  Avg latency:       %d μs/write\n", writeTime.Microseconds()/int64(totalKeys))

	// Sort phase
	fmt.Println("\n=== SORT PHASE ===")
	sortStart := time.Now()
	err = hf.SortAllKeySets()
	assert.NoError(t, err, "SortAllKeySets failed")
	sortTime := time.Since(sortStart)
	fmt.Printf("Sort complete in %.3fs\n", sortTime.Seconds())

	// Read phase with detailed metrics
	fmt.Println("\n=== READ PHASE WITH DETAILED METRICS ===")
	fmt.Println("Testing different types of reads with latency measurements")
	fmt.Println()

	// Generate test keys
	readPipeline := NewKeyPipeline(readTestKeys, readTestKeys, 1)
	readPipeline.Start()
	batch, _ := readPipeline.GetBatch()
	readPipeline.Stop()

	// Test 1: Cold reads (first access)
	fmt.Println("1. COLD READS (no cache):")
	coldStart := time.Now()
	found := 0
	notFound := 0

	for i := 0; i < 1000; i++ {
		key := batch.Keys[i]
		readStart := time.Now()
		_, err := hf.Get(key)
		readLatency := time.Since(readStart)

		if err == nil {
			found++
		} else {
			notFound++
		}

		if i == 0 || i == 99 || i == 999 {
			fmt.Printf("   Read %4d: %d μs (cumulative avg: %d μs)\n",
				i+1,
				readLatency.Microseconds(),
				time.Since(coldStart).Microseconds()/int64(i+1))
		}
	}

	coldTime := time.Since(coldStart)
	fmt.Printf("   Summary: 1000 reads in %.3fs = %d μs/read avg, %.0f reads/sec\n",
		coldTime.Seconds(),
		coldTime.Microseconds()/1000,
		1000/coldTime.Seconds())
	fmt.Printf("   Hit rate: %.0f%% (%d found, %d not found)\n",
		float64(found)/10.0, found, notFound)

	// Test 2: Warm reads (cached)
	fmt.Println("\n2. WARM READS (with cache):")
	warmStart := time.Now()
	found = 0

	for i := 0; i < 1000; i++ {
		key := batch.Keys[i] // Same keys as cold test
		readStart := time.Now()
		_, err := hf.Get(key)
		readLatency := time.Since(readStart)

		if err == nil {
			found++
		}

		if i == 0 || i == 99 || i == 999 {
			fmt.Printf("   Read %4d: %d μs (cumulative avg: %d μs)\n",
				i+1,
				readLatency.Microseconds(),
				time.Since(warmStart).Microseconds()/int64(i+1))
		}
	}

	warmTime := time.Since(warmStart)
	fmt.Printf("   Summary: 1000 reads in %.3fs = %d μs/read avg, %.0f reads/sec\n",
		warmTime.Seconds(),
		warmTime.Microseconds()/1000,
		1000/warmTime.Seconds())
	fmt.Printf("   Hit rate: %.0f%% (%d found)\n", float64(found)/10.0, found)

	// Test 3: Random reads (mix of hits and misses)
	fmt.Println("\n3. RANDOM READS (new keys, likely cache misses):")
	randomPipeline := NewKeyPipeline(1000, 1000, 1)
	randomPipeline.Start()
	randomBatch, _ := randomPipeline.GetBatch()
	randomPipeline.Stop()

	randomStart := time.Now()
	found = 0
	notFound = 0

	for i := 0; i < 1000; i++ {
		key := randomBatch.Keys[i]
		readStart := time.Now()
		_, err := hf.Get(key)
		readLatency := time.Since(readStart)

		if err == nil {
			found++
		} else {
			notFound++
		}

		if i == 0 || i == 99 || i == 999 {
			fmt.Printf("   Read %4d: %d μs (cumulative avg: %d μs)\n",
				i+1,
				readLatency.Microseconds(),
				time.Since(randomStart).Microseconds()/int64(i+1))
		}
	}

	randomTime := time.Since(randomStart)
	fmt.Printf("   Summary: 1000 reads in %.3fs = %d μs/read avg, %.0f reads/sec\n",
		randomTime.Seconds(),
		randomTime.Microseconds()/1000,
		1000/randomTime.Seconds())
	fmt.Printf("   Hit rate: %.0f%% (%d found, %d not found)\n",
		float64(found)/10.0, found, notFound)

	// Performance comparison
	fmt.Println("\n=== PERFORMANCE COMPARISON ===")
	fmt.Printf("Cold reads:   %d μs/read\n", coldTime.Microseconds()/1000)
	fmt.Printf("Warm reads:   %d μs/read (%.1fx faster)\n",
		warmTime.Microseconds()/1000,
		float64(coldTime.Microseconds())/float64(warmTime.Microseconds()))
	fmt.Printf("Random reads: %d μs/read\n", randomTime.Microseconds()/1000)

	// Show improvement from cold to warm
	if warmTime < coldTime {
		improvement := (1 - float64(warmTime)/float64(coldTime)) * 100
		fmt.Printf("\nCache effectiveness: %.1f%% reduction in read latency\n", improvement)
	}
}