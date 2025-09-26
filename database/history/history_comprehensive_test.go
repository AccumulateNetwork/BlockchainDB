package history

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AccumulateNetwork/BlockchainDB/database/utils"
	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

// TestHistoryComprehensive provides the most realistic test of HistoryFile performance
// It tests:
// 1. Bulk sequential writes (simulating blockchain sync)
// 2. Random reads (simulating queries)
// 3. Mixed read/write workload (simulating normal operation)
// 4. Hot key access patterns (simulating popular addresses)
func TestHistoryComprehensive(t *testing.T) {
	directory := "/tmp/HistoryComprehensive"
	os.RemoveAll(directory)

	fmt.Println("=== COMPREHENSIVE HISTORY FILE TEST ===")
	fmt.Println("This test simulates realistic blockchain database usage patterns")
	fmt.Println()

	// Configuration - realistic for blockchain use
	const (
		totalKeys      = 1_000_000 // 1M keys (reduced from 200M for faster testing)
		batchSize      = 100_000     // 100K keys per batch (typical block batch)
		numBins        = 4096        // 2^12 bins for good distribution
		readTestKeys   = 100_000     // Number of keys to test reading
		hotKeyRatio    = 0.2         // 20% of reads go to hot keys
		hotKeyPoolSize = 1000        // Number of hot keys
	)

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Total keys:      %s\n", humanize.Comma(int64(totalKeys)))
	fmt.Printf("  Batch size:      %s\n", humanize.Comma(int64(batchSize)))
	fmt.Printf("  Bins:            %d (2^12)\n", numBins)
	fmt.Printf("  Read test size:  %s keys\n", humanize.Comma(int64(readTestKeys)))
	fmt.Printf("  Hot key ratio:   %.0f%%\n", hotKeyRatio*100)
	fmt.Printf("\n")

	// Create HistoryFile
	hf, err := NewHistoryFile(numBins, directory)
	assert.NoError(t, err, "failed to create HistoryFile")

	// Phase 1: Bulk Write Test (simulating initial sync)
	fmt.Println("=== PHASE 1: BULK WRITE (Initial Sync Simulation) ===")

	allKeys := make([][32]byte, 0, totalKeys)
	startWrite := time.Now()

	for batch := 0; batch < totalKeys/batchSize; batch++ {
		batchStart := time.Now()

		// Generate batch of keys with deterministic seed
		seed := []byte{
			byte(batch),
			byte(batch >> 8),
			byte(batch >> 16),
			byte(batch >> 24),
		}
		fr := utils.NewFastRandom(seed)

		// Generate and sort batch for this write
		batchKeys := make([]utils.DBBKeyFull, batchSize)

		// Generate keys
		for i := 0; i < batchSize; i++ {
			key := fr.NextHash()
			allKeys = append(allKeys, key)

			batchKeys[i] = utils.DBBKeyFull{
				Key: key,
				utils.DBBKey: utils.DBBKey{
					Offset: uint64(batch*batchSize+i) * 1024, // Simulated offset
					Length: uint64(256 + rand.Intn(1024)),    // Variable length data
				},
			}
		}

		// Sort by HistoryFile index (bin)
		sort.Slice(batchKeys, func(i, j int) bool {
			idxI := hf.Index(batchKeys[i].Key)
			idxJ := hf.Index(batchKeys[j].Key)
			if idxI != idxJ {
				return idxI < idxJ
			}
			// Within same bin, sort by key
			return string(batchKeys[i].Key[:]) < string(batchKeys[j].Key[:])
		})

		// Build sorted buffer
		keyBuffer := make([]byte, 0, batchSize*utils.DBKeyFullSize)
		for _, dbKey := range batchKeys {
			keyBuffer = append(keyBuffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
		}

		// Write batch to history
		err := hf.AddKeys(keyBuffer)
		assert.NoError(t, err, "AddKeys failed")

		batchTime := time.Since(batchStart)
		writeRate := float64(batchSize) / batchTime.Seconds()
		usPerWrite := batchTime.Microseconds() / int64(batchSize)

		if (batch+1)%10 == 0 {
			elapsed := time.Since(startWrite)
			totalRate := float64((batch+1)*batchSize) / elapsed.Seconds()
			fmt.Printf("Batch %3d: %s @ %.0f keys/sec (%d μs/write) | Total: %.0f keys/sec\n",
				batch+1, humanize.Comma(int64(batchSize)), writeRate, usPerWrite, totalRate)
		}
	}

	// Sort all KeySets for efficient reading
	fmt.Println("\nSorting KeySets for binary search...")
	sortStart := time.Now()
	err = hf.SortAllKeySets()
	assert.NoError(t, err, "SortAllKeySets failed")
	sortTime := time.Since(sortStart)

	writeTime := time.Since(startWrite)
	fmt.Printf("\n=== WRITE PHASE COMPLETE ===\n")
	fmt.Printf("Total keys written:  %s\n", humanize.Comma(int64(totalKeys)))
	fmt.Printf("Total write time:    %.2fs\n", writeTime.Seconds())
	fmt.Printf("Write throughput:    %.0f keys/sec\n", float64(totalKeys)/writeTime.Seconds())
	fmt.Printf("Sort time:          %.2fs\n", sortTime.Seconds())
	fmt.Printf("\n")

	// Phase 2: Random Read Test (simulating queries)
	fmt.Println("=== PHASE 2: RANDOM READS (Query Simulation) ===")
	fmt.Println("Reading keys that EXIST in the database (100% hit rate expected)")

	// Select random subset of keys for reading - these are guaranteed to exist
	readKeys := make([][32]byte, readTestKeys)
	for i := 0; i < readTestKeys; i++ {
		readKeys[i] = allKeys[rand.Intn(len(allKeys))]
	}

	// Perform cold reads (no cache warming)
	fmt.Println("\n--- Cold Read Test (no cache, first access) ---")
	fmt.Println("Testing: Reading 10,000 existing keys without cache")
	coldReadTest(t, hf, readKeys[:10000])

	// Perform warm reads (cache warmed up)
	fmt.Println("\n--- Warm Read Test (with cache) ---")
	fmt.Println("Testing: Reading same 10,000 keys again (should be cached)")
	warmReadTest(t, hf, readKeys[:10000])

	// Phase 3: Hot Key Pattern Test
	fmt.Println("\n=== PHASE 3: HOT KEY ACCESS PATTERN ===")

	// Select hot keys (frequently accessed)
	hotKeys := make([][32]byte, hotKeyPoolSize)
	for i := 0; i < hotKeyPoolSize; i++ {
		hotKeys[i] = allKeys[rand.Intn(1000)] // First 1000 keys are "hot"
	}

	hotKeyTest(t, hf, hotKeys, readTestKeys)

	// Phase 4: Mixed Workload Test
	fmt.Println("\n=== PHASE 4: MIXED READ/WRITE WORKLOAD ===")
	mixedWorkloadTest(t, hf, allKeys)

	// Phase 5: Concurrent Access Test
	fmt.Println("\n=== PHASE 5: CONCURRENT ACCESS TEST ===")
	concurrentAccessTest(t, hf, allKeys)

	// Final Statistics
	fmt.Println("\n=== FINAL STATISTICS ===")
	printHistoryStats(hf)
}

// coldReadTest tests read performance without cache warming
func coldReadTest(_ *testing.T, hf *HistoryFile, keys [][32]byte) {
	startTime := time.Now()
	found := 0
	notFound := 0

	// Track individual read times for percentiles
	readTimes := make([]time.Duration, 0, len(keys))

	for i, key := range keys {
		readStart := time.Now()
		_, err := hf.Get(key)
		readTime := time.Since(readStart)
		readTimes = append(readTimes, readTime)

		if err == nil {
			found++
		} else {
			notFound++
		}

		if (i+1)%1000 == 0 {
			elapsed := time.Since(startTime)
			avgUsPerRead := elapsed.Microseconds() / int64(i+1)

			// Calculate last 1000 reads average
			last1000Start := time.Duration(0)
			for j := i - 999; j <= i; j++ {
				last1000Start += readTimes[j]
			}
			last1000Avg := last1000Start / 1000

			fmt.Printf("  %5d reads: Avg %d μs/read, Last 1000: %d μs/read, Found: %d/%d (%.0f%%)\n",
				i+1, avgUsPerRead, last1000Avg.Microseconds(), found, i+1,
				float64(found)/float64(i+1)*100)
		}
	}

	totalTime := time.Since(startTime)
	avgMicros := totalTime.Microseconds() / int64(len(keys))

	fmt.Printf("\n  COLD READ SUMMARY:\n")
	fmt.Printf("    Total reads:     %d\n", len(keys))
	fmt.Printf("    Total time:      %.3fs\n", totalTime.Seconds())
	fmt.Printf("    Average latency: %d μs/read\n", avgMicros)
	fmt.Printf("    Throughput:      %.0f reads/sec\n", float64(len(keys))/totalTime.Seconds())
	fmt.Printf("    Hit rate:        %.1f%% (%d found, %d not found)\n",
		float64(found)/float64(len(keys))*100, found, notFound)
}

// warmReadTest tests read performance with warmed cache
func warmReadTest(_ *testing.T, hf *HistoryFile, keys [][32]byte) {
	// Warm up cache first
	for _, key := range keys[:100] {
		hf.Get(key)
	}

	startTime := time.Now()
	found := 0
	notFound := 0

	// Track individual read times
	readTimes := make([]time.Duration, 0, len(keys))

	for i, key := range keys {
		readStart := time.Now()
		_, err := hf.Get(key)
		readTime := time.Since(readStart)
		readTimes = append(readTimes, readTime)

		if err == nil {
			found++
		} else {
			notFound++
		}

		if (i+1)%1000 == 0 {
			elapsed := time.Since(startTime)
			avgUsPerRead := elapsed.Microseconds() / int64(i+1)

			// Calculate last 1000 reads average
			last1000Start := time.Duration(0)
			for j := i - 999; j <= i; j++ {
				last1000Start += readTimes[j]
			}
			last1000Avg := last1000Start / 1000

			fmt.Printf("  %5d reads: Avg %d μs/read, Last 1000: %d μs/read, Found: %d/%d (%.0f%%)\n",
				i+1, avgUsPerRead, last1000Avg.Microseconds(), found, i+1,
				float64(found)/float64(i+1)*100)
		}
	}

	totalTime := time.Since(startTime)
	avgMicros := totalTime.Microseconds() / int64(len(keys))

	fmt.Printf("\n  WARM READ SUMMARY:\n")
	fmt.Printf("    Total reads:     %d\n", len(keys))
	fmt.Printf("    Total time:      %.3fs\n", totalTime.Seconds())
	fmt.Printf("    Average latency: %d μs/read\n", avgMicros)
	fmt.Printf("    Throughput:      %.0f reads/sec\n", float64(len(keys))/totalTime.Seconds())
	fmt.Printf("    Hit rate:        %.1f%% (%d found, %d not found)\n",
		float64(found)/float64(len(keys))*100, found, notFound)
}

// hotKeyTest simulates realistic access patterns with hot keys
func hotKeyTest(_ *testing.T, hf *HistoryFile, hotKeys [][32]byte, numReads int) {
	fmt.Printf("Testing with %d hot keys (80%% of reads) and random keys (20%% of reads)\n",
		len(hotKeys))

	startTime := time.Now()
	hotReads := 0
	coldReads := 0
	hotHits := 0
	coldHits := 0

	// Track hot and cold read times separately
	hotReadTimes := make([]time.Duration, 0)
	coldReadTimes := make([]time.Duration, 0)

	for i := 0; i < numReads; i++ {
		var key [32]byte
		isHot := false

		if rand.Float64() < 0.8 { // 80% hot key access
			key = hotKeys[rand.Intn(len(hotKeys))]
			isHot = true
			hotReads++
		} else { // 20% random access
			// Generate random key (might not exist)
			fr := utils.NewFastRandom([]byte{byte(rand.Int())})
			key = fr.NextHash()
			coldReads++
		}

		readStart := time.Now()
		_, err := hf.Get(key)
		readTime := time.Since(readStart)

		if isHot {
			hotReadTimes = append(hotReadTimes, readTime)
			if err == nil {
				hotHits++
			}
		} else {
			coldReadTimes = append(coldReadTimes, readTime)
			if err == nil {
				coldHits++
			}
		}

		if (i+1)%10000 == 0 {
			elapsed := time.Since(startTime)
			avgUsPerRead := elapsed.Microseconds() / int64(i+1)
			fmt.Printf("  %6d reads: %d μs/read avg, Hot: %d/%d (%.0f%% hit), Cold: %d/%d (%.0f%% hit)\n",
				i+1, avgUsPerRead,
				hotHits, hotReads, float64(hotHits)/float64(hotReads)*100,
				coldHits, coldReads, float64(coldHits)/float64(coldReads)*100)
		}
	}

	totalTime := time.Since(startTime)
	avgMicros := totalTime.Microseconds() / int64(numReads)

	// Calculate averages for hot and cold reads
	hotAvg := time.Duration(0)
	for _, d := range hotReadTimes {
		hotAvg += d
	}
	if len(hotReadTimes) > 0 {
		hotAvg = hotAvg / time.Duration(len(hotReadTimes))
	}

	coldAvg := time.Duration(0)
	for _, d := range coldReadTimes {
		coldAvg += d
	}
	if len(coldReadTimes) > 0 {
		coldAvg = coldAvg / time.Duration(len(coldReadTimes))
	}

	fmt.Printf("\n  HOT KEY TEST SUMMARY:\n")
	fmt.Printf("    Total reads:      %d\n", numReads)
	fmt.Printf("    Average latency:  %d μs/read\n", avgMicros)
	fmt.Printf("    Hot reads:        %d (avg %d μs, %.0f%% hit rate)\n",
		hotReads, hotAvg.Microseconds(), float64(hotHits)/float64(hotReads)*100)
	fmt.Printf("    Cold reads:       %d (avg %d μs, %.0f%% hit rate)\n",
		coldReads, coldAvg.Microseconds(), float64(coldHits)/float64(coldReads)*100)
	fmt.Printf("    Throughput:       %.0f reads/sec\n", float64(numReads)/totalTime.Seconds())
}

// mixedWorkloadTest simulates realistic mixed read/write operations
func mixedWorkloadTest(_ *testing.T, hf *HistoryFile, existingKeys [][32]byte) {
	fmt.Println("Simulating mixed 70% read, 30% write workload...")

	const operations = 100_000
	writeBuffer := make([]byte, 0, 1000*utils.DBKeyFullSize)
	writes := 0
	reads := 0
	readHits := 0

	// Track operation times
	readTimes := make([]time.Duration, 0)
	writeTimes := make([]time.Duration, 0)

	startTime := time.Now()

	for i := 0; i < operations; i++ {
		if rand.Float64() < 0.7 { // 70% reads
			key := existingKeys[rand.Intn(len(existingKeys))]
			readStart := time.Now()
			_, err := hf.Get(key)
			readTime := time.Since(readStart)
			readTimes = append(readTimes, readTime)
			reads++
			if err == nil {
				readHits++
			}
		} else { // 30% writes
			fr := utils.NewFastRandom([]byte{byte(i), byte(i >> 8)})
			key := fr.NextHash()

			var dbKey utils.DBBKeyFull
			dbKey.Key = key
			dbKey.DBBKey.Offset = uint64(i) * 1024
			dbKey.DBBKey.Length = uint64(256)

			writeBuffer = append(writeBuffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
			writes++

			// Flush buffer periodically
			if len(writeBuffer) >= 100*utils.DBKeyFullSize {
				writeStart := time.Now()
				hf.AddKeys(writeBuffer)
				writeTime := time.Since(writeStart)
				writeTimes = append(writeTimes, writeTime/100) // Per-key write time
				writeBuffer = writeBuffer[:0]
			}
		}

		if (i+1)%10000 == 0 {
			elapsed := time.Since(startTime)
			opsPerSec := float64(i+1) / elapsed.Seconds()
			avgUsPerOp := elapsed.Microseconds() / int64(i+1)
			fmt.Printf("  %6d ops: %.0f ops/sec (%d μs/op avg), Reads: %d (%.0f%% hit), Writes: %d\n",
				i+1, opsPerSec, avgUsPerOp, reads,
				float64(readHits)/float64(reads)*100, writes)
		}
	}

	// Flush remaining writes
	if len(writeBuffer) > 0 {
		writeStart := time.Now()
		hf.AddKeys(writeBuffer)
		writeTime := time.Since(writeStart)
		writeTimes = append(writeTimes, writeTime/time.Duration(len(writeBuffer)/utils.DBKeyFullSize))
	}

	totalTime := time.Since(startTime)

	// Calculate averages
	readAvg := time.Duration(0)
	for _, d := range readTimes {
		readAvg += d
	}
	if len(readTimes) > 0 {
		readAvg = readAvg / time.Duration(len(readTimes))
	}

	writeAvg := time.Duration(0)
	for _, d := range writeTimes {
		writeAvg += d
	}
	if len(writeTimes) > 0 {
		writeAvg = writeAvg / time.Duration(len(writeTimes))
	}

	fmt.Printf("\n  MIXED WORKLOAD SUMMARY:\n")
	fmt.Printf("    Total operations: %d\n", operations)
	fmt.Printf("    Total time:       %.3fs\n", totalTime.Seconds())
	fmt.Printf("    Throughput:       %.0f ops/sec\n", float64(operations)/totalTime.Seconds())
	fmt.Printf("    Reads:            %d (avg %d μs, %.0f%% hit rate)\n",
		reads, readAvg.Microseconds(), float64(readHits)/float64(reads)*100)
	fmt.Printf("    Writes:           %d (avg %d μs per key)\n", writes, writeAvg.Microseconds())
}

// concurrentAccessTest tests thread-safe concurrent access
func concurrentAccessTest(_ *testing.T, hf *HistoryFile, keys [][32]byte) {
	fmt.Println("Testing concurrent access with multiple readers and writers...")

	const (
		numReaders = 10
		numWriters = 2
		duration   = 5 * time.Second
	)

	var (
		totalReads  atomic.Int64
		totalWrites atomic.Int64
		stop        atomic.Bool
	)

	var wg sync.WaitGroup

	// Start readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for !stop.Load() {
				key := keys[rand.Intn(len(keys))]
				hf.Get(key)
				totalReads.Add(1)
			}
		}(r)
	}

	// Start writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			buffer := make([]byte, 0, 100*utils.DBKeyFullSize)
			batchID := 0

			for !stop.Load() {
				// Build a small batch
				for i := 0; i < 100; i++ {
					fr := utils.NewFastRandom([]byte{byte(id), byte(batchID), byte(i)})
					key := fr.NextHash()

					var dbKey utils.DBBKeyFull
					dbKey.Key = key
					dbKey.DBBKey.Offset = uint64(batchID*100+i) * 1024
					dbKey.DBBKey.Length = uint64(256)

					buffer = append(buffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
				}

				hf.AddKeys(buffer)
				buffer = buffer[:0]
				totalWrites.Add(100)
				batchID++
			}
		}(w)
	}

	// Run for specified duration
	startTime := time.Now()
	time.Sleep(duration)
	stop.Store(true)

	wg.Wait()

	elapsed := time.Since(startTime)
	reads := totalReads.Load()
	writes := totalWrites.Load()

	fmt.Printf("  Concurrent test results:\n")
	fmt.Printf("    Duration:     %.1fs\n", elapsed.Seconds())
	fmt.Printf("    Total reads:  %s (%.0f/sec)\n",
		humanize.Comma(reads), float64(reads)/elapsed.Seconds())
	fmt.Printf("    Total writes: %s (%.0f/sec)\n",
		humanize.Comma(writes), float64(writes)/elapsed.Seconds())
	fmt.Printf("    Combined:     %.0f ops/sec\n",
		float64(reads+writes)/elapsed.Seconds())
}

// printHistoryStats prints detailed statistics about the HistoryFile
func printHistoryStats(hf *HistoryFile) {
	totalKeys := 0
	maxKeysInBin := 0
	minKeysInBin := int(^uint(0) >> 1) // Max int
	emptyBins := 0

	for _, ks := range hf.KeySets {
		numKeys := int((ks.End - ks.Start) / uint64(utils.DBKeyFullSize))
		totalKeys += numKeys

		if numKeys > maxKeysInBin {
			maxKeysInBin = numKeys
		}
		if numKeys < minKeysInBin && numKeys > 0 {
			minKeysInBin = numKeys
		}
		if numKeys == 0 {
			emptyBins++
		}
	}

	avgKeysPerBin := totalKeys / len(hf.KeySets)

	fmt.Printf("HistoryFile Statistics:\n")
	fmt.Printf("  Total bins:        %d\n", len(hf.KeySets))
	fmt.Printf("  Total keys:        %s\n", humanize.Comma(int64(totalKeys)))
	fmt.Printf("  Empty bins:        %d (%.1f%%)\n",
		emptyBins, float64(emptyBins)/float64(len(hf.KeySets))*100)
	fmt.Printf("  Avg keys/bin:      %d\n", avgKeysPerBin)
	fmt.Printf("  Max keys in bin:   %d\n", maxKeysInBin)
	fmt.Printf("  Min keys in bin:   %d\n", minKeysInBin)
	fmt.Printf("  Load factor:       %.2f\n",
		float64(maxKeysInBin)/float64(avgKeysPerBin))
}
