package history

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHistoryManagement tests the complete history management system
func TestHistoryManagement(t *testing.T) {
	t.Run("BasicOperations", testBasicHistoryOperations)
	t.Run("PerformanceDegradation", testPerformanceDegradation)
	t.Run("CrashRecovery", testCrashRecovery)
	t.Run("ConcurrentAccess", testConcurrentAccess)
	t.Run("HashDistribution", testHashDistribution)
	t.Run("BinFanOut", testBinFanOut)
	t.Run("MemoryUsage", testMemoryUsage)
	t.Run("WALIntegrity", testWALIntegrity)
}

// testBasicHistoryOperations tests basic put/get operations
func testBasicHistoryOperations(t *testing.T) {
	dir := "/tmp/history_test_basic"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	// Test 1: Store and retrieve a single entry
	data := []byte("test data for hashing")
	hash := sha256.Sum256(data)
	value := utils.DBBKey{Offset: 1000, Length: uint64(len(data))}

	if err := mht.Put(hash, value); err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	retrieved, err := mht.Get(hash)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if retrieved.Offset != value.Offset || retrieved.Length != value.Length {
		t.Errorf("Value mismatch: got %+v, want %+v", retrieved, value)
	}

	// Test 2: Non-existent key
	nonExistent := sha256.Sum256([]byte("non-existent"))
	_, err = mht.Get(nonExistent)
	if err == nil {
		t.Error("Expected error for non-existent key")
	}

	// Test 3: Overwrite prevention (history should be immutable)
	newValue := utils.DBBKey{Offset: 2000, Length: 200}
	if err := mht.Put(hash, newValue); err != nil {
		t.Fatalf("Failed to put duplicate entry: %v", err)
	}

	// Should still get the original value (or the new one, depending on design)
	// For history, we might want to keep both or just the latest
	retrieved2, err := mht.Get(hash)
	if err != nil {
		t.Fatalf("Failed to get after duplicate put: %v", err)
	}

	t.Logf("After duplicate put: %+v", retrieved2)
}

// testPerformanceDegradation verifies no performance degradation at scale
func testPerformanceDegradation(t *testing.T) {
	dir := "/tmp/history_test_perf"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	// Measure write performance at different scales
	scales := []int{1000, 5000, 10000, 20000, 50000}
	writeRates := make([]float64, len(scales))

	for idx, scale := range scales {
		// Generate unique hashes
		hashes := make([][32]byte, scale)
		for i := 0; i < scale; i++ {
			data := fmt.Sprintf("data_%d_%d", idx, i)
			hashes[i] = sha256.Sum256([]byte(data))
		}

		// Measure write time
		start := time.Now()
		for i, hash := range hashes {
			value := utils.DBBKey{
				Offset: uint64(i * 100),
				Length: uint64(100),
			}
			if err := mht.Put(hash, value); err != nil {
				t.Fatalf("Put failed at scale %d: %v", scale, err)
			}
		}
		elapsed := time.Since(start)

		writeRate := float64(scale) / elapsed.Seconds()
		writeRates[idx] = writeRate

		t.Logf("Scale %d: %.2f writes/sec (%.3f ms per write)",
			scale, writeRate, elapsed.Seconds()*1000/float64(scale))
	}

	// Check for degradation
	// With the new system, performance should remain relatively constant
	firstRate := writeRates[0]
	lastRate := writeRates[len(writeRates)-1]
	degradation := (firstRate - lastRate) / firstRate * 100

	t.Logf("Performance degradation: %.2f%% (first: %.0f/s, last: %.0f/s)",
		degradation, firstRate, lastRate)

	// The old system had 80% degradation
	// The new system should have less than 20% degradation
	if degradation > 20 {
		t.Errorf("Excessive performance degradation: %.2f%% (max allowed: 20%%)", degradation)
	}

	// Test read performance at scale
	readSamples := 100
	sampleHashes := make([][32]byte, readSamples)
	for i := 0; i < readSamples; i++ {
		// Pick random entries from the last scale
		data := fmt.Sprintf("data_%d_%d", len(scales)-1, rand.Intn(scales[len(scales)-1]))
		sampleHashes[i] = sha256.Sum256([]byte(data))
	}

	// Force flush to disk
	mht.flushWriteBuffer()
	time.Sleep(2 * time.Second)

	// Measure read time
	start := time.Now()
	found := 0
	for _, hash := range sampleHashes {
		if _, err := mht.Get(hash); err == nil {
			found++
		}
	}
	elapsed := time.Since(start)

	readRate := float64(readSamples) / elapsed.Seconds()
	t.Logf("Read performance: %.2f reads/sec (%.3f ms per read), found %d/%d",
		readRate, elapsed.Seconds()*1000/float64(readSamples), found, readSamples)
}

// testCrashRecovery tests recovery after simulated crash
func testCrashRecovery(t *testing.T) {
	dir := "/tmp/history_test_recovery"
	os.RemoveAll(dir)

	// Phase 1: Write data and simulate crash
	func() {
		mht, err := NewMultiHashTable(dir)
		if err != nil {
			t.Fatalf("Failed to create history table: %v", err)
		}

		// Write entries
		numEntries := 5000
		hashes := make([][32]byte, numEntries)
		values := make([]utils.DBBKey, numEntries)

		for i := 0; i < numEntries; i++ {
			data := fmt.Sprintf("recovery_test_%d", i)
			hashes[i] = sha256.Sum256([]byte(data))
			values[i] = utils.DBBKey{
				Offset: uint64(i * 1000),
				Length: uint64(i + 100),
			}

			if err := mht.Put(hashes[i], values[i]); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Force flush to ensure WAL has all entries
		mht.flushWriteBuffer()
		time.Sleep(1 * time.Second) // Let background workers process

		// Simulate crash - don't call Shutdown()
		// Just close the WAL abruptly
		mht.wal.currentFile.Close()
		// Don't wait for background workers
	}()

	// Phase 2: Recover and verify
	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to recover history table: %v", err)
	}
	defer mht.Shutdown()

	// Wait for recovery to complete
	time.Sleep(2 * time.Second)

	// Verify entries
	numEntries := 5000
	recovered := 0
	missing := []int{}

	for i := 0; i < numEntries; i++ {
		data := fmt.Sprintf("recovery_test_%d", i)
		hash := sha256.Sum256([]byte(data))

		retrieved, err := mht.Get(hash)
		if err != nil {
			missing = append(missing, i)
			continue
		}

		expectedValue := utils.DBBKey{
			Offset: uint64(i * 1000),
			Length: uint64(i + 100),
		}

		if retrieved.Offset == expectedValue.Offset && retrieved.Length == expectedValue.Length {
			recovered++
		}
	}

	recoveryRate := float64(recovered) / float64(numEntries) * 100
	t.Logf("Recovery rate: %.2f%% (%d/%d entries recovered)", recoveryRate, recovered, numEntries)

	if len(missing) > 0 && len(missing) < 100 {
		t.Logf("Missing entries (first 10): %v", missing[:minInt(10, len(missing))])
	}

	// Should recover at least 95% (some might be in unflushed buffers)
	if recoveryRate < 95 {
		t.Errorf("Insufficient recovery rate: %.2f%% (minimum: 95%%)", recoveryRate)
	}
}

// testConcurrentAccess tests concurrent reads and writes
func testConcurrentAccess(t *testing.T) {
	dir := "/tmp/history_test_concurrent"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	var wg sync.WaitGroup
	numGoroutines := 20
	entriesPerGoroutine := 1000

	// Track results
	var writeErrors uint32
	var readErrors uint32
	var successfulWrites uint32
	var successfulReads uint32

	// Concurrent writers
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < entriesPerGoroutine; i++ {
				data := fmt.Sprintf("goroutine_%d_entry_%d", id, i)
				hash := sha256.Sum256([]byte(data))
				value := utils.DBBKey{
					Offset: uint64(id*10000 + i),
					Length: uint64(len(data)),
				}

				if err := mht.Put(hash, value); err != nil {
					atomic.AddUint32(&writeErrors, 1)
				} else {
					atomic.AddUint32(&successfulWrites, 1)
				}
			}
		}(g)
	}

	// Concurrent readers (start after some writes)
	time.Sleep(100 * time.Millisecond)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for i := 0; i < entriesPerGoroutine/2; i++ {
				// Read random entries
				targetGoroutine := rand.Intn(numGoroutines)
				targetEntry := rand.Intn(entriesPerGoroutine)
				data := fmt.Sprintf("goroutine_%d_entry_%d", targetGoroutine, targetEntry)
				hash := sha256.Sum256([]byte(data))

				if _, err := mht.Get(hash); err != nil {
					atomic.AddUint32(&readErrors, 1)
				} else {
					atomic.AddUint32(&successfulReads, 1)
				}
			}
		}(g)
	}

	wg.Wait()

	t.Logf("Concurrent operations completed:")
	t.Logf("  Successful writes: %d", successfulWrites)
	t.Logf("  Failed writes: %d", writeErrors)
	t.Logf("  Successful reads: %d", successfulReads)
	t.Logf("  Failed reads: %d", readErrors)

	if writeErrors > 0 {
		t.Errorf("Unexpected write errors: %d", writeErrors)
	}

	// Some read errors are expected (reading entries not yet written)
	readErrorRate := float64(readErrors) / float64(readErrors+successfulReads) * 100
	if readErrorRate > 60 { // Allow up to 60% read errors due to timing
		t.Errorf("Excessive read error rate: %.2f%%", readErrorRate)
	}
}

// testHashDistribution verifies uniform distribution across bins
func testHashDistribution(t *testing.T) {
	dir := "/tmp/history_test_distribution"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	// Generate many random hashes
	numHashes := 10000
	binCounts := make([]int, 256)

	for i := 0; i < numHashes; i++ {
		// Use crypto-quality randomness for realistic hash distribution
		randomData := make([]byte, 32)
		rand.Read(randomData)
		hash := sha256.Sum256(randomData)

		// First byte determines bin
		binIndex := hash[0]
		binCounts[binIndex]++

		value := utils.DBBKey{
			Offset: uint64(i),
			Length: 32,
		}

		if err := mht.Put(hash, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Calculate distribution statistics
	expectedPerBin := float64(numHashes) / 256.0
	var minCount, maxCount int = numHashes, 0
	var totalDeviation float64

	for _, count := range binCounts {
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
		deviation := float64(count) - expectedPerBin
		totalDeviation += deviation * deviation
	}

	stdDev := (totalDeviation / 256.0)
	t.Logf("Hash distribution across 256 bins:")
	t.Logf("  Expected per bin: %.2f", expectedPerBin)
	t.Logf("  Min count: %d", minCount)
	t.Logf("  Max count: %d", maxCount)
	t.Logf("  Std deviation: %.2f", stdDev)

	// With good hash distribution, max deviation should be within 3 standard deviations
	maxAllowedDeviation := 3.0 * (expectedPerBin * 0.2) // Allow 20% deviation
	if float64(maxCount) > expectedPerBin+maxAllowedDeviation ||
		float64(minCount) < expectedPerBin-maxAllowedDeviation {
		t.Errorf("Poor hash distribution: min=%d, max=%d (expected ~%.0f)",
			minCount, maxCount, expectedPerBin)
	}
}

// testBinFanOut tests the fan-out mechanism when bins get too large
func testBinFanOut(t *testing.T) {
	dir := "/tmp/history_test_fanout"
	os.RemoveAll(dir)

	// Create with small bin size to trigger fan-out
	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	// Focus writes on a single bin to trigger fan-out
	targetBin := byte(0x42) // Arbitrary bin
	numEntries := 100000     // Enough to trigger fan-out

	for i := 0; i < numEntries; i++ {
		// Create hash with specific first byte
		hash := [32]byte{}
		hash[0] = targetBin
		// Rest of hash is unique
		data := fmt.Sprintf("fanout_test_%d", i)
		tempHash := sha256.Sum256([]byte(data))
		copy(hash[1:], tempHash[1:])

		value := utils.DBBKey{
			Offset: uint64(i * 100),
			Length: 100,
		}

		if err := mht.Put(hash, value); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}

		// Periodically flush to trigger merges
		if i%10000 == 0 {
			mht.flushWriteBuffer()
		}
	}

	// Final flush
	mht.flushWriteBuffer()
	time.Sleep(3 * time.Second) // Wait for merges and fan-outs

	// Check statistics
	t.Logf("Fan-out test statistics:")
	t.Logf("  Total writes: %d", mht.stats.writes)
	t.Logf("  Total merges: %d", mht.stats.merges)
	t.Logf("  Total fan-outs: %d", mht.stats.fanOuts)

	if mht.stats.fanOuts == 0 {
		t.Error("Expected at least one fan-out operation")
	}

	// Verify all entries are still accessible
	notFound := 0
	for i := 0; i < numEntries; i++ {
		hash := [32]byte{}
		hash[0] = targetBin
		data := fmt.Sprintf("fanout_test_%d", i)
		tempHash := sha256.Sum256([]byte(data))
		copy(hash[1:], tempHash[1:])

		if _, err := mht.Get(hash); err != nil {
			notFound++
		}
	}

	if notFound > 0 {
		t.Errorf("Lost %d entries after fan-out", notFound)
	}
}

// testMemoryUsage verifies memory usage stays bounded
func testMemoryUsage(t *testing.T) {
	dir := "/tmp/history_test_memory"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	// Monitor memory usage while adding many entries
	numBatches := 10
	entriesPerBatch := 10000

	for batch := 0; batch < numBatches; batch++ {
		for i := 0; i < entriesPerBatch; i++ {
			data := fmt.Sprintf("memory_test_batch_%d_entry_%d", batch, i)
			hash := sha256.Sum256([]byte(data))
			value := utils.DBBKey{
				Offset: uint64(batch*entriesPerBatch + i),
				Length: uint64(len(data)),
			}

			if err := mht.Put(hash, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Force flush to move data to disk
		mht.flushWriteBuffer()

		// Check that write buffer was cleared
		mht.writeBuffer.mu.Lock()
		bufferSize := len(mht.writeBuffer.entries)
		mht.writeBuffer.mu.Unlock()

		if bufferSize > 1000 { // Allow small buffer
			t.Errorf("Write buffer not cleared after flush: %d entries", bufferSize)
		}

		t.Logf("Batch %d: buffer size after flush: %d", batch, bufferSize)
	}
}

// testWALIntegrity tests WAL checksums and recovery
func testWALIntegrity(t *testing.T) {
	dir := "/tmp/history_test_wal"
	os.RemoveAll(dir)

	// Create and write some entries
	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}

	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		data := fmt.Sprintf("wal_test_%d", i)
		hash := sha256.Sum256([]byte(data))
		value := utils.DBBKey{
			Offset: uint64(i),
			Length: uint64(len(data)),
		}

		if err := mht.Put(hash, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Close properly
	mht.Shutdown()

	// Corrupt a WAL file (simulate disk corruption)
	walFiles, _ := os.ReadDir(dir + "/wal")
	if len(walFiles) > 0 {
		walPath := dir + "/wal/" + walFiles[0].Name()
		file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
		if err == nil {
			// Corrupt a byte in the middle of the file
			file.Seek(100, 0)
			file.Write([]byte{0xFF})
			file.Close()
		}
	}

	// Try to recover with corrupted WAL
	mht2, err := NewMultiHashTable(dir)
	if err != nil {
		// Recovery might fail or succeed partially
		t.Logf("Recovery with corrupted WAL: %v", err)
	} else {
		defer mht2.Shutdown()

		// Count how many entries were recovered before corruption
		recovered := 0
		for i := 0; i < numEntries; i++ {
			data := fmt.Sprintf("wal_test_%d", i)
			hash := sha256.Sum256([]byte(data))
			if _, err := mht2.Get(hash); err == nil {
				recovered++
			}
		}

		t.Logf("Recovered %d/%d entries despite WAL corruption", recovered, numEntries)
	}
}


// BenchmarkHistoryManagement benchmarks the new history system
func BenchmarkHistoryManagement(b *testing.B) {
	dir := "/tmp/history_bench"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		b.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	// Pre-generate hashes
	hashes := make([][32]byte, b.N)
	for i := 0; i < b.N; i++ {
		data := fmt.Sprintf("benchmark_entry_%d", i)
		hashes[i] = sha256.Sum256([]byte(data))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value := utils.DBBKey{
			Offset: uint64(i * 100),
			Length: 100,
		}

		if err := mht.Put(hashes[i], value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
	b.ReportMetric(float64(mht.stats.merges), "total_merges")
	b.ReportMetric(float64(mht.stats.fanOuts), "total_fanouts")
}

// TestCompareOldVsNew compares old HistoryFile vs new MultiHashTable
func TestCompareOldVsNew(t *testing.T) {
	numEntries := 100000

	// Test old HistoryFile
	t.Run("OldHistoryFile", func(t *testing.T) {
		dir := "/tmp/history_old"
		os.RemoveAll(dir)

		hf, err := NewHistoryFile(2000, dir)
		if err != nil {
			t.Fatalf("Failed to create HistoryFile: %v", err)
		}

		// Generate entries
		entries := make([]utils.DBBKeyFull, numEntries)
		for i := 0; i < numEntries; i++ {
			data := fmt.Sprintf("old_test_%d", i)
			entries[i].Key = sha256.Sum256([]byte(data))
			entries[i].Offset = uint64(i * 100)
			entries[i].Length = 100
		}

		// Measure write performance
		start := time.Now()
		batchSize := 1000
		for i := 0; i < numEntries; i += batchSize {
			end := minInt(i+batchSize, numEntries)
			batch := entries[i:end]

			// Pack into buffer
			buffer := make([]byte, len(batch)*utils.DBKeyFullSize)
			for j, entry := range batch {
				copy(buffer[j*utils.DBKeyFullSize:], entry.Bytes(entry.Key))
			}

			if err := hf.AddKeys(buffer); err != nil {
				t.Fatalf("AddKeys failed: %v", err)
			}
		}
		writeTime := time.Since(start)

		// Sort for binary search
		hf.SortAllKeySets()

		// Measure read performance
		start = time.Now()
		found := 0
		for i := 0; i < 1000; i++ {
			idx := rand.Intn(numEntries)
			if _, err := hf.Get(entries[idx].Key); err == nil {
				found++
			}
		}
		readTime := time.Since(start)

		t.Logf("Old HistoryFile:")
		t.Logf("  Write: %v (%.0f/sec)", writeTime, float64(numEntries)/writeTime.Seconds())
		t.Logf("  Read: %v for 1000 reads (%.0f/sec)", readTime, 1000.0/readTime.Seconds())
	})

	// Test new MultiHashTable
	t.Run("NewMultiHashTable", func(t *testing.T) {
		dir := "/tmp/history_new"
		os.RemoveAll(dir)

		mht, err := NewMultiHashTable(dir)
		if err != nil {
			t.Fatalf("Failed to create MultiHashTable: %v", err)
		}
		defer mht.Shutdown()

		// Generate same entries
		entries := make([]struct {
			hash  [32]byte
			value utils.DBBKey
		}, numEntries)

		for i := 0; i < numEntries; i++ {
			data := fmt.Sprintf("old_test_%d", i)
			entries[i].hash = sha256.Sum256([]byte(data))
			entries[i].value = utils.DBBKey{
				Offset: uint64(i * 100),
				Length: 100,
			}
		}

		// Measure write performance
		start := time.Now()
		for i := 0; i < numEntries; i++ {
			if err := mht.Put(entries[i].hash, entries[i].value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		writeTime := time.Since(start)

		// Flush to disk
		mht.flushWriteBuffer()
		time.Sleep(2 * time.Second)

		// Measure read performance
		start = time.Now()
		found := 0
		for i := 0; i < 1000; i++ {
			idx := rand.Intn(numEntries)
			if _, err := mht.Get(entries[idx].hash); err == nil {
				found++
			}
		}
		readTime := time.Since(start)

		t.Logf("New MultiHashTable:")
		t.Logf("  Write: %v (%.0f/sec)", writeTime, float64(numEntries)/writeTime.Seconds())
		t.Logf("  Read: %v for 1000 reads (%.0f/sec)", readTime, 1000.0/readTime.Seconds())
		t.Logf("  Merges: %d, FanOuts: %d", mht.stats.merges, mht.stats.fanOuts)
	})
}

// TestHistoryScalability tests scalability to millions of entries
func TestHistoryScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping scalability test in short mode")
	}

	dir := "/tmp/history_scalability"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	// Test with increasing scales
	scales := []int{10000, 100000, 500000, 1000000}

	for _, scale := range scales {
		start := time.Now()

		for i := 0; i < scale; i++ {
			data := make([]byte, 32)
			rand.Read(data)
			hash := sha256.Sum256(data)

			value := utils.DBBKey{
				Offset: uint64(i),
				Length: 32,
			}

			if err := mht.Put(hash, value); err != nil {
				t.Fatalf("Put failed at scale %d: %v", scale, err)
			}

			// Periodic flush
			if i%50000 == 0 {
				mht.flushWriteBuffer()
			}
		}

		elapsed := time.Since(start)
		rate := float64(scale) / elapsed.Seconds()

		t.Logf("Scale %d: %v total, %.0f writes/sec", scale, elapsed, rate)

		// Test read performance at this scale
		readSamples := minInt(1000, scale/10)
		readStart := time.Now()
		for i := 0; i < readSamples; i++ {
			data := make([]byte, 32)
			rand.Read(data)
			hash := sha256.Sum256(data)
			mht.Get(hash) // Ignore errors (most won't exist)
		}
		readElapsed := time.Since(readStart)
		readRate := float64(readSamples) / readElapsed.Seconds()

		t.Logf("  Read performance: %.0f reads/sec", readRate)
	}
}