package blockchainDB

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

// KeyGenerator generates deterministic keys based on seed and range
type KeyGenerator struct {
	batchSize int
	keysPerBatch int
}

// NewKeyGenerator creates a generator that can produce keys for any range
func NewKeyGenerator(batchSize int) *KeyGenerator {
	return &KeyGenerator{
		batchSize: batchSize,
		keysPerBatch: batchSize,
	}
}

// GenerateBatch generates keys for a specific batch number
func (kg *KeyGenerator) GenerateBatch(batchNum int) [][32]byte {
	// Use batch number as seed - ensures different keys for each batch
	seed := []byte{byte(batchNum), byte(batchNum >> 8), byte(batchNum >> 16), byte(batchNum >> 24)}
	fr := NewFastRandom(seed)

	keys := make([][32]byte, kg.keysPerBatch)
	for i := range keys {
		keys[i] = fr.NextHash()
	}
	return keys
}

// GenerateKey generates a single key at a specific global index
func (kg *KeyGenerator) GenerateKey(globalIndex int) [32]byte {
	batchNum := globalIndex / kg.keysPerBatch
	indexInBatch := globalIndex % kg.keysPerBatch

	// Use batch number as seed
	seed := []byte{byte(batchNum), byte(batchNum >> 8), byte(batchNum >> 16), byte(batchNum >> 24)}
	fr := NewFastRandom(seed)

	// Skip to the desired index within the batch
	for i := 0; i < indexInBatch; i++ {
		fr.NextHash()
	}

	return fr.NextHash()
}

// GenerateRange generates keys for a specific range [start, end)
func (kg *KeyGenerator) GenerateRange(start, end int) [][32]byte {
	keys := make([][32]byte, end-start)

	// Group by batch for efficiency
	currentBatch := -1
	var fr *FastRandom

	for i := start; i < end; i++ {
		batchNum := i / kg.keysPerBatch
		indexInBatch := i % kg.keysPerBatch

		// Create new generator if we moved to a new batch
		if batchNum != currentBatch {
			currentBatch = batchNum
			seed := []byte{byte(batchNum), byte(batchNum >> 8), byte(batchNum >> 16), byte(batchNum >> 24)}
			fr = NewFastRandom(seed)

			// Skip to the right position if not starting at beginning of batch
			if i == start && indexInBatch > 0 {
				for j := 0; j < indexInBatch; j++ {
					fr.NextHash()
				}
			}
		}

		keys[i-start] = fr.NextHash()
	}

	return keys
}

// TestSeedBasedGeneration tests the seed-based key generation
func TestSeedBasedGeneration(t *testing.T) {
	kg := NewKeyGenerator(10000) // 10k keys per batch

	// Test that same batch generates same keys
	batch1a := kg.GenerateBatch(1)
	batch1b := kg.GenerateBatch(1)

	matches := 0
	for i := range batch1a {
		if batch1a[i] == batch1b[i] {
			matches++
		}
	}

	if matches != len(batch1a) {
		t.Errorf("Batch generation not deterministic: %d/%d matches", matches, len(batch1a))
	} else {
		t.Logf("✓ Batch generation is deterministic: %d keys match", matches)
	}

	// Test that different batches generate different keys
	batch2 := kg.GenerateBatch(2)
	different := 0
	for i := range batch1a {
		if batch1a[i] != batch2[i] {
			different++
		}
	}

	if different != len(batch1a) {
		t.Errorf("Different batches too similar: only %d/%d different", different, len(batch1a))
	} else {
		t.Logf("✓ Different batches generate different keys: %d keys different", different)
	}

	// Test single key generation matches batch generation
	key5000 := kg.GenerateKey(15000) // Key at index 15000 (batch 1, index 5000)
	batch1 := kg.GenerateBatch(1)

	if key5000 != batch1[5000] {
		t.Error("Single key generation doesn't match batch generation")
	} else {
		t.Log("✓ Single key generation matches batch generation")
	}

	// Test range generation
	rangeKeys := kg.GenerateRange(9998, 10002) // Crosses batch boundary

	// Verify first two keys are from batch 0
	batch0 := kg.GenerateBatch(0)
	if rangeKeys[0] != batch0[9998] || rangeKeys[1] != batch0[9999] {
		t.Error("Range generation failed for batch 0 keys")
	}

	// Verify last two keys are from batch 1
	batch1 = kg.GenerateBatch(1)
	if rangeKeys[2] != batch1[0] || rangeKeys[3] != batch1[1] {
		t.Error("Range generation failed for batch 1 keys")
	} else {
		t.Log("✓ Range generation works across batch boundaries")
	}
}

// TestSeedBasedPerformance tests large-scale performance with seed-based generation
func TestSeedBasedPerformance(t *testing.T) {
	fanOuts := []int{256, 1024, 2048}
	totalEntries := 1_000_000 // 1M entries for demo
	batchSize := 100_000        // 100k per batch = 100 batches total

	t.Logf("=== SEED-BASED PERFORMANCE TEST (%d entries) ===", totalEntries)

	kg := NewKeyGenerator(batchSize)
	numBatches := totalEntries / batchSize

	for _, fanOut := range fanOuts {
		t.Logf("\n=== Testing Fan-Out %d ===", fanOut)

		dir := fmt.Sprintf("/tmp/seed_based_%d", fanOut)
		os.RemoveAll(dir)

		cht, err := NewConfigurableHashTable(dir, fanOut)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// WRITE TEST - Generate and write in batches
		writeStart := time.Now()
		writtenCount := 0

		for batch := 0; batch < numBatches; batch++ {
			// Generate this batch's keys
			keys := kg.GenerateBatch(batch)

			// Write them
			for i, key := range keys {
				value := DBBKey{
					Offset: uint64(batch*batchSize + i),
					Length: 100,
				}
				if err := cht.Put(key, value); err != nil {
					t.Fatalf("Put failed: %v", err)
				}
				writtenCount++
			}

			// Report progress every 10 batches
			if (batch+1)%10 == 0 {
				elapsed := time.Since(writeStart)
				rate := float64(writtenCount) / elapsed.Seconds()
				t.Logf("  Written %d/%d entries (%.0f/sec)", writtenCount, totalEntries, rate)
			}
		}

		writeTime := time.Since(writeStart)
		writeRate := float64(totalEntries) / writeTime.Seconds()
		t.Logf("Write complete: %v (%.0f entries/sec)", writeTime, writeRate)

		// Flush
		cht.flushWriteBuffer()
		time.Sleep(100 * time.Millisecond)

		// READ TEST - Test reading from different parts of keyspace
		t.Log("Testing reads from different regions...")

		testRegions := []struct {
			name  string
			start int
			count int
		}{
			{"Early (0-1000)", 0, 1000},
			{"Middle (500k-501k)", 500_000, 1000},
			{"Late (990k-1M)", 990_000, 10000},
			{"Cross-batch (99k-101k)", 99_000, 2000},
		}

		for _, region := range testRegions {
			readStart := time.Now()
			found := 0

			// Generate keys for this region
			keys := kg.GenerateRange(region.start, region.start+region.count)

			for i, key := range keys {
				expectedOffset := uint64(region.start + i)
				if retrieved, err := cht.Get(key); err == nil {
					if retrieved.Offset == expectedOffset {
						found++
					}
				}
			}

			readTime := time.Since(readStart)
			readRate := float64(found) / readTime.Seconds()
			t.Logf("  %s: %d/%d found (%.0f reads/sec)",
				region.name, found, region.count, readRate)
		}

		// Random access test
		t.Log("Testing random access...")
		randomStart := time.Now()
		randomFound := 0
		randomTests := 1000

		for i := 0; i < randomTests; i++ {
			// Pick a random index
			index := int(time.Now().UnixNano()) % totalEntries
			key := kg.GenerateKey(index)

			if retrieved, err := cht.Get(key); err == nil {
				if retrieved.Offset == uint64(index) {
					randomFound++
				}
			}
		}

		randomTime := time.Since(randomStart)
		randomRate := float64(randomFound) / randomTime.Seconds()
		t.Logf("  Random: %d/%d found (%.0f reads/sec)",
			randomFound, randomTests, randomRate)

		// Memory stats
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("  Memory in use: %.2f MB", float64(m.Alloc)/1024/1024)

		cht.Shutdown()
		os.RemoveAll(dir)
	}
}

// TestBatchVerification verifies that batch generation is working correctly
func TestBatchVerification(t *testing.T) {
	kg := NewKeyGenerator(1000)

	// Generate a few batches and verify uniqueness
	batches := make([][][32]byte, 5)
	for i := range batches {
		batches[i] = kg.GenerateBatch(i)
	}

	// Check for duplicates across batches
	seen := make(map[[32]byte]bool)
	duplicates := 0

	for batchNum, batch := range batches {
		for i, key := range batch {
			if seen[key] {
				duplicates++
				t.Errorf("Duplicate key found: batch %d, index %d", batchNum, i)
			}
			seen[key] = true
		}
	}

	totalKeys := len(batches) * 1000
	t.Logf("Checked %d keys across %d batches: %d duplicates",
		totalKeys, len(batches), duplicates)

	if duplicates == 0 {
		t.Log("✓ All keys are unique across batches")
	}
}