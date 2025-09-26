package blockchainDB

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestSimpleHistoryOperations tests basic functionality
func TestSimpleHistoryOperations(t *testing.T) {
	dir := "/tmp/simple_history_test"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create history table: %v", err)
	}
	defer mht.Shutdown()

	// Test 1: Write and read back immediately
	data1 := []byte("Hello, World!")
	hash1 := sha256.Sum256(data1)
	value1 := DBBKey{Offset: 100, Length: uint64(len(data1))}

	if err := mht.Put(hash1, value1); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	retrieved1, err := mht.Get(hash1)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if retrieved1.Offset != value1.Offset || retrieved1.Length != value1.Length {
		t.Errorf("Value mismatch: got %+v, want %+v", retrieved1, value1)
	}

	// Test 2: Write multiple entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		data := fmt.Sprintf("entry_%d", i)
		hash := sha256.Sum256([]byte(data))
		value := DBBKey{Offset: uint64(i * 100), Length: uint64(len(data))}

		if err := mht.Put(hash, value); err != nil {
			t.Fatalf("Failed to put entry %d: %v", i, err)
		}
	}

	// Test 3: Force flush and read back
	mht.flushWriteBuffer()
	time.Sleep(1 * time.Second)

	// Verify all entries
	for i := 0; i < numEntries; i++ {
		data := fmt.Sprintf("entry_%d", i)
		hash := sha256.Sum256([]byte(data))
		expected := DBBKey{Offset: uint64(i * 100), Length: uint64(len(data))}

		retrieved, err := mht.Get(hash)
		if err != nil {
			t.Errorf("Failed to get entry %d: %v", i, err)
			continue
		}

		if retrieved.Offset != expected.Offset || retrieved.Length != expected.Length {
			t.Errorf("Entry %d mismatch: got %+v, want %+v", i, retrieved, expected)
		}
	}

	t.Logf("Test completed successfully:")
	t.Logf("  Writes: %d", mht.stats.writes)
	t.Logf("  Reads: %d", mht.stats.reads)
	t.Logf("  Merges: %d", mht.stats.merges)
}

// TestSimplePerformanceComparison compares old vs new performance
func TestSimplePerformanceComparison(t *testing.T) {
	numEntries := 10000

	// Test new system
	t.Run("NewSystem", func(t *testing.T) {
		dir := "/tmp/perf_new"
		os.RemoveAll(dir)

		mht, err := NewMultiHashTable(dir)
		if err != nil {
			t.Fatalf("Failed to create history table: %v", err)
		}
		defer mht.Shutdown()

		start := time.Now()
		for i := 0; i < numEntries; i++ {
			data := fmt.Sprintf("test_entry_%d", i)
			hash := sha256.Sum256([]byte(data))
			value := DBBKey{Offset: uint64(i), Length: uint64(len(data))}

			if err := mht.Put(hash, value); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		writeTime := time.Since(start)

		writeRate := float64(numEntries) / writeTime.Seconds()
		t.Logf("New System - Writes: %.0f/sec (%.3f ms per write)",
			writeRate, writeTime.Seconds()*1000/float64(numEntries))

		// Test reads
		mht.flushWriteBuffer()
		time.Sleep(1 * time.Second)

		start = time.Now()
		found := 0
		for i := 0; i < 1000; i++ {
			data := fmt.Sprintf("test_entry_%d", i)
			hash := sha256.Sum256([]byte(data))
			if _, err := mht.Get(hash); err == nil {
				found++
			}
		}
		readTime := time.Since(start)

		readRate := float64(found) / readTime.Seconds()
		t.Logf("New System - Reads: %.0f/sec (found %d/1000)",
			readRate, found)
	})

	// Test old system
	t.Run("OldSystem", func(t *testing.T) {
		dir := "/tmp/perf_old"
		os.RemoveAll(dir)

		hf, err := NewHistoryFile(2000, dir)
		if err != nil {
			t.Fatalf("Failed to create HistoryFile: %v", err)
		}

		entries := make([]DBBKeyFull, numEntries)
		for i := 0; i < numEntries; i++ {
			data := fmt.Sprintf("test_entry_%d", i)
			entries[i].Key = sha256.Sum256([]byte(data))
			entries[i].Offset = uint64(i)
			entries[i].Length = uint64(len(data))
		}

		start := time.Now()
		// Add all keys at once (AddKeys expects them grouped by KeySet)
		for i := 0; i < numEntries; i++ {
			buffer := make([]byte, DBKeyFullSize)
			copy(buffer, entries[i].Bytes(entries[i].Key))

			if err := hf.AddKeys(buffer); err != nil {
				t.Fatalf("AddKeys failed at %d: %v", i, err)
			}
		}
		writeTime := time.Since(start)

		writeRate := float64(numEntries) / writeTime.Seconds()
		t.Logf("Old System - Writes: %.0f/sec (%.3f ms per write)",
			writeRate, writeTime.Seconds()*1000/float64(numEntries))

		// Sort and test reads
		hf.SortAllKeySets()

		start = time.Now()
		found := 0
		for i := 0; i < 1000; i++ {
			if _, err := hf.Get(entries[i].Key); err == nil {
				found++
			}
		}
		readTime := time.Since(start)

		readRate := float64(found) / readTime.Seconds()
		t.Logf("Old System - Reads: %.0f/sec (found %d/1000)",
			readRate, found)
	})
}