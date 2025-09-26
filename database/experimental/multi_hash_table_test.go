package blockchainDB

import (
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMultiHashTable(t *testing.T) {
	dir := "/tmp/mht_test"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create MHT: %v", err)
	}
	defer mht.Shutdown()

	// Test single put/get
	key := [32]byte{1, 2, 3, 4}
	value := DBBKey{Offset: 100, Length: 200}

	if err := mht.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Should be in write buffer
	retrieved, err := mht.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Offset != value.Offset || retrieved.Length != value.Length {
		t.Fatalf("Value mismatch: got %+v, want %+v", retrieved, value)
	}
}

func TestMultiHashTableBulkOperations(t *testing.T) {
	dir := "/tmp/mht_bulk_test"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create MHT: %v", err)
	}
	defer mht.Shutdown()

	// Insert many keys
	numKeys := 10000
	keys := make([][32]byte, numKeys)
	values := make([]DBBKey, numKeys)

	for i := 0; i < numKeys; i++ {
		rand.Read(keys[i][:])
		values[i] = DBBKey{
			Offset: uint64(i * 100),
			Length: uint64(i + 1),
		}

		if err := mht.Put(keys[i], values[i]); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Force flush to bins
	time.Sleep(2 * time.Second)

	// Verify all keys
	for i := 0; i < numKeys; i++ {
		retrieved, err := mht.Get(keys[i])
		if err != nil {
			t.Errorf("Get key %d failed: %v", i, err)
			continue
		}

		if retrieved.Offset != values[i].Offset || retrieved.Length != values[i].Length {
			t.Errorf("Value mismatch for key %d: got %+v, want %+v", i, retrieved, values[i])
		}
	}

	// Check statistics
	fmt.Printf("Statistics:\n")
	fmt.Printf("  Writes: %d\n", mht.stats.writes)
	fmt.Printf("  Reads: %d\n", mht.stats.reads)
	fmt.Printf("  Merges: %d\n", mht.stats.merges)
	fmt.Printf("  FanOuts: %d\n", mht.stats.fanOuts)
}

func TestMultiHashTableConcurrency(t *testing.T) {
	dir := "/tmp/mht_concurrent_test"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create MHT: %v", err)
	}
	defer mht.Shutdown()

	// Concurrent writes and reads
	var wg sync.WaitGroup
	numGoroutines := 10
	keysPerGoroutine := 1000

	// Writers
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < keysPerGoroutine; i++ {
				key := [32]byte{}
				key[0] = byte(goroutineID)
				key[1] = byte(i >> 8)
				key[2] = byte(i & 0xFF)

				value := DBBKey{
					Offset: uint64(goroutineID*1000 + i),
					Length: uint64(i + 1),
				}

				if err := mht.Put(key, value); err != nil {
					t.Errorf("Put failed: %v", err)
				}
			}
		}(g)
	}

	// Readers (start after some writes)
	time.Sleep(100 * time.Millisecond)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < keysPerGoroutine/2; i++ {
				key := [32]byte{}
				key[0] = byte(goroutineID)
				key[1] = byte(i >> 8)
				key[2] = byte(i & 0xFF)

				if _, err := mht.Get(key); err != nil {
					// Some keys might not be written yet
					continue
				}
			}
		}(g)
	}

	wg.Wait()

	fmt.Printf("Concurrent test completed:\n")
	fmt.Printf("  Total writes: %d\n", mht.stats.writes)
	fmt.Printf("  Total reads: %d\n", mht.stats.reads)
}

func TestMultiHashTableRecovery(t *testing.T) {
	dir := "/tmp/mht_recovery_test"
	os.RemoveAll(dir)

	// Phase 1: Write data
	mht, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to create MHT: %v", err)
	}

	numKeys := 1000
	keys := make([][32]byte, numKeys)
	values := make([]DBBKey, numKeys)

	for i := 0; i < numKeys; i++ {
		rand.Read(keys[i][:])
		values[i] = DBBKey{
			Offset: uint64(i * 100),
			Length: uint64(i + 1),
		}

		if err := mht.Put(keys[i], values[i]); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Don't flush - simulate crash
	// Just close without flushing
	mht.wal.Close()

	// Phase 2: Recover and verify
	mht2, err := NewMultiHashTable(dir)
	if err != nil {
		t.Fatalf("Failed to recover MHT: %v", err)
	}
	defer mht2.Shutdown()

	// Wait for recovery to complete
	time.Sleep(1 * time.Second)

	// Verify all keys are recovered
	recovered := 0
	for i := 0; i < numKeys; i++ {
		retrieved, err := mht2.Get(keys[i])
		if err != nil {
			continue // Some might not have been distributed yet
		}

		if retrieved.Offset == values[i].Offset && retrieved.Length == values[i].Length {
			recovered++
		}
	}

	fmt.Printf("Recovery test: %d/%d keys recovered\n", recovered, numKeys)

	if recovered < numKeys*90/100 { // Expect at least 90% recovery
		t.Errorf("Insufficient recovery: only %d/%d keys recovered", recovered, numKeys)
	}
}

func BenchmarkMultiHashTableWrites(b *testing.B) {
	dir := "/tmp/mht_bench"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		b.Fatalf("Failed to create MHT: %v", err)
	}
	defer mht.Shutdown()

	keys := make([][32]byte, b.N)
	for i := 0; i < b.N; i++ {
		rand.Read(keys[i][:])
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value := DBBKey{
			Offset: uint64(i * 100),
			Length: uint64(i + 1),
		}

		if err := mht.Put(keys[i], value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
}

func BenchmarkMultiHashTableReads(b *testing.B) {
	dir := "/tmp/mht_bench_read"
	os.RemoveAll(dir)

	mht, err := NewMultiHashTable(dir)
	if err != nil {
		b.Fatalf("Failed to create MHT: %v", err)
	}
	defer mht.Shutdown()

	// Pre-populate with keys
	numKeys := 100000
	keys := make([][32]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		rand.Read(keys[i][:])
		value := DBBKey{
			Offset: uint64(i * 100),
			Length: uint64(i + 1),
		}
		mht.Put(keys[i], value)
	}

	// Force flush
	time.Sleep(2 * time.Second)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[i%numKeys]
		if _, err := mht.Get(key); err != nil {
			b.Errorf("Get failed: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/sec")
}