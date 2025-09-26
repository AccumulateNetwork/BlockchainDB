package dkv

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestTreeDKVIntegration tests the complete tree-based DKV implementation
func TestTreeDKVIntegration(t *testing.T) {
	dir := t.TempDir()
	tdkv, err := NewTreeDKV(dir)
	if err != nil {
		t.Fatalf("Failed to create TreeDKV: %v", err)
	}
	defer tdkv.Close()

	// Test basic put/get
	t.Run("BasicPutGet", func(t *testing.T) {
		key := sha256.Sum256([]byte("test-key"))
		value := []byte("test-value")

		if err := tdkv.Put(key, value); err != nil {
			t.Errorf("Put failed: %v", err)
		}

		retrieved, found, err := tdkv.Get(key)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if !found {
			t.Error("Key not found")
		}
		if !bytes.Equal(retrieved, value) {
			t.Errorf("Value mismatch: got %s, want %s", retrieved, value)
		}
	})

	// Test iteration
	t.Run("Iteration", func(t *testing.T) {
		// Add multiple sorted keys
		keys := make([][32]byte, 10)
		for i := 0; i < 10; i++ {
			keys[i] = sha256.Sum256([]byte(fmt.Sprintf("iter-key-%02d", i)))
			value := []byte(fmt.Sprintf("value-%d", i))
			if err := tdkv.Put(keys[i], value); err != nil {
				t.Errorf("Put failed: %v", err)
			}
		}

		// Iterate and verify order
		iter := tdkv.NewIterator()
		defer iter.Close()

		count := 0
		var lastKey [32]byte
		for iter.Next() {
			key := iter.Key()
			if count > 0 && bytes.Compare(lastKey[:], key[:]) >= 0 {
				t.Error("Keys not in sorted order")
			}
			lastKey = key
			count++
		}

		if count < 10 {
			t.Errorf("Iterator returned %d items, expected at least 10", count)
		}
	})

	// Test range queries
	t.Run("RangeQuery", func(t *testing.T) {
		// Define range
		start := sha256.Sum256([]byte("range-aaa"))
		end := sha256.Sum256([]byte("range-zzz"))

		// Add keys within and outside range
		inRange := []string{"range-bbb", "range-mmm", "range-yyy"}
		outRange := []string{"range-000", "range-~~~"}

		for _, k := range inRange {
			key := sha256.Sum256([]byte(k))
			tdkv.Put(key, []byte(k))
		}
		for _, k := range outRange {
			key := sha256.Sum256([]byte(k))
			tdkv.Put(key, []byte(k))
		}

		// Query range
		results, err := tdkv.Range(start, end)
		if err != nil {
			t.Errorf("Range query failed: %v", err)
		}

		// Verify results
		foundInRange := make(map[string]bool)
		for _, r := range results {
			value := string(r.Value)
			foundInRange[value] = true
		}

		for _, expected := range inRange {
			if !foundInRange[expected] {
				t.Errorf("Expected key %s not in range results", expected)
			}
		}

		for _, unexpected := range outRange {
			if foundInRange[unexpected] {
				t.Errorf("Unexpected key %s in range results", unexpected)
			}
		}
	})
}

// TestBloomFilter tests the bloom filter implementation
func TestBloomFilter(t *testing.T) {
	t.Run("BasicOperations", func(t *testing.T) {
		bf := NewBloomFilter(1000, 0.01)

		// Add keys
		keys := make([][32]byte, 100)
		for i := 0; i < 100; i++ {
			keys[i] = sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
			bf.Add(keys[i])
		}

		// Check contains
		for _, key := range keys {
			if !bf.MayContain(key) {
				t.Error("Bloom filter false negative")
			}
		}

		// Check false positive rate
		falsePositives := 0
		for i := 1000; i < 2000; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("nonexistent-%d", i)))
			if bf.MayContain(key) {
				falsePositives++
			}
		}

		fpRate := float64(falsePositives) / 1000.0
		if fpRate > 0.02 { // Allow 2% false positive rate (double the target)
			t.Errorf("False positive rate too high: %.2f%%", fpRate*100)
		}
	})

	t.Run("Serialization", func(t *testing.T) {
		bf := NewBloomFilter(100, 0.01)

		// Add some keys
		keys := make([][32]byte, 10)
		for i := 0; i < 10; i++ {
			keys[i] = sha256.Sum256([]byte(fmt.Sprintf("serialize-%d", i)))
			bf.Add(keys[i])
		}

		// Serialize
		data := bf.Serialize()

		// Deserialize
		bf2 := NewBloomFilterFromBytes(data)

		// Verify all keys still present
		for _, key := range keys {
			if !bf2.MayContain(key) {
				t.Error("Key lost after serialization")
			}
		}
	})
}

// TestCompaction tests the compaction logic
func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	tdkv, err := NewTreeDKV(dir)
	if err != nil {
		t.Fatalf("Failed to create TreeDKV: %v", err)
	}
	defer tdkv.Close()

	// Generate enough data to trigger compaction
	t.Run("TriggerCompaction", func(t *testing.T) {
		// Write enough data to fill multiple MemTables
		for i := 0; i < 1000; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("compact-%d", i)))
			value := make([]byte, 4096) // 4KB values
			rand.Read(value)

			if err := tdkv.Put(key, value); err != nil {
				t.Errorf("Put failed: %v", err)
			}
		}

		// Force flush to trigger level 0 compaction
		tdkv.flushMemTable()

		// Wait for background compaction
		time.Sleep(100 * time.Millisecond)

		// Verify data is still accessible
		for i := 0; i < 100; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("compact-%d", i)))
			_, found, err := tdkv.Get(key)
			if err != nil {
				t.Errorf("Get failed after compaction: %v", err)
			}
			if !found {
				t.Errorf("Key lost after compaction: compact-%d", i)
			}
		}

		// Check compaction stats
		if tdkv.stats.Compactions.Load() == 0 {
			t.Log("Warning: No compactions occurred")
		}
	})

	// Test duplicate key handling during compaction
	t.Run("DuplicateKeys", func(t *testing.T) {
		key := sha256.Sum256([]byte("duplicate"))

		// Write same key multiple times
		for i := 0; i < 10; i++ {
			value := []byte(fmt.Sprintf("version-%d", i))
			if err := tdkv.Put(key, value); err != nil {
				t.Errorf("Put failed: %v", err)
			}
			// Force flush to create multiple SSTables
			if i%3 == 0 {
				tdkv.flushMemTable()
			}
		}

		// Final value should be the last one
		retrieved, found, err := tdkv.Get(key)
		if err != nil || !found {
			t.Error("Failed to retrieve duplicate key")
		}
		if !bytes.Equal(retrieved, []byte("version-9")) {
			t.Errorf("Wrong value for duplicate key: %s", retrieved)
		}
	})
}

// TestURLStore tests URL-specific storage features
func TestURLStore(t *testing.T) {
	dir := t.TempDir()
	store, err := NewURLStore(dir)
	if err != nil {
		t.Fatalf("Failed to create URLStore: %v", err)
	}
	defer store.Close()

	t.Run("URLOperations", func(t *testing.T) {
		// Store URLs
		urls := []string{
			"https://example.com/page1",
			"https://example.com/page2",
			"https://test.com/api/v1",
			"https://test.com/api/v2",
		}

		for _, url := range urls {
			data := []byte(fmt.Sprintf("content of %s", url))
			if err := store.PutURL(url, data, "test"); err != nil {
				t.Errorf("PutURL failed: %v", err)
			}
		}

		// Retrieve by URL
		data, meta, err := store.GetURL(urls[0])
		if err != nil {
			t.Errorf("GetURL failed: %v", err)
		}
		if meta.URL != urls[0] {
			t.Errorf("Metadata URL mismatch: %s", meta.URL)
		}

		// Iterate by domain
		var exampleCount int
		store.IterateDomain("example.com", func(url string, data []byte) error {
			exampleCount++
			return nil
		})
		if exampleCount != 2 {
			t.Errorf("Expected 2 URLs for example.com, got %d", exampleCount)
		}
	})

	t.Run("GroupedIteration", func(t *testing.T) {
		iter, err := store.NewGroupedIterator("domain")
		if err != nil {
			t.Errorf("Failed to create grouped iterator: %v", err)
		}

		domainCount := 0
		for iter.NextGroup() {
			domainCount++
			groupName := iter.GroupName()
			if groupName == "" {
				t.Error("Empty group name")
			}

			entryCount := 0
			for iter.NextEntry() {
				entryCount++
				entry := iter.CurrentEntry()
				if entry == nil {
					t.Error("Nil entry in iteration")
				}
			}

			if entryCount == 0 {
				t.Errorf("Empty group: %s", groupName)
			}
		}

		if domainCount < 2 {
			t.Errorf("Expected at least 2 domains, got %d", domainCount)
		}
	})
}

// TestPrefixRouter tests prefix-based routing
func TestPrefixRouter(t *testing.T) {
	dir := t.TempDir()
	router, err := NewPrefixRouter(dir, 2)
	if err != nil {
		t.Fatalf("Failed to create PrefixRouter: %v", err)
	}

	t.Run("PrefixRouting", func(t *testing.T) {
		// Generate keys with different prefixes
		keys := make([][32]byte, 256)
		for i := 0; i < 256; i++ {
			var key [32]byte
			key[0] = byte(i)
			for j := 1; j < 32; j++ {
				key[j] = byte(rand.Intn(256))
			}
			keys[i] = key

			value := []byte(fmt.Sprintf("value-%d", i))
			if err := router.Put(key, value); err != nil {
				t.Errorf("Put failed for key %d: %v", i, err)
			}
		}

		// Verify retrieval
		for i, key := range keys {
			value, found, err := router.Get(key)
			if err != nil || !found {
				t.Errorf("Failed to retrieve key %d", i)
			}
			expected := fmt.Sprintf("value-%d", i)
			if !bytes.Equal(value, []byte(expected)) {
				t.Errorf("Value mismatch for key %d", i)
			}
		}

		// Check bucket distribution
		bucketCount := len(router.buckets)
		if bucketCount == 0 {
			t.Error("No buckets created")
		}
		t.Logf("Created %d buckets for 256 keys", bucketCount)
	})
}

// TestShardedDKV tests sharded storage
func TestShardedDKV(t *testing.T) {
	dir := t.TempDir()
	sharded, err := NewShardedDKV(dir, 4)
	if err != nil {
		t.Fatalf("Failed to create ShardedDKV: %v", err)
	}

	t.Run("ShardDistribution", func(t *testing.T) {
		// Add keys to different shards
		for i := 0; i < 100; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("shard-key-%d", i)))
			value := []byte(fmt.Sprintf("shard-value-%d", i))

			if err := sharded.Put(key, value); err != nil {
				t.Errorf("Put failed: %v", err)
			}
		}

		// Verify all keys are retrievable
		for i := 0; i < 100; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("shard-key-%d", i)))
			value, found, err := sharded.Get(key)
			if err != nil || !found {
				t.Errorf("Failed to retrieve sharded key %d", i)
			}

			expected := fmt.Sprintf("shard-value-%d", i)
			if !bytes.Equal(value, []byte(expected)) {
				t.Errorf("Value mismatch in shard for key %d", i)
			}
		}
	})

	t.Run("MergedIteration", func(t *testing.T) {
		iter := sharded.NewIterator()
		defer iter.Close()

		count := 0
		var lastKey [32]byte
		for iter.Next() {
			key := iter.Key()
			// Verify sorted order
			if count > 0 && bytes.Compare(lastKey[:], key[:]) >= 0 {
				t.Error("Merged iterator not maintaining sorted order")
			}
			lastKey = key
			count++
		}

		if count < 100 {
			t.Errorf("Merged iterator returned %d items, expected at least 100", count)
		}
	})
}

// TestSSTableFormat tests the SSTable file format
func TestSSTableFormat(t *testing.T) {
	dir := t.TempDir()
	tdkv, err := NewTreeDKV(dir)
	if err != nil {
		t.Fatalf("Failed to create TreeDKV: %v", err)
	}
	defer tdkv.Close()

	t.Run("SSTableCreation", func(t *testing.T) {
		// Create a MemTable and fill it
		mem := NewMemTable()
		for i := 0; i < 100; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("sst-%d", i)))
			value := []byte(fmt.Sprintf("value-%d", i))
			mem.Put(key, value)
		}

		// Write as SSTable
		sst, err := tdkv.writeSSTable(mem, 0)
		if err != nil {
			t.Fatalf("Failed to write SSTable: %v", err)
		}
		defer sst.file.Close()

		// Verify SSTable properties
		if sst.numKeys != 100 {
			t.Errorf("SSTable has %d keys, expected 100", sst.numKeys)
		}

		// Test bloom filter accuracy
		for i := 0; i < 100; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("sst-%d", i)))
			if !sst.bloomFilter.MayContain(key) {
				t.Error("Bloom filter false negative in SSTable")
			}
		}

		// Test direct SSTable lookup
		testKey := sha256.Sum256([]byte("sst-50"))
		value, found, err := sst.Get(testKey)
		if err != nil || !found {
			t.Error("Failed to retrieve from SSTable")
		}
		if !bytes.Equal(value, []byte("value-50")) {
			t.Error("SSTable returned wrong value")
		}
	})
}

// TestAdaptiveRouter tests adaptive routing based on access patterns
func TestAdaptiveRouter(t *testing.T) {
	dir := t.TempDir()
	router, err := NewAdaptiveRouter(dir)
	if err != nil {
		t.Fatalf("Failed to create AdaptiveRouter: %v", err)
	}

	t.Run("HotColdSeparation", func(t *testing.T) {
		// Add keys to cold storage
		coldKeys := make([][32]byte, 100)
		for i := 0; i < 100; i++ {
			coldKeys[i] = sha256.Sum256([]byte(fmt.Sprintf("cold-%d", i)))
			router.coldStorage.Put(coldKeys[i], []byte(fmt.Sprintf("cold-value-%d", i)))
		}

		// Access one key repeatedly to make it hot
		hotKey := coldKeys[0]
		for i := 0; i < 15; i++ {
			router.Get(hotKey)
		}

		// Verify key was promoted to hot storage
		_, hotFound, _ := router.hotStorage.Get(hotKey)
		_, coldFound, _ := router.coldStorage.Get(hotKey)

		if !hotFound {
			t.Error("Frequently accessed key not promoted to hot storage")
		}
		if coldFound {
			t.Error("Hot key still in cold storage")
		}

		// Verify other keys remain in cold storage
		for i := 1; i < 10; i++ {
			_, hotFound, _ := router.hotStorage.Get(coldKeys[i])
			_, coldFound, _ := router.coldStorage.Get(coldKeys[i])

			if hotFound {
				t.Errorf("Infrequently accessed key %d incorrectly in hot storage", i)
			}
			if !coldFound {
				t.Errorf("Key %d missing from cold storage", i)
			}
		}
	})
}

// BenchmarkTreeDKV benchmarks the tree-based DKV
func BenchmarkTreeDKV(b *testing.B) {
	dir := b.TempDir()
	tdkv, err := NewTreeDKV(dir)
	if err != nil {
		b.Fatalf("Failed to create TreeDKV: %v", err)
	}
	defer tdkv.Close()

	// Prepare keys
	keys := make([][32]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("bench-%d", i)))
	}

	b.Run("Put", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tdkv.Put(keys[i], []byte("value"))
		}
	})

	b.Run("Get", func(b *testing.B) {
		// Populate first
		for i := 0; i < 1000; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("get-%d", i)))
			tdkv.Put(key, []byte("value"))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("get-%d", i%1000)))
			tdkv.Get(key)
		}
	})

	b.Run("Iterator", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			iter := tdkv.NewIterator()
			for j := 0; j < 100 && iter.Next(); j++ {
				_ = iter.Key()
				_ = iter.Value()
			}
			iter.Close()
		}
	})
}

// BenchmarkBloomFilter benchmarks bloom filter operations
func BenchmarkBloomFilter(b *testing.B) {
	bf := NewBloomFilter(10000, 0.01)

	keys := make([][32]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("bloom-%d", i)))
	}

	b.Run("Add", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(keys[i])
		}
	})

	b.Run("MayContain", func(b *testing.B) {
		// Populate first
		for i := 0; i < 1000; i++ {
			bf.Add(keys[i])
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.MayContain(keys[i%1000])
		}
	})
}

// TestConcurrentAccess tests thread safety
func TestConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	tdkv, err := NewTreeDKV(dir)
	if err != nil {
		t.Fatalf("Failed to create TreeDKV: %v", err)
	}
	defer tdkv.Close()

	// Run concurrent puts and gets
	done := make(chan bool)
	errors := make(chan error, 100)

	// Writers
	for w := 0; w < 10; w++ {
		go func(id int) {
			for i := 0; i < 100; i++ {
				key := sha256.Sum256([]byte(fmt.Sprintf("writer-%d-%d", id, i)))
				value := []byte(fmt.Sprintf("value-%d-%d", id, i))
				if err := tdkv.Put(key, value); err != nil {
					errors <- err
				}
			}
			done <- true
		}(w)
	}

	// Readers
	for r := 0; r < 10; r++ {
		go func(id int) {
			for i := 0; i < 100; i++ {
				key := sha256.Sum256([]byte(fmt.Sprintf("writer-%d-%d", id%10, i)))
				tdkv.Get(key) // Ignore not found errors
			}
			done <- true
		}(r)
	}

	// Wait for completion
	for i := 0; i < 20; i++ {
		<-done
	}

	// Check for errors
	select {
	case err := <-errors:
		t.Errorf("Concurrent access error: %v", err)
	default:
		// No errors
	}

	// Verify data integrity
	for w := 0; w < 10; w++ {
		for i := 0; i < 100; i++ {
			key := sha256.Sum256([]byte(fmt.Sprintf("writer-%d-%d", w, i)))
			value, found, err := tdkv.Get(key)
			if err != nil {
				t.Errorf("Get error after concurrent writes: %v", err)
			}
			if !found {
				t.Errorf("Key missing after concurrent writes: writer-%d-%d", w, i)
			}
			expected := fmt.Sprintf("value-%d-%d", w, i)
			if !bytes.Equal(value, []byte(expected)) {
				t.Errorf("Value corruption detected: got %s, want %s", value, expected)
			}
		}
	}
}