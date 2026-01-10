package kv

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"syscall"
	"testing"
)

func TestKVShard_Basic(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	key := sha256.Sum256([]byte("test key"))
	value := []byte("test value")

	if err := kvs.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := kvs.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get returned %v, want %v", got, value)
	}
}

func TestKVShard_PutDyna(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	key := sha256.Sum256([]byte("dyna key"))
	value := []byte("dyna value")

	if err := kvs.PutDyna(key, value); err != nil {
		t.Fatalf("PutDyna failed: %v", err)
	}

	got, err := kvs.GetDyna(key)
	if err != nil {
		t.Fatalf("GetDyna failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("GetDyna returned %v, want %v", got, value)
	}
}

func TestKVShard_PutPerm(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	key := sha256.Sum256([]byte("perm key"))
	value := []byte("perm value")

	if err := kvs.PutPerm(key, value); err != nil {
		t.Fatalf("PutPerm failed: %v", err)
	}

	got, err := kvs.GetPerm(key)
	if err != nil {
		t.Fatalf("GetPerm failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("GetPerm returned %v, want %v", got, value)
	}
}

func TestKVShard_Has(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	key := sha256.Sum256([]byte("exists"))
	missing := sha256.Sum256([]byte("missing"))
	value := []byte("value")

	if err := kvs.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if !kvs.Has(key) {
		t.Error("Has returned false for existing key")
	}

	if kvs.Has(missing) {
		t.Error("Has returned true for missing key")
	}
}

func TestKVShard_BloomFilter(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	// Add some keys
	for i := 0; i < 100; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := kvs.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Test bloom filter with non-existent keys
	falsePositives := 0
	for i := 1000; i < 2000; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
		kvs.mu.RLock()
		mayExist := kvs.Bloom.MayContain(key)
		kvs.mu.RUnlock()
		if mayExist {
			falsePositives++
		}
	}

	// Should have very few false positives (< 2% for 1% FPR bloom filter)
	fpRate := float64(falsePositives) / 1000.0
	if fpRate > 0.02 {
		t.Errorf("Bloom filter false positive rate = %.2f%%, want < 2%%", fpRate*100)
	}
	t.Logf("Bloom filter false positive rate: %.2f%%", fpRate*100)
}

func TestKVShard_ShardDistribution(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 100000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	// Count entries per shard
	shardCounts := make(map[int]int)

	// Add 1000 random keys
	for i := 0; i < 1000; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("distributed-key-%d", i)))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := kvs.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		shardIdx := ShardIndex(key[:])
		shardCounts[shardIdx]++
	}

	// Should have reasonable distribution across shards
	// With 1000 keys and 256 shards, average is ~4 per shard
	usedShards := len(shardCounts)
	if usedShards < 100 {
		t.Errorf("Only %d shards used for 1000 keys, expected better distribution", usedShards)
	}
	t.Logf("Used %d shards for 1000 keys", usedShards)
}

func TestKVShard_Persistence(t *testing.T) {
	dir := t.TempDir()

	// Create and populate using both DynaKV and PermKV (via Put)
	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}

	dynaKeys := make([][32]byte, 50)
	permKeys := make([][32]byte, 50)

	// Add DynaKV entries
	for i := 0; i < 50; i++ {
		dynaKeys[i] = sha256.Sum256([]byte(fmt.Sprintf("dyna-key-%d", i)))
		value := []byte(fmt.Sprintf("dyna-value-%d", i))
		if err := kvs.PutDyna(dynaKeys[i], value); err != nil {
			t.Fatalf("PutDyna failed: %v", err)
		}
	}

	// Add PermKV entries
	for i := 0; i < 50; i++ {
		permKeys[i] = sha256.Sum256([]byte(fmt.Sprintf("perm-key-%d", i)))
		value := []byte(fmt.Sprintf("perm-value-%d", i))
		if err := kvs.PutPerm(permKeys[i], value); err != nil {
			t.Fatalf("PutPerm failed: %v", err)
		}
	}

	if err := kvs.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen
	kvs, err = OpenKVShard(dir)
	if err != nil {
		t.Fatalf("OpenKVShard failed: %v", err)
	}
	defer kvs.Close()

	// Verify DynaKV keys
	for i := 0; i < 50; i++ {
		got, err := kvs.GetDyna(dynaKeys[i])
		if err != nil {
			t.Errorf("GetDyna after reopen failed for key %d: %v", i, err)
			continue
		}
		expected := []byte(fmt.Sprintf("dyna-value-%d", i))
		if !bytes.Equal(got, expected) {
			t.Errorf("GetDyna returned %s after reopen, want %s", got, expected)
		}
	}

	// Verify PermKV keys
	for i := 0; i < 50; i++ {
		got, err := kvs.GetPerm(permKeys[i])
		if err != nil {
			t.Errorf("GetPerm after reopen failed for key %d: %v", i, err)
			continue
		}
		expected := []byte(fmt.Sprintf("perm-value-%d", i))
		if !bytes.Equal(got, expected) {
			t.Errorf("GetPerm returned %s after reopen, want %s", got, expected)
		}
	}
}

func TestKVShard_ForEachDyna(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	prefix := []byte("account:")

	// Add entries with prefix
	for i := 0; i < 50; i++ {
		var key [32]byte
		copy(key[:], prefix)
		key[8] = byte(i)
		if err := kvs.PutDyna(key, []byte{byte(i)}); err != nil {
			t.Fatalf("PutDyna failed: %v", err)
		}
	}

	// Add entries without prefix
	for i := 0; i < 30; i++ {
		var key [32]byte
		key[0] = byte(i + 200)
		if err := kvs.PutDyna(key, []byte{byte(i)}); err != nil {
			t.Fatalf("PutDyna failed: %v", err)
		}
	}

	count := 0
	err = kvs.ForEachDyna(prefix, func(key, value []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachDyna failed: %v", err)
	}

	if count != 50 {
		t.Errorf("ForEachDyna found %d entries, want 50", count)
	}
}

func TestKVShard_Compress(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	// Add entries
	for i := 0; i < 100; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("compress-key-%d", i)))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := kvs.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Compress should not error
	if err := kvs.Compress(); err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Verify data still accessible
	for i := 0; i < 10; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("compress-key-%d", i)))
		got, err := kvs.Get(key)
		if err != nil {
			t.Errorf("Get after compress failed for key %d: %v", i, err)
			continue
		}
		expected := []byte(fmt.Sprintf("value-%d", i))
		if !bytes.Equal(got, expected) {
			t.Errorf("Get returned %s, want %s", got, expected)
		}
	}
}

func TestKVShard_Stats(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	// Add dyna entries
	for i := 0; i < 50; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("dyna-key-%d", i)))
		if err := kvs.PutDyna(key, []byte("dyna")); err != nil {
			t.Fatalf("PutDyna failed: %v", err)
		}
	}

	// Add perm entries
	for i := 0; i < 30; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("perm-key-%d", i)))
		if err := kvs.PutPerm(key, []byte("perm")); err != nil {
			t.Fatalf("PutPerm failed: %v", err)
		}
	}

	stats := kvs.Stats()

	if stats.TotalDynaEntries != 50 {
		t.Errorf("TotalDynaEntries = %d, want 50", stats.TotalDynaEntries)
	}
	if stats.TotalPermWrites != 30 {
		t.Errorf("TotalPermWrites = %d, want 30", stats.TotalPermWrites)
	}
}

func TestShardIndex(t *testing.T) {
	tests := []struct {
		key      []byte
		expected int
	}{
		{[]byte{0, 1, 2}, 0},
		{[]byte{1, 2, 3}, 1},
		{[]byte{255, 0, 0}, 255},
		{[]byte{128}, 128},
		{[]byte{}, 0}, // Empty key returns 0
	}

	for _, tt := range tests {
		got := ShardIndex(tt.key)
		if got != tt.expected {
			t.Errorf("ShardIndex(%v) = %d, want %d", tt.key, got, tt.expected)
		}
	}
}

func TestBloomFilter_Basic(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	key1 := [32]byte{1, 2, 3}
	key2 := [32]byte{4, 5, 6}

	// Initially empty
	if bf.MayContain(key1) {
		t.Error("Empty bloom filter should not contain any keys")
	}

	// Add key1
	bf.Add(key1)

	if !bf.MayContain(key1) {
		t.Error("Bloom filter should contain added key")
	}

	// key2 might false-positive but probably not
	t.Logf("key2 MayContain: %v (expected false, but may false-positive)", bf.MayContain(key2))
}

func TestBloomFilter_Serialize(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	keys := make([][32]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("bloom-key-%d", i)))
		bf.Add(keys[i])
	}

	// Serialize
	data := bf.Serialize()

	// Deserialize
	bf2 := NewBloomFilterFromBytes(data)

	// All keys should still be present
	for i, key := range keys {
		if !bf2.MayContain(key) {
			t.Errorf("Key %d not found after deserialization", i)
		}
	}
}

func TestKVShard_PutPermAsync(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	// Write async
	keys := make([][32]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("async-key-%d", i)))
		value := []byte(fmt.Sprintf("async-value-%d", i))
		kvs.PutPermAsync(keys[i], value)
	}

	// Flush to ensure all writes complete
	kvs.Flush()

	// Verify all keys
	for i := 0; i < 100; i++ {
		got, err := kvs.GetPerm(keys[i])
		if err != nil {
			t.Errorf("GetPerm failed for key %d: %v", i, err)
			continue
		}
		expected := []byte(fmt.Sprintf("async-value-%d", i))
		if !bytes.Equal(got, expected) {
			t.Errorf("GetPerm returned %s, want %s", got, expected)
		}
	}
}

func TestKVShard_PutPermAsyncPersistence(t *testing.T) {
	dir := t.TempDir()

	// Write async
	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}

	keys := make([][32]byte, 50)
	for i := 0; i < 50; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("persist-async-%d", i)))
		value := []byte(fmt.Sprintf("persist-value-%d", i))
		kvs.PutPermAsync(keys[i], value)
	}

	// Close (should flush and persist)
	if err := kvs.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify
	kvs, err = OpenKVShard(dir)
	if err != nil {
		t.Fatalf("OpenKVShard failed: %v", err)
	}
	defer kvs.Close()

	for i := 0; i < 50; i++ {
		got, err := kvs.GetPerm(keys[i])
		if err != nil {
			t.Errorf("GetPerm after reopen failed for key %d: %v", i, err)
			continue
		}
		expected := []byte(fmt.Sprintf("persist-value-%d", i))
		if !bytes.Equal(got, expected) {
			t.Errorf("GetPerm returned %s after reopen, want %s", got, expected)
		}
	}
}

func TestKVShard_PendingWrites(t *testing.T) {
	dir := t.TempDir()

	kvs, err := NewKVShard(dir, 16, 10000)
	if err != nil {
		t.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	// Initially no pending writes
	if kvs.PendingWrites() != 0 {
		t.Errorf("Initial PendingWrites = %d, want 0", kvs.PendingWrites())
	}

	// Write many async
	for i := 0; i < 1000; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("pending-key-%d", i)))
		kvs.PutPermAsync(key, []byte("value"))
	}

	// Should have pending writes (unless they're processed very fast)
	// Just verify it doesn't crash

	// Flush and verify
	kvs.Flush()
	if kvs.PendingWrites() != 0 {
		t.Errorf("After Flush, PendingWrites = %d, want 0", kvs.PendingWrites())
	}
}

func BenchmarkKVShard_Put(b *testing.B) {
	dir := b.TempDir()

	kvs, err := NewKVShard(dir, 256, b.N)
	if err != nil {
		b.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	value := make([]byte, 200)
	for i := range value {
		value[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("bench-key-%d", i)))
		if err := kvs.Put(key, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkKVShard_PutPerm(b *testing.B) {
	dir := b.TempDir()

	kvs, err := NewKVShard(dir, 256, b.N)
	if err != nil {
		b.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	value := make([]byte, 200)
	for i := range value {
		value[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("bench-perm-%d", i)))
		if err := kvs.PutPerm(key, value); err != nil {
			b.Fatalf("PutPerm failed: %v", err)
		}
	}
}

func BenchmarkKVShard_PutPermAsync(b *testing.B) {
	dir := b.TempDir()

	kvs, err := NewKVShard(dir, 256, b.N)
	if err != nil {
		b.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	value := make([]byte, 200)
	for i := range value {
		value[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("bench-async-%d", i)))
		kvs.PutPermAsync(key, value)
	}
	b.StopTimer()
	kvs.Flush() // Don't count flush time
}

func BenchmarkKVShard_GetDyna(b *testing.B) {
	dir := b.TempDir()

	kvs, err := NewKVShard(dir, 256, 10000)
	if err != nil {
		b.Fatalf("NewKVShard failed: %v", err)
	}
	defer kvs.Close()

	// Pre-populate
	keys := make([][32]byte, 10000)
	value := make([]byte, 200)
	for i := 0; i < 10000; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("bench-key-%d", i)))
		kvs.PutDyna(keys[i], value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := kvs.GetDyna(keys[i%10000])
		if err != nil {
			b.Fatalf("GetDyna failed: %v", err)
		}
	}
}

func TestMmapBloomFilter_CrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create bloom filter, add keys, simulate crash (don't call Close)
	bloom1, needsRecovery, err := NewMmapBloomFilter(dir, 10000, 0.01)
	if err != nil {
		t.Fatalf("NewMmapBloomFilter failed: %v", err)
	}
	if needsRecovery {
		t.Error("New bloom filter should not need recovery")
	}

	// Add some keys
	keys := make([][32]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("crash-test-key-%d", i)))
		bloom1.Add(keys[i])
	}

	// Simulate crash: don't call Close(), just unmap
	// The dirty flag should remain set
	syscall.Munmap(bloom1.data)
	bloom1.file.Close()

	// Phase 2: Reopen - should detect dirty flag and need recovery
	bloom2, needsRecovery, err := NewMmapBloomFilter(dir, 10000, 0.01)
	if err != nil {
		t.Fatalf("NewMmapBloomFilter (recovery) failed: %v", err)
	}
	if !needsRecovery {
		t.Error("Bloom filter after crash should need recovery")
	}

	// All keys should still be present (mmap persisted them)
	for i, key := range keys {
		if !bloom2.MayContain(key) {
			t.Errorf("Key %d not found after crash recovery", i)
		}
	}

	// Simulate writing recovered data to database
	if err := bloom2.WriteToDatabase(dir); err != nil {
		t.Fatalf("WriteToDatabase failed: %v", err)
	}

	// Phase 3: Clean shutdown
	if err := bloom2.Close(dir); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Phase 4: Reopen after clean shutdown - should NOT need recovery
	bloom3, needsRecovery, err := NewMmapBloomFilter(dir, 10000, 0.01)
	if err != nil {
		t.Fatalf("NewMmapBloomFilter (after clean) failed: %v", err)
	}
	if needsRecovery {
		t.Error("Bloom filter after clean shutdown should not need recovery")
	}

	// Keys should still be present
	for i, key := range keys {
		if !bloom3.MayContain(key) {
			t.Errorf("Key %d not found after clean reopen", i)
		}
	}

	bloom3.Close(dir)
}

func TestMmapBloomFilter_Basic(t *testing.T) {
	dir := t.TempDir()

	bloom, _, err := NewMmapBloomFilter(dir, 10000, 0.01)
	if err != nil {
		t.Fatalf("NewMmapBloomFilter failed: %v", err)
	}
	defer bloom.Close(dir)

	key1 := [32]byte{1, 2, 3}
	key2 := [32]byte{4, 5, 6}

	// Initially empty
	if bloom.MayContain(key1) {
		t.Error("Empty bloom filter should not contain key1")
	}

	// Add key1
	bloom.Add(key1)

	if !bloom.MayContain(key1) {
		t.Error("Bloom filter should contain added key")
	}

	// key2 should (probably) not be present
	t.Logf("key2 MayContain: %v (expected false)", bloom.MayContain(key2))
}
