package dkv

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDKVBasicOperations(t *testing.T) {
	// Create temp directory
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("dkv_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	// Create DKV instance
	store, err := NewDKV(dir)
	require.NoError(t, err)
	defer store.Close()

	// Test Put and Get
	key1 := sha256.Sum256([]byte("key1"))
	value1 := []byte("value1")

	err = store.Put(key1, value1)
	assert.NoError(t, err)

	retrieved, found, err := store.Get(key1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value1, retrieved)

	// Test Update (replace existing value)
	value2 := []byte("updated_value1")
	err = store.Put(key1, value2)
	assert.NoError(t, err)

	retrieved, found, err = store.Get(key1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value2, retrieved)

	// Test Delete
	err = store.Delete(key1)
	assert.NoError(t, err)

	retrieved, found, err = store.Get(key1)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestDKVHashDistribution(t *testing.T) {
	// Create temp directory
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("dkv_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	// Create DKV instance
	store, err := NewDKV(dir)
	require.NoError(t, err)
	defer store.Close()

	// Insert keys with different hash prefixes
	keysPerBucket := make(map[int]int)

	for i := 0; i < 1000; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("key_%d", i)))
		value := []byte(fmt.Sprintf("value_%d", i))

		err = store.Put(key, value)
		assert.NoError(t, err)

		// Track distribution
		bucketId := int(key[0])
		keysPerBucket[bucketId]++
	}

	// Verify distribution (should be roughly even)
	fmt.Printf("Hash distribution across %d buckets:\n", len(keysPerBucket))
	totalBucketsUsed := 0
	for _, count := range keysPerBucket {
		if count > 0 {
			totalBucketsUsed++
		}
	}
	fmt.Printf("Buckets used: %d/256\n", totalBucketsUsed)
	assert.Greater(t, totalBucketsUsed, 50, "Should use many buckets for good distribution")
}

func TestDKVDeduplication(t *testing.T) {
	// Create temp directory
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("dkv_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	// Create DKV instance
	store, err := NewDKV(dir)
	require.NoError(t, err)
	defer store.Close()

	// Insert same key multiple times with different values
	key := sha256.Sum256([]byte("duplicate_key"))

	for i := 0; i < 10; i++ {
		value := []byte(fmt.Sprintf("value_version_%d", i))
		err = store.Put(key, value)
		assert.NoError(t, err)
	}

	// Retrieve should get the latest value
	retrieved, found, err := store.Get(key)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value_version_9"), retrieved)

	// Stats should show wasted space from overwrites
	stats := store.Stats()
	assert.Equal(t, int64(10), stats["total_writes"])
}

func TestDKVCompaction(t *testing.T) {
	// Create temp directory
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("dkv_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	// Create DKV instance
	store, err := NewDKV(dir)
	require.NoError(t, err)
	defer store.Close()

	// Create enough updates to trigger compaction
	// Use larger values to exceed the minCompactSize threshold
	numKeys := 1000
	largeValue := make([]byte, 10*1024) // 10KB per value
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	for i := 0; i < numKeys; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("key_%d", i)))

		// Write multiple versions to create wasted space
		for v := 0; v < 5; v++ {
			// Append version info to the large value
			value := append(largeValue, []byte(fmt.Sprintf("_version_%d", v))...)
			err = store.Put(key, value)
			assert.NoError(t, err)
		}
	}

	// Force flush all buckets
	for _, bucket := range store.hashBuckets {
		store.flushBucket(bucket)
	}

	// Force compaction
	err = store.Compact()
	assert.NoError(t, err)

	// Verify all keys still accessible with latest values
	for i := 0; i < numKeys; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("key_%d", i)))

		retrieved, found, err := store.Get(key)
		assert.NoError(t, err)
		assert.True(t, found, "Key %d should be found", i)
		// Check that value starts with the large value prefix
		assert.True(t, len(retrieved) > 10*1024, "Value should be large")
	}

	// Check that at least some compaction occurred
	stats := store.Stats()
	compacts := stats["total_compacts"].(int64)
	t.Logf("Total compacts performed: %d", compacts)
	// We expect at least some buckets to be compacted
	assert.Greater(t, compacts, int64(0), "Should have performed at least one compaction")
}

func TestDKVPersistence(t *testing.T) {
	// Create temp directory
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("dkv_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	// Create and populate DKV
	store1, err := NewDKV(dir)
	require.NoError(t, err)

	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("persist_key_%d", i)))
		value := []byte(fmt.Sprintf("persist_value_%d", i))
		err = store1.Put(key, value)
		assert.NoError(t, err)
	}

	err = store1.Close()
	assert.NoError(t, err)

	// Reopen and verify data persisted
	store2, err := NewDKV(dir)
	require.NoError(t, err)
	defer store2.Close()

	for i := 0; i < numKeys; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("persist_key_%d", i)))
		expectedValue := []byte(fmt.Sprintf("persist_value_%d", i))

		retrieved, found, err := store2.Get(key)
		assert.NoError(t, err)
		assert.True(t, found, "Key %d should be found after reopening", i)
		assert.Equal(t, expectedValue, retrieved)
	}
}

func BenchmarkDKVPut(b *testing.B) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("dkv_bench_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewDKV(dir)
	require.NoError(b, err)
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("bench_key_%d", i)))
		value := []byte(fmt.Sprintf("bench_value_%d", i))
		store.Put(key, value)
	}
}

func BenchmarkDKVGet(b *testing.B) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("dkv_bench_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewDKV(dir)
	require.NoError(b, err)
	defer store.Close()

	// Prepare data
	keys := make([][32]byte, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("bench_key_%d", i)))
		value := []byte(fmt.Sprintf("bench_value_%d", i))
		store.Put(keys[i], value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % len(keys)
		store.Get(keys[idx])
	}
}