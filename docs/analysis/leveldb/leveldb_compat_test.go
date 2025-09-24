package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashDBBasicOperations(t *testing.T) {
	dir := "/tmp/test_hashdb_basic"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Test Put and Get
	key1 := []byte("key1")
	value1 := []byte("value1")

	err = db.Put(key1, value1)
	assert.NoError(t, err)

	got, err := db.Get(key1)
	assert.NoError(t, err)
	assert.Equal(t, value1, got)

	// Test overwrite
	value2 := []byte("value2")
	err = db.Put(key1, value2)
	assert.NoError(t, err)

	got, err = db.Get(key1)
	assert.NoError(t, err)
	assert.Equal(t, value2, got)

	// Test non-existent key
	_, err = db.Get([]byte("nonexistent"))
	assert.Equal(t, ErrNotFound, err)
}

func TestHashDBDelete(t *testing.T) {
	dir := "/tmp/test_hashdb_delete"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db.Close()

	key := []byte("deleteMe")
	value := []byte("goodbye")

	// Put then delete
	err = db.Put(key, value)
	assert.NoError(t, err)

	err = db.Delete(key)
	assert.NoError(t, err)

	// Should not exist after delete
	_, err = db.Get(key)
	assert.Equal(t, ErrNotFound, err)

	// Delete non-existent key should not error
	err = db.Delete([]byte("nonexistent"))
	assert.NoError(t, err)
}

func TestHashDBBatch(t *testing.T) {
	dir := "/tmp/test_hashdb_batch"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Create batch
	batch := NewWriteBatch()
	batch.Put([]byte("b1"), []byte("v1"))
	batch.Put([]byte("b2"), []byte("v2"))
	batch.Put([]byte("b3"), []byte("v3"))
	batch.Delete([]byte("b2"))

	// Execute batch
	err = db.Write(batch)
	assert.NoError(t, err)

	// Verify results
	v1, err := db.Get([]byte("b1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), v1)

	_, err = db.Get([]byte("b2"))
	assert.Equal(t, ErrNotFound, err)

	v3, err := db.Get([]byte("b3"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v3"), v3)
}

func TestHashDBDeduplication(t *testing.T) {
	dir := "/tmp/test_hashdb_dedup"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Put identical values with different keys
	value := []byte("duplicate content")
	key1 := []byte("key1")
	key2 := []byte("key2")
	key3 := []byte("key3")

	err = db.Put(key1, value)
	assert.NoError(t, err)

	err = db.Put(key2, value)
	assert.NoError(t, err)

	err = db.Put(key3, value)
	assert.NoError(t, err)

	// All keys should return same value
	v1, _ := db.Get(key1)
	v2, _ := db.Get(key2)
	v3, _ := db.Get(key3)

	assert.Equal(t, value, v1)
	assert.Equal(t, value, v2)
	assert.Equal(t, value, v3)

	// Stats should show deduplication
	stats := db.GetStats()
	assert.Equal(t, uint64(3), stats.Puts)
}

func TestHashDBIterator(t *testing.T) {
	dir := "/tmp/test_hashdb_iterator"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data
	testData := map[string]string{
		"alpha":   "first",
		"beta":    "second",
		"gamma":   "third",
		"delta":   "fourth",
		"epsilon": "fifth",
	}

	for k, v := range testData {
		err := db.Put([]byte(k), []byte(v))
		assert.NoError(t, err)
	}

	// Test forward iteration
	iter := db.NewIterator()
	defer iter.Close()

	var keys []string
	var values []string

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
	}

	// Keys should be in sorted order
	assert.Equal(t, []string{"alpha", "beta", "delta", "epsilon", "gamma"}, keys)

	// Test Seek
	iter.Seek([]byte("delta"))
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("delta"), iter.Key())

	// Test Prev
	iter.Prev()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("beta"), iter.Key())

	// Test SeekToLast
	iter.SeekToLast()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("gamma"), iter.Key())
}

func TestHashDBLargeValues(t *testing.T) {
	dir := "/tmp/test_hashdb_large"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Create large value (1MB)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	key := []byte("largeKey")
	err = db.Put(key, largeValue)
	assert.NoError(t, err)

	// Retrieve and verify
	retrieved, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, retrieved)
}

func TestHashDBConcurrency(t *testing.T) {
	dir := "/tmp/test_hashdb_concurrent"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Concurrent puts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := fmt.Sprintf("key%d", id)
			value := fmt.Sprintf("value%d", id)
			err := db.Put([]byte(key), []byte(value))
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all puts
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all values
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		value, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, []byte(expectedValue), value)
	}
}

func TestHashDBReopenDatabase(t *testing.T) {
	dir := "/tmp/test_hashdb_reopen"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Create and populate database
	db, err := NewHashDB(dir)
	require.NoError(t, err)

	testData := map[string]string{
		"persist1": "value1",
		"persist2": "value2",
		"persist3": "value3",
	}

	for k, v := range testData {
		err := db.Put([]byte(k), []byte(v))
		assert.NoError(t, err)
	}

	err = db.Close()
	assert.NoError(t, err)

	// Reopen database
	db2, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db2.Close()

	// Verify data persisted
	for k, v := range testData {
		value, err := db2.Get([]byte(k))
		assert.NoError(t, err)
		assert.Equal(t, []byte(v), value)
	}
}

func BenchmarkHashDBPut(b *testing.B) {
	dir := "/tmp/bench_hashdb_put"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(b, err)
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		db.Put([]byte(key), []byte(value))
	}
}

func BenchmarkHashDBGet(b *testing.B) {
	dir := "/tmp/bench_hashdb_get"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(b, err)
	defer db.Close()

	// Populate with test data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		db.Put([]byte(key), []byte(value))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		db.Get([]byte(key))
	}
}

func BenchmarkHashDBBatch(b *testing.B) {
	dir := "/tmp/bench_hashdb_batch"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(b, err)
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch := NewWriteBatch()
		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key%d_%d", i, j)
			value := fmt.Sprintf("value%d_%d", i, j)
			batch.Put([]byte(key), []byte(value))
		}
		db.Write(batch)
	}
}

// TestLevelDBCompatibility verifies compatibility with LevelDB semantics
func TestLevelDBCompatibility(t *testing.T) {
	dir := "/tmp/test_leveldb_compat"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := NewHashDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Empty key and value
	err = db.Put([]byte{}, []byte{})
	assert.NoError(t, err)

	val, err := db.Get([]byte{})
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, val)

	// Test 2: Binary keys and values
	binaryKey := []byte{0x00, 0x01, 0x02, 0xff}
	binaryValue := []byte{0xff, 0xfe, 0xfd, 0x00}

	err = db.Put(binaryKey, binaryValue)
	assert.NoError(t, err)

	val, err = db.Get(binaryKey)
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(binaryValue, val))

	// Test 3: Overwrite with empty value
	key := []byte("test_key")
	err = db.Put(key, []byte("initial"))
	assert.NoError(t, err)

	err = db.Put(key, []byte{})
	assert.NoError(t, err)

	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, val)

	// Test 4: Delete then Put
	err = db.Delete(key)
	assert.NoError(t, err)

	err = db.Put(key, []byte("new_value"))
	assert.NoError(t, err)

	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("new_value"), val)
}