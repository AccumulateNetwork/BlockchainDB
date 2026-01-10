package dkv

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBadgerEquivalence demonstrates that BlockchainDB behaves identically to BadgerDB
// for all standard key-value operations.
//
// Minor differences (semantically equivalent):
// - Empty values: BadgerDB returns nil, BlockchainDB returns []byte{} (both len=0)
//
// Additional BlockchainDB features not in BadgerDB:
// - Content-addressed storage (PutByHash/GetByHash)

func TestEquivalence_BasicPutGet(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	// Test data
	// Note: BadgerDB does not allow empty keys, so we don't test that case
	testCases := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("longer-key-here"), []byte("longer-value-here")},
		{[]byte("empty-value"), []byte("")},
		{[]byte("binary\x00key"), []byte("binary\x00value")},
	}

	// Put in both
	for _, tc := range testCases {
		// BadgerDB
		err := badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Set(tc.key, tc.value)
		})
		require.NoError(t, err)

		// BlockchainDB
		err = bdb.Put(tc.key, tc.value)
		require.NoError(t, err)
	}

	// Get from both and compare
	for _, tc := range testCases {
		// BadgerDB
		var badgerValue []byte
		err := badgerDB.View(func(txn *badger.Txn) error {
			item, err := txn.Get(tc.key)
			if err != nil {
				return err
			}
			badgerValue, err = item.ValueCopy(nil)
			return err
		})
		require.NoError(t, err)

		// BlockchainDB
		bdbValue, err := bdb.Get(tc.key)
		require.NoError(t, err)

		// Compare using bytes.Equal which treats nil and []byte{} as equal
		assert.True(t, bytes.Equal(badgerValue, bdbValue), "values differ for key %q: badger=%v, bdb=%v", tc.key, badgerValue, bdbValue)
	}
}

func TestEquivalence_NotFound(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	// Get non-existent key from BadgerDB
	var badgerErr error
	badgerDB.View(func(txn *badger.Txn) error {
		_, badgerErr = txn.Get([]byte("nonexistent"))
		return nil
	})

	// Get non-existent key from BlockchainDB
	_, bdbErr := bdb.Get([]byte("nonexistent"))

	// Both should return an error
	assert.Error(t, badgerErr, "badger should return error for missing key")
	assert.Error(t, bdbErr, "bdb should return error for missing key")
}

func TestEquivalence_EmptyKeyRejected(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	// Both should reject empty keys
	var badgerErr error
	badgerDB.Update(func(txn *badger.Txn) error {
		badgerErr = txn.Set([]byte{}, []byte("value"))
		return nil
	})

	bdbErr := bdb.Put([]byte{}, []byte("value"))

	assert.Error(t, badgerErr, "badger should reject empty key")
	assert.Error(t, bdbErr, "bdb should reject empty key")
	assert.Contains(t, badgerErr.Error(), "empty")
	assert.ErrorIs(t, bdbErr, ErrEmptyKey)
}

func TestEquivalence_Delete(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	key := []byte("to-delete")
	value := []byte("will-be-gone")

	// Put in both
	badgerDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	bdb.Put(key, value)

	// Verify exists
	badgerDB.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		require.NoError(t, err)
		return nil
	})
	_, err = bdb.Get(key)
	require.NoError(t, err)

	// Delete from both
	badgerDB.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	err = bdb.Delete(key)
	require.NoError(t, err)

	// Verify deleted in both
	var badgerErr error
	badgerDB.View(func(txn *badger.Txn) error {
		_, badgerErr = txn.Get(key)
		return nil
	})
	_, bdbErr := bdb.Get(key)

	assert.Error(t, badgerErr, "badger should return error after delete")
	assert.Error(t, bdbErr, "bdb should return error after delete")
}

func TestEquivalence_Update(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	key := []byte("update-key")
	value1 := []byte("original")
	value2 := []byte("updated")

	// Initial put
	badgerDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value1)
	})
	bdb.Put(key, value1)

	// Update
	badgerDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value2)
	})
	bdb.Put(key, value2)

	// Verify updated value
	var badgerValue []byte
	badgerDB.View(func(txn *badger.Txn) error {
		item, _ := txn.Get(key)
		badgerValue, _ = item.ValueCopy(nil)
		return nil
	})
	bdbValue, _ := bdb.Get(key)

	assert.Equal(t, value2, badgerValue)
	assert.Equal(t, value2, bdbValue)
	assert.Equal(t, badgerValue, bdbValue)
}

func TestEquivalence_Iteration(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	// Insert test data with common prefix
	testData := map[string]string{
		"user:001": "alice",
		"user:002": "bob",
		"user:003": "charlie",
		"other:1":  "x",
		"other:2":  "y",
	}

	for k, v := range testData {
		badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(k), []byte(v))
		})
		bdb.Put([]byte(k), []byte(v))
	}

	// Iterate with prefix "user:" in BadgerDB
	var badgerResults []string
	badgerDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("user:")
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, _ := item.ValueCopy(nil)
			badgerResults = append(badgerResults, fmt.Sprintf("%s=%s", item.Key(), val))
		}
		return nil
	})

	// Iterate with prefix "user:" in BlockchainDB
	var bdbResults []string
	bdb.ForEachPrefix([]byte("user:"), func(key, value []byte) error {
		bdbResults = append(bdbResults, fmt.Sprintf("%s=%s", key, value))
		return nil
	})

	// Sort both for comparison (iteration order may differ)
	sort.Strings(badgerResults)
	sort.Strings(bdbResults)

	assert.Equal(t, badgerResults, bdbResults, "prefix iteration results differ")
	assert.Len(t, badgerResults, 3, "should find 3 user: entries")
}

func TestEquivalence_FullIteration(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	// Insert test data
	testData := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}

	for k, v := range testData {
		badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(k), []byte(v))
		})
		bdb.Put([]byte(k), []byte(v))
	}

	// Full iteration in BadgerDB
	var badgerResults []string
	badgerDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, _ := item.ValueCopy(nil)
			badgerResults = append(badgerResults, fmt.Sprintf("%s=%s", item.Key(), val))
		}
		return nil
	})

	// Full iteration in BlockchainDB
	var bdbResults []string
	bdb.ForEach(func(key, value []byte) error {
		bdbResults = append(bdbResults, fmt.Sprintf("%s=%s", key, value))
		return nil
	})

	// Sort both for comparison
	sort.Strings(badgerResults)
	sort.Strings(bdbResults)

	assert.Equal(t, badgerResults, bdbResults, "full iteration results differ")
}

func TestEquivalence_Persistence(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	key := []byte("persistent-key")
	value := []byte("persistent-value")

	// Write and close BadgerDB
	{
		db := openBadger(t, badgerDir)
		db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		db.Close()
	}

	// Write and close BlockchainDB
	{
		db, err := NewBlockchainStore(bdbDir)
		require.NoError(t, err)
		db.Put(key, value)
		db.Close()
	}

	// Reopen and verify BadgerDB
	{
		db := openBadger(t, badgerDir)
		defer db.Close()
		var got []byte
		db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			require.NoError(t, err)
			got, _ = item.ValueCopy(nil)
			return nil
		})
		assert.Equal(t, value, got)
	}

	// Reopen and verify BlockchainDB
	{
		db, err := NewBlockchainStore(bdbDir)
		require.NoError(t, err)
		defer db.Close()
		got, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, value, got)
	}
}

func TestEquivalence_LargeValues(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	// Test with various sizes
	sizes := []int{1024, 64 * 1024, 1024 * 1024} // 1KB, 64KB, 1MB

	for _, size := range sizes {
		key := []byte(fmt.Sprintf("large-%d", size))
		value := bytes.Repeat([]byte("x"), size)

		// Put in both
		badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		bdb.Put(key, value)

		// Get and compare
		var badgerValue []byte
		badgerDB.View(func(txn *badger.Txn) error {
			item, _ := txn.Get(key)
			badgerValue, _ = item.ValueCopy(nil)
			return nil
		})
		bdbValue, _ := bdb.Get(key)

		assert.Equal(t, len(value), len(badgerValue), "badger value size mismatch for size %d", size)
		assert.Equal(t, len(value), len(bdbValue), "bdb value size mismatch for size %d", size)
		assert.Equal(t, badgerValue, bdbValue, "values differ for size %d", size)
	}
}

func TestEquivalence_ManyKeys(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	numKeys := 1000

	// Insert many keys
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d", i))

		badgerDB.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		bdb.Put(key, value)
	}

	// Verify all keys
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		expected := []byte(fmt.Sprintf("value-%05d", i))

		var badgerValue []byte
		badgerDB.View(func(txn *badger.Txn) error {
			item, _ := txn.Get(key)
			badgerValue, _ = item.ValueCopy(nil)
			return nil
		})
		bdbValue, _ := bdb.Get(key)

		assert.Equal(t, expected, badgerValue, "badger mismatch at key %d", i)
		assert.Equal(t, expected, bdbValue, "bdb mismatch at key %d", i)
	}

	// Count via iteration
	badgerCount := 0
	badgerDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			badgerCount++
		}
		return nil
	})

	bdbCount := 0
	bdb.ForEach(func(key, value []byte) error {
		bdbCount++
		return nil
	})

	assert.Equal(t, numKeys, badgerCount)
	assert.Equal(t, numKeys, bdbCount)
}

func TestEquivalence_BatchOperations(t *testing.T) {
	badgerDir, bdbDir := setupDirs(t)
	defer os.RemoveAll(filepath.Dir(badgerDir))

	badgerDB := openBadger(t, badgerDir)
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	require.NoError(t, err)
	defer bdb.Close()

	// Batch write in BadgerDB
	badgerDB.Update(func(txn *badger.Txn) error {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("batch-%03d", i))
			value := []byte(fmt.Sprintf("bval-%03d", i))
			txn.Set(key, value)
		}
		return nil
	})

	// Sequential writes in BlockchainDB (no native batch)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("batch-%03d", i))
		value := []byte(fmt.Sprintf("bval-%03d", i))
		bdb.Put(key, value)
	}

	// Verify identical results
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("batch-%03d", i))

		var badgerValue []byte
		badgerDB.View(func(txn *badger.Txn) error {
			item, _ := txn.Get(key)
			badgerValue, _ = item.ValueCopy(nil)
			return nil
		})
		bdbValue, _ := bdb.Get(key)

		assert.Equal(t, badgerValue, bdbValue, "batch value mismatch at %d", i)
	}
}

// Helper functions

func setupDirs(t *testing.T) (badgerDir, bdbDir string) {
	t.Helper()
	baseDir := t.TempDir()
	badgerDir = filepath.Join(baseDir, "badger")
	bdbDir = filepath.Join(baseDir, "bdb")
	return
}

func openBadger(t *testing.T, dir string) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil // Suppress logging
	db, err := badger.Open(opts)
	require.NoError(t, err)
	return db
}
