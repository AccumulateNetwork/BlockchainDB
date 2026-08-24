package blockchainDB

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file cover the create -> write -> close -> open -> read
// round trip.  See issues #3 and #4: no test reopened a database, which let
// several reopen-path bugs (empty offset table, unpopulated Bloom filter,
// nil KeySets panic, lost KeyLimit) go undetected.

// putN writes n deterministic key/value pairs and returns the keys
func putN(t *testing.T, kv *KV, seed byte, n int) (keys [][32]byte, values [][]byte) {
	t.Helper()
	kr := NewFastRandom([]byte{seed})
	vr := NewFastRandom([]byte{seed, seed})
	for i := 0; i < n; i++ {
		key := kr.NextHash()
		value := vr.RandBuff(10, 100)
		require.NoError(t, kv.Put(key, value), "put failed")
		keys = append(keys, key)
		values = append(values, value)
	}
	return keys, values
}

// getN verifies that all the given key/value pairs can be read back
func getN(t *testing.T, kv *KV, keys [][32]byte, values [][]byte) {
	t.Helper()
	for i, key := range keys {
		v, err := kv.Get(key)
		require.NoErrorf(t, err, "get failed for key %d", i)
		assert.Equalf(t, values[i], v, "wrong value for key %d", i)
	}
}

// TestKVReopenDyna
// A history-disabled (mutable) KV must return its keys after close/reopen
func TestKVReopenDyna(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	defer os.RemoveAll(dir)

	kv, err := NewKV(false, dir, 16, 1000, 5)
	require.NoError(t, err, "create kv")
	keys, values := putN(t, kv, 1, 100)
	getN(t, kv, keys, values)
	require.NoError(t, kv.Close(), "close kv")

	kv2, err := OpenKV(dir)
	require.NoError(t, err, "reopen kv")
	require.NoError(t, kv2.Open(), "open kv")
	getN(t, kv2, keys, values)

	// The reopened KV must keep accepting writes without losing anything
	keys2, values2 := putN(t, kv2, 2, 100)
	getN(t, kv2, keys2, values2)
	getN(t, kv2, keys, values)
	require.NoError(t, kv2.Close(), "close kv again")
}

// TestKVReopenPerm
// A history-enabled (immutable) KV must return its keys after close/reopen,
// including keys that were pushed into the history file
func TestKVReopenPerm(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	defer os.RemoveAll(dir)

	// KeyLimit of 50 forces history pushes during the 200 puts
	kv, err := NewKV(true, dir, 16, 50, 5)
	require.NoError(t, err, "create kv")
	keys, values := putN(t, kv, 3, 200)
	getN(t, kv, keys, values)
	require.NoError(t, kv.Close(), "close kv")

	kv2, err := OpenKV(dir)
	require.NoError(t, err, "reopen kv")
	require.NoError(t, kv2.Open(), "open kv")
	getN(t, kv2, keys, values)
	require.NoError(t, kv2.Close(), "close kv again")
}

// TestKV2Reopen
// A KV2 (Perm + Dyna layers) must return keys from both layers after reopen
func TestKV2Reopen(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	defer os.RemoveAll(dir)

	kv2, err := NewKV2(dir, 16, 50, 5)
	require.NoError(t, err, "create kv2")

	kr := NewFastRandom([]byte{4})
	vr := NewFastRandom([]byte{4, 4})
	var keys [][32]byte
	var values [][]byte
	for i := 0; i < 100; i++ {
		key := kr.NextHash()
		value := vr.RandBuff(10, 100)
		if i%2 == 0 {
			_, err = kv2.PutPerm(key, value)
		} else {
			_, err = kv2.PutDyna(key, value)
		}
		require.NoError(t, err, "put failed")
		keys = append(keys, key)
		values = append(values, value)
	}
	require.NoError(t, kv2.Close(), "close kv2")

	reopened, err := OpenKV2(dir)
	require.NoError(t, err, "reopen kv2")
	require.NoError(t, reopened.Open(), "open kv2")
	for i, key := range keys {
		v, err := reopened.Get(key)
		require.NoErrorf(t, err, "get failed for key %d", i)
		assert.Equalf(t, values[i], v, "wrong value for key %d", i)
	}
	require.NoError(t, reopened.Close(), "close kv2 again")
}

// TestDynaKeyLimitPreservesKeys
// A history-disabled KV must not lose keys when KeyLimit is exceeded.
// Regression test for issue #4: the old PushHistory deleted the kfile,
// discarding every key, when history was disabled.
func TestDynaKeyLimitPreservesKeys(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	defer os.RemoveAll(dir)

	// KeyLimit of 10, then write 25 keys to force multiple push cycles
	kv, err := NewKV(false, dir, 16, 10, 5)
	require.NoError(t, err, "create kv")
	keys, values := putN(t, kv, 5, 25)
	getN(t, kv, keys, values)

	// Overwrites must survive push cycles too (Dyna values are mutable)
	newValue := []byte("updated value")
	require.NoError(t, kv.Put(keys[0], newValue), "overwrite failed")
	for i := 0; i < 15; i++ { // push past KeyLimit again
		_, _ = putN(t, kv, byte(6+i), 1)
	}
	v, err := kv.Get(keys[0])
	require.NoError(t, err, "get after overwrite")
	assert.Equal(t, newValue, v, "overwrite lost")
}
