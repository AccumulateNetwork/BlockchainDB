package blockchainDB

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompress
// Overwrite keys many times, compress, and verify the values file
// shrinks while every key still returns its latest value.
// Regression test for issue #7 (Compress was a no-op).
func TestCompress(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kv, err := NewKV(false, dir, 16, 100_000, 5)
	require.NoError(t, err, "create kv")

	// Write 100 keys, then overwrite each 9 more times: ~90% of the
	// values file is trash
	kr := NewFastRandom([]byte{11})
	vr := NewFastRandom([]byte{11, 11})
	keys := make([][32]byte, 100)
	latest := make([][]byte, 100)
	for i := range keys {
		keys[i] = kr.NextHash()
	}
	for round := 0; round < 10; round++ {
		for i := range keys {
			latest[i] = vr.RandBuff(50, 200)
			require.NoError(t, kv.Put(keys[i], latest[i]))
		}
	}
	require.NoError(t, kv.vFile.Flush())

	before, err := os.Stat(filepath.Join(dir, valueFilename))
	require.NoError(t, err)

	require.NoError(t, kv.Compress(), "compress failed")

	after, err := os.Stat(filepath.Join(dir, valueFilename))
	require.NoError(t, err)
	assert.Lessf(t, after.Size(), before.Size()/5,
		"values file did not shrink (before %d, after %d)", before.Size(), after.Size())

	// Every key must return its latest value
	for i := range keys {
		v, err := kv.Get(keys[i])
		require.NoErrorf(t, err, "get failed for key %d after compress", i)
		assert.Equalf(t, latest[i], v, "wrong value for key %d after compress", i)
	}

	// And the DB must still work across close/reopen after a compress
	require.NoError(t, kv.Close())
	kv2, err := OpenKV(dir)
	require.NoError(t, err, "reopen after compress")
	require.NoError(t, kv2.Open())
	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "get failed for key %d after reopen", i)
		assert.Equalf(t, latest[i], v, "wrong value for key %d after reopen", i)
	}

	// New writes after compress must work
	newVal := []byte("post-compress value")
	require.NoError(t, kv2.Put(keys[0], newVal))
	v, err := kv2.Get(keys[0])
	require.NoError(t, err)
	assert.Equal(t, newVal, v)
	require.NoError(t, kv2.Close())
}

// TestCompressPermIsNoop
// A history-enabled KV holds immutable values; Compress must be a no-op
// and must not disturb the data.
func TestCompressPermIsNoop(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kv, err := NewKV(true, dir, 16, 1000, 5)
	require.NoError(t, err, "create kv")
	kr := NewFastRandom([]byte{12})
	vr := NewFastRandom([]byte{12, 12})
	keys := make([][32]byte, 50)
	values := make([][]byte, 50)
	for i := range keys {
		keys[i] = kr.NextHash()
		values[i] = vr.RandBuff(10, 50)
		require.NoError(t, kv.Put(keys[i], values[i]))
	}
	require.NoError(t, kv.Compress())
	for i := range keys {
		v, err := kv.Get(keys[i])
		require.NoError(t, err)
		assert.Equal(t, values[i], v)
	}
}
