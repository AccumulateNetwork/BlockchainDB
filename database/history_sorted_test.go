package blockchainDB

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHistoryKeySetsSorted
// Verifies the KeySet sorting invariant Get's binary search relies on:
// after multiple pushes, every KeySet's records are sorted by key, and
// every key is still found.
func TestHistoryKeySetsSorted(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Small KeyLimit forces many history pushes
	kv, err := NewKV(true, dir, 16, 100, 5)
	require.NoError(t, err, "create kv")

	kr := NewFastRandom([]byte{7})
	vr := NewFastRandom([]byte{7, 7})
	keys := make([][32]byte, 1000)
	values := make([][]byte, 1000)
	for i := range keys {
		keys[i] = kr.NextHash()
		values[i] = vr.RandBuff(10, 50)
		require.NoError(t, kv.Put(keys[i], values[i]))
	}

	// Every KeySet must be sorted by key with no duplicates
	hf := kv.kFile.History
	require.NotNil(t, hf, "history must exist")
	for bin, ks := range hf.KeySets {
		length := ks.End - ks.Start
		if length == 0 {
			continue
		}
		buff := make([]byte, length)
		_, err := hf.File.ReadAt(buff, int64(ks.Start))
		require.NoError(t, err)
		for pos := DBKeyFullSize; pos < len(buff); pos += DBKeyFullSize {
			prev := buff[pos-DBKeyFullSize : pos-DBKeyFullSize+32]
			cur := buff[pos : pos+32]
			assert.Negativef(t, bytes.Compare(prev, cur),
				"bin %d not strictly sorted at record %d", bin, pos/DBKeyFullSize)
		}
	}

	// And every key must still resolve to its value
	for i := range keys {
		v, err := kv.Get(keys[i])
		require.NoErrorf(t, err, "key %d not found", i)
		assert.Equalf(t, values[i], v, "wrong value for key %d", i)
	}
}

// TestImmutabilityThroughHistory
// A key that has been pushed to history must still be immutable: a Put
// with a different value must fail even though the key is no longer in
// the current kfile.  (Previously the Put existence check only looked at
// the current kfile, so pushed keys could be silently overwritten.)
func TestImmutabilityThroughHistory(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))
	defer os.RemoveAll(dir)

	kFile, err := NewKFile(true, dir, 16, 10, 5)
	require.NoError(t, err, "create kfile")

	kr := NewFastRandom([]byte{8})
	first := kr.NextHash()
	firstVal := &DBBKey{Offset: 1, Length: 10}
	require.NoError(t, kFile.Put(first, firstVal))

	// Push the first key into history by exceeding KeyLimit
	for i := 0; i < 20; i++ {
		require.NoError(t, kFile.Put(kr.NextHash(), &DBBKey{Offset: uint64(i + 2), Length: 10}))
	}

	// Overwriting with a different value must fail
	err = kFile.Put(first, &DBBKey{Offset: 999, Length: 10})
	assert.Error(t, err, "immutable key in history was overwritten")

	// Overwriting with the same value is a no-op
	assert.NoError(t, kFile.Put(first, firstVal), "same-value rewrite should be a no-op")
}
