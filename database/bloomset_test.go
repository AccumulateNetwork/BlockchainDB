package blockchainDB

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBloomSetGrowth
// Layers must grow as keys outrun capacity, no key may be lost across
// growth, and the false positive rate must stay bounded.
func TestBloomSetGrowth(t *testing.T) {
	bs := NewBloomSet(1, 3) // Floor capacity (~2,700 keys); forces growth

	fr := NewFastRandom([]byte{71})
	const n = 100_000
	keys := make([][32]byte, n)
	for i := range keys {
		keys[i] = fr.NextHash()
		bs.Set(keys[i])
	}
	assert.Greater(t, len(bs.Layers), 1, "the set should have grown layers")
	assert.Equal(t, uint64(n), bs.Count(), "count should track adds")

	// No false negatives, ever
	for i := range keys {
		require.Truef(t, bs.Test(keys[i]), "key %d lost after growth", i)
	}

	// False positive rate stays bounded (~3% design point; assert < 6%)
	fp := 0
	const probes = 100_000
	for i := 0; i < probes; i++ {
		if bs.Test(fr.NextHash()) {
			fp++
		}
	}
	assert.Lessf(t, float64(fp)/probes, 0.06, "false positive rate too high: %d/%d", fp, probes)
}

// TestBloomSetSaveLoad
// A saved BloomSet must load back with identical behavior
func TestBloomSetSaveLoad(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))
	defer os.RemoveAll(dir)

	bs := NewBloomSet(1, 3)
	fr := NewFastRandom([]byte{72})
	keys := make([][32]byte, 10_000)
	for i := range keys {
		keys[i] = fr.NextHash()
		bs.Set(keys[i])
	}
	require.NoError(t, bs.Save(dir))

	loaded, err := LoadBloomSet(dir)
	require.NoError(t, err)
	assert.Equal(t, len(bs.Layers), len(loaded.Layers), "layer count differs")
	assert.Equal(t, bs.Count(), loaded.Count(), "count differs")
	for i := range keys {
		require.Truef(t, loaded.Test(keys[i]), "key %d missing after load", i)
	}
	// A loaded set keeps accepting keys and growing
	extra := fr.NextHash()
	loaded.Set(extra)
	assert.True(t, loaded.Test(extra))
}

// TestBloomPersistedOpen
// After history pushes, bloom.dat must exist, and a reopened KFile
// must find keys via the loaded filter (covering both the persisted
// layers and the kfile scan for keys added after the last push).
func TestBloomPersistedOpen(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kv, err := NewKV(true, dir, 16, 50, 5)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{73})
	vr := NewFastRandom([]byte{73, 73})
	keys := make([][32]byte, 220) // 220 keys, KeyLimit 50 -> several pushes
	values := make([][]byte, 220)
	for i := range keys {
		keys[i] = kr.NextHash()
		values[i] = vr.RandBuff(10, 50)
		require.NoError(t, kv.Put(keys[i], values[i]))
	}
	require.NoError(t, kv.Close())

	_, err = os.Stat(filepath.Join(dir, bloomFilename))
	require.NoError(t, err, "bloom.dat should exist after history pushes")

	kv2, err := OpenKV(dir)
	require.NoError(t, err)
	require.NoError(t, kv2.Open())
	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d missing after reopen with persisted bloom", i)
		assert.Equal(t, values[i], v)
	}
	require.NoError(t, kv2.Close())
}
