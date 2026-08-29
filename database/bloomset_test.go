package blockchainDB

import (
	"bytes"
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
// A streamed BloomSet must read back with identical behavior
func TestBloomSetSaveLoad(t *testing.T) {
	bs := NewBloomSet(1, 3)
	fr := NewFastRandom([]byte{72})
	keys := make([][32]byte, 10_000)
	for i := range keys {
		keys[i] = fr.NextHash()
		bs.Set(keys[i])
	}
	var buf bytes.Buffer
	require.NoError(t, bs.write(&buf))

	loaded, err := readBloomSet(&buf)
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
