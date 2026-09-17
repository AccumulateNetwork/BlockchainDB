package blockchainDB

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A data file rolled by a seal between two manifest commits is still
// replayed on reopen: every block sealed before Close is found, and
// the store keeps writing after (spec 1.7: what a seal made durable
// stays durable).
func TestPermStoreReopenAcrossRolledFiles(t *testing.T) {
	size := PermFileBytes
	PermFileBytes = 64 << 10
	defer func() { PermFileBytes = size }()
	dir := filepath.Join(t.TempDir(), "perm")
	p, err := NewPermStore(dir, MinFilterBlocks)
	require.NoError(t, err)
	fr := NewFastRandom([]byte{7})
	var keys [][32]byte
	value := make([]byte, 1000)
	for b := uint64(1); b <= 8; b++ { // 8 blocks x 40 KB: several rolls, no merge
		p.AdvanceBlock(b)
		for i := 0; i < 40; i++ {
			k := fr.NextHash()
			keys = append(keys, k)
			value[0] = byte(b)
			require.NoError(t, p.Put(k, value))
		}
		sealPerm(t, p, b)
	}
	require.Greater(t, len(p.files), 1, "the test must roll a data file")
	require.NoError(t, p.Close())

	p, err = OpenPermStore(dir)
	require.NoError(t, err)
	for i, k := range keys {
		v, err := p.GetDeep(k)
		require.NoError(t, err, "key %d after reopen", i)
		require.Equal(t, byte(i/40+1), v[0])
	}
	require.Equal(t, uint64(9), p.BlockHeight())
	// And the store keeps going: a new block, sealed, merged, reopened
	p.AdvanceBlock(9)
	k := fr.NextHash()
	require.NoError(t, p.Put(k, value))
	sealPerm(t, p, 9)
	require.NoError(t, p.Merge())
	require.NoError(t, p.Close())
	p, err = OpenPermStore(dir)
	require.NoError(t, err)
	_, err = p.GetDeep(k)
	require.NoError(t, err)
	_, err = p.GetDeep(keys[0])
	require.NoError(t, err)
	require.NoError(t, p.Close())
}
