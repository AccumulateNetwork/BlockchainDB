package blockchainDB

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func sealPerm(t *testing.T, p *PermStore, height uint64) {
	t.Helper()
	s, err := p.beginSeal(height)
	require.NoError(t, err)
	require.NoError(t, s.finish())
}

// Records are written once and found in the window; past the window
// Get says absent and GetDeep finds them through the buckets and,
// after a pack, the retired run; the manifest and the deltas replay
// on reopen; a torn delta is dropped whole.
func TestPermStoreTiersAndReopen(t *testing.T) {
	every := PermMergeEvery
	PermMergeEvery = 4 // 64 buckets a merge
	defer func() { PermMergeEvery = every }()
	dir := filepath.Join(t.TempDir(), "perm")
	p, err := NewPermStore(dir, MinFilterBlocks)
	require.NoError(t, err)
	fr := NewFastRandom([]byte{21})
	const perBlock = 300
	var keys [][32]byte
	value := func(b uint64, i int) []byte { return []byte{byte(b >> 8), byte(b), byte(i >> 8), byte(i), 'v'} }
	for b := uint64(1); b <= 3*MinFilterBlocks; b++ {
		p.AdvanceBlock(b)
		for i := 0; i < perBlock; i++ {
			k := fr.NextHash()
			keys = append(keys, k)
			existing, existed, err := p.PutIfAbsent(k, value(b, i))
			require.NoError(t, err)
			require.False(t, existed)
			require.Nil(t, existing)
		}
		// A key written again in its block is refused with its value
		existing, existed, err := p.PutIfAbsent(keys[len(keys)-1], []byte("other"))
		require.NoError(t, err)
		require.True(t, existed)
		require.Equal(t, value(b, perBlock-1), existing)
		sealPerm(t, p, b)
		if b%5 == 0 {
			require.NoError(t, p.Merge())
		}
	}
	// The window holds the last MinFilterBlocks blocks; older keys are
	// absent to Get and present to GetDeep
	inWindow := keys[2*MinFilterBlocks*perBlock:]
	older := keys[:2*MinFilterBlocks*perBlock]
	for i, k := range inWindow[:perBlock] {
		v, err := p.Get(k)
		require.NoError(t, err)
		require.Equal(t, value(2*MinFilterBlocks+1, i), v)
	}
	for _, k := range older[:100] {
		_, err := p.Get(k)
		require.ErrorIs(t, err, errNotFound, "past the window, absent on the protocol path")
		_, err = p.GetDeep(k)
		require.NoError(t, err, "and found deep")
	}
	_, err = p.Get(fr.NextHash())
	require.ErrorIs(t, err, errNotFound)
	st := p.Stats()
	require.Greater(t, st.HistorySegments, 0, "buckets hold runs")

	// A pack retires the buckets and the pending deltas into one run
	require.NoError(t, p.Pack())
	require.Len(t, p.retired, 1)
	for b := range p.buckets {
		require.Empty(t, p.buckets[b].runs)
	}
	for _, k := range older[:200] {
		_, err = p.GetDeep(k)
		require.NoError(t, err, "found in the retired run")
	}

	// More blocks, sealed but never in a manifest, then reopen
	for b := uint64(3*MinFilterBlocks + 1); b <= 3*MinFilterBlocks+5; b++ {
		p.AdvanceBlock(b)
		for i := 0; i < perBlock; i++ {
			k := fr.NextHash()
			keys = append(keys, k)
			require.NoError(t, p.Put(k, value(b, i)))
		}
		sealPerm(t, p, b)
	}
	// A torn entry after the last seal: the crash
	_, err = p.cur.f.WriteAt([]byte{9, 0, 0, 0, 1, 2, 3, 4, 5, 6}, p.cur.size)
	require.NoError(t, err)
	last := keys[len(keys)-perBlock:]
	for _, hf := range p.files {
		hf.f.Close()
	}
	for _, rf := range p.runs {
		rf.f.Close()
	}
	r, err := OpenPermStore(dir)
	require.NoError(t, err)
	defer r.Close()
	require.EqualValues(t, 3*MinFilterBlocks+6, r.height, "the replayed deltas leave the height at the next block, as the seal does")
	for i, k := range last {
		v, err := r.Get(k)
		require.NoError(t, err, "a delta replayed after the manifest")
		require.Equal(t, value(3*MinFilterBlocks+5, i), v)
	}
	for _, k := range older[:100] {
		_, err = r.GetDeep(k)
		require.NoError(t, err, "the retired run came back through the manifest")
	}
	_, err = r.Get(fr.NextHash())
	require.ErrorIs(t, err, errNotFound)
}

// A shard built with both layers as files seals, merges, packs,
// closes and reopens as files, through the same KV2 and KVShard
// surface; permanent keys are found in the window and, once past it,
// by GetDeep; dynamic keys keep their last value.
func TestFilesShardRoundTrip(t *testing.T) {
	every := PermMergeEvery
	PermMergeEvery = 8
	defer func() { PermMergeEvery = every }()
	dir := filepath.Join(t.TempDir(), "shards")
	kvs, err := NewKVShardFilesN(dir, 2, 1000)
	require.NoError(t, err)
	require.NoError(t, kvs.SetFilterBlocks(MinFilterBlocks))
	fr := NewFastRandom([]byte{9})
	hot := make([][32]byte, 100)
	for i := range hot {
		hot[i] = fr.NextHash()
	}
	var perm [][32]byte
	for b := uint64(1); b <= 3*MinFilterBlocks; b++ {
		for _, k := range hot {
			require.NoError(t, kvs.PutDyna(k, append([]byte{byte(b)}, k[:4]...)))
		}
		for i := 0; i < 50; i++ {
			k := fr.NextHash()
			perm = append(perm, k)
			require.NoError(t, kvs.PutPerm(k, append([]byte{byte(b)}, k[:4]...)))
		}
		require.NoError(t, kvs.SealBlock(b))
		if b%10 == 0 {
			require.NoError(t, kvs.Compress())
			_, err := kvs.MergeFinalized(b)
			require.NoError(t, err)
		}
	}
	for _, k := range hot {
		v, err := kvs.GetDyna(k)
		require.NoError(t, err)
		require.Equal(t, byte(3*MinFilterBlocks), v[0])
	}
	recent := perm[len(perm)-50:]
	old := perm[:50]
	for _, k := range recent {
		_, err := kvs.GetPerm(k)
		require.NoError(t, err, "in the window")
	}
	for _, k := range old {
		_, err := kvs.GetPerm(k)
		require.ErrorIs(t, err, errNotFound, "past the window")
		v, err := kvs.Shards[kvs.ShardIndex(k[:])].GetPermDeep(k)
		require.NoError(t, err, "found deep")
		require.Equal(t, byte(1), v[0])
	}
	_, packed, err := kvs.PackFinalized(3 * MinFilterBlocks)
	require.NoError(t, err)
	require.True(t, packed)
	require.NoError(t, kvs.Close())

	re, err := OpenKVShard(dir)
	require.NoError(t, err)
	defer re.Close()
	require.NotNil(t, re.Shards[0].Perm, "reopened as files")
	require.NotNil(t, re.Shards[0].Heap)
	for _, k := range hot {
		v, err := re.GetDyna(k)
		require.NoError(t, err)
		require.Equal(t, byte(3*MinFilterBlocks), v[0])
	}
	for _, k := range old {
		v, err := re.Shards[re.ShardIndex(k[:])].GetPermDeep(k)
		require.NoError(t, err, "the retired run came back")
		require.Equal(t, byte(1), v[0])
	}
}
