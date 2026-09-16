package blockchainDB

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func heapDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "heap")
}

func key(b byte) (k [32]byte) { k[0] = b; return }

// A key rewritten within the block reuses its slot; rewritten in a
// later block it is appended, the old slot dead where it lies until a
// clean pass moves the live entries past it and the next sync
// releases the region.
func TestHeapRewriteReusesWithinTheBlockAndCleansOneSyncLate(t *testing.T) {
	h, err := NewHeapStore(heapDir(t))
	require.NoError(t, err)
	defer h.Close()
	h.AdvanceBlock(1)
	require.NoError(t, h.Put(key(1), []byte("one")))
	off := h.index[key(1)].off
	require.NoError(t, h.Put(key(1), []byte("uno")))
	require.Equal(t, off, h.index[key(1)].off, "same block, fits: in place")
	require.EqualValues(t, 1, h.putInPlace.Load())
	v, err := h.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, "uno", string(v))

	// Block 1 durable; block 2 rewrites the key
	sync := func() {
		p, err := h.beginBlockSync()
		require.NoError(t, err)
		require.NoError(t, p.finish())
	}
	sync()
	h.AdvanceBlock(2)
	require.NoError(t, h.Put(key(1), []byte("two")))
	require.NotEqual(t, off, h.index[key(1)].off, "a durable slot is never rewritten")
	dead, live := h.HoleRatio()
	require.EqualValues(t, heapMinCap, dead, "the old slot is dead where it lies")
	require.EqualValues(t, heapMinCap, live)
	sync()

	// A clean pass in block 3 moves the live entry past the dead one;
	// the region is released only by the sync after it
	h.AdvanceBlock(3)
	cleaned, err := h.clean(1 << 20)
	require.NoError(t, err)
	require.True(t, cleaned)
	require.EqualValues(t, 0, h.head, "not released yet: the copies are not durable")
	scanned, moved := h.Cleaned()
	require.EqualValues(t, 2*heapMinCap, scanned)
	require.EqualValues(t, heapMinCap, moved, "one live entry copied, one dead skipped")
	sync()
	require.EqualValues(t, 2*heapMinCap, h.head, "released after the sync")
	dead, live = h.HoleRatio()
	require.Zero(t, dead)
	require.EqualValues(t, heapMinCap, live)
	v, err = h.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, "two", string(v))
}

// Reopening replays the log: every synced value is back, the holes
// are derived, and a block that was never synced is gone -- its
// slots unnamed and its bytes cut from the file.
func TestHeapReopenKeepsTheDurableAndDropsTheRest(t *testing.T) {
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	h.AdvanceBlock(1)
	for i := byte(1); i <= 50; i++ {
		require.NoError(t, h.Put(key(i), []byte{i}))
	}
	p, err := h.beginBlockSync()
	require.NoError(t, err)
	require.NoError(t, p.finish())
	h.AdvanceBlock(2)
	require.NoError(t, h.Put(key(1), []byte("rewritten in block 2"))) // New slot, old pending
	p, err = h.beginBlockSync()
	require.NoError(t, err)
	require.NoError(t, p.finish())
	size := h.size
	// Block 3: written, never synced -- the crash
	h.AdvanceBlock(3)
	require.NoError(t, h.Put(key(2), []byte("lost")))
	require.NoError(t, h.Put(key(99), []byte("lost too")))
	// Drop the store without Close: the OS has the bytes, the log has no delta
	h.file.Close()
	h.log.Close()

	r, err := OpenHeapStore(dir)
	require.NoError(t, err)
	defer r.Close()
	v, err := r.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, "rewritten in block 2", string(v))
	v, err = r.Get(key(2))
	require.NoError(t, err)
	require.Equal(t, []byte{2}, v, "block 3's rewrite was never durable")
	_, err = r.Get(key(99))
	require.ErrorIs(t, err, errNotFound)
	require.Equal(t, size, r.size, "the file is cut back to the durable append point")
	dead, _ := r.HoleRatio()
	require.EqualValues(t, heapMinCap, dead, "key 1's block-1 slot is dead where it lies")
	require.EqualValues(t, 2, r.height, "the durable height: block 3 never synced")
}

// A torn delta at the end of the log is dropped whole.
func TestHeapTornLogTailIsDropped(t *testing.T) {
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	h.AdvanceBlock(1)
	require.NoError(t, h.Put(key(1), []byte("one")))
	require.NoError(t, h.Close())
	f, err := os.OpenFile(filepath.Join(dir, "index.log"), os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x50, 0x41, 0x45, 0x48, 9, 9}) // A marker and six bytes of nothing
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, err := OpenHeapStore(dir)
	require.NoError(t, err)
	defer r.Close()
	v, err := r.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, "one", string(v))
	st, err := os.Stat(filepath.Join(dir, "index.log"))
	require.NoError(t, err)
	require.EqualValues(t, heapDeltaHdr+heapDeltaRec+4, st.Size(), "one whole delta of one key remains")
}

// A snapshot carries the map and empties the log; what comes after is
// replayed on top of it.
func TestHeapSnapshotBoundsTheReplay(t *testing.T) {
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	every := HeapSnapshotEvery
	HeapSnapshotEvery = 1
	defer func() { HeapSnapshotEvery = every }()
	for b := uint64(1); b <= 30; b++ {
		h.AdvanceBlock(b)
		for i := byte(1); i <= 20; i++ {
			require.NoError(t, h.Put(key(i), []byte{byte(b), i}))
		}
		p, err := h.beginBlockSync()
		require.NoError(t, err)
		require.NoError(t, p.finish())
		if b == 20 {
			ok, err := h.compact()
			require.NoError(t, err)
			require.True(t, ok)
			st, err := os.Stat(filepath.Join(dir, "index.log"))
			require.NoError(t, err)
			require.Zero(t, st.Size(), "the log is empty after the snapshot")
		}
	}
	require.NoError(t, h.Close())
	r, err := OpenHeapStore(dir)
	require.NoError(t, err)
	defer r.Close()
	for i := byte(1); i <= 20; i++ {
		v, err := r.Get(key(i))
		require.NoError(t, err)
		require.Equal(t, []byte{30, i}, v)
	}
	// The one clean pass at block 20 found blocks 1-19 all dead and
	// stopped at block 20's slots, which the sync at block 21 released;
	// blocks 20-29 lie dead behind block 30's live slots.
	dead, live := r.HoleRatio()
	require.EqualValues(t, 20*heapMinCap, live)
	require.EqualValues(t, 10*20*heapMinCap, dead)
	require.EqualValues(t, 19*20*heapMinCap, r.head, "released by the sync after the clean")
}

// A slot whose bytes were damaged is an error, never a value.
func TestHeapChecksumCatchesADamagedSlot(t *testing.T) {
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	h.AdvanceBlock(1)
	require.NoError(t, h.Put(key(1), []byte("intact")))
	off := h.index[key(1)].off
	require.NoError(t, h.Close())
	f, err := os.OpenFile(filepath.Join(dir, "heap.dat"), os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("damaged"), off+heapHeader)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	r, err := OpenHeapStore(dir)
	require.NoError(t, err)
	defer r.Close()
	_, err = r.Get(key(1))
	require.ErrorContains(t, err, "checksum")
}

// A shard built with the heap seals, compacts, closes and reopens as
// a heap, through the same KV2 and KVShard surface.
func TestHeapShardRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shards")
	kvs, err := NewKVShardHeapN(dir, 2, 1000)
	require.NoError(t, err)
	require.NoError(t, kvs.SetFilterBlocks(MinFilterBlocks))
	fr := NewFastRandom([]byte{7})
	hot := make([][32]byte, 200)
	for i := range hot {
		hot[i] = fr.NextHash()
	}
	for b := uint64(1); b <= 45; b++ {
		for _, k := range hot {
			require.NoError(t, kvs.PutDyna(k, append([]byte{byte(b)}, k[:8]...)))
		}
		require.NoError(t, kvs.PutPerm(fr.NextHash(), []byte("perm")))
		require.NoError(t, kvs.SealBlock(b))
		if b%20 == 0 {
			require.NoError(t, kvs.Compress())
			_, err := kvs.MergeFinalized(b - MinFilterBlocks)
			require.NoError(t, err)
		}
	}
	for _, k := range hot {
		v, err := kvs.GetDyna(k)
		require.NoError(t, err)
		require.Equal(t, byte(45), v[0])
	}
	_, dyna := kvs.Stats()
	require.EqualValues(t, 45*200, dyna.PutTotal)
	require.NoError(t, kvs.Close())

	re, err := OpenKVShard(dir)
	require.NoError(t, err)
	defer re.Close()
	require.NotNil(t, re.Shards[0].Heap, "reopened as a heap")
	require.Nil(t, re.Shards[0].DynaKV)
	for _, k := range hot {
		v, err := re.GetDyna(k)
		require.NoError(t, err)
		require.Equal(t, byte(45), v[0])
	}
}
