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

func syncHeap(t *testing.T, h *HeapStore) {
	t.Helper()
	p, err := h.beginBlockSync()
	require.NoError(t, err)
	require.NoError(t, p.finish())
}

// Small files, so that rolling and deletion happen within a test.
func smallFiles(t *testing.T) {
	t.Helper()
	was := HeapFileBytes
	HeapFileBytes = 1024
	t.Cleanup(func() { HeapFileBytes = was })
}

// A key rewritten within the block reuses its slot; rewritten in a
// later block it is appended, the old slot dead where it lies until
// the mover copies the file's live entries out and the next sync
// deletes the file.
func TestHeapRewriteReusesWithinTheBlockAndMovesOneSyncLate(t *testing.T) {
	smallFiles(t)
	h, err := NewHeapStore(heapDir(t))
	require.NoError(t, err)
	defer h.Close()
	h.AdvanceBlock(1)
	require.NoError(t, h.Put(key(1), []byte("one")))
	first := h.index[key(1)]
	require.NoError(t, h.Put(key(1), []byte("uno")))
	require.Equal(t, first, h.index[key(1)], "same block, fits: in place")
	require.EqualValues(t, 1, h.putInPlace.Load())
	v, err := h.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, "uno", string(v))

	syncHeap(t, h)
	h.AdvanceBlock(2)
	require.NoError(t, h.Put(key(1), []byte("two")))
	require.NotEqual(t, first, h.index[key(1)], "a durable slot is never rewritten")
	dead, live := h.HoleRatio()
	require.EqualValues(t, entrySize(3), dead, "the old slot is dead where it lies")
	require.EqualValues(t, entrySize(3), live-h.deltaBytes(), "one live entry besides the deltas")
	// Fill the block's file past its size so it rolls, then kill most
	// of what the first file holds
	for i := byte(10); i < 40; i++ {
		require.NoError(t, h.Put(key(i), make([]byte, 64)))
	}
	syncHeap(t, h)
	require.Greater(t, len(h.files), 1, "the block's file rolled")
	h.AdvanceBlock(3)
	for i := byte(10); i < 39; i++ {
		require.NoError(t, h.Put(key(i), make([]byte, 64)))
	}
	syncHeap(t, h)
	require.NoError(t, h.Snapshot()) // The deltas so far are superseded: dead where they lie

	h.AdvanceBlock(4)
	moved, err := h.clean(1 << 20)
	require.NoError(t, err)
	require.True(t, moved, "a mostly dead file is taken")
	require.NotEmpty(t, h.release, "a file emptied by the pass waits for the sync")
	for _, id := range h.release {
		_, err := os.Stat(filepath.Join(h.Directory, dataName(id)))
		require.NoError(t, err, "not deleted yet: the copies are not durable")
	}
	released := append([]uint32(nil), h.release...)
	syncHeap(t, h)
	for _, id := range released {
		require.Nil(t, h.files[id], "out of the map after the sync")
		_, err := os.Stat(filepath.Join(h.Directory, dataName(id)))
		require.NoError(t, err, "the unlink is the mover's, off the block's path")
	}
	require.NoError(t, h.unlinkReleased())
	for _, id := range released {
		_, err := os.Stat(filepath.Join(h.Directory, dataName(id)))
		require.ErrorIs(t, err, os.ErrNotExist, "deleted by the mover")
	}
	_, copied := h.Cleaned()
	require.Greater(t, copied, uint64(0), "the live entries left in it were copied out")
	v, err = h.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, "two", string(v))
	for i := byte(10); i < 40; i++ {
		_, err := h.Get(key(i))
		require.NoError(t, err)
	}
}

// Reopening replays the generation: every synced value is back, the
// files' accounting is derived, and a block that was never synced is
// gone -- its slots unnamed and its bytes cut from the file.
func TestHeapReopenKeepsTheDurableAndDropsTheRest(t *testing.T) {
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	h.AdvanceBlock(1)
	for i := byte(1); i <= 50; i++ {
		require.NoError(t, h.Put(key(i), []byte{i}))
	}
	syncHeap(t, h)
	h.AdvanceBlock(2)
	require.NoError(t, h.Put(key(1), []byte("rewritten in block 2")))
	syncHeap(t, h)
	size := h.deltaAt.off // Where block 2's delta ended: the durable append point
	// Block 3: written, never synced -- the crash
	h.AdvanceBlock(3)
	require.NoError(t, h.Put(key(2), []byte("lost")))
	require.NoError(t, h.Put(key(99), []byte("lost too")))
	for _, hf := range h.files {
		hf.f.Close()
	}
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
	require.Len(t, r.files, 1)
	for _, hf := range r.files {
		require.Equal(t, size, hf.size, "the file is cut back to the durable append point")
	}
	dead, _ := r.HoleRatio()
	require.EqualValues(t, entrySize(1), dead, "key 1's block-1 slot is dead where it lies")
	require.EqualValues(t, 2, r.height, "the durable height: block 3 never synced")
}

// A torn entry at the end of the data file -- a crash mid-write --
// is cut, and the deltas before it stand.
func TestHeapTornTailIsCut(t *testing.T) {
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	h.AdvanceBlock(1)
	require.NoError(t, h.Put(key(1), []byte("one")))
	syncHeap(t, h)
	end := h.deltaAt.off
	require.NoError(t, h.Close())
	f, err := os.OpenFile(filepath.Join(dir, dataName(0)), os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{9, 0, 0, 0, 1, 2, 3, 4, 5, 6}) // A length and a few bytes of nothing
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, err := OpenHeapStore(dir)
	require.NoError(t, err)
	defer r.Close()
	v, err := r.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, "one", string(v))
	st, err := os.Stat(filepath.Join(dir, dataName(0)))
	require.NoError(t, err)
	require.Equal(t, end, st.Size(), "cut back to the last delta")
}

// A snapshot starts a new generation in its own file and retires the
// old one; the replay lands on the new one; and a generation whose
// write was interrupted is ignored.
func TestHeapSnapshotStartsAGenerationSafely(t *testing.T) {
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	every := HeapSnapshotBlocks
	HeapSnapshotBlocks = 1
	defer func() { HeapSnapshotBlocks = every }()
	for b := uint64(1); b <= 30; b++ {
		h.AdvanceBlock(b)
		for i := byte(1); i <= 20; i++ {
			require.NoError(t, h.Put(key(i), []byte{byte(b), i}))
		}
		syncHeap(t, h)
		if b == 20 {
			_, err := h.compact(HeapCleanBytes)
			require.NoError(t, err)
			_, err = os.Stat(filepath.Join(dir, indexName(1)))
			require.ErrorIs(t, err, os.ErrNotExist, "generation 1 retired")
			_, err = os.Stat(filepath.Join(dir, indexName(2)))
			require.NoError(t, err)
		}
	}
	require.NoError(t, h.Close())
	// A crash mid-snapshot leaves a .tmp; it is not a generation
	require.NoError(t, os.WriteFile(filepath.Join(dir, indexName(3)+".tmp"), []byte("half"), 0o644))
	r, err := OpenHeapStore(dir)
	require.NoError(t, err)
	defer r.Close()
	require.EqualValues(t, 2, r.gen)
	_, err = os.Stat(filepath.Join(dir, indexName(3)+".tmp"))
	require.ErrorIs(t, err, os.ErrNotExist)
	for i := byte(1); i <= 20; i++ {
		v, err := r.Get(key(i))
		require.NoError(t, err)
		require.Equal(t, []byte{30, i}, v)
	}
}

// A slot whose bytes were damaged is an error, never a value.
func TestHeapChecksumCatchesADamagedSlot(t *testing.T) {
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	h.AdvanceBlock(1)
	require.NoError(t, h.Put(key(1), []byte("intact")))
	s := h.index[key(1)]
	syncHeap(t, h)
	h.AdvanceBlock(2)
	require.NoError(t, h.Put(key(2), []byte("later"))) // A later block's sync covers block 1
	require.NoError(t, h.Close())
	f, err := os.OpenFile(filepath.Join(dir, dataName(s.file)), os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("damaged"), int64(s.off)+heapHeader)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	r, err := OpenHeapStore(dir)
	require.NoError(t, err)
	defer r.Close()
	_, err = r.Get(key(1))
	require.ErrorContains(t, err, "checksum")
}

// With the index gone, the map is rebuilt from the data: the highest
// committed copy of each key wins, an entry from the block that never
// synced is dropped.
func TestHeapRepairReadsTheKeysFromTheData(t *testing.T) {
	smallFiles(t)
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	h.AdvanceBlock(1)
	for i := byte(1); i <= 10; i++ {
		require.NoError(t, h.Put(key(i), make([]byte, 100)))
	}
	syncHeap(t, h)
	h.AdvanceBlock(2)
	require.NoError(t, h.Put(key(1), []byte{2, 1})) // Newer copy, in a later file
	syncHeap(t, h)
	h.AdvanceBlock(3)
	require.NoError(t, h.Put(key(2), []byte{3, 2})) // Never synced: not committed
	for _, hf := range h.files {
		hf.f.Close()
	}
	h.log.Close()
	require.NoError(t, os.Remove(filepath.Join(dir, indexName(1))))

	_, err = OpenHeapStore(dir)
	require.ErrorIs(t, err, ErrHeapNeedsRepair)
	r, err := RepairHeapStore(dir, 2)
	require.NoError(t, err)
	defer r.Close()
	v, err := r.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, []byte{2, 1}, v, "the block-2 copy wins")
	v, err = r.Get(key(2))
	require.NoError(t, err)
	require.Len(t, v, 100, "block 3 never committed: its copy is dropped")
	require.EqualValues(t, 10, r.LiveRecords())
	require.NoError(t, r.Close())
	re, err := OpenHeapStore(dir)
	require.NoError(t, err, "the repair left a generation: an ordinary open")
	defer re.Close()
	v, err = re.Get(key(1))
	require.NoError(t, err)
	require.Equal(t, []byte{2, 1}, v)
}

// A shard built with the heap seals, compacts, closes and reopens as
// a heap, through the same KV2 and KVShard surface, with files rolled
// and deleted along the way.
func TestHeapShardRoundTrip(t *testing.T) {
	was := HeapFileBytes
	HeapFileBytes = 256 << 10
	defer func() { HeapFileBytes = was }()
	dir := filepath.Join(t.TempDir(), "shards")
	kvs, err := NewKVShardHeapN(dir, 2, 1000)
	require.NoError(t, err)
	require.NoError(t, kvs.SetFilterBlocks(MinFilterBlocks))
	fr := NewFastRandom([]byte{7})
	hot := make([][32]byte, 200)
	for i := range hot {
		hot[i] = fr.NextHash()
	}
	for b := uint64(1); b <= 60; b++ {
		for _, k := range hot {
			require.NoError(t, kvs.PutDyna(k, append([]byte{byte(b)}, fr.RandBuff(100, 300)...)))
		}
		require.NoError(t, kvs.PutPerm(fr.NextHash(), []byte("perm")))
		require.NoError(t, kvs.SealBlock(b))
		if b%10 == 0 {
			require.NoError(t, kvs.Compress())
			if b > MinFilterBlocks {
				_, err := kvs.MergeFinalized(b - MinFilterBlocks)
				require.NoError(t, err)
			}
		}
	}
	for _, k := range hot {
		v, err := kvs.GetDyna(k)
		require.NoError(t, err)
		require.Equal(t, byte(60), v[0])
	}
	_, dyna := kvs.Stats()
	require.EqualValues(t, 60*200, dyna.PutTotal)
	// Maintenance is a slice per call, sized by the blocks since the
	// last: a call a block over a few blocks visits every shard with
	// budget to spare for what the last ten blocks left dead, and each
	// seal releases what the pass before it emptied
	for b := uint64(61); b <= 64; b++ {
		require.NoError(t, kvs.SealBlock(b))
		require.NoError(t, kvs.Compress())
	}
	require.NoError(t, kvs.SealBlock(65))
	dead, live := kvs.Shards[0].Heap.HoleRatio()
	require.Less(t, dead, 2*live+HeapFileBytes, "dead bytes are bounded: at most the current file, which the mover never takes, beyond the live set")
	require.NoError(t, kvs.Close())

	re, err := OpenKVShard(dir)
	require.NoError(t, err)
	defer re.Close()
	require.NotNil(t, re.Shards[0].Heap, "reopened as a heap")
	require.Nil(t, re.Shards[0].DynaKV)
	for _, k := range hot {
		v, err := re.GetDyna(k)
		require.NoError(t, err)
		require.Equal(t, byte(60), v[0])
	}
}

// A key rewritten between the mover's copy and its naming leaves the
// copy dead on arrival: the put's value stands, and the copy's bytes
// are accounted dead in the mover's file.
func TestHeapMoveIsDeadOnArrivalIfTheKeyWasRewritten(t *testing.T) {
	smallFiles(t) // Nine 112-byte entries to a file
	h, err := NewHeapStore(heapDir(t))
	require.NoError(t, err)
	defer h.Close()
	h.AdvanceBlock(1)
	for i := byte(1); i <= 20; i++ {
		require.NoError(t, h.Put(key(i), make([]byte, 64)))
	}
	syncHeap(t, h)
	// Two more blocks rewriting every key but 20 leave key 20 the one
	// live entry in a file otherwise dead: the mover's next pick
	for b := uint64(2); b <= 3; b++ {
		h.AdvanceBlock(b)
		for i := byte(1); i <= 19; i++ {
			require.NoError(t, h.Put(key(i), make([]byte, 64)))
		}
		syncHeap(t, h)
	}
	require.NoError(t, h.Snapshot()) // The deltas so far are superseded: the file is mostly dead
	files := HeapCleanFiles
	HeapCleanFiles = 64 // One pass takes every eligible file, key 20's half-dead one included
	defer func() { HeapCleanFiles = files }()
	h.AdvanceBlock(4)
	moverHook = func() {
		require.NoError(t, h.Put(key(20), []byte("rewritten while moving")))
	}
	defer func() { moverHook = nil }()
	deadBefore, _ := h.HoleRatio()
	moved, err := h.clean(1 << 20)
	require.NoError(t, err)
	require.True(t, moved)
	_, copied := h.Cleaned()
	require.GreaterOrEqual(t, copied, uint64(entrySize(64)), "key 20 was among the entries copied")
	v, err := h.Get(key(20))
	require.NoError(t, err)
	require.Equal(t, "rewritten while moving", string(v), "the put wins")
	// Every copied entry left its old slot dead; key 20's copy is dead
	// as well, since the put took the key elsewhere before it was named
	dead, _ := h.HoleRatio()
	require.EqualValues(t, deadBefore+int64(copied)+entrySize(64), dead, "the old slots and the unwanted copy are dead")
	syncHeap(t, h)
	v, err = h.Get(key(20))
	require.NoError(t, err)
	require.Equal(t, "rewritten while moving", string(v))
}

// A file the mover copied into is named by the deltas of the current
// generation, as the destination of their ranges.  When every copy in
// it dies it must not be unlinked until a snapshot supersedes those
// deltas: never unlink what a durable index names (spec 1.7).  Found
// by the platform's reopen check: a store closed cleanly would not
// open, "the index names heap-000006.dat: missing".
func TestHeapMoverFileNamedByDeltasOutlivesItsEntries(t *testing.T) {
	was, blocks, pinned := HeapFileBytes, HeapSnapshotBlocks, HeapSnapshotPinnedFiles
	HeapFileBytes, HeapSnapshotBlocks, HeapSnapshotPinnedFiles = 64<<10, 1<<20, 1<<30 // No snapshot at all
	defer func() { HeapFileBytes, HeapSnapshotBlocks, HeapSnapshotPinnedFiles = was, blocks, pinned }()
	dir := heapDir(t)
	h, err := NewHeapStore(dir)
	require.NoError(t, err)
	value := make([]byte, 400)
	last := map[byte]byte{}
	// Half the keys are rewritten every block, half every seventh: a
	// file is soon half dead with live entries the mover copies out,
	// and the copies die within seven blocks, emptying the mover's file
	put := func(b uint64) {
		h.AdvanceBlock(b)
		for i := byte(1); i <= 100; i++ {
			if i > 50 && b%7 != 0 {
				continue
			}
			value[0] = byte(b)
			require.NoError(t, h.Put(key(i), value))
			last[i] = byte(b)
		}
		syncHeap(t, h)
	}
	for b := uint64(1); b <= 70; b++ {
		put(b)
		if b%3 == 0 {
			_, err := h.compact(HeapCleanBytes) // Copies live entries into mover files, releases what emptied
			require.NoError(t, err)
		}
	}
	_, moved := h.Cleaned()
	require.Greater(t, moved, uint64(0), "the mover must have copied")
	releases, _, snapshots, _ := h.MoverCost()
	t.Logf("moved %d bytes, %d files unlinked, %d snapshots, %d files open, gen %d", moved, releases, snapshots, len(h.files), h.gen)
	require.NoError(t, h.Close())
	h, err = OpenHeapStore(dir)
	require.NoError(t, err, "a store closed cleanly reopens")
	t.Logf("after open: %d files, gen %d, replay point %v", len(h.files), h.gen, h.deltaAt)
	for i := byte(1); i <= 100; i++ {
		v, err := h.Get(key(i))
		require.NoError(t, err, "key %d", i)
		require.Equal(t, last[i], v[0], "key %d", i)
	}
	require.NoError(t, h.Close())
}
