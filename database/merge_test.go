package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMergeBelowKeepsEveryKeyAndCutsFiles
// Tier one of issue #47: a block boundary seals a segment per shard
// that took writes, so a store accumulates one file pair per block and
// the count is bounded by nothing.  Merging a finished block set down
// to one segment must cut the files without losing a key.
func TestMergeBelowKeepsEveryKeyAndCutsFiles(t *testing.T) {
	dir := storeDir(t, "merge")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{171})
	const blocks = 20
	const perBlock = 25
	keys := make([][32]byte, 0, blocks*perBlock)
	values := make(map[[32]byte]string)
	for h := uint64(1); h <= blocks; h++ {
		for i := 0; i < perBlock; i++ {
			key := kr.NextHash()
			val := fmt.Sprintf("b%d-k%d", h, i)
			require.NoError(t, store.Put(key, []byte(val)))
			keys = append(keys, key)
			values[key] = val
		}
		_, err = store.Seal(h)
		require.NoError(t, err)
	}
	require.Len(t, store.sealedSegments(), blocks, "one segment per block before merging")

	// A merge works on history, so roll the window past every block
	// (a merge below a watermark still inside the window waits until
	// the window has passed it)
	ageOut(t, store)

	// Finalise everything below block 20, leaving the newest alone
	meta, merged, err := store.MergeBelow(20)
	require.NoError(t, err)
	require.True(t, merged, "19 segments below the watermark must merge")
	assert.Len(t, store.sealedSegments(), 2, "19 merged into 1, plus the segment at block 20")
	assert.Equal(t, uint64(19*perBlock), meta.Count, "the merged segment holds every key below the watermark")

	// The merged segment must order before what was left standing
	assert.True(t, store.sealedSegments()[1].meta.after(store.sealedSegments()[0].meta),
		"the merged segment must still order before the segments it did not replace")

	// Every key still reads back, from a segment that no longer exists
	for i, key := range keys {
		v, err := store.GetDeep(key)
		require.NoErrorf(t, err, "key %d lost by the merge", i)
		assert.Equal(t, values[key], string(v))
	}

	// And it survives a reopen: the manifest names the merged segment.
	// The files the merge replaced are gone by then: the active
	// manifest still named them as handoffs in flight, and the close
	// is the active commit that stops naming them and deletes them.
	require.NoError(t, store.Close())
	segFiles := 0
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if e.Name() == segLiveName || (strings.HasPrefix(e.Name(), segFilePrefix) && filepath.Ext(e.Name()) == segDataSuffix) {
			segFiles++
		}
	}
	assert.Equal(t, 3, segFiles, "2 sealed segments plus live.dat: %v", entries)
	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	require.Len(t, reopened.sealedSegments(), 2)
	for i, key := range keys {
		v, err := reopened.GetDeep(key)
		require.NoErrorf(t, err, "key %d lost across the reopen", i)
		assert.Equal(t, values[key], string(v))
	}
	require.NoError(t, reopened.Close())
}

// TestMergeBelowLeavesTheWatermarkAlone
// The watermark is what keeps block export working: a block a peer
// might still ask for by number must not be merged away.
func TestMergeBelowLeavesTheWatermarkAlone(t *testing.T) {
	dir := storeDir(t, "watermark")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{172})
	for h := uint64(1); h <= 10; h++ {
		require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
		_, err = store.Seal(h)
		require.NoError(t, err)
	}
	ageOut(t, store)

	_, merged, err := store.MergeBelow(6)
	require.NoError(t, err)
	require.True(t, merged)

	// Blocks 6..10 must still be individually present
	heights := map[uint64]bool{}
	for _, seg := range store.sealedSegments() {
		heights[seg.meta.Height] = true
	}
	for h := uint64(6); h <= 10; h++ {
		assert.Truef(t, heights[h], "block %d is at or above the watermark and must not be merged", h)
	}
	assert.Len(t, store.sealedSegments(), 6, "blocks 1-5 merged into one, 6..10 left standing")

	// Merging again changes nothing: only one segment is below 6 now
	_, merged, err = store.MergeBelow(6)
	require.NoError(t, err)
	assert.False(t, merged, "a single segment below the watermark is not worth merging")
}

// ageOutShards
// Close a run of empty blocks so that every block up to `last` falls
// below every shard's window and is handed to history -- what
// finalisation works on.  Returns the next block the set will seal.
func ageOutShards(t *testing.T, kvs *KVShard, last uint64) (next uint64) {
	t.Helper()
	next = last + 3*MinFilterBlocks
	require.NoError(t, kvs.SealBlock(next))
	return next + 1
}

// TestMergeFinalizedAcrossShards
// The sharded wrapper merges each shard independently.
func TestMergeFinalizedAcrossShards(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kvs, err := NewKVShard(dir, 100_000) // High, so only block boundaries seal
	require.NoError(t, err)
	require.NoError(t, kvs.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{173})
	vr := NewFastRandom([]byte{173, 173})
	keys := make([][32]byte, 0, 400)
	values := make(map[[32]byte][]byte)
	for h := uint64(1); h <= 8; h++ {
		for i := 0; i < 50; i++ {
			key := kr.NextHash()
			val := vr.RandBuff(20, 60)
			require.NoError(t, kvs.PutPerm(key, val))
			keys = append(keys, key)
			values[key] = val
		}
		require.NoError(t, kvs.SealBlock(h))
	}
	ageOutShards(t, kvs, 8) // A merge works on history: roll the window past the blocks

	before := 0
	for _, sh := range kvs.Shards {
		before += len(sh.PermKV.sealedSegments())
	}

	mergedShards, err := kvs.MergeFinalized(7)
	require.NoError(t, err)
	require.Greater(t, mergedShards, 0, "some shard must have had two segments below the watermark")

	after := 0
	for _, sh := range kvs.Shards {
		after += len(sh.PermKV.sealedSegments())
	}
	assert.Less(t, after, before, "merging must reduce the segment count (%d -> %d)", before, after)

	for i, key := range keys {
		v, err := kvs.GetDeep(key)
		require.NoErrorf(t, err, "key %d lost by the merge", i)
		assert.Equal(t, values[key], v)
	}
	require.NoError(t, kvs.Close())
}

// TestMergeDoesNotDisturbBlockExport
// The reason merging was blocked (issue #37): merging permanent
// segments destroys the block->segment mapping ExportBlock depends on.
// The watermark is what makes it safe -- nothing at or above it is
// merged, so a block a peer is still fetching is never merged away.
// This checks the whole round trip across a merge, which is the claim
// that actually matters.
func TestMergeDoesNotDisturbBlockExport(t *testing.T) {
	dirA := filepath.Join(os.TempDir(), t.Name()+"_A")
	dirB := filepath.Join(os.TempDir(), t.Name()+"_B")
	exportDir := filepath.Join(os.TempDir(), t.Name()+"_export")
	for _, d := range []string{dirA, dirB, exportDir} {
		os.RemoveAll(d)
		defer os.RemoveAll(d)
	}

	nodeA, err := NewKVShard(dirA, 100_000)
	require.NoError(t, err)
	require.NoError(t, nodeA.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{174})
	vr := NewFastRandom([]byte{174, 174})
	keys := make([][32]byte, 0, 300)
	values := make(map[[32]byte][]byte)

	// Export blocks 1..6 as a peer would fetch them
	var prev *Manifest
	for h := uint64(1); h <= 6; h++ {
		for i := 0; i < 50; i++ {
			key := kr.NextHash()
			val := vr.RandBuff(20, 60)
			require.NoError(t, nodeA.PutPerm(key, val))
			keys = append(keys, key)
			values[key] = val
		}
		prev, err = nodeA.ExportBlock(exportDir, h, prev)
		require.NoError(t, err)
	}

	// Now finalise the blocks a peer has already taken: both stages, so
	// blocks 1-4 leave the shards altogether for a block-set file.
	// Finalisation works on history, so first the window rolls past
	// them, which closes a run of empty blocks.
	next := ageOutShards(t, nodeA, 6)
	mergedShards, err := nodeA.MergeFinalized(5)
	require.NoError(t, err)
	require.Greater(t, mergedShards, 0, "the merge must have done something for this to test anything")
	_, packed, err := nodeA.PackFinalized(5)
	require.NoError(t, err)
	require.True(t, packed, "the pack must have done something for this to test anything")

	// A later block still exports correctly after the merge
	for i := 0; i < 50; i++ {
		key := kr.NextHash()
		val := vr.RandBuff(20, 60)
		require.NoError(t, nodeA.PutPerm(key, val))
		keys = append(keys, key)
		values[key] = val
	}
	_, err = nodeA.ExportBlock(exportDir, next, prev)
	require.NoError(t, err)

	// A fresh node syncs from the exported blocks and gets everything
	nodeB, err := NewKVShard(dirB, 100_000)
	require.NoError(t, err)
	var total uint64
	for _, h := range []uint64{1, 2, 3, 4, 5, 6, next} {
		n, err := nodeB.ImportBlock(filepath.Join(exportDir, fmt.Sprintf("block-%08d", h)))
		require.NoErrorf(t, err, "import block %d after a merge on the exporting node", h)
		total += n
	}
	assert.Equal(t, uint64(len(keys)), total, "every record must reach the peer")
	for i, key := range keys {
		v, err := nodeB.GetDeep(key)
		require.NoErrorf(t, err, "key %d never reached the peer", i)
		assert.Equal(t, values[key], v)
	}
	require.NoError(t, nodeA.Close())
	require.NoError(t, nodeB.Close())
}

// TestRepeatedMergesKeepDeepEntriesReachable
// The steady state, which one merge does not exercise: a node merges
// every completed set, forever, so a later merge's input run includes
// segments a previous merge produced.  What has to keep working is a
// lookup of an entry written long ago and merged repeatedly since --
// the "reach back 20N blocks" case.
func TestRepeatedMergesKeepDeepEntriesReachable(t *testing.T) {
	dir := storeDir(t, "deep")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	const sets = 8                  // Eight completed block sets
	const setSize = MinFilterBlocks // Blocks per set: N, so each set rolls the window once
	const perBlock = 10

	kr := NewFastRandom([]byte{175})
	type entry struct {
		key   [32]byte
		value string
		block uint64
	}
	var all []entry

	block := uint64(0)
	for set := 0; set < sets; set++ {
		for b := 0; b < setSize; b++ {
			block++
			for i := 0; i < perBlock; i++ {
				key := kr.NextHash()
				val := fmt.Sprintf("blk%d-%d", block, i)
				require.NoError(t, store.Put(key, []byte(val)))
				all = append(all, entry{key, val, block})
			}
			_, err = store.Seal(block)
			require.NoError(t, err)
		}
		// Finalise every set but the one just written.  A merge works on
		// history -- what the window, N to 2N blocks, has rolled past --
		// so the first set has nothing to merge and every later one
		// merges the set before the one the window still holds.  Each
		// merge leaves a merged block behind and never touches it again
		// (issue #63); TestMergeFoldsEachWindowOnce pins that.
		watermark := block - uint64(setSize) + 1
		if watermark > 1 {
			_, merged, err := store.MergeBelow(watermark)
			require.NoErrorf(t, err, "merge after set %d", set)
			require.Equalf(t, set >= 1, merged, "set %d: merged=%v", set, merged)
		}
	}

	// The oldest entries have now been through several merges.  Every
	// entry must still be reachable, and with the right value.
	for i, e := range all {
		v, err := store.GetDeep(e.key)
		require.NoErrorf(t, err, "entry %d from block %d unreachable after repeated merges", i, e.block)
		require.Equalf(t, e.value, string(v), "entry %d from block %d has the wrong value", i, e.block)
	}

	// A key that was never written must still be absent -- the filter
	// and the segment walk have to agree after all that rewriting
	for i := 0; i < 50; i++ {
		_, err := store.Get(kr.NextHash())
		require.ErrorIsf(t, err, errNotFound, "an unwritten key %d must be reported absent", i)
	}

	// And it all survives a reopen, which rebuilds from the manifest
	require.NoError(t, store.Close())
	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	for i, e := range all {
		v, err := reopened.GetDeep(e.key)
		require.NoErrorf(t, err, "entry %d from block %d lost across the reopen", i, e.block)
		require.Equal(t, e.value, string(v))
	}
	t.Logf("%d entries over %d blocks, %d merges, %d segments standing",
		len(all), block, sets-1, len(reopened.sealedSegments()))
	require.NoError(t, reopened.Close())
}

// TestMergeFoldsEachWindowOnce
// A merged block is finished: merged once, then permanent (spec 1.4).
// MergeBelow folded the whole prefix below the watermark, and the
// previous merge's output is in that prefix -- so every pass re-copied
// the entire permanent layer accumulated so far.  Lifetime IO was
// O(chain^2) and one pass grew without limit: ~10 GB per pass after
// four hours at 500 tx/s (issue #63).
//
// The proof is in what each pass writes.  With one window's worth of
// blocks arriving between merges, every merge must fold ONE window --
// its output holding one window's keys, the merged blocks before it
// untouched -- rather than one output holding everything ever written.
func TestMergeFoldsEachWindowOnce(t *testing.T) {
	dir := storeDir(t, "mergeonce")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	const windows = 5
	const perBlock = 4
	kr := NewFastRandom([]byte{177})

	block := uint64(0)
	var outputs []SegmentMeta
	for w := 0; w < windows; w++ {
		for b := 0; b < MinFilterBlocks; b++ {
			block++
			for i := 0; i < perBlock; i++ {
				require.NoError(t, store.Put(kr.NextHash(), []byte{byte(block), byte(i)}))
			}
			_, err = store.Seal(block)
			require.NoError(t, err)
		}
		watermark := block - MinFilterBlocks + 1
		if watermark <= 1 {
			continue
		}
		meta, merged, err := store.MergeBelow(watermark)
		require.NoErrorf(t, err, "merge %d", w)
		if !merged {
			continue
		}
		outputs = append(outputs, meta)
	}
	require.GreaterOrEqual(t, len(outputs), 3, "several merges must have run")

	// Each pass folds one window, so its output holds about one
	// window's keys -- never the whole layer.  Before the fix the
	// counts grew with every pass: 80, 160, 240...
	windowKeys := uint64(MinFilterBlocks * perBlock)
	for i, m := range outputs {
		require.LessOrEqualf(t, m.Count, 2*windowKeys,
			"merge %d wrote %d keys; one window is ~%d -- it folded the previous output back in",
			i, m.Count, windowKeys)
	}

	// And every merged block from an earlier pass is still on disk under
	// its own name: merged once, then permanent
	for i, m := range outputs[:len(outputs)-1] {
		_, err := os.Stat(filepath.Join(dir, m.File))
		require.NoErrorf(t, err, "merged block %d (%s) was rewritten by a later pass", i, m.File)
	}

	// History is the merged blocks, oldest first, plus whatever has not
	// been merged yet -- not one segment holding everything
	store.History.RLock()
	n := len(store.history)
	store.History.RUnlock()
	require.GreaterOrEqualf(t, n, len(outputs),
		"each merged window must stand as its own block; history has %d", n)
}

// TestRejectedImportIsNotAdoptedAfterACrash
// Rejecting a peer's conflicting segment used to be undone by two
// os.Remove calls with no barrier behind them.  A crash in that window
// left the file on disk, and recoverOrphans could not tell it apart
// from an interrupted seal -- it IS a complete, correctly hashed
// segment above the newest height; it was refused for conflicting with
// local data, not for being malformed.  So the next open adopted the
// segment whose import had been refused, and the conflict check never
// ran again (issue #45).
//
// The crash is simulated the only way it can be without killing a
// process: the import is rejected, and then the store is reopened
// WITHOUT closing, so recovery runs against whatever the rejection
// left on disk.
func TestRejectedImportIsNotAdoptedAfterACrash(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))

	// A peer's segment holding one key
	srcDir := filepath.Join(dir, "peer")
	src, err := NewSegmentStore(srcDir, false)
	require.NoError(t, err)
	key := NewFastRandom([]byte{64}).NextHash()
	require.NoError(t, src.Put(key, []byte("the peer's value")))
	meta, err := src.Seal(1)
	require.NoError(t, err)
	metas, paths := src.SegmentPaths()
	require.Len(t, metas, 1)
	segPath := paths[0]

	// A local store holding the SAME key with a DIFFERENT value: the
	// divergence the check exists to catch
	dstDir := filepath.Join(dir, "local")
	dst, err := NewSegmentStore(dstDir, false)
	require.NoError(t, err)
	require.NoError(t, dst.Put(key, []byte("the local value")))
	// Durable, but not sealed: the local key stays in the live tail, so
	// the peer's segment is still above the newest and the import is
	// refused for CONFLICTING rather than for being out of order
	require.NoError(t, dst.Sync())

	err = dst.ImportSegmentFile(segPath, metas[0])
	require.Error(t, err, "a conflicting segment must be refused")
	require.Empty(t, dst.sealedSegments(), "nothing may be adopted by a refused import")

	// Reopen without closing: recovery sees whatever the refusal left
	reopened, err := OpenSegmentStore(dstDir)
	require.NoError(t, err)
	assert.Empty(t, reopened.sealedSegments(),
		"recovery adopted the segment whose import was refused")

	v, err := reopened.GetDeep(key)
	require.NoError(t, err)
	assert.Equal(t, "the local value", string(v),
		"the local value must stand; the peer's was rejected")

	// Nothing of the refused import may be left on disk
	entries, err := os.ReadDir(dstDir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotEqualf(t, meta.File, e.Name(),
			"the refused segment is still on disk and will be adopted on a later open")
		assert.NotContainsf(t, e.Name(), segTmpSuffix,
			"a temporary file from the refused import was left behind: %s", e.Name())
	}
	require.NoError(t, reopened.Close())
	require.NoError(t, src.Close())
}

// TestImportRefusesToReplaceAFileAtItsIdentity
// No publish may replace an existing segment file (spec 1.7).  Seals
// and merge outputs claim their names exclusively; the import path was
// still renaming over whatever stood there.  Its identity checks
// consult the segments the MANIFESTS name, and a complete file can sit
// at an identity without being named -- an earlier seal or import that
// reached disk and not its commit -- so a rename could destroy a
// committed-by-construction file quietly (issue #67).
func TestImportRefusesToReplaceAFileAtItsIdentity(t *testing.T) {
	dir := storeDir(t, "importclaim")
	src, err := NewSegmentStore(dir+"-src", false)
	require.NoError(t, err)
	defer src.Close()

	kr := NewFastRandom([]byte{181})
	for i := 0; i < 20; i++ {
		require.NoError(t, src.Put(kr.NextHash(), []byte{byte(i)}))
	}
	meta, err := src.Seal(7)
	require.NoError(t, err)
	exported := filepath.Join(dir+"-src", meta.File)

	dst, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer dst.Close()

	// An orphan at the identity the import would take: complete, and
	// named by no manifest, exactly as an interrupted seal leaves one
	squatter := filepath.Join(dir, meta.File)
	require.NoError(t, os.WriteFile(squatter, []byte("an earlier segment, uncommitted"), 0o644))

	err = dst.ImportSegmentFile(exported, meta)
	require.Error(t, err, "the import must refuse an identity that is taken")
	require.Contains(t, err.Error(), "already exists")

	// And it left the file alone
	got, err := os.ReadFile(squatter)
	require.NoError(t, err)
	require.Equal(t, "an earlier segment, uncommitted", string(got),
		"the file standing at that identity must be untouched")

	// With the identity free, the same import succeeds
	require.NoError(t, os.Remove(squatter))
	require.NoError(t, dst.ImportSegmentFile(exported, meta))
}

// TestRecoveryNeverAdoptsAMergeOutput
// recoverOrphans classifies an unnamed data file by height: above the
// manifest's newest is an interrupted seal, complete by construction,
// and is adopted.  That is right for a seal, whose records exist
// nowhere else -- and wrong for a merge output, whose every key is
// still in the inputs the manifest names.  A shard whose segments are
// ALL below the watermark (a shard that took no writes for a whole
// set, ordinary at low entry rates) merges its entire list, so the
// output takes an identity above everything and the height rule adopts
// it: every key of that shard, stored twice, until a later merge folds
// the duplicate away (issue #52).
//
// The output says what it is in its header now, and recovery believes
// the file over the arithmetic.
func TestRecoveryNeverAdoptsAMergeOutput(t *testing.T) {
	dir := storeDir(t, "orphanmerge")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{183})
	var keys [][32]byte
	for b := uint64(1); b <= 3; b++ {
		for i := 0; i < 10; i++ {
			k := kr.NextHash()
			keys = append(keys, k)
			require.NoError(t, store.Put(k, []byte{byte(b), byte(i)}))
		}
		_, err = store.Seal(b)
		require.NoError(t, err)
	}
	// Roll the window so all three are history, and merge them the way
	// MergeBelow would -- but stop before the commit, which is the crash
	for b := uint64(4); b <= 3*MinFilterBlocks; b++ {
		_, err = store.Seal(b)
		require.NoError(t, err)
	}
	store.History.RLock()
	run := append([]*segment(nil), store.history...)
	store.History.RUnlock()
	require.GreaterOrEqual(t, len(run), 3, "the run must be history for this to test anything")

	last := run[len(run)-1].meta
	meta, seg, err := store.concatSegments(run, last.Height, last.Seq+1)
	require.NoError(t, err)
	seg.close()
	orphan := filepath.Join(dir, meta.File)
	_, err = os.Stat(orphan)
	require.NoError(t, err, "the merge output must be on disk for the crash to matter")

	// The crash: reopen without closing, so recovery runs against a
	// store whose manifest never learned about that file
	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer reopened.Close()

	_, err = os.Stat(orphan)
	require.Truef(t, os.IsNotExist(err),
		"recovery adopted the merge output %s; its keys are already in the segments the manifest names", meta.File)

	// Every key is still there, exactly once
	for i, k := range keys {
		v, err := reopened.GetDeep(k)
		require.NoErrorf(t, err, "key %d lost", i)
		require.Equal(t, []byte{byte(i/10 + 1), byte(i % 10)}, v)
	}
	seen := map[string]bool{}
	for _, s := range reopened.sealedSegments() {
		require.Falsef(t, seen[s.meta.File], "%s counted twice", s.meta.File)
		seen[s.meta.File] = true
	}
}

// TestRecoveryStillAdoptsAnInterruptedSeal
// The other half of the rule: a SEAL that reached disk before its
// commit is the only copy of its records, and must still be adopted
// (issue #45).  Marking maintenance output must not cost that.
func TestRecoveryStillAdoptsAnInterruptedSeal(t *testing.T) {
	dir := storeDir(t, "orphanseal")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{184})
	first := kr.NextHash()
	require.NoError(t, store.Put(first, []byte("committed")))
	_, err = store.Seal(1)
	require.NoError(t, err)

	// A seal whose manifest commit never happened: write the tail,
	// promote it, and reopen without committing
	late := kr.NextHash()
	require.NoError(t, store.Put(late, []byte("late")))
	store.Mutex.Lock()
	sl, seq, err := store.promoteLiveFile(2, 0)
	require.NoError(t, err)
	require.NoError(t, writeIndexFile(
		filepath.Join(dir, strings.TrimSuffix(segmentFileName(2, seq), segDataSuffix)+segIndexSuffix),
		sl.order, sl.entries))
	store.Mutex.Unlock()

	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer reopened.Close()
	v, err := reopened.GetDeep(late)
	require.NoError(t, err, "an interrupted seal is the only copy of its records; it must be adopted")
	require.Equal(t, []byte("late"), v)
	v, err = reopened.GetDeep(first)
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), v)
}
