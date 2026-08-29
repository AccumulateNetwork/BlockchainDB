package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
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
	require.Len(t, store.segments, blocks, "one segment per block before merging")

	// Finalise everything below block 20, leaving the newest alone
	meta, merged, err := store.MergeBelow(20)
	require.NoError(t, err)
	require.True(t, merged, "19 segments below the watermark must merge")
	assert.Len(t, store.segments, 2, "19 merged into 1, plus the segment at block 20")
	assert.Equal(t, uint64(19*perBlock), meta.Count, "the merged segment holds every key below the watermark")

	// The merged segment must order before what was left standing
	assert.True(t, store.segments[1].meta.after(store.segments[0].meta),
		"the merged segment must still order before the segments it did not replace")

	// Every key still reads back, from a segment that no longer exists
	for i, key := range keys {
		v, err := store.Get(key)
		require.NoErrorf(t, err, "key %d lost by the merge", i)
		assert.Equal(t, values[key], string(v))
	}

	// The files the merge replaced are gone
	segFiles := 0
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == segDataSuffix {
			segFiles++
		}
	}
	assert.Equal(t, 3, segFiles, "2 sealed segments plus live.dat")

	// And it survives a reopen: the manifest names the merged segment
	require.NoError(t, store.Close())
	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	require.Len(t, reopened.segments, 2)
	for i, key := range keys {
		v, err := reopened.Get(key)
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

	kr := NewFastRandom([]byte{172})
	for h := uint64(1); h <= 10; h++ {
		require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
		_, err = store.Seal(h)
		require.NoError(t, err)
	}

	_, merged, err := store.MergeBelow(6)
	require.NoError(t, err)
	require.True(t, merged)

	// Blocks 6..10 must still be individually present
	heights := map[uint64]bool{}
	for _, seg := range store.segments {
		heights[seg.meta.Height] = true
	}
	for h := uint64(6); h <= 10; h++ {
		assert.Truef(t, heights[h], "block %d is at or above the watermark and must not be merged", h)
	}
	assert.Len(t, store.segments, 6, "blocks 1-5 merged into one, 6..10 left standing")

	// Merging again changes nothing: only one segment is below 6 now
	_, merged, err = store.MergeBelow(6)
	require.NoError(t, err)
	assert.False(t, merged, "a single segment below the watermark is not worth merging")
}

// TestMergeFinalizedAcrossShards
// The sharded wrapper merges each shard independently.
func TestMergeFinalizedAcrossShards(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kvs, err := NewKVShard(dir, 100_000) // High, so only block boundaries seal
	require.NoError(t, err)

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

	before := 0
	for _, sh := range kvs.Shards {
		before += len(sh.PermKV.segments)
	}

	mergedShards, err := kvs.MergeFinalized(7)
	require.NoError(t, err)
	require.Greater(t, mergedShards, 0, "some shard must have had two segments below the watermark")

	after := 0
	for _, sh := range kvs.Shards {
		after += len(sh.PermKV.segments)
	}
	assert.Less(t, after, before, "merging must reduce the segment count (%d -> %d)", before, after)

	for i, key := range keys {
		v, err := kvs.GetPerm(key)
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
	// blocks 1-4 leave the shards altogether for a block-set file
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
	_, err = nodeA.ExportBlock(exportDir, 7, prev)
	require.NoError(t, err)

	// A fresh node syncs from the exported blocks and gets everything
	nodeB, err := NewKVShard(dirB, 100_000)
	require.NoError(t, err)
	var total uint64
	for h := 1; h <= 7; h++ {
		n, err := nodeB.ImportBlock(filepath.Join(exportDir, fmt.Sprintf("block-%08d", h)))
		require.NoErrorf(t, err, "import block %d after a merge on the exporting node", h)
		total += n
	}
	assert.Equal(t, uint64(len(keys)), total, "every record must reach the peer")
	for i, key := range keys {
		v, err := nodeB.GetPerm(key)
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

	const sets = 8    // Eight completed block sets
	const setSize = 5 // Blocks per set, standing in for N
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
		// Finalise every set but the one just written, so each merge
		// after the first folds in what the previous merge produced
		watermark := block - uint64(setSize) + 1
		if watermark > 1 {
			_, merged, err := store.MergeBelow(watermark)
			require.NoErrorf(t, err, "merge after set %d", set)
			require.Truef(t, merged, "set %d should have merged", set)
		}
	}

	// The oldest entries have now been through several merges.  Every
	// entry must still be reachable, and with the right value.
	for i, e := range all {
		v, err := store.Get(e.key)
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
		v, err := reopened.Get(e.key)
		require.NoErrorf(t, err, "entry %d from block %d lost across the reopen", i, e.block)
		require.Equal(t, e.value, string(v))
	}
	t.Logf("%d entries over %d blocks, %d merges, %d segments standing",
		len(all), block, sets-1, len(reopened.segments))
	require.NoError(t, reopened.Close())
}
