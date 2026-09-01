package blockchainDB

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The second stage of finalization (issue #47): the per-shard merged
// segments of one block set are packed into a single block-set file
// outside any shard, and the shards stop serving them.

// packedFixture builds a sharded database of `blocks` blocks with
// `perBlock` Perm entries each, sealed at every block, and returns the
// keys and values written in order.
func packedFixture(t *testing.T, dir string, seed byte, blocks, perBlock int) (kvs *KVShard, keys [][32]byte, values map[[32]byte][]byte) {
	t.Helper()
	os.RemoveAll(dir)
	t.Cleanup(func() { os.RemoveAll(dir) })
	kvs, err := NewKVShard(dir, 100_000) // High, so only block boundaries seal
	require.NoError(t, err)
	require.NoError(t, kvs.SetFilterBlocks(MinFilterBlocks))
	kr := NewFastRandom([]byte{seed})
	vr := NewFastRandom([]byte{seed, seed})
	values = make(map[[32]byte][]byte)
	for h := 1; h <= blocks; h++ {
		for i := 0; i < perBlock; i++ {
			key := kr.NextHash()
			val := vr.RandBuff(20, 60)
			require.NoError(t, kvs.PutPerm(key, val))
			keys = append(keys, key)
			values[key] = val
		}
		require.NoError(t, kvs.SealBlock(uint64(h)))
	}
	return kvs, keys, values
}

// permSegmentsBelow counts, across every shard, the Perm segments the
// shards are still serving from blocks below height
func permSegmentsBelow(kvs *KVShard, height uint64) (n int) {
	for _, sh := range kvs.Shards {
		for _, seg := range sh.PermKV.sealedSegments() {
			if seg.meta.Height < height {
				n++
			}
		}
	}
	return n
}

// permDataFiles counts the sealed segment data files on disk across
// every shard's Perm directory, and how many of them belong to blocks
// below height
func permDataFiles(t *testing.T, kvs *KVShard, height uint64) (n, below int) {
	t.Helper()
	for _, sh := range kvs.Shards { // Deletion is a goroutine's; let it finish first
		sh.PermKV.awaitUnlinks()
	}
	for i := range kvs.Shards {
		entries, err := os.ReadDir(filepath.Join(kvs.ShardDir(i), PermDirName))
		require.NoError(t, err)
		for _, e := range entries {
			if !strings.HasPrefix(e.Name(), segFilePrefix) || filepath.Ext(e.Name()) != segDataSuffix {
				continue // live.dat, bloom.dat, indexes, the manifest
			}
			n++
			h, _, err := keyFromName(e.Name())
			require.NoError(t, err)
			if h < height {
				below++
			}
		}
	}
	return n, below
}

// checkEveryKey reads every key back through both the Perm and the
// layered lookup
func checkEveryKey(t *testing.T, kvs *KVShard, keys [][32]byte, values map[[32]byte][]byte, when string) {
	t.Helper()
	for i, key := range keys {
		v, err := kvs.GetDeep(key)
		require.NoErrorf(t, err, "key %d lost %s", i, when)
		require.Equalf(t, values[key], v, "key %d wrong %s", i, when)
		v, err = kvs.GetDeep(key)
		require.NoErrorf(t, err, "key %d lost via Get %s", i, when)
		require.Equal(t, values[key], v)
	}
}

// TestPackFinalizedLeavesOneFileAndEveryKey
// One block set, packed: one file in the set directory, no segment
// below the watermark left in any shard, every key still readable, and
// the per-shard files gone once the shards next commit.
func TestPackFinalizedLeavesOneFileAndEveryKey(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 181, 8, 100)
	next := ageOutShards(t, kvs, 8) // Finalisation works on history

	_, err := kvs.MergeFinalized(7)
	require.NoError(t, err)
	filesBefore, belowBefore := permDataFiles(t, kvs, 7)
	require.Greater(t, permSegmentsBelow(kvs, 7), 0)

	meta, packed, err := kvs.PackFinalized(7)
	require.NoError(t, err)
	require.True(t, packed, "six finalized blocks must pack")
	assert.Equal(t, uint64(0), meta.First, "the first set covers every block there has been")
	assert.Equal(t, uint64(6), meta.Last)
	assert.Equal(t, uint64(600), meta.Keys, "every key below the watermark is in the set")

	entries, err := os.ReadDir(kvs.setDir())
	require.NoError(t, err)
	require.Len(t, entries, 1, "one block set is one file")
	assert.Equal(t, meta.File, entries[0].Name())

	assert.Equal(t, 0, permSegmentsBelow(kvs, 7), "the shards must stop serving what the set holds")
	// The drop is a history commit per shard, so the merged segments'
	// files are gone; what stays is the merge inputs, which the active
	// manifest still named when the merge replaced them, until each
	// shard's next active commit
	files, below := permDataFiles(t, kvs, 7)
	assert.Less(t, files, filesBefore, "the drop retires the merged segments' files")
	assert.Less(t, below, belowBefore)
	assert.Greater(t, below, 0, "the merge inputs wait for the shards' next active commit")

	checkEveryKey(t, kvs, keys, values, "after packing")

	// An unwritten key is still absent, and settled without an error
	kr := NewFastRandom([]byte{182})
	for i := 0; i < 50; i++ {
		_, err := kvs.GetPerm(kr.NextHash())
		require.ErrorIsf(t, err, errNotFound, "unwritten key %d", i)
	}

	// Packing again does nothing: everything below 7 is already packed
	_, packed, err = kvs.PackFinalized(7)
	require.NoError(t, err)
	assert.False(t, packed)

	// The next block boundary commits the active manifest of every
	// shard that took a write, which is when that shard's merge inputs
	// are retired
	for i := 0; i < 200; i++ { // Enough to touch most shards
		require.NoError(t, kvs.PutPerm(kr.NextHash(), []byte("v")))
	}
	require.NoError(t, kvs.SealBlock(next))
	_, belowAfter := permDataFiles(t, kvs, 7)
	assert.Less(t, belowAfter, below, "the shards' next active commit retires the merge inputs")

	// And a reopen finds it all from the set file
	require.NoError(t, kvs.Close())
	reopened, err := OpenKVShard(dir)
	require.NoError(t, err)
	require.Len(t, reopened.Sets.Sets(), 1)
	assert.Equal(t, 0, permSegmentsBelow(reopened, 7))
	checkEveryKey(t, reopened, keys, values, "after a reopen")
	require.NoError(t, reopened.Close())
}

// TestPackFinalizedCountsFiles
// The point of the second stage, measured at the size a test can
// afford: one set, before and after.
func TestPackFinalizedCountsFiles(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 183, 6, 2000)
	next := ageOutShards(t, kvs, 6)

	segs, _ := permDataFiles(t, kvs, 6)
	_, err := kvs.MergeFinalized(6)
	require.NoError(t, err)
	merged, mergedBelow := permDataFiles(t, kvs, 6)
	_, packed, err := kvs.PackFinalized(6)
	require.NoError(t, err)
	require.True(t, packed)
	for i := 0; i < 2000; i++ { // Touch every shard, so every drop is committed
		require.NoError(t, kvs.PutPerm(NewFastRandom([]byte{183, byte(i), byte(i >> 8)}).NextHash(), []byte("v")))
	}
	require.NoError(t, kvs.SealBlock(next)) // Retire the merge inputs
	_, after := permDataFiles(t, kvs, 6)
	t.Logf("5 blocks x 2000 entries, below the watermark: %d segment files -> %d merged (%d files in all) -> %d left in shards + 1 set file",
		segs, mergedBelow, merged, after)
	assert.Equal(t, 0, permSegmentsBelow(kvs, 6))
	assert.Less(t, after, mergedBelow/10, "nearly every shard sealed, so nearly every packed file is retired")
	checkEveryKey(t, kvs, keys, values, "after packing")
	require.NoError(t, kvs.Close())
}

// TestPackFinalizedSurvivesACrashBeforeTheShardsCommit
// The set's commit is its rename; the shards drop the packed segments
// without a manifest of their own.  A crash between the two leaves
// every shard's manifest naming segments the set already holds.  The
// next open must drop them again -- nothing served twice, nothing lost
// -- and the next commit must retire their files.
func TestPackFinalizedSurvivesACrashBeforeTheShardsCommit(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 184, 5, 80)
	next := ageOutShards(t, kvs, 5)

	_, err := kvs.MergeFinalized(5)
	require.NoError(t, err)
	meta, packed, err := kvs.PackFinalized(5)
	require.NoError(t, err)
	require.True(t, packed)
	_, below := permDataFiles(t, kvs, 5)
	require.Greater(t, below, 0, "the merge inputs' files are still on disk: the active manifest names them")

	// "Crash": reopen without closing.  The shards' active manifests
	// were not rewritten since before the merge, so they still name
	// the merge inputs, whose keys the set also holds.
	reopened, err := OpenKVShard(dir)
	require.NoError(t, err)
	sets := reopened.Sets.Sets()
	require.Len(t, sets, 1)
	assert.Equal(t, meta, sets[0])
	assert.Equal(t, 0, permSegmentsBelow(reopened, 5), "recovery must drop what the set already holds")
	checkEveryKey(t, reopened, keys, values, "after the crash")

	// The next commit in each shard retires the files it no longer needs
	kr := NewFastRandom([]byte{185})
	for i := 0; i < 200; i++ { // Enough to touch most shards
		require.NoError(t, reopened.PutPerm(kr.NextHash(), []byte("v")))
	}
	require.NoError(t, reopened.SealBlock(next))
	_, belowAfter := permDataFiles(t, reopened, 5)
	assert.Less(t, belowAfter, below, "the next active commit retires the packed files")

	// A clean close, then a clean open: still all there
	require.NoError(t, reopened.Close())
	again, err := OpenKVShard(dir)
	require.NoError(t, err)
	checkEveryKey(t, again, keys, values, "after a clean reopen")
	require.NoError(t, again.Close())
}

// TestPackedKeysStayImmutable
// The Perm layer refuses a different value for a key it holds inside
// the window.  That check is answered by the shard's key filters
// first, and they do not cover the keys that have left the segments
// for a set, so a filter whose window reaches a set must be built
// with the set's keys -- including on a store whose filters were
// rebuilt from scratch on open, from segments that no longer hold the
// key, which is the case this test exists for.
//
// Only history is packed, so a set lies below the window by
// construction and a rebuilt filter normally has no set to add.  The
// window reaches a set when it widens: a shard reopened with a block
// behind the set's, or SetFilterBlocks raising N, which is what this
// test uses.
func TestPackedKeysStayImmutable(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 186, 4, 60)
	ageOutShards(t, kvs, 4)

	_, err := kvs.MergeFinalized(4)
	require.NoError(t, err)
	_, packed, err := kvs.PackFinalized(4)
	require.NoError(t, err)
	require.True(t, packed)

	// Close commits every shard's manifest, so the packed segments are
	// gone from the shards, and saves every filter.  Deleting the saved
	// filters forces each shard to rebuild on open from what it holds --
	// which is not the packed keys: only the set store has those.
	require.NoError(t, kvs.Close())
	for i := range kvs.Shards {
		saved := filepath.Join(kvs.ShardDir(i), PermDirName, filtersFilename)
		if err := os.Remove(saved); err != nil {
			require.True(t, errors.Is(err, os.ErrNotExist), "%v", err)
		}
	}
	reopened, err := OpenKVShard(dir)
	require.NoError(t, err)
	shard := reopened.Shards[reopened.ShardIndex(keys[0][:])]
	// The set holds the key now, and no segment of its shard does.
	// (Not "the shard holds no segments": how many a shard keeps
	// depends on how many there are, and this test is about one key.)
	for _, seg := range shard.PermKV.sealedSegments() {
		_, found, err := seg.lookup(keys[0])
		require.NoError(t, err)
		require.Falsef(t, found, "%s still holds the packed key", seg.meta.File)
	}

	packedKey := keys[0]
	other := append([]byte("changed-"), values[packedKey]...)

	// As reopened, the window starts above the set: the packed key is
	// older than the window, not consulted on write (issue #44), and
	// the filters do not claim it
	shard.PermKV.Mutex.RLock()
	start, ok := shard.PermKV.windowStart()
	claimed := shard.PermKV.filterTest(packedKey)
	shard.PermKV.Mutex.RUnlock()
	require.True(t, ok)
	require.Greater(t, start, uint64(4), "the set lies below the window")
	require.False(t, claimed, "a filter must not claim a key below its window")

	// Widen the window until it reaches the set: the rebuilt filters
	// must take the set's keys, or the packed key could be rewritten
	require.NoError(t, shard.PermKV.SetFilterBlocks(100))

	// PutPerm: a different value is refused, the same is a no-op
	err = reopened.PutPerm(packedKey, other)
	require.ErrorIs(t, err, ErrImmutable, "a packed key must still be immutable")
	require.NoError(t, reopened.PutPerm(packedKey, values[packedKey]), "an identical rewrite is a no-op")
	assert.Equal(t, 0, shard.PermKV.LiveCount(), "a rewrite of a packed key must not append a record")

	// Put: a different value moves the key to Dyna, as it always has
	require.NoError(t, reopened.Put(packedKey, other))
	v, err := reopened.Get(packedKey)
	require.NoError(t, err)
	assert.Equal(t, other, v, "Get answers with the dynamic copy")
	v, err = shard.PermKV.GetDeep(packedKey)
	require.NoError(t, err)
	assert.Equal(t, values[packedKey], v, "the permanent copy is unchanged")
	require.NoError(t, reopened.Close())
}

// TestForEachReachesPackedKeys
// Iteration must see the packed keys once each.
func TestForEachReachesPackedKeys(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 187, 6, 40)
	ageOutShards(t, kvs, 6)

	// Pack two sets in turn, so the second set is not the first, and
	// leave block 6 unpacked
	for _, w := range []uint64{3, 6} {
		_, err := kvs.MergeFinalized(w)
		require.NoError(t, err)
		_, packed, err := kvs.PackFinalized(w)
		require.NoError(t, err)
		require.Truef(t, packed, "watermark %d", w)
	}
	require.Len(t, kvs.Sets.Sets(), 2)

	seen := make(map[[32]byte]int)
	err := kvs.ForEach(func(key [32]byte, value []byte) error {
		seen[key]++
		assert.Equal(t, values[key], value)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, seen, len(keys), "every key, packed or not, is visited")
	for i, key := range keys {
		assert.Equalf(t, 1, seen[key], "key %d visited %d times", i, seen[key])
	}
	require.NoError(t, kvs.Close())
}

// TestRepeatedPacksKeepDeepEntriesReachable
// The steady state: set after set packed, the oldest entries several
// sets deep, all reachable, and again after a reopen.
func TestRepeatedPacksKeepDeepEntriesReachable(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)
	kvs, err := NewKVShard(dir, 100_000)
	require.NoError(t, err)
	require.NoError(t, kvs.SetFilterBlocks(MinFilterBlocks))

	// A set is N blocks, so that every set rolls the window once and
	// the set before the window is what each pack finalises
	const sets, setSize, perBlock = 6, MinFilterBlocks, 30
	kr := NewFastRandom([]byte{188})
	vr := NewFastRandom([]byte{188, 188})
	var keys [][32]byte
	values := make(map[[32]byte][]byte)
	block := uint64(0)
	for set := 0; set < sets; set++ {
		for b := 0; b < setSize; b++ {
			block++
			for i := 0; i < perBlock; i++ {
				key := kr.NextHash()
				val := vr.RandBuff(10, 40)
				require.NoError(t, kvs.PutPerm(key, val))
				keys = append(keys, key)
				values[key] = val
			}
			require.NoError(t, kvs.SealBlock(block))
		}
		watermark := block - setSize + 1
		if watermark > 1 {
			_, err = kvs.MergeFinalized(watermark)
			require.NoError(t, err)
			_, packed, err := kvs.PackFinalized(watermark)
			require.NoError(t, err)
			// The first set is still inside the window at the end of
			// the second; from the third on, each pack finalises the
			// set the window has just rolled past
			require.Equalf(t, set >= 1, packed, "set %d: packed=%v", set, packed)
		}
	}
	metas := kvs.Sets.Sets()
	require.Len(t, metas, sets-1)
	for i := 1; i < len(metas); i++ {
		assert.Equal(t, metas[i-1].Last+1, metas[i].First, "sets are contiguous")
	}
	checkEveryKey(t, kvs, keys, values, "after repeated packs")

	require.NoError(t, kvs.Close())
	reopened, err := OpenKVShard(dir)
	require.NoError(t, err)
	checkEveryKey(t, reopened, keys, values, "after a reopen")
	for i := 0; i < 50; i++ {
		_, err := reopened.GetPerm(kr.NextHash())
		require.ErrorIsf(t, err, errNotFound, "unwritten key %d", i)
	}
	require.NoError(t, reopened.Close())
}

// TestPackedBlocksRefuseAnImport
// A block in a set is finalized.  A shard that has dropped every
// segment has no newest segment to measure an import against, so the
// set's watermark has to be the bound, or a peer could re-import a
// block the node already holds cold.
func TestPackedBlocksRefuseAnImport(t *testing.T) {
	dirA := filepath.Join(os.TempDir(), t.Name()+"_A")
	dirB := filepath.Join(os.TempDir(), t.Name()+"_B")
	exportDir := filepath.Join(os.TempDir(), t.Name()+"_export")
	os.RemoveAll(exportDir)
	defer os.RemoveAll(exportDir)

	nodeA, _, _ := packedFixture(t, dirA, 189, 1, 0)
	nodeB, _, _ := packedFixture(t, dirB, 190, 0, 0)

	// A exports blocks 2 and 3; B takes both and packs them
	kr := NewFastRandom([]byte{191})
	var prev *Manifest
	var err error
	for h := uint64(2); h <= 3; h++ {
		for i := 0; i < 50; i++ {
			require.NoError(t, nodeA.PutPerm(kr.NextHash(), []byte("v")))
		}
		prev, err = nodeA.ExportBlock(exportDir, h, prev)
		require.NoError(t, err)
		_, err = nodeB.ImportBlock(filepath.Join(exportDir, fmt.Sprintf("block-%08d", h)))
		require.NoError(t, err)
	}
	ageOutShards(t, nodeB, 3)
	_, err = nodeB.MergeFinalized(4)
	require.NoError(t, err)
	_, packed, err := nodeB.PackFinalized(4)
	require.NoError(t, err)
	require.True(t, packed)

	// Block 3 again: already packed, must be refused rather than adopted
	_, err = nodeB.ImportBlock(filepath.Join(exportDir, "block-00000003"))
	require.Error(t, err, "a block already packed into a set must not be imported again")
	assert.Contains(t, err.Error(), "block set")

	require.NoError(t, nodeA.Close())
	require.NoError(t, nodeB.Close())
}

// TestBlockSetHeaderIsChecked
// A set file carries its magic, version and shard count, and open
// refuses one it cannot read rather than guessing.  A .tmp left by an
// interrupted build is deleted.
func TestBlockSetHeaderIsChecked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset int64
		want   string
	}{
		{"magic", 0, "not a block set"},
		{"version", 4, "block set format version"},
		{"shards", 8, "shards"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(os.TempDir(), t.Name())
			kvs, _, _ := packedFixture(t, dir, 192, 3, 20)
			ageOutShards(t, kvs, 3)
			_, err := kvs.MergeFinalized(3)
			require.NoError(t, err)
			meta, packed, err := kvs.PackFinalized(3)
			require.NoError(t, err)
			require.True(t, packed)
			require.NoError(t, kvs.Close())

			path := filepath.Join(kvs.setDir(), meta.File)
			f, err := os.OpenFile(path, os.O_RDWR, 0644)
			require.NoError(t, err)
			var bad [4]byte
			binary.BigEndian.PutUint32(bad[:], 0xDEADBEEF)
			_, err = f.WriteAt(bad[:], tc.offset)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			_, err = OpenKVShard(dir)
			require.Error(t, err, "a set file this build cannot read must be refused")
			assert.Contains(t, err.Error(), tc.want)
		})
	}

	t.Run("tmp", func(t *testing.T) {
		dir := filepath.Join(os.TempDir(), t.Name())
		kvs, _, _ := packedFixture(t, dir, 193, 1, 5)
		require.NoError(t, kvs.Close())
		stray := filepath.Join(kvs.setDir(), setFileName(1, 1)+segTmpSuffix)
		require.NoError(t, os.WriteFile(stray, []byte("half a set"), 0644))
		reopened, err := OpenKVShard(dir)
		require.NoError(t, err)
		_, err = os.Stat(stray)
		assert.True(t, errors.Is(err, os.ErrNotExist), "an interrupted build's .tmp is deleted on open")
		assert.Empty(t, reopened.Sets.Sets())
		require.NoError(t, reopened.Close())
	})
}

// TestDayFiltersSkipWholeGroups
// A deep read rules a key out of cold storage by probing every block
// set's filter: one cheap probe each, but sets accumulate for the life
// of the chain, so the WALK grows with age even though each step does
// not.  Packing every 1,000 blocks, a year is ~31,500 sets, and every
// deep miss walks all of them.
//
// A finished group of sets carries one filter over every key in it, so
// a miss skips the group in a single probe, and a hit still walks
// (issue #47).  The group is a BLOCK RANGE, not a clock reading, so
// this test moves the group size rather than time.
func TestDayFiltersSkipWholeGroups(t *testing.T) {
	old := SetGroupBlocks
	defer func() { setGroupBlocksForTest(old) }()
	setGroupBlocksForTest(8) // Eight blocks to a group

	dir := storeDir(t, "daygroups")
	// Enough blocks that the window (N to 2N) has rolled past what is
	// packed: only HISTORY is packed, whatever the watermark says
	kvs, keys, values := packedFixture(t, dir, 0xd1, 100, 3)
	defer kvs.Close()

	// Three rounds, each landing in a later group, so the earlier
	// groups finish and the last is still being filled
	for _, upTo := range []uint64{20, 40, 60} {
		_, packed, err := kvs.PackFinalized(upTo)
		require.NoErrorf(t, err, "pack below %d", upTo)
		require.Truef(t, packed, "pack below %d must have packed something", upTo)
	}

	filters := kvs.Sets.groupFilters()
	require.NotEmpty(t, filters, "a finished group must carry a filter")
	for _, f := range filters {
		require.NotZero(t, f.bloomBytes, "group %d: the filter must have bits", f.group)
	}

	// Every packed key is still found -- a filter that denies a key it
	// holds is a wrong answer, and the walk behind it must still run
	for i, k := range keys {
		v, err := kvs.GetDeep(k)
		require.NoErrorf(t, err, "key %d unreachable through the group filters", i)
		require.Equal(t, values[k], v)
	}

	// And the filters actually rule things out: a key that was never
	// written must be denied by every finished group
	kr := NewFastRandom([]byte{0xd2})
	ruledOut := 0
	for i := 0; i < 200; i++ {
		absent := kr.NextHash()
		_, err := kvs.GetDeep(absent)
		require.ErrorIsf(t, err, errNotFound, "unwritten key %d", i)
		for _, f := range filters {
			if !f.mightHold(absent) {
				ruledOut++
				break
			}
		}
	}
	require.Greaterf(t, ruledOut, 150,
		"the group filters ruled out only %d of 200 absent keys; they are not skipping groups", ruledOut)

	// It survives a reopen: the filters are on disk, read by header
	require.NoError(t, kvs.Close())
	re, err := OpenKVShard(dir)
	require.NoError(t, err)
	defer re.Close()
	require.Len(t, re.Sets.groupFilters(), len(filters), "the filters must be found again on open")
	for i, k := range keys {
		v, err := re.GetDeep(k)
		require.NoErrorf(t, err, "key %d lost across reopen", i)
		require.Equal(t, values[k], v)
	}
}

// TestPackSurvivesAFailedGroupFilter
// Everything before the group filter is the pack's commit: the set is
// fsynced, renamed, the directory synced, and the set is in the list.
// The caller's next act is to drop the segments the set now holds.
// Failing the pack for a filter that could not be written would abort
// something that has already happened -- the shards would keep the
// segments, and the retry would name a second set over the same blocks
// with First above Last.
func TestPackSurvivesAFailedGroupFilter(t *testing.T) {
	dir := storeDir(t, "packfilterfail")
	old := SetGroupBlocks
	defer func() { setGroupBlocksForTest(old) }()
	setGroupBlocksForTest(8)

	kvs, keys, values := packedFixture(t, dir, 0xf1, 100, 3)
	defer kvs.Close()
	_, packed, err := kvs.PackFinalized(20)
	require.NoError(t, err)
	require.True(t, packed)

	// Fail the FILTER and nothing else: a directory standing where the
	// filter's file goes makes its rename fail while the set itself
	// still writes normally.  (A read-only set directory would fail the
	// set too, and a pack that cannot write its set SHOULD fail.)
	blocked := filepath.Join(kvs.Sets.Directory, dayFilterName(0))
	require.NoError(t, os.MkdirAll(filepath.Join(blocked, "in the way"), 0o755))

	// The pack that closes out the first group must still succeed
	_, packed, err = kvs.PackFinalized(40)
	require.NoError(t, err, "a filter that cannot be written must not fail a committed pack")
	require.True(t, packed)

	require.NoError(t, os.RemoveAll(blocked))

	// Every key is still readable, through sets with no group filter
	for i, k := range keys {
		v, err := kvs.GetDeep(k)
		require.NoErrorf(t, err, "key %d after a pack whose filter failed", i)
		require.Equal(t, values[k], v)
	}

	// And the next pack builds what the failed one could not
	_, _, err = kvs.PackFinalized(60)
	require.NoError(t, err)
	require.NotEmpty(t, kvs.Sets.groupFilters(), "the next pack must build the missing filter")
}

// TestADayFilterRefusesADifferentGroupSize
// SetGroupBlocks is not persisted, and a filter's file name records
// only its group's index.  Reopening with a different size would
// consult a filter built over one range while walking the sets of
// another, and answer a definitive "not here" for keys it never saw --
// the one answer a filter may never give.
func TestADayFilterRefusesADifferentGroupSize(t *testing.T) {
	dir := storeDir(t, "groupsize")
	old := SetGroupBlocks
	defer func() { setGroupBlocksForTest(old) }()
	setGroupBlocksForTest(8)

	kvs, keys, values := packedFixture(t, dir, 0xf2, 100, 3)
	for _, upTo := range []uint64{20, 40, 60} {
		_, _, err := kvs.PackFinalized(upTo)
		require.NoError(t, err)
	}
	require.NotEmpty(t, kvs.Sets.groupFilters())
	require.NoError(t, kvs.Close())

	// Reopen with a different group size: the filters on disk describe
	// ranges that are no longer their groups
	setGroupBlocksForTest(32)
	re, err := OpenKVShard(dir)
	require.NoError(t, err, "a store must open even when its filters no longer match")
	defer re.Close()
	require.Empty(t, re.Sets.groupFilters(),
		"a filter whose range is not its group must be refused, not consulted")

	// And every key is still found, because the groups are walked
	for i, k := range keys {
		v, err := re.GetDeep(k)
		require.NoErrorf(t, err, "key %d lost after the group size changed", i)
		require.Equal(t, values[k], v)
	}
}

// TestRecoveryDropsASegmentTheSetAlreadyHolds
// DropBelow removes a packed segment from history.  One can be in the
// ACTIVE tier instead: a segment whose unlink a crash interrupted is
// named by no manifest and above nothing, so recoverOrphans adopts it
// as an interrupted seal, and the block height it carries then puts it
// inside the window, where the drop does not reach.  The shard
// resurrects a segment the set already holds (issue #80).
//
// recoverOrphans cannot settle this itself: it runs before a cold
// store is attached, so its watermark rule has nothing to ask.
func TestRecoveryDropsASegmentTheSetAlreadyHolds(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 0xb4, 4, 60)
	defer os.RemoveAll(dir)
	// Age past every block and pack ALL of them, so the shards keep no
	// segment at all.  That is what makes the restored file an orphan
	// above nothing: with any segment left, recoverOrphans sees it as
	// superseded by a newer one and deletes it on the old rule.
	ageOutShards(t, kvs, 5)
	_, err := kvs.MergeFinalized(5)
	require.NoError(t, err)

	// Keep a copy of a segment about to be packed away, so it can be
	// put back the way an interrupted unlink leaves one
	shardIdx := kvs.ShardIndex(keys[0][:])
	segs := kvs.Shards[shardIdx].PermKV.sealedSegments()
	require.NotEmpty(t, segs, "the shard must hold a segment to pack")
	dataPath, indexPath := segs[0].dataPath, segs[0].indexPath
	data, err := os.ReadFile(dataPath)
	require.NoError(t, err)
	index, err := os.ReadFile(indexPath)
	require.NoError(t, err)

	_, packed, err := kvs.PackFinalized(5)
	require.NoError(t, err)
	require.True(t, packed)
	require.Empty(t, kvs.Shards[shardIdx].PermKV.sealedSegments(),
		"the shard must keep no segment, or the old superseded rule settles this")
	require.NoError(t, kvs.Close())

	// The crash: the manifest commit landed, the unlink did not
	require.NoError(t, os.WriteFile(dataPath, data, 0o644))
	require.NoError(t, os.WriteFile(indexPath, index, 0o644))

	reopened, err := OpenKVShard(dir)
	require.NoError(t, err)
	defer reopened.Close()

	_, err = os.Stat(dataPath)
	require.Truef(t, os.IsNotExist(err),
		"%s was adopted back; the set already holds every key in it", filepath.Base(dataPath))
	require.Empty(t, reopened.Shards[shardIdx].PermKV.sealedSegments(),
		"the shard must serve those keys from the set, not from a resurrected segment")

	// And nothing was lost with it: the set answers for those keys
	for i, k := range keys {
		v, err := reopened.GetDeep(k)
		require.NoErrorf(t, err, "key %d after the drop", i)
		require.Equal(t, values[k], v)
	}
}
