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
		sh.PermKV.Mutex.Lock()
		for _, seg := range sh.PermKV.segments {
			if seg.meta.Height < height {
				n++
			}
		}
		sh.PermKV.Mutex.Unlock()
	}
	return n
}

// permDataFiles counts the sealed segment data files on disk across
// every shard's Perm directory, and how many of them belong to blocks
// below height
func permDataFiles(t *testing.T, kvs *KVShard, height uint64) (n, below int) {
	t.Helper()
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
		v, err := kvs.GetPerm(key)
		require.NoErrorf(t, err, "key %d lost %s", i, when)
		require.Equalf(t, values[key], v, "key %d wrong %s", i, when)
		v, err = kvs.Get(key)
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
	files, below := permDataFiles(t, kvs, 7)
	assert.Equal(t, filesBefore, files, "the dropped files stay until the shards next commit a manifest")
	assert.Equal(t, belowBefore, below)

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

	// The next block boundary commits the manifest of every shard that
	// took a write, which is when that shard's dropped files are retired
	for i := 0; i < 200; i++ { // Enough to touch most shards
		require.NoError(t, kvs.PutPerm(kr.NextHash(), []byte("v")))
	}
	require.NoError(t, kvs.SealBlock(9))
	_, below = permDataFiles(t, kvs, 7)
	assert.Less(t, below, belowBefore, "the shards' next manifest commit retires the packed files")

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
	require.NoError(t, kvs.SealBlock(7)) // Commit the drops
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

	_, err := kvs.MergeFinalized(5)
	require.NoError(t, err)
	meta, packed, err := kvs.PackFinalized(5)
	require.NoError(t, err)
	require.True(t, packed)
	_, below := permDataFiles(t, kvs, 5)
	require.Greater(t, below, 0, "the dropped segments' files are still on disk")

	// "Crash": reopen without closing.  The shards' manifests were not
	// rewritten, so they still name the packed segments.
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
	require.NoError(t, reopened.SealBlock(6))
	_, belowAfter := permDataFiles(t, reopened, 5)
	assert.Less(t, belowAfter, below, "the next manifest commit retires the packed files")

	// A clean close, then a clean open: still all there
	require.NoError(t, reopened.Close())
	again, err := OpenKVShard(dir)
	require.NoError(t, err)
	checkEveryKey(t, again, keys, values, "after a clean reopen")
	require.NoError(t, again.Close())
}

// TestPackedKeysStayImmutable
// The Perm layer refuses a different value for a key it holds.  That
// check is answered by the shard's key filters first, and they do not
// cover the keys that have left the segments for a set, so a write the
// filters cannot place must go on to the sets before it is allowed --
// including on a store whose filters were rebuilt from scratch on
// open, from segments that no longer hold the key, which is the case
// this test exists for.
func TestPackedKeysStayImmutable(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 186, 4, 60)

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
	shard := reopened.Shards[ShardIndex(keys[0][:])]
	require.Equal(t, 0, len(shard.PermKV.segments), "the packed key's shard holds it in no segment now")

	packedKey := keys[0]
	other := append([]byte("changed-"), values[packedKey]...)

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
	v, err = reopened.GetPerm(packedKey)
	require.NoError(t, err)
	assert.Equal(t, values[packedKey], v, "the permanent copy is unchanged")
	require.NoError(t, reopened.Close())
}

// TestForEachReachesPackedKeys
// Iteration must see the packed keys once each.
func TestForEachReachesPackedKeys(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 187, 6, 40)

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

	const sets, setSize, perBlock = 6, 3, 30
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
			require.Truef(t, packed, "set %d", set)
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
