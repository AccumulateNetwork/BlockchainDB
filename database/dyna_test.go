package blockchainDB

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dynaDir is the Dyna layer's directory within a KV2
func dynaDir(kv2 *KV2) string { return filepath.Join(kv2.Directory, DynaDirName) }

// TestDynaCompressReclaims
// The Dyna layer accumulates trash as keys are overwritten, and
// Compress reclaims it -- the property KV.Compress used to provide,
// now by writing a new sealed generation instead of swapping the
// values file in place.
func TestDynaCompressReclaims(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv2, err := NewKV2(dir, 500) // SealLimit 500 records
	require.NoError(t, err)

	kr := NewFastRandom([]byte{201})
	vr := NewFastRandom([]byte{201, 201})
	keys := make([][32]byte, 200)
	for i := range keys {
		keys[i] = kr.NextHash()
	}

	// Ten rounds of overwrites: ~90% of what is written is shadowed
	latest := make([][]byte, len(keys))
	for round := 0; round < 10; round++ {
		for i := range keys {
			latest[i] = vr.RandBuff(50, 200)
			_, err = kv2.PutDyna(keys[i], latest[i])
			require.NoError(t, err)
		}
	}
	require.Greater(t, len(kv2.DynaKV.segments), 1, "overwrites should have sealed several generations")

	sizeBefore := dirSize(t, dynaDir(kv2))
	require.NoError(t, kv2.Compress(), "compress")
	sizeAfter := dirSize(t, dynaDir(kv2))
	assert.Lessf(t, sizeAfter, sizeBefore/3, "space not reclaimed (%d -> %d)", sizeBefore, sizeAfter)
	assert.Len(t, kv2.DynaKV.segments, 1, "compaction should leave one generation")

	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d after compress", i)
		assert.Equalf(t, latest[i], v, "key %d after compress", i)
	}

	// And across a reopen
	require.NoError(t, kv2.Close())
	reopened, err := OpenKV2(dir)
	require.NoError(t, err)
	require.NoError(t, reopened.Open())
	for i := range keys {
		v, err := reopened.Get(keys[i])
		require.NoErrorf(t, err, "key %d after compress+reopen", i)
		assert.Equalf(t, latest[i], v, "key %d after compress+reopen", i)
	}
	require.NoError(t, reopened.Close())
}

// TestDynaCompressIsIdempotent
// Compress must not rewrite a layer that has nothing to reclaim, and
// must not lose anything either way.  KVShard compresses on a write
// count, so a shard whose keys rarely repeat hits this path often.
//
// "Nothing to reclaim" now means Compress does nothing at all, rather
// than sealing the tail and rewriting the single generation that
// produced.  The seal was the reason it could not opt out: it added a
// second segment, so the single-generation early-out never applied and
// every call paid for a full rewrite of the layer (issue #31).
func TestDynaCompressIsIdempotent(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv2, err := NewKV2(dir, 1000)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{202})
	vr := NewFastRandom([]byte{202, 202})
	keys := make([][32]byte, 50)
	values := make([][]byte, 50)
	for i := range keys {
		keys[i], values[i] = kr.NextHash(), vr.RandBuff(20, 100)
		_, err = kv2.PutDyna(keys[i], values[i])
		require.NoError(t, err)
	}

	// Distinct keys, so not one record is superseded
	require.NoError(t, kv2.Compress())
	assert.Empty(t, kv2.DynaKV.segments,
		"nothing was overwritten: Compress must not seal a generation in order to rewrite it")
	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d after a no-op compress", i)
		assert.Equal(t, values[i], v)
	}

	// Now overwrite every key enough times to be worth reclaiming
	for round := 0; round < 3; round++ {
		for i := range keys {
			values[i] = vr.RandBuff(20, 100)
			_, err = kv2.PutDyna(keys[i], values[i])
			require.NoError(t, err)
		}
	}
	require.NoError(t, kv2.Compress())
	require.Len(t, kv2.DynaKV.segments, 1, "three quarters of the layer is garbage: it must compact")
	first := kv2.DynaKV.segments[0].meta

	// The generation just written has nothing superseded in it
	require.NoError(t, kv2.Compress(), "second compress")
	require.Len(t, kv2.DynaKV.segments, 1)
	assert.Equal(t, first, kv2.DynaKV.segments[0].meta, "nothing to reclaim: the generation should stand as it is")

	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d", i)
		assert.Equal(t, values[i], v)
	}
	require.NoError(t, kv2.Close())
}

// TestDynaLiveTailBounded
// The Dyna layer seals on physical records, not distinct keys.  A few
// hot keys rewritten in a loop hold the key count flat, so a key-count
// trigger would never fire and the live tail -- replayed in full on
// every open -- would grow without bound.
func TestDynaLiveTailBounded(t *testing.T) {
	dir := storeDir(t, "kv2")
	const sealLimit = 100
	kv2, err := NewKV2(dir, sealLimit)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{203})
	vr := NewFastRandom([]byte{203, 203})
	keys := make([][32]byte, 5) // Five keys, rewritten 400 times each
	for i := range keys {
		keys[i] = kr.NextHash()
	}
	latest := make([][]byte, len(keys))
	for round := 0; round < 400; round++ {
		for i := range keys {
			latest[i] = vr.RandBuff(20, 60)
			_, err = kv2.PutDyna(keys[i], latest[i])
			require.NoError(t, err)
		}
		require.LessOrEqualf(t, kv2.DynaKV.LiveRecords(), uint64(sealLimit),
			"live tail unbounded at round %d", round)
	}

	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d", i)
		assert.Equal(t, latest[i], v)
	}
	require.NoError(t, kv2.Close())
}

// TestDynaCompressCrashMidway
// A crash between the compacted generation landing on disk and the
// manifest naming it must not lose or corrupt a value.  This is what
// KV.Compress could not promise (issue #19): it swapped the values
// file and rewrote the key offsets as two steps, and a crash between
// them left keys pointing into the wrong layout.
func TestDynaCompressCrashMidway(t *testing.T) {
	dir := storeDir(t, "kv2")
	crashed := storeDir(t, "crashed")
	kv2, err := NewKV2(dir, 200)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{204})
	vr := NewFastRandom([]byte{204, 204})
	keys := make([][32]byte, 100)
	for i := range keys {
		keys[i] = kr.NextHash()
	}
	latest := make([][]byte, len(keys))
	for round := 0; round < 6; round++ {
		for i := range keys {
			latest[i] = vr.RandBuff(20, 100)
			_, err = kv2.PutDyna(keys[i], latest[i])
			require.NoError(t, err)
		}
	}
	_, err = kv2.DynaKV.SealNext() // Everything reclaimable is sealed
	require.NoError(t, err)
	require.Greater(t, len(kv2.DynaKV.segments), 1)

	// Snapshot the pre-commit state, then compact for real.  Copying
	// the compacted data file back over the snapshot reproduces
	// exactly what a crash between the rename and the manifest write
	// leaves behind: the new generation on disk, the manifest still
	// naming the old one.
	copyDir(t, dynaDir(kv2), crashed)
	meta, err := kv2.DynaKV.CompactNext()
	require.NoError(t, err)
	copyFile(t, filepath.Join(dynaDir(kv2), meta.File), filepath.Join(crashed, meta.File))
	require.NoError(t, kv2.Close())

	store, err := OpenSegmentStore(crashed)
	require.NoError(t, err, "open after a crash mid-compaction")

	// The compacted segment sits above every height the manifest names,
	// so recovery adopts it rather than discarding it: it joins the old
	// generation as the newest segment, and newest wins.  Nothing is
	// lost either way -- what a crash here costs is the space the old
	// generation still occupies, until the next compaction.
	require.Len(t, store.segments, 4, "old generation plus the adopted compaction")
	assert.Equal(t, meta.Height, store.segments[len(store.segments)-1].meta.Height)

	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "key %d lost by a crash mid-compaction", i)
		assert.Equalf(t, latest[i], v, "key %d wrong after a crash mid-compaction", i)
	}

	// And compaction still converges from the recovered state
	_, err = store.CompactNext()
	require.NoError(t, err)
	require.Len(t, store.segments, 1)
	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "key %d after recompaction", i)
		assert.Equal(t, latest[i], v)
	}
	require.NoError(t, store.Close())
}

// TestKV2PermOverwriteSurvivesCompress
// A Perm key overwritten with a different value moves to the Dyna
// layer.  Compaction of that layer must not resurrect the Perm copy,
// which is still on disk underneath it.
func TestKV2PermOverwriteSurvivesCompress(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv2, err := NewKV2(dir, 100)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{205})
	vr := NewFastRandom([]byte{205, 205})
	keys := make([][32]byte, 40)
	perm := make([][]byte, len(keys))
	for i := range keys {
		keys[i], perm[i] = kr.NextHash(), vr.RandBuff(20, 80)
		_, err = kv2.Put(keys[i], perm[i]) // Lands in Perm
		require.NoError(t, err)
	}

	// Overwrite every other key: those move to Dyna
	want := make([][]byte, len(keys))
	copy(want, perm)
	for i := 0; i < len(keys); i += 2 {
		want[i] = vr.RandBuff(20, 80)
		_, err = kv2.Put(keys[i], want[i])
		require.NoError(t, err)
	}

	require.NoError(t, kv2.Compress())
	require.NoError(t, kv2.Close())

	reopened, err := OpenKV2(dir)
	require.NoError(t, err)
	require.NoError(t, reopened.Open())
	for i := range keys {
		v, err := reopened.Get(keys[i])
		require.NoErrorf(t, err, "key %d", i)
		assert.Equalf(t, want[i], v, "key %d resolved to the wrong layer", i)
	}
	require.NoError(t, reopened.Close())
}

// copyDir copies the files of a directory (no recursion needed: a
// store directory holds only files)
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dst, os.ModePerm))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		copyFile(t, filepath.Join(src, e.Name()), filepath.Join(dst, e.Name()))
	}
}

// copyFile copies one file, overwriting the destination
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	require.NoError(t, err)
	defer in.Close()
	out, err := os.Create(dst)
	require.NoError(t, err)
	defer out.Close()
	_, err = io.Copy(out, in)
	require.NoError(t, err)
	require.NoError(t, out.Sync())
}

// TestCompressEstimateSurvivesReopen
// The estimate that decides whether Compress is worth running is
// maintained as writes arrive.  It used to be process state, so a
// reopened store believed it held no garbage whatever it actually
// held, and would not compact until it had accumulated a threshold
// fraction of the whole layer again in fresh overwrites (issue #40).
func TestCompressEstimateSurvivesReopen(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv2, err := NewKV2(dir, 1000)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{204})
	vr := NewFastRandom([]byte{204, 204})
	keys := make([][32]byte, 40)
	for i := range keys {
		keys[i] = kr.NextHash()
		_, err = kv2.PutDyna(keys[i], vr.RandBuff(20, 100))
		require.NoError(t, err)
	}
	// Overwrite every one of them, so most of the layer is garbage
	values := make([][]byte, len(keys))
	for round := 0; round < 3; round++ {
		for i := range keys {
			values[i] = vr.RandBuff(20, 100)
			_, err = kv2.PutDyna(keys[i], values[i])
			require.NoError(t, err)
		}
	}

	// Seal, so the estimate reaches the manifest, then reopen
	_, err = kv2.DynaKV.SealNext()
	require.NoError(t, err)
	before := kv2.DynaKV.shadowed
	require.Greater(t, before, uint64(0), "the writes must have been counted as superseded")
	require.NoError(t, kv2.Close())

	reopened, err := OpenKV2(dir)
	require.NoError(t, err)
	require.NoError(t, reopened.Open())
	assert.Equal(t, before, reopened.DynaKV.shadowed,
		"the reopened store must know the garbage it holds")

	// And it acts on it: a Compress right after reopening compacts,
	// rather than waiting to re-accumulate the same garbage
	require.NoError(t, reopened.Compress())
	assert.Len(t, reopened.DynaKV.segments, 1, "a reopened store must still compact what it holds")

	for i, key := range keys {
		v, err := reopened.Get(key)
		require.NoErrorf(t, err, "key %d", i)
		assert.Equal(t, values[i], v)
	}
	require.NoError(t, reopened.Close())
}
