package blockchainDB

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dynaDir is the Dyna layer's directory within a KV2
func dynaDir(kv2 *KV2) string { return filepath.Join(kv2.Directory, DynaDirName) }

// ageOut
// Advance a store's block far enough that every segment it has sealed
// so far falls below the window and is handed to history: what a
// mutable layer needs before CompactHistory has anything to compact,
// since a record last written inside the window is the protocol's and
// compaction never touches it (issue #57).
func ageOut(t *testing.T, s *SegmentStore) {
	t.Helper()
	s.AdvanceBlock(s.BlockHeight() + 3*s.FilterBlocks)
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	require.Empty(t, s.active, "every sealed segment should now be history")
}

// TestDynaCompressReclaims
// The Dyna layer accumulates trash as keys are overwritten, and
// Compress reclaims it -- the property KV.Compress used to provide,
// now by writing a new sealed generation instead of swapping the
// values file in place.  What it reclaims is history: the segments
// the window has rolled past.
func TestDynaCompressReclaims(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv2, err := NewKV2(dir, 500) // SealLimit 500 records
	require.NoError(t, err)
	require.NoError(t, kv2.SetFilterBlocks(MinFilterBlocks))

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
	_, err = kv2.DynaKV.SealNext()
	require.NoError(t, err)
	require.Greater(t, len(kv2.DynaKV.sealedSegments()), 1, "overwrites should have sealed several generations")

	// Nothing to reclaim while every generation is inside the window
	sizeBefore := dirSize(t, dynaDir(kv2))
	require.NoError(t, kv2.Compress())
	require.Equal(t, sizeBefore, dirSize(t, dynaDir(kv2)), "a record inside the window is not compaction's to touch")

	// Sealing blocks ages the Dyna layer: past 2N blocks the generations
	// are history, and Compress folds them into one
	for h := uint64(1); h <= 3*MinFilterBlocks; h++ {
		_, err = kv2.Seal(h)
		require.NoError(t, err)
	}
	require.NoError(t, kv2.Compress(), "compress")
	assert.Len(t, kv2.DynaKV.sealedSegments(), 1, "compaction should leave one generation")

	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d after compress", i)
		assert.Equalf(t, latest[i], v, "key %d after compress", i)
	}

	// The generations it replaced were still named by the active
	// manifest, as handoffs in flight, so their files wait for the
	// next active commit; the close is one
	require.NoError(t, kv2.Close())
	sizeAfter := dirSize(t, dynaDir(kv2))
	assert.Lessf(t, sizeAfter, sizeBefore/3, "space not reclaimed (%d -> %d)", sizeBefore, sizeAfter)

	// And across a reopen
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
// "Nothing to reclaim" means Compress does nothing at all: it never
// seals the tail, and a single history segment is left as it is.
func TestDynaCompressIsIdempotent(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv2, err := NewKV2(dir, 1000)
	require.NoError(t, err)
	require.NoError(t, kv2.SetFilterBlocks(MinFilterBlocks))

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
	assert.Empty(t, kv2.DynaKV.sealedSegments(),
		"nothing was overwritten: Compress must not seal a generation in order to rewrite it")
	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d after a no-op compress", i)
		assert.Equal(t, values[i], v)
	}

	// Now overwrite every key enough times to be worth reclaiming, a
	// generation per round, and age them out of the window
	for round := 0; round < 3; round++ {
		for i := range keys {
			values[i] = vr.RandBuff(20, 100)
			_, err = kv2.PutDyna(keys[i], values[i])
			require.NoError(t, err)
		}
		_, err = kv2.DynaKV.SealNext()
		require.NoError(t, err)
	}
	ageOut(t, kv2.DynaKV)
	require.NoError(t, kv2.Compress())
	require.Len(t, kv2.DynaKV.sealedSegments(), 1, "three quarters of the layer is garbage: it must compact")
	first := kv2.DynaKV.sealedSegments()[0].meta

	// The generation just written has nothing superseded in it
	require.NoError(t, kv2.Compress(), "second compress")
	require.Len(t, kv2.DynaKV.sealedSegments(), 1)
	assert.Equal(t, first, kv2.DynaKV.sealedSegments()[0].meta, "nothing to reclaim: the generation should stand as it is")

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
	require.NoError(t, kv2.SetFilterBlocks(MinFilterBlocks))

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
	ageOut(t, kv2.DynaKV) // ... and out of the window
	require.Greater(t, len(kv2.DynaKV.sealedSegments()), 1)
	// One more record, sealed inside the window, so the store has an
	// active segment above everything the compaction will replace
	_, err = kv2.PutDyna(keys[0], latest[0])
	require.NoError(t, err)
	_, err = kv2.DynaKV.SealNext()
	require.NoError(t, err)
	before := len(kv2.DynaKV.sealedSegments())

	// Snapshot the pre-commit state, then compact for real.  Copying
	// the compacted data file back over the snapshot reproduces
	// exactly what a crash between the rename and the manifest write
	// leaves behind: the new generation on disk, the manifest still
	// naming the old one.
	copyDir(t, dynaDir(kv2), crashed)
	compacted, err := kv2.DynaKV.CompactHistory()
	require.NoError(t, err)
	require.True(t, compacted)
	require.NoError(t, kv2.Close())
	kv2.DynaKV.History.RLock()
	require.Empty(t, kv2.DynaKV.history) // Closed; find the output on disk instead
	kv2.DynaKV.History.RUnlock()
	reopened, err := OpenSegmentStore(dynaDir(kv2))
	require.NoError(t, err)
	reopened.History.RLock()
	require.Len(t, reopened.history, 1)
	out := reopened.history[0].meta
	reopened.History.RUnlock()
	require.NoError(t, reopened.Close())
	copyFile(t, filepath.Join(dynaDir(kv2), out.File), filepath.Join(crashed, out.File))

	store, err := OpenSegmentStore(crashed)
	require.NoError(t, err, "open after a crash mid-compaction")

	// The compacted segment sits below the newest segment the manifests
	// name -- the one sealed inside the window -- so recovery deletes
	// it: the inputs it would have replaced are still named and still
	// whole, and nothing is lost.  What the crash costs is the copy.
	require.Len(t, store.sealedSegments(), before, "the old generations, and only those, after recovery")
	_, err = os.Stat(filepath.Join(crashed, out.File))
	assert.True(t, errors.Is(err, os.ErrNotExist), "an uncommitted compaction below the newest segment is swept")

	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "key %d lost by a crash mid-compaction", i)
		assert.Equalf(t, latest[i], v, "key %d wrong after a crash mid-compaction", i)
	}

	// And compaction still converges from the recovered state
	compacted, err = store.CompactHistory()
	require.NoError(t, err)
	require.True(t, compacted)
	require.Len(t, store.sealedSegments(), 2, "one compacted history segment plus the active one")
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

// TestCompressNeverTouchesTheWindow
// A record last written inside the window -- the last N to 2N blocks
// -- is the protocol's, and compaction never reads or rewrites it.
// Only what has rolled out of the window is reclaimed, and a record in
// history superseded by one still in the window waits until that one
// has rolled out too.  This is the rule that bounds what a compaction
// can cost a commit (issue #57).
func TestCompressNeverTouchesTheWindow(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv2, err := NewKV2(dir, 1000)
	require.NoError(t, err)
	require.NoError(t, kv2.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{206})
	vr := NewFastRandom([]byte{206, 206})
	keys := make([][32]byte, 40)
	values := make([][]byte, len(keys))
	for i := range keys {
		keys[i] = kr.NextHash()
	}
	// Three generations of every key, all sealed, then aged out
	for round := 0; round < 3; round++ {
		for i := range keys {
			values[i] = vr.RandBuff(20, 100)
			_, err = kv2.PutDyna(keys[i], values[i])
			require.NoError(t, err)
		}
		_, err = kv2.DynaKV.SealNext()
		require.NoError(t, err)
	}
	ageOut(t, kv2.DynaKV)
	// Then half the keys rewritten inside the window, in two generations
	for round := 0; round < 2; round++ {
		for i := 0; i < len(keys)/2; i++ {
			values[i] = vr.RandBuff(20, 100)
			_, err = kv2.PutDyna(keys[i], values[i])
			require.NoError(t, err)
		}
		_, err = kv2.DynaKV.SealNext()
		require.NoError(t, err)
	}
	kv2.DynaKV.Mutex.RLock()
	var active []SegmentMeta
	for _, seg := range kv2.DynaKV.active {
		active = append(active, seg.meta)
	}
	kv2.DynaKV.Mutex.RUnlock()
	require.Len(t, active, 2, "two generations inside the window")

	require.NoError(t, kv2.Compress())

	kv2.DynaKV.Mutex.RLock()
	for i, seg := range kv2.DynaKV.active {
		assert.Equal(t, active[i], seg.meta, "a segment inside the window was rewritten")
	}
	kv2.DynaKV.Mutex.RUnlock()
	kv2.DynaKV.History.RLock()
	require.Len(t, kv2.DynaKV.history, 1, "the three aged-out generations compact to one")
	hist := kv2.DynaKV.history[0]
	kv2.DynaKV.History.RUnlock()
	// Every key is still in history -- the rewrites inside the window
	// shadow it but cannot reclaim it until they age out themselves
	assert.Equal(t, int64(len(keys)), hist.count, "history keeps one record per key")

	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d", i)
		assert.Equal(t, values[i], v, "key %d resolved to the wrong generation", i)
	}

	// Once the window has rolled past the rewrites, they reach history
	// and the next compaction reclaims the copies they superseded
	ageOut(t, kv2.DynaKV)
	require.NoError(t, kv2.Compress())
	kv2.DynaKV.History.RLock()
	require.Len(t, kv2.DynaKV.history, 1)
	hist = kv2.DynaKV.history[0]
	kv2.DynaKV.History.RUnlock()
	assert.Equal(t, int64(len(keys)), hist.count)
	assert.Equal(t, hist.count, hist.records, "one record per key: the superseded copies are gone")
	for i := range keys {
		v, err := kv2.Get(keys[i])
		require.NoErrorf(t, err, "key %d", i)
		assert.Equal(t, values[i], v)
	}
	require.NoError(t, kv2.Close())
}
