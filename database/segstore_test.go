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

// storeDir makes a scratch directory for a store test
func storeDir(t *testing.T, name string) string {
	t.Helper()
	dir := filepath.Join(os.TempDir(), t.Name()+"_"+name)
	os.RemoveAll(dir)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestSegmentStoreBasics
// Writes are readable before and after sealing, and survive a reopen
// from both the live tail and the sealed segments.
func TestSegmentStoreBasics(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{101})
	vr := NewFastRandom([]byte{101, 101})
	keys := make([][32]byte, 300)
	values := make([][]byte, 300)
	for i := range keys {
		keys[i] = kr.NextHash()
		values[i] = vr.RandBuff(20, 200)
		require.NoError(t, store.Put(keys[i], values[i]))
	}

	// Readable from the live tail
	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "live key %d", i)
		assert.Equal(t, values[i], v)
	}

	// Seal the first 300, write 200 more, then seal again
	_, err = store.Seal(1)
	require.NoError(t, err, "seal 1")
	for i := 0; i < 200; i++ {
		key := kr.NextHash()
		value := vr.RandBuff(20, 200)
		require.NoError(t, store.Put(key, value))
		keys = append(keys, key)
		values = append(values, value)
	}
	_, err = store.Seal(2)
	require.NoError(t, err, "seal 2")

	// Everything readable across two segments
	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "sealed key %d", i)
		assert.Equal(t, values[i], v)
	}

	// Unsealed writes live in the tail
	tailKey := kr.NextHash()
	require.NoError(t, store.Put(tailKey, []byte("in the tail")))
	require.NoError(t, store.Close())

	// Reopen: sealed segments and the unsealed tail both come back
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err, "reopen")
	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "key %d after reopen", i)
		assert.Equal(t, values[i], v)
	}
	v, err := store.Get(tailKey)
	require.NoError(t, err, "tail key after reopen")
	assert.Equal(t, []byte("in the tail"), v)
	require.NoError(t, store.Close())
}

// TestSegmentStoreImmutability
// An immutable store treats an identical rewrite as a no-op (replay
// safety) and rejects a conflicting value, across the tail and sealed
// segments alike.
func TestSegmentStoreImmutability(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{102})
	key := kr.NextHash()
	require.NoError(t, store.Put(key, []byte("original")))

	require.NoError(t, store.Put(key, []byte("original")), "identical rewrite must be a no-op")
	require.Error(t, store.Put(key, []byte("different")), "conflicting value must fail")

	_, err = store.Seal(1)
	require.NoError(t, err)

	require.NoError(t, store.Put(key, []byte("original")), "identical rewrite of a sealed key must be a no-op")
	require.Error(t, store.Put(key, []byte("different")), "conflicting value with a sealed key must fail")

	v, err := store.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("original"), v)
	require.NoError(t, store.Close())
}

// TestSegmentStoreMutableShadowing
// In a mutable store a newer segment shadows an older one, and the
// live tail shadows every segment.
func TestSegmentStoreMutableShadowing(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{103})
	keys := make([][32]byte, 50)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte(fmt.Sprintf("v1-%d", i))))
	}
	_, err = store.Seal(1)
	require.NoError(t, err)

	// Overwrite half, seal again
	for i := 0; i < 25; i++ {
		require.NoError(t, store.Put(keys[i], []byte(fmt.Sprintf("v2-%d", i))))
	}
	_, err = store.Seal(2)
	require.NoError(t, err)

	// Overwrite ten more, leave them in the tail
	for i := 0; i < 10; i++ {
		require.NoError(t, store.Put(keys[i], []byte(fmt.Sprintf("v3-%d", i))))
	}

	check := func(stage string) {
		for i := range keys {
			v, err := store.Get(keys[i])
			require.NoErrorf(t, err, "%s: key %d", stage, i)
			switch {
			case i < 10:
				assert.Equalf(t, fmt.Sprintf("v3-%d", i), string(v), "%s: key %d", stage, i)
			case i < 25:
				assert.Equalf(t, fmt.Sprintf("v2-%d", i), string(v), "%s: key %d", stage, i)
			default:
				assert.Equalf(t, fmt.Sprintf("v1-%d", i), string(v), "%s: key %d", stage, i)
			}
		}
	}
	check("before reopen")
	require.NoError(t, store.Close())
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	check("after reopen")
	require.NoError(t, store.Close())
}

// TestSegmentStoreCompact
// Compaction replaces every segment with one generation holding only
// live keys: space is reclaimed, values are unchanged, and the commit
// is a single manifest replacement (issue #19).
func TestSegmentStoreCompact(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{104})
	vr := NewFastRandom([]byte{104, 104})
	keys := make([][32]byte, 200)
	for i := range keys {
		keys[i] = kr.NextHash()
	}
	// Ten generations of overwrites, sealed one per round: ~90% of the
	// sealed bytes end up shadowed
	latest := make([][]byte, 200)
	for round := 1; round <= 10; round++ {
		for i := range keys {
			latest[i] = vr.RandBuff(50, 200)
			require.NoError(t, store.Put(keys[i], latest[i]))
		}
		_, err = store.Seal(uint64(round))
		require.NoError(t, err)
	}

	sizeBefore := dirSize(t, dir)
	require.Len(t, store.segments, 10, "should have ten sealed segments")

	meta, err := store.Compact(11)
	require.NoError(t, err, "compact")
	assert.Equal(t, uint64(200), meta.Count, "compacted generation should hold the live keys")
	require.Len(t, store.segments, 1, "compaction should leave one generation")

	sizeAfter := dirSize(t, dir)
	assert.Lessf(t, sizeAfter, sizeBefore/5, "space not reclaimed (%d -> %d)", sizeBefore, sizeAfter)

	// Values intact, before and after a reopen
	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "key %d after compact", i)
		assert.Equal(t, latest[i], v)
	}
	require.NoError(t, store.Close())
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "key %d after compact+reopen", i)
		assert.Equal(t, latest[i], v)
	}
	require.NoError(t, store.Close())
}

// TestSegmentStoreSyncIsFileCopy
// A peer syncs by copying sealed files and committing a manifest
// update -- no record is re-inserted.  The imported store must hold
// byte-identical data, re-imports must be no-ops, and a tampered file
// must be rejected.
func TestSegmentStoreSyncIsFileCopy(t *testing.T) {
	srcDir := storeDir(t, "src")
	dstDir := storeDir(t, "dst")

	src, err := NewSegmentStore(srcDir, false)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{105})
	vr := NewFastRandom([]byte{105, 105})
	var keys [][32]byte
	var values [][]byte
	for block := uint64(1); block <= 3; block++ {
		for i := 0; i < 200; i++ {
			key := kr.NextHash()
			value := vr.RandBuff(20, 200)
			require.NoError(t, src.Put(key, value))
			keys = append(keys, key)
			values = append(values, value)
		}
		_, err = src.Seal(block)
		require.NoError(t, err)
	}
	metas, paths := src.SegmentPaths()
	require.Len(t, metas, 3)

	// Sync: copy each sealed file, import it (verify + adopt)
	dst, err := NewSegmentStore(dstDir, false)
	require.NoError(t, err)
	for i, meta := range metas {
		require.NoError(t, dst.ImportSegmentFile(paths[i], meta), "import segment %d", i)
	}
	for i := range keys {
		v, err := dst.Get(keys[i])
		require.NoErrorf(t, err, "key %d missing on the synced store", i)
		assert.Equal(t, values[i], v)
	}

	// The imported files are byte-identical to the source's
	for i, meta := range metas {
		want, err := os.ReadFile(paths[i])
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(dstDir, meta.File))
		require.NoError(t, err)
		assert.Equalf(t, want, got, "segment %d differs after sync", i)
	}

	// Re-import is a no-op (interrupted syncs resume)
	require.NoError(t, dst.ImportSegmentFile(paths[2], metas[2]))
	require.Len(t, dst.segments, 3)

	// A tampered file is rejected
	tampered := filepath.Join(os.TempDir(), t.Name()+"_tampered.dat")
	data, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	data[len(data)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(tampered, data, 0644))
	defer os.Remove(tampered)
	bad := metas[0]
	bad.Height = 99 // Above the newest, so only the hash can reject it
	require.Error(t, dst.ImportSegmentFile(tampered, bad), "tampered segment must be rejected")

	require.NoError(t, src.Close())
	require.NoError(t, dst.Close())
}

// TestSegmentStoreRecovery
// A seal that reached disk but not the manifest is adopted on open;
// files a committed compaction superseded are swept.
func TestSegmentStoreRecovery(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{106})
	vr := NewFastRandom([]byte{106, 106})
	keys := make([][32]byte, 100)
	values := make([][]byte, 100)
	for i := range keys {
		keys[i] = kr.NextHash()
		values[i] = vr.RandBuff(20, 100)
		require.NoError(t, store.Put(keys[i], values[i]))
	}
	_, err = store.Seal(1)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// Simulate a crash between the segment rename and the manifest
	// commit: the data file is on disk, but the manifest predates it
	// (and its index was never built)
	m, err := (&SegmentStore{Directory: dir}).readManifest()
	require.NoError(t, err)
	require.Len(t, m.Segments, 1)
	empty := &SegmentStore{Directory: dir, Mutable: m.Mutable}
	require.NoError(t, empty.writeManifest()) // A manifest naming no segments
	indexName := strings.TrimSuffix(m.Segments[0].File, segDataSuffix) + segIndexSuffix
	require.NoError(t, os.Remove(filepath.Join(dir, indexName)))

	// Open must adopt the orphaned segment (and rebuild its index)
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err, "open must recover the orphaned segment")
	require.Len(t, store.segments, 1, "orphaned segment should be adopted")
	for i := range keys {
		v, err := store.Get(keys[i])
		require.NoErrorf(t, err, "key %d lost in recovery", i)
		assert.Equal(t, values[i], v)
	}
	require.NoError(t, store.Close())

	// The adoption was committed to the manifest
	m2, err := (&SegmentStore{Directory: dir}).readManifest()
	require.NoError(t, err)
	require.Len(t, m2.Segments, 1)
}

// dirSize totals the bytes of files in a directory
func dirSize(t *testing.T, dir string) (total int64) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		info, err := e.Info()
		require.NoError(t, err)
		total += info.Size()
	}
	return total
}

// TestSegmentStoreGenesisHeight
// Height 0 is a legitimate first height (a genesis block), not a
// sentinel for "no segments".
func TestSegmentStoreGenesisHeight(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{107})
	key := kr.NextHash()
	require.NoError(t, store.Put(key, []byte("genesis")))
	meta, err := store.Seal(0)
	require.NoError(t, err, "sealing at height 0 must work")
	assert.Equal(t, uint64(0), meta.Height)

	// And a second seal at height 0 must be rejected
	require.NoError(t, store.Put(kr.NextHash(), []byte("next")))
	_, err = store.Seal(0)
	require.Error(t, err, "re-using a height must be rejected")

	require.NoError(t, store.Close())
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	v, err := store.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("genesis"), v)
	require.NoError(t, store.Close())
}
