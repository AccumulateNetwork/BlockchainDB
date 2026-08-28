package blockchainDB

import (
	"encoding/binary"
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

// TestSegmentStoreLiveTailAfterInterruptedSeal
// Regression test: a crash between seal()'s os.Create of the new live
// file and its first flush leaves live.dat existing at 0 bytes.
//
// seal() renames live.dat to seg-N.dat and calls newLiveFile(), which
// os.Create()s a fresh live.dat (0 bytes on disk) and writes the
// 24-byte header into the BFile *buffer*.  A crash in that window
// leaves live.dat existing at 0 bytes.
//
// Abandoning the store without Close reproduces exactly that on-disk
// state, so this needs no SIGKILL.
func TestSegmentStoreLiveTailAfterInterruptedSeal(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	k1, v1 := [32]byte{1}, []byte("first")
	require.NoError(t, store.Put(k1, v1))
	_, err = store.Seal(1)
	require.NoError(t, err)

	// The crash window: live.dat exists, header still only in memory
	fi, err := os.Stat(filepath.Join(dir, segLiveName))
	require.NoError(t, err, "live.dat must exist after a seal")
	require.Equal(t, int64(0), fi.Size(), "live.dat is 0 bytes: header is buffered, not written")

	// Crash here: `store` is abandoned, never closed.

	s2, err := OpenSegmentStore(dir)
	require.NoError(t, err, "reopen must succeed")
	k2, v2 := [32]byte{2}, []byte("written after the crash, then durably closed")
	require.NoError(t, s2.Put(k2, v2))
	require.NoError(t, s2.Close(), "Close is the durability point")

	// k2 was written and Close returned.  The contract says it survives.
	s3, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	got, err := s3.Get(k2)
	require.NoError(t, err, "key lost after a completed Close")
	require.Equal(t, v2, got)

	// k1 was sealed before the crash and must also survive
	got1, err := s3.Get(k1)
	require.NoError(t, err, "sealed key lost")
	require.Equal(t, v1, got1)
	require.NoError(t, s3.Close())
}

// TestSegmentStoreClosedRejectsOperations
// Regression test: Close() drops the sealed segment list but leaves
// the live map populated.  Without a closed guard, a Seal or Compact
// after Close commits a manifest built from no segments at all, and
// the next open deletes every segment that manifest no longer names.
func TestSegmentStoreClosedRejectsOperations(t *testing.T) {
	dir := storeDir(t, "closed")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{57})
	keys := make([][32]byte, 250)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte("v")))
		if (i+1)%50 == 0 {
			_, err = store.SealNext()
			require.NoError(t, err)
		}
	}
	sealed := len(store.segments)
	require.Equal(t, 5, sealed, "expected five sealed segments")
	require.NoError(t, store.Close())

	// Every operation must refuse rather than run against half a store
	_, err = store.Get(keys[0])
	require.ErrorIs(t, err, errStoreClosed, "Get on a closed store")
	require.ErrorIs(t, store.Put(kr.NextHash(), []byte("v")), errStoreClosed, "Put on a closed store")
	_, err = store.Seal(99)
	require.ErrorIs(t, err, errStoreClosed, "Seal on a closed store")
	_, err = store.SealNext()
	require.ErrorIs(t, err, errStoreClosed, "SealNext on a closed store")
	_, err = store.Compact(99)
	require.ErrorIs(t, err, errStoreClosed, "Compact on a closed store")
	_, err = store.CompactNext()
	require.ErrorIs(t, err, errStoreClosed, "CompactNext on a closed store")

	// and nothing they did may have reached the manifest
	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	require.Len(t, reopened.segments, sealed, "sealed segments lost")
	for i, key := range keys {
		got, err := reopened.Get(key)
		require.NoErrorf(t, err, "key %d lost", i)
		require.Equal(t, []byte("v"), got)
	}
	require.NoError(t, reopened.Close())
}

// TestSegmentStoreTornTailIsTruncated
// Regression test: openLive stops replaying at a torn record, but the
// bytes stay on disk unless it also truncates.  Left in place, the
// next append lands after them, so the following open reads the torn
// record's key as a record header and mis-parses everything written
// since -- losing records that a completed Close made durable.
func TestSegmentStoreTornTailIsTruncated(t *testing.T) {
	dir := storeDir(t, "torn")
	store, err := NewSegmentStore(dir, true) // Mutable, as the Dyna layer is
	require.NoError(t, err)

	kr := NewFastRandom([]byte{7})
	before := make([][32]byte, 5)
	for i := range before {
		before[i] = kr.NextHash()
		require.NoError(t, store.Put(before[i], []byte("value-before")))
	}
	require.NoError(t, store.Close())

	// The crash: a record header reached disk, its value bytes did not
	livePath := filepath.Join(dir, segLiveName)
	f, err := os.OpenFile(livePath, os.O_RDWR|os.O_APPEND, 0644)
	require.NoError(t, err)
	var hdr [segRecHdrSize]byte
	tornKey := kr.NextHash()
	copy(hdr[:32], tornKey[:])
	binary.BigEndian.PutUint64(hdr[32:], 500) // Claims 500 value bytes that never arrived
	_, err = f.Write(hdr[:])
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Reopen, write more, and close durably
	s2, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	after := make([][32]byte, 5)
	for i := range after {
		after[i] = kr.NextHash()
		require.NoError(t, s2.Put(after[i], []byte("value-after")))
	}
	require.NoError(t, s2.Close())

	// Everything durable must read back
	s3, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	for i, key := range before {
		got, err := s3.Get(key)
		require.NoErrorf(t, err, "pre-crash key %d lost", i)
		require.Equal(t, []byte("value-before"), got)
	}
	for i, key := range after {
		got, err := s3.Get(key)
		require.NoErrorf(t, err, "post-crash key %d lost", i)
		require.Equal(t, []byte("value-after"), got)
	}
	_, err = s3.Get(tornKey)
	require.Error(t, err, "the torn record must not resolve")
	require.NoError(t, s3.Close())
}

// TestAutoSealDoesNotConsumeBlockHeights
// Regression test for issue #27.  Auto-seals used to allocate
// newest+1 -- the same namespace block boundaries use -- so once a
// shard's live tail had filled more times than the current block
// number, every SealBlock/ExportBlock failed permanently.
func TestAutoSealDoesNotConsumeBlockHeights(t *testing.T) {
	dir := storeDir(t, "autoseal")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	// Eight auto-seals before the first block boundary ever arrives
	kr := NewFastRandom([]byte{41})
	for i := 0; i < 8; i++ {
		for j := 0; j < 5; j++ {
			require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
		}
		_, err = store.SealNext()
		require.NoError(t, err)
	}
	require.Len(t, store.segments, 8)
	for i, seg := range store.segments {
		assert.Equal(t, uint64(0), seg.meta.Height, "auto-seal %d must stay in block 0", i)
		assert.Equal(t, uint64(i), seg.meta.Seq, "auto-seal %d must take the next sequence", i)
	}

	// The first block boundary must still be sealable
	require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
	meta, err := store.Seal(1)
	require.NoError(t, err, "block 1 must be sealable after auto-seals")
	assert.Equal(t, uint64(1), meta.Height)
	assert.Equal(t, uint64(0), meta.Seq, "a new block starts a new sequence")

	// And auto-seals after the boundary belong to the NEXT block, not
	// the one just closed -- they hold writes that arrived after it
	require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
	auto, err := store.SealNext()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), auto.Height, "an auto-seal after block 1 belongs to block 2")

	require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
	meta, err = store.Seal(2)
	require.NoError(t, err, "block 2 must be sealable after an auto-seal inside it")
	assert.Equal(t, uint64(2), meta.Height)
	assert.Equal(t, uint64(1), meta.Seq)
	require.NoError(t, store.Close())
}

// TestAutoSealBlockSurvivesReopen
// The accumulating block is part of the store's durable state: losing
// it on reopen would let a post-restart auto-seal land back in a block
// that was already closed and exported.
func TestAutoSealBlockSurvivesReopen(t *testing.T) {
	dir := storeDir(t, "autoseal-reopen")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{42})
	require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
	_, err = store.Seal(7)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	require.NoError(t, re.Put(kr.NextHash(), []byte("v")))
	auto, err := re.SealNext()
	require.NoError(t, err)
	assert.Equal(t, uint64(8), auto.Height,
		"after reopening a store that closed block 7, writes belong to block 8")
	require.NoError(t, re.Close())
}
