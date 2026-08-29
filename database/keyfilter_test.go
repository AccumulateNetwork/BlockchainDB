package blockchainDB

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The store-level key filter answers "is this key absent?" without
// walking the sealed segments.  Its two failure modes are not
// symmetric: a filter that claims a key it does not have costs a
// pointless walk, while a filter that DENIES a key the store holds
// turns a Get into a wrong answer -- "not found" for a key sitting in
// a segment, with nothing downstream to catch it.
//
// These tests are about that second mode.  Every stage that changes
// what the store holds -- a write, a seal, a compaction, an import, a
// reopen, a reopen after a crash -- must leave every key still
// findable.

// TestKeyFilterNeverMissesAKey
// Walk a store through every stage that moves keys around, checking
// after each one that nothing has become invisible.
func TestKeyFilterNeverMissesAKey(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetSealLimit(64))

	kr := NewFastRandom([]byte{31})
	vr := NewFastRandom([]byte{31, 31})
	keys := make([][32]byte, 0, 600)
	values := make([][]byte, 0, 600)

	mustFindAll := func(s *SegmentStore, stage string) {
		t.Helper()
		for i, key := range keys {
			got, err := s.Get(key)
			require.NoErrorf(t, err, "%s: key %d went missing", stage, i)
			require.Equalf(t, values[i], got, "%s: key %d wrong", stage, i)
		}
	}
	write := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			key, value := kr.NextHash(), vr.RandBuff(20, 60)
			require.NoError(t, store.Put(key, value))
			keys = append(keys, key)
			values = append(values, value)
		}
	}

	// Keys in the live tail
	write(50)
	mustFindAll(store, "live tail")

	// Keys spread over several sealed segments
	for block := uint64(1); block <= 5; block++ {
		write(50)
		_, err = store.Seal(block)
		require.NoError(t, err)
		mustFindAll(store, "after seal")
	}
	require.Greater(t, len(store.segments), 1, "the walk being skipped must be worth skipping")

	// A tail on top of the sealed segments
	write(20)
	mustFindAll(store, "sealed plus tail")

	// Reopened cleanly: the filter is loaded from disk
	require.NoError(t, store.Close())
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	mustFindAll(store, "clean reopen")

	// Reopened after a seal that the saved filter predates, which is
	// what a crash leaves behind: the filter must be rebuilt, not
	// trusted
	write(30)
	_, err = store.Seal(6)
	require.NoError(t, err)
	store2, err := OpenSegmentStore(dir) // Without closing: no save
	require.NoError(t, err)
	mustFindAll(store2, "reopen with a stale saved filter")
	require.NoError(t, store2.Close())
	require.NoError(t, store.Close())
}

// TestKeyFilterRebuildsFromADamagedFile
// A saved filter that cannot be read is a reason to rebuild, not a
// reason to fail the open or to serve lookups from a half-read filter.
func TestKeyFilterRebuildsFromADamagedFile(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{32})
	var keys [][32]byte
	for i := 0; i < 200; i++ {
		key := kr.NextHash()
		require.NoError(t, store.Put(key, []byte("v")))
		keys = append(keys, key)
	}
	_, err = store.Seal(1)
	require.NoError(t, err)
	require.NoError(t, store.Close()) // Saves bloom.dat and claims coverage

	path := filepath.Join(dir, bloomFilename)
	require.FileExists(t, path, "Close must persist the filter")
	require.NoError(t, os.WriteFile(path, []byte("not a bloom filter"), 0644))

	store, err = OpenSegmentStore(dir)
	require.NoError(t, err, "a damaged filter must not fail the open")
	for i, key := range keys {
		_, err := store.Get(key)
		require.NoErrorf(t, err, "key %d went missing after a damaged filter was rebuilt", i)
	}
	require.NoError(t, store.Close())
}

// TestKeyFilterSurvivesCompaction
// Compaction rewrites the whole key set of a mutable store.  What
// survives must still be findable; what it reclaimed may linger in the
// filter, which costs a walk and nothing else.
func TestKeyFilterSurvivesCompaction(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{33})
	keys := make([][32]byte, 100)
	for i := range keys {
		keys[i] = kr.NextHash()
	}
	for round := 0; round < 4; round++ {
		for i, key := range keys {
			require.NoError(t, store.Put(key, []byte{byte(round), byte(i)}))
		}
		_, err = store.SealNext()
		require.NoError(t, err)
	}
	_, err = store.CompactNext()
	require.NoError(t, err)

	for i, key := range keys {
		got, err := store.Get(key)
		require.NoErrorf(t, err, "key %d went missing in compaction", i)
		require.Equal(t, []byte{3, byte(i)}, got, "key %d is stale", i)
	}

	// And a key written after the compaction
	fresh := kr.NextHash()
	require.NoError(t, store.Put(fresh, []byte("after")))
	got, err := store.Get(fresh)
	require.NoError(t, err, "a key written after compaction went missing")
	require.Equal(t, []byte("after"), got)
	require.NoError(t, store.Close())
}

// TestKeyFilterSurvivesImport
// An adopted segment's keys are keys the store now holds, so the
// filter has to learn them at adoption -- a syncing node's whole
// database arrives this way, never through Put.
func TestKeyFilterSurvivesImport(t *testing.T) {
	srcDir, dstDir := storeDir(t, "src"), storeDir(t, "dst")
	src, err := NewSegmentStore(srcDir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{34})
	var keys [][32]byte
	for i := 0; i < 150; i++ {
		key := kr.NextHash()
		require.NoError(t, src.Put(key, []byte("v")))
		keys = append(keys, key)
	}
	_, err = src.Seal(1)
	require.NoError(t, err)
	metas, paths := src.SegmentPaths()

	dst, err := NewSegmentStore(dstDir, false)
	require.NoError(t, err)
	for i, meta := range metas {
		require.NoError(t, dst.ImportSegmentFile(paths[i], meta))
	}
	for i, key := range keys {
		_, err := dst.Get(key)
		require.NoErrorf(t, err, "imported key %d is invisible", i)
	}
	require.NoError(t, src.Close())
	require.NoError(t, dst.Close())
}

// TestPutIfAbsent
// The three answers the layered lookup above it needs, from one pass:
// absent (and now written), present with the same value, present with
// a different one.
func TestPutIfAbsent(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{35})
	key := kr.NextHash()

	existing, existed, err := store.PutIfAbsent(key, []byte("first"))
	require.NoError(t, err)
	require.False(t, existed, "a key never written must report absent")
	require.Nil(t, existing)
	got, err := store.Get(key)
	require.NoError(t, err, "an absent key must have been written")
	require.Equal(t, []byte("first"), got)

	existing, existed, err = store.PutIfAbsent(key, []byte("first"))
	require.NoError(t, err)
	require.True(t, existed)
	require.Equal(t, []byte("first"), existing)

	// A conflicting value is reported, not written and not an error:
	// deciding what a conflict means belongs to the caller, which is
	// the layer that can move the key somewhere mutable
	existing, existed, err = store.PutIfAbsent(key, []byte("second"))
	require.NoError(t, err)
	require.True(t, existed)
	require.Equal(t, []byte("first"), existing, "the stored value must be reported")
	got, err = store.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), got, "PutIfAbsent must not overwrite")

	// The same across a seal, where the key is no longer in the tail
	_, err = store.Seal(1)
	require.NoError(t, err)
	existing, existed, err = store.PutIfAbsent(key, []byte("third"))
	require.NoError(t, err)
	require.True(t, existed, "a sealed key is still present")
	require.Equal(t, []byte("first"), existing)
	require.NoError(t, store.Close())
}

// TestStoreStatsCountWhatHappened
// The counters exist to answer "are duplicate permanent writes common
// enough to be worth checking for?", so they have to distinguish the
// three outcomes of a write and the two ways a lookup can be settled.
func TestStoreStatsCountWhatHappened(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{36})
	keys := make([][32]byte, 10)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte("v")))
	}
	_, err = store.Seal(1) // Push them out of the live tail
	require.NoError(t, err)

	for _, key := range keys[:4] { // Identical rewrites: avoidable writes
		require.NoError(t, store.Put(key, []byte("v")))
	}
	require.Error(t, store.Put(keys[0], []byte("different"))) // A conflict
	for i := 0; i < 20; i++ {                                 // Keys never written
		_, err = store.Get(kr.NextHash())
		require.Error(t, err)
	}

	s := store.Stats()
	require.Equal(t, uint64(15), s.PutTotal, "10 new, 4 duplicates, 1 conflict")
	require.Equal(t, uint64(10), s.PutNew)
	require.Equal(t, uint64(4), s.PutDuplicate)
	require.Equal(t, uint64(1), s.PutConflict)

	// The 20 absent keys must have been settled by the filter, not by
	// walking: that is the whole point of it
	require.GreaterOrEqual(t, s.FilterAbsent, uint64(19),
		"absent keys should be settled without touching a segment")
	require.Equal(t, s.FilterMisled, s.FilterWalked-uint64(5),
		"the only walks that found something were the 4 duplicates and the conflict")
	require.NoError(t, store.Close())
}

// TestKeyFilterEmptyDoesNotCoverSegmentZero
// A saved filter that covered NO segments must not be mistaken for one
// covering the first segment a store seals.
//
// Both record (0, 0) as the newest segment they cover -- the empty one
// because that is the zero value, the other because (0, 0) is a real
// identity, and the Dyna layer numbers every segment at height 0 so its
// first segment is exactly that.  A crash that leaves a sealed segment
// for recoverOrphans to adopt without rewriting the manifest produces
// the pair: the manifest still claims a valid filter covering "the
// newest segment (0,0)", and the segment now present is (0,0).
//
// Accepting the empty filter there is silent data loss, not a slow
// lookup: the filter answers "definitely absent" for every key in the
// segment, and Get returns not-found without ever opening it.  Found
// intermittently by TestCrashRecoverySeal.
func TestKeyFilterEmptyDoesNotCoverSegmentZero(t *testing.T) {
	dir := storeDir(t, "bloom0")

	// A store closed with no segments: bloom.dat covers nothing, and the
	// manifest says so with (0, 0) because that is the zero value
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	saved, err := os.ReadFile(filepath.Join(dir, segManifestName))
	require.NoError(t, err)
	var m StoreManifest
	require.NoError(t, json.Unmarshal(saved, &m))
	require.True(t, m.BloomValid, "the close must have left a coverage claim to test")
	require.Empty(t, m.Segments)

	// Fill a tail and seal it, which produces segment (0, 0)
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{61})
	keys := make([][32]byte, 40)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte(fmt.Sprintf("v%d", i))))
	}
	_, err = store.SealNext()
	require.NoError(t, err)
	require.Len(t, store.segments, 1)
	require.Equal(t, uint64(0), store.segments[0].meta.Height)
	require.Equal(t, uint64(0), store.segments[0].meta.Seq, "the case only arises for segment (0,0)")

	// Now put the manifest back as it was before the seal: this is a
	// crash after the segment file reached disk but before the manifest
	// commit, which is the window recoverOrphans exists for
	require.NoError(t, os.WriteFile(filepath.Join(dir, segManifestName), saved, 0644))

	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	require.Len(t, reopened.segments, 1, "the orphan segment must have been adopted")

	for i, key := range keys {
		v, err := reopened.Get(key)
		require.NoErrorf(t, err, "key %d reported absent by the filter but held by segment (0,0)", i)
		require.Equal(t, fmt.Sprintf("v%d", i), string(v))
	}
	require.NoError(t, reopened.Close())
}
