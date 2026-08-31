package blockchainDB

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	require.Greater(t, len(store.sealedSegments()), 1, "the walk being skipped must be worth skipping")

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

	path := filepath.Join(dir, filtersFilename)
	require.FileExists(t, path, "Close must persist the filters")
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
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

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
	ageOut(t, store)
	compacted, err := store.CompactHistory()
	require.NoError(t, err)
	require.True(t, compacted)

	for i, key := range keys {
		got, err := store.GetDeep(key)
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
// intermittently by TestCrashRecoverySeal (issue #35).  The claim is
// now a block range with a segment count (filterClaim), and this is
// the case the count exists for.
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
	require.True(t, m.FilterValid, "the close must have left a coverage claim to test")
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
	require.Len(t, store.sealedSegments(), 1)
	require.Equal(t, uint64(0), store.sealedSegments()[0].meta.Height)
	require.Equal(t, uint64(0), store.sealedSegments()[0].meta.Seq, "the case only arises for segment (0,0)")

	// Now put the manifest back as it was before the seal: this is a
	// crash after the segment file reached disk but before the manifest
	// commit, which is the window recoverOrphans exists for
	require.NoError(t, os.WriteFile(filepath.Join(dir, segManifestName), saved, 0644))

	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	require.Len(t, reopened.sealedSegments(), 1, "the orphan segment must have been adopted")

	for i, key := range keys {
		v, err := reopened.Get(key)
		require.NoErrorf(t, err, "key %d reported absent by the filter but held by segment (0,0)", i)
		require.Equal(t, fmt.Sprintf("v%d", i), string(v))
	}
	require.NoError(t, reopened.Close())
}

// The rolling window (issue #44).  A filter covers 2N blocks and a
// fresh one starts every N, so what is resident is the keys of the last
// N to 2N blocks and no more; what a write is checked against is the
// same window; and what a read can reach is everything.

// rollingStore is an immutable store rolling every MinFilterBlocks
// blocks, with `blocks` sealed blocks of `perBlock` keys each, so that
// the window has rolled several times.  Returns the keys by block.
func rollingStore(t *testing.T, dir string, seed byte, blocks, perBlock int) (store *SegmentStore, keys [][][32]byte) {
	t.Helper()
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))
	kr := NewFastRandom([]byte{seed})
	keys = make([][][32]byte, blocks+1) // keys[h] were written in block h
	for h := 1; h <= blocks; h++ {
		for i := 0; i < perBlock; i++ {
			key := kr.NextHash()
			require.NoError(t, store.Put(key, []byte(fmt.Sprintf("b%d-%d", h, i))))
			keys[h] = append(keys[h], key)
		}
		_, err = store.Seal(uint64(h))
		require.NoError(t, err)
	}
	return store, keys
}

// filterBytes is what the live filters hold in memory
func filterBytes(s *SegmentStore) (n uint64) {
	for _, f := range s.filters {
		for _, l := range f.keys.Layers {
			n += l.NumBytes
		}
	}
	return n
}

// TestKeyFilterRollsWithTheBlock
// Two filters at most, starting where the schedule says, holding the
// keys of the last N to 2N blocks; and every key ever written is still
// readable, whether the window covers it or not.
func TestKeyFilterRollsWithTheBlock(t *testing.T) {
	dir := storeDir(t, "roll")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))
	const n = MinFilterBlocks

	kr := NewFastRandom([]byte{62})
	var all [][32]byte
	var sizes []uint64
	for h := uint64(1); h <= 5*n; h++ {
		for i := 0; i < 10; i++ {
			key := kr.NextHash()
			require.NoError(t, store.Put(key, []byte("v")))
			all = append(all, key)
		}
		_, err = store.Seal(h)
		require.NoError(t, err)

		// The schedule: the filter that began at the last multiple of N
		// and the one before it, and no other
		want := filterStarts(h+1, n)
		require.Equal(t, want, filterStartsOf(store), "after sealing block %d", h)
		require.LessOrEqual(t, len(store.filters), 2)
		if h%n == 0 {
			sizes = append(sizes, filterBytes(store))
		}
	}
	// Bounded: the filters at the fifth roll are no bigger than at the
	// second, because each holds the keys of at most 2N blocks
	require.Equal(t, sizes[1], sizes[len(sizes)-1],
		"filter memory must not grow with the chain: %v", sizes)

	// Everything is still readable below the window -- through the
	// explicit deep read, which is what reaches past the window
	for i, key := range all {
		_, err := store.GetDeep(key)
		require.NoErrorf(t, err, "key %d went missing (block %d)", i, i/10+1)
	}
	// The protocol read stops at the window: the filters settle a key
	// below it as absent, and no history segment is probed (spec 1.3)
	before := store.Stats()
	_, err = store.Get(all[0])
	require.ErrorIs(t, err, errNotFound,
		"outside the window the protocol read assumes absent")
	after := store.Stats()
	require.Equal(t, before.FilterAbsent+1, after.FilterAbsent,
		"a key below the window is outside what the filters cover")
	require.Equal(t, before.FilterWalked, after.FilterWalked,
		"and it costs no walk at all")
	require.NoError(t, store.Close())
}

func filterStartsOf(s *SegmentStore) (starts []uint64) {
	for _, f := range s.filters {
		starts = append(starts, f.start)
	}
	return starts
}

// TestKeyFilterCoversAMergedSegmentByItsOldestBlock
// A merged segment reaches back over every block in its run, and the
// filters must not claim it unless they saw its oldest keys.
//
// After sealing 60 blocks with N=20 the filters cover blocks 40 on.
// Merging the blocks below 50 merges the history below the window --
// blocks 1-39 -- into one segment at height 39 reaching back to block
// 1.  A filter that judged the segment by its height alone would
// call it covered, answer "absent" for a key from block 10, and skip
// the one segment that holds it: a false negative, with nothing
// downstream to catch it.  Without SegmentMeta.Span this fails with
// "key from block 1 went missing after the merge".  (With the tiers
// of issue #57 a merged segment is history and never covered; the
// span is still what says so on a reopen, when the tiers are placed
// by each segment's oldest block.)
func TestKeyFilterCoversAMergedSegmentByItsOldestBlock(t *testing.T) {
	dir := storeDir(t, "span")
	store, keys := rollingStore(t, dir, 63, 60, 5)
	require.Equal(t, []uint64{40, 60}, filterStartsOf(store))

	_, merged, err := store.MergeBelow(50)
	require.NoError(t, err)
	require.True(t, merged)
	require.Equal(t, uint64(39), store.sealedSegments()[0].meta.Height)
	require.Equal(t, uint64(1), store.sealedSegments()[0].meta.first(), "the merged segment reaches back to block 1")

	find := func(s *SegmentStore, when string) {
		t.Helper()
		for h := 1; h < len(keys); h++ {
			for _, key := range keys[h] {
				_, err := s.GetDeep(key)
				require.NoErrorf(t, err, "key from block %d went missing %s", h, when)
			}
		}
	}
	find(store, "after the merge")

	// The span is in the manifest, so a reopen -- with the filters
	// loaded, and again with them rebuilt -- keeps the same answer
	require.NoError(t, store.Close())
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	find(store, "after a clean reopen")
	require.NoError(t, os.Remove(filepath.Join(dir, filtersFilename)))
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	find(store, "after a reopen that rebuilt the filters")
	require.NoError(t, store.Close())
}

// TestImmutabilityIsWindowed
// What the immutability check promises, exactly: a key written in the
// last N to 2N blocks cannot be overwritten, wherever it now sits; a
// key older than that is not consulted.  The same for a segment a peer
// sends (checkNoConflicts).
func TestImmutabilityIsWindowed(t *testing.T) {
	dir := storeDir(t, "window")
	store, keys := rollingStore(t, dir, 64, 60, 5)
	require.Equal(t, []uint64{40, 60}, filterStartsOf(store), "the window begins at block 40")

	// In the window, in a sealed block: refused
	inWindow := keys[45][0]
	require.ErrorIs(t, store.Put(inWindow, []byte("other")), ErrImmutable)
	require.NoError(t, store.Put(inWindow, []byte("b45-0")), "an identical rewrite is a no-op")
	_, existed, err := store.PutIfAbsent(inWindow, []byte("other"))
	require.NoError(t, err)
	require.True(t, existed)

	// In the window, but in a segment that a merge has stretched back
	// below it: still refused, because the filters hold the key and a
	// hit is followed wherever it leads
	_, merged, err := store.MergeBelow(50)
	require.NoError(t, err)
	require.True(t, merged)
	require.ErrorIs(t, store.Put(inWindow, []byte("other")), ErrImmutable)

	// Below the window: not consulted.  The write is accepted, and the
	// newest record is what a read returns
	old := keys[10][0]
	require.NoError(t, store.Put(old, []byte("rewritten")),
		"a key older than the window is not checked")
	got, err := store.Get(old)
	require.NoError(t, err)
	require.Equal(t, []byte("rewritten"), got)
	require.NoError(t, store.Close())

	// A peer's segment holding a different value for a key: refused
	// when the key is in the window, adopted when it is older
	peerDir := storeDir(t, "peer")
	peer, err := NewSegmentStore(peerDir, false)
	require.NoError(t, err)
	require.NoError(t, peer.SetFilterBlocks(MinFilterBlocks))
	require.NoError(t, peer.Put(keys[10][1], []byte("diverged")))
	require.NoError(t, peer.Put(NewFastRandom([]byte{65}).NextHash(), []byte("new")))
	oldOnly, err := peer.Seal(61)
	require.NoError(t, err)
	require.NoError(t, peer.Put(keys[45][1], []byte("diverged")))
	inWindowToo, err := peer.Seal(62)
	require.NoError(t, err)
	_, paths := peer.SegmentPaths()

	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	require.NoError(t, store.ImportSegmentFile(paths[0], oldOnly),
		"a conflict older than the window is not detected")
	err = store.ImportSegmentFile(paths[1], inWindowToo)
	require.Error(t, err, "a conflict inside the window is")
	require.Contains(t, err.Error(), "conflicts")
	require.NoError(t, store.Close())
	require.NoError(t, peer.Close())
}

// TestKeyFilterFollowsABlockJump
// A shard reopened after a quiet spell learns the block the set has
// reached, which can be many rolls past the block its own manifest
// recorded.  The filters must roll to match, and the tail's keys must
// be in the filters that result: the tail is sealed into the block the
// store is now in, and a rolled-in filter that lacked it would let a
// key written inside the window be rewritten.
func TestKeyFilterFollowsABlockJump(t *testing.T) {
	dir := storeDir(t, "jump")
	store, keys := rollingStore(t, dir, 66, 5, 5)
	kr := NewFastRandom([]byte{67})
	tail := kr.NextHash()
	require.NoError(t, store.Put(tail, []byte("in the tail")))
	require.NoError(t, store.Close())

	store, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, filterStartsOf(store), "block 6 is in the first span")
	store.AdvanceBlock(100) // What adoptBlockHeight does
	require.Equal(t, []uint64{80, 100}, filterStartsOf(store))

	_, err = store.Seal(100)
	require.NoError(t, err)
	require.ErrorIs(t, store.Put(tail, []byte("rewritten")), ErrImmutable,
		"the tail's key was sealed into block 100, inside the window")
	for h := 1; h < len(keys); h++ {
		for _, key := range keys[h] {
			_, err := store.GetDeep(key)
			require.NoErrorf(t, err, "key from block %d went missing after the jump", h)
		}
	}
	require.NoError(t, store.Close())
}

// TestFilterBlocksIsValidatedAndPersisted
// The roll period is the reach of the immutability check, so a store
// carries it, refuses one it cannot honour, and refuses to open on a
// manifest that lost it (TestManifestVersionIsWrittenAndChecked).
func TestFilterBlocksIsValidatedAndPersisted(t *testing.T) {
	dir := storeDir(t, "fb")
	kv2, err := NewKV2(dir, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(DefaultFilterBlocks), kv2.PermKV.FilterBlocks)

	err = kv2.SetFilterBlocks(MinFilterBlocks - 1)
	require.Error(t, err, "a window shorter than healing must be refused")
	require.Contains(t, err.Error(), "minimum")
	require.Equal(t, uint64(DefaultFilterBlocks), kv2.PermKV.FilterBlocks, "a refused value must not stick")

	require.NoError(t, kv2.SetFilterBlocks(MinFilterBlocks+5))
	require.NoError(t, kv2.Close())

	reopened, err := OpenKV2(dir)
	require.NoError(t, err)
	require.NoError(t, reopened.Open())
	require.Equal(t, uint64(MinFilterBlocks+5), reopened.PermKV.FilterBlocks, "the period must survive a reopen")
	require.NoError(t, reopened.Close())
}

// TestResidentFiltersFollowTheWindow
// A segment's bloom is worth memory only while the segment is in the
// window a protocol read walks.  Holding every filter resident cost
// 1.5 bytes per key for every key the store had ever held: memory
// growing with the age of the chain, scanned by every GC, and read in
// full at open, so opening got slower forever (issue #64).  History is
// probed on disk instead -- the same bits, K one-byte reads -- so what
// the store holds follows the working set, not the chain (spec 1.2).
func TestResidentFiltersFollowTheWindow(t *testing.T) {
	dir := storeDir(t, "residency")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{179})
	var keys [][32]byte
	const blocks = 6 * MinFilterBlocks
	for b := uint64(1); b <= blocks; b++ {
		for i := 0; i < 5; i++ {
			k := kr.NextHash()
			keys = append(keys, k)
			require.NoError(t, store.Put(k, []byte{byte(b), byte(i)}))
		}
		_, err = store.Seal(b)
		require.NoError(t, err)
	}

	resident := func(segs []*segment) (n int) {
		for _, seg := range segs {
			if seg.bloom != nil {
				n++
			}
		}
		return n
	}
	store.History.RLock()
	history := append([]*segment(nil), store.history...)
	store.History.RUnlock()
	store.Mutex.RLock()
	active := append([]*segment(nil), store.active...)
	store.Mutex.RUnlock()

	require.NotEmpty(t, history, "the window must have rolled past something")
	require.Equal(t, 0, resident(history),
		"history holds %d resident filters; they belong on disk", resident(history))
	require.Equal(t, len(active), resident(active),
		"the window's filters are the ones worth memory")

	// Cold filters still answer, and answer correctly: every key is
	// found through the history walk, and unwritten keys are not
	for i, k := range keys {
		v, err := store.GetDeep(k)
		require.NoErrorf(t, err, "key %d unreachable with cold filters", i)
		require.Equal(t, []byte{byte(i/5 + 1), byte(i % 5)}, v)
	}
	for i := 0; i < 200; i++ {
		_, err := store.GetDeep(kr.NextHash())
		require.ErrorIsf(t, err, errNotFound, "unwritten key %d", i)
	}

	// A cold probe agrees with the resident one, bit for bit: load a
	// history segment's filter and compare the two answers
	seg := history[0]
	require.Nil(t, seg.bloom)
	var cold []bool
	for _, k := range keys {
		mightBe, err := seg.bloomTest(k)
		require.NoError(t, err)
		cold = append(cold, mightBe)
	}
	require.NoError(t, seg.loadBloom())
	require.NotNil(t, seg.bloom, "the filter must be loadable when it is wanted")
	for i, k := range keys {
		require.Equalf(t, cold[i], seg.bloom.Test(k), "key %d: cold and resident disagree", i)
	}
	seg.freeBloom()

	// And an open reads no filter it does not need
	require.NoError(t, store.Close())
	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer reopened.Close()
	reopened.History.RLock()
	rh := resident(reopened.history)
	nh := len(reopened.history)
	reopened.History.RUnlock()
	require.Equalf(t, 0, rh, "reopen materialised %d of %d history filters", rh, nh)
}

// TestFilterSizingFollowsRecentDemand
// A filter's size decides both what it costs and what it is worth: too
// small and it fills past its design point, so every false positive
// becomes a segment walk that finds nothing; too large and the bits
// are memory spent on nothing, doubled, because two filters are live
// at once.  Keys per span are not a constant -- they follow the
// transaction rate -- so each roll sizes the filter it starts from
// what spans actually took recently (issue #54).
func TestFilterSizingFollowsRecentDemand(t *testing.T) {
	dir := storeDir(t, "demand")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	now := time.Now()
	store.Mutex.Lock()
	// Yesterday's peak, outside the window, must not size anything
	store.recordSpanDemand(9_000_000, now.Add(-(FilterDemandHours+2)*time.Hour))
	// and this hour's demand must
	store.recordSpanDemand(1000, now.Add(-2*time.Hour))
	store.recordSpanDemand(4000, now.Add(-time.Hour))
	store.recordSpanDemand(2000, now)
	peak, ok := store.spanDemandPeak(now)
	want := store.filterCapacity(nil, now)
	store.Mutex.Unlock()

	require.True(t, ok)
	require.Equal(t, uint64(4000), peak, "the peak is the largest span inside the window")
	require.Equal(t, uint64(4000*FilterHeadroomPercent/100), want,
		"a filter is sized for the recent peak plus headroom")

	// The ceiling holds: a wild span cannot ask for unbounded memory
	store.Mutex.Lock()
	store.recordSpanDemand(FilterCapacityMax*100, now)
	capped := store.filterCapacity(nil, now)
	store.Mutex.Unlock()
	require.Equal(t, FilterCapacityMax, capped, "demand is bounded above")

	// And a filter is never sized under what a live span already holds
	store.Mutex.Lock()
	store.demand = nil
	live := []*keyFilter{{start: 0, keys: NewBloomSet(10, 3)}}
	for i := 0; i < 500; i++ {
		live[0].keys.Set(NewFastRandom([]byte{byte(i)}).NextHash())
	}
	floor := store.filterCapacity(live, now)
	store.Mutex.Unlock()
	require.GreaterOrEqual(t, floor, uint64(500),
		"a filter cannot be sized under what its own window has taken")
}

// TestFilterDemandSurvivesAReopen
// The measurement is only useful if a restart keeps it: a store that
// forgot yesterday's demand would size its first filters from a guess,
// which is what dynamic sizing exists to stop (issue #54).
func TestFilterDemandSurvivesAReopen(t *testing.T) {
	dir := storeDir(t, "demandreopen")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	now := time.Now()
	store.Mutex.Lock()
	store.recordSpanDemand(7500, now)
	store.Mutex.Unlock()
	require.NoError(t, store.Put(NewFastRandom([]byte{190}).NextHash(), []byte("x")))
	_, err = store.Seal(1) // A seal writes the manifest, demand and all
	require.NoError(t, err)
	require.NoError(t, store.Close())

	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer reopened.Close()
	reopened.Mutex.RLock()
	peak, ok := reopened.spanDemandPeak(now)
	reopened.Mutex.RUnlock()
	require.True(t, ok, "the demand record must survive a restart")
	require.Equal(t, uint64(7500), peak)

	capacity, reportedPeak, fill := reopened.FilterSizing()
	require.Equal(t, uint64(7500), reportedPeak)
	require.GreaterOrEqual(t, capacity, uint64(7500*FilterHeadroomPercent/100),
		"the reported capacity is what a filter starting now would take")
	require.NotEmpty(t, fill, "the live filters report what they hold")
}
