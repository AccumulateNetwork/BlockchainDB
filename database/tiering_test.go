package blockchainDB

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two tiers, two locks (issue #57).
//
// The active tier -- the live tail and the sealed segments of the last
// N to 2N blocks -- is the protocol's, under Mutex.  History -- every
// older segment -- is the maintainer's, under History.  Neither side
// takes the other's lock on its way, so a merge or a compaction never
// stops a commit or a read, and a commit never stops a merge.  Most
// of these tests are deterministic rather than timings: hold one lock
// from outside and show the other side still runs.  Two use the
// maintenance hook to hold a merge and a compaction between their
// copy and their swap, which is the moment the old code held the
// store lock through, and show a Put, a Seal and a Get complete
// meanwhile.

// tieredStore seals `blocks` blocks of `perBlock` keys with N =
// MinFilterBlocks, so that the last N to 2N blocks are active and
// everything older is history, and returns the keys by block.
func tieredStore(t *testing.T, dir string, seed byte, mutable bool, blocks, perBlock int) (store *SegmentStore, keys [][][32]byte) {
	t.Helper()
	store, err := NewSegmentStore(dir, mutable)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))
	kr := NewFastRandom([]byte{seed})
	keys = make([][][32]byte, blocks+1)
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

// tiers reads both lists' heights, each under its own lock
func tiers(s *SegmentStore) (history, active []uint64) {
	s.History.RLock()
	for _, seg := range s.history {
		history = append(history, seg.meta.Height)
	}
	s.History.RUnlock()
	s.Mutex.RLock()
	for _, seg := range s.active {
		active = append(active, seg.meta.Height)
	}
	s.Mutex.RUnlock()
	return history, active
}

// within runs fn and fails if it takes longer than d
func within(t *testing.T, d time.Duration, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(d):
		t.Fatalf("%s did not complete within %v", what, d)
	}
}

// TestSegmentsHandOffAtTheRoll
// After 60 blocks with N=20 the window starts at block 40: blocks 40
// on are active, blocks 1-39 are history, and both survive a reopen
// with each manifest naming its own tier.
func TestSegmentsHandOffAtTheRoll(t *testing.T) {
	dir := storeDir(t, "tiers")
	store, keys := tieredStore(t, dir, 71, false, 60, 5)
	require.Equal(t, uint64(40), func() uint64 { store.Mutex.RLock(); defer store.Mutex.RUnlock(); return store.tierStart() }())

	history, active := tiers(store)
	require.Len(t, history, 39, "blocks 1-39 are below the window")
	require.Len(t, active, 21, "blocks 40-60 are inside it")
	assert.Equal(t, uint64(39), history[len(history)-1])
	assert.Equal(t, uint64(40), active[0])

	// A key from either tier reads back
	for _, h := range []int{1, 39, 40, 60} {
		v, err := store.GetDeep(keys[h][0])
		require.NoErrorf(t, err, "block %d", h)
		assert.Equal(t, fmt.Sprintf("b%d-0", h), string(v))
	}

	// The handoffs are in flight: the active manifest still names them
	// and the history manifest does not, until a history commit
	m, err := store.readManifest()
	require.NoError(t, err)
	hm, err := store.readHistoryManifest()
	require.NoError(t, err)
	assert.Len(t, m.Segments, 60, "the active manifest names every segment until history records the handoffs")
	assert.Empty(t, hm.Segments)

	// A merge is that commit, and it merges history only: block 45 is
	// below the watermark but inside the window, and stays
	_, merged, err := store.MergeBelow(50)
	require.NoError(t, err)
	require.True(t, merged)
	history, active = tiers(store)
	require.Len(t, history, 1, "blocks 1-39 merged into one")
	assert.Len(t, active, 21, "the active tier is untouched")
	hm, err = store.readHistoryManifest()
	require.NoError(t, err)
	assert.Len(t, hm.Segments, 1)
	assert.Equal(t, uint64(38), hm.Segments[0].Span, "the merged segment reaches back to block 1")

	// The next active commit drops the recorded handoffs from the
	// active manifest and deletes the merge inputs
	require.NoError(t, store.Put(NewFastRandom([]byte{72}).NextHash(), []byte("v")))
	_, err = store.Seal(61)
	require.NoError(t, err)
	m, err = store.readManifest()
	require.NoError(t, err)
	assert.Len(t, m.Segments, 22, "the active manifest names the active tier alone")
	store.awaitUnlinks() // Deletion is a goroutine's, off both locks
	dataFiles := 0
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == segDataSuffix {
			dataFiles++
		}
	}
	assert.Equal(t, 1+22+1, dataFiles, "one merged, 22 active, live.dat")

	for h := 1; h <= 60; h++ {
		for _, key := range keys[h] {
			_, err := store.GetDeep(key)
			require.NoErrorf(t, err, "block %d", h)
		}
	}
	require.NoError(t, store.Close())

	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	history, active = tiers(reopened)
	assert.Len(t, history, 1)
	assert.Len(t, active, 22)
	for h := 1; h <= 60; h++ {
		for _, key := range keys[h] {
			_, err := reopened.GetDeep(key)
			require.NoErrorf(t, err, "block %d after a reopen", h)
		}
	}
	require.NoError(t, reopened.Close())
}

// TestProtocolPathDoesNotTakeTheHistoryLock
// Hold History exclusively from outside -- a swap in progress -- and
// show that a Put, a Seal, a Sync and a Get of active data all
// complete.  Deterministic: if any of them took History it would
// block forever, and the bound is the failure.
func TestProtocolPathDoesNotTakeTheHistoryLock(t *testing.T) {
	dir := storeDir(t, "hlock")
	store, keys := tieredStore(t, dir, 73, false, 45, 5)
	defer store.Close()

	store.History.Lock()
	defer store.History.Unlock()

	kr := NewFastRandom([]byte{74})
	within(t, 10*time.Second, "the protocol path with History held", func() error {
		if _, err := store.Get(keys[42][0]); err != nil { // Active
			return err
		}
		if _, err := store.Get(keys[45][0]); err != nil { // Active, newest block
			return err
		}
		if err := store.Put(kr.NextHash(), []byte("new")); err != nil { // Absent from the window: no history walk
			return err
		}
		if err := store.Put(keys[42][0], []byte("b42-0")); err != nil { // In the window: settled by the active tier
			return err
		}
		if err := store.Sync(); err != nil {
			return err
		}
		if _, err := store.Seal(46); err != nil {
			return err
		}
		_, err := store.SealNext()
		return err
	})
}

// TestHistoryMaintenanceDoesNotTakeTheStoreLock
// The other direction: hold the store's Mutex exclusively -- a commit
// in progress -- and show that a merge and a compaction of history
// both complete.
func TestHistoryMaintenanceDoesNotTakeTheStoreLock(t *testing.T) {
	t.Run("merge", func(t *testing.T) {
		dir := storeDir(t, "slock")
		store, _ := tieredStore(t, dir, 75, false, 60, 5)
		defer store.Close()
		store.Mutex.Lock()
		defer store.Mutex.Unlock()
		within(t, 10*time.Second, "MergeBelow with the store lock held", func() error {
			_, merged, err := store.MergeBelow(50)
			if err == nil && !merged {
				err = fmt.Errorf("nothing merged")
			}
			return err
		})
	})
	t.Run("compact", func(t *testing.T) {
		dir := storeDir(t, "slock")
		store, _ := tieredStore(t, dir, 76, true, 60, 5)
		defer store.Close()
		store.Mutex.Lock()
		defer store.Mutex.Unlock()
		within(t, 10*time.Second, "CompactHistory with the store lock held", func() error {
			compacted, err := store.CompactHistory()
			if err == nil && !compacted {
				err = fmt.Errorf("nothing compacted")
			}
			return err
		})
	})
}

// TestCommitsRunDuringHistoryMaintenance
// The regression test for the pause itself.  A merge and then a
// compaction are held between their copy and their swap -- with the
// old code that is inside the store lock, and a Put, a Seal and a Get
// wait there for as long as the copy takes -- and each of the three
// must complete within a second while the maintenance is held.  Then
// the maintenance is released and must still commit correctly.
func TestCommitsRunDuringHistoryMaintenance(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutable bool
		run     func(s *SegmentStore) (bool, error)
	}{
		{"merge", false, func(s *SegmentStore) (bool, error) { _, m, err := s.MergeBelow(50); return m, err }},
		{"compact", true, func(s *SegmentStore) (bool, error) { return s.CompactHistory() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := storeDir(t, "pause")
			store, keys := tieredStore(t, dir, 77, tc.mutable, 60, 5)
			defer store.Close()

			held := make(chan struct{})
			release := make(chan struct{})
			released := false
			maintenanceHook = func() {
				close(held)
				<-release
			}
			defer func() { // Let the maintenance finish if an assertion failed, so Close can run
				maintenanceHook = nil
				if !released {
					close(release)
				}
			}()

			done := make(chan error, 1)
			go func() {
				did, err := tc.run(store)
				if err == nil && !did {
					err = fmt.Errorf("the maintenance did nothing")
				}
				done <- err
			}()
			<-held // The copy is finished; the swap has not begun

			kr := NewFastRandom([]byte{78})
			fresh := kr.NextHash()
			within(t, time.Second, "a commit during "+tc.name, func() error {
				if err := store.Put(fresh, []byte("during")); err != nil {
					return err
				}
				_, err := store.Seal(61)
				return err
			})
			within(t, time.Second, "a read during "+tc.name, func() error {
				if _, err := store.Get(keys[55][0]); err != nil { // Active
					return err
				}
				if _, err := store.GetDeep(keys[5][0]); err != nil { // History: sees the inputs
					return err
				}
				_, err := store.Get(fresh)
				return err
			})

			close(release)
			released = true
			require.NoError(t, <-done)
			for h := 1; h <= 60; h++ {
				for _, key := range keys[h] {
					_, err := store.GetDeep(key)
					require.NoErrorf(t, err, "block %d after the %s", h, tc.name)
				}
			}
			history, _ := tiers(store)
			assert.Len(t, history, 1, "the %s committed", tc.name)
		})
	}
}

// TestKV2CommitsRunDuringCompress
// The same at the KV2 level, where Compress used to take the KV2 lock
// and the Dyna layer's lock for the whole rewrite: a PutPerm, a
// PutDyna, a Seal and a Get complete while a Compress is held between
// its copy and its swap.
func TestKV2CommitsRunDuringCompress(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv2, err := NewKV2(dir, 100)
	require.NoError(t, err)
	defer kv2.Close()
	require.NoError(t, kv2.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{79})
	vr := NewFastRandom([]byte{79, 79})
	hot := make([][32]byte, 50)
	for i := range hot {
		hot[i] = kr.NextHash()
	}
	var h uint64
	for round := 0; round < 3*MinFilterBlocks; round++ {
		for _, key := range hot {
			_, err = kv2.PutDyna(key, vr.RandBuff(20, 60))
			require.NoError(t, err)
		}
		_, err = kv2.PutPerm(kr.NextHash(), []byte("p"))
		require.NoError(t, err)
		h++
		_, err = kv2.Seal(h)
		require.NoError(t, err)
	}
	history, _ := tiers(kv2.DynaKV)
	require.Greater(t, len(history), 1, "the Dyna layer must hold history to compact")

	held := make(chan struct{})
	release := make(chan struct{})
	released := false
	maintenanceHook = func() {
		close(held)
		<-release
	}
	defer func() {
		maintenanceHook = nil
		if !released {
			close(release)
		}
	}()
	done := make(chan error, 1)
	go func() { done <- kv2.Compress() }()
	<-held

	within(t, time.Second, "a commit during Compress", func() error {
		if _, err := kv2.PutPerm(kr.NextHash(), []byte("p")); err != nil {
			return err
		}
		if _, err := kv2.PutDyna(hot[0], []byte("during")); err != nil {
			return err
		}
		if _, err := kv2.Get(hot[1]); err != nil {
			return err
		}
		h++
		_, err := kv2.Seal(h)
		return err
	})
	close(release)
	released = true
	require.NoError(t, <-done)
	v, err := kv2.Get(hot[0])
	require.NoError(t, err)
	assert.Equal(t, "during", string(v))
	history, _ = tiers(kv2.DynaKV)
	assert.Len(t, history, 1, "the compaction committed")
}

// TestPackRunsWithAShardLockHeld
// PackFinalized reads and drops history only, and the drop is a
// history commit: it must complete while a shard's store lock is held.
func TestPackRunsWithAShardLockHeld(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	kvs, keys, values := packedFixture(t, dir, 80, 4, 60)
	ageOutShards(t, kvs, 4)
	_, err := kvs.MergeFinalized(4)
	require.NoError(t, err)

	shard := kvs.Shards[ShardIndex(keys[0][:])]
	shard.PermKV.Mutex.Lock()
	within(t, 30*time.Second, "PackFinalized with a shard's store lock held", func() error {
		_, packed, err := kvs.PackFinalized(4)
		if err == nil && !packed {
			err = fmt.Errorf("nothing packed")
		}
		return err
	})
	shard.PermKV.Mutex.Unlock()
	checkEveryKey(t, kvs, keys, values, "after packing")
	require.NoError(t, kvs.Close())
}

// TestHandoffSurvivesACrash
// A segment in transit between the manifests is named by at least one
// of them at every moment.  Three crash points, each reproduced by
// reopening without closing: after the roll (named by the active
// manifest only), after the merge (its replacement named by history,
// itself still named by the active manifest, its file still on disk),
// and after the next seal (named by history only, inputs gone).
func TestHandoffSurvivesACrash(t *testing.T) {
	check := func(t *testing.T, dir string, keys [][][32]byte, when string) {
		t.Helper()
		store, err := OpenSegmentStore(dir)
		require.NoErrorf(t, err, "open %s", when)
		for h := 1; h < len(keys); h++ {
			for _, key := range keys[h] {
				v, err := store.GetDeep(key)
				require.NoErrorf(t, err, "block %d %s", h, when)
				require.Equal(t, fmt.Sprintf("b%d-0", h), string(v))
			}
		}
		// Reopening does not itself rewrite either manifest: what it
		// read was enough, and the commits that follow finish the job
		require.NoError(t, store.Close())
	}

	dir := storeDir(t, "crash")
	store, keys := tieredStore(t, dir, 81, false, 60, 1)
	check(t, dir, keys, "after the roll")

	_, merged, err := store.MergeBelow(50)
	require.NoError(t, err)
	require.True(t, merged)
	check(t, dir, keys, "after the merge")
	// The check closed a store opened on the same directory, which
	// committed both manifests; this handle is stale from here on, and
	// only its files matter.  Reopen it for the last step.
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	require.NoError(t, store.Put(NewFastRandom([]byte{82}).NextHash(), []byte("v")))
	_, err = store.Seal(61)
	require.NoError(t, err)
	check(t, dir, keys, "after the seal")
	require.NoError(t, store.Close())
}

// TestHistoryManifestIsRequired
// A store is two manifests, and a build reading one alone would
// silently lose every segment the other names.
func TestHistoryManifestIsRequired(t *testing.T) {
	dir := storeDir(t, "hist")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.Put(NewFastRandom([]byte{83}).NextHash(), []byte("v")))
	_, err = store.Seal(1)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	path := filepath.Join(dir, segHistoryName)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var hm HistoryManifest
	require.NoError(t, json.Unmarshal(data, &hm))
	assert.Equal(t, uint32(StoreFormatVersion), hm.Version)

	hm.Version = StoreFormatVersion + 1
	out, err := json.Marshal(&hm)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, out, 0644))
	_, err = OpenSegmentStore(dir)
	require.Error(t, err, "a history manifest of another version must be refused")
	assert.Contains(t, err.Error(), "format version")

	require.NoError(t, os.Remove(path))
	_, err = OpenSegmentStore(dir)
	require.Error(t, err, "a store without its history manifest must be refused, not opened as if history were empty")
}

// TestReopenRaisesTheBlockToTheNewestSegment
// The Dyna layer's block advances in memory with every KV2.Seal and is
// persisted only by its own seals.  A segment it sealed far above the
// block its manifest recorded, left for recoverOrphans by a crash
// before the manifest commit, must not leave the reopened store
// believing it is in an older block than its newest segment: its next
// auto-seal would refuse, "block height 5 is below the newest
// segment's block 68", and the layer would never seal again.  Found
// by TestCrashRecoverySeal, once in ~40 runs.
func TestReopenRaisesTheBlockToTheNewestSegment(t *testing.T) {
	dir := storeDir(t, "raise")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))
	kr := NewFastRandom([]byte{84})
	require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
	_, err = store.SealNext() // Segment (0,0); the manifest records block 0
	require.NoError(t, err)
	saved, err := os.ReadFile(filepath.Join(dir, segManifestName))
	require.NoError(t, err)

	// The blocks pass without a manifest of this store's own, then a
	// seal lands a segment at block 60 -- and the manifest naming it is
	// never written: a crash between the rename and the commit
	store.AdvanceBlock(3 * MinFilterBlocks)
	key := kr.NextHash()
	require.NoError(t, store.Put(key, []byte("late")))
	meta, err := store.SealNext()
	require.NoError(t, err)
	require.Equal(t, uint64(3*MinFilterBlocks), meta.Height)
	require.NoError(t, store.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, segManifestName), saved, 0644))

	reopened, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, reopened.BlockHeight(), meta.Height, "the block must be at least the newest segment's")
	v, err := reopened.Get(key)
	require.NoError(t, err, "the adopted segment's key")
	assert.Equal(t, "late", string(v))
	require.NoError(t, reopened.Put(kr.NextHash(), []byte("after")))
	_, err = reopened.SealNext()
	require.NoError(t, err, "the store must be able to seal again after adopting a segment above its recorded block")
	require.NoError(t, reopened.Close())
}
