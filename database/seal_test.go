package blockchainDB

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The seal in two halves (seal.go; issue #84): what the locks let
// through while a seal waits on the disk, what a crash between the
// halves leaves, and what a seal costs in barriers.

// gateFsyncs parks every fsync until release is called, counting how
// many are parked at once.  The gate is package-wide, so a test that
// sets it must not run alongside another.
func gateFsyncs(t *testing.T) (parked *atomic.Int32, release func()) {
	t.Helper()
	parked = new(atomic.Int32)
	hold := make(chan struct{})
	var once sync.Once
	gate := func() {
		parked.Add(1)
		<-hold
	}
	fsyncGate.Store(&gate)
	release = func() {
		once.Do(func() {
			close(hold)
			fsyncGate.Store(nil)
		})
	}
	t.Cleanup(release)
	return parked, release
}

// waitFor polls until the condition holds or the deadline passes
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestSealAnswersReadsFromTheCutTail
// Between the cut and the commit the tail's records are in neither the
// live tail nor a segment.  Reads must still find them, immutability
// must still reach them, and writes must land in the next tail.
func TestSealAnswersReadsFromTheCutTail(t *testing.T) {
	dir := storeDir(t, "cuttail")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{84})
	k1, k2, k3 := kr.NextHash(), kr.NextHash(), kr.NextHash()
	require.NoError(t, store.Put(k1, []byte("one")))
	_, err = store.Seal(1)
	require.NoError(t, err)
	require.NoError(t, store.Put(k2, []byte("two")))
	require.NoError(t, store.Put(k3, []byte("three")))

	pending, err := store.beginSeal(2)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, segSealingName))
	require.NoError(t, err, "the cut tail sits at sealing.dat")
	require.Equal(t, 0, store.LiveCount(), "the live tail is fresh")

	v, err := store.Get(k2)
	require.NoError(t, err, "a record in the cut tail must be readable")
	require.Equal(t, []byte("two"), v)
	v, err = store.Get(k1)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), v)

	k4 := kr.NextHash()
	require.NoError(t, store.Put(k4, []byte("four")), "a write lands in the next tail")
	require.Equal(t, 1, store.LiveCount())
	require.NoError(t, store.Put(k2, []byte("two")), "a duplicate of a cut record is a no-op")
	require.ErrorIs(t, store.Put(k3, []byte("changed")), ErrImmutable, "immutability reaches the cut tail")
	existing, existed, err := store.PutIfAbsent(k3, []byte("changed"))
	require.NoError(t, err)
	require.True(t, existed)
	require.Equal(t, []byte("three"), existing)

	meta, err := pending.finish()
	require.NoError(t, err)
	require.Equal(t, uint64(2), meta.Height)
	require.Equal(t, uint64(2), meta.Count)
	_, err = os.Stat(filepath.Join(dir, segSealingName))
	require.ErrorIs(t, err, os.ErrNotExist, "the staging name is gone once the segment has its own")
	require.Nil(t, store.sealing)
	for _, k := range [][32]byte{k1, k2, k3, k4} {
		_, err = store.Get(k)
		require.NoError(t, err)
	}

	require.NoError(t, store.Close())
	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer re.Close()
	for k, want := range map[[32]byte]string{k1: "one", k2: "two", k3: "three", k4: "four"} {
		v, err := re.Get(k)
		require.NoError(t, err)
		require.Equal(t, []byte(want), v)
	}
	require.Equal(t, 1, re.LiveCount(), "k4 was in the tail the seal started")
}

// TestKV2SealHoldsNoLockAcrossItsBarriers
// With the disk held still mid-seal, a read and a write of the shard
// must go through: the seal holds the shard's lock for the cut, not
// for the fsyncs (issue #84).
func TestKV2SealHoldsNoLockAcrossItsBarriers(t *testing.T) {
	dir := storeDir(t, "kv2sealgate")
	kv, err := NewKV2(dir, 1000)
	require.NoError(t, err)
	defer kv.Close()

	kr := NewFastRandom([]byte{85})
	kp, kd := kr.NextHash(), kr.NextHash()
	_, err = kv.PutPerm(kp, []byte("perm"))
	require.NoError(t, err)
	_, err = kv.PutDyna(kd, []byte("dyna"))
	require.NoError(t, err)

	parked, release := gateFsyncs(t)
	var sealErr error
	done := make(chan struct{})
	go func() {
		_, sealErr = kv.Seal(1)
		close(done)
	}()
	waitFor(t, "the seal to reach a barrier", func() bool { return parked.Load() >= 1 })

	// within: an operation that waits behind the seal's barriers never
	// returns, since the disk is held still until release
	within(t, 2*time.Second, "Get during a seal", func() error {
		v, err := kv.Get(kp)
		if err != nil {
			return err
		}
		require.Equal(t, []byte("perm"), v)
		v, err = kv.Get(kd)
		if err != nil {
			return err
		}
		require.Equal(t, []byte("dyna"), v)
		return nil
	})
	kp2, kd2 := kr.NextHash(), kr.NextHash()
	within(t, 2*time.Second, "Put during a seal", func() error {
		if _, err := kv.PutPerm(kp2, []byte("perm2")); err != nil {
			return err
		}
		_, err := kv.PutDyna(kd2, []byte("dyna2"))
		return err
	})
	// The Perm data fsync, the index fsync and the Dyna live fsync are
	// issued together: all three park before any of them returns
	waitFor(t, "the seal's first barriers to overlap", func() bool { return parked.Load() >= 3 })

	release()
	<-done
	require.NoError(t, sealErr)
	for k, want := range map[[32]byte]string{kp: "perm", kd: "dyna", kp2: "perm2", kd2: "dyna2"} {
		v, err := kv.Get(k)
		require.NoError(t, err)
		require.Equal(t, []byte(want), v)
	}
	require.Equal(t, uint64(2), kv.PermKV.BlockHeight())
}

// TestSealBlockSealsShardsConcurrently
// Every shard of the set must be in its barriers at the same time: a
// block boundary is one seal's worth of waiting, not eight.
func TestSealBlockSealsShardsConcurrently(t *testing.T) {
	dir := storeDir(t, "sealblockgate")
	kvs, err := NewKVShardN(dir, 8, 1000)
	require.NoError(t, err)
	defer kvs.Close()

	// One permanent key per shard, so every shard has a tail to seal
	kr := NewFastRandom([]byte{86})
	keys := make([][32]byte, len(kvs.Shards))
	have := 0
	for have < len(keys) {
		k := kr.NextHash()
		i := kvs.ShardIndex(k[:])
		if keys[i] != ([32]byte{}) {
			continue
		}
		keys[i] = k
		have++
		require.NoError(t, kvs.PutPerm(k, []byte{byte(i)}))
	}

	parked, release := gateFsyncs(t)
	var sealErr error
	done := make(chan struct{})
	go func() {
		sealErr = kvs.SealBlock(1)
		close(done)
	}()
	waitFor(t, "every shard to be in a barrier at once", func() bool {
		return int(parked.Load()) >= len(kvs.Shards)
	})
	within(t, 2*time.Second, "Get during SealBlock", func() error {
		for i, k := range keys {
			v, err := kvs.Get(k)
			if err != nil {
				return err
			}
			require.Equal(t, []byte{byte(i)}, v)
		}
		return nil
	})
	release()
	<-done
	require.NoError(t, sealErr)

	height, err := kvs.readBlockHeight()
	require.NoError(t, err)
	require.Equal(t, uint64(2), height, "the block record is written once every shard sealed")
	for i, k := range keys {
		v, err := kvs.Get(k)
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, v)
	}
}

// TestInterruptedSealFoldsTheCutTailBack
// A crash between the cut and the commit leaves the tail at
// sealing.dat and the next block's writes in live.dat.  Open must
// recover both, in order, as the live tail they would have been had
// the seal never started.
func TestInterruptedSealFoldsTheCutTailBack(t *testing.T) {
	dir := storeDir(t, "foldback")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{87})
	k1, k2, k3, k4 := kr.NextHash(), kr.NextHash(), kr.NextHash(), kr.NextHash()
	require.NoError(t, store.Put(k1, []byte("one")))
	_, err = store.Seal(1)
	require.NoError(t, err)
	require.NoError(t, store.Put(k2, []byte("two")))
	require.NoError(t, store.Put(k3, []byte("three")))

	pending, err := store.beginSeal(2)
	require.NoError(t, err)
	defer pending.release()
	require.NoError(t, store.Put(k4, []byte("four")))
	require.NoError(t, store.Put(k2, []byte("two, rewritten after the cut")))
	require.NoError(t, store.liveFile.Flush()) // The crash lands after the OS has the bytes

	// Crash here: `store` is abandoned, never closed
	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer re.Close()
	_, err = os.Stat(filepath.Join(dir, segSealingName))
	require.ErrorIs(t, err, os.ErrNotExist, "the cut tail is the live tail again")
	for k, want := range map[[32]byte]string{
		k1: "one", k2: "two, rewritten after the cut", k3: "three", k4: "four",
	} {
		v, err := re.Get(k)
		require.NoErrorf(t, err, "key %x lost", k[:4])
		require.Equal(t, []byte(want), v, "the newer record wins")
	}
	require.Equal(t, 3, re.LiveCount(), "k2, k3 and k4 are the live tail")
	require.Len(t, re.active, 1, "nothing was adopted: the seal never named its file")

	// And the recovered tail seals like any other
	meta, err := re.Seal(2)
	require.NoError(t, err)
	require.Equal(t, uint64(3), meta.Count)
	require.NoError(t, re.Close())
	re2, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer re2.Close()
	v, err := re2.Get(k2)
	require.NoError(t, err)
	require.Equal(t, []byte("two, rewritten after the cut"), v)
}

// TestInterruptedSealDropsATornCutTail
// The cut tail was never fsynced by the time of the crash, so it can
// be torn like any live tail; recovery keeps its whole records and
// drops the torn one, then appends live.dat's records after them.
func TestInterruptedSealDropsATornCutTail(t *testing.T) {
	dir := storeDir(t, "torncut")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{88})
	k1, k2, k3 := kr.NextHash(), kr.NextHash(), kr.NextHash()
	require.NoError(t, store.Put(k1, []byte("one")))
	require.NoError(t, store.Put(k2, []byte("two")))
	pending, err := store.beginSeal(1)
	require.NoError(t, err)
	defer pending.release()
	require.NoError(t, store.Put(k3, []byte("three")))
	require.NoError(t, store.liveFile.Flush())

	// Tear the cut tail inside its last record
	staged := filepath.Join(dir, segSealingName)
	fi, err := os.Stat(staged)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(staged, fi.Size()-1))

	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer re.Close()
	v, err := re.Get(k1)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), v)
	_, err = re.Get(k2)
	require.Error(t, err, "the torn record is dropped, as replay would drop it")
	v, err = re.Get(k3)
	require.NoError(t, err, "live.dat's record follows the whole ones")
	require.Equal(t, []byte("three"), v)
	require.Equal(t, 2, re.LiveCount())
}

// TestInterruptedSealAfterTheLinkIsAdopted
// A crash between the link and the removal of the staging name leaves
// the same file under both.  The segment name is an interrupted seal
// and is adopted (issue #45); the staging name must be dropped, not
// replayed as a second copy.
func TestInterruptedSealAfterTheLinkIsAdopted(t *testing.T) {
	dir := storeDir(t, "linkedcut")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{89})
	k1, k2 := kr.NextHash(), kr.NextHash()
	require.NoError(t, store.Put(k1, []byte("one")))
	_, err = store.Seal(1)
	require.NoError(t, err)
	require.NoError(t, store.Put(k2, []byte("two")))
	pending, err := store.beginSeal(2)
	require.NoError(t, err)
	defer pending.release()
	staged := filepath.Join(dir, segSealingName)
	require.NoError(t, os.Link(staged, filepath.Join(dir, segmentFileName(2, 0))))

	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer re.Close()
	_, err = os.Stat(staged)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Len(t, re.active, 2, "the linked file is adopted as the segment it was about to be")
	require.Equal(t, 0, re.LiveCount(), "and not replayed as well")
	v, err := re.Get(k2)
	require.NoError(t, err)
	require.Equal(t, []byte("two"), v)
}

// TestSealFailureIsResumedByTheNextSeal
// A seal that fails after its cut leaves the tail staged.  Reads keep
// working, the next seal finishes the staged one before cutting its
// own, and nothing is sealed twice.
func TestSealFailureIsResumedByTheNextSeal(t *testing.T) {
	dir := storeDir(t, "sealfail")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{90})
	k1 := kr.NextHash()
	require.NoError(t, store.Put(k1, []byte("one")))
	pending, err := store.beginSeal(1)
	require.NoError(t, err)
	// A squatter on the index's staging name makes the index write
	// fail, before the link
	require.NoError(t, os.Mkdir(filepath.Join(dir, segSealingIndexTmp), 0o755))
	_, err = pending.finish()
	require.Error(t, err, "the seal must report the failure")
	require.NotNil(t, store.staged)

	v, err := store.Get(k1)
	require.NoError(t, err, "the cut tail is still readable after a failed seal")
	require.Equal(t, []byte("one"), v)
	_, err = store.Seal(2)
	require.Error(t, err, "the squatter is still there: the resumed seal fails again")
	require.NoError(t, os.Remove(filepath.Join(dir, segSealingIndexTmp)))

	k2 := kr.NextHash()
	require.NoError(t, store.Put(k2, []byte("two")))
	meta, err := store.Seal(2)
	require.NoError(t, err, "with the fault gone, the next seal resumes the staged one and then seals its own tail")
	require.Nil(t, store.staged)
	require.Equal(t, uint64(2), meta.Height)
	require.Len(t, store.active, 2, "the staged seal at block 1, then this one")
	require.Equal(t, uint64(1), store.active[0].meta.Height)
	for k, want := range map[[32]byte]string{k1: "one", k2: "two"} {
		v, err := store.Get(k)
		require.NoError(t, err)
		require.Equal(t, []byte(want), v)
	}
	_, err = os.Stat(filepath.Join(dir, segSealingName))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// TestSealFailureAfterTheLinkIsResumedWithoutALink
// A failure after the cut file has its segment name must not link it
// a second time on resume: the name it claimed is kept, and the file
// keeps answering reads under it.
func TestSealFailureAfterTheLinkIsResumedWithoutALink(t *testing.T) {
	dir := storeDir(t, "sealfail-linked")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{93})
	k1 := kr.NextHash()
	require.NoError(t, store.Put(k1, []byte("one")))
	pending, err := store.beginSeal(1)
	require.NoError(t, err)
	// A squatter on the index's FINAL name: the link succeeds, the
	// index rename after it fails
	indexPath := filepath.Join(dir, strings.TrimSuffix(segmentFileName(1, 0), segDataSuffix)+segIndexSuffix)
	require.NoError(t, os.Mkdir(indexPath, 0o755))
	_, err = pending.finish()
	require.Error(t, err)
	require.Equal(t, filepath.Join(dir, segmentFileName(1, 0)), pending.dataPath, "linked before the failure")
	_, err = os.Stat(filepath.Join(dir, segSealingName))
	require.ErrorIs(t, err, os.ErrNotExist, "the staging name went with the link")

	v, err := store.Get(k1)
	require.NoError(t, err, "still readable through the file under its new name")
	require.Equal(t, []byte("one"), v)

	require.NoError(t, os.Remove(indexPath))
	_, err = store.Seal(2)
	require.NoError(t, err)
	require.Len(t, store.active, 1, "block 2 was empty; the resumed seal is the one segment")
	require.Equal(t, SegmentMeta{Height: 1, Seq: 0}, SegmentMeta{Height: store.active[0].meta.Height, Seq: store.active[0].meta.Seq})
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var dataFiles []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), segFilePrefix) && strings.HasSuffix(e.Name(), segDataSuffix) {
			dataFiles = append(dataFiles, e.Name())
		}
	}
	require.Equal(t, []string{segmentFileName(1, 0)}, dataFiles, "one name, not a second link at the next sequence")
	v, err = store.Get(k1)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), v)
}

// TestKV2SealSyncsDynaWhenThePermCutFails
// A Perm cut that is refused -- re-sealing a closed block -- must not
// cost the Dyna layer its sync: the boundary covers both layers
// (issue #29), whatever either says.
func TestKV2SealSyncsDynaWhenThePermCutFails(t *testing.T) {
	dir := storeDir(t, "kv2-permfail")
	kv, err := NewKV2(dir, 1000)
	require.NoError(t, err)
	defer kv.Close()

	kr := NewFastRandom([]byte{94})
	_, err = kv.PutPerm(kr.NextHash(), []byte("p"))
	require.NoError(t, err)
	_, err = kv.Seal(1)
	require.NoError(t, err)
	_, err = kv.PutDyna(kr.NextHash(), []byte("d"))
	require.NoError(t, err)
	require.True(t, kv.DynaKV.liveDirty)

	before := FsyncCount()
	_, err = kv.Seal(0)
	require.Error(t, err, "block 0 is closed")
	require.False(t, kv.DynaKV.liveDirty, "the Dyna tail was synced regardless")
	require.Equal(t, uint64(1), FsyncCount()-before, "the Dyna tail's one barrier")
	require.Equal(t, uint64(2), kv.DynaKV.BlockHeight(), "and the Dyna block advanced regardless")
}

// TestRecoveryFoldIsIdempotent
// A crash after the fold's rename landed but before the staging name
// was removed leaves live.dat already holding the staged records.  The
// next open must recognise that and not fold them in twice.
func TestRecoveryFoldIsIdempotent(t *testing.T) {
	dir := storeDir(t, "fold-twice")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{95})
	k1, k2 := kr.NextHash(), kr.NextHash()
	require.NoError(t, store.Put(k1, []byte("one")))
	pending, err := store.beginSeal(1)
	require.NoError(t, err)
	defer pending.release()
	require.NoError(t, store.Put(k2, []byte("two")))
	require.NoError(t, store.liveFile.Flush())

	staged, err := os.ReadFile(filepath.Join(dir, segSealingName))
	require.NoError(t, err)
	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(2), re.LiveRecords())
	require.NoError(t, re.Close())

	// The crash: the fold landed, the removal of sealing.dat did not
	require.NoError(t, os.WriteFile(filepath.Join(dir, segSealingName), staged, 0o644))
	re2, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer re2.Close()
	require.Equal(t, uint64(2), re2.LiveRecords(), "the staged records are in the tail once")
	_, err = os.Stat(filepath.Join(dir, segSealingName))
	require.ErrorIs(t, err, os.ErrNotExist)
	for k, want := range map[[32]byte]string{k1: "one", k2: "two"} {
		v, err := re2.Get(k)
		require.NoError(t, err)
		require.Equal(t, []byte(want), v)
	}
}

// TestAutoSealHoldsNoShardLockAcrossItsBarriers
// A Put that fills the tail begins the auto-seal under the shard's
// shared lock and finishes it after letting go: with its barriers
// parked, a Get of the shard -- and a writer queued for the exclusive
// lock -- must go through (spec 1.6).
func TestAutoSealHoldsNoShardLockAcrossItsBarriers(t *testing.T) {
	dir := storeDir(t, "autoseal-gate")
	kv, err := NewKV2(dir, 2)
	require.NoError(t, err)
	defer kv.Close()

	kr := NewFastRandom([]byte{96})
	k1, k2 := kr.NextHash(), kr.NextHash()
	_, err = kv.PutPerm(k1, []byte("one"))
	require.NoError(t, err)

	parked, release := gateFsyncs(t)
	var putErr error
	done := make(chan struct{})
	go func() {
		_, putErr = kv.PutPerm(k2, []byte("two")) // Tips SealLimit: auto-seals
		close(done)
	}()
	waitFor(t, "the auto-seal to reach a barrier", func() bool { return parked.Load() >= 1 })
	within(t, 2*time.Second, "Get during an auto-seal", func() error {
		for k, want := range map[[32]byte]string{k1: "one", k2: "two"} {
			v, err := kv.Get(k)
			if err != nil {
				return err
			}
			require.Equal(t, []byte(want), v)
		}
		return nil
	})
	within(t, 2*time.Second, "the exclusive lock during an auto-seal", func() error {
		kv.Mutex.Lock() // What Close, SetFilterBlocks and Seal take
		kv.Mutex.Unlock()
		return nil
	})
	release()
	<-done
	require.NoError(t, putErr)
	require.Equal(t, 0, kv.PermKV.LiveCount(), "the tail was sealed")
}

// TestSealBarrierCount
// What a seal costs, counted (spec 1.2): the barriers are the same
// ones issue #33 lists, and the count is pinned so that a change to
// it is a deliberate one.
func TestSealBarrierCount(t *testing.T) {
	count := func(fn func()) uint64 {
		before := FsyncCount()
		fn()
		return FsyncCount() - before
	}
	kr := NewFastRandom([]byte{91})

	// One layer: the data file, its index, the manifest and the directory
	dir := storeDir(t, "barriers-store")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
	require.Equal(t, uint64(4), count(func() {
		_, err := store.Seal(1)
		require.NoError(t, err)
	}), "data, index, manifest, directory")
	require.Equal(t, uint64(2), count(func() {
		_, err := store.Seal(2)
		require.NoError(t, err)
	}), "an empty boundary commits the block: manifest, directory")
	store.ExternalBlockRecord = true
	require.Equal(t, uint64(0), count(func() {
		_, err := store.Seal(3)
		require.NoError(t, err)
	}), "an empty boundary with the block recorded elsewhere costs nothing (issue #32)")

	// A shard: the Perm seal's four and the Dyna tail's one
	kdir := storeDir(t, "barriers-kv2")
	kv, err := NewKV2(kdir, 1000)
	require.NoError(t, err)
	defer kv.Close()
	_, err = kv.PutPerm(kr.NextHash(), []byte("p"))
	require.NoError(t, err)
	_, err = kv.PutDyna(kr.NextHash(), []byte("d"))
	require.NoError(t, err)
	require.Equal(t, uint64(5), count(func() {
		_, err := kv.Seal(1)
		require.NoError(t, err)
	}), "Perm data, index, manifest, directory; Dyna live tail")

	// A set: every shard's five, and the block record's two
	sdir := storeDir(t, "barriers-set")
	kvs, err := NewKVShardN(sdir, 8, 1000)
	require.NoError(t, err)
	defer kvs.Close()
	touched := make(map[int]bool)
	for len(touched) < 8 {
		k := kr.NextHash()
		i := kvs.ShardIndex(k[:])
		if touched[i] {
			continue
		}
		touched[i] = true
		require.NoError(t, kvs.PutPerm(k, []byte("p")))
		d := k
		d[0] ^= 0xff // Same shard: the shard index is not in the first byte
		require.Equal(t, i, kvs.ShardIndex(d[:]))
		require.NoError(t, kvs.PutDyna(d, []byte("d")))
	}
	require.Equal(t, uint64(8*5+2), count(func() {
		require.NoError(t, kvs.SealBlock(1))
	}), "eight shards' five each, then the block record's tmp and directory")
}

// TestFilterRebuildKeepsTheCutTail
// Between a seal's halves the cut tail is in no segment and not the
// live tail.  A filter rebuilt then must still hold its keys, or the
// segment they land in is denied by the filters that claim to cover
// it: a false "absent" (issue #35).
func TestFilterRebuildKeepsTheCutTail(t *testing.T) {
	dir := storeDir(t, "cutfilter")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{92})
	k := kr.NextHash()
	require.NoError(t, store.Put(k, []byte("cut")))
	pending, err := store.beginSeal(1)
	require.NoError(t, err)
	store.Mutex.Lock() // What any rebuild between the halves does
	require.NoError(t, store.rebuildKeyFilters())
	store.Mutex.Unlock()
	_, err = pending.finish()
	require.NoError(t, err)

	before := store.Stats()
	v, err := store.Get(k)
	require.NoError(t, err, "the rebuilt filters must not deny a key the cut tail held")
	require.Equal(t, []byte("cut"), v)
	after := store.Stats()
	require.Equal(t, before.FilterAbsent, after.FilterAbsent, "the filters said 'maybe', as they must")
}
