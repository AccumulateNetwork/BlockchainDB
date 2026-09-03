package blockchainDB

import (
	"os"
	"path/filepath"
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

// TestSealFailureLeavesTheStoreReadableAndRecoverable
// A seal that fails after its cut leaves the tail staged.  Reads keep
// working, the next seal says why it cannot proceed, and a close and
// reopen recover the records.
func TestSealFailureLeavesTheStoreReadableAndRecoverable(t *testing.T) {
	dir := storeDir(t, "sealfail")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{90})
	k1 := kr.NextHash()
	require.NoError(t, store.Put(k1, []byte("one")))
	pending, err := store.beginSeal(1)
	require.NoError(t, err)
	// Make the link impossible: a directory squats on every name the
	// remint loop would try is too much; a squatter on the index's
	// staging name makes the index write fail instead
	require.NoError(t, os.Mkdir(filepath.Join(dir, segSealingIndexTmp), 0o755))
	_, err = pending.finish()
	require.Error(t, err, "the seal must report the failure")
	require.NoError(t, os.Remove(filepath.Join(dir, segSealingIndexTmp)))

	v, err := store.Get(k1)
	require.NoError(t, err, "the cut tail is still readable after a failed seal")
	require.Equal(t, []byte("one"), v)
	_, err = store.Seal(1)
	require.ErrorIs(t, err, errSealStaged)

	require.NoError(t, store.Close())
	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer re.Close()
	v, err = re.Get(k1)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), v)
	_, err = re.Seal(1)
	require.NoError(t, err, "sealing works again once the tail is recovered")
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
// live tail.  A filter rebuilt then -- SetFilterBlocks does it -- must
// still hold its keys, or the segment they land in is denied by the
// filters that claim to cover it: a false "absent" (issue #35).
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
	require.NoError(t, store.SetFilterBlocks(2*MinFilterBlocks), "rebuilds every filter")
	_, err = pending.finish()
	require.NoError(t, err)

	before := store.Stats()
	v, err := store.Get(k)
	require.NoError(t, err, "the rebuilt filters must not deny a key the cut tail held")
	require.Equal(t, []byte("cut"), v)
	after := store.Stats()
	require.Equal(t, before.FilterAbsent, after.FilterAbsent, "the filters said 'maybe', as they must")
}
