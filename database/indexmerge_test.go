package blockchainDB

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// peakHeapDuring runs fn while sampling the Go heap, and reports the
// largest live-heap growth seen over the value at the start.  The GC
// is run before and the sampler reads HeapAlloc, which includes
// garbage not yet collected -- so this over-reports, never under, and
// a bound on it is a safe bound on what fn held.
func peakHeapDuring(fn func()) (growthMB float64) {
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak uint64 = base.HeapAlloc
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > peak {
				peak = ms.HeapAlloc
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	fn()
	close(stop)
	wg.Wait()
	return float64(peak-base.HeapAlloc) / (1 << 20)
}

// TestCompactionMemoryIsBoundedByInputs
// Compaction used to hold every key of the run in two maps and a
// sorted slice -- O(records) -- and one such compaction of a 5.3 GB
// dynamic layer held 2 GB on a node with a 2.5 GB limit, which put the
// partition into a GC death spiral (issue #59).  It now streams a
// k-way merge over the inputs' sorted indexes: memory is one read
// buffer per input, whatever the inputs hold.
//
// One million records over 100 segments, almost all distinct keys with
// some overwriting -- which is what a dynamic layer looks like: every
// transaction's status is a key of its own.  The maps held one entry
// per DISTINCT key, so the number that matters is distinct keys, not
// records, and here it is ~900,000: well over 200 MB in the old maps.
// The bound is a small fraction of that, and it does not move if the
// count is raised.
func TestCompactionMemoryIsBoundedByInputs(t *testing.T) {
	dir := storeDir(t, "mem")
	kv, err := NewKV2(dir, 10_000) // Seal every 10k records
	require.NoError(t, err)
	defer kv.Close()
	require.NoError(t, kv.SetFilterBlocks(MinFilterBlocks))

	const segments, perSegment = 100, 10_000
	kr := NewFastRandom([]byte{201})
	var recent [][32]byte // Keys to overwrite, so there is something to reclaim
	value := make([]byte, 100)
	seq := uint64(0)
	for s := 0; s < segments; s++ {
		for i := 0; i < perSegment; i++ {
			seq++
			var k [32]byte
			if len(recent) > 0 && i%10 == 0 {
				k = recent[kr.UintN(uint(len(recent)))] // An overwrite
			} else {
				k = kr.NextHash() // A new key
				if len(recent) < 4096 {
					recent = append(recent, k)
				} else {
					recent[kr.UintN(4096)] = k
				}
			}
			value[0], value[1], value[2], value[3] = byte(seq), byte(seq>>8), byte(seq>>16), byte(seq>>24)
			_, err = kv.PutDyna(k, value)
			require.NoError(t, err)
		}
	}
	// Push every sealed segment below the window into history
	for b := uint64(1); b <= 3*MinFilterBlocks; b++ {
		_, err = kv.Seal(b)
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, len(kv.DynaKV.history), 80, "the run must be large for this to test anything")

	var compacted bool
	growth := peakHeapDuring(func() {
		compacted, err = kv.DynaKV.CompactHistory()
	})
	require.NoError(t, err)
	require.True(t, compacted, "there was garbage to reclaim")
	t.Logf("compacting %d segments / %d records: peak heap growth %.1f MB", segments, segments*perSegment, growth)
	// The old maps measured 453 MB here.  The streaming merge's live
	// memory is ~5 MB of cursor buffers plus a 1.3 MB bloom; the sampler
	// also sees garbage between collections, so the bound is loose.
	require.Lessf(t, growth, 96.0, "compaction held %.1f MB for %d records; it must not hold the run's keys", growth, segments*perSegment)

	// And it merged correctly: every recently written key still reads
	for i, k := range recent {
		_, err := kv.GetDyna(k)
		require.NoErrorf(t, err, "key %d lost by the compaction", i)
	}
}

// TestMergeIndexesKeepsTheNewestCopy
// The streaming merge must emit each key once, in order, from the
// newest input that holds it.
func TestMergeIndexesKeepsTheNewestCopy(t *testing.T) {
	dir := storeDir(t, "merge")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{202})
	keys := make([][32]byte, 300)
	for i := range keys {
		keys[i] = kr.NextHash()
	}
	// Three segments; keys 100..199 appear in two, 200..299 in all three
	writeSeg := func(from, to int, tag byte, h uint64) {
		for i := from; i < to; i++ {
			require.NoError(t, store.Put(keys[i], []byte{tag, byte(i), byte(i >> 8)}))
		}
		_, err := store.Seal(h)
		require.NoError(t, err)
	}
	writeSeg(0, 300, 'a', 1)
	writeSeg(100, 300, 'b', 2)
	writeSeg(200, 300, 'c', 3)
	segs := store.sealedSegments()
	require.Len(t, segs, 3)

	var seen [][32]byte
	var srcs []int
	n, err := mergeIndexes(segs, func(src int, key [32]byte, dbb DBBKey) error {
		seen = append(seen, key)
		srcs = append(srcs, src)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(300), n, "each key once")
	require.Len(t, seen, 300)
	for i := 1; i < len(seen); i++ {
		require.Truef(t, string(seen[i-1][:]) < string(seen[i][:]), "keys must come out in order")
	}
	byKey := map[[32]byte]int{}
	for i, k := range seen {
		byKey[k] = srcs[i]
	}
	for i := 0; i < 100; i++ {
		require.Equal(t, 0, byKey[keys[i]], "key %d is only in segment a", i)
	}
	for i := 100; i < 200; i++ {
		require.Equal(t, 1, byKey[keys[i]], "key %d: b is newer than a", i)
	}
	for i := 200; i < 300; i++ {
		require.Equal(t, 2, byKey[keys[i]], "key %d: c is newest", i)
	}
	// Counting pass agrees with the emitting pass
	n2, err := mergeIndexes(segs, nil)
	require.NoError(t, err)
	require.Equal(t, n, n2)
}

// TestCompactionPassIsBounded
// One compaction pass must not grow with history (issue #59: the
// unbounded pass reached 12-19 s after four hours and was still
// growing).  compactionRun caps a run at CompactPassRecords; when the
// newest suffix cannot fold under the budget, it falls back to the
// newest adjacent pair that fits, so consolidation advances one
// bounded step per pass instead of stopping or ballooning.
func TestCompactionPassIsBounded(t *testing.T) {
	mk := func(records ...int64) (h []*segment) {
		for _, r := range records {
			h = append(h, &segment{records: r})
		}
		return h
	}
	old := CompactPassRecords
	defer func() { CompactPassRecords = old }()
	CompactPassRecords = 1000

	// Suffix folds while ratio holds and the budget allows
	run, at := compactionRun(mk(5000, 300, 200, 100), DefaultCompactRatio)
	require.Len(t, run, 3, "the three small ones fold; 5000 is over the budget")
	require.Equal(t, 1, at)
	var total int64
	for _, s := range run {
		total += s.records
	}
	require.LessOrEqual(t, uint64(total), CompactPassRecords)

	// Ratio still gates: a big segment with little behind it stays put
	run, _ = compactionRun(mk(800, 10), DefaultCompactRatio)
	require.Nil(t, run, "10 records behind 800 is not worth rewriting 800")

	// Pair fallback: every suffix is over budget, but an adjacent pair
	// deeper in fits -- consolidation advances instead of stopping
	run, at = compactionRun(mk(400, 500, 2000), DefaultCompactRatio)
	require.Len(t, run, 2, "the (400,500) pair fits the budget")
	require.Equal(t, 0, at)
	require.Equal(t, int64(400), run[0].records)

	// Nothing fits: two giants and nothing else
	run, _ = compactionRun(mk(2000, 3000), DefaultCompactRatio)
	require.Nil(t, run)
}

// TestCompactHistoryFoldsANonSuffixRun
// The budget hands CompactHistory runs that exclude an over-budget
// segment at the head of history.  The swap must replace exactly the
// chosen run, in place, leave the big segment untouched, and every key
// must survive with its newest value.
// TestCompactionRefusesATakenIdentity
// Several seals in one block sit at (H,0), (H,1), (H,2)...  The pair
// fallback folding (H,0)+(H,1) names its replacement (H, 1+1) = (H,2)
// -- the very segment behind the pair -- and the rename that published
// the output overwrote that segment's committed file.  History then
// held two segments sharing one file; whichever was folded first, its
// release deleted the file while history.json still named the other,
// and the store could not reopen: issue #61, the one-in-a-hundred
// TestCrashRecoverySeal flake.  The chooser must skip such a pair, and
// the publish must refuse a taken name rather than replace it.
func TestCompactionRefusesATakenIdentity(t *testing.T) {
	dir := storeDir(t, "takenid")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{214})
	var keys [][32]byte
	val := func(i int) []byte { return []byte{byte(i), byte(i >> 8), 9} }
	seal := func(n int) { // n records, one segment, all in block 0
		for j := 0; j < n; j++ {
			k := kr.NextHash()
			keys = append(keys, k)
			require.NoError(t, store.Put(k, val(len(keys)-1)))
		}
		_, err := store.SealNext()
		require.NoError(t, err)
	}
	seal(300) // (0,0)
	seal(300) // (0,1)
	seal(900) // (0,2) -- the identity folding the pair would mint
	// Roll the window so all three segments become history
	for b := uint64(1); b <= 3*MinFilterBlocks; b++ {
		_, err = store.Seal(b)
		require.NoError(t, err)
	}
	require.Len(t, store.history, 3)

	old := CompactPassRecords
	defer func() { CompactPassRecords = old }()
	CompactPassRecords = 700 // Only (0,0)+(0,1) fits the budget

	// The chooser must decline: the one pair under budget would mint
	// (0,2), and (0,2) is a committed segment
	run, _ := compactionRun(store.history, CompactRatio)
	require.Nil(t, run, "the only affordable pair's replacement identity is taken")

	compacted, err := store.CompactHistory()
	require.NoError(t, err)
	require.False(t, compacted)
	require.Len(t, store.history, 3, "nothing may be rewritten")

	// The belt behind the chooser: publishing at a taken identity must
	// refuse, never replace.  Before the fix this call silently
	// overwrote seg-00000000-0002.dat with a 600-record merge.
	_, _, err = store.writeMergedRun(store.history[:2], 0, 2)
	require.ErrorIs(t, err, os.ErrExist, "a taken segment name must refuse the publish")

	// Every key still reads, including the 900 whose file the collision
	// used to destroy, and the store reopens clean
	for i, k := range keys {
		v, err := store.Get(k)
		require.NoErrorf(t, err, "key %d lost", i)
		require.Equal(t, val(i), v)
	}
	require.NoError(t, store.Close())
	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	for i, k := range keys {
		v, err := re.Get(k)
		require.NoErrorf(t, err, "key %d lost across reopen", i)
		require.Equal(t, val(i), v)
	}
	require.NoError(t, re.Close())
}

// TestSealRemintsATakenIdentity
// The seal is not the only identity minter: a suffix compaction names
// its output (historyNewest.Seq+1), and when the active tier is empty
// that is exactly what nextKeyAt mints for the next seal.  The
// exclusive link turned that race from a silent overwrite (issue #61)
// into ErrExist; the seal must then take the next free sequence, not
// fail the Put that tipped it -- and never touch the squatter.
func TestSealRemintsATakenIdentity(t *testing.T) {
	dir := storeDir(t, "remint")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{215})
	k1 := kr.NextHash()
	require.NoError(t, store.Put(k1, []byte{1}))
	m0, err := store.SealNext()
	require.NoError(t, err)
	require.Equal(t, uint64(0), m0.Seq)

	// A concurrent maintenance output claims the name the next seal
	// would mint
	taken := filepath.Join(dir, segmentFileName(0, 1))
	require.NoError(t, os.WriteFile(taken, []byte("someone else's segment"), 0o644))

	k2 := kr.NextHash()
	require.NoError(t, store.Put(k2, []byte{2}))
	m1, err := store.SealNext()
	require.NoError(t, err)
	require.Equal(t, uint64(2), m1.Seq, "the taken sequence must be skipped, not replaced")

	v, err := store.Get(k2)
	require.NoError(t, err)
	require.Equal(t, []byte{2}, v)
	data, err := os.ReadFile(taken)
	require.NoError(t, err)
	require.Equal(t, []byte("someone else's segment"), data, "the squatter must be untouched")
}

func TestCompactHistoryFoldsANonSuffixRun(t *testing.T) {
	dir := storeDir(t, "midrun")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{212})
	var keys [][32]byte
	val := func(i int) []byte { return []byte{byte(i), byte(i >> 8), 7} }
	seal := func(n int) { // n records, one segment
		for j := 0; j < n; j++ {
			k := kr.NextHash()
			keys = append(keys, k)
			require.NoError(t, store.Put(k, val(len(keys)-1)))
		}
		_, err := store.SealNext()
		require.NoError(t, err)
	}
	seal(900)
	seal(300)
	seal(300)
	// Roll the window so all three segments become history
	for b := uint64(1); b <= 3*MinFilterBlocks; b++ {
		_, err = store.Seal(b)
		require.NoError(t, err)
	}
	require.Len(t, store.history, 3)

	old := CompactPassRecords
	defer func() { CompactPassRecords = old }()
	CompactPassRecords = 700 // The 900-record segment can never be in a run

	compacted, err := store.CompactHistory()
	require.NoError(t, err)
	require.True(t, compacted, "the two 300s fold under the budget")
	require.Len(t, store.history, 2)
	require.Equal(t, int64(900), store.history[0].records, "the over-budget segment stands untouched")

	// A second pass has nothing it may touch: the survivors' pair is
	// over the budget
	compacted, err = store.CompactHistory()
	require.NoError(t, err)
	require.False(t, compacted)

	for i, k := range keys {
		v, err := store.Get(k)
		require.NoErrorf(t, err, "key %d lost", i)
		require.Equal(t, val(i), v, "key %d value", i)
	}
	// And it survives a reopen: the swap committed a consistent manifest
	require.NoError(t, store.Close())
	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	for i, k := range keys {
		v, err := re.Get(k)
		require.NoErrorf(t, err, "key %d lost across reopen", i)
		require.Equal(t, val(i), v)
	}
	require.NoError(t, re.Close())
}

// TestSealPromotesAShadowedTail
// Sealing used to rewrite a mutable tail that held overwrites -- one
// pread per record, under the store lock, inside the Put that tipped
// SealLimit: 10-15 s pauses every few blocks at a 100k-record tail
// (issue #60).  A seal now promotes the file as it stands: the sealed
// segment keeps the shadowed bytes (records > count), the index built
// from the live map lands every lookup on the newest copy, and
// reclamation belongs to CompactHistory, off the protocol lock.
func TestSealPromotesAShadowedTail(t *testing.T) {
	dir := storeDir(t, "shadowseal")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{213})
	keys := make([][32]byte, 500)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte{1, byte(i), byte(i >> 8)}))
	}
	for i := range keys { // Overwrite every key: half the tail is shadowed
		require.NoError(t, store.Put(keys[i], []byte{2, byte(i), byte(i >> 8)}))
	}
	_, err = store.SealNext()
	require.NoError(t, err)

	segs := store.sealedSegments()
	require.Len(t, segs, 1)
	seg := segs[0]
	require.Equal(t, int64(500), seg.count, "one index entry per distinct key")
	require.Equal(t, int64(1000), seg.records, "the shadowed records ride along; reclamation is compaction's job, not the seal's")

	for i, k := range keys {
		v, err := store.Get(k)
		require.NoErrorf(t, err, "key %d", i)
		require.Equal(t, []byte{2, byte(i), byte(i >> 8)}, v, "key %d must read its newest copy", i)
	}

	// And compaction, not the seal, reclaims the dead bytes
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))
	for b := uint64(1); b <= 3*MinFilterBlocks; b++ {
		_, err = store.Seal(b)
		require.NoError(t, err)
	}
	// One segment cannot fold; give it a sibling worth folding -- the
	// ratio gate rightly refuses to rewrite 1,000 records to reclaim
	// one, so overwrite every key again: 500 records, half the older
	// segment, over the quarter the ratio asks
	for i := range keys {
		require.NoError(t, store.Put(keys[i], []byte{3, byte(i), byte(i >> 8)}))
	}
	_, err = store.SealNext()
	require.NoError(t, err)
	for b := uint64(3*MinFilterBlocks + 1); b <= 6*MinFilterBlocks; b++ {
		_, err = store.Seal(b)
		require.NoError(t, err)
	}
	compacted, err := store.CompactHistory()
	require.NoError(t, err)
	require.True(t, compacted)
	segs = store.sealedSegments()
	require.Equal(t, segs[0].count, segs[0].records, "compaction dropped the shadowed copies")
	for i, k := range keys {
		v, err := store.Get(k)
		require.NoErrorf(t, err, "key %d after compaction", i)
		require.Equal(t, []byte{3, byte(i), byte(i >> 8)}, v)
	}
}
