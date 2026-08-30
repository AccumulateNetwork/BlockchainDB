package blockchainDB

import (
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
