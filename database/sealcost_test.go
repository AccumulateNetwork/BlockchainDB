package blockchainDB

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSealBlockCost
// What a block boundary costs on an 8-shard store, and what a reader
// of the store pays while one is in progress (issue #84).  A
// measurement: it reports, it asserts nothing, and it is skipped in
// -short.  Run it on a real disk; a runner's virtual disk answers an
// fsync too fast to show anything.
//
// The workload is a block of ~100 permanent and ~100 dynamic records
// per shard, which is what a validator at a few hundred tx/s hands a
// shard per block, followed by SealBlock.  A reader goroutine runs
// Gets of earlier keys throughout, and records how long each took
// while a SealBlock was in flight.
func TestSealBlockCost(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement; skipped in -short")
	}
	const blocks = 30
	const perBlock = 800 // ~100 per layer per shard

	dir := storeDir(t, "sealblockcost")
	kvs, err := NewKVShardN(dir, 8, 100_000)
	require.NoError(t, err)
	defer kvs.Close()

	kr := NewFastRandom([]byte{84, 84})
	vr := NewFastRandom([]byte{84, 85})
	var known [][32]byte
	var knownMu sync.RWMutex

	var sealing atomic.Bool
	var readsDuringSeals []time.Duration
	var readMu sync.Mutex
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		rr := NewFastRandom([]byte{84, 86})
		for {
			select {
			case <-stop:
				return
			default:
			}
			knownMu.RLock()
			n := len(known)
			var key [32]byte
			if n > 0 {
				key = known[int(rr.Uint64()%uint64(n))]
			}
			knownMu.RUnlock()
			if n == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			inSeal := sealing.Load()
			start := time.Now()
			_, _ = kvs.Get(key)
			took := time.Since(start)
			if inSeal && sealing.Load() {
				readMu.Lock()
				readsDuringSeals = append(readsDuringSeals, took)
				readMu.Unlock()
			}
		}
	}()

	var seals []time.Duration
	for b := uint64(1); b <= blocks; b++ {
		var keys [][32]byte
		for i := 0; i < perBlock; i++ {
			k := kr.NextHash()
			v := vr.RandBuff(100, 300)
			if i%2 == 0 {
				require.NoError(t, kvs.PutPerm(k, v))
			} else {
				require.NoError(t, kvs.PutDyna(k, v))
			}
			keys = append(keys, k)
		}
		knownMu.Lock()
		known = append(known, keys...)
		knownMu.Unlock()

		sealing.Store(true)
		start := time.Now()
		require.NoError(t, kvs.SealBlock(b))
		seals = append(seals, time.Since(start))
		sealing.Store(false)
	}
	close(stop)
	readers.Wait()

	report := func(name string, ds []time.Duration) {
		if len(ds) == 0 {
			t.Logf("%-28s none", name)
			return
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		var sum time.Duration
		for _, d := range ds {
			sum += d
		}
		pct := func(p float64) time.Duration { return ds[int(float64(len(ds)-1)*p)] }
		t.Logf("%-28s n=%-6d mean=%-10v p50=%-10v p90=%-10v max=%v",
			name, len(ds), (sum / time.Duration(len(ds))).Round(time.Microsecond),
			pct(0.5).Round(time.Microsecond), pct(0.9).Round(time.Microsecond), ds[len(ds)-1].Round(time.Microsecond))
	}
	report("SealBlock, 8 shards", seals)
	report("Get during a SealBlock", readsDuringSeals)
}
