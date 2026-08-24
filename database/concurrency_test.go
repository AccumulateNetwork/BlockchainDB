package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentShardAccess
// Hammers a KVShard from many goroutines (run under -race in CI to
// catch data races).  Each goroutine owns a disjoint key space; all
// values are verified at the end.  Regression test for issue #8.
func TestConcurrentShardAccess(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kvs, err := NewKVShard(dir, 64, 10_000, 5)
	require.NoError(t, err, "create kvshard")

	const workers = 8
	const opsPerWorker = 500

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	allKeys := make([][][32]byte, workers)
	allVals := make([][][]byte, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			kr := NewFastRandom([]byte{byte(w), 1})
			vr := NewFastRandom([]byte{byte(w), 2})
			keys := make([][32]byte, opsPerWorker)
			vals := make([][]byte, opsPerWorker)
			for i := 0; i < opsPerWorker; i++ {
				keys[i] = kr.NextHash()
				vals[i] = vr.RandBuff(10, 100)
				var err error
				if i%2 == 0 {
					err = kvs.PutPerm(keys[i], vals[i])
				} else {
					err = kvs.PutDyna(keys[i], vals[i])
				}
				if err != nil {
					errs <- fmt.Errorf("worker %d put %d: %v", w, i, err)
					return
				}
				// Interleave reads of this worker's earlier writes
				if i > 0 {
					j := int(kr.UintN(uint(i)))
					v, err := kvs.Get(keys[j])
					if err != nil {
						errs <- fmt.Errorf("worker %d get %d: %v", w, j, err)
						return
					}
					if string(v) != string(vals[j]) {
						errs <- fmt.Errorf("worker %d got wrong value for key %d", w, j)
						return
					}
				}
			}
			allKeys[w] = keys
			allVals[w] = vals
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// Verify every key from every worker
	for w := 0; w < workers; w++ {
		for i, key := range allKeys[w] {
			v, err := kvs.Get(key)
			require.NoErrorf(t, err, "worker %d key %d missing after concurrent writes", w, i)
			assert.Equalf(t, allVals[w][i], v, "worker %d key %d wrong value", w, i)
		}
	}
	require.NoError(t, kvs.Close())
}

// TestConcurrentViewAccess
// Concurrent Put/Get through the KVView wrapper while views are being
// created and expiring (run under -race).
func TestConcurrentViewAccess(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	sdbv, err := NewShardDBViews(dir, 200*time.Millisecond, 1, 256, 1, 64, 10_000, 5)
	require.NoError(t, err, "create views")

	const workers = 4
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			kr := NewFastRandom([]byte{byte(w), 3})
			vr := NewFastRandom([]byte{byte(w), 4})
			for i := 0; i < 200; i++ {
				key := kr.NextHash()
				_ = sdbv.Put(key, vr.RandBuff(10, 50))
				_, _ = sdbv.Get(key)
				if i%50 == 0 {
					view := sdbv.NewView()
					_, _ = view.Get(key)
				}
			}
		}(w)
	}
	wg.Wait()
	require.NoError(t, sdbv.Close())
}
