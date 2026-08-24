package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardWriter
// Correctness of the async ingest path: values land, per-key write
// order is preserved (overwrites end at the last value), Flush is a
// real barrier, and Close is clean.  Run under -race in CI.
func TestShardWriter(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kvs, err := NewKVShard(dir, 64, 10_000, 5)
	require.NoError(t, err, "create kvshard")

	writer := kvs.NewShardWriter(8, 256)

	// Queue perm and dyna writes
	kr := NewFastRandom([]byte{21})
	vr := NewFastRandom([]byte{21, 21})
	const n = 2000
	keys := make([][32]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = kr.NextHash()
		vals[i] = vr.RandBuff(20, 100)
		if i%2 == 0 {
			require.NoError(t, writer.PutPerm(keys[i], vals[i]))
		} else {
			require.NoError(t, writer.PutDyna(keys[i], vals[i]))
		}
	}

	// Overwrite a few dyna keys many times: per-key ordering must hold,
	// so the LAST queued value must win
	final := make(map[int][]byte)
	for round := 0; round < 20; round++ {
		for i := 1; i < 40; i += 2 {
			v := vr.RandBuff(20, 100)
			require.NoError(t, writer.PutDyna(keys[i], v))
			final[i] = v
		}
	}

	// Flush is the consistency barrier: after it, everything must read back
	require.NoError(t, writer.Flush())
	for i := 0; i < n; i++ {
		expected := vals[i]
		if v, ok := final[i]; ok {
			expected = v
		}
		var got []byte
		var err error
		if i%2 == 0 {
			got, err = kvs.GetPerm(keys[i])
		} else {
			got, err = kvs.GetDyna(keys[i])
		}
		require.NoErrorf(t, err, "key %d missing after flush", i)
		assert.Equalf(t, expected, got, "key %d has wrong value", i)
	}

	// Multiple flushes are fine; writes queue after a flush too
	extra := kr.NextHash()
	require.NoError(t, writer.PutPerm(extra, []byte("after-flush")))
	require.NoError(t, writer.Flush())
	v, err := kvs.GetPerm(extra)
	require.NoError(t, err)
	assert.Equal(t, []byte("after-flush"), v)

	// Close drains and stops; later calls return ErrWriterClosed
	require.NoError(t, writer.Close())
	assert.Equal(t, ErrWriterClosed, writer.PutPerm(extra, []byte("x")))
	assert.Equal(t, ErrWriterClosed, writer.Flush())
	require.NoError(t, writer.Close()) // Idempotent

	require.NoError(t, kvs.Close())
}

// TestShardWriterConcurrentProducers
// Multiple producers feeding one writer while another goroutine
// flushes.  Run under -race in CI.
func TestShardWriterConcurrentProducers(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kvs, err := NewKVShard(dir, 64, 10_000, 5)
	require.NoError(t, err, "create kvshard")
	writer := kvs.NewShardWriter(8, 64)

	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			kr := NewFastRandom([]byte{byte(p), 31})
			vr := NewFastRandom([]byte{byte(p), 32})
			for i := 0; i < 500; i++ {
				_ = writer.PutPerm(kr.NextHash(), vr.RandBuff(10, 50))
				if i%100 == 0 {
					_ = writer.Flush()
				}
			}
		}(p)
	}
	wg.Wait()
	require.NoError(t, writer.Close())
	require.NoError(t, writer.Err())
	require.NoError(t, kvs.Close())
}

// TestMultiCoreScaling
// Measures write throughput of the synchronous concurrent path and the
// ShardWriter async path at several worker counts.  Reports numbers;
// asserts only that parallel ingest beats single-threaded.
func TestMultiCoreScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("scaling measurement; skipped in -short")
	}

	const opsPerConfig = 200_000
	const warmupOps = 100_000 // Untimed; faults in the fresh Bloom filter pages
	const minVal, maxVal = 100, 500

	// First-touch page faults on the 10MB-per-KFile Bloom filters (issue
	// #12) dominate a cold KVShard, so each configuration warms up with
	// untimed puts before the measured run.
	warmup := func(kvs *KVShard) {
		kr := NewFastRandom([]byte{61})
		vr := NewFastRandom([]byte{62})
		for i := 0; i < warmupOps; i++ {
			_ = kvs.PutPerm(kr.NextHash(), vr.RandBuff(minVal, maxVal))
		}
	}

	// Synchronous path: W goroutines calling kvs.PutPerm directly
	syncRun := func(workers int) float64 {
		dir := filepath.Join(os.TempDir(), fmt.Sprintf("Scale_sync_%d", workers))
		os.RemoveAll(dir)
		defer os.RemoveAll(dir)
		kvs, err := NewKVShard(dir, 1024, 100_000, 50)
		require.NoError(t, err)
		defer kvs.Close()
		warmup(kvs)

		start := time.Now()
		var wg sync.WaitGroup
		per := opsPerConfig / workers
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				kr := NewFastRandom([]byte{byte(w), 41})
				vr := NewFastRandom([]byte{byte(w), 42})
				for i := 0; i < per; i++ {
					if err := kvs.PutPerm(kr.NextHash(), vr.RandBuff(minVal, maxVal)); err != nil {
						t.Error(err)
						return
					}
				}
			}(w)
		}
		wg.Wait()
		return float64(per*workers) / time.Since(start).Seconds()
	}

	// Async path: one producer feeding a ShardWriter with W workers
	asyncRun := func(workers int) float64 {
		dir := filepath.Join(os.TempDir(), fmt.Sprintf("Scale_async_%d", workers))
		os.RemoveAll(dir)
		defer os.RemoveAll(dir)
		kvs, err := NewKVShard(dir, 1024, 100_000, 50)
		require.NoError(t, err)
		defer kvs.Close()
		warmup(kvs)
		writer := kvs.NewShardWriter(workers, 1024)

		kr := NewFastRandom([]byte{51})
		vr := NewFastRandom([]byte{52})
		start := time.Now()
		for i := 0; i < opsPerConfig; i++ {
			if err := writer.PutPerm(kr.NextHash(), vr.RandBuff(minVal, maxVal)); err != nil {
				t.Fatal(err)
			}
		}
		require.NoError(t, writer.Flush())
		elapsed := time.Since(start).Seconds()
		require.NoError(t, writer.Close())
		return float64(opsPerConfig) / elapsed
	}

	single := syncRun(1)
	fmt.Printf("%-28s %12s\n", "configuration", "puts/sec")
	fmt.Printf("%-28s %12s\n", "sync 1 goroutine", humanize.Comma(int64(single)))
	var bestSync, bestAsync float64
	for _, w := range []int{4, 8, 16} {
		r := syncRun(w)
		if r > bestSync {
			bestSync = r
		}
		fmt.Printf("%-28s %12s\n", fmt.Sprintf("sync %d goroutines", w), humanize.Comma(int64(r)))
	}
	for _, w := range []int{4, 8, 16} {
		r := asyncRun(w)
		if r > bestAsync {
			bestAsync = r
		}
		fmt.Printf("%-28s %12s\n", fmt.Sprintf("async 1 producer, %d workers", w), humanize.Comma(int64(r)))
	}

	assert.Greater(t, bestSync, single, "parallel sync should beat single-threaded")
	assert.Greater(t, bestAsync, single, "async ingest should beat single-threaded")
}
