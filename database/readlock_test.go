package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Reads take the store's lock SHARED (issue #50).
//
// The test for that is deterministic rather than a timing: hold the
// lock shared from outside, then call Get.  Under the exclusive mutex
// the reads used to take, Get blocks forever -- the harness's timeout
// is the failure.  Under a shared lock it proceeds.

func TestSegmentStoreReadsShareTheLock(t *testing.T) {
	dir := storeDir(t, "rlock")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{65})
	sealed := kr.NextHash()
	require.NoError(t, store.Put(sealed, []byte("sealed")))
	_, err = store.Seal(1)
	require.NoError(t, err)
	live := kr.NextHash()
	require.NoError(t, store.Put(live, []byte("live")))

	store.Mutex.RLock() // Another reader is in the store
	defer store.Mutex.RUnlock()

	done := make(chan error, 1)
	go func() {
		for _, key := range [][32]byte{sealed, live} {
			if _, err := store.Get(key); err != nil {
				done <- err
				return
			}
		}
		_, err := store.Get(kr.NextHash()) // And an absent key
		if err != errNotFound {
			done <- fmt.Errorf("absent key: %v", err)
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Get blocked behind another reader: reads are not sharing the lock")
	}
}

func TestKV2ReadsShareTheLock(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)
	kv, err := NewKV2(dir, 1000)
	require.NoError(t, err)
	defer kv.Close()

	kr := NewFastRandom([]byte{66})
	perm, dyna := kr.NextHash(), kr.NextHash()
	_, err = kv.PutPerm(perm, []byte("p"))
	require.NoError(t, err)
	_, err = kv.PutDyna(dyna, []byte("d"))
	require.NoError(t, err)

	kv.Mutex.RLock()
	defer kv.Mutex.RUnlock()

	done := make(chan error, 1)
	go func() {
		if _, err := kv.Get(perm); err != nil {
			done <- err
			return
		}
		if _, err := kv.GetPerm(perm); err != nil {
			done <- err
			return
		}
		_, err := kv.GetDyna(dyna)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("KV2.Get blocked behind another reader")
	}
}

// TestConcurrentReadersAndAWriter
// Many readers against sealed data and the live tail while one writer
// appends and seals.  Run under -race: the point is that the shared
// lock, the atomic counters, and pread on the live tail are enough.
func TestConcurrentReadersAndAWriter(t *testing.T) {
	dir := storeDir(t, "race")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{67})
	keys := make([][32]byte, 2000)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte(fmt.Sprintf("v%d", i))))
		if i%250 == 249 {
			_, err = store.Seal(uint64(i/250 + 1))
			require.NoError(t, err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rr := NewFastRandom([]byte{byte(r), 68})
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := int(rr.UintN(uint(len(keys))))
				v, err := store.Get(keys[i])
				if err != nil {
					t.Errorf("reader %d: key %d: %v", r, i, err)
					return
				}
				if string(v) != fmt.Sprintf("v%d", i) {
					t.Errorf("reader %d: key %d: wrong value %q", r, i, v)
					return
				}
			}
		}(r)
	}
	// The writer keeps appending and sealing underneath them
	for i := 0; i < 500; i++ {
		require.NoError(t, store.Put(kr.NextHash(), []byte("w")))
		if i%100 == 99 {
			_, err = store.Seal(uint64(9 + i/100))
			require.NoError(t, err)
		}
	}
	close(stop)
	wg.Wait()
	st := store.Stats()
	require.Greater(t, st.LookupTotal, uint64(0), "the counters must have counted under the shared lock")
}
