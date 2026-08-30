package blockchainDB

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestForEachSeesEveryCurrentValue
// Iteration has to see each key exactly once, with the value a Get
// would return -- which in a mutable store means the newest of several
// copies, not the first one found.
func TestForEachSeesEveryCurrentValue(t *testing.T) {
	for _, mutable := range []bool{false, true} {
		name := "immutable"
		if mutable {
			name = "mutable"
		}
		t.Run(name, func(t *testing.T) {
			dir := storeDir(t, name)
			store, err := NewSegmentStore(dir, mutable)
			require.NoError(t, err)

			kr := NewFastRandom([]byte{41})
			keys := make([][32]byte, 300)
			want := make(map[[32]byte]string, len(keys))
			for i := range keys {
				keys[i] = kr.NextHash()
			}

			// Spread the keys over several sealed segments and a tail
			for i, key := range keys {
				v := fmt.Sprintf("v%d", i)
				require.NoError(t, store.Put(key, []byte(v)))
				want[key] = v
				if (i+1)%70 == 0 {
					_, err = store.Seal(uint64(i / 70))
					require.NoError(t, err)
				}
			}
			if mutable {
				// Rewrite some keys so older segments hold stale copies
				for i := 0; i < len(keys); i += 3 {
					v := fmt.Sprintf("rewritten%d", i)
					require.NoError(t, store.Put(keys[i], []byte(v)))
					want[keys[i]] = v
				}
				_, err = store.SealNext()
				require.NoError(t, err)
			}

			got := map[[32]byte]string{}
			require.NoError(t, store.ForEach(func(key [32]byte, value []byte) error {
				_, dup := got[key]
				require.False(t, dup, "key emitted twice")
				got[key] = string(value)
				return nil
			}))
			require.Equal(t, len(want), len(got), "wrong number of keys")
			for key, v := range want {
				require.Equal(t, v, got[key], "iteration returned a stale value")
			}
			require.NoError(t, store.Close())
		})
	}
}

// TestKV2ForEachPrefersTheDynamicCopy
// A key that was rewritten lives in both layers: the original in Perm
// and the current value in Dyna.  Iteration must report it once, with
// the value Get answers with.
func TestKV2ForEachPrefersTheDynamicCopy(t *testing.T) {
	dir := storeDir(t, "kv2")
	kv, err := NewKV2(dir, 50)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{42})
	keys := make([][32]byte, 100)
	for i := range keys {
		keys[i] = kr.NextHash()
		_, err = kv.Put(keys[i], []byte(fmt.Sprintf("orig%d", i)))
		require.NoError(t, err)
	}
	// Rewrite half: these move to the Dyna layer, leaving Perm's copy
	for i := 0; i < len(keys); i += 2 {
		_, err = kv.Put(keys[i], []byte(fmt.Sprintf("new%d", i)))
		require.NoError(t, err)
	}

	got := map[[32]byte]string{}
	require.NoError(t, kv.ForEach(func(key [32]byte, value []byte) error {
		_, dup := got[key]
		require.False(t, dup, "key emitted twice across the two layers")
		got[key] = string(value)
		return nil
	}))
	require.Len(t, got, len(keys))
	for i, key := range keys {
		want := fmt.Sprintf("orig%d", i)
		if i%2 == 0 {
			want = fmt.Sprintf("new%d", i)
		}
		require.Equalf(t, want, got[key], "key %d", i)
		// And iteration agrees with what a lookup answers
		v, err := kv.Get(key)
		require.NoError(t, err)
		require.Equal(t, want, string(v), "iteration and Get disagree on key %d", i)
	}
	require.NoError(t, kv.Close())
}

// TestForEachStopsOnError
// A caller that gives up mid-iteration gets its error back, rather
// than the walk running to completion and swallowing it.
func TestForEachStopsOnError(t *testing.T) {
	dir := storeDir(t, "s")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{43})
	for i := 0; i < 100; i++ {
		require.NoError(t, store.Put(kr.NextHash(), []byte("v")))
	}
	_, err = store.Seal(1)
	require.NoError(t, err)

	stop := fmt.Errorf("enough")
	seen := 0
	err = store.ForEach(func([32]byte, []byte) error {
		if seen++; seen == 10 {
			return stop
		}
		return nil
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, 10, seen, "iteration continued past the error")
	require.NoError(t, store.Close())
}

// TestForEachCallbackMayUseTheStore
// The callback runs with no lock held, so it can read and write the
// store it is walking.  Both used to deadlock: ForEach took the
// store's mutex for the whole iteration, so the first Get inside the
// callback blocked forever on a lock its own caller held (issue #31).
//
// A deadlock hangs rather than failing, so this runs on its own
// goroutine against a deadline: a timeout is the failure.
func TestForEachCallbackMayUseTheStore(t *testing.T) {
	dir := storeDir(t, "cb")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	defer store.Close()

	kr := NewFastRandom([]byte{47})
	keys := make([][32]byte, 50)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte(fmt.Sprintf("v%d", i))))
	}
	_, err = store.Seal(1)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		seen := 0
		done <- store.ForEach(func(key [32]byte, value []byte) error {
			// Read the store from inside its own iteration
			got, err := store.Get(key)
			if err != nil {
				return fmt.Errorf("Get inside ForEach: %w", err)
			}
			if string(got) != string(value) {
				return fmt.Errorf("Get disagreed with ForEach for a key")
			}
			// And write to it: the snapshot means this is not re-emitted
			if seen == 0 {
				if err := store.Put(kr.NextHash(), []byte("added during iteration")); err != nil {
					return fmt.Errorf("Put inside ForEach: %w", err)
				}
			}
			seen++
			return nil
		})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("ForEach deadlocked: the callback could not use the store")
	}
}

// TestForEachSurvivesConcurrentCompaction
// A compaction that commits mid-iteration replaces the sealed
// generation and deletes the files the iteration is reading.  The
// iteration must still complete and still report the snapshot it
// started from.
func TestForEachSurvivesConcurrentCompaction(t *testing.T) {
	dir := storeDir(t, "compact")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{48})
	keys := make([][32]byte, 400)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte(fmt.Sprintf("v%d", i))))
		if i%100 == 99 {
			_, err = store.Seal(uint64(i/100 + 1))
			require.NoError(t, err)
		}
	}
	ageOut(t, store) // Compaction works on history: roll the window past them
	require.Greater(t, len(store.sealedSegments()), 1, "need several generations to compact away")

	compacted := make(chan error, 1)
	var once sync.Once
	seen := 0
	err = store.ForEach(func(key [32]byte, value []byte) error {
		seen++
		if seen == 10 { // Well into the sealed segments
			once.Do(func() {
				go func() {
					_, err := store.CompactHistory()
					compacted <- err
				}()
			})
			// Give the compaction a chance to commit and delete
			time.Sleep(200 * time.Millisecond)
		}
		return nil
	})
	require.NoError(t, err, "iteration must survive the files being retired under it")
	require.NoError(t, <-compacted, "the compaction itself must succeed")
	require.Equal(t, len(keys), seen, "the snapshot must still report every key")

	// And the store is intact afterwards
	for i, key := range keys {
		v, err := store.Get(key)
		require.NoErrorf(t, err, "key %d lost", i)
		require.Equal(t, fmt.Sprintf("v%d", i), string(v))
	}
}
