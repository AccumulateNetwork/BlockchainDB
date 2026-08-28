package blockchainDB

import (
	"fmt"
	"testing"

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
