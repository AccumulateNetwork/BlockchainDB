package blockchainDB

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file cover the create -> write -> close -> open -> read
// round trip.  See issue #3: no test reopened a database, which let several
// reopen-path bugs go undetected.

// TestKV2Reopen
// A KV2 (Perm + Dyna layers) must return keys from both layers after reopen
func TestKV2Reopen(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	defer os.RemoveAll(dir)

	kv2, err := NewKV2(dir, 50)
	require.NoError(t, err, "create kv2")

	kr := NewFastRandom([]byte{4})
	vr := NewFastRandom([]byte{4, 4})
	var keys [][32]byte
	var values [][]byte
	for i := 0; i < 100; i++ {
		key := kr.NextHash()
		value := vr.RandBuff(10, 100)
		if i%2 == 0 {
			_, err = kv2.PutPerm(key, value)
		} else {
			_, err = kv2.PutDyna(key, value)
		}
		require.NoError(t, err, "put failed")
		keys = append(keys, key)
		values = append(values, value)
	}
	require.NoError(t, kv2.Close(), "close kv2")

	reopened, err := OpenKV2(dir)
	require.NoError(t, err, "reopen kv2")
	require.NoError(t, reopened.Open(), "open kv2")
	for i, key := range keys {
		v, err := reopened.Get(key)
		require.NoErrorf(t, err, "get failed for key %d", i)
		assert.Equalf(t, values[i], v, "wrong value for key %d", i)
	}
	require.NoError(t, reopened.Close(), "close kv2 again")
}
