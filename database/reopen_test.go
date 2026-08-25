package blockchainDB

import (
	"math"
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

// TestSealLimitSurvivesReopen
// Regression test: sealLimit is the only parameter NewKV2 and
// NewKVShard take, and it used not to be persisted -- OpenKV2 could
// not set it and Open substituted DefaultBloomCapacity, so every
// restart silently ran at 100,000 no matter what the database was
// built with.  v1's Header round-tripped the equivalent (KeyLimit);
// retiring v1 deleted the last implementation of that guarantee.
func TestSealLimitSurvivesReopen(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	defer os.RemoveAll(dir)

	const sealLimit = 250
	kv2, err := NewKV2(dir, sealLimit)
	require.NoError(t, err, "create kv2")
	require.Equal(t, sealLimit, kv2.SealLimit)

	kr := NewFastRandom([]byte{9})
	vr := NewFastRandom([]byte{9, 9})
	for i := 0; i < 40; i++ {
		_, err = kv2.PutPerm(kr.NextHash(), vr.RandBuff(10, 40))
		require.NoError(t, err)
	}
	require.NoError(t, kv2.Close(), "close kv2")

	reopened, err := OpenKV2(dir)
	require.NoError(t, err, "reopen kv2")
	require.NoError(t, reopened.Open(), "open kv2")
	assert.Equal(t, sealLimit, reopened.SealLimit,
		"reopened database must seal at the limit it was built with, not a default")
	require.NoError(t, reopened.Close())
}

// TestSealLimitRejectsOversizedValue
// SealLimit is an int and sealPermIfFull/sealDynaIfFull read "<= 0" as
// sealing disabled, so a limit at or above 2^31 used to narrow to a
// negative number and silently mean "never seal": an unbounded live
// tail, replayed in full on every open.
func TestSealLimitRejectsOversizedValue(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	defer os.RemoveAll(dir)

	_, err := NewKV2(dir, math.MaxUint64)
	require.Error(t, err, "an unrepresentable seal limit must be rejected, not silently disable sealing")

	_, err = NewKV2(dir, math.MaxInt32+1)
	require.Error(t, err)
}
