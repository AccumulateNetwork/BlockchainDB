package blockchainDB

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// "Not found" has to mean not found.
//
// A key that was never written reads as errNotFound, and that is an
// ordinary answer rather than a fault -- it happens constantly.  What
// may never happen is the same answer for a key the store holds, or
// for a read that did not complete.  These pin the difference at the
// three places the store used to blur it.

// TestAFailedReadIsNotAnAbsence
// KV2.Get asks the dynamic layer and then the permanent one.  It used
// to test only `err == nil`, so any dynamic-layer failure fell through
// to Perm, and a Perm miss reported "not found" -- an I/O error
// wearing the answer for a key that does not exist.
func TestAFailedReadIsNotAnAbsence(t *testing.T) {
	dir := storeDir(t, "failed-read")
	kv, err := NewKV2(dir, 1000)
	require.NoError(t, err)
	defer kv.Close()

	kr := NewFastRandom([]byte{201})
	dynaKey, permKey, never := kr.NextHash(), kr.NextHash(), kr.NextHash()
	_, err = kv.PutDyna(dynaKey, []byte("state"))
	require.NoError(t, err)
	_, err = kv.PutPerm(permKey, []byte("record"))
	require.NoError(t, err)

	// Undefined keys are not an error, and stay that way
	_, err = kv.Get(never)
	require.ErrorIs(t, err, errNotFound, "a key that was never written is absent, not broken")

	// Now break the dynamic layer under it
	require.NoError(t, kv.DynaKV.Close())

	_, err = kv.Get(dynaKey)
	require.Error(t, err, "a key the store holds must not read as absent")
	require.NotErrorIs(t, err, errNotFound,
		"the dynamic layer failed; reporting 'not found' claims the key does not exist")
	require.ErrorIs(t, err, errStoreClosed)

	// And the same for a key the dynamic layer simply does not hold:
	// the read still did not complete, so the answer is the failure
	_, err = kv.Get(permKey)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNotFound)

	_, err = kv.GetDeep(dynaKey)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNotFound, "GetDeep has the same two layers and the same rule")
}

// TestACorruptIndexHeaderIsRebuiltNotBelieved
// An index is its header, then `count` records, then the bloom.  A
// header that does not add up moves both the binary search's bound and
// the filter's offset, and either one answers "not here" for a key the
// segment holds.  An index is derived data, so the header is checked
// against the file and a bad one is rebuilt from the .dat.
//
// The other half of the rule is pinned by
// TestSegmentHeaderVersionsAreChecked: a wrong magic or version is
// REFUSED rather than rebuilt, because those say the file is not ours
// or not our format, and spec 1.10 does not migrate silently.
func TestACorruptIndexHeaderIsRebuiltNotBelieved(t *testing.T) {
	dir := storeDir(t, "corrupt-index")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{202})
	keys := make([][32]byte, 400)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte{byte(i), byte(i >> 8)}))
	}
	_, err = store.Seal(1)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// Halve the record count in the header.  Believed, it hides the
	// upper half of the keys from the binary search and puts the bloom
	// where the records are.
	idx := findOne(t, dir, segIndexSuffix)
	f, err := os.OpenFile(idx, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [segIndexHdrSize]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)
	require.Equal(t, uint64(len(keys)), binary.BigEndian.Uint64(hdr[8:]))
	binary.BigEndian.PutUint64(hdr[8:], uint64(len(keys)/2))
	_, err = f.WriteAt(hdr[:], 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	re, err := OpenSegmentStore(dir)
	require.NoError(t, err, "a rebuildable index must not fail the open")
	defer re.Close()
	for i, k := range keys {
		v, err := re.Get(k)
		require.NoErrorf(t, err, "key %d read as absent after a corrupt header", i)
		require.Equal(t, []byte{byte(i), byte(i >> 8)}, v)
	}
}

// TestACorruptSetHeaderIsRefused
// A block set is the only copy of what it holds, so there is nothing
// to rebuild it from.  A header that does not describe the file is
// refused loudly rather than answered from, because answering from it
// denies keys that are on disk.
func TestACorruptSetHeaderIsRefused(t *testing.T) {
	dir := storeDir(t, "corrupt-set")
	// Enough blocks for the window to roll, so there is history to pack
	kvs, keys, values := packedFixture(t, dir, 203, 3*MinFilterBlocks, 5)
	_, packed, err := kvs.PackFinalized(2 * MinFilterBlocks)
	require.NoError(t, err)
	require.True(t, packed, "the fixture must leave something packed")
	setFile := findOne(t, filepath.Join(dir, setDirName), setFileSuffix)
	require.NoError(t, kvs.Close())

	f, err := os.OpenFile(setFile, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [setHdrSize]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)
	binary.BigEndian.PutUint64(hdr[32:], binary.BigEndian.Uint64(hdr[32:])/2) // Keys
	_, err = f.WriteAt(hdr[:], 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = OpenKVShard(dir)
	require.Error(t, err, "a set whose header does not describe it must not be opened and answered from")
	require.Contains(t, err.Error(), "header describes")
	_ = keys
	_ = values
}

// findOne returns the single file under dir with the given suffix
func findOne(t *testing.T, dir, suffix string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var found []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), suffix) && !strings.HasSuffix(e.Name(), segTmpSuffix) {
			found = append(found, filepath.Join(dir, e.Name()))
		}
	}
	require.Lenf(t, found, 1, "expected one %s under %s, found %v", suffix, dir, found)
	return found[0]
}
