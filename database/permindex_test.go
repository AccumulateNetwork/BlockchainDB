package blockchainDB

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A run round-trips: written sorted with its filter, reopened with the
// checksum verified, looked up resident and cold, merged newest-wins.
func TestPermRunWriteLookupMerge(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, permRunName(7))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	defer f.Close()
	fr := NewFastRandom([]byte{11})
	older := make([]permRecord, 5000)
	for i := range older {
		older[i] = permRecord{key: fr.NextHash(), file: 1, off: uint32(i * 100), n: 64}
	}
	sortPermRecords(older)
	// The newer run rewrites a tenth of the older keys and adds new ones
	newer := make([]permRecord, 0, 1000)
	for i := 0; i < 500; i++ {
		r := older[i*10]
		r.file, r.off = 2, uint32(i)
		newer = append(newer, r)
	}
	for i := 0; i < 500; i++ {
		newer = append(newer, permRecord{key: fr.NextHash(), file: 2, off: uint32(1000 + i), n: 32})
	}
	sortPermRecords(newer)

	r1, err := writePermRun(f, 0, 7, older, 1)
	require.NoError(t, err)
	r2, err := writePermRun(f, int64(r1.bytes), 7, newer, 2)
	require.NoError(t, err)
	_, err = writePermRun(f, int64(r1.bytes+r2.bytes), 7, []permRecord{older[3], older[2]}, 3)
	require.Error(t, err, "unsorted records are refused")

	// Reopen: resident and cold
	rr, err := openPermRun(f, 7, 0, true)
	require.NoError(t, err)
	require.EqualValues(t, 5000, rr.count)
	require.EqualValues(t, 1, rr.height)
	cold, err := openPermRun(f, 7, int64(r1.bytes), false)
	require.NoError(t, err)
	require.Nil(t, cold.bloom)
	for _, rec := range older[:200] {
		got, found, err := rr.lookup(f, rec.key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, rec, got)
	}
	for _, rec := range newer[:200] {
		got, found, err := cold.lookup(f, rec.key)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, rec, got)
	}
	misses := 0
	for i := 0; i < 2000; i++ {
		_, found, err := rr.lookup(f, fr.NextHash())
		require.NoError(t, err)
		if !found {
			misses++
		}
	}
	require.Equal(t, 2000, misses, "an absent key is never found")

	// Merge: the newer copy of a rewritten key wins
	recs1, err := rr.records(f)
	require.NoError(t, err)
	recs2, err := cold.records(f)
	require.NoError(t, err)
	merged := mergePermRuns([][]permRecord{recs1, recs2})
	require.Len(t, merged, 5500)
	byKey := map[[32]byte]permRecord{}
	for _, r := range merged {
		byKey[r.key] = r
	}
	for _, r := range newer {
		require.Equal(t, r, byKey[r.key])
	}
	require.EqualValues(t, 1, byKey[older[1].key].file, "an unrewritten key keeps its older record")

	// A damaged run is refused
	_, err = f.WriteAt([]byte{0xff, 0xff}, permRunHdr+40)
	require.NoError(t, err)
	_, err = openPermRun(f, 7, 0, true)
	require.ErrorContains(t, err, "checksum")
}
