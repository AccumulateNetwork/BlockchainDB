package blockchainDB

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The on-disk format carries a version, and every reader checks it.
//
// The check is strict on purpose.  No database predates it, so there is
// nothing to be compatible with, and a store that refuses to open says
// what is wrong at the one moment somebody can act on it.  The
// alternative -- opening anyway and working around what is missing --
// is how a format change becomes silent data loss: the fields added to
// the manifest so far all happen to have zero values that degrade
// safely, but nothing enforced that, and the next one need not.

// TestManifestVersionIsWrittenAndChecked
func TestManifestVersionIsWrittenAndChecked(t *testing.T) {
	dir := storeDir(t, "ver")
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.Put(NewFastRandom([]byte{61}).NextHash(), []byte("v")))
	_, err = store.Seal(1)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	path := filepath.Join(dir, segManifestName)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var m StoreManifest
	require.NoError(t, json.Unmarshal(data, &m))
	assert.Equal(t, uint32(StoreFormatVersion), m.Version, "a manifest must record the format it was written in")

	// A version this build does not know must be refused, not opened
	for _, v := range []uint32{0, StoreFormatVersion + 1} {
		m.Version = v
		out, err := json.MarshalIndent(&m, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, out, 0644))

		_, err = OpenSegmentStore(dir)
		require.Errorf(t, err, "format version %d must be refused", v)
		assert.Containsf(t, err.Error(), "format version", "the error must say what is wrong: %v", err)
	}
}

// TestSegmentHeaderVersionsAreChecked
// Both segment headers carried a version from the start and neither was
// ever read: openSegment took the record count on trust, so a file that
// was not a segment at all was parsed as though it were.
func TestSegmentHeaderVersionsAreChecked(t *testing.T) {
	for _, tc := range []struct {
		name   string
		suffix string
		offset int64
		want   string
	}{
		{"data version", segDataSuffix, 4, "segment format version"},
		{"data magic", segDataSuffix, 0, "not a segment file"},
		{"index version", segIndexSuffix, 4, "index format version"},
		{"index magic", segIndexSuffix, 0, "not a segment index"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := storeDir(t, "hdr")
			store, err := NewSegmentStore(dir, false)
			require.NoError(t, err)
			require.NoError(t, store.Put(NewFastRandom([]byte{62}).NextHash(), []byte("v")))
			meta, err := store.Seal(1)
			require.NoError(t, err)
			require.NoError(t, store.Close())

			name := meta.File
			if tc.suffix == segIndexSuffix {
				name = name[:len(name)-len(segDataSuffix)] + segIndexSuffix
			}
			path := filepath.Join(dir, name)
			f, err := os.OpenFile(path, os.O_RDWR, 0644)
			require.NoError(t, err)
			var bad [4]byte
			binary.BigEndian.PutUint32(bad[:], 0xDEADBEEF)
			_, err = f.WriteAt(bad[:], tc.offset)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			_, err = OpenSegmentStore(dir)
			require.Error(t, err, "a corrupt header must be refused")
			assert.Contains(t, err.Error(), tc.want, "the error must name the problem")
		})
	}
}

// TestBlockRecordVersionIsChecked
// block.json is the only record of which block the quiet shards are in,
// so guessing when it cannot be read would let them tag segments with a
// block they do not belong to (issue #32).
func TestBlockRecordVersionIsChecked(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kvs, err := NewKVShard(dir, 1000)
	require.NoError(t, err)
	require.NoError(t, kvs.PutPerm(NewFastRandom([]byte{63}).NextHash(), []byte("v")))
	require.NoError(t, kvs.SealBlock(1))
	require.NoError(t, kvs.Close())

	path := filepath.Join(dir, blockFileName)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var rec blockRecord
	require.NoError(t, json.Unmarshal(data, &rec))
	assert.Equal(t, uint32(StoreFormatVersion), rec.Version)
	assert.Equal(t, uint64(2), rec.BlockHeight, "after sealing block 1 the set accumulates into block 2")

	rec.Version = StoreFormatVersion + 1
	out, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, out, 0644))

	_, err = OpenKVShard(dir)
	require.Error(t, err, "a block record this build cannot read must be refused, not guessed at")
	assert.Contains(t, err.Error(), "format version")

	// A MISSING record is different: a set that never sealed a block has
	// none, and 0 constrains nothing
	require.NoError(t, os.Remove(path))
	reopened, err := OpenKVShard(dir)
	require.NoError(t, err, "a missing block record is not an error")
	require.NoError(t, reopened.Close())
}

// TestFreshDatabasesCarryTheVersion
// Both constructors must stamp it, not just the paths that reseal.
func TestFreshDatabasesCarryTheVersion(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))

	kv2, err := NewKV2(filepath.Join(dir, "kv2"), 1000)
	require.NoError(t, err)
	require.NoError(t, kv2.Close())

	for _, layer := range []string{PermDirName, DynaDirName} {
		data, err := os.ReadFile(filepath.Join(dir, "kv2", layer, segManifestName))
		require.NoError(t, err)
		var m StoreManifest
		require.NoError(t, json.Unmarshal(data, &m))
		assert.Equalf(t, uint32(StoreFormatVersion), m.Version, "%s layer", layer)
	}

	// And a freshly created store reopens, which is the check that the
	// version it writes is the version it accepts
	reopened, err := OpenKV2(filepath.Join(dir, "kv2"))
	require.NoError(t, err)
	require.NoError(t, reopened.Open())
	require.NoError(t, reopened.Close())
	fmt.Fprint(os.Stderr, "")
}
