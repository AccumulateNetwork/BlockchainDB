package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSegmentRoundTrip
// The node-sync flow: node A exports two blocks; a fresh node B
// verifies and imports them and ends with identical data.
func TestSegmentRoundTrip(t *testing.T) {
	dirA := filepath.Join(os.TempDir(), t.Name()+"_A")
	dirB := filepath.Join(os.TempDir(), t.Name()+"_B")
	exportDir := filepath.Join(os.TempDir(), t.Name()+"_export")
	for _, d := range []string{dirA, dirB, exportDir} {
		os.RemoveAll(d)
		defer os.RemoveAll(d)
	}

	// Node A: two "blocks" of writes.  The seal limit is deliberately
	// tiny relative to the keys per shard, so tails fill mid-block and
	// auto-seal -- the case ExportBlock claims to support and issue #27
	// showed it did not.  At 512 shards this needs thousands of keys to
	// reach even a handful per shard; the previous 400 never auto-sealed
	// once, so the test certified a path it never entered.
	nodeA, err := NewKVShard(dirA, 2)
	require.NoError(t, err)

	kr := NewFastRandom([]byte{91})
	vr := NewFastRandom([]byte{91, 91})
	var keys [][32]byte
	var values [][]byte
	writeBlock := func(n int) {
		for i := 0; i < n; i++ {
			key := kr.NextHash()
			value := vr.RandBuff(20, 200)
			require.NoError(t, nodeA.PutPerm(key, value))
			keys = append(keys, key)
			values = append(values, value)
		}
	}

	writeBlock(4000)

	// Confirm the premise: some shard really did auto-seal
	autoSealed := 0
	for _, sh := range nodeA.Shards {
		if len(sh.PermKV.segments) > 0 {
			autoSealed += len(sh.PermKV.segments)
		}
	}
	require.Greater(t, autoSealed, 0, "no shard auto-sealed; the test would not exercise issue #27")

	m1, err := nodeA.ExportBlock(exportDir, 1, nil)
	require.NoError(t, err, "export block 1")

	writeBlock(3000)
	m2, err := nodeA.ExportBlock(exportDir, 2, m1)
	require.NoError(t, err, "export block 2")

	// The two blocks must partition the data: block 2 exports only the
	// segments sealed after block 1
	var c1, c2 uint64
	for _, s := range m1.Segments {
		c1 += s.Count
	}
	for _, s := range m2.Segments {
		c2 += s.Count
	}
	assert.Equal(t, uint64(4000), c1, "block 1 record count")
	assert.Equal(t, uint64(3000), c2, "block 2 record count")

	// Node B: verify + import in height order
	nodeB, err := NewKVShard(dirB, 2)
	require.NoError(t, err)
	n1, err := nodeB.ImportBlock(filepath.Join(exportDir, "block-00000001"))
	require.NoError(t, err, "import block 1")
	assert.Equal(t, uint64(4000), n1)
	n2, err := nodeB.ImportBlock(filepath.Join(exportDir, "block-00000002"))
	require.NoError(t, err, "import block 2")
	assert.Equal(t, uint64(3000), n2)

	// Node B must now hold every key with the correct value
	for i, key := range keys {
		v, err := nodeB.GetPerm(key)
		require.NoErrorf(t, err, "key %d missing on node B", i)
		assert.Equalf(t, values[i], v, "key %d wrong on node B", i)
	}

	// Re-importing a block is a no-op (idempotent sync resume)
	_, err = nodeB.ImportBlock(filepath.Join(exportDir, "block-00000002"))
	require.NoError(t, err, "re-import should be idempotent")
	for i, key := range keys {
		v, err := nodeB.GetPerm(key)
		require.NoError(t, err)
		assert.Equal(t, values[i], v, "value changed by re-import (key %d)", i)
	}

	require.NoError(t, nodeA.Close())
	require.NoError(t, nodeB.Close())
}

// keyForShard
// A key that routes to the given shard, so a test can decide which
// shards take writes in a block rather than relying on the spread of
// random keys across 512 of them.
func keyForShard(kr *FastRandom, shard int) [32]byte {
	key := kr.NextHash()
	binary.BigEndian.PutUint32(key[indexShards:], uint32(shard))
	return key
}

// TestExportBlockQuietShardNotReexported
// A shard that takes no writes during a block seals nothing, so it
// appears nowhere in that block's manifest.  The next export must still
// know the peer already has its older segments.
//
// prev carries only the previous block's segments, so a per-shard
// high-water map built from it has no entry for a shard that was quiet
// -- and every segment that shard holds is copied again, into every
// block until it next seals.  Three blocks is the shortest sequence
// that shows it: shard A seals in block 1 and is quiet thereafter, so
// block 3 is where its segment reappears.
func TestExportBlockQuietShardNotReexported(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	exportDir := filepath.Join(os.TempDir(), t.Name()+"_export")
	for _, d := range []string{dir, exportDir} {
		os.RemoveAll(d)
		defer os.RemoveAll(d)
	}

	const shardA, shardB = 3, 7
	node, err := NewKVShard(dir, 1000)
	require.NoError(t, err)
	defer node.Close()

	kr := NewFastRandom([]byte{94})
	vr := NewFastRandom([]byte{94, 94})

	require.NoError(t, node.PutPerm(keyForShard(kr, shardA), vr.RandBuff(20, 100)))
	m1, err := node.ExportBlock(exportDir, 1, nil)
	require.NoError(t, err)
	require.Len(t, m1.Segments, 1, "block 1 seals shard A only")
	require.Equal(t, shardA, m1.Segments[0].Shard)

	// Shard A is quiet from here on
	require.NoError(t, node.PutPerm(keyForShard(kr, shardB), vr.RandBuff(20, 100)))
	m2, err := node.ExportBlock(exportDir, 2, m1)
	require.NoError(t, err)
	require.Len(t, m2.Segments, 1, "block 2 seals shard B only")
	require.Equal(t, shardB, m2.Segments[0].Shard)

	require.NoError(t, node.PutPerm(keyForShard(kr, shardB), vr.RandBuff(20, 100)))
	m3, err := node.ExportBlock(exportDir, 3, m2)
	require.NoError(t, err)
	for _, s := range m3.Segments {
		assert.NotEqualf(t, shardA, s.Shard,
			"shard %d sealed in block 1 and was quiet since; block 3 must not re-export it", s.Shard)
	}
	assert.Len(t, m3.Segments, 1, "block 3 seals shard B only")
}

// TestSegmentTamperDetection
// A modified segment file must fail verification before anything is
// imported.
func TestSegmentTamperDetection(t *testing.T) {
	dirA := filepath.Join(os.TempDir(), t.Name()+"_A")
	dirB := filepath.Join(os.TempDir(), t.Name()+"_B")
	exportDir := filepath.Join(os.TempDir(), t.Name()+"_export")
	for _, d := range []string{dirA, dirB, exportDir} {
		os.RemoveAll(d)
		defer os.RemoveAll(d)
	}

	nodeA, err := NewKVShard(dirA, 1000)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{92})
	vr := NewFastRandom([]byte{92, 92})
	for i := 0; i < 100; i++ {
		require.NoError(t, nodeA.PutPerm(kr.NextHash(), vr.RandBuff(20, 100)))
	}
	m, err := nodeA.ExportBlock(exportDir, 1, nil)
	require.NoError(t, err)
	require.NoError(t, nodeA.Close())

	// Flip one byte in the first non-empty segment
	blockDir := filepath.Join(exportDir, "block-00000001")
	var target string
	for _, s := range m.Segments {
		if s.Count > 0 {
			target = filepath.Join(blockDir, s.File)
			break
		}
	}
	require.NotEmpty(t, target, "no non-empty segment found")
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	data[len(data)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(target, data, 0644))

	nodeB, err := NewKVShard(dirB, 1000)
	require.NoError(t, err)
	_, err = nodeB.ImportBlock(blockDir)
	require.Error(t, err, "tampered segment must fail verification")
	assert.Contains(t, err.Error(), "hash mismatch")
	require.NoError(t, nodeB.Close())
}

// TestSegmentImmutableConflict
// Importing a segment whose key conflicts with a different local value
// must fail rather than silently overwrite.
func TestSegmentImmutableConflict(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	exportDir := filepath.Join(os.TempDir(), t.Name()+"_export")
	os.RemoveAll(dir)
	os.RemoveAll(exportDir)
	defer os.RemoveAll(dir)
	defer os.RemoveAll(exportDir)

	nodeA, err := NewKVShard(filepath.Join(dir, "A"), 1000)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{93})
	key := kr.NextHash()
	require.NoError(t, nodeA.PutPerm(key, []byte("the true value")))
	_, err = nodeA.ExportBlock(exportDir, 1, nil)
	require.NoError(t, err)
	require.NoError(t, nodeA.Close())

	nodeB, err := NewKVShard(filepath.Join(dir, "B"), 1000)
	require.NoError(t, err)
	require.NoError(t, nodeB.PutPerm(key, []byte("a conflicting value")))
	_, err = nodeB.ImportBlock(filepath.Join(exportDir, "block-00000001"))
	require.Error(t, err, "conflicting immutable value must fail the import")
	require.NoError(t, nodeB.Close())
}

// The export naming convention used by the tests
func TestSegmentBlockDirName(t *testing.T) {
	assert.Equal(t, "block-00000007", fmt.Sprintf("block-%08d", 7))
}
