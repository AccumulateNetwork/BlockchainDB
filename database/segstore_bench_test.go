package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

// TestSyncCostComparison
// The central claim of segments-as-storage: syncing a peer is a file
// copy plus a manifest commit, not a re-insertion of every record.
//
// Measured twice, because the difference is structural rather than
// constant: into an empty node, and into a node that already holds
// data (the partially synced node the design targets).  Re-inserting
// records costs a lookup per key against everything already stored;
// adopting a sealed segment does not.
func TestSyncCostComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement; skipped in -short")
	}
	const keys = 200_000
	const preloaded = 600_000 // Data the syncing node already holds

	base := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(base)
	defer os.RemoveAll(base)

	// --- v2: SegmentStore, sync by copying sealed files ---
	srcDir := filepath.Join(base, "v2src")
	dstDir := filepath.Join(base, "v2dst")
	src, err := NewSegmentStore(srcDir, false)
	require.NoError(t, err)
	kr := NewFastRandom([]byte{111})
	vr := NewFastRandom([]byte{111, 111})
	for i := 0; i < keys; i++ {
		require.NoError(t, src.Put(kr.NextHash(), vr.RandBuff(100, 500)))
	}
	_, err = src.Seal(1)
	require.NoError(t, err)
	metas, paths := src.SegmentPaths()

	dst, err := NewSegmentStore(dstDir, false)
	require.NoError(t, err)
	start := time.Now()
	for i, meta := range metas {
		require.NoError(t, dst.ImportSegmentFile(paths[i], meta))
	}
	v2Empty := time.Since(start)
	require.NoError(t, dst.Close())

	// v2 again, into a node that already holds `preloaded` keys
	dst2Dir := filepath.Join(base, "v2dst2")
	dst2, err := NewSegmentStore(dst2Dir, false)
	require.NoError(t, err)
	pk := NewFastRandom([]byte{121})
	pv := NewFastRandom([]byte{121, 121})
	for i := 0; i < preloaded; i++ {
		require.NoError(t, dst2.Put(pk.NextHash(), pv.RandBuff(100, 500)))
		if (i+1)%200_000 == 0 {
			_, err = dst2.Seal(uint64(i/200_000) + 1)
			require.NoError(t, err)
		}
	}
	start = time.Now()
	for i, meta := range metas {
		m := meta
		m.Height = 100 + uint64(i) // Above the preloaded segments
		require.NoError(t, dst2.ImportSegmentFile(paths[i], m))
	}
	v2Loaded := time.Since(start)
	require.NoError(t, src.Close())
	require.NoError(t, dst2.Close())

	// --- v1: KV Perm layer, sync by re-inserting every record ---
	v1SrcDir := filepath.Join(base, "v1src")
	v1DstDir := filepath.Join(base, "v1dst")
	v1Src, err := NewKV(true, v1SrcDir, 1024, 100_000, 50)
	require.NoError(t, err)
	kr2 := NewFastRandom([]byte{111})
	vr2 := NewFastRandom([]byte{111, 111})
	for i := 0; i < keys; i++ {
		require.NoError(t, v1Src.Put(kr2.NextHash(), vr2.RandBuff(100, 500)))
	}
	segPath := filepath.Join(base, "v1.seg")
	f, err := os.Create(segPath)
	require.NoError(t, err)
	_, _, _, err = v1Src.ExportSegment(f, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	v1Dst, err := NewKV(true, v1DstDir, 1024, 100_000, 50)
	require.NoError(t, err)
	in, err := os.Open(segPath)
	require.NoError(t, err)
	start = time.Now()
	n, err := v1Dst.ImportSegment(in)
	v1Empty := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, uint64(keys), n)
	require.NoError(t, in.Close())
	require.NoError(t, v1Dst.Close())

	// v1 again, into a node that already holds `preloaded` keys
	v1Dst2Dir := filepath.Join(base, "v1dst2")
	v1Dst2, err := NewKV(true, v1Dst2Dir, 1024, 100_000, 50)
	require.NoError(t, err)
	pk2 := NewFastRandom([]byte{121})
	pv2 := NewFastRandom([]byte{121, 121})
	for i := 0; i < preloaded; i++ {
		require.NoError(t, v1Dst2.Put(pk2.NextHash(), pv2.RandBuff(100, 500)))
	}
	in2, err := os.Open(segPath)
	require.NoError(t, err)
	start = time.Now()
	_, err = v1Dst2.ImportSegment(in2)
	v1Loaded := time.Since(start)
	require.NoError(t, err)
	require.NoError(t, in2.Close())
	require.NoError(t, v1Src.Close())
	require.NoError(t, v1Dst2.Close())

	rate := func(d time.Duration) string {
		return humanize.Comma(int64(float64(keys) / d.Seconds()))
	}
	fmt.Printf("syncing %s keys into a node:\n", humanize.Comma(keys))
	fmt.Printf("  %-34s %8s %14s\n", "", "seconds", "keys/s")
	fmt.Printf("  %-34s %8.2f %14s\n", "v1 re-insert, empty node", v1Empty.Seconds(), rate(v1Empty))
	fmt.Printf("  %-34s %8.2f %14s\n", "v2 copy segments, empty node", v2Empty.Seconds(), rate(v2Empty))
	fmt.Printf("  %-34s %8.2f %14s\n",
		fmt.Sprintf("v1 re-insert, %s keys held", humanize.Comma(preloaded)), v1Loaded.Seconds(), rate(v1Loaded))
	fmt.Printf("  %-34s %8.2f %14s\n",
		fmt.Sprintf("v2 copy segments, %s keys held", humanize.Comma(preloaded)), v2Loaded.Seconds(), rate(v2Loaded))
	fmt.Printf("  speedup: %.1fx empty, %.1fx partially synced\n",
		v1Empty.Seconds()/v2Empty.Seconds(), v1Loaded.Seconds()/v2Loaded.Seconds())
}

// BenchmarkKVGet
// Full key-to-value lookup through the v1 KV Perm layer, for
// comparison with BenchmarkSegmentStoreGet (same key count, same
// value sizes; both read the value, not just the key record).
func BenchmarkKVGet(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "BenchKVGet")
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	kv, err := NewKV(true, dir, 1024, 250_000, 50)
	if err != nil {
		b.Fatal(err)
	}
	const n = 1_000_000
	kr := NewFastRandom([]byte{112})
	vr := NewFastRandom([]byte{112, 112})
	keys := make([][32]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = kr.NextHash()
		if err := kv.Put(keys[i], vr.RandBuff(100, 500)); err != nil {
			b.Fatal(err)
		}
	}

	pick := NewFastRandom([]byte{113})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kv.Get(keys[int(pick.UintN(n))]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSegmentStoreGet measures lookups against sealed segments
func BenchmarkSegmentStoreGet(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "BenchSegStoreGet")
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	store, err := NewSegmentStore(dir, false)
	if err != nil {
		b.Fatal(err)
	}
	const n = 1_000_000
	kr := NewFastRandom([]byte{112})
	vr := NewFastRandom([]byte{112, 112})
	keys := make([][32]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = kr.NextHash()
		if err := store.Put(keys[i], vr.RandBuff(100, 500)); err != nil {
			b.Fatal(err)
		}
		if (i+1)%250_000 == 0 { // Four sealed segments
			if _, err := store.Seal(uint64(i/250_000) + 1); err != nil {
				b.Fatal(err)
			}
		}
	}

	pick := NewFastRandom([]byte{113})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Get(keys[int(pick.UintN(n))]); err != nil {
			b.Fatal(err)
		}
	}
}
