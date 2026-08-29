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

// TestSyncCost
// The central claim of segments-as-storage: syncing a peer is a file
// copy plus a manifest commit, not a re-insertion of every record.
//
// Measured twice, because the point is structural rather than
// constant: into an empty node, and into a node that already holds
// data (the partially synced node the design targets).  Re-inserting
// records costs a lookup per key against everything already stored;
// adopting a sealed segment does not, so the two numbers should be
// the same.  (The measured comparison against the v1 re-insertion
// path is in docs/design/segment-store.md; v1 itself is gone.)
func TestSyncCost(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement; skipped in -short")
	}
	const keys = 200_000
	const preloaded = 600_000 // Data the syncing node already holds

	base := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(base)
	defer os.RemoveAll(base)

	srcDir := filepath.Join(base, "src")
	dstDir := filepath.Join(base, "dst")
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
	empty := time.Since(start)
	require.NoError(t, dst.Close())

	// Again, into a node that already holds `preloaded` keys
	dst2Dir := filepath.Join(base, "dst2")
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
	loaded := time.Since(start)
	require.NoError(t, src.Close())
	require.NoError(t, dst2.Close())

	rate := func(d time.Duration) string {
		return humanize.Comma(int64(float64(keys) / d.Seconds()))
	}
	fmt.Printf("adopting %s keys as sealed segments:\n", humanize.Comma(keys))
	fmt.Printf("  %-34s %8s %14s\n", "", "seconds", "keys/s")
	fmt.Printf("  %-34s %8.2f %14s\n", "into an empty node", empty.Seconds(), rate(empty))
	fmt.Printf("  %-34s %8.2f %14s\n",
		fmt.Sprintf("into a node holding %s keys", humanize.Comma(preloaded)), loaded.Seconds(), rate(loaded))
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

// TestDynaCost
// The Dyna layer's workload is overwrites: a fixed set of state keys
// rewritten every block.  What that costs is dominated by two things
// -- what a Put does beyond appending the value, and what reclaiming
// the overwritten bytes costs.  A mutable SegmentStore appends the
// value and inserts into a map; Compress writes one new sealed
// generation and commits it with a single manifest rename.
//
// (The measured comparison against v1's kfile rewrite, bin relocation,
// and copy-every-value Compress is in docs/design/segment-store.md;
// v1 itself is gone.)
func TestDynaCost(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement; skipped in -short")
	}
	const keyCount = 50_000       // Distinct state keys
	const rounds = 8              // Times each is rewritten
	const compressEvery = 100_000 // Writes between compactions
	const sealLimit = 25_000      // Live-tail bound, in records

	base := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(base)
	require.NoError(t, os.MkdirAll(base, os.ModePerm))
	defer os.RemoveAll(base)

	keys := make([][32]byte, keyCount)
	kr := NewFastRandom([]byte{121})
	for i := range keys {
		keys[i] = kr.NextHash()
	}
	want := make([][]byte, keyCount) // Last value written for each key

	dir := filepath.Join(base, "dyna")
	store, err := NewSegmentStore(dir, true)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))
	vr := NewFastRandom([]byte{121, 121})
	written := 0
	block := uint64(0)
	start := time.Now()
	for round := 0; round < rounds; round++ {
		for i := range keys {
			value := vr.RandBuff(100, 300)
			require.NoError(t, store.Put(keys[i], value))
			if round == rounds-1 {
				want[i] = value
			}
			if store.LiveRecords() >= sealLimit { // What KV2.sealDynaIfFull does
				_, err = store.SealNext()
				require.NoError(t, err)
			}
			if written++; written%compressEvery == 0 {
				// Compaction works on history: what the window has
				// rolled past.  Roll it past everything sealed so far.
				_, err = store.SealNext()
				require.NoError(t, err)
				block += 3 * MinFilterBlocks
				store.AdvanceBlock(block)
				_, err = store.CompactHistory()
				require.NoError(t, err)
			}
		}
	}
	_, err = store.SealNext()
	require.NoError(t, err)
	block += 3 * MinFilterBlocks
	store.AdvanceBlock(block)
	_, err = store.CompactHistory()
	require.NoError(t, err)
	elapsed := time.Since(start)
	require.NoError(t, store.Close())
	size := dirSize(t, dir)

	// Compaction must not have lost or stale-ed any key
	store, err = OpenSegmentStore(dir)
	require.NoError(t, err)
	for i := range keys {
		got, err := store.Get(keys[i])
		require.NoErrorf(t, err, "key %d", i)
		require.Equalf(t, want[i], got, "wrong value for key %d after compaction", i)
	}
	require.NoError(t, store.Close())

	total := keyCount * rounds
	fmt.Printf("Dyna layer: %s writes over %s keys, compacting every %s\n",
		humanize.Comma(int64(total)), humanize.Comma(keyCount), humanize.Comma(compressEvery))
	fmt.Printf("  %-28s %8s %14s %12s\n", "", "seconds", "puts/s", "on disk")
	fmt.Printf("  %-28s %8.2f %14s %12s\n", "segments + Compact",
		elapsed.Seconds(), humanize.Comma(int64(float64(total)/elapsed.Seconds())),
		humanize.Bytes(uint64(size)))
}
