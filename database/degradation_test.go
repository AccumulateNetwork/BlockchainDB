package blockchainDB

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

// TestDegradationOverTime answers one question: does a KV2 database get
// slower as it grows, and if so, where does the time go?
//
// The shape of the answer is predictable from the code, which is why it
// is worth measuring rather than asserting.  A Get that misses walks
// every sealed segment newest to oldest, testing each segment's bloom
// filter and paying a binary search over any that returns a false
// positive.  KV2.Put misses on purpose: a new key is looked up in Dyna,
// then in Perm, and then SegmentStore.put looks it up in Perm a second
// time to enforce immutability.  So a put costs three full misses, and a
// miss costs O(sealed segments).  The Perm layer never compacts -- its
// segments are the unit a peer syncs -- so its segment count only ever
// rises.  Dyna's does not: Compress collapses every sealed generation
// into one.
//
// The test therefore reports, per bucket of puts: the put rate, the
// latency of a hit and of a miss, the segment count of each layer, and
// what sealing and compaction cost.  Degradation that tracks the Perm
// segment count is the structural cost above; degradation that does not
// is something else, and the columns are there to tell them apart.
//
// Flags, because a mistyped flag is a hard error and a mistyped
// environment variable is a silent default:
//
//	go test ./database/ -run TestDegradationOverTime -load -degrade-for=2h -degrade-csv=/tmp/degrade.csv -v
var (
	degradeFor = flag.Duration("degrade-for", 30*time.Minute,
		"how long TestDegradationOverTime writes for")
	degradeCSV = flag.String("degrade-csv", "",
		"write TestDegradationOverTime's per-bucket samples to this CSV file")
	degradeDir = flag.String("degrade-dir", "",
		"directory for TestDegradationOverTime's database (default: a temp dir, removed at the end)")
	degradeMaxGB = flag.Int64("degrade-max-gb", 200,
		"stop TestDegradationOverTime when its database reaches this many GB")
)

const (
	degSealLimit     = 100_000 // Records in a live tail before it auto-seals
	degPutsPerBlock  = 20_000  // Puts between block boundaries (a Perm seal)
	degCompressEvery = 25      // Blocks between Dyna compactions
	degStateKeys     = 200_000 // The mutable working set, rewritten forever
	degStatePct      = 30      // Percent of puts that rewrite a state key
	degBucketPuts    = 500_000 // Puts between samples
	degSampleGets    = 5_000   // Gets timed per sample, per kind

	degNSPerm  = 1 // Namespaces, so the three key kinds cannot collide
	degNSState = 2
	degNSMiss  = 3
)

// degKey derives a key from its namespace and index, so the test can
// name any key it has ever written without holding a map of them.  A
// map of a hundred million keys would be the largest thing in the
// process, and this test is trying to measure the database's memory.
func degKey(ns byte, i uint64) (key [32]byte) {
	var b [9]byte
	b[0] = ns
	binary.BigEndian.PutUint64(b[1:], i)
	return sha256.Sum256(b[:])
}

// degValue derives a value from its key's identity and version.  Values
// vary in length the way real ones do, and a state key's value changes
// every time it is rewritten, which is what moves it into the Dyna
// layer and what leaves trash behind for Compress to reclaim.
func degValue(ns byte, i, version uint64) []byte {
	var b [17]byte
	b[0] = ns
	binary.BigEndian.PutUint64(b[1:], i)
	binary.BigEndian.PutUint64(b[9:], version)
	h := sha256.Sum256(b[:])
	value := make([]byte, 100+int(h[0])) // 100..355 bytes
	x := binary.BigEndian.Uint64(h[:8]) | 1
	for n := 0; n < len(value); n += 8 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		var word [8]byte
		binary.BigEndian.PutUint64(word[:], x)
		copy(value[n:], word[:])
	}
	return value
}

// degSample is one row of the report
type degSample struct {
	elapsed    time.Duration
	blocks     int
	permKeys   uint64
	stateWrite uint64
	putRate    float64
	hitUS      float64
	missUS     float64
	permSegs   int
	dynaSegs   int
	sealMS     float64
	compactMS  float64
	disk       int64
	heapMB     float64
	rssMB      float64
}

func TestDegradationOverTime(t *testing.T) {
	skipUnlessLoad(t)

	dir := *degradeDir
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "DegradationOverTime")
		defer os.RemoveAll(dir)
	}
	os.RemoveAll(dir)

	kv, err := NewKV2(dir, degSealLimit)
	require.NoError(t, err)

	fmt.Printf("TestDegradationOverTime: writing for %v into %s\n", *degradeFor, dir)
	fmt.Printf("  seal limit %s records, %s puts per block, Compress every %d blocks,\n",
		humanize.Comma(degSealLimit), humanize.Comma(degPutsPerBlock), degCompressEvery)
	fmt.Printf("  %s state keys rewritten at %d%% of puts, sample every %s puts\n\n",
		humanize.Comma(degStateKeys), degStatePct, humanize.Comma(degBucketPuts))

	// Every state key is written once up front, so that from the first
	// sample onward a state read is a read of a key that exists.  These
	// first writes land in Perm; the second, differing write of each is
	// what moves it to Dyna.
	stateVersion := make([]uint64, degStateKeys)
	for i := uint64(0); i < degStateKeys; i++ {
		_, err = kv.Put(degKey(degNSState, i), degValue(degNSState, i, 0))
		require.NoError(t, err)
	}

	var (
		pick       = NewFastRandom([]byte{7, 7, 7})
		samples    []degSample
		permNext   uint64 // Next unused Perm key index
		stateWrite uint64 // State rewrites so far
		missNext   uint64 // Next unused never-written key
		height     uint64 = 1
		blocks     int
		bucketPuts int
		putNanos   int64
		sealNanos  int64
		compNanos  int64
		start      = time.Now()
		deadline   = start.Add(*degradeFor)
		maxBytes   = *degradeMaxGB << 30
	)

	fmt.Printf("%8s %7s %12s %11s %9s %9s %7s %7s %9s %10s %9s %8s\n",
		"elapsed", "blocks", "perm keys", "puts/s", "hit us", "miss us",
		"pSegs", "dSegs", "seal ms", "compact ms", "on disk", "heap MB")

	for time.Now().Before(deadline) {
		// One block
		blockStart := time.Now()
		for j := 0; j < degPutsPerBlock; j++ {
			if pick.UintN(100) < degStatePct {
				i := uint64(pick.UintN(degStateKeys))
				stateVersion[i]++
				_, err = kv.Put(degKey(degNSState, i), degValue(degNSState, i, stateVersion[i]))
				stateWrite++
			} else {
				_, err = kv.Put(degKey(degNSPerm, permNext), degValue(degNSPerm, permNext, 0))
				permNext++
			}
			require.NoError(t, err)
		}
		putNanos += time.Since(blockStart).Nanoseconds()
		bucketPuts += degPutsPerBlock

		sealStart := time.Now()
		_, err = kv.Seal(height)
		require.NoError(t, err)
		sealNanos += time.Since(sealStart).Nanoseconds()
		height++
		blocks++

		if blocks%degCompressEvery == 0 {
			compStart := time.Now()
			require.NoError(t, kv.Compress())
			compNanos += time.Since(compStart).Nanoseconds()
		}

		if bucketPuts < degBucketPuts {
			continue
		}

		// Sample.  Timed separately, and its time is not charged to the
		// put rate: what a sample costs is the point of two of these
		// columns, not noise to fold into a third.
		s := degSample{
			elapsed:    time.Since(start),
			blocks:     blocks,
			permKeys:   permNext,
			stateWrite: stateWrite,
			putRate:    float64(bucketPuts) / (float64(putNanos) / 1e9),
			permSegs:   degSegCount(kv.PermKV),
			dynaSegs:   degSegCount(kv.DynaKV),
			sealMS:     float64(sealNanos) / 1e6,
			compactMS:  float64(compNanos) / 1e6,
			disk:       degDirSize(t, dir),
		}

		// Hits: existing keys, verified.  A database that degrades into
		// wrong answers is not a performance result.
		got := make([][]byte, degSampleGets)
		idx := make([]uint64, degSampleGets)
		kinds := make([]byte, degSampleGets)
		hitStart := time.Now()
		for n := 0; n < degSampleGets; n++ {
			if n%2 == 0 && permNext > 0 {
				idx[n], kinds[n] = uint64(pick.UintN(uint(permNext))), degNSPerm
			} else {
				idx[n], kinds[n] = uint64(pick.UintN(degStateKeys)), degNSState
			}
			got[n], err = kv.Get(degKey(kinds[n], idx[n]))
			require.NoError(t, err)
		}
		s.hitUS = float64(time.Since(hitStart).Microseconds()) / degSampleGets
		for n := 0; n < degSampleGets; n++ {
			version := uint64(0)
			if kinds[n] == degNSState {
				version = stateVersion[idx[n]]
			}
			want := degValue(kinds[n], idx[n], version)
			require.Truef(t, bytes.Equal(want, got[n]),
				"wrong value for ns %d key %d version %d after %s puts",
				kinds[n], idx[n], version, humanize.Comma(int64(permNext+stateWrite)))
		}

		// Misses: keys never written, which is the path a put takes
		missStart := time.Now()
		for n := 0; n < degSampleGets; n++ {
			_, err = kv.Get(degKey(degNSMiss, missNext))
			missNext++
			require.Error(t, err, "a key that was never written was found")
		}
		s.missUS = float64(time.Since(missStart).Microseconds()) / degSampleGets

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		s.heapMB = float64(ms.HeapAlloc) / (1 << 20)
		s.rssMB = degRSSMB()

		samples = append(samples, s)
		fmt.Printf("%8s %7d %12s %11s %9.1f %9.1f %7d %7d %9.1f %10.1f %9s %8.0f\n",
			s.elapsed.Round(time.Second), s.blocks, humanize.Comma(int64(s.permKeys)),
			humanize.Comma(int64(s.putRate)), s.hitUS, s.missUS, s.permSegs, s.dynaSegs,
			s.sealMS, s.compactMS, humanize.Bytes(uint64(s.disk)), s.heapMB)

		bucketPuts, putNanos, sealNanos, compNanos = 0, 0, 0, 0

		if s.disk >= maxBytes {
			fmt.Printf("\nstopping: the database reached %s (-degrade-max-gb=%d)\n",
				humanize.Bytes(uint64(s.disk)), *degradeMaxGB)
			break
		}
	}

	require.NoError(t, kv.Close())
	require.NotEmpty(t, samples, "the run was too short to produce a single sample")

	degReport(t, samples)
	if *degradeCSV != "" {
		degWriteCSV(t, *degradeCSV, samples)
		fmt.Printf("\nper-bucket samples written to %s\n", *degradeCSV)
	}
}

// degReport states what the samples say, rather than leaving a wall of
// numbers to be eyeballed
func degReport(t *testing.T, samples []degSample) {
	t.Helper()
	first, last := samples[0], samples[len(samples)-1]
	fmt.Printf("\n%s\n", strings.Repeat("-", 78))
	fmt.Printf("over %v: %s Perm keys, %s state rewrites, %s on disk\n",
		last.elapsed.Round(time.Second), humanize.Comma(int64(last.permKeys)),
		humanize.Comma(int64(last.stateWrite)), humanize.Bytes(uint64(last.disk)))
	ratio := func(a, b float64) string {
		if a == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%.2fx", b/a)
	}
	fmt.Printf("  %-24s %14s %14s %10s\n", "", "first bucket", "last bucket", "change")
	fmt.Printf("  %-24s %14s %14s %10s\n", "puts/s",
		humanize.Comma(int64(first.putRate)), humanize.Comma(int64(last.putRate)),
		ratio(first.putRate, last.putRate))
	fmt.Printf("  %-24s %14.1f %14.1f %10s\n", "get hit, us",
		first.hitUS, last.hitUS, ratio(first.hitUS, last.hitUS))
	fmt.Printf("  %-24s %14.1f %14.1f %10s\n", "get miss, us",
		first.missUS, last.missUS, ratio(first.missUS, last.missUS))
	fmt.Printf("  %-24s %14d %14d %10s\n", "Perm segments",
		first.permSegs, last.permSegs, ratio(float64(first.permSegs), float64(last.permSegs)))
	fmt.Printf("  %-24s %14d %14d %10s\n", "Dyna segments",
		first.dynaSegs, last.dynaSegs, ratio(float64(first.dynaSegs), float64(last.dynaSegs)))
	fmt.Printf("  %-24s %14.1f %14.1f %10s\n", "seal, ms/bucket",
		first.sealMS, last.sealMS, ratio(first.sealMS, last.sealMS))
	fmt.Printf("  %-24s %14.1f %14.1f %10s\n", "compact, ms/bucket",
		first.compactMS, last.compactMS, ratio(first.compactMS, last.compactMS))
	fmt.Printf("  %-24s %14.0f %14.0f %10s\n", "heap, MB",
		first.heapMB, last.heapMB, ratio(first.heapMB, last.heapMB))
	fmt.Printf("  %-24s %14.0f %14.0f %10s\n", "RSS, MB",
		first.rssMB, last.rssMB, ratio(first.rssMB, last.rssMB))

	// Cost per sealed Perm segment, if the miss latency tracks the
	// segment count.  This is the number that says whether the walk over
	// segments is what is being paid for.
	if last.permSegs > first.permSegs {
		perSeg := (last.missUS - first.missUS) / float64(last.permSegs-first.permSegs)
		fmt.Printf("\n  miss latency per added Perm segment: %.3f us\n", perSeg)
	}
	fmt.Printf("%s\n", strings.Repeat("-", 78))
}

func degWriteCSV(t *testing.T, path string, samples []degSample) {
	t.Helper()
	var b strings.Builder
	b.WriteString("elapsed_s,blocks,perm_keys,state_writes,puts_per_s,hit_us,miss_us," +
		"perm_segments,dyna_segments,seal_ms,compact_ms,disk_bytes,heap_mb,rss_mb\n")
	for _, s := range samples {
		fmt.Fprintf(&b, "%.1f,%d,%d,%d,%.1f,%.3f,%.3f,%d,%d,%.1f,%.1f,%d,%.1f,%.1f\n",
			s.elapsed.Seconds(), s.blocks, s.permKeys, s.stateWrite, s.putRate,
			s.hitUS, s.missUS, s.permSegs, s.dynaSegs, s.sealMS, s.compactMS,
			s.disk, s.heapMB, s.rssMB)
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0644))
}

// degSegCount is the number of sealed segments in a layer
func degSegCount(s *SegmentStore) int {
	return len(s.sealedSegments())
}

// degDirSize totals a directory tree, since a KV2 has a directory per layer
func degDirSize(t *testing.T, dir string) (total int64) {
	t.Helper()
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	require.NoError(t, err)
	return total
}

// degRSSMB is the process's resident size, which counts what the heap
// does not: the index and data files the kernel is holding for us
func degRSSMB() float64 {
	buf, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(buf))
	if len(fields) < 2 {
		return 0
	}
	var pages float64
	fmt.Sscanf(fields[1], "%f", &pages)
	return pages * float64(os.Getpagesize()) / (1 << 20)
}
