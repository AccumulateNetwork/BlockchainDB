package blockchainDB

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The read path, measured outside the protocol (issue #83).
//
// A CPU profile of a validator at 500 tx/s said one second in six was
// `pread`, and 71.8% of those preads were segments' bloom filters
// being read in order to be tested.  The cost of a read is finding the
// record, not returning it.
//
// What a read costs is set by two things this measures directly:
//
//   - a filter that says NO costs 1 to 3 one-byte preads and skips a
//     whole segment -- 20 blocks of records -- which is a good trade;
//   - a filter that says MAYBE costs those plus a binary search, one
//     pread per probe, log2(records) of them, and if the filter was
//     wrong that is ~15 syscalls for nothing.
//
// So the levers are residency (a resident filter costs no syscall at
// all) and the search behind a hit.  This measures both, against the
// three workloads that behave differently: a validator's read, which
// is in the window; an explorer's, which is anywhere; and a miss,
// which walks everything and shows the false-positive rate.
//
// Syscalls are counted, not inferred: /proc/self/io reports syscr, so
// each case reports preads per lookup rather than a time that hardware
// decides.
//
//	go test ./database/ -run TestReadPathCost -load -v
//	go test ./database/ -run TestReadPathCost -load -readpath-segments=100,1000 -v
var readPathSegments = flag.String("readpath-segments", "10,100,1000",
	"segment counts to measure the read path at")

const (
	readPathRecords = 12_500 // Records a segment, as the adapter seals at
	readPathValue   = 200    // Value bytes, near Accumulate's average
	readPathLookups = 2_000  // Lookups timed per case
)

// syscallReads is this process's read syscalls so far.  Linux only;
// zero elsewhere, and the report says so rather than pretending.
func syscallReads() uint64 {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "syscr: "); ok {
			n, _ := strconv.ParseUint(strings.TrimSpace(rest), 10, 64)
			return n
		}
	}
	return 0
}

// readPathFixture builds a store of `segments` sealed segments, each
// holding readPathRecords records, and returns the keys of the first
// and last segments plus a key that is in none of them.
func readPathFixture(t *testing.T, dir string, segments int) (store *SegmentStore, inWindow, deep [][32]byte) {
	t.Helper()
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{91, byte(segments)})
	value := make([]byte, readPathValue)
	start := time.Now()
	for s := 0; s < segments; s++ {
		var first [32]byte
		for i := 0; i < readPathRecords; i++ {
			k := kr.NextHash()
			if i == 0 {
				first = k
			}
			require.NoError(t, store.Put(k, value))
		}
		_, err := store.Seal(uint64(s + 1))
		require.NoError(t, err)
		deep = append(deep, first)
		if s >= segments-2*MinFilterBlocks { // The newest 2N: the window
			inWindow = append(inWindow, first)
		}
	}
	t.Logf("fixture: %d segments x %d records in %v", segments, readPathRecords, time.Since(start).Round(time.Millisecond))
	return store, inWindow, deep
}

// readPathCase times lookups and counts the read syscalls they cost
func readPathCase(t *testing.T, name string, n int, lookup func(i int) error) (nsPer float64, preadsPer float64) {
	t.Helper()
	before := syscallReads()
	start := time.Now()
	for i := 0; i < n; i++ {
		require.NoError(t, lookup(i))
	}
	elapsed := time.Since(start)
	preads := syscallReads() - before
	return float64(elapsed.Nanoseconds()) / float64(n), float64(preads) / float64(n)
}

func TestReadPathCost(t *testing.T) {
	if !*runLoadTests {
		t.Skip("read-path measurement; -load to run it")
	}
	if syscallReads() == 0 {
		t.Log("NOTE: /proc/self/io unavailable; pread counts will read as zero")
	}

	for _, field := range strings.Split(*readPathSegments, ",") {
		segments, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err)

		t.Run(fmt.Sprintf("segments=%d", segments), func(t *testing.T) {
			dir := storeDir(t, fmt.Sprintf("readpath%d", segments))
			store, inWindow, deep := readPathFixture(t, dir, segments)
			defer store.Close()

			miss := NewFastRandom([]byte{92})
			// Residency is what a filter probe costs: a resident filter
			// is a memory probe, a cold one is up to K syscalls.  These
			// are the two ends the issue asks to compare.
			resident := func(all bool) {
				store.Mutex.Lock()
				store.History.Lock()
				for _, seg := range store.history {
					if all {
						_ = seg.loadBloom()
					} else {
						seg.freeBloom()
					}
				}
				store.History.Unlock()
				store.Mutex.Unlock()
			}

			for _, variant := range []struct {
				name string
				all  bool
			}{{"window-resident", false}, {"all-resident", true}} {
				resident(variant.all)

				hitWin, preadWin := readPathCase(t, "HitInWindow", readPathLookups, func(i int) error {
					_, err := store.GetDeep(inWindow[i%len(inWindow)])
					return err
				})
				hitDeep, preadDeep := readPathCase(t, "HitDeep", readPathLookups, func(i int) error {
					_, err := store.GetDeep(deep[i%len(deep)])
					return err
				})
				missNS, preadMiss := readPathCase(t, "Miss", readPathLookups, func(i int) error {
					_, err := store.GetDeep(miss.NextHash())
					if err != nil && err != errNotFound {
						return err
					}
					return nil
				})

				t.Logf("%-16s HitInWindow %8.0f ns %6.1f preads | HitDeep %8.0f ns %6.1f preads | Miss %8.0f ns %6.1f preads",
					variant.name, hitWin, preadWin, hitDeep, preadDeep, missNS, preadMiss)
			}

			st := store.Stats()
			t.Logf("filters: %d lookups, %d absent, %d walked, %d misled (%.2f%% of lookups walked for nothing)",
				st.LookupTotal, st.FilterAbsent, st.FilterWalked, st.FilterMisled,
				100*float64(st.FilterMisled)/float64(max64(st.LookupTotal, 1)))
		})
	}
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
