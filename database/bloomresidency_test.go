package blockchainDB

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// History's filters, and the budget that bounds them (issues #86, #88).
//
// A segment leaving the window used to give up its filter
// unconditionally, so every history probe read the filter off disk a
// byte at a time.  It now keeps it while BloomResidentBytes has room.
// What these pin is that the budget is honoured, that the newest
// segments are the ones that keep their filters, that a reopened store
// fills the budget again, and -- the one that matters -- that
// residency changes what a lookup COSTS and never what it answers.

// residentHistory reports how many history segments hold a filter, and
// how many bytes that is
func residentHistory(s *SegmentStore) (segments int, bytes uint64) {
	s.History.RLock()
	defer s.History.RUnlock()
	for _, seg := range s.history {
		if seg.bloom != nil {
			segments++
			bytes += seg.bloomBytes
		}
	}
	return segments, bytes
}

// withBloomBudget sets the budget for one test and restores it
func withBloomBudget(t *testing.T, n uint64) {
	t.Helper()
	old := BloomResidentBytes
	BloomResidentBytes = n
	t.Cleanup(func() { BloomResidentBytes = old })
}

// TestHistoryKeepsItsFiltersResident
// A segment handed to history keeps its filter, so the walk that
// proves a key absent is memory tests rather than preads.
func TestHistoryKeepsItsFiltersResident(t *testing.T) {
	withBloomBudget(t, 8<<20)
	store, _ := tieredStore(t, storeDir(t, "resident"), 71, true, 60, 5)
	defer store.Close()

	history, active := tiers(store)
	require.NotEmpty(t, history, "the window must have rolled past some segments")
	resident, bytes := residentHistory(store)
	require.Equal(t, len(history), resident,
		"every history segment should hold its filter while the budget has room")
	require.Greater(t, bytes, uint64(0))

	st := store.Stats()
	require.Equal(t, len(history), st.HistorySegments, "the walk's length is reported (#87)")
	require.Equal(t, len(active), st.ActiveSegments)
	require.Equal(t, bytes, st.ResidentBloomBytes)
}

// TestHistoryFiltersHonourTheBudget
// Past the budget the OLDEST history segments give their filters up:
// the newest are what a walk probes first and what a working set is
// likeliest to want.
func TestHistoryFiltersHonourTheBudget(t *testing.T) {
	withBloomBudget(t, 8<<20)
	dir := storeDir(t, "budget")
	store, _ := tieredStore(t, dir, 72, true, 60, 5)
	defer store.Close()

	store.History.RLock()
	total := len(store.history)
	per := store.history[0].bloomBytes
	store.History.RUnlock()
	require.Greater(t, total, 10)
	require.Greater(t, per, uint64(0))

	// Room for ten of them
	BloomResidentBytes = 10 * per
	store.History.Lock()
	store.keepHistoryBlooms()
	store.History.Unlock()

	resident, bytes := residentHistory(store)
	require.Equal(t, 10, resident, "the budget bounds what history holds")
	require.Equal(t, 10*per, bytes)

	// And it is the newest ten
	store.History.RLock()
	for i, seg := range store.history {
		if i < total-10 {
			require.Nilf(t, seg.bloom, "history[%d] is older than the newest ten", i)
		} else {
			require.NotNilf(t, seg.bloom, "history[%d] is in the newest ten", i)
		}
	}
	store.History.RUnlock()
}

// TestHistoryFiltersSurviveAReopen
// A reopened store fills the budget from disk.  Without this a restart
// would walk its whole history cold until every segment it had was
// replaced, which on a store that is mostly history is for good.
func TestHistoryFiltersSurviveAReopen(t *testing.T) {
	withBloomBudget(t, 8<<20)
	dir := storeDir(t, "reopen")
	store, keys := tieredStore(t, dir, 73, true, 60, 5)
	before, _ := residentHistory(store)
	require.NoError(t, store.Close())

	re, err := OpenSegmentStore(dir)
	require.NoError(t, err)
	defer re.Close()
	after, bytes := residentHistory(re)
	require.Equal(t, before, after, "a reopened store holds the filters it held")
	require.Equal(t, bytes, re.Stats().ResidentBloomBytes)

	// And it still answers
	for h := 1; h <= 60; h++ {
		for i, k := range keys[h] {
			v, err := re.Get(k)
			require.NoErrorf(t, err, "block %d key %d", h, i)
			require.Equal(t, []byte(fmt.Sprintf("b%d-%d", h, i)), v)
		}
	}
}

// TestResidencyChangesCostNotAnswers
// The safety property.  A resident filter and one probed on disk are
// the same bits at the same offsets, so every lookup -- hit, miss, and
// the misses that the filters get wrong -- must answer identically
// whichever way the store is holding them.
func TestResidencyChangesCostNotAnswers(t *testing.T) {
	withBloomBudget(t, 8<<20)
	store, keys := tieredStore(t, storeDir(t, "same"), 74, true, 60, 5)
	defer store.Close()

	var present [][32]byte
	for h := 1; h <= 60; h++ {
		present = append(present, keys[h]...)
	}
	absent := make([][32]byte, 0, 500)
	kr := NewFastRandom([]byte{174})
	for i := 0; i < 500; i++ {
		absent = append(absent, kr.NextHash())
	}

	read := func() ([]string, []error) {
		values := make([]string, 0, len(present))
		errs := make([]error, 0, len(absent))
		for _, k := range present {
			v, err := store.Get(k)
			require.NoError(t, err)
			values = append(values, string(v))
		}
		for _, k := range absent {
			_, err := store.Get(k)
			errs = append(errs, err)
		}
		return values, errs
	}

	hotValues, hotErrs := read()
	resident, _ := residentHistory(store)
	require.Greater(t, resident, 0, "the filters must be resident for this to be a comparison")

	// Now give every one of them up, and ask again
	BloomResidentBytes = 0
	store.History.Lock()
	store.keepHistoryBlooms()
	store.History.Unlock()
	cold, _ := residentHistory(store)
	require.Equal(t, 0, cold)

	coldValues, coldErrs := read()
	require.Equal(t, hotValues, coldValues, "a present key must read the same either way")
	for i := range hotErrs {
		require.Equal(t, hotErrs[i] == nil, coldErrs[i] == nil, "absent key %d", i)
	}
}

// TestHistoryWalkCost
// What residency is worth: the same absent-key lookups, with history's
// filters in memory and on disk.  A measurement, not an assertion --
// it reports and asserts nothing, because the number is the disk's.
func TestHistoryWalkCost(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement; skipped in -short")
	}
	withBloomBudget(t, 64<<20)
	// Segments big enough for a real filter: BloomBitsPerKey over 5,000
	// keys is 7.5 KB, where a tiny segment would floor at 4 KB
	store, _ := tieredStore(t, storeDir(t, "walkcost"), 75, true, 60, 5_000)
	defer store.Close()

	history, _ := tiers(store)
	resident, bytes := residentHistory(store)
	t.Logf("%d history segments, %d holding %s of filters",
		len(history), resident, ktBytes(int64(bytes)))

	kr := NewFastRandom([]byte{175})
	probes := make([][32]byte, 20_000)
	for i := range probes {
		probes[i] = kr.NextHash()
	}
	run := func() time.Duration {
		start := time.Now()
		for _, k := range probes {
			_, _ = store.Get(k)
		}
		return time.Since(start)
	}

	run() // Warm the page cache, so the cold case is not measuring the first read
	hot := run()

	BloomResidentBytes = 0
	store.History.Lock()
	store.keepHistoryBlooms()
	store.History.Unlock()
	run()
	cold := run()

	perHot := hot.Nanoseconds() / int64(len(probes))
	perCold := cold.Nanoseconds() / int64(len(probes))
	t.Logf("absent-key lookup over %d history segments:", len(history))
	t.Logf("  filters resident: %6d ns", perHot)
	t.Logf("  filters on disk:  %6d ns  (%.1fx)", perCold, float64(perCold)/float64(perHot))
}
