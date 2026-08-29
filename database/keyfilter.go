package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// The rolling key filter (issue #44).
//
// A store-level filter exists to prove a key ABSENT without touching a
// segment: that is the question every immutable write asks, and the
// answer to it used to decay with the seal count.  The first version
// covered every key the store had ever held, and that is what it cost:
// measured at 1M keys it was 7.45 MB of a 8.95 MB filter budget, ~9
// bytes a key, growing without limit, and rebuilding it on open meant
// scanning every segment the store had ever sealed.
//
// A running node does not reach far back in history, so the filter
// does not need to.  Filters cover 2N blocks and a fresh one starts
// every N blocks, so two are live at once and every write goes into
// both:
//
//	blocks:   0 ────── N ────── 2N ────── 3N ────── 4N
//	filter A  [───────────────)                          dropped at 2N
//	filter B          [───────────────)                  dropped at 3N
//	filter C                   [───────────────)         dropped at 4N
//
// At block t the live pair reaches back N blocks just after a roll and
// 2N just before the next one, so the store holds N to 2N blocks of
// keys in memory and never more.  A filter that completes its span is
// dropped: the block sets its blocks have been packed into carry a
// filter of their own (blockset.go), so writing it out would duplicate
// one.  N is FilterBlocks, persisted in the manifest.
//
// What a filter covers is a BLOCK RANGE, and that is the whole of its
// claim: filter S holds every key of every segment whose blocks all lie
// at or above S, and every key in the live tail.  A segment is not
// always one block -- a merge (MergeBelow) folds a run of blocks into
// one segment, and SegmentMeta.Span records how far back it reaches --
// and a segment reaching below S is simply not covered, so a lookup the
// filters cannot settle walks it.  The filters therefore settle "not in
// the window", and that is the whole of what a WRITE asks: immutability
// is a windowed guarantee, a key written in the last N to 2N blocks
// cannot be overwritten and older history is not consulted, because
// the check is there for replay safety and a Perm key is the hash of
// its value (SegmentStore.lookup).  A READ goes on below the window
// and into the packed sets, each of which carries a filter of its own.
//
// The failure modes are still not symmetric.  A filter claiming a key it
// does not have costs a walk; a filter DENYING a key that is in a
// covered segment turns a Get into a wrong answer.  So a filter is used
// only where its coverage is certain: built from the segments it claims
// (rebuildKeyFilters), or loaded from a save whose claim -- the window's
// start, the newest segment, the count of segments covered -- matches
// the store as it opens.  Any doubt rebuilds, and a rebuild is bounded
// by the window rather than by the store.

const (
	// DefaultFilterBlocks is the roll period N for a store that is not
	// told otherwise: a fresh filter every 1,000 blocks, so the live
	// pair covers 1,000 to 2,000 blocks -- a quarter to half an hour of
	// chain at a block a second.
	//
	// The window is the reach of the immutability check (lookup), so N
	// trades that reach against two bounded costs: what the two filters
	// hold, at ~1.5 bytes a key for the keys of 2N blocks, and what a
	// reopen after a crash rescans, the index of every segment inside
	// the window at 48 bytes a key.  At 5,000 entries a block that is
	// 15 MB a filter and a rescan of 480 MB; at 100 entries a block, the
	// 4 KB floor per filter and 10 MB.  1,000 is fifty times the healing
	// floor MinFilterBlocks sets and well past any finalisation
	// watermark that packs segments into sets, so the window reaches
	// every write a node makes on its own behalf and none it asks for
	// by API.
	DefaultFilterBlocks = 1_000

	// MinFilterBlocks is the smallest roll period a store accepts.
	// Healing writes reach back over several blocks, and a window that
	// cannot hold them would send every healing write below it.  20 is
	// the repository owner's floor for that.
	MinFilterBlocks = 20

	filtersFilename    = "filters.dat"     // Persisted live filters
	filtersTmpFilename = "filters_tmp.dat" // Written first, renamed over
	filtersMagic       = 0x4B465731        // "KFW1"
)

// keyFilter is one live filter: a membership set over every key the
// store holds from block `start` on.
type keyFilter struct {
	start uint64
	keys  *BloomSet
}

// first is the oldest block a segment holds.  A sealed block's segment
// holds only that block; a merged segment reaches Span blocks further
// back.
func (m SegmentMeta) first() uint64 {
	return m.Height - m.Span
}

// filterStarts
// The starts of the filters live at block height t with roll period n:
// the filter that began at the last multiple of n, and the one before
// it, oldest first.  There is no "before" until the first roll, so a
// store's first n blocks have one filter -- and a store whose block
// never advances (the Dyna layer) keeps that one filter forever, over
// everything, bounded by its own compaction.
func filterStarts(t, n uint64) (starts []uint64) {
	current := t / n * n
	if current >= n {
		starts = append(starts, current-n)
	}
	return append(starts, current)
}

// filterTest
// Whether the store might hold the key somewhere in the window.  A
// store with no filters cannot say, which reads as "might".
func (s *SegmentStore) filterTest(key [32]byte) bool {
	if s.filters == nil {
		return true
	}
	for _, f := range s.filters {
		if f.keys.Test(key) {
			return true
		}
	}
	return false
}

// filterSet records a key in every live filter
func (s *SegmentStore) filterSet(key [32]byte) {
	for _, f := range s.filters {
		f.keys.Set(key)
	}
}

// filterCount is the keys inserted into the live filters, summed
func (s *SegmentStore) filterCount() (n uint64) {
	for _, f := range s.filters {
		n += f.keys.Count()
	}
	return n
}

// windowStart is the block the oldest live filter begins at: a segment
// reaching below it is not covered by the filters.  ok is false when
// the store has no filters and covers nothing.
func (s *SegmentStore) windowStart() (start uint64, ok bool) {
	if len(s.filters) == 0 {
		return 0, false
	}
	return s.filters[0].start, true
}

// covered reports whether every block a segment holds lies inside the
// window, so that the filters answer for it.  The caller must hold the
// Mutex.
func (s *SegmentStore) covered(seg *segment) bool {
	start, ok := s.windowStart()
	return ok && seg.meta.first() >= start
}

// SetFilterBlocks
// Set the roll period N and record it in the manifest.  The live
// filters are rebuilt for the new schedule, so this is meant to be
// called when the store is created, before it holds much.
func (s *SegmentStore) SetFilterBlocks(n uint64) (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if err = s.checkOpen(); err != nil {
		return err
	}
	if err = checkFilterBlocks(n); err != nil {
		return err
	}
	if n == s.FilterBlocks {
		return nil
	}
	s.FilterBlocks = n
	if err = s.rebuildKeyFilters(); err != nil {
		return err
	}
	return s.writeManifest()
}

// checkFilterBlocks rejects a roll period the store cannot run on
func checkFilterBlocks(n uint64) error {
	if n < MinFilterBlocks {
		return fmt.Errorf("filter window of %d blocks is below the minimum of %d", n, MinFilterBlocks)
	}
	return nil
}

// advanceBlock
// Move the live tail on to a later block, and roll the filters to match.
// Every change to blockHeight goes through here so that no path can
// advance the block and leave the window behind.  The caller must hold
// the Mutex.
func (s *SegmentStore) advanceBlock(height uint64) {
	if height <= s.blockHeight {
		return
	}
	s.blockHeight = height
	s.rollKeyFilters()
}

// rollKeyFilters
// Bring the live filters into line with the block the tail is in: drop
// any whose span is complete, and start any the schedule now calls for.
// The caller must hold the Mutex.
//
// A filter that starts at S must hold every key from block S on.  At
// the roll that starts it -- a block boundary, when the tail has just
// been sealed into the block before S -- that is nothing at all, so a
// roll is an allocation and no more.  But the block can also jump: a
// shard reopened after a quiet spell learns the block the set has
// reached (AdvanceBlock), and a block-boundary seal may close a block
// well past the one the tail accumulated into.  Then segments may
// already lie at or above S, and the new filter is built from them and
// from the tail, which is what buildKeyFilter does in every case.
//
// A store with no filters -- one whose rebuild failed -- stays that
// way: walking is correct, and half a window would not be.
func (s *SegmentStore) rollKeyFilters() {
	if s.filters == nil {
		return
	}
	wanted := filterStarts(s.blockHeight, s.FilterBlocks)
	var kept []*keyFilter
	for _, f := range s.filters {
		live := false
		for _, start := range wanted {
			if f.start == start {
				live = true
			}
		}
		if live {
			kept = append(kept, f)
		} else if n := f.keys.Count(); n > s.spanKeys {
			s.spanKeys = n // Its span is complete: this is what a span takes
		}
	}
	// Size a filter that is starting for what a full span held, which
	// is the best estimate of what this one will take.  The estimate is
	// the largest span seen so far; issue #54 would make it the largest
	// over a recent period, which is why the completed count is recorded
	// above rather than read from the filter being dropped.
	expected := s.spanKeys
	for _, f := range kept {
		if n := f.keys.Count(); n > expected {
			expected = n
		}
	}
	for _, start := range wanted {
		have := false
		for _, f := range kept {
			if f.start == start {
				have = true
			}
		}
		if have {
			continue
		}
		f, err := s.buildKeyFilter(start, expected)
		if err != nil {
			s.filters = nil // Never under-report: walk instead
			return
		}
		kept = append(kept, f)
	}
	s.filters = kept
}

// buildKeyFilter
// A filter over every key the store holds from block `start` on: the
// segments whose blocks all lie at or above it, the live tail, and the
// packed sets that reach block `start` -- a set is packed from blocks
// the filter still claims, and a rebuild that left its keys out would
// let a key written inside the window be rewritten after a crash.  A
// set overlapping the window contributes all of its keys, which is a
// superset and bounded by the set's size, not the store's.  Sized for
// the larger of what those hold and the caller's estimate.  The caller
// must hold the Mutex.
//
// Cold is not attached until the shard set has opened every shard, so a
// filter built before that is marked as lacking the cold keys and
// attachCold adds them.
func (s *SegmentStore) buildKeyFilter(start, expected uint64) (f *keyFilter, err error) {
	var held uint64
	for _, seg := range s.segments {
		if seg.meta.first() >= start {
			held += uint64(seg.count)
		}
	}
	held += uint64(len(s.live))
	if held > expected {
		expected = held
	}
	f = &keyFilter{start: start, keys: s.newKeyFilter(expected)}
	for _, seg := range s.segments {
		if seg.meta.first() >= start {
			if err = s.addSegmentKeys(f.keys, seg); err != nil {
				return nil, err
			}
		}
	}
	for key := range s.live {
		f.keys.Set(key)
	}
	if s.cold == nil {
		s.filtersLackCold = true
	} else if err = s.cold.forEachKeySince(start, f.keys.Set); err != nil {
		return nil, err
	}
	return f, nil
}

// rebuildKeyFilters
// Build the live filters from what the store actually holds, for the
// block the tail is in.  Bounded by the window: a segment below it is
// never read.  A rebuild that fails leaves no filters, which is the
// safe direction -- lookups walk, as they did before filters existed.
// The caller must hold the Mutex.
func (s *SegmentStore) rebuildKeyFilters() (err error) {
	s.filters = nil
	s.filtersLackCold = false
	var filters []*keyFilter
	for _, start := range filterStarts(s.blockHeight, s.FilterBlocks) {
		f, err := s.buildKeyFilter(start, 0)
		if err != nil {
			return err
		}
		filters = append(filters, f)
	}
	s.filters = filters
	return nil
}

// filterClaim
// What a saved set of filters covers, recorded in the manifest written
// straight after the save and checked on open.  Three numbers, because
// each catches a way the store can change underneath a save:
//
//   - Start is the window's beginning; if the block the store reopens
//     at wants a different window, the save is for the wrong blocks.
//   - Segments counts the segments the window covers; a segment adopted
//     by recoverOrphans, or dropped for a set without a manifest, moves
//     it.  A count of zero says "covers nothing" out loud, which a
//     newest-segment identity alone could not (issue #35: the zero
//     value is also the first segment a store seals).
//   - Height and Seq name the newest covered segment, so that a count
//     which happens to match still has to be the same segments.
type filterClaim struct {
	Start    uint64
	Height   uint64
	Seq      uint64
	Segments uint64
}

// currentFilterClaim is the claim the live filters would make now.  The
// caller must hold the Mutex.
func (s *SegmentStore) currentFilterClaim() (c filterClaim, ok bool) {
	start, ok := s.windowStart()
	if !ok {
		return c, false
	}
	c.Start = start
	for _, seg := range s.segments {
		if seg.meta.first() < start {
			continue
		}
		c.Segments++
		if c.Segments == 1 || seg.meta.after(SegmentMeta{Height: c.Height, Seq: c.Seq}) {
			c.Height, c.Seq = seg.meta.Height, seg.meta.Seq
		}
	}
	return c, true
}

// loadKeyFilters
// Restore the live filters: from filters.dat when the manifest proves
// the save still covers the store as it stands, and by a bounded
// rebuild otherwise.  Called after recoverOrphans, because an adopted
// segment changes what a filter has to cover, and before openLive,
// which adds the tail's keys as it replays them.
func (s *SegmentStore) loadKeyFilters(m *StoreManifest, adopted int) (err error) {
	if m.FilterValid && adopted == 0 {
		if filters, err := loadFilterFile(s.Directory); err == nil {
			s.filters = filters
			want := filterClaim{Start: m.FilterStart, Height: m.FilterHeight, Seq: m.FilterSeq, Segments: m.FilterSegments}
			if have, ok := s.currentFilterClaim(); ok && have == want && s.filtersMatchSchedule() {
				s.filtersLackCold = false // Saved from filters that had them
				return nil
			}
		}
	}
	return s.rebuildKeyFilters()
}

// filtersMatchSchedule says the loaded filters are exactly the ones the
// block the store is in calls for
func (s *SegmentStore) filtersMatchSchedule() bool {
	wanted := filterStarts(s.blockHeight, s.FilterBlocks)
	if len(wanted) != len(s.filters) {
		return false
	}
	for i, f := range s.filters {
		if f.start != wanted[i] {
			return false
		}
	}
	return true
}

// saveKeyFilters
// Persist the live filters to filters.dat via a tmp file and an atomic
// rename, and return the claim the manifest should carry for them.
func (s *SegmentStore) saveKeyFilters() (c filterClaim, err error) {
	c, ok := s.currentFilterClaim()
	if !ok {
		return c, fmt.Errorf("no filters to save")
	}
	tmpPath := filepath.Join(s.Directory, filtersTmpFilename)
	f, err := os.Create(tmpPath)
	if err != nil {
		return c, err
	}
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tmpPath)
		}
	}()
	var header [8]byte
	binary.BigEndian.PutUint32(header[:], filtersMagic)
	binary.BigEndian.PutUint32(header[4:], uint32(len(s.filters)))
	if _, err = f.Write(header[:]); err != nil {
		return c, err
	}
	for _, kf := range s.filters {
		var start [8]byte
		binary.BigEndian.PutUint64(start[:], kf.start)
		if _, err = f.Write(start[:]); err != nil {
			return c, err
		}
		if err = kf.keys.write(f); err != nil {
			return c, err
		}
	}
	if err = f.Sync(); err != nil {
		return c, err
	}
	if err = f.Close(); err != nil {
		f = nil
		return c, err
	}
	f = nil
	if err = os.Rename(tmpPath, filepath.Join(s.Directory, filtersFilename)); err != nil {
		return c, err
	}
	return c, syncDir(s.Directory)
}

// loadFilterFile reads filters.dat back
func loadFilterFile(directory string) (filters []*keyFilter, err error) {
	f, err := os.Open(filepath.Join(directory, filtersFilename))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var header [8]byte
	if _, err = io.ReadFull(f, header[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(header[:]) != filtersMagic {
		return nil, fmt.Errorf("%s has the wrong magic number", filtersFilename)
	}
	n := binary.BigEndian.Uint32(header[4:])
	if n == 0 || n > 2 {
		return nil, fmt.Errorf("%s holds %d filters; a store has one or two", filtersFilename, n)
	}
	for i := uint32(0); i < n; i++ {
		var start [8]byte
		if _, err = io.ReadFull(f, start[:]); err != nil {
			return nil, err
		}
		keys, err := readBloomSet(f)
		if err != nil {
			return nil, err
		}
		filters = append(filters, &keyFilter{start: binary.BigEndian.Uint64(start[:]), keys: keys})
	}
	return filters, nil
}
