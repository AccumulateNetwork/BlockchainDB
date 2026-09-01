package blockchainDB

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Sealed segments as storage.
//
// The transport format from segment.go becomes the storage format: a
// SegmentStore keeps its data in a chain of immutable sealed segment
// files plus one live tail that accepts writes.  Nothing already
// written is ever moved or rewritten.
//
//	live.dat            records accepted since the last seal
//	seg-<height>.dat    a sealed, immutable segment (transport format)
//	seg-<height>.idx    its key index: sorted 48-byte records + a bloom
//	segments.json       the active manifest: the store's settings and
//	                    the sealed segments of the newest 2N blocks
//	history.json        the history manifest: every older segment
//	filters.dat         the live key filters, saved on close (keyfilter.go)
//
// A store is two tiers with two locks (issue #57).  The ACTIVE tier is
// the live tail and the sealed segments of the last N to 2N blocks --
// the window the key filters cover -- and it is what a commit writes
// and a consensus-path read hits; Mutex is its lock.  HISTORY is every
// sealed segment below that window, plus the packed sets; History is
// its lock, and merging, compacting and packing it are its business.
// A segment moves from active to history when the window rolls past
// it (handoffBelowWindow), which is the only moment both locks are
// held together.  See the field comments on SegmentStore for the
// rules, and docs/design/segment-store.md for why.
//
// An index record is key(32) offset(8) length(8), and the offset is
// relative to the segment's BODY -- its records, after the header --
// so that a body and its index can be copied into a larger file
// unchanged (blockset.go); see segment.value.
//
// What this buys over the v1 kfile/history/values arrangement it
// replaced (removed; see docs/design/segment-store.md):
//
//   - Sync is a file copy.  A sealed .dat is byte-identical to what a
//     peer sends; importing means copying the file, building its index,
//     and committing one manifest update -- no re-inserting records.
//   - No move-and-rewrite.  v1's history.dat relocated whole key bins
//     when they grew; sealed segments never move, so writes cost what
//     they weigh.
//   - Compaction is crash-atomic (issue #19).  A compaction writes a
//     new sealed generation and commits by replacing the manifest --
//     one atomic rename.  There is no window where keys and values
//     disagree; a crash simply leaves the old generation in force.
//
// Lookups check the live tail, then segments newest to oldest.  Each
// segment's bloom filter (sized from its key count) keeps that to
// about one binary search in the common case.  A mutable store lets a
// newer segment shadow an older one; an immutable store rejects a
// conflicting value and treats an identical rewrite as a no-op.

const (
	segManifestName = "segments.json"
	segHistoryName  = "history.json"
	segLiveName     = "live.dat"
	segFilePrefix   = "seg-"
	segDataSuffix   = ".dat"
	segIndexSuffix  = ".idx"
	segTmpSuffix    = ".tmp"
	segCompactName  = "compact"

	segWriteBuffer = 1 << 20 // Buffer for writing a segment's records

	segIndexMagic   = 0x53494458 // "SIDX"
	segIndexVersion = 1

	// StoreFormatVersion is the on-disk layout this build reads and
	// writes: the manifest's shape, the files beside it, and what the
	// segment headers mean.
	//
	// The check on it is STRICT -- a store whose manifest carries any
	// other version is refused, not opened and worked around.  There is
	// no database in the wild predating this, so there is nothing to be
	// compatible with, and a hard failure is worth more than a silent
	// one: every field added to the manifest so far happens to have a
	// zero value that degrades safely, but nothing enforced that and
	// the next field need not be so lucky.  Refusing to open says so
	// immediately, at the one moment a person can act on it.
	//
	// Version 2 added the block-set directory beside the shards
	// (blockset.go) and made index offsets relative to the segment body
	// rather than the file.  A build reading version 1 would open a
	// version 2 database without complaint and answer "not found" for
	// every key that had been packed into a set, which is exactly the
	// silent failure the check exists to refuse.
	//
	// Version 3 replaced the whole-history key filter (bloom.dat and its
	// newest-segment coverage claim) with the rolling window of
	// keyfilter.go: filters.dat, a block-range claim, the roll period
	// FilterBlocks, and a Span on each merged segment saying how far
	// back it reaches.  A version-2 build would take a merged segment
	// for a single block and let a filter claim keys it never held.
	//
	// Version 4 split the manifest in two (issue #57): segments.json
	// names the active tier and history.json names history, so that a
	// commit and a merge each write their own and neither waits for the
	// other.  The union of the two is the store; a segment may be named
	// by both while its handoff is being recorded.  Shadowed is gone
	// from the manifest with the whole-layer compaction it drove.  A
	// version-3 build would read segments.json alone and silently lose
	// every segment history.json names.
	StoreFormatVersion = 4
	segIndexHdrSize    = 32 // magic(4) version(4) count(8) bloomBytes(8) bloomK(4) reserved(4)
	segDataHdrSize     = 24 // magic(4) version(4) kind(1) unused(7) count(8) -- segment.go's stream header
	segRecHdrSize      = 40 // key(32) valueLen(8) precede each value in a .dat
)

// SegmentMeta
// One sealed segment as recorded in the manifest
// A segment is identified by (Height, Seq), not by Height alone.
// Height is the block the segment belongs to and is globally agreed,
// which is what lets a peer decide whether it already has a segment.
// Seq orders the segments within one block: a live tail that fills
// mid-block seals on its own, and those auto-seals must not consume
// block numbers -- doing so made every later block boundary fail
// permanently (issue #27).
type SegmentMeta struct {
	Height uint64 `json:"height"` // Block the segment belongs to
	Seq    uint64 `json:"seq"`    // Order within that block
	File   string `json:"file"`   // Data file name within the store directory
	Count  uint64 `json:"count"`  // Number of keys indexed
	Hash   string `json:"hash"`   // SHA-256 of the data file, hex

	// Span is how many blocks below Height the segment also holds: 0
	// for a sealed block, more for a merge of several.  The key filter
	// covers a block range, and a segment is inside that range only if
	// its oldest block is (SegmentMeta.first); a merged segment
	// recorded as its newest block alone would be claimed by a filter
	// that never saw its older keys.  Zero is the common value and the
	// safe one to omit: a segment that reaches back to block 0 from
	// block 39 has Span 39, not first 0, so nothing is ambiguous.
	Span uint64 `json:"span,omitempty"`
}

// after reports whether a is a strictly later segment than b
func (a SegmentMeta) after(b SegmentMeta) bool {
	if a.Height != b.Height {
		return a.Height > b.Height
	}
	return a.Seq > b.Seq
}

// StoreManifest
// The active manifest: the store's settings and the sealed segments of
// the active tier.  Replacing it is the commit point for sealing and
// importing.
//
// Segments is the active tier, oldest first, PLUS any segment the
// window has rolled into history that history.json has not yet
// recorded (SegmentStore.pendingHistory).  The rule that makes the two
// manifests one store is that every sealed segment is named by at
// least one of them at every moment: a handoff records nothing, so the
// segment stays here until a history commit names it there, and only
// a later active commit drops it from here.  A segment named by both
// is simply the same segment.
type StoreManifest struct {
	// Version is the on-disk format; see StoreFormatVersion.  It is
	// first so that it is readable at the head of the file.
	Version uint32 `json:"version"`

	Mutable      bool          `json:"mutable"`
	SealLimit    uint64        `json:"sealLimit"`    // 0: unset, caller decides
	FilterBlocks uint64        `json:"filterBlocks"` // Roll period of the key filter; never 0
	BlockHeight  uint64        `json:"blockHeight"`  // Block currently being accumulated
	Segments     []SegmentMeta `json:"segments"`     // Oldest first

	// FilterValid says the persisted key filters (filters.dat) cover
	// the store exactly as this manifest lists it, and the other three
	// fields are that claim: the window's start block, the newest
	// segment inside the window, and how many segments the window
	// covers.  See filterClaim for why each is needed.  The claim is
	// one-shot: only the manifest written straight after a save carries
	// it, and any later commit clears it.
	FilterValid    bool   `json:"filterValid,omitempty"`
	FilterStart    uint64 `json:"filterStart,omitempty"`
	FilterHeight   uint64 `json:"filterHeight,omitempty"`
	FilterSeq      uint64 `json:"filterSeq,omitempty"`
	FilterSegments uint64 `json:"filterSegments,omitempty"`

	// FilterDemand is what completed filter spans took recently, one
	// bucket per hour: what the next filter is sized from (issue #54).
	// Small by construction -- 24 entries -- because a seal rewrites
	// this file.
	FilterDemand []spanDemand `json:"filterDemand,omitempty"`
}

// HistoryManifest
// The history manifest: the sealed segments below the active window,
// oldest first.  Replacing it is the commit point for a merge, a
// history compaction, and the retirement of packed segments.  It
// carries no settings -- those are the active manifest's -- so that a
// history commit needs nothing from the active tier.
type HistoryManifest struct {
	Version  uint32        `json:"version"`
	Segments []SegmentMeta `json:"segments"` // Oldest first
}

// segment
// A sealed segment.
//
// What it keeps in memory is what a lookup needs to decide *whether* to
// read: the key count and the bloom filter.  The two files are
// borrowed from the shared pool for the length of a read and given
// back, so a store holds no descriptors between reads and the process
// stays under a bound no matter how many segments it has sealed
// (issue #30).
type segment struct {
	meta      SegmentMeta
	dataPath  string
	indexPath string
	count     int64 // Indexed keys
	records   int64 // Physical records in the data file; > count if any key repeats

	// bloom is the segment's membership filter, held in memory only
	// while the segment is worth the memory -- the active tier, the
	// window a commit writes and a protocol read walks.  A history
	// segment's filter is COLD: it stays on disk and is probed there
	// (bloomTest).  Holding every filter resident cost 1.5 bytes per
	// key for every key the store had ever held -- growing without
	// limit, scanned by every GC, and read in full at open, so opening
	// took longer the older the store was (issue #64).  What the bloom
	// bits cost cold is what Test reads: K bytes, at K offsets, from a
	// file the pool already holds open and the page cache keeps hot.
	bloom *Bloom

	// Where the bloom lives in the index file, so a cold filter can be
	// probed without materialising it.  Set for every segment.
	bloomOff   int64
	bloomBytes uint64
	bloomK     int
}

// loadBloom brings the segment's filter into memory, if it has one and
// it is not there already
func (s *segment) loadBloom() (err error) {
	if s.bloom != nil || s.bloomBytes == 0 {
		return nil
	}
	index, release, err := s.index()
	if err != nil {
		return err
	}
	defer release()
	bitmap := make([]byte, s.bloomBytes)
	if _, err = index.ReadAt(bitmap, s.bloomOff); err != nil {
		return err
	}
	s.bloom = &Bloom{
		SizeOfMap: float64(s.bloomBytes) / (1024 * 1024),
		NumBytes:  s.bloomBytes,
		Map:       bitmap,
		K:         s.bloomK,
		Capacity:  s.bloomBytes * 8 / BloomBitsPerKey,
		Count:     uint64(s.count),
	}
	return nil
}

// freeBloom drops the resident filter; the segment goes on answering
// from the one on disk
func (s *segment) freeBloom() { s.bloom = nil }

// bloomTest asks the segment's filter whether the key might be here.
// Resident: a memory probe.  Cold: one byte read per hash function,
// from the index file -- the same bits, at the same offsets, without
// holding the map.  An unreadable filter reads as "might", which costs
// a lookup and never a wrong answer.
func (s *segment) bloomTest(key [32]byte) (mightBe bool, err error) {
	if s.bloom != nil {
		return s.bloom.Test(key), nil
	}
	if s.bloomBytes == 0 || s.bloomK < 1 {
		return true, nil // No filter: everything might be here
	}
	index, release, err := s.index()
	if err != nil {
		return true, err
	}
	defer release()
	probe := Bloom{NumBytes: s.bloomBytes, K: s.bloomK}
	var b [1]byte
	for i := 0; i < s.bloomK; i++ {
		idx, mask := probe.ByteMask(key, i)
		if _, err = index.ReadAt(b[:], s.bloomOff+int64(idx)); err != nil {
			return true, err
		}
		if b[0]&mask == 0 {
			return false, nil // Definitely not in this segment
		}
	}
	return true, nil
}

// close
// Retire a segment: drop the pool's descriptors for its files.
//
// This is not "close what we hold" -- a segment holds nothing between
// reads -- it is "stop holding these paths open on our behalf", which
// matters when the caller is about to delete them.  A file being read
// right now is left to its reader, whose last release closes it.
func (s *segment) close() {
	segmentFiles.forget(s.dataPath)
	segmentFiles.forget(s.indexPath)
}

// data borrows the segment's data file; release when the read is done
func (s *segment) data() (f *os.File, release func(), err error) {
	return segmentFiles.acquire(s.dataPath)
}

// index borrows the segment's index file; release when the read is done
func (s *segment) index() (f *os.File, release func(), err error) {
	return segmentFiles.acquire(s.indexPath)
}

// lookup
// Find a key in this segment.  Returns where its value lives in the
// segment's data file.
func (s *segment) lookup(key [32]byte) (dbb *DBBKey, found bool, err error) {
	switch mightBe, err := s.bloomTest(key); {
	case err != nil:
		return nil, false, err
	case !mightBe:
		return nil, false, nil // Definitely not in this segment
	}
	// One borrow for the whole binary search rather than one per probe
	index, release, err := s.index()
	if err != nil {
		return nil, false, err
	}
	defer release()
	var rec [DBKeyFullSize]byte
	lo, hi := int64(0), s.count-1
	for lo <= hi { //                        Index records are sorted by key
		mid := (lo + hi) / 2
		if _, err = index.ReadAt(rec[:], segIndexHdrSize+mid*DBKeyFullSize); err != nil {
			return nil, false, err
		}
		switch bytes.Compare(key[:], rec[:32]) {
		case 0:
			d := new(DBBKey)
			if _, err = d.Unmarshal(rec[:]); err != nil {
				return nil, false, err
			}
			return d, true, nil
		case -1:
			hi = mid - 1
		default:
			lo = mid + 1
		}
	}
	return nil, false, nil
}

// value reads a value out of the segment's data file.
//
// An index entry's offset is RELATIVE to the segment's body -- the
// records after the header -- not to the file.  In a segment file the
// body starts at segDataHdrSize, so that is the base here.  The
// convention exists for the files that contain many bodies: a block
// set copies a segment's body and its index in verbatim and records
// where the body landed, and a reader adds that base instead of this
// one.  Nothing in an index is rewritten when it is moved.
func (s *segment) value(dbb *DBBKey) (value []byte, err error) {
	value = make([]byte, dbb.Length)
	return value, s.readValue(*dbb, value)
}

// readValue reads a value into buf, which must be dbb.Length long: for
// a caller that reads many values in a row and does not want an
// allocation per value.  The offset is body-relative, as every index
// entry's is.
func (s *segment) readValue(dbb DBBKey, buf []byte) (err error) {
	data, release, err := s.data()
	if err != nil {
		return err
	}
	defer release()
	_, err = data.ReadAt(buf, segDataHdrSize+int64(dbb.Offset))
	return err
}

// SegmentStore
// A key/value store built from sealed segments and a live tail.
// Methods are safe for concurrent use.
type SegmentStore struct {
	// Mutex guards the ACTIVE tier: the live tail, the key filters, the
	// block height, and the sealed segments inside the window.  It is
	// the protocol's lock -- what Put, PutIfAbsent, Seal, Sync and the
	// first half of Get take -- and nothing it protects grows with the
	// chain, so nothing held under it takes longer as history does.
	// Writers take it exclusively; readers take it SHARED, because a
	// sealed segment is immutable and the pool hands out descriptors
	// for pread, so the shared lock exists only to keep the live map
	// and the segment list stable while a reader looks (issue #50).
	//
	// History is never taken while Mutex is held on the protocol path.
	// The two meet in exactly two places, both under Mutex first:
	// handoffBelowWindow, which moves the segments the window has
	// rolled past into history and holds History for the length of an
	// append, and Close/Open, which are not the protocol path.  A
	// history operation never takes Mutex.
	//
	// It was an exclusive mutex for reads too, and that serialised
	// every read in the process on one lock whose hold time grew with
	// the segment walk.  Measured on an 8-node Accumulate network at
	// 500 tx/s: the busiest node ran at ~110% CPU -- one core, seven
	// idle -- while block time climbed from 3.0 s to 5.2 s (issue #50).
	Mutex sync.RWMutex

	// History guards the HISTORY tier: the sealed segments below the
	// window.  A merge, a history compaction, or a pack copies those
	// segments with NO lock -- they are immutable -- and takes History
	// exclusively only to swap the list and commit history.json; a
	// reader of history takes it shared for the length of its walk.
	//
	// It exists because one lock covered both tiers, and a merge or a
	// compaction held it for the whole copy: measured on an 8-node
	// Accumulate network at 500 tx/s, every node stopped producing
	// blocks for 12 s at block 400 and 23-32 s at block 1,040 -- ~2.3 s
	// per GB of the dynamic layer, growing without bound -- because a
	// lock on history was held against the protocol (issue #57).
	History sync.RWMutex

	// maint serialises history maintenance -- MergeBelow,
	// CompactHistory, DropBelow -- for the whole of an operation, copy
	// and swap.  Only maintainers take it; the protocol path never
	// does, so it can be held for as long as a copy takes.  Order:
	// maint, then History.  Never Mutex after either.
	maint sync.Mutex

	// handoffMu is a leaf lock over pendingHistory and
	// retireAfterActiveCommit, the two lists the tiers hand each other.
	// Held for the length of a slice append, under either tier lock,
	// never around anything that blocks.
	handoffMu sync.Mutex

	// retireMu is a leaf lock over iterating and pendingDelete, so that
	// either tier can retire a file and either can hold one open.
	retireMu sync.Mutex

	// unlinkMu guards the queue of files waiting to be deleted, which a
	// goroutine drains with no store lock held (unlinkLater): a
	// compaction can retire thousands of files, and deleting them under
	// either tier's lock was a pause proportional to history -- 236 ms
	// on a Seal after a 3,579-segment compaction.  unlinking says the
	// goroutine is running, and unlinkDone is signalled when it stops.
	unlinkMu    sync.Mutex
	unlinkQueue []unlinkItem
	unlinking   bool
	unlinkDone  *sync.Cond

	Directory string
	Mutable   bool // Mutable: newer segments shadow older; immutable: conflicts error

	// SealLimit is the live-tail bound this store was configured with.
	// The store records and restores it but does not act on it: KV2
	// owns the decision to seal (sealPermIfFull/sealDynaIfFull), and
	// before this was persisted a reopened database silently fell back
	// to a default, discarding the only parameter its constructor takes.
	SealLimit uint64

	// FilterBlocks is the key filter's roll period N (keyfilter.go): a
	// fresh filter every N blocks, each covering 2N.  Persisted, like
	// SealLimit, and never 0.
	FilterBlocks uint64

	// blockHeight is the block whose writes the live tail is
	// accumulating.  A block boundary seals at its own height and then
	// advances this, so auto-seals in between are tagged with the block
	// they actually belong to rather than allocating one of their own.
	blockHeight uint64

	// ExternalBlockRecord says something outside this store persists
	// blockHeight, so a block boundary with nothing to seal need not
	// commit a manifest just to record it.
	//
	// It exists because that commit was the dominant cost of a block.
	// A boundary seals every shard, most shards take no writes in any
	// given block, and each of those was paying two fsyncs -- ~11 ms --
	// to persist a number identical across all 512 of them.  KVShard
	// writes it once for the whole set instead (issue #32).
	//
	// It must not be set by a store whose block height nothing else
	// records.  Losing blockHeight is not harmless: SealNext tags an
	// auto-sealed segment with it, so a stale value labels a segment
	// with a block it does not belong to, and ExportBlock -- which
	// selects by block -- would then never export those records.
	ExternalBlockRecord bool

	// active is the sealed segments inside the window, oldest first:
	// every one has first() >= tierStart(), and the key filters cover
	// all of them.  Under Mutex.
	active []*segment

	// history is the sealed segments below the window, oldest first:
	// every one has first() < tierStart().  Under History.  A segment
	// is in exactly one of the two lists; ordered by (Height, Seq),
	// history precedes active, because the window only ever moves up.
	history []*segment

	// historyNewest is the identity of history's newest segment as the
	// active tier last saw it, kept under Mutex so that a seal needing
	// the store's newest identity (nextKeyAt) never takes History.  It
	// can lag: a merge replaces history's newest with one sequence
	// higher, under History alone.  That is safe, because the identity
	// is only compared against heights at or above the window, and
	// everything in history is below it.
	historyNewest SegmentMeta
	historyAny    bool

	// handoffs is the segments the window has rolled into history whose
	// files the active manifest on disk may still name.  An entry is
	// added at the handoff; `recorded` is set by the history commit
	// that names the segment (or what it was merged into); and the
	// active commit after that drops the entry, because it is the first
	// active manifest that does not name the segment.  Until an entry
	// is gone, a history operation that retires the segment's files
	// hands them to retireAfterActiveCommit instead of deleting them.
	// Under handoffMu.
	handoffs []handoff

	// retireAfterActiveCommit is the files history made unreachable
	// that the active manifest on disk may still name.  They are
	// deleted after the next active commit, which is the first manifest
	// that no longer names them.  Under handoffMu.
	retireAfterActiveCommit []string

	// epoch counts handoffs.  A write that had to consult history
	// without the active lock checks it before writing, and starts
	// over if the window rolled in between.  Under Mutex.
	epoch uint64

	live        map[[32]byte]*DBBKey // Keys written since the last seal
	liveFile    *BFile               // Their records
	liveRecords uint64               // Physical records in liveFile (>= len(live) if keys repeat)
	liveDirty   bool                 // Records written to the tail since the last fsync of it

	// closed is set by Close and cleared by Open, under BOTH tier
	// locks, so that a check under either is enough.  Atomic so that
	// Open on an open store can say so without taking either: every
	// KVShard operation calls Open first, maintenance included, and a
	// merge must not queue behind a commit to learn its store is open.
	closed atomic.Bool

	// cold is where this store's keys go once they leave its segments:
	// the block-set files a sharded database packs its finalized Perm
	// segments into (blockset.go).  Nil for a store that has nowhere
	// else to look, which is every Dyna layer and every store outside a
	// KVShard.
	//
	// It sits inside the store so that Get is one call: a key that has
	// left the segments is still the store's key, and a caller reading
	// history should not have to know where the store keeps it.  The
	// key filters do not cover the cold keys -- that is what bounds them
	// (keyfilter.go) -- so a key they rule out is looked for cold by Get,
	// and by nothing else: see lookupHistory.
	cold coldStore

	// iterating counts the iterations and pinned snapshots in flight,
	// and pendingDelete holds the files a commit wanted to delete while
	// one was.
	//
	// ForEach runs its callback without any store lock (issue #31), so
	// a merge or a compaction can commit underneath it.  That is fine
	// for what the iteration reports -- it snapshots the segment lists
	// and shows the store as it was when it started -- but not for the
	// files: the commit deletes the segments it replaced, and the
	// iterator is still reading them.  Deferring the unlink until the
	// last iteration finishes keeps those reads valid.  The files are
	// already unreachable through the manifests, so a crash in between
	// costs nothing: recoverOrphans sweeps them on the next open.
	// Under retireMu.
	iterating     int
	pendingDelete []string

	// filters are the live key filters, oldest first: one or two
	// membership sets over the keys of the last N to 2N blocks and the
	// live tail (keyfilter.go).  They exist to make proving a key ABSENT
	// from the window cheap.
	//
	// Without them, "not here" costs a bloom probe against every sealed
	// segment plus a binary search per false positive, so the answer
	// gets steadily more expensive as the store seals: a store that has
	// sealed 2,600 times pays 2,600 probes for it.  That is the
	// question a write asks -- a new key is absent by definition -- so
	// the write path decayed with the segment count.  A probe of the
	// live pair replaces it for every segment inside the window; the
	// segments below the window and the packed sets each carry a filter
	// of their own and are probed only when the window cannot answer.
	//
	// A false positive costs no more than the walk it replaced, so a
	// stale filter is slow, never wrong.  A false NEGATIVE would be
	// silent data loss -- "not found" for a key that is right there --
	// so the filters are used only where their coverage is proven, and
	// nil (falling back to the walk) wherever it is not.
	filters []*keyFilter

	// filterValid and filterSaved are the coverage claim writeManifest
	// records.  One-shot: only the manifest written immediately after a
	// save carries it.
	filterValid bool
	filterSaved filterClaim

	// filtersLackCold says a filter was built before the cold store was
	// attached, so the cold keys its window reaches still have to be
	// added (attachCold).  Filters loaded from disk were saved complete.
	filtersLackCold bool

	// demand is what completed filter spans took, one bucket per hour,
	// and it is what a filter that is starting is sized from
	// (keyfilter.go, issue #54).  Persisted in the manifest, so a
	// restart sizes from yesterday's demand rather than from a guess.
	demand []spanDemand

	stats storeCounters // Atomic: the read path holds only a shared lock
}

// coldStore
// Somewhere a store's keys can live once its own segments no longer
// hold them.  Every method is scoped to the one store it is attached
// to: a sharded database attaches a view onto its set store that
// answers for that shard alone.
type coldStore interface {
	// lookup finds a key, or reports that it is not held cold
	lookup(key [32]byte) (value []byte, found bool, err error)
	// forEachKeySince visits every key held cold from block `start` on
	// -- at set granularity, so a set straddling `start` is visited
	// whole -- for rebuilding a filter that begins there
	forEachKeySince(start uint64, fn func(key [32]byte)) error
	// forEach visits every key and its value, for iteration
	forEach(fn func(key [32]byte, value []byte) error) error
	// watermark is the highest block held cold, if any is
	watermark() (last uint64, ok bool)
}

// attachCold
// Give the store somewhere to look for keys its segments no longer
// hold.  If the key filters were built from the segments alone -- which
// they are on the first open, before anything could be attached -- the
// cold keys inside each filter's window are added now, so that a key
// written inside the window and already packed is still refused a
// rewrite.
func (s *SegmentStore) attachCold(cold coldStore) (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.History.Lock() // cold is read under either lock; set it under both
	s.cold = cold
	s.History.Unlock()
	if s.filtersLackCold {
		for _, f := range s.filters {
			if err = cold.forEachKeySince(f.start, f.keys.Set); err != nil {
				s.filters = nil // Never under-report: walk instead
				return err
			}
		}
	}
	s.filtersLackCold = false
	return nil
}

// StoreStats
// What the store has been asked to do, and what it did about it.
//
// These exist to settle a design question with measurement rather than
// intuition: an immutable store pays for a lookup on every write, and
// the only thing that lookup can discover is that the key is already
// there.  If that essentially never happens on a real workload, the
// lookup is a tax on every write to catch a case that does not occur,
// and a write could be a pure append with duplicates reconciled later.
// If it happens often, the check is earning its keep.  PutDuplicate
// over PutTotal is that ratio.
//
// The filter counters answer the companion question -- what the
// store-level filter is actually buying -- including its false
// positive rate in situ rather than at its design point.
type StoreStats struct {
	PutTotal     uint64 // Writes attempted
	PutNew       uint64 // Key was absent: a record was appended
	PutDuplicate uint64 // Key was present with the same value: write avoided
	PutConflict  uint64 // Key was present with a different value

	LookupTotal  uint64 // Lookups that reached the sealed segments logic
	FilterAbsent uint64 // Filters ruled the window out: no segment inside it touched
	FilterWalked uint64 // Filters said "maybe" (or were absent): the window walked
	FilterMisled uint64 // Walked the window on the filters' say-so and found nothing
	LiveHit      uint64 // Answered from the live tail, before any filter
}

// storeCounters is StoreStats as the store keeps it: atomics, because
// the counters on the read path are bumped under a SHARED lock, and a
// plain increment there is a data race.
type storeCounters struct {
	putTotal, putNew, putDuplicate, putConflict           atomic.Uint64
	lookupTotal, filterAbsent, filterWalked, filterMisled atomic.Uint64
	liveHit                                               atomic.Uint64
}

// Stats
// A snapshot of the store's counters.  Taken without any lock: each
// counter is read atomically, and a snapshot that straddles a
// concurrent operation is off by one, which is what a counter is for.
func (s *SegmentStore) Stats() StoreStats {
	return StoreStats{
		PutTotal:     s.stats.putTotal.Load(),
		PutNew:       s.stats.putNew.Load(),
		PutDuplicate: s.stats.putDuplicate.Load(),
		PutConflict:  s.stats.putConflict.Load(),
		LookupTotal:  s.stats.lookupTotal.Load(),
		FilterAbsent: s.stats.filterAbsent.Load(),
		FilterWalked: s.stats.filterWalked.Load(),
		FilterMisled: s.stats.filterMisled.Load(),
		LiveHit:      s.stats.liveHit.Load(),
	}
}

// NewSegmentStore
// Create an empty store.  An existing directory is replaced.
func NewSegmentStore(directory string, mutable bool) (store *SegmentStore, err error) {
	os.RemoveAll(directory)
	if err = os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}
	store = &SegmentStore{Directory: directory, Mutable: mutable, FilterBlocks: DefaultFilterBlocks}
	store.live = make(map[[32]byte]*DBBKey)
	if err = store.rebuildKeyFilters(); err != nil {
		return nil, err
	}
	if err = store.newLiveFile(); err != nil {
		return nil, err
	}
	if err = store.writeHistoryManifest(); err != nil {
		return nil, err
	}
	if err = store.writeManifest(); err != nil {
		return nil, err
	}
	return store, nil
}

// newLiveFile
// Start a fresh live tail, reserving room for the data header that
// Seal fills in
func (s *SegmentStore) newLiveFile() (err error) {
	if s.liveFile, err = NewBFile(filepath.Join(s.Directory, segLiveName)); err != nil {
		return err
	}
	var header [segDataHdrSize]byte // Filled in by Seal, when the count is known
	if _, err = s.liveFile.Write(header[:]); err != nil {
		return err
	}
	s.liveRecords = 0
	// The header is buffered, not written: a crash here leaves a 0-byte
	// file, and open rewrites the header rather than trusting it to be
	// there.  So there is nothing to lose until a record is appended.
	s.liveDirty = false
	return nil
}

// OpenSegmentStore
// Open an existing store: load the manifest, adopt any segment left by
// an interrupted seal, and replay the live tail.
func OpenSegmentStore(directory string) (store *SegmentStore, err error) {
	store = &SegmentStore{Directory: directory}
	if err = store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

// SetSealLimit
// Record the live-tail bound in the manifest so a reopened store
// reports the value it was built with rather than a default.
func (s *SegmentStore) SetSealLimit(limit uint64) (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if err = s.checkOpen(); err != nil {
		return err
	}
	s.SealLimit = limit
	// Resize the key filters' first layers to the scale the caller just
	// declared, while they are still empty and resizing is free.  A
	// store is created before its limit is known, so without this every
	// store starts at the same size regardless of what it is for -- and
	// a 512-shard database creates 1,024 of them.
	if s.filters != nil && s.filterCount() == 0 {
		if err = s.rebuildKeyFilters(); err != nil {
			return err
		}
	}
	return s.writeManifest()
}

// newKeyFilter
// A key filter sized for what this store is expected to hold: the
// caller's estimate if it has one, and otherwise the seal limit, which
// is the only scale the store is told about.  Both are a starting
// point rather than a cap -- a BloomSet adds layers as it fills -- but
// starting near the right size keeps the layer count, and so the cost
// of a lookup, low.  newBloomSized floors tiny filters, so a store
// configured to seal every two records gets 4KB rather than nothing.
func (s *SegmentStore) newKeyFilter(expected uint64) *BloomSet {
	if expected < s.SealLimit {
		expected = s.SealLimit
	}
	return NewBloomSet(expected, 3)
}

// Open
// Reopen a closed store.  Cheap and safe to call on an open store,
// which callers on the hot path rely on.
func (s *SegmentStore) Open() (err error) {
	if !s.closed.Load() {
		return nil // No lock: see closed
	}
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if !s.closed.Load() {
		return nil
	}
	s.History.Lock()
	defer s.History.Unlock()
	return s.load()
}

// load
// Read the store off disk.  The caller must hold both tier locks (or
// hold the only reference, as the constructors do).
//
// The two manifests are read and their union taken -- a segment named
// by both is one segment -- and each segment is then placed in the
// tier its oldest block puts it in, for the block the store is in.
// Tier membership is derived, never recorded: a segment's tier is a
// function of its block range and the window, so a crash between a
// handoff and either commit changes nothing about where a segment
// belongs.  Whichever manifest is found not to match its tier is
// rewritten, so that the store leaves open with each manifest naming
// its own tier and nothing pending between them.
func (s *SegmentStore) load() (err error) {
	s.live = make(map[[32]byte]*DBBKey)
	s.active, s.history = nil, nil
	s.historyNewest, s.historyAny = SegmentMeta{}, false
	s.handoffs, s.retireAfterActiveCommit = nil, nil // The manifests being read are the truth again
	s.liveRecords = 0

	m, err := s.readManifest()
	if err != nil {
		return err
	}
	if m.Version != StoreFormatVersion {
		return fmt.Errorf(
			"%s is on-disk format version %d; this build reads version %d",
			s.Directory, m.Version, StoreFormatVersion)
	}
	hm, err := s.readHistoryManifest()
	if err != nil {
		return err
	}
	if hm.Version != StoreFormatVersion {
		return fmt.Errorf(
			"%s is on-disk format version %d; this build reads version %d",
			filepath.Join(s.Directory, segHistoryName), hm.Version, StoreFormatVersion)
	}
	s.Mutable = m.Mutable
	s.SealLimit = m.SealLimit
	if err = checkFilterBlocks(m.FilterBlocks); err != nil {
		return fmt.Errorf("%s: manifest: %w", s.Directory, err)
	}
	s.FilterBlocks = m.FilterBlocks
	s.blockHeight = m.BlockHeight
	// What spans took before the restart (issue #54).  The ring is
	// indexed by hour%FilterDemandHours, so a manifest carrying fewer
	// buckets -- hand-edited, or written by a build with a different
	// constant -- would panic on the next roll, which is the block
	// commit path.  A ring that is not the right shape is no demand
	// record at all; the store sizes from what its filters hold and
	// learns again at its first roll.
	s.demand = nil
	if len(m.FilterDemand) == FilterDemandHours {
		s.demand = m.FilterDemand
	}

	// The union, in (Height, Seq) order, which is the order both tiers
	// keep and the order the lists had before any restart
	var all []*segment
	seen := make(map[string]bool)
	for _, meta := range append(append([]SegmentMeta(nil), hm.Segments...), m.Segments...) {
		if seen[meta.File] {
			continue
		}
		seen[meta.File] = true
		seg, err := s.openSegment(meta)
		if err != nil {
			return err
		}
		all = append(all, seg)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[j].meta.after(all[i].meta) })

	all, adopted, err := s.recoverOrphans(all)
	if err != nil {
		return err
	}
	// The block the tail is in is at least the block of the newest
	// segment.  The manifest can say less: the Dyna layer's block
	// advances in memory with every KV2.Seal and is persisted only by
	// its own seals, so a segment it sealed and a crash left for
	// recoverOrphans to adopt can sit many blocks above the block the
	// manifest recorded -- and a store that then auto-sealed at the
	// recorded block would refuse, "block height 5 is below the newest
	// segment's block 68".  It also keeps the tiers consistent: a
	// segment is never newer than the window it is placed by.
	if n := len(all); n > 0 && all[n-1].meta.Height > s.blockHeight {
		s.blockHeight = all[n-1].meta.Height
	}
	start := s.tierStart()
	for _, seg := range all {
		if seg.meta.first() < start {
			s.history = append(s.history, seg)
			continue
		}
		s.active = append(s.active, seg)
		// The window's filters are worth memory; history's are read
		// from disk (issue #64).  A filter that will not load is left
		// cold rather than failing the open: cold is correct, just
		// slower, and it is one pread per probe.
		_ = seg.loadBloom()
	}
	if n := len(s.history); n > 0 {
		s.historyNewest, s.historyAny = s.history[n-1].meta, true
	}

	// After recoverOrphans, because an adopted segment changes what the
	// filters have to cover; before openLive, which adds the live keys
	// as it replays them.
	if err = s.loadKeyFilters(m, adopted); err != nil {
		return err
	}
	if err = s.openLive(); err != nil {
		return err
	}
	s.closed.Store(false)

	// Reconcile the tiers with what the manifests name, writing as
	// little as possible: a store opens 1,024 of these at a time.  A
	// history segment the active manifest still names is a handoff in
	// flight, recorded or not according to whether history.json names
	// it, and the commits that follow finish it as they would have.  A
	// segment named by NEITHER -- an adopted orphan -- or an active
	// segment only history.json names -- one the window at the block
	// this store recorded has not yet passed, though a later history
	// commit would drop it -- needs its own tier's manifest to name it
	// before anything else is committed.
	inActive, inHistory := make(map[string]bool), make(map[string]bool)
	for _, meta := range m.Segments {
		inActive[meta.File] = true
	}
	for _, meta := range hm.Segments {
		inHistory[meta.File] = true
	}
	writeHistory, writeActive := false, false
	for _, seg := range s.history {
		switch {
		case inActive[seg.meta.File]:
			s.handoffs = append(s.handoffs, handoff{seg: seg, recorded: inHistory[seg.meta.File]})
		case !inHistory[seg.meta.File]:
			writeHistory = true
		}
	}
	for _, seg := range s.active {
		if !inActive[seg.meta.File] {
			writeActive = true
		}
	}
	if writeHistory {
		if err = s.writeHistoryManifest(); err != nil {
			return err
		}
	}
	if writeActive {
		if err = s.writeManifest(); err != nil {
			return err
		}
	}
	return nil
}

// tierStart is the block that divides the tiers: a segment whose
// oldest block is at or above it is active, and one below it is
// history.  It is the start of the oldest key filter the schedule
// calls for at the block the tail is in -- the same line the filters'
// coverage claim is drawn at (keyfilter.go) -- and a pure function of
// the block height, so it only ever moves up.  The caller must hold
// the Mutex.
func (s *SegmentStore) tierStart() uint64 {
	return filterStarts(s.blockHeight, s.FilterBlocks)[0]
}

// handoffBelowWindow
// Move the active segments the window has rolled past into history.
// Called from advanceBlock, under the Mutex, whenever the block moves;
// it is the one place the protocol path takes History, and it holds
// it for an append.
//
// Nothing is written.  The segments stay named by the active manifest
// until a history commit names them (pendingHistory), so a crash at
// any point leaves them named by at least one manifest.
func (s *SegmentStore) handoffBelowWindow() {
	start := s.tierStart()
	n := 0
	for _, seg := range s.active {
		if seg.meta.first() >= start {
			break // Ordered: the rest are inside the window too
		}
		n++
	}
	if n == 0 {
		return
	}
	moved := s.active[:n]
	s.active = append([]*segment(nil), s.active[n:]...)
	s.epoch++

	s.History.Lock()
	s.history = append(s.history, moved...)
	s.historyNewest, s.historyAny = moved[n-1].meta, true
	s.handoffMu.Lock()
	for _, seg := range moved {
		s.handoffs = append(s.handoffs, handoff{seg: seg})
		// Out of the window, out of memory: history is probed on disk,
		// so the filters the store holds cover the window and no more
		// (issue #64).  This is the one place a segment leaves the
		// active tier while the store runs.
		seg.freeBloom()
	}
	s.handoffMu.Unlock()
	s.History.Unlock()
}

// handoff is one segment in transit between the manifests; see
// SegmentStore.handoffs
type handoff struct {
	seg      *segment
	recorded bool // history.json names it (or its replacement)
}

// releaseFromHistory
// Retire a segment's files on history's behalf: now, if no manifest
// on disk can still name them, and after the next active commit if
// the active manifest might.  The caller must hold History and must
// already have committed a history manifest that does not name the
// segment.
func (s *SegmentStore) releaseFromHistory(seg *segment) {
	seg.close()
	s.handoffMu.Lock()
	defer s.handoffMu.Unlock()
	for i, h := range s.handoffs {
		if h.seg == seg {
			s.handoffs = append(s.handoffs[:i], s.handoffs[i+1:]...)
			s.retireAfterActiveCommit = append(s.retireAfterActiveCommit, seg.dataPath, seg.indexPath)
			return
		}
	}
	s.unlinkLater("release-after-history-commit", seg.dataPath, seg.indexPath)
}

// addSegmentKeys
// Add every key in a sealed segment to a filter, read from the
// segment's index: the keys are already there, sorted, 48 bytes apart.
func (s *SegmentStore) addSegmentKeys(keys *BloomSet, seg *segment) (err error) {
	index, release, err := seg.index() // One borrow for the whole scan
	if err != nil {
		return err
	}
	defer release()
	const batch = 4096 // Index records per read
	buff := make([]byte, batch*DBKeyFullSize)
	for i := int64(0); i < seg.count; {
		n := seg.count - i
		if n > batch {
			n = batch
		}
		b := buff[:n*DBKeyFullSize]
		if _, err = index.ReadAt(b, segIndexHdrSize+i*DBKeyFullSize); err != nil {
			return err
		}
		for j := int64(0); j < n; j++ {
			var key [32]byte
			copy(key[:], b[j*DBKeyFullSize:])
			keys.Set(key)
		}
		i += n
	}
	return nil
}

// checkSegmentHeader
// Verify a segment data file's header.  Both fields were written from
// the beginning and neither was ever read: openSegment took the record
// count on trust, so a file that was not a segment at all -- or was one
// written by a format this build does not understand -- was parsed as
// though it were.
// A segment's KIND, written in the header byte that segment.go's
// stream format documented as the start of "sinceOffset" and that
// nothing has ever written or read.  Zero -- what every file written
// before this says -- is segKindSealed, which is what those files are.
//
// It answers one question, and recovery is the only thing that asks
// it: is a file the manifests do not name a SEAL that reached disk
// before its commit, or the OUTPUT of maintenance that did the same?
// A seal is data that exists nowhere else and must be adopted.  A
// merge, compaction or pack output holds keys that its inputs still
// hold -- the inputs the manifest still names -- so adopting one
// stores every one of those keys twice, until a later merge folds the
// duplicate away.  It is space, not wrong answers, but it is space a
// crash can leave behind at any size (issue #52).
const (
	segKindSealed  byte = 0 // A sealed live tail: adopt it if it is not named
	segKindDerived byte = 1 // Maintenance output: named by a manifest, or garbage
)

// segKindOffset is where the kind byte sits in a segment data header
const segKindOffset = 8

// writeSegmentDataHeader fills in a segment file's header: what it is,
// and how many physical records follow.
func writeSegmentDataHeader(hdr []byte, kind byte, records uint64) {
	binary.BigEndian.PutUint32(hdr[:], segmentMagic)
	binary.BigEndian.PutUint32(hdr[4:], segmentVersion)
	hdr[segKindOffset] = kind
	binary.BigEndian.PutUint64(hdr[16:], records)
}

// segmentKind reads a segment data file's kind byte.  An unreadable
// file reads as sealed, which is the conservative answer: recovery
// adopts it and a later merge sorts out any duplication, rather than
// deleting something that might be the only copy.
func segmentKind(path string) byte {
	f, err := os.Open(path)
	if err != nil {
		return segKindSealed
	}
	defer f.Close()
	var hdr [segDataHdrSize]byte
	if _, err = f.ReadAt(hdr[:], 0); err != nil {
		return segKindSealed
	}
	return hdr[segKindOffset]
}

func checkSegmentHeader(path string, hdr []byte) error {
	if magic := binary.BigEndian.Uint32(hdr[:]); magic != segmentMagic {
		return fmt.Errorf("%s is not a segment file (magic %#08x)", path, magic)
	}
	if v := binary.BigEndian.Uint32(hdr[4:]); v != segmentVersion {
		return fmt.Errorf("%s is segment format version %d; this build reads version %d",
			path, v, segmentVersion)
	}
	return nil
}

// checkIndexHeader is checkSegmentHeader for an index.  Its version was
// likewise written and never read.
func checkIndexHeader(path string, hdr []byte) error {
	if magic := binary.BigEndian.Uint32(hdr[:]); magic != segIndexMagic {
		return fmt.Errorf("%s is not a segment index (magic %#08x)", path, magic)
	}
	if v := binary.BigEndian.Uint32(hdr[4:]); v != segIndexVersion {
		return fmt.Errorf("%s is index format version %d; this build reads version %d",
			path, v, segIndexVersion)
	}
	return nil
}

// openSegment
// Adopt a sealed segment: read what a lookup needs to keep in memory
// -- the record count, the key count, and the bloom filter -- and give
// the descriptors straight back.  Reads borrow them again as needed,
// so adopting a segment costs two opens once, not two descriptors
// forever (issue #30).
func (s *SegmentStore) openSegment(meta SegmentMeta) (seg *segment, err error) {
	dataPath := filepath.Join(s.Directory, meta.File)
	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	return s.openSegmentAt(meta, dataPath, indexPath)
}

// openSegmentAt is openSegment for a segment whose files are not yet at
// the names the manifest will know them by -- an import being checked
// before it is allowed into place.
func (s *SegmentStore) openSegmentAt(meta SegmentMeta, dataPath, indexPath string) (seg *segment, err error) {
	seg = &segment{meta: meta, dataPath: dataPath, indexPath: indexPath}

	data, releaseData, err := seg.data()
	if err != nil {
		return nil, err
	}
	var dataHdr [segDataHdrSize]byte
	if _, err = data.ReadAt(dataHdr[:], 0); err != nil {
		releaseData()
		return nil, err
	}
	if err = checkSegmentHeader(dataPath, dataHdr[:]); err != nil {
		releaseData()
		return nil, err
	}
	seg.records = int64(binary.BigEndian.Uint64(dataHdr[16:]))
	releaseData()

	if _, err = os.Stat(indexPath); err != nil { // Rebuild a missing index
		if err = buildIndexFor(dataPath, indexPath); err != nil {
			seg.close()
			return nil, err
		}
	}
	index, releaseIndex, err := seg.index()
	if err != nil {
		seg.close()
		return nil, err
	}
	defer releaseIndex()

	var header [segIndexHdrSize]byte
	if _, err = index.ReadAt(header[:], 0); err != nil {
		seg.close()
		return nil, err
	}
	if err = checkIndexHeader(indexPath, header[:]); err != nil {
		seg.close()
		return nil, err
	}
	seg.count = int64(binary.BigEndian.Uint64(header[8:]))
	seg.bloomBytes = binary.BigEndian.Uint64(header[16:])
	seg.bloomK = int(binary.BigEndian.Uint32(header[24:]))
	seg.bloomOff = segIndexHdrSize + seg.count*DBKeyFullSize
	// The filter is left on disk.  Whoever places the segment in a tier
	// loads it if the segment is worth the memory -- the active tier --
	// and a history segment is probed cold (issue #64).  Opening a
	// store therefore reads one header per segment rather than every
	// filter it ever wrote.
	return seg, nil
}

// openLive
// Replay the live tail into memory.  Records are read in order, so for
// a mutable store the last record for a key wins -- the same rule
// lookups use.
func (s *SegmentStore) openLive() (err error) {
	path := filepath.Join(s.Directory, segLiveName)
	fi, err := os.Stat(path)
	if err != nil {
		return s.newLiveFile() // No live tail (e.g. crash right after a seal)
	}
	if fi.Size() < segDataHdrSize {
		// seal() creates the new live file and leaves its header in the
		// BFile buffer, so a crash before the first flush leaves the
		// file existing at 0 bytes.  Such a file holds no records, so
		// rewriting it loses nothing -- while keeping it would put the
		// next record at offset 0 and shift the whole tail under the
		// header, where replay reads a key fragment as a record header
		// and silently discards the tail.
		return s.newLiveFile()
	}
	if s.liveFile, err = OpenBFile(path); err != nil {
		return err
	}

	offset := uint64(segDataHdrSize)
	var recHdr [segRecHdrSize]byte
	for offset+segRecHdrSize <= s.liveFile.EOD {
		if err = s.liveFile.ReadAt(offset, recHdr[:]); err != nil {
			return err
		}
		var key [32]byte
		copy(key[:], recHdr[:32])
		length := binary.BigEndian.Uint64(recHdr[32:])
		valueOffset := offset + segRecHdrSize
		if valueOffset+length > s.liveFile.EOD {
			break // Torn tail record from a crash mid-write; drop it
		}
		s.live[key] = &DBBKey{Offset: valueOffset, Length: length}
		s.liveRecords++
		s.filterSet(key)
		offset = valueOffset + length
	}

	// Drop whatever follows the last complete record: a torn record
	// from a crash mid-write, or bytes too few to form a header.
	// Truncating is what makes the drop real -- leaving the bytes puts
	// the next append after them, so every later open reads that
	// record's key as a header and mis-parses the whole tail from
	// there.  The bytes being discarded were never replayable, so no
	// durable record is lost.
	if offset < s.liveFile.EOD {
		if err = s.liveFile.File.Truncate(int64(offset)); err != nil {
			return err
		}
		if err = s.liveFile.File.Sync(); err != nil {
			return err
		}
		s.liveFile.EOD = offset
	}
	// Everything replayed came off the disk, so it is already durable
	s.liveDirty = false
	return nil
}

// recoverOrphans
// Adopt a sealed segment whose manifest update did not complete, and
// delete leftovers a completed compaction made unreachable.
//
// The rule is the manifests' newest height: a data file above it is a
// seal (or a compaction of a store whose every segment was history)
// that reached disk but not the manifest, and is complete by
// construction -- it was fsynced before being renamed into place.  A
// data file at or below that height was superseded by a committed
// merge or compaction.  Takes and returns the store's segments, both
// tiers in (Height, Seq) order, and reports how many were adopted.
func (s *SegmentStore) recoverOrphans(all []*segment) (segs []*segment, adopted int, err error) {
	entries, err := os.ReadDir(s.Directory)
	if err != nil {
		return all, 0, err
	}

	known := make(map[string]bool)
	for _, seg := range all {
		known[seg.meta.File] = true
	}
	var newest SegmentMeta
	haveSegments := len(all) > 0
	if haveSegments {
		newest = all[len(all)-1].meta
	}

	var orphans []SegmentMeta
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, segTmpSuffix) { // Never complete
			os.Remove(filepath.Join(s.Directory, name))
			continue
		}
		if !strings.HasPrefix(name, segFilePrefix) || !strings.HasSuffix(name, segDataSuffix) || known[name] {
			continue
		}
		height, seq, err := keyFromName(name)
		if err != nil {
			continue // Not ours
		}
		orphan := SegmentMeta{Height: height, Seq: seq}
		path := filepath.Join(s.Directory, name)
		drop := func(why string) {
			auditUnlink(s.Directory, why, path)
			os.Remove(path)
			os.Remove(strings.TrimSuffix(path, segDataSuffix) + segIndexSuffix)
		}
		if haveSegments && !orphan.after(newest) { // Superseded by a committed compaction
			drop(fmt.Sprintf("recoverOrphans-superseded(newest=%d-%d)", newest.Height, newest.Seq))
			continue
		}
		// Above the newest, so the height rule calls it an interrupted
		// seal -- but only a SEAL holds data that exists nowhere else.
		// A merge, compaction or pack output is named by a manifest or
		// it is garbage: its keys are all in the inputs the manifest
		// still names, and a run whose every segment is below the
		// watermark produces an output above them all, so the height
		// rule alone would adopt it and store those keys twice
		// (issue #52).
		if segmentKind(path) == segKindDerived {
			drop("recoverOrphans-derived-output")
			continue
		}
		// A shard whose segments were packed into a block set commits a
		// manifest naming none of them; a crash before the unlinks
		// leaves files above nothing at all.  The set holds those keys
		// now, so the cold watermark settles them as the manifest would
		// have (issue #52).
		if s.cold != nil {
			if last, ok := s.cold.watermark(); ok && height <= last {
				drop(fmt.Sprintf("recoverOrphans-packed(watermark=%d)", last))
				continue
			}
		}
		hash, count, err := s.identify(path)
		if err != nil {
			return all, 0, err
		}
		orphans = append(orphans, SegmentMeta{Height: height, Seq: seq, File: name, Count: count, Hash: hash})
	}
	if len(orphans) == 0 {
		return all, 0, nil
	}

	sort.Slice(orphans, func(i, j int) bool { return orphans[j].after(orphans[i]) })
	for _, meta := range orphans {
		seg, err := s.openSegment(meta)
		if err != nil {
			return all, 0, err
		}
		meta.Count = uint64(seg.count) // Indexed keys, not physical records
		seg.meta = meta
		all = append(all, seg) // Above the newest, so this keeps the order
	}
	// The manifests are rewritten by load once the tiers are placed
	return all, len(orphans), nil
}

// heightFromName parses seg-<height>.dat
// keyFromName recovers (height, seq) from a data file name.  Files
// written before segments carried a sequence have no "-seq" part and
// read back as seq 0, which is what they were.
func keyFromName(name string) (height, seq uint64, err error) {
	body := strings.TrimSuffix(strings.TrimPrefix(name, segFilePrefix), segDataSuffix)
	if h, s, ok := strings.Cut(body, "-"); ok {
		if _, err = fmt.Sscanf(h, "%d", &height); err != nil {
			return 0, 0, err
		}
		_, err = fmt.Sscanf(s, "%d", &seq)
		return height, seq, err
	}
	_, err = fmt.Sscanf(body, "%d", &height)
	return height, 0, err
}

// segmentFileName is the data file name for a (height, seq)
func segmentFileName(height, seq uint64) string {
	return fmt.Sprintf("%s%08d-%04d%s", segFilePrefix, height, seq, segDataSuffix)
}

// claimSegmentName publishes a finished file under a segment's name,
// refusing to replace a file already there.  A rename replaces
// silently, and every existing segment file is committed, immutable
// data: a taken name means two segments were minted the same identity
// -- the pair fallback did exactly that (issue #61) -- and overwriting
// turns the identity bug into data loss.  The hard link publishes the
// name atomically or fails with ErrExist leaving both files as they
// were; the temporary name is dropped once the claim holds.
func claimSegmentName(tmpPath, dataPath string) (err error) {
	if err = os.Link(tmpPath, dataPath); err != nil {
		return err
	}
	return os.Remove(tmpPath)
}

// readManifest loads segments.json
func (s *SegmentStore) readManifest() (m *StoreManifest, err error) {
	data, err := os.ReadFile(filepath.Join(s.Directory, segManifestName))
	if err != nil {
		return nil, err
	}
	m = new(StoreManifest)
	if err = json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

// readHistoryManifest loads history.json
func (s *SegmentStore) readHistoryManifest() (m *HistoryManifest, err error) {
	data, err := os.ReadFile(filepath.Join(s.Directory, segHistoryName))
	if err != nil {
		return nil, err
	}
	m = new(HistoryManifest)
	if err = json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

// writeManifest
// Replace the active manifest atomically.  This is the commit point
// for sealing and importing -- and the single durability barrier for
// the whole operation.  The caller must hold the Mutex.
//
// The directory fsync at the end is the only one either of those paths
// performs.  Each of them renames files into this directory before
// calling here -- the sealed data file, its index, then the manifest
// itself -- and one fsync of a directory commits every name change
// made in it, not just the last.  The renames are issued in order and
// journal transactions commit in order, so the manifest can never
// become durable ahead of the data file it names.
//
// What each file's own fsync still buys is different and still
// required: it makes the file's CONTENTS durable before its name is
// published, so a name that survives a crash never points at a file
// that does not.  A seal was paying six barriers for that; three of
// them were this directory, fsynced three times over (issue #33).
//
// What it names is the active tier plus the handoffs no history commit
// has recorded yet; what it takes with it is the handoffs one has, and
// the files history left for this commit to delete.  Both are read
// before the manifest is built, so a handoff or a retirement that
// arrives while the file is being written waits for the next commit.
func (s *SegmentStore) writeManifest() (err error) {
	// The coverage claim is true only of the manifest written directly
	// after the filters were saved.  Everything else that writes a
	// manifest -- a seal, an import -- has changed the segment set, so
	// clearing it here means no path can forget to.
	defer func() { s.filterValid = false }()

	s.handoffMu.Lock()
	var unrecorded, recorded []*segment
	for _, h := range s.handoffs {
		if h.recorded {
			recorded = append(recorded, h.seg)
		} else {
			unrecorded = append(unrecorded, h.seg)
		}
	}
	retire := append([]string(nil), s.retireAfterActiveCommit...)
	s.handoffMu.Unlock()

	m := StoreManifest{Version: StoreFormatVersion,
		Mutable: s.Mutable, SealLimit: s.SealLimit, FilterBlocks: s.FilterBlocks,
		BlockHeight: s.blockHeight, FilterDemand: s.demand}
	if s.filterValid {
		m.FilterValid = true
		m.FilterStart, m.FilterHeight = s.filterSaved.Start, s.filterSaved.Height
		m.FilterSeq, m.FilterSegments = s.filterSaved.Seq, s.filterSaved.Segments
	}
	for _, seg := range unrecorded { // Older than anything active
		m.Segments = append(m.Segments, seg.meta)
	}
	for _, seg := range s.active {
		m.Segments = append(m.Segments, seg.meta)
	}
	if err = commitJSON(s.Directory, segManifestName, &m); err != nil {
		return err
	}
	anames := make([]string, 0, len(m.Segments))
	for _, sm := range m.Segments {
		anames = append(anames, sm.File)
	}
	auditUnlink(s.Directory, "ACTIVE-COMMIT["+strings.Join(anames, ",")+"]")

	// Committed.  The handoffs history has recorded are named by neither
	// this manifest nor any later one, and the files history left for
	// this commit are named by nothing.
	s.handoffMu.Lock()
	kept := s.handoffs[:0]
	for _, h := range s.handoffs {
		done := false
		for _, seg := range recorded {
			if h.seg == seg {
				done = true
			}
		}
		if !done {
			kept = append(kept, h)
		}
	}
	s.handoffs = kept
	s.retireAfterActiveCommit = s.retireAfterActiveCommit[len(retire):]
	s.handoffMu.Unlock()
	s.unlinkLater("handoff-drop-after-active-commit", retire...)
	return nil
}

// writeHistoryManifest
// Replace the history manifest atomically: the commit point for a
// merge, a history compaction, and the retirement of packed segments.
// The caller must hold History exclusively (or the only reference).
//
// It names history as it stands, which includes every segment handed
// off so far -- a handoff appends under History, so none can be in
// flight -- and so every handoff is recorded by this commit: the next
// active commit may stop naming them.
func (s *SegmentStore) writeHistoryManifest() (err error) {
	m := HistoryManifest{Version: StoreFormatVersion}
	for _, seg := range s.history {
		m.Segments = append(m.Segments, seg.meta)
	}
	if err = commitJSON(s.Directory, segHistoryName, &m); err != nil {
		return err
	}
	hnames := make([]string, 0, len(m.Segments))
	for _, sm := range m.Segments {
		hnames = append(hnames, sm.File)
	}
	auditUnlink(s.Directory, "HISTORY-COMMIT["+strings.Join(hnames, ",")+"]")
	s.handoffMu.Lock()
	for i := range s.handoffs {
		s.handoffs[i].recorded = true
	}
	s.handoffMu.Unlock()
	return nil
}

// commitJSON writes a manifest to a tmp file, fsyncs it, renames it
// over the name, and fsyncs the directory: the one barrier that makes
// every rename issued before it durable too (see writeManifest)
func commitJSON(directory, name string, m any) (err error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(directory, name+segTmpSuffix)
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(directory, name)); err != nil {
		return err
	}
	return syncDir(directory)
}

// auditUnlink records every deletion of a segment or set file in an
// append-only log beside the data, with who deleted it and why.  A
// diagnostic for issue #61 -- a committed manifest naming a file that
// does not exist -- where the one fact the failure cannot tell you is
// which path removed the file.  Appends are O_APPEND writes with no
// sync: a lost tail costs diagnostic detail, never correctness, and
// the store never reads the log.
func auditUnlink(dir, why string, paths ...string) {
	f, err := os.OpenFile(filepath.Join(dir, "unlink.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	now := time.Now().UTC().Format("15:04:05.000000")
	if len(paths) == 0 { // A bare event, like a manifest commit
		fmt.Fprintf(f, "%s %s\n", now, why)
	}
	for _, p := range paths {
		fmt.Fprintf(f, "%s %s %s\n", now, why, filepath.Base(p))
	}
}

// errStoreClosed is returned by every operation that would read or
// mutate a store whose files are shut.  Close() drops the sealed
// segment list but leaves the live map populated, so without this an
// operation on a closed store runs against half a store: a Seal or
// Compact would commit a manifest built from no segments at all and
// silently drop every sealed segment on the next open, and a Get would
// answer "not found" for keys that are simply not loaded.  Callers
// reopen with Open, which is cheap and safe on an already-open store.
var errStoreClosed = errors.New("segment store is closed")

// errNotFound is what a lookup returns for a key the store does not
// hold.  It is a single value rather than a fresh error per call so
// that a caller can tell "absent" from "the read failed", which
// matters wherever absence is what authorises a write.
var errNotFound = errors.New("not found")

// ErrImmutable is returned when a key already in an immutable store is
// written with a different value.
//
// It is exported because refusing the write is a fact about the
// caller's record model, not a store failure: a caller that classifies
// its own records into the two layers -- which is the point of having
// two, and what ShardWriter assumes by exposing only PutPerm and
// PutDyna -- can carry on from a refusal by writing the record to the
// dynamic layer, but must stop for a store that failed.  Without a
// sentinel the two arrive as a bare error and the only way to separate
// them is to match the message, which made the wording load-bearing
// API that nothing here knew it had promised (issue #28).
var ErrImmutable = errors.New("cannot overwrite immutable value")

// checkOpen reports whether the store can be operated on.  The caller
// must hold the Mutex.
func (s *SegmentStore) checkOpen() error {
	if s.closed.Load() {
		return errStoreClosed
	}
	return nil
}

// Get
// Return the value for a key, wherever the store holds it: the live
// tail, the active segments newest to oldest, then history newest to
// oldest, then the packed sets.
//
// Two locks, never together.  The active tier is read under Mutex,
// shared, and released before history is read under History, shared.
// A segment that moves between the two in the gap is looked at twice
// at worst, and a merge that commits in the gap is seen either as its
// inputs or as its output: both hold the same keys.
func (s *SegmentStore) Get(key [32]byte) (value []byte, err error) {
	s.Mutex.RLock()
	value, inWindow, found, err := s.lookupActive(key)
	s.Mutex.RUnlock()
	if err != nil || found {
		return value, err
	}
	// In an IMMUTABLE store the window is the whole of the protocol's
	// horizon: a key the filters deny is ABSENT, and history is not
	// consulted (spec 1.3).  Probing history cost one resident bloom
	// test per history segment on every miss -- measured at 23% of a
	// validator's CPU (11% in Bloom.Test alone) at block ~245, and
	// growing with the segment count, which is the curve issues #9,
	// #30 and #50 each moved somewhere else.  Permanent data is what
	// grows without limit, and reaching into it is explicit: GetDeep.
	//
	// A MUTABLE store does not stop: dynamic keys are state -- the BPT
	// above all -- and state must resolve wherever it last landed,
	// however long ago it was written.  The walk is affordable because
	// the dynamic layer is small and grows slowly (spec 1.5), and
	// compaction keeps its history to a few segments.
	//
	// inWindow is false only when filters exist and answered; a store
	// without filters walks, which is correct and bounded by the
	// rebuild rule (keyfilter.go).
	if !inWindow && !s.Mutable {
		return nil, errNotFound
	}
	return s.lookupHistory(key, inWindow)
}

// GetDeep
// Find a key wherever it is, the history below the window and the
// packed sets included.  This is the explicit deep read -- export, a
// query API reaching into old blocks, a test proving durability across
// a merge -- and it is not the protocol path: the protocol's horizon
// is the window, and Get answers from it alone (spec 1.3, 1.4).
func (s *SegmentStore) GetDeep(key [32]byte) (value []byte, err error) {
	s.Mutex.RLock()
	value, inWindow, found, err := s.lookupActive(key)
	s.Mutex.RUnlock()
	if err != nil || found {
		return value, err
	}
	return s.lookupHistory(key, inWindow)
}

// lookupActive
// Find a key in the active tier: the live tail, then the sealed
// segments inside the window, newest first.  The caller must hold the
// Mutex.  inWindow reports what the key filters said, which decides
// whether history has to be consulted for a key not found here.
//
// One probe of the live filters settles the window: a "no" is
// definitive for every active segment -- every one of them lies
// inside the window, that being what makes it active -- and that is
// the part of the walk that grows with the seal count.  What the
// filters do not speak for is the history below the window and the
// packed sets, each of which carries a filter of its own.
func (s *SegmentStore) lookupActive(key [32]byte) (value []byte, inWindow, found bool, err error) {
	return s.findActive(key, true)
}

// findActive is lookupActive, counting what happened in the stats
// only when `count` says so: a write that had to consult history looks
// at the active tier twice, and that is one lookup.
func (s *SegmentStore) findActive(key [32]byte, count bool) (value []byte, inWindow, found bool, err error) {
	if err = s.checkOpen(); err != nil {
		return nil, false, false, err
	}
	if count {
		s.stats.lookupTotal.Add(1)
	}
	if dbb, ok := s.live[key]; ok {
		value = make([]byte, dbb.Length)
		if err = s.liveFile.ReadAt(dbb.Offset, value); err != nil {
			return nil, false, false, err
		}
		if count {
			s.stats.liveHit.Add(1)
		}
		return value, true, true, nil
	}
	inWindow = s.filterTest(key)
	if !inWindow {
		if count {
			s.stats.filterAbsent.Add(1)
		}
		return nil, false, false, nil
	}
	if count {
		s.stats.filterWalked.Add(1)
	}
	for i := len(s.active) - 1; i >= 0; i-- { // Newest segment wins
		dbb, found, err := s.active[i].lookup(key)
		if err != nil {
			return nil, true, false, err
		}
		if found {
			value, err = s.active[i].value(dbb)
			return value, true, true, err
		}
	}
	return nil, true, false, nil
}

// lookupHistory
// Find a key below the window: the history segments newest first, then
// the packed sets.  Takes History shared for the length of the walk,
// which is what lets a merge retire the segments it replaced the
// moment its swap is done.  inWindow is what the filters said, for
// the misled counter.
func (s *SegmentStore) lookupHistory(key [32]byte, inWindow bool) (value []byte, err error) {
	s.History.RLock()
	defer s.History.RUnlock()
	if err = s.checkOpen(); err != nil {
		return nil, err
	}
	for i := len(s.history) - 1; i >= 0; i-- { // Newest segment wins
		dbb, found, err := s.history[i].lookup(key)
		if err != nil {
			return nil, err
		}
		if found {
			return s.history[i].value(dbb)
		}
	}
	if s.cold != nil { // Then whatever left the segments for a block set
		value, found, err := s.cold.lookup(key)
		if err != nil {
			return nil, err
		}
		if found {
			return value, nil
		}
	}
	if inWindow && s.filters != nil {
		s.stats.filterMisled.Add(1) // The walk was the filters' fault
	}
	return nil, errNotFound
}

// Put
// Write a key/value pair into the live tail.
//
// In an immutable store, rewriting a key with the same value is a
// no-op (this is what makes replay and re-import idempotent) and
// rewriting it with a different value is an error.  The check reaches
// back over the key filters' window, N to 2N blocks, and no further:
// immutability is a windowed guarantee (issue #44).  The check exists
// for replay safety, which is recent by nature, and a Perm key is the
// hash of its value, so "same key, different value" beyond the window
// would be a hash collision.  Stopping at the window is what keeps a
// write's cost independent of how much history the store holds.  A
// filter hit -- a key that is in the window, or a false positive -- is
// followed wherever it leads, so that a key written in the last N to
// 2N blocks is refused whether it now sits in an active segment, a
// merged segment in history, or a set.
func (s *SegmentStore) Put(key [32]byte, value []byte) (err error) {
	existing, existed, err := s.putUnlessPresent(key, value, s.Mutable)
	if err != nil || !existed {
		return err
	}
	if bytes.Equal(existing, value) {
		s.stats.putDuplicate.Add(1)
		return nil // Same value: no-op
	}
	s.stats.putConflict.Add(1)
	return ErrImmutable
}

// putUnlessPresent
// Append a record unless the key is already held, and report what was
// found.  `blind` skips the check altogether, which is what a mutable
// store's Put wants: it appends and lets the newest record win.
//
// The check takes the Mutex, and only the Mutex, in the common cases:
// a key the filters rule out of the window is written at once, and a
// key found in the active tier is answered at once.  A filter hit
// that the active tier cannot settle -- the key is in history, or the
// hit was a false positive -- is followed into history WITHOUT the
// Mutex, so that the protocol path never holds the two locks together
// and never waits on a history swap while holding the store: the
// Mutex is released, history is read under its own lock, and the
// Mutex is taken again for the write.  If the window rolled in
// between (epoch), the whole check starts over, because a segment the
// active walk saw may now be one the history walk missed.
func (s *SegmentStore) putUnlessPresent(key [32]byte, value []byte, blind bool) (existing []byte, existed bool, err error) {
	s.stats.putTotal.Add(1)
	if blind {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()
		if err = s.checkOpen(); err != nil {
			return nil, false, err
		}
		s.stats.putNew.Add(1)
		return nil, false, s.writeRecord(key, value)
	}
	for {
		s.Mutex.Lock()
		existing, inWindow, found, err := s.lookupActive(key)
		if err != nil {
			s.Mutex.Unlock()
			return nil, false, err
		}
		if found {
			s.Mutex.Unlock()
			return existing, true, nil
		}
		if !inWindow {
			s.stats.putNew.Add(1)
			err = s.writeRecord(key, value)
			s.Mutex.Unlock()
			return nil, false, err
		}
		epoch := s.epoch
		s.Mutex.Unlock()

		existing, err = s.lookupHistory(key, true)
		if err == nil {
			return existing, true, nil
		}
		if !errors.Is(err, errNotFound) {
			return nil, false, err
		}

		s.Mutex.Lock()
		if s.epoch != epoch {
			s.Mutex.Unlock()
			continue // The window rolled while history was being read
		}
		// The key could have arrived in the tail meanwhile; the active
		// tier is the only place it can have gone, and it is still the
		// same tier this loop looked at -- and already counted
		existing, _, found, err = s.findActive(key, false)
		if err == nil && !found {
			s.stats.putNew.Add(1)
			err = s.writeRecord(key, value)
		}
		s.Mutex.Unlock()
		return existing, found, err
	}
}

// writeRecord
// Append a key/value to the live tail.  The caller must hold the Mutex
// and must already have settled whether the write is allowed:
// writeRecord enforces nothing, it just writes.
func (s *SegmentStore) writeRecord(key [32]byte, value []byte) (err error) {
	offset := s.liveFile.EOD + s.liveFile.EOB
	var recHdr [segRecHdrSize]byte
	copy(recHdr[:32], key[:])
	binary.BigEndian.PutUint64(recHdr[32:], uint64(len(value)))
	if _, err = s.liveFile.Write(recHdr[:]); err != nil {
		return err
	}
	if _, err = s.liveFile.Write(value); err != nil {
		return err
	}
	s.live[key] = &DBBKey{Offset: offset + segRecHdrSize, Length: uint64(len(value))}
	s.liveRecords++
	s.liveDirty = true
	s.filterSet(key)
	return nil
}

// PutIfAbsent
// Write a key/value pair only if the key is not already present, and
// report what was found either way.
//
// This exists because the layer above -- KV2.Put, deciding which layer
// a key belongs to -- asked "is this key here?" and then called Put,
// which asked again to enforce immutability.  On a miss each question
// costs a full lookup, and a new key is a miss by definition, so every
// new key paid for the answer twice.  Asking once is also the atomic
// version: the check and the write no longer straddle a gap that
// another writer could reach through.
//
// A read that fails is not proof of absence, so only errNotFound
// authorises the write; anything else is returned.  "Absent" means
// absent from the window the key filters cover, as it does for Put.
func (s *SegmentStore) PutIfAbsent(key [32]byte, value []byte) (existing []byte, existed bool, err error) {
	existing, existed, err = s.putUnlessPresent(key, value, false)
	if err != nil || !existed {
		return nil, false, err
	}
	// Split the two ways a key can already be here, because they mean
	// opposite things: an identical rewrite is a write this check
	// avoided, a differing one is a write it caught.
	if bytes.Equal(existing, value) {
		s.stats.putDuplicate.Add(1)
	} else {
		s.stats.putConflict.Add(1)
	}
	return existing, true, nil
}

// LiveCount
// The number of keys in the live tail (not yet sealed)
func (s *SegmentStore) LiveCount() int {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	return len(s.live)
}

// LiveRecords
// The number of physical records in the live tail.  A mutable store
// leaves one record per write, so a key rewritten n times leaves n
// records: this, not LiveCount, is what bounds the tail's size on disk
// and its replay cost on open.
func (s *SegmentStore) LiveRecords() uint64 {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	return s.liveRecords
}

// unlinkLater
// Queue files a commit has made unreachable for deletion by a
// goroutine that holds no store lock.  The files are already named by
// no manifest, so nothing depends on when the unlink happens: a crash
// first leaves orphans that recoverOrphans sweeps on the next open.
// Close and the tests wait for the queue to drain (awaitUnlinks).
func (s *SegmentStore) unlinkLater(why string, paths ...string) {
	if len(paths) == 0 {
		return
	}
	s.unlinkMu.Lock()
	defer s.unlinkMu.Unlock()
	for _, p := range paths {
		s.unlinkQueue = append(s.unlinkQueue, unlinkItem{why: why, path: p})
	}
	if s.unlinking {
		return
	}
	if s.unlinkDone == nil {
		s.unlinkDone = sync.NewCond(&s.unlinkMu)
	}
	s.unlinking = true
	go s.drainUnlinks()
}

// drainUnlinks deletes queued files until the queue is empty
func (s *SegmentStore) drainUnlinks() {
	for {
		s.unlinkMu.Lock()
		if len(s.unlinkQueue) == 0 {
			s.unlinking = false
			s.unlinkDone.Broadcast()
			s.unlinkMu.Unlock()
			return
		}
		batch := s.unlinkQueue
		s.unlinkQueue = nil
		s.unlinkMu.Unlock()
		for _, it := range batch {
			s.retireWhy(it.why, it.path)
		}
	}
}

// awaitUnlinks blocks until every queued deletion has been done
func (s *SegmentStore) awaitUnlinks() {
	s.unlinkMu.Lock()
	defer s.unlinkMu.Unlock()
	for s.unlinking {
		s.unlinkDone.Wait()
	}
}

// retire
// Delete a file a commit has made unreachable, or defer the delete
// until the iterations and pinned snapshots reading it have finished.
// Safe under either tier lock, or none.
func (s *SegmentStore) retire(path string) {
	s.retireWhy("retire", path)
}

// manifestStillNames reports whether an on-disk manifest still names
// the segment the path belongs to (an index file counts as its data
// file).  The deferred-deletion protocol guarantees it never does by
// the time a path reaches retireWhy; this makes the invariant
// executable, so any future gap in that protocol -- like the identity
// collision of issue #61 -- costs a leaked file and a loud audit line
// instead of a store that cannot open.  A byte scan, not a parse: the
// manifests are small and a segment's name appears in them quoted
// verbatim.
func (s *SegmentStore) manifestStillNames(path string) bool {
	name := filepath.Base(path)
	if strings.HasSuffix(name, segIndexSuffix) {
		name = strings.TrimSuffix(name, segIndexSuffix) + segDataSuffix
	}
	if !strings.HasPrefix(name, segFilePrefix) || !strings.HasSuffix(name, segDataSuffix) {
		return false
	}
	quoted := []byte(`"` + name + `"`)
	for _, mf := range []string{segManifestName, segHistoryName} {
		data, err := os.ReadFile(filepath.Join(s.Directory, mf))
		if err == nil && bytes.Contains(data, quoted) {
			return true
		}
	}
	return false
}

// unlinkItem is one queued deletion and the site that queued it, for
// the issue #61 audit trail
type unlinkItem struct{ why, path string }

// retireWhy is retire with the enqueuing site recorded
func (s *SegmentStore) retireWhy(why, path string) {
	s.retireMu.Lock()
	defer s.retireMu.Unlock()
	if s.iterating > 0 {
		s.pendingDelete = append(s.pendingDelete, path)
		return
	}
	if s.manifestStillNames(path) {
		auditUnlink(s.Directory, "REFUSED-"+why+": an on-disk manifest still names it", path)
		return
	}
	auditUnlink(s.Directory, why, path)
	os.Remove(path)
}

// pin
// Hold off every file deletion until unpin: what a snapshot of either
// tier needs in order to read the files it names after the locks are
// released.  Taken BEFORE the snapshot, so that nothing the snapshot
// names can be deleted between the two.
func (s *SegmentStore) pin() {
	s.retireMu.Lock()
	s.iterating++
	s.retireMu.Unlock()
}

// unpin releases a pin and, if it was the last, deletes what was
// retired meanwhile
func (s *SegmentStore) unpin() {
	s.retireMu.Lock()
	defer s.retireMu.Unlock()
	s.iterating--
	if s.iterating > 0 {
		return
	}
	for _, path := range s.pendingDelete {
		if s.manifestStillNames(path) {
			auditUnlink(s.Directory, "REFUSED-pendingDelete: an on-disk manifest still names it", path)
			continue
		}
		auditUnlink(s.Directory, "pendingDelete", path)
		os.Remove(path)
	}
	s.pendingDelete = nil
}

// beginIterate
// Take a snapshot to iterate: the sealed segments of both tiers as
// they stand, history first, and the live tail's values copied out.
// Holding the segments in a local slice is what makes the iteration a
// snapshot; the pin is what keeps their files readable.  Each tier is
// read under its own lock, one after the other, never both at once.
// The caller must unpin when done.
func (s *SegmentStore) beginIterate() (segs []*segment, live map[[32]byte][]byte, cold coldStore, err error) {
	s.pin()
	defer func() {
		if err != nil {
			s.unpin()
		}
	}()

	s.Mutex.RLock()
	if err = s.checkOpen(); err != nil {
		s.Mutex.RUnlock()
		return nil, nil, nil, err
	}
	// The live tail is not a file the pool can hold open for us -- it is
	// still being appended to -- so its values are copied out under the
	// lock rather than read during the iteration
	live = make(map[[32]byte][]byte, len(s.live))
	for key, dbb := range s.live {
		value := make([]byte, dbb.Length)
		if err = s.liveFile.ReadAt(dbb.Offset, value); err != nil {
			s.Mutex.RUnlock()
			return nil, nil, nil, err
		}
		live[key] = value
	}
	active := append([]*segment(nil), s.active...)
	cold = s.cold
	s.Mutex.RUnlock()

	s.History.RLock()
	segs = append(append([]*segment(nil), s.history...), active...)
	s.History.RUnlock()
	return segs, live, cold, nil
}

// BlockHeight
// The block the live tail is accumulating into: the lowest height
// Seal will accept.  A caller sealing at its own block numbers does
// not need this, but one resuming after a crash does -- the block it
// was about to seal may already be sealed and recorded.
func (s *SegmentStore) BlockHeight() uint64 {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	return s.blockHeight
}

// AdvanceBlock
// Raise the block the live tail is accumulating into, without sealing
// anything and without writing a manifest.
//
// This is how a store with ExternalBlockRecord learns, on open, the
// block that the shard set recorded on its behalf.  It only ever
// raises: a store that has sealed past the recorded block knows better
// than the record does.
func (s *SegmentStore) AdvanceBlock(height uint64) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.advanceBlock(height)
}

// Sync
// Make the live tail durable without sealing it: flush the buffer and
// fsync the file, so every record written so far survives a power loss.
//
// This is the durability point for a store that is not being sealed.
// Sealing is the other one, and it is the only one the Perm layer
// needs, because a block boundary seals its whole tail.  The Dyna
// layer is not sealed at a block boundary -- its segments are local,
// not a peer's unit of sync -- so without this its newest writes sat
// in a 32 KB buffer until the tail filled, and a crash restarted the
// node with permanent records newer than the mutable state that
// indexes them (issue #29).
//
// No manifest is written: the manifest names sealed segments, and the
// live tail is recovered by replaying the file itself.  A tail
// fsynced mid-record is fine -- open truncates a torn record and
// replay resumes on the boundary before it.
//
// Sync is a no-op on a tail that has taken no writes since the last
// one, so the shards a block did not touch cost nothing.
func (s *SegmentStore) Sync() (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.sync()
}

// sync is Sync; the caller must hold the Mutex
func (s *SegmentStore) sync() (err error) {
	if err = s.checkOpen(); err != nil {
		return err
	}
	if s.liveDirty {
		if err = s.liveFile.Flush(); err != nil {
			return err
		}
		if err = s.liveFile.File.Sync(); err != nil {
			return err
		}
		s.liveDirty = false
	}
	// A layer that is synced rather than sealed at the block boundary
	// -- the Dyna layer -- may go many blocks between active commits,
	// and history is waiting on the next one to finish a handoff or to
	// delete the inputs of a compaction the active manifest still
	// names.  Commit one here when there is anything to finish: two
	// barriers, once per compaction, at a boundary that already pays
	// for a sync.  Otherwise a compacted layer keeps its old
	// generations on disk until its tail next fills.
	s.handoffMu.Lock()
	finish := len(s.retireAfterActiveCommit) > 0
	for _, h := range s.handoffs {
		finish = finish || h.recorded
	}
	s.handoffMu.Unlock()
	if finish {
		return s.writeManifest()
	}
	return nil
}

// SealNext
// Seal the live tail without a block boundary, to bound the tail when
// it fills mid-block.  The segment is tagged with the block currently
// being accumulated and takes the next sequence within it, so an
// auto-seal never consumes a block number (issue #27).
func (s *SegmentStore) SealNext() (meta SegmentMeta, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.seal(s.blockHeight, false)
}

// Seal
// Seal the live tail into an immutable segment at the given height and
// start a fresh tail.  Sealing is the store's durability point.
//
// When the tail holds no shadowed records -- the usual case for an
// immutable store -- the file is renamed into place rather than
// copied, so sealing costs a header write, an index build, and a hash.
// Sealing at a block boundary also advances the block the live tail
// accumulates into, so the auto-seals that follow are tagged with the
// next block rather than the one just closed.
func (s *SegmentStore) Seal(height uint64) (meta SegmentMeta, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.seal(height, true)
}

// seal is Seal; the caller must hold the Mutex.  blockBoundary marks
// the seal as closing `height` rather than merely bounding the tail
// inside it.
func (s *SegmentStore) seal(height uint64, blockBoundary bool) (meta SegmentMeta, err error) {
	if err = s.checkOpen(); err != nil {
		return meta, err
	}
	if blockBoundary && s.blockHeight > height {
		return meta, fmt.Errorf("block %d is already closed; now accumulating block %d", height, s.blockHeight)
	}
	seq, err := s.nextKeyAt(height)
	if err != nil {
		return meta, err
	}
	if len(s.live) == 0 {
		// Nothing to seal, but a block boundary still closes the block:
		// the next writes belong to the block after this one
		if blockBoundary && s.blockHeight <= height {
			s.advanceBlock(height + 1)
			if s.ExternalBlockRecord {
				return meta, nil // Recorded once for the whole shard set
			}
			return meta, s.writeManifest()
		}
		return meta, nil
	}

	// The live file is promoted as it stands -- an fsync and a rename,
	// never a rewrite.  A mutable tail holds shadowed records
	// (overwrites), and sealing used to rewrite it to drop them: one
	// pread per record, under the store lock, inside the Put that
	// tipped SealLimit.  At 100,000 records that was a ~98 MB tail read
	// back a syscall at a time every few blocks, and every node paused
	// 10-15 s while 71 goroutines queued on the lock (issue #60).
	//
	// Nothing needs the rewrite.  The index is built from s.live, which
	// already holds the newest offset for every key, so lookups land on
	// the newest copy; the shadowed bytes ride along dead until
	// CompactHistory -- whose job reclamation is, off this lock -- folds
	// them away.  The seal's cost is the flush of what the tail's own
	// buffer holds, one fsync, and a rename: bounded by a constant, not
	// by SealLimit, which is the #57 invariant applied to the innermost
	// part of the active tier.  (An immutable tail never shadows -- a
	// duplicate put is refused or a no-op -- so the Perm layer loses
	// nothing it ever had.)
	sl, seq, err := s.promoteLiveFile(height, seq)
	if err != nil {
		return meta, err
	}
	dataName := segmentFileName(height, seq)
	dataPath := filepath.Join(s.Directory, dataName)

	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	if err = writeIndexFile(indexPath, sl.order, sl.entries); err != nil {
		return meta, err
	}

	meta = SegmentMeta{Height: height, Seq: seq, File: dataName, Count: uint64(len(sl.order)), Hash: sl.hash}
	seg, err := s.openSegment(meta)
	if err != nil {
		return meta, err
	}
	s.active = append(s.active, seg)
	_ = seg.loadBloom() // Active: worth the memory (issue #64)
	// The tail's keys are in every live filter already, and the segment
	// they now sit in is inside the window by construction, so the
	// filters cover it; advancing the block is what may roll them, and
	// what may hand the oldest active segments to history
	s.advanceBlock(height)
	if blockBoundary {
		s.advanceBlock(height + 1) // The next writes belong to the next block
	}

	if err = s.writeManifest(); err != nil { // Commit
		return meta, err
	}

	// The sealed data is committed; start a fresh tail
	s.live = make(map[[32]byte]*DBBKey)
	if err = s.newLiveFile(); err != nil {
		return meta, err
	}
	return meta, nil
}

// newestMeta
// The newest sealed segment by (Height, Seq).  ok is false when the
// store has no sealed segments, so that height 0 (a genesis block) is
// a usable height rather than a sentinel.  The caller must hold the
// Mutex; history is consulted through the identity the active tier
// keeps of it (historyNewest), not through History.
func (s *SegmentStore) newestMeta() (newest SegmentMeta, ok bool) {
	if n := len(s.active); n > 0 {
		return s.active[n-1].meta, true
	}
	return s.historyNewest, s.historyAny
}

// nextKeyAt
// The identity a new segment takes when it is sealed into block
// `height`: the next sequence within that block, or 0 if the block has
// no segments yet.  Returns an error if the store already holds a
// segment from a later block, which would break the ordering every
// other part of the store relies on.
func (s *SegmentStore) nextKeyAt(height uint64) (seq uint64, err error) {
	newest, ok := s.newestMeta()
	if !ok {
		return 0, nil
	}
	if height < newest.Height {
		return 0, fmt.Errorf("block height %d is below the newest segment's block %d", height, newest.Height)
	}
	if height == newest.Height {
		return newest.Seq + 1, nil
	}
	return 0, nil
}

// sealed
// What sealing produced: the segment's hash (empty in a mutable store,
// which never transports a segment) and the index records for it.
// Whoever writes the segment already knows where every value landed,
// so the index is built from these rather than by reading the segment
// back off disk.
type sealed struct {
	hash    string
	order   [][32]byte
	entries map[[32]byte]*DBBKey
}

// promoteLiveFile
// Finish the live file's header, make it durable, and link it into
// place as the sealed segment (height, seq) -- or the next free
// sequence above it.  No record is copied.
//
// The seal is not the only identity minter: a compaction of history's
// newest suffix names its output (historyNewest.Seq+1), and when the
// active tier is empty that is exactly what nextKeyAt mints for the
// seal.  The two race; the exclusive link turns what used to be a
// silent overwrite (issue #61) into ErrExist here, and the seal takes
// the sequence after -- correctly ordered, because the seal holds the
// Mutex and anything that claimed the name is maintenance output at or
// below it.  The caller must use the returned seq.
func (s *SegmentStore) promoteLiveFile(height, seq uint64) (sl sealed, seqOut uint64, err error) {
	count := s.liveRecords
	if err = s.liveFile.Flush(); err != nil {
		return sl, seq, err
	}
	var header [segDataHdrSize]byte
	writeSegmentDataHeader(header[:], segKindSealed, count)
	if err = s.liveFile.WriteAt(0, header[:]); err != nil {
		return sl, seq, err
	}
	if err = s.liveFile.File.Sync(); err != nil {
		return sl, seq, err
	}
	livePath := s.liveFile.Filename
	if err = s.liveFile.File.Close(); err != nil {
		return sl, seq, err
	}
	s.liveFile.File = nil
	// Link, not rename: a rename would silently replace the file of a
	// segment minted the same identity, turning the race above into
	// data loss.  A taken name is skipped, audibly, and the squatter is
	// never touched (issue #61).
	var dataPath string
	for {
		dataPath = filepath.Join(s.Directory, segmentFileName(height, seq))
		err = os.Link(livePath, dataPath)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return sl, seq, err
		}
		auditUnlink(s.Directory, fmt.Sprintf("seal-remint: %s is taken", segmentFileName(height, seq)))
		seq++
	}
	if err = os.Remove(livePath); err != nil {
		return sl, seq, err
	}
	// No directory fsync here: the manifest commit that ends this
	// operation fsyncs the same directory, and that one barrier makes
	// this rename durable too (see writeManifest)

	// The rename moved no bytes, so the live tail's offsets are the
	// sealed segment's offsets
	sl.entries = s.live
	sl.order = make([][32]byte, 0, len(s.live))
	for key := range s.live {
		sl.order = append(sl.order, key)
	}
	if !s.Mutable { // Immutable segments are transported; a peer verifies this
		if sl.hash, _, err = hashAndCount(dataPath); err != nil {
			return sl, seq, err
		}
	}
	return sl, seq, nil
}

// DefaultCompactRatio
// How much newer data must have gathered behind a history segment, as
// a share of that segment's size, before a compaction rewrites it to
// fold that data in.
//
// History is compacted in runs (CompactHistory): the newest segments
// are always the run, and an older segment joins it only once the run
// has grown to this share of the older one.  That is what makes the
// cost amortised: a segment is rewritten only when enough has arrived
// to make the rewrite worth its size, so the bytes rewritten over a
// store's life are a constant multiple -- a few levels deep -- of the
// bytes written, rather than a whole-layer copy per compaction
// (issue #31).  Between rewrites the older segment holds records the
// run's newer copies have superseded; that space is bounded by this
// ratio.
//
// 0.25 folds a segment in once a quarter of its size has gathered
// behind it.
const DefaultCompactRatio = 0.25

// CompactRatio is the ratio CompactHistory uses.  Raise it to rewrite
// large segments less often and hold more garbage; lower it for the
// reverse.
var CompactRatio = DefaultCompactRatio

// CompactPassRecords bounds one compaction pass: a run is chosen so
// that its inputs hold at most this many physical records, whatever
// has accumulated.  The default, 4Mi records, is roughly half a
// gigabyte of Accumulate's dynamic records -- a pass of a few seconds
// on NVMe -- so however large history grows, one pass costs about
// that, and the maint lock it holds never delays the per-shard merges
// behind it by more (issue #59; the unbounded pass measured 12-19 s
// after four hours at 500 tx/s and was still growing).
//
// The price of the bound is that a segment which grows to the budget
// is no longer folded INTO -- garbage that lands in it after that
// waits for the cross-shard consolidation (#47's daily tier) rather
// than for this loop.  Bounded latency for bounded reclamation reach:
// the budget trades the second for the first, and the default keeps a
// segment's frozen garbage under CompactRatio of its size at the
// moment it froze.
var CompactPassRecords uint64 = 4 << 20

// CompactDeepPassRecords bounds the rarer pass that folds two
// segments the ordinary budget has frozen (compactionRun).  It runs
// only when the ordinary pass has nothing to do, folds one pair, and
// halves the frozen count each time, so the dynamic layer converges to
// the size of its live key set instead of growing forever with the
// garbage inside frozen segments (issue #65).
//
// 32Mi records is eight ordinary passes: a few seconds of streaming
// merge, off the protocol path, at a moment when nothing cheaper is
// worth doing.  The dynamic layer is meant to be small (spec 1.5), so
// this is a ceiling that a healthy store never reaches.
var CompactDeepPassRecords uint64 = 8 * CompactPassRecords

// CompactPassSegments bounds a run by FILE COUNT as well as by
// records.  The record budget cannot: a segment holding none passes
// every record test, so a history of empty segments made a run as long
// as history itself, and the merge then opened and streamed every one
// of them under the maintenance lock.
var CompactPassSegments = 1024

// compactionRun
// The run of history worth compacting into one segment, and where it
// sits.  First choice: the newest suffix, taking each older segment in
// turn while it is no larger than 1/ratio times what has gathered
// behind it AND the run stays inside CompactPassRecords.  If that
// yields nothing (the next segment is too big for the budget), fall
// back to the newest ADJACENT PAIR that fits the budget, so
// consolidation still advances -- bounded, one pair per pass -- rather
// than stopping the moment any segment outgrows the budget.  A run
// shorter than two is nothing to do.  Sized by physical record count,
// which every segment holds in memory, so choosing costs no I/O.
func compactionRun(history []*segment, ratio float64) (run []*segment, at int) {
	// The ordinary pass, bounded so a compaction never stalls the
	// maintenance behind it
	if run, at = compactionRunWithin(history, ratio, CompactPassRecords); run != nil {
		return run, at
	}
	// Nothing fits that budget.  A segment that grows past it is
	// otherwise frozen for good: the suffix rule stops at it, and no
	// pair containing it can fit, so every key in it that is later
	// overwritten becomes garbage no pass will ever reclaim -- the
	// dynamic layer's disk grows without limit under exactly the
	// workload it is designed for, a bounded key set rewritten forever
	// (issue #65).  So when the cheap pass has nothing to do, one
	// deeper fold is allowed, on a budget of its own.
	//
	// This cannot run away.  It fires only when the ordinary pass is
	// idle; it folds one pair; the pair must still be worth folding by
	// the same ratio; and each fold leaves one segment where there
	// were two, so the frozen segments halve with each one until a
	// single segment stands and there is nothing left to pair.  The
	// price is that a rare pass costs up to CompactDeepPassRecords
	// instead of CompactPassRecords -- bounded latency, still, just a
	// larger bound, and paid only when the alternative is unbounded
	// disk.
	return compactionRunWithin(history, ratio, CompactDeepPassRecords)
}

// compactionRunWithin is compactionRun under one record budget
func compactionRunWithin(history []*segment, ratio float64, budget uint64) (run []*segment, at int) {
	n := len(history)
	if n == 0 {
		return nil, 0
	}
	// A run is bounded by records AND by segments.  The budget counts
	// records, and a segment with none passes every record test -- so a
	// history of empty or near-empty segments yielded a run of
	// unbounded length, and writeMergedRun then opened and streamed
	// every one of them under the maintenance lock.  What a pass costs
	// is both the bytes it moves and the files it touches.
	var behind uint64
	i := n - 1
	for ; i >= 0; i-- {
		r := uint64(history[i].records)
		if i < n-1 && (float64(r)*ratio > float64(behind) || behind+r > budget ||
			n-i > CompactPassSegments) {
			break // Too big for what gathered behind it, or for one pass
		}
		behind += r
	}
	if run = history[i+1:]; len(run) >= 2 {
		return run, i + 1
	}
	// Pair fallback: the newest adjacent pair that fits one pass AND
	// satisfies the same worth-it rule -- the newer member must be at
	// least ratio of the older, or the fold rewrites much to reclaim
	// little.  Without that gate the fallback would fold what the ratio
	// had just declined.
	for j := n - 2; j >= 0; j-- {
		older, newer := uint64(history[j].records), uint64(history[j+1].records)
		if older+newer > budget || float64(older)*ratio > float64(newer) {
			continue
		}
		// The pair's replacement is named (Height, Seq+1) after its
		// newer member.  A suffix ends where that identity is free, but
		// a pair can sit anywhere in history -- and when several seals
		// share one block, the segment right behind the pair IS
		// (Height, Seq+1).  Folding this pair would mint a second
		// segment under that segment's name and overwrite its committed
		// file; the store then held two segments sharing one file, and
		// releasing either deleted the other's data (issue #61).
		if j+2 < n {
			last, next := history[j+1].meta, history[j+2].meta
			if next.Height == last.Height && next.Seq == last.Seq+1 {
				continue
			}
		}
		return history[j : j+2], j
	}
	return nil, 0
}

// CompactHistory
// Reclaim what overwriting left behind in history: rewrite the run of
// history segments compactionRun chooses into one segment holding
// only the newest record for each key in the run.  Reports whether
// anything was compacted.
//
// This is the Dyna layer's compaction, and it touches HISTORY ONLY.  A
// record written inside the window -- the last N to 2N blocks -- is in
// the active tier, and compaction never reads or rewrites it; what it
// reclaims is a record superseded by a later one that has also left
// the window.  A record in history superseded only by an active one
// stays until that newer record reaches history and a run includes
// both, which bounds the garbage compaction cannot yet see by the size
// of the window, not the size of the layer.  It replaced Compact, which
// rewrote every generation under the store lock: on a node whose
// dynamic layer had grown to 9.8 GB that was a 23-32 s pause of every
// commit and every read, every 128 blocks (issue #57).
//
// The copy takes no store lock: the inputs are immutable and the
// output is written aside.  History is taken exclusively only to swap
// the run for its replacement and commit history.json, and the swap
// checks that the run is still exactly where it was -- a drop or an
// import could have changed history meanwhile -- and abandons the
// output if not.  The identity is the sequence after the run's
// newest.  For a suffix run that identity is free; the pair fallback
// refuses a pair whose successor already holds it, and publishing the
// output refuses to replace an existing file, so a taken identity can
// never overwrite a committed segment (issue #61).  Crash safety is
// the merge's rule: an uncommitted output sits below the newest active
// segment and recoverOrphans deletes it, while the inputs are still
// named.
func (s *SegmentStore) CompactHistory() (compacted bool, err error) {
	s.maint.Lock()
	defer s.maint.Unlock()

	s.History.RLock()
	if err = s.checkOpen(); err != nil {
		s.History.RUnlock()
		return false, err
	}
	run, at := compactionRun(s.history, CompactRatio)
	s.History.RUnlock()
	if run == nil {
		return false, nil
	}
	// A run of one segment that holds one record per key has nothing
	// to reclaim; compactionRun already refuses runs shorter than two,
	// so anything here has at least two segments to fold together.

	last := run[len(run)-1].meta
	meta, seg, err := s.writeMergedRun(run, last.Height, last.Seq+1)
	if errors.Is(err, os.ErrExist) {
		// The replacement's identity is already a segment on disk.
		// compactionRun refuses the pairs that would mint one, so this
		// is the last line of defence: skip the pass rather than let
		// anything overwrite a committed segment's file (issue #61).
		auditUnlink(s.Directory, fmt.Sprintf("compact-refused-name-taken(%d-%d)", last.Height, last.Seq+1))
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// The new segment holds every key of every input, so it reaches
	// back as far as the oldest of them
	meta.Span = last.Height - run[0].meta.first()
	seg.meta = meta
	if maintenanceHook != nil {
		maintenanceHook()
	}

	return s.swapHistory(run, seg, at)
}

// swapHistory
// Commit a history operation: replace `run`, which must still be the
// n-th through last..th segments of history, with `out`, and write the
// history manifest.  Takes History exclusively for exactly that.  If
// history no longer holds the run where it was, or the commit fails,
// the output is discarded and history is left as it was.  The caller
// must hold maint.
func (s *SegmentStore) swapHistory(run []*segment, out *segment, at int) (ok bool, err error) {
	s.History.Lock()
	defer s.History.Unlock()
	discard := func() {
		out.close()
		s.unlinkLater("discard-uncommitted-output", out.dataPath, out.indexPath)
	}
	if err = s.checkOpen(); err != nil {
		discard()
		return false, err
	}
	if at < 0 || at+len(run) > len(s.history) {
		discard()
		return false, nil
	}
	for i, seg := range run {
		if s.history[at+i] != seg {
			discard()
			return false, nil // History changed under the copy
		}
	}
	old := s.history
	s.history = make([]*segment, 0, len(old)-len(run)+1)
	s.history = append(s.history, old[:at]...)
	s.history = append(s.history, out)
	s.history = append(s.history, old[at+len(run):]...)
	if err = s.writeHistoryManifest(); err != nil {
		s.history = old // Uncommitted; the inputs still stand
		discard()
		return false, err
	}
	for _, seg := range run {
		s.releaseFromHistory(seg)
	}
	return true, nil
}

// maintenanceHook, when set, is called by a history operation after
// its copy and before its swap, with no store lock held.  It exists so
// that a test can hold a merge or a compaction at that point and show
// what the protocol path can do meanwhile.  Nil except under test.
var maintenanceHook func()

// writeMergedRun
// Write a run of segments into one new sealed segment at (height, seq)
// holding the newest value for every key in the run, build its index,
// and open it.  No lock is needed -- the inputs are immutable -- and
// the caller is responsible for committing it into the segment list.
//
// Two passes over the inputs' sorted indexes, both streaming
// (mergeIndexes).  The first only counts distinct keys, which is what
// the data header, the index header and the bloom filter need to be
// written up front -- and an immutable segment's hash covers its
// header, so the header cannot be patched in afterwards.  The second
// emits each winner in key order: its value is read from whichever
// input holds it and appended to the data file, and its index record
// goes straight to the index writer.  Nothing is held per key.
//
// The values are read back one at a time and in key order, which is
// random order within each input's data file; that was already so, and
// a merge that reclaims space has to touch every surviving value.  What
// changed is that the keys are no longer all in memory at once (issue
// #59).
//
// The segment is complete and durable when this returns; it is simply
// not yet named by the manifest, which is what makes the commit that
// follows the only thing that decides whether the merge happened.
func (s *SegmentStore) writeMergedRun(run []*segment, height, seq uint64) (meta SegmentMeta, merged *segment, err error) {
	count, err := mergeIndexes(run, nil)
	if err != nil {
		return meta, nil, err
	}

	dataName := segmentFileName(height, seq)
	dataPath := filepath.Join(s.Directory, dataName)
	tmpPath := filepath.Join(s.Directory, segCompactName+segTmpSuffix)
	f, err := os.Create(tmpPath)
	if err != nil {
		return meta, nil, err
	}
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriterSize(f, segWriteBuffer)
	var h hash.Hash
	var out io.Writer = bw
	if !s.Mutable { // Immutable segments are transported; a peer verifies this
		h = sha256.New()
		out = io.MultiWriter(bw, h)
	}
	var header [segDataHdrSize]byte
	writeSegmentDataHeader(header[:], segKindDerived, count) // Maintenance output (issue #52)
	if _, err = out.Write(header[:]); err != nil {
		return meta, nil, err
	}

	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	iw, err := newIndexWriter(indexPath, count)
	if err != nil {
		return meta, nil, err
	}
	defer iw.abort()

	body := uint64(0) // Bytes of records written so far: the next value's body-relative offset
	var recHdr [segRecHdrSize]byte
	value := make([]byte, 0, 4096)
	_, err = mergeIndexes(run, func(src int, key [32]byte, dbb DBBKey) error {
		if uint64(cap(value)) < dbb.Length {
			value = make([]byte, dbb.Length)
		}
		value = value[:dbb.Length]
		if err := run[src].readValue(dbb, value); err != nil {
			return err
		}
		copy(recHdr[:32], key[:])
		binary.BigEndian.PutUint64(recHdr[32:], dbb.Length)
		if _, err := out.Write(recHdr[:]); err != nil {
			return err
		}
		if _, err := out.Write(value); err != nil {
			return err
		}
		rel := DBBKey{Offset: body + segRecHdrSize, Length: dbb.Length}
		body += segRecHdrSize + dbb.Length
		return iw.write(key, rel)
	})
	if err != nil {
		return meta, nil, err
	}
	if err = bw.Flush(); err != nil {
		return meta, nil, err
	}
	if err = f.Sync(); err != nil {
		return meta, nil, err
	}
	if err = f.Close(); err != nil {
		f = nil
		return meta, nil, err
	}
	f = nil
	if err = claimSegmentName(tmpPath, dataPath); err != nil {
		return meta, nil, err
	}
	// The manifest commit's directory fsync covers this rename
	if err = iw.finish(); err != nil {
		return meta, nil, err
	}

	meta = SegmentMeta{Height: height, Seq: seq, File: dataName, Count: count}
	if h != nil {
		meta.Hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	seg, err := s.openSegment(meta)
	if err != nil {
		return meta, nil, err
	}
	return meta, seg, nil
}

// bodySize
// The length of a segment's records: everything after the header.
func (s *segment) bodySize() (size int64, err error) {
	data, release, err := s.data()
	if err != nil {
		return 0, err
	}
	defer release()
	info, err := data.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() < segDataHdrSize {
		return 0, fmt.Errorf("%s is shorter than its header", s.dataPath)
	}
	return info.Size() - segDataHdrSize, nil
}

// copyBody
// Copy a segment's records -- its body, without the header -- to out
// as one sequential read.  size is what bodySize reported, and is
// checked against what actually arrived.
func (s *segment) copyBody(out io.Writer, size int64, buf []byte) (err error) {
	data, release, err := s.data()
	if err != nil {
		return err
	}
	defer release()
	n, err := io.CopyBuffer(out, io.NewSectionReader(data, segDataHdrSize, size), buf)
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("%s: copied %d of %d bytes", s.dataPath, n, size)
	}
	return nil
}

// layoutBodies
// Where each segment's body will land when the bodies are laid end to
// end to form one body: each body's size, and its offset within the
// combined body.  The first is at 0; where the combined body itself
// sits in a file is the file's business.
func layoutBodies(segs []*segment) (sizes []int64, bases []uint64, total uint64, err error) {
	sizes = make([]int64, len(segs))
	bases = make([]uint64, len(segs))
	for i, seg := range segs {
		if sizes[i], err = seg.bodySize(); err != nil {
			return nil, nil, 0, err
		}
		bases[i] = total
		total += uint64(sizes[i])
	}
	return sizes, bases, total, nil
}

// concatSegments
// Build one segment from several by COPYING their bodies end to end
// and shifting their index offsets, rather than reading every value
// back and re-encoding it.
//
// This is the difference between the two reasons to combine segments,
// and they want different work.  Compaction exists to RECLAIM: the Dyna
// layer's segments are full of records a later write superseded, so it
// must decide what survives and rewrite only that.  A merge of the Perm
// layer exists to reduce the FILE COUNT: its keys are unique and
// immutable, so there is nothing dead to drop, and re-encoding every
// record to produce a byte-identical result is pure cost.
//
// So the data file is a concatenation.  Each source contributes its
// body -- everything after its 24-byte header -- as one sequential
// copy, and an entry that pointed at offset O in source i points at
// base[i] + O in the result, base[i] being where that body landed
// within the combined body.  Nothing is read per record; the only
// per-key work is the index (shiftedIndex).
//
// A key that appears in two sources leaves both copies in the data file
// and one entry in the index, pointing at the newest.  The older copy
// becomes dead bytes.  That is the trade: a little space in exchange
// for never touching a value.
//
// No lock: the sources are immutable and the output is written aside.
func (s *SegmentStore) concatSegments(segs []*segment, height, seq uint64) (meta SegmentMeta, merged *segment, err error) {
	// The index first: it needs only the sizes, and it is where a
	// damaged source would be found, before anything is written
	sizes, bases, _, err := layoutBodies(segs)
	if err != nil {
		return meta, nil, err
	}
	// Count first, so the index header and bloom can be written before
	// the records stream through (issue #59)
	count, err := mergeIndexes(segs, nil)
	if err != nil {
		return meta, nil, err
	}

	dataName := segmentFileName(height, seq)
	dataPath := filepath.Join(s.Directory, dataName)
	tmpPath := dataPath + segTmpSuffix
	f, err := os.Create(tmpPath)
	if err != nil {
		return meta, nil, err
	}
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriterSize(f, segWriteBuffer)
	var h hash.Hash
	var out io.Writer = bw
	if !s.Mutable { // Immutable segments are transported; a peer verifies this
		h = sha256.New()
		out = io.MultiWriter(bw, h)
	}

	var header [segDataHdrSize]byte
	var physical uint64
	for _, seg := range segs {
		physical += uint64(seg.records)
	}
	writeSegmentDataHeader(header[:], segKindDerived, physical) // Maintenance output (issue #52)
	if _, err = out.Write(header[:]); err != nil {
		return meta, nil, err
	}
	buf := make([]byte, segWriteBuffer)
	for i, seg := range segs {
		if err = seg.copyBody(out, sizes[i], buf); err != nil {
			return meta, nil, err
		}
	}
	if err = bw.Flush(); err != nil {
		return meta, nil, err
	}
	if err = f.Sync(); err != nil {
		return meta, nil, err
	}
	if err = f.Close(); err != nil {
		f = nil
		return meta, nil, err
	}
	f = nil
	if err = claimSegmentName(tmpPath, dataPath); err != nil {
		return meta, nil, err
	}
	// The manifest commit's directory fsync covers this rename

	// The index: each input's entries shifted by where its body landed,
	// merged in key order, newest winning a repeated key, streamed
	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	iw, err := newIndexWriter(indexPath, count)
	if err != nil {
		return meta, nil, err
	}
	defer iw.abort()
	if _, err = mergeIndexes(segs, func(src int, key [32]byte, dbb DBBKey) error {
		return iw.write(key, DBBKey{Offset: dbb.Offset + bases[src], Length: dbb.Length})
	}); err != nil {
		return meta, nil, err
	}
	if err = iw.finish(); err != nil {
		return meta, nil, err
	}

	meta = SegmentMeta{Height: height, Seq: seq, File: dataName, Count: count}
	if h != nil {
		meta.Hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	seg, err := s.openSegment(meta)
	if err != nil {
		return meta, nil, err
	}
	return meta, seg, nil
}

// MergeBelow
// Merge every history segment belonging to a block below `height` into
// one, leaving the rest untouched.  Reports whether it merged anything.
//
// This is tier one of keeping the file count survivable.  A block
// boundary seals one segment per shard that took writes, so a shard
// accumulates one file pair per block -- measured at ~1,016 files per
// block across 512 shards at 5,000 entries a block, which is ~88M
// files a day and exhausts a 240M-inode filesystem in under three days
// (issue #47).  Merging a finished block set down to one segment per
// shard cuts that by the number of blocks in the set.
//
// It merges WITHIN one store, never across shards.  That keeps the
// working set small -- a shard holds a few hundred entries per set --
// so shards merge independently and in parallel.  Merging across
// shards is what produces globally sorted runs, and it is a separate,
// rarer pass.
//
// `height` is the caller's finalisation watermark: the block below
// which nothing more will arrive and nothing is still being healed.
// Segments at or above it are left alone, so a block a peer might
// still ask for by number is never merged away, and block export is
// untouched.  Only HISTORY is merged: a segment still inside the
// window is the protocol's, whatever the watermark says, and it is
// merged once the window has rolled past it.  The watermark is free
// to lag behind the window, and so is the merge behind the watermark;
// nothing about correctness needs either to be current (issue #47).
//
// A MERGED BLOCK IS MERGED ONCE.  What this folds is the segments that
// have arrived since the last merged block -- the newly finished
// window -- and never the merged blocks themselves, which are
// permanent: entries are file:offset:length and never move, so the
// cost of a merge is the size of one window forever, and the lifetime
// cost is linear in the data (spec 1.4).  Folding the previous output
// back in made each pass copy the whole permanent layer accumulated so
// far -- O(chain^2) over a store's life, and a single pass growing
// without limit: ~10 GB per pass after four hours at 500 tx/s, every
// 128 commits, competing with the protocol path for the device
// (issue #63).  A deep read walks the merged blocks newest-first by
// their filters; that is what they are for.
//
// The copy takes no store lock.  Sealed segments are immutable and the
// pool hands out descriptors for pread, so the run is read and the
// merged file written aside while commits and reads carry on; History
// is taken exclusively only to swap the run for the merged segment and
// commit history.json.  A concurrent read of history sees either the
// run or the merged segment, and both hold the same keys.
//
// Crash safety follows the same rule as compaction.  The merged file
// is written and renamed before the manifest names it, and it takes an
// identity BELOW the newest segment, so a crash before the commit
// leaves it as an orphan at or below the manifest's newest height --
// which recoverOrphans deletes.  The originals are still named by the
// manifest and are only retired after the commit succeeds, so the
// discarded merge costs space and nothing else.
func (s *SegmentStore) MergeBelow(height uint64) (meta SegmentMeta, merged bool, err error) {
	s.maint.Lock()
	defer s.maint.Unlock()

	s.History.RLock()
	if err = s.checkOpen(); err != nil {
		s.History.RUnlock()
		return meta, false, err
	}
	// Segments are oldest first, so the finalised ones are a prefix
	n := 0
	for _, seg := range s.history {
		if seg.meta.Height >= height {
			break
		}
		n++
	}
	// Which of them to fold is the same question compaction answers,
	// and the same rule answers it: take the newest suffix, and reach
	// one segment further back only when what has gathered behind it is
	// worth its size (CompactRatio), inside one pass's budget.
	//
	// Folding the WHOLE prefix every time -- which is what this did --
	// re-copied the previous merge's output on every pass, so a pass
	// copied the entire permanent layer accumulated so far: O(chain^2)
	// over a store's life (issue #63).  Never folding a merged block
	// again is the other extreme, and it trades that for file count:
	// one merged block per pass, forever, which is the inode problem
	// merging exists to solve (issue #30).  The ratio is the middle and
	// the standard answer -- a block is rewritten only once enough has
	// arrived to justify its size, so a byte is copied a few times over
	// the store's life rather than once per pass, and the merged blocks
	// stay few.  It is the rule the dynamic layer already uses (#31).
	run, first := compactionRunWithin(s.history[:n], CompactRatio, CompactPassRecords)
	if run == nil { // Nothing worth folding yet
		s.History.RUnlock()
		return meta, false, nil
	}
	run = append([]*segment(nil), run...)
	s.History.RUnlock()

	// The merged segment takes the sequence after the newest it
	// replaces.  That is free and correctly ordered: everything merged
	// is at or below (H, S), and the first segment left standing is in
	// a block above H, because the run ends at the last segment below
	// `height` and the remainder is exactly those at or above it.
	last := run[len(run)-1].meta
	outHeight, outSeq := last.Height, last.Seq+1

	meta, seg, err := s.concatSegments(run, outHeight, outSeq)
	if errors.Is(err, os.ErrExist) {
		// The merged segment's identity is already a file on disk.  The
		// prefix rule should make that impossible; refuse rather than
		// overwrite, and leave the run for a later pass (issue #61).
		auditUnlink(s.Directory, fmt.Sprintf("merge-refused-name-taken(%d-%d)", outHeight, outSeq))
		return meta, false, nil
	}
	if err != nil {
		return meta, false, err
	}
	// The merged segment reaches back to the oldest block in the run.
	// The key filters cover a block range, and this is what tells them
	// the segment is not one block: a filter that started after the
	// run's first block never saw its oldest keys and must not claim it.
	meta.Span = outHeight - run[0].meta.first()
	seg.meta = meta
	if maintenanceHook != nil {
		maintenanceHook()
	}

	merged, err = s.swapHistory(run, seg, first)
	return meta, merged, err
}

// historyBelow
// The history segments belonging to blocks below `height`, oldest
// first, pinned: their files stay readable until release is called,
// whatever a merge or a drop does to them meanwhile.  Takes History
// shared for the length of the copy of the list.
func (s *SegmentStore) historyBelow(height uint64) (segs []*segment, release func(), err error) {
	s.pin()
	s.History.RLock()
	defer s.History.RUnlock()
	if err = s.checkOpen(); err != nil {
		s.unpin()
		return nil, nil, err
	}
	n := 0
	for _, seg := range s.history {
		if seg.meta.Height >= height {
			break
		}
		n++
	}
	return append([]*segment(nil), s.history[:n]...), s.unpin, nil
}

// DropBelow
// Stop serving the history segments belonging to blocks below `height`
// from this store, because their keys are now held cold: commit a
// history manifest without them and retire their files.  Reports how
// many were dropped.
//
// The commit is this store's own, two barriers, off the protocol
// path.  It used to be deferred to the shard's next seal, which
// recorded it for free -- but the seal writes the active manifest
// now and the drop is history's to record, and leaving the files to
// the next merge would keep a packed set's worth of segments on disk
// in every shard that merges rarely.  A pack drops its shards
// concurrently, so the barriers overlap.  Dropping is a consequence
// of a commit that has already happened -- the block set holding
// these keys is durable before anyone calls this -- so a crash before
// this commit leaves a store that opens exactly as before and simply
// drops them again.
//
// The caller asserts the keys are held cold; the store cannot check.
func (s *SegmentStore) DropBelow(height uint64) (dropped int, err error) {
	s.maint.Lock()
	defer s.maint.Unlock()
	s.History.Lock()
	defer s.History.Unlock()
	if err = s.checkOpen(); err != nil {
		return 0, err
	}
	n := 0
	for _, seg := range s.history {
		if seg.meta.Height >= height {
			break
		}
		n++
	}
	if n == 0 {
		return 0, nil
	}
	old := s.history
	s.history = append([]*segment(nil), old[n:]...)
	if err = s.writeHistoryManifest(); err != nil {
		s.history = old
		return 0, err
	}
	for _, seg := range old[:n] {
		s.releaseFromHistory(seg)
	}
	return n, nil
}

// SegmentPaths
// The sealed segment files backing this store, oldest first, with the
// manifest metadata that identifies and verifies each one.  Syncing a
// peer is copying these files.
func (s *SegmentStore) SegmentPaths() (metas []SegmentMeta, paths []string) {
	for _, seg := range s.sealedSegments() {
		metas = append(metas, seg.meta)
		paths = append(paths, filepath.Join(s.Directory, seg.meta.File))
	}
	return metas, paths
}

// sealedSegments
// Both tiers as they stand, history then active, oldest first.  Each
// tier is read under its own lock, one after the other: the list can
// name a segment twice or miss one that moved between the two reads
// only if the window rolled in between, which a caller wanting a
// consistent view avoids by not sealing meanwhile.
func (s *SegmentStore) sealedSegments() (segs []*segment) {
	s.History.RLock()
	segs = append(segs, s.history...)
	s.History.RUnlock()
	s.Mutex.RLock()
	segs = append(segs, s.active...)
	s.Mutex.RUnlock()
	return segs
}

// ImportSegmentFile
// Adopt a peer's segment file: verify it against the expected hash,
// copy it in, build its index, and commit one manifest update.  No
// record is re-inserted, which is what makes sync a file copy.
//
// Importing a segment already present is a no-op, so an interrupted
// sync resumes by re-running.
func (s *SegmentStore) ImportSegmentFile(path string, meta SegmentMeta) (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if err = s.checkOpen(); err != nil {
		return err
	}
	if meta.Span > meta.Height {
		return fmt.Errorf("segment (block %d, seq %d) claims to reach %d blocks back", meta.Height, meta.Seq, meta.Span)
	}

	// Only the active tier can hold it: a peer's segment is at or above
	// the window, and one in history would fail the ordering check below
	for _, seg := range s.active {
		if seg.meta.Height == meta.Height && seg.meta.Seq == meta.Seq && seg.meta.Hash == meta.Hash {
			return nil // Already have it
		}
	}
	// A block already packed into a set is finalized: the set is what
	// answers for it, and a store that has dropped every segment has no
	// newest to be measured against, so the set's watermark is the bound
	if s.cold != nil {
		if last, ok := s.cold.watermark(); ok && meta.Height <= last {
			return fmt.Errorf("segment (block %d, seq %d) is not above the newest block set, which ends at block %d",
				meta.Height, meta.Seq, last)
		}
	}
	if newest, ok := s.newestMeta(); ok && !meta.after(newest) {
		return fmt.Errorf("segment (block %d, seq %d) is not above the newest segment (block %d, seq %d)",
			meta.Height, meta.Seq, newest.Height, newest.Seq)
	}
	if err = VerifySegmentFile(path, meta.Hash); err != nil {
		return err
	}

	// Build and check the incoming segment under TEMPORARY names, and
	// only rename it into place once it has been accepted.
	//
	// The obvious order -- put it in place, then check, then undo if the
	// check fails -- cannot be made safe.  The undo is two os.Remove
	// calls with no barrier behind them, so a crash in that window
	// leaves the file on disk; and recoverOrphans cannot tell it apart
	// from an interrupted seal, because it IS a complete, correctly
	// hashed segment above the newest height.  It was rejected for
	// conflicting with local data, not for being malformed.  So the next
	// open adopted the very segment the import refused, and
	// checkNoConflicts never ran again (issue #45).
	//
	// Checking first removes the undo instead of making it durable.  A
	// crash before the rename leaves *.tmp files, which recoverOrphans
	// deletes unconditionally.
	dataName := segmentFileName(meta.Height, meta.Seq)
	dataPath := filepath.Join(s.Directory, dataName)
	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	tmpData := dataPath + segTmpSuffix
	tmpIndex := indexPath + segTmpSuffix
	if err = copyFileSynced(path, tmpData); err != nil {
		return err
	}
	defer func() {
		if err != nil { // Rejected or failed: leave nothing behind
			os.Remove(tmpData)
			os.Remove(tmpIndex)
		}
	}()
	if err = buildIndexFor(tmpData, tmpIndex); err != nil {
		return err
	}

	meta.File = dataName
	seg, err := s.openSegmentAt(meta, tmpData, tmpIndex)
	if err != nil {
		return err
	}
	meta.Count = uint64(seg.count)
	seg.meta = meta

	// An immutable store must not silently shadow a value it already
	// holds: that is a divergence, not an update.  Checking is cheap
	// because the keys a syncing node receives are almost all new --
	// the existing segments' bloom filters reject them from memory, and
	// only a bloom hit costs a real lookup.
	if !s.Mutable {
		if err = s.checkNoConflicts(seg); err != nil {
			seg.close()
			return err
		}
	}

	// Accepted.  Publish the names, and point the segment at them; its
	// contents and index are unchanged, so nothing needs re-reading.
	seg.close() // Release the pool's handles on the temporary names
	// Claim the name, never replace it (spec 1.7).  The checks above
	// consult the segments the manifests NAME, and a complete file can
	// sit at this identity without being named -- an earlier seal or
	// import that reached disk and not its commit.  A rename would
	// silently replace that committed-by-construction file; the
	// exclusive claim refuses, and the import fails with a name a
	// human can act on rather than losing data quietly (issue #67).
	if err = claimSegmentName(tmpData, dataPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("import of (block %d, seq %d): %s already exists; "+
				"the store holds a file at that identity", meta.Height, meta.Seq, dataName)
		}
		return err
	}
	// The data file's name is the identity; its index follows it, and
	// is derived data a reader rebuilds if it is missing
	if err = os.Rename(tmpIndex, indexPath); err != nil {
		return err
	}
	// The manifest commit at the end of the import covers both renames
	seg.dataPath, seg.indexPath = dataPath, indexPath
	s.advanceBlock(meta.Height)

	if seg.meta.first() < s.tierStart() {
		// A segment reaching below the window belongs to history, and
		// the filters must not claim it.  It is the newest thing in the
		// store, so it goes last, and the active manifest names it
		// until a history commit does (a handoff, like any other).
		s.History.Lock()
		s.history = append(s.history, seg)
		s.historyNewest, s.historyAny = seg.meta, true
		s.handoffMu.Lock()
		s.handoffs = append(s.handoffs, handoff{seg: seg})
		s.handoffMu.Unlock()
		s.History.Unlock()
		s.epoch++
		return s.writeManifest() // Commit
	}
	s.active = append(s.active, seg)
	_ = seg.loadBloom() // Active: worth the memory (issue #64)
	// Into every live filter, whether or not the segment's block is in
	// its range: a filter holding a key it need not is slower, never
	// wrong, and the segment is the newest thing in the store
	for _, f := range s.filters {
		if err = s.addSegmentKeys(f.keys, seg); err != nil {
			s.filters = nil // The filters must never under-report; drop them
			break
		}
	}
	return s.writeManifest() // Commit
}

// checkNoConflicts
// Verify that no key in an incoming segment already has a different
// value in this store, within the window the key filters cover: the
// same N-to-2N-block guarantee a write gets, for the same reason (see
// Put).  A peer's segment is always the newest thing in the store, so
// what it could diverge from is recent by construction.  The caller
// must hold the Mutex, and seg must not yet be in either tier.
//
// A filter hit is followed into history under History, shared, while
// the Mutex is held -- the one nesting the protocol path never does.
// An import is not the protocol path: a node syncing is not one in
// consensus, and what it waits for is at most a history swap.
func (s *SegmentStore) checkNoConflicts(seg *segment) (err error) {
	index, release, err := seg.index() // One borrow for the whole scan
	if err != nil {
		return err
	}
	defer release()
	const batch = 4096 // Index records read per pass
	buff := make([]byte, batch*DBKeyFullSize)
	for i := int64(0); i < seg.count; i += batch {
		n := seg.count - i
		if n > batch {
			n = batch
		}
		chunk := buff[:n*DBKeyFullSize]
		if _, err = index.ReadAt(chunk, segIndexHdrSize+i*DBKeyFullSize); err != nil {
			return err
		}
		for pos := 0; pos+DBKeyFullSize <= len(chunk); pos += DBKeyFullSize {
			key, dbb, err := GetDBBKey(chunk[pos : pos+DBKeyFullSize])
			if err != nil {
				return err
			}
			// Filter-gated; no disk I/O unless a filter hits, and history
			// only on a hit the active tier could not settle
			existing, inWindow, found, err := s.lookupActive(key)
			if err != nil {
				return err
			}
			if !found && inWindow {
				existing, err = s.lookupHistory(key, true)
				found = err == nil
				if err != nil && !errors.Is(err, errNotFound) {
					return err
				}
			}
			if !found {
				continue // Not held locally: the common case
			}
			incoming, err := seg.value(dbb)
			if err != nil {
				return err
			}
			if !bytes.Equal(existing, incoming) {
				return fmt.Errorf("segment conflicts with the value already stored for key %x", key)
			}
		}
	}
	return nil
}

// Close
// Flush the live tail and release the segment files.  Records in the
// live tail survive: they are replayed on open.
func (s *SegmentStore) Close() (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.History.Lock() // Both, in the one order they may nest; not the protocol path
	defer s.History.Unlock()
	if s.closed.Load() {
		return nil
	}

	// History first, then the active manifest: the history commit
	// records every handoff, and the active commit that follows is the
	// one that may stop naming them and delete what history retired.
	// Then the key filters, and the manifest that says they are
	// current -- in that order, so a crash between the two leaves a
	// manifest saying "rebuild" rather than one pointing at filters
	// that have fallen behind the segments.  A save that fails is not a
	// reason to fail the close: the next open rebuilds.
	if err = s.writeHistoryManifest(); err != nil {
		return err
	}
	if s.filters != nil {
		if claim, err := s.saveKeyFilters(); err == nil {
			s.filterSaved = claim
			s.filterValid = true
		}
	}
	if err = s.writeManifest(); err != nil {
		return err
	}

	if s.liveFile != nil && s.liveFile.File != nil {
		if err = s.liveFile.Close(); err != nil {
			return err
		}
	}
	for _, seg := range s.active {
		seg.close()
	}
	for _, seg := range s.history {
		seg.close()
	}
	s.active, s.history = nil, nil
	s.historyNewest, s.historyAny = SegmentMeta{}, false
	s.closed.Store(true)
	s.awaitUnlinks() // So that a closed store has deleted what it retired
	return nil
}

// buildIndexFor
// Scan a sealed segment and write its index: the newest record per key
// as sorted 48-byte entries, followed by a bloom filter over the keys.
// Written to a tmp file and renamed, so an index is either absent or
// complete.
func buildIndexFor(dataPath, indexPath string) (err error) {
	f, err := os.Open(dataPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var header [segDataHdrSize]byte
	if _, err = f.ReadAt(header[:], 0); err != nil {
		return err
	}
	if err = checkSegmentHeader(dataPath, header[:]); err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := uint64(info.Size())

	// Scan the records.  Later records win, matching lookup order.
	// The scan is sequential, so it reads through a buffer rather than
	// issuing a read per record.
	if _, err = f.Seek(segDataHdrSize, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(f, segWriteBuffer)
	entries := make(map[[32]byte]*DBBKey)
	var order [][32]byte
	offset := uint64(segDataHdrSize)
	var recHdr [segRecHdrSize]byte
	for offset+segRecHdrSize <= size {
		if _, err = io.ReadFull(r, recHdr[:]); err != nil {
			return err
		}
		var key [32]byte
		copy(key[:], recHdr[:32])
		length := binary.BigEndian.Uint64(recHdr[32:])
		valueOffset := offset + segRecHdrSize
		if valueOffset+length > size {
			return fmt.Errorf("%s is truncated at offset %d", dataPath, offset)
		}
		if _, err = r.Discard(int(length)); err != nil { // The index holds offsets, not values
			return err
		}
		if _, seen := entries[key]; !seen {
			order = append(order, key)
		}
		entries[key] = &DBBKey{Offset: valueOffset, Length: length}
		offset = valueOffset + length
	}
	return writeIndexFile(indexPath, order, entries)
}

// recordSort
// Sorts a buffer of 48-byte key records in place by their 32-byte key
type recordSort []byte

func (r recordSort) Len() int { return len(r) / DBKeyFullSize }
func (r recordSort) Less(i, j int) bool {
	return bytes.Compare(r[i*DBKeyFullSize:i*DBKeyFullSize+32], r[j*DBKeyFullSize:j*DBKeyFullSize+32]) < 0
}
func (r recordSort) Swap(i, j int) {
	var tmp [DBKeyFullSize]byte
	copy(tmp[:], r[i*DBKeyFullSize:])
	copy(r[i*DBKeyFullSize:(i+1)*DBKeyFullSize], r[j*DBKeyFullSize:])
	copy(r[j*DBKeyFullSize:(j+1)*DBKeyFullSize], tmp[:])
}

// writeIndexFile
// Write a segment's index from records already in memory: the keys and
// where each one's value sits in the data file.  Sealing and
// compaction both know that as they write, so neither has to read the
// segment back to index it.
//
// entries carry offsets into the data FILE, which is what every writer
// has in hand -- the live tail's map, or the position it just wrote a
// record at.  The index stores them relative to the body instead (see
// segment.value), so the header's length comes off here, once, rather
// than in every writer.
func writeIndexFile(indexPath string, order [][32]byte, entries map[[32]byte]*DBBKey) (err error) {
	buff := make([]byte, 0, len(order)*DBKeyFullSize)
	for _, key := range order {
		e := entries[key]
		rel := DBBKey{Offset: e.Offset - segDataHdrSize, Length: e.Length}
		buff = append(buff, rel.Bytes(key)...)
	}
	sort.Sort(recordSort(buff)) // Sorted by key, for binary search
	return writeIndexRecords(indexPath, buff)
}

// writeIndexRecords
// Write a segment's index from its records already sorted, encoded and
// body-relative: what a merge has in hand, since the sources' indexes
// are already in this form and combining them never leaves the form.
func writeIndexRecords(indexPath string, buff []byte) (err error) {
	count := uint64(len(buff) / DBKeyFullSize)
	bloom := NewBloomSizedForKeys(count, 3)
	for pos := 0; pos < len(buff); pos += DBKeyFullSize {
		var key [32]byte
		copy(key[:], buff[pos:])
		bloom.Set(key)
	}

	tmpPath := indexPath + segTmpSuffix
	out, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		if out != nil {
			out.Close()
			os.Remove(tmpPath)
		}
	}()

	var idxHdr [segIndexHdrSize]byte
	binary.BigEndian.PutUint32(idxHdr[:], segIndexMagic)
	binary.BigEndian.PutUint32(idxHdr[4:], segIndexVersion)
	binary.BigEndian.PutUint64(idxHdr[8:], count)
	binary.BigEndian.PutUint64(idxHdr[16:], bloom.NumBytes)
	binary.BigEndian.PutUint32(idxHdr[24:], uint32(bloom.K))
	if _, err = out.Write(idxHdr[:]); err != nil {
		return err
	}
	if _, err = out.Write(buff); err != nil {
		return err
	}
	if _, err = out.Write(bloom.Map); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	if err = out.Close(); err != nil {
		out = nil
		return err
	}
	out = nil
	// No directory fsync: an index is derived data -- buildIndexFor
	// reconstructs it from the .dat, which is what openSegment does
	// when one is missing -- and the manifest commit that follows
	// fsyncs this directory anyway
	return os.Rename(tmpPath, indexPath)
}

// identify
// The manifest identity of a segment file on disk.  An immutable
// store's segments travel to peers, so they carry a SHA-256; a mutable
// store's never leave the node, and hashing one on every open would be
// a full read of the store for nothing.
func (s *SegmentStore) identify(path string) (hash string, count uint64, err error) {
	if !s.Mutable {
		return hashAndCount(path)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	var header [segDataHdrSize]byte
	if _, err = f.ReadAt(header[:], 0); err != nil {
		return "", 0, err
	}
	return "", binary.BigEndian.Uint64(header[16:]), nil
}

// hashAndCount
// The SHA-256 of a segment file and the record count in its header
func hashAndCount(path string) (hash string, count uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	var header [segDataHdrSize]byte
	if _, err = f.ReadAt(header[:], 0); err != nil {
		return "", 0, err
	}
	count = binary.BigEndian.Uint64(header[16:])

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return "", 0, err
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", 0, err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), count, nil
}

// copyFileSynced copies src to dst and makes dst durable
func copyFileSynced(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if out != nil {
			out.Close()
			os.Remove(dst)
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	if err = out.Close(); err != nil {
		out = nil
		return err
	}
	out = nil
	return nil
}
