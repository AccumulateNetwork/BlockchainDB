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
//	segments.json       the manifest: the segments, counts, and hashes
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
	segLiveName     = "live.dat"
	segFilePrefix   = "seg-"
	segDataSuffix   = ".dat"
	segIndexSuffix  = ".idx"
	segTmpSuffix    = ".tmp"
	segCompactName  = "compact"

	segWriteBuffer = 1 << 20 // Buffer for writing a segment's records

	segIndexMagic   = 0x53494458 // "SIDX"
	segIndexVersion = 1
	segIndexHdrSize = 32 // magic(4) version(4) count(8) bloomBytes(8) bloomK(4) reserved(4)
	segDataHdrSize  = 24 // magic(4) version(4) sinceOffset(8) count(8) -- segment.go's stream header
	segRecHdrSize   = 40 // key(32) valueLen(8) precede each value in a .dat
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
}

// after reports whether a is a strictly later segment than b
func (a SegmentMeta) after(b SegmentMeta) bool {
	if a.Height != b.Height {
		return a.Height > b.Height
	}
	return a.Seq > b.Seq
}

// StoreManifest
// The authoritative list of a store's sealed segments.  Replacing it
// is the commit point for both sealing and compaction.
type StoreManifest struct {
	Mutable     bool          `json:"mutable"`
	SealLimit   uint64        `json:"sealLimit"`   // 0: unset, caller decides
	BlockHeight uint64        `json:"blockHeight"` // Block currently being accumulated
	Segments    []SegmentMeta `json:"segments"`    // Oldest first

	// BloomValid says the persisted key filter (bloom.dat) covers
	// exactly the sealed segments listed here, and BloomHeight/BloomSeq
	// name the newest of them.  Height 0, Seq 0 is a legitimate
	// segment, so the flag carries the claim rather than the numbers.
	BloomValid  bool   `json:"bloomValid,omitempty"`
	BloomHeight uint64 `json:"bloomHeight,omitempty"`
	BloomSeq    uint64 `json:"bloomSeq,omitempty"`
}

// segment
// An open sealed segment
type segment struct {
	meta    SegmentMeta
	data    *os.File
	index   *os.File
	count   int64  // Indexed keys
	records int64  // Physical records in the data file; > count if any key repeats
	bloom   *Bloom // Membership filter over this segment's keys
}

// close releases a segment's file handles
func (s *segment) close() {
	if s.data != nil {
		s.data.Close()
		s.data = nil
	}
	if s.index != nil {
		s.index.Close()
		s.index = nil
	}
}

// lookup
// Find a key in this segment.  Returns where its value lives in the
// segment's data file.
func (s *segment) lookup(key [32]byte) (dbb *DBBKey, found bool, err error) {
	if s.bloom != nil && !s.bloom.Test(key) {
		return nil, false, nil // Definitely not in this segment
	}
	var rec [DBKeyFullSize]byte
	lo, hi := int64(0), s.count-1
	for lo <= hi { //                        Index records are sorted by key
		mid := (lo + hi) / 2
		if _, err = s.index.ReadAt(rec[:], segIndexHdrSize+mid*DBKeyFullSize); err != nil {
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

// value reads a value out of the segment's data file
func (s *segment) value(dbb *DBBKey) (value []byte, err error) {
	value = make([]byte, dbb.Length)
	if _, err = s.data.ReadAt(value, int64(dbb.Offset)); err != nil {
		return nil, err
	}
	return value, nil
}

// SegmentStore
// A key/value store built from sealed segments and a live tail.
// Methods are safe for concurrent use.
type SegmentStore struct {
	Mutex     sync.Mutex
	Directory string
	Mutable   bool // Mutable: newer segments shadow older; immutable: conflicts error

	// SealLimit is the live-tail bound this store was configured with.
	// The store records and restores it but does not act on it: KV2
	// owns the decision to seal (sealPermIfFull/sealDynaIfFull), and
	// before this was persisted a reopened database silently fell back
	// to a default, discarding the only parameter its constructor takes.
	SealLimit uint64

	// blockHeight is the block whose writes the live tail is
	// accumulating.  A block boundary seals at its own height and then
	// advances this, so auto-seals in between are tagged with the block
	// they actually belong to rather than allocating one of their own.
	blockHeight uint64

	segments    []*segment           // Sealed segments, oldest first
	live        map[[32]byte]*DBBKey // Keys written since the last seal
	liveFile    *BFile               // Their records
	liveRecords uint64               // Physical records in liveFile (>= len(live) if keys repeat)
	closed      bool                 // Set by Close; cleared by Open

	// keys is a membership filter over everything the store holds --
	// every sealed segment and the live tail.  It exists to make
	// proving a key ABSENT cheap.
	//
	// Without it, "not here" costs a bloom probe against every sealed
	// segment plus a binary search per false positive, so the answer
	// gets steadily more expensive as the store seals: a store that has
	// sealed 2,600 times pays 2,600 probes for it.  That is the
	// question a write asks -- a new key is absent by definition -- so
	// the write path decayed with the segment count.  One probe per
	// BloomSet layer replaces it, and the layer count grows with the
	// logarithm of the key count rather than with the seal count.
	//
	// A false positive costs no more than the walk it replaced, so a
	// stale filter is slow, never wrong.  A false NEGATIVE would be
	// silent data loss -- "not found" for a key that is right there --
	// so the filter is used only where its coverage is proven, and nil
	// (falling back to the walk) wherever it is not: see loadKeyFilter.
	keys *BloomSet

	// bloomValid/bloomAt are the coverage claim writeManifest records.
	// It is a one-shot: only the manifest written immediately after a
	// save carries it.
	bloomValid bool
	bloomAt    SegmentMeta

	stats StoreStats // Counted under the Mutex, like everything else here
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
	FilterAbsent uint64 // Settled by the filter alone: no segment touched
	FilterWalked uint64 // Filter said "maybe" (or was absent): segments walked
	FilterMisled uint64 // Walked on the filter's say-so and found nothing
	LiveHit      uint64 // Answered from the live tail, before any filter
}

// Stats
// A snapshot of the store's counters.
func (s *SegmentStore) Stats() StoreStats {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.stats
}

// NewSegmentStore
// Create an empty store.  An existing directory is replaced.
func NewSegmentStore(directory string, mutable bool) (store *SegmentStore, err error) {
	os.RemoveAll(directory)
	if err = os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}
	store = &SegmentStore{Directory: directory, Mutable: mutable}
	store.live = make(map[[32]byte]*DBBKey)
	store.keys = store.newKeyFilter(0)
	if err = store.newLiveFile(); err != nil {
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
	// Resize the key filter's first layer to the scale the caller just
	// declared, while it is still empty and resizing is free.  A store
	// is created before its limit is known, so without this every store
	// starts at the same size regardless of what it is for -- and a
	// 512-shard database creates 1,024 of them.
	if s.keys != nil && s.keys.Count() == 0 {
		s.keys = s.newKeyFilter(0)
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
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if !s.closed {
		return nil
	}
	return s.load()
}

// load
// Read the store off disk.  The caller must hold the Mutex (or hold
// the only reference, as the constructors do).
func (s *SegmentStore) load() (err error) {
	s.live = make(map[[32]byte]*DBBKey)
	s.segments = nil
	s.liveRecords = 0

	m, err := s.readManifest()
	if err != nil {
		return err
	}
	s.Mutable = m.Mutable
	s.SealLimit = m.SealLimit
	s.blockHeight = m.BlockHeight

	for _, meta := range m.Segments {
		seg, err := s.openSegment(meta)
		if err != nil {
			return err
		}
		s.segments = append(s.segments, seg)
	}

	if err = s.recoverOrphans(); err != nil {
		return err
	}
	// After recoverOrphans, because an adopted segment changes what the
	// filter has to cover; before openLive, which adds the live keys as
	// it replays them.
	if err = s.loadKeyFilter(m); err != nil {
		return err
	}
	if err = s.openLive(); err != nil {
		return err
	}
	s.closed = false
	return nil
}

// loadKeyFilter
// Restore the store-level key filter: load the persisted one when the
// manifest proves it covers exactly the sealed segments now open, and
// rebuild it from those segments' own indexes otherwise.
//
// Coverage is proven rather than assumed because the two failure modes
// are not symmetric.  A filter holding keys the store no longer has
// costs a wasted walk.  A filter MISSING a key the store does hold
// turns a Get into a wrong answer -- "not found" for a key sitting in
// a segment -- and nothing downstream would catch it.  A crash, an
// interrupted save, and a seal after the last save all leave the file
// behind the segment list, so each of them rebuilds.
func (s *SegmentStore) loadKeyFilter(m *StoreManifest) (err error) {
	newest, ok := s.newestMeta()
	covered := m.BloomValid &&
		((ok && newest.Height == m.BloomHeight && newest.Seq == m.BloomSeq) ||
			(!ok && len(s.segments) == 0)) // An empty filter covers no segments
	if covered {
		if s.keys, err = LoadBloomSet(s.Directory); err == nil {
			return nil
		}
		s.keys = nil // Unreadable or truncated; rebuild instead
	}
	return s.rebuildKeyFilter()
}

// rebuildKeyFilter
// Build the key filter from what the store actually holds.  Layer 0 is
// sized for the current key count, so a store reopened at 30 million
// keys starts with one layer covering all of them rather than growing
// a stack of layers to reach them.
//
// A rebuild that fails leaves the filter nil, which is the safe
// direction: lookups fall back to walking the segments, which is what
// they did before the filter existed.
func (s *SegmentStore) rebuildKeyFilter() (err error) {
	var total uint64
	for _, seg := range s.segments {
		total += uint64(seg.count)
	}
	s.keys = s.newKeyFilter(total)
	for _, seg := range s.segments {
		if err = s.addSegmentKeys(seg); err != nil {
			s.keys = nil
			return err
		}
	}
	for key := range s.live { // The tail is part of what the store holds
		s.keys.Set(key)
	}
	return nil
}

// addSegmentKeys
// Add every key in a sealed segment to the filter, read from the
// segment's index: the keys are already there, sorted, 48 bytes apart.
func (s *SegmentStore) addSegmentKeys(seg *segment) (err error) {
	const batch = 4096 // Index records per read
	buff := make([]byte, batch*DBKeyFullSize)
	for i := int64(0); i < seg.count; {
		n := seg.count - i
		if n > batch {
			n = batch
		}
		b := buff[:n*DBKeyFullSize]
		if _, err = seg.index.ReadAt(b, segIndexHdrSize+i*DBKeyFullSize); err != nil {
			return err
		}
		for j := int64(0); j < n; j++ {
			var key [32]byte
			copy(key[:], b[j*DBKeyFullSize:])
			s.keys.Set(key)
		}
		i += n
	}
	return nil
}

// openSegment opens a sealed segment's data and index files
func (s *SegmentStore) openSegment(meta SegmentMeta) (seg *segment, err error) {
	seg = &segment{meta: meta}
	dataPath := filepath.Join(s.Directory, meta.File)
	if seg.data, err = os.Open(dataPath); err != nil {
		return nil, err
	}
	var dataHdr [segDataHdrSize]byte
	if _, err = seg.data.ReadAt(dataHdr[:], 0); err != nil {
		seg.close()
		return nil, err
	}
	seg.records = int64(binary.BigEndian.Uint64(dataHdr[16:]))
	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	if _, err = os.Stat(indexPath); err != nil { // Rebuild a missing index
		if err = buildIndexFor(dataPath, indexPath); err != nil {
			seg.close()
			return nil, err
		}
	}
	if seg.index, err = os.Open(indexPath); err != nil {
		seg.close()
		return nil, err
	}

	var header [segIndexHdrSize]byte
	if _, err = seg.index.ReadAt(header[:], 0); err != nil {
		seg.close()
		return nil, err
	}
	if binary.BigEndian.Uint32(header[:]) != segIndexMagic {
		seg.close()
		return nil, fmt.Errorf("%s is not a segment index", indexPath)
	}
	seg.count = int64(binary.BigEndian.Uint64(header[8:]))
	bloomBytes := binary.BigEndian.Uint64(header[16:])
	bloomK := int(binary.BigEndian.Uint32(header[24:]))
	if bloomBytes > 0 {
		bitmap := make([]byte, bloomBytes)
		if _, err = seg.index.ReadAt(bitmap, segIndexHdrSize+seg.count*DBKeyFullSize); err != nil {
			seg.close()
			return nil, err
		}
		seg.bloom = &Bloom{
			SizeOfMap: float64(bloomBytes) / (1024 * 1024),
			NumBytes:  bloomBytes,
			Map:       bitmap,
			K:         bloomK,
			Capacity:  bloomBytes * 8 / BloomBitsPerKey,
			Count:     uint64(seg.count),
		}
	}
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
		if s.keys != nil {
			s.keys.Set(key)
		}
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
	return nil
}

// recoverOrphans
// Adopt a sealed segment whose manifest update did not complete, and
// delete leftovers a completed compaction made unreachable.
//
// The rule is the manifest's newest height: a data file above it is a
// seal (or compaction) that reached disk but not the manifest, and is
// complete by construction -- it was fsynced before being renamed into
// place.  A data file at or below that height was superseded by a
// committed compaction.
func (s *SegmentStore) recoverOrphans() (err error) {
	entries, err := os.ReadDir(s.Directory)
	if err != nil {
		return err
	}

	known := make(map[string]bool)
	for _, seg := range s.segments {
		known[seg.meta.File] = true
	}
	newest, haveSegments := s.newestMeta()

	var adopted []SegmentMeta
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
		if haveSegments && !orphan.after(newest) { // Superseded by a committed compaction
			os.Remove(path)
			os.Remove(strings.TrimSuffix(path, segDataSuffix) + segIndexSuffix)
			continue
		}
		hash, count, err := s.identify(path)
		if err != nil {
			return err
		}
		adopted = append(adopted, SegmentMeta{Height: height, Seq: seq, File: name, Count: count, Hash: hash})
	}
	if len(adopted) == 0 {
		return nil
	}

	sort.Slice(adopted, func(i, j int) bool { return adopted[j].after(adopted[i]) })
	for _, meta := range adopted {
		seg, err := s.openSegment(meta)
		if err != nil {
			return err
		}
		meta.Count = uint64(seg.count) // Indexed keys, not physical records
		seg.meta = meta
		s.segments = append(s.segments, seg)
	}
	return s.writeManifest()
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

// writeManifest
// Replace the manifest atomically.  This is the commit point for
// sealing, importing, and compaction.
func (s *SegmentStore) writeManifest() (err error) {
	// The coverage claim is true only of the manifest written directly
	// after the filter was saved.  Everything else that writes a
	// manifest -- a seal, a compaction, an import -- has changed the
	// segment set, so clearing it here means no path can forget to.
	defer func() { s.bloomValid = false }()

	m := StoreManifest{Mutable: s.Mutable, SealLimit: s.SealLimit, BlockHeight: s.blockHeight,
		BloomValid: s.bloomValid, BloomHeight: s.bloomAt.Height, BloomSeq: s.bloomAt.Seq}
	for _, seg := range s.segments {
		m.Segments = append(m.Segments, seg.meta)
	}
	data, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.Directory, segManifestName+segTmpSuffix)
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
	if err = os.Rename(tmp, filepath.Join(s.Directory, segManifestName)); err != nil {
		return err
	}
	return syncDir(s.Directory)
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
	if s.closed {
		return errStoreClosed
	}
	return nil
}

// Get
// Return the value for a key.  Checks the live tail, then the sealed
// segments newest to oldest.
func (s *SegmentStore) Get(key [32]byte) (value []byte, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.get(key)
}

// get is Get; the caller must hold the Mutex
func (s *SegmentStore) get(key [32]byte) (value []byte, err error) {
	if err = s.checkOpen(); err != nil {
		return nil, err
	}
	s.stats.LookupTotal++
	if dbb, ok := s.live[key]; ok {
		value = make([]byte, dbb.Length)
		if err = s.liveFile.ReadAt(dbb.Offset, value); err != nil {
			return nil, err
		}
		s.stats.LiveHit++
		return value, nil
	}
	// One probe settles the common case.  The filter covers every key
	// in the store, so a "no" here is definitive and the walk below --
	// which is what grows with the seal count -- is skipped entirely.
	if s.keys != nil && !s.keys.Test(key) {
		s.stats.FilterAbsent++
		return nil, errNotFound
	}
	s.stats.FilterWalked++
	for i := len(s.segments) - 1; i >= 0; i-- { // Newest segment wins
		dbb, found, err := s.segments[i].lookup(key)
		if err != nil {
			return nil, err
		}
		if found {
			return s.segments[i].value(dbb)
		}
	}
	if s.keys != nil {
		s.stats.FilterMisled++ // The walk was the filter's fault
	}
	return nil, errNotFound
}

// Put
// Write a key/value pair into the live tail.
//
// In an immutable store, rewriting a key with the same value is a
// no-op (this is what makes replay and re-import idempotent) and
// rewriting it with a different value is an error.
func (s *SegmentStore) Put(key [32]byte, value []byte) (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.put(key, value)
}

// put is Put; the caller must hold the Mutex
func (s *SegmentStore) put(key [32]byte, value []byte) (err error) {
	if err = s.checkOpen(); err != nil {
		return err
	}
	s.stats.PutTotal++
	if !s.Mutable {
		if existing, err := s.get(key); err == nil {
			if bytes.Equal(existing, value) {
				s.stats.PutDuplicate++
				return nil // Same value: no-op
			}
			s.stats.PutConflict++
			return ErrImmutable
		}
	}
	s.stats.PutNew++
	return s.writeRecord(key, value)
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
	if s.keys != nil {
		s.keys.Set(key)
	}
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
// authorises the write; anything else is returned.
func (s *SegmentStore) PutIfAbsent(key [32]byte, value []byte) (existing []byte, existed bool, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if err = s.checkOpen(); err != nil {
		return nil, false, err
	}
	s.stats.PutTotal++
	switch existing, err = s.get(key); {
	case err == nil:
		// Split the two ways a key can already be here, because they
		// mean opposite things: an identical rewrite is a write this
		// check avoided, a differing one is a write it caught.
		if bytes.Equal(existing, value) {
			s.stats.PutDuplicate++
		} else {
			s.stats.PutConflict++
		}
		return existing, true, nil
	case !errors.Is(err, errNotFound):
		return nil, false, err
	}
	s.stats.PutNew++
	return nil, false, s.writeRecord(key, value)
}

// LiveCount
// The number of keys in the live tail (not yet sealed)
func (s *SegmentStore) LiveCount() int {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return len(s.live)
}

// LiveRecords
// The number of physical records in the live tail.  A mutable store
// leaves one record per write, so a key rewritten n times leaves n
// records: this, not LiveCount, is what bounds the tail's size on disk
// and its replay cost on open.
func (s *SegmentStore) LiveRecords() uint64 {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.liveRecords
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
			s.blockHeight = height + 1
			return meta, s.writeManifest()
		}
		return meta, nil
	}

	dataName := segmentFileName(height, seq)
	dataPath := filepath.Join(s.Directory, dataName)

	var sl sealed
	if uint64(len(s.live)) == s.liveRecords {
		// No shadowed records: promote the live file as it stands
		if sl, err = s.promoteLiveFile(dataPath); err != nil {
			return meta, err
		}
	} else {
		// Shadowed records present (overwrites): write a compacted copy
		if sl, err = s.rewriteLiveFile(dataPath); err != nil {
			return meta, err
		}
	}

	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	if err = writeIndexFile(indexPath, sl.order, sl.entries); err != nil {
		return meta, err
	}

	meta = SegmentMeta{Height: height, Seq: seq, File: dataName, Count: uint64(len(sl.order)), Hash: sl.hash}
	seg, err := s.openSegment(meta)
	if err != nil {
		return meta, err
	}
	s.segments = append(s.segments, seg)
	if s.blockHeight < height {
		s.blockHeight = height
	}
	if blockBoundary {
		s.blockHeight = height + 1 // The next writes belong to the next block
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
// a usable height rather than a sentinel.
func (s *SegmentStore) newestMeta() (newest SegmentMeta, ok bool) {
	for _, seg := range s.segments {
		if !ok || seg.meta.after(newest) {
			newest, ok = seg.meta, true
		}
	}
	return newest, ok
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
// Finish the live file's header, make it durable, and rename it into
// place as a sealed segment.  No record is copied.
func (s *SegmentStore) promoteLiveFile(dataPath string) (sl sealed, err error) {
	count := s.liveRecords
	if err = s.liveFile.Flush(); err != nil {
		return sl, err
	}
	var header [segDataHdrSize]byte
	binary.BigEndian.PutUint32(header[:], segmentMagic)
	binary.BigEndian.PutUint32(header[4:], segmentVersion)
	binary.BigEndian.PutUint64(header[16:], count)
	if err = s.liveFile.WriteAt(0, header[:]); err != nil {
		return sl, err
	}
	if err = s.liveFile.File.Sync(); err != nil {
		return sl, err
	}
	livePath := s.liveFile.Filename
	if err = s.liveFile.File.Close(); err != nil {
		return sl, err
	}
	s.liveFile.File = nil
	if err = os.Rename(livePath, dataPath); err != nil {
		return sl, err
	}
	if err = syncDir(s.Directory); err != nil {
		return sl, err
	}

	// The rename moved no bytes, so the live tail's offsets are the
	// sealed segment's offsets
	sl.entries = s.live
	sl.order = make([][32]byte, 0, len(s.live))
	for key := range s.live {
		sl.order = append(sl.order, key)
	}
	if !s.Mutable { // Immutable segments are transported; a peer verifies this
		if sl.hash, _, err = hashAndCount(dataPath); err != nil {
			return sl, err
		}
	}
	return sl, nil
}

// rewriteLiveFile
// Write the live tail's newest record per key to a new sealed segment,
// dropping records shadowed by later writes
func (s *SegmentStore) rewriteLiveFile(dataPath string) (sl sealed, err error) {
	keys := make([][32]byte, 0, len(s.live))
	for key := range s.live {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })

	tmpPath := dataPath + segTmpSuffix
	f, err := os.Create(tmpPath)
	if err != nil {
		return sl, err
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
	binary.BigEndian.PutUint32(header[:], segmentMagic)
	binary.BigEndian.PutUint32(header[4:], segmentVersion)
	binary.BigEndian.PutUint64(header[16:], uint64(len(keys)))
	if _, err = out.Write(header[:]); err != nil {
		return sl, err
	}

	// Record where each value lands, so the index needs no read-back
	sl.order, sl.entries = keys, make(map[[32]byte]*DBBKey, len(keys))
	offset := uint64(segDataHdrSize)

	var recHdr [segRecHdrSize]byte
	value := make([]byte, 0, 1024)
	for _, key := range keys {
		dbb := s.live[key]
		if uint64(cap(value)) < dbb.Length {
			value = make([]byte, dbb.Length)
		}
		value = value[:dbb.Length]
		if err = s.liveFile.ReadAt(dbb.Offset, value); err != nil {
			return sl, err
		}
		copy(recHdr[:32], key[:])
		binary.BigEndian.PutUint64(recHdr[32:], dbb.Length)
		if _, err = out.Write(recHdr[:]); err != nil {
			return sl, err
		}
		if _, err = out.Write(value); err != nil {
			return sl, err
		}
		sl.entries[key] = &DBBKey{Offset: offset + segRecHdrSize, Length: dbb.Length}
		offset += segRecHdrSize + dbb.Length
	}
	if err = bw.Flush(); err != nil {
		return sl, err
	}
	if err = f.Sync(); err != nil {
		return sl, err
	}
	if err = f.Close(); err != nil {
		f = nil
		return sl, err
	}
	f = nil
	if err = os.Rename(tmpPath, dataPath); err != nil {
		return sl, err
	}
	if err = syncDir(s.Directory); err != nil {
		return sl, err
	}

	// The old live file is superseded by the sealed segment
	if s.liveFile.File != nil {
		s.liveFile.File.Close()
		s.liveFile.File = nil
	}
	os.Remove(s.liveFile.Filename)
	if h != nil {
		sl.hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	return sl, nil
}

// Compact
// Replace every sealed segment with one new segment holding only the
// keys that are still live, and commit it by replacing the manifest.
//
// Crash-atomic: the new generation is fully durable before the
// manifest names it, and a crash before that leaves the old generation
// in force (issue #19).  The live tail is untouched.
func (s *SegmentStore) Compact(height uint64) (meta SegmentMeta, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.compact(height)
}

// CompactNext
// Compact at the next available height.  The Dyna layer numbers its
// segments by generation rather than by block height, so it compacts
// by generation too.
func (s *SegmentStore) CompactNext() (meta SegmentMeta, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.compact(s.blockHeight)
}

// compact is Compact; the caller must hold the Mutex
func (s *SegmentStore) compact(height uint64) (meta SegmentMeta, err error) {
	if err = s.checkOpen(); err != nil {
		return meta, err
	}
	if len(s.segments) == 0 {
		return meta, nil
	}
	seq, err := s.nextKeyAt(height)
	if err != nil {
		return meta, err
	}
	if s.blockHeight < height {
		s.blockHeight = height
	}

	// Newest wins: walk segments newest to oldest, keeping the first
	// value seen for each key
	type liveVal struct {
		seg *segment
		dbb *DBBKey
	}
	winners := make(map[[32]byte]liveVal)
	for i := len(s.segments) - 1; i >= 0; i-- {
		seg := s.segments[i]
		entries := make([]byte, seg.count*DBKeyFullSize)
		if _, err = seg.index.ReadAt(entries, segIndexHdrSize); err != nil {
			return meta, err
		}
		for pos := 0; pos+DBKeyFullSize <= len(entries); pos += DBKeyFullSize {
			key, dbb, err := GetDBBKey(entries[pos : pos+DBKeyFullSize])
			if err != nil {
				return meta, err
			}
			if _, seen := winners[key]; !seen {
				winners[key] = liveVal{seg, dbb}
			}
		}
	}

	// A single generation holding one record per key has nothing to
	// reclaim: rewriting it would copy every value to produce the same
	// file.  The Dyna layer compacts on a write count, so it lands here
	// whenever a compaction has already run since the last seal.  The
	// meta returned is the standing generation's, at its own height --
	// not the height asked for, since no segment was written.
	if len(s.segments) == 1 && int64(len(winners)) == s.segments[0].records {
		return s.segments[0].meta, nil
	}

	keys := make([][32]byte, 0, len(winners))
	for key := range winners {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })

	dataName := segmentFileName(height, seq)
	dataPath := filepath.Join(s.Directory, dataName)
	tmpPath := filepath.Join(s.Directory, segCompactName+segTmpSuffix)
	f, err := os.Create(tmpPath)
	if err != nil {
		return meta, err
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
	binary.BigEndian.PutUint32(header[:], segmentMagic)
	binary.BigEndian.PutUint32(header[4:], segmentVersion)
	binary.BigEndian.PutUint64(header[16:], uint64(len(keys)))
	if _, err = out.Write(header[:]); err != nil {
		return meta, err
	}

	// Record where each value lands, so the index needs no read-back
	entries := make(map[[32]byte]*DBBKey, len(keys))
	offset := uint64(segDataHdrSize)

	var recHdr [segRecHdrSize]byte
	for _, key := range keys {
		w := winners[key]
		value, err := w.seg.value(w.dbb)
		if err != nil {
			return meta, err
		}
		copy(recHdr[:32], key[:])
		binary.BigEndian.PutUint64(recHdr[32:], uint64(len(value)))
		if _, err = out.Write(recHdr[:]); err != nil {
			return meta, err
		}
		if _, err = out.Write(value); err != nil {
			return meta, err
		}
		entries[key] = &DBBKey{Offset: offset + segRecHdrSize, Length: uint64(len(value))}
		offset += segRecHdrSize + uint64(len(value))
	}
	if err = bw.Flush(); err != nil {
		return meta, err
	}
	if err = f.Sync(); err != nil {
		return meta, err
	}
	if err = f.Close(); err != nil {
		f = nil
		return meta, err
	}
	f = nil
	if err = os.Rename(tmpPath, dataPath); err != nil {
		return meta, err
	}
	if err = syncDir(s.Directory); err != nil {
		return meta, err
	}
	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	if err = writeIndexFile(indexPath, keys, entries); err != nil {
		return meta, err
	}

	meta = SegmentMeta{Height: height, Seq: seq, File: dataName, Count: uint64(len(keys)), Hash: ""}
	if h != nil {
		meta.Hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	seg, err := s.openSegment(meta)
	if err != nil {
		return meta, err
	}

	// Commit: the manifest now names only the new generation
	old := s.segments
	s.segments = []*segment{seg}
	if err = s.writeManifest(); err != nil {
		s.segments = old // Uncommitted; keep serving the old generation
		seg.close()
		return meta, err
	}

	// Committed.  The old files are unreachable; removing them is
	// cleanup, and anything left behind is swept on the next open.
	for _, o := range old {
		path := filepath.Join(s.Directory, o.meta.File)
		o.close()
		os.Remove(path)
		os.Remove(strings.TrimSuffix(path, segDataSuffix) + segIndexSuffix)
	}

	// Compaction is the one moment the true key set is already in hand:
	// what survived is exactly the new generation plus the live tail.
	// Keeping the old filter would only cost lookups -- it can name
	// keys that are gone but never miss one that stayed -- but a
	// rebuild here is free of that drift and bounds the layer count.
	if s.keys != nil {
		if err := s.rebuildKeyFilter(); err != nil {
			s.keys = nil // Correct, just slower: lookups walk again
		}
	}
	return meta, nil
}

// SegmentPaths
// The sealed segment files backing this store, oldest first, with the
// manifest metadata that identifies and verifies each one.  Syncing a
// peer is copying these files.
func (s *SegmentStore) SegmentPaths() (metas []SegmentMeta, paths []string) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	for _, seg := range s.segments {
		metas = append(metas, seg.meta)
		paths = append(paths, filepath.Join(s.Directory, seg.meta.File))
	}
	return metas, paths
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

	for _, seg := range s.segments {
		if seg.meta.Height == meta.Height && seg.meta.Seq == meta.Seq && seg.meta.Hash == meta.Hash {
			return nil // Already have it
		}
	}
	if newest, ok := s.newestMeta(); ok && !meta.after(newest) {
		return fmt.Errorf("segment (block %d, seq %d) is not above the newest segment (block %d, seq %d)",
			meta.Height, meta.Seq, newest.Height, newest.Seq)
	}
	if err = VerifySegmentFile(path, meta.Hash); err != nil {
		return err
	}

	dataName := segmentFileName(meta.Height, meta.Seq)
	dataPath := filepath.Join(s.Directory, dataName)
	tmpPath := dataPath + segTmpSuffix
	if err = copyFileSynced(path, tmpPath); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, dataPath); err != nil {
		return err
	}
	if err = syncDir(s.Directory); err != nil {
		return err
	}
	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	if err = buildIndexFor(dataPath, indexPath); err != nil {
		return err
	}

	meta.File = dataName
	seg, err := s.openSegment(meta)
	if err != nil {
		return err
	}
	meta.Count = uint64(seg.count)
	seg.meta = meta
	if s.blockHeight < meta.Height {
		s.blockHeight = meta.Height
	}

	// An immutable store must not silently shadow a value it already
	// holds: that is a divergence, not an update.  Checking is cheap
	// because the keys a syncing node receives are almost all new --
	// the existing segments' bloom filters reject them from memory, and
	// only a bloom hit costs a real lookup.
	if !s.Mutable {
		if err = s.checkNoConflicts(seg); err != nil {
			seg.close()
			os.Remove(dataPath)
			os.Remove(indexPath)
			return err
		}
	}

	s.segments = append(s.segments, seg)
	if s.keys != nil {
		if err = s.addSegmentKeys(seg); err != nil {
			s.keys = nil // The filter must never under-report; drop it
		}
	}
	return s.writeManifest() // Commit
}

// checkNoConflicts
// Verify that no key in an incoming segment already has a different
// value in this store.  The caller must hold the Mutex, and seg must
// not yet be in s.segments.
func (s *SegmentStore) checkNoConflicts(seg *segment) (err error) {
	const batch = 4096 // Index records read per pass
	buff := make([]byte, batch*DBKeyFullSize)
	for i := int64(0); i < seg.count; i += batch {
		n := seg.count - i
		if n > batch {
			n = batch
		}
		chunk := buff[:n*DBKeyFullSize]
		if _, err = seg.index.ReadAt(chunk, segIndexHdrSize+i*DBKeyFullSize); err != nil {
			return err
		}
		for pos := 0; pos+DBKeyFullSize <= len(chunk); pos += DBKeyFullSize {
			key, dbb, err := GetDBBKey(chunk[pos : pos+DBKeyFullSize])
			if err != nil {
				return err
			}
			existing, err := s.get(key) // Bloom-gated; no disk I/O unless a filter hits
			if err != nil {
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

	// Persist the key filter, then record in the manifest that it is
	// current -- in that order, so a crash between the two leaves a
	// manifest saying "rebuild" rather than one pointing at a filter
	// that has fallen behind the segments.  A save that fails is not a
	// reason to fail the close: the next open rebuilds.
	if !s.closed && s.keys != nil {
		if err := s.keys.Save(s.Directory); err == nil {
			s.bloomAt, _ = s.newestMeta() // Zero when there are none, which is what covers none
			s.bloomValid = true
			if err := s.writeManifest(); err != nil {
				return err
			}
		}
	}

	if s.liveFile != nil && s.liveFile.File != nil {
		if err = s.liveFile.Close(); err != nil {
			return err
		}
	}
	for _, seg := range s.segments {
		seg.close()
	}
	s.segments = nil
	s.closed = true
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
	if binary.BigEndian.Uint32(header[:]) != segmentMagic {
		return fmt.Errorf("%s is not a segment file", dataPath)
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
func writeIndexFile(indexPath string, order [][32]byte, entries map[[32]byte]*DBBKey) (err error) {
	buff := make([]byte, 0, len(order)*DBKeyFullSize)
	bloom := NewBloomSizedForKeys(uint64(len(order)), 3)
	for _, key := range order {
		buff = append(buff, entries[key].Bytes(key)...)
		bloom.Set(key)
	}
	sort.Sort(recordSort(buff)) // Sorted by key, for binary search

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
	binary.BigEndian.PutUint64(idxHdr[8:], uint64(len(order)))
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
	if err = os.Rename(tmpPath, indexPath); err != nil {
		return err
	}
	return syncDir(filepath.Dir(indexPath))
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
