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
// What this buys over the kfile/history/values arrangement:
//
//   - Sync is a file copy.  A sealed .dat is byte-identical to what a
//     peer sends; importing means copying the file, building its index,
//     and committing one manifest update -- no re-inserting records.
//   - No move-and-rewrite.  history.dat relocates whole key bins when
//     they grow (UpdateKeySet); sealed segments never move, so writes
//     cost what they weigh.
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
type SegmentMeta struct {
	Height uint64 `json:"height"` // Block height the segment was sealed at
	File   string `json:"file"`   // Data file name within the store directory
	Count  uint64 `json:"count"`  // Number of keys indexed
	Hash   string `json:"hash"`   // SHA-256 of the data file, hex
}

// StoreManifest
// The authoritative list of a store's sealed segments.  Replacing it
// is the commit point for both sealing and compaction.
type StoreManifest struct {
	Mutable  bool          `json:"mutable"`
	Segments []SegmentMeta `json:"segments"` // Oldest first
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

	segments    []*segment           // Sealed segments, oldest first
	live        map[[32]byte]*DBBKey // Keys written since the last seal
	liveFile    *BFile               // Their records
	liveRecords uint64               // Physical records in liveFile (>= len(live) if keys repeat)
	closed      bool                 // Set by Close; cleared by Open
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
	if err = s.openLive(); err != nil {
		return err
	}
	s.closed = false
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
	newest, haveSegments := s.newestHeight()

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
		height, err := heightFromName(name)
		if err != nil {
			continue // Not ours
		}
		path := filepath.Join(s.Directory, name)
		if haveSegments && height <= newest { // Superseded by a committed compaction
			os.Remove(path)
			os.Remove(strings.TrimSuffix(path, segDataSuffix) + segIndexSuffix)
			continue
		}
		hash, count, err := s.identify(path)
		if err != nil {
			return err
		}
		adopted = append(adopted, SegmentMeta{Height: height, File: name, Count: count, Hash: hash})
	}
	if len(adopted) == 0 {
		return nil
	}

	sort.Slice(adopted, func(i, j int) bool { return adopted[i].Height < adopted[j].Height })
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
func heightFromName(name string) (height uint64, err error) {
	body := strings.TrimSuffix(strings.TrimPrefix(name, segFilePrefix), segDataSuffix)
	_, err = fmt.Sscanf(body, "%d", &height)
	return height, err
}

// segmentFileName is the data file name for a height
func segmentFileName(height uint64) string {
	return fmt.Sprintf("%s%08d%s", segFilePrefix, height, segDataSuffix)
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
	m := StoreManifest{Mutable: s.Mutable}
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
	if dbb, ok := s.live[key]; ok {
		value = make([]byte, dbb.Length)
		if err = s.liveFile.ReadAt(dbb.Offset, value); err != nil {
			return nil, err
		}
		return value, nil
	}
	for i := len(s.segments) - 1; i >= 0; i-- { // Newest segment wins
		dbb, found, err := s.segments[i].lookup(key)
		if err != nil {
			return nil, err
		}
		if found {
			return s.segments[i].value(dbb)
		}
	}
	return nil, errors.New("not found")
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
	if !s.Mutable {
		if existing, err := s.get(key); err == nil {
			if bytes.Equal(existing, value) {
				return nil // Same value: no-op
			}
			return errors.New("cannot overwrite immutable value")
		}
	}

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
	return nil
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

// nextHeight
// The height at which a seal or compaction lands above every existing
// segment.  The caller must hold the Mutex.
func (s *SegmentStore) nextHeight() uint64 {
	if newest, ok := s.newestHeight(); ok {
		return newest + 1
	}
	return 0
}

// SealNext
// Seal the live tail at the next available height.  Used to bound the
// live tail when no block boundary has arrived; block boundaries call
// Seal with the block height instead.
func (s *SegmentStore) SealNext() (meta SegmentMeta, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.seal(s.nextHeight())
}

// Seal
// Seal the live tail into an immutable segment at the given height and
// start a fresh tail.  Sealing is the store's durability point.
//
// When the tail holds no shadowed records -- the usual case for an
// immutable store -- the file is renamed into place rather than
// copied, so sealing costs a header write, an index build, and a hash.
func (s *SegmentStore) Seal(height uint64) (meta SegmentMeta, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.seal(height)
}

// seal is Seal; the caller must hold the Mutex
func (s *SegmentStore) seal(height uint64) (meta SegmentMeta, err error) {
	if err = s.checkOpen(); err != nil {
		return meta, err
	}
	if len(s.live) == 0 {
		return meta, nil // Nothing to seal
	}
	if newest, ok := s.newestHeight(); ok && height <= newest {
		return meta, fmt.Errorf("seal height %d is not above the newest segment height %d", height, newest)
	}

	dataName := segmentFileName(height)
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

	meta = SegmentMeta{Height: height, File: dataName, Count: uint64(len(sl.order)), Hash: sl.hash}
	seg, err := s.openSegment(meta)
	if err != nil {
		return meta, err
	}
	s.segments = append(s.segments, seg)

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

// newestHeight
// The height of the newest sealed segment.  ok is false when the store
// has no sealed segments, so that height 0 (a genesis block) is a
// usable height rather than a sentinel.
func (s *SegmentStore) newestHeight() (newest uint64, ok bool) {
	for _, seg := range s.segments {
		if !ok || seg.meta.Height > newest {
			newest, ok = seg.meta.Height, true
		}
	}
	return newest, ok
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
	return s.compact(s.nextHeight())
}

// compact is Compact; the caller must hold the Mutex
func (s *SegmentStore) compact(height uint64) (meta SegmentMeta, err error) {
	if err = s.checkOpen(); err != nil {
		return meta, err
	}
	if len(s.segments) == 0 {
		return meta, nil
	}
	if newest, ok := s.newestHeight(); ok && height <= newest {
		return meta, fmt.Errorf("compaction height %d is not above the newest segment height %d", height, newest)
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

	dataName := segmentFileName(height)
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

	meta = SegmentMeta{Height: height, File: dataName, Count: uint64(len(keys)), Hash: ""}
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
		if seg.meta.Height == meta.Height && seg.meta.Hash == meta.Hash {
			return nil // Already have it
		}
	}
	if newest, ok := s.newestHeight(); ok && meta.Height <= newest {
		return fmt.Errorf("segment height %d is not above the newest segment height %d", meta.Height, newest)
	}
	if err = VerifySegmentFile(path, meta.Hash); err != nil {
		return err
	}

	dataName := segmentFileName(meta.Height)
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
