package blockchainDB

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// PermStore is the permanent layer as files of entries and buckets of
// keys (proposal docs/proposals/2026-09-16-entries-written-once.md,
// step 2).  An entry is written once at the end of a data file and
// never moves; everything the store does to stay fast is done on
// 44-byte records that name entries (permindex.go).
//
// Files, in one directory:
//
//	perm-N.dat   entries, [len][height][key][value][crc] (heap.go's
//	             layout), appended in the order written, rolled at
//	             PermFileBytes; never rewritten
//	runs-N.dat   maintenance's run file: a bucket's run at every merge,
//	             a retired run at every pack; rolled at PermFileBytes
//	             and deleted once no run in it is referenced
//
// A block's delta -- its records' run, with its filter -- is appended
// to the block's data file right behind the block's entries, as an
// entry under the reserved key heap.go's deltas use, so a block is
// ONE fsync per shard for the permanent layer: entries and delta
// together.  Open replays the deltas in the data files after the
// point the manifest records, verifies the last one's entries by
// their checksums before trusting it, and cuts the newest data file
// back to the last delta.
//
//	perm.json    the manifest: which runs are the window, the buckets
//	             and the retired history, and where in the run files
//	             the deltas not yet in the manifest begin
//
// Tiers, newest to oldest:
//
//   - the live map: this block's records, in memory until the seal;
//   - the window: the last FilterBlocks deltas, each a run with its
//     filter resident.  The protocol path reads the live map and the
//     window and nothing older (spec 1.3): an immutable key the window
//     does not hold is absent;
//   - the buckets: PermBuckets buckets by the key's first byte, each a
//     few sorted runs.  A delta that leaves the window feeds each
//     bucket's recent records; every bucket is merged every
//     PermMergeEvery blocks, in rotation, its recent records written
//     as a run and its runs folded by the store's ratio.  The buckets
//     hold only the history above the pack watermark, so a bucket is
//     bounded by the pack period and hashed keys keep it even;
//   - retired runs: at every pack, every bucket's runs and every
//     delta below the watermark fold into one sorted run, and the
//     buckets start again.  Retired runs are the deep history.
//
// GetDeep walks the tiers in that order; Get stops after the window.
//
// Durability (spec 1.8): a seal fsyncs the data files the block wrote
// and then appends and fsyncs the block's delta, so a delta names
// only durable entries.  Open reads the manifest, then every whole
// delta after the offset it records; a torn delta is dropped whole.
// Maintenance writes new runs, fsyncs them, and commits the manifest
// (written aside and renamed) before any run file is deleted: never
// unlink what a durable manifest names.
type PermStore struct {
	Directory string

	mu     sync.RWMutex
	files  map[uint32]*heapFile // Data files, by id
	cur    *heapFile            // The data file the block appends to
	nextID uint32
	dirty  map[uint32]*heapFile

	runs      map[uint32]*runFile // Run files, by id
	maintRun  *runFile            // The run file maintenance appends to: never the seal's, so their barriers never share an inode
	nextRun   uint32
	live      map[[32]byte]permRecord // This block's records
	window    []*permDelta            // The last FilterBlocks deltas, oldest first
	pending   []*permDelta            // Deltas below the window not yet in every bucket
	buckets   [PermBuckets]permBucket // The history above the watermark
	retired   []*permRun              // The history below it, newest last
	height    uint64
	window_n  uint64 // FilterBlocks
	rotation  int    // The next bucket to merge
	lastMerge uint64 // The height Merge last ran at: what decides how many buckets are due

	// The manifest's view: the run file and offset after which deltas
	// are replayed on open
	deltasFrom struct {
		file uint32
		off  int64
	}
	syncMu        sync.Mutex // Serializes seals with each other and with manifest commits
	manifestAt    uint64     // The height the manifest was last committed at
	manifestDirty bool       // Merges since then
	closed        bool

	putTotal, putDuplicate, lookups, windowHits, deepHits atomic.Uint64
	mergeRuns, foldRuns, packRuns                         atomic.Uint64
	indexBytes                                            atomic.Uint64
}

// PermManifestBlocks is how many blocks may pass between manifest
// commits when no fold made one necessary.  Counted in blocks so the
// caller's cadence does not set the commit cadence.
var PermManifestBlocks uint64 = 160

// PermBuckets is how many buckets a shard's history above the
// watermark is kept in, by the key's first byte.
const PermBuckets = 256

// PermMergeEvery is how many blocks pass between merges of one
// bucket: PermBuckets/PermMergeEvery buckets are merged each block.
// It bounds how many deltas wait unmerged, which every deep read
// probes and every merge reads a slice of: at 256, read p99 climbed
// from 7 to 47 us over five minutes and merge work from 157 to 305 s
// a minute.  One window's worth keeps both flat; the extra folds
// cost a fraction of the index bytes.
var PermMergeEvery uint64 = MinFilterBlocks

// PermFileBytes is the size data files and run files are rolled at.
var PermFileBytes int64 = 64 << 20

// PermFoldRatio is the ratio a bucket's runs fold by: a suffix of runs
// folds while each older run is no larger than 1/ratio of what has
// gathered behind it.
var PermFoldRatio = 0.25

type permDelta struct {
	height uint64
	run    *permRun
	f      *os.File // The data file the delta lies in
	data   uint32   // Its id
}

type permBucket struct {
	runs   []*permRun // Oldest first
	files  []*runFile
	merged uint64 // The height of the newest delta merged in
}

// runFile is a file of runs and how many runs still reference it.
type runFile struct {
	id       uint32
	f        *os.File
	size     int64
	refs     int
	unsynced bool // Runs written since the file was last fsynced
}

func permDataName(id uint32) string { return fmt.Sprintf("perm-%06d.dat", id) }
func permRunName(id uint32) string  { return fmt.Sprintf("runs-%06d.dat", id) }
func (rf *runFile) name() string    { return permRunName(rf.id) }

// NewPermStore creates an empty store in directory, replacing anything
// there.
func NewPermStore(directory string, filterBlocks uint64) (*PermStore, error) {
	os.RemoveAll(directory)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	p := &PermStore{Directory: directory, window_n: filterBlocks}
	return p, p.Open()
}

// OpenPermStore opens the store in directory as it was left.
func OpenPermStore(directory string) (*PermStore, error) {
	if _, err := os.Stat(filepath.Join(directory, "perm.json")); err != nil {
		return nil, fmt.Errorf("open perm at %s: %w", directory, err)
	}
	p := &PermStore{Directory: directory}
	return p, p.Open()
}

// permManifest is perm.json.
type permManifest struct {
	Version      int           `json:"version"`
	Height       uint64        `json:"height"`
	FilterBlocks uint64        `json:"filterBlocks"`
	Rotation     int           `json:"rotation"`
	NextData     uint32        `json:"nextData"`
	NextRun      uint32        `json:"nextRun"`
	DeltasFile   uint32        `json:"deltasFile"` // Deltas after this file:offset are replayed
	DeltasOff    int64         `json:"deltasOff"`
	Window       []permRunRef  `json:"window"`
	Pending      []permRunRef  `json:"pending"`
	Buckets      []permBucketM `json:"buckets"`
	Retired      []permRunRef  `json:"retired"`
	DataFiles    []uint32      `json:"dataFiles"`
	RunFiles     []uint32      `json:"runFiles"`
}

type permRunRef struct {
	File   uint32 `json:"file"` // A data file for a delta, a run file otherwise
	Off    int64  `json:"off"`
	Height uint64 `json:"height,omitempty"`
}

type permBucketM struct {
	Runs   []permRunRef `json:"runs"`
	Merged uint64       `json:"merged"`
}

// Open loads the manifest and replays the deltas after it.
func (p *PermStore) Open() (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.files != nil {
		return nil
	}
	p.files = map[uint32]*heapFile{}
	p.dirty = map[uint32]*heapFile{}
	p.runs = map[uint32]*runFile{}
	p.live = map[[32]byte]permRecord{}
	p.closed = false
	var m permManifest
	buf, err := os.ReadFile(filepath.Join(p.Directory, "perm.json"))
	switch {
	case errors.Is(err, os.ErrNotExist):
		// A new store: the first data file and run file
		if p.window_n == 0 {
			p.window_n = MinFilterBlocks
		}
		if p.cur, err = p.newDataFile(); err != nil {
			return err
		}
		return p.commitManifest()
	case err != nil:
		return err
	}
	if err = json.Unmarshal(buf, &m); err != nil {
		return fmt.Errorf("perm.json: %w", err)
	}
	if m.Version != 1 {
		return fmt.Errorf("perm.json: version %d, want 1", m.Version)
	}
	p.height, p.window_n, p.rotation, p.nextID, p.nextRun = m.Height, m.FilterBlocks, m.Rotation, m.NextData, m.NextRun
	for _, id := range m.DataFiles {
		f, err := os.OpenFile(filepath.Join(p.Directory, permDataName(id)), os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("perm: the manifest names %s: %w", permDataName(id), err)
		}
		st, _ := f.Stat()
		p.files[id] = &heapFile{id: id, f: f, size: st.Size()}
	}
	for _, id := range m.RunFiles {
		f, err := os.OpenFile(filepath.Join(p.Directory, permRunName(id)), os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("perm: the manifest names %s: %w", permRunName(id), err)
		}
		st, _ := f.Stat()
		p.runs[id] = &runFile{id: id, f: f, size: st.Size()}
	}
	// Data files the seals rolled after the manifest's commit are not
	// named by it, but they hold committed deltas: they are taken in
	// id order from the manifest's next id for as long as they exist,
	// and the replay below covers them.  Run files past the manifest's
	// next id are maintenance output that was never named; nothing
	// durable refers to them, so they go, and their ids are free for
	// O_EXCL creation again.
	for {
		f, err := os.OpenFile(filepath.Join(p.Directory, permDataName(p.nextID)), os.O_RDWR, 0o644)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return err
		}
		st, _ := f.Stat()
		p.files[p.nextID] = &heapFile{id: p.nextID, f: f, size: st.Size()}
		p.nextID++
	}
	for id := p.nextRun; ; id++ {
		err := os.Remove(filepath.Join(p.Directory, (&runFile{id: id}).name()))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return err
		}
	}
	load := func(ref permRunRef, resident bool) (*permRun, *runFile, error) {
		rf := p.runs[ref.File]
		if rf == nil {
			return nil, nil, fmt.Errorf("perm: run file %d not open", ref.File)
		}
		r, err := openPermRun(rf.f, ref.File, ref.Off, resident)
		if err != nil {
			return nil, nil, err
		}
		rf.refs++
		return r, rf, nil
	}
	loadDelta := func(ref permRunRef) (*permDelta, error) {
		hf := p.files[ref.File]
		if hf == nil {
			return nil, fmt.Errorf("perm: data file %d not open", ref.File)
		}
		r, err := openPermRun(hf.f, ref.File, ref.Off, true)
		if err != nil {
			return nil, err
		}
		return &permDelta{height: ref.Height, run: r, f: hf.f, data: ref.File}, nil
	}
	for _, ref := range m.Window {
		d, err := loadDelta(ref)
		if err != nil {
			return err
		}
		p.window = append(p.window, d)
	}
	for _, ref := range m.Pending {
		d, err := loadDelta(ref)
		if err != nil {
			return err
		}
		p.pending = append(p.pending, d)
	}
	for i, bm := range m.Buckets {
		if i >= PermBuckets {
			break
		}
		p.buckets[i].merged = bm.Merged
		for _, ref := range bm.Runs {
			r, rf, err := load(ref, true)
			if err != nil {
				return err
			}
			p.buckets[i].runs = append(p.buckets[i].runs, r)
			p.buckets[i].files = append(p.buckets[i].files, rf)
		}
	}
	for _, ref := range m.Retired {
		r, _, err := load(ref, false)
		if err != nil {
			return err
		}
		p.retired = append(p.retired, r)
	}
	// The current data file is the newest; the seal's run file is the
	// manifest's; maintenance opens a file of its own when it runs
	for _, hf := range p.files {
		if p.cur == nil || hf.id > p.cur.id {
			p.cur = hf
		}
	}
	p.deltasFrom.file, p.deltasFrom.off = m.DeltasFile, m.DeltasOff
	return p.replayDeltas()
}

// replayDeltas applies every delta entry in the data files after the
// manifest's point, oldest first, into the window and pending.  The
// last delta is trusted only if every entry it names checks -- one
// fsync covered it and its entries together -- and the newest data
// file is cut back to the last delta admitted.  The caller holds the
// lock.
func (p *PermStore) replayDeltas() error {
	ids := make([]uint32, 0, len(p.files))
	for id := range p.files {
		if id >= p.deltasFrom.file {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	type found struct {
		hf       *heapFile
		off, end int64
		run      *permRun
	}
	var deltas []found
	for _, id := range ids {
		hf := p.files[id]
		buf := make([]byte, hf.size)
		if _, err := hf.f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		at := int64(0)
		if id == p.deltasFrom.file {
			at = p.deltasFrom.off
		}
		for at < int64(len(buf)) {
			size, _, key, _, ok := decodeEntry(buf[at:])
			if size == 0 {
				break
			}
			if ok && key == heapDeltaKey {
				r, err := openPermRun(hf.f, id, at+heapHeader, true)
				if err == nil {
					deltas = append(deltas, found{hf: hf, off: at, end: at + size, run: r})
				}
			}
			at += size
		}
	}
	var last *found
	for i := range deltas {
		d := &deltas[i]
		if i == len(deltas)-1 && !p.deltaEntriesCheck(d.run, d.hf.f) {
			break // The block did not commit
		}
		p.admit(&permDelta{height: d.run.height, run: d.run, f: d.hf.f, data: d.hf.id})
		if d.run.height >= p.height {
			p.height = d.run.height + 1
		}
		last = d
	}
	// The newest data file is cut back to what is committed: the last
	// delta admitted, or the manifest's point
	newest := p.files[ids[len(ids)-1]]
	end := int64(0)
	if newest.id == p.deltasFrom.file {
		end = p.deltasFrom.off
	}
	if last != nil && last.hf == newest {
		end = last.end
	}
	if newest.size > end {
		if err := newest.f.Truncate(end); err != nil {
			return err
		}
		newest.size = end
	}
	p.cur = newest
	return nil
}

// deltaEntriesCheck reads every entry a delta names and checks it.
func (p *PermStore) deltaEntriesCheck(r *permRun, f *os.File) bool {
	recs, err := r.records(f)
	if err != nil {
		return false
	}
	for _, rec := range recs {
		if _, err := p.readEntry(rec, rec.key); err != nil {
			return false
		}
	}
	return true
}

// admit puts a delta in the window and moves what falls out of it to
// pending.  The caller holds the lock.
func (p *PermStore) admit(d *permDelta) {
	p.window = append(p.window, d)
	for len(p.window) > int(p.window_n) {
		p.pending = append(p.pending, p.window[0])
		p.window = p.window[1:]
	}
}

func (p *PermStore) newDataFile() (*heapFile, error) {
	id := p.nextID
	p.nextID++
	f, err := os.OpenFile(filepath.Join(p.Directory, permDataName(id)), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	hf := &heapFile{id: id, f: f}
	p.files[id] = hf
	return hf, nil
}

func (p *PermStore) newRunFile() (*runFile, error) {
	id := p.nextRun
	p.nextRun++
	rf := &runFile{id: id}
	f, err := os.OpenFile(filepath.Join(p.Directory, rf.name()), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	rf.f = f
	p.runs[id] = rf
	return rf, nil
}

// Close seals what is pending and closes the files.
func (p *PermStore) Close() error {
	sync, err := p.beginSeal(p.height)
	if err != nil {
		return err
	}
	if err = sync.finish(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.manifestDirty {
		if err = p.syncRunFiles(); err != nil {
			return err
		}
		if err = p.commitManifest(); err != nil {
			return err
		}
		if err = p.dropUnreferencedRunFiles(); err != nil {
			return err
		}
	}
	p.closed = true
	for _, hf := range p.files {
		if cerr := hf.f.Close(); err == nil {
			err = cerr
		}
	}
	for _, rf := range p.runs {
		if cerr := rf.f.Close(); err == nil {
			err = cerr
		}
	}
	p.files, p.runs, p.cur, p.maintRun = nil, nil, nil, nil
	return err
}

// PutIfAbsent writes value under key unless the window already holds
// the key, in which case the existing value is returned instead.  The
// window is the store's immutability horizon (spec 1.3).
func (p *PermStore) PutIfAbsent(key [32]byte, value []byte) (existing []byte, existed bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false, errStoreClosed
	}
	p.putTotal.Add(1)
	if rec, ok := p.live[key]; ok {
		p.putDuplicate.Add(1)
		v, err := p.readEntry(rec, key)
		return v, true, err
	}
	for i := len(p.window) - 1; i >= 0; i-- {
		d := p.window[i]
		rec, found, err := d.run.lookup(d.f, key)
		if err != nil {
			return nil, false, err
		}
		if found {
			p.putDuplicate.Add(1)
			v, err := p.readEntry(rec, key)
			return v, true, err
		}
	}
	size := entrySize(len(value))
	if p.cur.size+size > PermFileBytes {
		if p.cur, err = p.newDataFile(); err != nil {
			return nil, false, err
		}
	}
	rec := permRecord{key: key, file: p.cur.id, off: uint32(p.cur.size), n: uint32(len(value))}
	if _, err = p.cur.f.WriteAt(encodeEntry(p.height, key, value), p.cur.size); err != nil {
		return nil, false, err
	}
	p.cur.size += size
	p.dirty[p.cur.id] = p.cur
	p.live[key] = rec
	return nil, false, nil
}

// Put is PutIfAbsent for callers that only need the error; a key
// already held is not an error, the value stands.
func (p *PermStore) Put(key [32]byte, value []byte) error {
	_, _, err := p.PutIfAbsent(key, value)
	return err
}

func (p *PermStore) readEntry(rec permRecord, key [32]byte) ([]byte, error) {
	hf := p.files[rec.file]
	if hf == nil {
		return nil, fmt.Errorf("perm: data file %d not open", rec.file)
	}
	buf := make([]byte, heapHeader+int(rec.n)+heapTrailer)
	if _, err := hf.f.ReadAt(buf, int64(rec.off)); err != nil {
		return nil, err
	}
	return heapEntryValue(buf, key)
}

// Get answers from the live map and the window; a key older than the
// window is absent here (spec 1.3), GetDeep reaches it.
func (p *PermStore) Get(key [32]byte) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, errStoreClosed
	}
	p.lookups.Add(1)
	if rec, ok := p.live[key]; ok {
		p.windowHits.Add(1)
		return p.readEntry(rec, key)
	}
	for i := len(p.window) - 1; i >= 0; i-- {
		d := p.window[i]
		rec, found, err := d.run.lookup(d.f, key)
		if err != nil {
			return nil, err
		}
		if found {
			p.windowHits.Add(1)
			return p.readEntry(rec, key)
		}
	}
	return nil, errNotFound
}

// GetDeep is Get, then the deltas not yet merged, the key's bucket
// newest run first, and the retired runs newest first.
func (p *PermStore) GetDeep(key [32]byte) ([]byte, error) {
	v, err := p.Get(key)
	if err == nil || !errors.Is(err, errNotFound) {
		return v, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for i := len(p.pending) - 1; i >= 0; i-- {
		d := p.pending[i]
		rec, found, err := d.run.lookup(d.f, key)
		if err != nil {
			return nil, err
		}
		if found {
			p.deepHits.Add(1)
			return p.readEntry(rec, key)
		}
	}
	b := &p.buckets[key[0]]
	for i := len(b.runs) - 1; i >= 0; i-- {
		rec, found, err := b.runs[i].lookup(b.files[i].f, key)
		if err != nil {
			return nil, err
		}
		if found {
			p.deepHits.Add(1)
			return p.readEntry(rec, key)
		}
	}
	for i := len(p.retired) - 1; i >= 0; i-- {
		r := p.retired[i]
		rf := p.runs[r.file]
		rec, found, err := r.lookup(rf.f, key)
		if err != nil {
			return nil, err
		}
		if found {
			p.deepHits.Add(1)
			return p.readEntry(rec, key)
		}
	}
	return nil, errNotFound
}

// permSeal is a seal in flight: the data files to fsync, the delta
// to append.  Holds syncMu until finished.
type permSeal struct {
	p      *PermStore
	dirty  []*heapFile
	recs   []permRecord
	height uint64
	run    *permRun  // The delta as written, admitted once durable
	hf     *heapFile // The data file it was written to
}

// beginSeal takes the block's records under the lock; finish makes
// them durable outside it.
func (p *PermStore) beginSeal(height uint64) (*permSeal, error) {
	p.syncMu.Lock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.files == nil {
		p.syncMu.Unlock()
		return nil, errStoreClosed
	}
	s := &permSeal{p: p, height: height}
	for _, hf := range p.dirty {
		s.dirty = append(s.dirty, hf)
	}
	p.dirty = map[uint32]*heapFile{}
	s.recs = make([]permRecord, 0, len(p.live))
	for _, rec := range p.live {
		s.recs = append(s.recs, rec)
	}
	p.live = map[[32]byte]permRecord{} // A put from here on is the next block's
	sortPermRecords(s.recs)
	if len(s.recs) == 0 {
		return s, nil
	}
	// The delta's run goes into the block's data file behind the
	// block's entries, under the reserved key: one fsync of that file
	// covers both, and replay trusts the last delta only if every
	// entry it names checks (spec 1.7)
	var w bufWriterAt
	run, err := writePermRun(&w, 0, p.cur.id, s.recs, s.height)
	if err != nil {
		p.syncMu.Unlock()
		return nil, err
	}
	entry := encodeEntry(s.height, heapDeltaKey, w.buf)
	if p.cur.size+int64(len(entry)) > PermFileBytes {
		if p.cur, err = p.newDataFile(); err != nil {
			p.syncMu.Unlock()
			return nil, err
		}
	}
	hf, at := p.cur, p.cur.size
	if _, err = hf.f.WriteAt(entry, at); err != nil {
		p.syncMu.Unlock()
		return nil, err
	}
	hf.size += int64(len(entry))
	run.file, run.off, run.bloomAt = hf.id, at+heapHeader, at+heapHeader+run.bloomAt
	s.run, s.hf = run, hf
	if _, dirty := p.dirty[hf.id]; !dirty {
		for _, d := range s.dirty {
			if d == hf {
				dirty = true
			}
		}
		if !dirty {
			s.dirty = append(s.dirty, hf)
		}
	}
	return s, nil
}

// finish: the block's entries and its delta durable, one fsync per
// file touched, with the lock released; then the delta admitted to
// the window.  Releases syncMu.
func (s *permSeal) finish() error {
	p := s.p
	defer p.syncMu.Unlock()
	for _, hf := range s.dirty {
		if err := fsync(hf.f); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.run != nil {
		p.indexBytes.Add(uint64(s.run.bytes))
		p.admit(&permDelta{height: s.height, run: s.run, f: s.hf.f, data: s.hf.id})
	}
	if s.height >= p.height {
		p.height = s.height + 1
	}
	return nil
}

// bufWriterAt collects what a run writer writes at offset 0.
type bufWriterAt struct{ buf []byte }

func (w *bufWriterAt) WriteAt(b []byte, off int64) (int, error) {
	if need := int(off) + len(b); need > len(w.buf) {
		w.buf = append(w.buf, make([]byte, need-len(w.buf))...)
	}
	copy(w.buf[off:], b)
	return len(b), nil
}

// AdvanceBlock sets the block new writes belong to.
func (p *PermStore) AdvanceBlock(height uint64) {
	p.mu.Lock()
	p.height = height
	p.mu.Unlock()
}

// BlockHeight is the block being written.
func (p *PermStore) BlockHeight() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.height
}

// LiveCount is the records this block has written so far.
func (p *PermStore) LiveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.live)
}

// maintFile is the run file maintenance appends to, rolled at
// PermFileBytes.  The caller holds the lock.
func (p *PermStore) maintFile() (*runFile, error) {
	if p.maintRun == nil || p.maintRun.size > PermFileBytes {
		rf, err := p.newRunFile()
		if err != nil {
			return nil, err
		}
		p.maintRun = rf
	}
	return p.maintRun, nil
}

// Merge is the maintenance step: the buckets due in rotation take
// their records from the pending deltas, fold, and the manifest is
// committed; deltas every bucket has absorbed are released.  Runs
// are written and fsynced outside the lock; the lock is held to
// choose and to swap.
func (p *PermStore) Merge() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errStoreClosed
	}
	if len(p.pending) == 0 {
		p.mu.Unlock()
		return nil
	}
	// Which buckets: every bucket is due once per PermMergeEvery
	// blocks, so a call that comes after n blocks takes n/PermMergeEvery
	// of them, whatever the caller's cadence
	elapsed := p.height - p.lastMerge
	if p.lastMerge == 0 || elapsed > PermMergeEvery {
		elapsed = PermMergeEvery
	}
	due := int(uint64(PermBuckets) * elapsed / PermMergeEvery)
	if due < 1 {
		due = 1
	}
	if due > PermBuckets {
		due = PermBuckets
	}
	p.lastMerge = p.height
	newest := p.pending[len(p.pending)-1].height
	type job struct{ b int }
	var jobs []job
	for i := 0; i < due; i++ {
		jobs = append(jobs, job{b: (p.rotation + i) % PermBuckets})
	}
	rotation := (p.rotation + due) % PermBuckets
	// Read the deltas' records for these buckets outside the lock:
	// runs are immutable.  The due buckets are a run of the rotation,
	// so their records are one or two slices of each sorted delta
	oldest := ^uint64(0)
	for _, j := range jobs {
		if p.buckets[j.b].merged < oldest {
			oldest = p.buckets[j.b].merged
		}
	}
	var ranges [][2]byte
	if last := p.rotation + due - 1; last < PermBuckets {
		ranges = append(ranges, [2]byte{byte(p.rotation), byte(last)})
	} else {
		ranges = append(ranges, [2]byte{byte(p.rotation), PermBuckets - 1}, [2]byte{0, byte(last - PermBuckets)})
	}
	pendingCopy := append([]*permDelta(nil), p.pending...)
	p.mu.Unlock()
	byBucket := map[int][][]permRecord{}
	for _, d := range pendingCopy {
		if d.height <= oldest {
			continue // Every due bucket has this delta already
		}
		var recs []permRecord
		for _, rg := range ranges {
			part, err := d.run.recordsBucketRange(d.f, rg[0], rg[1])
			if err != nil {
				return err
			}
			recs = append(recs, part...)
		}
		for _, j := range jobs {
			if d.height <= p.buckets[j.b].merged {
				continue
			}
			var mine []permRecord
			for _, r := range recs {
				if int(r.key[0]) == j.b {
					mine = append(mine, r)
				}
			}
			byBucket[j.b] = append(byBucket[j.b], mine)
		}
	}
	// Write each due bucket's new run to the maintenance run file
	p.mu.Lock()
	rf, err := p.maintFile()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	var written []struct {
		b   int
		run *permRun
	}
	for _, j := range jobs {
		recs := mergePermRuns(byBucket[j.b])
		if len(recs) == 0 {
			continue
		}
		p.mu.Lock()
		at := rf.size
		rf.size += 0
		p.mu.Unlock()
		run, err := writePermRun(rf.f, at, rf.id, recs, 0)
		if err != nil {
			return err
		}
		p.mu.Lock()
		rf.size = at + int64(run.bytes)
		p.mu.Unlock()
		p.indexBytes.Add(uint64(run.bytes))
		p.mergeRuns.Add(1)
		written = append(written, struct {
			b   int
			run *permRun
		}{b: j.b, run: run})
	}
	// No barrier here: a bucket's run is named by the manifest alone,
	// so it needs to be durable before the manifest commit and not
	// before.  Every merge call once ended with an fsync of its run
	// file, which at a call per shard per block was 72-144 barriers
	// a second at nine stores and doubled every seal's fsync.
	// Swap: the new runs join their buckets; then fold what the ratio
	// says, one bucket at a time
	p.mu.Lock()
	rf.unsynced = true
	for _, w := range written {
		bk := &p.buckets[w.b]
		bk.runs = append(bk.runs, w.run)
		bk.files = append(bk.files, rf)
		rf.refs++
	}
	for _, j := range jobs {
		p.buckets[j.b].merged = newest
	}
	p.rotation = rotation
	// Deltas every bucket has absorbed are released
	minMerged := ^uint64(0)
	for i := range p.buckets {
		if p.buckets[i].merged < minMerged {
			minMerged = p.buckets[i].merged
		}
	}
	var keep []*permDelta
	for _, d := range p.pending {
		if d.height > minMerged {
			keep = append(keep, d) // A delta in a data file costs nothing to drop: the file stays
		}
	}
	p.pending = keep
	folds := p.planFolds()
	p.mu.Unlock()
	// The folds' runs are written without a barrier too; the manifest
	// commit syncs every run file written since the last
	for _, f := range folds {
		if _, err := p.fold(f); err != nil {
			return err
		}
	}
	// The manifest names the new runs.  It is committed when a fold
	// left run files to drop, and otherwise every PermManifestEvery
	// merges: the runs are durable in their file already, and a
	// merge a crash loses is done again from the pending deltas, so a
	// commit per merge -- two barriers a shard -- bought nothing but
	// a shorter replay.  Open removes the run files a lost merge left
	// unnamed.
	p.mu.Lock()
	defer p.mu.Unlock()
	p.manifestDirty = true
	// A commit is due by age, or when a fold left a run file that
	// nothing references, since only a commit can drop it.  A fold
	// alone is not a reason: with a merge every block a bucket folds
	// on nearly every call, and a commit each time was two barriers
	// a shard a block.
	droppable := false
	for _, rf := range p.runs {
		if rf.refs == 0 && rf != p.maintRun {
			droppable = true
			break
		}
	}
	if !droppable && p.height-p.manifestAt < PermManifestBlocks {
		return nil
	}
	if err := p.syncRunFiles(); err != nil {
		return err
	}
	if err := p.commitManifest(); err != nil {
		return err
	}
	return p.dropUnreferencedRunFiles()
}

type permFold struct {
	b     int
	at    int // The runs from this index on fold into one
	count int
}

// planFolds chooses, per bucket, the suffix of runs the ratio says to
// fold.  The caller holds the lock.
func (p *PermStore) planFolds() (folds []permFold) {
	for b := range p.buckets {
		runs := p.buckets[b].runs
		if len(runs) < 2 {
			continue
		}
		var behind uint32
		i := len(runs) - 1
		for ; i >= 0; i-- {
			if i < len(runs)-1 && float64(runs[i].count)*PermFoldRatio > float64(behind) {
				break
			}
			behind += runs[i].count
		}
		if n := len(runs) - (i + 1); n >= 2 {
			folds = append(folds, permFold{b: b, at: i + 1, count: n})
		}
	}
	return folds
}

// fold merges a bucket's chosen runs into one, written to the
// maintenance run file (returned, for the caller to sync), and swaps
// it in under the lock.
func (p *PermStore) fold(f permFold) (*runFile, error) {
	p.mu.RLock()
	bk := &p.buckets[f.b]
	if f.at+f.count > len(bk.runs) {
		p.mu.RUnlock()
		return nil, nil // The bucket changed under us; next time
	}
	runs := append([]*permRun(nil), bk.runs[f.at:f.at+f.count]...)
	files := append([]*runFile(nil), bk.files[f.at:f.at+f.count]...)
	p.mu.RUnlock()
	inputs := make([][]permRecord, len(runs))
	for i, r := range runs {
		recs, err := r.records(files[i].f)
		if err != nil {
			return nil, err
		}
		inputs[i] = recs
	}
	merged := mergePermRuns(inputs)
	p.mu.Lock()
	rf, err := p.maintFile()
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	at := rf.size
	rf.size += 0
	p.mu.Unlock()
	run, err := writePermRun(rf.f, at, rf.id, merged, 0)
	if err != nil {
		return nil, err
	}
	p.indexBytes.Add(uint64(run.bytes))
	p.foldRuns.Add(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	rf.size = at + int64(run.bytes)
	rf.unsynced = true
	bk = &p.buckets[f.b]
	if f.at+f.count > len(bk.runs) {
		return rf, nil
	}
	for _, old := range files {
		old.refs--
	}
	rest := append([]*permRun(nil), bk.runs[f.at+f.count:]...)
	restF := append([]*runFile(nil), bk.files[f.at+f.count:]...)
	bk.runs = append(append(bk.runs[:f.at], run), rest...)
	bk.files = append(append(bk.files[:f.at], rf), restF...)
	rf.refs++
	return rf, nil
}

// Pack retires the history above the watermark: every bucket's runs
// and every pending delta fold into one sorted run, and the buckets
// start again.  The retired run is the deep history's newest.
func (p *PermStore) Pack() error {
	p.mu.RLock()
	var inputs [][]permRecord
	var release []*runFile
	for _, d := range p.pending {
		recs, err := d.run.records(d.f)
		if err != nil {
			p.mu.RUnlock()
			return err
		}
		inputs = append(inputs, recs)
	}
	for b := range p.buckets {
		for i, r := range p.buckets[b].runs {
			recs, err := r.records(p.buckets[b].files[i].f)
			if err != nil {
				p.mu.RUnlock()
				return err
			}
			inputs = append(inputs, recs)
			release = append(release, p.buckets[b].files[i])
		}
	}
	height := p.height
	p.mu.RUnlock()
	if len(inputs) == 0 {
		return nil
	}
	merged := mergePermRuns(inputs)
	p.mu.Lock()
	rf, err := p.maintFile()
	if err != nil {
		p.mu.Unlock()
		return err
	}
	at := rf.size
	p.mu.Unlock()
	run, err := writePermRun(rf.f, at, rf.id, merged, height)
	if err != nil {
		return err
	}
	if err = fsync(rf.f); err != nil {
		return err
	}
	p.indexBytes.Add(uint64(run.bytes))
	p.packRuns.Add(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	rf.size = at + int64(run.bytes)
	run.bloom = nil // Retired runs are probed cold
	p.retired = append(p.retired, run)
	rf.refs++
	for _, r := range release {
		r.refs--
	}
	p.pending = nil
	for b := range p.buckets {
		p.buckets[b] = permBucket{merged: height}
	}
	if err := p.syncRunFiles(); err != nil {
		return err
	}
	if err := p.commitManifest(); err != nil {
		return err
	}
	return p.dropUnreferencedRunFiles()
}

// syncRunFiles makes every run file written since its last fsync
// durable, with the lock released for the barriers.  Maintenance is
// one pass at a time, so nothing writes a run file meanwhile.  The
// caller holds the lock and gets it back.
func (p *PermStore) syncRunFiles() error {
	var dirty []*runFile
	for _, rf := range p.runs {
		if rf.unsynced {
			dirty = append(dirty, rf)
		}
	}
	if len(dirty) == 0 {
		return nil
	}
	p.mu.Unlock()
	var err error
	for _, rf := range dirty {
		if err = fsync(rf.f); err != nil {
			break
		}
	}
	p.mu.Lock()
	if err != nil {
		return err
	}
	for _, rf := range dirty {
		rf.unsynced = false
	}
	return nil
}

// commitManifest writes perm.json: encoded under the lock, written,
// fsynced and renamed with the lock released, so that a merge's
// manifest commit -- every twenty blocks, per shard -- is not a
// barrier the shard's puts and seal wait behind (spec 1.6).  A delta
// the seal appends meanwhile lies after the offset the manifest
// records, and open replays it.  The caller holds the lock and gets
// it back.
func (p *PermStore) commitManifest() error {
	buf, err := p.encodeManifest()
	if err != nil {
		return err
	}
	p.mu.Unlock()
	err = p.writeManifest(buf)
	p.mu.Lock()
	if err == nil {
		p.manifestAt, p.manifestDirty = p.height, false
	}
	return err
}

// encodeManifest is the manifest as of now.  The caller holds the lock.
func (p *PermStore) encodeManifest() ([]byte, error) {
	m := permManifest{Version: 1, Height: p.height, FilterBlocks: p.window_n, Rotation: p.rotation, NextData: p.nextID, NextRun: p.nextRun}
	ref := func(r *permRun, rf *runFile, height uint64) permRunRef {
		return permRunRef{File: rf.id, Off: r.off, Height: height}
	}
	for _, d := range p.window {
		m.Window = append(m.Window, permRunRef{File: d.data, Off: d.run.off, Height: d.height})
	}
	for _, d := range p.pending {
		m.Pending = append(m.Pending, permRunRef{File: d.data, Off: d.run.off, Height: d.height})
	}
	for b := range p.buckets {
		bm := permBucketM{Merged: p.buckets[b].merged}
		for i, r := range p.buckets[b].runs {
			bm.Runs = append(bm.Runs, ref(r, p.buckets[b].files[i], 0))
		}
		m.Buckets = append(m.Buckets, bm)
	}
	for _, r := range p.retired {
		m.Retired = append(m.Retired, permRunRef{File: r.file, Off: r.off, Height: r.height})
	}
	for id := range p.files {
		m.DataFiles = append(m.DataFiles, id)
	}
	// Only the run files kept: a file nothing references is dropped
	// once this manifest is durable, and a durable manifest must never
	// name a file that is gone (1.7, 1.8)
	for id, rf := range p.runs {
		if rf.refs > 0 || rf == p.maintRun {
			m.RunFiles = append(m.RunFiles, id)
		}
	}
	sort.Slice(m.DataFiles, func(i, j int) bool { return m.DataFiles[i] < m.DataFiles[j] })
	sort.Slice(m.RunFiles, func(i, j int) bool { return m.RunFiles[i] < m.RunFiles[j] })
	// Deltas sealed after this point lie in the data files from the
	// current one's end; open replays from there
	m.DeltasFile, m.DeltasOff = p.cur.id, p.cur.size
	p.deltasFrom.file, p.deltasFrom.off = m.DeltasFile, m.DeltasOff
	return json.MarshalIndent(m, "", " ")
}

// writeManifest writes the encoded manifest aside, fsyncs it, renames
// it into place and fsyncs the directory.  No lock is held.
func (p *PermStore) writeManifest(buf []byte) error {
	path := filepath.Join(p.Directory, "perm.json")
	tmp := path + segTmpSuffix
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err = fsync(f); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return fsyncDir(p.Directory)
}

// dropUnreferencedRunFiles deletes run files no run references, after
// the manifest that no longer names them is durable.  The caller
// holds the lock.
func (p *PermStore) dropUnreferencedRunFiles() error {
	for id, rf := range p.runs {
		if rf.refs > 0 || rf == p.maintRun {
			continue
		}
		rf.f.Close()
		delete(p.runs, id)
		if err := os.Remove(filepath.Join(p.Directory, rf.name())); err != nil {
			return err
		}
	}
	return nil
}

// Counters reports the maintenance so far: merge runs, folds, packs,
// and the index bytes written.
func (p *PermStore) Counters() (merges, folds, packs, indexBytes uint64) {
	return p.mergeRuns.Load(), p.foldRuns.Load(), p.packRuns.Load(), p.indexBytes.Load()
}

// Stats is the store's report.
func (p *PermStore) Stats() StoreStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var runs int
	for b := range p.buckets {
		runs += len(p.buckets[b].runs)
	}
	return StoreStats{
		PutTotal:        p.putTotal.Load(),
		PutNew:          p.putTotal.Load() - p.putDuplicate.Load(),
		PutDuplicate:    p.putDuplicate.Load(),
		LookupTotal:     p.lookups.Load(),
		LiveHit:         p.windowHits.Load(),
		ActiveSegments:  len(p.window),
		HistorySegments: runs + len(p.retired),
		HeapFiles:       len(p.files) + len(p.runs),
		HeapMovedBytes:  p.indexBytes.Load(),
	}
}

// beginPermSeal and mergeBelow are the permLayer surface (kv_2.go).
func (p *PermStore) beginPermSeal(height uint64) (blockSync, error) {
	s, err := p.beginSeal(height)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (p *PermStore) mergeBelow(uint64) (bool, error) { return true, p.Merge() }

// SetFilterBlocks sets the window.
func (p *PermStore) SetFilterBlocks(n uint64) error {
	if n < MinFilterBlocks {
		return fmt.Errorf("perm: filter blocks %d below the minimum %d", n, MinFilterBlocks)
	}
	p.mu.Lock()
	p.window_n = n
	p.mu.Unlock()
	return nil
}
