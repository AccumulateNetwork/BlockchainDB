package blockchainDB

import (
	"encoding/json"
	"errors"
	"fmt"
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
//	deltas-N.dat the seal's run file: a block's delta appended at every
//	             seal; open replays the deltas after the manifest from
//	             these files alone
//	runs-N.dat   maintenance's run file: a bucket's run at every merge,
//	             a retired run at every pack; never the seal's, so their
//	             barriers never share an inode; both kinds are rolled
//	             at PermFileBytes and deleted once no run in them is
//	             referenced
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
	curRun    *runFile            // The run file the seal appends deltas to
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
	syncMu sync.Mutex // Serializes seals with each other and with manifest commits
	closed bool

	putTotal, putDuplicate, lookups, windowHits, deepHits atomic.Uint64
	mergeRuns, foldRuns, packRuns                         atomic.Uint64
	indexBytes                                            atomic.Uint64
}

// PermBuckets is how many buckets a shard's history above the
// watermark is kept in, by the key's first byte.
const PermBuckets = 256

// PermMergeEvery is how many blocks pass between merges of one
// bucket: PermBuckets/PermMergeEvery buckets are merged each block.
var PermMergeEvery uint64 = 256

// PermFileBytes is the size data files and run files are rolled at.
var PermFileBytes int64 = 64 << 20

// PermFoldRatio is the ratio a bucket's runs fold by: a suffix of runs
// folds while each older run is no larger than 1/ratio of what has
// gathered behind it.
var PermFoldRatio = 0.25

type permDelta struct {
	height uint64
	run    *permRun
	rf     *runFile
}

type permBucket struct {
	runs   []*permRun // Oldest first
	files  []*runFile
	merged uint64 // The height of the newest delta merged in
}

// runFile is a file of runs and how many runs still reference it.
type runFile struct {
	id     uint32
	f      *os.File
	size   int64
	refs   int
	deltas bool // The seal's, replayed on open; else maintenance's
}

func permDataName(id uint32) string  { return fmt.Sprintf("perm-%06d.dat", id) }
func permRunName(id uint32) string   { return fmt.Sprintf("runs-%06d.dat", id) }
func permDeltaName(id uint32) string { return fmt.Sprintf("deltas-%06d.dat", id) }

func (rf *runFile) name() string {
	if rf.deltas {
		return permDeltaName(rf.id)
	}
	return permRunName(rf.id)
}

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
	DeltaFiles   []uint32      `json:"deltaFiles"`
}

type permRunRef struct {
	File   uint32 `json:"file"`
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
		if p.curRun, err = p.newRunFile(true); err != nil {
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
	// Delta files: the manifest's, and any the seal created after it
	deltaIDs := append([]uint32(nil), m.DeltaFiles...)
	if entries, err := os.ReadDir(p.Directory); err == nil {
		for _, e := range entries {
			var id uint32
			if n, _ := fmt.Sscanf(e.Name(), "deltas-%06d.dat", &id); n == 1 && id > m.NextRun-1 {
				deltaIDs = append(deltaIDs, id)
			}
		}
	}
	for _, id := range deltaIDs {
		if p.runs[id] != nil {
			continue
		}
		f, err := os.OpenFile(filepath.Join(p.Directory, permDeltaName(id)), os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("perm: delta file %s: %w", permDeltaName(id), err)
		}
		st, _ := f.Stat()
		p.runs[id] = &runFile{id: id, f: f, size: st.Size(), deltas: true}
		if id >= p.nextRun {
			p.nextRun = id + 1
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
	for _, ref := range m.Window {
		r, rf, err := load(ref, true)
		if err != nil {
			return err
		}
		p.window = append(p.window, &permDelta{height: ref.Height, run: r, rf: rf})
	}
	for _, ref := range m.Pending {
		r, rf, err := load(ref, true)
		if err != nil {
			return err
		}
		p.pending = append(p.pending, &permDelta{height: ref.Height, run: r, rf: rf})
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
	p.curRun = p.runs[m.DeltasFile]
	if p.curRun == nil {
		if p.curRun, err = p.newRunFile(true); err != nil {
			return err
		}
	}
	p.deltasFrom.file, p.deltasFrom.off = m.DeltasFile, m.DeltasOff
	return p.replayDeltas()
}

// replayDeltas reads every whole delta after the manifest's offset
// into the window (and pending), cutting a torn tail.  The caller
// holds the lock.
func (p *PermStore) replayDeltas() error {
	ids := make([]uint32, 0, len(p.runs))
	for id, rf := range p.runs {
		if rf.deltas && id >= p.deltasFrom.file {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		rf := p.runs[id]
		off := int64(0)
		if id == p.deltasFrom.file {
			off = p.deltasFrom.off
		}
		for off < rf.size {
			r, err := openPermRun(rf.f, id, off, true)
			if err != nil {
				// Torn: cut the file here
				if err := rf.f.Truncate(off); err != nil {
					return err
				}
				rf.size = off
				break
			}
			rf.refs++
			p.admit(&permDelta{height: r.height, run: r, rf: rf})
			if r.height > p.height {
				p.height = r.height
			}
			off += int64(r.bytes)
		}
		// The newest delta file is the seal's current one
		if p.curRun == nil || rf.id > p.curRun.id {
			p.curRun = rf
		}
	}
	return nil
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

func (p *PermStore) newRunFile(deltas bool) (*runFile, error) {
	id := p.nextRun
	p.nextRun++
	rf := &runFile{id: id, deltas: deltas}
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
	p.files, p.runs, p.cur, p.curRun = nil, nil, nil, nil
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
		rec, found, err := d.run.lookup(d.rf.f, key)
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
		rec, found, err := d.run.lookup(d.rf.f, key)
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
		rec, found, err := d.run.lookup(d.rf.f, key)
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
	sortPermRecords(s.recs)
	return s, nil
}

// finish: entries durable, then the delta appended and durable, then
// the delta admitted to the window and the live map cleared.
func (s *permSeal) finish() error {
	p := s.p
	defer p.syncMu.Unlock()
	for _, hf := range s.dirty {
		if err := fsync(hf.f); err != nil {
			return err
		}
	}
	if len(s.recs) == 0 {
		p.mu.Lock()
		if s.height >= p.height {
			p.height = s.height + 1
		}
		p.mu.Unlock()
		return nil
	}
	p.mu.Lock()
	rf := p.curRun
	if rf.size > PermFileBytes {
		var err error
		if rf, err = p.newRunFile(true); err != nil {
			p.mu.Unlock()
			return err
		}
		p.curRun = rf
	}
	at := rf.size
	p.mu.Unlock()
	run, err := writePermRun(rf.f, at, rf.id, s.recs, s.height)
	if err != nil {
		return err
	}
	if err = fsync(rf.f); err != nil {
		return err
	}
	p.indexBytes.Add(uint64(run.bytes))
	p.mu.Lock()
	defer p.mu.Unlock()
	rf.size = at + int64(run.bytes)
	rf.refs++
	p.admit(&permDelta{height: s.height, run: run, rf: rf})
	p.live = map[[32]byte]permRecord{}
	if s.height >= p.height {
		p.height = s.height + 1
	}
	return nil
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
		rf, err := p.newRunFile(false)
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
	// runs are immutable
	pendingCopy := append([]*permDelta(nil), p.pending...)
	p.mu.Unlock()
	byBucket := map[int][][]permRecord{}
	for _, d := range pendingCopy {
		recs, err := d.run.records(d.rf.f)
		if err != nil {
			return err
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
	if err := fsync(rf.f); err != nil {
		return err
	}
	// Swap: the new runs join their buckets; then fold what the ratio
	// says, one bucket at a time
	p.mu.Lock()
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
		if d.height <= minMerged {
			d.rf.refs--
		} else {
			keep = append(keep, d)
		}
	}
	p.pending = keep
	folds := p.planFolds()
	p.mu.Unlock()
	// The folds' runs are written without a barrier each and synced
	// once, before the manifest that names them
	var synced []*runFile
	for _, f := range folds {
		rf, err := p.fold(f)
		if err != nil {
			return err
		}
		if rf != nil && (len(synced) == 0 || synced[len(synced)-1] != rf) {
			synced = append(synced, rf)
		}
	}
	for _, rf := range synced {
		if err := fsync(rf.f); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
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
		recs, err := d.run.records(d.rf.f)
		if err != nil {
			p.mu.RUnlock()
			return err
		}
		inputs = append(inputs, recs)
		release = append(release, d.rf)
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
	if err := p.commitManifest(); err != nil {
		return err
	}
	return p.dropUnreferencedRunFiles()
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
	return err
}

// encodeManifest is the manifest as of now.  The caller holds the lock.
func (p *PermStore) encodeManifest() ([]byte, error) {
	m := permManifest{Version: 1, Height: p.height, FilterBlocks: p.window_n, Rotation: p.rotation, NextData: p.nextID, NextRun: p.nextRun}
	ref := func(r *permRun, rf *runFile, height uint64) permRunRef {
		return permRunRef{File: rf.id, Off: r.off, Height: height}
	}
	for _, d := range p.window {
		m.Window = append(m.Window, ref(d.run, d.rf, d.height))
	}
	for _, d := range p.pending {
		m.Pending = append(m.Pending, ref(d.run, d.rf, d.height))
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
	for id, rf := range p.runs {
		if rf.deltas {
			m.DeltaFiles = append(m.DeltaFiles, id)
		} else {
			m.RunFiles = append(m.RunFiles, id)
		}
	}
	sort.Slice(m.DataFiles, func(i, j int) bool { return m.DataFiles[i] < m.DataFiles[j] })
	sort.Slice(m.RunFiles, func(i, j int) bool { return m.RunFiles[i] < m.RunFiles[j] })
	sort.Slice(m.DeltaFiles, func(i, j int) bool { return m.DeltaFiles[i] < m.DeltaFiles[j] })
	// Deltas sealed after this point append to the current delta file
	// from its end; open replays from there
	m.DeltasFile, m.DeltasOff = p.curRun.id, p.curRun.size
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
		if rf.refs > 0 || rf == p.curRun || rf == p.maintRun {
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
