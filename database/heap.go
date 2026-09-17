package blockchainDB

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// HeapStore is the dynamic layer as files of entries and a map of keys
// (proposal docs/proposals/2026-09-16-entries-written-once.md): an
// entry is written once at the end of a file, a key is (file, offset,
// length), and space comes back by deleting a file once nothing in it
// is live -- never by rewriting an index or a filter.
//
// Files, in one directory:
//
//	heap-N.dat    entries, [len u32][height u64][key 32][value][crc32
//	              of height+key+value], 8-byte aligned, appended in the
//	              order written.  A block appends to the current data
//	              file; the mover appends to a file of its own, so the
//	              two never share a barrier.  A file is rolled at
//	              HeapFileBytes and deleted once nothing in it is live.
//	index-G.log   generation G of the key map: a snapshot record of the
//	              whole map and the point in the data files from which
//	              the deltas after it are replayed.  A snapshot starts a
//	              new generation in a file of its own, switched to by
//	              rename.
//
// A block's delta is appended to the block's data file right behind
// the block's entries, as an entry under a reserved key, so a block is
// ONE fsync per shard (entries and delta together) rather than two
// (measured: the second barrier was a third of the sync's cost, and
// the barrier count is what the device queue charges for).  The delta
// names no key: a block's entries are contiguous in its file and each
// carries its key, so the delta is the RANGES the block wrote -- its
// own appends, and the mover's copies named this block -- and the few
// copies that arrived dead, a few dozen bytes a block (a delta of
// 44-byte records was a fifth of the heap's writes, all of it dead at
// the next snapshot and churned by the mover).  Replay scans the
// ranges in order, later naming winning, and the last delta is
// trusted only if every entry in its ranges checks; an earlier delta
// was followed by a later block's fsync, which covers it.
//
// The key map is in memory for the live key set (spec 1.2: memory that
// scales with the working set).  Open loads the newest whole
// generation's snapshot, replays the deltas in the data files after
// its replay point, and derives every file's live and dead bytes; a
// data file nothing names is deleted then, and one that is missing
// while named is an error.
//
// Durability (spec 1.8).  The block sync fsyncs the data files the
// block wrote and then appends and fsyncs the block's delta, so an
// index entry is durable only after the entry it names is.  A slot the
// last durable index names is never overwritten: a key rewritten in a
// later block takes a new slot, and its old slot is dead where it lies
// until its file is deleted -- and a file is deleted only after the
// delta naming the mover's copies out of it is durable, one sync late,
// the way spec 2.6 defers deletion (never unlink what a durable index
// names).  A key rewritten again within the same block reuses the
// slot it took this block, since nothing durable names it yet.  A
// crash therefore leaves every durable entry intact and every torn
// slot unnamed; the checksum catches a torn slot a stale index could
// name; and RepairHeapStore rebuilds the map from the data alone,
// since every entry carries its key and the height that wrote it.
type HeapStore struct {
	Directory string

	mu     sync.RWMutex
	files  map[uint32]*heapFile
	cur    *heapFile // The file the block appends to
	mov    *heapFile // The file the mover appends to
	nextID uint32
	index  map[[32]byte]slot
	height uint64 // The block being written; slots taken in it may be rewritten in place

	// The block's delta in the making: the ranges the block appended
	// (the mover's named copies first, then the block's own) and the
	// copies that arrived dead; dirty the files written since the last
	// sync; release the files the mover emptied, deleted once the
	// delta naming their copies is durable.
	ranges   []heapRange
	excluded []heapRange
	dirty    map[uint32]*heapFile
	release  []uint32
	// deltaAt is where the last delta ended: the replay point a
	// snapshot records, and what a file is cut back to on open
	deltaAt struct {
		file uint32
		off  int64
	}

	// syncMu serializes block syncs with each other and with a
	// snapshot, so the map a snapshot writes is exactly the state of
	// the last finished delta and no delta is in flight into a log
	// about to be retired.
	syncMu sync.Mutex
	log    *os.File
	gen    uint64
	snapAt uint64 // The block the key map was last snapshotted at

	closed    bool
	liveBytes int64
	deadBytes int64
	bound     bool // The size bound engaged: the mover takes files below the ratio

	putTotal, putInPlace, putAppend atomic.Uint64
	lookups, hits                   atomic.Uint64
	cleanedBytes, movedBytes        atomic.Uint64
	// The sync's cost, split: nanoseconds in the data files' fsyncs
	// and in the delta's write and fsync, and the syncs and bytes
	syncs, syncHeapNs, syncLogNs, syncBytes atomic.Uint64
	// The mover's cost on and off the block's path: files unlinked by
	// a block's finish and the time in those unlinks; snapshots
	// written and the time in them
	releases, releaseNs, snapshotsN, snapshotNs atomic.Uint64
	unlink                                      []uint32 // Released files not yet deleted
}

// heapFile is one data file and its accounting.
type heapFile struct {
	id         uint32
	f          *os.File
	size       int64
	live, dead int64
	deltas     int64  // Bytes of delta entries not yet superseded by a snapshot: live until then
	firstBlock uint64 // The block that first appended to it
	cleaning   bool   // Taken by the pass in progress
	inflight   int    // Copies reserved in it and not yet written: its size runs ahead of its bytes
	releasing  bool   // Emptied by a pass; deleted by the next sync
}

// heapRange is a stretch of one data file: what a delta names.
type heapRange struct {
	file     uint32
	from, to uint32
}

// slot is where an entry lives: its file, its offset there, the value
// length, and the block that took it, which decides whether a rewrite
// may reuse it in place.
type slot struct {
	file  uint32
	off   uint32
	n     uint32
	block uint64
}

const (
	heapHeader   = 4 + 8 + 32        // len, height, key
	heapTrailer  = 4                 // crc32 of height+key+value
	heapAlign    = 8                 // Entries start on an 8-byte boundary
	heapMagic    = 0x48454150        // "HEAP", an index record's marker
	heapSnapshot = 0x50414E53        // "SNAP", the record that starts a generation
	heapIndexHdr = 4 + 8 + 4 + 4 + 4 // magic, height, count, replay file, replay offset
	heapIndexRec = 32 + 4 + 4 + 4
)

// heapDeltaKey is the reserved key a delta entry is written under in
// a data file.  A real key is a hash; the odds of one being all ones
// are those of a hash collision.
var heapDeltaKey = [32]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// HeapFileBytes is the size a data file is rolled at, and
// HeapFileBlocks the age: a shard that appends little per block (many
// shards, or a quiet one) would otherwise keep one file current for
// long stretches, and the current file is never the mover's, so its
// dead bytes could not be reclaimed (measured at 64 shards: 34 KB a
// block, no file rolled in three minutes, the store growing 2.3 GB a
// minute with the mover idle).
var HeapFileBytes int64 = 16 << 20

var HeapFileBlocks uint64 = 64

// HeapCleanBytes bounds one mover pass by the bytes it copies.  The
// pass syncs its own copies, so the bound is not about a block's
// barrier but about the device queue the barrier shares: 16 MB
// passes put the seal's p90 at 150-250 ms in the minutes they ran
// (run 7), 4 MB releases more than the soak appends per shard per
// cadence (~4 MB, of which a quarter to a third is live).
var HeapCleanBytes int64 = 4 << 20

// HeapStoreCleanBytes is a store's mover budget per pass, shared out
// among its shards: a store sharded eight ways moves the same bytes a
// pass as one sharded once.  Measured: with one shard and the
// per-shard budget alone the store grew to 6 GB in five minutes
// where eight shards held it under 4.
var HeapStoreCleanBytes int64 = 32 << 20

// HeapCleanFiles and HeapScanBytes bound a pass by the files it takes
// and the bytes it reads: with hot keys most of a file is dead and
// costs nothing to copy, so what limits the mover's pace is how much
// it looks at.  At the soak's rate a shard makes ~50 MB of dead bytes
// per cadence; a pass that scans 128 MB releases more than that with
// room to spare (four 16 MB files did not, and the store floated with
// the size bound engaged).
var (
	HeapCleanFiles       = 8
	HeapScanBytes  int64 = 128 << 20
)

// HeapCleanRatio is the dead fraction a file must reach before the
// mover takes it -- unless the heap is over its size bound, when the
// deadest file is taken at HeapCleanFloor or more.  The bound has
// hysteresis: it engages when dead bytes exceed live by
// HeapBoundOn and releases when they fall below live again, so the
// movers of nine stores in lockstep do not all engage on the same
// block and then all disengage (measured: p90 461-704 ms in the
// minutes the bound flipped, 70-90 ms otherwise).
var (
	HeapCleanRatio = 0.5
	HeapCleanFloor = 0.25
	HeapBoundOn    = 1.5
)

// HeapSnapshotBlocks is how many blocks pass between key-map
// snapshots; between them the generation's deltas are what open
// replays.  Counted in blocks, not maintenance calls, so the cadence
// of the caller does not set the cadence of the snapshots.
var HeapSnapshotBlocks uint64 = 100

// HeapSnapshotPinnedFiles is how many files' worth of bytes may be
// held only by their deltas before a snapshot is taken early,
// whatever the block count.
var HeapSnapshotPinnedFiles int64 = 2

// HeapCleanPeriod is the blocks over which a store spends
// HeapStoreCleanBytes: the store's mover rate is the one divided by
// the other, and a maintenance call moves what the blocks since the
// last call earned (KVShard.Compress).
var HeapCleanPeriod uint64 = 20

// entrySize is the bytes an entry of n value bytes takes, aligned.
func entrySize(n int) int64 {
	return (int64(heapHeader+n+heapTrailer) + heapAlign - 1) &^ (heapAlign - 1)
}

func dataName(id uint32) string   { return fmt.Sprintf("heap-%06d.dat", id) }
func indexName(gen uint64) string { return fmt.Sprintf("index-%06d.log", gen) }

// NewHeapStore creates an empty heap in directory, replacing anything
// there.
func NewHeapStore(directory string) (*HeapStore, error) {
	os.RemoveAll(directory)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	h := &HeapStore{Directory: directory}
	return h, h.Open()
}

// OpenHeapStore opens the heap in directory as it was left.  A heap
// with data files but no index generation is refused: that is what
// RepairHeapStore is for, and it needs the committed height, which
// only the store above knows.
func OpenHeapStore(directory string) (*HeapStore, error) {
	dataIDs, gens, err := listHeap(directory)
	if err != nil {
		return nil, fmt.Errorf("open heap at %s: %w", directory, err)
	}
	if len(dataIDs) > 0 && len(gens) == 0 {
		return nil, fmt.Errorf("open heap at %s: %w", directory, ErrHeapNeedsRepair)
	}
	h := &HeapStore{Directory: directory}
	return h, h.Open()
}

// ErrHeapNeedsRepair says the heap's data is there and its index is
// not: RepairHeapStore rebuilds the index from the data.
var ErrHeapNeedsRepair = errors.New("heap has data but no index; repair it")

// listHeap names the data files and the whole index generations in a
// directory, in order.  A generation still being written (.tmp) is
// not whole and is removed.
func listHeap(directory string) (dataIDs []uint32, gens []uint64, err error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasPrefix(name, "heap-") && strings.HasSuffix(name, ".dat"):
			id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "heap-"), ".dat"), 10, 32)
			if err == nil {
				dataIDs = append(dataIDs, uint32(id))
			}
		case strings.HasPrefix(name, "index-") && strings.HasSuffix(name, ".log"):
			g, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "index-"), ".log"), 10, 64)
			if err == nil {
				gens = append(gens, g)
			}
		case strings.HasSuffix(name, segTmpSuffix):
			os.Remove(filepath.Join(directory, name))
		}
	}
	sort.Slice(dataIDs, func(i, j int) bool { return dataIDs[i] < dataIDs[j] })
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	return dataIDs, gens, nil
}

// Open loads the key map from the newest generation, opens the data
// files and derives their accounting.  Idempotent.
func (h *HeapStore) Open() (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.files != nil {
		return nil
	}
	dataIDs, gens, err := listHeap(h.Directory)
	if err != nil {
		return err
	}
	h.index = map[[32]byte]slot{}
	h.dirty = map[uint32]*heapFile{}
	h.files = map[uint32]*heapFile{}
	h.release, h.ranges, h.excluded = nil, nil, nil
	h.closed = false
	h.liveBytes, h.deadBytes = 0, 0
	// The newest whole generation is the index; older ones are what a
	// snapshot left behind when it could not delete them
	if len(gens) > 0 {
		h.gen = gens[len(gens)-1]
		for _, g := range gens[:len(gens)-1] {
			os.Remove(filepath.Join(h.Directory, indexName(g)))
		}
		if err = h.replayGeneration(); err != nil {
			return err
		}
	} else {
		h.gen = 1
		// A fresh store: the first generation, written with the lock
		// released since startGeneration takes it itself
		h.mu.Unlock()
		err = h.startGeneration()
		h.mu.Lock()
		if err != nil {
			return err
		}
	}
	for _, id := range dataIDs {
		if id >= h.nextID {
			h.nextID = id + 1
		}
	}
	if err = h.openFiles(dataIDs); err != nil {
		return err
	}
	if err = h.replayDeltas(dataIDs); err != nil {
		return err
	}
	return h.deriveFiles(dataIDs)
}

// openFiles opens every data file present.  The caller holds the lock.
func (h *HeapStore) openFiles(dataIDs []uint32) error {
	for _, id := range dataIDs {
		f, err := os.OpenFile(filepath.Join(h.Directory, dataName(id)), os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		h.files[id] = &heapFile{id: id, f: f, size: st.Size()}
	}
	return nil
}

// replayGeneration loads the current generation's snapshot; the
// deltas after it are in the data files and replayDeltas applies
// them.  The caller holds the lock.
func (h *HeapStore) replayGeneration() (err error) {
	path := filepath.Join(h.Directory, indexName(h.gen))
	if h.log, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return err
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	n, err := h.applyRecord(buf)
	if err != nil {
		return fmt.Errorf("heap: generation %d: %w", h.gen, err)
	}
	if n != len(buf) {
		return fmt.Errorf("heap: generation %d has %d trailing bytes", h.gen, len(buf)-n)
	}
	return nil
}

// replayDeltas applies every delta entry in the data files after the
// snapshot's replay point, in file and offset order.  The last delta
// is trusted only if every entry it names checks: one fsync covered
// it and its entries together, and a crash inside that fsync can
// leave the delta durable and an entry torn.  An earlier delta was
// followed by a later block's fsync.  The caller holds the lock; the
// files are open.
func (h *HeapStore) replayDeltas(dataIDs []uint32) error {
	type found struct {
		file uint32
		off  int64
		end  int64
		recs []byte
	}
	var deltas []found
	for _, id := range dataIDs {
		if id < h.deltaAt.file {
			continue
		}
		hf := h.files[id]
		if hf == nil {
			continue
		}
		buf := make([]byte, hf.size)
		if _, err := hf.f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		at := int64(0)
		if id == h.deltaAt.file {
			at = h.deltaAt.off
		}
		for at < int64(len(buf)) {
			size, _, key, value, ok := decodeEntry(buf[at:])
			if size == 0 {
				break
			}
			if ok && key == heapDeltaKey {
				deltas = append(deltas, found{file: id, off: at, end: at + size, recs: append([]byte(nil), value...)})
			}
			at += size // A damaged entry is stepped over; if it is named, Get reports it
		}
	}
	for i, d := range deltas {
		if !h.applyDelta(d.recs, i == len(deltas)-1) {
			break // Torn, or the last block's entries do not all check: it did not commit
		}
		h.deltaAt.file, h.deltaAt.off = d.file, d.end
		h.files[d.file].deltas += d.end - d.off
	}
	return nil
}

// startGeneration begins an index generation: a snapshot of the map
// with the replay point -- where the last delta ended -- written
// aside and fsynced, renamed into place, the directory fsynced, and
// the previous generation's file removed once the new one is durable.
// The map and the point are taken together under the lock; a delta
// the seal appends after that lies past the point and is replayed
// over the snapshot on open, which is idempotent.  The snapshot's
// write holds no lock (measured: a 3 MB snapshot under syncMu put
// the seal's p90 at 276 ms every hundredth block).  A delta's bytes
// before the point are dead once the generation is durable.
func (h *HeapStore) startGeneration() error {
	next := h.gen
	if h.log != nil {
		next = h.gen + 1 // A snapshot starts the generation after the current one
	}
	path := filepath.Join(h.Directory, indexName(next))
	tmp := path + segTmpSuffix
	h.mu.Lock()
	all := func(emit func(key [32]byte)) {
		for key := range h.index {
			emit(key)
		}
	}
	snap := h.encodeIndexOf(heapSnapshot, all, len(h.index))
	superseded := map[uint32]int64{}
	for id, hf := range h.files {
		superseded[id] = hf.deltas
	}
	h.mu.Unlock()
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(snap); err != nil {
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
	if err = fsyncDir(h.Directory); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	old := h.log
	if h.log, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return err
	}
	if old != nil {
		old.Close()
		os.Remove(filepath.Join(h.Directory, indexName(h.gen)))
	}
	h.gen = next
	// The deltas the snapshot covers are dead where they lie
	for id, n := range superseded {
		if hf := h.files[id]; hf != nil && n > 0 {
			hf.deltas -= n
			hf.live -= n
			hf.dead += n
			h.liveBytes -= n
			h.deadBytes += n
		}
	}
	return nil
}

// fsyncDir makes a directory's entries durable: a rename or an unlink
// is not on disk until the directory is.
func fsyncDir(directory string) error {
	d, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer d.Close()
	return fsync(d)
}

// encodeDelta is the block's delta: marker, height, the ranges the
// block wrote (the mover's named copies first, then the block's own
// appends, so a later write of a key wins on replay) and the copies
// that arrived dead, checksummed.  The caller holds the lock.
func (h *HeapStore) encodeDelta() []byte {
	const rangeRec = 4 + 4 + 4
	buf := make([]byte, 4+8+4+4+len(h.ranges)*rangeRec+len(h.excluded)*rangeRec+4)
	binary.LittleEndian.PutUint32(buf, heapMagic)
	binary.LittleEndian.PutUint64(buf[4:], h.height)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(h.ranges)))
	binary.LittleEndian.PutUint32(buf[16:], uint32(len(h.excluded)))
	at := 20
	for _, r := range append(append([]heapRange(nil), h.ranges...), h.excluded...) {
		binary.LittleEndian.PutUint32(buf[at:], r.file)
		binary.LittleEndian.PutUint32(buf[at+4:], r.from)
		binary.LittleEndian.PutUint32(buf[at+8:], r.to)
		at += rangeRec
	}
	binary.LittleEndian.PutUint32(buf[at:], crc32.ChecksumIEEE(buf[:at]))
	return buf
}

// decodeDelta reads a delta's ranges and exclusions.
func decodeDelta(buf []byte) (height uint64, ranges, excluded []heapRange, ok bool) {
	const rangeRec = 4 + 4 + 4
	if len(buf) < 20 || binary.LittleEndian.Uint32(buf) != heapMagic {
		return
	}
	nr, ne := int(binary.LittleEndian.Uint32(buf[12:])), int(binary.LittleEndian.Uint32(buf[16:]))
	end := 20 + (nr+ne)*rangeRec
	if len(buf) < end+4 || crc32.ChecksumIEEE(buf[:end]) != binary.LittleEndian.Uint32(buf[end:]) {
		return
	}
	height = binary.LittleEndian.Uint64(buf[4:])
	at := 20
	for i := 0; i < nr+ne; i++ {
		r := heapRange{file: binary.LittleEndian.Uint32(buf[at:]), from: binary.LittleEndian.Uint32(buf[at+4:]), to: binary.LittleEndian.Uint32(buf[at+8:])}
		if i < nr {
			ranges = append(ranges, r)
		} else {
			excluded = append(excluded, r)
		}
		at += rangeRec
	}
	return height, ranges, excluded, true
}

// applyDelta names every entry in the delta's ranges, in order, later
// naming winning, except the excluded copies; verify makes it refuse
// a delta any of whose entries does not check.  The caller holds the
// lock; the files are open.
func (h *HeapStore) applyDelta(recs []byte, verify bool) bool {
	height, ranges, excluded, ok := decodeDelta(recs)
	if !ok {
		return false
	}
	skip := map[heapRange]bool{}
	for _, e := range excluded {
		skip[heapRange{file: e.file, from: e.from}] = true
	}
	type naming struct {
		key [32]byte
		s   slot
	}
	var names []naming
	for _, r := range ranges {
		hf := h.files[r.file]
		if hf == nil || int64(r.to) > hf.size {
			return false
		}
		buf := make([]byte, r.to-r.from)
		if _, err := hf.f.ReadAt(buf, int64(r.from)); err != nil {
			return false
		}
		at := int64(0)
		for at < int64(len(buf)) {
			size, _, key, value, ok := decodeEntry(buf[at:])
			if size == 0 || !ok && verify {
				return false // Torn; or the last block's entries do not all check
			}
			off := int64(r.from) + at
			// A damaged entry of a committed block is named all the
			// same, so a read of it reports the damage rather than
			// answering that the key is absent
			n := uint32(len(value))
			if !ok {
				n = binary.LittleEndian.Uint32(buf[at:])
			}
			if key != heapDeltaKey && !skip[heapRange{file: r.file, from: uint32(off)}] {
				names = append(names, naming{key: key, s: slot{file: r.file, off: uint32(off), n: n}})
			}
			at += size
		}
	}
	for _, n := range names {
		h.index[n.key] = n.s
	}
	if height > h.height {
		h.height = height
	}
	return true
}

func (h *HeapStore) encodeIndexOf(magic uint32, each func(emit func(key [32]byte)), count int) []byte {
	buf := make([]byte, heapIndexHdr+count*heapIndexRec+4)
	binary.LittleEndian.PutUint32(buf, magic)
	binary.LittleEndian.PutUint64(buf[4:], h.height)
	binary.LittleEndian.PutUint32(buf[12:], uint32(count))
	binary.LittleEndian.PutUint32(buf[16:], h.deltaAt.file)
	binary.LittleEndian.PutUint32(buf[20:], uint32(h.deltaAt.off))
	at := heapIndexHdr
	each(func(key [32]byte) {
		s := h.index[key]
		copy(buf[at:], key[:])
		binary.LittleEndian.PutUint32(buf[at+32:], s.file)
		binary.LittleEndian.PutUint32(buf[at+36:], s.off)
		binary.LittleEndian.PutUint32(buf[at+40:], s.n)
		at += heapIndexRec
	})
	binary.LittleEndian.PutUint32(buf[at:], crc32.ChecksumIEEE(buf[:at]))
	return buf
}

var errHeapTorn = errors.New("heap: torn index record")

// applyRecord applies one index record and returns its length.
func (h *HeapStore) applyRecord(buf []byte) (int, error) {
	if len(buf) < heapIndexHdr {
		return 0, errHeapTorn
	}
	magic := binary.LittleEndian.Uint32(buf)
	if magic != heapSnapshot {
		return 0, errHeapTorn
	}
	count := int(binary.LittleEndian.Uint32(buf[12:]))
	end := heapIndexHdr + count*heapIndexRec
	if len(buf) < end+4 || crc32.ChecksumIEEE(buf[:end]) != binary.LittleEndian.Uint32(buf[end:]) {
		return 0, errHeapTorn
	}
	if height := binary.LittleEndian.Uint64(buf[4:]); height > h.height {
		h.height = height
	}
	if magic == heapSnapshot {
		h.deltaAt.file = binary.LittleEndian.Uint32(buf[16:])
		h.deltaAt.off = int64(binary.LittleEndian.Uint32(buf[20:]))
	}
	for at := heapIndexHdr; at < end; at += heapIndexRec {
		var key [32]byte
		copy(key[:], buf[at:])
		h.index[key] = slot{file: binary.LittleEndian.Uint32(buf[at+32:]), off: binary.LittleEndian.Uint32(buf[at+36:]), n: binary.LittleEndian.Uint32(buf[at+40:])}
	}
	return end + 4, nil
}

// deriveFiles derives every open file's live and dead bytes, cuts
// the newest back to its last named slot or its last delta, and
// deletes any file the map does not name and no delta needed:
// nothing durable names it, and its bytes are a crash's or the
// mover's leftovers.  The caller holds the lock.
func (h *HeapStore) deriveFiles(dataIDs []uint32) error {
	named := map[uint32]int64{} // file -> end of its last named slot
	for _, s := range h.index {
		if e := int64(s.off) + entrySize(int(s.n)); e > named[s.file] {
			named[s.file] = e
		}
		h.liveBytes += entrySize(int(s.n))
	}
	for id := range named {
		if h.files[id] == nil {
			return fmt.Errorf("heap: the index names %s: missing", dataName(id))
		}
	}
	for _, id := range dataIDs {
		hf := h.files[id]
		end, live := named[id]
		if id == h.deltaAt.file && h.deltaAt.off > end {
			end, live = h.deltaAt.off, true
		}
		if !live && hf.deltas == 0 {
			hf.f.Close()
			delete(h.files, id)
			os.Remove(filepath.Join(h.Directory, dataName(id)))
			continue
		}
		if err := hf.f.Truncate(end); err != nil {
			return err
		}
		hf.size = end
	}
	for _, s := range h.index {
		h.files[s.file].live += entrySize(int(s.n))
	}
	for _, hf := range h.files {
		hf.live += hf.deltas
		h.liveBytes += hf.deltas
		hf.dead = hf.size - hf.live
		h.deadBytes += hf.dead
	}
	return nil
}

// Close syncs what is pending and closes the files.  Reopen with Open.
func (h *HeapStore) Close() error {
	p, err := h.beginBlockSync()
	if err != nil {
		if errors.Is(err, errStoreClosed) {
			return nil
		}
		return err
	}
	if err := p.finish(); err != nil {
		return err
	}
	if err := h.unlinkReleased(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for _, hf := range h.files {
		if cerr := hf.f.Close(); err == nil {
			err = cerr
		}
	}
	if cerr := h.log.Close(); err == nil {
		err = cerr
	}
	h.files, h.cur, h.mov, h.log = nil, nil, nil, nil
	return err
}

// encodeEntry lays out one entry: the block that wrote it is in the
// header so that, with the index gone, a scan can tell the current
// copy of a key (the highest height) from stale ones and a committed
// entry from one whose block never synced.
func encodeEntry(height uint64, key [32]byte, value []byte) []byte {
	buf := make([]byte, entrySize(len(value)))
	binary.LittleEndian.PutUint32(buf, uint32(len(value)))
	binary.LittleEndian.PutUint64(buf[4:], height)
	copy(buf[12:], key[:])
	copy(buf[heapHeader:], value)
	binary.LittleEndian.PutUint32(buf[heapHeader+len(value):], crc32.ChecksumIEEE(buf[4:heapHeader+len(value)]))
	return buf
}

// decodeEntry checks the entry at the start of buf and returns its
// fields and its aligned size; ok is false for an unwritten, torn or
// damaged slot.
func decodeEntry(buf []byte) (size int64, height uint64, key [32]byte, value []byte, ok bool) {
	if len(buf) < heapHeader {
		return
	}
	n := int(binary.LittleEndian.Uint32(buf))
	size = entrySize(n)
	if n == 0 && binary.LittleEndian.Uint64(buf[4:]) == 0 || size > int64(len(buf)) {
		return 0, 0, key, nil, false
	}
	copy(key[:], buf[12:])
	if crc32.ChecksumIEEE(buf[4:heapHeader+n]) != binary.LittleEndian.Uint32(buf[heapHeader+n:]) {
		return size, 0, key, nil, false // Damaged: its size lets a scan step over it, its key lets replay name it
	}
	height = binary.LittleEndian.Uint64(buf[4:])
	copy(key[:], buf[12:])
	return size, height, key, buf[heapHeader : heapHeader+n], true
}

// newFile opens the next data file.  The caller holds the lock.
func (h *HeapStore) newFile() (*heapFile, error) {
	id := h.nextID
	h.nextID++
	f, err := os.OpenFile(filepath.Join(h.Directory, dataName(id)), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	hf := &heapFile{id: id, f: f}
	h.files[id] = hf
	return hf, nil
}

// reserve takes size bytes at the end of the block's or the mover's
// file, rolling to a new file at HeapFileBytes.  The caller holds the
// lock.
func (h *HeapStore) reserve(mover bool, size int64) (hf *heapFile, off int64, err error) {
	at := &h.cur
	if mover {
		at = &h.mov
	}
	if *at == nil || (*at).size+size > HeapFileBytes || (!mover && h.height >= (*at).firstBlock+HeapFileBlocks) {
		if *at, err = h.newFile(); err != nil {
			return nil, 0, err
		}
		(*at).firstBlock = h.height
	}
	hf = *at
	off = hf.size
	hf.size += size
	hf.live += size
	h.liveBytes += size
	if mover {
		// The pass syncs its own file; the block's sync must never
		// capture it as dirty, or the block waits for the copies
		hf.inflight++
	} else {
		h.dirty[hf.id] = hf
		h.extendRange(hf.id, off, off+size)
	}
	return hf, off, nil
}

// extendRange adds [from, to) of a file to the block's delta,
// extending the last range when it abuts.  The caller holds the lock.
func (h *HeapStore) extendRange(file uint32, from, to int64) {
	if n := len(h.ranges); n > 0 && h.ranges[n-1].file == file && int64(h.ranges[n-1].to) == from {
		h.ranges[n-1].to = uint32(to)
		return
	}
	h.ranges = append(h.ranges, heapRange{file: file, from: uint32(from), to: uint32(to)})
}

// kill accounts a slot its key stopped naming.  The caller holds the
// lock.
func (h *HeapStore) kill(s slot) {
	size := entrySize(int(s.n))
	h.liveBytes -= size
	h.deadBytes += size
	hf := h.files[s.file]
	hf.live -= size
	hf.dead += size
}

// Put writes value under key: in place if the key took its slot this
// block and the entry keeps its size, else appended to the block's
// file.  The slot a durable index names is never rewritten.
func (h *HeapStore) Put(key [32]byte, value []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errStoreClosed
	}
	h.putTotal.Add(1)
	old, had := h.index[key]
	var s slot
	var hf *heapFile
	if had && old.block == h.height && entrySize(len(value)) == entrySize(int(old.n)) {
		// Taken this block, nothing durable names it, and the same
		// aligned size, so the file stays a contiguous sequence of
		// entries a scan can walk: rewrite in place
		s = old
		s.n = uint32(len(value))
		hf = h.files[s.file]
		h.dirty[hf.id] = hf
		h.putInPlace.Add(1)
	} else {
		var off int64
		var err error
		if hf, off, err = h.reserve(false, entrySize(len(value))); err != nil {
			return err
		}
		s = slot{file: hf.id, off: uint32(off), n: uint32(len(value)), block: h.height}
		if had {
			h.kill(old) // Dead where it lies
		}
		h.putAppend.Add(1)
	}
	if _, err := hf.f.WriteAt(encodeEntry(h.height, key, value), int64(s.off)); err != nil {
		return err
	}
	h.index[key] = s
	return nil
}

// Get answers from the key map and one read of the slot, taken
// outside the lock.  A slot whose checksum fails is reported as
// corrupt, never as a value.  A file deleted between the lookup and
// the read -- the mover moved the entry and a sync released the file
// meanwhile -- is looked up again.
func (h *HeapStore) Get(key [32]byte) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		h.mu.RLock()
		if h.closed {
			h.mu.RUnlock()
			return nil, errStoreClosed
		}
		h.lookups.Add(1)
		s, ok := h.index[key]
		var f *os.File
		if ok {
			f = h.files[s.file].f
		}
		h.mu.RUnlock()
		if !ok {
			return nil, errNotFound
		}
		h.hits.Add(1)
		buf := make([]byte, heapHeader+int(s.n)+heapTrailer)
		if _, err := f.ReadAt(buf, int64(s.off)); err != nil {
			if attempt == 0 && errors.Is(err, os.ErrClosed) {
				continue
			}
			return nil, err
		}
		return heapEntryValue(buf, key)
	}
}

// heapEntryValue checks an entry read from a slot and returns its
// value.
func heapEntryValue(buf []byte, key [32]byte) ([]byte, error) {
	n := int(binary.LittleEndian.Uint32(buf))
	if len(buf) != heapHeader+n+heapTrailer || [32]byte(buf[12:heapHeader]) != key {
		return nil, fmt.Errorf("heap: slot does not hold the key it is named for")
	}
	if crc32.ChecksumIEEE(buf[4:heapHeader+n]) != binary.LittleEndian.Uint32(buf[heapHeader+n:]) {
		return nil, fmt.Errorf("heap: entry checksum failed")
	}
	return append([]byte(nil), buf[heapHeader:heapHeader+n]...), nil
}

// GetDeep is Get: a heap has no history to reach into.
func (h *HeapStore) GetDeep(key [32]byte) ([]byte, error) { return h.Get(key) }

// AdvanceBlock sets the block new writes belong to.
func (h *HeapStore) AdvanceBlock(height uint64) {
	h.mu.Lock()
	h.height = height
	h.mu.Unlock()
}

// heapSync is a block sync in flight: the files to make durable, the
// delta, and the files to delete once it is.  It holds syncMu until
// finished.
type heapSync struct {
	h       *HeapStore
	dirty   []*heapFile
	delta   []byte
	release []uint32
	bytes   int64
}

// beginBlockSync takes the block's delta under the lock; finish makes
// it durable outside it.
func (h *HeapStore) beginBlockSync() (blockSync, error) {
	h.syncMu.Lock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.files == nil {
		h.syncMu.Unlock()
		return nil, errStoreClosed
	}
	p := &heapSync{h: h, release: h.release}
	h.release = nil
	if len(h.ranges) > 0 || len(p.release) > 0 {
		// The delta goes into the block's data file behind its entries,
		// under the reserved key, so the block's one fsync covers both
		p.delta = h.encodeDelta()
		h.ranges, h.excluded = nil, nil
		hf, off, err := h.reserve(false, entrySize(len(p.delta)))
		h.ranges = nil // The delta entry itself is not part of the block's range
		if err != nil {
			h.syncMu.Unlock()
			return nil, err
		}
		if _, err := hf.f.WriteAt(encodeEntry(h.height, heapDeltaKey, p.delta), off); err != nil {
			h.syncMu.Unlock()
			return nil, err
		}
		hf.deltas += entrySize(len(p.delta))
		h.deltaAt.file, h.deltaAt.off = hf.id, off+entrySize(len(p.delta))
	}
	for _, hf := range h.dirty {
		p.dirty = append(p.dirty, hf)
		p.bytes += hf.size
	}
	h.dirty = map[uint32]*heapFile{}
	return p, nil
}

// finish: entries durable, then the delta durable, then the files the
// delta's copies emptied are deleted.  Releases syncMu.
func (p *heapSync) finish() (err error) {
	h := p.h
	defer h.syncMu.Unlock()
	if p.delta == nil {
		return nil
	}
	t := time.Now()
	for _, hf := range p.dirty {
		if err = fsync(hf.f); err != nil {
			return err
		}
	}
	h.syncHeapNs.Add(uint64(time.Since(t)))
	h.syncs.Add(1)
	h.syncBytes.Add(uint64(p.bytes))
	h.mu.Lock()
	defer h.mu.Unlock()
	// The files the delta no longer names leave the map now; their
	// unlinks are the mover's, off the block's path.  Until then they
	// are unnamed files on disk, which an open deletes as such.
	for _, id := range p.release {
		hf := h.files[id]
		hf.f.Close()
		delete(h.files, id)
		h.deadBytes -= hf.dead
		h.unlink = append(h.unlink, id)
	}
	return nil
}

// unlinkReleased deletes the files the block syncs have released.
// Called without the lock.
func (h *HeapStore) unlinkReleased() error {
	h.mu.Lock()
	ids := h.unlink
	h.unlink = nil
	h.mu.Unlock()
	if len(ids) == 0 {
		return nil
	}
	t := time.Now()
	for _, id := range ids {
		if err := os.Remove(filepath.Join(h.Directory, dataName(id))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	h.releases.Add(uint64(len(ids)))
	h.releaseNs.Add(uint64(time.Since(t)))
	return nil
}

// compact is one slice of the heap's maintenance: the released
// files unlinked, a mover pass bounded by budget bytes copied, and
// every HeapSnapshotBlocks a new index generation, which bounds the
// replay on open.
func (h *HeapStore) compact(budget int64) (bool, error) {
	if err := h.unlinkReleased(); err != nil {
		return false, err
	}
	moved, err := h.clean(budget)
	if err != nil {
		return moved, err
	}
	// A snapshot is due by age, or when the files it would free
	// outweigh it: a file whose only live bytes are deltas cannot go
	// until a snapshot supersedes them, so the space those files hold
	// is what a snapshot buys, and it is taken when that exceeds
	// HeapSnapshotPinned
	h.mu.Lock()
	var pinned int64
	for _, hf := range h.files {
		if hf != h.cur && hf != h.mov && hf.deltas > 0 && hf.live == hf.deltas {
			pinned += hf.size
		}
	}
	due := h.height-h.snapAt >= HeapSnapshotBlocks || pinned >= HeapSnapshotPinnedFiles*HeapFileBytes
	if due {
		h.snapAt = h.height
	}
	h.mu.Unlock()
	if due {
		t := time.Now()
		err = h.Snapshot()
		h.snapshotsN.Add(1)
		h.snapshotNs.Add(uint64(time.Since(t)))
	}
	return moved, err
}

// Snapshot starts a new index generation: the whole map, then the
// deltas to come.  Serialized with block syncs, so the map is exactly
// the state of the last finished delta and no delta lands in the
// generation being retired.  Off the protocol path.
func (h *HeapStore) Snapshot() error {
	h.mu.RLock()
	closed := h.closed
	h.mu.RUnlock()
	if closed {
		return errStoreClosed
	}
	return h.startGeneration()
}

// moverHook, when set, runs between the mover's copy and its naming:
// the window in which a put can make a copy dead on arrival.
var moverHook func()

// heapMove is one live entry the mover copies: where it was, where
// it goes, and the bytes to write there.
type heapMove struct {
	key      [32]byte
	from, to slot
	hf       *heapFile
	height   uint64 // The block the copy is written as
	value    []byte
	entry    []byte
}

// heapPlanChunk is how many entries a pass plans per lock hold.
const heapPlanChunk = 4096

// clean is the mover: it takes the files with the most dead bytes --
// once HeapCleanRatio of each is dead, or the deadest whatever its
// ratio while dead bytes exceed live -- copies their live entries to
// the mover's file, at most budget bytes per pass, and marks a file
// left with nothing live for deletion once the delta naming the
// copies is durable.  The pass syncs its own copies, so they never
// land in a block's barrier, and it holds the shard's lock only to
// pick, to plan, and to name: never across a read, a write or an
// fsync (spec 1.6).  A byte released never costs more than a byte
// copied unless the size bound forces it (spec 1.2).
func (h *HeapStore) clean(budget int64) (bool, error) {
	// 1. Under the lock: pick the files
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return false, errStoreClosed
	}
	if len(h.release) > 0 {
		h.mu.Unlock()
		return false, nil // The last pass's deletions are still waiting on a sync
	}
	// A file with nothing live in it needs no scan and no copy: it is
	// released outright, any number of them a pass, one sync late as
	// every release is.  Without this the pass's file count paced the
	// mover at about the rate dead bytes appeared, and the store
	// floated with the size bound engaged.
	emptied := 0
	for _, hf := range h.files {
		if hf.live == 0 && hf != h.cur && hf != h.mov && !hf.cleaning && hf.inflight == 0 && !hf.releasing && hf.size > 0 {
			hf.releasing = true
			h.release = append(h.release, hf.id)
			emptied++
		}
	}
	var taken []*heapFile
	var scan int64
	for len(taken) < HeapCleanFiles && scan < HeapScanBytes {
		hf := h.pickFile()
		if hf == nil {
			break
		}
		hf.cleaning = true
		taken = append(taken, hf)
		scan += hf.size
	}
	h.mu.Unlock()
	if len(taken) == 0 {
		return emptied > 0, nil
	}
	// 2. Under the lock, briefly: the picked files' live slots, from
	// the index -- one pass over the map, not a decode of the files.
	// Scanning a file to find its live entries read and checksummed
	// up to 128 MB a shard per pass, and nine stores' passes together
	// starved the block loops for CPU (load 27 on 24 cores, seal p50
	// 200 ms for the minute).  Then without the lock: read those
	// entries, and only those
	type liveSlot struct {
		key [32]byte
		s   slot
	}
	h.mu.Lock()
	picked := map[uint32]int{}
	for i, hf := range taken {
		picked[hf.id] = i
	}
	slots := make([][]liveSlot, len(taken))
	for key, s := range h.index {
		if i, ok := picked[s.file]; ok {
			slots[i] = append(slots[i], liveSlot{key: key, s: s})
		}
	}
	h.mu.Unlock()
	entries := make([][]heapEntry, len(taken))
	for i, hf := range taken {
		sort.Slice(slots[i], func(a, b int) bool { return slots[i][a].s.off < slots[i][b].s.off })
		for _, ls := range slots[i] {
			buf := make([]byte, entrySize(int(ls.s.n)))
			if _, err := hf.f.ReadAt(buf, int64(ls.s.off)); err != nil {
				h.mu.Lock()
				for _, hf := range taken {
					hf.cleaning = false
				}
				h.mu.Unlock()
				return false, err
			}
			size, _, key, value, ok := decodeEntry(buf)
			if !ok || key != ls.key {
				continue // Damaged or stale: left where it is, and a read of it reports the damage
			}
			entries[i] = append(entries[i], heapEntry{off: ls.s.off, size: size, key: key, value: value})
		}
		h.cleanedBytes.Add(uint64(hf.size))
	}
	// 3. Under the lock, a chunk of entries at a time: confirm each is
	// still named and reserve its copy's slot in the mover's file
	var moves []heapMove
	var copied int64
	for i, hf := range taken {
		for at := 0; at < len(entries[i]) && copied < budget; at += heapPlanChunk {
			end := at + heapPlanChunk
			if end > len(entries[i]) {
				end = len(entries[i])
			}
			h.mu.Lock()
			m, n, err := h.planFile(hf, entries[i][at:end], budget-copied)
			h.mu.Unlock()
			if err != nil {
				h.mu.Lock()
				for _, hf := range taken {
					hf.cleaning = false
				}
				h.mu.Unlock()
				return false, err
			}
			moves = append(moves, m...)
			copied += n
		}
	}
	h.mu.Lock()
	for _, hf := range taken {
		hf.cleaning = false
	}
	h.mu.Unlock()
	var movFiles []*heapFile
	for _, m := range moves {
		if len(movFiles) == 0 || movFiles[len(movFiles)-1] != m.hf {
			movFiles = append(movFiles, m.hf)
		}
	}
	// The copies' bytes are laid out outside the lock
	for i := range moves {
		m := &moves[i]
		m.entry = encodeEntry(m.height, m.key, m.value)
	}
	// 4. Without the lock: write the copies and make them durable
	fail := func(err error) (bool, error) {
		h.mu.Lock()
		for _, m := range moves {
			m.hf.inflight-- // Reserved, never written: dead, and read no further
			h.kill(m.to)
		}
		h.mu.Unlock()
		return false, err
	}
	for _, m := range moves {
		if _, err := m.hf.f.WriteAt(m.entry, int64(m.to.off)); err != nil {
			return fail(err)
		}
	}
	for _, hf := range movFiles {
		if err := fsync(hf.f); err != nil {
			return fail(err)
		}
	}
	if moverHook != nil {
		moverHook() // Tests: a put between the copy and its naming
	}
	// 5. Under the lock: name the copies, unless the key was rewritten
	// meanwhile, in which case the copy is dead on arrival; and mark
	// the files the pass emptied
	h.mu.Lock()
	defer h.mu.Unlock()
	// The copies were reserved in order, so they form ranges of the
	// mover's files; a copy that arrived dead is excluded from them.
	// The mover's ranges go before the block's own in the delta, so
	// that a put of the same key later in the block wins on replay
	var mine []heapRange
	for _, m := range moves {
		m.hf.inflight--
		size := entrySize(int(m.to.n))
		if n := len(mine); n > 0 && mine[n-1].file == m.to.file && int64(mine[n-1].to) == int64(m.to.off) {
			mine[n-1].to = uint32(int64(m.to.off) + size)
		} else {
			mine = append(mine, heapRange{file: m.to.file, from: m.to.off, to: uint32(int64(m.to.off) + size)})
		}
		s, live := h.index[m.key]
		if live && s == m.from {
			h.index[m.key] = m.to
			h.kill(m.from)
		} else {
			h.kill(m.to) // Reserved and written, but no longer wanted
			h.excluded = append(h.excluded, heapRange{file: m.to.file, from: m.to.off})
		}
	}
	h.ranges = append(mine, h.ranges...)
	for _, hf := range taken {
		if hf.live == 0 && hf != h.cur && hf != h.mov {
			hf.releasing = true
			h.release = append(h.release, hf.id)
		}
	}
	h.movedBytes.Add(uint64(copied))
	return true, nil
}

// pickFile is the file the mover takes next: the deadest by fraction
// among those not open for append, if it is dead enough or the heap
// is over its size bound.  The caller holds the lock.
func (h *HeapStore) pickFile() *heapFile {
	var pick *heapFile
	best := 0.0
	for _, hf := range h.files {
		if hf == h.cur || hf == h.mov || hf.cleaning || hf.inflight > 0 || hf.releasing || hf.dead == 0 {
			continue
		}
		if f := float64(hf.dead) / float64(hf.size); f > best {
			pick, best = hf, f
		}
	}
	switch {
	case !h.bound && float64(h.deadBytes) > HeapBoundOn*float64(h.liveBytes):
		h.bound = true
	case h.bound && h.deadBytes < h.liveBytes:
		h.bound = false
	}
	if pick == nil || best < HeapCleanRatio && (!h.bound || best < HeapCleanFloor) {
		return nil
	}
	return pick
}

// heapEntry is one entry of a picked file as decoded outside the
// lock: where it is, and its bytes ready to be copied.
type heapEntry struct {
	off   uint32
	size  int64
	key   [32]byte
	value []byte
}

// planFile reserves, in the mover's file, a slot for each entry of a
// picked file the index still names, up to budget bytes; the rest
// wait for the next pass.  The caller holds the lock; the entries
// were decoded outside it.
func (h *HeapStore) planFile(hf *heapFile, entries []heapEntry, budget int64) (moves []heapMove, copied int64, err error) {
	for _, e := range entries {
		s, live := h.index[e.key]
		if !live || s.file != hf.id || s.off != e.off {
			continue
		}
		if copied >= budget {
			break // The rest next pass
		}
		to, off, err := h.reserve(true, e.size)
		if err != nil {
			return nil, 0, err
		}
		ns := slot{file: to.id, off: uint32(off), n: uint32(len(e.value)), block: h.height}
		// The copy is this block's write of the key: it carries this
		// height, so a repair scan prefers it to the original
		moves = append(moves, heapMove{key: e.key, from: s, to: ns, hf: to, height: h.height, value: e.value})
		copied += e.size
	}
	return moves, copied, nil
}

// RepairHeapStore rebuilds the key map by reading the keys from the
// data: every entry carries its key, its height and a checksum, so a
// scan of the data files in order recovers the map without any index.
// For each key the copy with the highest height wins, later in the
// scan on a tie; an entry above the committed height -- a block whose
// sync never finished -- is dropped, as is any torn or damaged slot.
// The rebuilt map starts a new generation, so the next open is an
// ordinary one.
func RepairHeapStore(directory string, committed uint64) (*HeapStore, error) {
	dataIDs, gens, err := listHeap(directory)
	if err != nil {
		return nil, err
	}
	for _, g := range gens {
		os.Remove(filepath.Join(directory, indexName(g)))
	}
	h := &HeapStore{Directory: directory, index: map[[32]byte]slot{}, dirty: map[uint32]*heapFile{}, files: map[uint32]*heapFile{}, height: committed}
	heights := map[[32]byte]uint64{}
	for _, id := range dataIDs {
		data, err := os.ReadFile(filepath.Join(directory, dataName(id)))
		if err != nil {
			return nil, err
		}
		var off int64
		for off < int64(len(data)) {
			size, height, key, value, ok := decodeEntry(data[off:])
			if size == 0 {
				break
			}
			if !ok || key == heapDeltaKey {
				off += size
				continue
			}
			if height <= committed {
				if prev, seen := heights[key]; !seen || height >= prev {
					heights[key] = height
					h.index[key] = slot{file: id, off: uint32(off), n: uint32(len(value))}
				}
			}
			off += size
		}
		if id >= h.nextID {
			h.nextID = id + 1
		}
	}
	// A repair trusts the data alone: the snapshot's replay point is
	// the end of the newest file, so no old delta is replayed over it
	if n := len(dataIDs); n > 0 {
		if st, err := os.Stat(filepath.Join(directory, dataName(dataIDs[n-1]))); err == nil {
			h.deltaAt.file, h.deltaAt.off = dataIDs[n-1], st.Size()
		}
	}
	h.gen = 1
	if err = h.startGeneration(); err != nil {
		return nil, err
	}
	if err = h.openFiles(dataIDs); err != nil {
		return nil, err
	}
	if err = h.deriveFiles(dataIDs); err != nil {
		return nil, err
	}
	return h, nil
}

// Stats is the heap's report in the store's terms: puts and lookups
// as the segment layer counts them (every hit is answered from the
// key map, so they are all LiveHit), and the heap's own figures.
func (h *HeapStore) Stats() StoreStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return StoreStats{
		PutTotal:           h.putTotal.Load(),
		PutNew:             h.putAppend.Load(),
		LookupTotal:        h.lookups.Load(),
		LiveHit:            h.hits.Load(),
		ResidentIndexBytes: uint64(len(h.index)) * (32 + 24),
		HeapFiles:          len(h.files),
		HeapLiveBytes:      uint64(h.liveBytes),
		HeapDeadBytes:      uint64(h.deadBytes),
		HeapScannedBytes:   h.cleanedBytes.Load(),
		HeapMovedBytes:     h.movedBytes.Load(),
	}
}

// HoleRatio reports the dead bytes in the files against the live
// bytes: what the mover has yet to reclaim.  Live includes the delta
// entries a snapshot has not yet superseded.
func (h *HeapStore) HoleRatio() (dead, live int64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.deadBytes, h.liveBytes
}

// deltaBytes is the bytes of delta entries not yet superseded.
func (h *HeapStore) deltaBytes() (n int64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, hf := range h.files {
		n += hf.deltas
	}
	return n
}

// SyncCost reports the block syncs so far: how many, the bytes their
// data fsyncs covered, and the time spent in the data files' fsyncs
// and in the delta's write and fsync.
func (h *HeapStore) SyncCost() (syncs, bytes uint64, heapFsync, delta time.Duration) {
	return h.syncs.Load(), h.syncBytes.Load(), time.Duration(h.syncHeapNs.Load()), time.Duration(h.syncLogNs.Load())
}

// Cleaned reports what the mover has scanned and what it had to
// copy: the ratio is the heap's write amplification.
func (h *HeapStore) Cleaned() (scanned, moved uint64) {
	return h.cleanedBytes.Load(), h.movedBytes.Load()
}

// MoverCost reports the files a block's finish has unlinked and the
// time in those unlinks, and the snapshots written and their time.
func (h *HeapStore) MoverCost() (releases uint64, release time.Duration, snapshots uint64, snapshot time.Duration) {
	return h.releases.Load(), time.Duration(h.releaseNs.Load()), h.snapshotsN.Load(), time.Duration(h.snapshotNs.Load())
}

// SetFilterBlocks and SetSealLimit are the segment layer's knobs; a
// heap has neither a window nor a tail.
func (h *HeapStore) SetFilterBlocks(uint64) error { return nil }
func (h *HeapStore) SetSealLimit(uint64) error    { return nil }

// LiveRecords is the live key count; a heap is never sealed on it.
func (h *HeapStore) LiveRecords() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return uint64(len(h.index))
}
