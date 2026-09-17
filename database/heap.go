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
//	              whole map, then one delta per block sync naming the
//	              keys the block touched.  A snapshot starts a new
//	              generation in a file of its own, switched to by
//	              rename, so no delta is ever truncated away.
//
// The key map is in memory for the live key set (spec 1.2: memory that
// scales with the working set).  Open loads the newest whole
// generation, replays its deltas and derives every file's live and
// dead bytes; a data file nothing names is deleted then, and one that
// is missing while named is an error.
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

	// touched is the block's delta in the making; dirty the files
	// written since the last sync; release the files the mover
	// emptied, deleted once the delta naming their copies is durable.
	touched map[[32]byte]struct{}
	dirty   map[uint32]*heapFile
	release []uint32

	// syncMu serializes block syncs with each other and with a
	// snapshot, so the map a snapshot writes is exactly the state of
	// the last finished delta and no delta is in flight into a log
	// about to be retired.
	syncMu    sync.Mutex
	log       *os.File
	gen       uint64
	snapshots int

	closed    bool
	liveBytes int64
	deadBytes int64

	putTotal, putInPlace, putAppend atomic.Uint64
	lookups, hits                   atomic.Uint64
	cleanedBytes, movedBytes        atomic.Uint64
	// The sync's cost, split: nanoseconds in the data files' fsyncs
	// and in the delta's write and fsync, and the syncs and bytes
	syncs, syncHeapNs, syncLogNs, syncBytes atomic.Uint64
}

// heapFile is one data file and its accounting.
type heapFile struct {
	id         uint32
	f          *os.File
	size       int64
	live, dead int64
	firstBlock uint64 // The block that first appended to it
	cleaning   bool   // Taken by the pass in progress
	inflight   int    // Copies reserved in it and not yet written: its size runs ahead of its bytes
	releasing  bool   // Emptied by a pass; deleted by the next sync
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
	heapHeader   = 4 + 8 + 32 // len, height, key
	heapTrailer  = 4          // crc32 of height+key+value
	heapAlign    = 8          // Entries start on an 8-byte boundary
	heapMagic    = 0x48454150 // "HEAP", an index record's marker
	heapSnapshot = 0x50414E53 // "SNAP", the record that starts a generation
	heapIndexHdr = 4 + 8 + 4  // magic, height, count
	heapIndexRec = 32 + 4 + 4 + 4
)

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

// HeapCleanFiles bounds a pass by files taken as well.
var HeapCleanFiles = 4

// HeapCleanRatio is the dead fraction a file must reach before the
// mover takes it -- unless dead bytes exceed live bytes overall, when
// the deadest file is taken regardless, which bounds the heap at
// twice its live set.
var HeapCleanRatio = 0.5

// HeapSnapshotEvery is how many compact calls pass between key-map
// snapshots; between them the generation's deltas are what open
// replays.
var HeapSnapshotEvery = 5

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
	h.touched = map[[32]byte]struct{}{}
	h.dirty = map[uint32]*heapFile{}
	h.files = map[uint32]*heapFile{}
	h.release = nil
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
		// A fresh store: the first generation, written with the locks
		// released since startGeneration takes them itself
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
	return h.deriveFiles(dataIDs)
}

// replayGeneration applies the current generation: its snapshot, then
// every whole delta; a torn tail is what a crash leaves and is
// dropped, its slots unnamed.  The caller holds the lock.
func (h *HeapStore) replayGeneration() (err error) {
	path := filepath.Join(h.Directory, indexName(h.gen))
	if h.log, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return err
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	at := 0
	for at < len(buf) {
		n, err := h.applyRecord(buf[at:])
		if err != nil {
			if err = h.log.Truncate(int64(at)); err != nil {
				return err
			}
			break
		}
		at += n
	}
	return nil
}

// startGeneration begins an index generation: a snapshot of the map,
// written aside and fsynced, then the deltas appended to the old
// generation meanwhile copied after it, renamed into place, the
// directory fsynced, and the previous generation's file removed once
// the new one is durable.  The snapshot itself is written with no
// lock held but the map's read lock; only the tail copy and the
// switch hold syncMu, so a block's sync waits milliseconds for a
// snapshot, not for 3 MB of map (measured: seal p90 276 ms at every
// hundredth block with the whole write under syncMu).  Open calls it
// with nothing else running.
func (h *HeapStore) startGeneration() error {
	next := h.gen
	if h.log != nil {
		next = h.gen + 1 // A snapshot starts the generation after the current one
	}
	path := filepath.Join(h.Directory, indexName(next))
	tmp := path + segTmpSuffix
	// 1. The map as of now, and where the old generation's log ends:
	// deltas after that point are copied over below
	h.mu.RLock()
	all := func(emit func(key [32]byte)) {
		for key := range h.index {
			emit(key)
		}
	}
	snap := h.encodeIndexOf(heapSnapshot, all, len(h.index))
	var copiedTo int64
	if h.log != nil {
		copiedTo, _ = h.log.Seek(0, io.SeekEnd)
	}
	h.mu.RUnlock()
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
	// 2. Under syncMu: no delta is in flight, so the old log's tail
	// past copiedTo is exactly the deltas since the snapshot
	h.syncMu.Lock()
	defer h.syncMu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.log != nil {
		rest, err := readFrom(h.log, copiedTo)
		if err != nil {
			f.Close()
			return err
		}
		if len(rest) > 0 {
			if _, err = f.Write(rest); err != nil {
				f.Close()
				return err
			}
			if err = fsync(f); err != nil {
				f.Close()
				return err
			}
		}
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
	old := h.log
	if h.log, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644); err != nil {
		return err
	}
	if old != nil {
		old.Close()
		os.Remove(filepath.Join(h.Directory, indexName(h.gen)))
	}
	h.gen = next
	return nil
}

// readFrom reads a file from off to its end.
func readFrom(f *os.File, off int64) ([]byte, error) {
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if end <= off {
		return nil, nil
	}
	buf := make([]byte, end-off)
	_, err = f.ReadAt(buf, off)
	return buf, err
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

// encodeIndex is one index record: a snapshot of the whole map or the
// block's delta of the keys it touched, checksummed.  The caller holds
// the lock.
func (h *HeapStore) encodeIndex(magic uint32, keys map[[32]byte]struct{}) []byte {
	return h.encodeIndexOf(magic, func(emit func(key [32]byte)) {
		for key := range keys {
			emit(key)
		}
	}, len(keys))
}

func (h *HeapStore) encodeIndexOf(magic uint32, each func(emit func(key [32]byte)), count int) []byte {
	buf := make([]byte, heapIndexHdr+count*heapIndexRec+4)
	binary.LittleEndian.PutUint32(buf, magic)
	binary.LittleEndian.PutUint64(buf[4:], h.height)
	binary.LittleEndian.PutUint32(buf[12:], uint32(count))
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
	if magic != heapMagic && magic != heapSnapshot {
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
	for at := heapIndexHdr; at < end; at += heapIndexRec {
		var key [32]byte
		copy(key[:], buf[at:])
		h.index[key] = slot{file: binary.LittleEndian.Uint32(buf[at+32:]), off: binary.LittleEndian.Uint32(buf[at+36:]), n: binary.LittleEndian.Uint32(buf[at+40:])}
	}
	return end + 4, nil
}

// deriveFiles opens every data file the map names, derives its live
// and dead bytes, cuts the newest back to its last named entry, and
// deletes any file the map does not name at all: nothing durable
// names it, and its bytes are a crash's or the mover's leftovers.  The
// caller holds the lock.
func (h *HeapStore) deriveFiles(dataIDs []uint32) error {
	named := map[uint32]int64{} // file -> end of its last named slot
	for _, s := range h.index {
		if e := int64(s.off) + entrySize(int(s.n)); e > named[s.file] {
			named[s.file] = e
		}
		h.liveBytes += entrySize(int(s.n))
	}
	for id := range named {
		if _, err := os.Stat(filepath.Join(h.Directory, dataName(id))); err != nil {
			return fmt.Errorf("heap: the index names %s: %w", dataName(id), err)
		}
	}
	for _, id := range dataIDs {
		end, live := named[id]
		if !live {
			os.Remove(filepath.Join(h.Directory, dataName(id)))
			continue
		}
		f, err := os.OpenFile(filepath.Join(h.Directory, dataName(id)), os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		if err = f.Truncate(end); err != nil {
			f.Close()
			return err
		}
		h.files[id] = &heapFile{id: id, f: f, size: end}
	}
	for _, s := range h.index {
		h.files[s.file].live += entrySize(int(s.n))
	}
	for _, hf := range h.files {
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
	if crc32.ChecksumIEEE(buf[4:heapHeader+n]) != binary.LittleEndian.Uint32(buf[heapHeader+n:]) {
		return 0, 0, key, nil, false
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
	}
	return hf, off, nil
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
	h.touched[key] = struct{}{}
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
	for _, hf := range h.dirty {
		p.dirty = append(p.dirty, hf)
		p.bytes += hf.size
	}
	h.dirty = map[uint32]*heapFile{}
	if len(h.touched) > 0 || len(p.release) > 0 {
		p.delta = h.encodeIndex(heapMagic, h.touched)
		h.touched = map[[32]byte]struct{}{}
	}
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
	t = time.Now()
	if _, err = h.log.Write(p.delta); err != nil {
		return err
	}
	if err = fsync(h.log); err != nil {
		return err
	}
	h.syncLogNs.Add(uint64(time.Since(t)))
	h.syncs.Add(1)
	h.syncBytes.Add(uint64(p.bytes))
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range p.release {
		hf := h.files[id]
		hf.f.Close()
		delete(h.files, id)
		h.deadBytes -= hf.dead
		if err = os.Remove(filepath.Join(h.Directory, dataName(id))); err != nil {
			return err
		}
	}
	return nil
}

// compact is the heap's maintenance on the adapter's cadence: one
// bounded mover pass, and every HeapSnapshotEvery calls a new index
// generation, which bounds the replay on open.
func (h *HeapStore) compact() (bool, error) {
	moved, err := h.clean(HeapCleanBytes)
	if err != nil {
		return moved, err
	}
	h.mu.Lock()
	h.snapshots++
	due := h.snapshots >= HeapSnapshotEvery
	if due {
		h.snapshots = 0
	}
	h.mu.Unlock()
	if due {
		err = h.Snapshot()
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
	var taken []*heapFile
	for len(taken) < HeapCleanFiles {
		hf := h.pickFile()
		if hf == nil {
			break
		}
		hf.cleaning = true
		taken = append(taken, hf)
	}
	h.mu.Unlock()
	if len(taken) == 0 {
		return false, nil
	}
	// 2. Without the lock: read and decode them.  A picked file is
	// neither the block's nor the mover's, so its bytes do not change;
	// only what the index says of them can, and that is checked under
	// the lock.  Decoding here, checksums included, keeps 16 MB of
	// CRC per file off the lock
	entries := make([][]heapEntry, len(taken))
	for i, hf := range taken {
		buf := make([]byte, hf.size)
		if _, err := hf.f.ReadAt(buf, 0); err != nil {
			h.mu.Lock()
			for _, hf := range taken {
				hf.cleaning = false
			}
			h.mu.Unlock()
			return false, err
		}
		entries[i] = decodeFile(buf)
		h.cleanedBytes.Add(uint64(hf.size))
	}
	// 3. Under the lock, a chunk of entries at a time: decide what is
	// live and reserve each copy's slot in the mover's file.  A pass
	// walks up to a million entries; taking the lock per chunk keeps
	// each hold to a millisecond or so.  A block sync may begin
	// between chunks; the mover's file is never in its dirty set
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
	for _, m := range moves {
		m.hf.inflight--
		s, live := h.index[m.key]
		if live && s == m.from {
			h.index[m.key] = m.to
			h.touched[m.key] = struct{}{}
			h.kill(m.from)
		} else {
			h.kill(m.to) // Reserved and written, but no longer wanted
		}
	}
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
	if pick == nil || (best < HeapCleanRatio && h.deadBytes <= h.liveBytes) {
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

// decodeFile walks a file's bytes into its entries, stopping at the
// first unwritten, torn or damaged slot.
func decodeFile(buf []byte) (entries []heapEntry) {
	var at int64
	for at < int64(len(buf)) {
		size, _, key, value, ok := decodeEntry(buf[at:])
		if !ok {
			break
		}
		entries = append(entries, heapEntry{off: uint32(at), size: size, key: key, value: value})
		at += size
	}
	return entries
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
	h := &HeapStore{Directory: directory, index: map[[32]byte]slot{}, touched: map[[32]byte]struct{}{},
		dirty: map[uint32]*heapFile{}, files: map[uint32]*heapFile{}, height: committed}
	heights := map[[32]byte]uint64{}
	for _, id := range dataIDs {
		data, err := os.ReadFile(filepath.Join(directory, dataName(id)))
		if err != nil {
			return nil, err
		}
		var off int64
		for off < int64(len(data)) {
			size, height, key, value, ok := decodeEntry(data[off:])
			if !ok {
				break
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
	h.gen = 1
	if err = h.startGeneration(); err != nil {
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
// bytes: what the mover has yet to reclaim.
func (h *HeapStore) HoleRatio() (dead, live int64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.deadBytes, h.liveBytes
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
