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
	"sync"
	"sync/atomic"
)

// HeapStore is the dynamic layer as a heap with holes (proposal
// docs/proposals/2026-09-16-entries-written-once.md): entries are
// written once, keys are managed separately, and space is reclaimed
// by reusing holes rather than by rewriting live data.
//
// Files, in one directory:
//
//	heap.dat   entries: [len u32][key 32][value][crc32 of key+value],
//	           each in a slot whose capacity is a size class (8 << c)
//	index.log  one delta per block sync: the (key, off, cap, len) of
//	           every key the block touched, checksummed, appended
//	index.snap the whole key map, rewritten on the maintenance cadence
//
// The key map is in memory for the live key set (spec 1.2: memory that
// scales with the working set).  Open loads the snapshot and replays
// the log; holes are the file's bytes no live slot covers.
//
// Durability (spec 1.8).  The block sync fsyncs heap.dat and then
// appends and fsyncs the block's delta, so an index entry is durable
// only after the slot it names is.  A slot the last durable index
// names is never overwritten: a key rewritten in a later block takes
// a new slot (a hole that fits, or the end of the file), and its old
// slot is freed only after the delta that stops naming it is durable
// -- one sync late, the way spec 2.6 defers deletion.  A key rewritten
// again within the same block reuses the slot it took this block,
// since nothing durable names it yet.  A crash therefore leaves every
// durable entry intact and every torn slot unnamed, and the checksum
// catches a torn slot that a stale index could name.
type HeapStore struct {
	Directory string

	mu     sync.RWMutex
	file   *os.File // heap.dat
	log    *os.File // index.log
	size   int64    // Append point: the end of heap.dat
	index  map[[32]byte]slot
	free   [heapClasses][]int64 // Reusable slots by size class
	height uint64               // The block being written; slots taken in it may be rewritten in place

	// pendingFree holds the slots the current block stopped naming,
	// reusable once the block's delta is durable.  touched is the
	// block's delta in the making.
	pendingFree []slot
	touched     map[[32]byte]struct{}

	closed    bool
	holeBytes int64 // Capacity of every free slot
	liveBytes int64 // Capacity of every named slot

	putTotal, putInPlace, putHole, putAppend atomic.Uint64
	lookups, hits                            atomic.Uint64
}

// slot is where an entry lives: its offset, the capacity of the slot
// (a size class) and the entry's value length.  block is the height
// that took the slot, which decides whether a rewrite may reuse it.
type slot struct {
	off   int64
	cap   uint32
	n     uint32
	block uint64
}

const (
	heapHeader  = 4 + 32     // len + key
	heapTrailer = 4          // crc32 of key+value
	heapClasses = 40         // 8 << 39 is far past any value
	heapMinCap  = 64         // Smallest slot
	heapMagic   = 0x48454150 // "HEAP", the delta record's marker
)

// heapClass is the size class that holds need bytes: capacity
// 8 << c, the smallest not below need and not below heapMinCap.
func heapClass(need int) (c int, capacity uint32) {
	capacity = heapMinCap
	c = 3 // 8 << 3
	for int(capacity) < need {
		capacity <<= 1
		c++
	}
	return c, capacity
}

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

// OpenHeapStore opens the heap in directory as it was left.
func OpenHeapStore(directory string) (*HeapStore, error) {
	if _, err := os.Stat(filepath.Join(directory, "heap.dat")); err != nil {
		return nil, fmt.Errorf("open heap at %s: %w", directory, err)
	}
	h := &HeapStore{Directory: directory}
	return h, h.Open()
}

// Open loads the key map from the snapshot and the log, and derives
// the holes.  Idempotent.
func (h *HeapStore) Open() (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file != nil {
		return nil
	}
	if h.file, err = os.OpenFile(filepath.Join(h.Directory, "heap.dat"), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return err
	}
	if h.log, err = os.OpenFile(filepath.Join(h.Directory, "index.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644); err != nil {
		return err
	}
	h.index = map[[32]byte]slot{}
	h.touched = map[[32]byte]struct{}{}
	h.closed = false
	if err = h.loadSnapshot(); err != nil {
		return err
	}
	if err = h.replayLog(); err != nil {
		return err
	}
	return h.deriveHoles()
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
	err = h.file.Close()
	if lerr := h.log.Close(); err == nil {
		err = lerr
	}
	h.file, h.log = nil, nil
	return err
}

// Put writes value under key: in place if the key took its slot this
// block and the value fits, else into a hole that fits, else at the
// end of the file.  The slot a durable index names is never rewritten.
func (h *HeapStore) Put(key [32]byte, value []byte) error {
	need := heapHeader + len(value) + heapTrailer
	buf := make([]byte, need)
	binary.LittleEndian.PutUint32(buf, uint32(len(value)))
	copy(buf[4:], key[:])
	copy(buf[heapHeader:], value)
	binary.LittleEndian.PutUint32(buf[heapHeader+len(value):], crc32.ChecksumIEEE(buf[4:heapHeader+len(value)]))

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errStoreClosed
	}
	h.putTotal.Add(1)
	old, had := h.index[key]
	var s slot
	switch {
	case had && old.block == h.height && int(old.cap) >= need:
		// Taken this block, nothing durable names it: rewrite in place
		s = old
		s.n = uint32(len(value))
		h.putInPlace.Add(1)
	default:
		c, capacity := heapClass(need)
		if n := len(h.free[c]); n > 0 {
			s = slot{off: h.free[c][n-1], cap: capacity}
			h.free[c] = h.free[c][:n-1]
			h.holeBytes -= int64(capacity)
			h.putHole.Add(1)
		} else {
			s = slot{off: h.size, cap: capacity}
			h.size += int64(capacity)
			h.putAppend.Add(1)
		}
		s.n = uint32(len(value))
		s.block = h.height
		h.liveBytes += int64(capacity)
		if had {
			h.pendingFree = append(h.pendingFree, old)
			h.liveBytes -= int64(old.cap)
		}
	}
	if _, err := h.file.WriteAt(buf, s.off); err != nil {
		return err
	}
	h.index[key] = s
	h.touched[key] = struct{}{}
	return nil
}

// Get answers from the key map and one read of the slot.  A slot
// whose checksum fails is reported as corrupt, never as a value.
func (h *HeapStore) Get(key [32]byte) ([]byte, error) {
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return nil, errStoreClosed
	}
	h.lookups.Add(1)
	s, ok := h.index[key]
	if !ok {
		h.mu.RUnlock()
		return nil, errNotFound
	}
	h.hits.Add(1)
	buf := make([]byte, heapHeader+int(s.n)+heapTrailer)
	_, err := h.file.ReadAt(buf, s.off)
	h.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return heapEntryValue(buf, key)
}

// heapEntryValue checks an entry read from a slot and returns its
// value.
func heapEntryValue(buf []byte, key [32]byte) ([]byte, error) {
	n := int(binary.LittleEndian.Uint32(buf))
	if len(buf) != heapHeader+n+heapTrailer || [32]byte(buf[4:heapHeader]) != key {
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

// heapSync is a block sync in flight: the delta to make durable and
// the slots to free once it is.
type heapSync struct {
	h     *HeapStore
	delta []byte
	freed []slot
}

// beginBlockSync takes the block's delta and its freed slots under the
// lock; finish makes them durable outside it.
func (h *HeapStore) beginBlockSync() (blockSync, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.file == nil {
		return nil, errStoreClosed
	}
	p := &heapSync{h: h, freed: h.pendingFree}
	h.pendingFree = nil
	if len(h.touched) > 0 {
		p.delta = h.encodeDelta()
		h.touched = map[[32]byte]struct{}{}
	}
	return p, nil
}

// encodeDelta is the block's index delta: marker, height, count, the
// touched keys' slots, and a checksum.  The caller holds the lock.
func (h *HeapStore) encodeDelta() []byte {
	const rec = 32 + 8 + 4 + 4
	buf := make([]byte, 4+8+4+len(h.touched)*rec+4)
	binary.LittleEndian.PutUint32(buf, heapMagic)
	binary.LittleEndian.PutUint64(buf[4:], h.height)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(h.touched)))
	at := 16
	for key := range h.touched {
		s := h.index[key]
		copy(buf[at:], key[:])
		binary.LittleEndian.PutUint64(buf[at+32:], uint64(s.off))
		binary.LittleEndian.PutUint32(buf[at+40:], s.cap)
		binary.LittleEndian.PutUint32(buf[at+44:], s.n)
		at += rec
	}
	binary.LittleEndian.PutUint32(buf[at:], crc32.ChecksumIEEE(buf[:at]))
	return buf
}

// finish: entries durable, then the delta durable, then the slots the
// block stopped naming become holes.
func (p *heapSync) finish() error {
	h := p.h
	if p.delta == nil && len(p.freed) == 0 {
		return nil
	}
	if err := fsync(h.file); err != nil {
		return err
	}
	if p.delta != nil {
		if _, err := h.log.Write(p.delta); err != nil {
			return err
		}
		if err := fsync(h.log); err != nil {
			return err
		}
	}
	h.mu.Lock()
	for _, s := range p.freed {
		h.addHole(s.off, s.cap)
	}
	h.mu.Unlock()
	return nil
}

// addHole puts a slot on its class's free list.  The caller holds the
// lock.
func (h *HeapStore) addHole(off int64, capacity uint32) {
	c, _ := heapClass(int(capacity))
	h.free[c] = append(h.free[c], off)
	h.holeBytes += int64(capacity)
}

// Snapshot writes the whole key map and truncates the log, so that
// the replay on open stays bounded.  Off the protocol path, on the
// maintenance cadence.
func (h *HeapStore) Snapshot() error {
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return errStoreClosed
	}
	const rec = 32 + 8 + 4 + 4
	buf := make([]byte, 4+8+4+len(h.index)*rec+4)
	binary.LittleEndian.PutUint32(buf, heapMagic)
	binary.LittleEndian.PutUint64(buf[4:], h.height)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(h.index)))
	at := 16
	for key, s := range h.index {
		copy(buf[at:], key[:])
		binary.LittleEndian.PutUint64(buf[at+32:], uint64(s.off))
		binary.LittleEndian.PutUint32(buf[at+40:], s.cap)
		binary.LittleEndian.PutUint32(buf[at+44:], s.n)
		at += rec
	}
	binary.LittleEndian.PutUint32(buf[at:], crc32.ChecksumIEEE(buf[:at]))
	logSize, err := h.log.Seek(0, io.SeekCurrent)
	h.mu.RUnlock()
	if err != nil {
		return err
	}
	// Written aside and renamed over the old snapshot, so a crash
	// leaves one or the other whole
	tmp := filepath.Join(h.Directory, "index.snap.tmp")
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
	if err = os.Rename(tmp, filepath.Join(h.Directory, "index.snap")); err != nil {
		return err
	}
	// The deltas the snapshot covers are what was in the log when it
	// was taken; deltas appended since stay.  The log is truncated
	// from the front by rewriting what follows: rare, small, and it
	// keeps one file per purpose.
	h.mu.Lock()
	defer h.mu.Unlock()
	rest, err := readFrom(h.log, logSize)
	if err != nil {
		return err
	}
	if err = h.log.Truncate(0); err != nil {
		return err
	}
	if _, err = h.log.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if len(rest) > 0 {
		if _, err = h.log.Write(rest); err != nil {
			return err
		}
	}
	return fsync(h.log)
}

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

// loadSnapshot reads index.snap into the key map, if there is one.
// The caller holds the lock.
func (h *HeapStore) loadSnapshot() error {
	buf, err := os.ReadFile(filepath.Join(h.Directory, "index.snap"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	n, err := h.applyDelta(buf)
	if err != nil {
		return fmt.Errorf("heap: snapshot: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("heap: snapshot has %d trailing bytes", len(buf)-n)
	}
	return nil
}

// replayLog applies every whole delta in index.log; a torn tail is
// what a crash leaves and is dropped, its slots unnamed.  The caller
// holds the lock.
func (h *HeapStore) replayLog() error {
	buf, err := readFrom(h.log, 0)
	if err != nil {
		return err
	}
	at := 0
	for at < len(buf) {
		n, err := h.applyDelta(buf[at:])
		if err != nil {
			// Torn: keep what is whole, drop the rest
			if err = h.log.Truncate(int64(at)); err != nil {
				return err
			}
			break
		}
		at += n
	}
	if _, err = h.log.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

var errHeapTorn = errors.New("heap: torn index record")

// applyDelta applies one delta or snapshot record and returns its
// length.
func (h *HeapStore) applyDelta(buf []byte) (int, error) {
	const rec = 32 + 8 + 4 + 4
	if len(buf) < 16 || binary.LittleEndian.Uint32(buf) != heapMagic {
		return 0, errHeapTorn
	}
	count := int(binary.LittleEndian.Uint32(buf[12:]))
	end := 16 + count*rec
	if len(buf) < end+4 || crc32.ChecksumIEEE(buf[:end]) != binary.LittleEndian.Uint32(buf[end:]) {
		return 0, errHeapTorn
	}
	if height := binary.LittleEndian.Uint64(buf[4:]); height > h.height {
		h.height = height
	}
	for at := 16; at < end; at += rec {
		var key [32]byte
		copy(key[:], buf[at:])
		h.index[key] = slot{off: int64(binary.LittleEndian.Uint64(buf[at+32:])),
			cap: binary.LittleEndian.Uint32(buf[at+40:]), n: binary.LittleEndian.Uint32(buf[at+44:])}
	}
	return end + 4, nil
}

// deriveHoles rebuilds the free lists and the append point from the
// key map: the file's bytes no live slot covers are holes, split into
// size classes.  The caller holds the lock.
func (h *HeapStore) deriveHoles() error {
	slots := make([]slot, 0, len(h.index))
	for _, s := range h.index {
		slots = append(slots, s)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].off < slots[j].off })
	h.free = [heapClasses][]int64{}
	h.holeBytes, h.liveBytes = 0, 0
	var at int64
	for _, s := range slots {
		if s.off < at {
			return fmt.Errorf("heap: slots overlap at %d", s.off)
		}
		h.gapToHoles(at, s.off)
		h.liveBytes += int64(s.cap)
		at = s.off + int64(s.cap)
	}
	h.size = at
	// Anything past the last live slot is unnamed: a crash's torn
	// writes, or entries of a block whose delta never became durable.
	// The file is cut back to the append point so they are not holes
	// that could be mistaken for anything.
	return h.file.Truncate(at)
}

// gapToHoles splits [from, to) into size-class slots, largest first.
func (h *HeapStore) gapToHoles(from, to int64) {
	for from < to {
		rest := to - from
		capacity := uint32(heapMinCap)
		for int64(capacity)*2 <= rest && capacity < 1<<30 {
			capacity <<= 1
		}
		if int64(capacity) > rest {
			return // A remainder smaller than the smallest slot is lost until a move reclaims it
		}
		h.addHole(from, capacity)
		from += int64(capacity)
	}
}

// Stats maps the heap's counters onto the store's report: every read
// is answered from the key map (LiveHit), there are no segments, and
// the resident memory is the key map.
func (h *HeapStore) Stats() StoreStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return StoreStats{
		PutTotal:           h.putTotal.Load(),
		PutNew:             h.putAppend.Load() + h.putHole.Load(),
		PutDuplicate:       h.putInPlace.Load(),
		LookupTotal:        h.lookups.Load(),
		LiveHit:            h.hits.Load(),
		ResidentBloomBytes: uint64(len(h.index)) * (32 + 24),
	}
}

// HoleRatio reports the heap's free capacity against its live
// capacity, the number a bounded move is scheduled on.
func (h *HeapStore) HoleRatio() (holes, live int64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.holeBytes, h.liveBytes
}

// SetFilterBlocks and SetSealLimit are the segment layer's knobs; a
// heap has neither a window nor a tail.
func (h *HeapStore) SetFilterBlocks(uint64) error { return nil }
func (h *HeapStore) SetSealLimit(uint64) error    { return nil }

// compact is the heap's maintenance on the adapter's cadence: the key
// map snapshot that bounds the replay on open.  (The bounded move
// that makes bigger holes is not written yet; holes cycle by size
// class meanwhile.)
func (h *HeapStore) compact() (bool, error) { return true, h.Snapshot() }

// LiveRecords is the live key count; a heap is never sealed on it.
func (h *HeapStore) LiveRecords() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return uint64(len(h.index))
}
