package blockchainDB

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// HeapStore is the dynamic layer as an append-and-clean heap (proposal
// docs/proposals/2026-09-16-entries-written-once.md): entries are
// managed by appending, keys are managed separately, and space is
// reclaimed by a bounded cleaner that moves the live entries out of
// the oldest region and releases it, never by rewriting an index or a
// filter.
//
// Files, in one directory:
//
//	heap.dat   entries: [cap u32][len u32][height u64][key 32][value]
//	           [crc32 of height+key+value], each in a slot of cap bytes
//	           (a size class), self-describing so that Repair can
//	           rebuild the key map from the data alone,
//	           appended in the order written; the file is a sequence of
//	           regions of HeapRegionBytes, and a region whose entries
//	           are all dead is released (a punched hole, size kept)
//	index.log  one delta per block sync: the (key, off, cap, len) of
//	           every key the block touched, checksummed
//	index.snap the whole key map, rewritten on the maintenance cadence
//
// A block's writes are contiguous, so the block sync is one sequential
// fsync of the heap: filling holes wherever they lie was measured at
// 4x the ingest in page writes at every barrier, and is not done.  A
// slot a key stops naming is dead where it lies until the cleaner
// takes its region: the region with the most dead bytes, once at
// least HeapCleanRatio of it is dead, so a pass copies little.
// Cleaning the oldest region regardless was measured too: with a
// skewed key set the oldest region is mostly live cold keys, and every
// pass copied most of it into one block's sync.
//
// Durability (spec 1.8).  The block sync fsyncs heap.dat and then
// appends and fsyncs the block's delta, so an index entry is durable
// only after the slot it names is.  A slot the last durable index
// names is never overwritten: a key rewritten in a later block takes
// a new slot at the end, and its old slot is dead but intact until its
// region is released -- and a region is released only after the delta
// naming the cleaner's copies is durable, one sync late, the way spec
// 2.6 defers deletion.  A key rewritten again within the same block
// reuses the slot it took this block, since nothing durable names it
// yet.  A crash therefore leaves every durable entry intact and every
// torn slot unnamed, and the checksum catches a torn slot that a
// stale index could name.
type HeapStore struct {
	Directory string

	mu     sync.RWMutex
	file   *os.File // heap.dat
	log    *os.File // index.log
	size   int64    // Append point: the end of heap.dat
	index  map[[32]byte]slot
	height uint64 // The block being written; slots taken in it may be rewritten in place

	// regions is the live and dead capacity in each HeapRegionBytes of
	// the file, what the cleaner chooses by; released marks the
	// regions punched.  touched is the block's delta in the making;
	// release holds the regions emptied by the last pass, punched once
	// the delta naming their copies is durable; snapshots counts
	// compact calls between snapshots.
	regions   []region
	touched   map[[32]byte]struct{}
	release   []int
	snapshots int
	syncedTo  int64 // The append point at the last sync

	closed    bool
	liveBytes int64 // Capacity of every named slot

	putTotal, putInPlace, putAppend atomic.Uint64
	lookups, hits                   atomic.Uint64
	cleanedBytes, movedBytes        atomic.Uint64
	// The sync's cost, split: nanoseconds in the heap's fsync and in
	// the delta's write and fsync, and the syncs and bytes they covered
	syncs, syncHeapNs, syncLogNs, syncBytes atomic.Uint64
}

// region is the accounting for one HeapRegionBytes of the heap.
type region struct {
	live, dead int64
	released   bool
	cleaning   bool // Taken by the pass in progress
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
	heapHeader   = 4 + 4 + 8 + 32 // cap, len, height, key
	heapTrailer  = 4              // crc32 of key+value
	heapMinCap   = 64             // Smallest slot
	heapMagic    = 0x48454150     // "HEAP", the delta record's marker
	heapDeltaHdr = 4 + 8 + 8 + 4  // magic, height, reserved, count
	heapDeltaRec = 32 + 8 + 4 + 4
)

// HeapRegionBytes is the size of the regions the heap is cleaned and
// released by.
var HeapRegionBytes int64 = 4 << 20

// HeapCleanBytes bounds one cleaning pass by the bytes it COPIES: the
// most a pass can add to the next block's sync.  A pass takes region
// after region until the budget is spent, so mostly-dead regions,
// which cost little to copy, are released several to a pass.
var HeapCleanBytes int64 = 2 << 20

// HeapCleanRegions bounds a pass by regions taken as well, so that a
// heap of wholly dead regions is not scanned end to end under the
// lock in one pass.
var HeapCleanRegions = 8

// HeapCleanRatio is the dead fraction a region must reach before the
// cleaner takes it: what bounds the copying a pass does per byte it
// releases.
var HeapCleanRatio = 0.5

// HeapSnapshotEvery is how many compact calls pass between key-map
// snapshots; between them the log is what open replays.
var HeapSnapshotEvery = 5

// heapCap is the size class that holds need bytes: a power of two no
// smaller than heapMinCap.
func heapCap(need int) uint32 {
	capacity := uint32(heapMinCap)
	for int(capacity) < need {
		capacity <<= 1
	}
	return capacity
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

// OpenHeapStore opens the heap in directory as it was left.  A heap
// whose data is there but whose index files are not is refused: that
// is what RepairHeapStore is for, and it needs the committed height,
// which only the store above knows.
func OpenHeapStore(directory string) (*HeapStore, error) {
	st, err := os.Stat(filepath.Join(directory, "heap.dat"))
	if err != nil {
		return nil, fmt.Errorf("open heap at %s: %w", directory, err)
	}
	_, snapErr := os.Stat(filepath.Join(directory, "index.snap"))
	_, logErr := os.Stat(filepath.Join(directory, "index.log"))
	if st.Size() > 0 && snapErr != nil && logErr != nil {
		return nil, fmt.Errorf("open heap at %s: %w", directory, ErrHeapNeedsRepair)
	}
	h := &HeapStore{Directory: directory}
	return h, h.Open()
}

// ErrHeapNeedsRepair says the heap's data is there and its index is
// not: RepairHeapStore rebuilds the index from the data.
var ErrHeapNeedsRepair = errors.New("heap has data but no index; repair it")

// RepairHeapStore rebuilds the key map by reading the keys from the
// data: every entry carries its key, its height and a checksum, so a
// sequential scan recovers the map without any index.  For each key
// the copy with the highest height wins, and an entry above the
// committed height -- a block whose sync never finished -- is
// dropped, as is any torn or damaged slot.  The rebuilt map is
// snapshotted, so the next open is an ordinary one.
func RepairHeapStore(directory string, committed uint64) (*HeapStore, error) {
	os.Remove(filepath.Join(directory, "index.snap"))
	os.Remove(filepath.Join(directory, "index.log"))
	h := &HeapStore{Directory: directory}
	var err error
	if h.file, err = os.OpenFile(filepath.Join(directory, "heap.dat"), os.O_RDWR, 0o644); err != nil {
		return nil, err
	}
	if h.log, err = os.OpenFile(filepath.Join(directory, "index.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644); err != nil {
		return nil, err
	}
	h.index = map[[32]byte]slot{}
	h.touched = map[[32]byte]struct{}{}
	data, err := readFrom(h.file, 0)
	if err != nil {
		return nil, err
	}
	heights := map[[32]byte]uint64{}
	var off int64
	for off < int64(len(data)) {
		capacity, height, key, value, ok := decodeEntry(data[off:])
		if !ok {
			// A released region reads as zeros; skip to the next region
			// boundary.  Anything else torn ends the scan: nothing past
			// a torn slot was named before it.
			if binary.LittleEndian.Uint32(data[off:]) == 0 {
				off = (off/HeapRegionBytes + 1) * HeapRegionBytes
				continue
			}
			break
		}
		if height <= committed {
			if prev, seen := heights[key]; !seen || height >= prev {
				heights[key] = height
				h.index[key] = slot{off: off, cap: capacity, n: uint32(len(value))}
			}
		}
		off += int64(capacity)
	}
	h.height = committed
	if err = h.deriveExtent(); err != nil {
		return nil, err
	}
	if err = h.Snapshot(); err != nil {
		return nil, err
	}
	return h, nil
}

// Open loads the key map from the snapshot and the log and derives
// the append point.  Idempotent.
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
	h.size, h.liveBytes, h.regions, h.release, h.syncedTo = 0, 0, nil, nil, 0
	if err = h.loadSnapshot(); err != nil {
		return err
	}
	if err = h.replayLog(); err != nil {
		return err
	}
	return h.deriveExtent()
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

// encodeEntry lays out one entry for its slot: the block that wrote
// it is in the header so that, with the index gone, a scan can tell
// the current copy of a key (the highest height) from stale ones and
// a committed entry from one whose block never synced.
func encodeEntry(capacity uint32, height uint64, key [32]byte, value []byte) []byte {
	buf := make([]byte, heapHeader+len(value)+heapTrailer)
	binary.LittleEndian.PutUint32(buf, capacity)
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(value)))
	binary.LittleEndian.PutUint64(buf[8:], height)
	copy(buf[16:], key[:])
	copy(buf[heapHeader:], value)
	binary.LittleEndian.PutUint32(buf[heapHeader+len(value):], crc32.ChecksumIEEE(buf[8:heapHeader+len(value)]))
	return buf
}

// decodeEntry checks the entry at the start of buf and returns its
// fields; ok is false for an unwritten, torn or damaged slot.
func decodeEntry(buf []byte) (capacity uint32, height uint64, key [32]byte, value []byte, ok bool) {
	if len(buf) < heapHeader {
		return
	}
	capacity = binary.LittleEndian.Uint32(buf)
	n := int(binary.LittleEndian.Uint32(buf[4:]))
	if capacity == 0 || heapHeader+n+heapTrailer > int(capacity) || int(capacity) > len(buf) {
		return
	}
	if crc32.ChecksumIEEE(buf[8:heapHeader+n]) != binary.LittleEndian.Uint32(buf[heapHeader+n:]) {
		return
	}
	height = binary.LittleEndian.Uint64(buf[8:])
	copy(key[:], buf[16:])
	return capacity, height, key, buf[heapHeader : heapHeader+n], true
}

// Put writes value under key: in place if the key took its slot this
// block and the value fits, else appended.  The slot a durable index
// names is never rewritten.
func (h *HeapStore) Put(key [32]byte, value []byte) error {
	need := heapHeader + len(value) + heapTrailer
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errStoreClosed
	}
	h.putTotal.Add(1)
	old, had := h.index[key]
	var s slot
	if had && old.block == h.height && int(old.cap) >= need {
		// Taken this block, nothing durable names it: rewrite in place
		s = old
		s.n = uint32(len(value))
		h.putInPlace.Add(1)
	} else {
		s = slot{off: h.size, cap: heapCap(need), n: uint32(len(value)), block: h.height}
		h.append(s)
		if had {
			h.kill(old) // Dead where it lies
		}
		h.putAppend.Add(1)
	}
	if _, err := h.file.WriteAt(encodeEntry(s.cap, h.height, key, value), s.off); err != nil {
		return err
	}
	h.index[key] = s
	h.touched[key] = struct{}{}
	return nil
}

// append accounts a slot taken at the end of the file.  The caller
// holds the lock and has set s.off to the append point.
func (h *HeapStore) append(s slot) {
	h.size += int64(s.cap)
	h.liveBytes += int64(s.cap)
	r := int(s.off / HeapRegionBytes)
	for len(h.regions) <= r {
		h.regions = append(h.regions, region{})
	}
	h.regions[r].live += int64(s.cap)
}

// kill accounts a slot its key stopped naming.  The caller holds the
// lock.
func (h *HeapStore) kill(s slot) {
	h.liveBytes -= int64(s.cap)
	r := &h.regions[int(s.off/HeapRegionBytes)]
	r.live -= int64(s.cap)
	r.dead += int64(s.cap)
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
	n := int(binary.LittleEndian.Uint32(buf[4:]))
	if len(buf) != heapHeader+n+heapTrailer || [32]byte(buf[16:heapHeader]) != key {
		return nil, fmt.Errorf("heap: slot does not hold the key it is named for")
	}
	if crc32.ChecksumIEEE(buf[8:heapHeader+n]) != binary.LittleEndian.Uint32(buf[heapHeader+n:]) {
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
// the regions to release once it is.
type heapSync struct {
	h       *HeapStore
	delta   []byte
	release []int
	bytes   int64 // Appended since the last sync: what the heap's fsync covers
}

// beginBlockSync takes the block's delta under the lock; finish makes
// it durable outside it.
func (h *HeapStore) beginBlockSync() (blockSync, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.file == nil {
		return nil, errStoreClosed
	}
	p := &heapSync{h: h, release: h.release, bytes: h.size - h.syncedTo}
	h.syncedTo = h.size
	h.release = nil
	if len(h.touched) > 0 || len(p.release) > 0 {
		p.delta = h.encodeDelta()
		h.touched = map[[32]byte]struct{}{}
	}
	return p, nil
}

// encodeDelta is the block's index delta: marker, height, a reserved
// word, the touched keys' slots, and a checksum.  The caller holds
// the lock.
func (h *HeapStore) encodeDelta() []byte {
	buf := make([]byte, heapDeltaHdr+len(h.touched)*heapDeltaRec+4)
	binary.LittleEndian.PutUint32(buf, heapMagic)
	binary.LittleEndian.PutUint64(buf[4:], h.height)
	binary.LittleEndian.PutUint32(buf[20:], uint32(len(h.touched)))
	at := heapDeltaHdr
	for key := range h.touched {
		at += putDeltaRec(buf[at:], key, h.index[key])
	}
	binary.LittleEndian.PutUint32(buf[at:], crc32.ChecksumIEEE(buf[:at]))
	return buf
}

func putDeltaRec(buf []byte, key [32]byte, s slot) int {
	copy(buf, key[:])
	binary.LittleEndian.PutUint64(buf[32:], uint64(s.off))
	binary.LittleEndian.PutUint32(buf[40:], s.cap)
	binary.LittleEndian.PutUint32(buf[44:], s.n)
	return heapDeltaRec
}

// finish: entries durable, then the delta durable, then the regions
// the delta's copies emptied are released.
func (p *heapSync) finish() error {
	h := p.h
	if p.delta == nil {
		return nil
	}
	t := time.Now()
	if err := fsync(h.file); err != nil {
		return err
	}
	h.syncHeapNs.Add(uint64(time.Since(t)))
	t = time.Now()
	if _, err := h.log.Write(p.delta); err != nil {
		return err
	}
	if err := fsync(h.log); err != nil {
		return err
	}
	h.syncLogNs.Add(uint64(time.Since(t)))
	h.syncs.Add(1)
	h.syncBytes.Add(uint64(p.bytes))
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range p.release {
		if err := punchHole(h.file, int64(r)*HeapRegionBytes, HeapRegionBytes); err != nil {
			return err
		}
		h.regions[r] = region{released: true}
	}
	return nil
}

// compact is the heap's maintenance on the adapter's cadence: one
// bounded cleaning pass, and every HeapSnapshotEvery calls the key-map
// snapshot that bounds the replay on open.
func (h *HeapStore) compact() (bool, error) {
	cleaned, err := h.clean(HeapCleanBytes)
	if err != nil {
		return cleaned, err
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
	return cleaned, err
}

// clean takes the region with the most dead bytes, once at least
// HeapCleanRatio of it is dead, re-appends its live entries -- at most
// budget bytes of them, the rest next pass -- and, when none are left,
// marks it for release at the next sync.  The cost of a pass is bounded
// by the copies it makes and by the ratio: a byte released never costs
// more than a byte copied (spec 1.2).  Holds the lock for the pass:
// bounded, and the copies are ordinary appends.
func (h *HeapStore) clean(budget int64) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, errStoreClosed
	}
	if len(h.release) > 0 {
		return false, nil // The last pass's release is still waiting on a sync
	}
	var moved int64
	taken := 0
	for taken < HeapCleanRegions && moved < budget {
		pick := h.pickRegion()
		if pick < 0 {
			break
		}
		m, err := h.cleanRegion(pick, budget-moved)
		if err != nil {
			return taken > 0, err
		}
		moved += m
		taken++
	}
	for i := range h.regions {
		h.regions[i].cleaning = false
	}
	return taken > 0, nil
}

// pickRegion is the region with the most dead bytes, once at least
// HeapCleanRatio of it is dead; -1 when none qualifies.  The region
// the block in progress is appending to is never taken: a slot taken
// this block may still be rewritten in place, and moving it would
// race that.  A region a pass has already partly cleaned keeps its
// dead bytes and is picked again.  The caller holds the lock.
func (h *HeapStore) pickRegion() int {
	current := int(h.size / HeapRegionBytes)
	pick, best := -1, 0.0
	for i, r := range h.regions {
		if i >= current || r.released || r.dead == 0 || r.cleaning {
			continue
		}
		if f := float64(r.dead) / float64(r.dead+r.live); f >= HeapCleanRatio && f > best {
			pick, best = i, f
		}
	}
	return pick
}

// cleanRegion re-appends the live entries of one region, at most
// budget bytes of them, and marks the region for release when none
// are left.  Returns the bytes copied.  The caller holds the lock.
func (h *HeapStore) cleanRegion(pick int, budget int64) (int64, error) {
	from := int64(pick) * HeapRegionBytes
	to := from + HeapRegionBytes
	if to > h.size {
		to = h.size
	}
	buf := make([]byte, to-from)
	if _, err := h.file.ReadAt(buf, from); err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	h.regions[pick].cleaning = true // Not picked again this pass
	var moved, at int64
	for at+heapHeader <= int64(len(buf)) {
		capacity := binary.LittleEndian.Uint32(buf[at:])
		if capacity == 0 || at+int64(capacity) > int64(len(buf)) {
			break // Unwritten, or a slot that straddles the region
		}
		var key [32]byte
		copy(key[:], buf[at+16:])
		if s, live := h.index[key]; live && s.off == from+at {
			if moved >= budget {
				h.cleanedBytes.Add(uint64(at))
				h.movedBytes.Add(uint64(moved))
				return moved, nil // The rest next pass
			}
			n := binary.LittleEndian.Uint32(buf[at+4:])
			// The copy is this block's write of the key: it carries
			// this height, so a repair scan prefers it to the original
			value := buf[at+int64(heapHeader) : at+int64(heapHeader)+int64(n)]
			ns := slot{off: h.size, cap: capacity, n: n, block: h.height}
			if _, err := h.file.WriteAt(encodeEntry(capacity, h.height, key, value), ns.off); err != nil {
				return moved, err
			}
			h.append(ns)
			h.kill(s)
			h.index[key] = ns
			h.touched[key] = struct{}{}
			moved += int64(capacity)
		}
		at += int64(capacity)
	}
	h.cleanedBytes.Add(uint64(at))
	h.movedBytes.Add(uint64(moved))
	if h.regions[pick].live == 0 {
		h.release = append(h.release, pick)
	}
	return moved, nil
}

// Snapshot writes the whole key map and drops the deltas it covers
// from the log, so that the replay on open stays bounded.  Off the
// protocol path, on the maintenance cadence.
func (h *HeapStore) Snapshot() error {
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return errStoreClosed
	}
	buf := make([]byte, heapDeltaHdr+len(h.index)*heapDeltaRec+4)
	binary.LittleEndian.PutUint32(buf, heapMagic)
	binary.LittleEndian.PutUint64(buf[4:], h.height)
	binary.LittleEndian.PutUint32(buf[20:], uint32(len(h.index)))
	at := heapDeltaHdr
	for key, s := range h.index {
		at += putDeltaRec(buf[at:], key, s)
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
	// was taken; deltas appended since stay
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
	if len(buf) < heapDeltaHdr || binary.LittleEndian.Uint32(buf) != heapMagic {
		return 0, errHeapTorn
	}
	count := int(binary.LittleEndian.Uint32(buf[20:]))
	end := heapDeltaHdr + count*heapDeltaRec
	if len(buf) < end+4 || crc32.ChecksumIEEE(buf[:end]) != binary.LittleEndian.Uint32(buf[end:]) {
		return 0, errHeapTorn
	}
	if height := binary.LittleEndian.Uint64(buf[4:]); height > h.height {
		h.height = height
	}
	for at := heapDeltaHdr; at < end; at += heapDeltaRec {
		var key [32]byte
		copy(key[:], buf[at:])
		h.index[key] = slot{off: int64(binary.LittleEndian.Uint64(buf[at+32:])),
			cap: binary.LittleEndian.Uint32(buf[at+40:]), n: binary.LittleEndian.Uint32(buf[at+44:])}
	}
	return end + 4, nil
}

// deriveExtent finds the append point and the regions' accounting
// from the key map and cuts the file back to it: anything past the
// last named slot is a crash's torn writes or the entries of a block
// whose delta never became durable, and must not be read as anything.
// A region with nothing live is released (idempotent for one already
// punched).  The caller holds the lock.
func (h *HeapStore) deriveExtent() error {
	var end int64
	h.liveBytes, h.regions, h.release = 0, nil, nil
	for _, s := range h.index {
		if e := s.off + int64(s.cap); e > end {
			end = e
		}
		h.liveBytes += int64(s.cap)
		r := int(s.off / HeapRegionBytes)
		for len(h.regions) <= r {
			h.regions = append(h.regions, region{})
		}
		h.regions[r].live += int64(s.cap)
	}
	h.size, h.syncedTo = end, end
	if err := h.file.Truncate(end); err != nil {
		return err
	}
	for i := range h.regions {
		extent := HeapRegionBytes
		if e := end - int64(i)*HeapRegionBytes; e < extent {
			extent = e
		}
		h.regions[i].dead = extent - h.regions[i].live
		if h.regions[i].live == 0 && int64(i+1)*HeapRegionBytes <= end {
			if err := punchHole(h.file, int64(i)*HeapRegionBytes, HeapRegionBytes); err != nil {
				return err
			}
			h.regions[i] = region{released: true}
		}
	}
	return nil
}

// Stats maps the heap's counters onto the store's report: every read
// is answered from the key map (LiveHit), there are no segments, and
// the resident memory is the key map.
func (h *HeapStore) Stats() StoreStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return StoreStats{
		PutTotal:           h.putTotal.Load(),
		PutNew:             h.putAppend.Load(),
		PutDuplicate:       h.putInPlace.Load(),
		LookupTotal:        h.lookups.Load(),
		LiveHit:            h.hits.Load(),
		ResidentBloomBytes: uint64(len(h.index)) * (32 + 24),
	}
}

// HoleRatio reports the dead bytes in unreleased regions against the
// live bytes: what the cleaner has yet to reclaim.
func (h *HeapStore) HoleRatio() (dead, live int64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, r := range h.regions {
		dead += r.dead
	}
	return dead, h.liveBytes
}

// SyncCost reports the block syncs so far: how many, the bytes their
// heap fsyncs covered, and the time spent in the heap's fsync and in
// the delta's write and fsync.
func (h *HeapStore) SyncCost() (syncs, bytes uint64, heapFsync, delta time.Duration) {
	return h.syncs.Load(), h.syncBytes.Load(), time.Duration(h.syncHeapNs.Load()), time.Duration(h.syncLogNs.Load())
}

// Cleaned reports what the cleaner has scanned and what it had to
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
