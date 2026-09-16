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
//	heap.dat   entries: [cap u32][len u32][key 32][value][crc32 of
//	           key+value], each in a slot of cap bytes (a size class),
//	           appended in the order written; the head is the oldest
//	           byte still in use, and everything before it is released
//	index.log  one delta per block sync: the head, and the (key, off,
//	           cap, len) of every key the block touched, checksummed
//	index.snap the whole key map, rewritten on the maintenance cadence
//
// A block's writes are contiguous, so the block sync is one sequential
// fsync of the heap: filling holes wherever they lie was measured at
// 4x the ingest in page writes at every barrier, and is not done.  A
// slot a key stops naming is dead where it lies until the cleaner
// reaches it.
//
// Durability (spec 1.8).  The block sync fsyncs heap.dat and then
// appends and fsyncs the block's delta, so an index entry is durable
// only after the slot it names is.  A slot the last durable index
// names is never overwritten: a key rewritten in a later block takes
// a new slot at the end, and its old slot is dead but intact until
// the head passes it -- and the head advances only after the delta
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
	head   int64    // The oldest byte in use; everything before it is released
	index  map[[32]byte]slot
	height uint64 // The block being written; slots taken in it may be rewritten in place

	// touched is the block's delta in the making; cleanedTo is where
	// the head moves once the delta naming the cleaner's copies is
	// durable; snapshots counts compact calls between snapshots.
	touched   map[[32]byte]struct{}
	cleanedTo int64
	snapshots int

	closed    bool
	liveBytes int64 // Capacity of every named slot

	putTotal, putInPlace, putAppend atomic.Uint64
	lookups, hits                   atomic.Uint64
	cleanedBytes, movedBytes        atomic.Uint64
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
	heapHeader   = 4 + 4 + 32    // cap, len, key
	heapTrailer  = 4             // crc32 of key+value
	heapMinCap   = 64            // Smallest slot
	heapMagic    = 0x48454150    // "HEAP", the delta record's marker
	heapDeltaHdr = 4 + 8 + 8 + 4 // magic, height, head, count
	heapDeltaRec = 32 + 8 + 4 + 4
)

// HeapCleanBytes bounds one cleaning pass: the bytes of the oldest
// region scanned, of which only the live entries are copied.  Sized so
// the cleaner keeps up with the adapter's cadence on the soak's
// volume (~10 MB appended per shard per 20 blocks) with room to spare.
var HeapCleanBytes int64 = 16 << 20

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

// OpenHeapStore opens the heap in directory as it was left.
func OpenHeapStore(directory string) (*HeapStore, error) {
	if _, err := os.Stat(filepath.Join(directory, "heap.dat")); err != nil {
		return nil, fmt.Errorf("open heap at %s: %w", directory, err)
	}
	h := &HeapStore{Directory: directory}
	return h, h.Open()
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
	h.head, h.size, h.liveBytes, h.cleanedTo = 0, 0, 0, 0
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

// encodeEntry lays out one entry for its slot.
func encodeEntry(capacity uint32, key [32]byte, value []byte) []byte {
	buf := make([]byte, heapHeader+len(value)+heapTrailer)
	binary.LittleEndian.PutUint32(buf, capacity)
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(value)))
	copy(buf[8:], key[:])
	copy(buf[heapHeader:], value)
	binary.LittleEndian.PutUint32(buf[heapHeader+len(value):], crc32.ChecksumIEEE(buf[8:heapHeader+len(value)]))
	return buf
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
		h.size += int64(s.cap)
		h.liveBytes += int64(s.cap)
		if had {
			h.liveBytes -= int64(old.cap) // Dead where it lies
		}
		h.putAppend.Add(1)
	}
	if _, err := h.file.WriteAt(encodeEntry(s.cap, key, value), s.off); err != nil {
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
	n := int(binary.LittleEndian.Uint32(buf[4:]))
	if len(buf) != heapHeader+n+heapTrailer || [32]byte(buf[8:heapHeader]) != key {
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
// the head to release once it is.
type heapSync struct {
	h         *HeapStore
	delta     []byte
	cleanedTo int64
}

// beginBlockSync takes the block's delta under the lock; finish makes
// it durable outside it.
func (h *HeapStore) beginBlockSync() (blockSync, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.file == nil {
		return nil, errStoreClosed
	}
	p := &heapSync{h: h, cleanedTo: h.cleanedTo}
	if len(h.touched) > 0 || h.cleanedTo > h.head {
		p.delta = h.encodeDelta()
		h.touched = map[[32]byte]struct{}{}
	}
	return p, nil
}

// encodeDelta is the block's index delta: marker, height, the head
// the block's copies let the heap release, the touched keys' slots,
// and a checksum.  The caller holds the lock.
func (h *HeapStore) encodeDelta() []byte {
	buf := make([]byte, heapDeltaHdr+len(h.touched)*heapDeltaRec+4)
	binary.LittleEndian.PutUint32(buf, heapMagic)
	binary.LittleEndian.PutUint64(buf[4:], h.height)
	binary.LittleEndian.PutUint64(buf[12:], uint64(h.cleanedTo))
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

// finish: entries durable, then the delta durable, then the head the
// delta records is released.
func (p *heapSync) finish() error {
	h := p.h
	if p.delta == nil {
		return nil
	}
	if err := fsync(h.file); err != nil {
		return err
	}
	if _, err := h.log.Write(p.delta); err != nil {
		return err
	}
	if err := fsync(h.log); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if p.cleanedTo > h.head {
		if err := punchHole(h.file, h.head, p.cleanedTo-h.head); err != nil {
			return err
		}
		h.head = p.cleanedTo
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

// clean scans up to budget bytes from the head, re-appends the entries
// still live, and marks the region for release at the next sync.  The
// cost of a pass is the live fraction of the oldest region: for a hot
// key set rewritten every block it is small, and for a cold one it is
// the price of a bounded move (spec 1.2).  Holds the lock for the
// pass: bounded, and the copies are ordinary appends.
func (h *HeapStore) clean(budget int64) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, errStoreClosed
	}
	if h.cleanedTo > h.head {
		return false, nil // The last pass's release is still waiting on a sync
	}
	from, to := h.head, h.head+budget
	if to > h.size {
		to = h.size
	}
	if from >= to || h.liveBytes == 0 {
		return false, nil
	}
	// The pass never eats the block in progress: a slot taken this
	// block may still be rewritten in place, and moving it would race
	// that.  Stop at the first slot of the current block.
	region := make([]byte, to-from)
	if _, err := h.file.ReadAt(region, from); err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	var off = from
	var moved int64
	for off < to {
		at := off - from
		if at+heapHeader > int64(len(region)) {
			break
		}
		capacity := binary.LittleEndian.Uint32(region[at:])
		n := binary.LittleEndian.Uint32(region[at+4:])
		if capacity == 0 || at+int64(capacity) > int64(len(region)) {
			break // Unwritten, or a slot that straddles the budget: next pass
		}
		var key [32]byte
		copy(key[:], region[at+8:])
		s, live := h.index[key]
		if live && s.off == off {
			if s.block == h.height {
				break
			}
			entry := region[at : at+int64(heapHeader)+int64(n)+heapTrailer]
			ns := slot{off: h.size, cap: capacity, n: n, block: h.height}
			if _, err := h.file.WriteAt(entry, ns.off); err != nil {
				return false, err
			}
			h.size += int64(capacity)
			h.index[key] = ns
			h.touched[key] = struct{}{}
			moved += int64(capacity)
		}
		off += int64(capacity)
	}
	if off == from {
		return false, nil
	}
	h.cleanedTo = off
	h.cleanedBytes.Add(uint64(off - from))
	h.movedBytes.Add(uint64(moved))
	return true, nil
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
	binary.LittleEndian.PutUint64(buf[12:], uint64(h.head))
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
	if head := int64(binary.LittleEndian.Uint64(buf[12:])); head > h.head {
		h.head = head
	}
	for at := heapDeltaHdr; at < end; at += heapDeltaRec {
		var key [32]byte
		copy(key[:], buf[at:])
		h.index[key] = slot{off: int64(binary.LittleEndian.Uint64(buf[at+32:])),
			cap: binary.LittleEndian.Uint32(buf[at+40:]), n: binary.LittleEndian.Uint32(buf[at+44:])}
	}
	return end + 4, nil
}

// deriveExtent finds the append point from the key map and cuts the
// file back to it: anything past the last named slot is a crash's
// torn writes or the entries of a block whose delta never became
// durable, and must not be read as anything.  The caller holds the
// lock.
func (h *HeapStore) deriveExtent() error {
	var end int64
	h.liveBytes = 0
	for _, s := range h.index {
		if s.off < h.head {
			return fmt.Errorf("heap: a live slot at %d lies below the head %d", s.off, h.head)
		}
		if e := s.off + int64(s.cap); e > end {
			end = e
		}
		h.liveBytes += int64(s.cap)
	}
	h.size = end
	h.cleanedTo = h.head
	if err := h.file.Truncate(end); err != nil {
		return err
	}
	if h.head > 0 {
		return punchHole(h.file, 0, h.head) // Idempotent: the region is already released
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

// HoleRatio reports the dead bytes between the head and the append
// point against the live bytes: what the cleaner has yet to reclaim.
func (h *HeapStore) HoleRatio() (dead, live int64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.size - h.head - h.liveBytes, h.liveBytes
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
