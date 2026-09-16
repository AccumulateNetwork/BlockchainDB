package blockchainDB

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// The permanent layer's index primitives (proposal
// docs/proposals/2026-09-16-entries-written-once.md, step 2): a
// record that names where an entry lives, sorted runs of records with
// a filter, a k-way merge over runs, and a lookup in a run.  Nothing
// here touches an entry's bytes; entries are written once and only
// ever named.
//
// A run on disk:
//
//	header   magic "PRUN"(4) count(4) bloomBytes(4) bloomK(4) crc(4)
//	records  count sorted 44-byte records: key(32) file(4) off(4) len(4)
//	bloom    bloomBytes of filter over the keys
//
// Runs are immutable once written; a merge writes a new run from
// several and the inputs are dropped by whoever owns them.

// permRecord names an entry: its key and where it is.
type permRecord struct {
	key  [32]byte
	file uint32
	off  uint32
	n    uint32
}

const (
	permRecSize   = 32 + 4 + 4 + 4
	permRunHdr    = 4 + 4 + 4 + 4 + 4
	permRunMagic  = 0x5052554E // "PRUN"
	permBloomBits = 12         // Bits per key in a run's filter
	permBloomK    = 3
)

func (r permRecord) put(buf []byte) {
	copy(buf, r.key[:])
	binary.LittleEndian.PutUint32(buf[32:], r.file)
	binary.LittleEndian.PutUint32(buf[36:], r.off)
	binary.LittleEndian.PutUint32(buf[40:], r.n)
}

func getPermRecord(buf []byte) (r permRecord) {
	copy(r.key[:], buf)
	r.file = binary.LittleEndian.Uint32(buf[32:])
	r.off = binary.LittleEndian.Uint32(buf[36:])
	r.n = binary.LittleEndian.Uint32(buf[40:])
	return r
}

// permRun is a sorted run of records with its filter, as held by a
// bucket: the file it lives in, where, and how many; the filter is
// resident when loaded and probed cold otherwise.
type permRun struct {
	path    string
	off     int64 // Where the run starts in its file
	count   uint32
	bloomAt int64 // Where the filter starts
	bloom   *Bloom
	k       int
	bytes   uint32
}

// writePermRun writes records, which must be sorted by key and free
// of duplicates, as a run appended to w, and returns it.  The caller
// fsyncs the file.
func writePermRun(w io.WriterAt, at int64, path string, recs []permRecord) (*permRun, error) {
	if !sort.SliceIsSorted(recs, func(i, j int) bool { return bytes.Compare(recs[i].key[:], recs[j].key[:]) < 0 }) {
		return nil, errors.New("perm run: records are not sorted")
	}
	bloom := NewBloomSizedForKeys(uint64(len(recs)), permBloomK)
	buf := make([]byte, permRunHdr+len(recs)*permRecSize+int(bloom.NumBytes))
	binary.LittleEndian.PutUint32(buf, permRunMagic)
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(recs)))
	binary.LittleEndian.PutUint32(buf[8:], uint32(bloom.NumBytes))
	binary.LittleEndian.PutUint32(buf[12:], uint32(bloom.K))
	p := permRunHdr
	for i, r := range recs {
		if i > 0 && recs[i-1].key == r.key {
			return nil, errors.New("perm run: duplicate key")
		}
		r.put(buf[p:])
		bloom.Set(r.key)
		p += permRecSize
	}
	copy(buf[p:], bloom.Map)
	binary.LittleEndian.PutUint32(buf[16:], crc32.ChecksumIEEE(buf[permRunHdr:]))
	if _, err := w.WriteAt(buf, at); err != nil {
		return nil, err
	}
	return &permRun{path: path, off: at, count: uint32(len(recs)), bloomAt: at + int64(p), bloom: bloom, k: bloom.K, bytes: uint32(len(buf))}, nil
}

// openPermRun reads a run's header at off in f and verifies the run.
func openPermRun(f *os.File, path string, off int64, resident bool) (*permRun, error) {
	hdr := make([]byte, permRunHdr)
	if _, err := f.ReadAt(hdr, off); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(hdr) != permRunMagic {
		return nil, fmt.Errorf("perm run at %s:%d: bad magic", path, off)
	}
	r := &permRun{path: path, off: off, count: binary.LittleEndian.Uint32(hdr[4:]), k: int(binary.LittleEndian.Uint32(hdr[12:]))}
	bloomBytes := binary.LittleEndian.Uint32(hdr[8:])
	r.bloomAt = off + permRunHdr + int64(r.count)*permRecSize
	r.bytes = permRunHdr + r.count*permRecSize + bloomBytes
	body := make([]byte, r.bytes-permRunHdr)
	if _, err := f.ReadAt(body, off+permRunHdr); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(hdr[16:]) {
		return nil, fmt.Errorf("perm run at %s:%d: checksum failed", path, off)
	}
	if resident {
		// Rebuilt from the stored byte count: ByteMask indexes by NumBytes
		r.bloom = &Bloom{NumBytes: uint64(bloomBytes), SizeOfMap: float64(bloomBytes) / (1 << 20), K: r.k,
			Map: make([]byte, bloomBytes), Capacity: uint64(bloomBytes) * 8 / BloomBitsPerKey, Count: uint64(r.count)}
		copy(r.bloom.Map, body[int64(r.count)*permRecSize:])
	}
	return r, nil
}

// lookup finds key in the run: the filter first (resident, or cold in
// the file), then a binary search over the records.
func (r *permRun) lookup(f *os.File, key [32]byte) (rec permRecord, found bool, err error) {
	if r.bloom != nil {
		if !r.bloom.Test(key) {
			return rec, false, nil
		}
	} else if ok, err := r.bloomTestCold(f, key); err != nil || !ok {
		return rec, false, err
	}
	lo, hi := int64(0), int64(r.count)
	buf := make([]byte, permRecSize)
	for lo < hi {
		mid := (lo + hi) / 2
		if _, err := f.ReadAt(buf, r.off+permRunHdr+mid*permRecSize); err != nil {
			return rec, false, err
		}
		switch c := bytes.Compare(buf[:32], key[:]); {
		case c == 0:
			return getPermRecord(buf), true, nil
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return rec, false, nil
}

// bloomTestCold probes the run's filter in the file: k one-byte
// reads, the way the segment store probes a cold filter.
func (r *permRun) bloomTestCold(f *os.File, key [32]byte) (bool, error) {
	bloomBytes := r.bytes - permRunHdr - r.count*permRecSize
	probe := &Bloom{NumBytes: uint64(bloomBytes), SizeOfMap: float64(bloomBytes) * 8, K: r.k}
	one := make([]byte, 1)
	for i := 0; i < r.k; i++ {
		idx, mask := probe.ByteMask(key, i)
		if _, err := f.ReadAt(one, r.bloomAt+int64(idx)); err != nil {
			return false, err
		}
		if one[0]&mask == 0 {
			return false, nil
		}
	}
	return true, nil
}

// records reads a run's records in order, for a merge.
func (r *permRun) records(f *os.File) ([]permRecord, error) {
	buf := make([]byte, int64(r.count)*permRecSize)
	if _, err := f.ReadAt(buf, r.off+permRunHdr); err != nil {
		return nil, err
	}
	recs := make([]permRecord, r.count)
	for i := range recs {
		recs[i] = getPermRecord(buf[i*permRecSize:])
	}
	return recs, nil
}

// mergePermRuns merges sorted record lists, oldest first, into one
// sorted list; a key present in several takes the newest.  Permanent
// keys are written once, so a duplicate is a replay or a fault, and
// newest-wins matches the store's rule everywhere else.
func mergePermRuns(inputs [][]permRecord) []permRecord {
	h := &permMergeHeap{}
	total := 0
	for src, in := range inputs {
		total += len(in)
		if len(in) > 0 {
			heap.Push(h, permMergeCursor{src: src, recs: in})
		}
	}
	out := make([]permRecord, 0, total)
	for h.Len() > 0 {
		c := heap.Pop(h).(permMergeCursor)
		r := c.recs[0]
		if n := len(out); n == 0 || out[n-1].key != r.key {
			out = append(out, r)
		}
		// The heap orders equal keys newest first, so the first copy
		// out is the one kept and the rest are dropped here
		if len(c.recs) > 1 {
			heap.Push(h, permMergeCursor{src: c.src, recs: c.recs[1:]})
		}
	}
	return out
}

type permMergeCursor struct {
	src  int
	recs []permRecord
}

type permMergeHeap []permMergeCursor

func (h permMergeHeap) Len() int { return len(h) }
func (h permMergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].recs[0].key[:], h[j].recs[0].key[:]); c != 0 {
		return c < 0
	}
	return h[i].src > h[j].src // Newest first on a tie
}
func (h permMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *permMergeHeap) Push(x any)   { *h = append(*h, x.(permMergeCursor)) }
func (h *permMergeHeap) Pop() any     { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }
func sortPermRecords(recs []permRecord) {
	sort.Slice(recs, func(i, j int) bool { return bytes.Compare(recs[i].key[:], recs[j].key[:]) < 0 })
}
