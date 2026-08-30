package blockchainDB

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"os"
)

// Streaming merges over sorted segment indexes.
//
// Every sealed segment's index is sorted by key, so a merge of several
// segments is a k-way merge of sorted runs: hold one cursor per input,
// always take the smallest key, and where the same key appears in more
// than one input keep the newest.  Memory is one read buffer per input,
// whatever the inputs hold.
//
// This replaces two maps that held EVERY key of a merge -- one to pick
// the winners, one to remember where each landed -- plus a slice of the
// keys to sort them.  On an 8-node Accumulate network at 500 tx/s a
// single compaction of a 5.3 GB dynamic layer held 2.0 GB of those maps
// on a node with a 2.5 GB limit, the garbage collector took 76% of the
// CPU trying to keep under it, and the partition wedged (issue #59).
// The same maps had existed before the store lock was split (#57); then
// the node stopped allocating while compaction held the lock, and the
// pause hid the memory.

// indexReadRecords is how many index records a cursor reads at a time:
// 1,024 records is 48 KB, a dozen sequential pages per refill.  It is
// the whole per-input cost of a merge, so a run of a hundred segments
// reads through ~5 MB of buffers however many records it holds.
const indexReadRecords = 1024

// indexCursor walks one segment's index in key order
type indexCursor struct {
	seg     *segment
	src     int // Position among the inputs; higher is newer
	file    *os.File
	release func()
	buf     []byte
	pos     int   // Byte offset of the current record in buf
	loaded  int   // Bytes of buf that hold records
	next    int64 // Index of the first record not yet read from the file
	key     [32]byte
	dbb     DBBKey
	ok      bool // A record is loaded in key/dbb
}

func openIndexCursor(seg *segment, src int) (c *indexCursor, err error) {
	c = &indexCursor{seg: seg, src: src, buf: make([]byte, indexReadRecords*DBKeyFullSize)}
	if c.file, c.release, err = seg.index(); err != nil {
		return nil, err
	}
	return c, c.advance()
}

// advance loads the next record, refilling the buffer from the file
// when it runs out.  ok is false once the index is exhausted.
func (c *indexCursor) advance() (err error) {
	if c.pos >= c.loaded {
		remaining := c.seg.count - c.next
		if remaining <= 0 {
			c.ok = false
			return nil
		}
		n := int64(indexReadRecords)
		if remaining < n {
			n = remaining
		}
		if _, err = c.file.ReadAt(c.buf[:n*DBKeyFullSize], segIndexHdrSize+c.next*DBKeyFullSize); err != nil {
			c.ok = false
			return err
		}
		c.next += n
		c.loaded = int(n * DBKeyFullSize)
		c.pos = 0
	}
	rec := c.buf[c.pos : c.pos+DBKeyFullSize]
	copy(c.key[:], rec[:32])
	c.dbb.Offset = binary.BigEndian.Uint64(rec[32:])
	c.dbb.Length = binary.BigEndian.Uint64(rec[40:])
	c.pos += DBKeyFullSize
	c.ok = true
	return nil
}

func (c *indexCursor) close() {
	if c.release != nil {
		c.release()
		c.release = nil
	}
}

// cursorHeap orders live cursors by key, and among equal keys by
// NEWEST first, so that the top of the heap is always the winner for
// its key and its older duplicates follow immediately behind it.
type cursorHeap []*indexCursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(i, j int) bool {
	switch bytes.Compare(h[i].key[:], h[j].key[:]) {
	case -1:
		return true
	case 1:
		return false
	}
	return h[i].src > h[j].src
}
func (h cursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)   { *h = append(*h, x.(*indexCursor)) }
func (h *cursorHeap) Pop() (x any) { n := len(*h); x = (*h)[n-1]; *h = (*h)[:n-1]; return x }

// mergeIndexes
// Walk the inputs' indexes together in key order and call emit once
// per distinct key with the newest input's entry for it.  segs must be
// oldest first.  Returns how many distinct keys there were, which with
// a nil emit is all the call does -- a cheap first pass, so a writer can
// size its header and bloom filter before the second pass streams the
// records through.
//
// Newest-wins matters even in the Perm layer, where a key is written
// once and never overwritten: adopting a peer's segment can introduce a
// second copy of a key the store already holds, because
// checkNoConflicts rejects a DIFFERING value and permits an identical
// one.  The copies agree, so which survives does not matter -- but the
// merge must emit only one.
//
// No lock: the segments are immutable and the caller holds them.
func mergeIndexes(segs []*segment, emit func(src int, key [32]byte, dbb DBBKey) error) (n uint64, err error) {
	h := make(cursorHeap, 0, len(segs))
	defer func() {
		for _, c := range h {
			c.close()
		}
	}()
	for i, seg := range segs {
		c, err := openIndexCursor(seg, i)
		if err != nil {
			return 0, err
		}
		if c.ok {
			h = append(h, c)
		} else {
			c.close()
		}
	}
	heap.Init(&h)

	for len(h) > 0 {
		top := h[0]
		key, dbb, src := top.key, top.dbb, top.src
		n++
		if emit != nil {
			if err = emit(src, key, dbb); err != nil {
				return n, err
			}
		}
		// Retire this key from every cursor that holds it: the winner,
		// then the older copies, which sit at the top in turn
		for len(h) > 0 && h[0].key == key {
			c := h[0]
			if err = c.advance(); err != nil {
				return n, err
			}
			if c.ok {
				heap.Fix(&h, 0)
			} else {
				heap.Pop(&h)
				c.close()
			}
		}
	}
	return n, nil
}

// indexWriter
// Streams a segment index to disk: header first, sized from a key count
// the caller already knows, then the records in the order given -- which
// must be key order -- and the bloom filter last.  Offsets are written as
// given; the caller decides whether they are body-relative.
type indexWriter struct {
	path, tmpPath string
	file          *os.File
	w             *bufio.Writer
	bloom         *Bloom
	count, seen   uint64
}

func newIndexWriter(indexPath string, count uint64) (iw *indexWriter, err error) {
	iw = &indexWriter{path: indexPath, tmpPath: indexPath + segTmpSuffix, count: count}
	iw.bloom = NewBloomSizedForKeys(count, 3)
	if iw.file, err = os.Create(iw.tmpPath); err != nil {
		return nil, err
	}
	iw.w = bufio.NewWriterSize(iw.file, segWriteBuffer)
	var hdr [segIndexHdrSize]byte
	binary.BigEndian.PutUint32(hdr[:], segIndexMagic)
	binary.BigEndian.PutUint32(hdr[4:], segIndexVersion)
	binary.BigEndian.PutUint64(hdr[8:], count)
	binary.BigEndian.PutUint64(hdr[16:], iw.bloom.NumBytes)
	binary.BigEndian.PutUint32(hdr[24:], uint32(iw.bloom.K))
	if _, err = iw.w.Write(hdr[:]); err != nil {
		iw.abort()
		return nil, err
	}
	return iw, nil
}

func (iw *indexWriter) write(key [32]byte, dbb DBBKey) error {
	iw.seen++
	iw.bloom.Set(key)
	// Encoded on the stack: a merge calls this once per key, and an
	// allocation per key is garbage the collector has to keep up with
	var rec [DBKeyFullSize]byte
	copy(rec[:], key[:])
	binary.BigEndian.PutUint64(rec[32:], dbb.Offset)
	binary.BigEndian.PutUint64(rec[40:], dbb.Length)
	_, err := iw.w.Write(rec[:])
	return err
}

// finish writes the bloom, makes the file durable, and renames it into
// place.  It refuses a record count other than the one the header
// promised: a reader trusts the header.
func (iw *indexWriter) finish() (err error) {
	defer iw.abort()
	if iw.seen != iw.count {
		return errIndexCount{want: iw.count, got: iw.seen}
	}
	if _, err = iw.w.Write(iw.bloom.Map); err != nil {
		return err
	}
	if err = iw.w.Flush(); err != nil {
		return err
	}
	if err = iw.file.Sync(); err != nil {
		return err
	}
	if err = iw.file.Close(); err != nil {
		iw.file = nil
		return err
	}
	iw.file = nil
	// No directory fsync: an index is derived data, and the manifest
	// commit that follows fsyncs the directory anyway (issue #33)
	return os.Rename(iw.tmpPath, iw.path)
}

// abort closes and removes the temporary file if finish has not
// renamed it away
func (iw *indexWriter) abort() {
	if iw.file != nil {
		iw.file.Close()
		iw.file = nil
		os.Remove(iw.tmpPath)
	}
}

type errIndexCount struct{ want, got uint64 }

func (e errIndexCount) Error() string {
	return "index writer: header promised " + itoa(e.want) + " records, wrote " + itoa(e.got)
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
