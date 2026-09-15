package blockchainDB

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"os"
)

// Auditing the permanent layer.
//
// A permanent value is supposed to be permanent.  The store enforces
// that over a WINDOW: a key rewritten with a different value inside
// the last N to 2N blocks is refused, and beyond that the check stops
// (spec 1.3, issue #44).  It stops because a permanent key is the hash
// of its value, so "same key, different value" further back would be a
// hash collision -- and because checking further costs a lookup on
// every write, which is the cost the windowed rule exists to avoid.
//
// So a divergent permanent value is a bug in whatever wrote it, not
// something the store can refuse cheaply.  It is found by ANALYSIS
// instead: this walks everything the permanent layer holds and reports
// every key held with more than one distinct value, so the fault can
// be taken back to the writer.
//
// It reads like a merge, not like a query.  Every index it walks is
// sorted, so a k-way merge over all of them emits each key once with
// its copies adjacent, holding one read buffer per file rather than
// any part of the key set: a store of a billion keys audits in a fixed
// amount of memory.  Values are read only for keys held in more than
// one place, which is rare by construction -- so the cost is a
// sequential pass over the indexes and almost nothing else.
//
// The cursor here is deliberately not indexmerge.go's.  That one
// addresses a segment's index and is on the compaction and merge
// paths; this one addresses a segment's index OR one shard's slice of
// a block set, and nothing on a protocol or maintenance path should
// change to make an offline tool shorter.

// PermPlace is one place the permanent layer holds a copy of a key
type PermPlace struct {
	File   string // Segment or block-set file, or "" for the live tail
	Offset uint64 // Body-relative, as every index entry is
	Length uint64
}

// PermConflict is one key the permanent layer holds with more than one
// distinct value: the thing that should never exist.  Places and
// Values run newest first, so Values[0] is what a read returns.
type PermConflict struct {
	Shard  int
	Key    [32]byte
	Places []PermPlace
	Values [][]byte
}

func (c PermConflict) String() string {
	s := fmt.Sprintf("shard %d key %x held with %d values:", c.Shard, c.Key[:8], len(c.Values))
	for i, p := range c.Places {
		s += fmt.Sprintf("\n    %-40s %d bytes: %x", placeName(p), len(c.Values[i]), head(c.Values[i]))
	}
	return s
}

func placeName(p PermPlace) string {
	if p.File == "" {
		return "(live tail)"
	}
	return fmt.Sprintf("%s+%d", p.File, p.Offset)
}

func head(v []byte) []byte {
	if len(v) > 16 {
		return v[:16]
	}
	return v
}

// PermAudit is what one pass looked at
type PermAudit struct {
	Files      int    // Index files walked
	Keys       uint64 // Distinct permanent keys
	Copies     uint64 // Key copies: a key in two files counts twice
	Duplicates uint64 // Keys held in more than one place
	Conflicts  uint64 // Keys held with more than one distinct VALUE
}

func (a *PermAudit) add(b PermAudit) {
	a.Files += b.Files
	a.Keys += b.Keys
	a.Copies += b.Copies
	a.Duplicates += b.Duplicates
	a.Conflicts += b.Conflicts
}

func (a PermAudit) String() string {
	return fmt.Sprintf("%d files, %d keys, %d copies, %d duplicated, %d CONFLICTING",
		a.Files, a.Keys, a.Copies, a.Duplicates, a.Conflicts)
}

// AuditPermanentValues
// Walk every shard's permanent layer and report each key held with
// more than one distinct value.  `report` may be nil, in which case
// the counts are all that comes back; returning an error from it stops
// the audit.
//
// This is an offline analysis, not a protocol operation.  It takes no
// store lock beyond a pin -- the files it reads are immutable, and the
// pin is what keeps a merge or a pack from deleting one under it -- so
// it can run against a live database, and what it sees is a snapshot
// taken shard by shard.
func (k *KVShard) AuditPermanentValues(report func(PermConflict) error) (audit PermAudit, err error) {
	sets := k.Sets.snapshot()
	for i, shard := range k.Shards {
		if shard == nil {
			continue
		}
		if err = shard.Open(); err != nil {
			return audit, fmt.Errorf("shard %d: %w", i, err)
		}
		one, err := auditPermShard(i, shard.PermKV, sets, report)
		audit.add(one)
		if err != nil {
			return audit, fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return audit, nil
}

// AuditPermanentValues audits a single unsharded store's permanent
// layer.  The sharded database's method is the one to use; this exists
// for a KV2 opened on its own.
func (k *KV2) AuditPermanentValues(report func(PermConflict) error) (audit PermAudit, err error) {
	if err = k.Open(); err != nil {
		return audit, err
	}
	return auditPermShard(0, k.PermKV, nil, report)
}

// permSource is one file the permanent layer keeps records in, as the
// audit needs to see it: a sorted index somewhere in a file, and a way
// to read a value the index points at.  A sealed segment and one
// shard's region of a block set are both this.
type permSource struct {
	name  string
	path  string
	base  int64 // Byte offset of the first index record
	count int64
	value func(DBBKey) ([]byte, error)
}

func segmentSource(seg *segment) permSource {
	return permSource{
		name: seg.meta.File, path: seg.indexPath,
		base: segIndexHdrSize, count: seg.count,
		value: func(dbb DBBKey) ([]byte, error) { return seg.value(&dbb) },
	}
}

func setSource(set *blockSet, shard int) permSource {
	e := set.dir[shard]
	return permSource{
		name: set.meta.File, path: set.path,
		base: int64(e.indexOff), count: int64(e.count),
		value: func(dbb DBBKey) ([]byte, error) { return set.value(shard, &dbb) },
	}
}

// auditPermShard merges one shard's sources and reports the conflicts.
// Sources are ordered OLDEST first, so that the merge's newest-wins
// ordering puts the copy a read would return at the front.
func auditPermShard(shard int, store *SegmentStore, sets []*blockSet,
	report func(PermConflict) error) (audit PermAudit, err error) {

	live, err := liveSnapshot(store)
	if err != nil {
		return audit, err
	}
	store.pin() // Nothing the walk names may be deleted under it
	defer store.unpin()

	var sources []permSource
	for _, set := range sets { // Oldest data first: the packed sets
		if shard < len(set.dir) && set.dir[shard].count > 0 {
			sources = append(sources, setSource(set, shard))
		}
	}
	for _, seg := range store.sealedSegments() { // Then history, then the window
		if seg.count > 0 {
			sources = append(sources, segmentSource(seg))
		}
	}
	audit.Files = len(sources)

	cursors := make([]*permCursor, 0, len(sources))
	defer func() {
		for _, c := range cursors {
			c.close()
		}
	}()
	h := make(permHeap, 0, len(sources))
	for i, src := range sources {
		c, err := openPermCursor(src, i)
		if err != nil {
			return audit, err
		}
		cursors = append(cursors, c)
		if c.ok {
			h = append(h, c)
		}
	}
	heap.Init(&h)

	for len(h) > 0 {
		key := h[0].key
		audit.Keys++
		var places []PermPlace
		var entries []DBBKey
		var from []permSource
		for len(h) > 0 && h[0].key == key { // Newest first; see permHeap
			c := h[0]
			audit.Copies++
			places = append(places, PermPlace{File: c.src.name, Offset: c.dbb.Offset, Length: c.dbb.Length})
			entries = append(entries, c.dbb)
			from = append(from, c.src)
			if err = c.advance(); err != nil {
				return audit, err
			}
			if c.ok {
				heap.Fix(&h, 0)
			} else {
				heap.Pop(&h)
			}
		}
		// The live tail is newer than anything sealed, so a copy there
		// goes at the front.  It is the one source that is not an index.
		var values [][]byte
		if v, ok := live[key]; ok {
			audit.Copies++
			places = append([]PermPlace{{Length: uint64(len(v))}}, places...)
			values = append(values, v)
		}
		if len(places) < 2 {
			continue // One copy: nothing to disagree with
		}
		audit.Duplicates++
		for i := range entries {
			v, err := from[i].value(entries[i])
			if err != nil {
				return audit, fmt.Errorf("%s: reading %x: %w", from[i].name, key[:8], err)
			}
			values = append(values, v)
		}
		if !differ(values) {
			continue // The same value in several places: waste, not a fault
		}
		audit.Conflicts++
		if report != nil {
			if err = report(PermConflict{Shard: shard, Key: key, Places: places, Values: values}); err != nil {
				return audit, err
			}
		}
	}
	return audit, nil
}

// differ reports whether the values are not all the same
func differ(values [][]byte) bool {
	for i := 1; i < len(values); i++ {
		if !bytes.Equal(values[0], values[i]) {
			return true
		}
	}
	return false
}

// liveSnapshot copies the live tail out under the store's lock: the
// newest source of all, and bounded by SealLimit rather than by the
// store, so holding it is safe at any size.
func liveSnapshot(s *SegmentStore) (live map[[32]byte][]byte, err error) {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	if err = s.checkOpen(); err != nil {
		return nil, err
	}
	live = make(map[[32]byte][]byte, len(s.live))
	for key, dbb := range s.live {
		value := make([]byte, dbb.Length)
		if err = s.liveFile.ReadAt(dbb.Offset, value); err != nil {
			return nil, err
		}
		live[key] = value
	}
	return live, nil
}

// permCursor walks one source's index in key order
type permCursor struct {
	src         permSource
	ord         int // Position among the sources; higher is newer
	file        *os.File
	release     func()
	buf         []byte
	pos, loaded int
	next        int64
	key         [32]byte
	dbb         DBBKey
	ok          bool
}

func openPermCursor(src permSource, ord int) (c *permCursor, err error) {
	c = &permCursor{src: src, ord: ord, buf: make([]byte, indexReadRecords*DBKeyFullSize)}
	if c.file, c.release, err = segmentFiles.acquire(src.path); err != nil {
		return c, err
	}
	return c, c.advance()
}

func (c *permCursor) advance() (err error) {
	if c.pos >= c.loaded {
		remaining := c.src.count - c.next
		if remaining <= 0 {
			c.ok = false
			return nil
		}
		n := int64(indexReadRecords)
		if remaining < n {
			n = remaining
		}
		if _, err = c.file.ReadAt(c.buf[:n*DBKeyFullSize], c.src.base+c.next*DBKeyFullSize); err != nil {
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

func (c *permCursor) close() {
	if c.release != nil {
		c.release()
		c.release = nil
	}
}

// permHeap orders cursors by key, and among equal keys by NEWEST
// first, so a key's copies come out in the order a read resolves them
type permHeap []*permCursor

func (h permHeap) Len() int { return len(h) }
func (h permHeap) Less(i, j int) bool {
	switch bytes.Compare(h[i].key[:], h[j].key[:]) {
	case -1:
		return true
	case 1:
		return false
	}
	return h[i].ord > h[j].ord
}
func (h permHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *permHeap) Push(x any)   { *h = append(*h, x.(*permCursor)) }
func (h *permHeap) Pop() (x any) { n := len(*h); x = (*h)[n-1]; *h = (*h)[:n-1]; return x }
