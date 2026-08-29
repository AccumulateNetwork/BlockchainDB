package blockchainDB

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Block-set files: the second stage of merging finalized Perm data.
//
// The first stage (MergeBelow) merges each shard's segments for a
// finished set of blocks into one segment per shard.  That bounds the
// file count per block, but the shard count still multiplies it: a
// 512-shard database produces 1,024 files per set, ~4.4M a day at a
// block a second (issue #47).  This stage folds the 512 per-shard
// results for one block set into ONE file, outside any shard:
//
//	<db>/sets/set-<first>-<last>.bset
//
// with a structure fixed at its head so that everything a lookup needs
// to decide WHERE to read is one contiguous, up-front region:
//
//	header      magic, version, shard count, block range, key count,
//	            bloom size, where the bodies begin
//	directory   per shard: body offset and length, index offset, key count
//	indexes     every shard's sorted 48-byte key records, contiguous
//	bloom       one filter over every key in the set
//	bodies      every shard's records, in shard order
//
// The directory, the indexes and the bloom are one region whose size is
// known from the key counts before a byte of it is written, so it is
// written with ONE call once the bodies are in place, and it can be
// loaded with one read.  What a lookup keeps in memory is the directory
// and the bloom; a shard's index slice is read when the bloom says the
// key may be here, in one read when the slice is small.
//
// Keys are sorted WITHIN each shard, not across the file.  A key routes
// to its shard by ShardIndex before anything is looked up, so a global
// order would buy nothing and cost a 512-way merge; per-shard order is
// exactly what the first stage already produced, so the index region is
// a concatenation of what the shards had.
//
// And it is a concatenation byte for byte.  An index entry's offset is
// relative to the body it indexes (segment.value), so a shard's merged
// index is copied in unchanged, its body is copied in unchanged, and
// the directory records where the body landed.  A reader resolves an
// entry as directory[shard].dataOff + entry.offset.
//
// A set is built by copying: header, then the bodies laid end to end
// past a reserved head region, then the head region filled in.  No
// record is read individually and, with one segment per shard, no
// index entry is touched.  Crash safety is the usual shape --
// written to a .tmp name, fsynced, renamed, the directory fsynced -- and
// the rename is the commit.  The per-shard segments the set replaces are
// dropped from their shards only after that commit, and their files are
// deleted only after the shard's next manifest commit names them no
// longer, so what a crash costs at any point is space.

const (
	setDirName    = "sets"
	setFilePrefix = "set-"
	setFileSuffix = ".bset"
	setMagic      = 0x42534554 // "BSET"
	setVersion    = 1
	setHdrSize    = 64 // magic(4) version(4) shards(4) bloomK(4) first(8) last(8) keys(8) bloomBytes(8) dataOffset(8) reserved(8)
	setDirEntSize = 32 // dataOffset(8) dataLength(8) indexOffset(8) count(8)

	// setIndexReadWhole is the largest index slice a lookup reads in one
	// call and searches in memory, rather than probing record by record.
	// A shard's slice of a 20-block set is ~10 KB at 5,000 entries a
	// block, so the usual lookup is one read for the index and one for
	// the value.
	setIndexReadWhole = 64 << 10
)

// SetMeta
// One block-set file, as the store describes it
type SetMeta struct {
	First uint64 // Lowest block whose records the set holds
	Last  uint64 // Highest
	File  string // File name within the set directory
	Keys  uint64 // Keys indexed, over every shard
}

// setEntry is one shard's directory entry
type setEntry struct {
	dataOff  uint64 // Where the shard's records begin
	dataLen  uint64 // How long they run
	indexOff uint64 // Where the shard's sorted index records begin
	count    uint64 // How many
}

// blockSet
// An open block-set file: what a lookup keeps in memory to decide
// whether, and where, to read.  The file itself is borrowed from the
// shared pool for the length of a read, like a segment's.
type blockSet struct {
	meta  SetMeta
	path  string
	dir   []setEntry // One per shard
	bloom *Bloom     // Over every key in the set
}

// SetStore
// The block-set files of one sharded database, oldest first.  Methods
// are safe for concurrent use: every shard consults it.
type SetStore struct {
	Mutex     sync.Mutex
	Directory string
	sets      []*blockSet
}

// NewSetStore creates an empty set directory, replacing any existing one
func NewSetStore(directory string) (store *SetStore, err error) {
	os.RemoveAll(directory)
	if err = os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}
	return &SetStore{Directory: directory}, nil
}

// OpenSetStore
// Open the set directory: every complete set file in it, in block
// order.  A file is complete by construction if it has its final name,
// because the rename that gave it that name followed the fsync of its
// contents; a .tmp is never complete and is deleted.
//
// The directory must exist.  A database is created with one, and its
// absence means the database is not one this build wrote.
func OpenSetStore(directory string) (store *SetStore, err error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	store = &SetStore{Directory: directory}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, segTmpSuffix) {
			os.Remove(filepath.Join(directory, name))
			continue
		}
		if !strings.HasPrefix(name, setFilePrefix) || !strings.HasSuffix(name, setFileSuffix) {
			continue
		}
		set, err := openBlockSet(filepath.Join(directory, name))
		if err != nil {
			return nil, err
		}
		store.sets = append(store.sets, set)
	}
	sort.Slice(store.sets, func(i, j int) bool { return store.sets[i].meta.First < store.sets[j].meta.First })
	for i := 1; i < len(store.sets); i++ {
		a, b := store.sets[i-1].meta, store.sets[i].meta
		if b.First <= a.Last {
			return nil, fmt.Errorf("block sets %s and %s overlap", a.File, b.File)
		}
	}
	return store, nil
}

// Sets describes every block-set file, oldest first
func (ss *SetStore) Sets() (metas []SetMeta) {
	for _, set := range ss.snapshot() {
		metas = append(metas, set.meta)
	}
	return metas
}

// Newest is the most recent set, if there is one
func (ss *SetStore) Newest() (meta SetMeta, ok bool) {
	sets := ss.snapshot()
	if len(sets) == 0 {
		return meta, false
	}
	return sets[len(sets)-1].meta, true
}

// snapshot is the set list as it stands.  A set, once opened, never
// changes, and the list only ever grows, so a caller can walk the
// snapshot without the lock.
func (ss *SetStore) snapshot() []*blockSet {
	ss.Mutex.Lock()
	defer ss.Mutex.Unlock()
	return ss.sets
}

// setFileName is the file name for a set covering blocks first..last
func setFileName(first, last uint64) string {
	return fmt.Sprintf("%s%08d-%08d%s", setFilePrefix, first, last, setFileSuffix)
}

// build
// Write one block-set file holding the given segments of every shard
// -- shards[i] is shard i's segments, oldest first, possibly none --
// and commit it.  first and last are the blocks it covers.
func (ss *SetStore) build(first, last uint64, shards [][]*segment) (set *blockSet, err error) {
	if len(shards) != NumShards {
		return nil, fmt.Errorf("block set needs %d shards, given %d", NumShards, len(shards))
	}
	if newest, ok := ss.Newest(); ok && first <= newest.Last {
		return nil, fmt.Errorf("block set %d..%d overlaps the newest set, %d..%d",
			first, last, newest.First, newest.Last)
	}

	// Pass one: each shard's index, and where its body will sit among
	// the bodies.  A shard's index entries are relative to its own body,
	// so they are copied as they are -- the usual case, one merged
	// segment per shard, touches no entry -- and the directory is what
	// records where the body landed.  Where the bodies begin in the file
	// depends on the key count, which is known only once every index has
	// been read, so body positions are laid out from 0 here.
	dir := make([]setEntry, NumShards)
	sizes := make([][]int64, NumShards)
	indexes := make([][]byte, NumShards)
	var keys, bodies uint64
	for i, segs := range shards {
		if len(segs) == 0 {
			continue
		}
		var bases []uint64
		var size uint64
		if sizes[i], bases, size, err = layoutBodies(segs); err != nil {
			return nil, err
		}
		if indexes[i], err = shiftedIndex(segs, bases); err != nil {
			return nil, err
		}
		dir[i] = setEntry{dataOff: bodies, dataLen: size, count: uint64(len(indexes[i]) / DBKeyFullSize)}
		keys += dir[i].count
		bodies += size
	}
	bloom := NewBloomSizedForKeys(keys, 3)
	dataStart := uint64(setHdrSize) + NumShards*setDirEntSize + keys*DBKeyFullSize + bloom.NumBytes

	// Now the head region can be assembled: the directory, with each
	// body's position made absolute; the indexes, verbatim; the bloom
	head := make([]byte, 0, dataStart-setHdrSize)
	indexOff := uint64(setHdrSize) + NumShards*setDirEntSize
	for i := range dir {
		if dir[i].count > 0 {
			dir[i].dataOff += dataStart
		}
		dir[i].indexOff = indexOff
		indexOff += dir[i].count * DBKeyFullSize
		head = binary.BigEndian.AppendUint64(head, dir[i].dataOff)
		head = binary.BigEndian.AppendUint64(head, dir[i].dataLen)
		head = binary.BigEndian.AppendUint64(head, dir[i].indexOff)
		head = binary.BigEndian.AppendUint64(head, dir[i].count)
	}
	for _, index := range indexes {
		for pos := 0; pos < len(index); pos += DBKeyFullSize {
			var key [32]byte
			copy(key[:], index[pos:])
			bloom.Set(key)
		}
		head = append(head, index...)
	}
	head = append(head, bloom.Map...)

	var header [setHdrSize]byte
	binary.BigEndian.PutUint32(header[0:], setMagic)
	binary.BigEndian.PutUint32(header[4:], setVersion)
	binary.BigEndian.PutUint32(header[8:], NumShards)
	binary.BigEndian.PutUint32(header[12:], uint32(bloom.K))
	binary.BigEndian.PutUint64(header[16:], first)
	binary.BigEndian.PutUint64(header[24:], last)
	binary.BigEndian.PutUint64(header[32:], keys)
	binary.BigEndian.PutUint64(header[40:], bloom.NumBytes)
	binary.BigEndian.PutUint64(header[48:], dataStart)

	// Pass two: the file.  Header, then the bodies past the reserved
	// head region, then the head region in one write.
	name := setFileName(first, last)
	path := filepath.Join(ss.Directory, name)
	tmpPath := path + segTmpSuffix
	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tmpPath)
		}
	}()
	if _, err = f.WriteAt(header[:], 0); err != nil {
		return nil, err
	}
	if _, err = f.Seek(int64(dataStart), 0); err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, segWriteBuffer)
	buf := make([]byte, segWriteBuffer)
	for i, segs := range shards {
		for j, seg := range segs {
			if err = seg.copyBody(bw, sizes[i][j], buf); err != nil {
				return nil, err
			}
		}
	}
	if err = bw.Flush(); err != nil {
		return nil, err
	}
	if _, err = f.WriteAt(head, setHdrSize); err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		f = nil
		return nil, err
	}
	f = nil
	if err = os.Rename(tmpPath, path); err != nil {
		return nil, err
	}
	if err = syncDir(ss.Directory); err != nil { // Commit
		return nil, err
	}

	set = &blockSet{
		meta:  SetMeta{First: first, Last: last, File: name, Keys: keys},
		path:  path,
		dir:   dir,
		bloom: bloom,
	}
	ss.Mutex.Lock()
	ss.sets = append(ss.sets, set)
	ss.Mutex.Unlock()
	return set, nil
}

// openBlockSet
// Read what a lookup keeps in memory: the header, the directory, and
// the bloom.  The indexes stay on disk.
func openBlockSet(path string) (set *blockSet, err error) {
	f, release, err := segmentFiles.acquire(path)
	if err != nil {
		return nil, err
	}
	defer release()

	var header [setHdrSize]byte
	if _, err = f.ReadAt(header[:], 0); err != nil {
		return nil, err
	}
	if magic := binary.BigEndian.Uint32(header[0:]); magic != setMagic {
		return nil, fmt.Errorf("%s is not a block set (magic %#08x)", path, magic)
	}
	if v := binary.BigEndian.Uint32(header[4:]); v != setVersion {
		return nil, fmt.Errorf("%s is block set format version %d; this build reads version %d", path, v, setVersion)
	}
	if shards := binary.BigEndian.Uint32(header[8:]); shards != NumShards {
		return nil, fmt.Errorf("%s was written for %d shards; this build has %d", path, shards, NumShards)
	}
	set = &blockSet{path: path}
	set.meta.File = filepath.Base(path)
	set.meta.First = binary.BigEndian.Uint64(header[16:])
	set.meta.Last = binary.BigEndian.Uint64(header[24:])
	set.meta.Keys = binary.BigEndian.Uint64(header[32:])
	bloomBytes := binary.BigEndian.Uint64(header[40:])
	bloomK := int(binary.BigEndian.Uint32(header[12:]))
	if bloomBytes == 0 || bloomK < 1 {
		return nil, fmt.Errorf("%s has no bloom filter", path)
	}

	raw := make([]byte, NumShards*setDirEntSize)
	if _, err = f.ReadAt(raw, setHdrSize); err != nil {
		return nil, err
	}
	set.dir = make([]setEntry, NumShards)
	for i := range set.dir {
		e := raw[i*setDirEntSize:]
		set.dir[i] = setEntry{
			dataOff:  binary.BigEndian.Uint64(e[0:]),
			dataLen:  binary.BigEndian.Uint64(e[8:]),
			indexOff: binary.BigEndian.Uint64(e[16:]),
			count:    binary.BigEndian.Uint64(e[24:]),
		}
	}

	bitmap := make([]byte, bloomBytes)
	bloomOff := int64(setHdrSize) + NumShards*setDirEntSize + int64(set.meta.Keys)*DBKeyFullSize
	if _, err = f.ReadAt(bitmap, bloomOff); err != nil {
		return nil, err
	}
	set.bloom = &Bloom{
		SizeOfMap: float64(bloomBytes) / (1024 * 1024),
		NumBytes:  bloomBytes,
		Map:       bitmap,
		K:         bloomK,
		Capacity:  bloomBytes * 8 / BloomBitsPerKey,
		Count:     set.meta.Keys,
	}
	return set, nil
}

// lookup
// Find a key in this set: bloom, then the shard's directory entry, then
// a binary search of that shard's index slice, then the value.
func (b *blockSet) lookup(shard int, key [32]byte) (value []byte, found bool, err error) {
	if !b.bloom.Test(key) {
		return nil, false, nil // Definitely not here: no file needed
	}
	e := b.dir[shard]
	if e.count == 0 {
		return nil, false, nil
	}
	f, release, err := segmentFiles.acquire(b.path)
	if err != nil {
		return nil, false, err
	}
	defer release()

	var dbb DBBKey
	if e.count*DBKeyFullSize <= setIndexReadWhole {
		// The whole slice in one read, searched in memory
		slice := make([]byte, e.count*DBKeyFullSize)
		if _, err = f.ReadAt(slice, int64(e.indexOff)); err != nil {
			return nil, false, err
		}
		n := int(e.count)
		i := sort.Search(n, func(i int) bool {
			return bytes.Compare(slice[i*DBKeyFullSize:i*DBKeyFullSize+32], key[:]) >= 0
		})
		if i == n || !bytes.Equal(slice[i*DBKeyFullSize:i*DBKeyFullSize+32], key[:]) {
			return nil, false, nil
		}
		if _, err = dbb.Unmarshal(slice[i*DBKeyFullSize:]); err != nil {
			return nil, false, err
		}
	} else {
		var rec [DBKeyFullSize]byte
		lo, hi := int64(0), int64(e.count)-1
		found = false
		for lo <= hi {
			mid := (lo + hi) / 2
			if _, err = f.ReadAt(rec[:], int64(e.indexOff)+mid*DBKeyFullSize); err != nil {
				return nil, false, err
			}
			switch bytes.Compare(key[:], rec[:32]) {
			case 0:
				if _, err = dbb.Unmarshal(rec[:]); err != nil {
					return nil, false, err
				}
				found = true
				lo = hi + 1
			case -1:
				hi = mid - 1
			default:
				lo = mid + 1
			}
		}
		if !found {
			return nil, false, nil
		}
	}
	value = make([]byte, dbb.Length)
	if _, err = f.ReadAt(value, int64(e.dataOff+dbb.Offset)); err != nil {
		return nil, false, err
	}
	return value, true, nil
}

// forEachKey
// Call fn with every key one shard holds in this set, read from the
// shard's index slice in batches.
func (b *blockSet) forEachKey(shard int, fn func(key [32]byte, dbb *DBBKey) error) (err error) {
	e := b.dir[shard]
	if e.count == 0 {
		return nil
	}
	f, release, err := segmentFiles.acquire(b.path)
	if err != nil {
		return err
	}
	defer release()
	const batch = 4096
	buff := make([]byte, batch*DBKeyFullSize)
	for at := uint64(0); at < e.count; {
		n := e.count - at
		if n > batch {
			n = batch
		}
		chunk := buff[:n*DBKeyFullSize]
		if _, err = f.ReadAt(chunk, int64(e.indexOff+at*DBKeyFullSize)); err != nil {
			return err
		}
		for pos := 0; pos < len(chunk); pos += DBKeyFullSize {
			key, dbb, err := GetDBBKey(chunk[pos : pos+DBKeyFullSize])
			if err != nil {
				return err
			}
			if err = fn(key, dbb); err != nil {
				return err
			}
		}
		at += n
	}
	return nil
}

// value reads one of a shard's values out of the set file.  The entry
// is relative to the shard's body; the directory says where that is.
func (b *blockSet) value(shard int, dbb *DBBKey) (value []byte, err error) {
	f, release, err := segmentFiles.acquire(b.path)
	if err != nil {
		return nil, err
	}
	defer release()
	value = make([]byte, dbb.Length)
	if _, err = f.ReadAt(value, int64(b.dir[shard].dataOff+dbb.Offset)); err != nil {
		return nil, err
	}
	return value, nil
}

// shardSets
// One shard's view of the set store: the coldStore a Perm layer
// consults for keys that have left its segments.
type shardSets struct {
	store *SetStore
	shard int
}

// lookup walks the sets newest first
func (c shardSets) lookup(key [32]byte) (value []byte, found bool, err error) {
	sets := c.store.snapshot()
	for i := len(sets) - 1; i >= 0; i-- {
		if value, found, err = sets[i].lookup(c.shard, key); err != nil || found {
			return value, found, err
		}
	}
	return nil, false, nil
}

// forEachKeySince visits every key the shard holds in a set that
// reaches block `start`.  Sets are in block order, so those are a
// suffix of the list.
func (c shardSets) forEachKeySince(start uint64, fn func(key [32]byte)) (err error) {
	for _, set := range c.store.snapshot() {
		if set.meta.Last < start {
			continue
		}
		err = set.forEachKey(c.shard, func(key [32]byte, _ *DBBKey) error {
			fn(key)
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// forEach visits every key and value the shard holds in any set, newest
// set first, the order a lookup uses
func (c shardSets) forEach(fn func(key [32]byte, value []byte) error) (err error) {
	sets := c.store.snapshot()
	for i := len(sets) - 1; i >= 0; i-- {
		set := sets[i]
		err = set.forEachKey(c.shard, func(key [32]byte, dbb *DBBKey) error {
			value, err := set.value(c.shard, dbb)
			if err != nil {
				return err
			}
			return fn(key, value)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// watermark is the highest block any set holds
func (c shardSets) watermark() (last uint64, ok bool) {
	newest, ok := c.store.Newest()
	return newest.Last, ok
}
