package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The day filter (issue #47).
//
// A deep read rules a key out of cold storage by probing every block
// set's filter.  That is one cheap probe per set -- K one-byte reads,
// page-cached -- but the sets accumulate for the life of the chain, so
// the WALK grows with age even though each step does not.  Packing
// every 1,000 blocks, a year is ~31,500 sets, and every deep miss
// walks all of them.
//
// So the sets are grouped, and each finished group carries one filter
// over every key in it.  A deep read probes the group first: a "no"
// skips the whole group in one probe, and only a "maybe" walks its
// sets.  The walk becomes (groups) + (sets in the group being filled),
// which is ~365 + ~86 after a year rather than 31,500, and ~1,900
// after five years rather than 158,000.
//
// A GROUP IS A BLOCK RANGE, not a wall-clock day: SetGroupBlocks
// blocks, and which group a set belongs to is a pure function of its
// first block, the same way filterStarts derives the key filters'
// windows.  Nothing here reads a clock -- two nodes replaying the same
// chain build the same groups -- and "day" is what the default size
// means at a block a second, not what the code depends on.
//
// A filter is written only when its group is COMPLETE: a set has
// arrived above the group's range, so nothing more can join it.  That
// makes the filter immutable, sized exactly for the keys it holds, and
// written once.  The group still being filled has no filter; its sets
// are probed directly, and there are few of them.
//
// The failure modes are the bloom's usual pair, and the same rule
// applies as everywhere else: a filter that claims a key it does not
// hold costs a walk it need not have taken, and a filter that DENIES a
// key it holds is a wrong answer with nothing downstream to catch it.
// So a group filter is built from the sets it claims and never from
// anything less, and a filter that cannot be read is treated as absent
// -- the sets are walked, which is correct and merely slower.

const (
	dayFilterPrefix = "day-"
	dayFilterSuffix = ".flt"
	dayFilterMagic  = 0x44415931 // "DAY1"
	dayFilterVer    = 1

	// dayFilterHdr: magic(4) version(4) groupBlocks(8) first(8)
	// last(8) keys(8) bloomBytes(8) K(4).  The bloom follows.
	dayFilterHdr = 52
)

// SetGroupBlocks is how many blocks one group of sets covers: 86,400,
// a day at a block a second.  It is a block count rather than a
// duration so that grouping is deterministic -- two nodes replaying
// the same chain group the same sets, with no clock involved.
var SetGroupBlocks uint64 = DefaultSetGroupBlocks

// DefaultSetGroupBlocks is what SetGroupBlocks starts at, and what a
// zero falls back to
const DefaultSetGroupBlocks = 86_400

// DayFilterMaxBytes bounds one group filter's bitmap.
//
// The filter is sized for every key in its group, and a group is a
// day: at 5,000 entries a block that is 432M keys, which at
// BloomBitsPerKey would be ~648 MB held in one allocation while the
// filter is built -- on the pack path, with every shard pinned.  Spec
// 1.2 does not allow that, and a group filter is an accelerator, not
// a record of anything.
//
// Past the bound the filter is built at the bound instead, which
// costs false POSITIVES -- a group walked that need not have been --
// and never a false negative.  A group whose keys need more than this
// is a group whose filter would skip nothing anyway.
var DayFilterMaxBytes uint64 = 32 << 20

// setGroupBlocksForTest sets the group size; tests move the size
// rather than the clock, because there is no clock to move.
func setGroupBlocksForTest(n uint64) { SetGroupBlocks = n }

// groupBlocks is SetGroupBlocks, never zero.  It is an exported var
// with no checked setter, and setGroup divides by it -- a zero would
// panic on every deep read and every pack, which is a long way from
// where it was set.  Falling back to the default is the loud-enough
// answer for a tunable that only a program can get wrong.
func groupBlocks() uint64 {
	if SetGroupBlocks == 0 {
		return DefaultSetGroupBlocks
	}
	return SetGroupBlocks
}

// setGroup is the group a block belongs to: groups tile the block
// space from 0, so a set's group is decided by its first block alone.
func setGroup(block uint64) uint64 { return block / groupBlocks() }

// dayFilter is one finished group's filter, held on disk and probed
// there -- the same residency rule the segments' filters follow
// (issue #64): a filter per day of chain, resident, would be memory
// growing with age for data that is read rarely.
type dayFilter struct {
	group      uint64 // Which group: blocks [group*N, (group+1)*N)
	first      uint64 // Oldest block the group's sets actually hold
	last       uint64 // Newest
	path       string
	bloomOff   int64
	bloomBytes uint64
	bloomK     int
}

// dayFilterName is the file name for a group's filter
func dayFilterName(group uint64) string {
	return fmt.Sprintf("%s%08d%s", dayFilterPrefix, group, dayFilterSuffix)
}

// mightHold reports whether the group might hold the key.  An
// unreadable filter reports true: a walk that was not needed, never a
// key reported absent that is there.
func (d *dayFilter) mightHold(key [32]byte) bool {
	if d.bloomBytes == 0 || d.bloomK < 1 {
		return true
	}
	f, release, err := segmentFiles.acquire(d.path)
	if err != nil {
		return true
	}
	defer release()
	probe := Bloom{NumBytes: d.bloomBytes, K: d.bloomK}
	var one [1]byte
	for i := 0; i < d.bloomK; i++ {
		idx, mask := probe.ByteMask(key, i)
		if _, err = f.ReadAt(one[:], d.bloomOff+int64(idx)); err != nil {
			return true
		}
		if one[0]&mask == 0 {
			return false // Definitely not in this group
		}
	}
	return true
}

// writeDayFilter builds a group's filter from the sets in it and
// commits it: every key of every shard of every set, one bloom, tmp
// file, fsync, rename.
//
// Built from the sets themselves rather than from anything remembered
// while they were written, so the filter cannot claim coverage it does
// not have -- the rule the key filters follow (keyfilter.go).
func writeDayFilter(directory string, group uint64, sets []*blockSet) (f *dayFilter, err error) {
	if len(sets) == 0 {
		return nil, nil
	}
	var keys uint64
	first, last := sets[0].meta.First, sets[0].meta.Last
	for _, s := range sets {
		keys += s.meta.Keys
		if s.meta.First < first {
			first = s.meta.First
		}
		if s.meta.Last > last {
			last = s.meta.Last
		}
	}
	// Sized for the group's keys, but never past the bound: see
	// DayFilterMaxBytes.  A filter over its design point has a higher
	// false-positive rate, which costs a walk, and that is the failure
	// this side may have.
	bloom := NewBloomSizedForKeys(keys, 3)
	if DayFilterMaxBytes > 0 && bloom.NumBytes > DayFilterMaxBytes {
		bloom = newBloomSized(DayFilterMaxBytes*8/BloomBitsPerKey, BloomBitsPerKey, 3)
	}
	for _, s := range sets {
		for shard := 0; shard < s.shards; shard++ {
			err = s.forEachKey(shard, func(key [32]byte, _ *DBBKey) error {
				bloom.Set(key)
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}

	path := filepath.Join(directory, dayFilterName(group))
	// A temporary name of its own, not one derived from the group: two
	// writers sharing a path interleave their bitmaps into one file,
	// and a bloom missing bits denies keys it holds.  PackFinalized
	// serializes packs, so this is the second lock on that door.
	tmp := fmt.Sprintf("%s.%d%s", path, os.Getpid(), segTmpSuffix)
	out, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	defer func() {
		if out != nil {
			out.Close()
			os.Remove(tmp)
		}
	}()
	var hdr [dayFilterHdr]byte
	binary.BigEndian.PutUint32(hdr[:], dayFilterMagic)
	binary.BigEndian.PutUint32(hdr[4:], dayFilterVer)
	// The group size the filter was built under.  Which sets belong to
	// a group is derived from it, and it is not persisted anywhere
	// else, so a filter that does not carry it could be consulted for
	// sets it never saw after the size changed -- and answer a
	// definitive "not here" for keys it never had, the one answer a
	// filter may never give.
	binary.BigEndian.PutUint64(hdr[8:], SetGroupBlocks)
	binary.BigEndian.PutUint64(hdr[16:], first)
	binary.BigEndian.PutUint64(hdr[24:], last)
	binary.BigEndian.PutUint64(hdr[32:], keys)
	binary.BigEndian.PutUint64(hdr[40:], bloom.NumBytes)
	binary.BigEndian.PutUint32(hdr[48:], uint32(bloom.K))
	if _, err = out.Write(hdr[:]); err != nil {
		return nil, err
	}
	if _, err = out.Write(bloom.Map); err != nil {
		return nil, err
	}
	if err = fsync(out); err != nil { // Durable before it is named
		return nil, err
	}
	if err = out.Close(); err != nil {
		out = nil
		return nil, err
	}
	out = nil
	if err = os.Rename(tmp, path); err != nil {
		return nil, err
	}
	if err = syncDir(directory); err != nil {
		return nil, err
	}
	return &dayFilter{group: group, first: first, last: last, path: path,
		bloomOff: dayFilterHdr, bloomBytes: bloom.NumBytes, bloomK: bloom.K}, nil
}

// openDayFilter reads a group filter's header.  The bloom stays on
// disk (issue #64).
func openDayFilter(path string) (f *dayFilter, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var hdr [dayFilterHdr]byte
	if _, err = file.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}
	if magic := binary.BigEndian.Uint32(hdr[:]); magic != dayFilterMagic {
		return nil, fmt.Errorf("%s is not a day filter (magic %#08x)", path, magic)
	}
	if v := binary.BigEndian.Uint32(hdr[4:]); v != dayFilterVer {
		return nil, fmt.Errorf("%s is day filter version %d; this build reads %d", path, v, dayFilterVer)
	}
	name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), dayFilterPrefix), dayFilterSuffix)
	var group uint64
	if _, err = fmt.Sscanf(name, "%d", &group); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	// Which sets belong to a group is derived from the group size, so a
	// filter built under a different one may not cover the sets this
	// store would consult it for -- and would then answer "not here"
	// for keys it never saw, the one answer a filter may never give.
	// It is refused, and its group is walked instead.
	if size := binary.BigEndian.Uint64(hdr[8:]); size != SetGroupBlocks {
		return nil, fmt.Errorf("%s was built at %d blocks to a group; this store uses %d",
			path, size, SetGroupBlocks)
	}
	// A bloom size the file cannot hold is a corrupt header, and a huge
	// one is worse than useless: ByteMask computes `v % (NumBytes<<8)`,
	// so a value at or above 2^56 wraps the modulus to zero and
	// divides by it.  Refuse it; the group is walked.
	bloomBytes := binary.BigEndian.Uint64(hdr[40:])
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if bloomBytes > 1<<32 || int64(bloomBytes) != info.Size()-dayFilterHdr {
		return nil, fmt.Errorf("%s claims a %d-byte filter in a %d-byte file",
			path, bloomBytes, info.Size())
	}
	return &dayFilter{
		group:      group,
		first:      binary.BigEndian.Uint64(hdr[16:]),
		last:       binary.BigEndian.Uint64(hdr[24:]),
		path:       path,
		bloomOff:   dayFilterHdr,
		bloomBytes: bloomBytes,
		bloomK:     int(binary.BigEndian.Uint32(hdr[48:])),
	}, nil
}

// loadDayFilters reads the group filters in a set directory
func loadDayFilters(directory string) (filters []*dayFilter, err error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, segTmpSuffix) {
			os.Remove(filepath.Join(directory, name))
			continue
		}
		if !strings.HasPrefix(name, dayFilterPrefix) || !strings.HasSuffix(name, dayFilterSuffix) {
			continue
		}
		f, err := openDayFilter(filepath.Join(directory, name))
		if err != nil {
			// A filter that cannot be trusted is not a broken store:
			// its group is walked, which is what a group without a
			// filter always was.  Keeping it would risk the one answer
			// a filter may never give.
			continue
		}
		filters = append(filters, f)
	}
	sort.Slice(filters, func(i, j int) bool { return filters[i].group < filters[j].group })
	return filters, nil
}
