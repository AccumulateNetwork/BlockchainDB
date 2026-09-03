package blockchainDB

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// The seal, in two halves, and why.
//
// A seal used to run whole under the store's Mutex -- and, through
// KV2.Seal, under the shard's lock: fsync the tail, write and fsync the
// index, hash, then commit the manifest (fsync the tmp, rename, fsync
// the directory).  Four or five barriers, ~17 ms each on a validator's
// disk, with every Get of the shard queued behind them.  Eight shards
// sealed one after another made that ~40 serialised barriers per
// block: 0.7-0.8 s of a 1 s block spent in fsync, and every submission
// that validated against a sealing shard waiting the seal out, which
// capped the network at ~80 tx/s with the CPUs idle (issue #84).
//
// Now the lock covers only the CUT: flush the tail, finish its header,
// rename the file aside to sealing.dat, and start a fresh tail.  That
// is a few syscalls and no barrier.  Everything that waits on the disk
// -- the data fsync, the index, the hash, the link, the manifest
// commit -- runs with the Mutex released, serialised against other
// commits by commitMu alone.  Reads meanwhile answer from the cut tail
// (SegmentStore.sealing) exactly as they answered from the live one,
// and writes go to the new tail, so nothing on the protocol path waits
// for a barrier.  The data and index fsyncs overlap each other, and a
// shard's Dyna sync overlaps its Perm seal (KV2.Seal); the shards of a
// set seal concurrently (KVShard.SealBlock).  The barriers a seal
// issues are the same ones; what changed is that nothing else waits
// behind them, and they no longer wait behind each other.
//
// Measured (TestSealBlockCost, NVMe, ~5 ms per fsync, 8 shards, ~200
// records per shard per block): a block boundary went from 253 ms
// mean and 382 ms worst to 50 ms and 67 ms, and the worst Get issued
// during one from 56 ms to 1.8 ms.  On the soak host's 17 ms fsyncs
// the boundary was 0.7-0.8 s; the same arithmetic puts it near 0.1 s.
//
// The crash contract is unchanged: a file is durable before it is
// named, and the manifest commit is the one commit point (spec 1.8).
// The cut adds one recoverable state -- a tail sitting at sealing.dat
// whose seal did not complete -- and recoverSealingTail folds it back
// into the live tail on open, so its records are exactly as durable
// as they were as the live tail they had just been.

// segSealingName is where a cut tail sits between the cut and the
// link that gives it its segment name.  One per store: seals are
// serialised by commitMu, so there is never more than one in flight.
const segSealingName = "sealing.dat"

// segSealingIndexTmp is where its index is built meanwhile; it is
// renamed to the segment's index name once the data file has its
// name.  A .tmp, so an open after a crash removes it unread.
const segSealingIndexTmp = "sealing.idx" + segTmpSuffix

// fsyncs counts every barrier this package issues, so that what a
// seal or a block boundary costs is measured rather than assumed
// (spec 1.2; issues #33, #84).
var fsyncs atomic.Uint64

// fsyncGate, when set, is called before every barrier.  A test hook,
// for holding the disk still while a test looks at what the locks let
// through meanwhile; nil in production.
var fsyncGate atomic.Pointer[func()]

// fsync is where every barrier goes through.  See fsyncs.
func fsync(f *os.File) error {
	fsyncs.Add(1)
	if gate := fsyncGate.Load(); gate != nil {
		(*gate)()
	}
	return f.Sync()
}

// FsyncCount
// How many fsyncs this package has issued since the process started,
// across every store.  A measurement, for tests and for the operator
// asking what a block boundary costs.
func FsyncCount() uint64 { return fsyncs.Load() }

// errSealStaged says a seal of this store was cut and then failed to
// finish, so its tail sits at sealing.dat.  The store still answers
// reads from it; closing and reopening the store folds it back into
// the live tail (recoverSealingTail), after which sealing works again.
var errSealStaged = errors.New("a previous seal of this store failed after cutting the tail; close and reopen the store to recover it")

// sealingTail is a live tail that has been cut but not yet named:
// its map and its file, now at sealing.dat, open for reading only.
// Readers consult it under the Mutex, between the live tail and the
// sealed segments, which is exactly where its records stand in age.
type sealingTail struct {
	entries map[[32]byte]*DBBKey
	file    *BFile // Flushed; File stays open for readers until the segment replaces it
	records uint64 // Physical records, for the header

	order [][32]byte // The keys, in the order the index is built in
	hash  string     // Set by promote in an immutable store
}

// pendingSeal is a seal between its two halves: the tail is cut, the
// store's commitMu is held, and finish owes the store a segment (or
// an error, or nothing at all when the tail was empty) and the release
// of commitMu.
type pendingSeal struct {
	s             *SegmentStore
	height, seq   uint64
	blockBoundary bool
	tail          *sealingTail    // nil when there was nothing to cut
	record        *manifestCommit // A boundary with nothing to seal may still owe a manifest
	released      bool
}

// Seal
// Seal the live tail into an immutable segment at the given height and
// start a fresh tail.  Sealing is the store's durability point.
//
// The file is renamed into place rather than copied, so sealing costs
// a header write, an index build, a hash, and the barriers: the tail's
// fsync, the index's, and the manifest commit's two.  The store's
// Mutex is held for none of them (see the top of this file).  Sealing
// at a block boundary also advances the block the live tail
// accumulates into, so the auto-seals that follow are tagged with the
// next block rather than the one just closed.
func (s *SegmentStore) Seal(height uint64) (meta SegmentMeta, err error) {
	p, err := s.beginSeal(height)
	if err != nil {
		return meta, err
	}
	return p.finish()
}

// SealNext
// Seal the live tail without a block boundary, to bound the tail when
// it fills mid-block.  The segment is tagged with the block currently
// being accumulated and takes the next sequence within it, so an
// auto-seal never consumes a block number (issue #27).
func (s *SegmentStore) SealNext() (meta SegmentMeta, err error) {
	p, err := s.beginSealNext()
	if err != nil {
		return meta, err
	}
	return p.finish()
}

// beginSeal is the first half of Seal: cut the tail at a block
// boundary.  On success the returned seal holds commitMu until it is
// finished, and the caller MUST finish it.
func (s *SegmentStore) beginSeal(height uint64) (p *pendingSeal, err error) {
	s.commitMu.Lock()
	s.Mutex.Lock()
	p, err = s.cutTail(height, true)
	s.Mutex.Unlock()
	if err != nil {
		s.commitMu.Unlock()
	}
	return p, err
}

// beginSealNext is the first half of SealNext: the cut at the block
// the tail is accumulating, which is read under the locks.
func (s *SegmentStore) beginSealNext() (p *pendingSeal, err error) {
	s.commitMu.Lock()
	s.Mutex.Lock()
	p, err = s.cutTail(s.blockHeight, false)
	s.Mutex.Unlock()
	if err != nil {
		s.commitMu.Unlock()
	}
	return p, err
}

// cutTail is the half of a seal that needs the store to hold still.
// The caller must hold commitMu and the Mutex.  blockBoundary marks
// the seal as closing `height` rather than merely bounding the tail
// inside it.
//
// Nothing here waits on the disk.  The identity is minted, the tail's
// buffer is flushed to the OS and its header finished, the file is
// renamed aside, and a fresh tail is started -- so that when the
// Mutex is released, writes land in the next block's tail and reads
// find the old one's records where they are.
func (s *SegmentStore) cutTail(height uint64, blockBoundary bool) (p *pendingSeal, err error) {
	if err = s.checkOpen(); err != nil {
		return nil, err
	}
	if s.sealing != nil {
		return nil, errSealStaged
	}
	if blockBoundary && s.blockHeight > height {
		return nil, fmt.Errorf("block %d is already closed; now accumulating block %d", height, s.blockHeight)
	}
	seq, err := s.nextKeyAt(height)
	if err != nil {
		return nil, err
	}
	p = &pendingSeal{s: s, height: height, seq: seq, blockBoundary: blockBoundary}
	if len(s.live) == 0 {
		// Nothing to seal, but a block boundary still closes the block:
		// the next writes belong to the block after this one
		if blockBoundary && s.blockHeight <= height {
			s.advanceBlock(height + 1)
			if !s.ExternalBlockRecord { // Else recorded once for the whole shard set
				c := s.buildManifest()
				p.record = &c
			}
		}
		return p, nil
	}

	// The live file is promoted as it stands -- an fsync and a link,
	// never a rewrite.  A mutable tail holds shadowed records
	// (overwrites), and sealing used to rewrite it to drop them: one
	// pread per record, under the store lock, inside the Put that
	// tipped SealLimit.  At 100,000 records that was a ~98 MB tail read
	// back a syscall at a time every few blocks, and every node paused
	// 10-15 s while 71 goroutines queued on the lock (issue #60).
	//
	// Nothing needs the rewrite.  The index is built from the live map,
	// which already holds the newest offset for every key, so lookups
	// land on the newest copy; the shadowed bytes ride along dead until
	// CompactHistory -- whose job reclamation is, off this lock -- folds
	// them away.  (An immutable tail never shadows -- a duplicate put is
	// refused or a no-op -- so the Perm layer loses nothing it ever
	// had.)
	count := s.liveRecords
	if err = s.liveFile.Flush(); err != nil {
		return nil, err
	}
	var header [segDataHdrSize]byte
	writeSegmentDataHeader(header[:], segKindSealed, count)
	if err = s.liveFile.WriteAt(0, header[:]); err != nil {
		return nil, err
	}
	livePath := s.liveFile.Filename
	stagedPath := filepath.Join(s.Directory, segSealingName)
	if err = os.Rename(livePath, stagedPath); err != nil {
		return nil, err
	}
	tail := &sealingTail{entries: s.live, file: s.liveFile, records: count}
	tail.file.Filename = stagedPath
	s.live = make(map[[32]byte]*DBBKey)
	if err = s.newLiveFile(); err != nil {
		// Put the tail back where it was, so the store still works and
		// the next seal can try again
		if back := os.Rename(stagedPath, livePath); back == nil {
			tail.file.Filename = livePath
		}
		s.live, s.liveFile, s.liveRecords, s.liveDirty = tail.entries, tail.file, count, true
		return nil, err
	}
	s.sealing = tail
	p.tail = tail
	return p, nil
}

// finish is the second half of a seal: make the cut tail durable,
// name it, and commit.  Releases commitMu whatever happens.
func (p *pendingSeal) finish() (meta SegmentMeta, err error) {
	defer p.release()
	if p.tail == nil {
		if p.record != nil {
			return meta, p.s.commitManifest(*p.record)
		}
		return meta, nil
	}
	seg, err := p.promote()
	if err != nil {
		return meta, err
	}
	return p.commit(seg)
}

// release gives commitMu back; idempotent
func (p *pendingSeal) release() {
	if !p.released {
		p.released = true
		p.s.commitMu.Unlock()
	}
}

// promote makes the cut tail durable and links it into place as the
// sealed segment (height, seq) -- or the next free sequence above it
// -- with its index beside it.  No record is copied.  The caller must
// hold commitMu and must NOT hold the Mutex: this is the half that
// waits on the disk.
//
// The data fsync and the index's build-and-fsync run together: the
// index is built from the map in hand, not from the file, so it needs
// nothing the fsync is waiting for, and two barriers in flight at once
// cost about one.
//
// The seal is not the only identity minter: a compaction of history's
// newest suffix names its output (historyNewest.Seq+1), and when the
// active tier is empty that is exactly what nextKeyAt mints for the
// seal.  The two race; the exclusive link turns what used to be a
// silent overwrite (issue #61) into ErrExist here, and the seal takes
// the sequence after -- correctly ordered, because the seal holds
// commitMu and anything that claimed the name is maintenance output
// at or below it.  The identity the seal ends up with is p.seq.
func (p *pendingSeal) promote() (seg *segment, err error) {
	s, tail := p.s, p.tail
	tail.order = make([][32]byte, 0, len(tail.entries))
	for key := range tail.entries {
		tail.order = append(tail.order, key)
	}
	indexTmp := filepath.Join(s.Directory, segSealingIndexTmp)

	var wg sync.WaitGroup
	var indexErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		indexErr = writeIndexFileTmp(indexTmp, tail.order, tail.entries)
	}()
	dataErr := fsync(tail.file.File)
	wg.Wait()
	if dataErr != nil {
		os.Remove(indexTmp)
		return nil, dataErr
	}
	if indexErr != nil {
		return nil, indexErr
	}

	// Link, not rename: a rename would silently replace the file of a
	// segment minted the same identity, turning the race above into
	// data loss.  A taken name is skipped, audibly, and the squatter is
	// never touched (issue #61).
	stagedPath := tail.file.Filename
	var dataPath string
	for {
		dataPath = filepath.Join(s.Directory, segmentFileName(p.height, p.seq))
		err = os.Link(stagedPath, dataPath)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) {
			os.Remove(indexTmp)
			return nil, err
		}
		auditUnlink(s.Directory, fmt.Sprintf("seal-remint: %s is taken", segmentFileName(p.height, p.seq)))
		p.seq++
	}
	if err = os.Remove(stagedPath); err != nil {
		return nil, err
	}
	// No directory fsync here: the manifest commit that ends this
	// operation fsyncs the same directory, and that one barrier makes
	// this link durable too (see commitManifest)

	if !s.Mutable { // Immutable segments are transported; a peer verifies this
		if tail.hash, _, err = hashAndCount(dataPath); err != nil {
			return nil, err
		}
	}
	indexPath := strings.TrimSuffix(dataPath, segDataSuffix) + segIndexSuffix
	if err = os.Rename(indexTmp, indexPath); err != nil {
		return nil, err
	}

	meta := SegmentMeta{Height: p.height, Seq: p.seq, File: filepath.Base(dataPath),
		Count: uint64(len(tail.order)), Hash: tail.hash}
	if seg, err = s.openSegment(meta); err != nil {
		return nil, err
	}
	_ = seg.loadBloom() // Active: worth the memory (issue #64)
	return seg, nil
}

// commit puts the promoted segment in the active tier, in place of the
// cut tail, and commits the manifest that names it.  The Mutex is
// held for the swap and the manifest's construction; the commit's
// barriers run without it.
func (p *pendingSeal) commit(seg *segment) (meta SegmentMeta, err error) {
	s, tail := p.s, p.tail
	s.Mutex.Lock()
	s.active = append(s.active, seg)
	s.sealing = nil // Readers find the keys in the segment from here on
	if tail.file.File != nil {
		tail.file.File.Close() // No reader holds it: they read under the Mutex
		tail.file.File = nil
	}
	// The tail's keys are in every live filter already, and the segment
	// they now sit in is inside the window by construction, so the
	// filters cover it; advancing the block is what may roll them, and
	// what may hand the oldest active segments to history
	s.advanceBlock(p.height)
	if p.blockBoundary {
		s.advanceBlock(p.height + 1) // The next writes belong to the next block
	}
	c := s.buildManifest()
	s.Mutex.Unlock()

	if err = s.commitManifest(c); err != nil {
		return seg.meta, err
	}
	return seg.meta, nil
}

// pendingSync is a Sync between its halves: the tail's buffer is
// flushed, commitMu is held, and finish owes the store the barrier.
type pendingSync struct {
	s        *SegmentStore
	fsync    bool // The tail had unsynced writes
	released bool
}

// Sync
// Make the live tail durable without sealing it: flush the buffer and
// fsync the file, so every record written so far survives a power loss.
//
// This is the durability point for a store that is not being sealed.
// Sealing is the other one, and it is the only one the Perm layer
// needs, because a block boundary seals its whole tail.  The Dyna
// layer is not sealed at a block boundary -- its segments are local,
// not a peer's unit of sync -- so without this its newest writes sat
// in a 32 KB buffer until the tail filled, and a crash restarted the
// node with permanent records newer than the mutable state that
// indexes them (issue #29).
//
// No manifest is written: the manifest names sealed segments, and the
// live tail is recovered by replaying the file itself.  A tail
// fsynced mid-record is fine -- open truncates a torn record and
// replay resumes on the boundary before it.
//
// Sync is a no-op on a tail that has taken no writes since the last
// one, so the shards a block did not touch cost nothing.  The Mutex
// is held for the flush and not for the fsync: writes that arrive
// during the barrier go to the same file behind it, and are what the
// next Sync is for.
func (s *SegmentStore) Sync() (err error) {
	p, err := s.beginSync()
	if err != nil {
		return err
	}
	return p.finish()
}

// beginSync is the first half of Sync: flush the tail's buffer under
// the locks.  On success the returned sync holds commitMu until it is
// finished, and the caller MUST finish it.
func (s *SegmentStore) beginSync() (p *pendingSync, err error) {
	s.commitMu.Lock()
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if err = s.checkOpen(); err != nil {
		s.commitMu.Unlock()
		return nil, err
	}
	p = &pendingSync{s: s}
	if s.liveDirty {
		if err = s.liveFile.Flush(); err != nil {
			s.commitMu.Unlock()
			return nil, err
		}
		// Clean from here: what is flushed is what finish will fsync,
		// and a write that lands meanwhile marks the tail dirty again
		s.liveDirty = false
		p.fsync = true
	}
	return p, nil
}

// finish is the second half of Sync: the barrier, and any manifest
// commit history has been waiting on.  Releases commitMu whatever
// happens.
func (p *pendingSync) finish() (err error) {
	defer p.release()
	s := p.s
	if p.fsync {
		// s.liveFile is stable: only a cut or a load replaces it, and
		// both hold commitMu.  Puts append to the same file meanwhile,
		// which an fsync is indifferent to.
		if err = fsync(s.liveFile.File); err != nil {
			s.Mutex.Lock()
			s.liveDirty = true // The flushed bytes are not known durable
			s.Mutex.Unlock()
			return err
		}
	}
	// A layer that is synced rather than sealed at the block boundary
	// -- the Dyna layer -- may go many blocks between active commits,
	// and history is waiting on the next one to finish a handoff or to
	// delete the inputs of a compaction the active manifest still
	// names.  Commit one here when there is anything to finish: two
	// barriers, once per compaction, at a boundary that already pays
	// for a sync.  Otherwise a compacted layer keeps its old
	// generations on disk until its tail next fills.
	s.handoffMu.Lock()
	finish := len(s.retireAfterActiveCommit) > 0
	for _, h := range s.handoffs {
		finish = finish || h.recorded
	}
	s.handoffMu.Unlock()
	if !finish {
		return nil
	}
	s.Mutex.Lock()
	c := s.buildManifest()
	s.Mutex.Unlock()
	return s.commitManifest(c)
}

// release gives commitMu back; idempotent
func (p *pendingSync) release() {
	if !p.released {
		p.released = true
		p.s.commitMu.Unlock()
	}
}

// recoverSealingTail
// Fold a tail that a crash left cut but unsealed back into the live
// tail, so that its records are recovered exactly as they would have
// been had the seal never started: replayed on open, sealed by the
// next seal.  Called by load before the orphan sweep and before the
// live tail is replayed.
//
// Where the crash landed decides what the file is:
//
//   - Already linked at a segment name (the crash fell between the
//     link and the removal of the staging name): the segment is an
//     orphan above everything the manifests name, and recoverOrphans
//     adopts it.  The staging name is dropped, so the records are not
//     replayed as well.
//   - Not linked: the seal got no further than the cut, or its fsync.
//     The file's records precede everything in live.dat -- the tail
//     started at the cut -- so live.dat's whole records are appended
//     to it and it becomes live.dat.  Either file may be torn at its
//     end, live.dat by the crash and sealing.dat because nothing had
//     fsynced it; each is cut at its last whole record first, as
//     replay would cut it.
func (s *SegmentStore) recoverSealingTail() (err error) {
	stagedPath := filepath.Join(s.Directory, segSealingName)
	fi, err := os.Stat(stagedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	livePath := filepath.Join(s.Directory, segLiveName)

	entries, err := os.ReadDir(s.Directory)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, segFilePrefix) || !strings.HasSuffix(name, segDataSuffix) {
			continue
		}
		if ofi, err := os.Stat(filepath.Join(s.Directory, name)); err == nil && os.SameFile(fi, ofi) {
			auditUnlink(s.Directory, "recoverSealingTail-linked-as-"+name, stagedPath)
			return os.Remove(stagedPath)
		}
	}

	if fi.Size() < segDataHdrSize { // Not even a header reached the disk: nothing to keep
		auditUnlink(s.Directory, "recoverSealingTail-empty", stagedPath)
		return os.Remove(stagedPath)
	}
	end, err := wholeRecordsEnd(stagedPath)
	if err != nil {
		return err
	}
	staged, err := os.OpenFile(stagedPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer staged.Close()
	if err = staged.Truncate(int64(end)); err != nil {
		return err
	}
	var appended int64
	if lfi, statErr := os.Stat(livePath); statErr == nil && lfi.Size() > segDataHdrSize {
		liveEnd, err := wholeRecordsEnd(livePath)
		if err != nil {
			return err
		}
		live, err := os.Open(livePath)
		if err != nil {
			return err
		}
		body := io.NewSectionReader(live, segDataHdrSize, int64(liveEnd)-segDataHdrSize)
		if _, err = staged.Seek(int64(end), io.SeekStart); err != nil {
			live.Close()
			return err
		}
		appended, err = io.Copy(staged, body)
		live.Close()
		if err != nil {
			return err
		}
	}
	if err = fsync(staged); err != nil {
		return err
	}
	if err = os.Remove(livePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = os.Rename(stagedPath, livePath); err != nil {
		return err
	}
	auditUnlink(s.Directory, fmt.Sprintf("recoverSealingTail-folded(%d bytes staged, %d bytes of live.dat appended)", end, appended))
	return syncDir(s.Directory)
}

// wholeRecordsEnd is the offset just past the last whole record in a
// tail file: where a replay would stop, and where a torn record from
// a crash mid-write begins.
func wholeRecordsEnd(path string) (end uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := uint64(fi.Size())
	end = segDataHdrSize
	var recHdr [segRecHdrSize]byte
	for end+segRecHdrSize <= size {
		if _, err = f.ReadAt(recHdr[:], int64(end)); err != nil {
			return 0, err
		}
		length := binary.BigEndian.Uint64(recHdr[32:])
		if length > size { // Garbage, or torn: what follows is not a record
			break
		}
		next := end + segRecHdrSize + length
		if next > size {
			break
		}
		end = next
	}
	return end, nil
}
