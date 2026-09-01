package blockchainDB

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const NumShards = 512

// indexShards is the byte offset of the 32-bit field in a key that
// selects its shard
const indexShards = 4

// ShardIndex
// Returns the shard a key routes to
func ShardIndex(key []byte) int {
	return int(binary.BigEndian.Uint32(key[indexShards:]) % NumShards)
}

// KVShard
// A sharded KV database.  Keys route to one of NumShards KV2 instances
// by ShardIndex.
//
// Concurrency: KVShard methods are safe for concurrent use.  The Shards
// array is fixed after creation, and each KV2 serializes access with its
// own mutex - so operations on different shards run in parallel, while
// operations on the same shard are serialized.  The finalisation
// methods -- MergeFinalized, PackFinalized, Compress -- take no
// shard's store lock: they work on history, under each store's own
// history lock, and run alongside the commits (issue #57).
type KVShard struct {
	Directory string
	Shards    [NumShards]*KV2

	// Sets holds the finalized Perm data that has left the shards: one
	// block-set file per completed set of blocks, packed from every
	// shard's merged segment (blockset.go).  Each shard's Perm layer
	// consults it for keys its own segments no longer hold.
	Sets *SetStore

	// packMu serializes PackFinalized; see there
	packMu sync.Mutex
}

// setDir is where the block-set files live
func (k *KVShard) setDir() string {
	return filepath.Join(k.Directory, setDirName)
}

// attachSets points every shard's Perm layer at the set store
func (k *KVShard) attachSets() (err error) {
	for i, shard := range k.Shards {
		if shard == nil || shard.PermKV == nil {
			continue
		}
		if err = shard.PermKV.attachCold(shardSets{k.Sets, i}); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return nil
}

func (k *KVShard) ShardDir(index int) string {
	dirname := fmt.Sprintf("Shard%04d", index)      // Shards are coded by name
	shardDir := filepath.Join(k.Directory, dirname) // Create the path
	return shardDir
}

// OpenKVShard
// Open an existing KVShard Database
func OpenKVShard(directory string) (kVShard *KVShard, err error) {
	kVShard = new(KVShard)
	kVShard.Directory = directory

	for i := range kVShard.Shards {
		shardDir := kVShard.ShardDir(i)
		if kVShard.Shards[i], err = OpenKV2(shardDir); err != nil {
			return nil, err
		}
	}
	kVShard.useSharedBlockRecord()
	if err = kVShard.adoptBlockHeight(); err != nil {
		return nil, err
	}
	if kVShard.Sets, err = OpenSetStore(kVShard.setDir()); err != nil {
		return nil, err
	}
	if err = kVShard.attachSets(); err != nil {
		return nil, err
	}
	// A shard still naming segments a committed set already holds was
	// interrupted between the set's commit and its own drop; drop them
	// again, which commits and retires them
	if newest, ok := kVShard.Sets.Newest(); ok {
		for i, shard := range kVShard.Shards {
			if _, err = shard.PermKV.DropBelow(newest.Last + 1); err != nil {
				return nil, fmt.Errorf("shard %d: %w", i, err)
			}
		}
	}

	return kVShard, nil
}

// NewKVShard
// Create a new KVShard database.  This database creates database shards to
// reduce the overhead of compressing large database files.
//
// This DESTROYS any existing database in directory.  Use OpenKVShard to
// reopen one.  sealLimit is recorded in each shard's manifest, so
// OpenKVShard restores it.
func NewKVShard(directory string, sealLimit uint64) (kvs *KVShard, err error) {
	os.RemoveAll(directory)                                    // Get rid of any existing directory
	if err = os.MkdirAll(directory, os.ModePerm); err != nil { // Make the directory
		return nil, err
	}

	kvs = new(KVShard)          // Create a new sharded directory
	kvs.Directory = directory   // Keep the directory
	for i := range kvs.Shards { // Then create all the shards
		shardDir := kvs.ShardDir(i)
		if kvs.Shards[i], err = NewKV2(shardDir, sealLimit); err != nil { // Create the KV2 for each shard
			return nil, err
		}
	}
	kvs.useSharedBlockRecord()
	if kvs.Sets, err = NewSetStore(kvs.setDir()); err != nil {
		return nil, err
	}
	if err = kvs.attachSets(); err != nil {
		return nil, err
	}

	return kvs, nil
}

// SetFilterBlocks
// Set N on every shard, both layers: the key filters' roll period --
// the window, N to 2N blocks, over which a permanent key cannot be
// overwritten (keyfilter.go) -- and the line between the active tier
// and history (issue #57).  A creation-time call -- it commits a
// manifest per layer per shard, two barriers each -- and a period
// below MinFilterBlocks is refused before any shard is touched.
func (k *KVShard) SetFilterBlocks(n uint64) (err error) {
	if err = checkFilterBlocks(n); err != nil {
		return err
	}
	for i, shard := range k.Shards {
		if err = shard.Open(); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
		if err = shard.SetFilterBlocks(n); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return nil
}

// PutDyna
// Find the right shard, and put the key/value in the DynaKV in the shard
func (k *KVShard) PutDyna(key [32]byte, value []byte) (err error) {
	index := ShardIndex(key[:])
	if err = k.Shards[index].Open(); err != nil { // A failed load leaves the
		return // shard empty, so a dropped error reads as "not found"
	}
	if writes, err := k.Shards[index].PutDyna(key, value); err != nil {
		return err
	} else if writes > 5000 {
		return k.Shards[index].Compress()
	}
	return nil
}

// PutPerm
// Find the right shard, and put the key/value in the PermKV in the shard
func (k *KVShard) PutPerm(key [32]byte, value []byte) (err error) {
	index := ShardIndex(key[:])
	if err = k.Shards[index].Open(); err != nil { // A failed load leaves the
		return // shard empty, so a dropped error reads as "not found"
	}
	if writes, err := k.Shards[index].PutPerm(key, value); err != nil {
		return err
	} else if writes > 5000 {
		return k.Shards[index].Compress()
	}
	return nil
}

// Put
// Find the right shard, and put the key/value in said shard
func (k *KVShard) Put(key [32]byte, value []byte) (err error) {
	index := ShardIndex(key[:])
	if err = k.Shards[index].Open(); err != nil { // A failed load leaves the
		return // shard empty, so a dropped error reads as "not found"
	}
	if writes, err := k.Shards[index].Put(key, value); err != nil {
		return err
	} else if writes > 5000 {
		return k.Shards[index].Compress()
	}
	return nil
}

// GetDyna
// Find the right shard, and extract the value from the DynaKV in the shard
func (k *KVShard) GetDyna(key [32]byte) (value []byte, err error) {
	index := ShardIndex(key[:])
	if err = k.Shards[index].Open(); err != nil { // A failed load leaves the
		return // shard empty, so a dropped error reads as "not found"
	}
	if value, err = k.Shards[index].GetDyna(key); err != nil {
		return nil, err
	}
	return value, nil
}

// GetDeep
// Find a key wherever it is: the shard's windows, its history, and the
// packed sets.  The explicit deep read (spec 1.3, 1.4) -- export and
// query APIs -- never the protocol path, which stops at the window.
func (k *KVShard) GetDeep(key [32]byte) (value []byte, err error) {
	index := ShardIndex(key[:])
	if err = k.Shards[index].Open(); err != nil {
		return
	}
	return k.Shards[index].GetDeep(key)
}

// GetPerm
// Find the right shard, and extract the value from the PermKV in the shard
func (k *KVShard) GetPerm(key [32]byte) (value []byte, err error) {
	index := ShardIndex(key[:])
	if err = k.Shards[index].Open(); err != nil { // A failed load leaves the
		return // shard empty, so a dropped error reads as "not found"
	}
	if value, err = k.Shards[index].GetPerm(key); err != nil {
		return nil, err
	}
	return value, nil
}

// Get
// Find the right shard, and extract the value from said shard
func (k *KVShard) Get(key [32]byte) (value []byte, err error) {
	index := ShardIndex(key[:])
	if err = k.Shards[index].Open(); err != nil { // A failed load leaves the
		return // shard empty, so a dropped error reads as "not found"
	}
	if value, err = k.Shards[index].Get(key); err != nil {
		return nil, err
	}
	return value, nil
}

// blockFileName is the shard set's record of which block its shards
// are accumulating into
const blockFileName = "block.json"

// blockRecord is what that file holds
type blockRecord struct {
	// Version is the on-disk format; see StoreFormatVersion
	Version uint32 `json:"version"`

	// BlockHeight is the block the shards accumulate into next: the
	// height above the last one sealed
	BlockHeight uint64 `json:"blockHeight"`
}

// readBlockHeight
// The block the shard set was last known to be accumulating into.
//
// A MISSING file is not an error: a set that has never sealed a block
// has none, and 0 constrains nothing.  A file that is present and
// unreadable is: it is the only record of which block the quiet shards
// are in, and guessing 0 there would let them tag segments with a
// block they do not belong to.
func (k *KVShard) readBlockHeight() (uint64, error) {
	data, err := os.ReadFile(filepath.Join(k.Directory, blockFileName))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var rec blockRecord
	if err = json.Unmarshal(data, &rec); err != nil {
		return 0, fmt.Errorf("%s: %w", blockFileName, err)
	}
	if rec.Version != StoreFormatVersion {
		return 0, fmt.Errorf("%s is on-disk format version %d; this build reads version %d",
			filepath.Join(k.Directory, blockFileName), rec.Version, StoreFormatVersion)
	}
	return rec.BlockHeight, nil
}

// writeBlockHeight
// Record, once for the whole shard set, the block its shards now
// accumulate into.
//
// This is the whole point of the exercise.  Every shard used to
// persist this number itself, and a shard with no writes in a block
// still committed a manifest to do it: two fsyncs, ~11 ms, for a value
// identical across all 512 shards.  That was the dominant cost of a
// block boundary -- ~5.6 seconds of pure device wait before any shard
// with actual data did any work (issue #32).
func (k *KVShard) writeBlockHeight(height uint64) (err error) {
	data, err := json.Marshal(blockRecord{Version: StoreFormatVersion, BlockHeight: height})
	if err != nil {
		return err
	}
	tmp := filepath.Join(k.Directory, blockFileName+segTmpSuffix)
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(k.Directory, blockFileName)); err != nil {
		return err
	}
	return syncDir(k.Directory)
}

// adoptBlockHeight
// Tell every shard the block the set is accumulating into, so a shard
// that sealed nothing for a long time still tags its next auto-seal
// with the block it belongs to
func (k *KVShard) adoptBlockHeight() error {
	height, err := k.readBlockHeight()
	if err != nil {
		return err
	}
	if height == 0 {
		return nil
	}
	for _, shard := range k.Shards {
		if shard == nil {
			continue
		}
		if shard.PermKV != nil {
			shard.PermKV.AdvanceBlock(height)
		}
		if shard.DynaKV != nil { // So that its window is where the set's is
			shard.DynaKV.AdvanceBlock(height)
		}
	}
	return nil
}

// useSharedBlockRecord marks every shard's Perm layer as having its
// block height recorded by the set rather than by itself
func (k *KVShard) useSharedBlockRecord() {
	for _, shard := range k.Shards {
		if shard != nil && shard.PermKV != nil {
			shard.PermKV.ExternalBlockRecord = true
		}
	}
}

// SealBlock
// Seal every shard's Perm layer at a block height.  This is the
// durability point for permanent data and the boundary a peer syncs;
// ExportBlock calls it as its first step.
//
// The block the shards move on to is recorded once, for the set, after
// they are all sealed -- so a shard with nothing to seal costs nothing
// rather than a manifest commit of its own (issue #32).  It is written
// before this returns, and so before any write belonging to the next
// block, which is what a shard needs it for.
func (k *KVShard) SealBlock(height uint64) (err error) {
	for i, shard := range k.Shards {
		if err = shard.Open(); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
		if _, err = shard.Seal(height); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return k.writeBlockHeight(height + 1)
}

// MergeFinalized
// Merge each shard's Perm segments below a block height into one, and
// report how many shards merged anything.
//
// This is meant to be driven in the background, on a cadence the
// caller chooses, and deliberately is not a goroutine this package
// starts.  Only the caller knows its block rate and how far back
// healing may still write, which is what sets the watermark; and a
// background goroutine owned by the store would need a lifecycle,
// somewhere to report errors, and a way to not be running during a
// block boundary.  A method the caller calls from its own scheduler
// has none of those problems and is testable besides.
//
// Shards merge independently -- each under its own lock, over its own
// few hundred entries -- so this can be spread out or run
// concurrently.  It walks them in order and keeps going past a failure
// so that one bad shard does not stop the rest from being merged,
// reporting the first error.
func (k *KVShard) MergeFinalized(height uint64) (mergedShards int, err error) {
	for i, shard := range k.Shards {
		if shard == nil {
			continue
		}
		if openErr := shard.Open(); openErr != nil {
			if err == nil {
				err = fmt.Errorf("shard %d: %w", i, openErr)
			}
			continue
		}
		_, merged, mergeErr := shard.MergeBelow(height)
		switch {
		case mergeErr != nil:
			if err == nil {
				err = fmt.Errorf("shard %d: %w", i, mergeErr)
			}
		case merged:
			mergedShards++
		}
	}
	return mergedShards, err
}

// PackFinalized
// Pack every shard's Perm segments below a block height into one
// block-set file, then drop those segments from their shards.  Reports
// whether anything was packed.  This is the second stage of
// finalization; MergeFinalized is the first, and this expects to find
// its output -- one segment per shard -- though it copes with more.
//
// The file count is the reason.  The first stage leaves one segment per
// shard per set, which at 512 shards is still 1,024 files a set; this
// leaves one file a set, outside any shard, and a lookup that reaches
// it costs a bloom probe, one read of the shard's index slice, and one
// read of the value (blockset.go).
//
// The set's commit is the rename of its file.  Nothing is dropped from
// a shard before that, so a crash during the build leaves a .tmp that
// the next open deletes and every shard as it was.  Dropping after it
// writes no manifest: the shard's next seal records the drop and only
// then deletes the files, so a crash in between costs the space of the
// segments until that seal (DropBelow).
//
// Only HISTORY is packed -- a segment still inside a shard's window
// is the protocol's -- so a watermark inside the window packs what
// has rolled out of it and no more; the rest follows once it has.  No
// shard's store lock is taken: the segments are read pinned, so a
// merge that commits meanwhile leaves their files readable until the
// pack is done, and whichever of the two lands first, the drop that
// follows removes what the set holds.
func (k *KVShard) PackFinalized(height uint64) (meta SetMeta, packed bool, err error) {
	// One pack at a time.  Every guard in here and below is a
	// check-then-act on state another pack can change: two calls read
	// the same watermark and pack the same segments into overlapping
	// sets, and -- worse -- both build the same group's filter through
	// one temporary path, interleaving two partial bitmaps into one
	// file.  A bloom missing bits DENIES keys it holds, and a denial
	// skips the group's sets without walking them, so that file is a
	// permanent silent not-found that survives restart.  A filter may
	// cost a walk; it may never do that (dayfilter.go).
	k.packMu.Lock()
	defer k.packMu.Unlock()

	if newest, ok := k.Sets.Newest(); ok && height <= newest.Last+1 {
		return meta, false, nil // Everything below is already packed
	}
	shards := make([][]*segment, NumShards)
	last, any := uint64(0), false
	for i, shard := range k.Shards {
		if err = shard.Open(); err != nil {
			return meta, false, fmt.Errorf("shard %d: %w", i, err)
		}
		var release func()
		if shards[i], release, err = shard.PermKV.historyBelow(height); err != nil {
			return meta, false, fmt.Errorf("shard %d: %w", i, err)
		}
		defer release()
		for _, seg := range shards[i] {
			if h := seg.meta.Height; !any || h > last {
				last = h
			}
			any = true
		}
	}
	if !any {
		return meta, false, nil
	}
	if packHook != nil {
		packHook() // Tests: the pins are held, the build has not run
	}
	// A set covers every block since the previous set, and the first
	// set covers every block there has been.  The segments cannot say
	// which those are: a merged segment carries the height of the
	// newest block it holds, not the oldest.
	first := uint64(0)
	if newest, ok := k.Sets.Newest(); ok {
		first = newest.Last + 1
	}
	set, err := k.Sets.build(first, last, shards)
	if err != nil {
		return meta, false, err
	}
	// Committed.  The shards can stop serving what the set now holds.
	// Each drop is a history commit of its own -- two barriers -- so
	// they run concurrently and the barriers overlap on the device:
	// what took 512 seals' worth of fsync in series takes a few dozen.
	var wg sync.WaitGroup
	var mu sync.Mutex
	slots := make(chan struct{}, packDropWorkers)
	for i, shard := range k.Shards {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, shard *KV2) {
			defer wg.Done()
			defer func() { <-slots }()
			if _, dropErr := shard.PermKV.DropBelow(height); dropErr != nil {
				mu.Lock()
				if err == nil {
					err = fmt.Errorf("shard %d: %w", i, dropErr)
				}
				mu.Unlock()
			}
		}(i, shard)
	}
	wg.Wait()
	return set.meta, true, err
}

// packHook, when set, is called by PackFinalized once every shard's
// segments are PINNED and before the set is built, with no store lock
// held.  It exists so that a test can hold a pack at its most
// expensive moment -- the whole database pinned, a set being written
// -- and show that the shards go on committing meanwhile.  Nil except
// under test.
var packHook func()

// packDropWorkers is how many shards PackFinalized drops at once
const packDropWorkers = 16

// Stats
// The two layers' counters, summed over every shard: what a sharded
// database did, in the shape one store reports it (StoreStats).  A
// caller watching a node has one database, not 512, and the per-shard
// split is an implementation detail of routing -- so the sum is the
// number that answers "is the immutability check earning its keep",
// "what are the filters buying".  Taken without any lock, like the
// per-store snapshot it is built from.
func (k *KVShard) Stats() (perm, dyna StoreStats) {
	add := func(dst *StoreStats, s StoreStats) {
		dst.PutTotal += s.PutTotal
		dst.PutNew += s.PutNew
		dst.PutDuplicate += s.PutDuplicate
		dst.PutConflict += s.PutConflict
		dst.LookupTotal += s.LookupTotal
		dst.FilterAbsent += s.FilterAbsent
		dst.FilterWalked += s.FilterWalked
		dst.FilterMisled += s.FilterMisled
		dst.LiveHit += s.LiveHit
	}
	for _, shard := range k.Shards {
		if shard == nil {
			continue
		}
		if shard.PermKV != nil {
			add(&perm, shard.PermKV.Stats())
		}
		if shard.DynaKV != nil {
			add(&dyna, shard.DynaKV.Stats())
		}
	}
	return perm, dyna
}

// Compress
// Compress all the shards
func (k *KVShard) Compress() (err error) {
	for i, kvs := range k.Shards {
		if err = kvs.Open(); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
		if err = kvs.Compress(); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return nil
}

// Close
// Close every shard, and report the first failure.
//
// Every shard, even after one fails.  Close is the durability point
// that flushes and fsyncs each shard's buffered live tail, so stopping
// at the first error abandoned the tail of every shard after it -- 511
// of them, in the worst case, for one bad shard (issue #38).
func (k *KVShard) Close() (err error) {
	for i, kvs := range k.Shards {
		if closeErr := kvs.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("shard %d: %w", i, closeErr)
		}
	}
	return err
}
