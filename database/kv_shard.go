package blockchainDB

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
// operations on the same shard are serialized.  (BFile and SegmentStore
// are NOT safe for concurrent use on their own; they rely on the KV2
// lock when accessed through this layer.)
type KVShard struct {
	Directory string
	Shards    [NumShards]*KV2
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
	kVShard.adoptBlockHeight()

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

	return kvs, nil
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
	// BlockHeight is the block the shards accumulate into next: the
	// height above the last one sealed
	BlockHeight uint64 `json:"blockHeight"`
}

// readBlockHeight
// The block the shard set was last known to be accumulating into.  A
// missing file reads as 0, which constrains nothing -- so a database
// written before this file existed opens unchanged.
func (k *KVShard) readBlockHeight() uint64 {
	data, err := os.ReadFile(filepath.Join(k.Directory, blockFileName))
	if err != nil {
		return 0
	}
	var rec blockRecord
	if err = json.Unmarshal(data, &rec); err != nil {
		return 0
	}
	return rec.BlockHeight
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
	data, err := json.Marshal(blockRecord{BlockHeight: height})
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
func (k *KVShard) adoptBlockHeight() {
	height := k.readBlockHeight()
	if height == 0 {
		return
	}
	for _, shard := range k.Shards {
		if shard != nil && shard.PermKV != nil {
			shard.PermKV.AdvanceBlock(height)
		}
	}
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
