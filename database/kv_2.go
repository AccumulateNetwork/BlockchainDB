package blockchainDB

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const PermDirName = "perm"
const DynaDirName = "dyna"

// KV2
// Maintains 2 layers of key value pairs with different immutability characteristics:
//
// 1. PermKV (Permanent KV): a SegmentStore in immutable mode
//    - Values are immutable - once a key is associated with a value, it cannot be changed
//    - Suitable for content-addressed storage where keys are derived from values (e.g., hash of value)
//    - Typically used for data that doesn't change, like transaction data or blockchain blocks
//    - Attempting to overwrite a key with a different value will result in the key being moved to DynaKV
//
// 2. DynaKV (Dynamic KV): a SegmentStore in mutable mode
//    - Values are mutable - keys can be freely associated with different values over time
//    - Suitable for state storage where keys have an arbitrary relationship to values
//    - Used for data that changes over time, like account balances or other state information
//    - We only compact this layer, since it's the only one that accumulates trash
//
// This two-layer design efficiently separates immutable data from mutable data, which is a
// common pattern in blockchain-style databases where most data is append-only and immutable,
// but some state needs to be updated.
//
// Both layers are segment stores; what differs is the rule for a repeated key.  The Perm
// layer rejects a conflicting value, so its sealed segments partition the key space and a
// peer syncs them by copying files.  The Dyna layer lets the newer write shadow the older,
// so its sealed segments overlap and Compress reclaims what the shadowing left behind.
//
// ToDo: Because KV2 can be used as a shard in a sharded database, and because the PermKV values don't
// change and that database does not benefit from sharding, then KV2 might ought to accept a
// *SegmentStore for the PermKV. That way, only the DynaKV is really sharded, while all the permanent
// key/values are kept in one store.

type KV2 struct {
	// Mutex is the protocol's lock at this level: Put, PutPerm, PutDyna
	// and Seal take it exclusively, Get and its variants shared.  The
	// history maintenance -- MergeBelow and Compress -- does NOT take
	// it: each layer has a lock of its own for history (SegmentStore.
	// History), and holding this one for a merge or a compaction held
	// every commit and read of the shard for the whole copy (issue #57).
	Mutex     sync.RWMutex
	Directory string        // Directory where the PermKV and DynaKV directories are
	PermKV    *SegmentStore // The Perm layer: sealed, immutable segments
	DynaKV    *SegmentStore // The Dyna layer: sealed, mutable segments
	DWrites   int           // Number of writes to the DynaKV since the last compress
	PWrites   int           // Number of writes to the PermKV since the last compress
	SealLimit int           // Seal a layer when its live tail reaches this many records

	// opened says both layers are open and SealLimit is set, so that
	// Open -- which every operation calls first -- can say so without
	// the lock.  Set at the end of Open and cleared by Close, under it.
	opened atomic.Bool
}

// NewKV2
// Create a two level KV database with different immutability characteristics:
//
// 1. PermKV: an immutable SegmentStore
//   - Once a key is associated with a value, it cannot be changed
//   - If a key in PermKV needs to be updated, it's moved to DynaKV
//
// 2. DynaKV: a mutable SegmentStore
//   - Keys can be freely associated with different values over time
//
// This design efficiently separates immutable data (content-addressed storage)
// from mutable data (state storage) in a blockchain-style database.
//
// sealLimit sets SealLimit, the point at which a layer seals its live
// tail.  It is recorded in the layers' manifests, so OpenKV2 restores
// it; only a store written before the field existed falls back to
// DefaultBloomCapacity.
//
// This DESTROYS any existing database in directory.  Use OpenKV2 to
// reopen one.
func NewKV2(directory string, sealLimit uint64) (kv2 *KV2, err error) {
	// SealLimit is an int, and sealPermIfFull/sealDynaIfFull read
	// "<= 0" as sealing disabled, so a large limit would silently mean
	// never seal -- an unbounded live tail replayed in full on open
	if sealLimit > math.MaxInt32 {
		return nil, fmt.Errorf("sealLimit %d is too large (max %d)", sealLimit, math.MaxInt32)
	}
	os.RemoveAll(directory)
	if err = os.Mkdir(directory, os.ModePerm); err != nil {
		return nil, err
	}

	kv2 = new(KV2)
	kv2.Directory = directory
	if kv2.PermKV, err = NewSegmentStore(filepath.Join(directory, PermDirName), false); err != nil {
		return nil, err
	}
	if kv2.DynaKV, err = NewSegmentStore(filepath.Join(directory, DynaDirName), true); err != nil {
		return nil, err
	}
	kv2.SealLimit = int(sealLimit)
	// Record it durably: it is the only parameter either public
	// constructor takes, and a reopened database used to fall back to a
	// default and discard it silently
	if err = kv2.PermKV.SetSealLimit(sealLimit); err != nil {
		return nil, err
	}
	if err = kv2.DynaKV.SetSealLimit(sealLimit); err != nil {
		return nil, err
	}
	return kv2, nil
}

// SetFilterBlocks
// Set N for both layers: the roll period of the key filters -- the
// window, N to 2N blocks, over which a permanent key cannot be
// overwritten (keyfilter.go) -- and the line between the active tier
// and history, which is what bounds the pause a commit can suffer from
// maintenance (issue #57).  Recorded in each layer's manifest.  Meant
// for the moment a database is created, since it rebuilds the filters
// and commits a manifest per layer.  Below MinFilterBlocks is refused.
//
// The Dyna layer takes the same N.  Its block advances with every
// Seal, so its window rolls like Perm's, and a dynamic record that
// left the window more than N to 2N blocks ago is history -- which is
// what makes it reclaimable without touching the protocol's tier.
func (k *KV2) SetFilterBlocks(n uint64) (err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	if err = k.PermKV.SetFilterBlocks(n); err != nil {
		return err
	}
	return k.DynaKV.SetFilterBlocks(n)
}

func OpenKV2(directory string) (kv2 *KV2, err error) {
	kv2 = new(KV2)
	kv2.Directory = directory
	permDirName := filepath.Join(directory, PermDirName) // Add directory names
	dynaDirName := filepath.Join(directory, DynaDirName) // Add directory names
	if kv2.PermKV, err = OpenSegmentStore(permDirName); err != nil {
		return nil, err
	}
	if kv2.DynaKV, err = OpenSegmentStore(dynaDirName); err != nil {
		return nil, err
	}
	return kv2, nil
}

func (k *KV2) Open() error {
	// Every operation on a shard calls this first, the maintenance
	// included, so an open database answers without the lock: a merge
	// waiting here behind a seal would be waiting on the protocol
	if k.opened.Load() {
		return nil
	}
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	if err := k.PermKV.Open(); err != nil {
		return err
	}
	if k.SealLimit == 0 {
		// Restore what the database was built with; only a store
		// predating the persisted field falls back to a default
		if limit := k.PermKV.SealLimit; limit > 0 {
			k.SealLimit = int(limit)
		} else {
			k.SealLimit = int(DefaultBloomCapacity)
		}
	}
	if err := k.DynaKV.Open(); err != nil {
		return err
	}
	k.opened.Store(true)
	return nil
}

// Close
// Close both layers, and report the first failure.
//
// Both, even when the first one fails.  Close is a durability point --
// it is what flushes and fsyncs the Dyna layer's live tail, which is
// buffered 32 KB at a time in process memory -- so returning early
// left that tail unflushed and dropped it.  A caller told "the close
// failed" was not told that one layer is durable and the other's
// newest writes are gone, which is the torn commit across layers that
// Seal was fixed for in issue #29 (issue #38).
func (k *KV2) Close() error {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	k.opened.Store(false)
	err := k.PermKV.Close()
	if dynaErr := k.DynaKV.Close(); err == nil {
		err = dynaErr
	}
	return err
}

// GetDyna
// Get a k/v from the DynaKV db.  Doesn't check the PermKV.
func (k *KV2) GetDyna(key [32]byte) (value []byte, err error) {
	k.Mutex.RLock()
	defer k.Mutex.RUnlock()

	if value, err = k.DynaKV.Get(key); err != nil { // Not in DynaKV, then return whatever
		return nil, err
	}
	return value, nil
}

// GetPerm
// Get a k/v from the PermKV db.  Doesn't check the DynaKV.
func (k *KV2) GetPerm(key [32]byte) (value []byte, err error) {
	k.Mutex.RLock()
	defer k.Mutex.RUnlock()

	if value, err = k.PermKV.Get(key); err != nil { // Not in PermKV, then return whatever
		return nil, err
	}
	return value, nil
}

// Get
// Get a value from the KV2.  Checks the DynaKV first, then the PermKV
func (k *KV2) Get(key [32]byte) (value []byte, err error) {
	k.Mutex.RLock()
	defer k.Mutex.RUnlock()

	// Check and see if this is a key that has been changed
	if value, err = k.DynaKV.Get(key); err == nil { // Not in DynaKV, then return whatever
		return value, nil
	}
	return k.PermKV.Get(key) //                      PermKV has.

}

// PutDyna
// Use when the k/v is known to be a dynamic k/v
func (k *KV2) PutDyna(key [32]byte, value []byte) (writes int, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	k.DWrites++
	if err = k.DynaKV.Put(key, value); err != nil {
		return k.DWrites, err
	}
	return k.DWrites, k.sealDynaIfFull()
}

// PutPerm
// Use when the k/v is known to be a permanent (immutable) k/v
func (k *KV2) PutPerm(key [32]byte, value []byte) (writes int, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	k.PWrites++
	if err = k.PermKV.Put(key, value); err != nil {
		return k.DWrites, err
	}
	return k.DWrites, k.sealPermIfFull()
}

// sealPermIfFull
// Seal the Perm layer once its live tail reaches SealLimit keys, so
// the unsealed tail (and the memory tracking it) stays bounded between
// block boundaries.  The caller must hold the Mutex.
func (k *KV2) sealPermIfFull() (err error) {
	if k.SealLimit <= 0 || k.PermKV.LiveCount() < k.SealLimit {
		return nil
	}
	_, err = k.PermKV.SealNext()
	return err
}

// sealDynaIfFull
// Seal the Dyna layer on the same limit, counted in physical records
// rather than distinct keys.  A mutable layer leaves a record per
// write, so a handful of hot keys rewritten in a loop would hold the
// key count flat while the tail grew without bound -- and that tail is
// replayed in full on every open.  The caller must hold the Mutex.
func (k *KV2) sealDynaIfFull() (err error) {
	if k.SealLimit <= 0 || k.DynaKV.LiveRecords() < uint64(k.SealLimit) {
		return nil
	}
	_, err = k.DynaKV.SealNext()
	return err
}

// Seal
// Close a block: seal the Perm layer at the block height, and make the
// Dyna layer's live tail durable.
//
// Both halves matter, and the second one used to be missing.  Sealing
// is the Perm layer's durability point and the unit a peer syncs, so
// permanent records were durable the moment Seal returned.  Dynamic
// writes were not: they sat in a 32 KB buffer until the tail filled,
// was compacted, or the store was closed.  A node killed after a
// commit therefore came back with permanent records -- chain elements
// -- newer than the mutable state that indexes them, and the two
// layers disagreed about where the block ended (issue #29).
//
// The Dyna layer is synced rather than sealed: its segments are local
// (a peer never receives one), so there is nothing to gain from
// cutting one per block and a full seal per block per shard to pay
// for.  A sync on a tail that took no writes is free.
//
// Both layers are advanced before either error is returned, so a
// failure to sync Dyna does not leave Perm unsealed and the block
// unrepeatable.
//
// The Dyna layer is also told the block, so that it ages.  Its
// segments are tagged with the block they were sealed in and its
// window rolls with Perm's, which is what puts a dynamic record that
// has not been rewritten for 2N blocks into history, where
// CompactHistory can reclaim what superseded it without touching the
// tier a commit writes (issue #57).  No manifest is written for it:
// the next Dyna seal records the block, and a reopened layer that is
// a few blocks behind simply hands the same segments off again.
func (k *KV2) Seal(height uint64) (meta SegmentMeta, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	meta, err = k.PermKV.Seal(height)
	if syncErr := k.DynaKV.Sync(); err == nil {
		err = syncErr
	}
	k.DynaKV.AdvanceBlock(height + 1)
	return meta, err
}

// MergeBelow
// Merge the Perm layer's history segments below a block height into
// one.  Reports whether anything was merged.
//
// Only the Perm layer.  The Dyna layer has Compress, which merges for
// a different reason -- to reclaim what overwriting left behind.
//
// The KV2 lock is NOT taken.  A merge is a history operation: it reads
// immutable segments with no lock, writes the merged segment aside,
// and takes the Perm layer's history lock to swap them.  Holding the
// KV2 lock here held every Put, Get and Seal of the shard for the
// whole copy, which is the pause issue #57 measures.
func (k *KV2) MergeBelow(height uint64) (meta SegmentMeta, merged bool, err error) {
	return k.PermKV.MergeBelow(height)
}

// Put
// Returns the number of writes since the last compress, and an err if the put failed
func (k *KV2) Put(key [32]byte, value []byte) (writes int, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	if value2, err2 := k.DynaKV.Get(key); err2 == nil { // Check.  Is this a DynaKV key?
		if bytes.Equal(value, value2) { // If the key is in DynaKV, it stays there.
			return k.DWrites, nil //       If the value is not changed, do nothing
		}
		k.DWrites++
		if err = k.DynaKV.Put(key, value); err != nil { // If the value DID change, update
			return k.DWrites, err
		}
		return k.DWrites, k.sealDynaIfFull()
	}
	// One lookup settles all three Perm cases: absent (in which case the
	// write has already happened), present and identical (nothing to
	// do), present and different (the key becomes dynamic).  Asking
	// whether the key was there and then calling Put, which asked again
	// to enforce immutability, made a new key -- the common case, and a
	// miss by definition -- pay for the answer twice.
	existing, existed, err := k.PermKV.PutIfAbsent(key, value)
	if err != nil {
		return k.DWrites, err
	}
	if !existed { // It is a new permanent key, and it is now written
		k.PWrites++
		return k.DWrites, k.sealPermIfFull() // PermKV is never compacted; report DWrites
	}
	if bytes.Equal(existing, value) { // If no change, ignore;
		// DWrites, like every other path here: the count the caller
		// gets back is what it uses to decide when to compact, and only
		// the Dyna layer has anything to reclaim.  This branch -- a
		// permanent key rewritten with the value it already has, which
		// is what a replay looks like -- used to report the permanent
		// count instead, a larger and unrelated number (issue #39).
		return k.DWrites, nil
	}
	k.DWrites++
	if err = k.DynaKV.Put(key, value); err != nil { // If the perm value changed, it is now a DynaKV
		return k.DWrites, err
	}
	return k.DWrites, k.sealDynaIfFull()
}

// Compress
// Reclaim the space the Dyna layer's overwrites left behind, in the
// part of the layer the protocol no longer touches: compact the run of
// history segments that is worth compacting (SegmentStore.CompactHistory)
// into one holding only the newest record per key.  The Perm layer is
// not compacted -- its values are immutable, so none of them are trash.
//
// Neither the KV2 lock nor the Dyna layer's store lock is taken.  The
// records compaction touches are the ones that left the window of the
// last N to 2N blocks; what the window holds -- the live tail and the
// active segments -- is the protocol's, and a record last written
// there is never read or rewritten here.  So a commit runs during a
// compaction as it does at any other time, and the pause a compaction
// can cause is the history swap, bounded by nothing that grows with
// the chain.  It used to seal the tail and rewrite every generation
// under both locks once a garbage ratio was crossed, and that stopped
// every commit and read on the node for 12 s at block 400 and up to
// 32 s at block 1,040 (issue #57).
//
// This is crash-atomic.  The compacted segment is fully durable
// before the history manifest names it, so there is no window in
// which keys and values disagree (issue #19); a crash before the
// commit costs only the space of a file recoverOrphans deletes.  The
// v1 kfile Compress this replaced swapped the values file and rewrote
// the key offsets as two separate steps, and a crash between them
// left keys pointing into the wrong layout -- reads returned wrong
// bytes with no error.
//
// A key written to Dyna keeps a stale copy in Perm, which compaction
// does not remove; Get resolves the layer order, so the copy is dead
// weight rather than a wrong answer.
//
// TODO: Cleanse PermKV of keys in DynaKV
func (k *KV2) Compress() error {
	if _, err := k.DynaKV.CompactHistory(); err != nil {
		return err
	}
	k.Mutex.Lock()
	k.DWrites = 0 // Clear write counts
	k.PWrites = 0
	k.Mutex.Unlock()
	return nil
}
