package blockchainDB

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
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
	Mutex     sync.Mutex    // Serializes access; KV2 methods are safe for concurrent use
	Directory string        // Directory where the PermKV and DynaKV directories are
	PermKV    *SegmentStore // The Perm layer: sealed, immutable segments
	DynaKV    *SegmentStore // The Dyna layer: sealed, mutable segments
	DWrites   int           // Number of writes to the DynaKV since the last compress
	PWrites   int           // Number of writes to the PermKV since the last compress
	SealLimit int           // Seal a layer when its live tail reaches this many records
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
	return k.DynaKV.Open()
}

func (k *KV2) Close() error {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	if err := k.PermKV.Close(); err != nil {
		return err
	}
	return k.DynaKV.Close()
}

// GetDyna
// Get a k/v from the DynaKV db.  Doesn't check the PermKV.
func (k *KV2) GetDyna(key [32]byte) (value []byte, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	if value, err = k.DynaKV.Get(key); err != nil { // Not in DynaKV, then return whatever
		return nil, err
	}
	return value, nil
}

// GetPerm
// Get a k/v from the PermKV db.  Doesn't check the DynaKV.
func (k *KV2) GetPerm(key [32]byte) (value []byte, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	if value, err = k.PermKV.Get(key); err != nil { // Not in PermKV, then return whatever
		return nil, err
	}
	return value, nil
}

// Get
// Get a value from the KV2.  Checks the DynaKV first, then the PermKV
func (k *KV2) Get(key [32]byte) (value []byte, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

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
// Seal the Perm layer at a block height.  Sealing is the Perm layer's
// durability point and the unit a peer syncs.
func (k *KV2) Seal(height uint64) (meta SegmentMeta, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	return k.PermKV.Seal(height)
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
	if value2, err2 := k.PermKV.Get(key); err2 == nil { // Check. Is it a PermKV
		if bytes.Equal(value, value2) { // If no change, ignore;
			return k.PWrites, nil
		}
		k.DWrites++
		if err = k.DynaKV.Put(key, value); err != nil { // If the perm value changed, it is now a DynaKV
			return k.DWrites, err
		}
		return k.DWrites, k.sealDynaIfFull()
	}
	// If not yet a DynaKV or not in k.PermKV, default to k.PermKV
	k.PWrites++
	if err = k.PermKV.Put(key, value); err != nil {
		return k.DWrites, err
	}
	return k.DWrites, k.sealPermIfFull() // We do not compact the PermKV ... Only report DWrites
}

// Compress
// Reclaim the space the Dyna layer's overwrites left behind: seal its
// live tail, then replace every sealed generation with one segment
// holding only the keys still reachable.  The Perm layer is not
// compacted -- its values are immutable, so none of them are trash.
//
// This is crash-atomic.  The compacted generation is fully durable
// before the manifest names it, so there is no window in which keys
// and values disagree (issue #19); a crash before the commit costs
// only the space the old generation still occupies, which the next
// compaction reclaims.  The v1 kfile Compress this replaced swapped
// the values file and rewrote the key offsets as two separate steps,
// and a crash between them left keys pointing into the wrong layout --
// reads returned wrong bytes with no error.
//
// A key written to Dyna keeps a stale copy in Perm, which compaction
// does not remove; Get resolves the layer order, so the copy is dead
// weight rather than a wrong answer.
//
// TODO: Cleanse PermKV of keys in DynaKV
func (k *KV2) Compress() error {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	if _, err := k.DynaKV.SealNext(); err != nil { // Everything to reclaim must be sealed
		return err
	}
	if _, err := k.DynaKV.CompactNext(); err != nil {
		return err
	}
	k.DWrites = 0 // Clear write counts
	k.PWrites = 0
	return nil
}
