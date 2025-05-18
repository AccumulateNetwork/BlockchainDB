package blockchainDB

import (
	"bytes"
	"os"
	"path/filepath"
)

const PermDirName = "perm"
const DynaDirName = "dyna"

// KV2
// Maintains 2 layers of key value pairs with different immutability characteristics:
//
// 1. PermKV (Permanent KV): Uses KFile with history enabled
//    - Values are immutable - once a key is associated with a value, it cannot be changed
//    - Suitable for content-addressed storage where keys are derived from values (e.g., hash of value)
//    - Typically used for data that doesn't change, like transaction data or blockchain blocks
//    - Attempting to overwrite a key with a different value will result in the key being moved to DynaKV
//
// 2. DynaKV (Dynamic KV): Uses KFile with history disabled
//    - Values are mutable - keys can be freely associated with different values over time
//    - Suitable for state storage where keys have an arbitrary relationship to values
//    - Used for data that changes over time, like account balances or other state information
//    - We only compress this layer since it's expected to change frequently
//
// This two-layer design efficiently separates immutable data from mutable data, which is a
// common pattern in blockchain-style databases where most data is append-only and immutable,
// but some state needs to be updated.
//
// ToDo: Because KV2 can be used as a shard in a sharded database, and because the PermKV values don't
// change and that database does not benefit from sharding, then KV2 might ought to accept a *KV for the
// the PermKV. That way, only the DynaKV is really sharded, while all the permanent key/values are kept in
// one KV database.
//
// ToDo furthermore, the PermKV can build databases that are blocked (by major blocks). Those separate KFiles
// could then be used to rapidly sync partially synced nodes

type KV2 struct {
	Directory string // Directory where the PermKV and DynaKV directories are
	PermKV    *KV    // The Perm KV
	DynaKV    *KV    // the Dyna KV
	DWrites   int    // Number of writes to the DynaKV since the last compress
	PWrites   int    // Number of writes to the PermKV since the last compress
}

// NewKV2
// Create a two level KV file with different immutability characteristics:
//
// 1. PermKV: Uses KFile with history enabled (immutable values)
//    - Created with history=true
//    - Once a key is associated with a value, it cannot be changed
//    - If a key in PermKV needs to be updated, it's moved to DynaKV
//
// 2. DynaKV: Uses KFile with history disabled (mutable values)
//    - Created with history=false
//    - Keys can be freely associated with different values over time
//
// This design efficiently separates immutable data (content-addressed storage)
// from mutable data (state storage) in a blockchain-style database.
func NewKV2(directory string, offsetsCnt, KeyLimit uint64, MaxCachedBlocks int) (kv2 *KV2, err error) {
	os.RemoveAll(directory)
	if err = os.Mkdir(directory, os.ModePerm); err != nil {
		return nil, err
	}

	kv2 = new(KV2)
	kv2.Directory = directory
	if kv2.PermKV, err = NewKV(true, filepath.Join(directory, PermDirName), offsetsCnt, KeyLimit, MaxCachedBlocks); err != nil {
		return nil, err
	}
	if kv2.DynaKV, err = NewKV(false, filepath.Join(directory, DynaDirName), offsetsCnt, KeyLimit, MaxCachedBlocks); err != nil {
		return nil, err
	}
	return kv2, nil
}

func OpenKV2(directory string) (kv2 *KV2, err error) {
	kv2 = new(KV2)
	kv2.Directory = directory
	permDirName := filepath.Join(directory, PermDirName) // Add directory names
	dynaDirName := filepath.Join(directory, DynaDirName) // Add directory names
	if kv2.PermKV, err = OpenKV(permDirName); err != nil {
		return nil, err
	}
	if kv2.DynaKV, err = OpenKV(dynaDirName); err != nil {
		return nil, err
	}
	return kv2, nil
}

func (k *KV2) Open() error {
	if err := k.PermKV.Open(); err != nil {
		return err
	}
	return k.DynaKV.Open()
}

func (k *KV2) Close() error {
	if err := k.PermKV.Close(); err != nil {
		return err
	}
	return k.DynaKV.Close()
}

// GetDyna
// Get a k/v from the DynaKV db.  Doesn't check the PermKV.
func (k *KV2) GetDyna(key [32]byte) (value []byte, err error) {

	if value, err = k.DynaKV.Get(key); err != nil { // Not in DynaKV, then return whatever
		return nil, err
	}
	return value, nil
}

// GetPerm
// Get a k/v from the PermKV db.  Doesn't check the DynaKV.
func (k *KV2) GetPerm(key [32]byte) (value []byte, err error) {

	if value, err = k.PermKV.Get(key); err != nil { // Not in PermKV, then return whatever
		return nil, err
	}
	return value, nil
}

// Get
// Get a value from the KV2.  Checks the DynaKV first, then the PermKV
func (k *KV2) Get(key [32]byte) (value []byte, err error) {

	// Check and see if this is a key that has been changed
	if value, err = k.DynaKV.Get(key); err == nil { // Not in DynaKV, then return whatever
		return value, nil
	}
	return k.PermKV.Get(key) //                      PermKV has.

}

// PutDyna
// Use when the k/v is known to be a dynamic k/v
func (k *KV2) PutDyna(key [32]byte, value []byte) (writes int, err error) {
	k.DWrites++
	err = k.DynaKV.Put(key, value)
	return k.DWrites, err
}

// PutPerm
// Use when the k/v is known to be a dynamic k/v
func (k *KV2) PutPerm(key [32]byte, value []byte) (writes int, err error) {
	k.PWrites++
	err = k.PermKV.Put(key, value)
	return k.DWrites, err
}

// Put
// Returns the number of writes since the last compress, and an err if the put failed
func (k *KV2) Put(key [32]byte, value []byte) (writes int, err error) {

	if value2, err2 := k.DynaKV.Get(key); err2 == nil { // Check.  Is this a DynaKV key?
		if bytes.Equal(value, value2) { // If the key is in DynaKV, it stays there.
			return k.DWrites, nil //       If the value is not changed, do nothing
		}
		k.DWrites++
		err = k.DynaKV.Put(key, value) // If the value DID change, update
		return k.DWrites, err
	}
	if value2, err2 := k.PermKV.Get(key); err2 == nil { // Check. Is it a PermKV
		if bytes.Equal(value, value2) { // If no change, ignore;
			return k.PWrites, nil
		}
		k.DWrites++
		err = k.DynaKV.Put(key, value) // If the perm value changed, it is now a DynaKV
		return k.DWrites, err
	}
	// If not yet a DynaKV or not in k.PermKV, default to k.PermKV
	k.PWrites++
	err = k.PermKV.Put(key, value)
	return k.DWrites, err // We do not compress the PermKV ... Only report DWrites
}

// Compress
// Only DynaKV is compressed, since PermKV doesn't change.  That does mean one
// bogus DynaKV key will exist in PermKV.
//
// TODO: Cleanse PermKV of keys in DynaKV
func (k *KV2) Compress() {
	k.DynaKV.Compress()
	k.DWrites = 0 // Clear write counts
	k.PWrites = 0
}
