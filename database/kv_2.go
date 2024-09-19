package blockchainDB

import (
	"bytes"
	"os"
	"path/filepath"
)

// KV2
// Maintains 2 layers of key value pairs.  The low level KVFile holds key/value pairs that don't change
// The high level KVFile holds keys that do change.  We only compress the high level KVFile
type KV2 struct {
	Directory string // Directory where the PermKV and DynaKV directories are
	PermKV    *KV    // The Perm KV
	DynaKV    *KV    // the Dyna KV
	DWrites   int    // Number of writes to the DynaKV since the last compress
	PWrites   int    // Number of writes to the PermKV since the last compress
}

// NewKV2
// Create a two level KV file, where one KV file holds k/v pairs that don't change,
// and another where k/v pairs do change
func NewKV2(directory string) (kv2 *KV2, err error) {
	os.RemoveAll(directory)
	if err = os.Mkdir(directory, os.ModePerm); err != nil {
		return nil, err
	}

	kv2 = new(KV2)
	kv2.Directory = directory
	if kv2.PermKV, err = NewKV(filepath.Join(directory, "perm")); err != nil {
		return nil, err
	}
	if kv2.DynaKV, err = NewKV(filepath.Join(directory, "dyna")); err != nil {
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

func (k *KV2) Get(key [32]byte) (value []byte, err error) {
	// Check and see if this is a key that has been changed
	if value, err = k.DynaKV.Get(key); err != nil { // Not in DynaKV, then return whatever
		return k.PermKV.Get(key) //                      PermKV has.
	} else { //                                        If this IS in DynaKV, return that!
		return value, nil
	}
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
			return k.DWrites, nil
		}
		k.DWrites++
		err = k.DynaKV.Put(key, value) // If the perm value changed, it is now a DynaKV
		return k.DWrites, err
	}
	// If not yet a DynaKV or not in k.PermKV, default to k.PermKV
	k.PWrites++
	err = k.PermKV.Put(key, value)
	return k.DWrites, err
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
