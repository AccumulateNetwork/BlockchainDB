package blockchainDB

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
)

const valueFilename = "values.dat"
const valueTmpFilename = "values_tmp.dat"

type KV struct {
	Directory  string
	vFile      *BFile
	kFile      *KFile
	UseHistory bool
}

// NewKV
// Overwrites any existing directory; directories are created for the vFile and kFile
func NewKV(history bool, directory string, offsetsCnt, KeyLimit uint64, MaxCachedBlocks int) (kv *KV, err error) {
	os.RemoveAll(directory)
	if err = os.Mkdir(directory, os.ModePerm); err != nil {
		return nil, err
	}
	kv = new(KV)
	kv.Directory = directory
	if kv.kFile, err = NewKFile(history, directory, offsetsCnt, KeyLimit, MaxCachedBlocks); err != nil {
		return nil, err
	}
	if kv.vFile, err = NewBFile(filepath.Join(directory, valueFilename)); err != nil {
		return nil, err
	}
	// Note: the history file (when enabled) is owned by the kFile, which
	// created it above.  Creating a second HistoryFile here would truncate
	// the one the kFile is using.
	kv.UseHistory = history
	return kv, nil
}

// OpenKV
// Open an existing Key/Value Database that uses separate BFiles to hold values and keys.
func OpenKV(directory string) (kv *KV, err error) {
	kv = new(KV)
	kv.Directory = directory
	filename := filepath.Join(directory, valueFilename)
	if kv.vFile, err = OpenBFile(filename); err != nil {
		return nil, err
	}
	if kv.kFile, err = OpenKFile(directory); err != nil {
		return nil, err
	}
	kv.UseHistory = kv.kFile.History != nil
	return kv, err
}

// Put
// Put the key into the kFile, and the value in the vFile
//
// With history enabled, values are immutable: a Put of a key that
// already exists is a no-op when the value is identical (this is what
// makes crash recovery by replay work -- a node re-applying writes
// after a restart must not fail on the ones that were already durable)
// and an error when the value differs.
func (k *KV) Put(key [32]byte, value []byte) (err error) {

	if k.UseHistory {
		if existing, err := k.Get(key); err == nil {
			if bytes.Equal(existing, value) {
				return nil // Same value: no-op (e.g. replay after a crash)
			}
			return errors.New("cannot overwrite immutable value when history is enabled")
		}
		// Not found -- or unreadable, as when a crash persisted the key
		// but not its value bytes.  Either way the write proceeds; the
		// new record supersedes any dangling one (kGet prefers the
		// newest record).
	}

	dbbKey := new(DBBKey)
	dbbKey.Offset, err = k.vFile.Offset()
	if err != nil {
		return err
	}
	dbbKey.Length = uint64(len(value))

	if _, err = k.vFile.Write(value); err != nil {
		return err
	}

	return k.kFile.Put(key, dbbKey)

}

// Get
// Get the key from the key file, then pull the value from the value file
func (k *KV) Get(key [32]byte) (value []byte, err error) {
	dbbKey, err := k.kFile.Get(key)
	if err != nil {
		return nil, err
	}
	value = make([]byte, dbbKey.Length)
	if err = k.vFile.ReadAt(dbbKey.Offset, value); err != nil {
		return nil, err
	}
	return value, nil
}

func (k *KV) Close() (err error) {
	// Close (and sync) the values first: keys reference offsets in the
	// value file, so values must be durable before the keys that point
	// at them.
	if err = k.vFile.Close(); err != nil {
		return err
	}
	if err = k.kFile.Close(); err != nil {
		return err
	}
	return nil
}

func (k *KV) Open() (err error) {
	if err = k.kFile.Open(); err != nil {
		return err
	}
	if err = k.vFile.Open(); err != nil {
		return err
	}
	return nil
}

// Compress
// Re-write the values file to remove trash values (values that have been
// overwritten and are no longer referenced by any key).
//
// Only meaningful for a mutable (history-disabled) KV: with history
// enabled values are immutable, so the values file holds no trash.
//
// No longer on the database's path.  KV2's Dyna layer is a mutable
// SegmentStore and compacts by writing a new sealed generation and
// committing it with one manifest rename; this remains for the legacy
// KV and as the v1 baseline the segment benchmarks measure against.
//
// It is not crash-atomic (issue #19): the swap of the values file and
// the kfile rewrite are two separate steps, and a crash between them
// leaves keys pointing into the wrong values layout -- reads return
// wrong bytes with no error.  That is why the Dyna layer moved off it.
func (k *KV) Compress() (err error) {
	if k.UseHistory {
		return nil // Immutable values are never trash; nothing to reclaim
	}
	if err = k.Open(); err != nil {
		return err
	}
	if err = k.vFile.Flush(); err != nil { // All values must be on disk to copy them
		return err
	}

	kvs, ks, err := k.kFile.GetKeyList()
	if err != nil {
		return err
	}
	if len(ks) == 0 {
		return nil // Nothing to compress
	}

	tvFile, err := NewBFile(filepath.Join(k.Directory, valueTmpFilename))
	if err != nil {
		return err
	}

	// Copy the live values into the tmp file, assigning new offsets
	var buffer []byte
	var newOffset uint64
	for _, key := range ks {
		dbbKey := kvs[key]
		if uint64(cap(buffer)) < dbbKey.Length {
			buffer = make([]byte, dbbKey.Length)
		}
		buffer = buffer[:dbbKey.Length]
		// Read the current key value
		if err := k.vFile.ReadAt(dbbKey.Offset, buffer); err != nil {
			return err
		}
		// Write the value into the tvFile and point the key at it
		if _, err = tvFile.Write(buffer); err != nil {
			return err
		}
		dbbKey.Offset = newOffset
		newOffset += dbbKey.Length
	}
	if err = tvFile.Close(); err != nil { // Flush + sync the compacted values
		return err
	}

	// Swap the compacted values file into place
	if k.vFile.File != nil {
		if err = k.vFile.File.Close(); err != nil {
			return err
		}
		k.vFile.File = nil
	}
	if err = os.Rename(tvFile.Filename, k.vFile.Filename); err != nil {
		return err
	}
	if err = syncDir(k.Directory); err != nil {
		return err
	}
	k.vFile.EOB = 0
	k.vFile.EOD = newOffset

	// Update the keys with their new offsets
	for _, key := range ks {
		if err = k.kFile.Put(key, kvs[key]); err != nil {
			return err
		}
	}
	if err = k.kFile.Close(); err != nil { // Persist the updated keys
		return err
	}

	return k.Open()
}
