package blockchainDB

import (
	"os"
	"path/filepath"
)

const valueFilename = "values.dat"
const valueTmpFilename = "values_tmp.dat"

type KV struct {
	Directory string
	vFile     *BFile
	kFile     *KFile
}

// NewKV
// Overwrites any existing directory; directories are created for the vFile and kFile
func NewKV(directory string) (kv *KV, err error) {
	os.RemoveAll(directory)
	if err = os.Mkdir(directory, os.ModePerm); err != nil {
		return nil, err
	}
	kv = new(KV)
	kv.Directory = directory
	if kv.kFile, err = NewKFile(directory); err != nil {
		return nil, err
	}
	if kv.vFile, err = NewBFile(filepath.Join(directory, valueFilename)); err != nil {
		return nil, err
	}
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
	return kv, err
}

// Put
// Put the key into the kFile, and the value in the vFile
func (k *KV) Put(key [32]byte, value []byte) (err error) {

	dbbKey := new(DBBKey)
	dbbKey.Offset, err = k.vFile.Offset()
	if err != nil {
		return err
	}
	dbbKey.Length = uint64(len(value))
	dbbKey.Height = 0

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
	if err = k.kFile.Close(); err != nil {
		return err
	}
	if err = k.vFile.Close(); err != nil {
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
// Re-write the values file to remove trash values
func (k *KV) Compress() (err error) {
	k.Open()
	k.Close()
	k.Open()

	tvFile, err := NewBFile(filepath.Join(k.Directory, valueTmpFilename))
	if err != nil {
		return err
	}

	kvs, ks, err := k.kFile.GetKeyList()
	if err != nil {
		return err
	}

	var buffer [10000]byte
	for _, key := range ks {
		dbbKey := kvs[key]
		// Read the current key value
		if err := k.vFile.ReadAt(dbbKey.Offset, buffer[:dbbKey.Length]); err != nil {
			return err
		}
		// Put the new offset in the tvFile into the dbbKey (length does not change)
		dbbKey.Offset, err = tvFile.Offset()
		if err != nil {
			return err
		}
		// Update the key in the kFile
		if err = k.kFile.Put(key, dbbKey); err != nil {
			return err
		}
		// Write the value into the tvFile
		if _, err = tvFile.Write(buffer[:dbbKey.Length]); err != nil {
			return err
		}
	}

	k.vFile.Close()
	tvFile.Close()
	k.kFile.Close()
	os.Remove(k.vFile.Filename)
	os.Rename(tvFile.Filename, k.vFile.Filename)
	return nil
}
