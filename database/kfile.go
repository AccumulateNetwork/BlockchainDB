package blockchainDB

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	kFileName       string = "kfile.dat"
	kTmpFileName    string = "kfile_tmp.dat"
	MaxCachedBlocks        = 20
)

// Block File
// Holds the buffers and ID stuff needed to build DBBlocks (Database Blocks)
type KFile struct {
	Header
	Directory    string               // Directory of the BFile
	File         *BFile               // Key File
	Cache        map[[32]byte]*DBBKey // Cache of DBBKey Offsets
	BlocksCached int                  // Track blocks cached before rewritten
}

// Open
// Open an existing k.File
func OpenKFile(directory string) (kFile *KFile, err error) {
	kFile = new(KFile)

	kFile.Directory = directory
	filename := filepath.Join(directory, kFileName)
	if kFile.File, err = OpenBFile(filename); err != nil {
		return nil, err
	}
	kFile.Cache = make(map[[32]byte]*DBBKey)
	kFile.BlocksCached = 0

	return kFile, nil
}

// NewKFile
// Creates a new KFile directory (holds a key file and a value file)
// Overwrites any existing KFile directory
func NewKFile(Directory string) (kFile *KFile, err error) {
	kFile = new(KFile)
	kFile.Cache = make(map[[32]byte]*DBBKey)
	os.RemoveAll(Directory)                                 // Don't care if this fails; usually does
	if err = os.Mkdir(Directory, os.ModePerm); err != nil { // Do care if Mkdir fails
		return nil, err
	}

	kFile.Directory = Directory
	if kFile.File, err = NewBFile(filepath.Join(Directory, kFileName)); err != nil {
		return nil, err
	}
	kFile.Header.Init()
	err = kFile.WriteHeader()
	return kFile, err
}

// Open
// Make sure the underlying File is open for adding keys.  Sets the
// location in the file for writing to the end of the file.
func (k *KFile) Open() error {
	return k.File.Open()
}

// LoadHeader
// Load the Header out of the Key File
func (k *KFile) LoadHeader() (err error) {
	h := make([]byte, HeaderSize)
	if err = k.File.ReadAt(0, h); err != nil {
		return err
	}
	k.Header.Unmarshal(h)
	return nil
}

// WriteHeader
// Write the Header to the Key File
func (k *KFile) WriteHeader() (err error) {
	h := k.Header.Marshal()
	return k.File.WriteAt(0, h)
}

// LoadKeys
// Loads all the keys

// Get
// Get the value for a given DBKeyFull.  The value returned
// is free for the user to use (i.e. not part of a buffer used
// by the BFile)
func (k *KFile) Get(Key [32]byte) (dbBKey *DBBKey, err error) {

	// Return the value if it is in the cache waiting to be written
	if value, ok := k.Cache[Key]; ok {
		return value, nil
	}

	// The header reflects what is on disk.  Points keys to the section where it is.
	index := OffsetIndex(Key[:])
	var start, end uint64         // The header gives us offsets to key sections
	start = k.Offsets[index]      // The index is where the section starts
	if index < len(k.Offsets)-1 { // Handle the last Offset special
		end = k.Offsets[index+1] //
	} else { //                              The last section ends at EOD
		end = k.File.EOD //

	}

	if start == end { //                     If the start is the end, the section is empty
		return nil, errors.New("not found")
	}

	keys := make([]byte, end-start) //       Create a buffer for the section

	if err = k.File.ReadAt(start, keys); err != nil { // Read the section
		return nil, err
	}

	var dbKey DBBKey             //          Search the keys by unmarshaling each key as we search
	for len(keys) >= DBKeySize { //          Search all DBBKey entries, note they are not sorted.
		if [32]byte(keys) == Key {
			if _, err := dbKey.Unmarshal(keys); err != nil {
				return nil, err
			}
			return &dbKey, nil
		}
		keys = keys[DBKeySize:] //       Move to the next DBBKey
	}
	return nil, errors.New("not found")
}

// Put
// Put a key value pair into the BFile, return the *DBBKeyFull
func (k *KFile) Put(Key [32]byte, dbBKey *DBBKey) (err error) {

	update, err := k.File.Write(dbBKey.Bytes(Key)) // Write the key to the file
	if update {                                    // If the file was updated && time to clear cache
		if k.BlocksCached <= 0 {
			clear(k.Cache)                   //           Clear the cache and update the key offsets
			if err = k.Close(); err != nil { //           In order to allow access to keys written to disk
				return err //                               the file has to be closed and opened to update
			} //                                            the key offsets
			if err = k.File.Open(); err != nil { //       Reopen the file
				return err
			}
			k.BlocksCached = MaxCachedBlocks
		} else {
			k.BlocksCached--
		}
	}
	k.Cache[Key] = dbBKey // Then add to the cache anyway, because this
	return err            //   key might span buffers
}

// Flush
// Flush the buffer to disk, and clear the cache
func (k *KFile) Flush() (err error) {
	if err = k.WriteHeader(); err != nil {
		return err
	}

	if err = k.File.Flush(); err != nil {
		return err
	}

	clear(k.Cache)
	return nil
}

// Close
// Take everything in flight and write it to disk, then close the file.
// Note that if an error occurs while updating the BFile, the BFile
// will be trashed.
func (k *KFile) Close() (err error) {

	keyValues, keyList, err := k.GetKeyList()
	if err != nil {
		return err
	}
	if keyList == nil {
		return nil
	}

	// Now we have a set of bin-sorted keys

	var currentOffset uint64 = HeaderSize // Set to the location of the first key in the file
	lastIndex := -1                       // This is the "last" offset processed. Start below 0
	for _, key := range keyList {         // Note that this never updates the first offset. That's okay, it doesn't change
		index := OffsetIndex(key[:]) //       Use the first two bytes as the index
		if lastIndex < int(index) {  //       If this index is greater than the last
			lastIndex++                                 //     Don't overwrite the previous offset
			for ; lastIndex < int(index); lastIndex++ { //     Update any skipped offsets
				k.Offsets[lastIndex] = currentOffset //
			}
			k.Offsets[lastIndex] = currentOffset //            Set this offset
		}
		currentOffset += DBKeySize //                          Add the size of the entry for next offset
	}

	// Fill in the rest of the offsets table;
	for i := lastIndex + 1; i < NumOffsets; i++ {
		k.Header.Offsets[i] = currentOffset
	}
	k.Header.EndOfList = currentOffset // End of List is where the currentOffset was left

	err = k.File.File.Close()
	if err != nil {
		return err
	}
	err = os.Remove(k.File.Filename)
	if err != nil {
		return err
	}
	if k.File.File, err = os.Create(k.File.Filename); err != nil {
		return err
	}
	err = k.WriteHeader()
	if err != nil {
		return err
	}
	// Write all the keys following the Header
	for _, key := range keyList {
		keyB := keyValues[key].Bytes(key)
		_, err = k.File.Write(keyB)
		if err != nil {
			return err
		}
	}

	return k.File.Close()
}

// GetKeyList
// Returns all the keys and their values, and a list of the keys sorted by
// the key bins.
func (k *KFile) GetKeyList() (keyValues map[[32]byte]*DBBKey, KeyList [][32]byte, err error) {
	// Pull in all the keys

	if err = k.Flush(); err != nil {
		return nil, nil, err
	}

	// TODO: Use ReadAt
	_, err = k.File.File.Seek(HeaderSize, io.SeekStart)
	if err != nil {
		return nil, nil, err
	}
	keyEntriesBytes, err := io.ReadAll(k.File.File)
	if err != nil {
		return nil, nil, err
	}
	numKeys := len(keyEntriesBytes) / DBKeySize
	if numKeys == 0 {
		return nil, nil, err
	}

	// Create a slice to hold all unique keys, giving priority to NewKeys
	keyValues = make(map[[32]byte]*DBBKey)
	for ; len(keyEntriesBytes) > 0; keyEntriesBytes = keyEntriesBytes[DBKeySize:] {
		dbbKey := new(DBBKey)
		key, err := dbbKey.Unmarshal(keyEntriesBytes)
		if err != nil {
			return nil, nil, err
		}
		if key != nilKey { // Should never happen, but don't allow the nil key
			keyValues[key] = dbbKey
		}
	}

	// Collect all the keys and just the keys
	KeyList = make([][32]byte, len(keyValues))
	offset := 0
	for k := range keyValues {
		copy(KeyList[offset][:], k[:])
		offset++
	}

	// Sort the keys into their offset bins.
	// They won't be sorted inside the bins.
	// The order will not be the same over multiple machines.
	sort.Slice(KeyList, func(i, j int) bool {
		a := OffsetIndex(KeyList[i][:]) // Bin for a
		b := OffsetIndex(KeyList[j][:]) // Bin for b
		return a < b
	})

	return keyValues, KeyList, nil

}
