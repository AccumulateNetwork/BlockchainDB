package blockchainDB

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	kFileName    string = "kfile.dat"
	kTmpFileName string = "kfile_tmp.dat"
)

// Block File
// Holds the buffers and ID stuff needed to build DBBlocks (Database Blocks)
type KFile struct {
	Header
	Directory string               // Directory of the BFile
	File      *BFile               // Key File
	Cache     map[[32]byte]*DBBKey // Cache of DBBKey Offsets
}

// NewBFile
// Creates a new Buffered file. A Header is created for an empty BFile
func NewKFile(Directory string) (kFile *KFile, err error) {
	kFile = new(KFile)
	kFile.Directory = Directory
	if kFile.File, err = NewBFile(filepath.Join(Directory, kFileName)); err != nil {
		return nil, err
	}
	kFile.Header.Init()
	kFile.UpdateHeader()
	kFile.Cache = make(map[[32]byte]*DBBKey)
	return kFile, err
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

// UpdateHeader
// Write the Header to the Key File
func (k *KFile) UpdateHeader() (err error) {
	h := k.Header.Marshal()
	if err = k.File.WriteAt(0, h); err != nil {
		return err
	}
	return nil
}

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
	index := k.Index(Key[:])
	var start, end uint64                 // The header gives us offsets to key sections
	start = k.Offsets[index]              // The index is where the section starts
	if index < uint16(len(k.Offsets)-1) { // Handle the last Offset special
		end = k.Offsets[index+1] //
	} else { //                              The last section ends at EOF
		if eofOffset, err := k.File.File.Seek(0, io.SeekEnd); err != nil {
			return nil, err //
		} else {
			end = uint64(eofOffset) //
		}
	}

	if start == end { //                     If the start is the end, the section is empty
		return nil, errors.New("not found")
	}

	keys := make([]byte, end-start) //       Create a buffer for the section

	if err = k.File.ReadAt(int64(start), keys); err != nil { // Read the section
		return nil, err
	}

	var dbKey DBBKey             //          Search the keys by unmarshaling each key as we search
	for len(keys) >= DBKeySize { //          Search all DBBKey entries, note they are not sorted.
		adr, err := dbKey.Unmarshal(keys) //
		if err != nil {
			return nil, err
		}
		switch {

		case adr == Key: //                  Is this the key sought?
			return &dbKey, nil

		default:
			keys = keys[DBKeySize:] //       Move to the next DBBKey
		}
	}

	return nil, errors.New("not found")
}

// Put
// Put a key value pair into the BFile, return the *DBBKeyFull
func (k *KFile) Put(Key [32]byte, dbBKey *DBBKey) (err error) {

	update, err := k.File.Write(dbBKey.Bytes(Key)) // Write the key to the file
	if update {                                    // If the key is ACTUALLY written to the file
		clear(k.Cache) //                             Clear the cache
	}
	k.Cache[Key] = dbBKey //                          Then add to the cache anyway, because this
	//                                                key might span buffers
	return err
}

// Flush
// Flush the buffer to disk, and clear the cache
func (k *KFile) Flush() (err error) {
	if err = k.UpdateHeader(); err != nil {
		return err
	}

	if err = k.File.Flush(); err != nil {
		return err
	}

	clear(k.Cache)
	return nil
}

// Open
// Make sure the underlying File is open for adding keys.  Sets the
// location in the file for writing to the end of the file.
func (k *KFile) Open() error {
	return k.File.Open()
}

// Close
// Take everything in flight and write it to disk, then close the file.
// Note that if an error occurs while updating the BFile, the BFile
// will be trashed.
func (k *KFile) Close() error {

	k.Flush()

	// Pull in all the keys
	k.File.File.Seek(HeaderSize, io.SeekStart)
	keyEntriesBytes, err := io.ReadAll(k.File.File)
	if err != nil {
		return err
	}
	numKeys := len(keyEntriesBytes) / DBKeySize
	v := numKeys*DBKeySize + HeaderSize
	v2 := numKeys * DBKeySize
	_ = v + v2
	if numKeys > 0 {
		// Create a slice to hold all unique keys, giving priority to NewKeys
		keyValues := make(map[[32]byte]*DBBKey)
		for ; len(keyEntriesBytes) > 0; keyEntriesBytes = keyEntriesBytes[DBKeySize:] {
			dbbKey := new(DBBKey)
			key, err := dbbKey.Unmarshal(keyEntriesBytes)
			if err != nil {
				return err
			}
			if key != nilKey { // Should never happen, but don't allow the nil key
				keyValues[key] = dbbKey
			}
		}

		// Collect all the keys and just the keys
		KeyList := make([][32]byte, len(keyValues))
		offset := 0
		for k := range keyValues {
			copy(KeyList[offset][:], k[:])
			offset++
		}

		// Sort the keys into their offset bins.
		// They won't be sorted inside the bins.
		// The order will not be the same over multiple machines.
		sort.Slice(KeyList, func(i, j int) bool {
			a := k.Index(KeyList[i][:]) // Bin for a
			b := k.Index(KeyList[j][:]) // Bin for b
			return a < b
		})

		// Now we have a set of bin-sorted keys

		var currentOffset uint64 = HeaderSize // Set to the location of the first key in the file
		lastIndex := -1                       // This is the "last" offset processed. Start below 0
		for _, key := range KeyList {         // Note that this never updates the first offset. That's okay, it doesn't change
			index := k.Index(key[:])    //       Use the first two bytes as the index
			if lastIndex < int(index) { //       If this index is greater than the last
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

		k.File.File.Close()
		if k.File.File, err = os.Create(k.File.Filename); err != nil {
			return err
		}
		k.UpdateHeader()
		// Write all the keys following the Header
		for _, key := range KeyList {
			keyB := keyValues[key].Bytes(key)
			k.File.File.Write(keyB)
		}

	}
	k.File.Close()
	return nil
}
