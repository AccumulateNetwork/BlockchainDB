package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	kFileName    string = "kfile.dat"     // Name of Key File
	kTmpFileName string = "kfile_tmp.dat" // Name of the tmp file
)

// Block File
// Holds the buffers and ID stuff needed to build DBBlocks (Database Blocks)
//
// KFile can operate in two modes based on whether history is enabled or disabled:
//
// 1. With History Enabled (used in PermKV):
//    - Values are immutable - once a key is associated with a value, it cannot be changed
//    - Attempting to overwrite a key with a different value will result in an error
//    - Overwriting a key with the same value is allowed (no-op)
//    - Suitable for content-addressed storage where keys are derived from values (e.g., hash of value)
//    - Uses a Bloom filter to optimize key lookups and avoid unnecessary disk I/O
//
// 2. With History Disabled (used in DynaKV):
//    - Values are mutable - keys can be freely associated with different values over time
//    - Overwriting a key with a different value is allowed
//    - Suitable for state storage where keys have an arbitrary relationship to values
//
// Performance Optimizations:
// - Uses a Bloom filter to quickly determine if a key definitely doesn't exist
// - Checks the Bloom filter before any disk I/O operations
// - In Put operations, uses the Bloom filter to avoid unnecessary disk lookups
// - Memory usage is optimized by having a single Bloom filter per KFile
type KFile struct {
	Header                               // kFile header (what is pushed to disk)
	Directory       string               // Directory of the BFile
	File            *BFile               // Key File
	History         *HistoryFile         // The History Database (nil if history is disabled)
	Cache           map[[32]byte]*DBBKey // Cache of DBBKey Offsets
	BlocksCached    int                  // Track blocks cached before rewritten
	HistoryMutex    sync.Mutex           // Allow the History to be merged in background
	KeyCnt          uint64               // Number of keys in the current KFile
	TotalCnt        uint64               // Total number of keys processed
	OffsetCnt       uint64               // Number of key sets in the kFile
	KeyLimit        uint64               // How many keys triggers to send keys to History
	MaxCachedBlocks int                  // Maximum number of keys cached before flushing to kfile
	HistoryOffsets  int                  // History offset cnt
	BloomFilter     *Bloom               // Bloom filter for quick key existence checks
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

	// Check if there's a history file and open it
	historyFilename := filepath.Join(directory, historyFilename)
	if _, err := os.Stat(historyFilename); err == nil {
		// History file exists, open it
		hf := new(HistoryFile)
		hf.Directory = directory
		hf.Filename = historyFilename
		if hf.File, err = os.OpenFile(historyFilename, os.O_RDWR, 0644); err != nil {
			return nil, err
		}
		
		// Read the header
		header := make([]byte, 4)
		if _, err = hf.File.ReadAt(header, 0); err != nil {
			return nil, err
		}
		hf.OffsetCnt = int32(binary.BigEndian.Uint32(header))
		hf.HeaderSize = 4 + KeySetSize*uint64(hf.OffsetCnt)
		
		// Read the KeySets
		keySetsData := make([]byte, hf.HeaderSize-4)
		if _, err = hf.File.ReadAt(keySetsData, 4); err != nil {
			return nil, err
		}
		hf.Unmarshal(append(header, keySetsData...))
		
		// The Bloom filter will be initialized and populated when Open is called
		
		kFile.History = hf
	}

	return kFile, nil
}

// NewKFile
// Creates a new KFile directory (holds a key file and a value file)
// Overwrites any existing KFile directory
//
// The 'history' parameter determines the behavior of the KFile:
// - When true: Values are immutable. Once a key is associated with a value, it cannot be changed.
//   This is suitable for content-addressed storage where keys are derived from values.
// - When false: Values are mutable. Keys can be freely associated with different values over time.
//   This is suitable for state storage where keys have an arbitrary relationship to values.
//
// When height is increased, the old kfile is renamed, and a new kfile
// is created. The old kfile is then merged into the kFileHistory.dat

func NewKFile(
	history bool, // Controls whether values are immutable (true) or mutable (false)
	directory string,
	offsetCnt uint64,
	keyLimit uint64,
	maxCachedBlocks int) (kFile *KFile, err error) {
	kFile = new(KFile)

	filename := kFileName
	kFile.Directory = directory
	if kFile.File, err = NewBFile(filepath.Join(directory, filename)); err != nil {
		return nil, err
	}
	
	// Only create a history file if history is enabled
	if history {
		if kFile.History, err = NewHistoryFile(offsetCnt, directory); err != nil {
			return nil, err
		}
	}
	
	kFile.KeyLimit = keyLimit
	kFile.MaxCachedBlocks = maxCachedBlocks
	kFile.Header.Init(offsetCnt)
	kFile.WriteHeader()
	kFile.Cache = make(map[[32]byte]*DBBKey)
	
	// Initialize the Bloom filter with a reasonable size
	// Using 10MB as a size, which is a good balance between memory usage and false positive rate
	kFile.BloomFilter = NewBloomFilter(10.0, 3) // 10MB Bloom filter with 3 hash functions

	return kFile, err
}

// PushHistory
// Creates a new kFile height.  Merges the keys of the current kFile into the History.
// Resets the KFile to accept more keys.
func (k *KFile) PushHistory() (err error) {
	// If history is disabled, just reset the KFile without pushing to history
	if k.History == nil {
		// Close the current file
		if err = k.Close(); err != nil {
			return err
		}
		
		// Remove the old file
		if err = os.Remove(filepath.Join(k.Directory, kFileName)); err != nil {
			return err
		}
		
		// Create a new file
		if k.File, err = NewBFile(filepath.Join(k.Directory, kFileName)); err != nil {
			return err
		}
		
		// Initialize the header
		k.Header.Init(uint64(k.OffsetsCnt))
		k.Close()
		k.Open()
		
		return nil
	}
	
	// Normal history-enabled flow
	if err = k.Close(); err != nil {
		return err
	}
	
	keyValues, keyList, err := k.GetKeyList()
	if err != nil {
		return err
	}
	
	if err = os.Remove(filepath.Join(k.Directory, kFileName)); err != nil {
		return err
	}
	
	if k.File, err = NewBFile(filepath.Join(k.Directory, kFileName)); err != nil {
		return err
	}
	
	k.Header.Init(uint64(k.OffsetsCnt))
	k.Close()
	k.Open()
	
	// Only attempt to push to history if we have keys to push
	if len(keyList) > 0 {
		// Process history synchronously to prevent memory buildup
		
		// Create a buffer for the keys
		buff := make([]byte, len(keyList)*DBKeyFullSize)
		buffPtr := buff
		for _, key := range keyList {
			// Add key to Bloom filter before pushing to history
			if k.BloomFilter != nil {
				k.BloomFilter.Set(key)
			}
			
			copy(buffPtr, (*keyValues[key]).Bytes(key))
			buffPtr = buffPtr[DBKeyFullSize:]
		}
		
		// Clear references to help with garbage collection
		keyValues = nil
		keyList = nil
		
		k.HistoryMutex.Lock()
		
		// Double-check that History is still valid before using it
		if k.History != nil {
			// Add keys to history
			if err = k.History.AddKeys(buff); err != nil {
				k.HistoryMutex.Unlock()
				return fmt.Errorf("Error sending data to history: %v", err)
			}
		}
		
		k.HistoryMutex.Unlock()
		
		// Clear the buffer to help with garbage collection
		buff = nil
	}
	
	return nil
}

// Open
// Make sure the underlying File is open for adding keys.  Sets the
// location in the file for writing to the end of the file.
func (k *KFile) Open() error {
	// Initialize the Bloom filter if it doesn't exist
	if k.BloomFilter == nil {
		// Using 10MB as a size, which is a good balance between memory usage and false positive rate
		k.BloomFilter = NewBloomFilter(10.0, 3) // 10MB Bloom filter with 3 hash functions
	}
	
	// If history is enabled, populate the Bloom filter with keys from history
	if k.History != nil {
		if err := k.PopulateBloomFilterFromHistory(); err != nil {
			return err
		}
	}
	
	return k.File.Open()
}

// Use the min function from history_file.go

// PopulateBloomFilterFromHistory
// Populates the Bloom filter with all existing keys in the history file
// This should be called when opening a KFile with history enabled
func (k *KFile) PopulateBloomFilterFromHistory() error {
	if k.History == nil || k.BloomFilter == nil {
		return nil
	}
	
	hf := k.History
	
	// Iterate through all KeySets in the history file
	for i := 0; i < int(hf.OffsetCnt); i++ {
		start := hf.KeySets[i].Start
		end := hf.KeySets[i].End
		keysLen := end - start

		// Skip empty KeySets
		if keysLen == 0 {
			continue
		}

		// Use a smaller buffer size to reduce memory usage
		// Process in chunks instead of loading the entire KeySet at once
		const chunkSize = 1024 * 1024 // 1MB chunks
		buffer := make([]byte, min(chunkSize, int(keysLen)))

		// Process the KeySet in chunks
		for offset := int64(start); offset < int64(end); offset += int64(len(buffer)) {
			// Adjust buffer size for the last chunk if needed
			if offset+int64(len(buffer)) > int64(end) {
				buffer = buffer[:int64(end)-offset]
			}

			// Read a chunk of the KeySet data
			if _, err := hf.File.ReadAt(buffer, offset); err != nil {
				return err
			}

			// Add each key in this chunk to the Bloom filter
			for keyPos := 0; keyPos+DBKeyFullSize <= len(buffer); keyPos += DBKeyFullSize {
				var key [32]byte
				copy(key[:], buffer[keyPos:keyPos+32])
				k.BloomFilter.Set(key)
			}
		}
	}

	return nil
}

// LoadHeader
// Load the Header out of the Key File
func (k *KFile) LoadHeader() (err error) {
	h := make([]byte, k.HeaderSize)
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

// Get
// Get the value for a given DBKeyFull.  The value returned
// is free for the user to use (i.e. not part of a buffer used
// by the BFile).  If the key is not found, the code will
// look at the history.
func (k *KFile) Get(Key [32]byte) (dbBKey *DBBKey, err error) {
	// First check if the key is in the cache (fastest lookup)
	if value, ok := k.Cache[Key]; ok {
		return value, nil
	}
	
	// If we have a Bloom filter, check it before doing any disk I/O
	// If the Bloom filter says the key doesn't exist, it's definitely not in the file or history
	if k.BloomFilter != nil && !k.BloomFilter.Test(Key) {
		return nil, errors.New("not found")
	}
	
	// Try to get the key from the current file
	dbBKey, err = k.kGet(Key)
	if err == nil {
		return dbBKey, nil
	}
	
	// If not found and history is enabled, try to get from history
	if k.History != nil {
		k.HistoryMutex.Lock()
		defer k.HistoryMutex.Unlock()
		return k.History.Get(Key)
	}
	
	// If no history or not found in history, return the original error
	return nil, err
}

// LoadKeys
// Loads all the keys

// kGet
// Get the value for a given DBKeyFull from the file (not cache).  The value returned
// is free for the user to use (i.e. not part of a buffer used
// by the BFile)
func (k *KFile) kGet(Key [32]byte) (dbBKey *DBBKey, err error) {

	// The header reflects what is on disk.  Points keys to the section where it is.
	index := k.OffsetIndex(Key[:])
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

	var dbKey DBBKey                 //          Search the keys by unmarshaling each key as we search
	for len(keys) >= DBKeyFullSize { //          Search all DBBKey entries, note they are not sorted.
		if [32]byte(keys) == Key {
			if _, err := dbKey.Unmarshal(keys); err != nil {
				return nil, err
			}
			return &dbKey, nil
		}
		keys = keys[DBKeyFullSize:] //       Move to the next DBBKey
	}
	return nil, errors.New("not found")
}

// Put
// Put a key value pair into the BFile, return the *DBBKeyFull
//
// Behavior depends on whether history is enabled:
// - With history enabled (k.History != nil): Values are immutable. If the key already exists
//   with a different value, an error is returned. Overwriting with the same value is a no-op.
// - With history disabled (k.History == nil): Values are mutable. Keys can be freely overwritten
//   with different values.
//
// This design supports two use cases:
// 1. Content-addressed storage (history enabled): Where keys are derived from values (e.g., hash)
//    and immutability is required.
// 2. State storage (history disabled): Where keys have an arbitrary relationship to values and
//    need to be updated over time.
func (k *KFile) Put(Key [32]byte, dbBKey *DBBKey) (err error) {
	// If history is enabled, check if this key already exists with a different value
	if k.History != nil {
		// First check the cache
		if existingKey, ok := k.Cache[Key]; ok {
			// Key exists in cache, compare values
			if !bytes.Equal(existingKey.Bytes(Key), dbBKey.Bytes(Key)) {
				return errors.New("cannot overwrite immutable value when history is enabled")
			}
			// If values are the same, this is a no-op
			return nil
		}
		
		// Use the Bloom filter to avoid disk lookup if possible
		// If the Bloom filter says the key doesn't exist, we can skip the expensive disk lookup
		if k.BloomFilter != nil && !k.BloomFilter.Test(Key) {
			// Key definitely doesn't exist, proceed with the write
		} else {
			// Bloom filter says the key might exist, we need to check the file
			existingKey, err := k.kGet(Key)
			if err == nil {
				// Key exists in file, compare values
				if !bytes.Equal(existingKey.Bytes(Key), dbBKey.Bytes(Key)) {
					return errors.New("cannot overwrite immutable value when history is enabled")
				}
				// If values are the same, this is a no-op
				k.Cache[Key] = dbBKey // Update cache with the existing value
				return nil
			} else if err.Error() != "not found" {
				// If there was an error other than "not found", return it
				return err
			}
		}
		// If key doesn't exist, proceed with the write
	}
	
	// Add to the cache
	k.Cache[Key] = dbBKey
	
	// Add to the Bloom filter if it exists
	if k.BloomFilter != nil {
		k.BloomFilter.Set(Key)
	}
	
	k.KeyCnt++
	k.TotalCnt++
	update, err := k.File.Write(dbBKey.Bytes(Key)) // Write the key to the file
	if err != nil {
		// If write fails, remove from cache
		delete(k.Cache, Key)
		return err
	}
	
	if update {                                    // If the file was updated && time to clear cache
		if k.BlocksCached <= 0 {
			if err = k.Close(); err != nil { //           In order to allow access to keys written to disk
				return err //                               the file has to be closed and opened to update
			} //                                            the key offsets
			
			// Save our cache
			tempCache := make(map[[32]byte]*DBBKey)
			for key, val := range k.Cache {
				tempCache[key] = val
			}
			
			// Clear the cache as required by the original implementation
			clear(k.Cache)
			
			if err = k.File.Open(); err != nil { //       Reopen the file
				return err
			}
			
			// Restore our cache
			for key, val := range tempCache {
				k.Cache[key] = val
			}
			
			k.BlocksCached = k.MaxCachedBlocks
		} else {
			k.BlocksCached--
		}
	}
	
	if k.KeyCnt > k.KeyLimit {
		k.KeyCnt = 0
		
		// Push to history - we've modified PushHistory to be synchronous
		if err = k.PushHistory(); err != nil {
			return err
		}
		
		// Clear the cache to reduce memory footprint
		// We don't need to preserve the cache since all keys are now in history
		clear(k.Cache)
	}
	
	return nil
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

	var currentOffset uint64 = uint64(k.HeaderSize) // Set to the location of the first key in the file
	lastIndex := -1                                 // This is the "last" offset processed. Start below 0
	for _, key := range keyList {                   // Note that this never updates the first offset. That's okay, it doesn't change
		index := k.OffsetIndex(key[:]) //       Use the first two bytes as the index
		if lastIndex < int(index) {    //       If this index is greater than the last
			lastIndex++                                 // Don't overwrite the previous offset
			for ; lastIndex < int(index); lastIndex++ { // Update any skipped offsets
				k.Offsets[lastIndex] = currentOffset //
			}
			k.Offsets[lastIndex] = currentOffset //        Set this offset
		}
		currentOffset += DBKeyFullSize //                      Add the size of the entry for next offset
	}

	// Fill in the rest of the offsets table;
	for i := lastIndex + 1; i < int(k.OffsetsCnt); i++ {
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
	_, err = k.File.File.Seek(int64(k.HeaderSize), io.SeekStart)
	if err != nil {
		return nil, nil, err
	}
	keyEntriesBytes, err := io.ReadAll(k.File.File)
	if err != nil {
		return nil, nil, err
	}
	numKeys := len(keyEntriesBytes) / DBKeyFullSize
	if numKeys == 0 {
		return nil, nil, err
	}

	// Create a slice to hold all unique keys, giving priority to NewKeys
	keyValues = make(map[[32]byte]*DBBKey)
	for ; len(keyEntriesBytes) > 0; keyEntriesBytes = keyEntriesBytes[DBKeyFullSize:] {
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
	// Note: Keys must be sorted in ascending order by bin for HistoryFile.AddKeys to work correctly
	sort.Slice(KeyList, func(i, j int) bool {
		a := k.OffsetIndex(KeyList[i][:]) // Bin for a
		b := k.OffsetIndex(KeyList[j][:]) // Bin for b
		return a < b // Sort in ascending order
	})

	return keyValues, KeyList, nil

}
