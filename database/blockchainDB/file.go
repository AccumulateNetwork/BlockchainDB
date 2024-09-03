package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// bFile
// A basic buffered file that supports keys and values
const BufferSize = 1024 * 64 // Buffer size for values written to the BFile
var NilKey [32]byte          // The Nil Key (all zeros)

// Block File
// Holds the buffers and ID stuff needed to build DBBlocks (Database Blocks)
type BFile struct {
	Header                         // Offset to EOD, and lookup table into keys
	File       *os.File            // The file being buffered. It doesn't have to be kept open
	Filename   string              // Fully qualified file name for the BFle
	Keys       map[[32]byte]DBBKey // The set of all keys to values in the BFile
	KeysLoaded bool                // Keep up with the keys loaded state
	NewKeys    map[[32]byte]DBBKey // The set of keys collected while filling the current buffer
	LastKey    [32]byte            // Last Key written to the BFile (only one key/value can span buffers)
	LastValue  []byte              // Last Value written to the BFile
	Cache      map[[32]byte][]byte // Cache for key/values that have not yet been written to disk
	Buffer     [BufferSize]byte    // The current BFBuffer under construction
	RecurseCnt int                 //
	EOB        uint64              // End within the buffer
}

// Get
// Get the value for a given DBKeyFull.  The value returned
// is free for the user to use (i.e. not part of a buffer used
// by the BFile)
func (b *BFile) Get(Key [32]byte) (value []byte, err error) {

	// Return the value if it is in the cache waiting to be written
	if value, ok := b.Cache[Key]; ok {
		return value, nil
	}

	// The File needs to be opened to get the value.  If the file
	// was closed, then leave the file closed when done. If the file
	// is opened, then keep the offset and restore the file location
	// once the value is pulled.
	var offset int64
	opened := false
	if err != nil {
		return nil, err
	}
	// Open the file
	if b.File == nil {
		b.Open(false)
		opened = true // Note that we did open
	} else {
		offset, err = b.File.Seek(0, io.SeekCurrent)
	}

	defer func() { // When we leave, close the file if we opened it
		if opened {
			b.Close()
		}
		b.File.Seek(offset, io.SeekStart) // Or restore location if
	}() //                                   the file was opened

	// The header reflects what is on disk.  Find the section where the key is.
	index := b.Index(Key)
	var start, end uint64                 // The header gives us offsets to key sections
	start = b.Offsets[index]              // The index is where the section starts
	if index < uint16(len(b.Offsets)-1) { // If not the last section,
		end = b.Offsets[index+1] //             the next section is the end
	} else { //                              The last section ends at EOF
		if e, err := b.File.Seek(0, io.SeekEnd); err != nil {
			return nil, err //
		} else {
			end = uint64(e) //               Seek gets the offset to the EOF
		}
	}

	if start == end { //                     If the start is the end, the section is empty
		return nil, errors.New("not found")
	}

	keys := make([]byte, end-start) //       Create a buffer for the section

	if _, err = b.File.Seek(int64(start), io.SeekStart); err != nil {
		return nil, err
	}

	if _, err = b.File.Read(keys); err != nil { // Read the section
		return nil, err
	}

	var dbKey DBBKey             //                      Search the keys by unmarshaling each key as we search
	for len(keys) >= DBKeySize { //          Search all DBBKey entries, note they are not sorted.
		adr, err := dbKey.Unmarshal(keys) //
		if err != nil {
			return nil, err
		}
		switch {

		case adr == Key: //                  Is this the key sought
			value = make([]byte, dbKey.Length) // Then read the value
			if _, err = b.File.Seek(int64(dbKey.Offset), io.SeekStart); err != nil {
				return nil, err
			}
			if _, err = b.File.Read(value); err != nil {
				return nil, err
			}
			return value, nil

		default:
			keys = keys[DBKeySize:] //       Move to the next DBBKey
		}
	}

	return nil, errors.New("not found")
}

// Put
// Put a key value pair into the BFile, return the *DBBKeyFull
func (b *BFile) Put(Key [32]byte, Value []byte) (err error) {
	if b.File == nil {
		if b.File, err = os.OpenFile(b.Filename, os.O_RDWR, os.ModePerm); err != nil {
			return err
		}
	}

	b.Cache[Key] = Value

	dbbKey := new(DBBKey) // Save away the Key and offset
	dbbKey.Offset = b.EOD + b.EOB
	dbbKey.Length = uint64(len(Value))

	b.NewKeys[Key] = *dbbKey
	err = b.Write(Value)
	return err
}

// FlushValueBuffer
// FlushValueBuffer the buffer to the BFile.
func (b *BFile) FlushValueBuffer() (err error) {
	if err = b.Open(true); err != nil {
		return err
	}
	if _, err = b.File.Seek(int64(b.EOD), io.SeekStart); err != nil {
		return err
	}
	if _, err := b.File.Write(b.Buffer[:b.EOB]); err != nil {
		return err
	}
	b.EOD += b.EOB // Writing the buffer moves the EOD
	b.EOB = 0      // Clear the buffer "end of buffer" mark
	return nil
}

// Close
// Take everything in flight and write it to disk, then close the file.
// Note that if an error occurs while updating the BFile, the BFile
// will be trashed.
func (b *BFile) Close() error {

	if err := b.Open(true); err != nil { // Do a proper open to get the proper header and
		return err //                     key set.
	}

	if err := b.FlushValueBuffer(); err != nil { // Flush all pending values to the BFile
		return err
	}

	// Create a slice to hold all unique keys, giving priority to NewKeys
	allKeys := make([][32]byte, 0, len(b.Keys)+len(b.NewKeys))
	allValues := make(map[[32]byte]DBBKey)

	// Collect all the key values that are not overwritten by b.NewKeys
	for k, v := range b.Keys {
		if _, exists := b.NewKeys[k]; !exists {
			allValues[k] = v
			allKeys = append(allKeys, k)
		}
	}

	// Update the keys with the NewKeys
	for k, v := range b.NewKeys {
		allValues[k] = v
		allKeys = append(allKeys, k)
	}

	// Sort the keys into their offset bins.  They won't be sorted inside the bins.
	sort.Slice(allKeys, func(i, j int) bool {
		a := binary.BigEndian.Uint16(allKeys[i][:])
		b := binary.BigEndian.Uint16(allKeys[j][:])
		return a%NumOffsets < b%NumOffsets
	})

	// Now we have a set of sorted keys

	currentOffset := b.EOD      // The offset to the end of values/beginning of keys.
	lastIndex := -1             // This is the "last" offset processed. Start below 0
	for _, k := range allKeys { // Note that this never updates the first offset. That's okay, it doesn't change
		index := b.Index(k)         // Use the first two bytes as the index
		if lastIndex < int(index) { // If this index is greater than the last
			lastIndex++                                 //     Don't overwrite the previous offset
			for ; lastIndex < int(index); lastIndex++ { //     Update any skipped offsets
				b.Offsets[lastIndex] = currentOffset //
			}
			b.Offsets[lastIndex] = currentOffset //            Set this offset
		}
		currentOffset += DBKeySize //                          Add the size of the entry for next offset
	}

	// Fill in the rest of the offsets table; Usually we set the last offset twice.
	for i := lastIndex + 1; i < NumOffsets; i++ {
		b.Header.Offsets[i] = currentOffset
	}

	// Write the updated header
	if _, err := b.File.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := b.File.Write(b.Header.Marshal()); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := b.File.Seek(int64(b.EOD), io.SeekStart); err != nil {
		return err
	}
	for _, k := range allKeys {
		b.File.Write(allValues[k].Bytes(k))
	}

	// Close the file, and clear all cache-like stuff.
	b.File.Close()
	b.File = nil
	clear(b.Cache)
	clear(b.Keys)
	clear(b.NewKeys)
	b.LastValue = nil
	return nil
}

// NewBFile
// Creates a new Buffered file. A Header is created for an empty BFile
func NewBFile(Filename string) (bFile *BFile, err error) {
	bFile = new(BFile)                        // create a new BFile
	bFile.Filename = Filename                 //
	bFile.Cache = make(map[[32]byte][]byte)   // Allocate the key/value cache
	bFile.Keys = make(map[[32]byte]DBBKey)    // Allocate the Keys map
	bFile.NewKeys = make(map[[32]byte]DBBKey) // Allocate the NewKeys map
	if bFile.File, err = os.Create(Filename); err != nil {
		return nil, err
	}
	bFile.Header = NewHeader()   // Create the default header
	bFile.EOD = HeaderSize       // Set Current EOD per the header
	hs := bFile.Header.Marshal() // Write the Header to disk
	bFile.File.Write(hs)
	return bFile, nil
}

// ClearBFile
// Creates a new Buffered file. The caller's responsible for writing the header
func (b *BFile) ClearBFile() (err error) {
	clear(b.Cache)
	clear(b.Keys)
	clear(b.NewKeys)
	if b.File, err = os.Create(b.Filename); err != nil {
		return err
	}
	var offsetB [8]byte // Offset to end of file (8, the length of the offset)
	if err := b.Write(offsetB[:]); err != nil {
		return err
	}
	return nil
}

// Write
// Writes given Data into the BFile onto the End of the BFile.
// The data is copied into a buffer.  If the buffer is full, it is flushed
// to disk.  If more data is involved, add to the next buffer.  Rinse and
// repeat until all the data is written to disk.
//
// EOB and EOD are updated as needed.
func (b *BFile) Write(Data []byte) (err error) {

	space := uint64(BufferSize - b.EOB)

	// Write to the current buffer
	dLen := len(Data)
	if dLen <= int(space) { //          If the current buffer has room, just
		copy(b.Buffer[b.EOB:], Data) // add to the buffer then return
		b.EOB += uint64(dLen)        // Well, after updating offsets...
		return nil
	}

	if space > 0 {
		copy(b.Buffer[b.EOB:], Data[:space]) // Copy what fits into the current buffer
		b.EOB += space                       // Update b.EOB (should be equal to BufferSize)
		Data = Data[space:]
	}

	// Write out the current buffer, and put the rest of the Data into the buffer

	if b.File == nil {
		if err := b.Open(); err != nil { // Open the file
			return err
		}
	}

	defer func() {
		if err == nil {
			if len(b.NewKeys) > 10000 {
				err = b.Close()
				b.File, err = os.Open(b.Filename)
			}
		}
	}()

	if _, err := b.File.Write(b.Buffer[:b.EOB]); err != nil {
		return err
	}
	if b.LastValue != nil {
		b.Cache[b.LastKey] = b.LastValue // Put the last key/value pair in the next cache too.
	}
	b.EOD += b.EOB     // Update the offset to the end of data
	b.EOB = 0          // Start at the beginning of the buffer
	if len(Data) > 0 { // If more data to write, recurse
		b.RecurseCnt++
		return b.Write(Data) //         Write out the remaining data
	}
	return nil
}

// OpenBFile
// Open a DBBlock file at a given height for read/write access
// The only legitimate writes to a BFile would be to add/update keys
func OpenBFile(Filename string) (bFile *BFile, err error) {
	b := new(BFile) // create a new BFile
	b.Filename = Filename
	if err := b.Open(); err != nil {
		return nil, err
	}
	return b, nil
}

// Open
// Open a BFile that has been closed. Report any errors.  If the BFile is
// open, do nothing.
func (b *BFile) Open(LoadKeys bool) (err error) {
	b.Close()
	if b.File != nil && (!LoadKeys || b.KeysLoaded) {
		return nil
	}
	if b.File == nil {
		if b.File, err = os.OpenFile(b.Filename, os.O_RDWR, os.ModePerm); err != nil {
			return err
		}
	}
	var offsetB [HeaderSize]byte
	if _, err := b.File.Read(offsetB[:]); err != nil {
		return fmt.Errorf("%s is not set up as a BFile", b.Filename)
	}

	b.Header.Unmarshal(offsetB[:])

	n, err := b.File.Seek(int64(b.EOD), io.SeekStart)
	if err != nil {
		return err
	}
	if uint64(n) != b.EOD {
		return fmt.Errorf("offset in %s is %d expected %d", b.Filename, n, b.EOD)
	}

	if LoadKeys {
		// Load all the keys into the map
		b.Keys = map[[32]byte]DBBKey{}
		if _, err = b.File.Seek(int64(b.EOD), io.SeekStart); err != nil {
			return err
		}
		keyList, err := io.ReadAll(b.File)
		if err != nil {
			return err
		}
		cnt := len(keyList) / DBKeySize
		for i := 0; i < cnt; i++ {
			dbBKey := new(DBBKey)
			address, err := dbBKey.Unmarshal(keyList)
			if err != nil {
				return err
			}
			b.Keys[address] = *dbBKey
			keyList = keyList[DBKeySize:]
		}

		// The assumption is that the keys will be over written, and data will be
		// added where the keys start at this point
		if _, err := b.File.Seek(int64(b.EOD), io.SeekStart); err != nil {
			return err
		}
		b.KeysLoaded = true
	}
	b.EOB = 0
	return err
}

// Compress
// Reads the entire BFile into memory then writes it back out again.
// The BFile is closed.  The new compressed BFile is returned, along with an error
// If an error is reported, the BFile is unchanged.
func (b *BFile) Compress() (err error) {
	if err = b.Close(); err != nil {
		return err
	}

	// Close will force all key/values in flight to disk
	if err := b.Open(); err != nil { // Open in case the BFile is closed
		return err
	}
	keyValues := make(map[[32]byte][]byte) // Collect all the key value pairs
	for k := range b.Keys {
		value, err := b.Get(k)
		if err != nil {
			return err
		}
		keyValues[k] = value
	}

	if err = b.Close(); err != nil {
		return err
	}

	if err = b.ClearBFile(); err != nil { // Reset the file and rebuild it
		return err
	}

	var keys = make([][32]byte, len(keyValues))
	i := 0
	var nilKey [32]byte // the nil key
	for k := range keyValues {
		if k != nilKey {
			keys[i] = k
			i++
		}
	}
	// Sort all the keys.  A compressed BFile at the same height will be produced by all
	// nodes. Writing the keys to the new BFile will put the values in the same order as
	// the keys
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })

	for _, k := range keys { // Populate the new file with all the relevant key/value pairs
		b.Put(k, keyValues[k])
	}

	return nil

}
