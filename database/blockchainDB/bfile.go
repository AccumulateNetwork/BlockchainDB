package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// BFile
// A Buffered File for a Key/Value database
// Writes are buffered and cached then written to disk on nice file system boundaries

const (
	BufferSize = 64 * 1024 * 1 // N MB, i.e. N *(1024^2)
)

// Block File
// Holds the buffers and ID stuff needed to build DBBlocks (Database Blocks)
type BFile struct {
	File         *os.File            // The file being buffered. It doesn't have to be kept open
	Directory    string              // Where the DB is on disk
	Filename     string              // + The file name to open, and optional details to create a filename
	Keys         map[[32]byte]DBBKey // The set of all keys to values in the BFile
	NewKeys      map[[32]byte]DBBKey // The set of keys collected while filling the current buffer
	LastKey      [32]byte            // Last Key written to the BFile (only one key/value can span buffers)
	LastValue    []byte              // Last Value written to the BFile
	Cache        map[[32]byte][]byte // Cache for key/values that have not yet been written to disk
	Buffer       [BufferSize]byte    // The current BFBuffer under construction
	OffsetToKeys uint64              // Offset to the end of Data for the whole file(place to hold it until the file is closed)
	EOB          int                 // End within the buffer
}

// Get
// Get the value for a given DBKeyFull.  The value returned
// is free for the user to use (i.e. not part of a buffer used
// by the BFile)
func (b *BFile) Get(Key [32]byte) (value []byte, err error) {
	return b.get(Key, true)
}

// get
// Internal get allows us to repeatedly get values without closing the file.
// Compress really needs this.
func (b *BFile) get(Key [32]byte, close bool) (value []byte, err error) {
	// Check the cache
	value, ok := b.Cache[Key] // If the key value is in the cache, get it
	if ok {                   // and done.
		v := append([]byte{}, value...) // Return a copy of the value
		return v, nil
	}

	// Check the new keys (avoids loading the whole BFile)
	if dBBKey, ok := b.NewKeys[Key]; ok {
		filename := filepath.Join(b.Directory, b.Filename) // Build the full filename
		file, err := os.OpenFile(filename, os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}
		if _, err = file.Seek(int64(dBBKey.Offset), io.SeekStart); err != nil {
			return nil, err
		}
		value = make([]byte, dBBKey.Length)
		_, err = b.File.Read(value)
		return value, err
	}

	// If not in the cache and not in the NewKeys, then a full load of the BFile
	// is required.
	if err := b.Open(); err != nil {
		return nil, err
	}

	// We are far slower if we close after a get.
	if close {
		defer func() {
			b.File.Close() // Close the file, and
			b.File = nil   // Mark File as closed
			b.Keys = nil   // Clear the keys
		}()
	}

	dBBKey, ok := b.Keys[Key] // Make sure the key is defined.  If not
	if !ok {                  // then there is nothing to do
		return nil, fmt.Errorf("not found")
	}

	if _, err = b.File.Seek(int64(dBBKey.Offset), io.SeekStart); err != nil {
		return nil, err
	}
	value = make([]byte, dBBKey.Length)
	_, err = b.File.Read(value)
	return value, err
}

// Put
// Put a key value pair into the BFile, return the *DBBKeyFull
func (b *BFile) Put(Key [32]byte, Value []byte) (err error) {

	b.Cache[Key] = Value

	dbbKey := new(DBBKey) // Save away the Key and offset
	dbbKey.Offset = b.OffsetToKeys
	dbbKey.Length = uint64(len(Value))

	b.NewKeys[Key] = *dbbKey
	err = b.Write(Value)
	return err
}

// Flush
// Flush the buffer to the BFile.
func (b *BFile) Flush() (err error) {
	if err = b.Open(); err != nil {
		return err
	}
	if _, err := b.File.Write(b.Buffer[:b.EOB]); err != nil {
		return err
	}
	b.EOB = 0 // Clear the buffer "end of buffer" mark
	return nil
}

// Close
// Take everything in flight and write it to disk, then close the file.
// Note that if an error occurs while updating the BFile, the BFile
// will be trashed.
func (b *BFile) Close() error {

	if err := b.Open(); err != nil {
		return err
	}

	if err := b.Flush(); err != nil { // Flush all pending values to the BFile
		return err
	}

	// Write the offset to keys (end of data, i.e. EOD) to disk
	if _, err := b.File.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var buff [8]byte                                            // Write out the offset to the keys into
	binary.BigEndian.PutUint64(buff[:], uint64(b.OffsetToKeys)) // the DBBlock file.
	if _, err := b.File.Write(buff[:]); err != nil {            //
		return err
	}
	// Put the file pointer to the offsetToKeys.  Because the following
	// code writes out all the keys.
	if _, err := b.File.Seek(int64(b.OffsetToKeys), io.SeekStart); err != nil {
		return err
	}

	// Create a byte slice to hold all keys
	keyCount := len(b.Keys) + len(b.NewKeys)
	keySlice := make([]byte, keyCount*DBKeySize)

	// Encode all the keys into the byte slice
	i := 0
	nilKey := [32]byte{}
	encodeKey := func(k [32]byte, v DBBKey) {
		if k != nilKey {
			copy(keySlice[i:], v.Bytes(k))
			i += DBKeySize
		}
	}

	for k, v := range b.Keys {
		encodeKey(k, v)
	}
	for k, v := range b.NewKeys {
		encodeKey(k, v)
	}

	// Write all the keys to the end of the DBBlock in one operation
	if _, err := b.File.Write(keySlice); err != nil {
		return fmt.Errorf("failed to write keys: %w", err)
	}

	// Close the file, and clear all cache like stuff.
	b.File.Close()
	b.File = nil
	clear(b.Cache)
	clear(b.Keys)
	clear(b.NewKeys)
	b.LastValue = nil
	b.OffsetToKeys = 0
	return nil
}

// NewBFile
// Creates a new Buffered file.  The caller is responsible for writing the header
func NewBFile(Directory string, Filename string) (bFile *BFile, err error) {
	bFile = new(BFile)                        // create a new BFile
	bFile.Directory = Directory               // Directory for the BFile
	bFile.Filename = Filename                 //
	bFile.Cache = make(map[[32]byte][]byte)   // Allocate the key/value cache
	bFile.Keys = make(map[[32]byte]DBBKey)    // Allocate the Keys map
	bFile.NewKeys = make(map[[32]byte]DBBKey) // Allocate the NewKeys map
	if bFile.File, err = os.Create(filepath.Join(Directory, Filename)); err != nil {
		return nil, err
	}
	var offsetB [8]byte // Offset to end of file (8, the length of the offset)
	if err := bFile.Write(offsetB[:]); err != nil {
		return nil, err
	}
	return bFile, nil
}

// ClearBFile
// Creates a new Buffered file.  The caller is responsible for writing the header
func (b *BFile) ClearBFile() (err error) {
	clear(b.Cache)
	clear(b.Keys)
	clear(b.NewKeys)
	if b.File, err = os.Create(filepath.Join(b.Directory, b.Filename)); err != nil {
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

	space := BufferSize - b.EOB

	// Write to the current buffer
	dLen := len(Data)
	if dLen <= space { //               If the current buffer has room, just
		copy(b.Buffer[b.EOB:], Data) // add to the buffer then return
		b.EOB += dLen                // Well, after updating offsets...
		b.OffsetToKeys += uint64(dLen)
		return nil
	}

	if space > 0 {
		copy(b.Buffer[b.EOB:], Data[:space]) // Copy what fits into the current buffer
		b.EOB += space                       // Update b.EOB (should be equal to BufferSize)
		b.OffsetToKeys += uint64(space)
		Data = Data[space:]
	}

	// Write out the current buffer, and put the rest of the Data into the buffer

	if err := b.Open(); err != nil { // Open the file
		return err
	}

	defer func() {
		if err == nil {
			if len(b.NewKeys) > 10000 {
				err = b.Close()
			}
		}
	}()

	if _, err := b.File.Write(b.Buffer[:b.EOB]); err != nil {
		return err
	}
	if b.LastValue != nil {
		b.Cache[b.LastKey] = b.LastValue // Put the last key/value pair in the next cache too.
	}
	b.EOB = 0          // Start at the beginning of the buffer
	if len(Data) > 0 { // If more data to write, recurse
		return b.Write(Data) //         Write out the remaining data
	}
	return nil
}

// OpenBFile
// Open a DBBlock file at a given height for read/write access
// The only legitimate writes to a BFile would be to add/update keys
func OpenBFile(Directory, Filename string) (bFile *BFile, err error) {
	b := new(BFile) // create a new BFile
	b.Directory = Directory
	b.Filename = Filename
	if err := b.Open(); err != nil {
		return nil, err
	}
	return b, nil
}

// Open
// Open a BFile that has been closed. Report any errors.  If the BFile is
// open, do nothing.
func (b *BFile) Open() (err error) {
	if b.File != nil {
		return nil
	}
	filename := filepath.Join(b.Directory, b.Filename) // Build the full filename
	if b.File, err = os.OpenFile(filename, os.O_RDWR, os.ModePerm); err != nil {
		return err
	}
	var offsetB [8]byte
	if _, err := b.File.Read(offsetB[:]); err != nil {
		return fmt.Errorf("%s is not set up as a BFile", b.Filename)
	}
	EOD := binary.BigEndian.Uint64(offsetB[:])
	n, err := b.File.Seek(int64(EOD), io.SeekStart)
	if err != nil {
		return err
	}
	if uint64(n) != EOD {
		return fmt.Errorf("offset in %s is %d expected %d", b.Filename, n, EOD)
	}

	// Load all the keys into the map
	b.Keys = map[[32]byte]DBBKey{}
	keyList, err := io.ReadAll(b.File)
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
	// added beginning at the end of the data section (as was stored at offsetB)
	if _, err := b.File.Seek(int64(EOD), io.SeekStart); err != nil {
		return err
	}

	b.OffsetToKeys = EOD
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
		value, err := b.get(k, false)
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
