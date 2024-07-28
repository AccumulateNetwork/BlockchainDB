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
	File      *os.File            // The file being buffered
	Directory string              // Where the DB is on disk
	FileName  string              // + The file name to open, and optional details to create a filename
	Keys      map[[32]byte]DBBKey // The set of keys written to the BFile
	LastKey   [32]byte            // Last Key written to the BFile
	LastValue []byte              // Last Value written to the BFile
	Cache     map[[32]byte][]byte // Cache for key/values that have not yet been written to disk
	Buffer    [BufferSize]byte    // The current BFBuffer under construction
	EOD       uint64              // Offset to the end of Data for the whole file(place to hold it until the file is closed)
	EOB       int                 // End within the buffer
}

// Get
// Get the value for a given DBKeyFull.  The value returned
// is free for the user to use (i.e. not part of a buffer used
// by the BFile)
func (b *BFile) Get(Key [32]byte) (value []byte, err error) {
	value, ok := b.Cache[Key] // If the key value is in the cache, get it
	if ok {                   // and done.
		v := append([]byte{}, value...) // Return a copy of the value
		return v, nil
	}

	dBBKey, ok := b.Keys[Key] // Pull the value from disk
	if !ok {
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
	dbbKey.Offset = b.EOD
	dbbKey.Length = uint64(len(Value))

	b.Keys[Key] = *dbbKey
	err = b.Write(Value)
	return err
}

// Close
// Closes the BFile.  All buffers remain in the buffer pool.  Does not flush
// the buffers to disk.  If that is needed, then caller needs to call BFile.Block()
// after the call to BFile.Close()
func (b *BFile) Close() {

	eod := b.EOD // Keep the current EOD so we can close the BFile properly with an offset to the keys

	keys := make([][DBKeySize]byte, len(b.Keys)) // Collect all the keys into a list to sort them
	i := 0                                // This ensures that all users get the same DBBlocks
	for k, v := range b.Keys {            // Since maps randomize order
		value := v.Bytes(k)              //  Get the value
		keys[i] = [DBKeySize]byte(value) //  Copy each key into a byte entry
		i++                              //  Get the next key
	}

	// Sort all the entries by the keys.  Because no key will be a duplicate, it doesn't matter
	// that the offset and length are at the end of the byte entry
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })

	for _, k := range keys { // Once keys are sorted, write all the keys to the end of the DBBlock
		err := b.Write(k[:]) //
		if err != nil {
			panic(err)
		}
	}
	b.Keys = nil // Drop the reference to the Keys map
	b.File.Write(b.Buffer[:b.EOB])
	if _, err := b.File.Seek(0, io.SeekStart); err != nil { // Seek to start
		panic(err)
	}
	var buff [8]byte                                 // Write out the offset to the keys into
	binary.BigEndian.PutUint64(buff[:], uint64(eod)) // the DBBlock file.
	if _, err := b.File.Write(buff[:]); err != nil { //
		panic(err)
	}
	b.File.Close()

}

// NewBFile
// Creates a new Buffered file.  The caller is responsible for writing the header
func NewBFile(Directory string, Filename string) (bFile *BFile, err error) {
	bFile = new(BFile)                      // create a new BFile
	bFile.Directory = Directory             // Directory for the BFile
	bFile.FileName = Filename               //
	bFile.Cache = make(map[[32]byte][]byte) // Allocate the key/value cache
	bFile.Keys = make(map[[32]byte]DBBKey)  // Allocate the Keys map
	if bFile.File, err = os.Create(filepath.Join(Directory, Filename)); err != nil {
		return nil, err
	}
	var offsetB [8]byte // Offset to end of file (8, the length of the offset)
	if err := bFile.Write(offsetB[:]); err != nil {
		return nil, err
	}
	return bFile, nil
}

// Write
// Writes given Data into the BFile onto the End of the BFile.
// The data is copied into a buffer.  If the buffer is full, it is flushed
// to disk.  If more data is involved, add to the next buffer.  Rinse and
// repeat until all the data is written to disk.
//
// EOB and EOD are updated as needed.
func (b *BFile) Write(Data []byte) error {

	space := BufferSize - b.EOB

	// Write to the current buffer
	dLen := len(Data)
	if dLen <= space { //               If the current buffer has room, just
		copy(b.Buffer[b.EOB:], Data) // add to the buffer then return
		b.EOB += dLen                // Well, after updating offsets...
		b.EOD += uint64(dLen)
		return nil
	}

	if space > 0 {
		copy(b.Buffer[b.EOB:], Data[:space]) // Copy what fits into the current buffer
		b.EOB += space                       // Update b.EOB (should be equal to BufferSize)
		b.EOD += uint64(space)
		Data = Data[space:]
	}

	// Write out the current buffer, and put the rest of the Data into the buffer

	if _, err := b.File.Write(b.Buffer[:b.EOB]); err != nil {
		return err
	}
	b.Cache[b.LastKey] = b.LastValue // Put the last key/value pair in the next cache too.
	b.EOB = 0                        // Start at the beginning of the buffer
	if len(Data) > 0 {               // If more data to write, recurse
		return b.Write(Data) //         Write out the remaining data
	}
	return nil
}

// OpenBFile
// Open a DBBlock file at a given height for read/write access
// The only legitimate writes to a BFile would be to add/update keys
func OpenBFile(Directory, Filename string) (bFile *BFile, err error) {
	filename := filepath.Join(Directory, Filename) // Build the full filename
	b := new(BFile)                                // create a new BFile
	if b.File, err = os.OpenFile(filename, os.O_RDWR, os.ModePerm); err != nil {
		return nil, err
	}
	b.Directory = Directory
	b.FileName = filename
	b.Cache = make(map[[32]byte][]byte)
	var offsetB [8]byte
	if _, err := b.File.Read(offsetB[:]); err != nil {
		return nil, fmt.Errorf("%s is not set up as a BFile", Filename)
	}
	off := binary.BigEndian.Uint64(offsetB[:])
	n, err := b.File.Seek(int64(off), io.SeekStart)
	if err != nil {
		return nil, err
	}
	if uint64(n) != off {
		return nil, fmt.Errorf("offset in %s is %d expected %d", Filename, n, off)
	}

	// Load all the keys into the map
	b.Keys = map[[32]byte]DBBKey{}
	keyList, err := io.ReadAll(b.File)
	cnt := len(keyList) / DBKeySize
	for i := 0; i < cnt; i++ {
		dbBKey := new(DBBKey)
		address, err := dbBKey.Unmarshal(keyList)
		if err != nil {
			return nil, err
		}
		b.Keys[address] = *dbBKey
		keyList = keyList[DBKeySize:]
	}

	// The assumption is that the keys will be over written, and data will be
	// added beginning at the end of the data section (as was stored at offsetB)
	if _, err := b.File.Seek(int64(off), io.SeekStart); err != nil {
		return nil, err
	}
	b.EOD = off
	b.EOB = 0
	return b, err
}

// Compress
// Reads the entire BFile into memory then writes it back out again.
// The BFile is closed.  The new compressed BFile is returned, along with an error
// If an error is reported, the BFile is unchanged.
func (b *BFile) Compress() (newBFile *BFile, err error) {
	keyValues := make(map[[32]byte][]byte)
	for k := range b.Keys {
		value, err := b.Get(k)
		if err != nil {
			return b, err
		}
		keyValues[k] = value
	}

	b.Close()
	os.Remove(b.FileName)
	b2, err := NewBFile(b.Directory, b.FileName)
	if err != nil {
		return b, err // This should never happen
	}

	for k, v := range keyValues {
		b2.Put(k, v)
	}

	return b2, nil

}
