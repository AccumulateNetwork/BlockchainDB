package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// BFile
// This is a Block buffered file designed to support a write only file
// where data is only appended to the file.
//
// BFile keeps a set of buffers.
//
// Each Buffer:
//    The first 8 bytes points to the end of the data portion of the
//    the file.
//
//    UpdateEndOfData updates the end of data offset
//
//    Key entries follow the EndOfData
//
//    Data is added to the buffer until it is full.
//
//    When the buffer is full, a go routine is created to flush
//    the buffer is written to disk.
//
//    Once the buffer is reset, it is put back into the waiting channel
//
// When the BFile is closed, the first 8 bytes are updated to point
// to the end of the file
//

const (
	BufferSize = 64 * 1024 * 1 // N MB, i.e. N *(1024^2)

	BFilePerm    = iota // Key/Value pairs where the key is a function of the Value (can't change)
	BFileDynamic        // Key/Value pair where the value can be updated

	BFileDN = iota // Some partitions. Could do this some other way? Use Strings?
	BFileBVN0
	BFileBVN1
	BFileBVN2
	BFileBVN3
	BFileBVN4
	BFileBVN5
	BFileBVN6
	BFileBVN7
	BFileBVN8
	BFileBVN9
	BFileBVN10
)

// Block File
// Holds the buffers and ID stuff needed to build DBBlocks (Database Blocks)
type BFile struct {
	Mutex     sync.Mutex          // Locking to allow compression to run
	OSFile    *OSFile             // The file being buffered
	FileName  string              // + The file name to open, and optional details to create a filename
	Keys      map[[32]byte]DBBKey // The set of keys written to the BFile
	BuffPool  chan *BFBuffer      // Buffer Pool (buffers not in use)
	KVCache   *BFBuffer           // The current KVCache under construction
	KVCaches  []*BFBuffer         // List of KVCaches to support Get while key/values are in flight
	BufferCnt int                 // Number of buffers used by the bfWriter
	bfWriter  *BFileWriter        // Writes buffers to the File
	EOD       uint64              // Offset to the end of Data for the whole file(place to hold it until the file is closed)
	EOB       int                 // End of the current buffer... Where to put the next data 'write'
}

// cacheGet
// Internal use
// Return the value for the key if it is in the any of the buffer's BFBuffer
// Return nil if not found.
func (b *BFile) cacheGet(Key [32]byte) (value []byte) {
	for _, b := range b.KVCaches {
		v := b.GetFromCache(Key)
		if v != nil {
			return v
		}
	}
	return nil
}

// Get
// Get the value for a given DBKeyFull.  The value returned
// is free for the user to use (i.e. not part of a buffer used
// by the BFile)
func (b *BFile) Get(Key [32]byte) (value []byte, err error) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	return b.get(Key)
}
// get 
// Internal
// Does a get without messing with locking
func (b *BFile) get(Key [32]byte) (value []byte, err error) {

	value = b.cacheGet(Key) // If the key value is in the cache, get it
	if value != nil {       // and done.
		v := append([]byte{}, value...)
		return v, nil
	}

	dBBKey, ok := b.Keys[Key] // Pull the value from disk
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	b.OSFile.Lock()
	defer b.OSFile.UnLock()

	if _, err = b.OSFile.Seek(int64(dBBKey.Offset), io.SeekStart); err != nil {
		return nil, err
	}
	value = make([]byte, dBBKey.Length)
	_, err = b.OSFile.Read(value)
	return value, err
}

// Put
// Put a key value pair into the BFile, return the *DBBKeyFull
func (b *BFile) Put(Key [32]byte, Value []byte) (err error) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	return b.put(Key,Value)
}
// put
// Internal
// Does a put without locking
func (b *BFile) put(Key [32]byte, Value []byte) (err error) {

	b.KVCache.Put2Cache(Key, Value) // Caches the key value so we can Get it

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
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	b.close()
}
// close
// Internal
// Close without locking
func (b *BFile) close() {

	if b.bfWriter != nil {
		eod := b.EOD // Keep the current EOD so we can close the BFile properly with an offset to the keys

		keys := make([][48]byte, len(b.Keys)) // Collect all the keys into a list to sort them
		i := 0                                // This ensures that all users get the same DBBlocks
		for k, v := range b.Keys {            // Since maps randomize order
			value := v.Bytes(k)       //         Get the value
			keys[i] = [48]byte(value) //         Copy each key into a 48 byte entry
			i++                       //         Get the next key
		}
		b.Keys = make(map[[32]byte]DBBKey) //    Once we have the list of keys, we don't need the map anymore

		// Sort all the entries by the keys.  Because no key will be a duplicate, it doesn't matter
		// that the offset and length are at the end of the 48 byte entry
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })

		for _, k := range keys { // Once keys are sorted, write all the keys to the end of the DBBlock
			err := b.Write(k[:]) //
			if err != nil {
				panic(err)
			}
		}
		b.Keys = nil                            // Drop the reference to the Keys map
		b.bfWriter.Close(b.KVCache, b.EOB, eod) // Close that file
		b.bfWriter = nil                        // kill any reference to the bfWriter
		b.KVCache = nil                         // Close writes the buffer, and the file is closed. clear the buffer
	}
}

// Block
// Block waits until all buffers have been returned to the BufferPool.
func (b *BFile) Block() {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	b.block()
}
// block
// Internal
// Block without locking
func (b *BFile) block() {

	for len(b.BuffPool) < b.BufferCnt {
		time.Sleep(time.Microsecond * 50)
	}
}

// createBuffers
// Internal
// Create the buffers for a BFile, and set up the BWriter
func (b *BFile) createBuffers() {
	b.BuffPool = make(chan *BFBuffer, b.BufferCnt) // Create the waiting channel
	for i := 0; i < b.BufferCnt; i++ {
		bfb := NewBFBuffer()
		b.BuffPool <- bfb // Put some buffers in the waiting queue
		b.KVCaches = append(b.KVCaches, bfb)
	}
	b.bfWriter = NewBFileWriter(b.OSFile, b.BuffPool)
}

// NewBFile
// Creates a new Buffered file.  The caller is responsible for writing the header
func NewBFile(Filename string, BufferCnt int) (bFile *BFile, err error) {
	bFile = new(BFile)                     // create a new BFile
	bFile.FileName = Filename              //
	bFile.Keys = make(map[[32]byte]DBBKey) // Allocate the Keys map
	bFile.BufferCnt = BufferCnt            // How many buffers we are going to use
	if bFile.OSFile, err = NewOSFile(Filename); err != nil {
		return nil, err
	}
	bFile.createBuffers()

	var offsetB [8]byte // Offset to end of file (8, the length of the offset)
	if err := bFile.Write(offsetB[:]); err != nil {
		return nil, err
	}
	return bFile, nil
}

// space
// Internal
// Returns the number of bytes to the end of the current buffer
func (b *BFile) space() int {
	return BufferSize - b.EOB
}

// Write
// Writes given Data into the BFile onto the End of the BFile.
// The data is copied into a buffer.  If the buffer is full, it is flushed
// to disk.  Left over data goes into the next buffer.
// EOB and EOD are updated as needed.
func (b *BFile) Write(Data []byte) error {

	if b.KVCache == nil { // Get a buffer if it is needed
		b.KVCache = <-b.BuffPool
		b.EOB = 0
	}

	space := b.space()
	// Write to the current buffer
	dLen := len(Data)
	if dLen <= space { //               If the current buffer has room, just
		copy(b.KVCache.Buffer[b.EOB:], Data) // add to the buffer then return
		b.EOB += dLen                        // Well, after updating offsets...
		b.EOD += uint64(dLen)
		return nil
	}

	if space > 0 {
		copy(b.KVCache.Buffer[b.EOB:], Data[:space]) // Copy what fits into the current buffer
		b.EOB += space                               // Update b.EOB (should be equal to BufferSize)
		b.EOD += uint64(space)
		Data = Data[space:]
	}

	// Write out the current buffer, get the other buffer, and put the rest of Value there.
	lastKey := b.KVCache.lastKey            // The next Buffer gets the lastKey
	lastValue := b.KVCache.lastValue        //  and lastValue of the previous buffer in its cache
	b.bfWriter.Write(b.KVCache, b.EOB)      // Write out this buffer
	b.KVCache = <-b.BuffPool                // Get the next buffer
	b.KVCache.Put2Cache(lastKey, lastValue) // Put the last key/value pair in the next cache too.

	b.EOB = 0            // Start at the beginning of the buffer
	return b.Write(Data) // Write out the remaining data
}

// OpenBFile
// Open a DBBlock file at a given height for read/write access
// The only legitimate writes to a BFile would be to add/update keys
func OpenBFile(Filename string, BufferCnt int) (bFile *BFile, err error) {
	b := new(BFile) // create a new BFile
	b.BufferCnt = BufferCnt
	b.createBuffers()
	if b.OSFile, err = OpenOSFile(Filename, os.O_RDWR, os.ModePerm); err != nil {
		return nil, err
	}

	var offsetB [8]byte
	if _, err := b.OSFile.Read(offsetB[:]); err != nil {
		return nil, fmt.Errorf("%s is not set up as a BFile", Filename)
	}
	off := binary.BigEndian.Uint64(offsetB[:])
	n, err := b.OSFile.Seek(int64(off), io.SeekStart)
	if err != nil {
		return nil, err
	}
	if uint64(n) != off {
		return nil, fmt.Errorf("offset in %s is %d expected %d", Filename, n, off)
	}

	// Load all the keys into the map
	b.Keys = map[[32]byte]DBBKey{}
	keyList, err := io.ReadAll(b.OSFile)
	cnt := len(keyList) / 48
	for i := 0; i < cnt; i++ {
		dbBKey := new(DBBKey)
		address, err := dbBKey.Unmarshal(keyList)
		if err != nil {
			return nil, err
		}
		b.Keys[address] = *dbBKey
		keyList = keyList[48:]
	}

	// The assumption is that the keys will be over written, and data will be
	// added beginning at the end of the data section (as was stored at offsetB)
	if _, err := b.OSFile.Seek(int64(off), io.SeekStart); err != nil {
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
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	keyValues := make(map[[32]byte][]byte)
	i := 0
	for k := range b.Keys {
		value, err := b.get(k)
		i++
		if err != nil {
			return b, err
		}
		keyValues[k] = value
	}

	b.close()
	b.block()

	b2, err := NewBFile(b.FileName, b.BufferCnt)
	if err != nil {
		return b, err // This should never happen
	}

	for k, v := range keyValues {
		b2.put(k, v)
	}

	return b2, nil

}
