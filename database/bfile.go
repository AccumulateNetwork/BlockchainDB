package blockchainDB

import (
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Buffered File

const BufferSize = 1024 * 64 // Buffer size for values written to the BFile

// Block BFile
// Holds the buffers and ID stuff needed to build DBBlocks (Database Blocks)
type BFile struct {
	File     *os.File         // The file being buffered. It doesn't have to be kept open
	Filename string           // Fully qualified file name for the BFle
	Buffer   [BufferSize]byte // The current BFBuffer under construction
	EOB      uint64           // End within the buffer
}

// NewBFile
// Any extension provided will be ignored in favor of ".dat"
// A file name such as
//
//	"/tmp/523423/filename.txt"
//
// will result in creating
//
//	"/tmp/523423/filename_keys.dat"
func NewBFile(filename string) (file *BFile, err error) {
	file = new(BFile)
	base := filepath.Base(filename)           // Get the base of the file
	ext := filepath.Ext(base)                 // Get whatever extension given
	baseName := strings.TrimSuffix(base, ext) // update name, add ".dat"
	file.Filename = baseName + "_keys.dat"    // Set the Filename
	file.File, err = os.Create(file.Filename) // Create the file
	return file, err                          //
}

// OpenBFile
func OpenBFile(filename string) (file *BFile, err error) {
	file = new(BFile)
	base := filepath.Base(filename)           // Get the base of the file
	ext := filepath.Ext(base)                 // Get whatever extension given
	baseName := strings.TrimSuffix(base, ext) // update name, add ".dat"
	file.Filename = baseName + "_keys.dat"    // Set the Filename
	file.File, err = os.Create(file.Filename) // Create the file
	return file, err                          //
}

// Open
// Opens the underlying file and positions the file location to
// the end of the file.
func (b *BFile) Open() (err error) {

	if b.File != nil { // Don't try if already open
		return nil
	}

	if b.File, err = os.OpenFile(b.Filename, os.O_RDWR, os.ModePerm); err != nil {
		return err
	}
	if _, err = b.File.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// WriteAt
// Seek to the offset from start and write data ito the BFile
func (b *BFile) WriteAt(offset int64, data []byte) (err error) {
	if err = b.Open(); err != nil {
		return err
	}

	if _, err = b.File.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if _, err = b.File.Write(data); err != nil {
		return err
	}
	return nil
}

// ReadAt
// Seek to the offset from start and read into the given data buffer
func (b *BFile) ReadAt(offset int64, data []byte) (err error) {
	if err = b.Open(); err != nil {
		return err
	}

	if _, err = b.File.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if _, err = b.File.Read(data); err != nil {
		return err
	}

	return nil
}

// Flush
// Write out the buffer, and reset the EOB
func (b *BFile) Flush() (err error) {
	b.Open()
	if _, err = b.File.Write(b.Buffer[:b.EOB]); err != nil {
		return err
	}
	b.EOB = 0
	return err
}

// Close
// Close the underlying file
func (b *BFile) Close() (err error) {
	if err = b.Flush(); err != nil {
		return err
	}
	if err = b.Close(); err != nil {
		return err
	}
	return nil
}

// Write
// A Buffered Write given Data into the File. Returns:
//
// update -- true if a actual file update occurs
// err    -- nil on no error, the error if an error occurs
func (b *BFile) Write(Data []byte) (update bool, err error) {

	b.Open()

	space := uint64(BufferSize - b.EOB)

	// Write to the current buffer
	dLen := len(Data)
	if dLen <= int(space) { //          If the current buffer has room, just
		copy(b.Buffer[b.EOB:], Data) // add to the buffer then return
		b.EOB += uint64(dLen)        // Well, after updating offsets...
		return false, nil
	}

	if space > 0 {
		copy(b.Buffer[b.EOB:], Data[:space]) // Copy what fits into the current buffer
		b.EOB += space                       // Update b.EOB (should be equal to BufferSize)
		Data = Data[space:]
	}

	// Write out the current buffer, and put the rest of the Data into the buffer

	if b.File == nil {
		if b.File, err = os.OpenFile(b.Filename, os.O_RDWR, os.ModePerm); err != nil {
			return false, err
		}
	}

	if _, err := b.File.Seek(0, io.SeekEnd); err != nil {
		return false, err
	}

	if _, err := b.File.Write(b.Buffer[:b.EOB]); err != nil {
		return false, err
	}

	b.EOB = 0          //       Start at the beginning of the buffer
	if len(Data) > 0 { //       If more data to write, recurse
		return b.Write(Data) // Write out the remaining data
	}
	return true, nil
}
