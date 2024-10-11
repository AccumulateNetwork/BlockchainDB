package blockchainDB

import (
	"fmt"
	"io"
	"os"
)

// Buffered File

const BufferSize = 1024 * 16 // Buffer size for values written to the BFile

var nilKey [32]byte

// Block BFile
// Holds the buffers and ID stuff needed to build DBBlocks (Database Blocks)
type BFile struct {
	File     *os.File         // The file being buffered. It doesn't have to be kept open
	Filename string           // Fully qualified file name for the BFle
	Buffer   [BufferSize]byte // The current BFBuffer under construction
	EOB      uint64           // End within the buffer
	EOD      uint64           // Current EOD
}

// Open
// Open an existing BFile given a fully qualified filename
func OpenBFile(filename string) (bFile *BFile, err error) {
	bFile = new(BFile)
	bFile.Filename = filename
	if bFile.File, err = os.OpenFile(filename, os.O_RDWR, os.ModePerm); err != nil {
		return nil, err
	}
	if fileInfo, err := os.Stat(filename); err != nil {
		return nil, err
	} else {
		bFile.EOD = uint64(fileInfo.Size())
	}
	bFile.EOB = 0
	return bFile, err
}

// NewBFile
// Create a new BFile.  An existing one will be overwritten
func NewBFile(filename string) (file *BFile, err error) {
	file = new(BFile)
	file.Filename = filename
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
	if eod, err := b.File.Seek(0, io.SeekEnd); err != nil {
		return err
	} else {
		b.EOD = uint64(eod)
	}
	return nil
}

// Flush
// Write out the buffer, and reset the EOB
func (b *BFile) Flush() (err error) {
	err = b.Open()
	if err != nil {
		return err
	}
	eod, err := b.File.Seek(0, io.SeekEnd)
	b.EOD = uint64(eod)
	if err != nil {
		return err
	}
	delta, err := b.File.Write(b.Buffer[:b.EOB])
	if err != nil {
		return err
	}
	b.EOB = 0
	b.EOD += uint64(delta)
	return err
}

// Close
// Close the underlying file
func (b *BFile) Close() (err error) {
	if err = b.Flush(); err != nil {
		return err
	}
	err = b.File.Close()
	b.File = nil
	return err
}

// Offset
// Returns the current real size (file + EOB) of the BFile
func (b *BFile) Offset() (offset uint64, err error) {
	// Question: Why isn't b.EOD + b.EOB sufficient?
	fileInfo, err := b.File.Stat()
	if err != nil {
		return 0, err
	}
	offset = uint64(fileInfo.Size()) + b.EOB
	return offset, nil
}

// Write
// A Buffered Write given Data into the File. Returns:
//
// update -- true if a actual file update occurs
// err    -- nil on no error, the error if an error occurs
func (b *BFile) Write(Data []byte) (update bool, err error) {

	space := uint64(BufferSize - b.EOB)

	// Write to the current buffer
	dLen := len(Data)
	if dLen <= int(space) { //          If the current buffer has room, just
		copy(b.Buffer[b.EOB:], Data) // add to the buffer then return
		b.EOB += uint64(dLen)        // Well, after updating offsets...
		return false, nil
	}

	err = b.Open()
	if err != nil {
		return false, err
	}

	if space > 0 {
		n := copy(b.Buffer[b.EOB:], Data) // Copy what fits into the current buffer
		b.EOB += uint64(n)                // Update b.EOB (should be equal to BufferSize)
		Data = Data[n:]
	}

	// Question: Why is it necessary to seek?
	if eod, err := b.File.Seek(0, io.SeekEnd); err != nil {
		return false, err
	} else {
		b.EOD = uint64(eod)
	}

	if written, err := b.File.Write(b.Buffer[:b.EOB]); err != nil {
		return false, err
	} else {
		b.EOD += uint64(written)
	}

	b.EOB = 0          //         Start at the beginning of the buffer
	if len(Data) > 0 { //         If more data to write, recurse
		_, err = b.Write(Data) // Write out the remaining data
		return err == nil, err // Return false if we get an error, true if not
	}
	return true, nil //           Everything worked out
}

// WriteAt
// This is an unbuffered write; Does not involve the buffered writing
// Seek to the offset from start and write data ito the BFile
//
// To Do: modify to allow writing to locations that overlap the buffer
// (Not really required as yet, since it is used to update buffers...)
func (b *BFile) WriteAt(offset int64, data []byte) (err error) {
	// Question: This is only ever used by WriteHeader - is there a reason to
	// preserve this as a separate function?
	if err = b.Open(); err != nil {
		return err
	}

	if _, err = b.File.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	// Question: Can we remove the call to Stat by simply comparing the value
	// returned by Write to EOD?
	if _, err = b.File.Write(data); err != nil {
		return err
	}
	if fileInfo, err := b.File.Stat(); err != nil {
		return err
	} else {
		b.EOD = uint64(fileInfo.Size())
	}

	return nil
}

// ReadAt
// Seek to the offset from start and read into the given data buffer.  Note
// that to avoid a flush to disk, ReadAt must be smart about what is in the buffer vs
// what is on disk, and to open the file and read from it only if required.
func (b *BFile) ReadAt(offset uint64, data []byte) (err error) {
	DataLastOffset := uint64(offset) + uint64(len(data))

	switch {
	case DataLastOffset <= b.EOD:
		if err = b.Open(); err != nil {
			return err
		}
		// Question: Why not call File.ReadAt and avoid the call to Seek and
		// avoid clobbering the file offset?
		if _, err = b.File.Seek(int64(offset), io.SeekStart); err != nil {
			return err
		}
		if _, err = b.File.Read(data); err != nil {
			return err
		}
	case DataLastOffset > b.EOB+b.EOD:
		// Include `io.EOF` so `errors.Is(err, io.EOF)` works
		err = fmt.Errorf("%w: attempt to read past the EOF(%d) attempt(%d)", io.EOF, b.EOB+b.EOD, DataLastOffset)
	case offset > b.EOD:
		copy(data, b.Buffer[offset-b.EOD:])
	default:
		err = b.ReadAt(offset, data[:b.EOD-offset])
		copy(data[b.EOD-offset:], b.Buffer[:])
	}
	return err
}
