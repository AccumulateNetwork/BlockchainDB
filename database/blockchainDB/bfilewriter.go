package blockchainDB

import (
	"encoding/binary"
	"io"
)

const (
	opWrite = iota
	opClose
)

type cmd struct {
	op     int
	buffer *BFBuffer
	EOB    int
	EOD    uint64
}

// BFileWriter
// Takes buffers in order and writes them to a file, then closes the file.
// It uses a channel to keep the buffers in order, and to finish writing to the
// file before it is closed.
type BFileWriter struct {
	OSFile       *OSFile
	BuffPool   chan *BFBuffer
	Operations chan cmd
}

// process
// orders writes to the BFile, and the closing of a BFile.  Once Closed, the
// BFileWriter closes its process.
func (b *BFileWriter) process() {
	for {
		c := <-b.Operations
		switch c.op {
		case opWrite:
			b.OSFile.Lock()
			b.OSFile.Write(c.buffer.Buffer[:c.EOB])
			b.OSFile.Unlock()
			c.buffer.PurgeCache()
			b.BuffPool <- c.buffer
		case opClose:
			b.OSFile.Lock()
			defer b.OSFile.Unlock()

			b.OSFile.Write(c.buffer.Buffer[:c.EOB])
			if _, err := b.OSFile.Seek(0, io.SeekStart); err != nil { // Seek to start
				panic(err)
			}
			var buff [8]byte                                   // Write out the offset to the keys into
			binary.BigEndian.PutUint64(buff[:], uint64(c.EOD)) // the DBBlock file.
			if _, err := b.OSFile.Write(buff[:]); err != nil {   //
				panic(err)
			}
			b.OSFile.Close()
			b.BuffPool <- c.buffer
			return
		}
	}
}

// NewBFileWriter
// Create a new BFileWriter.  As long as the BFile is open, the BFileWriter will
// process commands.  Close the BFileWriter to close the BFile
func NewBFileWriter(file *OSFile, buffPool chan *BFBuffer) *BFileWriter {
	bfWriter := new(BFileWriter)
	bfWriter.OSFile = file
	bfWriter.BuffPool = buffPool
	bfWriter.Operations = make(chan cmd, 10)
	go bfWriter.process()
	return bfWriter
}

// Write
// Write a buffer or part of a buffer to the BFile.
func (b *BFileWriter) Write(buffer *BFBuffer, EOB int) {
	b.Operations <- cmd{opWrite, buffer, EOB, 0}
}

// Close
// Writes whatever is in the buffer, updates the offset to EOD, and Closes the BFile
// Note that if what is in the buffer needs to be reflected in the EOD, the caller has
// to provide the updated EOD.
func (b *BFileWriter) Close(buffer *BFBuffer, EOB int, EOD uint64) {
	if b.OSFile != nil {
		b.Operations <- cmd{opClose, buffer, EOB, EOD}
	}
}
