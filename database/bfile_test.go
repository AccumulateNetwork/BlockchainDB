package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

var MDMutex sync.Mutex
var FR = NewFastRandom(nil)

// MakeDir
// Create a Directory, return its name, and a function to remove that directory
func MakeDir() (directory string, deferF func()) {
	MDMutex.Lock()
	defer MDMutex.Unlock()
	name := filepath.Join(os.TempDir(), fmt.Sprintf("BlockDB-%06d", FR.UintN(1000000)))
	os.RemoveAll(name)
	os.Mkdir(name, os.ModePerm)
	return name, func() { os.RemoveAll(name) }
}

// MakeFilename
// Create a Directory and a File in that directory.  Returns the full path to the filename
// and a function to remove that Directory.
func MakeFilename(filename string) (Filename string, deferF func()) {
	directory, rm := MakeDir()
	Filename = filepath.Join(directory, filename)
	return Filename, rm
}

func TestNewBFile(t *testing.T) {
	filename, deferF := MakeFilename("BFile.dat")
	defer deferF()

	const numEntries = 50000
	const min = 10
	const max = 500

	ne := humanize.Comma(numEntries)
	fmt.Printf("Write %s records\n", ne)
	fr := NewFastRandom(nil)
	bFile, err := NewBFile(filename)
	assert.NoError(t, err, "failed to create BFile")
	cnt := 0
	dataCnt := 0
	for i := 0; i < numEntries; i++ {
		data := fr.RandChar(min, max)
		data[len(data)-1] = '\n'
		update, err := bFile.Write(data)
		assert.NoError(t, err, "write error")
		if err != nil {
			return
		}
		if update {
			cnt++
		}
		dataCnt += len(data)
	}
	ct := humanize.Comma(int64(cnt))
	dc := humanize.Comma(int64(dataCnt))
	fmt.Printf(""+
		"  Total updates: %s\n"+
		"Entries written: %s\n"+
		"    Total bytes: %s\n", ct, ne, dc)
	fmt.Printf("Check that reads of everything without close work\n")
	fr.Reset()
	var offset uint64 = 0
	assert.NoError(t, err, "failed to create BFile")
	var buff [max]byte
	for i := 0; i < numEntries; i++ {
		data := fr.RandChar(min, max)
		if i%17 == 0 {
			data[len(data)-1] = '\n'
			err := bFile.ReadAt(offset, buff[:len(data)])
			assert.NoError(t, err, "write error")
			assert.Equal(t, data, buff[:len(data)], "Didn't get the expected data")
			if err != nil || !bytes.Equal(data, buff[:len(data)]) {
				fmt.Printf("Failed at %d\n", i)
				return
			}
		}

		offset += uint64(len(data))
	}
	fmt.Printf("Check that reads of everything work after close\n")
	err = bFile.Close()
	assert.NoError(t, err, "failed to close")
	err = bFile.Open()
	assert.NoError(t, err, "failed to close")

	fr.Reset()
	offset = 0
	assert.NoError(t, err, "failed to create BFile")
	for i := 0; i < numEntries; i++ {
		data := fr.RandChar(min, max)
		if i%11 == 0 {
			data[len(data)-1] = '\n'
			err := bFile.ReadAt(offset, buff[:len(data)])
			assert.NoError(t, err, "write error")
			assert.Equal(t, data, buff[:len(data)], "Didn't get the expected data")
			if err != nil || !bytes.Equal(data, buff[:len(data)]) {
				fmt.Printf("Failed at %d\n", i)
				return
			}
		}

		offset += uint64(len(data))
	}
}
