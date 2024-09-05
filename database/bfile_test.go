package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

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

func TestFP(t *testing.T) {
	path := "/tmp/xxx/yyy.dat"
	fmt.Println(filepath.Base(path))
}

func TestNewBFile(t *testing.T) {
	filename, deferF := MakeFilename("BFile.dat")
	defer deferF()

	bFile, err := NewBFile(filename)
	cnt := 1
	f := "%10d This is something we are writing\n"
	for i := 0; i < 100; i++ {
		update, err := bFile.Write([]byte(fmt.Sprintf(f, cnt)))
		assert.NoError(t, err, "write error")
		if err != nil {
			return
		}
		if update {
			cnt = 0

		}
		cnt++
	}
	bFile.Flush()
	dLen := len([]byte(fmt.Sprintf(f, 0)))
	data2 := make([]byte, dLen)
	for i := 1; i < 1; i++ {
		data := []byte(fmt.Sprintf(f, i))
		bFile.ReadAt(int64(i*dLen), data2[:])
		assert.Equal(t, data, data2[:], "didn't get my data back")
	}
	assert.NoError(t, err, "failed to create BFile")
	bFile.Close()
	fmt.Println("Done!")
}
