package blockchainDB

import (
	"fmt"
	"path/filepath"
	"testing"
)

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

func TestFP(t *testing.T){
	path := "/tmp/xxx/yyy.dat"
	fmt.Println(filepath.Base(path))
}

func TestNewBFile(t *testing.T){
     filename, MakeFilename("BFile.dat")
defer deferF()

bFile, err := NewBFile(filename string)

}