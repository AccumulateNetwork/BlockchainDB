package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Create and close a BlockFile
func TestCreateBlockFile(t *testing.T) {
	Directory := filepath.Join(os.TempDir(), "BFTest")
	os.RemoveAll(Directory)

	bf, err := NewBlockList(Directory, 1, 5)
	assert.NoError(t, err, "error creating BlockList")
	assert.NotNil(t, bf, "failed to create BlockList")
	bf.Close()

	os.RemoveAll(Directory)
}

func TestOpenBlockFile(t *testing.T) {
	Directory := filepath.Join(os.TempDir(), "BFTest")
	os.RemoveAll(Directory)

	bf, err := NewBlockList(Directory, 1, 5)
	assert.NoError(t, err, "failed to create a BlockList")
	bf.Close()

	os.RemoveAll(Directory)
}

func StopOnError (t *testing.T, err error, msg string){
	assert.NoError(t,err,msg)
	if err != nil {panic("error")}
}

func TestBlockFileLoad(t *testing.T) {
	fmt.Println("Testing open and close of a BlockList")
	Directory := filepath.Join(os.TempDir(), "BFTest")
	os.RemoveAll(Directory)
	bf, err := NewBlockList(Directory, 1, 5)
	StopOnError(t, err, "Failed to create BlockList")
	bf.Close()

	fmt.Println("Writing BlockFiles: ")

	bf, err = NewBlockList(Directory, 1,3)
	StopOnError(t, err, "failed to create a BlockFile")

	fr := NewFastRandom([]byte{1, 2, 3})
	for i := 0; i < 2; i++ { // Create so many Blocks
		bf.NextBlockFile()
		fmt.Printf("Writing to BlockFile %d\n",bf.BlockHeight)
		for i := 0; i < 10; i++ { // Write so many key value pairs
			bf.Put(fr.NextHash(), fr.RandBuff(100, 300))
		}
	}

	fmt.Printf("\nReading BlockFiles: ")

	bf.Close()
	bf, err = OpenBlockList(Directory, 3)
	StopOnError(t, err, "failed to open a BlockList")
	fr = NewFastRandom([]byte{1, 2, 3})
	for i := 1; i <= 2; i++ {
		fmt.Printf("%3d ", i)
		_, err = bf.OpenBFile(i, 5)
		fmt.Printf("Reading from BlockFile %d\n",i)
		StopOnError(t, err, fmt.Sprintf("failed to open block file %d", i))
		for j := 0; j < 10; j++ {
			hash := fr.NextHash()
			value := fr.RandBuff(100, 300)
			v, err := bf.Get(hash)
			StopOnError(t, err, "failed to get value for key")
			assert.Equalf(t, value, v, "blk %d pair %d value was not the value expected", j, j)
			if !bytes.Equal(value,v) {
				panic("error")
			}
		}
	}
	fmt.Print("\nDone\n")
}
