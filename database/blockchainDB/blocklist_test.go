package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Create and close a BlockFile
func TestCreateBlockFile(t *testing.T) {
	Directory, rm := MakeDir()
	defer rm()

	bf, err := NewBlockList(Directory, 1)
	assert.NoError(t, err, "error creating BlockList")
	assert.NotNil(t, bf, "failed to create BlockList")
	err = bf.Close()
	assert.NoError(t, err, "close failed")
	os.RemoveAll(Directory)
}

func TestOpenBlockFile(t *testing.T) {
	Directory := filepath.Join(os.TempDir(), "BList")
	os.RemoveAll(Directory)
	os.Mkdir(Directory, os.ModePerm)

	bf, err := NewBlockList(Directory, 1)
	assert.NoError(t, err, "failed to create a BlockList")
	assert.NotNil(t, bf, "failed to create BlockList")
	err = bf.Close()
	assert.NoError(t, err, "close failed")

	bf, err = OpenBlockList(Directory)
	assert.NoError(t, err, "failed to create a BlockList")
	assert.NotNil(t, bf, "failed to create BlockList")
	err = bf.Close()
	assert.NoError(t, err, "close failed")

	os.RemoveAll(Directory)
}

func TestBlockFileLoad(t *testing.T) {
	const (
		NumberOfBlocks    = 10
		NumberOfKeyValues = 10_000
	)

	fmt.Println("Testing open and close of a BlockList")
	Directory := filepath.Join(os.TempDir(), "BTest")
	BListDir := filepath.Join(Directory, "BList")
	os.RemoveAll(Directory)
	os.Mkdir(Directory, os.ModePerm)
	os.Mkdir(BListDir, os.ModePerm)
	defer os.RemoveAll(Directory)

	bf, err := NewBlockList(BListDir, 1)
	if err != nil {
		assert.NoError(t, err, "Failed to create BlockList")
		return
	}

	err = bf.Close()
	assert.NoError(t, err, "close failed")

	fmt.Println("Writing BlockFiles: ")

	bf, err = OpenBlockList(BListDir)
	if err != nil {
		assert.NoError(t, err, "failed to create a BlockFile")
		return
	}

	fr := NewFastRandom([]byte{1, 2, 3})
	for i := 0; i < NumberOfBlocks; i++ { // Create so many Blocks
		fmt.Printf("Writing to BlockFile %d\n", bf.BlockHeight)
		for i := 0; i < NumberOfKeyValues; i++ { // Write so many key value pairs
			key := fr.NextHash()
			value := fr.RandBuff(100, 300)
			bf.Put(key, value)
		}
		err = bf.NextBlockFile()
		assert.NoError(t, err, "NextBlockFile failed")
	}

	fmt.Printf("\nReading BlockFiles: ")

	err = bf.Close()
	assert.NoError(t, err, "close failed")
	if true {
		return
	}
	bf, err = OpenBlockList(Directory)
	if err != nil {
		assert.NoError(t, err, "failed to open a BlockList")
		return
	}
	start := time.Now()
	fr = NewFastRandom([]byte{1, 2, 3})
	for i := 0; i <= NumberOfBlocks; i++ {
		fmt.Printf("%3d ", i)
		_, err = bf.OpenBFile(i)
		fmt.Printf("Reading from BlockFile %d\n", i)
		if err != nil {
			assert.NoError(t, err, fmt.Sprintf("failed to open block file %d", i))
			return
		}
		for j := 0; j < NumberOfKeyValues; j++ {
			hash := fr.NextHash()
			value := fr.RandBuff(100, 300)
			v, err := bf.Get(hash)
			if err != nil || !bytes.Equal(value, v) {
				assert.NoError(t, err, "failed to get value for key")
				assert.Equalf(t, value, v, "blk %d pair %d value was not the value expected", j, j)
				return
			}
			if !bytes.Equal(value, v) {
				panic("error")
			}
		}
	}
	fmt.Printf("\nDone: %v\n", time.Since(start))
}
