package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var Directory = filepath.Join(os.TempDir(), "BlockDB")
var Partition = 1

func NoErrorStop(t *testing.T, err error, msg string) {
	assert.NoError(t, err, msg)
	if err != nil {
		panic("error")
	}
}

func TestBFile(t *testing.T) {
	os.Mkdir(Directory, os.ModePerm)
	defer os.RemoveAll(Directory)
	filename := filepath.Join(Directory, "BFile.dat")
	bFile, err := NewBFile(filename, 3)
	NoErrorStop(t, err, "failed to open BFile")

	r := NewFastRandom([]byte{1, 2, 3})
	for i := 0; i < 100_000; i++ {
		key := r.NextHash()
		value := r.RandBuff(100, 100)
		err = bFile.Put(key, value)
		NoErrorStop(t, err, "failed to put")
	}
	bFile.Close()
	bFile.Block()
	bFile, err = OpenBFile(filename, 3)
	NoErrorStop(t, err, "open failed")
	r = NewFastRandom([]byte{1, 2, 3})
	for i := 0; i < 100_000; i++ {
		key := r.NextHash()
		value := r.RandBuff(100, 100)
		v, err := bFile.Get(key)
		NoErrorStop(t, err, "failed to get value")
		assert.Equal(t, value, v, "failed to get the right value")
		if !bytes.Equal(value, v) {
			panic("value not v")
		}
	}
}

func TestBFile2(t *testing.T) {
	os.Mkdir(Directory, os.ModePerm)
	defer os.RemoveAll(Directory)
	filename := filepath.Join(Directory, "BFile.dat")
	bFile, err := NewBFile(filename, 3)
	NoErrorStop(t, err, "failed to open BFile")

	r := NewFastRandom([]byte{1, 2, 3})
	
	for i := 0; i < 1000; i++ {
		key := r.NextHash()
		value := r.RandBuff(100, 1000)
		err = bFile.Put(key, value)
		assert.NoError(t, err, "failed to put")
	}
	for i := 0; i < 1000; i++ {
		kn := 0
		for k := range bFile.Keys {
			kn++
			_, err := bFile.Get(k)
			assert.NoErrorf(t, err, "failed to get all on i=%d kn=%d", i, kn)
		}
	}
	bFile.Close()
}

func TestCompress(t *testing.T) {
	os.Mkdir(Directory, os.ModePerm)
	defer os.RemoveAll(Directory)
	filename := filepath.Join(Directory, "BFile.dat")
	bFile, err := NewBFile(filename, 3)
	NoErrorStop(t, err, "failed to open BFile")
	Compresses := 1000
	TestSet := 1000

	for i := 0; i < Compresses; i++ {
		r := NewFastRandom([]byte{1, 2, 3})
		bFile, err = bFile.Compress()
		if err != nil {
			fmt.Println(i)
		}
		assert.NoError(t, err, "Failed to compress")
		bFile, err = bFile.Compress()
		if err != nil {
			fmt.Println(i, "b")
		}
		assert.NoError(t, err, "Failed to compress")
		for j := 0; j < TestSet; j++ {
			key := r.NextHash()
			value := r.RandBuff(100, 100)
			err = bFile.Put(key, value)
			assert.NoError(t, err, "failed to put")
			v, err := bFile.Get(key)
			assert.NoError(t, err, "failed to get")
			assert.Equal(t, value, v, "failed to get value")
		}
	}
	bFile.Compress()
	bFile.Close()
}
