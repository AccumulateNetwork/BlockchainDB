package blockchainDB

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

	var Directory = filepath.Join(os.TempDir(),"BlockDB")
	var Partition = 1

func NoErrorStop(t *testing.T,err error,msg string){
	assert.NoError(t,err,msg)
	if err != nil {
		panic("error")
	}
}

func TestBFile(t *testing.T) {
	os.Mkdir(Directory,os.ModePerm)
	defer os.RemoveAll(Directory)
	filename := filepath.Join(Directory,"BFile.dat")
	bFile, err := NewBFile(filename,3)
	NoErrorStop(t,err,"failed to open BFile")
	 
	r := NewFastRandom([32]byte{1,2,3})
	for i:= 0;i<100_000;i++ {
		key := r.NextHash()
		value := r.RandBuff(100,100)
		err = bFile.Put(key,value)
		NoErrorStop(t,err,"failed to put")
	}
	bFile.Close()
	bFile.Block()
	bFile, err = OpenBFile(filename,3)
	NoErrorStop(t,err,"open failed")
	r = NewFastRandom([32]byte{1,2,3})
	for i:= 0;i<100_000;i++ {
		key := r.NextHash()
		value := r.RandBuff(100,100)
		v,err := bFile.Get(key)
		NoErrorStop(t,err,"failed to get value")
		assert.Equal(t,value,v,"failed to get the right value")
		if !bytes.Equal(value,v){
			panic("value not v")
		}
	}
}