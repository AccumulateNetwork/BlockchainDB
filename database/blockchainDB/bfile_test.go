package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// MakeDir
// Makes a temp directory, and returns a function to remove it.
// Use:
//
//	Directory,rm := MakeDir()
//	defer rm()
var FR = NewFastRandom(nil) // nil uses the computer to pick a seed
var MDMutex sync.Mutex

func MakeDir() (directory string, delDir func()) {
	MDMutex.Lock()
	defer MDMutex.Unlock()
	name := filepath.Join(os.TempDir(), fmt.Sprintf("BlockDB-%d", FR.UintN(1000000)))
	os.RemoveAll(name)
	os.Mkdir(name, os.ModePerm)
	return name, func() { os.RemoveAll(name) }
}

func NoErrorStop(t *testing.T, err error, msg string) {
	assert.NoError(t, err, msg)
	if err != nil {
		panic("error")
	}
}

func TestBFile(t *testing.T) {
	Directory, rm := MakeDir()
	defer rm()

	bFile, err := NewBFile(Directory, "BFile.dat")
	NoErrorStop(t, err, "failed to open BFile")

	r := NewFastRandom([]byte{1, 2, 3})
	for i := 0; i < 100_000; i++ {
		key := r.NextHash()
		value := r.RandBuff(100, 100)
		err = bFile.Put(key, value)
		NoErrorStop(t, err, "failed to put")
	}
	bFile.Close()
	bFile, err = OpenBFile(Directory, "BFile.dat")
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
	Directory, rm := MakeDir()
	defer rm()
	bFile, err := NewBFile(Directory, "BFile.dat")
	NoErrorStop(t, err, "failed to open BFile")

	r := NewFastRandom([]byte{1, 2, 3})

	keyValues := make(map[[32]byte][]byte)

	for i := 0; i < 4; i++ {
		for i := 0; i < 100; i++ {
			key := r.NextHash()
			value := r.RandBuff(100, 1000)
			keyValues[key] = value
			err = bFile.Put(key, value)
			assert.NoError(t, err, "failed to put")
		}
		assert.Equal(t, len(keyValues), len(bFile.Keys), "length of keys doesn't match Puts")
		for i := 0; i < 10; i++ {
			for k := range bFile.Keys {
				v, err := bFile.Get(k)
				assert.NoErrorf(t, err, "failed to get all on i=%d", i)
				assert.Equal(t, keyValues[k], v, "failed to get the value on i=%d", i)
				if err != nil || !bytes.Equal(keyValues[k], v) {
					return
				}
			}
		}
	}
	bFile.Close()
}

func TestCompress(t *testing.T) {
	Directory, rm := MakeDir()
	defer rm()
	bFile, err := NewBFile(Directory, "BFile.dat")
	NoErrorStop(t, err, "failed to open BFile")

	Compresses := 10
	TestSet := 100000

	fr := NewFastRandom([]byte{1, 2, 3})

	// Create a set of key value pairs, and put those in the bFile
	keyValues := make(map[[32]byte][]byte)

	for i := 0; i < TestSet; i++ {
		key := fr.NextHash()
		value := fr.RandBuff(100, 1000)
		keyValues[key] = value
		bFile.Put(key, value)
	}
	txs := 0
	start := time.Now()
	// Compress the BFile so many times
	for i := 1; i <= Compresses; i++ {
		if i%10 == 0 {
			fmt.Printf("%4d %6.2f tps -- ", i, float64(txs)/time.Since(start).Seconds())
			if i%100 == 0 {
				fmt.Println()
			}
			txs = 0
			start = time.Now()
		}
		// Compress the bFile
		bf, err := bFile.Compress()
		if err != nil {
			assert.NoError(t, err, "compress failed")
			return
		}
		bFile = bf

		// Check that what we think is in the bFile is in the bFile
		for k, v := range keyValues {
			value, err := bFile.Get(k)
			if err != nil || !bytes.Equal(v, value) {
				assert.NoErrorf(t, err, "value not found in db, compress=%d", i)
				assert.Equal(t, v, value, "Value is incorrect, compress=%d", i)
				return
			}
		}

		// Update some of the values in the bFile
		for key := range keyValues {
			if fr.UintN(100) < 20 {
				txs++
				value := fr.RandBuff(100, 100)
				err = bFile.Put(key, value)
				assert.NoError(t, err, "failed to put")
				keyValues[key] = value
			}
		}

	}
}
