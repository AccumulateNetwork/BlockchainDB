package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	name := filepath.Join(os.TempDir(), fmt.Sprintf("BlockDB-%06d", FR.UintN(1000000)))
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

	const NumEntries = 10
	const EntrySize = 10
	Seed := []byte{1, 2, 3, 4}
	r := NewFastRandom(Seed)

	nextKeyValue := func() (key [32]byte, value []byte) {
		key, value = r.NextHash(), r.RandBuff(EntrySize, EntrySize)
		fmt.Printf("key %32x value %10x \n", key, value[:10])
		return key, value
	}

	bFile, err := NewBFile(Directory, "BFile.dat")
	assert.NoError(t, err, "failed to open BFile")

	for i := 0; i < NumEntries; i++ {
		key, value := nextKeyValue()
		err = bFile.Put(key, value)
		assert.NoError(t, err, "failed to put")
	}

	Dump(t, bFile)

	err = bFile.Close()
	assert.NoError(t, err, "failed to close")

	Dump(t, bFile)

	r.Reset() // Reset the random number sequence
	bFile, err = OpenBFile(Directory, "BFile.dat")
	assert.NoError(t, err, "open failed")

	fmt.Println("\nloaded keys")
	for k, v := range bFile.Keys {
		fmt.Printf(" > %032x %02d %08d %08d \n", k, v.Height, v.Offset, v.Length)
	}

	for i := 0; i < NumEntries; i++ {
		key, value := nextKeyValue()
		v, err := bFile.Get(key)

		assert.NoError(t, err, "failed to get value")
		assert.Equal(t, value, v, "failed to get the right value")
		if err != nil || !bytes.Equal(v, value) {
			return
		}
	}
}

func TestBFile2(t *testing.T) {
	const loops = 10
	const kvNum = 100

	Directory, rm := MakeDir()
	defer rm()
	bFile, err := NewBFile(Directory, "BFile.dat")
	NoErrorStop(t, err, "failed to open BFile")

	r := NewFastRandom([]byte{1, 2, 3})

	keyValues := make(map[[32]byte][]byte)

	for i := 0; i < loops; i++ {
		for i := 0; i < kvNum; i++ {
			key := r.NextHash()
			value := r.RandBuff(100, 1000)
			keyValues[key] = value
			err = bFile.Put(key, value)
			assert.NoError(t, err, "failed to put")
		}
		assert.Equal(t, 100, len(bFile.NewKeys), "length of keys doesn't match Puts")

		err = bFile.Close()
		assert.NoError(t, err, "failed to close")

		for i := 0; i < kvNum; i++ {
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
	err = bFile.Close()
	assert.NoError(t, err, "failed to close")
}

func TestCompress(t *testing.T) {
	Directory, rm := MakeDir()
	defer rm()
	bFile, err := NewBFile(Directory, "BFile.dat")
	NoErrorStop(t, err, "failed to open BFile")

	Compresses := 10
	TestSet := 10_000
	Repeats := 100

	frKeys := NewFastRandom([]byte{1, 2, 3})
	frValues := NewFastRandom([]byte{4, 5, 6})

	keyValues := make(map[[32]byte][]byte)

	fmt.Println("Init")
	// Create a set of key value pairs, and put those in the bFile
	for i := 0; i < TestSet; i++ {
		key := frKeys.NextHash()
		value := frValues.RandBuff(100, 1000)
		bFile.Put(key, value)
		keyValues[key] = value
	}
	txs := 0
	start := time.Now()

	// Compress the BFile so many times
	for i := 1; i <= Compresses; i++ {
		// Compress the bFile
		fmt.Println("Compress")
		err := bFile.Compress()
		if err != nil {
			assert.NoError(t, err, "compress failed")
			return
		}

		fmt.Println("CheckDB")

		// Check that what we think is in the bFile is in the bFile
		for k, v := range keyValues {
			txs++
			value, err := bFile.Get(k)
			if err != nil || !bytes.Equal(v, value) {
				assert.NoErrorf(t, err, "value not found in db, compress=%d", i)
				assert.Equal(t, v, value, "Value is incorrect, compress=%d", i)
				return
			}
		}

		fmt.Println("Write a bunch of updates")

		// Update some of the values in the bFile
		for i := 0; i < Repeats; i++ {
			for key := range keyValues {
				if frValues.UintN(100) < 5 {
					txs++
					value := frValues.RandBuff(100, 1000)
					err = bFile.Put(key, value)
					assert.NoError(t, err, "failed to put")
					keyValues[key] = value
				}
			}
		}
		fmt.Printf("%4d %6.2f tps\n",
			i,
			float64(txs)/time.Since(start).Seconds(),
		)
	}
}

func Dump(t *testing.T, bf *BFile) {
	filename := filepath.Join(bf.Directory, bf.Filename)
	fmt.Printf("                    Path %s\n", filename)
	if bf.File == nil {
		fmt.Printf("           BFile.File not open\n")
	} else {
		fmt.Printf("               BFile.File open\n")
	}
	fmt.Printf("                   Cache %8d\n", len(bf.Cache))
	fmt.Printf("            OffsetToKeys %8x\n", bf.OffsetToKeys)
	fmt.Printf("                    Keys %8x\n", len(bf.Keys))
	fmt.Printf("                 NewKeys %8x\n", len(bf.NewKeys))
	fmt.Printf("                     EOB %8x\n", bf.EOB)
	fmt.Printf("              BFile Path %s\n", filepath.Join(bf.Directory, bf.Filename))

	err := bf.Open()
	assert.NoError(t, err, "could not open BFile")
	_, err = bf.File.Seek(0, io.SeekStart)
	assert.NoError(t, err, "could not seek")
	var offsetB [8]byte
	bf.File.Read(offsetB[:])
	offset := binary.BigEndian.Uint64(offsetB[:])
	fmt.Printf("\nDump:\n")
	fmt.Printf("                  Offset %x %x\n", offsetB, offset)
	var valuesB [1]byte

	_, err = bf.File.Seek(0, io.SeekStart)
	assert.NoError(t, err, "could not seek")
	for i := 0; i < int(offset) && 1 < 512; i++ {
		bf.File.Read(valuesB[:])
		switch {
		case i%64 == 0:
			fmt.Printf("\n%06x ", i)
		case i%16 == 0:
			fmt.Print(" ")
		}
		fmt.Printf("%2x", valuesB)
	}
	fmt.Printf("\n")

	_, err = bf.File.Seek(int64(offset), io.SeekStart)
	assert.NoError(t, err, "seek failed")
	keys, err := io.ReadAll(bf.File)
	assert.NoError(t, err, "read keys failed")
	for ; len(keys) > 0; keys = keys[DBKeySize:] {
		fmt.Printf(" key %032x height %04x offset %016x length %016x\n",
			keys[:32], keys[32:34], keys[34:42], keys[42:50])
	}
}
