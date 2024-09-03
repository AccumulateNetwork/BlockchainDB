package blockchainDB

import (
	"bytes"
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

// MakeDir
// Create a Directory, return its name, and a function to remove that directory
func MakeDir() (directory string, delDir func()) {
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
func MakeFilename(filename string) (Filename string, delDir func()) {
	directory, rm := MakeDir()
	Filename = filepath.Join(directory, filename)
	return Filename, rm
}

func NoErrorStop(t *testing.T, err error, msg string) {
	assert.NoError(t, err, msg)
	if err != nil {
		panic("error")
	}
}

func TestNew(t *testing.T) {
	Filename, rm := MakeFilename("BFile.dat")
	defer rm()

	var numEntries = 100000

	fr := NewFastRandom([]byte{1})

	bFile, err := NewBFile(Filename)
	assert.NoError(t, err, "Failed to create BFile.dat")

	for i := 0; i < numEntries; i++ {
		k, v := fr.NextHash(), fr.RandBuff(10, 10)
		bFile.Put(k, v)
		v2, _ := bFile.Get(k)
		if !bytes.Equal(v, v2) {
			fmt.Printf("Failed at %d\n", i)
			return
		}
		assert.Equal(t, v, v2, "Not the same")

	}

	bFile.Close()
	hs1 := bFile.Header.Marshal() // Get a string for the header
	bFile.Open(false)
	bFile.Close()
	hs2 := bFile.Header.Marshal() // Get a string for the header after opening and closing
	assert.Equal(t, hs1, hs2, "Headers should be the same")

	fr.Reset()
	for i := 0; i < numEntries; i++ {
		k, v := fr.NextHash(), fr.RandBuff(10, 10)
		v2, err := bFile.Get(k)
		assert.NoError(t, err, "couldn't get the value back")
		if !bytes.Equal(v, v2) {
			fmt.Printf("Failed at %d\n", i)
			return
		}
		assert.Equal(t, v, v2, "didn't get the value we expected")
	}

}

func TestBFile(t *testing.T) {
	Filename, rm := MakeFilename("BFile.dat")
	defer rm()

	bFile, err := NewBFile(Filename)
	assert.NoError(t, err, "failed to create/open BFile")
	const NumEntries = 1000
	const EntrySize = 100
	Seed := []byte{2, 2, 2, 2, 5}

	r := NewFastRandom(Seed)

	fmt.Printf("Write %d entries of %d bytes\n", NumEntries, EntrySize)
	for i := 0; i < NumEntries; i++ {
		key, value := r.NextHash(), r.RandBuff(EntrySize, EntrySize)
		err = bFile.Put(key, value)
		if err != nil {
			fmt.Printf("Put error: %d %v\n", i, err)
			return
		}

		bFile.Close()

		v, err := bFile.Get(key)
		if err != nil {
			fmt.Printf("error: %d %v\n", i, err)
			return
		}
		assert.NoError(t, err, "failed to put")

		if !bytes.Equal(value, v) {
			fmt.Printf("Not equal %x != %x\n", value, v)
		}
		assert.Equal(t, value, v, "Not the same value")
	}
	err = bFile.Close()
	assert.NoError(t, err, "failed to close")

	fmt.Printf("Read all the %d entries and check them.\n", NumEntries)

	r.Reset() // Reset the random number sequence
	for i := 0; i < NumEntries; i++ {
		key, value := r.NextHash(), r.RandBuff(EntrySize, EntrySize)
		v, err := bFile.Get(key)
		assert.NoErrorf(t, err, "%d failed to get value", i)
		assert.Equalf(t, value, v, "%d failed to get the right value", i)
		if err != nil || !bytes.Equal(value,v) {
			e := ""
			if err != nil {
				e = err.Error()
			}
			fmt.Printf("       i %d\n", i)
			fmt.Printf("     err %v\n", e)
			fmt.Printf("Expected %x\n", value)
			fmt.Printf("     Got %x\n", v)
			return
		}
	}
}

func TestBFile2(t *testing.T) {
	const loops = 10
	const kvNum = 100

	Filename, rm := MakeFilename("BFile.dat")
	defer rm()

	bFile, err := NewBFile(Filename)
	assert.NoError(t, err, "failed to create/open BFile")

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
	Filename, rm := MakeFilename("BFile.dat")
	defer rm()

	bFile, err := NewBFile(Filename)
	assert.NoError(t, err, "failed to create/open BFile")

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
	bf.Close()
	err := bf.Open(true)
	assert.NoError(t, err, "Failed to open BFile")
	fmt.Printf("                   Cache %8d\n", len(bf.Cache))
	fmt.Printf("              KeysOffset %8x\n", bf.Header.Offsets[0])
	fmt.Printf("                    Keys %8x\n", len(bf.Keys))
	fmt.Printf("                 NewKeys %8x\n", len(bf.NewKeys))
	fmt.Printf("                     EOB %8x\n", bf.EOB)
	fmt.Printf("              BFile Path %s\n", bf.Filename)

	_, err = bf.File.Seek(0, io.SeekStart)
	assert.NoError(t, err, "could not seek")
	var offsetB [HeaderSize]byte
	bf.File.Read(offsetB[:])
	fmt.Printf("\nDump:\n")
	fmt.Printf("             EndOfValues %08x\n", offsetB[:8])
	for i := 8; i+64 < HeaderSize; i += 8 * 8 {
		fmt.Printf("%08x %08x %08x %08x  %08x %08x %08x %08x \n",
			offsetB[i+0:i+8], offsetB[i+8:i+16], offsetB[i+16:i+24], offsetB[i+24:i+32],
			offsetB[i+32:i+40], offsetB[i+40:i+48], offsetB[i+48:i+56], offsetB[i+56:i+64])
	}

	if true {
		return
	}

	offset := 0
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
