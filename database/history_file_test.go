package blockchainDB

import (
	"bytes"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHistory(t *testing.T) {
	directory := "/tmp/History"
	os.RemoveAll(directory)

	const numKeys = 10000

	fr := NewFastRandom([]byte{1, 2})
	hf, err := NewHistoryFile(8, directory)
	assert.NoError(t, err, "failed to create directory")

	// Create a random set of keys to values;
	// No we are not creating those values in this test of HistoryFile
	var keyList = make([]DBBKeyFull, numKeys)
	var keyMap = make(map[[32]byte]DBBKey)

	offset := 0x100000 // This is some offset in some file external to the HistoryFile

	// Create a DBBKeyFull value for every numKeys
	for i := 0; i < numKeys; i++ {
		keyList[i].Key = fr.NextHash()
		keyList[i].Length = uint64(0x11111111 * (i + 1))
		keyList[i].Offset = uint64(0x10101010 * (i + 1))
		offset += int(keyList[i].Length)
		keyMap[keyList[i].Key] = keyList[i].DBBKey
	}

	// All the DBBKeyFull entries need to be sorted by their index in the HistoryFile
	sort.Slice(keyList, func(i, j int) bool {
		return hf.Index(keyList[i].Key) < hf.Index(keyList[j].Key)
	})

	// Now pack all those DBBKeyFull values into a buffer, like they would be
	// in a kFile.
	buff := make([]byte, DBKeyFullSize*numKeys)
	offset = 0
	for _, DBFull := range keyList {
		copy(buff[offset:], DBFull.DBBKey.Bytes(DBFull.Key))
		offset += DBKeyFullSize
	}

	// Add that list of keys to the HistoryFile

	err = hf.AddKeys(buff)
	assert.NoError(t, err, "AddKeys failed")

	for k, v := range keyMap {
		v2, err := hf.Get(k)
		assert.NoErrorf(t, err, "failed to get %x", k[:4])
		if err != nil {
			return
		}
		assert.Equalf(t, v.Bytes(k), v2.Bytes(k), "value does not match %x", k[:4])
		if !bytes.Equal(v.Bytes(k), v2.Bytes(k)) {
			return
		}
	}
}
