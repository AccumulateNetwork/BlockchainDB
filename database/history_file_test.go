package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

func TestHistory(t *testing.T) {
	directory := "/tmp/History"
	os.RemoveAll(directory)

	const numKeys = 5_000_000
	const batchSize = 1_000_000

	fr := NewFastRandom([]byte{1, 2})
	frSave := fr.Clone()
	hf, err := NewHistoryFile(1000, directory)
	assert.NoError(t, err, "failed to create directory")

	// Create a random set of keys to values;
	// No we are not creating those values in this test of HistoryFile
	var keyList = make([]DBBKeyFull, batchSize)
	offset := 0x100000 // This is some offset in some file external to the HistoryFile
	start := time.Now()
	var last uint64
	var cnt, cntSave int
	// Create a DBBKeyFull value for every numKeys
	for i := 0; i < numKeys/batchSize; i++ {
		frSave = fr.Clone()
		cntSave = cnt
		for i := uint64(0); i < batchSize; i++ {
			keyList[i].Key = fr.NextHash()
			keyList[i].Length = uint64(0x11111111 * (cnt + 1))
			keyList[i].Offset = uint64(0x10101010 * (cnt + 1))
			offset += int(keyList[i].Length)
			last++
			cnt++
		}
		comma := humanize.Comma(int64(cnt))
		tps := float64(last) / time.Since(start).Seconds()
		fmt.Printf("%12s txs @ %12.2f tps %12s per write\n", comma, tps, ComputeTimePerOp(tps))
		last = 0
		start = time.Now()
		// All the DBBKeyFull entries need to be sorted by their index in the HistoryFile
		sort.Slice(keyList, func(i, j int) bool {
			return hf.Index(keyList[i].Key) < hf.Index(keyList[j].Key)
		})

		// Now pack all those DBBKeyFull values into a buffer, like they would be
		// in a kFile.
		buff := make([]byte, DBKeyFullSize*batchSize)
		offset = 0
		for _, DBFull := range keyList {
			copy(buff[offset:], DBFull.DBBKey.Bytes(DBFull.Key))
			offset += DBKeyFullSize
		}

		// Add that list of keys to the HistoryFile

		err = hf.AddKeys(buff)
		assert.NoError(t, err, "AddKeys failed")

	}
	fmt.Println("Build DB done")
	fmt.Println("Last added")
	start = time.Now()
	var dbFull DBBKeyFull
	for i := uint64(0); i < batchSize/10; i++ {
		k := frSave.NextHash()
		dbFull.Key = k
		dbFull.Length = uint64(0x11111111 * (cntSave + 1))
		dbFull.Offset = uint64(0x10101010 * (cntSave + 1))
		cntSave++
		v2, err := hf.Get(dbFull.Key)
		assert.NoErrorf(t, err, "failed to get %d %x", i, k[:4])
		if err != nil {
			return
		}
		assert.Equalf(t, dbFull.Bytes(k), v2.Bytes(k), "value does not match %d %x", i, k[:4])
		if !bytes.Equal(dbFull.Bytes(k), v2.Bytes(k)) {
			return
		}
	}
	fmt.Println("First added")
	tps := float64(batchSize/10) / time.Since(start).Seconds()
	comma := humanize.Comma(batchSize / 10)
	fmt.Printf("Done!\n%s txs %10.2f tps\n", comma, tps)
	fmt.Printf("Per Read %s\n", ComputeTimePerOp(tps))

	start = time.Now()
	fr.Reset()
	cnt = 0
	for i := uint64(0); i < batchSize/10; i++ {
		k := fr.NextHash()
		dbFull.Key = k
		dbFull.Length = uint64(0x11111111 * (cnt + 1))
		dbFull.Offset = uint64(0x10101010 * (cnt + 1))
		cnt++
		v2, err := hf.Get(dbFull.Key)
		assert.NoErrorf(t, err, "failed to get %d %x", i, k[:4])
		if err != nil {
			return
		}
		assert.Equalf(t, dbFull.Bytes(k), v2.Bytes(k), "value does not match %d %x", cnt, k[:4])
		if !bytes.Equal(dbFull.Bytes(k), v2.Bytes(k)) {
			return
		}
	}
	println("First added")
	tps = float64(batchSize) / time.Since(start).Seconds()
	comma = humanize.Comma(batchSize)
	fmt.Printf("%s txs %10.2f tps\n", comma, tps)
	fmt.Printf("Per Read %s\n", ComputeTimePerOp(tps))
}
