package blockchainDB

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKFile(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKeys = 1_000_000

	fr := NewFastRandom([]byte{1})

	kf, err := NewKFile(true, dir, 1024)
	assert.NoError(t, err, "failed to create KFile")

	fmt.Printf("Adding Keys\n")

	numWrites := 0
	numReads := 0
	start := time.Now()
	var dbbKey DBBKey
	for i := 0; i < numKeys; i++ {
		k := fr.NextHash()
		dbbKey.Offset = uint64(i) * 100
		dbbKey.Length = 1000
		err = kf.Put(k, &dbbKey)
		numWrites++
		assert.NoError(t, err, "failed to put")
	}
	err = kf.Close()
	assert.NoError(t, err, "failed to close")
	err = kf.Open()
	assert.NoError(t, err, "failed to open")

	fr.Reset()

	writesPerSec := float64(numWrites) / time.Since(start).Seconds()
	start = time.Now()

	fmt.Printf("Check Keys\n")

	for i := 0; i < numKeys; i++ {
		cnt := numKeys                        // stupid cnt necessary, because if numKeys == 1
		if cnt > 100 && (i+1)%(cnt/10) == 0 { // this line still throws a div by 0 on numKeys/10
			fmt.Printf("Processing %d\n", i+1)
		}
		k := fr.NextHash()
		dbbKey := new(DBBKey)
		dbbKey.Offset = uint64(i) * 100
		dbbKey.Length = 1000
		dbk, err := kf.Get(k)
		assert.NoErrorf(t, err, "key %d", i)
		if err != nil {
			return
		}
		assert.Equalf(t, dbbKey.Bytes(k), dbk.Bytes(k), "dbbKey not same. key %d", i)
		if !bytes.Equal(dbbKey.Bytes(k), dbk.Bytes(k)) {
			return
		}
		numReads++
	}
	readsPerSec := float64(numReads) / time.Since(start).Seconds()

	fmt.Printf("writes %10.3f/s  Reads %10.3f/s\n", writesPerSec, readsPerSec)
	fmt.Printf("write time : %s\n", ComputeTimePerOp(writesPerSec))
	fmt.Printf("read time  : %s\n", ComputeTimePerOp(readsPerSec))
	assert.NoError(t, err, "failed to close KFile")
}
