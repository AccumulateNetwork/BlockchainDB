package blockchainDB

import (
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

	kf, err := NewKFile(dir)
	assert.NoError(t, err, "failed to create KFile")

	fmt.Printf("Adding Keys\n")

	numWrites := 0
	numReads := 0
	start := time.Now()

	for i := 0; i < numKeys; i++ {
		k := fr.NextHash()
		dbbKey := new(DBBKey)
		dbbKey.Height = 0
		dbbKey.Offset = uint64(i) * 100
		dbbKey.Length = 1000
		err = kf.Put(k, dbbKey)
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
		if (i+1)%(numKeys/10) == 0 {
			fmt.Printf("Processing %d\n", i+1)
		}
		k := fr.NextHash()
		dbbKey := new(DBBKey)
		dbbKey.Height = 0
		dbbKey.Offset = uint64(i) * 100
		dbbKey.Length = 1000
		if dbk, err := kf.Get(k); err == nil {
			d1b := dbbKey.Bytes(k)
			d2b := dbk.Bytes(k)
			assert.Equal(t, d1b, d2b, "didn't get the dbbKey back")
		} else {
			assert.NoErrorf(t, err, "failed to Get %x %d", k[:4], i)
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
