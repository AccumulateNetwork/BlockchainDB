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

func TestKVShard(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKVs = 1_000_000

	start := time.Now()
	var cntWrites, cntReads float64

	fr := NewFastRandom([]byte{1})
	kvs, err := NewKVShard(dir)
	assert.NoError(t, err, "create kv")

	fmt.Print("Writing\n")

	for i := 0; i < numKVs; i++ {
		key := fr.NextHash()
		value := fr.RandBuff(100, 200)

		err = kvs.Put(key, value)
		assert.NoError(t, err, "Failed to put")

		cntWrites++
	}

	wps := cntWrites / time.Since(start).Seconds()
	start = time.Now()

	fmt.Print("Reading\n")

	fr.Reset()
	for i := 0; i < numKVs; i++ {
		key := fr.NextHash()
		value := fr.RandBuff(100, 200)

		value2, err := kvs.Get(key)
		assert.NoError(t, err, "Failed to put")
		assert.Equal(t, value, value2, "Didn't the the value back")
		if !bytes.Equal(value, value2) || err != nil {
			fmt.Printf("which failed %d\n", i)
			return
		}

		cntReads++
	}
	err = kvs.Close()
	assert.NoError(t, err, "failed to close KVFile")

	rps := cntReads / time.Since(start).Seconds()

	fmt.Printf("Writes per second %10.3f Reads per second %10.3f\n", wps, rps)
	fmt.Printf("Write -- %s\n", ComputeTimePerOp(wps))
	fmt.Printf("Read  -- %s\n", ComputeTimePerOp(rps))
}

func TestKVShard_2(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "BigDB")

	//dir, rm := MakeDir()
	//defer rm()

	const numPermKeys = 10_000_000
	const numModKeys = 100_000

	keyValues := map[[32]byte][]byte{}
	var keyList [numPermKeys][32]byte
	modKeyValues := map[[32]byte][]byte{}
	var modKeyList [numModKeys][32]byte

	start := time.Now()
	var cntWrites, cntReads float64

	kvs, err := NewKVShard(dir)
	assert.NoError(t, err, "create kv")

	fmt.Print("Generating Keys\n")

	fr := NewFastRandom([]byte{1})
	for i := 0; i < numModKeys; i++ {
		key := fr.NextHash()
		value := fr.RandChar(100, 200)
		modKeyValues[key] = value
		modKeyList[i] = key
	}

	fr = NewFastRandom([]byte{2})
	for i := 0; i < numPermKeys; i++ {
		key := fr.NextHash()
		value := fr.RandChar(100, 200)
		keyValues[key] = value
		keyList[i] = key
	}

	fmt.Print("Writing Perm and Dyna Keys\n")

	fr = NewFastRandom([]byte{3})
	// Put some keys into the file
	for _, key := range keyList {
		value := keyValues[key]
		kvs.Put(key, value)
		cntWrites++
	}

	cnt := 0
	for _, key := range modKeyList {
		// Overwrite Some keys
		if fr.UintN(100) < 30 {
			value := fr.RandChar(100, 200)
			modKeyValues[key] = value

			kvs.Put(key, value)
			cntWrites++
		} else { // Or don't, but put the key/value in the database
			value := modKeyValues[key]
			kvs.Put(key, value)
			cntWrites++
		}
		value, err := kvs.Get(key)
		assert.NoError(t, err, "didn't write key/value")
		assert.Equal(t, modKeyValues[key], value, "Didn't get the value")
		if !bytes.Equal(value, modKeyValues[key]) || err != nil {
			fmt.Println("error on key number ", cnt)
			return
		}
		cnt++
	}

	wps := cntWrites / time.Since(start).Seconds()
	start = time.Now()

	//==================================================================
	fmt.Print("Compressing\n")

	kvs.Compress()

	fmt.Println("Test post-compression")

	fmt.Print("Reading Perm Keys\n")
	cnt = 0
	for _, k := range modKeyList {
		v := modKeyValues[k]
		value, err := kvs.Get(k)
		assert.NoError(t, err, "Failed to get")
		assert.Equal(t, v, value, "Didn't the the value back")
		if !bytes.Equal(v, value) || err != nil {
			fmt.Printf("kv.Get failed: value did not match when it should. pass %d\n", cnt)
			return
		}
		cnt++
		cntReads++
	}

	fmt.Print("Reading Dyna Keys\n")

	for _, k := range keyList {
		v := keyValues[k]
		value, err := kvs.Get(k)
		assert.NoError(t, err, "Failed to get")
		assert.Equal(t, v, value, "Didn't the the value back")
		if !bytes.Equal(v, value) || err != nil {
			fmt.Printf("kv.Get failed: value did not match when it should\n")
			return
		}
		cntReads++
	}

	rps := cntReads / time.Since(start).Seconds()

	fmt.Printf("Writes per second %10.3f Reads per second %10.3f\n", wps, rps)
	fmt.Printf("Writes %s Reads %s\n", ComputeTimePerOp(wps), ComputeTimePerOp(rps))
}

func TestBuildBig(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "BigDB")

	const numPermKeys = 100_000_000
	const numModKeys = 100_000
	const minData = 100
	const maxData = 2000
	start := time.Now()
	var cntWrites, cntReads float64

	kvs, err := NewKVShard(dir)
	assert.NoError(t, err, "create kv")

	fmt.Print("Generating Keys\n")

	frD := NewFastRandom([]byte{1})
	frDV := NewFastRandom([]byte{1, 1})
	for i := 0; i < numModKeys; i++ {
		key := frD.NextHash()
		value := frDV.RandBuff(minData, maxData)
		kvs.Put(key, value)
		cntWrites++
	}

	frP := NewFastRandom([]byte{2})
	frPV := NewFastRandom([]byte{1, 1})
	for i := 0; i < numPermKeys; i++ {
		if (i+1)%(numPermKeys/100) == 0 {
			wps := cntWrites / time.Since(start).Seconds()
			tpw := ComputeTimePerOp(wps)
			fmt.Printf("perm entries %d puts/s %8.3f average put %s\n", i+1, wps, tpw)
		}
		key := frP.NextHash()
		value := frPV.RandBuff(minData, maxData)
		kvs.Put(key, value)
		cntWrites++
	}

	frD = NewFastRandom([]byte{1})
	for i := 0; i < numModKeys; i++ {
		key := frD.NextHash()
		value := frDV.RandBuff(minData, maxData)
		kvs.Put(key, value)
		cntWrites++
	}

	wps := cntWrites / time.Since(start).Seconds()
	tpw := ComputeTimePerOp(wps)
	fmt.Printf("wps %8.3f %s\n", wps, tpw)
	cntReads++

}
