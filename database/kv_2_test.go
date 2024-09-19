package blockchainDB

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKV2(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKVs = 1_000_000

	start := time.Now()
	var cntWrites, cntReads float64

	fr := NewFastRandom([]byte{1})
	kv2, err := NewKV2(dir)
	assert.NoError(t, err, "create kv")

	fmt.Print("Writing\n")

	for i := 0; i < numKVs; i++ {
		key := fr.NextHash()
		value := fr.RandBuff(100, 200)

		_, err = kv2.Put(key, value)
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

		value2, err := kv2.Get(key)
		assert.NoError(t, err, "Failed to put")
		assert.Equal(t, value, value2, "Didn't the the value back")
		if !bytes.Equal(value, value2) || err != nil {
			fmt.Printf("which failed %d\n", i)
			return
		}

		cntReads++
	}
	err = kv2.Close()
	assert.NoError(t, err, "failed to close KVFile")

	rps := cntReads / time.Since(start).Seconds()

	fmt.Printf("Writes per second %10.3f Reads per second %10.3f\n", wps, rps)
	fmt.Printf("Write -- %s\n", ComputeTimePerOp(wps))
	fmt.Printf("Read  -- %s\n", ComputeTimePerOp(rps))
}

func TestKV2_2(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKVs = 3_000_000
	const DynaPercent = 5

	fr := NewFastRandom([]byte{1})

	keyValues := map[[32]byte][]byte{}
	modKeyValues := map[[32]byte][]byte{}

	start := time.Now()
	var cntWrites, cntReads float64

	kv2, err := NewKV2(dir)
	assert.NoError(t, err, "create kv")

	fmt.Print("Writing\n")

	// Put some keys into the file
	for i := 0; i < numKVs; i++ {
		key := fr.NextHash()
		value := fr.RandChar(100, 200)
		keyValues[key] = value

		cnt, _ := kv2.Put(key, value)
		if cnt > 5000 {
			kv2.Compress()
			kv2.Open()
		}

		cntWrites++
	}

	// Overwrite Some keys
	for k, v := range keyValues {
		if fr.UintN(100) < DynaPercent {
			v = fr.RandChar(100, 200)
			modKeyValues[k] = v
		}
		cnt, _ := kv2.Put(k, v)
		if cnt > 5000 {
			kv2.Compress()
			kv2.Open()
		}
	}

	wps := cntWrites / time.Since(start).Seconds()
	start = time.Now()

	//==================================================================
	fmt.Print("Reading\n")

	kv2.Compress()
	kv2.Open()

	fmt.Println("Test post-compression")

	for k, v := range keyValues {
		if value, ok := modKeyValues[k]; ok {
			v = value
		}
		value, err := kv2.Get(k)
		assert.NoError(t, err, "Failed to put")
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
