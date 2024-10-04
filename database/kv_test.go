package blockchainDB

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKV(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKVs = 2_000_000

	start := time.Now()
	var cntWrites, cntReads float64

	fr := NewFastRandom([]byte{1})
	kv, err := NewKVShard(0, dir, 1024)
	assert.NoError(t, err, "create kv")

	fmt.Print("Writing\n")

	for i := 0; i < numKVs; i++ {
		key := fr.NextHash()
		value := fr.RandBuff(100, 200)

		err = kv.Put(key, value)
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

		value2, err := kv.Get(key)
		assert.NoError(t, err, "Failed to put")
		assert.Equal(t, value, value2, "Didn't the the value back")
		if !bytes.Equal(value, value2) || err != nil {
			fmt.Printf("which failed %d\n", i)
			return
		}

		cntReads++
	}
	err = kv.Close()
	assert.NoError(t, err, "failed to close KVFile")

	rps := cntReads / time.Since(start).Seconds()

	fmt.Printf("Writes per second %10.3f Reads per second %10.3f\n", wps, rps)
	fmt.Printf("Write -- %s\n", ComputeTimePerOp(wps))
	fmt.Printf("Read  -- %s\n", ComputeTimePerOp(rps))
}

func TestKV_2(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKVs = 50_000

	start := time.Now()
	var cntWrites, cntReads float64

	frKeys := NewFastRandom([]byte{1})
	frValues := NewFastRandom([]byte{2})

	kv2, err := NewKV2(0, dir, 1024)
	assert.NoError(t, err, "create kv")

	fmt.Print("Writing\n")

	// Put some keys into the file
	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues.RandChar(100, 200)

		_, err = kv2.Put(key, value)
		assert.NoErrorf(t, err, "Failed to put %d", i)
		if err != nil {
			return
		}
		if i%10000 == 0 {
			kv2.Compress()
			kv2.Open()
		}

		cntWrites++
	}

	frValues2 := frValues.Clone()
	// Overwrite those same keys
	frKeys.Reset()
	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues.RandChar(100, 200)
		_, err = kv2.Put(key, value)
		assert.NoError(t, err, "Failed to put")
		if err != nil {
			return
		}
		if i%10000 == 0 {
			kv2.Compress()
			kv2.Open()
		}

		cntWrites++
	}

	wps := cntWrites / time.Since(start).Seconds()
	start = time.Now()

	//==================================================================
	fmt.Print("Reading\n")

	// First run through the keys making sure the first value
	// written for each key has been overwritten
	frKeys.Reset()
	frValues.Reset()
	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues.RandChar(100, 200)

		value2, err := kv2.Get(key)
		assert.NoError(t, err, "Should be not found")
		assert.NotEqual(t, value, value2, "Should not match")
		if bytes.Equal(value, value2) || err != nil {
			fmt.Printf("kv.Get failed: value matched when it should not %d\n", i)
			return
		}
		cntReads++
	}

	// Now reset the key generation, but not the value generation, to
	// test that all the keys reflect the updated values
	frKeys.Reset()

	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues.RandChar(100, 200)

		value2, err := kv2.Get(key)
		assert.NoError(t, err, "Failed to put")
		assert.Equal(t, value, value2, "Didn't the the value back")
		if !bytes.Equal(value, value2) || err != nil {
			fmt.Printf("kv.Get failed: value did not match when it should %d\n", i)
			return
		}
		cntReads++
	}

	// Lastly, don't reset the keys, so that means that these keys
	// were never written to disk.  None of these should be set.
	for i := 0; i < numKVs; i += int(frKeys.UintN(20)) {
		key := frKeys.NextHash()

		value2, err := kv2.Get(key)
		assert.Error(t, err, "Failed to put")
		assert.Nil(t, value2, "Should return nothing")
		if value2 != nil || err == nil {
			fmt.Printf("kv.Get failed: Not Found failed %d\n", i)
			return
		}
		cntReads++
	}

	kv2.Compress()
	kv2.Open()

	fmt.Println("Test post-compression")

	// Now reset the key generation, but not the value generation, to
	// test that all the keys reflect the updated values
	frKeys.Reset()

	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues2.RandChar(100, 200) // Use the frValues clone

		value2, err := kv2.Get(key)
		assert.NoError(t, err, "Failed to put")
		assert.Equal(t, value, value2, "Didn't the the value back")
		if !bytes.Equal(value, value2) || err != nil {
			fmt.Printf("kv.Get failed: value did not match when it should %d\n", i)
			return
		}
		cntReads++
	}

	rps := cntReads / time.Since(start).Seconds()

	fmt.Printf("Writes per second %10.3f Reads per second %10.3f\n", wps, rps)
	fmt.Printf("Writes %s Reads %s\n", ComputeTimePerOp(wps), ComputeTimePerOp(rps))
}
