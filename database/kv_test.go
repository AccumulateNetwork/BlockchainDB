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

	const numKVs = 10_000_000

	start := time.Now()
	var cntWrites, cntReads float64

	fr := NewFastRandom([]byte{1})
	kv, err := NewKV(dir)
	assert.NoError(t, err, "create kv")

	fmt.Print("Reading\n")

	for i := 0; i < numKVs; i++ {
		key := fr.NextHash()
		value := fr.RandBuff(100, 200)

		err = kv.Put(key, value)
		assert.NoError(t, err, "Failed to put")

		cntWrites++
	}

	wps := cntWrites / time.Since(start).Seconds()
	start = time.Now()

	fmt.Print("Writing\n")

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
}

func TestKV2(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKVs = 100_000

	start := time.Now()
	var cntWrites, cntReads float64

	frKeys := NewFastRandom([]byte{1})
	frValues := NewFastRandom([]byte{2})

	kv, err := NewKV(dir)
	assert.NoError(t, err, "create kv")

	fmt.Print("Writing\n")

	// Put some keys into the file
	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues.RandBuff(100, 200)

		err = kv.Put(key, value)
		assert.NoError(t, err, "Failed to put")

		cntWrites++
	}

	// Overwrite those same keys
	frKeys.Reset()
	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues.RandBuff(100, 200)

		err = kv.Put(key, value)
		assert.NoError(t, err, "Failed to put")

		cntWrites++
	}

	wps := cntWrites / time.Since(start).Seconds()
	start = time.Now()

	kv.Close()
	kv.Open()

	//==================================================================
	fmt.Print("Reading\n")

	frKeys.Reset()
	frValues.Reset()
	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues.RandBuff(100, 200)

		value2, err := kv.Get(key)
		assert.NoError(t, err, "Failed to get")
		assert.NotEqual(t, value, value2, "Should not match")
		if bytes.Equal(value, value2) || err != nil {
			fmt.Printf("kv.Get failed: value matched when it should not %d\n", i)
			return
		}

		cntReads++
	}

	frKeys.Reset()

	for i := 0; i < numKVs; i++ {
		key := frKeys.NextHash()
		value := frValues.RandBuff(100, 200)

		value2, err := kv.Get(key)
		assert.NoError(t, err, "Failed to put")
		assert.Equal(t, value, value2, "Didn't the the value back")
		if !bytes.Equal(value, value2) || err != nil {
			fmt.Printf("kv.Get failed: value did not match when it should %d\n", i)
			return
		}

		cntReads++
	}

	err = kv.Close()
	assert.NoError(t, err, "failed to close KVFile")

	rps := cntReads / time.Since(start).Seconds()

	fmt.Printf("Writes per second %10.3f Reads per second %10.3f\n", wps, rps)
}
