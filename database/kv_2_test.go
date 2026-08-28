package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKV2(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKVs = 1000
	const keyLimit = 100

	start := time.Now()
	var cntWrites, cntReads float64

	fr := NewFastRandom([]byte{1})
	kv2, err := NewKV2(dir, keyLimit)
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
		assert.Equalf(t, value, value2, "Didn't the the %d value back", i)
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

	const numKVs = 10_000
	const DynaPercent = 5
	const keyLimit = 100_000

	fr := NewFastRandom([]byte{1})

	keyValues := map[[32]byte][]byte{}
	modKeyValues := map[[32]byte][]byte{}

	start := time.Now()
	var cntWrites, cntReads float64

	kv2, err := NewKV2(dir, keyLimit)
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

// TestKV2BlockSealAfterAutoSeals
// The reproduction from issue #27, at the layer a node actually uses:
// a live tail that fills eight times before the first block boundary
// used to make Seal(1) -- and so every later block -- fail forever.
func TestKV2BlockSealAfterAutoSeals(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	defer os.RemoveAll(dir)

	kv, err := NewKV2(dir, 60) // sealLimit 60
	require.NoError(t, err)

	kr := NewFastRandom([]byte{41})
	keys := make([][32]byte, 500)
	for i := range keys {
		keys[i] = kr.NextHash()
		_, err = kv.PutPerm(keys[i], []byte("v"))
		require.NoError(t, err)
	}
	require.Greater(t, len(kv.PermKV.segments), 1, "the tail must have auto-sealed for this to test anything")

	_, err = kv.Seal(1)
	require.NoError(t, err, "the first block boundary must be sealable after auto-seals")

	// Keep going: more writes, more auto-seals, more blocks
	for b := uint64(2); b <= 4; b++ {
		for i := 0; i < 200; i++ {
			k := kr.NextHash()
			keys = append(keys, k)
			_, err = kv.PutPerm(k, []byte("v"))
			require.NoError(t, err)
		}
		_, err = kv.Seal(b)
		require.NoErrorf(t, err, "block %d must be sealable", b)
	}

	for i, k := range keys {
		v, err := kv.Get(k)
		require.NoErrorf(t, err, "key %d lost across blocks", i)
		require.Equal(t, []byte("v"), v)
	}
	require.NoError(t, kv.Close())
}
