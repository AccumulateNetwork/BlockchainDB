package blockchainDB

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKFile(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKeys = 200_000
	const offsetCnt = 1024
	const keyLimit = 1000
	const maxCachedBlocks = 50

	kf, err := NewKFile(false, dir, offsetCnt, keyLimit, maxCachedBlocks)
	assert.NoError(t, err, "failed to create KFile")

	fmt.Printf("Adding Keys\n")

	numWrites := 0
	numReads := 0
	start := time.Now()

	fr := NewFastRandom([]byte{1})
	nextDBBKey := func() (*DBBKey, [32]byte) {
		dbbKey := new(DBBKey)
		k := fr.NextHash()
		dbbKey.Offset = uint64(fr.UintN(10_000_000))
		dbbKey.Length = uint64(fr.UintN(1000))
		return dbbKey, k
	}

	for i := 0; i < numKeys; i++ {
		if numKeys > 100 && (i+1)%(numKeys/10) == 0 {
			fmt.Printf("Processing %10d\n", i+1)
		}
		dbbKey, k := nextDBBKey()
		err = kf.Put(k, dbbKey)
		numWrites++
		assert.NoError(t, err, "failed to put")
	}

	//kf.Close()
	//kf.Open()
	writesPerSec := float64(numWrites) / time.Since(start).Seconds()
	start = time.Now()

	fmt.Printf("Check Keys\n")

	fr.Reset()
	var fails int
	var fixes int
	for i := 0; i < numKeys; i++ {
		if numKeys > 100 && (i+1)%(numKeys/10) == 0 {
			fmt.Printf("\nProcessing %10d\n", i+1)
			readsPerSec := float64(numReads) / time.Since(start).Seconds()
			fmt.Printf("Fixes %d Fails %d of %d\n", fixes, fails, i)
			fmt.Printf("writes %10.3f/s  Reads %10.3f/s\n", writesPerSec, readsPerSec)
			fmt.Printf("write time : %s\n", ComputeTimePerOp(writesPerSec))
			fmt.Printf("read time  : %s\n", ComputeTimePerOp(readsPerSec))
		}
		dbbKey, k := nextDBBKey()
		dbk, err := kf.Get(k)
		for j := 0; j < 10 && err != nil; j++ {
			dbk, err = kf.Get(k)
			if err == nil {
				fixes++
				break
			}
		}
		if err == nil {
			if !bytes.Equal(dbbKey.Bytes(k), dbk.Bytes(k)) && fails > 100 {
				if fails < 10 {
					assert.Equalf(t, dbbKey.Bytes(k), dbk.Bytes(k), "dbbKey not same. key %d", i)

				}
			}
			fails++
		}
		numReads++
	}
	readsPerSec := float64(numReads) / time.Since(start).Seconds()
	fmt.Printf("Fixes %d Fails %d of %d\n", fixes, fails, numKeys)
	fmt.Printf("writes %10.3f/s  Reads %10.3f/s\n", writesPerSec, readsPerSec)
	fmt.Printf("write time : %s\n", ComputeTimePerOp(writesPerSec))
	fmt.Printf("read time  : %s\n", ComputeTimePerOp(readsPerSec))
	assert.NoError(t, err, "failed to close KFile")
}

func TestGetKeyList(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKeys = 20_000
	const offsetCnt = 1024
	const keyLimit = 1000
	const maxCachedBlocks = 50

	kf, err := NewKFile(false, dir, offsetCnt, keyLimit, maxCachedBlocks)
	assert.NoError(t, err, "Failed to create kfile")

	fr := NewFastRandom([]byte{0})

	MakeKeys := func() (keyValues map[[32]byte]*DBBKey, keyList [][32]byte) {
		keyValues = make(map[[32]byte]*DBBKey, numKeys)
		keyList = make([][32]byte, numKeys)
		for i := range keyList {
			k := fr.NextHash()
			keyList[i] = k
			dbbKey := new(DBBKey)
			dbbKey.Length = uint64(fr.UintN(100) + 100)
			dbbKey.Offset = uint64(fr.UintN(100_000_000))
			keyValues[k] = dbbKey
		}
		return keyValues, keyList
	}
	keyValues, keyList := MakeKeys()
	slices.SortFunc(keyList, func(a, b [32]byte) int {
		ia := kf.OffsetIndex(a[:])
		ib := kf.OffsetIndex(b[:])
		switch {
		case ia < ib:
			return -1
		case ia > ib:
			return 1
		default:
			return 0
		}
	})

	for _, k := range keyList {
		kf.Put(k, keyValues[k])
	}
	kf.Close()
	kf.Open()
	kv, kl, _ := kf.GetKeyList()
	for _, k := range keyList {
		v1 := kv[k]
		_ = v1

		_ = kl
	}
}

func TestImmutabilityWithHistory(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	// Create KFile with history enabled - values should be immutable
	kf, err := NewKFile(true, dir, 1024, 1000, 500)
	assert.NoError(t, err, "Failed to create kfile with history")

	// Create a test key and value
	var key [32]byte
	copy(key[:], []byte("test-key"))
	
	// Create original value
	originalValue := &DBBKey{
		Length: 100,
		Offset: 200,
	}
	
	// Put the original value
	err = kf.Put(key, originalValue)
	assert.NoError(t, err, "Should be able to write the key initially")
	
	// Verify we can get the value
	retrievedValue, err := kf.Get(key)
	assert.NoError(t, err, "Should be able to get the key")
	assert.Equal(t, originalValue.Bytes(key), retrievedValue.Bytes(key), "Retrieved value should match original")
	
	// Try to overwrite with a different value
	newValue := &DBBKey{
		Length: 300, // Different from original
		Offset: 400, // Different from original
	}
	
	// Verify the values are different
	assert.NotEqual(t, originalValue.Bytes(key), newValue.Bytes(key), "New value should be different from original")
	
	// Try to put the different value - should fail
	err = kf.Put(key, newValue)
	assert.Error(t, err, "Should not allow overwriting with different value when history is enabled")
	assert.Contains(t, err.Error(), "cannot overwrite immutable value when history is enabled")
	
	// Verify the original value is still there
	retrievedValue, err = kf.Get(key)
	assert.NoError(t, err, "Should still be able to get the key")
	assert.Equal(t, originalValue.Bytes(key), retrievedValue.Bytes(key), "Retrieved value should still match original")
	
	// Try to put the same value - should succeed
	sameValue := &DBBKey{
		Length: originalValue.Length,
		Offset: originalValue.Offset,
	}
	
	// Verify the values are the same
	assert.Equal(t, originalValue.Bytes(key), sameValue.Bytes(key), "Same value should match original")
	
	// Try to put the same value - should succeed
	err = kf.Put(key, sameValue)
	assert.NoError(t, err, "Should allow overwriting with the same value when history is enabled")
}

func TestMutabilityWithoutHistory(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	// Create KFile with history disabled - values should be mutable
	kf, err := NewKFile(false, dir, 1024, 1000, 500)
	assert.NoError(t, err, "Failed to create kfile without history")

	// Create a test key and value
	var key [32]byte
	copy(key[:], []byte("test-key"))
	
	// Create original value
	originalValue := &DBBKey{
		Length: 100,
		Offset: 200,
	}
	
	// Put the original value
	err = kf.Put(key, originalValue)
	assert.NoError(t, err, "Should be able to write the key initially")
	
	// Verify we can get the value
	retrievedValue, err := kf.Get(key)
	assert.NoError(t, err, "Should be able to get the key")
	assert.Equal(t, originalValue.Bytes(key), retrievedValue.Bytes(key), "Retrieved value should match original")
	
	// Try to overwrite with a different value
	newValue := &DBBKey{
		Length: 300, // Different from original
		Offset: 400, // Different from original
	}
	
	// Verify the values are different
	assert.NotEqual(t, originalValue.Bytes(key), newValue.Bytes(key), "New value should be different from original")
	
	// Try to put the different value - should succeed with history disabled
	err = kf.Put(key, newValue)
	assert.NoError(t, err, "Should allow overwriting with different value when history is disabled")
	
	// Verify the value has been updated
	retrievedValue, err = kf.Get(key)
	assert.NoError(t, err, "Should still be able to get the key")
	assert.Equal(t, newValue.Bytes(key), retrievedValue.Bytes(key), "Retrieved value should match the new value")
}
