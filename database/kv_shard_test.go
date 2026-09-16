package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKVShard(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()

	const numKVs = 1_000_000

	start := time.Now()
	var cntWrites, cntReads float64

	fr := NewFastRandom([]byte{1})
	kvs, err := NewKVShard(dir, 100_000)
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
	skipUnlessLoad(t)
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

	kvs, err := NewKVShard(dir, 100_000)
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
	skipUnlessLoad(t)
	dir := filepath.Join(os.TempDir(), "BigDB")

	const numPermKeys = 200_000_000
	const numModKeys = 100_000
	const minData = 100
	const maxData = 500
	start := time.Now()
	var cntWrites, cntReads float64

	kvs, err := NewKVShard(dir, 100_000)
	assert.NoError(t, err, "create kv")

	fmt.Print("Writing Keys to the Databases\n")

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
			wpss := humanize.Comma(int64(wps))
			tps := humanize.Comma(int64(i + 1))
			fmt.Printf("perm entries: %11s | puts/s: %8s | average put: %7s\n", tps, wpss, tpw)
		}
		key := frP.NextHash()
		value := frPV.RandBuff(minData, maxData)
		kvs.Put(key, value)
		cntWrites++
	}

	frD.Reset()
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

func TestBuildBig2(t *testing.T) {
	skipUnlessLoad(t)
	dir := filepath.Join(os.TempDir(), "BigDB")

	const numPermKeys = 20_000_000
	const numModKeys = 100_000
	const minData = 100
	const maxData = 500
	start := time.Now()
	var cntWrites float64

	kvs, err := NewKVShard(dir, 100_000)
	assert.NoError(t, err, "create kv")

	fmt.Print("Writing Keys to the Databases\n")

	frD := NewFastRandom([]byte{1})
	frDV := NewFastRandom([]byte{1, 1})
	for i := 0; i < numModKeys; i++ {
		key := frD.NextHash()
		value := frDV.RandBuff(minData, maxData)
		kvs.PutDyna(key, value)
		cntWrites++
	}

	frP := NewFastRandom([]byte{2})
	frPV := NewFastRandom([]byte{1, 1})
	for i := 0; i < numPermKeys; i++ {
		if (i+1)%(numPermKeys/100) == 0 {
			wps := cntWrites / time.Since(start).Seconds()
			tpw := ComputeTimePerOp(wps)
			wpss := humanize.Comma(int64(wps))
			tps := humanize.Comma(int64(i + 1))
			fmt.Printf("perm entries: %11s | puts/s: %8s | average put: %12s\n", tps, wpss, tpw)
		}
		key := frP.NextHash()
		value := frPV.RandBuff(minData, maxData)
		kvs.PutPerm(key, value)
		cntWrites++
	}

	frD.Reset()
	for i := 0; i < numModKeys; i++ {
		key := frD.NextHash()
		value := frDV.RandBuff(minData, maxData)
		kvs.PutDyna(key, value)
		cntWrites++
	}

	wps := cntWrites / time.Since(start).Seconds()
	tpw := ComputeTimePerOp(wps)
	fmt.Printf("wps %8.3f %s\n", wps, tpw)
	cntWrites++

}

// TestShardStatsSumTheShards
// A caller watching a node has one database, not 512: KVShard.Stats
// reports what the two layers did across every shard, in the shape one
// store reports it.  Wired for the Accumulate adapter's move to the
// sharded store (issue #62).
func TestShardStatsSumTheShards(t *testing.T) {
	dir := storeDir(t, "shardstats")
	kvs, err := NewKVShard(dir, 1000)
	require.NoError(t, err)
	defer kvs.Close()

	kr := NewFastRandom([]byte{178})
	keys := make([][32]byte, 200)
	for i := range keys {
		keys[i] = kr.NextHash()
		require.NoError(t, kvs.PutPerm(keys[i], []byte{byte(i)}))
	}
	for i := range keys { // Every one a duplicate: same key, same value
		require.NoError(t, kvs.PutPerm(keys[i], []byte{byte(i)}))
	}
	for i := 0; i < 50; i++ {
		require.NoError(t, kvs.PutDyna(kr.NextHash(), []byte{9}))
	}

	perm, dyna := kvs.Stats()
	require.Equal(t, uint64(len(keys)*2), perm.PutTotal, "every permanent write is counted once")
	require.Equal(t, uint64(len(keys)), perm.PutNew)
	require.Equal(t, uint64(len(keys)), perm.PutDuplicate, "the second pass was all duplicates")
	require.Equal(t, uint64(50), dyna.PutTotal, "the dynamic layer is counted apart")

	// The sum is the shards' sum, not one shard's
	var byShard uint64
	for _, shard := range kvs.Shards {
		byShard += shard.PermKV.Stats().PutTotal
	}
	require.Equal(t, byShard, perm.PutTotal)
}

// A put never compacts.  Maintenance runs on the caller's cadence, off
// the protocol path; the write-count trigger that once ran
// CompactHistory from inside a put cost the committing goroutine 3.4 s
// of every 30 s at nineteen minutes of load (#92).
func TestPutNeverCompacts(t *testing.T) {
	dir, rm := MakeDir()
	defer rm()
	kvs, err := NewKVShardN(dir, 1, 100) // Dyna segments form on the seal limit, not the block
	require.NoError(t, err)
	defer kvs.Close()
	require.NoError(t, kvs.SetFilterBlocks(MinFilterBlocks))

	inPut := false
	compactions := 0
	maintenanceHook = func() {
		compactions++
		if inPut {
			t.Errorf("a put ran CompactHistory")
		}
	}
	defer func() { maintenanceHook = nil }()

	// Past the old 5,000-write trigger, with blocks sealed along the
	// way so there is history to compact
	fr := NewFastRandom([]byte{92})
	for block := uint64(1); block <= 3*MinFilterBlocks; block++ {
		inPut = true
		for i := 0; i < 100; i++ {
			require.NoError(t, kvs.PutDyna(fr.NextHash(), []byte("v")))
			require.NoError(t, kvs.PutPerm(fr.NextHash(), []byte("v")))
			require.NoError(t, kvs.Put(fr.NextHash(), []byte("v")))
		}
		inPut = false
		require.NoError(t, kvs.SealBlock(block))
	}
	history, _ := tiers(kvs.Shards[0].DynaKV)
	require.Greater(t, len(history), 1, "the Dyna layer must hold history to compact")
	// The hook is live: compaction still runs when the caller asks
	before := compactions
	require.NoError(t, kvs.Compress())
	require.Greater(t, compactions, before, "Compress must compact the history the puts made")
}
