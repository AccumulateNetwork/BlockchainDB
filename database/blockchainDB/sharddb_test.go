package blockchainDB

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAShard(t *testing.T) {
	Directory, rm := MakeDir()
	defer rm()
	shardDB, err := NewShardDB(Directory, 1, 3)
	assert.NoError(t, err, "failed to create a shardDB")
	shard := shardDB.Shards[0]
	fr := NewFastRandom([]byte{1, 2, 3})
	for i := 0; i < 1000000; i++ {
		key := fr.NextHash()
		value := fr.RandBuff(100, 500)
		shard.Put(key, value)
	}
	err = shardDB.Close()
	assert.NoError(t, err, "close failed")
	_, err = OpenShardDB(Directory, 1)
	assert.NoError(t, err, "failed to open shardDB")
}

func TestShardDB(t *testing.T) {

	const numWrites = 100_000
	const EntrySize = 5000

	Directory, rm := MakeDir()
	defer rm()
	shardDB, err := NewShardDB(Directory, 1, 1024)
	defer os.RemoveAll(Directory)

	assert.NoError(t, err, "failed to create ShardDB")
	if err != nil {
		return
	}
	start := time.Now()
	r := NewFastRandom([]byte{1, 2, 3, 4}) // Regenerate key value pairs
	x := NewFastRandom([]byte{1})          // Pick random key value pairs to test
	for i := 0; i < numWrites; i++ {
		key := r.NextHash()
		value := r.RandBuff(100, EntrySize)
		shardDB.Put(key, value)
	}
	err = shardDB.Close()
	assert.NoError(t, err, "close failed")
	fmt.Printf("Write time:%v  %16.2f t/s\n", time.Since(start), numWrites/time.Since(start).Seconds())
	shardDB, err = OpenShardDB(Directory, 1)
	assert.NoError(t, err, "failed to open ShardDB")
	for i := 0; i < 5; i++ {
		numReads := float64(0)
		start := time.Now()              // Time the reading
		r.Reset()                        // We are going to generate the same key value pairs
		for j := 0; j < numWrites; j++ { // And check some of them.
			key := r.NextHash()
			value := r.RandBuff(100, EntrySize)
			if x.UintN(10000) < 1 {
				numReads++
				v := shardDB.Get(key)
				assert.Equal(t, value, v, "did not get the same value back")
				if !bytes.Equal(value, v) {
					return
				}
			}
		}
		fmt.Printf("Read Pass %d %v  %16.2f t/s\n", i, time.Since(start), numReads/time.Since(start).Seconds())
	}
	fmt.Printf("Done %v\n", time.Since(start))
}
