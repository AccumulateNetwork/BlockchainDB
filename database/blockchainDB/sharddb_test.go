package blockchainDB

import (
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
	for i := 0; i < 100000; i++ {
		shard.Put(fr.NextHash(), fr.RandBuff(100, 500))
	}
	shardDB.Close()
	_, err = OpenShardDB(Directory, 1)
	assert.NoError(t, err, "failed to open shardDB")
}

func TestShardDB(t *testing.T) {

	const numWrites = 10_000_000

	Directory, rm := MakeDir()
	defer rm()
	shardDB, err := NewShardDB(Directory, 1, 3)
	defer os.RemoveAll(Directory)

	assert.NoError(t, err, "failed to create ShardDB")
	if err != nil {
		return
	}
	start := time.Now()
	r := NewFastRandom([]byte{1, 2, 3, 4})
	for i := 0; i < numWrites; i++ {
		if i%1_000_000 == 0 {
			fmt.Printf("%4d ", i)
			if i%1_000_000 == 0 {
				fmt.Println()
			}
		}
		key := r.NextHash()
		value := r.RandBuff(100, 1000)
		shardDB.Put(key, value)
	}
	shardDB.Close()
	fmt.Printf("Write time:%v  %16.2f\n", time.Since(start), numWrites/time.Since(start).Seconds())

	shardDB, err = OpenShardDB(Directory, 1)
	assert.NoError(t, err, "failed to open ShardDB")
	for i := 0; i < 5; i++ {
		start := time.Now()
		r = NewFastRandom([]byte{1, 2, 3, 4})
		for i := 0; i < numWrites; i++ {
			key := r.NextHash()
			value := r.RandBuff(100, 1000)
			v := shardDB.Get(key)
			assert.Equal(t, value, v, "did not get the same value back")
		}
		fmt.Printf("Read Pass %d %v  %16.2f\n", i, time.Since(start), numWrites/time.Since(start).Seconds())
	}
	fmt.Printf("Done %v\n", time.Since(start))
}
