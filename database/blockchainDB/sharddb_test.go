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
	shardDB, err := NewShardDB(Directory, 1, 3, 1)
	assert.NoError(t, err, "failed to create a shardDB")
	shard := shardDB.Shards[0]
	fr := NewFastRandom([]byte{1, 2, 3})
	for i := 0; i < 100000; i++ {
		shard.Put(fr.NextHash(), fr.RandBuff(100, 500))
	}
	shardDB.Close()
	_, err = OpenShardDB(Directory, 1, 3)
	assert.NoError(t, err, "failed to open shardDB")
}

func TestShardDB(t *testing.T) {
    Directory, rm := MakeDir()
	defer rm()
	shardDB, err := NewShardDB(Directory, 1, 3, 3)
	defer os.RemoveAll(Directory)

	assert.NoError(t, err, "failed to create directory")
	if err != nil {
		return
	}
	start := time.Now()
	r := NewFastRandom([]byte{1, 2, 3, 4})
	for i := 0; i < 1_000_000; i++ {
		key := r.NextHash()
		value := r.RandBuff(100, 1000)
		shardDB.Put(key, value)
	}
	fmt.Printf("Write %v\n", time.Since(start))

	for i := 0; i < 5; i++ {
		start := time.Now()
		r = NewFastRandom([]byte{1, 2, 3, 4})
		for i := 0; i < 1_000_000; i++ {
			key := r.NextHash()
			value := r.RandBuff(100, 1000)
			v := shardDB.Get(key)
			assert.Equal(t, value, v, "did not get the same value back")
		}
		fmt.Printf("Read Pass %d %v\n", i, time.Since(start))
	}
	fmt.Printf("Done %v\n", time.Since(start))
}
