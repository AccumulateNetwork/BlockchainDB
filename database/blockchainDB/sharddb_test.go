package blockchainDB

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAShard(t *testing.T) {
	shardDB, err := NewShardDB(Directory, Partition, 3, 3)
	assert.NoError(t, err, "failed to create a shardDB")
	shard := shardDB.Shards[0]
	fr := NewFastRandom([]byte{1, 2, 3})
	for i := 0; i < 100000; i++ {
		shard.Put(fr.NextHash(), fr.RandBuff(100, 500))
	}
	shardDB.Close()
	_, err = OpenShardDB(Directory, Partition, 3)
	assert.NoError(t, err, "failed to open shardDB")
}

func TestShardDB(t *testing.T) {

	shardDB, err := NewShardDB(Directory, Partition, 3, 5)
	defer os.RemoveAll(Directory)

	assert.NoError(t, err, "failed to create directory")
	if err != nil {
		return
	}
	r := NewFastRandom([]byte{1, 2, 3, 4})
	for i := 0; i < 100_000; i++ {
		key := r.NextHash()
		value := r.RandBuff(100, 1000)
		shardDB.Put(key, value)
	}
	r = NewFastRandom([]byte{1, 2, 3, 4})
	for i := 0; i < 100_000; i++ {
		key := r.NextHash()
		value := r.RandBuff(100, 1000)
		v := shardDB.Get(key)
		assert.Equal(t, value, v, "did not get the same value back")
	}
}
