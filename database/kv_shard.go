package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
)

const NumShards = 512

type KVShard struct {
	Directory string
	Shards    [NumShards]*KV2
}

func (k *KVShard) ShardDir(index int) string {
	dirname := fmt.Sprintf("Shard%04d", index)      // Shards are coded by name
	shardDir := filepath.Join(k.Directory, dirname) // Create the path
	return shardDir
}

// OpenKVShard
// Open an existing KVShard Database
func OpenKVShard(directory string) (kVShard *KVShard, err error) {
	kVShard = new(KVShard)
	kVShard.Directory = directory

	for i := range kVShard.Shards {
		shardDir := kVShard.ShardDir(i)
		if kVShard.Shards[i], err = OpenKV2(shardDir); err != nil {
			return nil, err
		}
	}

	return kVShard, nil
}

// NewKVShard
// Create a new KVShard database.  This database creates database shards to
// reduce the overhead of compressing large database files.
func NewKVShard(height int, directory string, offsetsCnt int) (kvs *KVShard, err error) {
	os.RemoveAll(directory)                                    // Get rid of any existing directory
	if err = os.MkdirAll(directory, os.ModePerm); err != nil { // Make the directory
		return nil, err
	}

	kvs = new(KVShard)          // Create a new sharded directory
	kvs.Directory = directory   // Keep the directory
	for i := range kvs.Shards { // Then create all the shards
		shardDir := kvs.ShardDir(i)
		if kvs.Shards[i], err = NewKV2(height, shardDir, offsetsCnt); err != nil { // Create the KV2 for each shard
			return nil, err
		}
	}

	return kvs, nil
}

// PutDyna
// Find the right shard, and put the key/value in the DynaKV in the shard
func (k *KVShard) PutDyna(key [32]byte, value []byte) (err error) {
	index := ShardIndex(key[:])
	k.Shards[index].Open()
	if writes, err := k.Shards[index].PutDyna(key, value); err != nil {
		return err
	} else if writes > 5000 {
		k.Shards[index].Compress()
	}
	return nil
}

// PutPerm
// Find the right shard, and put the key/value in the PermKV in the shard
func (k *KVShard) PutPerm(key [32]byte, value []byte) (err error) {
	index := ShardIndex(key[:])
	k.Shards[index].Open()
	if writes, err := k.Shards[index].PutPerm(key, value); err != nil {
		return err
	} else if writes > 5000 {
		k.Shards[index].Compress()
	}
	return nil
}

// Put
// Find the right shard, and put the key/value in said shard
func (k *KVShard) Put(key [32]byte, value []byte) (err error) {
	index := ShardIndex(key[:])
	k.Shards[index].Open()
	if writes, err := k.Shards[index].Put(key, value); err != nil {
		return err
	} else if writes > 5000 {
		k.Shards[index].Compress()
	}
	return nil
}

// GetDyna
// Find the right shard, and extract the value from the DynaKV in the shard
func (k *KVShard) GetDyna(key [32]byte) (value []byte, err error) {
	index := ShardIndex(key[:])
	k.Shards[index].Open()
	if value, err = k.Shards[index].GetDyna(key); err != nil {
		return nil, err
	}
	return value, nil
}

// GetPerm
// Find the right shard, and extract the value from the PermKV in the shard
func (k *KVShard) GetPerm(key [32]byte) (value []byte, err error) {
	index := ShardIndex(key[:])
	k.Shards[index].Open()
	if value, err = k.Shards[index].GetPerm(key); err != nil {
		return nil, err
	}
	return value, nil
}

// Get
// Find the right shard, and extract the value from said shard
func (k *KVShard) Get(key [32]byte) (value []byte, err error) {
	index := ShardIndex(key[:])
	k.Shards[index].Open()
	if value, err = k.Shards[index].Get(key); err != nil {
		return nil, err
	}
	return value, nil
}

// Compress
// Compress all the shards
func (k *KVShard) Compress() (err error) {
	for _, kvs := range k.Shards {
		if err = kvs.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Close
// Close all the shards
func (k *KVShard) Close() (err error) {
	for _, kvs := range k.Shards {
		if err = kvs.Close(); err != nil {
			return err
		}
	}
	return nil
}
