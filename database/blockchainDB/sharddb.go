package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// ShardDB
// Maintains shards of key value pairs to allow reading and writing of
// key value pairs even during compression and eventually multi-thread
// transactions.
type ShardDB struct {
	State     SState   // Holds any state we keep on the ShardDB
	BufferCnt int      // Buffer count used for BFiles
	BFiles    []*BFile // List of all the Shards
}

// NewShardDB
// Creates a ShardDB directory and structures.  Will overwrite an existing
// file or directory if it exists.
func NewShardDB(Directory string, Partition, ShardCnt int) (SDB *ShardDB, err error) {
	os.RemoveAll(Directory)

	SDB = new(ShardDB)
	SDB.BFiles = make([]*BFile, ShardCnt)
	err = os.Mkdir(Directory, os.ModePerm)
	if err != nil {
		return nil, err
	}
	f, e := os.Create(filepath.Join(Directory, "state.dat"))
	if e != nil {
		return nil, err
	}
	defer f.Close()

	var BShardCnt [2]byte                                      // Write big endian 16 bit shard cnt
	binary.BigEndian.PutUint16(BShardCnt[:], uint16(ShardCnt)) //
	f.Write(BShardCnt[:])                                      //

	for i := 0; i < len(SDB.BFiles); i++ {
		ithShard := filepath.Join(Directory, fmt.Sprintf("shard%03d-%03d", Partition, i))
		SDB.BFiles[i], err = NewBFile(ithShard)
		if err != nil {
			os.RemoveAll(Directory)
			return nil, err
		}
	}

	return SDB, nil
}

// OpenShardDB
// Opens an existing ShardDB.
func OpenShardDB(Directory string, Partition int) (SDB *ShardDB, err error) {
	SDB = new(ShardDB)
	state, err := NewState(filepath.Join(Directory, "state.dat"))
	if err != nil {
		return nil, err
	}
	SDB.State = *state
	SDB.BFiles = make([]*BFile, SDB.State.ShardCnt)
	for i := 0; i < len(SDB.BFiles); i++ {
		shard := filepath.Join(Directory, fmt.Sprintf("shard%03d-%03d", Partition, i))
		if SDB.BFiles[i], err = OpenBFile(shard); err != nil {
			return nil, err
		}
	}
	return SDB, nil
}

func (s *ShardDB) Close() (err error) {
	for _, bFile := range s.BFiles {
		if bFile != nil {
			if err = bFile.Close(); err != nil { // Close everything we have opened
				return err
			}
		}
	}
	return nil
}

// GetShard
// Get the shard responsible for a given key
func (s *ShardDB) GetShard(key [32]byte) *BFile {
	v := int(binary.BigEndian.Uint16(key[2:4])) // Use the second 2 bytes of an address to
	i := v % len(s.BFiles)                      // shard the database
	return s.BFiles[i]
}

// Put
// Put a key into the database
func (s *ShardDB) Put(key [32]byte, value []byte) error {
	shard := s.GetShard(key)
	return shard.Put(key, value)
}

// Get
// Get a key from the DB.  Returns a nil on an error
func (s *ShardDB) Get(key [32]byte) (value []byte) {
	shard := s.GetShard(key)
	v, _ := shard.Get(key)
	return v
}
