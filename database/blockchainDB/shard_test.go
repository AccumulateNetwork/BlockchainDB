package blockchainDB

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShard(t *testing.T) {
	directory, rm := MakeDir()
	os.Mkdir(directory, os.ModePerm)
	defer rm()

	const Rounds = 10
	const Prt = 2
	const AddEntries = 100

	shard, err := NewShard(directory, "shard")
	assert.NoError(t, err, err)

	entries := make(map[[32]byte][]byte)
	fr := NewFastRandom([]byte{1, 2, 3, 4, 5})

	writes := 0
	reads := 0
	start := time.Now()
	for i := 0; i < Rounds; i++ {
		if i%Prt == 0 && i != 0 {
			fmt.Printf("Writes: %10d Reads %10d %13.0f/s \n", writes, reads,
				float64(writes+reads)/time.Since(start).Seconds())
		}
		for i := 0; i < AddEntries; i++ {
			entries[fr.NextHash()] = fr.RandBuff(100, 500)
		}
		for k := range entries {
			nv := fr.RandBuff(100, 500)
			writes++
			shard.Put(k, nv)
			err := shard.BFile.Close()
			assert.NoError(t, err, "close failed")
			entries[k] = nv
		}
		shard.BFile.Compress()
		for k, v := range entries {
			reads++
			v2, err := shard.Get(k)
			assert.NoError(t, err, err)
			assert.Equal(t, v, v2, "Didn't get the right value back")
		}
	}
	err = shard.Close()
	assert.NoError(t, err, "close failed")
}
