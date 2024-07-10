package blockchainDB

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	bfc := NewBFBuffer()
	fr := NewFastRandom([]byte{1, 2, 3, 4, 5, 6})

	key := fr.NextHash()
	value := fr.RandBuff(10, 100)
	bfc.Put(key, value)
	for i := 0; i < 1000; i++ {
		v := bfc.Get(key)
		assert.Equal(t, value, v, "Didn't get my value back after purge")
		for j := 0; j < 1000; j++ {
			key = fr.NextHash()
			value = fr.RandBuff(10, 1000)
			bfc.Put(key, value)
			v = bfc.Get(key)
			assert.Equal(t, value, v, "Didn't get my value back")
		}
		bfc.Purge()
		assert.True(t, len(bfc.keyValues) == 1, "Length of key value pairs should be 1")
	}
}
