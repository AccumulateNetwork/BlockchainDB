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
	bfc.Put2Cache(key, value)
	for i := 0; i < 1000; i++ {
		v := bfc.GetFromCache(key)
		assert.Equal(t, value, v, "Didn't get my value back after purge")
		for j := 0; j < 1000; j++ {
			key = fr.NextHash()
			value = fr.RandBuff(10, 1000)
			bfc.Put2Cache(key, value)
			v = bfc.GetFromCache(key)
			assert.Equal(t, value, v, "Didn't get my value back")
		}
		bfc.PurgeCache()
		assert.True(t, len(bfc.keyValues) == 1, "Length of key value pairs should be 1")
	}
}
