package blockchainDB

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_dBBkey(t *testing.T) {

	const numTests = 1000

	fr := NewFastRandom([]byte{1})
	for i := 0; i < numTests; i++ {
		dbbKey := new(DBBKey)
		dbbKey.Length = uint64(fr.UintN(100 + 100))
		dbbKey.Offset = uint64(fr.UintN(100000))

		k := fr.NextHash()
		b := dbbKey.Bytes(k)
		nk, dbbKey2, err := GetDBBKey(b)
		assert.NoError(t, err, "should un-marshal")
		assert.Equal(t, k, nk, "Keys should be the same")
		assert.Equal(t, dbbKey2.Offset, dbbKey.Offset, "Offset should be the same")
		assert.Equal(t, dbbKey2.Length, dbbKey.Length, "Length should be the same")
	}
}
