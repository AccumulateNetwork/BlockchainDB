package blockchainDB

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKFile(t *testing.T) {
	//	dir, rm := MakeDir()
	//	defer rm()
	os.Remove("/tmp/KFileTest")
	dir := "/tmp/KFileTest"
	os.Mkdir(dir, os.ModePerm)

	const numKeys = 10

	fr := NewFastRandom([]byte{1})

	kf, err := NewKFile(dir)
	assert.NoError(t, err, "failed to create KFile")

	for i := 0; i < numKeys; i++ {
		k := fr.NextHash()
		dbbKey := new(DBBKey)
		dbbKey.Height = 0
		dbbKey.Offset = uint64(i) * 100
		dbbKey.Length = 1000
		err = kf.Put(k, dbbKey)
		assert.NoError(t, err, "failed to put")
	}

	err = kf.Close()
	assert.NoError(t, err, "failed to close")
	err = kf.Open()
	assert.NoError(t, err, "failed to open")

	fr.Reset()

	for i := 0; i < numKeys; i++ {
		k := fr.NextHash()
		dbbKey := new(DBBKey)
		dbbKey.Height = 0
		dbbKey.Offset = uint64(i) * 100
		dbbKey.Length = 1000
		dbk, err := kf.Get(k)
		assert.NoError(t, err, "failed to open")
		d1b := dbbKey.Bytes(k)
		d2b := dbk.Bytes(k)
		assert.Equal(t, d1b, d2b, "didn't get the dbbKey back")
	}

	assert.NoError(t, err, "failed to close KFile")
}
