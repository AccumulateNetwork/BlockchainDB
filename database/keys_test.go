package blockchainDB

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_dBBkey(t *testing.T) {
	fr := NewFastRandom([]byte{1, 2, 3, 4, 5, 6})

	dbbKey := new(DBBKey)
	dbbKey.Length = 7
	dbbKey.Offset = 101

	k := fr.NextHash()
	b := dbbKey.Bytes(k)
	nk, dbbKey2, err := GetDBBKey(b)
	assert.NoError(t, err, "should un-marshal")
	assert.Equal(t, k, nk, "Keys should be the same")
	assert.Equal(t, dbbKey2.Offset, dbbKey.Offset, "Offset should be the same")
	assert.Equal(t, dbbKey2.Length, dbbKey.Length, "Length should be the same")
}

func BenchmarkDBBKey(b *testing.B) {
	// Base: 6.509 ns/op
	var db DBBKey
	var c [50]byte
	var addr [32]byte
	for range b.N {
		db.WriteTo(addr, &c)
	}
	require.Equal(b, make([]byte, len(c)), c[:])
}

func BenchmarkKeyWrite(b *testing.B) {
	fr := NewFastRandom([]byte{1})
	keys := make([][32]byte, 1_000_000)
	keyValues := make(map[[32]byte]DBBKey, 1_000_000)
	for i := range keys {
		keys[i] = fr.NextHash()
		keyValues[keys[i]] = DBBKey{
			Offset: uint64(i) * 100,
			Length: 1000,
		}
	}

	null, err := os.Create(os.DevNull)
	require.NoError(b, err)

	// This is a copy of the inner loop of KFile.Close
	// Write all the keys following the Header

	// Apparently the compiler is smart enough to inline Bytes and realize that
	// the buffer does not escape, so there's no difference between Bytes and
	// WriteTo

	b.Run("Bytes", func(b *testing.B) {
		b.SetBytes(int64(len(keys)))
		for range b.N {
			for _, key := range keys {
				keyB := keyValues[key].Bytes(key)
				null.Write(keyB)
			}
		}
	})

	b.Run("WriteTo", func(b *testing.B) {
		b.SetBytes(int64(len(keys)))
		for range b.N {
			var buf [50]byte
			for _, key := range keys {
				keyValues[key].WriteTo(key, &buf)
				null.Write(buf[:])
			}
		}
	})

	b.Run("Block", func(b *testing.B) {
		b.SetBytes(int64(len(keys)))
		for range b.N {
			const N = 2 << 10
			var buf [50 * N]byte
			for i := 0; i < len(keys); i += N {
				n := N
				if n >= len(keys[i:]) {
					n = len(keys[i:])
				}
				for j, key := range keys[i : i+n] {
					keyValues[key].WriteTo(key, (*[50]byte)(buf[j*50:]))
				}
				null.Write(buf[:n*50])
			}
		}
	})
}
