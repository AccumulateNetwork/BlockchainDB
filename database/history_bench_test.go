package blockchainDB

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// BenchmarkHistoryGet measures key lookups against a HistoryFile loaded
// with a realistic number of records (the dominant cost in the profile;
// see issue #9).
func BenchmarkHistoryGet(b *testing.B) {
	dir := filepath.Join(os.TempDir(), "BenchHistoryGet")
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	hf, err := NewHistoryFile(1024, dir)
	if err != nil {
		b.Fatal(err)
	}

	// Load 1M keys the way PushHistory does: batches sorted by bin
	const totalKeys = 1_000_000
	const batch = 100_000
	fr := NewFastRandom([]byte{42})
	var keys [][32]byte
	kf := new(KFile) // borrow OffsetIndex via a header
	kf.Header.Init(1024)
	for n := 0; n < totalKeys; n += batch {
		buff := make([]byte, 0, batch*DBKeyFullSize)
		batchKeys := make([][32]byte, batch)
		for i := range batchKeys {
			batchKeys[i] = fr.NextHash()
		}
		// sort by bin as GetKeyList does
		sort.Slice(batchKeys, func(i, j int) bool {
			return kf.OffsetIndex(batchKeys[i][:]) < kf.OffsetIndex(batchKeys[j][:])
		})
		dbb := new(DBBKey)
		for _, k := range batchKeys {
			dbb.Offset += 100
			dbb.Length = 100
			buff = append(buff, dbb.Bytes(k)...)
		}
		if err := hf.AddKeys(buff); err != nil {
			b.Fatal(err)
		}
		keys = append(keys, batchKeys...)
	}

	fr2 := NewFastRandom([]byte{43})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[int(fr2.UintN(uint(len(keys))))]
		if _, err := hf.Get(key); err != nil {
			b.Fatal("key not found")
		}
	}
}
