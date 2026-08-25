package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	bloomFilename    = "bloom.dat"     // Persisted BloomSet
	bloomTmpFilename = "bloom_tmp.dat" // Tmp file for atomic replace
	bloomMagic       = 0x424C4D31      // "BLM1"
)

// BloomSet
// A scalable Bloom filter: a stack of layers where only the newest
// layer accepts keys.  When the active layer reaches its capacity it is
// frozen and a new layer roughly double the total size is started, so
// the layer count stays logarithmic in the key count and NO existing
// key is ever re-read or re-hashed to grow the filter.
//
// Test checks every layer; a key is possibly present if any layer says
// so.  Later layers get more bits per key so the aggregate false
// positive rate stays near the single-layer design point (~1-3%)
// instead of adding ~1% per layer.
//
// Frozen layers are immutable, which makes the set cheap to persist:
// see Save/LoadBloomSet.
type BloomSet struct {
	Layers []*Bloom
}

// NewBloomSet
// Create a BloomSet whose first layer is sized for initialCapacity
// keys with k hash functions
func NewBloomSet(initialCapacity uint64, k int) *BloomSet {
	return &BloomSet{Layers: []*Bloom{NewBloomSizedForKeys(initialCapacity, k)}}
}

// Count
// Total number of keys added across all layers (duplicates counted)
func (b *BloomSet) Count() (n uint64) {
	for _, l := range b.Layers {
		n += l.Count
	}
	return n
}

// Set
// Add a key.  Grows a new layer when the active one is full.
func (b *BloomSet) Set(key [32]byte) {
	active := b.Layers[len(b.Layers)-1]
	if active.Count >= active.Capacity {
		// Freeze the active layer; the new layer is sized for double
		// everything so far, with more bits per key to keep the
		// aggregate false positive rate bounded
		bitsPerKey := BloomBitsPerKey + 4*uint64(len(b.Layers))
		active = newBloomSized(2*b.Count(), bitsPerKey, active.K)
		b.Layers = append(b.Layers, active)
	}
	active.Set(key)
}

// Test
// Returns false if the key is definitely not present; true if it might be
func (b *BloomSet) Test(key [32]byte) bool {
	for _, l := range b.Layers {
		if l.Test(key) {
			return true
		}
	}
	return false
}

// Save
// Persist the BloomSet to directory/bloom.dat via a tmp file and an
// atomic rename.  Layers are written verbatim; loading is a read, not
// a rebuild.
func (b *BloomSet) Save(directory string) (err error) {
	tmpPath := filepath.Join(directory, bloomTmpFilename)
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			f.Close()
			os.Remove(tmpPath)
		}
	}()

	var header [8]byte
	binary.BigEndian.PutUint32(header[:], bloomMagic)
	binary.BigEndian.PutUint32(header[4:], uint32(len(b.Layers)))
	if _, err = f.Write(header[:]); err != nil {
		return err
	}
	var lh [28]byte
	for _, l := range b.Layers {
		binary.BigEndian.PutUint32(lh[:], uint32(l.K))
		binary.BigEndian.PutUint64(lh[4:], l.NumBytes)
		binary.BigEndian.PutUint64(lh[12:], l.Capacity)
		binary.BigEndian.PutUint64(lh[20:], l.Count)
		if _, err = f.Write(lh[:]); err != nil {
			return err
		}
		if _, err = f.Write(l.Map); err != nil {
			return err
		}
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		f = nil
		return err
	}
	f = nil
	if err = os.Rename(tmpPath, filepath.Join(directory, bloomFilename)); err != nil {
		return err
	}
	return syncDir(directory)
}

// LoadBloomSet
// Load a persisted BloomSet from directory/bloom.dat
func LoadBloomSet(directory string) (b *BloomSet, err error) {
	f, err := os.Open(filepath.Join(directory, bloomFilename))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var header [8]byte
	if _, err = io.ReadFull(f, header[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(header[:]) != bloomMagic {
		return nil, fmt.Errorf("bloom.dat has the wrong magic number")
	}
	layerCnt := binary.BigEndian.Uint32(header[4:])
	if layerCnt == 0 || layerCnt > 64 {
		return nil, fmt.Errorf("bloom.dat has an invalid layer count %d", layerCnt)
	}

	b = new(BloomSet)
	var lh [28]byte
	for i := uint32(0); i < layerCnt; i++ {
		if _, err = io.ReadFull(f, lh[:]); err != nil {
			return nil, err
		}
		l := new(Bloom)
		l.K = int(binary.BigEndian.Uint32(lh[:]))
		l.NumBytes = binary.BigEndian.Uint64(lh[4:])
		l.Capacity = binary.BigEndian.Uint64(lh[12:])
		l.Count = binary.BigEndian.Uint64(lh[20:])
		if l.K < 1 || l.NumBytes < minBloomBytes || l.NumBytes > 1<<32 {
			return nil, fmt.Errorf("bloom.dat layer %d is invalid", i)
		}
		l.SizeOfMap = float64(l.NumBytes) / (1024 * 1024)
		l.Map = make([]byte, l.NumBytes)
		if _, err = io.ReadFull(f, l.Map); err != nil {
			return nil, err
		}
		b.Layers = append(b.Layers, l)
	}
	return b, nil
}
