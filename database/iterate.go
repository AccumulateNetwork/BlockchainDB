package blockchainDB

import (
	"encoding/binary"
	"fmt"
)

// Iteration over a store's keys.
//
// This exists for the things that have to see everything -- a
// snapshot, an export, a consistency check -- rather than for the
// lookup path, and it is written for that: correctness and a simple
// contract over speed.
//
// A sealed segment's index is already a sorted list of keys, so
// iterating one is a sequential read rather than a traversal.  What
// costs something is shadowing: in a mutable store a key may appear in
// several segments and only the newest counts, so iteration walks
// newest to oldest and remembers what it has already emitted.  That
// set is proportional to the number of distinct keys, which is why
// this is not something to call on the hot path of a large store.
//
// (A k-way merge across the segments' sorted indexes would emit keys
// in order with no such set.  It is the better implementation and the
// obvious next step if iteration ever moves off the cold path.)

// ForEach
// Call fn for every key the store holds, with its current value.
// Iteration stops and returns the error if fn returns one.
//
// The order is unspecified.  A store must not be written to while
// ForEach runs: it holds the store's lock for the duration.
func (s *SegmentStore) ForEach(fn func(key [32]byte, value []byte) error) (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	if err = s.checkOpen(); err != nil {
		return err
	}

	seen := make(map[[32]byte]struct{}, len(s.live))

	// The live tail first: it is the newest thing in the store, so
	// anything it holds shadows every sealed copy
	for key, dbb := range s.live {
		value := make([]byte, dbb.Length)
		if err = s.liveFile.ReadAt(dbb.Offset, value); err != nil {
			return err
		}
		seen[key] = struct{}{}
		if err = fn(key, value); err != nil {
			return err
		}
	}

	const batch = 4096
	buff := make([]byte, batch*DBKeyFullSize)
	for i := len(s.segments) - 1; i >= 0; i-- { // Newest segment wins
		seg := s.segments[i]
		for at := int64(0); at < seg.count; {
			n := seg.count - at
			if n > batch {
				n = batch
			}
			b := buff[:n*DBKeyFullSize]
			if _, err = seg.index.ReadAt(b, segIndexHdrSize+at*DBKeyFullSize); err != nil {
				return err
			}
			for j := int64(0); j < n; j++ {
				rec := b[j*DBKeyFullSize:]
				var key [32]byte
				copy(key[:], rec[:32])
				if _, ok := seen[key]; ok {
					continue // A newer copy was already emitted
				}
				seen[key] = struct{}{}
				dbb := &DBBKey{
					Offset: binary.BigEndian.Uint64(rec[32:]),
					Length: binary.BigEndian.Uint64(rec[40:]),
				}
				value, err := seg.value(dbb)
				if err != nil {
					return err
				}
				if err = fn(key, value); err != nil {
					return err
				}
			}
			at += n
		}
	}
	return nil
}

// ForEach
// Call fn for every key the database holds.  The Dyna layer is walked
// first, and a key it holds is not emitted again from Perm: a key that
// moved to Dyna leaves its original behind in Perm, and the Dyna copy
// is the one Get answers with.
func (k *KV2) ForEach(fn func(key [32]byte, value []byte) error) error {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()

	dyna := make(map[[32]byte]struct{})
	err := k.DynaKV.ForEach(func(key [32]byte, value []byte) error {
		dyna[key] = struct{}{}
		return fn(key, value)
	})
	if err != nil {
		return err
	}
	return k.PermKV.ForEach(func(key [32]byte, value []byte) error {
		if _, ok := dyna[key]; ok {
			return nil // Shadowed by the dynamic copy
		}
		return fn(key, value)
	})
}

// ForEach
// Call fn for every key in the sharded database, shard by shard.
func (k *KVShard) ForEach(fn func(key [32]byte, value []byte) error) error {
	for i, shard := range k.Shards {
		if shard == nil {
			continue
		}
		if err := shard.ForEach(fn); err != nil {
			return fmt.Errorf("shard %d: %w", i, err)
		}
	}
	return nil
}
