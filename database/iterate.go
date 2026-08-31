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
// in order with no such set.  mergeIndexes in indexmerge.go is exactly
// that, and the merges use it; iteration could too if it ever moves off
// the cold path.)

// ForEach
// Call fn for every key the store holds, with its current value.
// Iteration stops and returns the error if fn returns one.
//
// The order is unspecified.  What fn sees is a SNAPSHOT: the sealed
// segments and live tail as they stood when ForEach was called.
// Writes made while it runs are not reported, and neither are the
// results of a compaction that commits underneath it.
//
// fn is called with no lock held, so it may read and write the store
// it is iterating.  It used to run under the store's lock for the
// whole iteration, which made a callback that called Get deadlock and
// made any callback block every other reader and writer for as long as
// it ran (issue #31).  The snapshot itself reads each tier under its
// own lock, one after the other, and pins the files first so that
// nothing either tier retires meanwhile is deleted under the walk.
func (s *SegmentStore) ForEach(fn func(key [32]byte, value []byte) error) (err error) {
	segs, live, cold, err := s.beginIterate()
	if err != nil {
		return err
	}
	defer s.unpin()

	seen := make(map[[32]byte]struct{}, len(live))

	// The live tail first: it is the newest thing in the store, so
	// anything it holds shadows every sealed copy
	for key, value := range live {
		seen[key] = struct{}{}
		if err = fn(key, value); err != nil {
			return err
		}
	}

	const batch = 4096
	buff := make([]byte, batch*DBKeyFullSize)
	for i := len(segs) - 1; i >= 0; i-- { // Newest segment wins
		if err = forEachInSegment(segs[i], buff, batch, seen, fn); err != nil {
			return err
		}
	}
	// Then what has left the segments for a block set.  The set list is
	// append-only, so this is a snapshot too; a key that a set and a
	// not-yet-retired segment both hold is emitted once.
	if cold != nil {
		return cold.forEach(func(key [32]byte, value []byte) error {
			if _, ok := seen[key]; ok {
				return nil
			}
			seen[key] = struct{}{}
			return fn(key, value)
		})
	}
	return nil
}

// forEachInSegment
// Emit the keys of one sealed segment that no newer segment already
// covered.  It is a function rather than the body of the loop so that
// the segment's index file is borrowed for exactly this segment and
// given back before the next one -- a deferred release inside the loop
// would hold one descriptor per segment, which is the cost ForEach
// exists downstream of (issue #30).
func forEachInSegment(
	seg *segment, buff []byte, batch int64,
	seen map[[32]byte]struct{}, fn func(key [32]byte, value []byte) error,
) error {
	index, release, err := seg.index()
	if err != nil {
		return err
	}
	defer release()
	for at := int64(0); at < seg.count; {
		n := seg.count - at
		if n > batch {
			n = batch
		}
		b := buff[:n*DBKeyFullSize]
		if _, err = index.ReadAt(b, segIndexHdrSize+at*DBKeyFullSize); err != nil {
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
	return nil
}

// ForEach
// Call fn for every key the database holds.  The Dyna layer is walked
// first, and a key it holds is not emitted again from Perm: a key that
// moved to Dyna leaves its original behind in Perm, and the Dyna copy
// is the one Get answers with.
//
// The KV2 lock is NOT held: each layer snapshots itself, and fn runs
// unlocked, so a callback is free to use the database it is walking.
// Holding it here would have reinstated exactly the deadlock the
// per-layer fix removed -- fn calling KV2.Get would block on the mutex
// its own iteration was holding (issue #31).
func (k *KV2) ForEach(fn func(key [32]byte, value []byte) error) error {
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
