// Package adapter provides an adapter to use BlockchainDB with the accumulate keyvalue interface.
package adapter

import (
	"sync"
	"sync/atomic"

	"github.com/AccumulateNetwork/BlockchainDB/database/dkv"
	"gitlab.com/accumulatenetwork/accumulate/pkg/database"
	"gitlab.com/accumulatenetwork/accumulate/pkg/database/keyvalue"
	"gitlab.com/accumulatenetwork/accumulate/pkg/types/record"
)

// Stats tracks database statistics
type Stats struct {
	Puts          atomic.Uint64
	Gets          atomic.Uint64
	Deletes       atomic.Uint64
	BloomHits     atomic.Uint64
	BloomMisses   atomic.Uint64
	Deduplication atomic.Uint64
}

// Database wraps a BlockchainStore to implement keyvalue.Beginner
type Database struct {
	store *dkv.BlockchainStore
	stats Stats
}

// Open opens a BlockchainDB at the given path
func Open(path string) (*Database, error) {
	store, err := dkv.OpenBlockchainStore(path)
	if err != nil {
		// Try creating new if doesn't exist
		store, err = dkv.NewBlockchainStore(path)
		if err != nil {
			return nil, err
		}
	}
	return &Database{store: store}, nil
}

// Begin starts a new change set with the given prefix
func (d *Database) Begin(prefix *database.Key, writable bool) keyvalue.ChangeSet {
	return &changeSet{
		db:       d,
		prefix:   prefix,
		writable: writable,
		pending:  make(map[[32]byte][]byte),
		deleted:  make(map[[32]byte]bool),
	}
}

// Stats returns database statistics
func (d *Database) Stats() Stats {
	storeStats := d.store.Stats()
	var stats Stats
	stats.Puts.Store(d.stats.Puts.Load())
	stats.Gets.Store(d.stats.Gets.Load())
	stats.Deletes.Store(d.stats.Deletes.Load())
	stats.BloomHits.Store(storeStats.BloomHits.Load())
	stats.BloomMisses.Store(storeStats.BloomMisses.Load())
	stats.Deduplication.Store(storeStats.Deduplication.Load())
	return stats
}

// Close closes the database
func (d *Database) Close() error {
	return d.store.Close()
}

// changeSet implements keyvalue.ChangeSet
type changeSet struct {
	db       *Database
	prefix   *database.Key
	writable bool

	mu      sync.RWMutex
	pending map[[32]byte][]byte
	deleted map[[32]byte]bool
}

// makeKey creates a storage key from prefix and record key
func (cs *changeSet) makeKey(key *record.Key) [32]byte {
	if cs.prefix != nil {
		key = cs.prefix.AppendKey(key)
	}
	return [32]byte(key.Hash())
}

// Get loads a value
func (cs *changeSet) Get(key *record.Key) ([]byte, error) {
	cs.db.stats.Gets.Add(1)
	storageKey := cs.makeKey(key)

	// Check pending changes first
	cs.mu.RLock()
	if cs.deleted[storageKey] {
		cs.mu.RUnlock()
		return nil, nil
	}
	if val, ok := cs.pending[storageKey]; ok {
		cs.mu.RUnlock()
		return val, nil
	}
	cs.mu.RUnlock()

	// Get from underlying store
	val, err := cs.db.store.Get(storageKey[:])
	if err == dkv.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// Put stores a value
func (cs *changeSet) Put(key *record.Key, value []byte) error {
	if !cs.writable {
		return nil // Read-only, ignore writes
	}

	cs.db.stats.Puts.Add(1)
	storageKey := cs.makeKey(key)

	cs.mu.Lock()
	cs.pending[storageKey] = value
	delete(cs.deleted, storageKey)
	cs.mu.Unlock()

	return nil
}

// Delete deletes a key-value pair
func (cs *changeSet) Delete(key *record.Key) error {
	if !cs.writable {
		return nil // Read-only, ignore deletes
	}

	cs.db.stats.Deletes.Add(1)
	storageKey := cs.makeKey(key)

	cs.mu.Lock()
	cs.deleted[storageKey] = true
	delete(cs.pending, storageKey)
	cs.mu.Unlock()

	return nil
}

// ForEach iterates over each value
func (cs *changeSet) ForEach(fn func(*record.Key, []byte) error) error {
	// Note: This is a simplified implementation that doesn't support prefix iteration
	// For a full implementation, you'd need to track the original keys
	return cs.db.store.ForEach(func(key, value []byte) error {
		// We can't reconstruct the original record.Key from the hash
		// This is a limitation of the content-addressed storage
		return nil
	})
}

// Begin starts a nested change set
func (cs *changeSet) Begin(prefix *database.Key, writable bool) keyvalue.ChangeSet {
	newPrefix := prefix
	if cs.prefix != nil {
		if prefix != nil {
			newPrefix = cs.prefix.AppendKey(prefix)
		} else {
			newPrefix = cs.prefix
		}
	}

	return &changeSet{
		db:       cs.db,
		prefix:   newPrefix,
		writable: cs.writable && writable,
		pending:  make(map[[32]byte][]byte),
		deleted:  make(map[[32]byte]bool),
	}
}

// Commit commits pending changes
func (cs *changeSet) Commit() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Apply all pending writes
	for key, value := range cs.pending {
		if err := cs.db.store.Put(key[:], value); err != nil {
			return err
		}
	}

	// Apply all deletes
	for key := range cs.deleted {
		if err := cs.db.store.Delete(key[:]); err != nil {
			return err
		}
	}

	// Clear pending changes
	cs.pending = make(map[[32]byte][]byte)
	cs.deleted = make(map[[32]byte]bool)

	return nil
}

// Discard discards pending changes
func (cs *changeSet) Discard() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.pending = make(map[[32]byte][]byte)
	cs.deleted = make(map[[32]byte]bool)
}
