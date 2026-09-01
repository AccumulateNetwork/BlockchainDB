package blockchainDB

import (
	"errors"
	"sync"
)

// ErrWriterClosed is returned by ShardWriter methods after Close
var ErrWriterClosed = errors.New("shard writer is closed")

// putReq is one queued write (or a flush barrier when flush != nil)
type putReq struct {
	key   [32]byte
	value []byte
	perm  bool            // true -> PutPerm, false -> PutDyna
	flush *sync.WaitGroup // non-nil marks a flush barrier
}

// ShardWriter
// An asynchronous, multi-core ingest front end for a KVShard.
//
// A single producer (e.g. a blockchain's transaction executor) queues
// writes with PutPerm/PutDyna; a pool of workers applies them in
// parallel across the shards.  Keys are routed to workers by shard, so:
//
//   - Writes to the SAME key are always applied in the order queued
//     (same key -> same shard -> same worker -> FIFO queue).
//   - Writes to different shards proceed in parallel with no lock
//     contention (each KV2 shard has its own mutex).
//
// Ordering across different keys is NOT guaranteed between flushes.
// Call Flush at a consistency point (e.g. the end of a block): it
// blocks until every write queued before it has been applied.
//
// Reads through the underlying KVShard do not see queued writes until
// they are applied; call Flush first when read-your-writes is required.
//
// The value slice is owned by the writer once queued and must not be
// modified by the caller afterward.
//
// PutPerm/PutDyna and Flush may be called from multiple goroutines.
// Close stops the workers; further calls return ErrWriterClosed.
type ShardWriter struct {
	kvs      *KVShard
	queues   []chan putReq
	wg       sync.WaitGroup
	mu       sync.Mutex   // guards firstErr
	closeMu  sync.RWMutex // senders hold RLock; Close holds Lock
	closed   bool         // guarded by closeMu
	firstErr error
}

// NewShardWriter
// Create a ShardWriter over this KVShard with the given number of
// worker goroutines and per-worker queue depth.  workers is clamped to
// [1, the database's shard count]; queueDepth to a minimum of 1.
func (k *KVShard) NewShardWriter(workers, queueDepth int) *ShardWriter {
	if workers < 1 {
		workers = 1
	}
	if n := len(k.Shards); workers > n {
		workers = n
	}
	if queueDepth < 1 {
		queueDepth = 1
	}
	w := &ShardWriter{kvs: k, queues: make([]chan putReq, workers)}
	for i := range w.queues {
		w.queues[i] = make(chan putReq, queueDepth)
		w.wg.Add(1)
		go w.run(w.queues[i])
	}
	return w
}

// run
// Worker loop: apply queued writes, release flush barriers
func (w *ShardWriter) run(q chan putReq) {
	defer w.wg.Done()
	for req := range q {
		if req.flush != nil {
			req.flush.Done()
			continue
		}
		var err error
		if req.perm {
			err = w.kvs.PutPerm(req.key, req.value)
		} else {
			err = w.kvs.PutDyna(req.key, req.value)
		}
		if err != nil {
			w.mu.Lock()
			if w.firstErr == nil {
				w.firstErr = err
			}
			w.mu.Unlock()
		}
	}
}

// route
// A key's worker.  Derived from the shard index so a key always lands
// on the same worker (per-key write ordering) and a worker's shards are
// touched by no other worker.
func (w *ShardWriter) route(key [32]byte) int {
	return ShardIndex(key[:]) % len(w.queues)
}

// PutPerm
// Queue a write to the Perm (immutable) layer.  Returns any error a
// worker has hit so far; the write itself is applied asynchronously.
func (w *ShardWriter) PutPerm(key [32]byte, value []byte) error {
	return w.send(putReq{key: key, value: value, perm: true})
}

// PutDyna
// Queue a write to the Dyna (mutable) layer.  Returns any error a
// worker has hit so far; the write itself is applied asynchronously.
func (w *ShardWriter) PutDyna(key [32]byte, value []byte) error {
	return w.send(putReq{key: key, value: value, perm: false})
}

// send
// Queue one write.  The read lock makes a concurrent Close wait for
// in-flight sends instead of closing the channels under them.
func (w *ShardWriter) send(req putReq) error {
	w.closeMu.RLock()
	defer w.closeMu.RUnlock()
	if w.closed {
		return ErrWriterClosed
	}
	w.queues[w.route(req.key)] <- req
	return w.Err()
}

// Err
// The first error any worker has hit, or nil
func (w *ShardWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.firstErr
}

// Flush
// Block until every write queued before this call has been applied.
// Use at consistency points such as block boundaries.
func (w *ShardWriter) Flush() error {
	w.closeMu.RLock()
	if w.closed {
		w.closeMu.RUnlock()
		return ErrWriterClosed
	}
	var barrier sync.WaitGroup
	barrier.Add(len(w.queues))
	for _, q := range w.queues {
		q <- putReq{flush: &barrier}
	}
	w.closeMu.RUnlock()
	barrier.Wait()
	return w.Err()
}

// Close
// Drain the queues, stop the workers, and return the first error any
// worker hit.  The underlying KVShard remains open.
func (w *ShardWriter) Close() error {
	w.closeMu.Lock()
	if w.closed {
		w.closeMu.Unlock()
		return w.Err() // Already closed
	}
	w.closed = true
	for _, q := range w.queues {
		close(q)
	}
	w.closeMu.Unlock()
	w.wg.Wait()
	return w.Err()
}
