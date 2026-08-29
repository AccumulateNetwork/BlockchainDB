package blockchainDB

import (
	"container/list"
	"os"
	"sync"
)

// A bounded pool of open, read-only files.
//
// A sealed segment is two files, and the store used to hold both open
// for the life of the process: nothing ever closed them, and nothing
// merged the segments they belonged to.  A node sealing per block
// therefore leaked two descriptors per block -- EMFILE inside ten
// minutes at the default 1024, and a measured 14,935 descriptors in one
// test process here (issue #30).
//
// The fix is not to hold them.  A segment keeps its metadata in memory
// -- the key count and the bloom filter, which is what makes a lookup
// cheap -- and borrows a descriptor only while it is reading.  The pool
// closes the least recently used file once it is over its limit, so the
// descriptor count is bounded by the limit rather than by the segment
// count.
//
// Borrowing is reference counted, which buys the other half: a file
// with a live borrow is never closed, and on Unix an open descriptor
// keeps reading correctly after the path is unlinked.  So a reader can
// hold a segment open across a compaction that retires and deletes it
// -- which is what lets iteration run without the store's lock
// (issue #31).

// defaultOpenFiles is the pool's limit unless SetOpenFileLimit changes
// it.  It is a process-wide budget: a sharded database is 512 shards of
// two stores each, so a per-store limit would multiply by 1024 and be
// no bound at all.
//
// 512 is chosen to sit an order of magnitude below the 1024 descriptor
// limit that is still a common default, leaving room for the live tails
// (one per store), the manifests being rewritten, and whatever the host
// application holds.
const defaultOpenFiles = 512

// fileCache hands out shared read-only descriptors, keyed by path, and
// closes the least recently used once it is over its limit.
//
// Only files with no live borrow are eviction candidates: `lru` holds
// exactly the entries whose refs have fallen to zero, most recently
// released at the front.  A file borrowed more times than the limit
// allows simply exceeds it -- the limit bounds what the pool keeps
// open for its own convenience, never what a caller is actively using,
// because closing that would break a read in flight.
type fileCache struct {
	mu    sync.Mutex
	limit int
	open  map[string]*cachedFile
	lru   *list.List // *cachedFile, front = most recently released
}

type cachedFile struct {
	path string
	f    *os.File
	refs int
	el   *list.Element // Its place in lru while refs == 0; nil otherwise

	// dropped marks a file forget() removed from the pool while a
	// borrow was live.  It is no longer reachable by path, so the last
	// release closes it rather than returning it to the pool.
	dropped bool
}

func newFileCache(limit int) *fileCache {
	if limit < 0 {
		limit = 0 // Cache nothing; every release closes
	}
	return &fileCache{limit: limit, open: make(map[string]*cachedFile), lru: list.New()}
}

// filePoolShards is how many independent caches the process-wide pool
// is split into, by a hash of the path.
//
// One cache under one mutex was the read path's serialisation point
// once the store's own lock went shared (issue #50): every lookup
// borrows an index file and a data file, so every read took that mutex
// twice, and a mutex profile at eight readers put 100% of the wait on
// it -- acquire 69%, release 31%.  Sixty-four shards make the
// contention per shard a sixty-fourth of what it was, and a path's
// shard is fixed, so a file is only ever in one cache.
//
// Each shard enforces a sixty-fourth of the limit.  That is an
// approximation of a global bound -- a skewed working set can hold one
// shard at its limit while others sit empty -- but the limit was
// always a target rather than a ceiling, and this keeps eviction a
// per-shard decision that needs no cross-shard lock.
const filePoolShards = 64

// filePool is the process-wide pool: filePoolShards caches, each with
// its own lock, LRU, and share of the limit.
type filePool struct {
	shards [filePoolShards]*fileCache
}

func newFilePool(limit int) *filePool {
	p := &filePool{}
	for i := range p.shards {
		p.shards[i] = newFileCache(perShardLimit(limit))
	}
	return p
}

// perShardLimit divides the pool's limit among its shards.  A share of
// zero is legitimate and means the shard caches nothing: a file it
// hands out is closed as soon as its last borrow ends.  No floor is
// needed to serve a file -- a file in use is never an eviction
// candidate, whatever the limit -- and a floor of one would make the
// pool's real minimum filePoolShards, not what the caller asked for.
func perShardLimit(limit int) int {
	if limit < 0 {
		return 0
	}
	return limit / filePoolShards
}

// shard picks the cache for a path.  FNV-1a over the bytes: cheap, and
// what matters is only that the same path always lands in the same
// shard.
func (p *filePool) shard(path string) *fileCache {
	var h uint32 = 2166136261
	for i := 0; i < len(path); i++ {
		h ^= uint32(path[i])
		h *= 16777619
	}
	return p.shards[h%filePoolShards]
}

func (p *filePool) acquire(path string) (*os.File, func(), error) {
	return p.shard(path).acquire(path)
}

func (p *filePool) forget(path string) { p.shard(path).forget(path) }

func (p *filePool) setLimit(limit int) {
	for _, c := range p.shards {
		c.setLimit(perShardLimit(limit))
	}
}

// stats sums the shards.  Not a consistent snapshot -- each shard is
// read under its own lock -- which is fine for what it is used for.
func (p *filePool) stats() (open, idle, limit int) {
	for _, c := range p.shards {
		o, i, l := c.stats()
		open, idle, limit = open+o, idle+i, limit+l
	}
	return open, idle, limit
}

// segmentFiles is the pool every store borrows from.  It is process
// wide on purpose; see defaultOpenFiles.
var segmentFiles = newFilePool(defaultOpenFiles)

// SetOpenFileLimit
// Bound how many segment files the process keeps open.  Files
// currently being read are not closed, so the limit is a target the
// pool returns to rather than a hard ceiling at every instant.
//
// Lowering it closes the least recently used files immediately.
func SetOpenFileLimit(limit int) {
	segmentFiles.setLimit(limit)
}

func (c *fileCache) setLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.limit = limit
	c.evict()
}

// acquire
// Borrow the file at path, opening it if the pool does not already
// hold it.  The returned release must be called exactly once; until it
// is, the file is not an eviction candidate and will not be closed.
func (c *fileCache) acquire(path string) (f *os.File, release func(), err error) {
	c.mu.Lock()
	if cf, ok := c.open[path]; ok {
		cf.refs++
		if cf.el != nil { // Was idle: no longer a candidate
			c.lru.Remove(cf.el)
			cf.el = nil
		}
		c.mu.Unlock()
		return cf.f, func() { c.release(cf) }, nil
	}
	c.mu.Unlock()

	// Open outside the lock: opening is a syscall, and another
	// goroutine opening a different segment should not wait behind it.
	opened, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	c.mu.Lock()
	if cf, ok := c.open[path]; ok {
		// Someone else opened the same path while we were; keep theirs
		// so the pool holds one descriptor per path, not one per race
		opened.Close()
		cf.refs++
		if cf.el != nil {
			c.lru.Remove(cf.el)
			cf.el = nil
		}
		c.mu.Unlock()
		return cf.f, func() { c.release(cf) }, nil
	}
	cf := &cachedFile{path: path, f: opened, refs: 1}
	c.open[path] = cf
	c.evict() // The new file has a ref, so it is not a candidate itself
	c.mu.Unlock()
	return cf.f, func() { c.release(cf) }, nil
}

// release gives back one borrow, making the file evictable at zero
func (c *fileCache) release(cf *cachedFile) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cf.refs--
	if cf.refs > 0 {
		return
	}
	if cf.refs < 0 {
		panic("blockchainDB: segment file released more times than acquired")
	}
	if cf.dropped {
		cf.f.Close() // Retired while we were reading it; we were the last
		return
	}
	// Still in the map, so a later acquire reuses it rather than
	// reopening; the pool closes it only when it needs the room
	cf.el = c.lru.PushFront(cf)
	c.evict()
}

// forget
// Close and drop the pool's descriptor for path, if it holds an idle
// one.  A file still being read is left alone: it is unreachable by
// path once the caller deletes it, and its last release will close it.
//
// This exists so that retiring a segment does not leave the pool
// holding a descriptor to a deleted file, which on Unix keeps the
// blocks allocated until the descriptor goes.
func (c *fileCache) forget(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cf, ok := c.open[path]
	if !ok {
		return
	}
	delete(c.open, path)
	if cf.refs > 0 {
		// In use: whoever holds it finishes the read against the open
		// descriptor, and dropCloses it when the last borrow ends
		cf.dropped = true
		return
	}
	if cf.el != nil {
		c.lru.Remove(cf.el)
		cf.el = nil
	}
	cf.f.Close()
}

// evict closes idle files while the pool is over its limit.  The
// caller must hold mu.
func (c *fileCache) evict() {
	for len(c.open) > c.limit {
		el := c.lru.Back()
		if el == nil {
			return // Everything open is in use; the limit yields to that
		}
		cf := el.Value.(*cachedFile)
		c.lru.Remove(el)
		cf.el = nil
		delete(c.open, cf.path)
		cf.f.Close()
	}
}

// stats reports what the pool holds, for tests and diagnostics
func (c *fileCache) stats() (open, idle, limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.open), c.lru.Len(), c.limit
}
