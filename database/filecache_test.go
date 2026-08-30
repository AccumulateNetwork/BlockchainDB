package blockchainDB

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openFDs counts this process's open file descriptors.  Linux only,
// which is where the limit bites and where the tests run.
func openFDs(t *testing.T) int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("no /proc/self/fd: %v", err)
	}
	return len(entries)
}

// TestSegmentFDsStayBounded
// The point of issue #30: a store that seals many times must not hold
// two descriptors per segment for the life of the process.
//
// Sealing 400 times used to cost 800 descriptors and nothing ever gave
// them back, so a node sealing per block hit EMFILE at the common 1024
// limit inside ten minutes.  The count must now track the pool's
// limit, not the segment count -- and lookups must still work, which
// is the half a leak-free implementation could get wrong.
func TestSegmentFDsStayBounded(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	const limit = 16
	SetOpenFileLimit(limit)
	defer SetOpenFileLimit(defaultOpenFiles)

	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)

	const seals = 400
	before := openFDs(t)
	kr := NewFastRandom([]byte{131})
	keys := make([][32]byte, seals)
	for i := 0; i < seals; i++ {
		keys[i] = kr.NextHash()
		require.NoError(t, store.Put(keys[i], []byte(fmt.Sprintf("value-%d", i))))
		_, err = store.Seal(uint64(i + 1))
		require.NoError(t, err)
	}
	require.Len(t, store.sealedSegments(), seals, "every seal must have produced a segment")

	after := openFDs(t)
	growth := after - before
	assert.LessOrEqualf(t, growth, limit+8,
		"descriptors grew by %d across %d seals; the pool limit is %d", growth, seals, limit)

	// The bound is worth nothing if the data is not still reachable:
	// every key lives in a segment whose files the pool has closed
	for i, key := range keys {
		v, err := store.Get(key)
		require.NoErrorf(t, err, "key %d unreachable after its segment's files were closed", i)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), v)
	}

	open, _, _ := segmentFiles.stats()
	assert.LessOrEqual(t, open, limit, "pool holds more files than its limit")
	require.NoError(t, store.Close())
}

// TestFileCacheReusesAndEvicts
// The pool keeps one descriptor per path, hands the same one to
// concurrent borrowers, and evicts only what nobody is reading.
func TestFileCacheReusesAndEvicts(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))
	defer os.RemoveAll(dir)

	paths := make([]string, 4)
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("f%d", i))
		require.NoError(t, os.WriteFile(paths[i], []byte{byte(i)}, 0644))
	}

	c := newFileCache(2)

	// Two borrows of the same path share one descriptor
	f1, rel1, err := c.acquire(paths[0])
	require.NoError(t, err)
	f2, rel2, err := c.acquire(paths[0])
	require.NoError(t, err)
	assert.Same(t, f1, f2, "the same path must yield the same descriptor")
	open, idle, _ := c.stats()
	assert.Equal(t, 1, open)
	assert.Equal(t, 0, idle, "a file with live borrows is not idle")

	rel1()
	open, idle, _ = c.stats()
	assert.Equal(t, 0, idle, "still borrowed once")
	rel2()
	_, idle, _ = c.stats()
	assert.Equal(t, 1, idle, "fully released: now an eviction candidate")

	// Filling past the limit evicts the idle one
	_, r1, err := c.acquire(paths[1])
	require.NoError(t, err)
	_, r2, err := c.acquire(paths[2])
	require.NoError(t, err)
	open, _, _ = c.stats()
	assert.LessOrEqual(t, open, 2, "over the limit with an idle file available to evict")

	// With both in use, the limit yields rather than closing a file
	// someone is reading
	_, r3, err := c.acquire(paths[3])
	require.NoError(t, err)
	open, _, _ = c.stats()
	assert.Equal(t, 3, open, "the limit must never close a file in use")
	r1()
	r2()
	r3()
	open, _, _ = c.stats()
	assert.LessOrEqual(t, open, 2, "back under the limit once the borrows end")
}

// TestFileCacheForgetDuringBorrow
// Retiring a file that is being read must not close it underneath the
// reader.  This is what lets a compaction delete the generation an
// iteration is still walking.
func TestFileCacheForgetDuringBorrow(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "f")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0644))

	c := newFileCache(4)
	f, release, err := c.acquire(path)
	require.NoError(t, err)

	c.forget(path)                      // Retired while we hold it
	require.NoError(t, os.Remove(path)) // And unlinked

	buf := make([]byte, 5)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err, "an open descriptor must survive forget and unlink")
	assert.Equal(t, "hello", string(buf))

	release() // The last borrow closes it
	open, _, _ := c.stats()
	assert.Equal(t, 0, open)
}

// TestFileCacheConcurrentAcquire
// The pool is shared across shards, so it is hit from many goroutines
// at once.  Run under -race.
func TestFileCacheConcurrentAcquire(t *testing.T) {
	dir := filepath.Join(os.TempDir(), t.Name())
	os.RemoveAll(dir)
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))
	defer os.RemoveAll(dir)

	const files = 20
	paths := make([]string, files)
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("f%d", i))
		require.NoError(t, os.WriteFile(paths[i], []byte("x"), 0644))
	}

	c := newFileCache(4) // Deliberately far below the working set
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			buf := make([]byte, 1)
			for i := 0; i < 200; i++ {
				p := paths[(w*7+i)%files]
				f, release, err := c.acquire(p)
				if err != nil {
					t.Errorf("acquire %s: %v", p, err)
					return
				}
				if _, err := f.ReadAt(buf, 0); err != nil {
					t.Errorf("read %s: %v", p, err)
				}
				release()
			}
		}(w)
	}
	wg.Wait()

	open, idle, _ := c.stats()
	assert.LessOrEqual(t, open, 4, "pool over its limit with nothing borrowed")
	assert.Equal(t, open, idle, "everything released must be idle")
}
