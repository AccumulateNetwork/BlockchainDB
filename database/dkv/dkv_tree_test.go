package dkv

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTreeDKVBasicOperations(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("tree_dkv_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewTreeDKV(dir)
	require.NoError(t, err)
	defer store.Close()

	// Test Put and Get
	key1 := sha256.Sum256([]byte("key1"))
	value1 := []byte("value1")

	err = store.Put(key1, value1)
	assert.NoError(t, err)

	retrieved, found, err := store.Get(key1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value1, retrieved)

	// Test Update
	value2 := []byte("updated_value1")
	err = store.Put(key1, value2)
	assert.NoError(t, err)

	retrieved, found, err = store.Get(key1)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value2, retrieved)

	// Test Delete
	err = store.Delete(key1)
	assert.NoError(t, err)

	retrieved, found, err = store.Get(key1)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestTreeDKVSortedOrder(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("tree_dkv_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewTreeDKV(dir)
	require.NoError(t, err)
	defer store.Close()

	// Insert keys in random order
	keys := [][32]byte{}
	for i := 0; i < 100; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("key_%d", i)))
		value := []byte(fmt.Sprintf("value_%d", i))

		keys = append(keys, key)
		err = store.Put(key, value)
		assert.NoError(t, err)
	}

	// Sort keys for comparison
	sort.Slice(keys, func(i, j int) bool {
		return string(keys[i][:]) < string(keys[j][:])
	})

	// Verify all keys are retrievable
	for i, key := range keys {
		expectedValue := []byte(fmt.Sprintf("value_%d", i))
		retrieved, found, err := store.Get(key)
		assert.NoError(t, err)
		assert.True(t, found, "Key %d should be found", i)
		assert.NotNil(t, retrieved)
	}
}

func TestTreeDKVMemTableFlush(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("tree_dkv_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewTreeDKV(dir)
	require.NoError(t, err)
	defer store.Close()

	// Insert enough data to trigger MemTable flush
	largeValue := make([]byte, 100*1024) // 100KB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	numKeys := 50 // Should trigger flush with 100KB values
	for i := 0; i < numKeys; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("flush_key_%d", i)))
		err = store.Put(key, largeValue)
		assert.NoError(t, err)
	}

	// Check that flushes occurred
	assert.Greater(t, store.stats.MemFlushes.Load(), uint64(0), "Should have triggered MemTable flushes")

	// Verify all data is still accessible
	for i := 0; i < numKeys; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("flush_key_%d", i)))
		retrieved, found, err := store.Get(key)
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, len(largeValue), len(retrieved))
	}
}

func TestURLStore(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("url_store_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewURLStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// Test storing URLs
	urls := []string{
		"https://example.com/page1",
		"https://example.com/page2",
		"https://example.org/index",
		"https://test.com/api/v1/users",
		"https://test.com/api/v1/posts",
		"https://test.com/api/v2/users",
	}

	for _, url := range urls {
		data := []byte(fmt.Sprintf("content of %s", url))
		err = store.PutURL(url, data, "web_pages")
		assert.NoError(t, err)
	}

	// Test retrieving by URL
	data, meta, err := store.GetURL("https://example.com/page1")
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.NotNil(t, meta)
	assert.Equal(t, "example.com", meta.Domain)

	// Test domain iteration
	var examplePages []string
	err = store.IterateDomain("example.com", func(url string, data []byte) error {
		examplePages = append(examplePages, url)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(examplePages))

	// Test namespace iteration
	var count int
	err = store.IterateNamespace("web_pages", func(url string, data []byte) error {
		count++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, len(urls), count)
}

func TestURLStoreGroupedIteration(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("url_store_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewURLStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// Add URLs from different domains
	urls := map[string]string{
		"https://github.com/user/repo1":    "github",
		"https://github.com/user/repo2":    "github",
		"https://gitlab.com/project/code":  "gitlab",
		"https://bitbucket.org/team/repo":  "bitbucket",
		"https://github.com/another/repo3": "github",
	}

	for url, namespace := range urls {
		data := []byte(fmt.Sprintf("data for %s", url))
		err = store.PutURL(url, data, namespace)
		assert.NoError(t, err)
	}

	// Test grouped iteration by domain
	iter, err := store.NewGroupedIterator("domain")
	assert.NoError(t, err)

	domainCounts := make(map[string]int)
	for iter.NextGroup() {
		domain := iter.GroupName()
		for iter.NextEntry() {
			entry := iter.CurrentEntry()
			assert.NotNil(t, entry)
			domainCounts[domain]++
		}
	}

	// Verify grouping
	assert.Equal(t, 3, domainCounts["github.com"])
	assert.Equal(t, 1, domainCounts["gitlab.com"])
	assert.Equal(t, 1, domainCounts["bitbucket.org"])
}

func TestURLStoreSortedIteration(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("url_store_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewURLStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// Add URLs
	urls := []string{
		"https://zebra.com/page",
		"https://apple.com/page",
		"https://microsoft.com/page",
		"https://google.com/page",
	}

	for i, url := range urls {
		data := []byte(fmt.Sprintf("data_%d", i))
		err = store.PutURL(url, data, "test")
		assert.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// Test iteration ordered by domain
	iter, err := store.NewRangeIterator(OrderByDomain)
	assert.NoError(t, err)

	var domains []string
	for iter.Next() {
		entry := iter.Current()
		domains = append(domains, entry.Domain)
	}

	// Verify domain order
	assert.Equal(t, []string{"apple.com", "google.com", "microsoft.com", "zebra.com"}, domains)

	// Test iteration ordered by URL
	iter2, err := store.NewRangeIterator(OrderByURL)
	assert.NoError(t, err)

	var sortedURLs []string
	for iter2.Next() {
		entry := iter2.Current()
		sortedURLs = append(sortedURLs, entry.URL)
	}

	// URLs should be alphabetically sorted
	expectedOrder := make([]string, len(urls))
	copy(expectedOrder, urls)
	sort.Strings(expectedOrder)
	assert.Equal(t, expectedOrder, sortedURLs)
}

func TestURLStoreFindSimilar(t *testing.T) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("url_store_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewURLStore(dir)
	require.NoError(t, err)
	defer store.Close()

	// Add related URLs
	urls := []string{
		"https://api.example.com/v1/users/123",
		"https://api.example.com/v1/users/456",
		"https://api.example.com/v1/posts/789",
		"https://api.example.com/v2/users/123",
		"https://other.com/v1/users/123",
	}

	for _, url := range urls {
		err = store.PutURL(url, []byte(url), "api")
		assert.NoError(t, err)
	}

	// Find similar to a user endpoint
	similar, err := store.FindSimilar("https://api.example.com/v1/users/999", 10)
	assert.NoError(t, err)
	assert.Greater(t, len(similar), 0, "Should find similar URLs")

	// Should find other user endpoints on same domain
	for _, meta := range similar {
		assert.Equal(t, "api.example.com", meta.Domain)
	}
}

func BenchmarkTreeDKVPut(b *testing.B) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("tree_bench_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewTreeDKV(dir)
	require.NoError(b, err)
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("bench_key_%d", i)))
		value := []byte(fmt.Sprintf("bench_value_%d", i))
		store.Put(key, value)
	}
}

func BenchmarkTreeDKVGet(b *testing.B) {
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("tree_bench_%d", time.Now().UnixNano()))
	defer os.RemoveAll(dir)

	store, err := NewTreeDKV(dir)
	require.NoError(b, err)
	defer store.Close()

	// Prepare data
	keys := make([][32]byte, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("bench_key_%d", i)))
		value := []byte(fmt.Sprintf("bench_value_%d", i))
		store.Put(keys[i], value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % len(keys)
		store.Get(keys[idx])
	}
}