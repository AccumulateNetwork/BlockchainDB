package dkv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// URLStore wraps TreeDKV to handle URL hashing and metadata intelligently
type URLStore struct {
	tree *TreeDKV

	// Secondary indices for grouping
	domainIndex   map[string][][32]byte // domain -> list of hashes
	namespaceIndex map[string][][32]byte // namespace -> list of hashes
	indexMu       sync.RWMutex

	// Metadata storage
	metaDB        *TreeDKV // Separate tree for metadata
}

// URLMetadata stores information about a URL
type URLMetadata struct {
	URL       string    `json:"url"`
	Domain    string    `json:"domain"`
	Path      string    `json:"path"`
	Namespace string    `json:"namespace"`
	Timestamp int64     `json:"timestamp"`
	Tags      []string  `json:"tags,omitempty"`
	Size      int64     `json:"size,omitempty"`
}

// ExtendedKey combines hash with sortable metadata
type ExtendedKey struct {
	Hash      [32]byte // Primary key (hash of URL)
	Domain    []byte   // For domain-based sorting
	Timestamp int64    // For time-based sorting
	Namespace []byte   // For namespace grouping
}

// NewURLStore creates a new URL-aware store
func NewURLStore(directory string) (*URLStore, error) {
	// Main tree for data
	mainTree, err := NewTreeDKV(filepath.Join(directory, "data"))
	if err != nil {
		return nil, err
	}

	// Metadata tree
	metaTree, err := NewTreeDKV(filepath.Join(directory, "metadata"))
	if err != nil {
		return nil, err
	}

	store := &URLStore{
		tree:           mainTree,
		metaDB:         metaTree,
		domainIndex:    make(map[string][][32]byte),
		namespaceIndex: make(map[string][][32]byte),
	}

	// Load indices
	if err := store.loadIndices(); err != nil {
		return nil, err
	}

	return store, nil
}

// PutURL stores a URL and its associated data
func (s *URLStore) PutURL(urlStr string, data []byte, namespace string) error {
	// Parse URL
	u, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	// Generate hash key
	hash := sha256.Sum256([]byte(urlStr))

	// Create metadata
	meta := URLMetadata{
		URL:       urlStr,
		Domain:    u.Host,
		Path:      u.Path,
		Namespace: namespace,
		Timestamp: time.Now().Unix(),
		Size:      int64(len(data)),
	}

	// Store data with hash key
	if err := s.tree.Put(hash, data); err != nil {
		return err
	}

	// Store metadata
	metaData, _ := json.Marshal(meta)
	metaKey := s.makeMetaKey(hash)
	if err := s.metaDB.Put(metaKey, metaData); err != nil {
		return err
	}

	// Update indices
	s.indexMu.Lock()
	s.domainIndex[u.Host] = append(s.domainIndex[u.Host], hash)
	if namespace != "" {
		s.namespaceIndex[namespace] = append(s.namespaceIndex[namespace], hash)
	}
	s.indexMu.Unlock()

	return s.saveIndices()
}

// GetURL retrieves data by URL
func (s *URLStore) GetURL(urlStr string) ([]byte, *URLMetadata, error) {
	hash := sha256.Sum256([]byte(urlStr))

	// Get data
	data, found, err := s.tree.Get(hash)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		return nil, nil, errors.New("URL not found")
	}

	// Get metadata
	meta, err := s.getMetadata(hash)
	if err != nil {
		return data, nil, err
	}

	return data, meta, nil
}

// IterateDomain iterates over all URLs from a specific domain
func (s *URLStore) IterateDomain(domain string, fn func(url string, data []byte) error) error {
	s.indexMu.RLock()
	hashes, exists := s.domainIndex[domain]
	s.indexMu.RUnlock()

	if !exists {
		return nil
	}

	// Sort hashes for consistent iteration order
	sortedHashes := make([][32]byte, len(hashes))
	copy(sortedHashes, hashes)
	sort.Slice(sortedHashes, func(i, j int) bool {
		return bytes.Compare(sortedHashes[i][:], sortedHashes[j][:]) < 0
	})

	for _, hash := range sortedHashes {
		// Get metadata
		meta, err := s.getMetadata(hash)
		if err != nil {
			continue
		}

		// Get data
		data, found, err := s.tree.Get(hash)
		if err != nil || !found {
			continue
		}

		if err := fn(meta.URL, data); err != nil {
			return err
		}
	}

	return nil
}

// IterateNamespace iterates over all entries in a namespace
func (s *URLStore) IterateNamespace(namespace string, fn func(url string, data []byte) error) error {
	s.indexMu.RLock()
	hashes, exists := s.namespaceIndex[namespace]
	s.indexMu.RUnlock()

	if !exists {
		return nil
	}

	for _, hash := range hashes {
		meta, err := s.getMetadata(hash)
		if err != nil {
			continue
		}

		data, found, err := s.tree.Get(hash)
		if err != nil || !found {
			continue
		}

		if err := fn(meta.URL, data); err != nil {
			return err
		}
	}

	return nil
}

// RangeIterator provides sorted iteration with various ordering options
type RangeIterator struct {
	store      *URLStore
	entries    []RangeEntry
	position   int
	orderBy    OrderBy
}

type RangeEntry struct {
	Hash     [32]byte
	URL      string
	Domain   string
	Metadata *URLMetadata
	Data     []byte
}

type OrderBy int

const (
	OrderByHash OrderBy = iota
	OrderByDomain
	OrderByTimestamp
	OrderByURL
)

// NewRangeIterator creates an iterator with specified ordering
func (s *URLStore) NewRangeIterator(orderBy OrderBy) (*RangeIterator, error) {
	iter := &RangeIterator{
		store:   s,
		orderBy: orderBy,
	}

	// Collect all entries
	entries := []RangeEntry{}

	// Use tree iterator to get all keys
	treeIter := s.tree.NewIterator()
	for treeIter.Next() {
		hash := treeIter.Key()
		value := treeIter.Value()

		meta, err := s.getMetadata(hash)
		if err != nil {
			continue
		}

		entries = append(entries, RangeEntry{
			Hash:     hash,
			URL:      meta.URL,
			Domain:   meta.Domain,
			Metadata: meta,
			Data:     value,
		})
	}

	// Sort based on orderBy
	switch orderBy {
	case OrderByHash:
		sort.Slice(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].Hash[:], entries[j].Hash[:]) < 0
		})
	case OrderByDomain:
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Domain != entries[j].Domain {
				return entries[i].Domain < entries[j].Domain
			}
			return entries[i].URL < entries[j].URL
		})
	case OrderByTimestamp:
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Metadata.Timestamp < entries[j].Metadata.Timestamp
		})
	case OrderByURL:
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].URL < entries[j].URL
		})
	}

	iter.entries = entries
	iter.position = -1

	return iter, nil
}

func (iter *RangeIterator) Next() bool {
	iter.position++
	return iter.position < len(iter.entries)
}

func (iter *RangeIterator) Current() *RangeEntry {
	if iter.position >= 0 && iter.position < len(iter.entries) {
		return &iter.entries[iter.position]
	}
	return nil
}

// CompoundKey creates a sortable compound key for special ordering
func (s *URLStore) CompoundKey(hash [32]byte, secondary []byte) []byte {
	// Combine hash with secondary key for custom sorting
	result := make([]byte, 32+len(secondary))
	copy(result[:32], hash[:])
	copy(result[32:], secondary)
	return result
}

// GroupedIterator iterates over entries grouped by a specific criteria
type GroupedIterator struct {
	store   *URLStore
	groups  map[string][]RangeEntry
	keys    []string
	keyIdx  int
	entryIdx int
}

// NewGroupedIterator creates an iterator that groups entries
func (s *URLStore) NewGroupedIterator(groupBy string) (*GroupedIterator, error) {
	iter := &GroupedIterator{
		store:  s,
		groups: make(map[string][]RangeEntry),
		keyIdx: -1,
	}

	// Collect and group entries
	treeIter := s.tree.NewIterator()
	for treeIter.Next() {
		hash := treeIter.Key()
		value := treeIter.Value()

		meta, err := s.getMetadata(hash)
		if err != nil {
			continue
		}

		entry := RangeEntry{
			Hash:     hash,
			URL:      meta.URL,
			Domain:   meta.Domain,
			Metadata: meta,
			Data:     value,
		}

		// Determine group key
		var groupKey string
		switch groupBy {
		case "domain":
			groupKey = meta.Domain
		case "namespace":
			groupKey = meta.Namespace
		case "path":
			// Group by path prefix
			parts := strings.Split(meta.Path, "/")
			if len(parts) > 1 {
				groupKey = "/" + parts[1]
			} else {
				groupKey = "/"
			}
		default:
			groupKey = "default"
		}

		iter.groups[groupKey] = append(iter.groups[groupKey], entry)
	}

	// Sort group keys
	for key := range iter.groups {
		iter.keys = append(iter.keys, key)
	}
	sort.Strings(iter.keys)

	return iter, nil
}

func (iter *GroupedIterator) NextGroup() bool {
	iter.keyIdx++
	iter.entryIdx = 0
	return iter.keyIdx < len(iter.keys)
}

func (iter *GroupedIterator) GroupName() string {
	if iter.keyIdx >= 0 && iter.keyIdx < len(iter.keys) {
		return iter.keys[iter.keyIdx]
	}
	return ""
}

func (iter *GroupedIterator) NextEntry() bool {
	if iter.keyIdx < 0 || iter.keyIdx >= len(iter.keys) {
		return false
	}

	key := iter.keys[iter.keyIdx]
	entries := iter.groups[key]

	iter.entryIdx++
	return iter.entryIdx <= len(entries)
}

func (iter *GroupedIterator) CurrentEntry() *RangeEntry {
	if iter.keyIdx < 0 || iter.keyIdx >= len(iter.keys) {
		return nil
	}

	key := iter.keys[iter.keyIdx]
	entries := iter.groups[key]

	if iter.entryIdx > 0 && iter.entryIdx <= len(entries) {
		return &entries[iter.entryIdx-1]
	}

	return nil
}

// Helper methods

func (s *URLStore) makeMetaKey(hash [32]byte) [32]byte {
	// Create a separate key for metadata
	return sha256.Sum256(append([]byte("meta:"), hash[:]...))
}

func (s *URLStore) getMetadata(hash [32]byte) (*URLMetadata, error) {
	metaKey := s.makeMetaKey(hash)
	data, found, err := s.metaDB.Get(metaKey)
	if err != nil || !found {
		return nil, err
	}

	var meta URLMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

func (s *URLStore) loadIndices() error {
	// Load indices from metadata DB
	// This would iterate through metadata and rebuild indices
	return nil
}

func (s *URLStore) saveIndices() error {
	// Save indices to metadata DB for persistence
	return nil
}

// BatchPutURLs efficiently stores multiple URLs
func (s *URLStore) BatchPutURLs(urls map[string][]byte, namespace string) error {
	for urlStr, data := range urls {
		if err := s.PutURL(urlStr, data, namespace); err != nil {
			return err
		}
	}
	return nil
}

// FindSimilar finds URLs with similar paths or domains
func (s *URLStore) FindSimilar(urlStr string, limit int) ([]*URLMetadata, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var results []*URLMetadata

	// Find same domain
	s.indexMu.RLock()
	domainHashes := s.domainIndex[u.Host]
	s.indexMu.RUnlock()

	for _, hash := range domainHashes {
		if len(results) >= limit {
			break
		}

		meta, err := s.getMetadata(hash)
		if err != nil {
			continue
		}

		// Check path similarity
		if strings.HasPrefix(meta.Path, filepath.Dir(u.Path)) {
			results = append(results, meta)
		}
	}

	return results, nil
}

// Stats returns statistics about the URL store
func (s *URLStore) Stats() map[string]interface{} {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()

	stats := map[string]interface{}{
		"total_domains":    len(s.domainIndex),
		"total_namespaces": len(s.namespaceIndex),
		"domains":          make(map[string]int),
	}

	// Count URLs per domain
	for domain, hashes := range s.domainIndex {
		stats["domains"].(map[string]int)[domain] = len(hashes)
	}

	return stats
}

// Close closes the URL store
func (s *URLStore) Close() error {
	if err := s.saveIndices(); err != nil {
		return err
	}

	if err := s.tree.Close(); err != nil {
		return err
	}

	return s.metaDB.Close()
}