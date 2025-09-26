package dkv

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Tree configuration
	maxKeysPerLeaf   = 100000 // Maximum keys in a leaf file before split
	maxLevels        = 7       // Maximum tree depth
	memTableSize     = 4 * 1024 * 1024 // 4MB MemTable before flush

	// File prefixes
	sstablePrefix = "sstable_"
	manifestFile  = "manifest.json"

	// SSTable format
	blockSize     = 4096  // 4KB blocks
	restartInterval = 16  // Restart points every 16 keys
)

// TreeDKV implements a tree-based dynamic KV store with sorted organization
type TreeDKV struct {
	directory string

	// MemTable for recent writes
	memTable    *MemTable
	memTableMu  sync.RWMutex

	// Tree structure
	manifest    *Manifest
	manifestMu  sync.RWMutex

	// SSTable files organized by level
	levels      [maxLevels][]*SSTable
	levelsMu    sync.RWMutex

	// Compaction control
	compacting  atomic.Bool
	lastCompact time.Time

	// Statistics
	stats       TreeStats

	// Iteration support
	iterSeq     atomic.Uint64
}

// TreeStats tracks statistics
type TreeStats struct {
	Writes       atomic.Uint64
	Reads        atomic.Uint64
	Compactions  atomic.Uint64
	MemFlushes   atomic.Uint64
}

// Manifest tracks the tree structure and file organization
type Manifest struct {
	Version      int                `json:"version"`
	Levels       []LevelManifest    `json:"levels"`
	NextFileNum  uint64             `json:"next_file_num"`
	LastCompact  time.Time          `json:"last_compact"`
}

// LevelManifest describes files at each level
type LevelManifest struct {
	Level    int            `json:"level"`
	Files    []FileManifest `json:"files"`
}

// FileManifest describes an SSTable file
type FileManifest struct {
	FileNum    uint64    `json:"file_num"`
	MinKey     [32]byte  `json:"min_key"`
	MaxKey     [32]byte  `json:"max_key"`
	Size       int64     `json:"size"`
	NumKeys    int       `json:"num_keys"`
	Created    time.Time `json:"created"`
}

// MemTable is an in-memory sorted buffer
type MemTable struct {
	entries    *SkipList
	size       atomic.Int64
	frozen     atomic.Bool
}

// SkipList implements an in-memory sorted data structure
type SkipList struct {
	head   *skipListNode
	height int
	size   int
	mu     sync.RWMutex
}

type skipListNode struct {
	key     [32]byte
	value   []byte
	next    []*skipListNode
	deleted bool
}

// SSTable represents a sorted string table file
type SSTable struct {
	file        *os.File
	path        string
	index       *SSTableIndex
	bloomFilter *BloomFilter
	minKey      [32]byte
	maxKey      [32]byte
	numKeys     int
}

// SSTableIndex holds block offsets for efficient lookups
type SSTableIndex struct {
	entries []IndexEntry
}

// IndexEntry points to a block in the SSTable
type IndexEntry struct {
	FirstKey [32]byte
	Offset   int64
	Size     int32
}

// CompositeKey supports metadata and grouping
type CompositeKey struct {
	Key          [32]byte `json:"key"`
	Namespace    string   `json:"namespace,omitempty"`
	OriginalData []byte   `json:"original,omitempty"`
	Timestamp    int64    `json:"timestamp,omitempty"`
}

// NewTreeDKV creates a new tree-based DKV store
func NewTreeDKV(directory string) (*TreeDKV, error) {
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	tdkv := &TreeDKV{
		directory:   directory,
		memTable:    NewMemTable(),
		lastCompact: time.Now(),
	}

	// Load or create manifest
	if err := tdkv.loadOrCreateManifest(); err != nil {
		return nil, err
	}

	// Load existing SSTables
	if err := tdkv.loadSSTables(); err != nil {
		return nil, err
	}

	// Start background compaction
	go tdkv.backgroundCompaction()

	return tdkv, nil
}

// NewMemTable creates a new MemTable
func NewMemTable() *MemTable {
	return &MemTable{
		entries: NewSkipList(),
	}
}

// NewSkipList creates a new skip list
func NewSkipList() *SkipList {
	return &SkipList{
		head: &skipListNode{
			next: make([]*skipListNode, 32), // Max height 32
		},
		height: 1,
	}
}

// Put stores a key-value pair
func (t *TreeDKV) Put(key [32]byte, value []byte) error {
	t.stats.Writes.Add(1)

	// Add to MemTable
	t.memTableMu.Lock()
	t.memTable.Put(key, value)
	size := t.memTable.size.Load()
	t.memTableMu.Unlock()

	// Check if MemTable needs flushing
	if size > memTableSize {
		return t.flushMemTable()
	}

	return nil
}

// Get retrieves a value by key
func (t *TreeDKV) Get(key [32]byte) ([]byte, bool, error) {
	t.stats.Reads.Add(1)

	// Check MemTable first
	t.memTableMu.RLock()
	if value, found, deleted := t.memTable.Get(key); found {
		t.memTableMu.RUnlock()
		if deleted {
			return nil, false, nil
		}
		return value, true, nil
	}
	t.memTableMu.RUnlock()

	// Search through levels
	t.levelsMu.RLock()
	defer t.levelsMu.RUnlock()

	for level := 0; level < maxLevels; level++ {
		for _, sst := range t.levels[level] {
			if t.keyInRange(key, sst.minKey, sst.maxKey) {
				if value, found, err := sst.Get(key); found {
					return value, true, err
				} else if err != nil {
					return nil, false, err
				}
			}
		}
	}

	return nil, false, nil
}

// Delete marks a key as deleted
func (t *TreeDKV) Delete(key [32]byte) error {
	t.memTableMu.Lock()
	t.memTable.Delete(key)
	size := t.memTable.size.Load()
	t.memTableMu.Unlock()

	if size > memTableSize {
		return t.flushMemTable()
	}

	return nil
}

// MemTable operations

func (m *MemTable) Put(key [32]byte, value []byte) {
	m.entries.Put(key, value)
	m.size.Add(int64(32 + len(value)))
}

func (m *MemTable) Get(key [32]byte) ([]byte, bool, bool) {
	return m.entries.Get(key)
}

func (m *MemTable) Delete(key [32]byte) {
	m.entries.Delete(key)
	m.size.Add(32) // Tombstone size
}

// SkipList operations

func (s *SkipList) Put(key [32]byte, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find position
	update := make([]*skipListNode, 32)
	current := s.head

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key[:], key[:]) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	// Check if key exists
	if current.next[0] != nil && current.next[0].key == key {
		// Update existing
		current.next[0].value = value
		current.next[0].deleted = false
		return
	}

	// Insert new node
	newHeight := s.randomHeight()
	if newHeight > s.height {
		for i := s.height; i < newHeight; i++ {
			update[i] = s.head
		}
		s.height = newHeight
	}

	newNode := &skipListNode{
		key:   key,
		value: value,
		next:  make([]*skipListNode, newHeight),
	}

	for i := 0; i < newHeight; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	s.size++
}

func (s *SkipList) Get(key [32]byte) ([]byte, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	current := s.head
	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key[:], key[:]) < 0 {
			current = current.next[i]
		}
	}

	current = current.next[0]
	if current != nil && current.key == key {
		return current.value, true, current.deleted
	}

	return nil, false, false
}

func (s *SkipList) Delete(key [32]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*skipListNode, 32)
	current := s.head

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key[:], key[:]) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	current = current.next[0]
	if current != nil && current.key == key {
		current.deleted = true
	}
}

func (s *SkipList) randomHeight() int {
	height := 1
	for height < 32 && (time.Now().UnixNano()&1) == 0 {
		height++
	}
	return height
}

// flushMemTable writes MemTable to disk as SSTable
func (t *TreeDKV) flushMemTable() error {
	t.memTableMu.Lock()

	// Freeze current MemTable
	oldMem := t.memTable
	oldMem.frozen.Store(true)

	// Create new MemTable
	t.memTable = NewMemTable()
	t.memTableMu.Unlock()

	// Write to Level 0
	sst, err := t.writeSSTable(oldMem, 0)
	if err != nil {
		return err
	}

	// Add to level 0
	t.levelsMu.Lock()
	t.levels[0] = append(t.levels[0], sst)
	t.levelsMu.Unlock()

	t.stats.MemFlushes.Add(1)

	// Trigger compaction if needed
	if len(t.levels[0]) > 4 {
		go t.compactLevel(0)
	}

	return t.saveManifest()
}

// writeSSTable creates an SSTable file from MemTable
func (t *TreeDKV) writeSSTable(mem *MemTable, level int) (*SSTable, error) {
	// Generate filename
	t.manifestMu.Lock()
	fileNum := t.manifest.NextFileNum
	t.manifest.NextFileNum++
	t.manifestMu.Unlock()

	filename := fmt.Sprintf("%s%d_%d.sst", sstablePrefix, level, fileNum)
	path := filepath.Join(t.directory, fmt.Sprintf("level%d", level), filename)

	// Create directory if needed
	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return nil, err
	}

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	sst := &SSTable{
		file:  file,
		path:  path,
		index: &SSTableIndex{},
	}

	// Write sorted entries
	var blockBuffer bytes.Buffer
	var indexEntries []IndexEntry
	var currentBlock []KVPair
	var minKey, maxKey [32]byte
	var firstKey = true
	offset := int64(0)

	// Iterate through MemTable in order
	iter := mem.entries.Iterator()
	for iter.Next() {
		key, value, deleted := iter.Current()
		if deleted {
			continue // Skip deleted entries
		}

		if firstKey {
			minKey = key
			firstKey = false
		}
		maxKey = key

		currentBlock = append(currentBlock, KVPair{Key: key, Value: value})

		// Write block if it's full
		if len(currentBlock) >= restartInterval || blockBuffer.Len() > blockSize {
			blockData := encodeBlock(currentBlock)
			n, err := file.Write(blockData)
			if err != nil {
				return nil, err
			}

			indexEntries = append(indexEntries, IndexEntry{
				FirstKey: currentBlock[0].Key,
				Offset:   offset,
				Size:     int32(n),
			})

			offset += int64(n)
			currentBlock = nil
			blockBuffer.Reset()
		}

		// Add to bloom filter
		sst.bloomFilter.Add(key)
		sst.numKeys++
	}

	// Write final block
	if len(currentBlock) > 0 {
		blockData := encodeBlock(currentBlock)
		n, err := file.Write(blockData)
		if err != nil {
			return nil, err
		}

		indexEntries = append(indexEntries, IndexEntry{
			FirstKey: currentBlock[0].Key,
			Offset:   offset,
			Size:     int32(n),
		})
		offset += int64(n)
	}

	sst.minKey = minKey
	sst.maxKey = maxKey
	sst.index.entries = indexEntries

	// Write bloom filter
	bloomData := sst.bloomFilter.Serialize()
	bloomOffset := offset
	if _, err := file.Write(bloomData); err != nil {
		return nil, err
	}
	offset += int64(len(bloomData))

	// Write index at end of file
	if err := sst.writeIndexWithBloom(bloomOffset, int64(len(bloomData))); err != nil {
		return nil, err
	}

	return sst, file.Sync()
}

// KVPair represents a key-value pair
type KVPair struct {
	Key   [32]byte
	Value []byte
}

// encodeBlock encodes a block of KV pairs
func encodeBlock(pairs []KVPair) []byte {
	var buf bytes.Buffer

	// Write count
	binary.Write(&buf, binary.BigEndian, uint32(len(pairs)))

	// Write pairs
	for _, pair := range pairs {
		buf.Write(pair.Key[:])
		binary.Write(&buf, binary.BigEndian, uint32(len(pair.Value)))
		buf.Write(pair.Value)
	}

	return buf.Bytes()
}

// SSTable operations

func (s *SSTable) Get(key [32]byte) ([]byte, bool, error) {
	// Check bloom filter first for fast negative lookups
	if s.bloomFilter != nil && !s.bloomFilter.MayContain(key) {
		return nil, false, nil
	}

	// Binary search through index
	idx := sort.Search(len(s.index.entries), func(i int) bool {
		return bytes.Compare(s.index.entries[i].FirstKey[:], key[:]) > 0
	})

	if idx > 0 {
		idx--
	}

	// Read block
	entry := s.index.entries[idx]
	blockData := make([]byte, entry.Size)
	if _, err := s.file.ReadAt(blockData, entry.Offset); err != nil {
		return nil, false, err
	}

	// Decode block and search
	pairs := decodeBlock(blockData)
	for _, pair := range pairs {
		if pair.Key == key {
			return pair.Value, true, nil
		}
	}

	return nil, false, nil
}

// decodeBlock decodes a block of KV pairs
func decodeBlock(data []byte) []KVPair {
	buf := bytes.NewReader(data)

	var count uint32
	binary.Read(buf, binary.BigEndian, &count)

	pairs := make([]KVPair, count)
	for i := uint32(0); i < count; i++ {
		buf.Read(pairs[i].Key[:])

		var valueLen uint32
		binary.Read(buf, binary.BigEndian, &valueLen)

		pairs[i].Value = make([]byte, valueLen)
		buf.Read(pairs[i].Value)
	}

	return pairs
}

func (s *SSTable) writeIndex() error {
	return s.writeIndexWithBloom(0, 0)
}

func (s *SSTable) writeIndexWithBloom(bloomOffset, bloomSize int64) error {
	var buf bytes.Buffer

	// Write index entries
	binary.Write(&buf, binary.BigEndian, uint32(len(s.index.entries)))
	for _, entry := range s.index.entries {
		buf.Write(entry.FirstKey[:])
		binary.Write(&buf, binary.BigEndian, entry.Offset)
		binary.Write(&buf, binary.BigEndian, entry.Size)
	}

	// Write index offset at end
	indexOffset := s.getFileSize()
	if _, err := s.file.Write(buf.Bytes()); err != nil {
		return err
	}

	// Write metadata footer (24 bytes)
	// - Bloom filter offset (8 bytes)
	// - Bloom filter size (8 bytes)
	// - Index offset (8 bytes)
	binary.Write(s.file, binary.BigEndian, bloomOffset)
	binary.Write(s.file, binary.BigEndian, bloomSize)
	return binary.Write(s.file, binary.BigEndian, indexOffset)
}

func (s *SSTable) getFileSize() int64 {
	info, _ := s.file.Stat()
	return info.Size()
}

// Iteration support

type Iterator struct {
	tdkv       *TreeDKV
	memIter    *SkipListIterator
	sstIters   []*SSTableIterator
	currentKey [32]byte
	currentVal []byte
	heap       *IteratorHeap
	start      [32]byte
	end        [32]byte
	hasRange   bool
}

type IteratorOptions struct {
	Start [32]byte
	End   [32]byte
}

func (t *TreeDKV) NewIterator() *Iterator {
	return t.NewIteratorWithOptions(nil)
}

func (t *TreeDKV) NewIteratorWithOptions(opts *IteratorOptions) *Iterator {
	iter := &Iterator{
		tdkv: t,
		heap: NewIteratorHeap(),
	}

	if opts != nil {
		iter.start = opts.Start
		iter.end = opts.End
		iter.hasRange = true
	}

	// Create MemTable iterator
	t.memTableMu.RLock()
	iter.memIter = t.memTable.entries.Iterator()
	t.memTableMu.RUnlock()

	// Create SSTable iterators for all levels
	t.levelsMu.RLock()
	for level := 0; level < maxLevels; level++ {
		for _, sst := range t.levels[level] {
			if iter.hasRange {
				// Skip SSTables that don't overlap with range
				if bytes.Compare(sst.maxKey[:], iter.start[:]) < 0 ||
					bytes.Compare(sst.minKey[:], iter.end[:]) > 0 {
					continue
				}
			}
			sstIter := sst.NewIterator()
			if sstIter != nil {
				iter.sstIters = append(iter.sstIters, sstIter)
			}
		}
	}
	t.levelsMu.RUnlock()

	// Initialize heap with first entry from each iterator
	if iter.memIter.Next() {
		key, val, deleted := iter.memIter.Current()
		if !deleted && iter.keyInRange(key) {
			iter.heap.Push(&HeapEntry{
				Key:     key,
				Value:   val,
				Source:  0, // MemTable
				Deleted: deleted,
			})
		}
	}

	for i, sstIter := range iter.sstIters {
		if sstIter.Next() {
			key, val := sstIter.Current()
			if iter.keyInRange(key) {
				iter.heap.Push(&HeapEntry{
					Key:    key,
					Value:  val,
					Source: i + 1, // SSTable index + 1
				})
			}
		}
	}

	return iter
}

func (iter *Iterator) Next() bool {
	for iter.heap.Len() > 0 {
		entry := iter.heap.Pop()

		// Skip if we've seen this key before (deduplication)
		if bytes.Equal(entry.Key[:], iter.currentKey[:]) {
			// Advance the source iterator
			iter.advanceSource(entry.Source)
			continue
		}

		// Skip deleted entries
		if entry.Deleted {
			iter.advanceSource(entry.Source)
			continue
		}

		iter.currentKey = entry.Key
		iter.currentVal = entry.Value

		// Advance the source iterator and add next entry to heap
		iter.advanceSource(entry.Source)

		return true
	}

	return false
}

func (iter *Iterator) advanceSource(source int) {
	if source == 0 {
		// MemTable
		if iter.memIter.Next() {
			key, val, deleted := iter.memIter.Current()
			if iter.keyInRange(key) {
				iter.heap.Push(&HeapEntry{
					Key:     key,
					Value:   val,
					Source:  0,
					Deleted: deleted,
				})
			}
		}
	} else {
		// SSTable
		sstIdx := source - 1
		if sstIdx < len(iter.sstIters) {
			if iter.sstIters[sstIdx].Next() {
				key, val := iter.sstIters[sstIdx].Current()
				if iter.keyInRange(key) {
					iter.heap.Push(&HeapEntry{
						Key:    key,
						Value:  val,
						Source: source,
					})
				}
			}
		}
	}
}

func (iter *Iterator) keyInRange(key [32]byte) bool {
	if !iter.hasRange {
		return true
	}
	return bytes.Compare(key[:], iter.start[:]) >= 0 &&
		bytes.Compare(key[:], iter.end[:]) <= 0
}

func (iter *Iterator) Key() [32]byte {
	return iter.currentKey
}

func (iter *Iterator) Value() []byte {
	return iter.currentVal
}

func (iter *Iterator) Close() error {
	for _, sstIter := range iter.sstIters {
		if err := sstIter.Close(); err != nil {
			return err
		}
	}
	return nil
}

// MemTable Iterator
func (s *SkipList) Iterator() *SkipListIterator {
	return &SkipListIterator{
		list:    s,
		current: s.head,
	}
}

type SkipListIterator struct {
	list    *SkipList
	current *skipListNode
}

func (iter *SkipListIterator) Next() bool {
	if iter.current.next[0] != nil {
		iter.current = iter.current.next[0]
		return true
	}
	return false
}

func (iter *SkipListIterator) Current() ([32]byte, []byte, bool) {
	if iter.current != nil && iter.current != iter.list.head {
		return iter.current.key, iter.current.value, iter.current.deleted
	}
	return [32]byte{}, nil, false
}

// Helper functions

func (t *TreeDKV) keyInRange(key, min, max [32]byte) bool {
	return bytes.Compare(key[:], min[:]) >= 0 && bytes.Compare(key[:], max[:]) <= 0
}

func (t *TreeDKV) loadOrCreateManifest() error {
	manifestPath := filepath.Join(t.directory, manifestFile)

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Create new manifest
			t.manifest = &Manifest{
				Version:     1,
				Levels:      make([]LevelManifest, maxLevels),
				NextFileNum: 1,
			}
			return t.saveManifest()
		}
		return err
	}

	t.manifest = &Manifest{}
	return json.Unmarshal(data, t.manifest)
}

func (t *TreeDKV) saveManifest() error {
	t.manifestMu.Lock()
	defer t.manifestMu.Unlock()

	data, err := json.MarshalIndent(t.manifest, "", "  ")
	if err != nil {
		return err
	}

	manifestPath := filepath.Join(t.directory, manifestFile)
	return os.WriteFile(manifestPath, data, 0644)
}

func (t *TreeDKV) loadSSTables() error {
	// Load SSTable files based on manifest
	for level := 0; level < maxLevels; level++ {
		levelDir := filepath.Join(t.directory, fmt.Sprintf("level%d", level))
		if _, err := os.Stat(levelDir); os.IsNotExist(err) {
			continue
		}

		// Read SSTable files for this level
		// Implementation details...
	}
	return nil
}

func (t *TreeDKV) backgroundCompaction() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if !t.compacting.CompareAndSwap(false, true) {
			continue
		}

		// Check each level for compaction needs
		for level := 0; level < maxLevels-1; level++ {
			t.levelsMu.RLock()
			needsCompaction := len(t.levels[level]) > (4 << level) // Exponential growth
			t.levelsMu.RUnlock()

			if needsCompaction {
				t.compactLevel(level)
			}
		}

		t.compacting.Store(false)
	}
}

func (t *TreeDKV) compactLevel(level int) error {
	if level >= maxLevels-1 {
		return nil
	}

	t.levelsMu.Lock()
	// Get SSTables to compact
	toCompact := t.levels[level]
	if len(toCompact) < 2 {
		t.levelsMu.Unlock()
		return nil
	}

	// Get overlapping SSTables from next level
	var minKey, maxKey [32]byte
	for i, sst := range toCompact {
		if i == 0 || bytes.Compare(sst.minKey[:], minKey[:]) < 0 {
			minKey = sst.minKey
		}
		if i == 0 || bytes.Compare(sst.maxKey[:], maxKey[:]) > 0 {
			maxKey = sst.maxKey
		}
	}

	var overlapping []*SSTable
	for _, sst := range t.levels[level+1] {
		if t.rangesOverlap(minKey, maxKey, sst.minKey, sst.maxKey) {
			overlapping = append(overlapping, sst)
		}
	}
	t.levelsMu.Unlock()

	// Merge all SSTables
	merged, err := t.mergeSSTables(append(toCompact, overlapping...), level+1)
	if err != nil {
		return err
	}

	// Update levels atomically
	t.levelsMu.Lock()
	// Remove compacted files from current level
	newLevel := make([]*SSTable, 0)
	for _, sst := range t.levels[level] {
		compacted := false
		for _, c := range toCompact {
			if sst.path == c.path {
				compacted = true
				break
			}
		}
		if !compacted {
			newLevel = append(newLevel, sst)
		}
	}
	t.levels[level] = newLevel

	// Remove overlapping files from next level and add merged
	newNextLevel := make([]*SSTable, 0)
	for _, sst := range t.levels[level+1] {
		overlapped := false
		for _, o := range overlapping {
			if sst.path == o.path {
				overlapped = true
				break
			}
		}
		if !overlapped {
			newNextLevel = append(newNextLevel, sst)
		}
	}
	t.levels[level+1] = append(newNextLevel, merged...)
	t.levelsMu.Unlock()

	// Clean up old files
	for _, sst := range toCompact {
		sst.file.Close()
		os.Remove(sst.path)
	}
	for _, sst := range overlapping {
		sst.file.Close()
		os.Remove(sst.path)
	}

	t.stats.Compactions.Add(1)
	return t.saveManifest()
}

func (t *TreeDKV) rangesOverlap(min1, max1, min2, max2 [32]byte) bool {
	return bytes.Compare(min1[:], max2[:]) <= 0 && bytes.Compare(max1[:], min2[:]) >= 0
}

func (t *TreeDKV) mergeSSTables(tables []*SSTable, targetLevel int) ([]*SSTable, error) {
	// Create iterators for all tables
	var iters []*SSTableIterator
	for _, sst := range tables {
		iter := sst.NewIterator()
		iters = append(iters, iter)
	}

	// Use a heap to merge in sorted order
	heap := NewIteratorHeap()
	for i, iter := range iters {
		if iter.Next() {
			key, val := iter.Current()
			heap.Push(&HeapEntry{
				Key:    key,
				Value:  val,
				Source: i,
			})
		}
	}

	// Write merged data to new SSTables
	var result []*SSTable
	var currentKVs []KVPair
	var currentSize int64

	for heap.Len() > 0 {
		entry := heap.Pop()

		// Skip duplicates (keep most recent)
		if len(currentKVs) > 0 && currentKVs[len(currentKVs)-1].Key == entry.Key {
			// Advance source iterator
			if iters[entry.Source].Next() {
				key, val := iters[entry.Source].Current()
				heap.Push(&HeapEntry{
					Key:    key,
					Value:  val,
					Source: entry.Source,
				})
			}
			continue
		}

		currentKVs = append(currentKVs, KVPair{Key: entry.Key, Value: entry.Value})
		currentSize += int64(32 + len(entry.Value))

		// Advance source iterator
		if iters[entry.Source].Next() {
			key, val := iters[entry.Source].Current()
			heap.Push(&HeapEntry{
				Key:    key,
				Value:  val,
				Source: entry.Source,
			})
		}

		// Write SSTable if it's large enough
		if currentSize > memTableSize || len(currentKVs) > maxKeysPerLeaf {
			sst, err := t.writeSSTableFromKVs(currentKVs, targetLevel)
			if err != nil {
				return nil, err
			}
			result = append(result, sst)
			currentKVs = nil
			currentSize = 0
		}
	}

	// Write remaining data
	if len(currentKVs) > 0 {
		sst, err := t.writeSSTableFromKVs(currentKVs, targetLevel)
		if err != nil {
			return nil, err
		}
		result = append(result, sst)
	}

	// Close iterators
	for _, iter := range iters {
		iter.Close()
	}

	return result, nil
}

func (t *TreeDKV) writeSSTableFromKVs(kvs []KVPair, level int) (*SSTable, error) {
	// Generate filename
	t.manifestMu.Lock()
	fileNum := t.manifest.NextFileNum
	t.manifest.NextFileNum++
	t.manifestMu.Unlock()

	filename := fmt.Sprintf("%s%d_%d.sst", sstablePrefix, level, fileNum)
	path := filepath.Join(t.directory, fmt.Sprintf("level%d", level), filename)

	// Create directory if needed
	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return nil, err
	}

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Create bloom filter
	bloomFilter := NewBloomFilter(len(kvs), 0.01)

	sst := &SSTable{
		file:        file,
		path:        path,
		index:       &SSTableIndex{},
		bloomFilter: bloomFilter,
		minKey:      kvs[0].Key,
		maxKey:      kvs[len(kvs)-1].Key,
		numKeys:     len(kvs),
	}

	// Write blocks
	var indexEntries []IndexEntry
	offset := int64(0)

	for i := 0; i < len(kvs); i += restartInterval {
		end := i + restartInterval
		if end > len(kvs) {
			end = len(kvs)
		}

		block := kvs[i:end]
		blockData := encodeBlock(block)

		n, err := file.Write(blockData)
		if err != nil {
			return nil, err
		}

		// Add to bloom filter
		for _, kv := range block {
			bloomFilter.Add(kv.Key)
		}

		indexEntries = append(indexEntries, IndexEntry{
			FirstKey: block[0].Key,
			Offset:   offset,
			Size:     int32(n),
		})

		offset += int64(n)
	}

	sst.index.entries = indexEntries

	// Write bloom filter
	bloomData := bloomFilter.Serialize()
	bloomOffset := offset
	if _, err := file.Write(bloomData); err != nil {
		return nil, err
	}
	offset += int64(len(bloomData))

	// Write index
	if err := sst.writeIndexWithBloom(bloomOffset, int64(len(bloomData))); err != nil {
		return nil, err
	}

	return sst, file.Sync()
}

// Close closes the TreeDKV store
// SSTableIterator provides iteration over an SSTable
type SSTableIterator struct {
	sst         *SSTable
	blockIdx    int
	pairIdx     int
	currentBlock []KVPair
}

func (s *SSTable) NewIterator() *SSTableIterator {
	return &SSTableIterator{
		sst:      s,
		blockIdx: -1,
		pairIdx:  -1,
	}
}

func (iter *SSTableIterator) Next() bool {
	// Move to next pair in current block
	iter.pairIdx++
	if iter.pairIdx < len(iter.currentBlock) {
		return true
	}

	// Need to load next block
	iter.blockIdx++
	if iter.blockIdx >= len(iter.sst.index.entries) {
		return false
	}

	// Load block
	entry := iter.sst.index.entries[iter.blockIdx]
	blockData := make([]byte, entry.Size)
	if _, err := iter.sst.file.ReadAt(blockData, entry.Offset); err != nil {
		return false
	}

	iter.currentBlock = decodeBlock(blockData)
	iter.pairIdx = 0

	return len(iter.currentBlock) > 0
}

func (iter *SSTableIterator) Current() ([32]byte, []byte) {
	if iter.pairIdx >= 0 && iter.pairIdx < len(iter.currentBlock) {
		pair := iter.currentBlock[iter.pairIdx]
		return pair.Key, pair.Value
	}
	return [32]byte{}, nil
}

func (iter *SSTableIterator) Close() error {
	return nil
}

// IteratorHeap is a min-heap for merge iteration
type IteratorHeap struct {
	entries []*HeapEntry
}

type HeapEntry struct {
	Key     [32]byte
	Value   []byte
	Source  int
	Deleted bool
}

func NewIteratorHeap() *IteratorHeap {
	return &IteratorHeap{
		entries: make([]*HeapEntry, 0),
	}
}

func (h *IteratorHeap) Len() int {
	return len(h.entries)
}

func (h *IteratorHeap) Push(entry *HeapEntry) {
	h.entries = append(h.entries, entry)
	h.up(len(h.entries) - 1)
}

func (h *IteratorHeap) Pop() *HeapEntry {
	if len(h.entries) == 0 {
		return nil
	}

	result := h.entries[0]
	last := len(h.entries) - 1
	h.entries[0] = h.entries[last]
	h.entries = h.entries[:last]

	if len(h.entries) > 0 {
		h.down(0)
	}

	return result
}

func (h *IteratorHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if bytes.Compare(h.entries[i].Key[:], h.entries[parent].Key[:]) >= 0 {
			break
		}
		h.entries[i], h.entries[parent] = h.entries[parent], h.entries[i]
		i = parent
	}
}

func (h *IteratorHeap) down(i int) {
	for {
		left := 2*i + 1
		right := 2*i + 2
		smallest := i

		if left < len(h.entries) && bytes.Compare(h.entries[left].Key[:], h.entries[smallest].Key[:]) < 0 {
			smallest = left
		}
		if right < len(h.entries) && bytes.Compare(h.entries[right].Key[:], h.entries[smallest].Key[:]) < 0 {
			smallest = right
		}

		if smallest == i {
			break
		}

		h.entries[i], h.entries[smallest] = h.entries[smallest], h.entries[i]
		i = smallest
	}
}

// Range performs a range query
func (t *TreeDKV) Range(start, end [32]byte) ([]KVPair, error) {
	var results []KVPair

	iter := t.NewIteratorWithOptions(&IteratorOptions{
		Start: start,
		End:   end,
	})
	defer iter.Close()

	for iter.Next() {
		results = append(results, KVPair{
			Key:   iter.Key(),
			Value: iter.Value(),
		})
	}

	return results, nil
}

// BatchPut performs batch write operations
func (t *TreeDKV) BatchPut(pairs []KVPair) error {
	for _, pair := range pairs {
		if err := t.Put(pair.Key, pair.Value); err != nil {
			return err
		}
	}
	return nil
}

func (t *TreeDKV) Close() error {
	// Flush MemTable
	if err := t.flushMemTable(); err != nil {
		return err
	}

	// Close all SSTable files
	t.levelsMu.Lock()
	defer t.levelsMu.Unlock()

	for level := 0; level < maxLevels; level++ {
		for _, sst := range t.levels[level] {
			if err := sst.file.Close(); err != nil {
				return err
			}
		}
	}

	return t.saveManifest()
}