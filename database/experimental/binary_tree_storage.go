package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LeafMaxKeys = 100_000 // Maximum keys per leaf before split
	KeyEntrySize = 48     // 32 byte key + 8 byte offset + 8 byte length
)

// BinaryNode represents a node in the binary tree
type BinaryNode struct {
	mu       sync.RWMutex
	IsLeaf   bool
	Level    int // Depth in tree (0 = root)
	PathBits uint32 // Bits used to reach this node (for leaf naming)

	// For internal nodes
	Left  *BinaryNode
	Right *BinaryNode

	// For leaf nodes
	LeafID    uint32       // Unique leaf identifier
	Keys      uint32       // Number of keys in this leaf
	LeafFile  string       // Path to leaf file
	IsFull    bool         // True when leaf has reached capacity
	WriteLock sync.Mutex   // Lock for writing to leaf file
}

// BinaryTreeStorage implements efficient binary tree based storage
type BinaryTreeStorage struct {
	mu        sync.RWMutex
	Directory string
	Root      *BinaryNode

	// Leaf management
	NextLeafID   atomic.Uint32
	TotalKeys    atomic.Uint64
	ActiveLeaves atomic.Uint32
	TotalLeaves  atomic.Uint32

	// Write buffering
	writeBuffer chan *KeyWrite
	flushSignal chan struct{}
	wg          sync.WaitGroup

	// Read caching
	leafCache map[string][][]byte // Maps leaf file path to cached entries
}

// KeyWrite represents a key write operation
type KeyWrite struct {
	Key    [32]byte
	Offset uint64
	Length uint64
}

// NewBinaryTreeStorage creates a new binary tree storage system
func NewBinaryTreeStorage(directory string) (*BinaryTreeStorage, error) {
	os.RemoveAll(directory)
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	bts := &BinaryTreeStorage{
		Directory:   directory,
		writeBuffer: make(chan *KeyWrite, 10000),
		flushSignal: make(chan struct{}, 1),
	}

	// Start with a single leaf as root
	leafID := bts.NextLeafID.Add(1)
	leafFile := filepath.Join(directory, fmt.Sprintf("leaf_%08x.dat", leafID))

	bts.Root = &BinaryNode{
		IsLeaf:   true,
		Level:    0,
		PathBits: 0,
		LeafID:   leafID,
		Keys:     0,
		LeafFile: leafFile,
		IsFull:   false,
	}

	if err := bts.createLeafFile(leafFile); err != nil {
		return nil, err
	}

	bts.ActiveLeaves.Store(1)
	bts.TotalLeaves.Store(1)

	// Start background writer
	bts.wg.Add(1)
	go bts.backgroundWriter()

	return bts, nil
}

// createLeafFile creates a new leaf file with header
func (bts *BinaryTreeStorage) createLeafFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header: [4 bytes: key count][4 bytes: reserved][56 bytes: reserved]
	header := make([]byte, 64)
	_, err = file.Write(header)
	return err
}

// findLeaf navigates the tree using hash bits to find the appropriate leaf
func (bts *BinaryTreeStorage) findLeaf(key [32]byte) (*BinaryNode, error) {
	bts.mu.RLock()
	node := bts.Root
	bts.mu.RUnlock()

	// Navigate using bits from MSB
	level := 0
	for {
		node.mu.RLock()

		if node.IsLeaf {
			if !node.IsFull {
				node.mu.RUnlock()
				return node, nil
			}

			// Leaf is full, needs splitting
			node.mu.RUnlock()
			return bts.splitLeaf(node, key)
		}

		// Navigate based on bit at current level
		bit := getBit(key, level)
		var next *BinaryNode
		if bit == 0 {
			next = node.Left
		} else {
			next = node.Right
		}
		node.mu.RUnlock()

		if next == nil {
			// Should not happen after proper initialization
			return nil, fmt.Errorf("missing child node at level %d", level)
		}

		node = next
		level++
	}
}

// getBit returns the bit at position 'pos' from MSB
func getBit(key [32]byte, pos int) byte {
	byteIndex := pos / 8
	bitIndex := uint(7 - (pos % 8)) // MSB first
	if byteIndex >= len(key) {
		return 0
	}
	return (key[byteIndex] >> bitIndex) & 1
}

// splitLeaf converts a full leaf into an internal node with two leaf children
func (bts *BinaryTreeStorage) splitLeaf(leaf *BinaryNode, triggerKey [32]byte) (*BinaryNode, error) {
	leaf.mu.Lock()
	defer leaf.mu.Unlock()

	// Double-check leaf is still full
	if !leaf.IsFull {
		return leaf, nil
	}

	// Convert leaf to internal node
	level := leaf.Level
	leaf.IsLeaf = false
	leaf.IsFull = false

	// Create two new leaf nodes
	leftID := bts.NextLeafID.Add(1)
	leftFile := filepath.Join(bts.Directory, fmt.Sprintf("leaf_%08x.dat", leftID))

	rightID := bts.NextLeafID.Add(1)
	rightFile := filepath.Join(bts.Directory, fmt.Sprintf("leaf_%08x.dat", rightID))

	leaf.Left = &BinaryNode{
		IsLeaf:   true,
		Level:    level + 1,
		PathBits: leaf.PathBits << 1, // Add 0 bit
		LeafID:   leftID,
		Keys:     0,
		LeafFile: leftFile,
		IsFull:   false,
	}

	leaf.Right = &BinaryNode{
		IsLeaf:   true,
		Level:    level + 1,
		PathBits: (leaf.PathBits << 1) | 1, // Add 1 bit
		LeafID:   rightID,
		Keys:     0,
		LeafFile: rightFile,
		IsFull:   false,
	}

	// Create the new leaf files
	if err := bts.createLeafFile(leftFile); err != nil {
		return nil, err
	}
	if err := bts.createLeafFile(rightFile); err != nil {
		return nil, err
	}

	bts.TotalLeaves.Add(2)
	bts.ActiveLeaves.Add(1) // Net +1 (removed 1 full, added 2 empty)

	// TODO: Redistribute keys from old leaf file to new leaves
	// For now, mark old leaf file for redistribution

	// Return the appropriate new leaf for the trigger key
	bit := getBit(triggerKey, level)
	if bit == 0 {
		return leaf.Left, nil
	}
	return leaf.Right, nil
}

// AddKey adds a key to the storage
func (bts *BinaryTreeStorage) AddKey(key [32]byte, offset, length uint64) error {
	write := &KeyWrite{
		Key:    key,
		Offset: offset,
		Length: length,
	}

	select {
	case bts.writeBuffer <- write:
		return nil
	default:
		// Buffer full, trigger flush
		select {
		case bts.flushSignal <- struct{}{}:
		default:
		}
		bts.writeBuffer <- write
		return nil
	}
}

// backgroundWriter processes write batches
func (bts *BinaryTreeStorage) backgroundWriter() {
	defer bts.wg.Done()

	batch := make([]*KeyWrite, 0, 1000)
	ticker := time.NewTicker(100 * time.Millisecond) // Auto-flush every 100ms
	defer ticker.Stop()

	for {
		select {
		case write, ok := <-bts.writeBuffer:
			if !ok {
				if len(batch) > 0 {
					bts.processBatch(batch)
				}
				return
			}

			batch = append(batch, write)
			if len(batch) >= 1000 {
				bts.processBatch(batch)
				batch = make([]*KeyWrite, 0, 1000)
			}

		case <-bts.flushSignal:
			if len(batch) > 0 {
				bts.processBatch(batch)
				batch = make([]*KeyWrite, 0, 1000)
			}

		case <-ticker.C:
			// Periodic flush
			if len(batch) > 0 {
				bts.processBatch(batch)
				batch = make([]*KeyWrite, 0, 1000)
			}
		}
	}
}

// processBatch writes a batch of keys to appropriate leaves
func (bts *BinaryTreeStorage) processBatch(batch []*KeyWrite) {
	// Group by leaf
	leafGroups := make(map[*BinaryNode][]*KeyWrite)

	for _, write := range batch {
		leaf, err := bts.findLeaf(write.Key)
		if err != nil {
			fmt.Printf("Error finding leaf: %v\n", err)
			continue
		}
		leafGroups[leaf] = append(leafGroups[leaf], write)
	}

	// Write to each leaf
	var wg sync.WaitGroup
	for leaf, writes := range leafGroups {
		wg.Add(1)
		go func(l *BinaryNode, w []*KeyWrite) {
			defer wg.Done()
			if err := bts.writeToLeaf(l, w); err != nil {
				fmt.Printf("Error writing to leaf: %v\n", err)
			}
		}(leaf, writes)
	}
	wg.Wait()
}

// writeToLeaf writes keys to a leaf file
func (bts *BinaryTreeStorage) writeToLeaf(leaf *BinaryNode, writes []*KeyWrite) error {
	leaf.WriteLock.Lock()
	defer leaf.WriteLock.Unlock()

	// Open file for read/write
	file, err := os.OpenFile(leaf.LeafFile, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read header
	header := make([]byte, 64)
	if _, err := file.Read(header); err != nil {
		return err
	}

	keyCount := binary.BigEndian.Uint32(header[0:4])

	// Seek to end for appending
	if _, err := file.Seek(0, 2); err != nil {
		return err
	}

	// Write entries
	entry := make([]byte, KeyEntrySize)
	for _, write := range writes {
		copy(entry[0:32], write.Key[:])
		binary.BigEndian.PutUint64(entry[32:40], write.Offset)
		binary.BigEndian.PutUint64(entry[40:48], write.Length)

		if _, err := file.Write(entry); err != nil {
			return err
		}

		keyCount++
		bts.TotalKeys.Add(1)
	}

	// Update header
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(header[0:4], keyCount)
	if _, err := file.Write(header[:4]); err != nil {
		return err
	}

	// Update leaf state
	atomic.StoreUint32(&leaf.Keys, keyCount)
	if keyCount >= LeafMaxKeys {
		leaf.mu.Lock()
		leaf.IsFull = true
		leaf.mu.Unlock()
		bts.ActiveLeaves.Add(^uint32(0)) // Decrement
	}

	return nil
}

// Get retrieves a key from storage
func (bts *BinaryTreeStorage) Get(key [32]byte) (offset, length uint64, err error) {
	// Navigate to the correct leaf
	bts.mu.RLock()
	node := bts.Root
	bts.mu.RUnlock()

	level := 0
	for {
		node.mu.RLock()

		if node.IsLeaf {
			leafFile := node.LeafFile
			node.mu.RUnlock()
			return bts.searchLeaf(leafFile, key)
		}

		// Navigate based on bit
		bit := getBit(key, level)
		if bit == 0 {
			next := node.Left
			node.mu.RUnlock()
			if next == nil {
				return 0, 0, fmt.Errorf("key not found")
			}
			node = next
		} else {
			next := node.Right
			node.mu.RUnlock()
			if next == nil {
				return 0, 0, fmt.Errorf("key not found")
			}
			node = next
		}
		level++
	}
}

// searchLeaf searches for a key in a leaf file
func (bts *BinaryTreeStorage) searchLeaf(leafFile string, key [32]byte) (offset, length uint64, err error) {
	// Check cache first
	bts.mu.Lock()
	if bts.leafCache == nil {
		bts.leafCache = make(map[string][][]byte)
	}

	entries, cached := bts.leafCache[leafFile]
	if !cached {
		// Read from disk
		file, err := os.Open(leafFile)
		if err != nil {
			bts.mu.Unlock()
			return 0, 0, fmt.Errorf("key not found")
		}
		defer file.Close()

		// Read header
		header := make([]byte, 64)
		if _, err := file.Read(header); err != nil {
			bts.mu.Unlock()
			return 0, 0, err
		}

		keyCount := binary.BigEndian.Uint32(header[0:4])

		// Read all entries at once
		entries = make([][]byte, keyCount)
		for i := uint32(0); i < keyCount; i++ {
			entry := make([]byte, KeyEntrySize)
			if _, err := file.Read(entry); err != nil {
				bts.mu.Unlock()
				return 0, 0, err
			}
			entries[i] = entry
		}

		// Cache for future reads (limit cache size)
		if len(bts.leafCache) < 100 {
			bts.leafCache[leafFile] = entries
		}
	}
	bts.mu.Unlock()

	// Binary search through entries
	left, right := 0, len(entries)-1
	for left <= right {
		mid := (left + right) / 2
		cmp := bytes.Compare(entries[mid][0:32], key[:])

		if cmp == 0 {
			// Found it
			offset = binary.BigEndian.Uint64(entries[mid][32:40])
			length = binary.BigEndian.Uint64(entries[mid][40:48])
			return offset, length, nil
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	// Fallback to linear search if not sorted properly
	for _, entry := range entries {
		if bytes.Equal(entry[0:32], key[:]) {
			offset = binary.BigEndian.Uint64(entry[32:40])
			length = binary.BigEndian.Uint64(entry[40:48])
			return offset, length, nil
		}
	}

	return 0, 0, fmt.Errorf("key not found")
}

// Close shuts down the storage system
func (bts *BinaryTreeStorage) Close() error {
	close(bts.writeBuffer)
	bts.wg.Wait()
	return nil
}

// Stats returns storage statistics
func (bts *BinaryTreeStorage) Stats() string {
	return fmt.Sprintf("Keys: %d, Leaves: %d (Active: %d)",
		bts.TotalKeys.Load(),
		bts.TotalLeaves.Load(),
		bts.ActiveLeaves.Load())
}