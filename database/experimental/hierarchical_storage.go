package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	LeafCapacity = 100_000 // Fixed number of keys per leaf
	BranchFactor = 256     // Number of children per internal node (using 1 byte of hash)
	DBKeySize    = 48      // Size of each key entry (32 byte key + 8 byte offset + 8 byte length)
)

// NodeType represents the type of node in the tree
type NodeType int

const (
	InternalNode NodeType = iota
	LeafNode
)

// TreeNode represents a node in the hierarchical storage tree
type TreeNode struct {
	Type     NodeType
	Level    int    // Depth in the tree (0 = root)
	Path     []byte // Path from root to this node (hash prefix bytes)
	Parent   *TreeNode
	Children [BranchFactor]*TreeNode // For internal nodes

	// For leaf nodes
	LeafFile   string       // Path to the leaf file on disk
	KeyCount   uint32       // Number of keys in this leaf
	IsFull     bool         // Whether this leaf has reached capacity
	WriteLock  sync.Mutex   // Lock for writing to this leaf

	// Statistics
	TotalKeys  uint64       // Total keys in this subtree
}

// HierarchicalStorage implements a tree-based storage system with fixed-size leaves
type HierarchicalStorage struct {
	mu        sync.RWMutex
	Directory string
	Root      *TreeNode

	// In-memory index of all active (non-full) leaves
	ActiveLeaves map[string]*TreeNode // Path string -> leaf node

	// Statistics
	TotalKeys    atomic.Uint64
	TotalLeaves  atomic.Uint32
	ActiveCount  atomic.Uint32

	// Write buffer for batching
	writeBuffer  chan *KeyEntry
	flushSignal  chan struct{}
	wg          sync.WaitGroup
}

// KeyEntry represents a key to be stored
type KeyEntry struct {
	Key    [32]byte
	Offset uint64
	Length uint64
}

// NewHierarchicalStorage creates a new hierarchical storage system
func NewHierarchicalStorage(directory string) (*HierarchicalStorage, error) {
	// Remove existing directory and create fresh
	os.RemoveAll(directory)
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	hs := &HierarchicalStorage{
		Directory:    directory,
		ActiveLeaves: make(map[string]*TreeNode),
		writeBuffer:  make(chan *KeyEntry, 10000),
		flushSignal:  make(chan struct{}, 1),
	}

	// Create root node
	hs.Root = &TreeNode{
		Type:     InternalNode,
		Level:    0,
		Path:     []byte{},
		Children: [BranchFactor]*TreeNode{},
	}

	// Start background writer
	hs.wg.Add(1)
	go hs.backgroundWriter()

	return hs, nil
}

// ComputePath computes the deterministic path for a key through the tree
func (hs *HierarchicalStorage) ComputePath(key [32]byte) []byte {
	// Use the key bytes directly as the path
	// Each level uses one byte of the hash
	return key[:]
}

// FindOrCreateLeaf finds the appropriate leaf for a key, creating nodes as needed
func (hs *HierarchicalStorage) FindOrCreateLeaf(key [32]byte) (*TreeNode, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	path := hs.ComputePath(key)
	current := hs.Root

	// Traverse down the tree, creating nodes as needed
	for level := 0; ; level++ {
		if level >= len(path) {
			// Shouldn't happen with 32-byte keys
			return nil, fmt.Errorf("path exhausted at level %d", level)
		}

		childIndex := path[level]
		child := current.Children[childIndex]

		if child == nil {
			// Need to create a new node
			// Decide if it should be a leaf or internal node based on depth and key distribution
			// For now, create leaves at level 2 (using 2 bytes of hash = 65536 possible leaves)
			if level >= 1 {
				// Create a leaf node
				leafPath := path[:level+1]
				leafPathStr := string(leafPath)

				// Create the leaf file path
				leafFile := filepath.Join(hs.Directory, fmt.Sprintf("leaf_%x.dat", leafPath))

				child = &TreeNode{
					Type:      LeafNode,
					Level:     level + 1,
					Path:      leafPath,
					Parent:    current,
					LeafFile:  leafFile,
					KeyCount:  0,
					IsFull:    false,
				}

				// Add to active leaves index
				hs.ActiveLeaves[leafPathStr] = child
				hs.ActiveCount.Add(1)
				hs.TotalLeaves.Add(1)

				// Create the leaf file
				if err := hs.createLeafFile(leafFile); err != nil {
					return nil, err
				}
			} else {
				// Create an internal node
				child = &TreeNode{
					Type:     InternalNode,
					Level:    level + 1,
					Path:     path[:level+1],
					Parent:   current,
					Children: [BranchFactor]*TreeNode{},
				}
			}

			current.Children[childIndex] = child
		}

		if child.Type == LeafNode {
			if child.IsFull {
				// This leaf is full, need to split or go deeper
				return hs.handleFullLeaf(child, key)
			}
			return child, nil
		}

		current = child
	}
}

// handleFullLeaf handles the case when a leaf is full
func (hs *HierarchicalStorage) handleFullLeaf(leaf *TreeNode, key [32]byte) (*TreeNode, error) {
	// Convert the leaf to an internal node and create new leaves beneath it
	parent := leaf.Parent
	if parent == nil {
		return nil, fmt.Errorf("cannot split root leaf")
	}

	// Remove from active leaves
	delete(hs.ActiveLeaves, string(leaf.Path))
	hs.ActiveCount.Add(^uint32(0)) // Decrement

	// Create a new internal node to replace the leaf
	newInternal := &TreeNode{
		Type:     InternalNode,
		Level:    leaf.Level,
		Path:     leaf.Path,
		Parent:   parent,
		Children: [BranchFactor]*TreeNode{},
	}

	// Update parent's child pointer
	childIndex := leaf.Path[len(leaf.Path)-1]
	parent.Children[childIndex] = newInternal

	// Now find/create a new leaf under this internal node
	path := hs.ComputePath(key)
	nextIndex := path[newInternal.Level]

	// Create new leaf
	leafPath := append(append([]byte{}, newInternal.Path...), nextIndex)
	leafPathStr := string(leafPath)
	leafFile := filepath.Join(hs.Directory, fmt.Sprintf("leaf_%x.dat", leafPath))

	newLeaf := &TreeNode{
		Type:      LeafNode,
		Level:     newInternal.Level + 1,
		Path:      leafPath,
		Parent:    newInternal,
		LeafFile:  leafFile,
		KeyCount:  0,
		IsFull:    false,
	}

	newInternal.Children[nextIndex] = newLeaf
	hs.ActiveLeaves[leafPathStr] = newLeaf
	hs.ActiveCount.Add(1)
	hs.TotalLeaves.Add(1)

	// Create the leaf file
	if err := hs.createLeafFile(leafFile); err != nil {
		return nil, err
	}

	// TODO: Redistribute keys from the old full leaf to new leaves
	// For now, we just mark the old leaf as archived

	return newLeaf, nil
}

// createLeafFile creates a new empty leaf file
func (hs *HierarchicalStorage) createLeafFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header (placeholder for now)
	header := make([]byte, 64)
	binary.BigEndian.PutUint32(header[0:4], 0) // Key count
	_, err = file.Write(header)
	return err
}

// AddKey adds a key to the storage system
func (hs *HierarchicalStorage) AddKey(key [32]byte, offset, length uint64) error {
	entry := &KeyEntry{
		Key:    key,
		Offset: offset,
		Length: length,
	}

	// Send to write buffer
	select {
	case hs.writeBuffer <- entry:
		return nil
	default:
		// Buffer full, trigger flush and retry
		select {
		case hs.flushSignal <- struct{}{}:
		default:
		}
		hs.writeBuffer <- entry
		return nil
	}
}

// backgroundWriter handles batched writes to leaf files
func (hs *HierarchicalStorage) backgroundWriter() {
	defer hs.wg.Done()

	batch := make([]*KeyEntry, 0, 1000)

	for {
		select {
		case entry, ok := <-hs.writeBuffer:
			if !ok {
				// Channel closed, flush remaining batch and exit
				if len(batch) > 0 {
					hs.flushBatch(batch)
				}
				return
			}

			batch = append(batch, entry)
			if len(batch) >= 1000 {
				hs.flushBatch(batch)
				batch = make([]*KeyEntry, 0, 1000)
			}

		case <-hs.flushSignal:
			if len(batch) > 0 {
				hs.flushBatch(batch)
				batch = make([]*KeyEntry, 0, 1000)
			}
		}
	}
}

// flushBatch writes a batch of entries to their respective leaf files
func (hs *HierarchicalStorage) flushBatch(batch []*KeyEntry) {
	// Group entries by leaf
	leafGroups := make(map[*TreeNode][]*KeyEntry)

	for _, entry := range batch {
		leaf, err := hs.FindOrCreateLeaf(entry.Key)
		if err != nil {
			fmt.Printf("Error finding leaf for key: %v\n", err)
			continue
		}
		leafGroups[leaf] = append(leafGroups[leaf], entry)
	}

	// Write to each leaf
	for leaf, entries := range leafGroups {
		hs.writeToLeaf(leaf, entries)
	}
}

// writeToLeaf writes entries to a specific leaf file
func (hs *HierarchicalStorage) writeToLeaf(leaf *TreeNode, entries []*KeyEntry) error {
	leaf.WriteLock.Lock()
	defer leaf.WriteLock.Unlock()

	// Open the leaf file for appending
	file, err := os.OpenFile(leaf.LeafFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read current key count from header
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
	for _, entry := range entries {
		data := make([]byte, DBKeySize)
		copy(data[0:32], entry.Key[:])
		binary.BigEndian.PutUint64(data[32:40], entry.Offset)
		binary.BigEndian.PutUint64(data[40:48], entry.Length)

		if _, err := file.Write(data); err != nil {
			return err
		}

		keyCount++
		atomic.AddUint32(&leaf.KeyCount, 1)
		hs.TotalKeys.Add(1)
	}

	// Update header with new key count
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(header[0:4], keyCount)
	if _, err := file.Write(header); err != nil {
		return err
	}

	// Check if leaf is now full
	if keyCount >= LeafCapacity {
		leaf.IsFull = true
		hs.mu.Lock()
		delete(hs.ActiveLeaves, string(leaf.Path))
		hs.ActiveCount.Add(^uint32(0)) // Decrement
		hs.mu.Unlock()
	}

	return nil
}

// Get retrieves a key from the storage system
func (hs *HierarchicalStorage) Get(key [32]byte) (offset, length uint64, err error) {
	// Find the leaf that should contain this key
	hs.mu.RLock()
	path := hs.ComputePath(key)
	current := hs.Root

	// Traverse down to the leaf
	for level := 0; current != nil && current.Type == InternalNode; level++ {
		if level >= len(path) {
			hs.mu.RUnlock()
			return 0, 0, fmt.Errorf("key not found")
		}

		childIndex := path[level]
		current = current.Children[childIndex]
	}

	hs.mu.RUnlock()

	if current == nil || current.Type != LeafNode {
		return 0, 0, fmt.Errorf("key not found")
	}

	// Read the leaf file to find the key
	return hs.searchLeafFile(current.LeafFile, key)
}

// searchLeafFile searches for a key in a leaf file
func (hs *HierarchicalStorage) searchLeafFile(leafFile string, key [32]byte) (offset, length uint64, err error) {
	file, err := os.Open(leafFile)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, fmt.Errorf("key not found")
		}
		return 0, 0, err
	}
	defer file.Close()

	// Read header
	header := make([]byte, 64)
	if _, err := file.Read(header); err != nil {
		return 0, 0, err
	}

	keyCount := binary.BigEndian.Uint32(header[0:4])

	// Binary search through the sorted keys
	// For now, do a linear search (can optimize later)
	entry := make([]byte, DBKeySize)
	for i := uint32(0); i < keyCount; i++ {
		if _, err := file.Read(entry); err != nil {
			return 0, 0, err
		}

		if string(entry[0:32]) == string(key[:]) {
			offset = binary.BigEndian.Uint64(entry[32:40])
			length = binary.BigEndian.Uint64(entry[40:48])
			return offset, length, nil
		}
	}

	return 0, 0, fmt.Errorf("key not found")
}

// Close closes the hierarchical storage system
func (hs *HierarchicalStorage) Close() error {
	close(hs.writeBuffer)
	hs.wg.Wait()
	return nil
}

// Stats returns statistics about the storage system
func (hs *HierarchicalStorage) Stats() string {
	return fmt.Sprintf("Total Keys: %d, Total Leaves: %d, Active Leaves: %d",
		hs.TotalKeys.Load(), hs.TotalLeaves.Load(), hs.ActiveCount.Load())
}