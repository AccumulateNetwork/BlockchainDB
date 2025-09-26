package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LeafSplitSize = 64 * 1024  // Split leaves at 64KB
	KeyEntrySizeV2 = 48        // 32 byte key + 8 byte offset + 8 byte length
	HeaderSize    = 64          // File header size
	SplitDepth    = 2           // Split 2 levels deep (creates 4 leaves)
)

// BinaryNodeV2 represents a node in the binary tree
type BinaryNodeV2 struct {
	mu       sync.RWMutex
	IsLeaf   bool
	Level    int    // Depth in tree (bits consumed)
	Prefix   uint64 // The bit prefix to reach this node

	// For internal nodes
	Left  *BinaryNodeV2
	Right *BinaryNodeV2

	// For leaf nodes
	LeafID       uint32
	LeafFile     string
	FileSize     int64        // Current file size
	KeyCount     uint32       // Number of keys in this leaf
	NeedsSplit   bool         // Flag when file exceeds split size
	WriteLock    sync.Mutex   // Lock for writing to this leaf
}

// BinaryTreeStorageV2 implements file-size-based splitting
type BinaryTreeStorageV2 struct {
	mu        sync.RWMutex
	Directory string
	Root      *BinaryNodeV2

	// Statistics
	NextLeafID   atomic.Uint32
	TotalKeys    atomic.Uint64
	TotalLeaves  atomic.Uint32
	TotalSplits  atomic.Uint32

	// Write buffering
	writeBuffer chan *KeyWrite
	flushSignal chan struct{}
	wg          sync.WaitGroup
}

// NewBinaryTreeStorageV2 creates a new binary tree storage with size-based splitting
func NewBinaryTreeStorageV2(directory string) (*BinaryTreeStorageV2, error) {
	os.RemoveAll(directory)
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}

	bts := &BinaryTreeStorageV2{
		Directory:   directory,
		writeBuffer: make(chan *KeyWrite, 10000),
		flushSignal: make(chan struct{}, 1),
	}

	// Start with a single root leaf
	leafID := bts.NextLeafID.Add(1)
	leafFile := filepath.Join(directory, fmt.Sprintf("leaf_%08x.dat", leafID))

	bts.Root = &BinaryNodeV2{
		IsLeaf:   true,
		Level:    0,
		Prefix:   0,
		LeafID:   leafID,
		LeafFile: leafFile,
		FileSize: HeaderSize,
		KeyCount: 0,
	}

	if err := bts.createLeafFile(leafFile); err != nil {
		return nil, err
	}

	bts.TotalLeaves.Store(1)

	// Start background writer
	bts.wg.Add(1)
	go bts.backgroundWriter()

	return bts, nil
}

// createLeafFile creates a new empty leaf file
func (bts *BinaryTreeStorageV2) createLeafFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	header := make([]byte, HeaderSize)
	_, err = file.Write(header)
	return err
}

// findLeaf navigates to the correct leaf for a key
func (bts *BinaryTreeStorageV2) findLeaf(key [32]byte) (*BinaryNodeV2, error) {
	bts.mu.RLock()
	node := bts.Root
	bts.mu.RUnlock()

	level := 0
	for {
		node.mu.RLock()

		if node.IsLeaf {
			needsSplit := node.NeedsSplit
			node.mu.RUnlock()

			if needsSplit {
				// This leaf needs splitting
				return bts.splitLeaf(node, key)
			}
			return node, nil
		}

		// Navigate based on the bit at current level
		bit := getBitAt(key, level)
		var next *BinaryNodeV2
		if bit == 0 {
			next = node.Left
		} else {
			next = node.Right
		}
		node.mu.RUnlock()

		if next == nil {
			return nil, fmt.Errorf("missing child at level %d", level)
		}

		node = next
		level++
	}
}

// getBitAt returns the bit at position pos from the key
func getBitAt(key [32]byte, pos int) byte {
	byteIndex := pos / 8
	bitIndex := uint(7 - (pos % 8)) // MSB first
	if byteIndex >= len(key) {
		return 0
	}
	return (key[byteIndex] >> bitIndex) & 1
}

// splitLeaf splits a leaf multiple levels deep based on file size
func (bts *BinaryTreeStorageV2) splitLeaf(leaf *BinaryNodeV2, triggerKey [32]byte) (*BinaryNodeV2, error) {
	leaf.mu.Lock()
	defer leaf.mu.Unlock()

	// Double-check split is still needed
	if !leaf.NeedsSplit {
		return leaf, nil
	}

	fmt.Printf("Splitting leaf at level %d into %d levels (file size: %d bytes, %d keys)\n",
		leaf.Level, SplitDepth, leaf.FileSize, leaf.KeyCount)

	// Read all keys from the current leaf file
	oldKeys, err := bts.readLeafKeys(leaf.LeafFile)
	if err != nil {
		return nil, err
	}

	// Convert leaf to internal node
	leaf.IsLeaf = false
	leaf.NeedsSplit = false

	// Create the tree structure for SplitDepth levels
	// This will create 2^SplitDepth leaves
	numLeaves := 1 << SplitDepth // 2^SplitDepth

	// Build the intermediate tree structure
	bts.buildSplitTree(leaf, SplitDepth)

	// Collect all the new leaf nodes
	leaves := make([]*BinaryNodeV2, 0, numLeaves)
	bts.collectLeaves(leaf, &leaves, SplitDepth)

	// Group keys by their target leaf based on SplitDepth bits
	leafKeys := make([][]KeyWrite, numLeaves)
	for i := range leafKeys {
		leafKeys[i] = make([]KeyWrite, 0, len(oldKeys)/numLeaves)
	}

	for _, kw := range oldKeys {
		// Get SplitDepth bits starting from leaf.Level
		leafIndex := 0
		for i := 0; i < SplitDepth; i++ {
			bit := getBitAt(kw.Key, leaf.Level + i)
			leafIndex = (leafIndex << 1) | int(bit)
		}
		leafKeys[leafIndex] = append(leafKeys[leafIndex], kw)
	}

	// Write keys to each leaf and report distribution
	var totalRedistributed int
	for i, keys := range leafKeys {
		if len(keys) > 0 {
			if err := bts.writeToLeaf(leaves[i], keys); err != nil {
				return nil, err
			}
			totalRedistributed += len(keys)
			fmt.Printf("  Leaf %d (prefix %0*b): %d keys\n",
				i, SplitDepth, i, len(keys))
		}
	}

	fmt.Printf("Split complete: %d keys redistributed to %d leaves\n",
		totalRedistributed, numLeaves)

	bts.TotalSplits.Add(1)

	// Delete the old leaf file
	os.Remove(leaf.LeafFile)

	// Navigate to the correct leaf for the trigger key
	return bts.navigateToLeaf(leaf, triggerKey, SplitDepth)
}

// buildSplitTree recursively builds a tree of the specified depth
func (bts *BinaryTreeStorageV2) buildSplitTree(node *BinaryNodeV2, depth int) {
	if depth <= 0 {
		return
	}

	if depth == 1 {
		// Create leaf nodes
		leftID := bts.NextLeafID.Add(1)
		leftFile := filepath.Join(bts.Directory, fmt.Sprintf("leaf_%08x.dat", leftID))

		rightID := bts.NextLeafID.Add(1)
		rightFile := filepath.Join(bts.Directory, fmt.Sprintf("leaf_%08x.dat", rightID))

		node.Left = &BinaryNodeV2{
			IsLeaf:   true,
			Level:    node.Level + 1,
			Prefix:   node.Prefix << 1,
			LeafID:   leftID,
			LeafFile: leftFile,
			FileSize: HeaderSize,
			KeyCount: 0,
		}

		node.Right = &BinaryNodeV2{
			IsLeaf:   true,
			Level:    node.Level + 1,
			Prefix:   (node.Prefix << 1) | 1,
			LeafID:   rightID,
			LeafFile: rightFile,
			FileSize: HeaderSize,
			KeyCount: 0,
		}

		bts.createLeafFile(leftFile)
		bts.createLeafFile(rightFile)
		bts.TotalLeaves.Add(2)
	} else {
		// Create internal nodes
		node.Left = &BinaryNodeV2{
			IsLeaf: false,
			Level:  node.Level + 1,
			Prefix: node.Prefix << 1,
		}

		node.Right = &BinaryNodeV2{
			IsLeaf: false,
			Level:  node.Level + 1,
			Prefix: (node.Prefix << 1) | 1,
		}

		// Recursively build subtrees
		bts.buildSplitTree(node.Left, depth-1)
		bts.buildSplitTree(node.Right, depth-1)
	}
}

// collectLeaves collects all leaf nodes at a given depth
func (bts *BinaryTreeStorageV2) collectLeaves(node *BinaryNodeV2, leaves *[]*BinaryNodeV2, depth int) {
	if depth <= 0 || node.IsLeaf {
		*leaves = append(*leaves, node)
		return
	}

	if node.Left != nil {
		bts.collectLeaves(node.Left, leaves, depth-1)
	}
	if node.Right != nil {
		bts.collectLeaves(node.Right, leaves, depth-1)
	}
}

// navigateToLeaf navigates to the correct leaf for a key after a split
func (bts *BinaryTreeStorageV2) navigateToLeaf(node *BinaryNodeV2, key [32]byte, depth int) (*BinaryNodeV2, error) {
	current := node
	for i := 0; i < depth; i++ {
		if current.IsLeaf {
			return current, nil
		}

		bit := getBitAt(key, current.Level)
		if bit == 0 {
			current = current.Left
		} else {
			current = current.Right
		}

		if current == nil {
			return nil, fmt.Errorf("navigation error at depth %d", i)
		}
	}
	return current, nil
}

// readLeafKeys reads all keys from a leaf file
func (bts *BinaryTreeStorageV2) readLeafKeys(leafFile string) ([]KeyWrite, error) {
	file, err := os.Open(leafFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read header
	header := make([]byte, HeaderSize)
	if _, err := file.Read(header); err != nil {
		return nil, err
	}

	keyCount := binary.BigEndian.Uint32(header[0:4])
	keys := make([]KeyWrite, 0, keyCount)

	// Read all entries
	entry := make([]byte, KeyEntrySizeV2)
	for i := uint32(0); i < keyCount; i++ {
		if _, err := file.Read(entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		kw := KeyWrite{}
		copy(kw.Key[:], entry[0:32])
		kw.Offset = binary.BigEndian.Uint64(entry[32:40])
		kw.Length = binary.BigEndian.Uint64(entry[40:48])
		keys = append(keys, kw)
	}

	return keys, nil
}

// AddKey adds a key to storage
func (bts *BinaryTreeStorageV2) AddKey(key [32]byte, offset, length uint64) error {
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

// backgroundWriter processes writes
func (bts *BinaryTreeStorageV2) backgroundWriter() {
	defer bts.wg.Done()

	batch := make([]*KeyWrite, 0, 1000)
	ticker := time.NewTicker(100 * time.Millisecond)
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
			if len(batch) > 0 {
				bts.processBatch(batch)
				batch = make([]*KeyWrite, 0, 1000)
			}
		}
	}
}

// processBatch groups keys by leaf and writes them
func (bts *BinaryTreeStorageV2) processBatch(batch []*KeyWrite) {
	// Group by leaf
	leafGroups := make(map[*BinaryNodeV2][]KeyWrite)

	for _, write := range batch {
		leaf, err := bts.findLeaf(write.Key)
		if err != nil {
			fmt.Printf("Error finding leaf: %v\n", err)
			continue
		}
		leafGroups[leaf] = append(leafGroups[leaf], *write)
	}

	// Write to each leaf in parallel
	var wg sync.WaitGroup
	for leaf, writes := range leafGroups {
		wg.Add(1)
		go func(l *BinaryNodeV2, w []KeyWrite) {
			defer wg.Done()
			if err := bts.writeToLeaf(l, w); err != nil {
				fmt.Printf("Error writing to leaf: %v\n", err)
			}
		}(leaf, writes)
	}
	wg.Wait()
}

// writeToLeaf writes keys to a leaf file
func (bts *BinaryTreeStorageV2) writeToLeaf(leaf *BinaryNodeV2, writes []KeyWrite) error {
	leaf.WriteLock.Lock()
	defer leaf.WriteLock.Unlock()

	// Open file for append
	file, err := os.OpenFile(leaf.LeafFile, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read current key count
	header := make([]byte, HeaderSize)
	if _, err := file.Read(header); err != nil {
		return err
	}

	keyCount := binary.BigEndian.Uint32(header[0:4])

	// Seek to end for appending
	if _, err := file.Seek(0, 2); err != nil {
		return err
	}

	// Write entries
	entry := make([]byte, KeyEntrySizeV2)
	bytesWritten := 0

	for _, write := range writes {
		copy(entry[0:32], write.Key[:])
		binary.BigEndian.PutUint64(entry[32:40], write.Offset)
		binary.BigEndian.PutUint64(entry[40:48], write.Length)

		if _, err := file.Write(entry); err != nil {
			return err
		}

		bytesWritten += KeyEntrySizeV2
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

	// Update leaf metadata
	leaf.mu.Lock()
	leaf.KeyCount = keyCount
	leaf.FileSize += int64(bytesWritten)

	// Check if split is needed based on file size
	if leaf.FileSize > LeafSplitSize {
		leaf.NeedsSplit = true
	}
	leaf.mu.Unlock()

	return nil
}

// Get retrieves a key from storage
func (bts *BinaryTreeStorageV2) Get(key [32]byte) (offset, length uint64, err error) {
	// Navigate to correct leaf
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

		bit := getBitAt(key, level)
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
func (bts *BinaryTreeStorageV2) searchLeaf(leafFile string, key [32]byte) (offset, length uint64, err error) {
	file, err := os.Open(leafFile)
	if err != nil {
		return 0, 0, fmt.Errorf("key not found")
	}
	defer file.Close()

	// Read header
	header := make([]byte, HeaderSize)
	if _, err := file.Read(header); err != nil {
		return 0, 0, err
	}

	keyCount := binary.BigEndian.Uint32(header[0:4])

	// Linear search (could optimize later)
	entry := make([]byte, KeyEntrySizeV2)
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

// Close shuts down the storage
func (bts *BinaryTreeStorageV2) Close() error {
	close(bts.writeBuffer)
	bts.wg.Wait()
	return nil
}

// Stats returns statistics
func (bts *BinaryTreeStorageV2) Stats() string {
	return fmt.Sprintf("Keys: %d, Leaves: %d, Splits: %d",
		bts.TotalKeys.Load(),
		bts.TotalLeaves.Load(),
		bts.TotalSplits.Load())
}