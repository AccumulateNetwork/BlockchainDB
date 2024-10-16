package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

type DBLocation struct {
}

type BlockList struct {
	Directory     string // Directory of the DB
	KeyLocation   map[[32]byte]*DBLocation
	BlockHeight   int
	CurrentHeight int
	Partition     int
	BufferCnt     int
	BFile         *BFile
}

// GetFilename
// Returns the filename for a block at a given height
func (b *BlockList) GetFilename(height int) (filename string) {
	filename = fmt.Sprintf("Block%dPart%d", b.BlockHeight, b.Partition)
	return filename
}

// SaveState
// Saves the state of a given BlockList
func (b *BlockList) SaveState() error {
	stateName := filepath.Join(b.Directory, "BlockState.dat")

	stateFile, err := os.Create(stateName)
	if err != nil {
		return err
	}
	defer stateFile.Close()

	var height [8]byte
	var partition [8]byte
	binary.BigEndian.PutUint64(height[:], uint64(b.BlockHeight))
	binary.BigEndian.PutUint64(partition[:], uint64(b.Partition))
	if _, err := stateFile.Write(height[:]); err != nil {
		return err
	}
	if _, err := stateFile.Write(partition[:]); err != nil {
		return err
	}

	return nil
}

// LoadState
// Loads the state of an existing block list
func (b *BlockList) LoadState() error {
	stateName := filepath.Join(b.Directory, "BlockState.dat")

	stateFile, err := os.Open(stateName)
	if err != nil {
		return err
	}
	defer stateFile.Close()

	var height [8]byte
	var partition [8]byte
	if _, err := stateFile.Read(height[:]); err != nil {
		return err
	}
	if _, err := stateFile.Read(partition[:]); err != nil {
		return err
	}
	b.BlockHeight = int(binary.BigEndian.Uint64(height[:]))
	b.Partition = int(binary.BigEndian.Uint64(partition[:]))

	return nil
}

// NewBlockList
// This is a directory of blocks.  Blocks are created one after another in the directory.
// The BlockFile is left open, and should be closed when it is complete.  The state of
// the BlockFile is not updated until the BlockFile is closed.
func NewBlockList(Directory string, Partition int) (blockFile *BlockList, err error) {
	os.RemoveAll(Directory)                                 // Remove any directory there, if there is one
	if err = os.Mkdir(Directory, os.ModePerm); err != nil { // Create the BlockList Directory
		return nil, err
	}

	bf := new(BlockList)

	bf.Directory = Directory
	bf.Partition = Partition
	bf.BlockHeight = 1
	bf.SaveState()
	if bf.BFile, err = NewBFile(Directory, bf.GetFilename(bf.BlockHeight)); err != nil {
		return nil, err
	}
	return bf, nil
}

// NextBlockFile
// If a BFile is open, then the BFile is closed.
// Creates the next BFile.
func (b *BlockList) NextBlockFile() (err error) {
	if b.BFile != nil {
		b.BFile.Close()
		if err = b.SaveState(); err != nil {
			return err
		}
	}
	b.BlockHeight++

	filename := b.GetFilename(b.BlockHeight)
	if b.BFile, err = NewBFile(b.Directory, filename); err != nil {
		return err
	}

	return nil
}

// OpenBlockList
// Open an existing BlockList
func OpenBlockList(Directory string) (blockList *BlockList, err error) {
	blockList = new(BlockList)
	blockList.Directory = Directory
	
	if err = blockList.LoadState(); err != nil {
		return nil, err
	}
	filename := blockList.GetFilename(blockList.BlockHeight)
	if blockList.BFile, err = OpenBFile(blockList.Directory, filename); err != nil {
		return nil, err
	}

	return blockList, err
}

// OpenBFile
// Open a particular BFile in a BlockList at a given height. If a BFile is
// currently opened, then it is closed.  If the BFile being opened does not
// exist (has a height > b.BlockHeight) then the provided Height must be
// b.BlockHeight+1
func (b *BlockList) OpenBFile(Height int) (bFile *BFile, err error) {
	if Height > b.BlockHeight+1 {
		return nil, fmt.Errorf("height %d is invalid. current BlockList height is: %d",
			Height, b.BlockHeight)
	}
	filename := b.GetFilename(Height)
	if bFile, err = OpenBFile(b.Directory, filename); err != nil {
		return nil, err
	}
	if b.BFile != nil {
		b.BFile.Close()
	}
	b.BFile = bFile
	return bFile, err
}

// Close
// Closes the BlockFile and the underlying BFile, and updates
// the BlockFile state.  Note that the rest of the BlockList state
// is unaltered.
func (b *BlockList) Close() {
	if b.BFile == nil {
		return
	}
	b.BFile.Close()
	b.BFile = nil
	if err := b.SaveState(); err != nil {
		fmt.Printf("%v", err)
	}

}

// Put
// For neatness.  Pass through to the underlying BFile
func (b *BlockList) Put(Key [32]byte, Value []byte) error {
	return b.BFile.Put(Key, Value)
}

// Get
// For neatness.  Pass through to the underlying BFile
func (b *BlockList) Get(Key [32]byte) (Value []byte, err error) {
	return b.BFile.Get(Key)
}
