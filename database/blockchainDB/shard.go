package blockchainDB

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

//  Note that this code does not behave properly if compiled prior to 1.20
//  See notes on rand
//  https://stackoverflow.com/questions/75597325/rand-seedseed-is-deprecated-how-to-use-newrandnewseed

// Shard
// Holds the stuff required to access a shard.
type Shard struct {
	Directory  string              // Directory for the shard
	SDirectory string              // Directory for the Database
	BFile      *BFile              // The BFile
	Cache      map[[32]byte][]byte // Cache of what is being written to the cache
	KeyCount   int                 // How many keys in the BFile
	KeyWrites  int                 // How many writes since compression
	Mutex      sync.Mutex          // Keeps compression from conflict with access
}

// OpenShard
// Open an existing shard
func OpenShard(Directory, SDirectory string) (shard *Shard, err error) {
	shard = new(Shard)
	shard.Directory = Directory
	shard.SDirectory = SDirectory
	sDir := filepath.Join(Directory, SDirectory)
	shard.Cache = make(map[[32]byte][]byte)
	if shard.BFile, err = OpenBFile(sDir, "shard.dat"); err != nil {
		return nil, err
	}
	go shard.process()
	return shard, err
}

// NewShard
// Create and open a new Shard
func NewShard(Directory, SDirectory string) (shard *Shard, err error) {
	shard = new(Shard)
	shard.Directory = Directory
	shard.SDirectory = SDirectory
	sDir := filepath.Join(Directory, SDirectory)
	os.RemoveAll(sDir)
	os.Mkdir(sDir, os.ModePerm)
	shard.Cache = make(map[[32]byte][]byte)
	if shard.BFile, err = NewBFile(sDir, "shard.dat"); err != nil {
		return nil, err
	}
	go shard.process()
	return shard, err
}

// process
// Note that process calls rand.Intn() which isn't randomized without a call to
// rand.Seed()
func (s *Shard) process() {
	for s.BFile != nil { //                 Quit when the Shard is closed
		r := time.Duration(rand.Intn(5)) // Wait some random amount of time
		time.Sleep(r*time.Second + 15)   // time being 15 +/- 5 seconds
		s.compress()
	}
}

func (s *Shard) compress() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	var err error
	if s.KeyWrites > 1000 {
		s.BFile, err = s.BFile.Compress()
		if err != nil {
			panic(err)
		}
	}
}

// Close
// Clean up and close the Shard
func (s *Shard) Close() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.Cache = nil
	s.BFile.Close()
	s.BFile = nil
}

// Open
// Open an existing Shard.  If the BFile does not exist, create it
func (s *Shard) Open() (err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if s.BFile != nil {
		return
	}
	if s.BFile, err = OpenBFile(s.Directory, s.SDirectory); err != nil {
		if !os.IsNotExist(err) { // Can't deal with errors other than does not exist
			return err
		}
		if s.BFile, err = NewBFile(s.Directory, s.SDirectory); err != nil {
			return err // If file creation fails, return that error
		}
	}
	s.KeyCount = len(s.BFile.Keys)
	s.KeyWrites = 0
	return nil
}

// Put
// Put a key value pair into the shard
func (s *Shard) Put(key [32]byte, value []byte) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.KeyWrites++
	s.Cache[key] = value
	return s.BFile.Put(key, value)
}

// Get
// Get the value for the key out of the shard
func (s *Shard) Get(key [32]byte) (value []byte, err error) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if value, ok := s.Cache[key]; ok {
		return value, nil
	}
	return s.BFile.Get(key)
}
