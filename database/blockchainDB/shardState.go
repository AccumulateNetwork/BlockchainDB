package blockchainDB

import (
	"encoding/binary"
	"os"
)

// SState
// The Shard State holding information about the ShardDB
type SState struct {
	Filename string
	ShardCnt int
}

// NewState
// opens and loads a state, or creates a blank one if it does
// not exist.
func NewState(filename string) (*SState, error) {
	s := new(SState)
	s.Filename = filename
	if err := s.Load(); err != nil { // Try to open a state
		s.ShardCnt = 1024 // Shard the database by 1 K
		if err = s.Flush(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Marshal
// Convert the state to binary
func (s *SState) Marshal() []byte {
	var data [2]byte
	binary.BigEndian.PutUint16(data[:], uint16(s.ShardCnt))
	return data[:]
}

// Unmarshal
// load up the state from binary
func (s *SState) Unmarshal(data []byte) {
	s.ShardCnt = int(binary.BigEndian.Uint16(data))
}

// Load
// Load the state off disk
func (s *SState) Load() error {
	f, err := os.Open(s.Filename)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close() // The error on close doesn't matter
	}()
	var data [2]byte
	if _, err = f.Read(data[:]); err != nil {
		return err
	}
	s.Unmarshal(data[:])
	return nil
}

// Flush
// Flush the state to disk
func (s *SState) Flush() error {
	f, err := os.Create(s.Filename)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close() // The error on close doesn't matter
	}()
	f.Write(s.Marshal())
	return nil
}
