package blockchainDB

import (
	"encoding/binary"
	"fmt"
)

const DBKeySize = 50

type DBBKey struct {
	Height uint16 // For index for a blockList
	Offset uint64
	Length uint64
}

// Bytes
// Writes out the address with the offset and length of the DBBKey
func (d DBBKey) Bytes(address [32]byte) []byte {
	var b [50]byte
	d.WriteTo(address, &b)
	return b[:]
}

func (d DBBKey) WriteTo(address [32]byte, b *[50]byte) {
	// This is ~6.5ns vs ~55ns for Bytes due to avoiding an allocation. That
	// could matter in a tight loop with millions of keys. Unless the compiler
	// inlines Bytes and eliminates the allocation, in which case there's no
	// difference.
	copy(b[:], address[:])
	binary.BigEndian.PutUint64(b[34:], d.Offset)
	binary.BigEndian.PutUint64(b[42:], d.Length)
	binary.BigEndian.PutUint16(b[32:], d.Height)
}

// GetDBBKey
// Converts a byte slice into an Address and a DBBKey
func GetDBBKey(data []byte) (address [32]byte, dBBKey *DBBKey, err error) {
	dBBKey = new(DBBKey)
	address, err = dBBKey.Unmarshal(data)
	return address, dBBKey, err
}

// Unmarshal
// Returns the address and the DBBKey from a slice of bytes
func (d *DBBKey) Unmarshal(data []byte) (address [32]byte, err error) {
	if len(data) < 50 {
		return address, fmt.Errorf("data source is short %d", len(data))
	}
	copy(address[:], data[:32])
	d.Height = binary.BigEndian.Uint16(data[32:])
	d.Offset = binary.BigEndian.Uint64(data[34:])
	d.Length = binary.BigEndian.Uint64(data[42:])
	return address, nil
}
