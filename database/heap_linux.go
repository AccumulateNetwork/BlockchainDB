//go:build linux

package blockchainDB

import (
	"os"
	"syscall"
)

// punchHole releases the bytes of [off, off+n) back to the filesystem
// without changing the file's size: the cleaned head of a heap.
func punchHole(f *os.File, off, n int64) error {
	const punch = 0x02 | 0x01 // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
	return syscall.Fallocate(int(f.Fd()), punch, off, n)
}
