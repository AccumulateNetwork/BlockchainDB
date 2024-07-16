package blockchainDB

import (
	"io"
	"os"
	"sync"
)

// OSFile
// We have different processes accessing the underlying file.  Access
// has to be locked and unlocked to prevent stomping on toes.
type OSFile struct {
	mutex    sync.Mutex
	File     *os.File
	Position int64
}

// Lock the file, and maintain the position of the file
func (o *OSFile) Lock() {
	o.mutex.Lock()
	o.Position, _ = o.File.Seek(0, io.SeekCurrent)
}

// Unlock the file, and restore the position of the file
func (o *OSFile) Unlock() {
	o.File.Seek(o.Position, io.SeekStart)
	o.mutex.Unlock()
}

func (o *OSFile) Seek(offset int64, whence int) (ret int64, err error) {
	return o.File.Seek(offset, whence)
}

func (o *OSFile) Read(b []byte) (n int, err error) {
	return o.File.Read(b)
}

func (o *OSFile) Write(b []byte) (n int, err error) {
	return o.File.Write(b)
}

func CreateOSFile(Filename string) (osFile *OSFile, err error) {
	if file, err := os.Create(Filename); err == nil {
		osFile = new(OSFile)
		osFile.File = file
		return osFile, nil
	} else {
		return nil, err
	}
}

func OpenOSFile(name string, flag int, perm os.FileMode) (*OSFile, error) {
	if file, err := os.OpenFile(name, flag, perm); err == nil {
		osFile := new(OSFile)
		osFile.File = file
		return osFile, nil
	} else {
		return nil, err
	}
}

func (o *OSFile) Close() error {
	return o.File.Close()
}

