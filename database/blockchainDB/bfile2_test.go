package blockchainDB

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBfile2(t *testing.T) {
	dir,rm := MakeDir()
	defer rm()
	filename := filepath.Join(dir,"Bfile2.dat")
	bf,err := NewBFile2(filename)
	assert.NoError(t,err,"failed to create BFile2")
	bf.File.Close()
}