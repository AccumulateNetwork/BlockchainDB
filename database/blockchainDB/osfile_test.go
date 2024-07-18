package blockchainDB

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOSFile(t *testing.T) {
	Directory := filepath.Join(os.TempDir(), "OSFile")
	os.Mkdir(Directory, os.ModePerm)
	osFile, err := NewOSFile(filepath.Join(Directory, "OSFile.dat"))
	assert.NoError(t, err, "OpenOSFile failed")

	osFile.Lock()
	osFile.UnLock()
	osFile.Lock()
	osFile.UnLock()
	osFile.Lock()
	osFile.UnLock()
	osFile.Lock()
	osFile.UnLock()

}
