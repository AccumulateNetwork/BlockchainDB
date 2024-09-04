package blockchainDB

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestFP(t *testing.T){
	path := "/tmp/xxx/yyy.dat"
	fmt.Println(filepath.Base(path))
}

func 