package blockchainDB

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeader(t *testing.T) {
	//filename, rm := MakeFilename("header.dat")
	//defer rm()

	h1 := new(Header).Init(1024)
	s1 := h1.Marshal()
	h2 := new(Header)
	h2.Unmarshal(s1)
	s2 := h2.Marshal()
	assert.Equal(t, s1, s2, "Should get back what was in the header")

}
