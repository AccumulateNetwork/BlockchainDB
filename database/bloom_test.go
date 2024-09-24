package blockchainDB

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloom(t *testing.T) {
	const NumTests = 1_000_000
	bloom := NewBloom(4.5)
	fr := NewFastRandom([]byte{1})

	for i := 0; i < NumTests; i++ {
		bloom.Set(fr.NextHash())
	}

	var falseCnt float64 = 0

	fr.Reset()
	for i := 0; i < NumTests; i++ {
		assert.True(t, bloom.Test(fr.NextHash()))
	}
	for i := 0; i < NumTests; i++ {
		if bloom.Test(fr.NextHash()) {
			falseCnt++
		}
	}
	assert.True(t, falseCnt/NumTests < .05, "too many false positives, %3.1f%%", falseCnt/NumTests*100)
	fmt.Printf("False Positives: %3.1f\n", falseCnt/NumTests*100)
}
