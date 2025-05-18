package blockchainDB

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloom(t *testing.T) {
	const NumTests = 1_000_000
	
	// Increased size from 4.5MB to 6MB
	// Using 3 hash functions (default in NewBloom)
	bloom := NewBloom(6.0)
	fr := NewFastRandom([]byte{1})

	// Add 1 million items to the bloom filter
	for i := 0; i < NumTests; i++ {
		bloom.Set(fr.NextHash())
	}

	var falseCnt float64 = 0

	// Verify all items that should be in the filter are found
	fr.Reset()
	for i := 0; i < NumTests; i++ {
		assert.True(t, bloom.Test(fr.NextHash()))
	}
	
	// Test with new random data to measure false positive rate
	for i := 0; i < NumTests; i++ {
		if bloom.Test(fr.NextHash()) {
			falseCnt++
		}
	}
	
	// Assert that false positive rate is below 5%
	fpRate := falseCnt/NumTests*100
	assert.True(t, falseCnt/NumTests < .05, "too many false positives, %3.1f%%", fpRate)
	fmt.Printf("False Positives: %3.1f%%\n", fpRate)
}
