package blockchainDB

import (
	"fmt"
	"os"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
)

func TestBinaryTreeQuick(t *testing.T) {
	directory := "/tmp/BinaryTreeQuick"
	os.RemoveAll(directory)

	const totalKeys = 50_000 // 50K keys for quick test

	fmt.Println("=== BINARY TREE QUICK TEST ===")
	fmt.Printf("Total keys: %s\n", humanize.Comma(totalKeys))
	fmt.Printf("Leaf capacity: %s\n", humanize.Comma(LeafMaxKeys))

	bts, err := NewBinaryTreeStorage(directory)
	assert.NoError(t, err)
	defer bts.Close()

	// Generate and write keys
	fr := NewFastRandom([]byte{1, 2, 3})
	startTime := time.Now()

	for i := 0; i < totalKeys; i++ {
		key := fr.NextHash()
		err := bts.AddKey(key, uint64((i+1)*100), uint64((i+1)*10))
		assert.NoError(t, err)

		if (i+1)%10000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			fmt.Printf("Progress: %d keys, %.0f keys/sec, %s\n",
				i+1, rate, bts.Stats())
		}
	}

	// Final flush
	select {
	case bts.flushSignal <- struct{}{}:
	default:
	}
	time.Sleep(200 * time.Millisecond)

	totalTime := time.Since(startTime)
	fmt.Printf("\nWrite complete: %s keys in %.2fs = %.0f keys/sec\n",
		humanize.Comma(totalKeys), totalTime.Seconds(), float64(totalKeys)/totalTime.Seconds())
	fmt.Printf("Final: %s\n", bts.Stats())
}