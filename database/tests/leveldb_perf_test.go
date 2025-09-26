package blockchainDB

import (
	"fmt"
	"os"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestLevelDBPerformance(t *testing.T) {
	directory := "/tmp/LevelDBPerf"
	os.RemoveAll(directory)

	const numBatches = 10 // Same as HistoryFile test
	const batchSize = 1_000_000 // 1M per batch, 10M total

	// Open LevelDB with optimized settings
	opts := &opt.Options{
		WriteBuffer:         256 * 1024 * 1024, // 256MB write buffer
		BlockCacheCapacity:  512 * 1024 * 1024, // 512MB block cache
		CompactionTableSize: 64 * 1024 * 1024,  // 64MB SST files
		CompactionTotalSize: 640 * 1024 * 1024, // 640MB level size
	}

	db, err := leveldb.OpenFile(directory, opts)
	assert.NoError(t, err, "failed to open LevelDB")
	defer db.Close()

	fr := NewFastRandom([]byte{1, 2})

	// Pre-generate all keys (same as HistoryFile test)
	allKeys := make([][32]byte, numBatches*batchSize)
	for i := range allKeys {
		allKeys[i] = fr.NextHash()
	}

	// Write performance test
	fmt.Println("=== LEVELDB WRITE PERFORMANCE ===")
	start := time.Now()
	var cnt int

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		batchStart := time.Now()
		batch := new(leveldb.Batch)

		for i := 0; i < batchSize; i++ {
			key := allKeys[batchIdx*batchSize+i]
			value := make([]byte, 16) // Store offset and length similar to HistoryFile
			// Pack offset and length into value
			offset := uint64(0x1010 * (cnt + 1))
			length := uint64(0x1111 * (cnt + 1))
			for j := 0; j < 8; j++ {
				value[j] = byte(offset >> (8 * j))
				value[j+8] = byte(length >> (8 * j))
			}
			batch.Put(key[:], value)
			cnt++
		}

		// Write batch
		writeOpts := &opt.WriteOptions{
			Sync: false, // Don't sync every batch for better performance
		}
		err = db.Write(batch, writeOpts)
		assert.NoError(t, err, "LevelDB write failed")

		tps := float64(batchSize) / time.Since(batchStart).Seconds()
		comma := humanize.Comma(int64(cnt))
		fmt.Printf("%12s txs @ %12.2f tps %12s per write\n", comma, tps, ComputeTimePerOp(tps))
	}

	totalWriteTime := time.Since(start)
	avgWriteTPS := float64(numBatches*batchSize) / totalWriteTime.Seconds()
	fmt.Printf("Write complete: %v total, %.0f avg TPS\n\n", totalWriteTime, avgWriteTPS)

	// Force compaction to ensure all data is written
	fmt.Println("=== COMPACTING ===")
	compactStart := time.Now()
	err = db.CompactRange(util.Range{})
	assert.NoError(t, err, "Compaction failed")
	fmt.Printf("Compaction complete: %v\n\n", time.Since(compactStart))

	// Read performance test
	fmt.Println("=== LEVELDB READ PERFORMANCE ===")
	fr.Reset()
	readStart := time.Now()
	found := 0
	notFound := 0

	// Test reading in batches of 10k keys (same as HistoryFile)
	testBatches := 10
	readBatchSize := 10000
	totalReads := testBatches * readBatchSize

	for batch := 0; batch < testBatches; batch++ {
		batchStart := time.Now()
		batchFound := 0
		batchNotFound := 0

		for i := 0; i < readBatchSize; i++ {
			k := fr.NextHash()

			_, err := db.Get(k[:], nil)
			if err == nil {
				batchFound++
				found++
			} else {
				batchNotFound++
				notFound++
			}
		}

		// Print stats in same format as HistoryFile
		tps := float64(readBatchSize) / time.Since(batchStart).Seconds()
		comma := humanize.Comma(int64((batch + 1) * readBatchSize))
		fmt.Printf("%12s reads @ %12.2f tps %12s per read\n", comma, tps, ComputeTimePerOp(tps))
	}

	totalReadTime := time.Since(readStart)
	avgReadTPS := float64(totalReads) / totalReadTime.Seconds()
	fmt.Printf("Read complete: %v total, %.0f avg TPS\n", totalReadTime, avgReadTPS)

	// Print database statistics
	fmt.Println("\n=== LEVELDB STATISTICS ===")
	stats, err := db.GetProperty("leveldb.stats")
	if err == nil {
		fmt.Println(stats)
	}
}