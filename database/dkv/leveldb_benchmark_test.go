package dkv

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// TestLevelDB_10M runs 10 million entry benchmark
func TestLevelDB_10M(t *testing.T) {
	runLevelDBBenchmark(t, 10_000_000)
}

// TestLevelDB_100M runs 100 million entry benchmark
func TestLevelDB_100M(t *testing.T) {
	runLevelDBBenchmark(t, 100_000_000)
}

// TestLevelDB_200M runs 200 million entry benchmark
func TestLevelDB_200M(t *testing.T) {
	runLevelDBBenchmark(t, 200_000_000)
}

// TestLevelDB_500M runs 500 million entry benchmark
func TestLevelDB_500M(t *testing.T) {
	runLevelDBBenchmark(t, 500_000_000)
}

func runLevelDBBenchmark(t *testing.T, targetEntries uint64) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Printf("LEVELDB %s BENCHMARK\n", formatNum(targetEntries))
	fmt.Println(strings.Repeat("=", 60))

	const (
		statsInterval = 20 * time.Second
		batchSize     = 101 // 1 account + 100 txs
	)

	baseDir := t.TempDir()
	leveldbDir := filepath.Join(baseDir, "leveldb")
	logPath := "/tmp/leveldb_benchmark.log"

	// Delete old log file to start fresh
	os.Remove(logPath)

	tracker, err := NewPerfTracker("LevelDB", logPath, leveldbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	opts := &opt.Options{
		WriteBuffer:         64 * 1024 * 1024,  // 64MB write buffer
		BlockCacheCapacity:  128 * 1024 * 1024, // 128MB block cache
		CompactionTableSize: 32 * 1024 * 1024,  // 32MB SST files
	}

	db, err := leveldb.OpenFile(leveldbDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fmt.Printf("Target: %s entries\n", formatNum(targetEntries))
	fmt.Printf("Stats log: %s\n", logPath)
	fmt.Printf("Data dir: %s\n\n", leveldbDir)

	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()

	opCounter := uint64(0)
	accountIdx := uint64(0)

	for tracker.Entries() < targetEntries {
		select {
		case <-statsTicker.C:
			tracker.WriteStats(targetEntries, false)
		default:
		}

		// 90% writes, 10% reads
		op := opCounter % 100
		opCounter++

		if op < 90 {
			start := time.Now()
			var batchBytes int64

			batch := new(leveldb.Batch)

			accKey := generateAccountKey(accountIdx)
			accVal := generateAccountValue(accountIdx)
			batchBytes += int64(len(accKey) + len(accVal))
			batch.Put(accKey, accVal)

			for i := uint64(0); i < 100; i++ {
				txHash := generateTxHash(accountIdx, i)
				txVal := generateTxValue(accountIdx, i)
				batchBytes += int64(len(txHash) + len(txVal))
				batch.Put(txHash[:], txVal)
			}

			if err := db.Write(batch, nil); err != nil {
				continue
			}

			tracker.AddWriteWithBytes(uint64(batchSize), 1, 100, batchBytes, time.Since(start))
			accountIdx++

		} else {
			// READ
			maxAccount := tracker.Accounts()
			if maxAccount == 0 {
				continue
			}

			start := time.Now()
			randAccount := opCounter % maxAccount

			if opCounter%100 < 99 {
				key := generateAccountKey(randAccount)
				db.Get(key, nil)
				tracker.AddRead("get", time.Since(start))
			} else {
				txHash := generateTxHash(randAccount, opCounter%100)
				db.Get(txHash[:], nil)
				tracker.AddRead("hash", time.Since(start))
			}
		}
	}

	tracker.WriteStats(targetEntries, true)
	fmt.Printf("\nLevelDB %s benchmark complete!\n", formatNum(targetEntries))
}

// Helper for prefix iteration (if needed)
func leveldbPrefixIterator(db *leveldb.DB, prefix []byte, limit int) int {
	iter := db.NewIterator(util.BytesPrefix(prefix), nil)
	defer iter.Release()

	count := 0
	for iter.Next() && count < limit {
		count++
	}
	return count
}
