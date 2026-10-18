package dkv

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// TestWriteOnly_BDB_500M runs 500 million write-only benchmark for BDB
func TestWriteOnly_BDB_500M(t *testing.T) {
	runWriteOnlyBDB(t, 500_000_000)
}

// TestWriteOnly_LevelDB_500M runs 500 million write-only benchmark for LevelDB
func TestWriteOnly_LevelDB_500M(t *testing.T) {
	runWriteOnlyLevelDB(t, 500_000_000)
}

// TestWriteOnly_Badger_500M runs 500 million write-only benchmark for Badger
func TestWriteOnly_Badger_500M(t *testing.T) {
	runWriteOnlyBadger(t, 500_000_000)
}

const (
	writeOnlyReportInterval = 3 * time.Minute
	writeOnlyBatchSize      = 101 // 1 account + 100 txs
)

func runWriteOnlyBDB(t *testing.T, targetEntries uint64) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("BLOCKCHAINDB %s WRITE-ONLY BENCHMARK\n", formatNum(targetEntries))
	fmt.Println(strings.Repeat("=", 80))

	baseDir := t.TempDir()
	bdbDir := filepath.Join(baseDir, "bdb")
	logPath := "/tmp/bdb_benchmark.log" // Use standard log path for reporter

	os.Remove(logPath)

	tracker, err := NewPerfTracker("BlockchainDB", logPath, bdbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	config := getConfigFromEnv(int(targetEntries))
	db, err := NewBlockchainStoreWithConfig(bdbDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fmt.Printf("Target: %s entries\n", formatNum(targetEntries))
	fmt.Printf("Config: shards=%d, bins=%d, channels=%d, buffer=%d\n",
		config.NumShards, config.BinCount, config.NumChannelGroups, config.WriteChannelSize)
	fmt.Printf("Report interval: %v\n", writeOnlyReportInterval)
	fmt.Printf("Log: %s\n", logPath)
	fmt.Printf("Data: %s\n\n", bdbDir)

	reportTicker := time.NewTicker(writeOnlyReportInterval)
	defer reportTicker.Stop()

	accountIdx := uint64(0)

	for tracker.Entries() < targetEntries {
		select {
		case <-reportTicker.C:
			tracker.WriteStats(targetEntries, false)
		default:
		}

		start := time.Now()
		var batchBytes int64

		batch := db.NewBatch()

		accKey := generateAccountKey(accountIdx)
		accVal := generateAccountValue(accountIdx)
		batchBytes += int64(len(accKey) + len(accVal))
		batch.Put(accKey, accVal)

		for i := uint64(0); i < 100; i++ {
			txVal := generateTxValue(accountIdx, i)
			batchBytes += int64(32 + len(txVal))
			batch.PutByHash(txVal)
		}

		batch.Commit()
		tracker.AddWriteWithBytes(uint64(writeOnlyBatchSize), 1, 100, batchBytes, time.Since(start))
		accountIdx++
	}

	tracker.WriteStats(targetEntries, true)
	fmt.Printf("\nBlockchainDB write-only %s complete!\n", formatNum(targetEntries))
}

func runWriteOnlyLevelDB(t *testing.T, targetEntries uint64) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("LEVELDB %s WRITE-ONLY BENCHMARK\n", formatNum(targetEntries))
	fmt.Println(strings.Repeat("=", 80))

	baseDir := t.TempDir()
	leveldbDir := filepath.Join(baseDir, "leveldb")
	logPath := "/tmp/leveldb_benchmark.log" // Use standard log path for reporter

	os.Remove(logPath)

	tracker, err := NewPerfTracker("LevelDB", logPath, leveldbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	opts := &opt.Options{
		WriteBuffer:         64 * 1024 * 1024,
		BlockCacheCapacity:  128 * 1024 * 1024,
		CompactionTableSize: 32 * 1024 * 1024,
	}

	db, err := leveldb.OpenFile(leveldbDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fmt.Printf("Target: %s entries\n", formatNum(targetEntries))
	fmt.Printf("Report interval: %v\n", writeOnlyReportInterval)
	fmt.Printf("Log: %s\n", logPath)
	fmt.Printf("Data: %s\n\n", leveldbDir)

	reportTicker := time.NewTicker(writeOnlyReportInterval)
	defer reportTicker.Stop()

	accountIdx := uint64(0)

	for tracker.Entries() < targetEntries {
		select {
		case <-reportTicker.C:
			tracker.WriteStats(targetEntries, false)
		default:
		}

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

		tracker.AddWriteWithBytes(uint64(writeOnlyBatchSize), 1, 100, batchBytes, time.Since(start))
		accountIdx++
	}

	tracker.WriteStats(targetEntries, true)
	fmt.Printf("\nLevelDB write-only %s complete!\n", formatNum(targetEntries))
}

func runWriteOnlyBadger(t *testing.T, targetEntries uint64) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("BADGER %s WRITE-ONLY BENCHMARK\n", formatNum(targetEntries))
	fmt.Println(strings.Repeat("=", 80))

	baseDir := t.TempDir()
	badgerDir := filepath.Join(baseDir, "badger")
	logPath := "/tmp/badger_benchmark.log" // Use standard log path for reporter

	os.Remove(logPath)

	tracker, err := NewPerfTracker("BadgerDB", logPath, badgerDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	badgerOpts := badger.DefaultOptions(badgerDir)
	badgerOpts.Logger = nil
	badgerOpts.SyncWrites = false
	db, err := badger.Open(badgerOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fmt.Printf("Target: %s entries\n", formatNum(targetEntries))
	fmt.Printf("Report interval: %v\n", writeOnlyReportInterval)
	fmt.Printf("Log: %s\n", logPath)
	fmt.Printf("Data: %s\n\n", badgerDir)

	reportTicker := time.NewTicker(writeOnlyReportInterval)
	defer reportTicker.Stop()

	accountIdx := uint64(0)

	for tracker.Entries() < targetEntries {
		select {
		case <-reportTicker.C:
			tracker.WriteStats(targetEntries, false)
		default:
		}

		start := time.Now()
		var batchBytes int64

		err := db.Update(func(txn *badger.Txn) error {
			accKey := generateAccountKey(accountIdx)
			accVal := generateAccountValue(accountIdx)
			batchBytes += int64(len(accKey) + len(accVal))
			if err := txn.Set(accKey, accVal); err != nil {
				return err
			}

			for i := uint64(0); i < 100; i++ {
				txHash := generateTxHash(accountIdx, i)
				txVal := generateTxValue(accountIdx, i)
				batchBytes += int64(len(txHash) + len(txVal))
				if err := txn.Set(txHash[:], txVal); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			continue
		}

		tracker.AddWriteWithBytes(uint64(writeOnlyBatchSize), 1, 100, batchBytes, time.Since(start))
		accountIdx++
	}

	tracker.WriteStats(targetEntries, true)
	fmt.Printf("\nBadger write-only %s complete!\n", formatNum(targetEntries))
}
