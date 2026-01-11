package dkv

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
)

// TestBadger_10M runs 10 million entry benchmark
func TestBadger_10M(t *testing.T) {
	runBadgerBenchmark(t, 10_000_000)
}

// TestBadger_100M runs 100 million entry benchmark
func TestBadger_100M(t *testing.T) {
	runBadgerBenchmark(t, 100_000_000)
}

// TestBadger_200M runs 200 million entry benchmark
func TestBadger_200M(t *testing.T) {
	runBadgerBenchmark(t, 200_000_000)
}

// TestBadger_500M runs 500 million entry benchmark
func TestBadger_500M(t *testing.T) {
	runBadgerBenchmark(t, 500_000_000)
}

func runBadgerBenchmark(t *testing.T, targetEntries uint64) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Printf("BADGER %s BENCHMARK\n", formatNum(targetEntries))
	fmt.Println(strings.Repeat("=", 60))

	const (
		statsInterval = 20 * time.Second
		batchSize     = 101 // 1 account + 100 txs
	)

	baseDir := t.TempDir()
	badgerDir := filepath.Join(baseDir, "badger")
	logPath := "/tmp/badger_benchmark.log"

	// Delete old log file to start fresh
	os.Remove(logPath)

	tracker, err := NewPerfTracker("BadgerDB", logPath, badgerDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	// Open Badger with async writes (like LevelDB/BDB) for fair comparison
	badgerOpts := badger.DefaultOptions(badgerDir)
	badgerOpts.Logger = nil
	badgerOpts.SyncWrites = false
	db, err := badger.Open(badgerOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fmt.Printf("Target: %s entries\n", formatNum(targetEntries))
	fmt.Printf("Stats log: %s\n", logPath)
	fmt.Printf("Data dir: %s\n\n", badgerDir)

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
				db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(key)
					if err == nil {
						item.ValueCopy(nil)
					}
					return nil
				})
				tracker.AddRead("get", time.Since(start))
			} else {
				txHash := generateTxHash(randAccount, opCounter%100)
				db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(txHash[:])
					if err == nil {
						item.ValueCopy(nil)
					}
					return nil
				})
				tracker.AddRead("hash", time.Since(start))
			}
		}
	}

	tracker.WriteStats(targetEntries, true)
	fmt.Printf("\nBadger %s benchmark complete!\n", formatNum(targetEntries))
}
