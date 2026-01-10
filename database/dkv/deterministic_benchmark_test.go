package dkv

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
)

// Given a global entry index, return account index and whether it's an account or tx
func entryToAccountTx(entryIdx uint64) (accountIdx uint64, txIdx uint64, isAccount bool) {
	// Pattern: 1 account + 100 txs = 101 entries per account
	accountIdx = entryIdx / 101
	remainder := entryIdx % 101
	if remainder == 0 {
		return accountIdx, 0, true // This is an account entry
	}
	return accountIdx, remainder - 1, false // This is a tx entry (0-99)
}

// TestDeterministic_100M runs both databases with identical deterministic data
// Both must reach 100M entries, then verify data integrity
func TestDeterministic_100M(t *testing.T) {
	fmt.Println("\n" + strings.Repeat("=", 95))
	fmt.Println("DETERMINISTIC 100M BENCHMARK")
	fmt.Println("Both databases receive identical data, verified at completion")
	fmt.Println(strings.Repeat("=", 95))

	const (
		targetEntries  = 100_000_000
		reportInterval = 3 * time.Minute
		batchSize      = 101 // 1 account + 100 txs
	)

	baseDir := t.TempDir()
	badgerDir := filepath.Join(baseDir, "badger")
	bdbDir := filepath.Join(baseDir, "bdb")

	// Open databases
	badgerOpts := badger.DefaultOptions(badgerDir)
	badgerOpts.Logger = nil
	badgerDB, err := badger.Open(badgerOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer badgerDB.Close()

	bdb, err := NewBlockchainStore(bdbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()

	// Stats tracking
	badgerStats := newPerfStats("BadgerDB")
	bdbStats := newPerfStats("BlockchainDB")

	// Shared entry counter - both DBs process same entries
	var currentEntry atomic.Uint64
	var badgerDone, bdbDone atomic.Bool
	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	startTime := time.Now()

	fmt.Printf("\nTarget: %s entries per database\n", formatNum(targetEntries))
	fmt.Printf("Pattern: 1 account + 100 txs per batch (101 entries)\n")
	fmt.Printf("Report interval: %v\n", reportInterval)

	// Badger worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		runDeterministicBadger(badgerDB, badgerStats, &currentEntry, targetEntries, batchSize, stopCh, &badgerDone)
	}()

	// BDB worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		runDeterministicBDB(bdb, bdbStats, &currentEntry, targetEntries, batchSize, stopCh, &bdbDone)
	}()

	// Reporter with progress updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		reportTicker := time.NewTicker(reportInterval)
		progressTicker := time.NewTicker(10 * time.Second)
		defer reportTicker.Stop()
		defer progressTicker.Stop()

		lastReportTime := time.Now()

		for {
			select {
			case <-stopCh:
				return
			case <-reportTicker.C:
				fmt.Println() // Clear the progress line
				printPerformanceReport(badgerStats, bdbStats, badgerDir, bdbDir, startTime)
				lastReportTime = time.Now()
			case <-progressTicker.C:
				// Show progress with countdown to next report
				badgerEntries := badgerStats.entries.Load()
				bdbEntries := bdbStats.entries.Load()
				elapsed := time.Since(startTime).Round(time.Second)
				untilReport := reportInterval - time.Since(lastReportTime)
				if untilReport < 0 {
					untilReport = 0
				}

				badgerPct := float64(badgerEntries) / float64(targetEntries) * 100
				bdbPct := float64(bdbEntries) / float64(targetEntries) * 100

				fmt.Printf("\r[%v] Badger: %s (%.1f%%) | BDB: %s (%.1f%%) | Next report: %v   ",
					elapsed, formatNum(badgerEntries), badgerPct,
					formatNum(bdbEntries), bdbPct,
					untilReport.Round(time.Second))
			}
		}
	}()

	// Wait for both to complete
	for {
		time.Sleep(1 * time.Second)
		if badgerDone.Load() && bdbDone.Load() {
			break
		}
	}

	close(stopCh)
	wg.Wait()

	// Final report
	fmt.Println("\n" + strings.Repeat("=", 95))
	fmt.Println("FINAL PERFORMANCE REPORT")
	fmt.Println(strings.Repeat("=", 95))
	printPerformanceReport(badgerStats, bdbStats, badgerDir, bdbDir, startTime)

	// Verify data integrity
	fmt.Println("\n" + strings.Repeat("=", 95))
	fmt.Println("DATA INTEGRITY VERIFICATION")
	fmt.Println(strings.Repeat("=", 95))
	verifyDataIntegrity(t, badgerDB, bdb, targetEntries)
}

type perfStats struct {
	name string

	// Entry counts
	entries      atomic.Uint64
	accounts     atomic.Uint64
	transactions atomic.Uint64

	// Operation counts
	puts       atomic.Uint64
	gets       atomic.Uint64
	getsByHash atomic.Uint64
	has        atomic.Uint64
	iterations atomic.Uint64
	deletes    atomic.Uint64
	batches    atomic.Uint64

	// Timing (nanoseconds)
	writeTime atomic.Int64
	readTime  atomic.Int64

	// For ops/sec calculation
	lastEntries uint64
	lastTime    time.Time
}

func newPerfStats(name string) *perfStats {
	return &perfStats{
		name:     name,
		lastTime: time.Now(),
	}
}

func runDeterministicBadger(db *badger.DB, stats *perfStats, currentEntry *atomic.Uint64,
	targetEntries uint64, batchSize int, stopCh chan struct{}, done *atomic.Bool) {

	localEntry := uint64(0)

	for localEntry < targetEntries {
		select {
		case <-stopCh:
			return
		default:
		}

		// Determine operation: 90% write, 10% read
		op := localEntry % 100

		if op < 90 {
			// WRITE: Add next batch of entries
			start := time.Now()
			entryIdx := currentEntry.Add(uint64(batchSize)) - uint64(batchSize)

			err := db.Update(func(txn *badger.Txn) error {
				accountIdx := entryIdx / 101

				// Add account
				accKey := generateAccountKey(accountIdx)
				accVal := generateAccountValue(accountIdx)
				if err := txn.Set(accKey, accVal); err != nil {
					return err
				}

				// Add 100 transactions
				for i := uint64(0); i < 100; i++ {
					txHash := generateTxHash(accountIdx, i)
					txVal := generateTxValue(accountIdx, i)
					if err := txn.Set(txHash[:], txVal); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				continue
			}

			stats.entries.Add(uint64(batchSize))
			stats.accounts.Add(1)
			stats.transactions.Add(100)
			stats.puts.Add(uint64(batchSize))
			stats.batches.Add(1)
			stats.writeTime.Add(int64(time.Since(start)))
			localEntry += uint64(batchSize)

		} else {
			// READ: Query existing data
			if stats.entries.Load() < 101 {
				localEntry++
				continue
			}

			start := time.Now()
			maxAccount := stats.accounts.Load()
			if maxAccount == 0 {
				localEntry++
				continue
			}

			// Deterministic "random" based on localEntry
			randAccount := localEntry % maxAccount
			subOp := (localEntry / 100) % 5

			switch subOp {
			case 0, 1: // Get account
				key := generateAccountKey(randAccount)
				db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(key)
					if err == nil {
						item.ValueCopy(nil)
					}
					return nil
				})
				stats.gets.Add(1)

			case 2: // Get tx by hash
				txHash := generateTxHash(randAccount, localEntry%100)
				db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(txHash[:])
					if err == nil {
						item.ValueCopy(nil)
					}
					return nil
				})
				stats.getsByHash.Add(1)

			case 3: // Has check
				txHash := generateTxHash(randAccount, localEntry%100)
				db.View(func(txn *badger.Txn) error {
					_, err := txn.Get(txHash[:])
					_ = err == nil
					return nil
				})
				stats.has.Add(1)

			case 4: // Prefix iteration
				db.View(func(txn *badger.Txn) error {
					opts := badger.DefaultIteratorOptions
					opts.Prefix = []byte("account:")
					opts.PrefetchSize = 10
					it := txn.NewIterator(opts)
					defer it.Close()
					count := 0
					for it.Rewind(); it.Valid() && count < 100; it.Next() {
						count++
					}
					return nil
				})
				stats.iterations.Add(1)
			}

			stats.readTime.Add(int64(time.Since(start)))
			localEntry++
		}
	}

	done.Store(true)
}

func runDeterministicBDB(db *BlockchainStore, stats *perfStats, currentEntry *atomic.Uint64,
	targetEntries uint64, batchSize int, stopCh chan struct{}, done *atomic.Bool) {

	localEntry := uint64(0)

	for localEntry < targetEntries {
		select {
		case <-stopCh:
			return
		default:
		}

		// Determine operation: 90% write, 10% read
		op := localEntry % 100

		if op < 90 {
			// WRITE: Add next batch using Batch API
			start := time.Now()
			entryIdx := currentEntry.Add(uint64(batchSize)) - uint64(batchSize)

			batch := db.NewBatch()
			accountIdx := entryIdx / 101

			// Add account
			accKey := generateAccountKey(accountIdx)
			accVal := generateAccountValue(accountIdx)
			batch.Put(accKey, accVal)

			// Add 100 transactions using PutByHash
			for i := uint64(0); i < 100; i++ {
				txVal := generateTxValue(accountIdx, i)
				batch.PutByHash(txVal)
			}

			batch.Commit()

			stats.entries.Add(uint64(batchSize))
			stats.accounts.Add(1)
			stats.transactions.Add(100)
			stats.puts.Add(1)
			stats.batches.Add(1)
			stats.writeTime.Add(int64(time.Since(start)))
			localEntry += uint64(batchSize)

		} else {
			// READ: Query existing data
			if stats.entries.Load() < 101 {
				localEntry++
				continue
			}

			start := time.Now()
			maxAccount := stats.accounts.Load()
			if maxAccount == 0 {
				localEntry++
				continue
			}

			// Deterministic "random" based on localEntry
			randAccount := localEntry % maxAccount
			subOp := (localEntry / 100) % 5

			switch subOp {
			case 0, 1: // Get account
				key := generateAccountKey(randAccount)
				db.Get(key)
				stats.gets.Add(1)

			case 2: // Get tx by hash
				txHash := generateTxHash(randAccount, localEntry%100)
				db.GetByHash(txHash)
				stats.getsByHash.Add(1)

			case 3: // Has check
				txHash := generateTxHash(randAccount, localEntry%100)
				db.Has(txHash)
				stats.has.Add(1)

			case 4: // Prefix iteration
				count := 0
				db.ForEachPrefix([]byte("account:"), func(key, value []byte) error {
					count++
					if count >= 100 {
						return fmt.Errorf("stop")
					}
					return nil
				})
				stats.iterations.Add(1)
			}

			stats.readTime.Add(int64(time.Since(start)))
			localEntry++
		}
	}

	done.Store(true)
}

func printPerformanceReport(badgerStats, bdbStats *perfStats, badgerDir, bdbDir string, startTime time.Time) {
	elapsed := time.Since(startTime)

	// Calculate ops/sec
	badgerEntries := badgerStats.entries.Load()
	bdbEntries := bdbStats.entries.Load()

	badgerOpsPerSec := float64(badgerEntries-badgerStats.lastEntries) / elapsed.Seconds()
	bdbOpsPerSec := float64(bdbEntries-bdbStats.lastEntries) / elapsed.Seconds()

	if badgerStats.lastEntries > 0 {
		interval := time.Since(badgerStats.lastTime).Seconds()
		badgerOpsPerSec = float64(badgerEntries-badgerStats.lastEntries) / interval
		bdbOpsPerSec = float64(bdbEntries-bdbStats.lastEntries) / interval
	}

	badgerStats.lastEntries = badgerEntries
	badgerStats.lastTime = time.Now()
	bdbStats.lastEntries = bdbEntries
	bdbStats.lastTime = time.Now()

	// Disk sizes
	badgerDisk := getDirSize(badgerDir)
	bdbDisk := getDirSize(bdbDir)

	// Memory
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("\n┌─────────────────────────────────────────────────────────────────────────────────────────┐\n")
	fmt.Printf("│ PERFORMANCE REPORT @ %v                                                         │\n", elapsed.Round(time.Second))
	fmt.Printf("├─────────────────────────────────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "METRIC", "BadgerDB", "BlockchainDB")
	fmt.Printf("├─────────────────────────────────────────────────────────────────────────────────────────┤\n")

	// Entries
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Total Entries",
		formatNum(badgerEntries), formatNum(bdbEntries))
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "  Accounts",
		formatNum(badgerStats.accounts.Load()), formatNum(bdbStats.accounts.Load()))
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "  Transactions",
		formatNum(badgerStats.transactions.Load()), formatNum(bdbStats.transactions.Load()))

	fmt.Printf("├─────────────────────────────────────────────────────────────────────────────────────────┤\n")

	// Performance
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Entries/second",
		fmt.Sprintf("%.0f", badgerOpsPerSec), fmt.Sprintf("%.0f", bdbOpsPerSec))
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Disk Usage",
		formatBytes(badgerDisk), formatBytes(bdbDisk))

	if badgerEntries > 0 && bdbEntries > 0 {
		badgerBytesPerEntry := float64(badgerDisk) / float64(badgerEntries)
		bdbBytesPerEntry := float64(bdbDisk) / float64(bdbEntries)
		fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Bytes/Entry",
			fmt.Sprintf("%.1f B", badgerBytesPerEntry), fmt.Sprintf("%.1f B", bdbBytesPerEntry))
	}

	fmt.Printf("├─────────────────────────────────────────────────────────────────────────────────────────┤\n")

	// Operations breakdown
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Batches Committed",
		formatNum(badgerStats.batches.Load()), formatNum(bdbStats.batches.Load()))
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Gets (by key)",
		formatNum(badgerStats.gets.Load()), formatNum(bdbStats.gets.Load()))
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Gets (by hash)",
		formatNum(badgerStats.getsByHash.Load()), formatNum(bdbStats.getsByHash.Load()))
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Has Checks",
		formatNum(badgerStats.has.Load()), formatNum(bdbStats.has.Load()))
	fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Iterations",
		formatNum(badgerStats.iterations.Load()), formatNum(bdbStats.iterations.Load()))

	fmt.Printf("├─────────────────────────────────────────────────────────────────────────────────────────┤\n")

	// Timing
	if badgerStats.batches.Load() > 0 {
		badgerAvgWrite := time.Duration(badgerStats.writeTime.Load() / int64(badgerStats.batches.Load()))
		bdbAvgWrite := time.Duration(bdbStats.writeTime.Load() / int64(bdbStats.batches.Load()))
		fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Avg Batch Write Time",
			badgerAvgWrite.Round(time.Microsecond), bdbAvgWrite.Round(time.Microsecond))
	}

	totalReads := badgerStats.gets.Load() + badgerStats.getsByHash.Load() + badgerStats.has.Load()
	if totalReads > 0 {
		badgerAvgRead := time.Duration(badgerStats.readTime.Load() / int64(totalReads))
		bdbTotalReads := bdbStats.gets.Load() + bdbStats.getsByHash.Load() + bdbStats.has.Load()
		bdbAvgRead := time.Duration(bdbStats.readTime.Load() / int64(bdbTotalReads))
		fmt.Printf("│ %-30s │ %20s │ %20s │\n", "Avg Read Time",
			badgerAvgRead.Round(time.Microsecond), bdbAvgRead.Round(time.Microsecond))
	}

	fmt.Printf("├─────────────────────────────────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│ %-30s │ %20s │                      │\n", "Heap Memory", formatBytes(int64(m.Alloc)))
	fmt.Printf("└─────────────────────────────────────────────────────────────────────────────────────────┘\n")

	// Winner analysis
	if bdbEntries > badgerEntries {
		ratio := float64(bdbEntries) / float64(badgerEntries)
		fmt.Printf("\n→ BlockchainDB is %.1fx ahead in entries\n", ratio)
	} else if badgerEntries > bdbEntries {
		ratio := float64(badgerEntries) / float64(bdbEntries)
		fmt.Printf("\n→ BadgerDB is %.1fx ahead in entries\n", ratio)
	}
}

func verifyDataIntegrity(t *testing.T, badgerDB *badger.DB, bdb *BlockchainStore, totalEntries uint64) {
	fmt.Println("\nVerifying random sample of entries...")

	// Use deterministic seed for reproducible verification
	rng := rand.New(rand.NewSource(42))

	numSamples := 1000
	if totalEntries < 1000 {
		numSamples = int(totalEntries / 10)
	}

	verified := 0
	mismatches := 0

	for i := 0; i < numSamples; i++ {
		// Random entry index
		entryIdx := uint64(rng.Int63n(int64(totalEntries)))
		accountIdx, txIdx, isAccount := entryToAccountTx(entryIdx)

		if isAccount {
			// Verify account
			key := generateAccountKey(accountIdx)
			expectedVal := generateAccountValue(accountIdx)

			// Get from Badger
			var badgerVal []byte
			badgerDB.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				badgerVal, _ = item.ValueCopy(nil)
				return nil
			})

			// Get from BDB
			bdbVal, _ := bdb.Get(key)

			if string(badgerVal) != string(expectedVal) {
				fmt.Printf("  MISMATCH: Badger account %d: expected %q, got %q\n",
					accountIdx, expectedVal, badgerVal)
				mismatches++
			} else if string(bdbVal) != string(expectedVal) {
				fmt.Printf("  MISMATCH: BDB account %d: expected %q, got %q\n",
					accountIdx, expectedVal, bdbVal)
				mismatches++
			} else {
				verified++
			}
		} else {
			// Verify transaction by hash
			txHash := generateTxHash(accountIdx, txIdx)
			expectedVal := generateTxValue(accountIdx, txIdx)

			// Get from Badger
			var badgerVal []byte
			badgerDB.View(func(txn *badger.Txn) error {
				item, err := txn.Get(txHash[:])
				if err != nil {
					return err
				}
				badgerVal, _ = item.ValueCopy(nil)
				return nil
			})

			// Get from BDB by hash
			bdbVal, _ := bdb.GetByHash(txHash)

			if string(badgerVal) != string(expectedVal) {
				fmt.Printf("  MISMATCH: Badger tx %d/%d\n", accountIdx, txIdx)
				mismatches++
			} else if string(bdbVal) != string(expectedVal) {
				fmt.Printf("  MISMATCH: BDB tx %d/%d\n", accountIdx, txIdx)
				mismatches++
			} else {
				verified++
			}
		}
	}

	fmt.Printf("\n┌─────────────────────────────────────────────────────────────────────────────────────────┐\n")
	fmt.Printf("│ VERIFICATION RESULTS                                                                    │\n")
	fmt.Printf("├─────────────────────────────────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│ Samples verified: %d                                                                  │\n", numSamples)
	fmt.Printf("│ Passed: %d                                                                            │\n", verified)
	fmt.Printf("│ Mismatches: %d                                                                        │\n", mismatches)
	fmt.Printf("└─────────────────────────────────────────────────────────────────────────────────────────┘\n")

	if mismatches > 0 {
		t.Errorf("Data integrity check failed: %d mismatches out of %d samples", mismatches, numSamples)
	} else {
		fmt.Println("\n✓ All samples verified - databases contain identical data")
	}
}
