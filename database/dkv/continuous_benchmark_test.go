package dkv

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
)

// TestContinuous_BlockchainGrowth runs an open-ended test simulating blockchain growth
// Profile: 90% write (add data), 10% read/operate on existing data
// Runs BadgerDB and BlockchainDB in parallel with periodic status reports
//
// Run with: go test -v -run TestContinuous_BlockchainGrowth -timeout 60m
// Press Ctrl+C to stop
func TestContinuous_BlockchainGrowth(t *testing.T) {
	fmt.Println("\n" + strings.Repeat("=", 90))
	fmt.Println("CONTINUOUS BLOCKCHAIN GROWTH BENCHMARK")
	fmt.Println("Profile: 90% writes (grow DB), 10% reads/operations")
	fmt.Println("Pattern: 100 tx hashes per account, accounts accessed 100x more than tx")
	fmt.Println(strings.Repeat("=", 90))

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

	// Shared state
	var (
		badgerStats = &dbStats{name: "BadgerDB"}
		bdbStats    = &dbStats{name: "BlockchainDB"}

		// Counters for data generation
		accountCounter atomic.Uint64
		txCounter      atomic.Uint64

		// Track existing keys for read operations
		accountKeys   [][]byte
		accountKeysMu sync.RWMutex
		txHashes      [][32]byte
		txHashesMu    sync.RWMutex

		stopCh = make(chan struct{})
		wg     sync.WaitGroup
	)

	// Report interval
	reportInterval := 3 * time.Minute
	startTime := time.Now()

	// Target entries
	const targetEntries = 100_000_000 // 100M entries

	fmt.Printf("\nTarget: %s entries\n", formatNum(targetEntries))
	fmt.Printf("Report interval: %v\n\n", reportInterval)

	// Print header
	fmt.Printf("%-8s | %-42s | %-42s\n", "Time", "BadgerDB", "BlockchainDB")
	fmt.Printf("%-8s | %-14s %-13s %-13s | %-14s %-13s %-13s\n",
		"", "Entries", "Ops/sec", "Disk", "Entries", "Ops/sec", "Disk")
	fmt.Println(strings.Repeat("-", 100))

	// Badger worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		runBadgerWorkload(badgerDB, badgerStats, &accountCounter, &txCounter,
			&accountKeys, &accountKeysMu, &txHashes, &txHashesMu, stopCh)
	}()

	// BDB worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		runBDBWorkload(bdb, bdbStats, &accountCounter, &txCounter,
			&accountKeys, &accountKeysMu, &txHashes, &txHashesMu, stopCh)
	}()

	// Reporter
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(reportInterval)
		defer ticker.Stop()

		lastBadgerOps := uint64(0)
		lastBdbOps := uint64(0)
		lastTime := time.Now()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				now := time.Now()
				elapsed := now.Sub(startTime).Round(time.Second)
				interval := now.Sub(lastTime).Seconds()

				// Calculate ops/sec
				badgerOps := badgerStats.totalOps.Load()
				bdbOps := bdbStats.totalOps.Load()
				badgerOpsPerSec := float64(badgerOps-lastBadgerOps) / interval
				bdbOpsPerSec := float64(bdbOps-lastBdbOps) / interval

				// Get disk sizes
				badgerDisk := getDirSize(badgerDir)
				bdbDisk := getDirSize(bdbDir)

				// Get entry counts
				badgerEntries := badgerStats.entries.Load()
				bdbEntries := bdbStats.entries.Load()

				fmt.Printf("%-8s | %-14s %-13s %-13s | %-14s %-13s %-13s\n",
					elapsed,
					formatNum(badgerEntries), fmt.Sprintf("%.0f/s", badgerOpsPerSec), formatBytes(badgerDisk),
					formatNum(bdbEntries), fmt.Sprintf("%.0f/s", bdbOpsPerSec), formatBytes(bdbDisk))

				lastBadgerOps = badgerOps
				lastBdbOps = bdbOps
				lastTime = now
			}
		}
	}()

	// Run until target entries reached
	fmt.Printf("Running until %s entries reached...\n\n", formatNum(targetEntries))

	// Check for completion
	checkTicker := time.NewTicker(1 * time.Second)
	defer checkTicker.Stop()

	for {
		select {
		case <-checkTicker.C:
			// Check if either DB reached target
			badgerEntries := badgerStats.entries.Load()
			bdbEntries := bdbStats.entries.Load()

			if badgerEntries >= targetEntries || bdbEntries >= targetEntries {
				fmt.Printf("\n*** Target reached! Badger: %s, BDB: %s ***\n",
					formatNum(badgerEntries), formatNum(bdbEntries))
				close(stopCh)
				wg.Wait()
				printFinalReport(badgerStats, bdbStats, badgerDir, bdbDir, startTime)
				return
			}
		}
	}
}

type dbStats struct {
	name     string
	entries  atomic.Uint64
	totalOps atomic.Uint64

	// Operation counts
	puts        atomic.Uint64
	gets        atomic.Uint64
	deletes     atomic.Uint64
	putsByHash  atomic.Uint64
	getsByHash  atomic.Uint64
	has         atomic.Uint64
	iterations  atomic.Uint64
	batchOps    atomic.Uint64

	// Timing
	putTime     atomic.Int64
	getTime     atomic.Int64
	batchTime   atomic.Int64
}

func runBadgerWorkload(db *badger.DB, stats *dbStats,
	accountCounter, txCounter *atomic.Uint64,
	accountKeys *[][]byte, accountKeysMu *sync.RWMutex,
	txHashes *[][32]byte, txHashesMu *sync.RWMutex,
	stopCh chan struct{}) {

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		// 90% write, 10% read/operate
		op := int(stats.totalOps.Load() % 100)

		if op < 90 {
			// WRITE: Add new data (simulating new block)
			start := time.Now()

			// Create batch of accounts and transactions
			db.Update(func(txn *badger.Txn) error {
				// Add 1 account
				accNum := accountCounter.Add(1)
				accKey := []byte(fmt.Sprintf("account:%012d", accNum))
				accVal := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d,"created":%d}`,
					accNum*1000, 0, time.Now().Unix()))
				txn.Set(accKey, accVal)

				accountKeysMu.Lock()
				*accountKeys = append(*accountKeys, accKey)
				accountKeysMu.Unlock()

				// Add 100 transactions for this account
				for i := 0; i < 100; i++ {
					txNum := txCounter.Add(1)
					txHash := sha256.Sum256([]byte(fmt.Sprintf("tx-%d-%d", accNum, txNum)))
					txVal := []byte(fmt.Sprintf(`{"from":"acc%d","to":"acc%d","amount":%d,"nonce":%d}`,
						accNum, (accNum+1)%1000, i*100, i))
					txn.Set(txHash[:], txVal)

					txHashesMu.Lock()
					*txHashes = append(*txHashes, txHash)
					txHashesMu.Unlock()
				}

				stats.entries.Add(101) // 1 account + 100 txs
				return nil
			})

			stats.puts.Add(101)
			stats.batchOps.Add(1)
			stats.putTime.Add(int64(time.Since(start)))

		} else {
			// READ/OPERATE: Work with existing data
			accountKeysMu.RLock()
			numAccounts := len(*accountKeys)
			accountKeysMu.RUnlock()

			txHashesMu.RLock()
			numTxs := len(*txHashes)
			txHashesMu.RUnlock()

			if numAccounts == 0 || numTxs == 0 {
				stats.totalOps.Add(1)
				continue
			}

			subOp := op % 10 // 0-9 within the 10% read operations
			start := time.Now()

			switch subOp {
			case 0, 1, 2, 3, 4: // 50% of reads: Get account (state read - most common)
				accountKeysMu.RLock()
				key := (*accountKeys)[int(stats.totalOps.Load())%numAccounts]
				accountKeysMu.RUnlock()

				db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(key)
					if err == nil {
						item.ValueCopy(nil)
					}
					return nil
				})
				stats.gets.Add(1)
				stats.getTime.Add(int64(time.Since(start)))

			case 5, 6: // 20% of reads: Get transaction by hash
				txHashesMu.RLock()
				hash := (*txHashes)[int(stats.totalOps.Load())%numTxs]
				txHashesMu.RUnlock()

				db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(hash[:])
					if err == nil {
						item.ValueCopy(nil)
					}
					return nil
				})
				stats.getsByHash.Add(1)

			case 7: // 10% of reads: Has check
				txHashesMu.RLock()
				hash := (*txHashes)[int(stats.totalOps.Load())%numTxs]
				txHashesMu.RUnlock()

				db.View(func(txn *badger.Txn) error {
					_, err := txn.Get(hash[:])
					_ = err == nil
					return nil
				})
				stats.has.Add(1)

			case 8: // 10% of reads: Prefix iteration (scan some accounts)
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

			case 9: // 10% of reads: Update account (state write)
				accountKeysMu.RLock()
				key := (*accountKeys)[int(stats.totalOps.Load())%numAccounts]
				accountKeysMu.RUnlock()

				db.Update(func(txn *badger.Txn) error {
					newVal := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d,"updated":%d}`,
						stats.totalOps.Load()*100, stats.totalOps.Load(), time.Now().Unix()))
					return txn.Set(key, newVal)
				})
				stats.puts.Add(1)
			}
		}

		stats.totalOps.Add(1)
	}
}

func runBDBWorkload(db *BlockchainStore, stats *dbStats,
	accountCounter, txCounter *atomic.Uint64,
	accountKeys *[][]byte, accountKeysMu *sync.RWMutex,
	txHashes *[][32]byte, txHashesMu *sync.RWMutex,
	stopCh chan struct{}) {

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		// 90% write, 10% read/operate
		op := int(stats.totalOps.Load() % 100)

		if op < 90 {
			// WRITE: Add new data using batch
			start := time.Now()
			batch := db.NewBatch()

			// Add 1 account
			accNum := accountCounter.Add(1)
			accKey := []byte(fmt.Sprintf("account:%012d", accNum))
			accVal := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d,"created":%d}`,
				accNum*1000, 0, time.Now().Unix()))
			batch.Put(accKey, accVal)

			accountKeysMu.Lock()
			*accountKeys = append(*accountKeys, accKey)
			accountKeysMu.Unlock()

			// Add 100 transactions using PutByHash
			for i := 0; i < 100; i++ {
				txNum := txCounter.Add(1)
				txHash := sha256.Sum256([]byte(fmt.Sprintf("tx-%d-%d", accNum, txNum)))
				txVal := []byte(fmt.Sprintf(`{"from":"acc%d","to":"acc%d","amount":%d,"nonce":%d}`,
					accNum, (accNum+1)%1000, i*100, i))
				batch.PutByHash(txVal)

				txHashesMu.Lock()
				*txHashes = append(*txHashes, txHash)
				txHashesMu.Unlock()
			}

			batch.Commit()
			stats.entries.Add(101)
			stats.puts.Add(1)
			stats.putsByHash.Add(100)
			stats.batchOps.Add(1)
			stats.putTime.Add(int64(time.Since(start)))

		} else {
			// READ/OPERATE: Work with existing data
			accountKeysMu.RLock()
			numAccounts := len(*accountKeys)
			accountKeysMu.RUnlock()

			txHashesMu.RLock()
			numTxs := len(*txHashes)
			txHashesMu.RUnlock()

			if numAccounts == 0 || numTxs == 0 {
				stats.totalOps.Add(1)
				continue
			}

			subOp := op % 10
			start := time.Now()

			switch subOp {
			case 0, 1, 2, 3, 4: // 50% of reads: Get account
				accountKeysMu.RLock()
				key := (*accountKeys)[int(stats.totalOps.Load())%numAccounts]
				accountKeysMu.RUnlock()

				db.Get(key)
				stats.gets.Add(1)
				stats.getTime.Add(int64(time.Since(start)))

			case 5, 6: // 20% of reads: GetByHash
				txHashesMu.RLock()
				hash := (*txHashes)[int(stats.totalOps.Load())%numTxs]
				txHashesMu.RUnlock()

				db.GetByHash(hash)
				stats.getsByHash.Add(1)

			case 7: // 10% of reads: Has check
				txHashesMu.RLock()
				hash := (*txHashes)[int(stats.totalOps.Load())%numTxs]
				txHashesMu.RUnlock()

				db.Has(hash)
				stats.has.Add(1)

			case 8: // 10% of reads: Prefix iteration
				count := 0
				db.ForEachPrefix([]byte("account:"), func(key, value []byte) error {
					count++
					if count >= 100 {
						return fmt.Errorf("stop")
					}
					return nil
				})
				stats.iterations.Add(1)

			case 9: // 10% of reads: Update account
				accountKeysMu.RLock()
				key := (*accountKeys)[int(stats.totalOps.Load())%numAccounts]
				accountKeysMu.RUnlock()

				newVal := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d,"updated":%d}`,
					stats.totalOps.Load()*100, stats.totalOps.Load(), time.Now().Unix()))
				db.Put(key, newVal)
				stats.puts.Add(1)
			}
		}

		stats.totalOps.Add(1)
	}
}

func printFinalReport(badgerStats, bdbStats *dbStats, badgerDir, bdbDir string, startTime time.Time) {
	elapsed := time.Since(startTime)

	fmt.Println("\n" + strings.Repeat("=", 90))
	fmt.Println("FINAL REPORT")
	fmt.Println(strings.Repeat("=", 90))

	fmt.Printf("\nTest Duration: %v\n", elapsed.Round(time.Second))

	// Disk usage
	badgerDisk := getDirSize(badgerDir)
	bdbDisk := getDirSize(bdbDir)

	fmt.Printf("\n%-25s %20s %20s\n", "Metric", "BadgerDB", "BlockchainDB")
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("%-25s %20s %20s\n", "Total Entries",
		formatNum(badgerStats.entries.Load()),
		formatNum(bdbStats.entries.Load()))

	fmt.Printf("%-25s %20s %20s\n", "Total Operations",
		formatNum(badgerStats.totalOps.Load()),
		formatNum(bdbStats.totalOps.Load()))

	fmt.Printf("%-25s %20.0f %20.0f\n", "Ops/second",
		float64(badgerStats.totalOps.Load())/elapsed.Seconds(),
		float64(bdbStats.totalOps.Load())/elapsed.Seconds())

	fmt.Printf("%-25s %20s %20s\n", "Disk Usage",
		formatBytes(badgerDisk),
		formatBytes(bdbDisk))

	// Memory
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	fmt.Printf("%-25s %20s\n", "Current Heap", formatBytes(int64(m.Alloc)))

	// Operation breakdown
	fmt.Printf("\n%-25s %20s %20s\n", "Operation Breakdown", "BadgerDB", "BlockchainDB")
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("%-25s %20s %20s\n", "Puts",
		formatNum(badgerStats.puts.Load()),
		formatNum(bdbStats.puts.Load()))

	fmt.Printf("%-25s %20s %20s\n", "PutsByHash",
		formatNum(badgerStats.putsByHash.Load()),
		formatNum(bdbStats.putsByHash.Load()))

	fmt.Printf("%-25s %20s %20s\n", "Gets",
		formatNum(badgerStats.gets.Load()),
		formatNum(bdbStats.gets.Load()))

	fmt.Printf("%-25s %20s %20s\n", "GetsByHash",
		formatNum(badgerStats.getsByHash.Load()),
		formatNum(bdbStats.getsByHash.Load()))

	fmt.Printf("%-25s %20s %20s\n", "Has Checks",
		formatNum(badgerStats.has.Load()),
		formatNum(bdbStats.has.Load()))

	fmt.Printf("%-25s %20s %20s\n", "Iterations",
		formatNum(badgerStats.iterations.Load()),
		formatNum(bdbStats.iterations.Load()))

	fmt.Printf("%-25s %20s %20s\n", "Batch Commits",
		formatNum(badgerStats.batchOps.Load()),
		formatNum(bdbStats.batchOps.Load()))

	// Performance comparison
	fmt.Printf("\n%-25s %20s\n", "WINNER ANALYSIS", "")
	fmt.Println(strings.Repeat("-", 70))

	if bdbStats.totalOps.Load() > badgerStats.totalOps.Load() {
		ratio := float64(bdbStats.totalOps.Load()) / float64(badgerStats.totalOps.Load())
		fmt.Printf("Throughput: BlockchainDB %.1fx faster\n", ratio)
	} else {
		ratio := float64(badgerStats.totalOps.Load()) / float64(bdbStats.totalOps.Load())
		fmt.Printf("Throughput: BadgerDB %.1fx faster\n", ratio)
	}

	if bdbDisk < badgerDisk {
		savings := float64(badgerDisk-bdbDisk) / float64(badgerDisk) * 100
		fmt.Printf("Disk Usage: BlockchainDB %.1f%% smaller (%s saved)\n",
			savings, formatBytes(badgerDisk-bdbDisk))
	} else {
		savings := float64(bdbDisk-badgerDisk) / float64(bdbDisk) * 100
		fmt.Printf("Disk Usage: BadgerDB %.1f%% smaller (%s saved)\n",
			savings, formatBytes(bdbDisk-badgerDisk))
	}

	// BDB specific stats
	dbStats := bdbStats
	_ = dbStats // Use later if needed

	fmt.Println()
}

func formatNum(n uint64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	} else if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	} else if n < 1000000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	return fmt.Sprintf("%.1fB", float64(n)/1000000000)
}
