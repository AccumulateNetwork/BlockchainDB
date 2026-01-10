package dkv

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
)

// Performance comparison between BlockchainDB and BadgerDB
// Run with: go test -v -run TestPerformance -timeout 10m

func TestPerformance_Comparison(t *testing.T) {
	sizes := []int{100, 1000, 10000, 100000}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("PERFORMANCE COMPARISON: BlockchainDB vs BadgerDB")
	fmt.Println(strings.Repeat("=", 80) + "\n")

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
			runComparison(t, size)
		})
	}
}

func runComparison(t *testing.T, numKeys int) {
	baseDir := t.TempDir()
	badgerDir := filepath.Join(baseDir, "badger")
	bdbDir := filepath.Join(baseDir, "bdb")

	// Prepare test data
	keys := make([][]byte, numKeys)
	values := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		values[i] = []byte(fmt.Sprintf("value-%08d-padding-to-make-it-longer", i))
	}

	fmt.Printf("\n--- %d Keys ---\n", numKeys)
	fmt.Printf("%-20s %15s %15s %15s\n", "Operation", "BadgerDB", "BlockchainDB", "Winner")
	fmt.Println(strings.Repeat("-", 70))

	// Sequential Writes
	badgerWriteTime := benchBadgerSequentialWrite(t, badgerDir, keys, values)
	bdbWriteTime := benchBDBSequentialWrite(t, bdbDir, keys, values)
	printResult("Sequential Write", badgerWriteTime, bdbWriteTime)

	// Open existing DBs for read tests
	badgerDB := openBadgerForBench(t, badgerDir)
	defer badgerDB.Close()
	bdb := openBDBForBench(t, bdbDir)
	defer bdb.Close()

	// Sequential Reads
	badgerReadTime := benchBadgerSequentialRead(t, badgerDB, keys)
	bdbReadTime := benchBDBSequentialRead(t, bdb, keys)
	printResult("Sequential Read", badgerReadTime, bdbReadTime)

	// Random Reads (read keys in different order)
	randomKeys := shuffleKeys(keys)
	badgerRandomTime := benchBadgerSequentialRead(t, badgerDB, randomKeys)
	bdbRandomTime := benchBDBSequentialRead(t, bdb, randomKeys)
	printResult("Random Read", badgerRandomTime, bdbRandomTime)

	// Prefix Iteration (scan 10% of keys)
	prefix := []byte("key-0000")
	badgerIterTime := benchBadgerPrefixIteration(t, badgerDB, prefix)
	bdbIterTime := benchBDBPrefixIteration(t, bdb, prefix)
	printResult("Prefix Iteration", badgerIterTime, bdbIterTime)

	// Full Iteration
	badgerFullIterTime := benchBadgerFullIteration(t, badgerDB)
	bdbFullIterTime := benchBDBFullIteration(t, bdb)
	printResult("Full Iteration", badgerFullIterTime, bdbFullIterTime)

	// Close and reopen (persistence test)
	badgerDB.Close()
	bdb.Close()

	badgerReopenTime := benchBadgerReopen(t, badgerDir)
	bdbReopenTime := benchBDBReopen(t, bdbDir)
	printResult("Reopen DB", badgerReopenTime, bdbReopenTime)

	// Mixed workload: 50% reads, 50% writes
	badgerDB2 := openBadgerForBench(t, badgerDir)
	defer badgerDB2.Close()
	bdb2 := openBDBForBench(t, bdbDir)
	defer bdb2.Close()

	badgerMixedTime := benchBadgerMixed(t, badgerDB2, keys, values)
	bdbMixedTime := benchBDBMixed(t, bdb2, keys, values)
	printResult("Mixed Read/Write", badgerMixedTime, bdbMixedTime)

	fmt.Println()
}

func printResult(op string, badgerTime, bdbTime time.Duration) {
	winner := "TIE"
	if badgerTime < bdbTime {
		ratio := float64(bdbTime) / float64(badgerTime)
		winner = fmt.Sprintf("Badger %.1fx", ratio)
	} else if bdbTime < badgerTime {
		ratio := float64(badgerTime) / float64(bdbTime)
		winner = fmt.Sprintf("BDB %.1fx", ratio)
	}
	fmt.Printf("%-20s %15s %15s %15s\n", op, badgerTime.Round(time.Microsecond), bdbTime.Round(time.Microsecond), winner)
}

// Badger benchmarks

func benchBadgerSequentialWrite(t *testing.T, dir string, keys, values [][]byte) time.Duration {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	start := time.Now()
	err = db.Update(func(txn *badger.Txn) error {
		for i := range keys {
			if err := txn.Set(keys[i], values[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return time.Since(start)
}

func benchBadgerSequentialRead(t *testing.T, db *badger.DB, keys [][]byte) time.Duration {
	start := time.Now()
	err := db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			_, err = item.ValueCopy(nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return time.Since(start)
}

func benchBadgerPrefixIteration(t *testing.T, db *badger.DB, prefix []byte) time.Duration {
	start := time.Now()
	count := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			_, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return time.Since(start)
}

func benchBadgerFullIteration(t *testing.T, db *badger.DB) time.Duration {
	start := time.Now()
	count := 0
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			_, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return time.Since(start)
}

func benchBadgerReopen(t *testing.T, dir string) time.Duration {
	start := time.Now()
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	return time.Since(start)
}

func benchBadgerMixed(t *testing.T, db *badger.DB, keys, values [][]byte) time.Duration {
	start := time.Now()
	numOps := len(keys)
	if numOps > 1000 {
		numOps = 1000 // Cap mixed ops
	}

	for i := 0; i < numOps; i++ {
		if i%2 == 0 {
			// Read
			db.View(func(txn *badger.Txn) error {
				item, _ := txn.Get(keys[i])
				if item != nil {
					item.ValueCopy(nil)
				}
				return nil
			})
		} else {
			// Write
			db.Update(func(txn *badger.Txn) error {
				return txn.Set(keys[i], values[i])
			})
		}
	}
	return time.Since(start)
}

// BlockchainDB benchmarks

func benchBDBSequentialWrite(t *testing.T, dir string, keys, values [][]byte) time.Duration {
	db, err := NewBlockchainStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	start := time.Now()
	for i := range keys {
		if err := db.Put(keys[i], values[i]); err != nil {
			t.Fatal(err)
		}
	}
	return time.Since(start)
}

func benchBDBSequentialRead(t *testing.T, db *BlockchainStore, keys [][]byte) time.Duration {
	start := time.Now()
	for _, key := range keys {
		_, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
	}
	return time.Since(start)
}

func benchBDBPrefixIteration(t *testing.T, db *BlockchainStore, prefix []byte) time.Duration {
	start := time.Now()
	count := 0
	db.ForEachPrefix(prefix, func(key, value []byte) error {
		count++
		return nil
	})
	return time.Since(start)
}

func benchBDBFullIteration(t *testing.T, db *BlockchainStore) time.Duration {
	start := time.Now()
	count := 0
	db.ForEach(func(key, value []byte) error {
		count++
		return nil
	})
	return time.Since(start)
}

func benchBDBReopen(t *testing.T, dir string) time.Duration {
	start := time.Now()
	db, err := NewBlockchainStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	return time.Since(start)
}

func benchBDBMixed(t *testing.T, db *BlockchainStore, keys, values [][]byte) time.Duration {
	start := time.Now()
	numOps := len(keys)
	if numOps > 1000 {
		numOps = 1000 // Cap mixed ops
	}

	for i := 0; i < numOps; i++ {
		if i%2 == 0 {
			// Read
			db.Get(keys[i])
		} else {
			// Write
			db.Put(keys[i], values[i])
		}
	}
	return time.Since(start)
}

// Helpers

func openBadgerForBench(t *testing.T, dir string) *badger.DB {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func openBDBForBench(t *testing.T, dir string) *BlockchainStore {
	db, err := NewBlockchainStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func shuffleKeys(keys [][]byte) [][]byte {
	shuffled := make([][]byte, len(keys))
	copy(shuffled, keys)
	// Simple deterministic shuffle
	for i := len(shuffled) - 1; i > 0; i-- {
		j := (i * 7) % (i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	return shuffled
}

// Benchmark functions for go test -bench

func BenchmarkBadger_Write_100(b *testing.B)    { benchmarkBadgerWrite(b, 100) }
func BenchmarkBadger_Write_1000(b *testing.B)   { benchmarkBadgerWrite(b, 1000) }
func BenchmarkBadger_Write_10000(b *testing.B)  { benchmarkBadgerWrite(b, 10000) }
func BenchmarkBDB_Write_100(b *testing.B)       { benchmarkBDBWrite(b, 100) }
func BenchmarkBDB_Write_1000(b *testing.B)      { benchmarkBDBWrite(b, 1000) }
func BenchmarkBDB_Write_10000(b *testing.B)     { benchmarkBDBWrite(b, 10000) }

func BenchmarkBadger_Read_100(b *testing.B)     { benchmarkBadgerRead(b, 100) }
func BenchmarkBadger_Read_1000(b *testing.B)    { benchmarkBadgerRead(b, 1000) }
func BenchmarkBadger_Read_10000(b *testing.B)   { benchmarkBadgerRead(b, 10000) }
func BenchmarkBDB_Read_100(b *testing.B)        { benchmarkBDBRead(b, 100) }
func BenchmarkBDB_Read_1000(b *testing.B)       { benchmarkBDBRead(b, 1000) }
func BenchmarkBDB_Read_10000(b *testing.B)      { benchmarkBDBRead(b, 10000) }

func benchmarkBadgerWrite(b *testing.B, numKeys int) {
	keys, values := generateTestData(numKeys)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		opts := badger.DefaultOptions(dir)
		opts.Logger = nil
		db, _ := badger.Open(opts)
		db.Update(func(txn *badger.Txn) error {
			for j := range keys {
				txn.Set(keys[j], values[j])
			}
			return nil
		})
		db.Close()
	}
}

func benchmarkBDBWrite(b *testing.B, numKeys int) {
	keys, values := generateTestData(numKeys)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		db, _ := NewBlockchainStore(dir)
		for j := range keys {
			db.Put(keys[j], values[j])
		}
		db.Close()
	}
}

func benchmarkBadgerRead(b *testing.B, numKeys int) {
	keys, values := generateTestData(numKeys)
	dir := b.TempDir()

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, _ := badger.Open(opts)
	db.Update(func(txn *badger.Txn) error {
		for j := range keys {
			txn.Set(keys[j], values[j])
		}
		return nil
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.View(func(txn *badger.Txn) error {
			for _, key := range keys {
				item, _ := txn.Get(key)
				if item != nil {
					item.ValueCopy(nil)
				}
			}
			return nil
		})
	}
	db.Close()
}

func benchmarkBDBRead(b *testing.B, numKeys int) {
	keys, values := generateTestData(numKeys)
	dir := b.TempDir()

	db, _ := NewBlockchainStore(dir)
	for j := range keys {
		db.Put(keys[j], values[j])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			db.Get(key)
		}
	}
	db.Close()
}

func generateTestData(numKeys int) ([][]byte, [][]byte) {
	keys := make([][]byte, numKeys)
	values := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		values[i] = []byte(fmt.Sprintf("value-%08d-padding", i))
	}
	return keys, values
}

// TestPerformance_BlockchainWorkload simulates realistic blockchain access patterns:
// - 1M total entries
// - 100 hash-keyed values (transactions) per 1 state key (account)
// - State keys accessed 100x more frequently than hash keys
func TestPerformance_BlockchainWorkload(t *testing.T) {
	const (
		numStateKeys    = 10000           // Account state entries (r/w)
		hashKeysPerState = 100            // Transaction hashes per account
		totalHashKeys   = numStateKeys * hashKeysPerState // 1M hash keys
		stateReadRatio  = 100             // Read state 100x per hash read
	)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("BLOCKCHAIN WORKLOAD SIMULATION")
	fmt.Printf("State keys: %d, Hash keys: %d (total: %d)\n", numStateKeys, totalHashKeys, numStateKeys+totalHashKeys)
	fmt.Printf("Access pattern: %d state reads per 1 hash read\n", stateReadRatio)
	fmt.Println(strings.Repeat("=", 80))

	baseDir := t.TempDir()
	badgerDir := filepath.Join(baseDir, "badger")
	bdbDir := filepath.Join(baseDir, "bdb")

	// Generate test data
	fmt.Println("\nGenerating test data...")
	stateKeys := make([][]byte, numStateKeys)
	stateValues := make([][]byte, numStateKeys)
	for i := 0; i < numStateKeys; i++ {
		stateKeys[i] = []byte(fmt.Sprintf("account:%08d", i))
		stateValues[i] = []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d}`, i*1000, i))
	}

	// Hash keys simulate transaction hashes (32 bytes)
	hashKeys := make([][32]byte, totalHashKeys)
	hashValues := make([][]byte, totalHashKeys)
	for i := 0; i < totalHashKeys; i++ {
		// Simulate a hash key
		h := sha256.Sum256([]byte(fmt.Sprintf("tx-%d", i)))
		hashKeys[i] = h
		hashValues[i] = []byte(fmt.Sprintf(`{"from":"addr%d","to":"addr%d","amount":%d}`, i%1000, (i+1)%1000, i))
	}

	fmt.Printf("%-25s %15s %15s %15s\n", "Operation", "BadgerDB", "BlockchainDB", "Winner")
	fmt.Println(strings.Repeat("-", 75))

	// === WRITE PHASE ===

	// Badger: Write all data
	badgerWriteTime := func() time.Duration {
		opts := badger.DefaultOptions(badgerDir)
		opts.Logger = nil
		db, err := badger.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		start := time.Now()

		// Write state keys
		err = db.Update(func(txn *badger.Txn) error {
			for i := range stateKeys {
				if err := txn.Set(stateKeys[i], stateValues[i]); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		// Write hash keys in batches
		batchSize := 10000
		for start := 0; start < totalHashKeys; start += batchSize {
			end := start + batchSize
			if end > totalHashKeys {
				end = totalHashKeys
			}
			err = db.Update(func(txn *badger.Txn) error {
				for i := start; i < end; i++ {
					if err := txn.Set(hashKeys[i][:], hashValues[i]); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		return time.Since(start)
	}()

	// BDB: Write all data
	bdbWriteTime := func() time.Duration {
		db, err := NewBlockchainStore(bdbDir)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		start := time.Now()

		// Write state keys (using Put with key)
		for i := range stateKeys {
			if err := db.Put(stateKeys[i], stateValues[i]); err != nil {
				t.Fatal(err)
			}
		}

		// Write hash keys (using PutByHash for content-addressed storage)
		for i := range hashValues {
			if _, err := db.PutByHash(hashValues[i]); err != nil {
				t.Fatal(err)
			}
		}
		return time.Since(start)
	}()

	printResult("Write All Data", badgerWriteTime, bdbWriteTime)

	// === READ PHASE ===

	// Reopen databases
	badgerDB := openBadgerForBench(t, badgerDir)
	defer badgerDB.Close()
	bdb := openBDBForBench(t, bdbDir)
	defer bdb.Close()

	// Simulate blockchain read pattern:
	// For each hash key read, read state key 100 times
	numHashReads := 1000  // Sample of hash reads
	numStateReadsPerHash := stateReadRatio

	// Badger read workload
	badgerReadTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numHashReads; i++ {
			// Read 1 hash key
			badgerDB.View(func(txn *badger.Txn) error {
				item, err := txn.Get(hashKeys[i][:])
				if err == nil {
					item.ValueCopy(nil)
				}
				return nil
			})

			// Read state key 100 times
			stateIdx := i % numStateKeys
			for j := 0; j < numStateReadsPerHash; j++ {
				badgerDB.View(func(txn *badger.Txn) error {
					item, err := txn.Get(stateKeys[stateIdx])
					if err == nil {
						item.ValueCopy(nil)
					}
					return nil
				})
			}
		}
		return time.Since(start)
	}()

	// BDB read workload
	bdbReadTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numHashReads; i++ {
			// Read 1 hash key (by hash)
			bdb.GetByHash(hashKeys[i])

			// Read state key 100 times
			stateIdx := i % numStateKeys
			for j := 0; j < numStateReadsPerHash; j++ {
				bdb.Get(stateKeys[stateIdx])
			}
		}
		return time.Since(start)
	}()

	printResult("Blockchain Read Pattern", badgerReadTime, bdbReadTime)

	// === STATE UPDATE PATTERN ===
	// Simulate updating account state (common operation)
	numUpdates := 10000

	badgerUpdateTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numUpdates; i++ {
			stateIdx := i % numStateKeys
			newValue := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d}`, i*1000+i, i+1))
			badgerDB.Update(func(txn *badger.Txn) error {
				return txn.Set(stateKeys[stateIdx], newValue)
			})
		}
		return time.Since(start)
	}()

	bdbUpdateTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numUpdates; i++ {
			stateIdx := i % numStateKeys
			newValue := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d}`, i*1000+i, i+1))
			bdb.Put(stateKeys[stateIdx], newValue)
		}
		return time.Since(start)
	}()

	printResult("State Updates (10k)", badgerUpdateTime, bdbUpdateTime)

	// === MIXED REALISTIC WORKLOAD ===
	// 70% state reads, 20% state writes, 10% hash reads
	numOps := 10000

	badgerMixedTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numOps; i++ {
			op := i % 10
			switch {
			case op < 7: // 70% state read
				stateIdx := i % numStateKeys
				badgerDB.View(func(txn *badger.Txn) error {
					item, _ := txn.Get(stateKeys[stateIdx])
					if item != nil {
						item.ValueCopy(nil)
					}
					return nil
				})
			case op < 9: // 20% state write
				stateIdx := i % numStateKeys
				badgerDB.Update(func(txn *badger.Txn) error {
					return txn.Set(stateKeys[stateIdx], stateValues[stateIdx])
				})
			default: // 10% hash read
				hashIdx := i % totalHashKeys
				badgerDB.View(func(txn *badger.Txn) error {
					item, _ := txn.Get(hashKeys[hashIdx][:])
					if item != nil {
						item.ValueCopy(nil)
					}
					return nil
				})
			}
		}
		return time.Since(start)
	}()

	bdbMixedTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numOps; i++ {
			op := i % 10
			switch {
			case op < 7: // 70% state read
				stateIdx := i % numStateKeys
				bdb.Get(stateKeys[stateIdx])
			case op < 9: // 20% state write
				stateIdx := i % numStateKeys
				bdb.Put(stateKeys[stateIdx], stateValues[stateIdx])
			default: // 10% hash read
				hashIdx := i % totalHashKeys
				bdb.GetByHash(hashKeys[hashIdx])
			}
		}
		return time.Since(start)
	}()

	printResult("Mixed (70r/20w/10h)", badgerMixedTime, bdbMixedTime)

	// === PREFIX SCAN (e.g., get all accounts) ===
	badgerScanTime := func() time.Duration {
		start := time.Now()
		count := 0
		badgerDB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte("account:")
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				item.ValueCopy(nil)
				count++
			}
			return nil
		})
		return time.Since(start)
	}()

	bdbScanTime := func() time.Duration {
		start := time.Now()
		count := 0
		bdb.ForEachPrefix([]byte("account:"), func(key, value []byte) error {
			count++
			return nil
		})
		return time.Since(start)
	}()

	printResult("Scan All Accounts", badgerScanTime, bdbScanTime)

	// Print dedup stats for BDB
	stats := bdb.Stats()
	fmt.Printf("\nBlockchainDB Stats:\n")
	fmt.Printf("  Puts: %d, Gets: %d\n", stats.Puts.Load(), stats.Gets.Load())
	fmt.Printf("  PutsByHash: %d\n", stats.PutsByHash.Load())
	fmt.Printf("  Deduplications: %d\n", stats.Deduplication.Load())
	fmt.Printf("  Bloom Hits: %d, Misses: %d\n", stats.BloomHits.Load(), stats.BloomMisses.Load())
	fmt.Println()
}

// TestPerformance_10M_BlockchainWorkload tests with 10M entries using full interface
// Profile: 100 hash keys per state key, state accessed 100x more frequently
// Operations tested:
//   - Put, Get, Delete (key-value)
//   - PutByHash, GetByHash, Has (content-addressed)
//   - ForEach, ForEachPrefix (iteration)
//   - NewBatch, Batch.Put, Batch.PutByHash, Batch.Delete, Batch.Commit (batching)
//   - Stats, Close
func TestPerformance_10M_BlockchainWorkload(t *testing.T) {
	const (
		numStateKeys     = 100000                            // Account state entries (r/w)
		hashKeysPerState = 100                               // Transaction hashes per account
		totalHashKeys    = numStateKeys * hashKeysPerState   // 10M hash keys
		totalEntries     = numStateKeys + totalHashKeys      // 10.1M total
		stateReadRatio   = 100                               // Read state 100x per hash read
	)

	fmt.Println("\n" + strings.Repeat("=", 90))
	fmt.Println("10M BLOCKCHAIN WORKLOAD - FULL INTERFACE TEST")
	fmt.Printf("State keys: %d, Hash keys: %d (total: %d)\n", numStateKeys, totalHashKeys, totalEntries)
	fmt.Printf("Access pattern: %d state reads per 1 hash read\n", stateReadRatio)
	fmt.Println(strings.Repeat("=", 90))

	baseDir := t.TempDir()
	badgerDir := filepath.Join(baseDir, "badger")
	bdbDir := filepath.Join(baseDir, "bdb")

	// Track memory before
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Generate test data
	fmt.Println("\nGenerating 10M test entries...")
	genStart := time.Now()

	stateKeys := make([][]byte, numStateKeys)
	stateValues := make([][]byte, numStateKeys)
	for i := 0; i < numStateKeys; i++ {
		stateKeys[i] = []byte(fmt.Sprintf("account:%08d", i))
		stateValues[i] = []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d,"delegate":"acc:%d"}`, i*1000, i, i%1000))
	}

	// Hash keys simulate transaction hashes (32 bytes)
	hashKeys := make([][32]byte, totalHashKeys)
	hashValues := make([][]byte, totalHashKeys)
	for i := 0; i < totalHashKeys; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("tx-%d", i)))
		hashKeys[i] = h
		hashValues[i] = []byte(fmt.Sprintf(`{"from":"addr%d","to":"addr%d","amount":%d,"data":"0x%x"}`,
			i%10000, (i+1)%10000, i, i))
	}
	fmt.Printf("Data generation took: %v\n\n", time.Since(genStart))

	// Results table header
	fmt.Printf("%-30s %15s %15s %12s\n", "Operation", "BadgerDB", "BlockchainDB", "Winner")
	fmt.Println(strings.Repeat("-", 75))

	var badgerDiskSize, bdbDiskSize int64
	var badgerMemAfter, bdbMemAfter runtime.MemStats

	// ============== WRITE PHASE ==============

	// Badger Write
	badgerWriteTime := func() time.Duration {
		opts := badger.DefaultOptions(badgerDir)
		opts.Logger = nil
		db, err := badger.Open(opts)
		if err != nil {
			t.Fatal(err)
		}

		start := time.Now()

		// Write state keys in batches
		batchSize := 10000
		for bStart := 0; bStart < numStateKeys; bStart += batchSize {
			bEnd := bStart + batchSize
			if bEnd > numStateKeys {
				bEnd = numStateKeys
			}
			db.Update(func(txn *badger.Txn) error {
				for i := bStart; i < bEnd; i++ {
					txn.Set(stateKeys[i], stateValues[i])
				}
				return nil
			})
		}

		// Write hash keys in batches
		for bStart := 0; bStart < totalHashKeys; bStart += batchSize {
			bEnd := bStart + batchSize
			if bEnd > totalHashKeys {
				bEnd = totalHashKeys
			}
			db.Update(func(txn *badger.Txn) error {
				for i := bStart; i < bEnd; i++ {
					txn.Set(hashKeys[i][:], hashValues[i])
				}
				return nil
			})
		}

		elapsed := time.Since(start)
		db.Close()

		runtime.GC()
		runtime.ReadMemStats(&badgerMemAfter)
		badgerDiskSize = getDirSize(badgerDir)
		return elapsed
	}()

	// BDB Write (using batches for fair comparison)
	bdbWriteTime := func() time.Duration {
		db, err := NewBlockchainStore(bdbDir)
		if err != nil {
			t.Fatal(err)
		}

		start := time.Now()

		// Write state keys in batches
		batchSize := 10000
		for bStart := 0; bStart < numStateKeys; bStart += batchSize {
			bEnd := bStart + batchSize
			if bEnd > numStateKeys {
				bEnd = numStateKeys
			}
			batch := db.NewBatch()
			for i := bStart; i < bEnd; i++ {
				batch.Put(stateKeys[i], stateValues[i])
			}
			batch.Commit()
		}

		// Write hash keys using PutByHash in batches
		for bStart := 0; bStart < totalHashKeys; bStart += batchSize {
			bEnd := bStart + batchSize
			if bEnd > totalHashKeys {
				bEnd = totalHashKeys
			}
			batch := db.NewBatch()
			for i := bStart; i < bEnd; i++ {
				batch.PutByHash(hashValues[i])
			}
			batch.Commit()
		}

		elapsed := time.Since(start)
		db.Close()

		runtime.GC()
		runtime.ReadMemStats(&bdbMemAfter)
		bdbDiskSize = getDirSize(bdbDir)
		return elapsed
	}()

	printResult("Write 10M (batched)", badgerWriteTime, bdbWriteTime)

	// Print disk usage
	fmt.Printf("\n%-30s %15s %15s %12s\n", "DISK USAGE", "BadgerDB", "BlockchainDB", "Smaller")
	fmt.Println(strings.Repeat("-", 75))
	printSizeResult("Total disk size", badgerDiskSize, bdbDiskSize)

	// Print memory usage
	fmt.Printf("\n%-30s %15s %15s %12s\n", "MEMORY (after write)", "BadgerDB", "BlockchainDB", "Lower")
	fmt.Println(strings.Repeat("-", 75))
	badgerMem := badgerMemAfter.Alloc - memBefore.Alloc
	bdbMem := bdbMemAfter.Alloc - memBefore.Alloc
	printSizeResult("Heap allocated", int64(badgerMem), int64(bdbMem))

	// ============== READ PHASE ==============
	fmt.Printf("\n%-30s %15s %15s %12s\n", "READ OPERATIONS", "BadgerDB", "BlockchainDB", "Winner")
	fmt.Println(strings.Repeat("-", 75))

	// Reopen databases
	badgerDB := openBadgerForBench(t, badgerDir)
	defer badgerDB.Close()
	bdb := openBDBForBench(t, bdbDir)
	defer bdb.Close()

	// Get (state keys) - 10k random reads
	numReads := 10000
	badgerGetTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numReads; i++ {
			idx := (i * 7919) % numStateKeys // pseudo-random
			badgerDB.View(func(txn *badger.Txn) error {
				item, _ := txn.Get(stateKeys[idx])
				if item != nil {
					item.ValueCopy(nil)
				}
				return nil
			})
		}
		return time.Since(start)
	}()

	bdbGetTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numReads; i++ {
			idx := (i * 7919) % numStateKeys
			bdb.Get(stateKeys[idx])
		}
		return time.Since(start)
	}()
	printResult("Get (10k random)", badgerGetTime, bdbGetTime)

	// GetByHash - 10k reads
	badgerGetHashTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numReads; i++ {
			idx := (i * 7919) % totalHashKeys
			badgerDB.View(func(txn *badger.Txn) error {
				item, _ := txn.Get(hashKeys[idx][:])
				if item != nil {
					item.ValueCopy(nil)
				}
				return nil
			})
		}
		return time.Since(start)
	}()

	bdbGetHashTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numReads; i++ {
			idx := (i * 7919) % totalHashKeys
			bdb.GetByHash(hashKeys[idx])
		}
		return time.Since(start)
	}()
	printResult("GetByHash (10k)", badgerGetHashTime, bdbGetHashTime)

	// Has - check existence (10k)
	badgerHasTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numReads; i++ {
			idx := (i * 7919) % totalHashKeys
			badgerDB.View(func(txn *badger.Txn) error {
				_, err := txn.Get(hashKeys[idx][:])
				_ = err == nil
				return nil
			})
		}
		return time.Since(start)
	}()

	bdbHasTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numReads; i++ {
			idx := (i * 7919) % totalHashKeys
			bdb.Has(hashKeys[idx])
		}
		return time.Since(start)
	}()
	printResult("Has (10k checks)", badgerHasTime, bdbHasTime)

	// ForEachPrefix - scan accounts
	badgerPrefixTime := func() time.Duration {
		start := time.Now()
		count := 0
		badgerDB.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte("account:")
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				count++
			}
			return nil
		})
		return time.Since(start)
	}()

	bdbPrefixTime := func() time.Duration {
		start := time.Now()
		count := 0
		bdb.ForEachPrefix([]byte("account:"), func(key, value []byte) error {
			count++
			return nil
		})
		return time.Since(start)
	}()
	printResult(fmt.Sprintf("ForEachPrefix (%dk)", numStateKeys/1000), badgerPrefixTime, bdbPrefixTime)

	// ForEach - full scan (sample first 100k)
	badgerForEachTime := func() time.Duration {
		start := time.Now()
		count := 0
		badgerDB.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid() && count < 100000; it.Next() {
				count++
			}
			return nil
		})
		return time.Since(start)
	}()

	bdbForEachTime := func() time.Duration {
		start := time.Now()
		count := 0
		bdb.ForEach(func(key, value []byte) error {
			count++
			if count >= 100000 {
				return fmt.Errorf("stop")
			}
			return nil
		})
		return time.Since(start)
	}()
	printResult("ForEach (100k sample)", badgerForEachTime, bdbForEachTime)

	// ============== WRITE/UPDATE PHASE ==============
	fmt.Printf("\n%-30s %15s %15s %12s\n", "WRITE OPERATIONS", "BadgerDB", "BlockchainDB", "Winner")
	fmt.Println(strings.Repeat("-", 75))

	// Put updates - 10k state updates (individual)
	badgerPutTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numReads; i++ {
			idx := (i * 7919) % numStateKeys
			newVal := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d}`, i, i+1))
			badgerDB.Update(func(txn *badger.Txn) error {
				return txn.Set(stateKeys[idx], newVal)
			})
		}
		return time.Since(start)
	}()

	bdbPutTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numReads; i++ {
			idx := (i * 7919) % numStateKeys
			newVal := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d}`, i, i+1))
			bdb.Put(stateKeys[idx], newVal)
		}
		return time.Since(start)
	}()
	printResult("Put individual (10k)", badgerPutTime, bdbPutTime)

	// Batched updates - 10k in batches of 100 (simulating block commits)
	badgerBatchPutTime := func() time.Duration {
		start := time.Now()
		batchSize := 100
		for bStart := 0; bStart < numReads; bStart += batchSize {
			bEnd := bStart + batchSize
			if bEnd > numReads {
				bEnd = numReads
			}
			badgerDB.Update(func(txn *badger.Txn) error {
				for i := bStart; i < bEnd; i++ {
					idx := (i * 7919) % numStateKeys
					newVal := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d}`, i+10000, i+1))
					txn.Set(stateKeys[idx], newVal)
				}
				return nil
			})
		}
		return time.Since(start)
	}()

	bdbBatchPutTime := func() time.Duration {
		start := time.Now()
		batchSize := 100
		for bStart := 0; bStart < numReads; bStart += batchSize {
			bEnd := bStart + batchSize
			if bEnd > numReads {
				bEnd = numReads
			}
			batch := bdb.NewBatch()
			for i := bStart; i < bEnd; i++ {
				idx := (i * 7919) % numStateKeys
				newVal := []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d}`, i+10000, i+1))
				batch.Put(stateKeys[idx], newVal)
			}
			batch.Commit()
		}
		return time.Since(start)
	}()
	printResult("Put batched (10k/100)", badgerBatchPutTime, bdbBatchPutTime)

	// Delete - 1k deletes (individual)
	numDeletes := 1000
	badgerDeleteTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numDeletes; i++ {
			idx := (i * 7919) % numStateKeys
			badgerDB.Update(func(txn *badger.Txn) error {
				return txn.Delete(stateKeys[idx])
			})
		}
		return time.Since(start)
	}()

	bdbDeleteTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numDeletes; i++ {
			idx := (i * 7919) % numStateKeys
			bdb.Delete(stateKeys[idx])
		}
		return time.Since(start)
	}()
	printResult("Delete individual (1k)", badgerDeleteTime, bdbDeleteTime)

	// Batched deletes - 1k in batches of 100
	badgerBatchDelTime := func() time.Duration {
		start := time.Now()
		batchSize := 100
		for bStart := 0; bStart < numDeletes; bStart += batchSize {
			bEnd := bStart + batchSize
			if bEnd > numDeletes {
				bEnd = numDeletes
			}
			badgerDB.Update(func(txn *badger.Txn) error {
				for i := bStart; i < bEnd; i++ {
					idx := ((i + 500) * 7919) % numStateKeys // Different keys
					txn.Delete(stateKeys[idx])
				}
				return nil
			})
		}
		return time.Since(start)
	}()

	bdbBatchDelTime := func() time.Duration {
		start := time.Now()
		batchSize := 100
		for bStart := 0; bStart < numDeletes; bStart += batchSize {
			bEnd := bStart + batchSize
			if bEnd > numDeletes {
				bEnd = numDeletes
			}
			batch := bdb.NewBatch()
			for i := bStart; i < bEnd; i++ {
				idx := ((i + 500) * 7919) % numStateKeys
				batch.Delete(stateKeys[idx])
			}
			batch.Commit()
		}
		return time.Since(start)
	}()
	printResult("Delete batched (1k/100)", badgerBatchDelTime, bdbBatchDelTime)

	// ============== MIXED BLOCKCHAIN WORKLOAD ==============
	fmt.Printf("\n%-30s %15s %15s %12s\n", "MIXED WORKLOAD", "BadgerDB", "BlockchainDB", "Winner")
	fmt.Println(strings.Repeat("-", 75))

	// Blockchain pattern: for every hash read, 100 state reads
	// Mix: 49% state read, 49% state read (for hash), 1% hash read, 1% state write
	numMixedOps := 100000

	badgerMixedTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numMixedOps; i++ {
			op := i % 100
			switch {
			case op < 49: // 49% state read
				idx := (i * 7919) % numStateKeys
				badgerDB.View(func(txn *badger.Txn) error {
					item, _ := txn.Get(stateKeys[idx])
					if item != nil {
						item.ValueCopy(nil)
					}
					return nil
				})
			case op < 98: // 49% more state reads (for the hash ratio)
				idx := (i * 7919) % numStateKeys
				badgerDB.View(func(txn *badger.Txn) error {
					item, _ := txn.Get(stateKeys[idx])
					if item != nil {
						item.ValueCopy(nil)
					}
					return nil
				})
			case op < 99: // 1% hash read
				idx := (i * 7919) % totalHashKeys
				badgerDB.View(func(txn *badger.Txn) error {
					item, _ := txn.Get(hashKeys[idx][:])
					if item != nil {
						item.ValueCopy(nil)
					}
					return nil
				})
			default: // 1% state write
				idx := (i * 7919) % numStateKeys
				badgerDB.Update(func(txn *badger.Txn) error {
					return txn.Set(stateKeys[idx], stateValues[idx])
				})
			}
		}
		return time.Since(start)
	}()

	bdbMixedTime := func() time.Duration {
		start := time.Now()
		for i := 0; i < numMixedOps; i++ {
			op := i % 100
			switch {
			case op < 49: // 49% state read
				idx := (i * 7919) % numStateKeys
				bdb.Get(stateKeys[idx])
			case op < 98: // 49% more state reads
				idx := (i * 7919) % numStateKeys
				bdb.Get(stateKeys[idx])
			case op < 99: // 1% hash read
				idx := (i * 7919) % totalHashKeys
				bdb.GetByHash(hashKeys[idx])
			default: // 1% state write
				idx := (i * 7919) % numStateKeys
				bdb.Put(stateKeys[idx], stateValues[idx])
			}
		}
		return time.Since(start)
	}()
	printResult("Blockchain mix (100k ops)", badgerMixedTime, bdbMixedTime)

	// ============== FINAL STATS ==============
	fmt.Println("\n" + strings.Repeat("=", 75))
	fmt.Println("FINAL STATISTICS")
	fmt.Println(strings.Repeat("=", 75))

	// Final disk sizes
	badgerDB.Close()
	bdb.Close()

	finalBadgerSize := getDirSize(badgerDir)
	finalBdbSize := getDirSize(bdbDir)

	fmt.Printf("\nDisk Usage:\n")
	fmt.Printf("  BadgerDB:     %s\n", formatBytes(finalBadgerSize))
	fmt.Printf("  BlockchainDB: %s\n", formatBytes(finalBdbSize))
	if finalBadgerSize > finalBdbSize {
		fmt.Printf("  BDB saves:    %s (%.1f%% smaller)\n",
			formatBytes(finalBadgerSize-finalBdbSize),
			float64(finalBadgerSize-finalBdbSize)/float64(finalBadgerSize)*100)
	} else {
		fmt.Printf("  Badger saves: %s (%.1f%% smaller)\n",
			formatBytes(finalBdbSize-finalBadgerSize),
			float64(finalBdbSize-finalBadgerSize)/float64(finalBdbSize)*100)
	}

	// BDB stats
	bdb2 := openBDBForBench(t, bdbDir)
	stats := bdb2.Stats()
	fmt.Printf("\nBlockchainDB Internal Stats:\n")
	fmt.Printf("  Total Puts:       %d\n", stats.Puts.Load())
	fmt.Printf("  Total Gets:       %d\n", stats.Gets.Load())
	fmt.Printf("  Total Deletes:    %d\n", stats.Deletes.Load())
	fmt.Printf("  PutsByHash:       %d\n", stats.PutsByHash.Load())
	fmt.Printf("  Deduplications:   %d (%.1f%% dedup rate)\n",
		stats.Deduplication.Load(),
		float64(stats.Deduplication.Load())/float64(stats.Puts.Load()+stats.PutsByHash.Load())*100)
	fmt.Printf("  Bloom Hits:       %d\n", stats.BloomHits.Load())
	fmt.Printf("  Bloom Misses:     %d\n", stats.BloomMisses.Load())
	if stats.BloomHits.Load()+stats.BloomMisses.Load() > 0 {
		fmt.Printf("  Bloom Efficiency: %.1f%%\n",
			float64(stats.BloomHits.Load())/float64(stats.BloomHits.Load()+stats.BloomMisses.Load())*100)
	}
	bdb2.Close()

	fmt.Println()
}

func printSizeResult(op string, badgerSize, bdbSize int64) {
	winner := "TIE"
	if badgerSize < bdbSize {
		ratio := float64(bdbSize) / float64(badgerSize)
		winner = fmt.Sprintf("Badger %.1fx", ratio)
	} else if bdbSize < badgerSize {
		ratio := float64(badgerSize) / float64(bdbSize)
		winner = fmt.Sprintf("BDB %.1fx", ratio)
	}
	fmt.Printf("%-30s %15s %15s %12s\n", op, formatBytes(badgerSize), formatBytes(bdbSize), winner)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
