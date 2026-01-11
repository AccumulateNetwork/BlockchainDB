package dkv

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AccumulateNetwork/BlockchainDB/database/kv"
)

// TestBDB_10M runs 10 million entry benchmark
func TestBDB_10M(t *testing.T) {
	runBDBBenchmark(t, 10_000_000)
}

// TestBDB_100M runs 100 million entry benchmark
func TestBDB_100M(t *testing.T) {
	runBDBBenchmark(t, 100_000_000)
}

// TestBDB_200M runs 200 million entry benchmark
func TestBDB_200M(t *testing.T) {
	runBDBBenchmark(t, 200_000_000)
}

// TestBDB_500M runs 500 million entry benchmark (long-run degradation test)
func TestBDB_500M(t *testing.T) {
	runBDBBenchmark(t, 500_000_000)
}

// getConfigFromEnv reads benchmark configuration from environment variables.
// Supported variables:
//   - BDB_SHARDS: Number of shards (default 1024)
//   - BDB_BINS: Number of bins per shard (default 256)
//   - BDB_CHANNELS: Number of channel groups (default 8)
//   - BDB_BUFFER: Write channel buffer size (default 20000)
//   - BDB_BLOOM: Expected items for bloom filter sizing (default: target entries)
func getConfigFromEnv(expectedItems int) kv.KVShardConfig {
	config := kv.DefaultConfig(expectedItems)

	// Bloom filter sizing - allow override for testing
	if v := os.Getenv("BDB_BLOOM"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.ExpectedItems = n
		}
	}
	if v := os.Getenv("BDB_SHARDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.NumShards = n
		}
	}
	if v := os.Getenv("BDB_BINS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.BinCount = n
		}
	}
	if v := os.Getenv("BDB_CHANNELS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.NumChannelGroups = n
		}
	}
	if v := os.Getenv("BDB_BUFFER"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.WriteChannelSize = n
		}
	}
	return config
}

func runBDBBenchmark(t *testing.T, targetEntries uint64) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Printf("BLOCKCHAINDB %s BENCHMARK\n", formatNum(targetEntries))
	fmt.Println(strings.Repeat("=", 60))

	const (
		statsInterval = 20 * time.Second
		batchSize     = 101 // 1 account + 100 txs
	)

	baseDir := t.TempDir()
	bdbDir := filepath.Join(baseDir, "bdb")
	logPath := "/tmp/bdb_benchmark.log"

	// Delete old log file to start fresh
	os.Remove(logPath)

	tracker, err := NewPerfTracker("BlockchainDB", logPath, bdbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	// Get config from environment variables (or use defaults)
	config := getConfigFromEnv(int(targetEntries))
	db, err := NewBlockchainStoreWithConfig(bdbDir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fmt.Printf("Target: %s entries\n", formatNum(targetEntries))
	fmt.Printf("Config: shards=%d, bins=%d, channels=%d, buffer=%d, bloom=%s\n",
		config.NumShards, config.BinCount, config.NumChannelGroups, config.WriteChannelSize,
		formatNum(uint64(config.ExpectedItems)))
	fmt.Printf("Stats log: %s\n", logPath)
	fmt.Printf("Data dir: %s\n\n", bdbDir)

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

			batch := db.NewBatch()

			accKey := generateAccountKey(accountIdx)
			accVal := generateAccountValue(accountIdx)
			batchBytes += int64(len(accKey) + len(accVal))
			batch.Put(accKey, accVal)

			for i := uint64(0); i < 100; i++ {
				txVal := generateTxValue(accountIdx, i)
				batchBytes += int64(32 + len(txVal)) // 32 byte hash + value
				batch.PutByHash(txVal)
			}

			batch.Commit()

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
				db.Get(key)
				tracker.AddRead("get", time.Since(start))
			} else {
				txHash := generateTxHash(randAccount, opCounter%100)
				db.GetByHash(txHash)
				tracker.AddRead("hash", time.Since(start))
			}
		}
	}

	tracker.WriteStats(targetEntries, true)
	fmt.Printf("\nBlockchainDB %s benchmark complete!\n", formatNum(targetEntries))
}
