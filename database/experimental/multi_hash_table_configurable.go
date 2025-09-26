package blockchainDB

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ConfigurableHashTable allows testing different fan-out values
type ConfigurableHashTable struct {
	directory    string
	fanOut       int // Number of bins per level (16, 64, 256, 1024, etc.)
	writeBuffer  *WriteBuffer
	levels       []*ConfigurableTreeLevel
	// wal removed - not needed for performance testing
	stats        *Statistics
	shutdownCh   chan struct{}
	wg           sync.WaitGroup
	mu           sync.RWMutex

	// Performance metrics
	writeLatencies []time.Duration
	readLatencies  []time.Duration
	perfMu         sync.Mutex
}

// ConfigurableTreeLevel represents one level with configurable bins
type ConfigurableTreeLevel struct {
	depth   int
	bins    map[int]*Bin // Use map for flexible bin count
	binMask int          // Bit mask for bin selection
}

// NewConfigurableHashTable creates a hash table with specified fan-out
func NewConfigurableHashTable(directory string, fanOut int) (*ConfigurableHashTable, error) {
	// Validate fan-out is power of 2
	if fanOut&(fanOut-1) != 0 || fanOut < 2 {
		return nil, fmt.Errorf("fan-out must be power of 2 (2, 4, 8, 16, 32, 64, 128, 256, 512, 1024)")
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	cht := &ConfigurableHashTable{
		directory:      directory,
		fanOut:        fanOut,
		writeBuffer:   NewWriteBuffer(WriteBufferSize),
		levels:        make([]*ConfigurableTreeLevel, 0),
		stats:         &Statistics{},
		shutdownCh:    make(chan struct{}),
		writeLatencies: make([]time.Duration, 0, 10000),
		readLatencies:  make([]time.Duration, 0, 10000),
	}

	// WAL removed - not needed for performance testing

	// Initialize first level
	cht.levels = append(cht.levels, cht.newTreeLevel(0))

	// Start background workers
	cht.startBackgroundWorkers()

	return cht, nil
}

// newTreeLevel creates a new level with the configured fan-out
func (cht *ConfigurableHashTable) newTreeLevel(depth int) *ConfigurableTreeLevel {
	level := &ConfigurableTreeLevel{
		depth:   depth,
		bins:    make(map[int]*Bin),
		binMask: cht.fanOut - 1, // For power-of-2 fan-out
	}

	// Pre-create bins
	for i := 0; i < cht.fanOut; i++ {
		binID := fmt.Sprintf("L%d_%04X", depth, i)
		level.bins[i] = &Bin{
			id:    binID,
			level: depth,
			index: byte(i),
			cache: &BinCache{
				entries: make([]HashEntry, 0),
				maxSize: BinCacheSize,
			},
			file: filepath.Join(cht.directory, binID+".bin"),
		}
	}

	return level
}

// getBinIndex calculates which bin a key belongs to at a given depth
func (cht *ConfigurableHashTable) getBinIndex(key [32]byte, depth int) int {
	// Calculate bits needed for fan-out
	bitsNeeded := 0
	temp := cht.fanOut - 1
	for temp > 0 {
		bitsNeeded++
		temp >>= 1
	}

	// Extract the appropriate bits from the hash
	byteOffset := (depth * bitsNeeded) / 8
	bitOffset := (depth * bitsNeeded) % 8

	if byteOffset >= 32 {
		return 0 // Ran out of hash bits
	}

	// Extract bits from hash
	var value int
	if byteOffset < 31 {
		// Can read 2 bytes
		value = int(key[byteOffset])<<8 | int(key[byteOffset+1])
		value >>= (8 - bitOffset)
	} else {
		// Only 1 byte available
		value = int(key[byteOffset])
		value >>= (8 - bitOffset)
	}

	return value & (cht.fanOut - 1)
}

// Put adds a key-value pair
func (cht *ConfigurableHashTable) Put(key [32]byte, value DBBKey) error {
	start := time.Now()

	// WAL removed - no logging needed for performance testing

	// Add to write buffer
	cht.writeBuffer.mu.Lock()
	cht.writeBuffer.entries = append(cht.writeBuffer.entries, HashEntry{key, value})
	cht.writeBuffer.size += 48

	if cht.writeBuffer.size >= cht.writeBuffer.maxSize {
		entries := cht.writeBuffer.entries
		cht.writeBuffer.entries = make([]HashEntry, 0, 1000)
		cht.writeBuffer.size = 0
		cht.writeBuffer.mu.Unlock()

		cht.wg.Add(1)
		go cht.distributeEntries(entries)
	} else {
		cht.writeBuffer.mu.Unlock()
	}

	atomic.AddUint64(&cht.stats.writes, 1)

	// Record latency
	cht.perfMu.Lock()
	cht.writeLatencies = append(cht.writeLatencies, time.Since(start))
	cht.perfMu.Unlock()

	return nil
}

// Get retrieves a value by key
func (cht *ConfigurableHashTable) Get(key [32]byte) (*DBBKey, error) {
	start := time.Now()
	atomic.AddUint64(&cht.stats.reads, 1)

	// Check write buffer first
	if value := cht.writeBuffer.Get(key); value != nil {
		cht.recordReadLatency(time.Since(start))
		return value, nil
	}

	// Search through levels
	for _, level := range cht.levels {
		binIndex := cht.getBinIndex(key, level.depth)
		bin := level.bins[binIndex]

		if bin == nil {
			continue
		}

		bin.mu.RLock()
		if value := bin.cache.Get(key); value != nil {
			bin.mu.RUnlock()
			cht.recordReadLatency(time.Since(start))
			return value, nil
		}

		if bin.file != "" && fileExists(bin.file) {
			value, err := cht.searchBinFile(bin.file, key)
			bin.mu.RUnlock()
			if err == nil {
				cht.recordReadLatency(time.Since(start))
				return value, nil
			}
		} else {
			bin.mu.RUnlock()
		}
	}

	cht.recordReadLatency(time.Since(start))
	return nil, fmt.Errorf("key not found")
}

func (cht *ConfigurableHashTable) recordReadLatency(latency time.Duration) {
	cht.perfMu.Lock()
	cht.readLatencies = append(cht.readLatencies, latency)
	cht.perfMu.Unlock()
}

// distributeEntries distributes entries to bins
func (cht *ConfigurableHashTable) distributeEntries(entries []HashEntry) {
	defer cht.wg.Done()

	// Group by bin
	groups := make(map[int][]HashEntry)
	for _, entry := range entries {
		binIndex := cht.getBinIndex(entry.Key, 0)
		groups[binIndex] = append(groups[binIndex], entry)
	}

	// Process each group
	var wg sync.WaitGroup
	for binIndex, binEntries := range groups {
		wg.Add(1)
		go func(idx int, entries []HashEntry) {
			defer wg.Done()
			cht.addToBin(0, idx, entries)
		}(binIndex, binEntries)
	}
	wg.Wait()

	// WAL removed - no marking needed
}

// addToBin adds entries to a specific bin
func (cht *ConfigurableHashTable) addToBin(levelDepth int, binIndex int, entries []HashEntry) {
	if levelDepth >= len(cht.levels) {
		cht.mu.Lock()
		if levelDepth >= len(cht.levels) {
			cht.levels = append(cht.levels, cht.newTreeLevel(levelDepth))
		}
		cht.mu.Unlock()
	}

	bin := cht.levels[levelDepth].bins[binIndex]

	bin.mu.Lock()
	defer bin.mu.Unlock()

	bin.cache.entries = append(bin.cache.entries, entries...)
	bin.cache.size += int64(len(entries) * 48)
	bin.cache.sorted = false

	if bin.cache.size >= bin.cache.maxSize {
		cht.mergeBin(bin)
	}
}

// Helper functions remain the same
func (cht *ConfigurableHashTable) mergeBin(bin *Bin) error {
	atomic.AddUint64(&cht.stats.merges, 1)

	bin.cache.Sort()
	tempFile := fmt.Sprintf("%s.tmp.%d", bin.file, time.Now().UnixNano())

	if fileExists(bin.file) {
		existing, err := cht.readBinFile(bin.file)
		if err != nil {
			return err
		}

		merged := mergeSortedEntries(bin.cache.entries, existing)

		if int64(len(merged)*48) > MaxBinFileSize {
			atomic.AddUint64(&cht.stats.fanOuts, 1)
			return cht.fanOutBin(bin, merged)
		}

		if err := cht.writeBinFile(tempFile, merged); err != nil {
			return err
		}
	} else {
		if err := cht.writeBinFile(tempFile, bin.cache.entries); err != nil {
			return err
		}
	}

	bin.version++
	if err := os.Rename(tempFile, bin.file); err != nil {
		return err
	}

	bin.cache.Clear()
	return nil
}

// fanOutBin distributes entries to next level
func (cht *ConfigurableHashTable) fanOutBin(bin *Bin, entries []HashEntry) error {
	nextLevel := bin.level + 1

	if nextLevel >= len(cht.levels) {
		cht.mu.Lock()
		if nextLevel >= len(cht.levels) {
			cht.levels = append(cht.levels, cht.newTreeLevel(nextLevel))
		}
		cht.mu.Unlock()
	}

	// Group by next-level bin
	groups := make(map[int][]HashEntry)
	for _, entry := range entries {
		nextBinIndex := cht.getBinIndex(entry.Key, nextLevel)
		groups[nextBinIndex] = append(groups[nextBinIndex], entry)
	}

	for binIndex, binEntries := range groups {
		cht.addToBin(nextLevel, binIndex, binEntries)
	}

	os.Remove(bin.file)
	bin.file = ""
	bin.size = 0

	return nil
}

// GetStatistics returns performance statistics
func (cht *ConfigurableHashTable) GetStatistics() map[string]interface{} {
	cht.perfMu.Lock()
	defer cht.perfMu.Unlock()

	stats := make(map[string]interface{})
	stats["fan_out"] = cht.fanOut
	stats["total_writes"] = atomic.LoadUint64(&cht.stats.writes)
	stats["total_reads"] = atomic.LoadUint64(&cht.stats.reads)
	stats["total_merges"] = atomic.LoadUint64(&cht.stats.merges)
	stats["total_fanouts"] = atomic.LoadUint64(&cht.stats.fanOuts)
	stats["num_levels"] = len(cht.levels)

	// Calculate latency percentiles
	if len(cht.writeLatencies) > 0 {
		sort.Slice(cht.writeLatencies, func(i, j int) bool {
			return cht.writeLatencies[i] < cht.writeLatencies[j]
		})
		stats["write_p50"] = cht.writeLatencies[len(cht.writeLatencies)*50/100]
		stats["write_p99"] = cht.writeLatencies[len(cht.writeLatencies)*99/100]
		stats["write_avg"] = cht.averageLatency(cht.writeLatencies)
	}

	if len(cht.readLatencies) > 0 {
		sort.Slice(cht.readLatencies, func(i, j int) bool {
			return cht.readLatencies[i] < cht.readLatencies[j]
		})
		stats["read_p50"] = cht.readLatencies[len(cht.readLatencies)*50/100]
		stats["read_p99"] = cht.readLatencies[len(cht.readLatencies)*99/100]
		stats["read_avg"] = cht.averageLatency(cht.readLatencies)
	}

	// Memory usage estimate
	totalBins := 0
	for i := 0; i < len(cht.levels); i++ {
		totalBins += cht.fanOut
	}
	stats["total_bins"] = totalBins
	stats["memory_per_bin"] = BinCacheSize
	stats["total_cache_memory"] = totalBins * BinCacheSize

	return stats
}

func (cht *ConfigurableHashTable) averageLatency(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	return sum / time.Duration(len(latencies))
}

// Binary file operations (reuse from original)
func (cht *ConfigurableHashTable) writeBinFile(path string, entries []HashEntry) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	header := make([]byte, 16)
	binary.BigEndian.PutUint64(header[0:8], uint64(len(entries)))
	binary.BigEndian.PutUint64(header[8:16], uint64(time.Now().Unix()))
	if _, err := file.Write(header); err != nil {
		return err
	}

	for _, entry := range entries {
		if _, err := file.Write(entry.Key[:]); err != nil {
			return err
		}
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], entry.Value.Offset)
		binary.BigEndian.PutUint64(data[8:16], entry.Value.Length)
		if _, err := file.Write(data); err != nil {
			return err
		}
	}

	return file.Sync()
}

func (cht *ConfigurableHashTable) readBinFile(path string) ([]HashEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	header := make([]byte, 16)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, err
	}

	count := binary.BigEndian.Uint64(header[0:8])
	entries := make([]HashEntry, count)

	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(file, entries[i].Key[:]); err != nil {
			return nil, err
		}
		data := make([]byte, 16)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, err
		}
		entries[i].Value.Offset = binary.BigEndian.Uint64(data[0:8])
		entries[i].Value.Length = binary.BigEndian.Uint64(data[8:16])
	}

	return entries, nil
}

func (cht *ConfigurableHashTable) searchBinFile(path string, key [32]byte) (*DBBKey, error) {
	entries, err := cht.readBinFile(path)
	if err != nil {
		return nil, err
	}

	idx := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].Key[:], key[:]) >= 0
	})

	if idx < len(entries) && entries[idx].Key == key {
		return &entries[idx].Value, nil
	}

	return nil, fmt.Errorf("not found")
}

func (cht *ConfigurableHashTable) startBackgroundWorkers() {
	cht.wg.Add(1)
	go func() {
		defer cht.wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cht.flushWriteBuffer()
			case <-cht.shutdownCh:
				return
			}
		}
	}()
}

func (cht *ConfigurableHashTable) flushWriteBuffer() {
	cht.writeBuffer.mu.Lock()
	if len(cht.writeBuffer.entries) == 0 {
		cht.writeBuffer.mu.Unlock()
		return
	}

	entries := cht.writeBuffer.entries
	cht.writeBuffer.entries = make([]HashEntry, 0, 1000)
	cht.writeBuffer.size = 0
	cht.writeBuffer.mu.Unlock()

	cht.wg.Add(1)
	go cht.distributeEntries(entries)
}

func (cht *ConfigurableHashTable) Shutdown() error {
	close(cht.shutdownCh)
	cht.flushWriteBuffer()
	cht.wg.Wait()
	return nil // No WAL to close
}