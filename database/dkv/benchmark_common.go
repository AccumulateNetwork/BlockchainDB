package dkv

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Deterministic data generation functions
// These generate consistent key/value pairs from an index without storing in memory

func generateAccountKey(accountIdx uint64) []byte {
	return []byte(fmt.Sprintf("account:%012d", accountIdx))
}

func generateAccountValue(accountIdx uint64) []byte {
	return []byte(fmt.Sprintf(`{"balance":%d,"nonce":%d,"delegate":"acc:%d","created":%d}`,
		accountIdx*1000, accountIdx%100, accountIdx%1000, 1700000000+accountIdx))
}

func generateTxHash(accountIdx, txIdx uint64) [32]byte {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], accountIdx)
	binary.BigEndian.PutUint64(data[8:16], txIdx)
	return sha256.Sum256(data)
}

func generateTxValue(accountIdx, txIdx uint64) []byte {
	return []byte(fmt.Sprintf(`{"from":"acc%d","to":"acc%d","amount":%d,"nonce":%d,"hash":"%x"}`,
		accountIdx, (accountIdx+1)%10000, txIdx*100, txIdx, txIdx))
}

// BenchmarkStats holds performance metrics written to log files
type BenchmarkStats struct {
	Timestamp time.Time `json:"timestamp"`
	Elapsed   string    `json:"elapsed"`
	DBName    string    `json:"db_name"`

	// Entry counts
	Entries      uint64 `json:"entries"`
	Accounts     uint64 `json:"accounts"`
	Transactions uint64 `json:"transactions"`

	// Operation counts
	Puts       uint64 `json:"puts"`
	Gets       uint64 `json:"gets"`
	GetsByHash uint64 `json:"gets_by_hash"`
	Has        uint64 `json:"has_checks"`
	Iterations uint64 `json:"iterations"`
	Batches    uint64 `json:"batches"`
	Deletes    uint64 `json:"deletes"`

	// Performance - throughput
	EntriesPerSec float64 `json:"entries_per_sec"`
	WriteMBPerSec float64 `json:"write_mb_per_sec"`
	ReadMBPerSec  float64 `json:"read_mb_per_sec"`

	// Storage
	DiskBytes     int64   `json:"disk_bytes"`
	BytesPerEntry float64 `json:"bytes_per_entry"`

	// Timing (microseconds) - averages
	AvgWriteTimeUs int64 `json:"avg_write_time_us"`
	AvgReadTimeUs  int64 `json:"avg_read_time_us"`

	// Timing - min/max for variance analysis
	MinWriteTimeUs int64 `json:"min_write_time_us"`
	MaxWriteTimeUs int64 `json:"max_write_time_us"`
	MinReadTimeUs  int64 `json:"min_read_time_us"`
	MaxReadTimeUs  int64 `json:"max_read_time_us"`

	// Write amplification (disk bytes / logical bytes written)
	WriteAmplify       float64 `json:"write_amplification"`
	TotalBytesWritten  int64   `json:"total_bytes_written_logical"`

	// Memory
	HeapBytes    uint64  `json:"heap_bytes"`
	SysBytes     uint64  `json:"sys_bytes"`
	NumGoroutine int     `json:"num_goroutine"`
	HeapPerEntry float64 `json:"heap_per_entry"`

	// GC stats
	GCPauseMs    float64 `json:"gc_pause_ms"`
	NumGC        uint32  `json:"num_gc"`
	GCPauseLast  float64 `json:"gc_pause_last_ms"`

	// Progress
	TargetEntries uint64  `json:"target_entries"`
	PercentDone   float64 `json:"percent_done"`
	Complete      bool    `json:"complete"`
}

// PerfTracker tracks performance stats with atomic counters
type PerfTracker struct {
	name      string
	startTime time.Time
	logFile   *os.File
	dataDir   string

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
	batches    atomic.Uint64
	deletes    atomic.Uint64

	// Timing (nanoseconds)
	writeTime atomic.Int64
	readTime  atomic.Int64

	// Min/max tracking (need mutex for compare-and-swap)
	timingMu       sync.Mutex
	minWriteTimeNs int64
	maxWriteTimeNs int64
	minReadTimeNs  int64
	maxReadTimeNs  int64

	// Bytes written (logical, before any amplification)
	bytesWritten atomic.Int64
	bytesRead    atomic.Int64

	// For rate calculation
	lastEntries      uint64
	lastBytesWritten int64
	lastBytesRead    int64
	lastTime         time.Time

	// For GC tracking
	lastNumGC uint32
	lastGCPauseNs uint64
}

func NewPerfTracker(name, logPath, dataDir string) (*PerfTracker, error) {
	logDir := filepath.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	f, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}

	return &PerfTracker{
		name:           name,
		startTime:      time.Now(),
		logFile:        f,
		dataDir:        dataDir,
		lastTime:       time.Now(),
		minWriteTimeNs: math.MaxInt64,
		minReadTimeNs:  math.MaxInt64,
	}, nil
}

func (p *PerfTracker) Close() {
	if p.logFile != nil {
		p.logFile.Close()
	}
}

func (p *PerfTracker) WriteStats(targetEntries uint64, complete bool) {
	now := time.Now()
	entries := p.entries.Load()
	bytesWritten := p.bytesWritten.Load()
	bytesRead := p.bytesRead.Load()

	// Calculate entries/sec and throughput
	entriesPerSec := float64(0)
	writeMBPerSec := float64(0)
	readMBPerSec := float64(0)

	interval := now.Sub(p.lastTime).Seconds()
	if interval > 0 && p.lastEntries > 0 {
		entriesPerSec = float64(entries-p.lastEntries) / interval

		bytesDelta := bytesWritten - p.lastBytesWritten
		if bytesDelta > 0 {
			writeMBPerSec = float64(bytesDelta) / interval / (1024 * 1024)
		}

		readDelta := bytesRead - p.lastBytesRead
		if readDelta > 0 {
			readMBPerSec = float64(readDelta) / interval / (1024 * 1024)
		}
	}
	p.lastEntries = entries
	p.lastBytesWritten = bytesWritten
	p.lastBytesRead = bytesRead
	p.lastTime = now

	// Disk size
	diskBytes := getDirSize(p.dataDir)

	// Bytes per entry
	bytesPerEntry := float64(0)
	if entries > 0 {
		bytesPerEntry = float64(diskBytes) / float64(entries)
	}

	// Write amplification
	writeAmplify := float64(0)
	if bytesWritten > 0 {
		writeAmplify = float64(diskBytes) / float64(bytesWritten)
	}

	// Avg timing
	avgWriteUs := int64(0)
	if batches := p.batches.Load(); batches > 0 {
		avgWriteUs = p.writeTime.Load() / int64(batches) / 1000
	}

	avgReadUs := int64(0)
	totalReads := p.gets.Load() + p.getsByHash.Load() + p.has.Load()
	if totalReads > 0 {
		avgReadUs = p.readTime.Load() / int64(totalReads) / 1000
	}

	// Min/max timing
	p.timingMu.Lock()
	minWriteUs := int64(0)
	maxWriteUs := int64(0)
	if p.minWriteTimeNs != math.MaxInt64 {
		minWriteUs = p.minWriteTimeNs / 1000
	}
	if p.maxWriteTimeNs > 0 {
		maxWriteUs = p.maxWriteTimeNs / 1000
	}

	minReadUs := int64(0)
	maxReadUs := int64(0)
	if p.minReadTimeNs != math.MaxInt64 {
		minReadUs = p.minReadTimeNs / 1000
	}
	if p.maxReadTimeNs > 0 {
		maxReadUs = p.maxReadTimeNs / 1000
	}
	p.timingMu.Unlock()

	// Memory
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Heap per entry
	heapPerEntry := float64(0)
	if entries > 0 {
		heapPerEntry = float64(m.Alloc) / float64(entries)
	}

	// GC stats
	gcPauseMs := float64(m.PauseTotalNs) / 1_000_000
	gcPauseLast := float64(0)
	if m.NumGC > 0 {
		// Get the most recent pause time
		idx := (m.NumGC + 255) % 256
		gcPauseLast = float64(m.PauseNs[idx]) / 1_000_000
	}

	stats := BenchmarkStats{
		Timestamp:         now,
		Elapsed:           time.Since(p.startTime).Round(time.Second).String(),
		DBName:            p.name,
		Entries:           entries,
		Accounts:          p.accounts.Load(),
		Transactions:      p.transactions.Load(),
		Puts:              p.puts.Load(),
		Gets:              p.gets.Load(),
		GetsByHash:        p.getsByHash.Load(),
		Has:               p.has.Load(),
		Iterations:        p.iterations.Load(),
		Batches:           p.batches.Load(),
		Deletes:           p.deletes.Load(),
		EntriesPerSec:     entriesPerSec,
		WriteMBPerSec:     writeMBPerSec,
		ReadMBPerSec:      readMBPerSec,
		DiskBytes:         diskBytes,
		BytesPerEntry:     bytesPerEntry,
		AvgWriteTimeUs:    avgWriteUs,
		AvgReadTimeUs:     avgReadUs,
		MinWriteTimeUs:    minWriteUs,
		MaxWriteTimeUs:    maxWriteUs,
		MinReadTimeUs:     minReadUs,
		MaxReadTimeUs:     maxReadUs,
		WriteAmplify:      writeAmplify,
		TotalBytesWritten: bytesWritten,
		HeapBytes:         m.Alloc,
		SysBytes:          m.Sys,
		NumGoroutine:      runtime.NumGoroutine(),
		HeapPerEntry:      heapPerEntry,
		GCPauseMs:         gcPauseMs,
		NumGC:             m.NumGC,
		GCPauseLast:       gcPauseLast,
		TargetEntries:     targetEntries,
		PercentDone:       float64(entries) / float64(targetEntries) * 100,
		Complete:          complete,
	}

	data, _ := json.Marshal(stats)
	p.logFile.Write(data)
	p.logFile.Write([]byte("\n"))
	p.logFile.Sync()
}

// AddWrite records a batch write with byte tracking
func (p *PerfTracker) AddWrite(entries, accounts, txs uint64, duration time.Duration) {
	p.entries.Add(entries)
	p.accounts.Add(accounts)
	p.transactions.Add(txs)
	p.puts.Add(entries)
	p.batches.Add(1)
	p.writeTime.Add(int64(duration))

	// Track min/max write time
	ns := int64(duration)
	p.timingMu.Lock()
	if ns < p.minWriteTimeNs {
		p.minWriteTimeNs = ns
	}
	if ns > p.maxWriteTimeNs {
		p.maxWriteTimeNs = ns
	}
	p.timingMu.Unlock()
}

// AddWriteWithBytes records a batch write including byte count
func (p *PerfTracker) AddWriteWithBytes(entries, accounts, txs uint64, bytes int64, duration time.Duration) {
	p.AddWrite(entries, accounts, txs, duration)
	p.bytesWritten.Add(bytes)
}

// AddRead records a read operation
func (p *PerfTracker) AddRead(opType string, duration time.Duration) {
	switch opType {
	case "get":
		p.gets.Add(1)
	case "hash":
		p.getsByHash.Add(1)
	case "has":
		p.has.Add(1)
	case "iter":
		p.iterations.Add(1)
	}
	p.readTime.Add(int64(duration))

	// Track min/max read time
	ns := int64(duration)
	p.timingMu.Lock()
	if ns < p.minReadTimeNs {
		p.minReadTimeNs = ns
	}
	if ns > p.maxReadTimeNs {
		p.maxReadTimeNs = ns
	}
	p.timingMu.Unlock()
}

// AddReadWithBytes records a read operation including bytes read
func (p *PerfTracker) AddReadWithBytes(opType string, bytes int64, duration time.Duration) {
	p.AddRead(opType, duration)
	p.bytesRead.Add(bytes)
}

// AddDelete records a delete operation
func (p *PerfTracker) AddDelete(count uint64, duration time.Duration) {
	p.deletes.Add(count)
	p.writeTime.Add(int64(duration))
}

func (p *PerfTracker) Entries() uint64 {
	return p.entries.Load()
}

func (p *PerfTracker) Accounts() uint64 {
	return p.accounts.Load()
}

func getDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}
