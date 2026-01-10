package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

type BenchmarkStats struct {
	Timestamp time.Time `json:"timestamp"`
	Elapsed   string    `json:"elapsed"`
	DBName    string    `json:"db_name"`

	Entries      uint64 `json:"entries"`
	Accounts     uint64 `json:"accounts"`
	Transactions uint64 `json:"transactions"`

	Puts       uint64 `json:"puts"`
	Gets       uint64 `json:"gets"`
	GetsByHash uint64 `json:"gets_by_hash"`
	Has        uint64 `json:"has_checks"`
	Iterations uint64 `json:"iterations"`
	Batches    uint64 `json:"batches"`

	EntriesPerSec float64 `json:"entries_per_sec"`
	DiskBytes     int64   `json:"disk_bytes"`
	BytesPerEntry float64 `json:"bytes_per_entry"`

	AvgWriteTimeUs int64 `json:"avg_write_time_us"`
	AvgReadTimeUs  int64 `json:"avg_read_time_us"`
	MinWriteTimeUs int64 `json:"min_write_time_us"`
	MaxWriteTimeUs int64 `json:"max_write_time_us"`
	MinReadTimeUs  int64 `json:"min_read_time_us"`
	MaxReadTimeUs  int64 `json:"max_read_time_us"`

	// Throughput
	WriteMBPerSec float64 `json:"write_mb_per_sec"`
	ReadMBPerSec  float64 `json:"read_mb_per_sec"`

	// Memory efficiency
	HeapBytes       uint64  `json:"heap_bytes"`
	SysBytes        uint64  `json:"sys_bytes"`
	NumGoroutine    int     `json:"num_goroutine"`
	HeapPerEntry    float64 `json:"heap_per_entry"`
	GCPauseMs       float64 `json:"gc_pause_ms"`
	NumGC           uint32  `json:"num_gc"`
	WriteAmplify    float64 `json:"write_amplification"`
	TotalBytesIn    int64   `json:"total_bytes_written_logical"`
	TotalBytesOnDisk int64  `json:"total_bytes_on_disk"`

	TargetEntries uint64  `json:"target_entries"`
	PercentDone   float64 `json:"percent_done"`
	Complete      bool    `json:"complete"`
}

const (
	badgerLog  = "/tmp/badger_benchmark.log"
	bdbLog     = "/tmp/bdb_benchmark.log"
	leveldbLog = "/tmp/leveldb_benchmark.log"
)

// Metric defines how to extract and compare a metric
type Metric struct {
	Name        string
	Extract     func(*BenchmarkStats) (float64, string) // returns numeric value and display string
	LowerBetter bool                                    // true if lower values are better (like latency)
}

func main() {
	fmt.Println("\n" + strings.Repeat("=", 140))
	fmt.Println("BENCHMARK REPORTER")
	fmt.Println("Watching: " + badgerLog + ", " + bdbLog + ", " + leveldbLog)
	fmt.Println("Press SPACE for instant report, Q to quit")
	fmt.Println(strings.Repeat("=", 140))

	// Set terminal to raw mode for key detection
	exec.Command("stty", "-F", "/dev/tty", "cbreak", "min", "1").Run()
	exec.Command("stty", "-F", "/dev/tty", "-echo").Run()
	defer exec.Command("stty", "-F", "/dev/tty", "sane").Run()

	// Key input channel
	keyCh := make(chan byte)
	go func() {
		b := make([]byte, 1)
		for {
			os.Stdin.Read(b)
			keyCh <- b[0]
		}
	}()

	// Report ticker - every 3 minutes
	reportTicker := time.NewTicker(3 * time.Minute)
	defer reportTicker.Stop()

	// Progress ticker - every 10 seconds
	progressTicker := time.NewTicker(10 * time.Second)
	defer progressTicker.Stop()

	lastReportTime := time.Now()

	for {
		select {
		case key := <-keyCh:
			if key == ' ' {
				printReport()
				lastReportTime = time.Now()
			} else if key == 'q' || key == 'Q' {
				fmt.Println("\nExiting...")
				return
			}

		case <-reportTicker.C:
			printReport()
			lastReportTime = time.Now()

		case <-progressTicker.C:
			printProgress(lastReportTime)
		}
	}
}

func readLatestStats(logPath string) *BenchmarkStats {
	file, err := os.Open(logPath)
	if err != nil {
		return nil
	}
	defer file.Close()

	var lastLine string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lastLine = scanner.Text()
	}

	if lastLine == "" {
		return nil
	}

	var stats BenchmarkStats
	if err := json.Unmarshal([]byte(lastLine), &stats); err != nil {
		return nil
	}
	return &stats
}

func printProgress(lastReport time.Time) {
	badger := readLatestStats(badgerLog)
	bdb := readLatestStats(bdbLog)
	leveldb := readLatestStats(leveldbLog)

	untilReport := 3*time.Minute - time.Since(lastReport)
	if untilReport < 0 {
		untilReport = 0
	}

	badgerStr := "waiting..."
	bdbStr := "waiting..."
	leveldbStr := "waiting..."

	if badger != nil {
		badgerStr = fmt.Sprintf("%s (%.1f%%)", formatNum(badger.Entries), badger.PercentDone)
		if badger.Complete {
			badgerStr += " DONE"
		}
	}
	if bdb != nil {
		bdbStr = fmt.Sprintf("%s (%.1f%%)", formatNum(bdb.Entries), bdb.PercentDone)
		if bdb.Complete {
			bdbStr += " DONE"
		}
	}
	if leveldb != nil {
		leveldbStr = fmt.Sprintf("%s (%.1f%%)", formatNum(leveldb.Entries), leveldb.PercentDone)
		if leveldb.Complete {
			leveldbStr += " DONE"
		}
	}

	fmt.Printf("\r[Progress] Badger: %s | BDB: %s | LevelDB: %s | Next: %v   ",
		badgerStr, bdbStr, leveldbStr, untilReport.Round(time.Second))
}

type dbStats struct {
	name  string
	stats *BenchmarkStats
}

func printReport() {
	badger := readLatestStats(badgerLog)
	bdb := readLatestStats(bdbLog)
	leveldb := readLatestStats(leveldbLog)

	dbs := []dbStats{
		{"BadgerDB", badger},
		{"BlockchainDB", bdb},
		{"LevelDB", leveldb},
	}

	// Table width calculation
	const (
		metricCol = 26
		dbCol     = 16
		rankCol   = 12
	)
	totalWidth := metricCol + 3*(dbCol+3) + 2*(rankCol+3) + 4

	fmt.Printf("\n\n┌%s┐\n", strings.Repeat("─", totalWidth-2))
	fmt.Printf("│ PERFORMANCE REPORT @ %s%s│\n", time.Now().Format("15:04:05"), strings.Repeat(" ", totalWidth-27))
	fmt.Printf("├%s┤\n", strings.Repeat("─", totalWidth-2))

	// Header
	fmt.Printf("│ %-*s │ %*s │ %*s │ %*s │ %*s │ %*s │\n",
		metricCol, "METRIC",
		dbCol, "BadgerDB",
		dbCol, "BlockchainDB",
		dbCol, "LevelDB",
		rankCol, "BEST",
		rankCol, "2ND BEST")
	fmt.Printf("├%s┤\n", strings.Repeat("─", totalWidth-2))

	// Define all metrics with their extraction functions and comparison direction
	metrics := []Metric{
		// Progress section
		{"Elapsed", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			secs := parseElapsed(s.Elapsed)
			return secs, s.Elapsed
		}, true}, // Lower is better (faster completion)
		{"Progress", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return s.PercentDone, fmt.Sprintf("%.2f%%", s.PercentDone)
		}, false}, // Higher is better

		// Throughput section - calculate from totals for final stats
		{"Entries/second", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			// Use interval-based if available, otherwise calculate from totals
			if s.EntriesPerSec > 0 {
				return s.EntriesPerSec, formatNum(uint64(s.EntriesPerSec))
			}
			// Calculate from totals
			secs := parseElapsed(s.Elapsed)
			if secs > 0 && s.Entries > 0 {
				eps := float64(s.Entries) / secs
				return eps, formatNum(uint64(eps))
			}
			return 0, "N/A"
		}, false}, // Higher is better
		{"Write MB/s", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			// Use interval-based if available, otherwise calculate from totals
			if s.WriteMBPerSec > 0 {
				return s.WriteMBPerSec, fmt.Sprintf("%.2f", s.WriteMBPerSec)
			}
			// Calculate from totals
			secs := parseElapsed(s.Elapsed)
			if secs > 0 && s.TotalBytesIn > 0 {
				mbps := float64(s.TotalBytesIn) / secs / (1024 * 1024)
				return mbps, fmt.Sprintf("%.2f", mbps)
			}
			return 0, "N/A"
		}, false}, // Higher is better

		// Latency section
		{"Avg Write Time", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.AvgWriteTimeUs), fmt.Sprintf("%d µs", s.AvgWriteTimeUs)
		}, true}, // Lower is better
		{"Min Write Time", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			if s.MinWriteTimeUs == 0 { return 0, "N/A" }
			return float64(s.MinWriteTimeUs), fmt.Sprintf("%d µs", s.MinWriteTimeUs)
		}, true}, // Lower is better
		{"Max Write Time", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			if s.MaxWriteTimeUs == 0 { return 0, "N/A" }
			return float64(s.MaxWriteTimeUs), fmt.Sprintf("%d µs", s.MaxWriteTimeUs)
		}, true}, // Lower is better
		{"Avg Read Time", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.AvgReadTimeUs), fmt.Sprintf("%d µs", s.AvgReadTimeUs)
		}, true}, // Lower is better

		// Storage efficiency section
		{"Disk Usage", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.DiskBytes), formatBytes(s.DiskBytes)
		}, true}, // Lower is better
		{"Bytes/Entry", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return s.BytesPerEntry, fmt.Sprintf("%.1f B", s.BytesPerEntry)
		}, true}, // Lower is better
		{"Write Amplification", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			if s.WriteAmplify == 0 { return 0, "N/A" }
			return s.WriteAmplify, fmt.Sprintf("%.2fx", s.WriteAmplify)
		}, true}, // Lower is better

		// Memory section
		{"Heap Memory", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.HeapBytes), formatBytes(int64(s.HeapBytes))
		}, true}, // Lower is better
		{"Heap/Entry", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			if s.HeapPerEntry == 0 && s.Entries > 0 {
				hpe := float64(s.HeapBytes) / float64(s.Entries)
				return hpe, fmt.Sprintf("%.1f B", hpe)
			}
			return s.HeapPerEntry, fmt.Sprintf("%.1f B", s.HeapPerEntry)
		}, true}, // Lower is better
		{"GC Pause Total", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			if s.GCPauseMs == 0 { return 0, "N/A" }
			return s.GCPauseMs, fmt.Sprintf("%.1f ms", s.GCPauseMs)
		}, true}, // Lower is better
		{"Num GC Cycles", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.NumGC), fmt.Sprintf("%d", s.NumGC)
		}, true}, // Lower is better

		// Operation counts (per-database progress)
		{"Total Entries", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.Entries), formatNum(s.Entries)
		}, false},
		{"Batches Committed", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.Batches), formatNum(s.Batches)
		}, false},
		{"Gets (by key)", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.Gets), formatNum(s.Gets)
		}, false},
		{"Gets (by hash)", func(s *BenchmarkStats) (float64, string) {
			if s == nil { return 0, "N/A" }
			return float64(s.GetsByHash), formatNum(s.GetsByHash)
		}, false},
	}

	// Print metrics with rankings
	lastSection := ""
	sections := map[string]bool{
		"Progress": true, "Entries/second": true, "Avg Write Time": true,
		"Disk Usage": true, "Heap Memory": true, "Total Entries": true,
	}

	for _, m := range metrics {
		// Add section separator
		if sections[m.Name] && lastSection != "" {
			fmt.Printf("├%s┤\n", strings.Repeat("─", totalWidth-2))
		}
		lastSection = m.Name

		// Get values for each DB
		var values []struct {
			name  string
			val   float64
			disp  string
			valid bool
		}

		displays := make([]string, 3)
		for i, db := range dbs {
			val, disp := m.Extract(db.stats)
			displays[i] = disp
			valid := db.stats != nil && disp != "N/A" && val > 0
			values = append(values, struct {
				name  string
				val   float64
				disp  string
				valid bool
			}{db.name, val, disp, valid})
		}

		// Find best and second best
		best, secondBest := findRankings(values, m.LowerBetter)

		fmt.Printf("│ %-*s │ %*s │ %*s │ %*s │ %*s │ %*s │\n",
			metricCol, m.Name,
			dbCol, displays[0],
			dbCol, displays[1],
			dbCol, displays[2],
			rankCol, best,
			rankCol, secondBest)
	}

	fmt.Printf("└%s┘\n", strings.Repeat("─", totalWidth-2))

	// Print comparison summary
	printComparisonSummary(dbs)

	fmt.Println()
}

func findRankings(values []struct {
	name  string
	val   float64
	disp  string
	valid bool
}, lowerBetter bool) (string, string) {
	// Filter valid values
	type ranked struct {
		name string
		val  float64
	}
	var valid []ranked
	for _, v := range values {
		if v.valid {
			valid = append(valid, ranked{v.name, v.val})
		}
	}

	if len(valid) == 0 {
		return "-", "-"
	}
	if len(valid) == 1 {
		return shortName(valid[0].name), "-"
	}

	// Sort by value
	sort.Slice(valid, func(i, j int) bool {
		if lowerBetter {
			return valid[i].val < valid[j].val
		}
		return valid[i].val > valid[j].val
	})

	best := shortName(valid[0].name)
	second := shortName(valid[1].name)

	return best, second
}

func shortName(name string) string {
	switch name {
	case "BadgerDB":
		return "Badger"
	case "BlockchainDB":
		return "BDB"
	case "LevelDB":
		return "LevelDB"
	default:
		return name
	}
}

func printComparisonSummary(dbs []dbStats) {
	// Count wins for each DB
	wins := map[string]int{
		"BadgerDB":     0,
		"BlockchainDB": 0,
		"LevelDB":      0,
	}

	// Key performance metrics to count wins
	keyMetrics := []struct {
		name        string
		extract     func(*BenchmarkStats) float64
		lowerBetter bool
	}{
		{"Entries/sec", func(s *BenchmarkStats) float64 { return s.EntriesPerSec }, false},
		{"Write Latency", func(s *BenchmarkStats) float64 { return float64(s.AvgWriteTimeUs) }, true},
		{"Read Latency", func(s *BenchmarkStats) float64 { return float64(s.AvgReadTimeUs) }, true},
		{"Disk Efficiency", func(s *BenchmarkStats) float64 { return s.BytesPerEntry }, true},
		{"Memory Usage", func(s *BenchmarkStats) float64 { return float64(s.HeapBytes) }, true},
	}

	for _, m := range keyMetrics {
		var best string
		var bestVal float64
		first := true

		for _, db := range dbs {
			if db.stats == nil {
				continue
			}
			val := m.extract(db.stats)
			if val == 0 {
				continue
			}

			if first {
				best = db.name
				bestVal = val
				first = false
				continue
			}

			if m.lowerBetter {
				if val < bestVal {
					best = db.name
					bestVal = val
				}
			} else {
				if val > bestVal {
					best = db.name
					bestVal = val
				}
			}
		}

		if best != "" {
			wins[best]++
		}
	}

	fmt.Println()
	fmt.Println("PERFORMANCE SUMMARY (key metrics):")
	for name, count := range wins {
		if count > 0 {
			fmt.Printf("  %s: %d wins\n", shortName(name), count)
		}
	}
}

// parseElapsed converts elapsed string like "16s", "1m30s", "2h5m10s" to seconds
func parseElapsed(elapsed string) float64 {
	d, err := time.ParseDuration(elapsed)
	if err != nil {
		return 0
	}
	return d.Seconds()
}

func formatNum(n uint64) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.2fB", float64(n)/1_000_000_000)
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.2fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}

func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/GB)
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/MB)
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/KB)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
