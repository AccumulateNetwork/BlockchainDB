package timeseries

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TimeStore manages time-based value files with automatic rotation
type TimeStore struct {
	baseDir        string
	rotateInterval time.Duration
	currentFile    *ValueFile
	currentPath    string

	// File registry
	fileRegistry map[string]*FileInfo
	timeIndex    []TimeRange

	// Rotation control
	rotationTimer *time.Timer
	stopRotation  chan struct{}

	mu sync.RWMutex
}

// FileInfo contains metadata about a value file
type FileInfo struct {
	Filename  string
	StartTime int64
	EndTime   int64
	FileSize  int64
	Entries   uint32
}

// TimeRange represents a time range covered by a file
type TimeRange struct {
	Filename  string
	StartTime int64
	EndTime   int64
}

// NewTimeStore creates a new time-based value store
func NewTimeStore(baseDir string, rotateHours int) (*TimeStore, error) {
	if rotateHours <= 0 || rotateHours > 24 {
		rotateHours = 12 // Default to 12 hours
	}

	ts := &TimeStore{
		baseDir:        baseDir,
		rotateInterval: time.Duration(rotateHours) * time.Hour,
		fileRegistry:   make(map[string]*FileInfo),
		timeIndex:      make([]TimeRange, 0),
		stopRotation:   make(chan struct{}),
	}

	// Create base directory structure
	if err := ts.createDirectories(); err != nil {
		return nil, err
	}

	// Start with initial file
	if err := ts.rotateFile(); err != nil {
		return nil, err
	}

	// Start rotation scheduler
	ts.scheduleNextRotation()

	return ts, nil
}

// Put adds a value to the current time-based file
func (ts *TimeStore) Put(value []byte) ([32]byte, error) {
	ts.mu.RLock()
	currentFile := ts.currentFile
	ts.mu.RUnlock()

	if currentFile == nil {
		return [32]byte{}, fmt.Errorf("no current file available")
	}

	return currentFile.Append(value)
}

// Get retrieves a value by key from any time file
func (ts *TimeStore) Get(key [32]byte, fileHint string) ([]byte, error) {
	// If we have a file hint, try that first
	if fileHint != "" {
		if file, err := ts.openFileForRead(fileHint); err == nil {
			defer file.Close()
			if value, err := file.Get(key); err == nil {
				return value, nil
			}
		}
	}

	// Otherwise search all files (expensive - should use key index instead)
	ts.mu.RLock()
	files := make([]string, 0, len(ts.fileRegistry))
	for filename := range ts.fileRegistry {
		files = append(files, filename)
	}
	ts.mu.RUnlock()

	for _, filename := range files {
		file, err := ts.openFileForRead(filename)
		if err != nil {
			continue
		}

		value, err := file.Get(key)
		file.Close()

		if err == nil {
			return value, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// IterateTimeRange returns all entries within a time range
func (ts *TimeStore) IterateTimeRange(start, end int64) <-chan ValueEntry {
	ch := make(chan ValueEntry, 100)

	go func() {
		defer close(ch)

		// Find relevant files
		files := ts.findFilesInRange(start, end)

		for _, filename := range files {
			file, err := ts.openFileForRead(filename)
			if err != nil {
				continue
			}

			// Iterate entries in this file's time range
			for entry := range file.IterateTimeRange(start, end) {
				ch <- entry
			}

			file.Close()
		}
	}()

	return ch
}

// rotateFile closes current file and creates a new one
func (ts *TimeStore) rotateFile() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Close current file if exists
	if ts.currentFile != nil {
		ts.currentFile.Close()

		// Update registry with final stats
		stats := ts.currentFile.Stats()
		ts.fileRegistry[ts.currentPath] = &FileInfo{
			Filename:  ts.currentPath,
			StartTime: stats.StartTime.Unix(),
			EndTime:   stats.EndTime.Unix(),
			FileSize:  stats.FileSize,
			Entries:   stats.EntryCount,
		}
	}

	// Generate new filename
	now := time.Now()
	filename := ts.generateFilename(now)

	// Create new file
	newFile, err := NewValueFile(filename, now.Unix())
	if err != nil {
		return err
	}

	ts.currentFile = newFile
	ts.currentPath = filename

	// Add to time index
	ts.timeIndex = append(ts.timeIndex, TimeRange{
		Filename:  filename,
		StartTime: now.Unix(),
		EndTime:   now.Add(ts.rotateInterval).Unix(),
	})

	return nil
}

// generateFilename creates a filename based on timestamp
func (ts *TimeStore) generateFilename(t time.Time) string {
	// Determine which 12-hour block
	hour := 0
	if ts.rotateInterval == 12*time.Hour {
		if t.Hour() >= 12 {
			hour = 12
		}
	}

	// Create directory structure: baseDir/YYYY/MM/
	year := fmt.Sprintf("%04d", t.Year())
	month := fmt.Sprintf("%02d", t.Month())
	dir := filepath.Join(ts.baseDir, year, month)

	// Ensure directory exists
	os.MkdirAll(dir, 0755)

	// Format: v_YYYYMMDD_HH.dat
	filename := fmt.Sprintf("v_%04d%02d%02d_%02d.dat",
		t.Year(), t.Month(), t.Day(), hour)

	return filepath.Join(dir, filename)
}

// createDirectories creates the base directory structure
func (ts *TimeStore) createDirectories() error {
	dirs := []string{
		ts.baseDir,
		filepath.Join(ts.baseDir, "metadata"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	return nil
}

// scheduleNextRotation schedules the next file rotation
func (ts *TimeStore) scheduleNextRotation() {
	now := time.Now()
	var next time.Time

	if ts.rotateInterval == 12*time.Hour {
		// Rotate at 00:00 and 12:00
		if now.Hour() < 12 {
			next = time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, now.Location())
		} else {
			next = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		}
	} else {
		// Generic rotation interval
		next = now.Truncate(ts.rotateInterval).Add(ts.rotateInterval)
	}

	duration := next.Sub(now)
	ts.rotationTimer = time.AfterFunc(duration, func() {
		ts.rotateFile()
		ts.scheduleNextRotation()
	})
}

// findFilesInRange returns filenames that might contain entries in the time range
func (ts *TimeStore) findFilesInRange(start, end int64) []string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	var files []string

	for _, tr := range ts.timeIndex {
		// Check if file's time range overlaps with query range
		if tr.EndTime >= start && tr.StartTime <= end {
			files = append(files, tr.Filename)
		}
	}

	return files
}

// openFileForRead opens a value file for reading
func (ts *TimeStore) openFileForRead(filename string) (*ValueFile, error) {
	return OpenValueFile(filename)
}

// Close shuts down the time store
func (ts *TimeStore) Close() error {
	// Stop rotation timer
	if ts.rotationTimer != nil {
		ts.rotationTimer.Stop()
	}

	// Close current file
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.currentFile != nil {
		return ts.currentFile.Close()
	}

	return nil
}

// Stats returns statistics about the time store
func (ts *TimeStore) Stats() TimeStoreStats {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	stats := TimeStoreStats{
		TotalFiles:   len(ts.fileRegistry),
		CurrentFile:  ts.currentPath,
		TimeRanges:   make([]TimeRange, len(ts.timeIndex)),
	}

	copy(stats.TimeRanges, ts.timeIndex)

	// Calculate total size and entries
	for _, info := range ts.fileRegistry {
		stats.TotalSize += info.FileSize
		stats.TotalEntries += info.Entries
	}

	return stats
}

// TimeStoreStats contains statistics about the time store
type TimeStoreStats struct {
	TotalFiles   int
	TotalSize    int64
	TotalEntries uint32
	CurrentFile  string
	TimeRanges   []TimeRange
}

// RebuildKeyIndex rebuilds the key index from all value files
func (ts *TimeStore) RebuildKeyIndex() (*KeyIndex, error) {
	index := NewKeyIndex()

	ts.mu.RLock()
	files := make([]TimeRange, len(ts.timeIndex))
	copy(files, ts.timeIndex)
	ts.mu.RUnlock()

	// Process files in chronological order
	for _, tr := range files {
		file, err := ts.openFileForRead(tr.Filename)
		if err != nil {
			return nil, fmt.Errorf("failed to open %s: %w", tr.Filename, err)
		}

		// Iterate all entries
		for entry := range file.Iterate() {
			// Add to index
			index.Put(entry.Key, KeyLocation{
				Filename:  tr.Filename,
				Offset:    0, // Would need to track this during iteration
				Length:    entry.Length,
				Timestamp: entry.Timestamp,
			})
		}

		file.Close()
	}

	return index, nil
}

// Compact removes old files beyond retention period
func (ts *TimeStore) Compact(retentionDays int) error {
	if retentionDays <= 0 {
		return fmt.Errorf("retention days must be positive")
	}

	cutoff := time.Now().AddDate(0, 0, -retentionDays).Unix()

	ts.mu.Lock()
	defer ts.mu.Unlock()

	var filesToRemove []string

	// Find files older than retention period
	for filename, info := range ts.fileRegistry {
		if info.EndTime < cutoff {
			filesToRemove = append(filesToRemove, filename)
		}
	}

	// Remove old files
	for _, filename := range filesToRemove {
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("failed to remove %s: %w", filename, err)
		}

		delete(ts.fileRegistry, filename)

		// Remove from time index
		for i, tr := range ts.timeIndex {
			if tr.Filename == filename {
				ts.timeIndex = append(ts.timeIndex[:i], ts.timeIndex[i+1:]...)
				break
			}
		}
	}

	return nil
}