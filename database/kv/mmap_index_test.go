package kv

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"testing"
)

func TestMmapIndex_CreateOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mmix")

	// Create new index
	idx, err := NewMmapIndex(path)
	if err != nil {
		t.Fatalf("NewMmapIndex failed: %v", err)
	}

	if idx.EntryCount() != 0 {
		t.Errorf("New index should have 0 entries, got %d", idx.EntryCount())
	}

	if err := idx.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen
	idx, err = OpenMmapIndex(path)
	if err != nil {
		t.Fatalf("OpenMmapIndex failed: %v", err)
	}
	defer idx.Close()

	if idx.EntryCount() != 0 {
		t.Errorf("Reopened index should have 0 entries, got %d", idx.EntryCount())
	}
}

func TestMmapIndex_Build(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mmix")

	idx, err := NewMmapIndex(path)
	if err != nil {
		t.Fatalf("NewMmapIndex failed: %v", err)
	}
	defer idx.Close()

	// Build with 100 entries
	entries := make([]IndexEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = IndexEntry{
			Key:    sha256.Sum256([]byte(fmt.Sprintf("key-%d", i))),
			Offset: uint64(i * 1000),
			Length: uint64(100 + i),
		}
	}

	if err := idx.Build(entries); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if idx.EntryCount() != 100 {
		t.Errorf("Entry count = %d, want 100", idx.EntryCount())
	}

	// Verify all entries can be found
	for i := 0; i < 100; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("key-%d", i)))
		loc, err := idx.Get(key)
		if err != nil {
			t.Errorf("Get failed for key %d: %v", i, err)
			continue
		}
		if loc.Offset != uint64(i*1000) {
			t.Errorf("Key %d: offset = %d, want %d", i, loc.Offset, i*1000)
		}
		if loc.Length != uint64(100+i) {
			t.Errorf("Key %d: length = %d, want %d", i, loc.Length, 100+i)
		}
	}
}

func TestMmapIndex_Persistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mmix")

	// Create and build
	idx, err := NewMmapIndex(path)
	if err != nil {
		t.Fatalf("NewMmapIndex failed: %v", err)
	}

	entries := make([]IndexEntry, 50)
	for i := 0; i < 50; i++ {
		entries[i] = IndexEntry{
			Key:    sha256.Sum256([]byte(fmt.Sprintf("persist-key-%d", i))),
			Offset: uint64(i * 500),
			Length: uint64(200),
		}
	}

	if err := idx.Build(entries); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if err := idx.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify
	idx, err = OpenMmapIndex(path)
	if err != nil {
		t.Fatalf("OpenMmapIndex failed: %v", err)
	}
	defer idx.Close()

	if idx.EntryCount() != 50 {
		t.Errorf("Entry count after reopen = %d, want 50", idx.EntryCount())
	}

	// Verify all entries
	for i := 0; i < 50; i++ {
		key := sha256.Sum256([]byte(fmt.Sprintf("persist-key-%d", i)))
		loc, err := idx.Get(key)
		if err != nil {
			t.Errorf("Get failed after reopen for key %d: %v", i, err)
			continue
		}
		if loc.Offset != uint64(i*500) {
			t.Errorf("Key %d: offset = %d, want %d", i, loc.Offset, i*500)
		}
	}
}

func TestMmapIndex_Has(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mmix")

	idx, err := NewMmapIndex(path)
	if err != nil {
		t.Fatalf("NewMmapIndex failed: %v", err)
	}
	defer idx.Close()

	key1 := sha256.Sum256([]byte("exists"))
	key2 := sha256.Sum256([]byte("missing"))

	entries := []IndexEntry{{Key: key1, Offset: 100, Length: 50}}
	if err := idx.Build(entries); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if !idx.Has(key1) {
		t.Error("Has returned false for existing key")
	}
	if idx.Has(key2) {
		t.Error("Has returned true for missing key")
	}
}

func TestMmapIndex_ForEach(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mmix")

	idx, err := NewMmapIndex(path)
	if err != nil {
		t.Fatalf("NewMmapIndex failed: %v", err)
	}
	defer idx.Close()

	// Build with entries
	entries := make([]IndexEntry, 25)
	for i := 0; i < 25; i++ {
		entries[i] = IndexEntry{
			Key:    sha256.Sum256([]byte(fmt.Sprintf("iter-key-%d", i))),
			Offset: uint64(i * 100),
			Length: uint64(50),
		}
	}

	if err := idx.Build(entries); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Iterate and count
	count := 0
	err = idx.ForEach(func(key [32]byte, offset, length uint64) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach failed: %v", err)
	}

	if count != 25 {
		t.Errorf("ForEach count = %d, want 25", count)
	}
}

func TestMmapIndex_SortedOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.mmix")

	idx, err := NewMmapIndex(path)
	if err != nil {
		t.Fatalf("NewMmapIndex failed: %v", err)
	}
	defer idx.Close()

	// Build with unsorted entries
	entries := []IndexEntry{
		{Key: sha256.Sum256([]byte("zebra")), Offset: 1, Length: 1},
		{Key: sha256.Sum256([]byte("apple")), Offset: 2, Length: 2},
		{Key: sha256.Sum256([]byte("mango")), Offset: 3, Length: 3},
	}

	if err := idx.Build(entries); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify entries are sorted
	var lastKey [32]byte
	first := true
	err = idx.ForEach(func(key [32]byte, offset, length uint64) error {
		if !first && bytes.Compare(lastKey[:], key[:]) >= 0 {
			t.Error("Entries not in sorted order")
		}
		lastKey = key
		first = false
		return nil
	})
	if err != nil {
		t.Fatalf("ForEach failed: %v", err)
	}
}

func BenchmarkMmapIndex_Get(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.mmix")

	idx, err := NewMmapIndex(path)
	if err != nil {
		b.Fatalf("NewMmapIndex failed: %v", err)
	}
	defer idx.Close()

	// Build with 100k entries
	entries := make([]IndexEntry, 100000)
	for i := 0; i < 100000; i++ {
		entries[i] = IndexEntry{
			Key:    sha256.Sum256([]byte(fmt.Sprintf("bench-key-%d", i))),
			Offset: uint64(i * 100),
			Length: uint64(100),
		}
	}

	if err := idx.Build(entries); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	// Benchmark lookups
	keys := make([][32]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = sha256.Sum256([]byte(fmt.Sprintf("bench-key-%d", i*100)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = idx.Get(keys[i%1000])
	}
}

func BenchmarkMmapIndex_Build(b *testing.B) {
	dir := b.TempDir()

	entries := make([]IndexEntry, 10000)
	for i := 0; i < 10000; i++ {
		entries[i] = IndexEntry{
			Key:    sha256.Sum256([]byte(fmt.Sprintf("build-key-%d", i))),
			Offset: uint64(i * 100),
			Length: uint64(100),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, fmt.Sprintf("bench-%d.mmix", i))
		idx, err := NewMmapIndex(path)
		if err != nil {
			b.Fatalf("NewMmapIndex failed: %v", err)
		}
		if err := idx.Build(entries); err != nil {
			b.Fatalf("Build failed: %v", err)
		}
		idx.Close()
	}
}
