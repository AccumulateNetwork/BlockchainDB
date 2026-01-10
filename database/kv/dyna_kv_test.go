package kv

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDynaKV_PutGet(t *testing.T) {
	dir := t.TempDir()

	d, err := NewDynaKV(dir)
	if err != nil {
		t.Fatalf("NewDynaKV failed: %v", err)
	}
	defer d.Close()

	// Test basic put/get
	key := [32]byte{1, 2, 3}
	value := []byte("test value")

	if err := d.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get returned %v, want %v", got, value)
	}
}

func TestDynaKV_Has(t *testing.T) {
	dir := t.TempDir()

	d, err := NewDynaKV(dir)
	if err != nil {
		t.Fatalf("NewDynaKV failed: %v", err)
	}
	defer d.Close()

	key := [32]byte{1, 2, 3}
	value := []byte("test value")

	// Should not exist initially
	if d.Has(key) {
		t.Error("Has returned true for non-existent key")
	}

	if err := d.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Should exist after put
	if !d.Has(key) {
		t.Error("Has returned false for existing key")
	}
}

func TestDynaKV_Delete(t *testing.T) {
	dir := t.TempDir()

	d, err := NewDynaKV(dir)
	if err != nil {
		t.Fatalf("NewDynaKV failed: %v", err)
	}
	defer d.Close()

	key := [32]byte{1, 2, 3}
	value := []byte("test value")

	if err := d.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if err := d.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if d.Has(key) {
		t.Error("Key still exists after delete")
	}

	_, err = d.Get(key)
	if err == nil {
		t.Error("Get should return error for deleted key")
	}
}

func TestDynaKV_Persistence(t *testing.T) {
	dir := t.TempDir()

	// Create and populate
	d, err := NewDynaKV(dir)
	if err != nil {
		t.Fatalf("NewDynaKV failed: %v", err)
	}

	key := [32]byte{1, 2, 3}
	value := []byte("persistent value")

	if err := d.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Close and reopen
	if err := d.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	d2, err := OpenDynaKV(dir)
	if err != nil {
		t.Fatalf("OpenDynaKV failed: %v", err)
	}
	defer d2.Close()

	// Value should still exist
	got, err := d2.Get(key)
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get returned %v after reopen, want %v", got, value)
	}
}

func TestDynaKV_ForEachPrefix(t *testing.T) {
	dir := t.TempDir()

	d, err := NewDynaKV(dir)
	if err != nil {
		t.Fatalf("NewDynaKV failed: %v", err)
	}
	defer d.Close()

	// Insert keys with different prefixes
	prefixA := []byte("account:")
	prefixB := []byte("balance:")

	keysA := [][32]byte{}
	for i := 0; i < 5; i++ {
		var key [32]byte
		copy(key[:], prefixA)
		key[8] = byte(i)
		keysA = append(keysA, key)
		if err := d.Put(key, []byte{byte(i)}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		var key [32]byte
		copy(key[:], prefixB)
		key[8] = byte(i)
		if err := d.Put(key, []byte{byte(i + 10)}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Iterate over prefixA
	count := 0
	err = d.ForEachPrefix(prefixA, func(key, value []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachPrefix failed: %v", err)
	}

	if count != 5 {
		t.Errorf("ForEachPrefix found %d keys, want 5", count)
	}
}

func TestDynaKV_Compact(t *testing.T) {
	dir := t.TempDir()

	d, err := NewDynaKV(dir)
	if err != nil {
		t.Fatalf("NewDynaKV failed: %v", err)
	}
	defer d.Close()

	// Write many entries
	for i := 0; i < 100; i++ {
		var key [32]byte
		key[0] = byte(i)
		if err := d.Put(key, []byte{byte(i)}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete half
	for i := 0; i < 50; i++ {
		var key [32]byte
		key[0] = byte(i)
		if err := d.Delete(key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	walSizeBefore := d.WALSize()

	// Compact
	if err := d.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	walSizeAfter := d.WALSize()

	// WAL should be smaller after compaction
	if walSizeAfter >= walSizeBefore {
		t.Errorf("WAL size after compaction (%d) should be less than before (%d)",
			walSizeAfter, walSizeBefore)
	}

	// Verify remaining keys still work
	for i := 50; i < 100; i++ {
		var key [32]byte
		key[0] = byte(i)
		got, err := d.Get(key)
		if err != nil {
			t.Errorf("Get after compact failed for key %d: %v", i, err)
			continue
		}
		if len(got) != 1 || got[0] != byte(i) {
			t.Errorf("Get returned wrong value for key %d", i)
		}
	}
}

func TestDynaKV_Stats(t *testing.T) {
	dir := t.TempDir()

	d, err := NewDynaKV(dir)
	if err != nil {
		t.Fatalf("NewDynaKV failed: %v", err)
	}
	defer d.Close()

	// Initial stats
	stats := d.Stats()
	if stats["entries"] != 0 {
		t.Errorf("Initial entries = %d, want 0", stats["entries"])
	}

	// After puts
	for i := 0; i < 10; i++ {
		var key [32]byte
		key[0] = byte(i)
		if err := d.Put(key, []byte{byte(i)}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	stats = d.Stats()
	if stats["entries"] != 10 {
		t.Errorf("After puts, entries = %d, want 10", stats["entries"])
	}
	if stats["puts"] != 10 {
		t.Errorf("After puts, puts = %d, want 10", stats["puts"])
	}
}

func TestDynaKV_WALFile(t *testing.T) {
	dir := t.TempDir()

	d, err := NewDynaKV(dir)
	if err != nil {
		t.Fatalf("NewDynaKV failed: %v", err)
	}

	key := [32]byte{1, 2, 3}
	value := []byte("test value")

	if err := d.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify WAL file exists
	walPath := filepath.Join(dir, "wal.dat")
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file not created")
	}
}
