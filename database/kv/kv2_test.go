package kv

import (
	"bytes"
	"testing"
)

func TestKV2_PutGetDyna(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	key := [32]byte{1, 2, 3}
	value := []byte("dynamic value")

	if _, err := kv2.PutDyna(key, value); err != nil {
		t.Fatalf("PutDyna failed: %v", err)
	}

	got, err := kv2.GetDyna(key)
	if err != nil {
		t.Fatalf("GetDyna failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("GetDyna returned %v, want %v", got, value)
	}

	// Should also be retrievable via Get
	got, err = kv2.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get returned %v, want %v", got, value)
	}
}

func TestKV2_PutGetPerm(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	key := [32]byte{1, 2, 3}
	value := []byte("permanent value")

	if _, err := kv2.PutPerm(key, value); err != nil {
		t.Fatalf("PutPerm failed: %v", err)
	}

	got, err := kv2.GetPerm(key)
	if err != nil {
		t.Fatalf("GetPerm failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("GetPerm returned %v, want %v", got, value)
	}

	// Should also be retrievable via Get
	got, err = kv2.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Get returned %v, want %v", got, value)
	}
}

func TestKV2_AutoTiering(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	key := [32]byte{1, 2, 3}
	value1 := []byte("initial value")
	value2 := []byte("updated value")

	// First put should go to PermKV (new key)
	if _, err := kv2.Put(key, value1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Should be in PermKV
	got, err := kv2.GetPerm(key)
	if err != nil {
		t.Fatalf("GetPerm failed: %v", err)
	}
	if !bytes.Equal(got, value1) {
		t.Errorf("Value not in PermKV")
	}

	// Second put with different value should migrate to DynaKV
	if _, err := kv2.Put(key, value2); err != nil {
		t.Fatalf("Put (update) failed: %v", err)
	}

	// Should now be in DynaKV with new value
	got, err = kv2.GetDyna(key)
	if err != nil {
		t.Fatalf("GetDyna failed after migration: %v", err)
	}
	if !bytes.Equal(got, value2) {
		t.Errorf("GetDyna returned %v after migration, want %v", got, value2)
	}

	// Get should return the new value
	got, err = kv2.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, value2) {
		t.Errorf("Get returned %v, want %v", got, value2)
	}
}

func TestKV2_NoChangeNoop(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	key := [32]byte{1, 2, 3}
	value := []byte("constant value")

	// First put
	writes1, err := kv2.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Same value again should be no-op
	writes2, err := kv2.Put(key, value)
	if err != nil {
		t.Fatalf("Put (same) failed: %v", err)
	}

	// Write count should not increase
	if writes2 != writes1 {
		t.Errorf("Write count changed for same value: %d -> %d", writes1, writes2)
	}
}

func TestKV2_Has(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	keyDyna := [32]byte{1}
	keyPerm := [32]byte{2}
	keyMissing := [32]byte{3}

	// Put in different tiers
	kv2.PutDyna(keyDyna, []byte("dyna"))
	kv2.PutPerm(keyPerm, []byte("perm"))

	if !kv2.Has(keyDyna) {
		t.Error("Has returned false for DynaKV key")
	}
	if !kv2.Has(keyPerm) {
		t.Error("Has returned false for PermKV key")
	}
	if kv2.Has(keyMissing) {
		t.Error("Has returned true for missing key")
	}
}

func TestKV2_HasDyna(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	keyDyna := [32]byte{1}
	keyPerm := [32]byte{2}

	kv2.PutDyna(keyDyna, []byte("dyna"))
	kv2.PutPerm(keyPerm, []byte("perm"))

	if !kv2.HasDyna(keyDyna) {
		t.Error("HasDyna returned false for DynaKV key")
	}
	if kv2.HasDyna(keyPerm) {
		t.Error("HasDyna returned true for PermKV key")
	}
}

func TestKV2_ForEachDyna(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	prefix := []byte("account:")

	// Add some entries with prefix
	for i := 0; i < 5; i++ {
		var key [32]byte
		copy(key[:], prefix)
		key[8] = byte(i)
		kv2.PutDyna(key, []byte{byte(i)})
	}

	// Add some without prefix
	for i := 0; i < 3; i++ {
		var key [32]byte
		key[0] = byte(i + 100)
		kv2.PutDyna(key, []byte{byte(i)})
	}

	count := 0
	err = kv2.ForEachDyna(prefix, func(key, value []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachDyna failed: %v", err)
	}

	if count != 5 {
		t.Errorf("ForEachDyna found %d entries, want 5", count)
	}
}

func TestKV2_Persistence(t *testing.T) {
	dir := t.TempDir()

	// Create and populate
	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}

	keyDyna := [32]byte{1}
	keyPerm := [32]byte{2}
	valueDyna := []byte("dyna value")
	valuePerm := []byte("perm value")

	kv2.PutDyna(keyDyna, valueDyna)
	kv2.PutPerm(keyPerm, valuePerm)

	if err := kv2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen
	kv2, err = OpenKV2(dir)
	if err != nil {
		t.Fatalf("OpenKV2 failed: %v", err)
	}
	defer kv2.Close()

	// Verify DynaKV data (DynaKV persists via WAL)
	got, err := kv2.GetDyna(keyDyna)
	if err != nil {
		t.Fatalf("GetDyna after reopen failed: %v", err)
	}
	if !bytes.Equal(got, valueDyna) {
		t.Errorf("GetDyna returned %v after reopen, want %v", got, valueDyna)
	}

	// Verify PermKV data (PermKV persists via bin metadata)
	got, err = kv2.GetPerm(keyPerm)
	if err != nil {
		t.Fatalf("GetPerm after reopen failed: %v", err)
	}
	if !bytes.Equal(got, valuePerm) {
		t.Errorf("GetPerm returned %v after reopen, want %v", got, valuePerm)
	}
}

func TestKV2_Compress(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	// Add entries
	for i := 0; i < 100; i++ {
		var key [32]byte
		key[0] = byte(i)
		kv2.PutDyna(key, []byte{byte(i)})
	}

	// Compress should not error
	if err := kv2.Compress(); err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Write counts should reset
	stats := kv2.Stats()
	if stats.DynaWALSize > 0 {
		// After compress, WAL should be compacted
		t.Log("WAL size after compress:", stats.DynaWALSize)
	}
}

func TestKV2_Stats(t *testing.T) {
	dir := t.TempDir()

	kv2, err := NewKV2(dir, 16)
	if err != nil {
		t.Fatalf("NewKV2 failed: %v", err)
	}
	defer kv2.Close()

	// Add entries
	for i := 0; i < 10; i++ {
		var key [32]byte
		key[0] = byte(i)
		kv2.PutDyna(key, []byte{byte(i)})
	}

	for i := 0; i < 5; i++ {
		var key [32]byte
		key[0] = byte(i + 100)
		kv2.PutPerm(key, []byte{byte(i)})
	}

	stats := kv2.Stats()

	if stats.DynaEntries != 10 {
		t.Errorf("DynaEntries = %d, want 10", stats.DynaEntries)
	}
	if stats.DynaPuts != 10 {
		t.Errorf("DynaPuts = %d, want 10", stats.DynaPuts)
	}
}
