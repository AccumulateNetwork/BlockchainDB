package blockchainDB

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// Block segmentation: the node-sync half of the BlockchainDB design.
//
// The Perm layer is append-only: values.dat only grows, and every key
// maps to an immutable value at a fixed offset.  That makes "everything
// written since offset X" a well-defined, never-changing set -- a
// SEGMENT.  Sealing a segment per (major) block and hashing it yields
// verifiable units a new node can fetch to sync:
//
//   1. Fetch the manifests up to the current height.
//   2. Fetch each block's segment files; verify each against its
//      SHA-256 in the manifest.
//   3. Import the segments in height order.  Imports are idempotent
//      (replay-safe immutability), so interrupted syncs just resume.
//
// A segment file is a stream:
//
//	header:  magic(4) version(4) sinceOffset(8) count(8)
//	records: key(32) valueLen(8) value(valueLen)  ... repeated count times
//
// The manifest records, per shard: the segment file name, its record
// count, the values.dat end offset it reached, and the SHA-256 of the
// whole segment file.  The end offsets chain manifests together: block
// N+1 exports from block N's end offsets.

const (
	segmentMagic   = 0x53454731 // "SEG1"
	segmentVersion = 1
	manifestName   = "manifest.json"
)

// SegmentInfo
// One shard's segment within a block export
type SegmentInfo struct {
	Shard     int    `json:"shard"`
	File      string `json:"file"`      // Segment file name within the block directory
	Count     uint64 `json:"count"`     // Number of key/value records
	EndOffset uint64 `json:"endOffset"` // values.dat offset the segment reaches
	Hash      string `json:"hash"`      // SHA-256 of the segment file, hex
}

// Manifest
// The manifest for one exported block
type Manifest struct {
	Height   uint64        `json:"height"`
	Segments []SegmentInfo `json:"segments"`
}

// segRecord pairs a key with its DBBKey for offset-sorted export
type segRecord struct {
	key [32]byte
	dbb *DBBKey
}

// permKeys
// Collect every Perm key with a value offset >= sinceOffset, sorted by
// offset so the values read sequentially from values.dat
func (k *KV) permKeys(sinceOffset uint64) (records []segRecord, err error) {
	// The kfile holds the keys since the last history push
	keyValues, keyList, err := k.kFile.GetKeyList()
	if err != nil {
		return nil, err
	}
	for _, key := range keyList {
		if keyValues[key].Offset >= sinceOffset {
			records = append(records, segRecord{key, keyValues[key]})
		}
	}

	// History holds the rest.  Bins are read whole; records are fixed
	// size, so this is one sequential pass over history.dat.
	hf := k.kFile.History
	if hf != nil {
		seen := make(map[[32]byte]bool, len(records))
		for _, r := range records {
			seen[r.key] = true
		}
		for _, ks := range hf.KeySets {
			length := ks.End - ks.Start
			if length == 0 {
				continue
			}
			buff := make([]byte, length)
			if _, err := hf.File.ReadAt(buff, int64(ks.Start)); err != nil {
				return nil, err
			}
			for pos := 0; pos+DBKeyFullSize <= len(buff); pos += DBKeyFullSize {
				key, dbb, err := GetDBBKey(buff[pos : pos+DBKeyFullSize])
				if err != nil {
					return nil, err
				}
				if dbb.Offset >= sinceOffset && !seen[key] {
					records = append(records, segRecord{key, dbb})
				}
			}
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].dbb.Offset < records[j].dbb.Offset
	})
	return records, nil
}

// ExportSegment
// Write every Perm key/value with a value offset >= sinceOffset to w
// as a segment stream.  Returns the values.dat end offset the segment
// reaches (pass it as sinceOffset of the next export), the record
// count, and the SHA-256 of the bytes written.
func (k *KV) ExportSegment(w io.Writer, sinceOffset uint64) (endOffset uint64, count uint64, hash [32]byte, err error) {
	if !k.UseHistory {
		return 0, 0, hash, errors.New("segments are exported from the Perm (history-enabled) layer")
	}
	if err = k.vFile.Flush(); err != nil {
		return 0, 0, hash, err
	}
	endOffset = k.vFile.EOD

	records, err := k.permKeys(sinceOffset)
	if err != nil {
		return 0, 0, hash, err
	}

	h := sha256.New()
	out := bufio.NewWriter(io.MultiWriter(w, h))

	var header [24]byte
	binary.BigEndian.PutUint32(header[:], segmentMagic)
	binary.BigEndian.PutUint32(header[4:], segmentVersion)
	binary.BigEndian.PutUint64(header[8:], sinceOffset)
	binary.BigEndian.PutUint64(header[16:], uint64(len(records)))
	if _, err = out.Write(header[:]); err != nil {
		return 0, 0, hash, err
	}

	value := make([]byte, 0, 1024)
	var lenBuff [8]byte
	for _, r := range records {
		if uint64(cap(value)) < r.dbb.Length {
			value = make([]byte, r.dbb.Length)
		}
		value = value[:r.dbb.Length]
		if err = k.vFile.ReadAt(r.dbb.Offset, value); err != nil {
			return 0, 0, hash, fmt.Errorf("reading value for key %x: %w", r.key, err)
		}
		if _, err = out.Write(r.key[:]); err != nil {
			return 0, 0, hash, err
		}
		binary.BigEndian.PutUint64(lenBuff[:], r.dbb.Length)
		if _, err = out.Write(lenBuff[:]); err != nil {
			return 0, 0, hash, err
		}
		if _, err = out.Write(value); err != nil {
			return 0, 0, hash, err
		}
	}
	if err = out.Flush(); err != nil {
		return 0, 0, hash, err
	}
	copy(hash[:], h.Sum(nil))
	return endOffset, uint64(len(records)), hash, nil
}

// ImportSegment
// Read a segment stream and put every record into the Perm layer.
// Imports are idempotent: records already present with identical
// values are no-ops, so an interrupted import can simply be re-run.
func (k *KV) ImportSegment(r io.Reader) (count uint64, err error) {
	if !k.UseHistory {
		return 0, errors.New("segments are imported into the Perm (history-enabled) layer")
	}
	in := bufio.NewReader(r)

	var header [24]byte
	if _, err = io.ReadFull(in, header[:]); err != nil {
		return 0, err
	}
	if binary.BigEndian.Uint32(header[:]) != segmentMagic {
		return 0, errors.New("not a segment stream (bad magic)")
	}
	if v := binary.BigEndian.Uint32(header[4:]); v != segmentVersion {
		return 0, fmt.Errorf("unsupported segment version %d", v)
	}
	total := binary.BigEndian.Uint64(header[16:])

	var key [32]byte
	var lenBuff [8]byte
	value := make([]byte, 0, 1024)
	for i := uint64(0); i < total; i++ {
		if _, err = io.ReadFull(in, key[:]); err != nil {
			return count, err
		}
		if _, err = io.ReadFull(in, lenBuff[:]); err != nil {
			return count, err
		}
		length := binary.BigEndian.Uint64(lenBuff[:])
		if uint64(cap(value)) < length {
			value = make([]byte, length)
		}
		value = value[:length]
		if _, err = io.ReadFull(in, value); err != nil {
			return count, err
		}
		// Put copies through the value file, so the reused buffer is safe
		if err = k.Put(key, value); err != nil {
			return count, fmt.Errorf("importing key %x: %w", key, err)
		}
		count++
	}
	return count, nil
}

// VerifySegmentFile
// Compute the SHA-256 of a segment file and compare it to wantHex
func VerifySegmentFile(path string, wantHex string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return err
	}
	got := fmt.Sprintf("%x", h.Sum(nil))
	if got != wantHex {
		return fmt.Errorf("segment %s hash mismatch: have %s want %s", filepath.Base(path), got, wantHex)
	}
	return nil
}

// ExportBlock
// Seal a block: export every shard's Perm data written since the
// previous block into <exportDir>/block-<height>/ and write a manifest
// with the record counts, end offsets, and SHA-256 of each segment.
// prev is the previous block's manifest (nil for the first block); it
// supplies the offsets to export from.
func (k *KVShard) ExportBlock(exportDir string, height uint64, prev *Manifest) (m *Manifest, err error) {
	blockDir := filepath.Join(exportDir, fmt.Sprintf("block-%08d", height))
	if err = os.MkdirAll(blockDir, os.ModePerm); err != nil {
		return nil, err
	}

	since := make([]uint64, NumShards)
	if prev != nil {
		for _, s := range prev.Segments {
			if s.Shard >= 0 && s.Shard < NumShards {
				since[s.Shard] = s.EndOffset
			}
		}
	}

	m = &Manifest{Height: height}
	for i, shard := range k.Shards {
		name := fmt.Sprintf("shard-%04d.seg", i)
		f, err := os.Create(filepath.Join(blockDir, name))
		if err != nil {
			return nil, err
		}
		end, count, hash, err := shard.ExportPermSegment(f, since[i])
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
		if err = f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		if err = f.Close(); err != nil {
			return nil, err
		}
		m.Segments = append(m.Segments, SegmentInfo{
			Shard: i, File: name, Count: count, EndOffset: end,
			Hash: fmt.Sprintf("%x", hash),
		})
	}

	// Write the manifest last: its presence marks a complete export
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	tmp := filepath.Join(blockDir, manifestName+".tmp")
	if err = os.WriteFile(tmp, data, 0644); err != nil {
		return nil, err
	}
	if err = os.Rename(tmp, filepath.Join(blockDir, manifestName)); err != nil {
		return nil, err
	}
	return m, syncDir(blockDir)
}

// ExportPermSegment
// KV2-level export of the Perm layer, serialized with the shard's
// other operations by the KV2 mutex
func (k *KV2) ExportPermSegment(w io.Writer, sinceOffset uint64) (endOffset uint64, count uint64, hash [32]byte, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	return k.PermKV.ExportSegment(w, sinceOffset)
}

// ImportPermSegment
// KV2-level import into the Perm layer, serialized with the shard's
// other operations by the KV2 mutex
func (k *KV2) ImportPermSegment(r io.Reader) (count uint64, err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	return k.PermKV.ImportSegment(r)
}

// LoadManifest
// Read the manifest of an exported block directory
func LoadManifest(blockDir string) (m *Manifest, err error) {
	data, err := os.ReadFile(filepath.Join(blockDir, manifestName))
	if err != nil {
		return nil, err
	}
	m = new(Manifest)
	if err = json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

// ImportBlock
// Verify and import one exported block directory into this KVShard.
// Every segment is checked against its manifest hash before any record
// is imported.  Idempotent: re-importing a block is a no-op.
func (k *KVShard) ImportBlock(blockDir string) (count uint64, err error) {
	m, err := LoadManifest(blockDir)
	if err != nil {
		return 0, err
	}

	// Verify everything before importing anything
	for _, s := range m.Segments {
		if err = VerifySegmentFile(filepath.Join(blockDir, s.File), s.Hash); err != nil {
			return 0, err
		}
	}

	for _, s := range m.Segments {
		if s.Shard < 0 || s.Shard >= NumShards {
			return count, fmt.Errorf("manifest references invalid shard %d", s.Shard)
		}
		f, err := os.Open(filepath.Join(blockDir, s.File))
		if err != nil {
			return count, err
		}
		n, err := k.Shards[s.Shard].ImportPermSegment(f)
		f.Close()
		count += n
		if err != nil {
			return count, fmt.Errorf("shard %d: %w", s.Shard, err)
		}
		if n != s.Count {
			return count, fmt.Errorf("shard %d: imported %d records, manifest says %d", s.Shard, n, s.Count)
		}
	}
	return count, nil
}
