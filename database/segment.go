package blockchainDB

import (
	"crypto/sha256"
	"encoding/json"
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
// One sealed segment within a block export
type SegmentInfo struct {
	Shard  int    `json:"shard"`
	Height uint64 `json:"height"` // The block the segment belongs to
	Seq    uint64 `json:"seq"`    // Its order within that block
	File   string `json:"file"`   // Segment file name within the block directory
	Count  uint64 `json:"count"`  // Number of keys in the segment
	Hash   string `json:"hash"`   // SHA-256 of the segment file, hex
}

// Manifest
// The manifest for one exported block
type Manifest struct {
	Height   uint64        `json:"height"`
	Segments []SegmentInfo `json:"segments"`
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
// Seal a block and export it: every shard seals its live tail at the
// block height, and every segment sealed since the previous block is
// copied into <exportDir>/block-<height>/ with a manifest recording
// its shard, seal height, key count, and SHA-256.
//
// Copying sealed files is the whole export: the segments are already
// the storage format, so nothing is re-encoded.  prev is the previous
// block's manifest (nil for the first block); it says which segments
// a peer already has.  A shard that sealed more than once since the
// previous block (an auto-seal when its live tail filled) contributes
// each of those segments.
func (k *KVShard) ExportBlock(exportDir string, height uint64, prev *Manifest) (m *Manifest, err error) {
	blockDir := filepath.Join(exportDir, fmt.Sprintf("block-%08d", height))
	if err = os.MkdirAll(blockDir, os.ModePerm); err != nil {
		return nil, err
	}

	// The newest segment a peer already has, per shard
	exported := make(map[int]SegmentMeta)
	if prev != nil {
		for _, s := range prev.Segments {
			cur, ok := exported[s.Shard]
			m := SegmentMeta{Height: s.Height, Seq: s.Seq}
			if !ok || m.after(cur) {
				exported[s.Shard] = m
			}
		}
	}

	// Seal every shard before copying anything.  Sealing is durable and
	// cannot be rolled back, so a failure partway through the copy
	// phase would otherwise leave a block half-built on disk with some
	// shards already closed at this height.
	for i, shard := range k.Shards {
		if _, err = shard.Seal(height); err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
	}

	m = &Manifest{Height: height}
	for i, shard := range k.Shards {
		metas, paths := shard.PermKV.SegmentPaths()
		for j, meta := range metas {
			if last, ok := exported[i]; ok && !meta.after(last) {
				continue // The peer already has this one
			}
			name := fmt.Sprintf("shard-%04d-%08d-%04d.seg", i, meta.Height, meta.Seq)
			if err = copyFileSynced(paths[j], filepath.Join(blockDir, name)); err != nil {
				return nil, fmt.Errorf("shard %d: %w", i, err)
			}
			m.Segments = append(m.Segments, SegmentInfo{
				Shard: i, Height: meta.Height, Seq: meta.Seq, File: name,
				Count: meta.Count, Hash: meta.Hash,
			})
		}
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

// ImportPermSegment
// KV2-level adoption of a sealed segment file into the Perm layer,
// serialized with the shard's other operations by the KV2 mutex
func (k *KV2) ImportPermSegment(path string, meta SegmentMeta) (err error) {
	k.Mutex.Lock()
	defer k.Mutex.Unlock()
	return k.PermKV.ImportSegmentFile(path, meta)
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
// Verify and import one exported block directory.  Every segment is
// checked against its manifest hash before any of them is adopted,
// and adopting one is a file copy plus a manifest commit -- no record
// is re-inserted.
//
// Idempotent: a segment the store already has is skipped, so an
// interrupted sync resumes by re-running.
func (k *KVShard) ImportBlock(blockDir string) (count uint64, err error) {
	m, err := LoadManifest(blockDir)
	if err != nil {
		return 0, err
	}

	// Verify everything before adopting anything
	for _, s := range m.Segments {
		if s.Shard < 0 || s.Shard >= NumShards {
			return 0, fmt.Errorf("manifest references invalid shard %d", s.Shard)
		}
		if err = VerifySegmentFile(filepath.Join(blockDir, s.File), s.Hash); err != nil {
			return 0, err
		}
	}

	// Adopt in seal order so (block, seq) stays ascending within each shard
	segments := append([]SegmentInfo(nil), m.Segments...)
	sort.Slice(segments, func(i, j int) bool {
		a := SegmentMeta{Height: segments[i].Height, Seq: segments[i].Seq}
		b := SegmentMeta{Height: segments[j].Height, Seq: segments[j].Seq}
		return b.after(a)
	})
	for _, s := range segments {
		meta := SegmentMeta{Height: s.Height, Seq: s.Seq, Count: s.Count, Hash: s.Hash}
		if err = k.Shards[s.Shard].ImportPermSegment(filepath.Join(blockDir, s.File), meta); err != nil {
			return count, fmt.Errorf("shard %d: %w", s.Shard, err)
		}
		count += s.Count
	}
	return count, nil
}
