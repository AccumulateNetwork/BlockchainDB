# Block Segmentation & Node Sync

Status: v1 implemented (`database/segment.go`), 2026-08-24.

## Why

The original design thesis: data organized so blocks can be copied to
efficiently sync new nodes. The Perm layer makes this natural — it is
append-only (`values.dat` only grows) and immutable (a key's value
never changes) — so *"everything written since offset X"* is a
well-defined set that never changes after the fact. That set, sealed
and hashed, is a **segment**: the verifiable unit of node sync.

## The flow

**Sealing (producing node):**

    m1, _ := kvs.ExportBlock(exportDir, 1, nil)   // block 1: everything so far
    ...more writes...
    m2, _ := kvs.ExportBlock(exportDir, 2, m1)    // block 2: only what's new

Each block directory holds one segment file per shard plus a
`manifest.json` recording, per shard: record count, the `values.dat`
end offset reached, and the SHA-256 of the segment file. Manifests
chain: block N+1 exports from block N's end offsets, so consecutive
blocks partition the data exactly. The manifest is written last (tmp +
rename), so its presence marks a complete export.

**Syncing (new node):**

    for each height in order:
        count, err := kvs.ImportBlock(blockDir)

`ImportBlock` verifies every segment against its manifest hash *before*
importing anything, then applies the records. Imports are **idempotent**
(replay-safe immutability from the durability work): an interrupted
sync is resumed by re-running it, and re-importing an already-applied
block is a no-op. A key conflicting with a *different* local value
fails the import — divergent nodes are detected, not silently merged.

## Format

Segment file (streamed, hashed whole):

    header:  magic(4) version(4) sinceOffset(8) count(8)
    records: key(32) valueLen(8) value(valueLen)   ... × count

Records are sorted by value offset, so the export reads `values.dat`
sequentially and the import writes it sequentially.

## Scope and future work

- **v1 export cost**: selecting "keys since offset X" scans the shard's
  key records (kfile + one sequential pass of history.dat) per export.
  Fine at major-block cadence; for per-minor-block sealing, add a
  per-put segment journal so the export reads only the new records.
- **Dyna layer**: state is mutable and belongs to a *snapshot*, not a
  segment chain. A state snapshot export (same file format, whole
  layer) composes with segments: sync = latest snapshot + segments
  since.
- **Sealed segments as storage** (v2 direction): today segments are a
  transport format and the DB re-indexes on import. The deeper design —
  keeping sealed segments as the storage itself with per-segment key
  indexes — would make sync a file copy plus manifest update, remove
  the history file's move-and-rewrite costs, and make Compress
  crash-atomic (#19) by replacing in-place swaps with new sealed
  generations.
- Transport (fetching manifests/segments from peers) is the
  application's concern; the DB provides the sealed, verifiable units.
