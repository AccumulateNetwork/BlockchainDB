# Block Segmentation & Node Sync

Status: implemented (`database/segment.go`). Superseded in part by
`segment-store.md`: the Perm layer now *stores* sealed segments, so a
block export is seal-then-copy rather than a re-encode, and an import
adopts files rather than re-inserting records.

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

Each block directory holds a `manifest.json` and the segment files it
names, recording for each: its shard, its `(height, seq)`, its record
count, and the SHA-256 of the file. A shard contributes as many
segments as it sealed during the block — one per auto-seal when its
live tail filled, plus the boundary seal — and a shard that took no
writes contributes none. The manifest is written last (tmp + rename),
so its presence marks a complete export.

Manifests chain on the **block number**: block N+1 exports every
segment whose height is above N. That works because a boundary seal
advances the block each shard accumulates into, so every segment
sealed after block N's export carries a height above N. It is
deliberately not a per-shard high-water mark taken from the previous
manifest: a shard quiet during block N appears nowhere in it, and a
per-shard rule has no entry to skip that shard by, so it re-exports
every segment the shard holds — into every block until it next seals
(`TestExportBlockQuietShardNotReexported`).

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

- **Export cost**: resolved by sealed segments. Export selects whole
  sealed files rather than scanning key records, so it reads only what
  the block actually added. (In v1, since removed, selecting "keys
  since offset X" scanned the shard's key records once per export.)
- **Dyna layer**: state is mutable and belongs to a *snapshot*, not a
  segment chain. A state snapshot export (same file format, whole
  layer) composes with segments: sync = latest snapshot + segments
  since. The layer is *stored* as segments now (mutable mode, newest
  shadows oldest, compaction reclaims what the shadowing left) — but
  its segments stay local: they are not exported by `ExportBlock` and
  carry no hash, because a peer never receives them one at a time.
- **Sealed segments as storage**: done, for both layers — see
  `segment-store.md`. Sync is a file copy plus a manifest update, v1's
  move-and-rewrite is off the write path, and compaction is
  crash-atomic (#19) because a new sealed generation replaces the
  in-place swap.
- Transport (fetching manifests/segments from peers) is the
  application's concern; the DB provides the sealed, verifiable units.
