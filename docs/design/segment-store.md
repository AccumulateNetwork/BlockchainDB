# Sealed Segments as Storage

Status: implemented (`database/segstore.go`), measured 2026-08-24.
This is the v2 direction sketched in `block-segmentation.md`.

## The change

v1 keeps data in `kfile.dat` + `history.dat` + `values.dat` and treats
segments as an *export format*: syncing a peer re-inserts every record.
v2 makes the segment format the *storage* itself.

    live.dat            records accepted since the last seal
    seg-<height>.dat    a sealed, immutable segment (the transport format)
    seg-<height>.idx    its index: sorted 48-byte key records + a bloom
    segments.json       the manifest: segments, counts, hashes

Writes append to the live tail. `Seal(height)` turns the tail into an
immutable segment; nothing already written is ever moved or rewritten.
Lookups check the tail, then segments newest to oldest — each segment's
bloom filter (sized from its own key count) keeps that to about one
binary search.

An immutable store rejects a conflicting value and treats an identical
rewrite as a no-op; a mutable store lets newer segments shadow older
ones, which is what makes state overwrites and compaction work.

## What it buys

**Sync is a file copy.** A sealed `.dat` is byte-identical to what a
peer sends. `ImportSegmentFile` verifies the hash, copies the file,
builds the index, and commits one manifest update — no record is
re-inserted. Measured, 200K keys of 100–500 B:

| | seconds | keys/s |
|---|---|---|
| v1 re-insert, empty node | 0.64 | 313,021 |
| v2 copy segments, empty node | 0.30 | 665,478 |
| v1 re-insert, node holding 600K keys | 0.90 | 222,859 |
| v2 copy segments, node holding 600K keys | 0.29 | 679,941 |

2.1× on an empty node, 3.1× on a partially synced one — but the
multiple is not the point. **v2's cost is flat** (679K vs 665K keys/s
with 600K keys already stored) because adopting a segment does not
consult existing data, while v1 must check each incoming key against
everything the node already holds, so its gap widens with database
size. Partially synced nodes are exactly the case the original design
ToDo named.

**Lookups are competitive.** Full key→value reads at 1M keys, four
sealed segments: **4,927 ns/op vs 5,853 ns/op** for v1's KV — ~16%
faster, without v1's `history.dat` bin relocation on the write side.

**No move-and-rewrite.** `HistoryFile.UpdateKeySet` relocates a whole
key bin whenever it outgrows its slot. Sealed segments never move, so
a write costs what it weighs.

**Compaction is crash-atomic (issue #19).** `Compact(height)` writes a
new generation holding only live keys, fsyncs it, and commits by
replacing the manifest — one atomic rename. There is no window where
keys and values disagree; a crash before the commit leaves the old
generation in force, and the orphaned file is swept on the next open.
Measured: ten sealed generations of overwrites compact to one, ~90% of
bytes reclaimed, values unchanged.

## Crash recovery

The manifest is the commit point for sealing, importing, and
compaction, and its newest height decides what to do with a data file
the manifest does not name:

- **above** the newest height — a seal or import that reached disk but
  not the manifest. It is complete by construction (fsync precedes the
  rename), so it is adopted, its index rebuilt if missing, and the
  manifest updated.
- **at or below** — superseded by a committed compaction; deleted.
- `*.tmp` — never complete; deleted.

The live tail is replayed record by record on open; a torn trailing
record from a crash mid-write is dropped.

## Status and integration path

1. **Done** — `SegmentStore` is KV2's Perm layer. `KV2.Seal(height)` /
   `KVShard.SealBlock(height)` seal at block boundaries, and the layer
   auto-seals when its live tail reaches `SealLimit` keys (carried over
   from `KeyLimit`) so unsealed state stays bounded between blocks.
   Block export is now seal-then-copy: `ExportBlock` copies the sealed
   files, and `ImportBlock` adopts them.

   Measured effect on the running database (`TestMultiCoreScaling`,
   Perm writes, same seal cadence as the old history-push cadence):

   | | before (kfile+history) | after (segments) |
   |---|---|---|
   | 1 goroutine | 683,000 puts/s | **1,299,000** |
   | 16 goroutines | 3,668,000 puts/s | **11,328,000** |

   The Perm write path is now an append plus a map insert: no history
   push, no kfile rewrite, no bin relocation, and no `Stat` per put.

2. Next — use it as the Dyna layer; mutable mode plus `Compact`
   retires `KV.Compress` and #19 with it.
3. Then — delete the now-unused Perm paths in `KFile`/`HistoryFile`
   (`PushHistory` and the bin relocation logic).
4. Then — wire `ShardWriter.Flush` to `Seal` so a block boundary is one
   durability, sync, and compaction boundary.

Not yet done here: a per-segment key count cap (segments grow with the
seal cadence; a size-triggered seal or a tiered merge keeps the
segment count bounded on long chains), and reading a value with one
`ReadAt` on the value bytes rather than through the record header.

### Divergence detection on import

Adopting a segment file skips the per-key immutability check that
re-inserting records performed, so an immutable store verifies the
incoming keys against what it already holds before committing the
adoption. That check is bloom-gated: a syncing node's incoming keys
are almost all new, so the existing segments' filters reject them from
memory and only a filter hit costs a real lookup. Measured cost is
within noise of an unchecked import (724K keys/s into a node holding
600K keys), and a conflicting value fails the import instead of being
silently shadowed.
