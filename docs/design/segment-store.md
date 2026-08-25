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

`SegmentStore` is a complete, tested store, not yet wired in as KV2's
layers. Migration, in order:

1. Use it as the Perm layer behind the existing `KV` interface
   (`Put`/`Get`/`Close` already match; `Seal` is the added call, at
   block boundaries).
2. Use it as the Dyna layer — mutable mode plus `Compact` retires
   `KV.Compress` and #19 with it.
3. Retire `kfile.dat`/`history.dat` for Perm data, and with them
   `PushHistory` and the bin relocation logic.
4. Wire `ShardWriter.Flush` to `Seal` so a block boundary is one
   durability, sync, and compaction boundary.

Not yet done here: a per-segment key count cap (segments grow with the
seal cadence; a size-triggered seal or a tiered merge keeps the
segment count bounded on long chains), and reading a value with one
`ReadAt` on the value bytes rather than through the record header.
