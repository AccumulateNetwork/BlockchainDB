# Sealed Segments as Storage

Status: implemented (`database/segstore.go`); both KV2 layers are
segment stores. Measured 2026-08-25.
This is the v2 direction sketched in `block-segmentation.md`.

## The change

v1 keeps data in `kfile.dat` + `history.dat` + `values.dat` and treats
segments as an *export format*: syncing a peer re-inserts every record.
v2 makes the segment format the *storage* itself.

    live.dat                  records accepted since the last seal
    seg-<block>-<seq>.dat     a sealed, immutable segment (the transport format)
    seg-<block>-<seq>.idx     its index: sorted 48-byte key records + a bloom
    segments.json             the manifest: segments, counts, hashes

Writes append to the live tail. `Seal(height)` turns the tail into an
immutable segment; nothing already written is ever moved or rewritten.

A segment is identified by **(block, seq)**, not by block alone. The
block is globally agreed, which is what lets a peer decide whether it
already holds a segment; `seq` orders the segments within one block. A
tail that fills mid-block seals itself, and those auto-seals take the
next `seq` rather than a block number of their own. Conflating the two
was issue #27: auto-seals consumed block numbers, so once a shard's
tail had filled more times than the current block, every subsequent
block boundary failed permanently and block export stopped working
altogether.

`Seal(height)` closes a block and advances the block the tail
accumulates into, so auto-seals that follow are tagged with the block
they actually belong to. Re-closing a block that is already closed is
rejected.
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

**Sealing does not read the segment back.** A seal (or a compaction)
knows where every value landed as it writes it, so it builds the index
from those records rather than rescanning the file it just produced.
That took a 25,000-key seal from ~56 ms to ~49 ms; the scan it replaced
issued one read per record. `buildIndexFor` still exists for the cases
that genuinely arrive with only a file — an imported segment, or one
adopted in recovery — and now scans through a buffer.

**A mutable store does not hash its segments.** The SHA-256 in the
manifest is what a peer checks against what it was sent. Dyna segments
are never sent anywhere: they hold state, which a node rebuilds or
snapshots rather than syncing segment by segment. Hashing them would
cost a full read of the store on every seal and every open, to verify
bytes against nothing.

**No move-and-rewrite.** v1's `HistoryFile.UpdateKeySet` relocated a
whole key bin whenever it outgrew its slot. Sealed segments never
move, so a write costs what it weighs.

**Compaction is crash-atomic (issue #19).** `Compact(height)` writes a
new generation holding only live keys, fsyncs it, and commits by
replacing the manifest — one atomic rename. There is no window where
keys and values disagree. Measured: ten sealed generations of
overwrites compact to one, ~90% of bytes reclaimed, values unchanged.

A crash before that commit is safe but not free. The compacted file
sits above every height the manifest names, so the recovery rule below
adopts it as the newest segment rather than deleting it — correct,
since newest wins and it holds every live key, but the old generation
it was meant to replace is still on disk. What the crash costs is that
space, until the next compaction. (An earlier draft of this document
said the orphan was swept; it is adopted. `TestDynaCompressCrashMidway`
pins the behaviour.)

## Crash recovery

The manifest is the commit point for sealing, importing, and
compaction, and its newest `(block, seq)` decides what to do with a
data file the manifest does not name:

- **above** the newest `(block, seq)` — a seal or import that reached
  disk but not the manifest. It is complete by construction (fsync
  precedes the rename), so it is adopted, its index rebuilt if missing,
  and the manifest updated.
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

2. **Done** — `SegmentStore` is KV2's Dyna layer too, in mutable mode.
   `KV2.Compress` is now a seal plus a `Compact`, so the layer's
   compaction is crash-atomic and `KV.Compress` is off the database's
   path (#19). The Dyna layer seals on **physical records**, not
   distinct keys: a mutable layer leaves one record per write, so a
   handful of hot state keys rewritten every block would hold the key
   count flat while the tail — replayed in full on every open — grew
   without bound.

   Measured on the layer's own workload (`TestDynaCost`:
   400,000 writes over 50,000 keys, compacting every 100,000):

   | | puts/s | on disk |
   |---|---|---|
   | v1 kfile + `KV.Compress` | 136,000–171,000 | 12 MB |
   | v2 segments + `Compact` | 223,000–248,000 | 14 MB |

   ~1.5×, and the write path no longer has a two-file swap to crash in
   the middle of. The extra ~2 MB is the 40-byte record header each
   value carries in a segment; v1 stores values bare and keeps its keys
   in a separate file.

3. **Done** — v1 is gone. Nothing on the database's path reached
   `KV`, `KFile`, or `HistoryFile` any more, so `PushHistory` and the
   bin relocation logic went with the layer they belonged to, along
   with `offsetsCnt` and `MaxCachedBlocks`, which no longer reached
   anything: `NewKV2(directory, sealLimit)` and
   `NewKVShard(directory, sealLimit)` are the constructors now.

   `ShardIndex` and `recordSort` moved to the files that still use
   them.  `DefaultBloomCapacity` moved to `bloomset.go` for lack
   of a better home: despite the name, nothing sizes a filter with it
   -- segments size theirs from their own key count -- and its only
   consumer is `KV2.Open`, as the fallback for a store written before
   `SealLimit` was persisted.  Renaming it is follow-up work.

   `SealLimit` itself is now persisted in each layer's manifest
   alongside `Mutable`.  It had not been, which would have made
   retiring v1 a regression rather than a simplification: v1's `Header`
   round-tripped the equivalent `KeyLimit`, so deleting v1 deleted the
   last implementation of that guarantee.  `NewKV2` also rejects a
   limit above `MaxInt32`, which used to narrow to a negative number
   and silently disable sealing altogether.

   The measurement tests that compared the
   two layers kept their v2 halves (`TestSyncCost`, `TestDynaCost`);
   the numbers in this document are the last measured comparison, and
   reproducing them means checking out a commit before the removal.
   `TestCrashRecovery` was retargeted from `KV` to `KV2` rather than
   deleted, so the SIGKILL durability contract is still enforced end
   to end.
4. **Done** — a segment is identified by `(block, seq)` rather than by
   block alone, so a tail that fills mid-block no longer consumes block
   numbers (issue #27). `ExportBlock` also seals every shard before
   copying any of them: sealing is durable and cannot be rolled back,
   so a failure partway through the copy phase would otherwise leave a
   block half-built with some shards already closed at that height.
5. Next — wire `ShardWriter.Flush` to `Seal` so a block boundary is one
   durability, sync, and compaction boundary.

Not yet done here: a per-segment key count cap (segments grow with the
seal cadence; a size-triggered seal or a tiered merge keeps the
segment count bounded on long chains), and reading a value with one
`ReadAt` on the value bytes rather than through the record header.

Sealing is also still where the Perm layer's time goes — about 60% of
it at a 25,000-key block, of which the largest remaining piece is the
SHA-256 read-back of the segment just written. It cannot be folded
into the write, because the record count in the header is not known
until the seal and the hash covers the header. Hashing the body alone
would let the write stream it, at the cost of a transport format that
no longer authenticates its own header.

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
