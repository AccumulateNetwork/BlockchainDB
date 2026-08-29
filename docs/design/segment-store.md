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

An index record is `key(32) offset(8) length(8)`. The offset is
**relative to the segment's body** — its records, after the 24-byte
header — not to the file. That is what lets a body and its index be
copied into a larger file byte for byte, with the container recording
where the body landed and a reader adding that base (see *Block-set
files*, below). For a segment file on its own the base is the header
length.

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

Not yet done here: reading a value with one `ReadAt` on the value bytes
rather than through the record header, and merging block sets into
larger ones — the levelled runs of issue #47 beyond the first two
stages below.

### Merging finalized segments

A block boundary seals one segment per shard that took writes, so the
file count grows with the chain and is bounded by nothing. Measured at
512 shards, 5,000 entries a block, that is ~1,016 files per block —
~88M a day at one block per second, against 240M inodes on the
filesystem this was measured on. Inodes run out in under three days,
and the failure is `ENOSPC` with terabytes free.

`MergeBelow(height)` merges the sealed segments belonging to blocks
below a watermark into one. Measured on a completed 20-block set:
**~8,980 segments to ~512, i.e. 17,960 files to 1,024**.

The watermark is what makes this safe, and it is why merging was
blocked before (issue #37). Merging permanent segments destroys the
block→segment mapping `ExportBlock` depends on — the mapping #27
established. Only segments *below* the watermark are merged, and the
caller sets it behind the healing window, so a block a peer might
still ask for by number is never merged away.
`TestMergeDoesNotDisturbBlockExport` runs the whole export/import round
trip across a merge.

The merged segment takes the sequence after the newest it replaces.
That slot is always free and correctly ordered: everything merged is at
or below `(H, S)`, and the first segment left standing is in a block
above `H`, because the run is exactly the segments below the watermark.

Crash safety inverts compaction's rule deliberately. A compacted
generation is written *above* the manifest's newest height, so an
uncommitted one is adopted — correct, because it holds every live key.
A merge is written *below* it, so an uncommitted one is deleted by
`recoverOrphans` — also correct, because it holds only part of the
store and the originals it would replace are still named by the
manifest. Either way the crash costs space and nothing else.

Merging happens **within a shard**, never across. That keeps the
working set small — a shard holds a few hundred entries per set — and
keeps the merge under the shard's own lock, so shards merge
independently and in parallel. `KVShard.MergeFinalized` drives it per
shard and keeps going past a failure, reporting the first error.

It is not a goroutine this package starts. Only the caller knows its
block rate and how far back healing may still write, which is what
sets the watermark.

The merge is a **copy, not a rewrite**. Perm keys are unique and
immutable, so there is nothing to reclaim and nothing to resolve: each
source segment's body is copied in as one sequential read, and its
index entries are shifted by where that body landed. No value is read
individually. The per-key work is the index — 48 bytes a key, already
sorted per source — merged and sorted per shard. (The first version
read every value back and re-encoded it, which is what compaction has
to do and a merge does not.)

Measured on the same fixture as before, a 20-block set of 2,000
entries a block across 512 shards:

| | time |
|---|---|
| serial, read-and-rewrite (previous) | 12.2 s |
| serial, copy (this) | 12.1 s |
| 16 goroutines, copy | **1.3 s** |

The serial figure barely moves because the merge is **fsync-bound, not
read-bound**: each shard pays four barriers — data, index, manifest,
directory — at ~5.5 ms each on this NVMe, which is ~11 s of the 12
across 512 shards. What the copy buys is the CPU and the read I/O,
which do not show at this size. What buys the wall time is running
shards concurrently, as the barriers overlap: 16 goroutines take the
set to 1.3 s, against 20 seconds of chain time. The driver wants
that concurrency, a watermark free to lag, and rate limiting (issue
#47).

### Block-set files

Stage one leaves one segment per shard per set. At 512 shards that is
still 1,024 files a set — ~4.4M a day at a block a second — so the
second stage packs the 512 per-shard results for one block set into
**one file** outside any shard:

    <db>/sets/set-<first>-<last>.bset

    header      magic "BSET", version, shard count, bloom K,
                first block, last block, key count, bloom bytes,
                offset where the bodies begin              (64 bytes)
    directory   per shard: body offset, body length,
                index offset, key count                    (512 × 32)
    indexes     every shard's sorted 48-byte key records, contiguous
    bloom       one filter over every key in the set
    bodies      every shard's records, in shard order

The head — directory, indexes, bloom — is one contiguous region whose
size is known from the key counts before anything is written, so it is
written with one call after the bodies are in place, and can be loaded
with one read. What a lookup keeps in memory is the directory (16 KB)
and the bloom (~1.5 bytes a key); the indexes stay on disk.

Keys are sorted **within each shard**, not across the file. A key
routes to its shard by `ShardIndex` before anything is looked up, so a
global order would cost a 512-way merge and buy nothing. And because
index offsets are body-relative, building the set is a byte-for-byte
concatenation: each shard's merged index is copied in unchanged, its
body is copied in unchanged, and the directory records where the body
landed. No index entry is touched.

A lookup that reaches a set costs a bloom probe, one read of the
shard's index slice (~10 KB at 5,000 entries a block, searched in
memory; slices over 64 KB are binary-searched on disk), and one read of
the value. Sets are walked newest to oldest; a key the shard's own
filter says is absent never reaches them.

`KVShard.PackFinalized(height)` builds it: every shard's segments below
the watermark — expected to be stage one's single merged segment each,
though it copes with more — into one set, committed by the rename of
the file and one fsync of the set directory. Then each shard **drops**
those segments (`SegmentStore.DropBelow`) without writing a manifest:
the shard's next seal records the drop, and only then are the files
deleted. That is deliberate. Recording it immediately would cost two
barriers per shard — ~5.6 s a set, the same cost issue #32 removed from
the block boundary — for a fact the next seal records for free. A crash
in between leaves a shard whose manifest still names segments the set
also holds; on open it drops them again
(`TestPackFinalizedSurvivesACrashBeforeTheShardsCommit`).

Measured, the same 20-block set of 40,000 keys: **~30 ms** for the
pack, one file of 7.6 MB, and the set's files go from 1,024 after
stage one to 1. Reading every packed key back took 2.5 µs a key from
the page cache; an absent key, settled by the shard's filter, 0.45 µs. A lookup of a packed key,
a rebuilt filter, an import, iteration, and block export are all
covered in `blockset_test.go` and `TestMergeDoesNotDisturbBlockExport`.

**The shard's key filter covers its packed keys.** Every write asks
whether its key is new, and the store-level filter is what answers
"no" without a disk read; that answer is only definitive if the filter
holds the keys that have left the segments. So the set store is
attached *inside* each shard's Perm layer rather than beside it: the
filter adds the cold keys whenever it is rebuilt, `get` consults the
sets only after the filter has said "maybe" and the segments have
said no, and a packed key rewritten with a different value is still
refused (`TestPackedKeysStayImmutable` — which fails against a filter
rebuilt from the segments alone).

Sets are the finalized record, so a block in one cannot be imported
again: a shard that has dropped every segment has no newest segment to
measure an import against, and the set's watermark is the bound.

Memory per set is the directory plus the bloom, so it grows with the
key count at ~1.5 bytes a key — the same order as the per-segment
filters it replaces. The next stage of #47, merging sets into larger
ones, is what bounds the number of sets a lookup walks.

### File descriptors are borrowed, not held

A segment used to keep its data and index files open for the life of
the process, and nothing closed or merged them: a node sealing per
block leaked two descriptors per block, and one test process here was
measured holding 14,935 (issue #30).

A segment now keeps only what a lookup needs in order to decide
*whether* to read — the key count and the bloom filter — and borrows
its files from a process-wide pool for the length of a read.
`SetOpenFileLimit` bounds the pool; the default is 512, an order of
magnitude under the 1024 that is still a common `ulimit -n`, leaving
room for the live tails and the host application. So the descriptor
count tracks the limit rather than the segment count
(`TestSegmentFDsStayBounded`).

Borrowing is reference counted, and that buys the second half: a file
with a live borrow is never closed, and on Unix an open descriptor
keeps reading after the path is unlinked. A reader can therefore hold
a segment open across a compaction that retires and deletes it, which
is what lets iteration run without the store's lock.

### Iteration takes a snapshot and holds no lock

`ForEach` used to hold the store's mutex for the whole iteration, so a
callback that called `Get` deadlocked and any callback blocked every
other reader and writer for as long as it ran (issue #31).

It now copies the live tail's values and the segment list under the
lock, releases it, and calls back with nothing held. What the callback
sees is the store as it stood when iteration began. A compaction is
free to commit underneath it; the files it retires are unlinked when
the last iteration finishes rather than immediately, so the reads stay
valid (`TestForEachSurvivesConcurrentCompaction`).

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
