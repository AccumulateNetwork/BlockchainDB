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
    segments.json             the active manifest: settings, the active tier's segments
    history.json              the history manifest: every segment below the window
    filters.dat               the live key filters, saved on close

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
binary search, and a pair of store-level filters over the last N to 2N
blocks settles most of the walk before it starts (*The key filter rolls
over a window of blocks*, below).

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

**Compaction is crash-atomic (issue #19).** `CompactHistory` writes a
new segment holding only the newest record per key of the run it
replaces, fsyncs it, and commits by replacing the history manifest —
one atomic rename. There is no window where keys and values disagree.
Measured: ten sealed generations of overwrites compact to one, ~90% of
bytes reclaimed, values unchanged.

A crash before that commit is safe and costs only the copy. The
compacted file takes the sequence after the run's newest segment, which
sits below the active tier, so the recovery rule below deletes it while
the run it would have replaced is still named and whole
(`TestDynaCompressCrashMidway`). Only a store with no active segment
at all — every segment in history — leaves the file above everything
the manifests name, and then it is adopted as a duplicate that the
next compaction folds away; that is the shape of issue #52.

## Crash recovery

The manifests are the commit points — the active manifest for sealing
and importing, the history manifest for merging, compacting, and
dropping — and the newest `(block, seq)` either of them names decides
what to do with a data file neither names:

- **above** the newest `(block, seq)` — a seal or import that reached
  disk but not the manifest. It is complete by construction (fsync
  precedes the rename), so it is adopted, its index rebuilt if missing,
  and the manifest updated.
- **at or below** — superseded by a committed merge or compaction;
  deleted.
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
   `KV2.Compress` is now a `CompactHistory` of the layer's history
   tier, so the layer's compaction is crash-atomic, off the protocol's
   lock, and `KV.Compress` is off the database's path (#19, #57). The Dyna layer seals on **physical records**, not
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
keeps the merge to the shard's own files, so shards merge
independently and in parallel. `KVShard.MergeFinalized` drives it per
shard and keeps going past a failure, reporting the first error.

Only **history** is merged — the segments the window of the last N to
2N blocks has rolled past (*Two tiers, two locks*, below). A watermark
inside the window merges what has left it and no more, and the rest
follows once the window has passed; the merge lags the watermark by up
to N blocks, and nothing about correctness needs either to be current.
With Accumulate's N of 64 and its watermark of `version − 64`, a
shard holds per-block segments for at most 128 blocks before they are
merged.

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

`KVShard.PackFinalized(height)` builds it: every shard's history
segments below the watermark — expected to be stage one's single
merged segment each, though it copes with more — into one set,
committed by the rename of the file and one fsync of the set
directory. The segments are read pinned, so a merge that commits
meanwhile leaves their files readable until the pack is done. Then
each shard **drops** those segments (`SegmentStore.DropBelow`), which
is a history commit of its own: two barriers per shard, off the
protocol path, run sixteen shards at a time so the barriers overlap.
It used to ride on the shard's next seal for free, but the seal writes
the active manifest now and the drop is history's to record. A crash
before a shard's drop leaves its history manifest naming segments the
set also holds; on open it drops them again
(`TestPackFinalizedSurvivesACrashBeforeTheShardsCommit`).

Measured, the same 20-block set of 40,000 keys: **~30 ms** for the
pack, one file of 7.6 MB, and the set's files go from 1,024 after
stage one to 1. Reading every packed key back took 2.5 µs a key from
the page cache; an absent key, settled by the shard's filter, 0.45 µs. A lookup of a packed key,
a rebuilt filter, an import, iteration, and block export are all
covered in `blockset_test.go` and `TestMergeDoesNotDisturbBlockExport`.

**The set store is attached inside each shard's Perm layer**, so that
`Get` is one call: a key that has left the segments is still the
shard's key, and a reader should not have to know where the shard
keeps it. The rolling key filters do not cover the packed keys — that
is what bounds them — with one exception: a set packed from blocks
still inside the window. Those keys were written inside the window, so
the immutability guarantee below covers them, and a filter rebuilt on
open from the segments alone would have lost them. A rebuild therefore
also adds every set that reaches the window's start block
(`coldStore.forEachKeySince`), bounded by the sets that overlap the
window rather than by the store. `TestPackedKeysStayImmutable` rewrites
a packed key after a rebuild and fails against a rebuild that skips
the sets.

Sets are the finalized record, so a block in one cannot be imported
again: a shard that has dropped every segment has no newest segment to
measure an import against, and the set's watermark is the bound.

Memory per set is the directory plus the bloom, so it grows with the
key count at ~1.5 bytes a key — the same order as the per-segment
filters it replaces. The next stage of #47, merging sets into larger
ones, is what bounds the number of sets a lookup walks.

### The key filter rolls over a window of blocks

A store-level filter exists to prove a key *absent* without touching a
segment. That is the question every immutable write asks — a new key
is absent by definition — and before the filter existed the answer
cost a bloom probe per sealed segment, so writes decayed with the seal
count (measured: throughput fell to 1.4% of its opening rate over two
hours). The first filter covered every key the store had ever held,
and that was its cost (issue #44). Measured on a store built to 1M
keys, sealing every 5,000:

| | per-segment blooms | store-level filter | total | bytes/key |
|---|---|---|---|---|
| whole-history filter, 1M keys | 1.5 MB | 7.45 MB (4 layers) | 8.95 MB | 8.95 |

85% of it was the store-level filter, and it grew with every key ever
written — a billion permanent entries would be ~10 GB of RAM.
Rebuilding it on open scanned every segment the store had ever sealed.

A running node does not reach far back in history, so the filter does
not need to. **Filters cover 2N blocks and a fresh one starts every N**,
so two are live at once and every write goes into both:

    blocks:   0 ────── N ────── 2N ────── 3N ────── 4N
    filter A  [───────────────)                          dropped at 2N
    filter B          [───────────────)                  dropped at 3N
    filter C                   [───────────────)         dropped at 4N

At block t the live pair reaches back N blocks just after a roll and 2N
just before the next, so what is resident is the keys of the last N to
2N blocks and never more. A filter that completes its span is dropped:
the block sets its blocks were packed into carry a filter of their own,
so writing it out would duplicate one. N is `FilterBlocks`, persisted
in each layer's manifest, `DefaultFilterBlocks` (128) unless
`SetFilterBlocks` says otherwise, and refused below `MinFilterBlocks`
(20) — the floor healing sets, since healing writes reach back several
blocks. A manifest without it is refused rather than defaulted.

Measured on the same fixture (1M keys, 200 blocks of 5,000; and 2,000
blocks of 500):

| fixture | store-level filters | of which resident at steady state | total filter bytes | bytes/key | crash reopen (rebuild) | puts/s |
|---|---|---|---|---|---|---|
| main, 200 blocks | 7.45 MB (4 layers) | grows without bound | 8.95 MB | 8.95 | 33 ms, every segment | 1,130,000 |
| **N=20, 200 blocks** | **615 KB** (two filters) | flat from block 60 on | 2.12 MB | **2.12** | **10 ms**, 40 blocks | 1,175,000 |
| N=100, 200 blocks | 4.2 MB, 0.9 MB once right-sized | two filters over 200 blocks | 5.7 MB | 5.71 | 31 ms, 200 blocks | 1,142,000 |
| main, 2,000 blocks | 7.45 MB | grows without bound | 15.6 MB | 15.64 | 96 ms, every segment | 531,000 |
| **N=20, 2,000 blocks** | **300 KB** | flat | 8.5 MB | 8.49 | **24 ms**, 40 blocks | 958,000 |

The 2,000-block rows are dominated by the per-segment filters — 4 KB
apiece at the 500-key floor, 8.2 MB in all — which is the cost merging
and packing exist to remove; the store-level share goes from 7.45 MB
to 300 KB. The put rates were taken with another suite on the disk and
are noisy to ±20%; the write path does two filter probes and two
insertions where it did one of each, and no cost shows.

The N=100 row shows the sizing settling: the first filter starts at
the seal limit's size and grows by layers as its 2N blocks fill,
which is the 4.2 MB; the next one is sized for what a full span took
and is 0.9 MB for the same coverage, and that is the steady state.

What the window costs is a `Get` for a key the store does not hold
anywhere: it used to be settled in memory (430 ns) and now walks what
the window does not cover — every segment below it in this fixture,
which packs nothing — at 4.7 µs (N=100) to 17.6 µs (N=20, 2,000
segments). In a sharded database those segments are merged and packed
and the cost is one bloom probe per set instead, below.

**What a filter covers is a block range**, and that is the whole of
its claim: filter S holds every key of every segment whose blocks all
lie at or above S, plus the live tail. A segment is not always one
block — a merge folds a run of blocks into one, and
`SegmentMeta.Span` records how far back it reaches — and a segment
reaching below S is simply not covered, so a lookup the filters cannot
settle walks it. Without the span a merged segment sitting at height
49 and reaching back to block 1 would be claimed by a filter that
started at block 40, and a key from block 10 would answer "not found"
(`TestKeyFilterCoversAMergedSegmentByItsOldestBlock`, which fails
exactly that way against a merge that records no span).

Every change to the block the tail is in — a block-boundary seal, an
import, a compaction, and `AdvanceBlock` on a shard that learns the
set has moved on — rolls the filters. A filter rolling in is built
from whatever already lies at or above its start: usually nothing,
because the roll is a block boundary and the tail was just sealed
below it, but a shard reopened after a quiet spell can jump many
rolls at once, and the tail it replayed is about to be sealed into
the block it now sits in (`TestKeyFilterFollowsABlockJump`). The
filter that starts is sized for the most keys any completed span has
taken, recorded at each roll — the hook issue #54 wants for sizing
from a recent history of spans.

**Immutability is a windowed guarantee.** A permanent key written in
the last N to 2N blocks cannot be overwritten, wherever it now sits —
a segment, a merged segment, a packed set — because the filters hold
it and a hit is followed to the end. A key older than the window is
*not consulted* on write: a rewrite of it appends a record, and a
read returns the newest. That is the repository owner's decision, on
two grounds: the check exists for replay safety, which is recent by
nature, and a Perm key is the hash of its value, so "same key,
different value" beyond the window would be a hash collision.
Searching back into cold data is API support — `Get` does it, one
bloom probe per set — and the consensus write path never needs it.
`TestImmutabilityIsWindowed` pins both halves.

The alternative was measured before it was declined: consulting every
set's bloom on the write path costs 19–48 ns per set (cache-bound at
scale), so 45 µs per write at 1,000 sets and 208 µs at the 4,320 sets
a day of 20-block sets produces, against a write that costs about a
microsecond. That is also what a `Get` for a key the store does not
hold costs now, and merging sets into larger ones (issue #47) is what
bounds it.

`ImportSegmentFile`'s divergence detection (`checkNoConflicts`) rests
on the same lookup and gives the same guarantee: a peer's segment is
refused if it holds a different value for a key written inside the
window, and adopted if the key it disagrees with is older. A peer's
segment is always the newest thing in the store, so what it could
diverge from is recent by construction.

The persisted filters carry a claim the manifest records only in the
commit written straight after the save: the window's start, the newest
segment inside it, and how many segments the window covers
(`filterClaim`). Any of the three moving — a segment adopted by
`recoverOrphans`, a drop for a set without a manifest, a reopen at a
block that wants a different window — rebuilds instead, and a rebuild
reads only the segments inside the window (and the sets that reach
it), not the store. The count is what separates "covers nothing" from
"covers segment (0, 0)", the ambiguity of issue #35; it is kept, and
`TestKeyFilterEmptyDoesNotCoverSegmentZero` still exercises it.

The Dyna layer runs on the same code and its block advances with every
`KV2.Seal`, so its filters roll like Perm's: what is resident is the
keys of the last N to 2N blocks of state writes, and what has left the
window is history, which is what makes it compactable off the
protocol path (*Two tiers, two locks*, below).

### Two tiers, two locks

A store holds two kinds of files with two different owners, and one
mutex used to cover both. Measured on an 8-node Accumulate network at
500 tx/s, with the adapter merging below `version − 64` and compacting
every 128 commits (issue #57):

| block | pause on every node | dynamic layer (one engine) |
|---|---|---|
| 400 | 12 s | — |
| 656 | 13–16 s | 5.9 GB, 292 segments |
| 784 | 18–19 s | 7.0 GB, 326 |
| 1,040 | 23–32 s | 9.8 GB, 400 |

About 2.3 s per GB of the dynamic layer, every 128 blocks, growing
without bound — because `Compress` rewrote the whole layer and
`MergeBelow` copied the whole run under the store's lock, so every
commit and every read on the node waited for the copy. Each pause
seeded a cross-partition backlog and the partitions fell behind.

The invariant now is: **the consensus path only ever reads and writes
the last 2N blocks, and the pause it can suffer from storage
maintenance is bounded by the size of those 2N blocks, never by the
size of the chain.**

**The tiers.** The *active* tier is the live tail plus the sealed
segments whose oldest block is at or above the window's start — the
same line the key filters draw, `tierStart`, a pure function of the
block height and N — and every active segment is covered by the
filters. *History* is every older segment, plus the packed sets. A
segment moves from active to history when the window rolls past it
(`handoffBelowWindow`, from `advanceBlock`), never back, except when
`SetFilterBlocks` widens the window at creation time. Tier membership
is derived, not recorded: on open each segment is placed by its oldest
block, so a crash at any moment changes nothing about where a segment
belongs.

**The locks**, and who takes which:

| operation | Mutex (active) | History | maint |
|---|---|---|---|
| `Put`, `PutIfAbsent` | exclusive | — (see below) | — |
| `Get` | shared, released | then shared, released | — |
| `Seal`, `SealNext`, `Sync`, `AdvanceBlock` | exclusive | exclusive, inside Mutex, for an append (the handoff) | — |
| `MergeBelow`, `CompactHistory` | — | shared to read the list; exclusive for the swap | whole operation |
| `DropBelow` | — | exclusive for the swap | whole operation |
| `PackFinalized` (per shard) | — | shared to read the list, pinned | — |
| `ImportSegmentFile` | exclusive | shared inside Mutex on a filter hit; exclusive inside Mutex if the segment lands in history | — |
| `ForEach` | shared, released | then shared, released; files pinned first | — |
| `Close`, `Open`, `SetFilterBlocks` | exclusive | exclusive, inside Mutex | — |

The order is always Mutex → History → the leaf locks (`handoffMu`,
`retireMu`), and maint → History; nothing ever takes Mutex while
holding History or maint, so no cycle exists. On the protocol path the
two are never held together: a `Get` reads the active tier under Mutex,
releases it, and only then reads history under History. A `Put` whose
key the filters rule out of the window — a new key — writes under
Mutex alone; a filter hit the active tier settles is answered under
Mutex alone; a filter hit it cannot settle releases Mutex, reads
history under History, and takes Mutex again for the write, starting
over if the window rolled in between (`epoch`). A history operation
copies immutable segments with no lock at all, writes its output
aside, and takes History exclusively for the swap and the history
manifest commit — two barriers — so the longest a history reader can
wait is that commit, and a commit or an active read never waits at
all. `KV2.MergeBelow` and `KV2.Compress` do not take the KV2 lock
either, and `Open`, which every shard operation calls first, answers
without a lock on an open store.

**Two manifests.** A commit and a merge each write their own, so
neither waits for the other and neither manifest grows with the
other's tier: `segments.json` carries the settings and the active
tier, `history.json` the history tier, and the store is their union
(a segment named by both is one segment). The handoff records nothing:
a segment the window has just rolled past stays named by the active
manifest until a history commit names it, and only the active commit
after *that* stops naming it — so at every moment every sealed segment
is named by at least one manifest on disk, and a crash between any two
commits is recovered by reading both. The files a history operation
retires are deleted at once if only the history manifest could have
named them, and after the next active commit if the active manifest
still might (`retireAfterActiveCommit`); until then a merge's inputs
sit beside its output, for one seal. Format version 4.

**The dynamic layer ages.** `KV2.Seal` advances the Dyna layer's block
too, so its auto-sealed segments carry the block they were sealed in
and its window rolls with Perm's. A state record last written inside
the window is active and compaction never reads or rewrites it; one
that has not been rewritten for 2N blocks is history. `CompactHistory`
— what `KV2.Compress` calls — rewrites a **run** of history segments
into one holding the newest record per key of the run: the newest
segment always, and each older one while it is no larger than
1/`CompactRatio` (4×, at the default 0.25) of what has gathered behind
it. A large old segment is therefore rewritten only once a quarter of
its size has arrived, which is what keeps the bytes rewritten over the
store's life a constant multiple of the bytes written rather than a
whole-layer copy per call; between rewrites it holds records the newer
segments have superseded, bounded by the ratio. A record in history
superseded only by one still in the window waits until that one has
rolled out and a run holds both — garbage the compaction cannot yet
see, bounded by the window. `TestCompressNeverTouchesTheWindow` pins
both halves. The garbage estimate that used to gate `Compress`
(`Shadowed`, issue #40) is gone with the whole-layer rewrite it gated.

**Measured.** A `KV2` with N=20, 1,000 blocks of 500 permanent keys
and a dynamic layer of the size shown, with a state key rewritten ~5
times on average so ~20% of the layer is live; a writer doing
`PutPerm` + `PutDyna` + `Seal` every 50 ms and a reader doing `GetPerm`
+ `GetDyna` of recent keys every 2 ms, while `Compress` and then
`MergeBelow(height − 64)` run once. The longest any of their calls
took while the maintenance ran, before and after, on one NVMe:

| dyna layer | segments | `Compress` before → after | `MergeBelow` before → after | max Put before → after | max Get before → after |
|---|---|---|---|---|---|
| 0.60 GB | 447 | 0.77 s → 0.71 s | 0.34 s → 0.32 s | **1.06 s** → <1 ms | **1.10 s** → 20 ms |
| 1.20 GB | 894 | 1.83 s → 1.71 s | 0.34 s → 0.33 s | **2.13 s** → <1 ms | **2.17 s** → 24 ms |
| 2.40 GB | 1,789 | 4.60 s → 4.81 s | 0.33 s → 0.35 s | **4.88 s** → <1 ms | **4.92 s** → 48 ms |
| 4.81 GB | 3,579 | 11.75 s → 11.09 s | 0.35 s → 0.33 s | **12.09 s** → <1 ms | **12.09 s** → 55 ms |

Before, the pause is the compaction: 2.4 s per GB, the curve of the
issue's table. After, the compaction takes the same time (it is the same copy, off the lock) and the longest a `Put` waited was under a millisecond in every row; the longest a `Seal` waited was 22, 22, 50 and 56 ms against an idle 15-23 ms, and the longest a `Get` 20, 24, 48 and 55 ms -- the history swap and the disk contention of a multi-GB copy, not the copy itself. Memory is lower too: heap 7/10/16/28 MB before against 6/7/10/14 MB after, and the Dyna layer's resident filters 2.4/4.7/9.4/18.7 MB before against 0.3/0.6/1.1/2.1 MB after, because the filter covers the window rather than the whole layer. The idle maxima — the same
writer and reader with no maintenance running — were 15–23 ms for a
`Seal` and under 1 ms for a `Put` or a `Get` in every row.

The deterministic tests are in `tiering_test.go`: hold `History`
exclusively and a `Put`, `Seal`, `Sync` and `Get` complete; hold
`Mutex` exclusively and a merge, a compaction and a pack complete; hold
a merge and a compaction between their copy and their swap
(`maintenanceHook`) and a `Put`+`Seal` and a `Get` complete within a
second. Against the old locking the last two fail with "did not
complete within 1s" and "did not complete within 10s".

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

### Reads share the lock; the file pool is sharded

Every read used to take the store's mutex exclusively, and `KV2.Get`
took its own exclusively above that. Sealed segments are immutable and
the pool hands out descriptors for `pread`, so a lookup against sealed
data needs no lock at all; the exclusive one existed only by default.
Measured on an 8-node Accumulate network at 500 tx/s: the busiest node
ran at ~110% CPU — one core, seven idle — while every shard's reads
queued on that lock and its hold time grew with the segment walk
(issue #50).

Readers now take both locks shared. Writers — anything that appends,
seals, imports, or closes — still take them exclusively, so a reader
never sees the live map or the segment list mid-change; a merge or a
compaction takes neither (*Two tiers, two locks*). Three things had to
follow:

- **The counters are atomic.** A plain increment under a shared lock
  is a data race.
- **The live tail is read with `pread`.** `BFile.ReadAt` was seek then
  read: two syscalls sharing one file offset, which interleaved between
  two readers hand each the other's bytes.
- **The file pool is sharded 64 ways** by a hash of the path. Once the
  store lock went shared, a mutex profile at eight readers put 100% of
  the remaining wait on the pool's single mutex — every lookup borrows
  an index file and a data file through it. Each shard enforces a
  sixty-fourth of the limit; a share of zero means the shard caches
  nothing, which is what a very small limit should mean.

Measured, one store with 1,000 sealed segments and 100,000 keys, random
`Get` from N goroutines:

| readers | exclusive lock | shared lock, one pool | shared lock, sharded pool |
|---|---|---|---|
| 1 | 107,000/s | 120,000/s | 112,000/s |
| 2 | 50,000/s | 207,000/s | 205,000/s |
| 4 | 50,000/s | 142,000/s | 392,000/s |
| 8 | 53,000/s | 125,000/s | 668,000/s |
| 16 | 48,000/s | 131,000/s | **976,000/s** |

The first column is the finding: under the exclusive lock, adding a
second reader *halved* throughput and it never recovered — a lock
convoy. The middle column shows the store lock was only the first wall.

Two deterministic tests pin the property rather than the timing: hold
the lock shared from outside and call `Get`; under an exclusive lock it
blocks forever, so the harness timeout is the failure.

What remains on the read path is the walk itself: at 1,000 segments a
hit probes 1,000 per-segment blooms (`Bloom.Test` was 25% of CPU in the
profile). That is the segment count, which block-set packing collapses
— a packed set is one bloom, not one per block.

### Iteration takes a snapshot and holds no lock

`ForEach` used to hold the store's mutex for the whole iteration, so a
callback that called `Get` deadlocked and any callback blocked every
other reader and writer for as long as it ran (issue #31).

It now copies the live tail's values and the segment lists — each tier
under its own lock, one after the other — releases them, and calls back
with nothing held. What the callback sees is the store as it stood when
iteration began. A compaction is free to commit underneath it; the
files it retires are unlinked when the last iteration finishes rather
than immediately, so the reads stay valid
(`TestForEachSurvivesConcurrentCompaction`).

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
adoption. That check is filter-gated: a syncing node's incoming keys
are almost all new, so the rolling key filters reject them from memory
and only a filter hit costs a real lookup. Measured cost is within
noise of an unchecked import (724K keys/s into a node holding 600K
keys), and a conflicting value fails the import instead of being
silently shadowed. The check reaches over the filters' window, N to 2N
blocks, and no further — the same guarantee a write gets, for the same
reasons (*The key filter rolls over a window of blocks*).
