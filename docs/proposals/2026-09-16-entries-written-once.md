# Proposal: entries are written once; keys are managed separately

*Status: proposal, 2026-09-16.  On acceptance this becomes spec text
in 1.5 (dynamic layer), 1.8 (durability), 2.3 (seal) and 2.7
(maintenance), and #94 leaves the register.*

## The problem, measured

The store rewrites entries in order to manage keys, and the rewrites
are what the commit path waits for.

`cmd/bdbench -stores 9` (spec 2.11; PR #95) on the soak machine's
NVMe, nine stores of eight shards, each taking the soak's per-block
volume (11k dynamic rewrites, 8.3k permanent appends, 40k lookups) on
a 1 s block:

| minute | seal p50 / p90 / max | block p90 / max | blocks of 540 | maintenance work per minute | write |
|---|---|---|---|---|---|
| 1 | 65 / 84 / 107 ms | 426 / 651 ms | 540 | 20 s | 117 MB/s |
| 3 | 116 / 731 / 1,244 ms | 1,218 / 2,889 ms | 494 | 229 s | 240 MB/s |
| 5 | 122 / 798 / 1,960 ms | 1,635 / 4,791 ms | 454 | 215 s | 233 MB/s |

One store alone ran 30 minutes flat (seal p50 51 ms, block p50
205 ms).  Twenty goroutine snapshots of the nine-store run at minute
4: 518 of 519 seal shard goroutine samples in `fsync`; the maintenance
goroutines in `fsync` as well (55 of 56).  The device is not short of
bandwidth.  It is short of barriers, and two things fill its queue:

1. **The seal's own barriers.**  The permanent layer seals every shard
   every block: four fsyncs per shard, three deep (data and index,
   then the manifest's temp file, then the directory), plus the
   dynamic tail's sync.  Nine stores of eight shards ask the device for
   about 360 fsyncs a second before any maintenance runs (#33).
2. **Maintenance rewriting entries.**  Per minute across the nine
   stores: dynamic compaction 121 s, permanent merge 127 s.  The merge
   costs as much as the compaction and rewrites data that never
   changes: every block's segment is folded every 20 blocks, then
   packed every 1,000, then packed again.  Write amplification of
   three to four on write-once data; and on the dynamic side a fold
   reads a 220 MB segment and its 90 MB index back to drop the dead
   records between them (soak 20260916T185711Z, bvn2-val4).

## The rule

> **An entry is written to the store once.  Reclaiming space and
> bounding the walk are done on keys, never by rewriting entries.**

Entries move only to make a bigger hole, one entry per step, bounded
(1.2).  Everything else the store does to stay small and fast --
compaction, merge, pack -- operates on indexes, which are an order of
magnitude smaller than what they index (a key and a location, ~40 B,
against a ~300 B value) and can be rebuilt from the entries.

## The dynamic layer: files of entries, a map of keys

The dynamic layer holds a bounded key set rewritten forever.  Today
every rewrite appends and the old copy is garbage until a fold
rewrites everything around it, index and filter included.  Instead
(`database/heap.go`, opened with `NewKVShardHeapN` / `NewKV2Heap`,
recognised on open by its directory; `bdbench -dyna-heap`):

- **An entry is written once, at the end of a file.**  An entry is
  `[len][height][key][value][checksum]` at its exact aligned length;
  a key is `(file, offset, length)`.  Data files are fixed-size
  (`HeapFileBytes`, 16 MB); a block appends to the current file, so
  the block sync is one sequential fsync per file the block touched.
  (Filling holes at put time was built first and measured: a block's
  11k rewrites scattered over a 380 MB heap dirtied a page each, and
  the barrier wrote 4x the ingest -- run 3, 443 MB/s against run 2's
  117.  Size classes were measured too: a third of the store wasted
  for nothing, once nothing was allocated from free lists.)
- **A rewrite within the block reuses the slot** when the entry keeps
  its aligned size: nothing durable names it yet, and the file stays a
  contiguous sequence of entries a scan can walk.
- **A rewrite in a later block appends.**  The slot the last durable
  index names is never overwritten, or a crash between the write and
  the block's sync would expose an uncommitted value under a committed
  name.  The old slot is dead where it lies.
- **The mover makes the space.**  On the maintenance cadence a pass
  takes the deadest files -- once half dead, or whatever their ratio
  while dead bytes exceed live, which bounds the heap at twice its
  live set -- copies their live entries into a file of its own, and
  marks a file left with nothing live for deletion by the sync after
  the delta naming the copies.  The mover's file is not the block's,
  so their barriers never share an inode (measured: the two fsyncs on
  one file flushed each other's pages, seal p50 193 ms).  The pass
  holds the shard's lock only to pick, to plan and to name, never
  across a read, a write or an fsync (measured: reading a 16 MB file
  under the lock put the seal at 229 ms).  A key rewritten while its
  copy is in flight leaves the copy dead on arrival.  The store
  reports bytes scanned against bytes copied: the heap's write
  amplification.
- **The key map is in memory** for the live dynamic key set (the
  soak's half million keys are ~50 MB per store in Go; 1.2 allows
  memory that scales with the working set).  What the seal makes
  durable is a delta of the keys the block touched; a snapshot starts
  a new index generation in a file of its own on the maintenance
  cadence, written aside, fsynced and renamed into place, so no delta
  is ever truncated away.

The layer's size converges to O(live keys) by construction (1.5),
without the deeper fold 2.7 allows today, and nothing but entries is
ever copied.

## The permanent layer: append-only data, merged indexes

The permanent layer is write-once; it has no garbage.  Its merges and
packs exist to bound the file count and the length of a history walk
(2.7).  Both are properties of the *index*, so:

- **A shard's data is one growing file** (or one per `PackEvery`
  blocks).  A block's records are appended; nothing is ever copied.
- **Each block seals an index delta**: the sorted `(key, offset)` pairs
  of the records the block appended, with its filter (2.5).
- **Merge and pack fold index deltas** into one sorted index per shard
  per level, on the adapter's cadence, off the protocol path, exactly
  as today's tiers do -- but moving ~40 B per record instead of ~300.
  The walk length and the file count are bounded by the index tiers;
  the data file is never in the walk (a key resolves to an offset,
  then one `pread`).

## The seal: one commit point per store per block

With indexes as the sealed object a block is one barrier round per
store rather than four per shard:

1. Every shard's data appends (permanent) and slot writes (dynamic)
   for the block are fsynced, in parallel: one round.
2. The block's index deltas -- permanent `(key, offset)` and dynamic
   `(key, location)` for every key the block touched -- are written to
   one per-store block file and fsynced: one round.
3. The store's manifest names the block file: the existing commit,
   one round.

Three rounds per store per block, none per shard beyond the parallel
data sync, in place of four rounds per shard.  This is 1.8's "one
commit point" and closes #33.

## Durability and crash consistency (1.8)

- **A file is deleted one seal late.**  A file emptied by the mover
  is deleted only after the delta naming the mover's copies out of it
  is durable.  Until then the durable index still names its slots,
  and a crash must find them intact: never unlink what a durable
  index names.  The adapter never asks the store for an old version
  (its pre-images are memoized on its side), so deletion waits on the
  seal and on nothing else.
- **A torn slot is detected, not misread.**  Every entry carries its
  length and a checksum; an entry above the committed height is a
  block that never synced.  An index entry is durable only after the
  slot it names is, so a torn slot is never named; open cuts the
  files back to their last named slot and drops a torn delta whole.
- **The key map is rebuilt on open** from the newest whole index
  generation: its snapshot, then its deltas.  A generation whose
  write was interrupted is not whole and is removed.
- **Repair reads the keys from the data.**  With the index gone, a
  scan of the data files rebuilds the map: every entry carries its
  key and the height that wrote it, so the highest committed copy of
  a key wins (later in the scan on a tie) and an entry above the
  committed height, which only the store above knows, is dropped.  A
  child process killed mid-block, three times over, reopens and
  repairs to exactly the last durable block (`heap_crash_test.go`).
- **Nothing durable is ever overwritten by a different entry.**  1.7's
  identity rule holds for files: a data file is appended, never
  republished.

## What it is measured with

`cmd/bdbench -stores 9`, the run above, is the acceptance test:

- seal p90 under `-seal-budget` (100 ms) and flat from minute 1 to
  minute 30;
- every block inside the interval;
- maintenance bytes per minute within a small factor of ingest, and
  a pass never longer than a block;
- store size tracking the live key set plus the permanent appends;
- zero reads returning anything but the last written value.

## Order of work

1. The dynamic heap, behind the existing `KV2` dynamic surface, so
   the sharding and the adapter do not change.  *Built.*  Alone on
   the disk with nine stores it holds the seal at ~54 ms p50 with no
   compaction spikes, reads at 1-2 µs p99, and maintenance at a tenth
   of the segment layer's; the segment layer alone had a compaction
   storm in minute 4 (seal max 1.5 s, 58 blocks missed).
2. The permanent index deltas and the single block file, which also
   brings the seal to one commit point.
3. Merge and pack over indexes.
