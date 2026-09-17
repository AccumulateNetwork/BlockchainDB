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
packs exist to bound the file count and the length of a lookup's walk
(2.7), and both are properties of the *index*.  Today (`segstore.go`,
`seal.go`, `blockset.go`) each block seals a segment -- a body file and
an index file with an embedded filter -- and `MergeBelow` folds
segments with `concatSegments`, which copies every body byte-verbatim
into a new body and rebuilds the index over it; `PackFinalized` copies
the bodies again into a cross-shard set file.  An index record is 48
bytes, `key, offset, length`, with no file identifier: several bodies
are only ever addressed through an out-of-band base, which is why the
bodies have to be concatenated to be merged.  Instead:

- **A shard's data is a sequence of fixed-size files, appended and
  never moved**, the heap's file model (`HeapFileBytes`): a block's
  records go to the end of the current file, and a record is
  `(file, offset, length)`.  Nothing is ever copied; a file is deleted
  only by `DropBelow` when a pack no longer needs it -- and packs no
  longer copy either, so a data file lives until the retention policy
  above the store says otherwise.
- **An index record carries the file.**  `key(32) file(4) offset(4)
  length(4)`, 44 bytes, sorted by key as today.  `mergeIndexes` and
  `indexWriter` already merge indexes without touching a body; with
  the file in the record they need no bases.
- **Each block seals an index delta** with its filter: the sorted
  records of what the block appended, ~40 bytes a record.  The delta
  is what the window is made of; the live key filters (`keyfilter.go`)
  are built over deltas exactly as they are built over segments now.
- **The long search is bucketed, and merged a bucket at a time.**  A
  shard's index below the window is `PermBuckets` buckets (256 by
  default; nothing about the number is special, and a modulus serves
  as well as a mask), each holding a few sorted runs of 44-byte
  records with a filter per run.  Every block the maintenance step
  takes the next bucket in rotation, gathers that bucket's records
  from the deltas sealed since the bucket was last merged, writes them
  as a new run, and folds the bucket's runs by ratio; a delta older
  than every bucket's last merge is dropped.  So the work every block
  is a fixed slice, proportional to what arrived for one bucket, and
  the big fold is rare and bounded per pass -- never a rewrite of the
  shard's whole index, which would grow with the chain (1.2).  A
  merge swaps one bucket's runs under that bucket's lock; the other
  buckets keep answering.  A pack is the same fold across shards for
  a block range, with no bodies in it.
- **Reads keep their protocol-path rule** (1.3): the window is the
  last N blocks' deltas behind the live filters; an immutable key the
  filters deny is absent.  Below the window a key's bucket is probed
  newest run first, one filter and one binary search per run, and
  that is `GetDeep`'s walk; a lookup resolves a key to `(file,
  offset, length)` and does one `pread` of the data file.

### Sizing the buckets

`TestPermSizingSim` (`go test ./database/ -run TestPermSizingSim -v
-args -sim`) models one shard at the soak's rate, 1,040 permanent
records a block, against the store as it is (segments folded by
copying bodies every 20 blocks under the ratio rule, packed every
1,000 blocks).  Its first result was a warning: a bucket that is
never drained holds 1/B of the whole chain, so with any fixed count
the largest fold grows with the age of the store (256 buckets: 14 MB
at a day, 102 at a week, 460 at a month), against 1.2.

The bound is the pack watermark, which the store already has.  Every
`PackEvery` (1,000) blocks the finished history is packed and dropped
from the shard; the buckets cover only the history above the
watermark, and since the keys are hashes their shares are even.  A
bucket then holds at most 1,000 blocks of its share -- ~4,000
records, ~180 KB, at 256 buckets -- so a fold is bounded by the pack
period, not the chain, and nothing needs to split.  The count is a
free choice; 256 keeps runs small and a merge's lock small.

Per shard per block that comes to about: the block's delta (46 KB),
the bucket runs (46 KB), their folds (~46 KB) and the pack amortized
(46 MB of index every 1,000 blocks, 46 KB): ~180 KB a block against
today's 2.5 MB, every byte an index byte.  The pack is index-only,
46 MB a shard in place of 358 MB of bodies, and the chain's growth
lives where it lives today: in the sets below the watermark, grouped
by block range with one filter per finished group, reached only by
`GetDeep`.  Filters for the buckets and the newest sets stay
resident within a budget; older groups are probed cold, as now.

Ratio 0.25 stays (0.1 halves the runs a lookup probes but nearly
doubles the bytes written); every bucket is merged every 256 blocks
(B/256 buckets a block), which keeps the recent sections at a few MB
a shard.

### Shards partition keys, not files (measured)

The dynamic layer alone, nine stores, five minutes: seal p50 54-65 ms
at 8 storage shards, 67-79 ms at 64, 75-86 ms at 128, with a burst of
lost blocks every other minute at 64 and 128 and 8x and 16x the
files.  Every shard's files sync on their own at every block, so the
barrier count rises with the shard count; a hundred-plus shards are
right for spreading the retiring work and shrinking every lock, and
they must not each bring a barrier.  A block's records for every
shard go into one per-store append file, shard-tagged, and each
shard's index points into it: one data fsync per store however many
shards.

## The seal: one commit point per store per block

Today a non-empty block costs each shard four barriers -- the data
fsync, the index file's, the manifest's temp file, the directory --
and the store two more for its block record (`seal.go`,
`commitJSON`).  Nine stores of eight shards ask the device for about
360 barriers a second before any maintenance runs, and the platform
measured every seal goroutine waiting in `fsync` (#94).

With deltas as the sealed object, a block's permanent index delta is
appended to the same data file right after the block's records, and
the dynamic layer's delta after its entries the same way, so that:

1. Every shard fsyncs its data files once: entries and delta together,
   in parallel across shards -- one round.
2. The store's block record names, for every shard, the file and
   offset of the block's deltas: the existing `block.json` commit,
   temp file and directory -- one round.

Two rounds per store per block, in place of four per shard plus two.
Recovery trusts a delta only if its own checksum holds and every entry
it names checks, since one fsync does not order the delta's bytes
after the entries'; a delta that fails is the block that did not
commit, exactly as a torn delta is today.  This is 1.8's "one commit
point" and closes #33.

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
   the sharding and the adapter do not change.  *Built (PR #97).*
   Alone on the disk with nine stores (run 7): seal p50 53-61 ms in
   every minute, read p99 1-2 µs, maintenance ~10 s a minute, store
   3.8 GB at five minutes, zero wrong answers; the segment layer alone
   seals at 33-38 ms p50 but reads at 11-13 µs, spends 22-110 s a
   minute compacting, and had a compaction storm in minute 4 (seal
   max 1.5 s, 58 blocks missed).  The heap's remaining tail (p90
   150-250 ms in the minutes the mover copies most) is the mover's
   own fsync volume in the device queue, which the pass size paces.
2. The permanent layer as files of records with index deltas
   (`PermStore`, behind `KV2`'s permanent surface), the 44-byte index
   record, the bucketed long search merged in rotation, and packs over
   indexes.  *First cut built and wired (`perm.go`,
   `NewKVShardFilesN`, `bdbench -perm-files`); its first full-load run
   put the seal at 59 ms p50 in minute 1, then climbing to 209 by
   minute 5 because maintenance shared the seal's run file; fixed,
   remeasured next.*  Measured
   alone first (`-stores 9 -dyna 0`), then with the heap under the
   full load, which is the acceptance run.
3. The block's deltas in the data files: one barrier per shard per
   layer.  *Built for both layers.*  The heap's delta names no key:
   a block's entries are contiguous in its file and carry their
   keys, so the delta is the ranges the block wrote (the mover's
   named copies first, then the block's own) and the copies that
   arrived dead, a few dozen bytes a block; a delta of 44-byte
   records was a fifth of the heap's writes.  Replay scans the
   ranges and trusts the last delta only if every entry in it
   checks.  The permanent layer's delta stays a run with its filter,
   since it is the window's search structure, but lives in the data
   file too.  Alone, the heap seals at 44-54 ms p50 at nine stores
   (57-64 with two barriers).  The mover finds a file's live entries
   through the index rather than by scanning the file, because nine
   stores' scans together starved the block loops for CPU.  Still to
   do: the store-level commit (one block record naming every
   shard's deltas) and the per-store data file, so that a hundred
   shards cost a block one barrier.
