# BlockchainDB Performance Guide

This guide covers the knobs that affect BlockchainDB's performance and
the trade-offs behind them.

## Performance Characteristics

1. **Writes are appends.**  A `Put` appends a record to the live tail
   and inserts into a map.  Nothing already written is moved or
   rewritten, so a write costs what it weighs no matter how much the
   database already holds.

2. **Reads are bounded by segment count, not key count.**  A lookup
   checks the live tail, then each sealed segment newest to oldest.
   Every segment carries a Bloom filter sized from its own key count,
   so a segment that does not hold the key is rejected from memory --
   and a pair of store-level filters over the last N to 2N blocks
   settles every segment inside that window with one probe, so a write
   (which asks whether its key is new) does not pay per segment.

3. **Sync is a file copy.**  Sealed segments are the storage format
   *and* the transport format, so adopting a peer's segment is a copy
   plus one manifest commit -- not a re-insertion of every record.
   The cost does not grow with what the receiving node already holds.

4. **Shards give you cores, and so do readers.**  Keys route to one
   of 512 shards by `ShardIndex`, and each shard has its own lock, so
   writes to different shards proceed in parallel.  Reads take every
   lock shared, so readers of the *same* shard proceed in parallel
   too: measured 107,000 gets/s with one reader and 976,000 with
   sixteen on a store of 1,000 sealed segments.  Under the exclusive
   lock reads used to take, a second reader halved throughput.

Measured on the running database (see
[Segments as storage](design/segment-store.md) for methodology):

| | puts/s |
|---|---|
| Perm writes, 1 goroutine | 1,299,000 |
| Perm writes, 16 goroutines | 11,328,000 |
| Dyna writes with compaction | 223,000-248,000 |

## Performance Tuning Parameters

### Seal Limit

`NewKVShard(directory, sealLimit)` sets the point at which a layer
seals its live tail into an immutable segment.  It is the main knob:

- **Higher** means fewer, larger segments.  Lookups check fewer
  segments, and sealing happens less often -- but the unsealed tail
  (and the memory tracking it) is bigger, and it is replayed in full
  on every open.
- **Lower** means a bounded tail and a fast open, at the cost of more
  segments to search and more time spent sealing.

The Perm layer seals on distinct keys; the Dyna layer seals on
*physical records*, because a mutable layer leaves one record per
write and a handful of hot keys rewritten every block would hold the
key count flat while the tail grew without bound.

### Filter Window

`SetFilterBlocks(n)` on a `KVShard` or `KV2` sets N for both layers:
the roll period of the key filters (default `DefaultFilterBlocks`, 128;
minimum `MinFilterBlocks`, 20), and the line between the **active
tier** -- the live tail and the sealed segments of the last N to 2N
blocks, which is all a commit writes and a consensus-path read hits --
and **history**, which is everything older and is what merging,
packing and compaction work on, under a lock of its own.  A fresh
filter starts every N blocks and covers 2N, so two are live at once
and the store holds the keys of the last N to 2N blocks in memory --
and no more, whatever the chain's length.  It is persisted in each
layer's manifest; set it when the database is created, since changing
it commits a manifest per layer per shard.

N is the caller's, and it is what bounds the pause a commit can suffer
from storage maintenance: a merge or a compaction copies history with
no lock and swaps under history's own lock, so the protocol path waits
for nothing that grows with the chain (see *Maintenance Pauses*,
below).

N is the reach of the immutability check: a permanent key written in
the last N to 2N blocks cannot be overwritten; older history is not
consulted on write (see [Segments as storage](design/segment-store.md)).
It also sets two bounded costs, at ~1.5 bytes a key for the keys of 2N
blocks resident, and the index of every segment inside the window
rescanned on a reopen after a crash (48 bytes a key).  Measured at 1M
keys:

| | store-level filter bytes | crash reopen | absent-key `Get` |
|---|---|---|---|
| whole-history filter (before) | 7.45 MB, growing without bound | rescans every segment: 33 ms at 200, 96 ms at 2,000 | 430 ns |
| N=20 | 615 KB, flat | 40 blocks: 10 ms | 8 µs (walks 180 uncovered segments) |
| N=100 | 0.9 MB at steady state | 200 blocks: 31 ms | 4.7 µs |

A `Get` for a key the store does not hold anywhere probes every packed
set's filter after the window says no: 19-48 ns per set, so ~45 µs at
1,000 sets.  Merging sets into larger ones (issue #47) is what bounds
that.

### Open File Limit

`SetOpenFileLimit(n)` bounds how many segment files the process keeps
open (default 512). Segments borrow their files from a shared pool for
the length of a read rather than holding them, so the descriptor count
tracks this number and not the number of segments sealed. Raise it to
spend descriptors on avoiding reopens; lower it under a tight
`ulimit -n`. Files being read are never closed, so the limit is a
target the pool returns to rather than a ceiling at every instant.

### Finalisation Watermark

Finalisation is two stages, both driven by the caller from its own
scheduler, both below a watermark `height` that must sit behind
whatever window may still be written to -- healing, in Accumulate's
case -- because segments at or above it are left alone so that block
export keeps working.

Both stages work on **history** -- the segments the window of the
last N to 2N blocks has rolled past -- and never on a segment still
inside the window, whatever the watermark says.  A watermark inside
the window finalises what has left it, and the rest follows once the
window has passed; the merge lags the watermark by up to N blocks.
Neither stage takes a shard's store lock: the copy is lock-free and
the commit is a swap under the shard's history lock.

1. `KVShard.MergeFinalized(height)` merges each shard's history
   segments below the watermark into one.  Sealing produces a segment
   per shard per block, so without this the file count grows with the
   chain and nothing bounds it: measured at 512 shards and 5,000
   entries a block, ~1,016 files per block, ~88M a day at one block per
   second against 240M inodes.  A completed 20-block set goes from
   17,960 files to 1,024.

   The merge copies bodies and shifts index entries; no value is read
   individually.  Its cost is fsync -- four barriers per shard -- so
   run it **concurrently across shards**: the same 20-block set of
   2,000 entries a block measured 12.1 s serially and 1.3 s with
   16 goroutines, against 20 seconds of chain time.

2. `KVShard.PackFinalized(height)` packs every shard's merged segment
   for the set into **one block-set file** under `<db>/sets/`, then
   drops those segments from the shards.  The 1,024 files become 1.
   Measured on the same set: ~30 ms for the file, one file of 7.6 MB
   for 40,000 keys.

   Each shard's drop is a history-manifest commit of its own -- two
   barriers -- run sixteen shards at a time, off the protocol path.
   A merge's inputs that the active manifest still names wait for the
   shard's next seal before they are deleted (*Two tiers, two locks*
   in [Segments as storage](design/segment-store.md)).

A lookup that reaches a set costs a bloom probe, one read of the
shard's index slice, and one read of the value; a key the shard's own
filter says is absent never reaches the sets.  Measured over the packed
20-block set: 2.5 µs per hit from the page cache, and 0.45 µs per
absent key.  Memory
per set is 16 KB of directory plus a bloom at ~1.5 bytes a key.

Run stage two after stage one for the same watermark.  Running them
concurrently is safe -- the pack reads pinned files -- but wasteful.

### Compaction Ratio

`Compress` compacts the Dyna layer's **history** -- the segments the
window has rolled past -- and never a record last written inside the
window.  It rewrites a run of history segments into one holding the
newest record per key: the newest segment always, and each older one
while it is no larger than 1/`CompactRatio` of what has gathered
behind it.  At the default 0.25 a large old segment is rewritten only
once a quarter of its size has arrived behind it, which keeps the
bytes rewritten over the store's life a constant multiple of the bytes
written rather than a whole-layer copy per call.  Raise the ratio to
rewrite large segments less often and hold more garbage; lower it for
the reverse.

Choosing the run costs no I/O -- it is decided from the record counts
the segments hold in memory -- so `Compress` on a cadence is cheap when
there is nothing to do.

### Maintenance Pauses

Every history operation -- merge, compaction, pack -- copies immutable
segments with no lock, writes its output aside, and takes the history
lock only to swap the segment list and commit `history.json`.  The
protocol path (`Put`, `Seal`, `Get` of recent data) takes the store's
own lock and never the history lock, so it waits for nothing the copy
does.  Measured with a writer committing every 50 ms and a reader
every 2 ms while `Compress` and `MergeBelow` ran (N=20; details in
[Segments as storage](design/segment-store.md)):

| dyna layer | `Compress` | max commit pause before → after | max read pause before → after |
|---|---|---|---|
| 0.60 GB | 0.77 s → 0.71 s | 1.06 s → <1 ms | 1.10 s → 20 ms |
| 1.20 GB | 1.83 s → 1.71 s | 2.13 s → <1 ms | 2.17 s → 24 ms |
| 2.40 GB | 4.60 s → 4.81 s | 4.88 s → <1 ms | 4.92 s → 48 ms |
| 4.81 GB | 11.75 s → 11.09 s | 12.09 s → <1 ms | 12.09 s → 55 ms |

Before, the pause was the copy: ~2.4 s per GB of the dynamic layer,
growing without bound.  After, the compaction takes the same time (it is the same copy, off the lock) and the longest a `Put` waited was under a millisecond in every row; the longest a `Seal` waited was 22, 22, 50 and 56 ms against an idle 15-23 ms, and the longest a `Get` 20, 24, 48 and 55 ms -- the history swap and the disk contention of a multi-GB copy, not the copy itself. Memory is lower too: heap 7/10/16/28 MB before against 6/7/10/14 MB after, and the Dyna layer's resident filters 2.4/4.7/9.4/18.7 MB before against 0.3/0.6/1.1/2.1 MB after, because the filter covers the window rather than the whole layer.

### Buffer Size

The `BufferSize` constant (default: 32KB) is the buffer `BFile` uses
for I/O.  Increasing it can help write-heavy workloads at the cost of
memory.

### Block Cadence

`SealBlock(height)` is the durability point for permanent data and the
boundary a peer syncs.  Sealing more often costs more of what follows;
sealing less often makes blocks coarser for peers.

What a seal costs is **fsync**, not computation.  Measured on a
consumer NVMe, a seal of a one-record tail takes ~24.6 ms, essentially
all of it device wait: four barriers at roughly 5.5 ms each.  Those
four are the data file's contents, the index's contents, the
manifest's contents, and one fsync of the directory that covers all
three renames.  It was six until the three separate directory fsyncs
were collapsed into one (issue #33).

Two consequences worth knowing:

- **Seal cost is flat in database size.** It did not change measurably
  between 250 and 2,000 sealed segments; the manifest rewrite that
  grows with the segment count added about 2 ms across that range,
  against ~24 ms of barriers.
- **A block boundary seals every shard**, but a shard with no writes
  now costs nothing.  It used to commit a manifest purely to record
  the block it had moved on to -- two barriers, ~11 ms, for a number
  identical across all 512 shards -- which made an otherwise idle
  block cost seconds.  `KVShard` records that number once for the set
  instead.  Measured over 512 shards with one shard writing:
  **5,789 ms per block to 34 ms** (issue #32).

## Performance Optimization Strategies

### 1. Put immutable data in Perm

The Perm layer assumes values never change, which is what lets it
append without a read-modify-write.  Content-addressed data -- keyed
by its own hash -- belongs there.  State that changes belongs in Dyna.

### 2. Compact Dyna, not Perm

`Compress()` rewrites a run of the Dyna layer's history segments into
one and commits it with a single history-manifest rename, reclaiming
the space that overwritten values still occupy.  It never touches the
segments inside the window, takes no lock a commit takes, and is
cheap to call when there is nothing to do, so run it on a cadence.
It is a no-op on the Perm layer, which has nothing to reclaim.

```go
// Example: compact once per N writes rather than per block
if writes%compactEvery == 0 {
    kvs.Compress()
}
```

### 3. Write from multiple goroutines

Shards are independently locked, so concurrent writers scale until
they saturate the disk.  `ShardWriter` does this for you; see
[Multi-core ingest](design/multicore-ingest.md).

### 4. Batch before closing

`Close` is the durability point.  Put everything, then close once --
an intermediate close forces a flush and sync that the next write
would otherwise have amortized.

## Performance Benchmarks

The test suite includes benchmarks and measurement tests you can run
on your own hardware:

```bash
# Benchmarks
go test -bench=. ./database/

# Measurement tests (skipped with -short)
go test -run 'TestSyncCost|TestDynaCost|TestMultiCoreScaling' -v ./database/

# The multi-GB load tests are opt-in
go test -load ./database/
```

## Common Performance Issues and Solutions

### Issue: Slow key lookups

**Possible causes:**
- Many small segments, because `sealLimit` is too low
- A compaction that has not run, leaving stale generations to search
- An open-file limit far below the working set, so every lookup reopens

**Solutions:**
- Raise `sealLimit` so seals produce fewer, larger segments
- Run `Compress()` on the Dyna layer
- Raise `SetOpenFileLimit` if the process can afford the descriptors

### Issue: Slow open

**Possible causes:**
- A large unsealed live tail, which is replayed record by record on
  open
- A reopen after a crash, which rebuilds the key filters from the
  index of every segment inside the filter window

**Solutions:**
- Lower `sealLimit`, or seal on a block boundary, so the tail stays
  bounded
- Lower the filter window (`SetFilterBlocks`) if crash reopens are
  slow; a clean close saves the filters and loads them in milliseconds

### Issue: High memory usage

**Possible causes:**
- A large live tail: every unsealed record is tracked in a map
- Bloom filters, which are sized from each segment's key count, plus
  the two rolling key filters sized for the keys of 2N blocks, plus one
  filter per packed set at ~1.5 bytes a key

**Solutions:**
- Lower `sealLimit`
- Lower the filter window (`SetFilterBlocks`)
- Reduce `BufferSize` if memory is severely constrained

### Issue: Slow write performance

**Possible causes:**
- Sealing too often
- A single writer goroutine leaving shards idle

**Solutions:**
- Raise `sealLimit` or lengthen the block cadence
- Write concurrently; see [Multi-core ingest](design/multicore-ingest.md)
