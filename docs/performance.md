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

`SetFilterBlocks(n)` on a `KVShard` or `KV2` sets N, the roll period of
the Perm layer's key filter (default `DefaultFilterBlocks`, 1,000;
minimum `MinFilterBlocks`, 20).  A fresh filter starts every N blocks
and covers 2N, so two are live at once and the store holds the keys of
the last N to 2N blocks in memory -- and no more, whatever the chain's
length.  It is persisted in each shard's manifest; set it when the
database is created, since changing it commits a manifest per shard.

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

1. `KVShard.MergeFinalized(height)` merges each shard's sealed Perm
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
   Measured on the same set: ~30 ms, one file of 7.6 MB for 40,000
   keys.

   The drop writes no manifest.  Each shard records it at its next
   seal, and only then deletes the files, so the pack costs two
   barriers in all rather than two per shard.  Until a shard next
   seals -- or closes -- its packed files linger on disk, harmlessly:
   a shard that takes no writes for a long time keeps them that long.

A lookup that reaches a set costs a bloom probe, one read of the
shard's index slice, and one read of the value; a key the shard's own
filter says is absent never reaches the sets.  Measured over the packed
20-block set: 2.5 µs per hit from the page cache, and 0.45 µs per
absent key.  Memory
per set is 16 KB of directory plus a bloom at ~1.5 bytes a key.

Run stage two after stage one for the same watermark, never
concurrently with it: the merge retires the files the pack is copying.

### Compaction Ratio

`CompactRatio` (default 0.25) is the share of a mutable layer that must
be superseded records before `Compress` does anything. `Compress` on a
cadence used to rewrite the whole layer every call, so reclaiming a
fixed amount of garbage grew more expensive as the database grew;
waiting for a fixed *fraction* makes the amortised cost per overwrite
constant instead. Raise it to compact less often and hold more
garbage; lower it for the reverse.

The estimate behind it is maintained as writes arrive -- a probe of the
store's own key filter, so deciding costs nothing -- and is carried in
the manifest, so a reopened store resumes it rather than believing it
holds no garbage.

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

`Compress()` writes a new sealed generation of the Dyna layer and
commits it with a single manifest rename, reclaiming the space that
overwritten values still occupy.  It is proportional to live data, so
run it on a cadence tied to write volume rather than on every block.
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
