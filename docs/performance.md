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
   so a segment that does not hold the key is rejected from memory.

3. **Sync is a file copy.**  Sealed segments are the storage format
   *and* the transport format, so adopting a peer's segment is a copy
   plus one manifest commit -- not a re-insertion of every record.
   The cost does not grow with what the receiving node already holds.

4. **Shards give you cores.**  Keys route to one of 512 shards by
   `ShardIndex`, and each shard has its own lock, so writes to
   different shards proceed in parallel.

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

### Buffer Size

The `BufferSize` constant (default: 32KB) is the buffer `BFile` uses
for I/O.  Increasing it can help write-heavy workloads at the cost of
memory.

### Block Cadence

`SealBlock(height)` is the durability point for permanent data and the
boundary a peer syncs.  Sealing is where the Perm layer's time goes --
about 60% of it at a 25,000-key block, most of that the SHA-256
read-back of the segment just written.  Sealing more often costs more
of that; sealing less often makes blocks coarser for peers.

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

**Solutions:**
- Raise `sealLimit` so seals produce fewer, larger segments
- Run `Compress()` on the Dyna layer

### Issue: Slow open

**Possible cause:** a large unsealed live tail, which is replayed
record by record on open.

**Solution:** lower `sealLimit`, or seal on a block boundary, so the
tail stays bounded.

### Issue: High memory usage

**Possible causes:**
- A large live tail: every unsealed record is tracked in a map
- Bloom filters, which are sized from each segment's key count

**Solutions:**
- Lower `sealLimit`
- Reduce `BufferSize` if memory is severely constrained

### Issue: Slow write performance

**Possible causes:**
- Sealing too often
- A single writer goroutine leaving shards idle

**Solutions:**
- Raise `sealLimit` or lengthen the block cadence
- Write concurrently; see [Multi-core ingest](design/multicore-ingest.md)
