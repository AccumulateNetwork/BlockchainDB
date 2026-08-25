# Multi-Core Ingest Design

Status: implemented (`database/kv_shard_writer.go`), measured 2026-08-24.

## Goal

Exploit the 512-way shard structure for parallel writes. Before the
concurrency work (#8, PR #15) the database was single-goroutine only;
after it, each shard (a `KV2`) serializes behind its own mutex, so
operations on *different* shards can run on different cores. This
design adds the ingest layer that turns that property into throughput.

## Two ways to go parallel

### 1. Synchronous concurrent callers

Callers that are already parallel (e.g. serving independent requests)
simply call `KVShard.Put*` from multiple goroutines. Keys route to
shards by `ShardIndex` (key bytes 4–8), so 512 shards give a low
collision rate for any reasonable worker count, and collisions only
serialize — they never break.

### 2. `ShardWriter`: async ingest for a single producer

A blockchain node usually *produces* writes single-threaded — a
transaction executor emits state changes in order. `ShardWriter` lets
that single producer feed parallel appliers:

    writer := kvs.NewShardWriter(workers, queueDepth)
    writer.PutPerm(key, value)   // queued, applied asynchronously
    writer.PutDyna(key, value)
    ...
    writer.Flush()               // barrier: all queued writes applied
    writer.Close()               // drain and stop

**Routing = ordering.** A key's worker is derived from its shard index
(`ShardIndex(key) % workers`), so:

- Writes to the *same key* always apply in the order queued
  (same key → same shard → same worker → FIFO queue). This is what
  makes Dyna overwrites safe.
- A shard is only ever touched by one worker, so workers never contend
  on a shard lock with each other.

**Consistency points.** Ordering across *different* keys is not
guaranteed between flushes. `Flush()` is the barrier and maps naturally
to a block boundary: apply a block's writes, flush, then the DB state
is that block's state. Reads do not see queued writes until they are
applied — flush first when read-your-writes matters.

**Errors.** Workers record the first error; `Put*`/`Flush`/`Close`
return it. After `Close`, calls return `ErrWriterClosed`.

## Measured scaling

`TestMultiCoreScaling`, 24-core (8P+16E) Intel Ultra 9 275HX, NVMe,
values 100–500 B, 512 shards, warmed Bloom filters, buffered writes
(no per-op fsync):

| configuration                | puts/sec   | vs single |
|------------------------------|------------|-----------|
| sync, 1 goroutine            |  1,299,000 |      1.0× |
| sync, 4 goroutines           |  4,474,000 |      3.4× |
| sync, 8 goroutines           |  7,521,000 |      5.8× |
| sync, 16 goroutines          | 11,328,000 |      8.7× |
| async, 1 producer, 8 workers |  2,624,000 |      2.0× |

(Perm writes, measured after the Perm layer moved to sealed segments;
the same run against the kfile+history layer peaked at 3.67M/s.)

Notes:

- Sync scaling is roughly linear per core at 8–16 workers. The low
  4-worker result is repeatable and consistent with hybrid P/E-core
  scheduling; treat low-worker numbers as noisy.
- The async path is **producer-bound** at ~2.6M puts/s: a single
  goroutine generating and queuing writes is the ceiling, independent
  of worker count. Now that the apply side is much faster, this is the
  binding constraint for single-producer ingest; batching the queue
  sends is the fix if it ever matters.
- Cold-start caveat (resolved by #12): fixed 10 MB Bloom filters made
  first-touch page faults dominate a cold `KVShard`. Filters are now
  layered and sized from the key count (~300 KB initial per KFile),
  and persisted at push time so opens load them instead of scanning
  history.

## Future work

- Crash-consistent flush: `Flush` is an application barrier, not a
  durability barrier — it does not fsync. Couple it with the recovery
  design (#5) so a block boundary can be made durable atomically.
- Block segmentation (sync thesis): sealed per-block Perm segments
  would let `Flush` also seal a segment, aligning ingest, durability,
  and node-sync units.
