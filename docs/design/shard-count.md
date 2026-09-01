# How many shards?

A sharded database routes keys by hash to independent stores, so the
count decides how finely the work is cut. This is the measurement of
what that costs and what it buys, and the answer it gives is that the
count must be set from the **rate a database will see** — it is not a
number to pick once and inherit.

## What was run

`test/kvbench` against the Accumulate adapter: 500 tx/s, 13 records a
transaction (~6,500 records a block), a commit every second, 6 minutes
a point, on tmpfs. Every point is the **same build** — BlockchainDB at
`09aba59`, the adapter at `feat/sharded-store` with `SealLimit`
100,000, `MergeLag` 20, `CompressEvery` 20, `PackEvery` 1,000 — with
only the shard count varying. The unsharded point is the same harness
against a single `KV2`.

tmpfs makes `fsync` cheap, so these numbers **understate** the cost of
sharding on a real disk.

## The curve

| shards | commit p50 | commit p99 | files on disk | RSS |
|---|---|---|---|---|
| 1 (unsharded) | 7.6 ms | 23–30 ms | 267 | 336 MB |
| 2 | 6.8 ms | 20.2 ms | 126 | 350 MB |
| 4 | 8.1 ms | 22.1 ms | 250 | 362 MB |
| 8 | 7.4 ms | **13.2 ms** | 480 | 372 MB |
| 16 | 10.6 ms | 19.7 ms | 926 | 433 MB |
| 32 | 12.1 ms | 25.7 ms | 1,844 | 529 MB |
| 64 | 16.6 ms | 26.1 ms | 3,684 | 537 MB |
| 128 | 23.2 ms | 37.0 ms | 7,374 | 621 MB |
| 512 | 57.8 ms | 80.1 ms | 29,604 | 1,335 MB |

Files and RSS are read at the same block (360) in every sharded point;
the unsharded row is at block 601, and its file count was flat well
before that. No point overran its block budget, and every point held
500 tx/s.

Read it in two halves.

**From 1 to 8 shards the median is flat within noise** — 6.8, 7.4,
7.6, 8.1 ms are one number at this sample size, and nothing here
justifies preferring 2 over 1 or 1 over 8 on the median alone. The
tails differ more than the medians: 8 shards has the tightest p99 in
the whole set (13.2 ms), which is what a little parallelism buys — a
writer waits behind fewer others — while its extra files are still
few enough not to cost anything.

**Above ~16 shards every axis rises monotonically.** Latency roughly
doubles per 4x shards, and memory climbs because each shard carries
its own live tail, its own filter pair and its own segment metadata.

Files are the plainest of the three: the count per shard is flat
across the whole sweep — 63.0 at 2 shards, 62.5 at 4, 60.0 at 8, 57.9
at 16, and 57.6 to 57.8 from 32 all the way to 512. Every shard pays
the same ~58 files whatever the rate it sees, so the total is simply
the count times that, and there is no economy of scale to be had
anywhere on the curve.

So the cost is not sharding; it is sharding **past the point where a
shard's files are worth writing**.

## Why sharding costs, at this rate

A shard's files have to be worth writing.

Keys route by hash, so a shard sees its share of the rate. At 512
shards and 500 tx/s that is ~13 records a block, so a merge of 20
blocks — the whole point of merging — produces a file of ~250 records,
a few tens of kilobytes. That file still costs an inode, an index file
of its own, a bloom filter, a header, and its share of the fsyncs. The
per-file overhead is most of the file.

The same arithmetic in reverse says what the count should be. For a
20-block merge to carry ~5,000 records at 500 tx/s:

```
6,500 records/block × 20 blocks ÷ 5,000 records/file ≈ 26 shards
```

512 shards is sized for something nearer 8,000 tx/s. Run at 500, it
cuts the work 20× finer than the files can carry — and the measured
curve agrees, with the cost becoming visible between 8 and 32 shards.

At 500 tx/s the measurement says **4 to 8 shards**: the median is
indistinguishable from one store, the p99 is the best in the set, and
the file count is still in the hundreds.

## What sharding buys, and when

Parallel ingest — independent locks, live tails and seals, so writers
do not queue behind one another. That is worth paying for **when a
single store's lock is the bottleneck**, and at 500 tx/s it is not:
the unsharded store commits in 7.6 ms and never overran a 1 s block.
Until the single-store latency is a real fraction of the block budget,
sharding is cost without benefit.

Where the curve inverts is a separate measurement, at a rate high
enough for the single store to bind. It has not been run.

## Two configuration traps found by the measurement

**The merge cadence must match N, not the old single-store default.**
Maintenance ran every `CompressEvery` = 128 commits, inherited from the
unsharded adapter where that meant 128 files. At 512 shards it means
128 per-block segments piling up in *every* shard between sweeps —
~131,000 files. Setting `CompressEvery` to 20 (matching N) took the
count from ~92,000 and climbing to ~30,000 and flat, and stopped the
latency drifting upward (56 → 92 ms became a flat 53–61 ms).

**`SealLimit` is per shard.** Carrying the single-store figure of
100,000 across to 512 shards means 51.2M records of unsealed tail for
the partition. It was not the dominant cost here — a control run at
100,000 matched the 512-record run within noise, because a block
boundary seals every shard regardless — but it is the wrong number by
construction.

## What this changed

`NumShards` was a compile-time constant. It is now
`DefaultNumShards`, with `NewKVShardN` taking the count, because the
right value is a property of a deployment's rate rather than of the
build.

The count is fixed for the life of a database: keys route by hash
modulo it, so reopening with a different one sends every key to a
different shard and reports data that is on disk as absent. The shard
directories are the record of it — `OpenKVShard` counts them, and
refuses a run with a gap, since routing over what is left would be the
same silent failure. A block set carries the count it was packed for
in its header, and a database refuses a set packed for another.

## Reproducing

```
# BlockchainDB at the commit under test, adapter configured as above
sed -i "s/^const NumShards = .*/const NumShards = $n/" database/kv_shard.go
go build -o /tmp/kvb ./test/kvbench/          # from the Accumulate tree
/tmp/kvb -backend bcdb -dir /dev/shm/sw -duration 6m -tps 500 -out /tmp/n$n.json
```

Every point in a comparison must come from the same build; a curve
assembled from two of them cannot be read.
