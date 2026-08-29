# Durability & Crash Recovery

Status: implemented and crash-tested (`database/crash_test.go`),
2026-08-24; retargeted from the v1 kfile layer to `SegmentStore` when
v1 was removed, and three defects it then surfaced fixed, 2026-08-25.
Tracks issue #5.

## The contract

**`KV2.Close()` is a durability point**, as is a `Seal` (and so
`KVShard.SealBlock` and `ExportBlock`) — **for both layers**.  Close
closes both even when the first fails, and `KVShard.Close` closes every
shard even after one fails, reporting the first error: stopping early
abandoned the buffered live tail of everything after the failure, which
is the same torn commit across layers that `Seal` was fixed for
(issue #38). After a
process dies (SIGKILL, power loss) at an arbitrary moment:

1. `OpenKV2` succeeds — the database is never left unopenable.
2. Every key written before the last completed durability point is
   readable with its correct value.
3. Writes after the last durability point may be wholly present,
   partially present, or absent — but they never corrupt the database,
   and **replaying them succeeds** (see below).

### A block boundary covers both layers

`Seal` closes a block on a *running* store, and until issue #29 it
only half did.  Sealing is the Perm layer's durability point, so
permanent records were on disk the moment it returned.  The Dyna layer
was not sealed at a block boundary — its segments are local, never a
peer's unit of sync — so its newest writes sat in `BFile`'s 32 KB
buffer until the tail filled, a compaction ran, or the store was
closed.

That is not a slow write, it is a *torn commit*.  In Accumulate every
chain element is permanent and every mutable record — account state,
chain heads, BPT nodes — is dynamic, so a node killed after a commit
came back holding chain elements newer than the heads that index them:
the two layers disagreed about where the block ended.

`KV2.Seal` now seals Perm and **syncs** Dyna: flush the live tail,
fsync it, done.  Syncing rather than sealing is deliberate — cutting a
Dyna segment per block per shard would buy nothing (no peer receives
one) and cost a seal's hash and manifest commit every block.  No
manifest is written either, because the live tail is recovered by
replaying the file, and a tail fsynced mid-record is fine: open
truncates the torn record and resumes on the boundary before it.  A
sync of a tail that took no writes is a no-op, so the shards a block
did not touch cost nothing.

`SegmentStore.Sync()` is exported, so a caller wanting a durability
point without a block boundary has one.

## The tests

`TestCrashRecovery` enforces this: a child process writes to both
layers with checkpoints at each `Close`, the parent SIGKILLs it at
random moments and verifies all checkpointed keys, across rounds that
land kills in the buffered-write, seal, compaction, and manifest-commit
paths.  The child compacts on its own cadence (`crashCompressEvery`)
because nothing in `KV2` compacts by itself -- `sealPermIfFull` and
`sealDynaIfFull` seal and stop there -- so without it the compaction
path would never be under the kill.  Adding it is what surfaced the
three defects listed under *Fixed here*.

`TestCrashRecoverySeal` runs the same rounds against the other
durability point: the child checkpoints at `Seal` and never closes.
It is the only one of the two that can see issue #29, because `Close`
flushes and fsyncs both layers and so hides a layer that a block
boundary was leaving buffered.  Against the unfixed code it loses the
first dynamic key in the first round.

The child seals the block the store is *accumulating*
(`PermKV.BlockHeight()`) rather than a counted one: a kill between a
seal and the `CHECKPOINT` line that reports it rolls the parent's
restart point back to before that seal, and re-sealing a block the
store has already closed is an error.

## How the windows are covered

Every multi-step rewrite follows write-new → fsync → atomic rename, and
the operation ends with **one** fsync of the directory, so the old
state survives until the new state is durable:

| operation | mechanism |
|---|---|
| seal | the usual path renames `live.dat` itself: the header is filled in, the file fsynced, then renamed to `seg-<height>.dat`, so no record is copied and a `.dat` on disk is complete by construction. A tail holding overwrites is rewritten to `seg-<height>.dat.tmp` first, then fsynced and renamed |
| manifest commit | `segments.json.tmp` + rename; the manifest is the single point at which a seal or compaction becomes real |
| compaction | writes a whole new sealed generation, then commits it with one manifest rename (issue #19) — there is no window in which keys and values disagree |
| interrupted seal | a data file *above* the manifest's newest height reached disk but not the manifest; `recoverOrphans` adopts it on open |
| superseded files | a data file at or below the newest height was replaced by a committed compaction; `recoverOrphans` deletes it |
| stale tmp files | removed on open; a `.tmp` file is by construction always incomplete |
| torn live-tail record | the live tail is replayed record by record on open; a record whose value bytes run past end-of-data is dropped **and the file is truncated to the last complete record**, so the next append lands on a record boundary |
| live file with no header | `seal` creates the new `live.dat` and leaves its 24-byte header in the `BFile` buffer, so a crash before the first flush leaves the file at 0 bytes; open rewrites the header rather than trusting it to be there |
| the block a shard is in | recorded once for the whole shard set (`block.json`, one fsync), not once per shard.  A shard with no writes advances in memory and commits nothing.  On open the set tells every shard the block it is in, which is what a shard needs it for: `SealNext` tags an auto-sealed segment with its block height, so a shard that came back believing it was in an older block would label segments with a block they do not belong to and `ExportBlock`, which selects by block, would never export them.  A missing file reads as 0 and constrains nothing, so an existing database opens unchanged (issue #32) |
| one barrier per operation | a seal renames up to three files into the same directory -- the sealed data file, its index, then the manifest -- and fsyncs that directory once, at the manifest commit. One directory fsync commits every name change made in it, and the renames are issued in order, so the manifest can never become durable ahead of the data file it names. Each file's own fsync is still taken before its rename: that is what makes a published name always point at durable contents. Six barriers a seal became four, measured 36 ms to 24.6 ms (issue #33) |
| operations on a closed store | `Close` drops the sealed segment list but keeps the live map, so reads and writes refuse with `errStoreClosed` rather than running against half a store |

## Recovery by replay

A blockchain node recovers by re-applying writes since its last
committed block. Two mechanics make that safe:

- **Immutability compares values, not offsets.** A replayed `PutPerm`
  of an identical value is a no-op even though the value bytes would
  land at a new offset. Only a *different* value for an existing key
  is an error.
- **The newest record wins.** A crash can persist a key record whose
  value bytes were still buffered; that record is dropped on open as a
  torn tail. A replayed write appends a fresh record, and in a mutable
  store the last record for a key wins — the same rule lookups use.

## Fixed here

Porting `TestCrashRecovery` to `KV2` and putting compaction under the
kill surfaced three silent-data-loss defects in `SegmentStore`, each
of which lost keys that a completed `Close` had made durable:

1. **Headerless live tail.** `seal` creates the new `live.dat` with
   `os.Create` and only *buffers* its 24-byte header.  A crash in that
   window left the file at 0 bytes; open trusted the header was there
   and started replay at offset 24, so the next `Put` wrote at offset
   0 and shifted the whole tail under the header.  Every later open
   then read a key fragment as a record header, called it torn, and
   dropped the tail.
2. **No closed-store guard.** No write path checked `closed`.  Because
   `Close` nils the segment list but leaves the live map, a `Seal` or
   `Compress` after `Close` committed a manifest built from no
   segments -- and the next open deleted every segment it no longer
   named.  Measured at 500 of 550 keys lost, with a nil error.
3. **Torn tail not truncated.** Replay stopped at a torn record but
   left its bytes on disk, so the next append landed after them and
   the following open mis-parsed everything written since.

4. **An empty key filter accepted as covering segment (0,0).** Not a
   durability defect — the data was durable and intact — but reached
   only through one. `loadKeyFilter` judged a saved filter still valid
   by comparing the newest segment's `(Height, Seq)` to what the
   manifest recorded, and a filter saved with *no* segments records the
   zero value, `(0,0)`. That is also the identity of the first segment
   a store seals, and the Dyna layer numbers every segment at height 0.
   So when a crash left a sealed segment for `recoverOrphans` to adopt
   without a manifest rewrite, the empty filter matched it, was loaded,
   and answered "definitely absent" for all 40 of its keys: `Get`
   returned not-found for records sitting on disk. The manifest now
   records how many segments the filter covered, which is what "covers
   nothing" needs to say out loud (issue #35).

   `TestCrashRecoverySeal` found it, intermittently — twice in 55 runs.
   `TestKeyFilterEmptyDoesNotCoverSegmentZero` builds the same state
   deterministically.

The first three have a regression test in `segstore_test.go`
(`TestSegmentStoreLiveTailAfterInterruptedSeal`,
`TestSegmentStoreClosedRejectsOperations`,
`TestSegmentStoreTornTailIsTruncated`) and the fourth one in
`keyfilter_test.go`; all four fail against the unfixed code.

## Known gaps (accepted, documented)

- **Initial creation** (`NewKV2`, `NewSegmentStore`) is not
  crash-atomic — a crash during first-time setup can leave a partial
  directory. Create once, verify, then rely on the contract. Recovery:
  delete and recreate.
- `Flush` on the `ShardWriter` is an application barrier, not an
  fsync: it drains the queues so the writes have *reached* the store,
  and says nothing about the disk. Follow it with `SealBlock` (or
  `Close`) at a block boundary that must be durable — `SealBlock` is
  now the whole durability point for both layers, so that pair is the
  block boundary.
- A key written to Dyna keeps a stale copy in Perm. `Get` resolves the
  layer order, so the copy is dead weight rather than a wrong answer,
  but compaction does not reclaim it.
