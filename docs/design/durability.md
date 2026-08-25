# Durability & Crash Recovery

Status: implemented and crash-tested (`database/crash_test.go`),
2026-08-24; retargeted from the v1 kfile layer to `SegmentStore` when
v1 was removed, 2026-08-25. Tracks issue #5.

## The contract

**`KV2.Close()` is a durability point**, as is a `Seal` (and so
`KVShard.SealBlock`). After a process dies (SIGKILL, power loss) at an
arbitrary moment:

1. `OpenKV2` succeeds — the database is never left unopenable.
2. Every key written before the last completed durability point is
   readable with its correct value.
3. Writes after the last durability point may be wholly present,
   partially present, or absent — but they never corrupt the database,
   and **replaying them succeeds** (see below).

`TestCrashRecovery` enforces this: a child process writes to both
layers with checkpoints at each `Close`, the parent SIGKILLs it at
random moments and verifies all checkpointed keys, across rounds that
land kills in the buffered-write, seal, compaction, and manifest-commit
paths.

## How the windows are covered

Every multi-step rewrite follows write-new → fsync → atomic rename →
fsync directory, so the old state survives until the new state is
durable:

| operation | mechanism |
|---|---|
| seal | the segment is written to `seg-<height>.dat.tmp`, fsynced, then renamed into place — a `.dat` on disk is complete by construction |
| manifest commit | `segments.json.tmp` + rename; the manifest is the single point at which a seal or compaction becomes real |
| compaction | writes a whole new sealed generation, then commits it with one manifest rename (issue #19) — there is no window in which keys and values disagree |
| interrupted seal | a data file *above* the manifest's newest height reached disk but not the manifest; `recoverOrphans` adopts it on open |
| superseded files | a data file at or below the newest height was replaced by a committed compaction; `recoverOrphans` deletes it |
| stale tmp files | removed on open; a `.tmp` file is by construction always incomplete |
| torn live-tail record | the live tail is replayed record by record on open, and a record whose value bytes run past end-of-data is dropped |

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

## Known gaps (accepted, documented)

- **Initial creation** (`NewKV2`, `NewSegmentStore`) is not
  crash-atomic — a crash during first-time setup can leave a partial
  directory. Create once, verify, then rely on the contract. Recovery:
  delete and recreate.
- `Flush` on the `ShardWriter` is an application barrier, not an
  fsync; pair it with `Close` (or `SealBlock`) at block boundaries that
  must be durable. Wiring `Flush` to `Seal` so that a block boundary is
  one durability, sync, and compaction boundary is the next step in
  [Segments as storage](segment-store.md).
- A key written to Dyna keeps a stale copy in Perm. `Get` resolves the
  layer order, so the copy is dead weight rather than a wrong answer,
  but compaction does not reclaim it.
