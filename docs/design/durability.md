# Durability & Crash Recovery

Status: implemented and crash-tested (`database/crash_test.go`), 2026-08-24.
Tracks issue #5.

## The contract

**`KV.Close()` and `KFile.PushHistory()` are durability points.** After
a process dies (SIGKILL, power loss) at an arbitrary moment:

1. `OpenKV` succeeds — the database is never left unopenable.
2. Every key written before the last completed durability point is
   readable with its correct value.
3. Writes after the last durability point may be wholly present,
   partially present, or absent — but they never corrupt the database,
   and **replaying them succeeds** (see below).

`TestCrashRecovery` enforces this: a child process writes with
checkpoints at each `Close`, the parent SIGKILLs it at random moments
and verifies all checkpointed keys, across rounds that land kills in
the buffered-write, kfile-rewrite, history-push, and bloom-save paths.

## How the windows are covered

Every multi-step rewrite follows write-new → fsync → atomic rename →
fsync directory, so the old state survives until the new state is
durable:

| operation | mechanism |
|---|---|
| kfile rewrite (`KFile.Close`) | `kfile_tmp.dat` + rename |
| kfile reset (`PushHistory`) | header-only tmp + rename — kfile.dat always exists |
| history push ordering | keys land (and fsync) in `history.dat` **before** the kfile resets; a crash between leaves benign duplicates that the next push's merge dedups |
| Bloom filter (`bloom.dat`) | written after history accepts the keys, before the kfile reset — a stale filter can only be missing keys that are still in the kfile, which the open path scans |
| values before keys | `KV.Close` syncs `values.dat` before the kfile, so durable keys never reference lost values |
| stale tmp files | removed on open; a tmp file is by construction always incomplete |

## Recovery by replay

A blockchain node recovers by re-applying writes since its last
committed block. Two mechanics make that safe:

- **Immutability compares values, not offsets.** A replayed `KV.Put`
  of an identical value is a no-op even though the value bytes would
  land at a new offset. (Previously the check compared `DBBKey`
  offsets, so every replay of a durable key errored.)
- **The newest record wins.** A crash can persist a key record whose
  value bytes were still buffered. The replayed write appends a fresh
  record, and `kGet` resolves a key to its newest record, superseding
  the dangling one. The next kfile rewrite discards old records
  entirely.

## Known gaps (accepted, documented)

- **`KV.Compress` is not crash-atomic**: the values-file swap and the
  kfile offset rewrite are two steps; a crash between them leaves keys
  pointing into the wrong values layout. **No longer on the database's
  path** — the Dyna layer is a mutable `SegmentStore` and compacts by
  writing a new sealed generation and committing it with one manifest
  rename (issue #19). `KV.Compress` remains only for the legacy `KV`
  and as the baseline the segment benchmarks measure against; the gap
  is theirs, not the database's.
- **Initial creation** (`NewKV`) is not crash-atomic — a crash during
  first-time setup can leave a partial directory. Create once, verify,
  then rely on the contract. Recovery: delete and recreate.
- `Flush` on the `ShardWriter` is an application barrier, not an
  fsync; pair it with `Close` (or a future `SyncFlush`) at block
  boundaries that must be durable.
