# BlockchainDB Specification

This document is the authority on what BlockchainDB is required to do.
Section 1 (Architecture) states the design as invariants — things that
must be true, each with the reason it must be true.  Section 2
(Implementation) describes how the current code satisfies each
invariant, and names the places where it does not yet.

A change that violates Section 1 is a bug even if every test passes.
When code and this document disagree, one of them is wrong on purpose:
either fix the code or change the spec in the same review, never
neither.  History has shown why: `MergeBelow` shipped merging *all*
history below a watermark while the design said "merge a finished
block set", and the only written record of intent was a code comment
rationalizing the deviation (#63).  This document exists so that
cannot happen silently again.

---

## 1. Architecture

### 1.1 Purpose and data model

BlockchainDB stores two kinds of data with different immutability:

- **Permanent** data is content-addressed: the key is the hash of the
  value, the value is written once and never changes.  Transactions,
  chain entries, anything a blockchain appends.  This is most of the
  data by volume.
- **Dynamic** data is mutable state keyed arbitrarily: account state,
  and above all the BPT.  This is a small, hot, heavily overwritten
  set.  **The dynamic layer is expected to stay small**; designs may
  rely on that.

A database is two layers, **Perm** and **Dyna**, one per kind.  A
generic read resolves Dyna first: a permanent key overwritten with a
different value has *become* dynamic, and the dynamic copy is the
truth (the stale Perm copy is dead weight, never a wrong answer).

### 1.2 The latency rule (governing invariant)

> **No protocol-path operation — Put, Get, Seal, commit — may have a
> cost that grows with the age or size of the store.  Forever.**

Every other rule in this section serves this one.  Corollaries:

- Maintenance (merging, compaction, packing) runs off the protocol
  path.  The protocol path never waits for maintenance except at a
  bounded swap (a list splice and one manifest commit).
- Maintenance itself must be **bounded per pass**: a single pass may
  not grow with the age of the store either, or it starves the disk
  and the tail latencies show it (#59, #63).
- Memory that must be resident may scale with the *working set*, never
  with the total history (#64).

### 1.3 Blocks, the window, and windowed immutability

Time is measured in blocks.  **N = FilterBlocks** is the store's one
tuning constant for time; it must be at least 20, because healing
executes across multiple blocks and must find what it needs inside the
window (MinFilterBlocks).

- The **window** is the most recent N to 2N blocks: the working set.
  A running node does not reach farther back than that in normal
  operation.
- **Key filters** cover exactly the window: each filter spans 2N
  blocks, a new one starts every N, so two are live at once and every
  write enters both.  A filter whose span completes is dropped — the
  blocks it covered now carry filters of their own on disk (1.4).
- **Immutability is a windowed guarantee.**  The overwrite check on a
  permanent write consults the window and nothing older.  The check
  exists for replay safety, which is recent by nature; a Perm key is
  the hash of its value, so "same key, different value" beyond the
  window would be a hash collision.  This is what makes a permanent
  write O(1) forever (#44).

### 1.4 The permanent layer

- **An entry is `file : offset : length` and never moves.**  Written
  once, copied only byte-verbatim.  Indexes carry body-relative
  offsets so an index survives its body being placed in a different
  file behind a different base; nothing in an index or a body is ever
  rewritten (#37, #47).
- **A finished window of perm data merges exactly once**, into one
  merged block (per shard).  Merged blocks are permanent: they are
  never re-merged, never rewritten, never moved.  Merge cost is
  therefore O(window) per window, and lifetime merge cost is linear in
  the data — never quadratic (#63).
- **Deep reads walk merged blocks newest-first**, one filter probe per
  block, binary-searching only a block whose filter claims the key.
  Old blocks are read rarely (1.1); their filters may live on disk and
  be probed there — resident filter memory follows the working set,
  not the chain (#64).
- **Cross-shard consolidation is a rarer, second pass**: per-shard
  merges fold into one set file per period (targeting daily), with one
  filter over the set.  Searching years back skips whole days by their
  filters; that deep search is API-level work, not protocol work.
- Perm accumulates **no garbage** — values are immutable — so the perm
  layer is never compacted, only merged and packed.

### 1.5 The dynamic layer

- Overwrites create garbage only here, and only compaction reclaims
  it.  Compaction touches **history only** — never the window, which
  is the protocol's — and holds no protocol lock (#57).
- **Newest wins**, at every level: the live tail over sealed segments,
  newer segments over older, and within a merge the newest input copy
  of a key is the one that survives.
- The dynamic layer's total size must converge to O(live keys): a
  bounded key set rewritten forever may not grow the disk forever
  (#65 is the open violation).

### 1.6 Tiers and locks

- Each store is two tiers: **Active** (live tail + the window's sealed
  segments; the protocol's tier) and **History** (everything older;
  maintenance's tier).  Tier membership is *derived* from a segment's
  block range and the window — never recorded — so a crash cannot
  strand a segment in the wrong tier.
- Each tier has its own lock, plus one maintenance lock serializing
  maintenance operations.  Rules:
  - The protocol path never holds its lock across a history walk or
    while maintenance copies (a put that must consult history drops
    the store lock, walks under the history lock, retakes, re-checks).
  - Maintenance takes History exclusively only to *swap* — splice a
    list and commit a manifest — never for the length of a copy.
- Segments cross tiers by **handoff** at the window boundary; a
  handoff is bookkeeping, it moves no bytes.

### 1.7 Identity

- A segment's identity is **(block height, sequence within block)**;
  its file name is a pure function of its identity.
- **An identity is minted exactly once.**  Minting is serialized per
  store; no two paths (a seal, a merge, a compaction) may ever produce
  two segments with one identity.
- **No publish may ever replace an existing file.**  Claiming a name
  is exclusive-or-fail; a taken name is a detected bug, never a silent
  overwrite.  This is the load-bearing backstop for the previous rule
  (#61: both rules were violated and the store lost a committed
  segment).

### 1.8 Durability and crash consistency

- **The manifest commit is the only commit point.**  A file is made
  durable (content fsync) *before* it is named; the manifest's own
  commit (tmp + fsync + rename + directory fsync) is the single
  barrier that makes the whole operation durable.  A crash at any
  instant falls back to the last committed manifest and loses nothing
  it named (#29, #33).
- **Nothing a durable manifest names may be deleted.**  Deletion is
  deferred until no on-disk manifest can name the file; the deleter
  verifies, and refuses loudly rather than trust bookkeeping (#61).
- The live tail is an append log; reopen replays it and truncates a
  torn final record at the last whole-record boundary.
- On-disk data is the transport format or a verbatim copy of it: a
  peer syncs permanent data by copying files, and can verify a
  segment by its recorded hash.

### 1.9 Sharding

- Keys route by hash to **512 shards** for parallel ingest.  **Both
  layers shard** — perm ingest needs the parallelism as much as dyna;
  content-addressed keys give near-perfect balance.  (The retired idea
  of un-sharding perm valued only compaction; see #62.)
- Per-shard work first, cross-shard work later: shards seal, merge and
  compact independently; the pack tier consolidates across shards once
  a window is finished everywhere.
- A block boundary commits **once per shard set**, not once per shard
  (the shared block record); shards that took no writes cost nothing
  at the boundary (#32).

### 1.10 Configuration

- Parameters (SealLimit, FilterBlocks, format version) are persisted
  in the manifests; a reopened store runs with what it was built with.
- **No environment-variable configuration.**  A typo must fail loudly,
  not silently select a default.
- A store refuses to open data written in a different on-disk format
  version; there is no silent migration.

---

## 2. Implementation

How the code satisfies Section 1 today.  File references are to
`database/`.  Deviations are marked **DEVIATION** with their issue.

### 2.1 Structures

| Unit | File | Role |
|---|---|---|
| `SegmentStore` | `segstore.go` | One layer: live tail (`live.dat` + in-memory map), sealed segments in two tiers, two manifests |
| `KV2` | `kv_2.go` | One database: `PermKV` (immutable SegmentStore) + `DynaKV` (mutable) |
| `KVShard` | `kv_shard.go` | 512 × KV2 (`NumShards`), keys routed by hash; plus one `SetStore` |
| `SetStore` / `blockSet` | `blockset.go` | The pack tier: `.bset` files, one per consolidation |
| Rolling filters | `keyfilter.go` | The window's key filters (1.3) |
| Streaming merge | `indexmerge.go` | k-way merge over sorted indexes; the engine under every merge/compact/pack |

Segment files: `seg-<height>-<seq>.dat` (records: 40-byte header —
32-byte key + 8-byte length — then the value) and `.idx` (sorted
48-byte records — key, offset, length — plus a bloom, 12 bits/key
`BloomBitsPerKey`).  Index offsets are body-relative (1.4).

### 2.2 The protocol path (1.2, 1.3)

- `SegmentStore.Put` → `putUnlessPresent` (`segstore.go:1687`).  A
  mutable store appends blind.  An immutable store probes the rolling
  filters: **not in the window → write immediately** (the common case,
  O(1)); a filter hit walks the active tier under the Mutex, and only
  a hit the active tier cannot settle drops the Mutex, walks history
  under its own lock, retakes, and re-checks by epoch (1.6).  Same
  value is an idempotent no-op; a different value is `ErrImmutable`
  (`KV2.Put` routes that key to Dyna instead).
- `Get` (`segstore.go:1537`): live map → filter probe → active
  segments newest-first (resident bloom, then on-disk binary search)
  → history newest-first → packed sets.  The filters settle only the
  window; a miss below it walks blocks by their own filters (1.4).
- `PutIfAbsent` answers absent / present-equal / present-different in
  one pass so `KV2.Put` pays one lookup, not two.
- Costs are counted (`StoreStats`): what the filter settled vs sent
  walking, hits, misleads — the latency rule is measured, not assumed.
- **DEVIATION #66**: `KV2.Put` holds the KV2-wide mutex across its
  cross-layer lookups, re-introducing head-of-line blocking the layer
  below was built to avoid.

### 2.3 Seal (1.7, 1.8)

`seal` (`segstore.go:2112`): mint `(height, seq)` via `nextKeyAt`,
then `promoteLiveFile` — flush, finish the header, fsync, and
**hard-link** the live file to its segment name.  No record is copied
(#60); a shadowed tail keeps its dead bytes and compaction reclaims
them later.  The link is exclusive: a taken name is skipped
(`seal-remint` audit) and the next free sequence is taken — a seal and
a suffix compaction can race to mint the same identity when the
active tier is empty, and the exclusive claim turns that from silent
overwrite into a re-mint (#61).  The index is built from the live map;
the manifest commit that follows is the sole durability barrier
(two fsyncs total; #33 tracks the remainder).

### 2.4 Manifests (1.8)

Two per store (#58): `segments.json` (active tier + unrecorded
handoffs; settings; block height) and `history.json` (everything
older).  The protocol path commits only the active manifest, so its
size is bounded by the window.  `commitJSON` = tmp + fsync + rename +
directory fsync.  `StoreFormatVersion` (4) is checked strictly (1.10).

### 2.5 Filters (1.3)

`keyfilter.go`: a pair of `BloomSet`s spanning 2N each, rolled every N
(`rollKeyFilters` — an allocation at a normal roll, a bounded rebuild
after a block jump).  Sizing feeds from the largest completed span
(`spanKeys`; #54 will bound it to recent demand).  Coverage claims are
verified before a persisted filter is trusted; any doubt rebuilds from
the window, and a store that cannot build filters *walks* — a filter
may cost a pointless walk, never a false "absent" (#35).

### 2.6 Tiers, handoffs, deferred deletion (1.6, 1.8)

Two-tier locking per #57: `Mutex` (active), `History` (RW), `maint`
(serializes maintenance).  `advanceBlock` → `handoffBelowWindow` moves
window-departed segments to history as handoffs; the next history
commit records them, the next active commit stops naming them.
Deletion is deferred through `retireAfterActiveCommit` and an unlink
queue; `retireWhy` **refuses to unlink any file an on-disk manifest
still names** and audits the refusal (`unlink.log`) — invariant 1.8
made executable (#61).  `recoverOrphans` deletes only files named by
*no* manifest, and only at or below the newest named identity; above
it, an orphan is an interrupted seal and is adopted (#45, #52 tracks a
duplicate-adoption gap).

### 2.7 Maintenance (1.2, 1.4, 1.5, 1.7)

All maintenance copies run with no store lock, publish aside, and
swap under History exclusively (`swapHistory`), which re-checks the
run is still in place and discards its output if not.

- **Dyna compaction** — `CompactHistory` (`segstore.go`): fold the run
  `compactionRun` chooses into one segment holding the newest record
  per key.  Streaming (`mergeIndexes` + `indexWriter`, #59): memory is
  one 48 KB cursor per input, never the run's keys (453 MB → 47 MB per
  million records).  Amortization by `CompactRatio` (0.25, #31); pass
  bound by `CompactPassRecords` (4 Mi records, #59) with an
  adjacent-pair fallback so consolidation advances one bounded step
  rather than stalling.  Identity: the output takes `(last.Height,
  last.Seq+1)`; the chooser refuses a pair whose successor already
  holds that identity, and `claimSegmentName` (hard link) refuses to
  replace any existing file — a taken name skips the pass, audibly
  (#61).  **DEVIATION #65**: a segment at the budget freezes forever;
  its garbage is never reclaimed.
- **Perm window merge** — `MergeBelow` → `concatSegments`: byte-
  verbatim body copies, indexes shifted by their body's base, one
  identity after the run's newest (1.4).  **DEVIATION #63**: the run
  is currently *everything* below the watermark, so each pass re-folds
  the previous output — the spec says a finished window merges once.
- **Pack tier** — `KVShard.PackFinalized` + `DropBelow`
  (`kv_shard.go`, `blockset.go`): builds a `.bset` from only the
  segments since the previous set (64 B header, 512-entry directory,
  contiguous per-shard sorted indexes, one bloom, verbatim bodies),
  then drops them from the shards, 16 at a time so the barriers
  overlap.  This is 1.4's "merge once, then permanent" done right.
  #47 tracks the daily cadence and levelled runs.
- **DEVIATION #64**: every segment's and set's bloom is loaded
  resident at open, so filter memory and open time grow with age; the
  spec keeps cold filters on disk.

### 2.8 Crash recovery (1.8)

`load()` (`segstore.go:773`): read both manifests (fail-fast if a
named file is missing), open the union, sweep orphans
(`recoverOrphans`), derive tiers from the block height and window
(1.6), rebuild handoff state from what each manifest names, rewrite
only the manifest that disagrees with its tier, replay the live tail
(truncating a torn record).  The crash suite (`crash_test.go`) kills a
child process mid-write across seal and close modes and must recover
every durable key; `unlink.log` records every deletion and manifest
commit for the post-mortem the failure itself cannot give (#61).

### 2.9 The Accumulate adapter

`accumulate/pkg/database/keyvalue/bcdb`: currently opens a **single
KV2** — SealLimit 100k, `CompressEvery` 128 commits, watermark
`MergeLag` 64 blocks — with maintenance async off the committing
goroutine (one run at a time, skipped not queued).  **DEVIATION #62**:
the adapter should open the sharded store; unsharded, it has no pack
tier and leans on `MergeBelow` alone (compounding #63), and perm
ingest serializes through one store.

### 2.10 Deviation register

The current gaps between Section 1 and the code, in one place:

| Issue | Invariant | Gap |
|---|---|---|
| #63 | 1.4 merge-once | `MergeBelow` re-folds the previous merge's output |
| #64 | 1.2 memory / 1.4 cold filters | All blooms resident; open scans every filter |
| #65 | 1.5 dyna converges | Budget-frozen segments never merge; frozen garbage is permanent |
| #66 | 1.6 lock rules | `KV2.Put` holds the store-wide lock across cross-layer reads |
| #62 | 1.9 sharding | Adapter opens one unsharded KV2 |
| #33 | 1.8 one commit point | Residual fsyncs; manifest rewritten whole per commit |
| #47 | 1.4 pack cadence | Daily cross-shard tier not yet scheduled |
| #52 | 1.8 recovery | recoverOrphans can adopt a duplicate of named data |
| #54 | 1.3 filter sizing | Filters sized from all-time max, not recent demand |

A pull request that closes one of these updates this table.  A pull
request that adds one updates it too — knowingly.
