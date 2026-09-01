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

### 1.2 The latency rule (governing aspiration)

> **Latency is minimized forever: no protocol-path operation — Put,
> Get, Seal, commit — may have a cost that degrades toward linear in
> the age or size of the store.**

This rule is aspirational in the honest sense: no database grows
without bound with zero decline in performance, and pretending
otherwise just hides where the decline lives.  The rule names the
decline we accept and forbids the rest.  Acceptable: costs that grow
no worse than logarithmically with total data — a deeper filter walk,
one more level in a levelled structure, a longer binary search.
Unacceptable: any protocol-path cost proportional to the age of the
store — a walk over every segment ever sealed, a pass that rewrites
everything accumulated, memory resident for every key ever written.
When a design choice must place a cost somewhere, it goes on the
rarely-taken path (a deep historical miss), never on the per-write or
per-commit path.

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
- **Permanent reads are windowed too.**  For the Perm layer the
  window is the whole of the protocol's horizon: a key the filters
  deny is absent, and the protocol path does not search deep
  permanent history — outside the last N to 2N blocks, we assume it
  isn't there.  Reaching below the window is an explicit, separate
  operation (1.4) for export and query APIs, never the consensus
  path.  Per-segment history probing on every miss is the decay curve
  this rule forbids: measured at 23% of a validator's CPU at block
  ~245 and growing with the segment count.
- **Dynamic reads are not windowed.**  A dynamic key is state — the
  BPT above all — and state must resolve wherever it last landed,
  however long ago that was.  The Dyna layer therefore searches its
  history on a miss.  That is affordable precisely because dynamic
  keys are rare and the layer grows slowly (1.5); if the dynamic
  layer ever stopped being small, this is the rule that would start
  to cost, and the answer would be to fix its size, not to window the
  read.

### 1.4 The permanent layer

- **An entry is `file : offset : length` and never moves.**  Written
  once, copied only byte-verbatim.  Indexes carry body-relative
  offsets so an index survives its body being placed in a different
  file behind a different base; nothing in an index or a body is ever
  rewritten (#37, #47).
- **A finished window of perm data is merged into a merged block**,
  per shard, and a merged block is folded again only when enough new
  data has gathered behind it to justify its size — the same
  amortisation the dynamic layer compacts by (1.5).  That is the
  middle of two failures: folding every pass re-copies the whole layer
  each time, which is quadratic in the life of the chain (#63); never
  folding a merged block again leaves one per pass forever, which is
  the file-count problem merging exists to solve (#30).  Under the
  ratio a byte is copied a few times over the store's life, and the
  merged blocks stay few.
- **Deep reads are explicit** (`GetDeep`): they walk merged blocks
  newest-first, one filter probe per block, binary-searching only a
  block whose filter claims the key.  The protocol path never takes
  this walk (1.3); it belongs to export and query APIs.  Old blocks
  are read rarely (1.1); their filters may live on disk and be probed
  there — resident filter memory follows the working set, not the
  chain (#64).
- **Cross-shard consolidation is a rarer, second pass**: per-shard
  merges fold into one set file every 1,000 blocks, with one filter
  over the set.  Sets are grouped by block range (`SetGroupBlocks`, a
  day at a block a second) and each FINISHED group carries a filter
  over every key in it, so a deep search skips a whole group in one
  probe instead of walking its sets — ~451 probes after a year rather
  than ~31,500.  What stops growing is the PROBING; the walk still
  visits the set list, which costs no I/O and is off the protocol
  path.  That deep
  search is API-level work, not protocol work (#47).
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
  bounded key set rewritten forever may not grow the disk forever.
  A segment that grows past one pass's budget would otherwise freeze
  with its garbage in it, so a deeper fold is allowed when the
  ordinary pass has nothing to do (#65, 2.7).

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

- `SegmentStore.Put` → `putUnlessPresent` (`segstore.go:1897`).  A
  mutable store appends blind.  An immutable store probes the rolling
  filters: **not in the window → write immediately** (the common case,
  O(1)); a filter hit walks the active tier under the Mutex, and only
  a hit the active tier cannot settle drops the Mutex, walks history
  under its own lock, retakes, and re-checks by epoch (1.6).  Same
  value is an idempotent no-op; a different value is `ErrImmutable`
  (`KV2.Put` routes that key to Dyna instead).
- `Get` (`segstore.go:1710`): live map → filter probe → active
  segments newest-first (resident bloom, then on-disk binary search)
  → history newest-first → packed sets.  The filters settle only the
  window; a miss below it walks blocks by their own filters (1.4).
- `PutIfAbsent` answers absent / present-equal / present-different in
  one pass so `KV2.Put` pays one lookup, not two.
- Costs are counted (`StoreStats`): what the filter settled vs sent
  walking, hits, misleads — the latency rule is measured, not assumed.
- `KV2.Put` takes the KV2 lock SHARED: it excludes only what needs
  both layers to hold still (Seal, Close, SetFilterBlocks), and each
  layer synchronises its own tail and resolves its own history under
  its own lock.  Two puts of one key race to the same place -- one
  writes it, the other finds it present and either no-ops or moves it
  to Dyna, where the newest record wins -- so nothing needed the
  exclusion, and one put's history walk no longer stops the shard
  (#66).

### 2.3 Seal (1.7, 1.8)

`seal` (`segstore.go:2322`): mint `(height, seq)` via `nextKeyAt`,
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
after a block jump).  Sizing feeds from recent demand: each completed span records what it
took in an hourly bucket (24 of them, carried in the manifest so a
restart does not size from a guess), and a filter starting now is
built for the largest span of the last day plus 50% headroom, floored
by what a live span already holds and by the seal limit, capped by
`FilterCapacityMax`.  `FilterSizing` reports capacity, peak and fill
so the sizing can be checked against reality (#54).  Coverage claims are
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
*no* manifest.  At or below the newest named identity an orphan was
superseded; above it, an orphan is an interrupted seal and is adopted
(#45) -- unless the file says it is maintenance output, which is named
by a manifest or is garbage, or the cold watermark already covers its
block, both of which would otherwise store the same keys twice (#52).
A segment's header records which it is; files written before that
field read as sealed, which is what they are.

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
  (#61).  A segment that grows past the budget would otherwise freeze
  for good, so when the ordinary pass has nothing to do one deeper
  fold is allowed, bounded by `CompactDeepPassRecords`.  It is the
  same choice under a larger budget — a suffix where one fits, a pair
  where it does not — still gated by the ratio, and each fold leaves
  one segment where there were several, so the layer converges to its
  live key set instead of holding frozen garbage forever.  Once
  segments exceed half the ordinary budget the deep pass is what runs
  on every call, not a rarity, and its cost is the larger bound (#65).
- **Perm window merge** — `MergeBelow` → `concatSegments`: byte-
  verbatim body copies, indexes shifted by their body's base, one
  identity after the run's newest (1.4).  The run is chosen by the
  same rule compaction uses (`compactionRunWithin`): the newest suffix
  below the watermark, reaching one block further back only when what
  has gathered behind it is worth its size, inside one pass's budget
  (#63).
- **Pack tier** — `KVShard.PackFinalized` + `DropBelow`
  (`kv_shard.go`, `blockset.go`): builds a `.bset` from only the
  segments since the previous set (64 B header, 512-entry directory,
  contiguous per-shard sorted indexes, one bloom, verbatim bodies),
  then drops them from the shards, 16 at a time so the barriers
  overlap.  This is 1.4's "merge once, then permanent" done right.
  Sets are grouped by block range and a finished group gets one filter
  over its keys (`dayfilter.go`), written once when a later set closes
  the group out, held on disk and probed there; a group with no filter
  — the one being filled, or one whose filter could not be built — is
  walked, which is correct and merely slower.
- **A cold read takes no lock.**  The set store publishes its sets and
  its group filters as one immutable snapshot, replaced wholesale, so
  a deep read loads it atomically and walks it — and sees a filter
  only alongside the sets it was built from.  Reading two slice
  headers under one exclusive global mutex put every cold read of all
  512 shards through the same lock.
- **A pack pins, it does not lock.**  Each shard's segments are pinned
  and its list copied (`historyBelow`), the set is built by reading
  those files, and only then does `DropBelow` remove them — so a pin
  defers a file's DELETION and nothing else.  Shards go on taking
  writes, sealing blocks and committing manifests with the whole
  database pinned, which is what keeps the most expensive operation in
  the store off the protocol path (#47).  `build` already streams
  shard by shard, so nothing per-shard is held for the set's size
  either.  #47 tracks what is left: the pack cadence in the adapter.
- **Filter residency** — a segment's bloom is held in memory only
  while the segment is in the active tier; a history segment's and
  every block set's filter stays on disk and is probed there
  (`segment.bloomTest`, `blockSet.bloomTest`): K one-byte reads from a
  file the pool already holds open, which the page cache keeps hot.
  Resident filter memory therefore follows the window, not the chain,
  and an open reads one index header per segment rather than every
  filter the store ever wrote (#64).

### 2.8 Crash recovery (1.8)

`load()` (`segstore.go:860`): read both manifests (fail-fast if a
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
| #62 | 1.9 sharding | Adapter opens one unsharded KV2 |
| #33 | 1.8 one commit point | Residual fsyncs; manifest rewritten whole per commit |
| #47 | 1.4 pack cadence | Pack cadence not scheduled in the adapter (group filters and pin-not-lock done) |

A pull request that closes one of these updates this table.  A pull
request that adds one updates it too — knowingly.
