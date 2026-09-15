# Searching history

Status: **step 1 built, the rest designed.**  History's filters are now
held to a budget rather than freed (2026-09-11, spec 1.2, 1.4, 2.7);
everything below that is design.  The simulations behind every number
here are in `database/keytable_sim_test.go` and run with
`go test -run TestKeyTable ./database/`.  Tracks issues #86, #87, #88.

## The problem

A read that misses the window walks history newest-first, probing one
segment's bloom filter after another until one admits the key
(`SegmentStore.lookupHistory`).  Each probe is cheap.  There is one per
segment, so the walk grows with the number of segments even though no
step does.

Measured on an 8-node Accumulate network at 500 tx/s (#86):
`segment.lookup` was 17.8% of a validator's CPU, 82% of it inside
`bloomTest`, over ~40 perm segments and 6-7 dyna segments a shard, and
the share grew as segments accumulated.  A later soak (#88) measured
the dynamic layer specifically:

| per node, 5,600 commits | count |
|---|---|
| dyna lookups | 78.9M |
| of those, proved absent in the window | 62.5M |
| of those, walked history to prove absent again | 11,152 per commit |

The dynamic layer is where this hurts, because a mutable store cannot
rule a key out by the window: `Get` of any account not touched for N
blocks crosses the whole of dyna history.  The permanent layer's walk
is taken by deep reads and by the immutability check behind a filter
hit, and it is the longer of the two.

Spec 1.2 does not allow a protocol-path cost proportional to the size
of the store.  This is one.

## What the target is

The database is expected to reach millions and eventually billions of
keys, and that has never meant making a billion keys fast to reach
(spec 1.1).  It means the SIZE of history may not load the active
execution path.  Reaching old data quickly is an application's need,
not the protocol's.

That changes what the work below is for.  Everything here is worth
having only insofar as it keeps history's growth off the execution
path; a structure that makes a deep read fast at the cost of work on
every commit is the wrong trade, however good the read number looks.
It also says where this ends: a packed set is permanent and never
rewritten, which makes it the unit that can leave the node for a data
server, and the location table below is that server's structure rather
than a validator's.

## Four designs, and what killed three of them

### A flat index over history, rebuilt as history changes

One sorted file mapping key to file and offset, covering a run of
history segments, rebuilt when enough segments fall outside it.

**Killed by write cost.**  Rebuilding is O(covered keys) per rebuild,
and bounding a pass only shrinks the fraction covered until the
accelerator stops accelerating.

| keys | write amplification |
|---|---|
| 1.0M | 3.0x |
| 67.1M | 33.7x |
| 268.4M | 59.3x |
| 1.07B | **75.7x, still climbing** |

### The same, with an unsorted tail

New keys append to a tail; the tail is merged into the sorted body
once it reaches a fraction of it.  This fixes the write cost
completely and holds it flat.

| tail policy | write amplification at 1.07B |
|---|---|
| no tail | 75.7x |
| tail at 25% of body | **7.2x, flat** |
| tail at 50% of body | 4.2x, flat |

**Killed by read cost.**  At 120,000 keys a bucket, a 25% tail is
5,127 entries, and scanning it on every lookup costs about 66 page
touches, which is worse than the segment walk being replaced.

### Sorted runs instead of a tail

The resolution is that a tail need not be unsorted: a fold already
holds its keys in order, so it can write a *sorted run* as cheaply,
and a sorted run carries a directory like the body does.  Measured on
real keys, counting distinct 4 KB pages touched:

| entries in a run | linear | binary | directory on leading bits |
|---|---|---|---|
| 100 | 1.1 | 1.2 | 1.0 |
| 1,024 | 6.4 | 3.7 | 1.2 |
| 16,384 | 97.8 | 7.7 | 2.0 |
| 120,000 | 711.7 | 10.5 | **2.2** |

The directory works because keys are hashes and therefore spread
evenly, so the leading bits say almost exactly where a key sits.  Two
rules fall out and are worth keeping whatever else changes: **search a
run of up to a couple of hundred entries linearly**, because below
that everything touches one or two pages and structure buys nothing;
**above that use a directory, never a binary search.**

A hash-partitioned table of such buckets, starting at 8 and splitting
at 120,000 keys, reaches 16,460 buckets at a billion keys and costs:

| merge ratio | runs per bucket | write amplification | pages per lookup |
|---|---|---|---|
| 0.10 | 2.7 | 20.8x | 5.4 |
| 0.25 | 3.1 | 14.1x | 5.8 |
| 0.50 | 3.7 | 10.1x | 6.5 |

with 52 GB of entries at a billion keys, or 20 GB using a ten-byte key
prefix instead of the whole key — which is safe, because the record
header carries the full key, so a prefix collision costs one wasted
read and never a wrong answer.

### A location table over a layer that compacts

**Killed by relocation, and this is the finding that matters.**

A table entry says where a record *is*.  Maintenance moves records.
An entry pointing at a moved record is wrong; reading it is safe,
because the record header carries the key and a moved record reads as
a miss, but a table whose entries are mostly wrong costs more than it
saves.

Two timescales decide it, and both are already fixed by choices made
elsewhere:

- An entry stays true until compaction rewrites its record.  The layer
  turns over every `N/(A*I)` seconds, for `N` keys, ingest `I` and
  compaction amplification `A`.
- An entry lives in the table until the run holding it is merged,
  which the 0.25 ratio puts at a quarter of a bucket's keys arriving.
  Shortening that is what takes write amplification from 14x back
  towards 76x, so it cannot simply be shortened.

Both scale the same way with `N`, so the stale fraction is
**scale-invariant**, and because the fallback walk grows with `N` the
effective read cost diverges:

| layer | compaction | entries stale | effective read pages |
|---|---|---|---|
| 1.0M | 4x | 39.3% | 3.0 |
| 100.0M | 4x | 39.3% | 30.0 |
| 1.00B | 1x | 11.8% | 86.7 |
| 1.00B | 4x | 39.3% | **283.3** |
| 1.00B | settled data, never rewritten | **0.0%** | **3.0** |

No tuning inside the table fixes this, because the two timescales move
together.  The conclusion is sharp: **a location table can only point
into data that maintenance never rewrites.**

The permanent layer has such a tier already — a packed set is
permanent by spec 1.4.  The dynamic layer has none, and by 1.5 never
will, because compacting away superseded records is its whole job.

## The design

Two artifacts, matched to what each layer actually guarantees.

### 1. History filters — membership, no locations

A blocked bloom filter over a **block range** of history, per layer per
shard, written once when the range is finished and never updated.

Range-keyed rather than segment-keyed, which is the day filter's rule
(`dayfilter.go`) one tier lower, and it is what makes the artifact
immune to maintenance.  A merge or a compaction preserves the key set
of a range — it drops superseded *records*, not keys — so a filter
over a finished range stays true however its segments are later
rewritten.  Packing moves keys out of the shard, which only makes the
filter over-claim.  Over-claiming costs a walk and can never produce a
false absent, which is the filters' existing rule (#35).

Blocked, meaning every bit for a key lands in one page, so a probe is
one page touch however large the filter is and however many hash
functions it uses.  The store's current `Bloom` scatters its bits,
which is why it is stuck at three hash functions:

| filter | pages per probe | false positives | build |
|---|---|---|---|
| scattered, 12 bits, k=3 (today) | 3 | 1.140% | 12.6 ns/key |
| blocked, 12 bits, k=3 | 1 | 1.071% | 11.5 ns/key |
| **blocked, 12 bits, k=8** | **1** | **0.318%** | 26.5 ns/key |
| blocked, 16 bits, k=8 | 1 | 0.060% | 26.9 ns/key |

Optimal hash count is about 0.7 times the bits per key, which the
measurements confirm.  Size is 2.9% of the entries it covers, so 1.5 GB
per billion keys.  Build time is not a constraint in any direction: one
key inserted is 26.5 ns, one bucket rebuilt at a split is 3.2 ms, and
the whole table rebuilt from nothing is 28 s per billion keys.

A filter is written once alongside the data it covers, from keys the
writer already has in hand, so there is no extra pass and no
maintenance.  **A filter must never be one-per-bucket kept up to
date**: a fold delivers keys across every block of the filter, so
adding a hundred keys dirties the whole thing — 176 KB rewritten to
record 117 bytes.

### 2. A key table — locations, over settled permanent data only

The hash-partitioned table above, restricted to data that is permanent
by construction: packed sets.  Three page touches per lookup at any
scale, per the relocation table.  This serves deep reads and the API
reads of #83, which have no working set to cache and are the ones that
walk furthest.

Not built until the filters are, and not over the dynamic layer at all.

## What is decided and what is not

Decided, with evidence above: range-keyed rather than segment-keyed;
blocked filters at twelve bits and eight hash functions; membership for
the dynamic layer and locations only over settled permanent data;
linear search under a couple of hundred entries and a leading-bits
directory above it; short entries with verification on read.

**Open: the level schedule.**  One filter per range means the probe
count grows with the age of the chain, which is the same problem the
day filter has (spec 1.4 quotes ~451 probes after a year).  Levels with
geometrically larger ranges bound it, but the coarsest level's count
still grows linearly with age, so the schedule has to be chosen against
an expected lifetime rather than derived.  This needs its own
simulation before anything is built.

**Residency: done.**  `handoffBelowWindow` freed the bloom of every
history segment, so each probe was K one-byte reads from disk.  A
segment now keeps its filter while `BloomResidentBytes` has room,
newest first, and an open refills the budget.  Measured over 39
history segments, an absent-key lookup went from 18.7 µs to 1.8 µs.
The budget is 8 MB a store, about 5.5 million keys of history, and
past it the oldest segments are probed on disk exactly as they all
were before — so this holds until history outgrows the bound, which is
what the filters of step 2 are for.

## Plan

1. ~~Keep history blooms resident under a budget, and add the
   segment-count gauges of #87.~~  **Done**: 10.3x on the measured
   absent-key walk, spec 1.2, 1.4 and 2.7 updated.
2. Simulate the level schedule, then build the history filters for when
   history outgrows the residency budget.
3. Revisit the key table for the permanent layer once the filters are
   in and measured.

Spec changes belong with step 2: 1.4 gains the filter tier below the
set groups, 2.7 gains the pass that writes them, and 1.5 gains the
statement that the dynamic layer is searched by membership rather than
by location, because it rewrites its own records.
