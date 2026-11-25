# rho-memory

what rho holds in RAM and on disk per entity, per shard and per store, and what grows with the total volume.

Items of this topic; the head of [index.md](index.md) says how to use them. Fields: [index.md](index.md#fields).
Code facts are read at origin/main f837590b (v4.107.17); paths without a prefix are under `pkg/graph/`. The files
under `tasks/evidence/` that earlier texts cite are not in the repository; no item relies on them.

### RHO-2 The share of rho's memory store in a session's live heap is measured again

- area: RHO
- kind: measure
- provides: MEM
- state: idea
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

On a consumer's two-day run a large share of the live heap belonged to rho's in-memory graph, which then held
every event and every derived fact. Derived facts no longer go to rho, so that share is out of date; the events'
share is what remains and is not known.

#### Current state

- `store/memory` is the in-memory store. Which runs of ai-soc use it and which use the tiered store was not read.
- No measurement after the session stopped writing derived facts exists.

#### Design

- One run of the same two-day set on the current pair (ai-soc with the tiered store as it deploys), a heap profile
  at the end, and `MemoryStats` beside it: bytes held by rho per holder, and their share of the live heap.
- If the deployed path does not use the memory store at all, delete this item with that finding.

#### Test

The run is the test; its report states rho's bytes by holder and the heap total. No pass line: it decides whether a
fix item follows.

#### Open points

- Which host and dataset the run uses is the maintainer's word.

### RHO-3 `docs/architecture.md` lists everything in rho that grows with the total volume, each bounded or queued

- area: RHO
- kind: measure
- provides: MEM,STABLE
- state: designed
- release: 5.18
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

rho's standing goal is ingest at about 2 TB a day grown only by more RAM, NVMe and block storage. That holds only
if nothing costs RAM per entity, or open, recovery, Clear or compaction time per store size, without a bound. This
item is the list: every such cost named, measured, and either bounded or an item of its own.

#### Current state

- `docs/architecture.md` has no such section.
- Bounded and released: the existence maps (about 8 bytes a node, reopen about 0.25 s per million), the walks at
  open, and the reopen after a crash.
- Known and not bounded: the reference shard keeps every keyed node for ever; per-entity side structures built on
  first use (the relationship valid-time index, belief watermarks, transaction membership, history presence, the
  ID table); the hot shard's write rate (RHO-34).
- The per-entity RAM is RHO-4's; this item only lists it.

#### Design

- A section in `docs/architecture.md` with one row per cost: what grows, with what (entities, shards, store
  bytes), the measured figure, its bound or the key of its item.
- Each row's figure comes from a test or a named run, not an estimate. A row without a bound and without an item
  gets its item in the same commit.

#### Test

A docs check in `internal/docscheck`: every row of the section names either a test that exists or an item key
that is in `tasks/index.md`.

#### Open points

- Since the session's derived facts left rho, "the reference shard keeps every Derived fact" no longer grows;
  what else a session still writes there (its Session and image nodes) is to be listed from ai-soc's writer.

### RHO-4 rho's RAM per entity is capped or moved to keys on disk

- area: RHO
- kind: perf
- provides: MEM
- state: parked: paused 19 Nov, necessary work first
- release: later
- owner: -
- depends: -
- blocks: RHO-5, RHO-25
- links: -

#### Goal

rho keeps several structures in RAM with one entry per entity ever seen: sets of live IDs, history presence and
stamps, and four caches without a configured cap. That is several hundred bytes an entity on a consumer's
workload, so RAM grows with the store for ever, against the standing goal.

#### Current state

- The design exists in three steps (caps for the four caches; existence and live-ID sets as keys on disk; history
  presence and stamps as keys on disk); its text was in the evidence folder and is not in the repository.
- The first step was built in a worktree that was not committed; nothing of it is on `main` or `dev`.

#### Design

- Step 1: each of the four caches gets a byte cap from the store's cache budget and evicts.
- Step 2: existence and the live-ID sets are answered from keys in badger, RAM holding a bounded cache.
- Step 3: history presence and stamps the same way.
- After each step the per-entity figure is measured again by the item's test.

#### Test

`TestHeapPerEntityFlat` in `store/badger` (Slow tier): 100,000 and 1,000,000 entities of one label with three
properties written and read once; asserts that live heap after a forced collection differs by less than the
caches' configured caps. The first test of the item runs a consumer's path: an import of generated rows through
ai-soc's writer with `MemoryStats` read at two sizes.

#### Open points

- The step design has to be written again or recovered from the session that held it.

### RHO-5 Per-entity answers leave the Go heap: the heap is fixed and RAM is page cache

- area: RHO
- kind: design
- provides: MEM
- state: parked: waits for RHO-4
- release: later
- owner: -
- depends: RHO-4
- blocks: -
- links: -

#### Goal

The end state for rho's memory: a heap whose size does not depend on the data, the data on NVMe and block storage,
and the operating system's page cache as the only RAM that follows the data. badger stays the store.

#### Current state

- Its first step (existence answered without a per-entity map) became step 2 of RHO-4. Nothing else is designed.

#### Design

- After RHO-4: list what still allocates per entity or per shard on a read or write (RHO-3's section), and move
  each to a bounded cache over keys or mapped files.

#### Test

To be named with the design. Its shape: RHO-4's heap test at ten times the entities with the same heap.

#### Open points

- RHO-32's option (a) is a step of this item.

### RHO-6 The RAM of a writable and of an inactive shard is measured on three production-sized examples

- area: RHO
- kind: measure
- provides: MEM
- state: parked: after the small production-sized example
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Each open shard costs fixed memory whether it is written or not. The write arenas stay as they are (decided); what
is missing is the measurement that shows what a deployment pays per open handle and per tier.

#### Current state

- A cold shard opens with an 8 MiB memtable (`coldMemTableSize`, `store/tiered/warm_memtable.go`), about 10.4 MiB
  of heap; warm shards take `Config.WarmMemTableSize`; `MemoryStats` reports the arenas per tier.

#### Design

- On three production-sized examples (small, medium, large) through ai-soc: RSS, the time a demotion takes, and bytes
  written
  per tier, with `MemoryStats` at the start and end.

#### Test

The three runs; the report gives fixed bytes per open shard by tier.

#### Open points

- No run on the test host for this (decided); the examples run where the maintainer allows.

### RHO-7 One advance of a session opens no cold day shard

- area: RHO
- kind: measure
- provides: MEM
- state: unknown
- release: 5.13.4
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A session's advance once read state back from every cold day shard, so the number of shards a cut opened grew with
the archive and no cap on open cold shards could be right. The session no longer keeps its state in rho's day
shards; whether an advance still opens cold shards has to be read once.

#### Current state

- `tiered.Config.MaxOpenColdShards` caps the cold shards a read leaves open; `ShardAccessStats.ColdOpens`
  (`store/tiered/shard_access_stats.go`) counts lazy opens.
- No measurement on the current ai-soc and Sigma pair.

#### Design

- One ai-soc run over at least five days of rows with three cold days: `ColdOpens` read before and after each
  advance.
- Zero per advance outside backfills: delete this item. Otherwise the reads that open them are named and become
  an item where the read is issued.

#### Test

In ai-soc: a test over test rows spanning five days asserts `ColdOpens` unchanged across an advance that carries
only current-day rows.

#### Open points

- None.

### RHO-8 What is never cold is bounded: rho's heap grows over days in parts no tier frees

- area: RHO
- kind: perf
- provides: MEM
- state: parked: waits for ai-soc's view of what holds the memory
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Over three days of a consumer's run rho's heap grew in parts that no tier frees: the reference shard, the hot and
warm shards, demoted handles still open, the degree tracker, and the per-shard entity caches of decoded rows.

#### Current state

- Not measured since `MemoryStats` exists, which reports exactly these holders per tier.

#### Design

- Read `MemoryStats` per day on a three-day run. Each holder that grows per day gets a bound (a byte cap from
  the cache budget) or is named in RHO-3's section with its item.

#### Test

In ai-soc: the three-day run asserts that the sum `MemoryStats` reports for the reference, hot and warm tiers on
day 3 is within 1.2 of day 2.

#### Open points

- Overlaps RHO-4 for the per-entity parts; this item is the per-shard parts.

### RHO-9 A clean reopen of a store takes time that does not grow with the store

- area: RHO
- kind: perf
- provides: PERF
- state: parked: waits for the maintainer's word to read profiles on the test host
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A consumer's restart waits for `tiered.New`. On a production-sized store that took tens of seconds, most of it in
the reference shard, and it grew between opens. A restart must cost the same at any store size.

#### Current state

- Bounded already: the existence maps' reopen and the open walks. What the remaining seconds are is not profiled.

#### Design

- A CPU profile of `tiered.New` on the production-sized store at two sizes; the parts that scale become fixes here.

#### Test

`TestCleanOpenFlatInStoreSize` in `store/tiered` (Slow tier): stores with 1 and 10 million reference nodes; asserts
open time within 1.5 of each other.

#### Open points

- None.

### RHO-10 Where the the extra storage goes: the split by layer

- area: RHO
- kind: measure
- provides: MEM
- state: parked: waits for the maintainer's word and a free test host
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A stored event takes several times its raw size in storage and in memory. The memory split by layer (ai-soc,
Sigma, rho) is known; the storage split is not, so nobody knows which layer to change.

#### Current state

- No figure per committed line exists; RHO-33's run produces it.

#### Design

- From RHO-33's whole run: bytes per committed line split into event rows, adjacency, property index entries,
  history versions and reference nodes, each by key prefix from badger's table statistics.

#### Test

The report of that run.

#### Open points

- Default while unanswered: no change to the layout before the numbers.

### RHO-11 ai-soc's store layout is agreed: encoding per label and the tier of each kind of node

- area: RHO
- kind: design
- provides: MEM
- state: unknown
- release: later
- owner: -
- depends: RHO-10
- blocks: -
- links: -

#### Goal

How a consumer lays its data out in rho decides rho's bytes: which labels use delta encoding, on which tier the
session's own nodes live, and how large the share of instance nodes is. Only one set of settings was ever
agreed.

#### Current state

- The session now writes only its Session and image nodes to rho; the earlier question of where derived facts
  live is gone. The rest is not read against ai-soc's current writer.
- `PropertyIndexCurrentOnlyLabels` (v4.107.17) lets a label's property index skip its membership entries; ai-soc
  lists no label yet.

#### Design

- Per label of ai-soc's writer: encoding, tier, which indexes, and whether it is read only at its current
  version (then it joins `PropertyIndexCurrentOnlyLabels`).

#### Test

In ai-soc: `TestStoreLayoutMatchesTheAgreedTable` asserts the store's configuration against the table.

#### Open points

- Needs the maintainer with ai-soc and Sigma.
