# rho-read

read doors, the read meter, the cost of reads over cold shards and long histories.

Items of this topic; the head of [index.md](index.md) says how to use them. Fields: [index.md](index.md#fields).
Code facts are read at origin/main f837590b (v4.107.17); paths without a prefix are under `pkg/graph/`..

Two terms used below. The read meter is `store.ReadMeter` (`store/read_budget.go`): one per cut, carried in the
context, charged by every `...Ctx` door before the work for candidates, decoded bytes, buffer bytes and shard opens;
a door that cannot count a dimension refuses a budget on it (`budgetSupported`). A cold shard is a day shard past
`Config.ColdAfter`; it is closed and opened by the read that needs it (`MaxOpenColdShards` may keep some open).

### RHO-13 The cost of a backfill read in rho is measured: per shard open, per row, rows read against rows kept

- area: RHO
- kind: measure
- provides: PERF
- state: idea
- release: 5.18
- owner: -
- depends: -
- blocks: RHO-14, RHO-1003; Sigma's backfill batching
- links: -

#### Goal

A consumer's rules read an entity's past rows when the entity is first flagged: a read by property value over
many days, most of them cold shards. Its cost in rho is not measured, so nobody can say whether a week-late event
makes a cut slow or whether rho needs a new door.

#### Current state

- A property read under a meter is `Nodes().ByLabelAndPropertyCtx` / `Rels().ByTypeAndPropertyCtx`
  (`internal/core/read_budget_lookup_doors.go`); on the tiered store it opens each closed shard it needs, charged.
- A metered read opens a cold shard read-only (`openColdReadOnlyLocked`, `store/tiered`); a late write opens it
  with an 8 MiB memtable.
- No benchmark reads one key over several cold days.

#### Design

- A benchmark on the tiered store with 28 day shards, 25 of them cold, filled with generated event edges (two
  indexed properties each): one key, and 100 keys in one pass, through the property door with a valid window of
  1, 7 and 28 days.
- Reported per case: milliseconds per shard open, microseconds per row returned, rows examined over rows kept,
  bytes charged to the meter, at `MaxOpenColdShards` 0, 2 and 28.
- The result decides RHO-14 (a byte cap on open shards) and RHO-1003 (a start-bounded door), and tells a
  consumer whether the read or its own work per row dominates a backfill.

#### Test

`BenchmarkBackfillByPropertyOverColdDays` in `store/tiered` (`make bench`), with the counters above as reported
metrics; one unlimited control per case asserts the rows returned equal the rows written for the key.

#### Open points

- The first test of an item runs ai-soc's path; for a measurement that is ai-soc's backfill of one flagged key
  on test rows, taken once beside the benchmark.

### RHO-14 Open shards are capped by bytes, not by count

- area: RHO
- kind: feature
- provides: MEM
- state: idea
- release: 5.18
- owner: -
- depends: RHO-13
- blocks: -
- links: -

#### Goal

A backfill over W days touches W minus 3 cold shards. With two kept open every further day is a reopen; with all
open, at the 10.4 MiB a cold open costs (RHO-6), the memory is about 0.26 GiB for 28 days and 3.8 GiB for a year.
The cap that fits both is a number of bytes the host gives rho, not a number of shards.

#### Current state

- Built: a metered read opens a cold shard read-only; `tiered.Store.MemoryStats` reports the RAM of open stores per
  tier, and ai-soc reads it.
- The cap is a count: `tiered.Config.MaxOpenColdShards` (0 closes after each read). ai-soc sets it.

#### Design

- `tiered.Config.MaxOpenColdBytes`: cold shards opened by reads stay open while the sum of their reported bytes
  (the figure `MemoryStats` gives per handle) is under it; the least recently read closes first. The count cap is
  deleted in the same change (no second mechanism), and ai-soc sets the byte value.
- The handle sweep stays the one place that closes shards.

#### Test

First in ai-soc: a backfill over ten cold days under a byte cap that fits three; asserts `MemoryStats` for the cold
tier never above the cap and equal answers to an uncapped run. Then `TestOpenColdShardsStayUnderTheByteCap` in
`store/tiered` with the pin, late-write and close-fault cases of the sweep's tests.

#### Open points

- Whether RHO-13 shows the need at all; without it this item is deleted.

### RHO-15 A deleted relationship's history read opens only the shard that holds it

- area: RHO
- kind: perf
- provides: PERF
- state: parked: waits for a cut refused at shard opens on ai-soc's path
- release: later
- owner: -
- depends: -
- blocks: -
- links: `TestReadBudget_AdjacentEndpointAtShardOpens`

#### Goal

Reading the history of a relationship that was deleted asks every history shard that is not sealed, and every
sealed one whose ID span could hold it. After a kill, until the sweep seals the cold shards again, that is every
cold shard, each open charged to the cut.

#### Current state

- The endpoint door `Rels().ForEachAdjacentEndpointAtCtx` opens a start node's own day shard alone once cold
  shards are sealed (`TestReadBudget_AdjacentEndpointAtShardOpens`, `internal/core`).
- A node's former relationships are kept in its ex-adjacency (`internal/core/adjacency_version.go`,
  `store.ExAdjacencyCapability`); the history read does not use it to pick the shard.

#### Design

- The ex-adjacency entry names the relationship's ID, whose time part names its day shard; the history read
  opens that shard and no other.

#### Test

In `internal/core`: the endpoint test's fixture with its cold shards unsealed; asserts one shard open for a deleted
edge's history against an unlimited control.

#### Open points

- None.

### RHO-16 A test can wait for a cold shard's close instead of timing it

- area: RHO
- kind: fix
- provides: STABLE
- state: designed
- release: 5.18
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A consumer's test of a read that must open no shard needs a cold shard to be closed before it reads. It has no
way to know when the close happened, so it fails rarely under load on its precondition.

#### Current state

- The handle sweep closes cold shards (`store/tiered/handle_sweep.go`). Its only doors for tests are in rho's own
  `test_export.go` (`SweepHandlesForTest`, `SweepPassForTest`), which another module cannot call.

#### Design

- A public, read-only observable on the tier API: the number of cold shards open now (it is already in
  `ShardAccessStats` / `MemoryStats` as counts per tier). The Sigma test polls that with a deadline; no new hook
  in production code if the existing counts suffice.
- If they do not: one exported method on `tier.API` that runs a sweep pass and returns when it is done.

#### Test

In Sigma: the test's precondition waits on the count; 500 runs under load green. In rho: the count is 0 after a
sweep pass over a store whose cold shards have no pin.

#### Open points

- Adding a public method needs the maintainer's word; the count route does not.

### RHO-17 The member-time statistics answer at a pin and for a filter value

- area: RHO
- kind: feature
- provides: PERF
- state: idea
- release: 5.20
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Sigma's planner prices a timed read from rho's statement of how a label's members are spread in time. The
statement exists for the present and the whole label only, so a read at a pin, or of one property value, is priced
from the wrong figures.

#### Current state

- `Stats().LabelTimeStats(label)` (`stats/api.go:420`, `internal/core/member_time_stats.go`) takes a label alone.
  Sigma's planner calls it when it prices a timed read.

#### Design

- `LabelTimeStatsAt(label, pin, filter)`: the same figures for the members at a transaction-time pin and, where a
  property filter is given, for the members holding that value, from the property index's member entries.
- A label whose index is current-only (`PropertyIndexCurrentOnlyLabels`) refuses the pinned form with
  `ErrPropertyIndexCurrentOnly`.

#### Test

First in Sigma: a timed read at a pin over a label that grew tenfold after the pin plans with the pinned figures
(EXPLAIN shows the row estimate of the pin). In rho `TestLabelTimeStatsAtAPin`: figures at a pin equal the figures
of a store written only up to the pin.

#### Open points

- None.

### RHO-18 Every rho read that ai-soc's cycle reaches is charged to the cut's read meter

- area: RHO
- kind: fix
- provides: MEM
- state: idea
- release: later
- owner: -
- depends: RHO-20
- blocks: ai-soc's one memory account per cut
- links: -

#### Goal

One memory account per cut is the system's rule. It holds for rho only if every read the cut's path can reach
stops at the cut's meter; one unmetered door is unbounded memory inside a bounded cut.

#### Current state

- The as-of reads are metered (released in v4.107.3). The doors named `...Ctx` charge; the plain doors of the
  same reads do not take a meter.
- No list exists of the rho doors ai-soc's and Sigma's cycle path calls, with metered or not per door.

#### Design

- The inventory: every rho call reachable from ai-soc's cycle (its writer, Sigma's sources, the restore), by a
  static call listing over both repositories, each marked metered, unmetered or refused under a meter.
- Each unmetered one becomes a call of its `...Ctx` door at the caller, or a row of RHO-20 where rho has no
  metered door for it.

#### Test

In ai-soc: one cut over test rows under a meter with a limit of 1 in each dimension in turn; asserts the cut is
refused by the meter in every dimension (no read completes outside it), against an unlimited control.

#### Open points

- Overlaps RHO-20, which builds the mechanism; this item is the list and the callers' side.

### RHO-20 Every native iterator and shard open goes through one mechanism that requires a tally

- area: RHO
- kind: feature
- provides: MEM
- state: parked: stopped 19 Nov mid-build; nothing of the build is on a branch
- release: later
- owner: -
- depends: -
- blocks: RHO-18; ai-soc's read admission
- links: -

#### Goal

A read door can do work its caller's meter has not charged: a tally that is optional or nil, a count taken after
the work, a private loop over an iterator. Found so far on the composite walk, the composite page, the range
doors, and the paged member door under a budget (it takes no tally). Fixing each door alone leaves the next one
open; the maintainer's order is one general mechanism.

#### Current state

- `store.ReadMeter` and `ReadBudget` are in `store/read_budget.go`; doors opt in through
  `opts.WithReadMeter(...)` and `budgetSupported`.
- Neither named test exists in rho or ai-soc. The half-built change (73 files) was not kept.

#### Design

- One type owns badger iterators and shard opens for reads. It cannot be constructed without a tally (an
  unlimited tally is explicit), and it charges each step before it yields. Every read door, public or internal,
  takes its iterators and opens from it; a private iterator loop outside it fails a lint test.
- A door that cannot charge a dimension refuses a budget on that dimension before it starts.
- The paged member door takes the tally of its caller.

#### Test

First, in ai-soc: `TestCompositeWalkStopsAtTheLimit` (a composite read over several keys and shards that each fit
the limit and exceed it together stops at the meter; an unlimited control returns the rows). Then
`TestReadDoorsChargeBeforeWork`: one row per public read door found by reflection; each is called under a limit of
one candidate with a counter at the native boundary and must be refused before the counter passes the limit.

#### Open points

- The earlier analysis was in the evidence folder and is not in the repository; the design above is the item's
  one-sentence aim spelled out and is to be written properly before the build.

### RHO-21 A read that pinned its view before Close returns `ErrStoreClosed`

- area: RHO
- kind: fix
- provides: CORRECT
- state: idea
- release: 5.18
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

On a plain badger or sharded store, a read that took its as-of view before `Close` and continues after it gets
badger's own untyped error. A caller cannot tell "the store was closed" from a fault. The tiered store waits for
shard pins and is not affected.

#### Current state

- `ErrStoreClosed` is returned at the doors' entry checks (`store/badger/badgerstore.go`, `store/tiered`). A read
  already past the check is not mapped.

#### Design

- The badger store's read helpers map badger's closed-database error to `ErrStoreClosed` where they return.

#### Test

`TestReadAfterCloseIsErrStoreClosed` in `store/badger`: a read blocked after it pinned its view, `Close`, then the
read continues; asserts `errors.Is(err, ErrStoreClosed)`.

#### Open points

- Low priority: ai-soc runs the tiered store.

### RHO-22 A bounded correction in a cold process plans without listing the whole chain

- area: RHO
- kind: perf
- provides: PERF
- state: designed
- release: 5.13.4
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A correction of a node's validity plans its change from the node's existing versions. In a process that has not
read the node yet (after a restart) the planner lists the node's whole version chain once, even when the
correction touches only its newest part. That is write CPU per fact after every restart.

#### Current state

- `Core.nodeMissPlan` (`internal/core/cascade_meta.go:132`) is the planner for a node whose head is not cached.
- The stamps sidecar exists per node; the planner does not read its two properties used below.

#### Design

- The stamps sidecar proves two properties of a chain: `Rising` (each row starts at or above every finite bound
  of the rows below it) and `Void`. With both, a head entry built from a suffix of the chain carries the bounds of
  the lowest row read and is enough to plan a correction inside that suffix.
- The planner reads the suffix the correction needs and plans; it lists the whole chain only when the sidecar
  cannot prove the properties.

#### Test

`TestBoundedCorrectionReadsASuffix` in `internal/core`: a node with 1,000 versions, a fresh process, one
correction of the newest interval; asserts decoded versions at most 8 by the native decode counter, and the same
result as a planner that lists all.

#### Open points

- None.

### RHO-23 A chain longer than the head entry's cap is still read in part

- area: RHO
- kind: perf
- provides: PERF
- state: parked: paused 19 Nov, necessary work first
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

The cached head of an entity keeps at most 2,048 pieces. For a chain beyond that the read of the tail falls back
to reading the whole chain, so the longest chains cost the most per read.

#### Current state

- `headMaxEntryPieces = headStripeMaxWeight / 8` (2,048) in `internal/core/effective_head.go:100`.

#### Design

- A chain past the cap keeps a head entry of its newest 2,048 pieces with the bound below which it is
  incomplete; a read above the bound is served from it, a read below reads the tail from that bound down.

#### Test

`TestLongChainTailReadIsBounded` in `internal/core`: a chain of 5,000 pieces, a read of its newest state; asserts
decoded pieces at most the cap.

#### Open points

- None.

### RHO-24 An untyped adjacency read of a reference node lists only the shards that hold its edges

- area: RHO
- kind: perf
- provides: PERF
- state: parked: paused 19 Nov, necessary work first
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Reading all relationships of a reference node without a type asks every shard that has rows at all, although the
node's edges are in a few of them. Shard opens per adjacency read grow with the archive.

#### Current state

- The typed and the endpoint doors are narrowed already (RHO-15's test). The untyped read's shard list was not
  read here.

#### Design

- The reference node's adjacency keeps, per day shard, whether it holds an edge of the node (the owner index
  already maps IDs to shards); the untyped read lists from that.

#### Test

`TestUntypedAdjacencyOpensOnlyItsShards` in `internal/core`: a reference node with edges on 2 of 20 day shards;
asserts 2 shard opens under a meter.

#### Open points

- None.

### RHO-25 The reads a write door makes run under a meter

- area: RHO
- kind: feature
- provides: MEM
- state: parked: waits for a design after RHO-4
- release: later
- owner: -
- depends: RHO-4
- blocks: -
- links: -

#### Goal

A write reads before it writes: it probes for the existing entity and reads endpoint rows. Those reads are charged
to no meter, so a cut's writes can use memory the cut's account does not see.

#### Current state

- The write doors take no meter. Which probes they make was listed only in the evidence folder.

#### Design

- None yet. Its direction: the write session carries the cut's meter and its probes charge it as reads do.

#### Test

To be named with the design. Its first test, in ai-soc: a cut's write of 1,000 edges to existing endpoints under
a meter limit below the endpoint rows' bytes is refused by the meter.

#### Open points

- None.

### RHO-26 Six read costs from ai-soc's read-back inventory are measured, then fixed or dropped

- area: RHO
- kind: perf
- provides: PERF
- state: parked: waits for ai-soc's measuring run
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

An inventory of ai-soc's reads of 3 Nov named six places where rho does more work than the read needs. Three of
them were expected to matter under Sigma's load. None has been measured.

#### Current state

- Only their names are in the repository: a miss on a unique lookup, an owner lookup that is dropped, an edge
  row read, a row read in a cascade, a decode at the floor, and the database view taken per read. Their
  descriptions were in the evidence folder.
- Sigma's read path changed since (the session no longer reads its state back from rho), so the list may be
  mostly moot.

#### Design

- One profile of ai-soc's cycle on the current pair. Each of the six that shows above 1 % of the cycle's CPU or
  allocation becomes an item with its own key; the rest are dropped here.

#### Test

The profile's report.

#### Open points

- Without the descriptions the six cannot be worked individually; the profile replaces them.

### RHO-27 A relationship property read on an on-disk index without a valid window counts the entries it rejects

- area: RHO
- kind: fix
- provides: MEM
- state: parked: waits for a caller
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

`Rels().ByTypeAndPropertyCtx` on a relationship type whose property index is kept on disk, read without a valid
window, walks the index with no budget: entries it examines and rejects are not charged. No caller does this
today (Sigma's reads pass a window and are charged).

#### Current state

- The door is `internal/core/read_budget_lookup_doors.go:33`; on-disk relationship indexes are listed in
  `Config.RelPropertyIndexOnDiskTypes`. The unbudgeted walk was not located here.

#### Design

- The walk takes the caller's tally and charges each examined entry before it is tested (RHO-20's mechanism does
  this for every door; if RHO-20 lands first this item is deleted).

#### Test

`TestRelPropertyDiskWalkCountsRejectedEntries` in `internal/core`: a type on disk with 1,000 entries of which 10
match, read without a window under a candidate limit of 100; asserts a refusal, against an unlimited control.

#### Open points

- None.

### RHO-29 History reads take less than 10 % of allocated bytes on ai-soc's workload

- area: RHO
- kind: perf
- provides: PERF,MEM
- state: parked: after the memory and profiling work on the small production-sized example
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Reads and writes once rebuilt a key's whole history each time. Most of that is released (v4.103.0 to v4.105.2).
What is left is not known to matter: the item ends when a profile of ai-soc's workload shows history reads below
10 % of allocated bytes.

#### Current state

- Left of the design, by name only (their text was in the evidence folder): a second cache option, misses on a
  first read, a pre-flight, and one further option.

#### Design

- A profile of ai-soc's cycle on the current pair. Below 10 %: delete this item. Above: the largest allocator
  under the history read is fixed, and the profile is taken again.

#### Test

The profile; then a part test at the allocator found.

#### Open points

- None.

### RHO-30 `HasHistory` answers for a batch of IDs in one scan

- area: RHO
- kind: perf
- provides: PERF
- state: designed
- release: 5.20
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A caller that asks `HasHistory` for many IDs pays one probe each. One scan over the batch would cost less at a
restore, which was where Sigma asked many at once.

#### Current state

- `Nodes().HasHistory(id)` and `HasHistoryCtx(ctx, id)` take one ID (`nodes/api.go`, `internal/core/
  history_presence.go`); the same for relationships. A cold shard answers by a per-ID key probe.
- Sigma calls `HasHistory` for the entities it adopts and reads by ID. Whether its restore still asks in bulk
  after its state left rho was not read.

#### Design

- First count Sigma's calls per restore on the current pair. Under 1,000: delete this item.
- Otherwise `HasHistoryManyCtx(ctx, ids)`: IDs sorted, one forward scan per shard over the history-presence
  keys, charged per key examined.

#### Test

In Sigma first: a restore of a session with 10,000 held entities; asserts rho's iterator seeks at most the number
of shards touched. In rho `TestHasHistoryManyEqualsSingle`.

#### Open points

- None.

### RHO-31 A transaction-time walk by IDs merges across shards

- area: RHO
- kind: feature
- provides: PERF
- state: building
- release: 5.20
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Sigma's state restore and as-of cuts read many entities as of one transaction-time pin. On the tiered store that
should be one ordered walk per shard merged across shards, so the cost does not grow with how often the entities
changed.

#### Current state

- Built: replacing a whole version row (v4.100.0, `internal/core/node_replace_version.go`) and the as-of walk
  capability (`store.NodeAsOfWalkCapability`, `internal/core/txtime.go`); as-of reads under a budget use admitted
  owner walks (v4.107.3).
- Not found: a public door that takes a list of IDs and a pin. The two further parts this item once named (a
  lookup by depth and a report) have no description in the repository.

#### Design

- `Temporal().NodesAsOfByIDsCtx(ctx, ids, pin)`: per shard the IDs it owns in key order, one walk each, results
  merged in the caller's order, every step charged.
- Only if Sigma's restore still reads so (its state is no longer in rho); else delete.

#### Test

In Sigma first: an as-of cut over 1,000 entities with 1 and with 50 versions each; asserts equal decoded entities
on rho's native counter. In rho `TestNodesAsOfByIDsEqualsSingleReads` on badger and tiered.

#### Open points

- Whether any caller is left is the first thing to read.

### RHO-32 A page of the as-of door with a valid window does not read one row per member of a cold shard

- area: RHO
- kind: perf
- provides: PERF
- state: parked: waits for the maintainer's choice
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A consumer's as-of page with a `valid=` window was slow on a production-sized store; most of the time was one
point read of a row per member of a cold shard, to learn each member's validity.

#### Current state

- Measured on v4.98.4; not measured since. A cold shard keeps no relationship temporal index (it is dropped at
  open, `shardOpenCfg`), which is why each member is read.

#### Design

- Option (b), the default while the maintainer has not answered (Q-25): when a read checks a cold shard out, it loads
  the
  relationship type's validity envelope for the shard once, so members outside the window are rejected without a
  row read. About two days.
- Option (a): RHO-5's next step, which answers validity from keys on disk for every shard.

#### Test

In ai-soc first: the as-of page over the test copy with a window that keeps 1 % of a cold shard's members;
asserts decoded rows at most twice the members kept. In rho a part test at the envelope.

#### Open points

- Q-25.

### RHO-1003 A candidate door bounded by valid start, if the backfill measurement shows the need

- area: RHO
- kind: feature
- provides: PERF
- state: idea
- release: later
- owner: -
- depends: RHO-13
- blocks: -
- links: -

#### Goal

A backfill wants the rows of one key whose validity starts inside a range. If the property door returns every
row of the key and the caller filters, rows read over rows kept is large on a busy key.

#### Current state

- No door bounded by valid start exists. The property doors take a valid interval that selects by overlap.

#### Design

- Only if RHO-13 reports rows examined over rows kept above 10: a door that seeks the property index at (value,
  start) and stops at the range's end, on the same admitted walk as the property door.

#### Test

In ai-soc first: the backfill of RHO-13's busy key; asserts rows examined at most twice the rows kept. In rho
`TestStartBoundedCandidatesEqualFilteredDoor`.

#### Open points

- None.

### RHO-1004 A pinned timeline stream lends the current relationship instead of copying it

- area: RHO
- kind: perf
- provides: PERF
- state: idea
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A consumer's benchmark of a three-hop chain at a pin allocates far more per run than without the pin, in rho's
read of the current relationship during a timeline stream: each relationship is copied for the caller.

#### Current state

- The allocation was attributed to an internal helper named `currentRel` in an earlier layout; no function of
  that name exists now, and the benchmark was not rerun on v4.107.17.

#### Design

- Rerun Sigma's benchmark with an allocation profile first. If the copy is still there: the stream lends the
  decoded relationship for the length of the callback (as the lend doors do), and copies only when the callback
  keeps it.

#### Test

Sigma's benchmark row: allocations per run at the pin within 1.1 of the unpinned run.

#### Open points

- None.

### RHO-1006 The composite, temporal and high-frequency index DDL take the caller's context and meter

- area: RHO
- kind: fix
- provides: MEM
- state: idea
- release: 5.18
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Creating an index over existing data reads that data. The property index has a door that takes a context, so its
build is charged to the caller's read meter and can be cancelled. The composite, temporal and high-frequency index
doors have none, so Sigma's session builds its composite index on no meter, outside the one memory account.

#### Current state

- `index/api.go` has `CreatePropertyWithOptionsCtx`; `CreateComposite`, `CreateTemporal`, `CreateRelTemporal` and
  `CreateHighFrequency` take no context.
- Sigma creates the composite index of its session pages through `CreateComposite`, without a context, right
  beside a `CreatePropertyWithOptionsCtx` call that has one; its Cypher DDL calls `CreateComposite` and
  `CreateTemporal` the same way.

#### Design

- `CreateCompositeCtx`, `CreateTemporalCtx`, `CreateRelTemporalCtx` and `CreateHighFrequencyCtx`: each checks the
  context, refuses a budget it cannot count (`budgetSupported`), and charges its backfill walk to the context's
  meter before each step, as the property door does. The doors without a context call them with an unlimited
  tally; there is one build path per index kind.
- Sigma's three calls pass their context in the same round.

#### Test

First, in Sigma on ai-soc's path: a session opened over a store with existing page nodes under a meter with a
candidate limit below their number; asserts the open is refused by the meter, against an unlimited control. In rho
`TestIndexDDLChargesItsBackfill` in `internal/core`: each of the four doors over 1,000 existing entities under a
limit of 100 candidates is refused before a native iterator counter passes 100.

#### Open points

- If the consumer stops creating its page index, its Cypher DDL calls are the only callers left and the item's
  first test moves there.
