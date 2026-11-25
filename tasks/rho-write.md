# rho-write

the write path: bytes and CPU per committed line, write rate, purges and retention, durability defects.

Items of this topic; the head of [index.md](index.md) says how to use them. Fields: [index.md](index.md#fields).
Code facts are read at origin/main f837590b (v4.107.17); paths without a prefix are under `pkg/graph/`..

### RHO-33 rho's bytes and commit cost per committed line are measured over a whole run

- area: RHO
- kind: measure
- provides: PERF
- state: designed
- release: 5.18
- owner: -
- depends: the release runs on the test host are finished
- blocks: RHO-10; a consumer's disk sizing
- links: -

#### Goal

Disk for raw events is rate times retention times bytes per line. Bytes per committed line has never been
measured to the end of a run; the only figures are a synthetic estimate and an upper bound from a run that was
stopped. The real figure sizes every installation's disk and decides how long history can be kept; the commit
cost per line says what share of a cut's time is rho's.

#### Current state

- One partial run on a 12-day production-sized dataset was stopped before its end. The number of committed lines
  was not read and the run log did not close, so neither figure per committed line exists.
- The run's driver is a consumer's script that is in no repository; ai-soc serves under a fixed memory bound and
  rows are pushed in time order.

#### Design

- One whole run (twice at most) of the same set on the released pair, to the end, with the run log closed.
- Reported: committed lines; store bytes per committed line split into events, reference and index; commit
  seconds per line from the run log's commit phase; `MemoryStats` at the end.
- The split by key kind is RHO-10's and is taken from this run's store.

#### Test

The run is the test; it is complete when the run log closes and the committed count equals the rows pushed minus
the rows the reader refused, which the report states.

#### Open points

- A run on the test host needs the maintainer's word and a free machine; release runs go first.
- The script should be in ai-soc's repository with the run's other drivers; that is ai-soc's change.

### RHO-34 A hot shard takes more than one writer's rate

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

The standing goal is ingest at about 2 TB a day (AGENTS.md). One writer through the durable commit path takes
16,000 to 38,000 facts a second in rho's own write-rate benchmark; the goal needs 46,000 a second sustained and
several times that at peaks. Today the consumers' own rate is lower than one writer's, so rho is not the limit.

#### Current state

- A hot shard is written through one serial flush path. In a consumer's profile of the writer about a quarter of
  the time is fsync; the rest is badger's memtable and value log and goroutine hand-offs.
- ai-soc has a writer-ceiling test over its durable commit path; rho's benchmark is `make bench`.
- No design exists. ai-soc's direction for scale-out is several processes, each with its own graph; a
  partitioned writer inside one process is not planned.

#### Design

- None yet. The two levers to price before any design: group commit across cuts (one fsync for several commits),
  and badger's write hand-off (batches handed to the write loop without a channel wake-up per batch).
- Nothing is built while Sigma's rate stays below one writer's.

#### Test

The consumer's writer-ceiling test is the first test: lines per second through the durable commit path, before
and after; then a part benchmark at the flush path.

#### Open points

- Whether this item is needed at all if scale-out is several processes: the maintainer's word.

### RHO-35 Raw rows older than the installation's retention are purged, so rho's disk stays flat

- area: RHO
- kind: feature
- provides: MEM,PROD
- state: idea
- release: 5.13.1
- owner: -
- depends: ai-soc's retention setting and its purge job
- blocks: -
- links: -

#### Goal

rho keeps one day shard per day of valid time for ever. Nothing in ai-soc calls a purge, so the disk of an
installation grows without end. The owner's decision: one retention setting per installation, default 30 days,
taken by both Sigma's state and rho's raw rows.

#### Current state

- rho has the purge doors: `Admin().PurgeExpiredRels(ctx, RelPurgePolicy{Type, Before})` and
  `PurgeExpiredNodes` (`admin/api.go:265,280`, `internal/core/retention_purge*.go`). They need
  `Config.AllowRetentionPurge`, set the type's retention watermark first (a read pinned below it fails closed
  with `ErrRetentionExpired`), then remove rows in chunks of 256, resumable. On a read-only store they return
  `tiered.ErrReadOnly`.
- On the tiered store the purge walks rows (`PurgeRelsByTypeBefore`, `store/tiered/rel_retention_purge.go`); a
  whole day shard behind the line is not dropped as a unit.
- ai-soc sets `AllowRetentionPurge` and has a retention setting; it calls no purge.
- Event relationships are written once, so their version chains are one version long; the bytes to free are the
  day shards themselves.

#### Design

- ai-soc runs one purge job between cuts with the line `now - retention`: for each event type it calls
  `PurgeExpiredRels`, for each event label `PurgeExpiredNodes`. Sigma's state drops behind the same line in the
  same step, so no session reads a row rho has purged.
- In rho the tiered purge drops a day shard whole when its ID span ends before the line and it holds no row of
  a type or label outside the purge: the watermark first, then the catalog entry removed and saved, then the
  directory deleted; a crash between the two is finished at the next open. Other shards are purged by rows as
  today.
- Reference nodes are not purged by age.
- The job charges its opens and its rows to a meter of its own and takes a bounded number of shards per run.

#### Test

First, in ai-soc: a store with 40 days of test rows under a retention of 30 days; after the job the store's
bytes on disk are within 1.1 of a store fed only the last 30 days, a read pinned below the line returns
`ErrRetentionExpired`, and the session's answers inside the window equal the answers before the purge. In rho:
`TestPurgeDropsAWholeDayShard` in `store/tiered` with a kill between catalog save and directory removal and a
reopen, and the purge refused with `ErrReadOnly` on a read-only store.

#### Open points

- Dropping a shard whole is the stated aim; the condition "holds no other type or label" is this rewrite's
  proposal and needs the design review.
- The bytes freed per day are not measured (RHO-33 gives bytes per line).

### RHO-36 The flush pass over every open writable shard has a bounded cost

- area: RHO
- kind: measure
- provides: PERF
- state: idea
- release: 5.20
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

The tiered store commits the reference shard last, so a kill cannot leave a cut's record without its rows. For
that, every reference flush that carries rows first runs a flush pass over each open writable shard. Nothing
bounds what that costs as the number of open shards grows.

#### Current state

- The order is released (v4.107.9). No test measures the pass against the number of open shards.

#### Design

- A cost test: reference flushes with rows at 2, 8 and 32 open writable shards of which one has pending rows.
  The pass must skip a shard with nothing pending at the cost of a check, not of a badger flush.
- If it does not, the pass keeps a set of shards with pending rows and visits only those.

#### Test

`TestReferenceFlushCostFlatInOpenShards` in `store/tiered`: the three sizes above; asserts badger flush calls equal
to the shards with pending rows plus one, by a counter at the flush hook.

#### Open points

- None.

### RHO-37 badger's Clear invalidates every cached relationship column

- area: RHO
- kind: fix
- provides: CORRECT
- state: parked: not on ai-soc's path, not reproduced
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

After `Clear` a store is empty. A column snapshot of a relationship type built before the Clear must not be served
as fresh afterwards, or a read returns rows of the cleared store.

#### Current state

- `Store.Clear` (`store/badger/badgerstore.go:2798`) advances `nodeEpoch`, `relEpoch`, the shared epochs and the
  history epochs and drops the document columns. It does not advance `relEpochCoarse` and does not drop
  `relColumns`; the memory store's Clear does drop `relColumns` (`store/memory/memorystore.go:377`).
- Whether a stale column can actually be read was not reproduced.

#### Design

- Clear advances `relEpochCoarse` and every relationship-type stripe and sets `relColumns` to nil, as the memory
  store does.

#### Test

`TestClearInvalidatesRelColumns` in `store/badger`: build a relationship type's column, Clear, write one
relationship of the type, read the column; asserts exactly the new relationship.

#### Open points

- ai-soc does not call Clear in production.

### RHO-38 A catalog rollback restores only what its own operation changed

- area: RHO
- kind: fix
- provides: CORRECT
- state: parked: waits for a failing test or a consumer's request
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

When a shard-catalog operation fails, it puts back a snapshot of the catalog taken at its start. If another lock
holder changed or added an entry in between, the rollback overwrites the change and drops the addition.

#### Current state

- `ShardCatalog.restoreShards(snapshot)` (`store/tiered/shard_catalog.go:447`) replaces the entries with the
  snapshot. Whether two holders can interleave there was not traced.

#### Design

- The rollback undoes the entries its operation touched (by name), not the whole list; or the operation holds
  the catalog lock from snapshot to rollback, if the trace shows that is already so, and then this item is
  deleted.

#### Test

`TestCatalogRollbackKeepsOthersChanges` in `store/tiered`: operation A snapshots, operation B adds a shard and
commits, A fails and rolls back; asserts B's shard is in the catalog.

#### Open points

- None.

### RHO-39 A second `WaitApplied` on a failed asynchronous token returns the failure

- area: RHO
- kind: fix
- provides: CORRECT
- state: parked: no consumer uses asynchronous sessions
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

`WaitApplied(token)` returns the apply error of an asynchronous submit. The failure record is removed by the first
call, so a second call for the same token returns nil: the caller is told a rejected write succeeded.

#### Current state

- `IngestOps.WaitApplied` (`internal/core/ingest.go:691`); its comment states the behaviour ("pruned on read: a
  second WaitApplied for the same token returns nil"). ai-soc submits synchronously.

#### Design

- The failure record stays until the token is below the lane's pruning horizon (a bounded number of tokens), not
  until its first read; the cap on retained failures stays.

#### Test

`TestWaitAppliedTwiceReturnsTheFailureTwice` in `internal/core`: an asynchronous submit that violates a unique
constraint; asserts the same error from two calls.

#### Open points

- None.

### RHO-40 A node property that is an append-only list is appended without rewriting the list

- area: RHO
- kind: feature
- provides: PERF
- state: parked: needs ai-soc's specification and the maintainer's word
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Appending one element to a list property through `Nodes().Update` writes the whole list twice (the new version and
the history row). A caller that only ever appends pays bytes and CPU in proportion to the list on every append.

#### Current state

- No append door exists. Estimated at 5 to 8 days.

#### Design

- None until ai-soc states what it needs (which lists, how they are read, whether history per append is
  required). Direction: a list stored as a header and segments, an append writing one segment.

#### Test

To be named with the specification; first in ai-soc on the list it names.

#### Open points

- Whether any caller still appends lists since the session's state left rho.

### RHO-41 The transaction-time seek index's cost on the write path is shown by a profile or left

- area: RHO
- kind: perf
- provides: PERF
- state: parked: waits for a write-path profile of ai-soc's that shows it
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

Since v4.107.3 every history append also writes a seek index for as-of reads: 52 bytes per indexed history row and
51 per entity proof on disk, and on rho's 512-byte in-memory append fixture an append went from 2.0 to 7.9 µs per
node and from 2.5 to 6.3 µs per relationship (CHANGELOG 4.107.3). On a consumer's write path that may or may not
be visible.

#### Current state

- The index is built on every badger history append. No profile of ai-soc's writer names it.

#### Design

- From a CPU profile of the consumer's writer-ceiling test: the share of the seek index's writes. Under 5 % of
  the writer's CPU: delete this item. Above: the index entry is written with the history row in one encode, or
  built lazily for entities that are read as of a pin.

#### Test

The profile; then a part benchmark of the history append.

#### Open points

- None.

### RHO-42 The value-log threshold for large values on the tiered store is measured on a current run

- area: RHO
- kind: measure
- provides: PERF
- state: unknown
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

A threshold of 32 KiB was expected to take 64 KiB page values out of badger's compaction, which was a large part
of the extra CPU in one consumer profile, at the price of an uncompressed value log. The values in question were
Sigma's derived pages.

#### Current state

- The threshold is configurable (released in v4.104.0). The measurement never ran.
- Sigma's session no longer writes its derived rows to rho as pages; whether any value near 64 KiB is still
  written on ai-soc's path was not read.

#### Design

- From a current ai-soc run: the histogram of value sizes written to rho. No value above 32 KiB: delete this
  item. Otherwise the two-day profile with the threshold at 32 KiB against the default.

#### Test

The run's report: compaction CPU seconds and store bytes at both settings.

#### Open points

- None.

### RHO-43 Compaction settings per tier follow a measured write amplification

- area: RHO
- kind: measure
- provides: PERF
- state: parked: waits for a quiet machine and an ai-soc run on the tiered store
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

How many bytes badger's compaction rewrites per byte flushed depends on `BaseLevelSize`, `BaseTableSize` and
`MemTableSize`. They are the same for every tier; a hot shard written in cut-sized bursts and a warm shard that is
rarely written want different values. Nobody has measured the amplification.

#### Current state

- No per-tier setting for the level sizes; `WarmMemTableSize` exists for the memtable.

#### Design

- Measure compaction input bytes over flushed bytes per shard on an ai-soc run of two days; then choose the three
  values per tier and measure again.

#### Test

The run's report before and after; store bytes and commit seconds per line must not rise.

#### Open points

- None.

### RHO-44 Sigma's page purge reaches rho's purge by ID on ai-soc's path

- area: RHO
- kind: feature
- provides: MEM
- state: unknown
- release: later
- owner: -
- depends: Sigma's purge of its dead units
- blocks: -
- links: -

#### Goal

`Nodes().PurgeByIDsCtx` was built so Sigma can remove the nodes of pages it no longer reads. By the system's rule
it counts as done only when ai-soc's production path calls it.

#### Current state

- `PurgeByIDsCtx(ctx, ids)` (`nodes/api.go:1480`, `internal/core/retention_purge.go:286`) removes up to 256 IDs a
  call with their history, metered before any shard open.
- Sigma calls it from one place, its page purge. Since Sigma's derived rows left rho that purge finds only pages
  an earlier version wrote; it needs units of its own to purge before the call does work again.

#### Design

- No rho change. The item ends when Sigma's purge of its dead units runs on ai-soc's path, or is deleted if Sigma
  stops writing units to rho altogether.

#### Test

Sigma's test that a reopened session leaves no dead unit in its namespace, run through ai-soc's graph manager.

#### Open points

- None.

### RHO-45 Whether a backfill with valid times before 2026-01-01 is accepted

- area: RHO
- kind: decision
- provides: PROD
- state: parked: default stands (refused)
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

rho's IDs carry the valid start in their time part, which begins on 2026-01-01. A consumer that loads older
events gets every such row refused. It is a product question: does an installation need history from before
2026?

#### Current state

- `ErrValidFromOutsideIDRange` (`store/valid_time_mint.go:13`): "ValidFrom outside the ID time range 2026-01-01
  to 2034-12-02 cannot be minted".

#### Design

- Default while the maintainer has not answered (Q-25): stays refused. Accepting older times means a second ID layout
  or an
  earlier epoch, which is a major version's change.

#### Test

None; a decision.

#### Open points

- Q-25.

### RHO-1005 A purge by ID that reaches an edge on another shard charges that open and can be resumed

- area: RHO
- kind: fix
- provides: CORRECT,MEM
- state: parked: waits for a caller whose purged nodes have such edges
- release: later
- owner: -
- depends: -
- blocks: -
- links: -

#### Goal

On the tiered store, purging a node by ID also removes its edges. When an edge's other end is on another shard,
that shard is opened after the purge without being charged, and if the purge stops between the two shards the
edge's other half stays for good: a rerun skips the node because it is already gone.

#### Current state

- `Store.PurgeNodesByIDs(ids, opens)` (`store/tiered/retention_purge.go:143`). The nodes Sigma purges carry no
  edges, so the case is not reached today.

#### Design

- The purge lists the other shards from the node's adjacency before it removes anything, charges their opens
  with the first, and records the pending edge halves durably in the node's shard so a rerun finishes them.

#### Test

`TestPurgeByIDsFinishesACrossShardEdgeAfterAStop` in `store/tiered`: a node with an edge to a node on another day
shard; the purge is stopped after the first shard and run again; asserts no adjacency entry of the edge is left on
either shard, and that a shard-open limit of 1 refuses the purge before it removes anything.

#### Open points

- None.
