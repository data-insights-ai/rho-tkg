# Review: the v5 architecture proposal against the v4 code (2026-10-09)

Inputs: `~/Downloads/PLAN.md`, `RESEARCH-REVIEW.md`, `DISCUSSION.md` (all dated 2026-10-09); rho-tkg main
`32568c4` (29 commits past v4.43.0); consumers sigma-tkgd (v4.43.0), ai-soc engine (v4.41.0), agent-bookkeeping
(v4.40.0); the ai-soc consumer requests received 2026-10-09 (eight items). Method: five read-only code surveys
(temporal model, storage, transactions/consistency, access API/indexes, consumer inventory), each cross-checked at
the cited lines; two probe tests run in a scratch copy (memory and badger), nothing written into the repo.

## 1. Verdict

The plan's **direction is right and is backed by v4's own scar tissue**: every semantic rule it insists on
(corrections are new revisions, a receipt is not a snapshot, the change log must be a logical transaction log,
rounds not wall clocks, points are not one-tick spans) corresponds to a v4 lesson or an open v4 bug
(lessons 43, 46, 55, 60, 62, 71, 73; backlog "temporal semantics review"). The plan is also honest where it must
be: §5.3 calls its cut protocol unproven, §8 puts the distributed correctness slice (V2) before format freeze.

Four things limit it:

1. **The plan cites material that does not exist in this repository or on this machine.** No `v5` branch, no
   `docs/v5/PLAN.md`, no `reference/README.md`, no `design_checks.py`, no `PLAN-2026-09-24.md`, no "original 16/52
   fixtures" (searched the repo, all branches, `~/Work/2026/datainsights` to depth 3, `/` to depth 6). The backlog
   (`tasks/backlog.md:31`) and the plan both point at them. V0 cannot start until they are checked in.
2. **The distributed half (§5) has no demonstrated consumer today and is the highest-risk component.** ai-soc embeds
   rho in one process (badger on disk); sigma uses rho's single-primary change-log replication for read replicas
   (`sigma-tkgd/internal/server/cluster/replica_watcher.go`), not partitions. The plan's buy-vs-build spike is the
   right gate, but it lacks a stated fallback: "embedded single-partition v5 with the same value/access contract is
   shippable" should be an explicit exit of V2, or V2 becomes a multi-month blocker for the temporal fixes
   consumers need now.
3. **The consumers' urgent needs are v4 needs.** René deleted the "v4 gets fixes only" rule on 2026-10-09: v4
   keeps taking consumer features; v5 re-implements them against its own contracts. The plan should therefore
   treat v4's door set at v5 start as the compatibility baseline (see §4a).
4. **Migration cost is understated.** sigma-tkgd imports 14 rho packages from 258 non-test files (`pkg/types` 442
   sites, `pkg/graph` 414); its adapter contract lists 20 capability groups. V6 "consumer migration contract" is one
   row. Without keeping `pkg/types` entity shapes or shipping an adapter, v5 adoption by sigma is a rewrite of its
   two source adapters and tests.

## 2. Alignment table (plan element × v4 status × evidence)

| Plan element | v4 status | Evidence |
|---|---|---|
| Typed temporal values: domain/axis/codec, bounds flags, rationals, tuples | **absent** | `Instant` is int64 Unix ms (`pkg/types/temporal.go:5-7`); one type for validity, tx time, audit; six opaque ISO `TemporalValue` kinds, no comparison (`temporal_value.go:24-58`) |
| Half-open int64 spans | have | predicate `from<=t && (to==0 \|\| to>t)` (`internal/storeutil/temporal_filter.go:82-92`) |
| Point scope | **absent / conflicts** | a point is only a query instant; a stored `[t,t+1)` is read as the cascade-eclipse sentinel and skipped (`core/temporal_cascade.go:63-77`; 10 reader sites) |
| Zero is an ordinary coordinate | **conflicts** | `ValidFrom=0` = mint time, `ValidTo=0` = open, `QueryOpts.ValidAt=0` = no filter, `Relate` rejects 0 (`temporal_filter.go:13-18,57-72`; `allen.go:163-166`); `MaxInt64` as +inf (`allen.go:183-196`) |
| Corrections are appended revisions; old cuts reproduce | have (since 994df82) | cascade appends `TxFrom=now` rows, existing rows untouched (`temporal_cascade.go:152-157,204,518`); but Delete still stamps the final row in place (`core/temporal.go:132-141`), rollback rewrites history (`tx.go:840-878`) |
| State replacement of exactly a region (E02) | have, with holes | `SetNodeVersionInterval` verified at an old cut; fails for width-1 pieces and coordinate 0 |
| Life-bound edges masked by endpoint lives, reopen does not rebind (E19) | partial / conflicts | effective view masks by endpoint validity (`temporal_queries.go:745-752`); no LifeID, node delete cascades tombstones into rels (`temporal.go:69-75`) |
| Knowledge / uncertainty, calendars, recurrence as descriptors | absent | recurrence expands to rows with a 1000-year cap (`recurrence.go:54,126`) |
| Allen as a pure value predicate, meets ≠ overlap | have | `allen.go:203-235`, composition tables; carry forward |
| Typed projected batches | partial | column scans / DocValues at 4,096 rows (`store/column_batch_build.go:11`); every row door returns whole frozen entities; no byte budget |
| Snapshot handle (OpenSnapshot) | absent | consistent multi-read needs a `GraphTx` holding the exclusive lock (`core/queries.go:35-40`) |
| Declared capabilities, declined predicate as a residual | partial, implicit | `ok`/`exact`/`ErrCapabilityNotSupported`; "over-selects, re-check" is prose (`queries.go:231-240`) |
| Interval access structure with sound bound pruning | partial | RAM max-end augmented tree (`internal/index/temporal_index.go:34-61,326-381`), sound for overlap, correctly not applied to Allen doors; not paged; HF index prunes on start only (`hf_index.go:11-21`) |
| Separate current-state accelerator (10 active vs 100k ended) | absent | time-filtered adjacency folds every deleted rel in the graph (`adjacency_version.go:24-27`, `temporal.go:752-769`) |
| Replicated log authoritative; one durability boundary | absent / conflicts | Badger WAL is the system of record; segments have their own manifest; the change log is a physical redo log beside the rows (`store/changefeed.go:134-138`) |
| MVCC memtable + typed column segments + manifest | partial | segments exist for declared rel types on the memory store only (`store/rel_segments.go:23-27`); nodes never in segments; one type per segment |
| No resident Go object per sealed fact | partial | true for sealed rows (0.3-0.7 B/HOP heap, S3); false for nodes, memtable, badger RAM maps (`badgerstore.go:434-454`), rebuilt indexes |
| Bounded replay at open | absent | segments fully verified and scanned at open (~0.46 M rows/s); badger decodes every row (`badgerstore.go:1164`) |
| Paged catalog/indexes | absent | Go maps; manifest is one JSON blob rewritten per commit (`segdir.go:402-422`) |
| Integrity roots bind TxID and profile version | absent | roots are over content hashes that exclude Tx/Valid time (`integrity.go:205-213`; `writer.go:353-368`) |
| Logical rounds, certified cuts | absent | wall-clock ms + per-Core monotonic floor (`context.go:31-58`), floor persisted only at Close (`instant_floor.go`); `NowTx`/`Watermark` are receipts, not cuts |
| 2PC, coordinator record, fencing | absent / hint | sharded `CommitLogScope` commits shards sequentially (`sharded/changelog.go:283-300`); lease is "a HINT, not a consensus primitive", last-writer-wins (`store/replication_source.go:31-43`) |
| Transaction envelopes, before/after images, gap-free handoff | partial | per-mutation `{LSN, Tag, Payload}`, after-image only, no TxID, gaps tolerated (`changefeed.go:16-19,134-153`); contract doc at `:154-158` is stale (rolled-back tx burns no LSN since `tx.go:878-885`) |
| Opaque IDs from durable blocks, no clock | absent / conflicts | snowflake 48-bit µs time + node + seq; horizon 2034-12-02; placement encoded in the ID (tiered by time window, sharded by node field) |
| Serializable read/write transactions | absent | GraphTx is write-through with compensating rollback: dirty reads, lost update on rollback (`docs/architecture.md:268-270`) |

## 3. What the plan gets right that v4 proves

- **Corrections as new revisions.** Lesson 46 (in-place cascade corrupted tx time) and lesson 60 (in-place delete
  stamps un-applied at every door) are exactly §2.2 and invariant 5.
- **A receipt is not a snapshot (E16).** `AllowTxBackfill` lets a write land below an existing pin; `CommittedTx`
  excludes backfills by its own doc (`txtime.go:192-196`); across machines `AdvanceClock` HLC merge gives "max
  observed stamp". Lesson 71 shows the wall-dominated floor is not reopen-safe.
- **Logical transaction log vs physical redo log.** Lesson 55: the feed ships store mutations, not tx groups; a
  replica transiently materializes uncommitted state. §3.3 is the fix.
- **Two doors, same shape (rule 17).** The plan's "named/generic/row/column parity" gate (V4) is the generalization
  of the rule v4 keeps rediscovering (lessons 58, 60, 62, 73). The survey found a live instance: column scans and
  `ForEachByLabelPropertyRange` apply temporal opts to current rows only and ignore `TxAt`/`TxPin`
  (`memory/memorystore_query.go:105-125`, `badger_column_scan.go:149-159`, `temporal_filter.go:70-72`); sigma
  passes empty opts so is not hit.
- **Accelerator equals the independent model (inv. 13).** Lesson 73's env-gated divergence oracle is the pattern
  the plan's V3/V4 gates need as CI, not fixtures.

## 4. Where the plan is thin or wrong

| Topic | Issue | Suggested change |
|---|---|---|
| §8 "existing v5 worktree based on v4.37.2" | not present on this machine or in `origin` | V0 first task: check in the reference material (September draft, 16/52 fixtures, `design_checks.py`) under `docs/v5/reference/` on a `v5` branch from `4126bb1`, or drop the references |
| §8 performance gates "22.8 M-row day when available" | ai-soc reports one BO day 58.8 M records, one BA day 36.9 M rows, 400-700 rows/s with bursts | update the capacity inventory at V0 from ai-soc's numbers; the 1 TB/day target stays a target |
| §5 distribution as a v5 requirement | no consumer runs partitions; sigma replicates, ai-soc embeds | keep V2 as the go/no-go, add the explicit fallback "single-partition embedded v5 ships with the same contracts" |
| §9 migration: one-tick rows | correctly says a v4 `[t,t+1)` row cannot be classified at import | add: no writer has produced the sentinel since 2026-06-12 (994df82); a store whose history starts after that date imports `[t,t+1)` as a genuine span |
| §9 migration: TxFrom | correctly says tx boundaries are unrecoverable from TxFrom | add the reasons found: one tx spans N instants, batch creates share T1/T2, ingest applier merges groups, `validInstantAfter` bumps stamps, TxFrom not in the integrity hash |
| §3.2 declared vs effective views | relationship scan/during/relating doors are "declared" (not endpoint-masked); Snapshot/NeighborsAt/`OutgoingRelsAt` are "effective" | the typed selector must name which view it means; today the two coexist by door |
| §3.2 `TxAt` without valid time | v4 adds an implicit valid-at-now (`store/opts.go:48-61`, `temporal.go:1119-1123`) | the plan's "current never substitutes for history" needs this listed as a must-not-copy |
| §4.1 carry-forward of segments | S4 compaction, S6, S7 unbuilt; point lookups O(segments); dictionaries copied to heap per segment | V3 inherits L0-only segments; budget the merge as V3 work, not a carry-over |
| §6 E19 | v4 has the masking view but node delete cascades tombstones into rels and there is no LifeID | fine as a v5 target; note that the v4 "effective view" cannot be the reference oracle for E19 |
| RESEARCH-REVIEW alternatives | "external transactional KV" compared on taste-free criteria: good | add Pebble vs Badger catalog measurement must remove badger's per-row RAM maps first, else the comparison measures v4's maps |
| Precision | plan never states the default axis unit; ai-soc source times carry 100 ns, v4 is ms | V0: declare the default axis unit per profile and the importer mapping ms → unit |

## 4a. Making v5 easy for sigma-tkgd

sigma already isolates rho behind one contract: `internal/cypher/core/access/contract.go` and
`contract_optional.go` (Catalog, Scan, Lookup, EqualityIndex, OrderedIndex, Adjacency, Statistics, Snapshot,
Columns, Epoch, Lend, Ordinals, Degrees, CountsAt, HistoryCounter, CommittedPin, RelColumns, EndpointOrdinals,
ScanOrder, IndexMaintenance), implemented by `internal/cypher/source/graphsource`; the Tyla adapter
(`internal/tyla/source/graph`) uses a handful of doors (`ScanRelSegments`, `ScanNodeColumns`, `ByLabel/ByType`,
`*ForNodesAtPin`, `Resolve()`). The 442 `pkg/types` sites are mostly `types.NodeID`, `types.RelID`,
`types.Instant`, `*types.Node`, `*types.Relationship`, `graph.QueryOpts`.

Three levers, cheapest first:

1. **Keep the names.** v5 keeps `types.NodeID`/`RelID` as opaque 64-bit IDs (the plan already wants that),
   `types.Instant` as the int64 millisecond codec of the default axis, and `Node`/`Relationship` accessor names.
   Most of sigma's 442 sites then compile with an import-path change only.
2. **Make sigma's access contract a v5 acceptance fixture.** The V4 phase gate "named/generic/row/column parity"
   becomes "graphsource's contract compiles and its tests pass against v5". That is concrete and already written.
3. **A `compat` façade over the v5 engine** for the v4 sub-API doors sigma and ai-soc call (`g.Nodes()`,
   `g.Rels()`, `g.Temporal()`, `g.Replication()` …), shipped with v5.0 and removed at v5.1. It cannot bridge
   semantics that v5 deliberately changes (one-tick spans, `TxAt` implicit valid-at-now, dirty-read `GraphTx`);
   those need a sigma change regardless and should be listed in V6.

Not recommended: evolving v4 into v5 in place. The ID, time-type and transaction changes break `pkg/types`
shapes; the plan's isolated reconciliation is right, the levers above make its edge thin.

## 5. Consumer requests (ai-soc, 2026-10-09) and their disposition

Dispositions below are the state when answered; René's decisions of the same day (3 yes, 4 withdrawn, 5 no,
7 group-instant no, 9 yes) are recorded in `tasks/backlog.md` items 9-11 and its DECIDED NO line, which win.

Answered to ai-soc-main-e7 in full; summary:

| # | Request | Finding | Disposition |
|---|---|---|---|
| 1 | `[t,t+1)` invisible to temporal doors | confirmed, wider (CloseVersion at vf+1 lost; delete at vf+1 hides row; width-1 correction invisible); sentinel has had no writer since 2026-06-12 | take as v4 HIGH fix: remove the eclipse skip, red tests first; v5 gets a Point scope |
| 2 | property-only and earlier-vs corrections | Update door confirmed (`relationship_update.go:157,160-164,218-225`); `SetRelVersionInterval` already does it | already have; decline changing Update |
| 3 | tiered composite + rel temporal indexes | tiered declines; doc wrong about sharded (has both) | not taken for v4 by default; fix comment |
| 4 | rel unique / idempotent key | node-only; `AddByIDIfAbsent` dedups on (start,end,type) | partial already; decline property-keyed for v4 |
| 5 | strong IndexProvider | best-effort confirmed | decline; change feed alternative named |
| 6 | tag with `*WithTx` | on main, CHANGELOG Unreleased empty, gates open | take: v4.44.0 after gates |
| 7 | commit pin | cut record as last tx write is sound under single-writer; no door returns a group instant | confirmed with condition; group-instant door not taken |
| 8 | successor keeping vs, later ve | cascade is append-only since 994df82; header comment `temporal_cascade.go:18-27` is stale; ingest Session lacks the door | already have via GraphTx; take Session door + comment fix |
| 9 | all-or-nothing durable tx, or open-time discard | Commit/Execute never flush; pending buffer is a map flushed in map order, badger splits big WriteBatches; no tx journal. ai-soc's "rows above the last cut record" recovery is sound only under `SyncWrites` | v5 §5.2; v4 candidate for René: durable-on-return commit option (flush at Commit/Execute); open-time discard not taken |
| 10 | does `RelAsOf` show a cascade extension | no, on memory/badger/tiered (probed): the cascade never changes a closed current row; `RelAtTx(id, t, pin)`, `RelsDuringTx`, `ByType{ValidAt, TxAt}` do | answered; sigma-facing: `edb.go:180` needs a valid-time coordinate |

## 6. v4 findings that need a failing test now (independent of v5)

1. **Eclipse skip hides legitimate one-tick rows** (HIGH, silent wrong answer, two doors disagree) — see §5 #1.
2. **Column scans and ordered range scans ignore `TxAt`/`TxPin` and filter current rows only** (HIGH?, rule 17).
3. **Stale contracts**: `store/changefeed.go:154-158` (rolled-back tx in feed), `temporal_cascade.go:18-27`
   (in-place classification), `index/api.go:267-269` (sharded), `constraints/unique.go:74-75` (batch enforcement).
4. **`RelAsOf` / `Get` / `ByType{TxPin}` keep answering the old closed row after a finite cascade extension** (by
   design: the current row is the latest open-ended row, `temporal_cascade.go:50-55`). Not a bug, but a contract
   gap for consumers growing closed facts (ai-soc bursts, sigma `edb.go:180`); v5 E02 needs "state at a cut" as one
   read, not two doors.
5. **Instant floor persisted only at Close**: a crash after a burst that outran the wall reopens with
   `NowTx` below committed stamps (lesson 71's reopen case for the crash path). `persistInstantFloor` is called
   only from `Close` (`core.go:2029`; `instant_floor.go:120-129`). MEDIUM; needs a crash-shaped red test.

## 7. Carry-forward list for v5 (contracts match)

Code: `internal/segment` (codec, dicts, `Batch`, fail-closed versions), `internal/segdir` (lock, seal protocol,
mmap refcount), `store/column_batch_build.go`, `storeutil.SelectAsOf` retraction rule, canonical predicates
(`temporal_filter.go:37-108`), the cascade's split/patch region logic as an E02 reducer reference, `AllenRelationSet`
and composition, order-preserving int64 key bits, `CoerceInstant`, commit-time LSN minting, validate-all-then-write
plus `PartialBatchError`, `ErrTxOrder` per-entity precondition, entity/value lock ordering.

Tests: `bitemporaloracle*_test.go`, `temporal_two_doors*_test.go`, `snapshot_cross_door_test.go`,
`rel_segments_oracle_test.go`, `segments_dir_kill_test.go`, `segment/fuzz_test.go`, `bench/segment_baseline_test.go`
with `synthhop`.

Must not copy: eclipse sentinel, zero sentinels, `MaxInt64` infinity, positional tiling and the inheritance
heuristic, `validInstantAfter` +1, Update-door bound reset, in-place delete stamps, snowflake time as validity,
recurrence expansion into rows, write-through tx with compensating rollback, per-shard atomicity as tx atomicity,
wall-clock/HLC instants as cut tokens, clock- and placement-encoded IDs, lease-as-hint failover, event timestamps
that differ from commit stamps, Badger as system of record beside a second manifest.

## 8. Measured v4 baselines the V3 gate must beat (790 K / 3.15 M / 12.6 M synthday)

| Metric | v4 |
|---|---|
| Row store B/rel; P7 compact HOP | 718-723; 388-390 |
| Badger resident / lean / on disk B/rel | 1,066 / 301 / 257-259 |
| Segment codec on disk B/HOP | 22.1 / 23.4 / 24.8 |
| S3 heap beyond memtable | 0.3-0.7 B/HOP |
| Reopen | 3.3-3.5 s per 1.58 M HOP |
| 12.6 M write | 7.86 s |
| `ScanRelSegments` / `ScanRelColumns` | 7.3-8.5 / 2.3-2.7 M rows/s |
| Memory / badger `ByType` | 6-11 / 0.11-0.15 M rows/s |
| sigma HOP materialize, columns vs rows | 1.00 s, 428 B/fact vs 1.75 s, 490 B/fact |

## 9. Recommended next steps, in order

1. Check the plan's reference material into the repo (`v5` branch from `4126bb1`, `docs/v5/`), or remove the
   references; without it V0's exit evidence cannot be produced.
2. Decide the ai-soc feature items (3 tiered indexes, 4 rel uniqueness, 5 strong provider, 7 group instant) on
   their own merits now that v4 takes features; the eclipse fix, the `*WithTx` release and the ingest
   `Set*VersionInterval` door are taken.
3. Red tests for §6 items 1 and 2 (two-phase, all four backends, both doors), then fix.
4. Release v4.44.0 (CHANGELOG, gates) so sigma and ai-soc can pin the `*WithTx` doors.
5. V0: declare axis units and the ms → unit import mapping; update the capacity inventory from ai-soc's day sizes;
   add the V2 fallback sentence.
