# v5 analysis (origin/v5 fd37fa0 vs main 95a1de3 = v4.50.0), 2026-10-11

Read-only; merge test run in a throwaway worktree and aborted.

**Verdict**
1. Merge: origin/v5 is a strict descendant of main (merge-base = 95a1de3 = origin/main), so it fast-forwards: no conflicts today; root `go build ./... && go vet ./...` pass, v4 core and memory tests pass. Risk is later: v5 edits shared v4 files (CHANGELOG, backlog, Makefile, ci.yml) and will conflict once main moves.
2. v4 coverage: all v4.44-v4.50 contracts are in PLAN §8a rows (PLAN.md:905-911) and the importer note. None has importer code. The v5 data model has a native `Retracted` flag, but nothing reads v4 rows (`fv=3`/`rx`).
3. Maturity: 47 k LOC code, 77 k test, 3299 tests pass. It is a distributed substrate (Raft log, 2-partition txn protocol, graph apply/store), not a v4 replacement: no public graph API, no read/query API, no importer. Consumers (sigma-tkgd, ai-soc) move to v5 through that public API; rho has no reverse dependency on them (PLAN.md:8, :48).

## 1. Merge

| Check | Result |
|---|---|
| merge-base vs origin/main | 95a1de3 = origin/main; v5 is 111 commits ahead, 0 behind (fast-forward). `git merge-tree --write-tree` unsupported (git 2.34); `git merge --no-ff --no-commit` reported "went well", 765 files staged |
| Conflicts now | none. Not predictive: the files below are edited on both lines of work once main moves |
| v4 paths v5 touches (outside v5/, tools/) | Makefile (+23/-3), .gitignore, .github/workflows x4 (ci.yml +130), AGENTS.md +13, CHANGELOG.md +357 in `[Unreleased]`, SECURITY.md +33, tasks/backlog.md +426, tasks/lessons.md +6, tasks/v4-changes-for-v5-importer note (+26/-5), tasks/evidence/v2-* |
| v4 source touched | `pkg/graph/internal/core/ingest.go` (inlines a local var, no behaviour change); `pkg/graph/store/memory/memorystore_index_3phase_more_test.go` (+2 tests, Phase-2 hook ordering; hook exists in main). No root go.mod change |
| Likely future conflicts | CHANGELOG.md `[Unreleased]` top (v4.50.1 entries land in the same hunk), tasks/backlog.md, tasks/lessons.md, AGENTS.md, ci.yml, Makefile |
| Root build after merge | `go build ./...` OK, `go vet ./...` OK (45 root packages, none from v5/ or tools/). `go test -short`: pkg/graph/internal/core 73 s ok, pkg/graph/store/memory 71 s ok (the two touched packages); full root suite not run |
| Root `make` side effect | Makefile now dispatches build/test/vet/fmt/lint/security/... to `make -C v5` and `tools/graph-compare` (V5_DISPATCH=1, COMPARE_DISPATCH=1): every root gate also runs v5 and compare. `fmt`/`fmt-check` exclude those dirs. Off with `V5_DISPATCH=0 COMPARE_DISPATCH=0` |
| FoundationDB cgo | `tools/v5-substrate/fdb` has its own go.mod (the brief said none) and needs `foundationdb/fdb_c.h`: `go build` there fails here (header missing). Nested module, so root and v5 builds are unaffected. `tools/graph-compare` and `tools/vendor-compare` are also separate modules |

## 2. Coverage of our v4 work

Plan = docs/v5/PLAN.md on origin/v5. Code = grep of v5/ and tools/ for the v4 door names: 0 hits for every row except where stated.

| v4 item | Plan | Code (v5/) | Verdict |
|---|---|---|---|
| As-of rule (v4.46), record vs state doors | PLAN:907 (effective timelines as oracle) | none; v5 has its own retained-view reads at a pin (graphstate, raftlog/application_view_history.go) | plan only |
| WithTx doors (v4.44) | PLAN:804 names only DeleteWithTx/UpdateWithTx as the branch base; importer note §2 covers caller-chosen instants | none | plan only (weak) |
| Effective timeline (v4.47) | PLAN:907 | none | plan only |
| Re-import lives, lossy chains, PrevHash (v4.48) | PLAN:908, §9 | none | plan only |
| LatestStamps / HasHistory (v4.49/4.46) | not in PLAN (0 hits); importer note §5 only | none | miss (note only) |
| Retract / RetractWithTx (v4.49.1/2) | PLAN:905 (not a validity close; pins >= T see no valid time) | native analog: `Retracted` flag on assertion revisions (internal/assertion/codec.go:103; E02 retraction case in graphstate tests). No v4 mapping, no reader | plan only; native concept exists |
| Row fv=3 + `rx` marker | PLAN:906 (decode fv<=3, fail closed) | none | plan only |
| InventoryEpoch (v4.50) | PLAN:909 | none | plan only |
| Range counts / PropertyOptions (v4.50) | PLAN:910 | none | plan only |
| NaN bound, sharded count-once, ordinals | PLAN:911 | none | plan only |
| Importer note §3 one-tick spans | PLAN §9 (verified provenance, else ambiguity report) | test `V6-one-tick-import-ambiguity` marked pending, "not implemented here" (graphstate/revised_contract_test.go:73) | plan only, explicitly pending |
| Importer note §6 MintInstant, §7 DurableCommit, tiered temporal indexes | not in PLAN (PLAN:910 names tiered paths only for index options) | none | miss |
| Backlog 34 (SetXVersionIntervalWithTx) | not on main | none | n/a yet |

No Go file under v5/ imports v4 packages (only tools/graph-compare does). There is no importer and no v4 store reader.

## 3. Maturity (PLAN §8 phases; status from tasks/backlog.md, checked against code)

Build/test: `cd v5 && go build ./...` OK, `go vet ./...` clean, `go test -short -count=1 ./...`: 12 packages ok, 3299 test passes, 0 fail, 7 skips (SIGKILL seams, disk measurement, fuzz guards), 49 s. 992 `Test*` funcs, 18 `Fuzz*`. Race not run.
LOC (Go): code 47,335, tests 77,273 (1.6x). Largest: graphstore 15.5 k, graphapply 9.0 k, raftlog 7.2 k, pkg/temporal 3.7 k, graphstate 3.5 k. TODO/FIXME/unimplemented in code: 0; production `panic(`: 0; one pending marker in tests (importer). No `cmd/`; `pkg/` has only `temporal` and `types`; the engine is all `internal/`.

| Phase | Est. | Reason |
|---|---|---|
| V0 Specification | 95% | Backlog marks it accepted 2026-10-10; reference corpus, units, v4.50 delta in PLAN §8a. Importer provenance rule is spec only |
| V1 Values and state | 90% | Accepted in backlog; pkg/temporal, state reducer, graphstate, assertion present with differential/fuzz tests. Public API is values only |
| V2 Distributed slice | 55% | Active, not accepted. Present: raftlog, replica driver, txnproto (2-group protocol, process-kill tests), ID allocation, checked partition-local graph writes on 2 partitions x 3 voters. Open: cross-partition graph txns, certified reads, ownership transfer, serial-history oracle, ID reuse fencing, KV comparison and engine decision |
| V3 Compact engine | 15% | internal/timeblock (701 LOC) candidate, Pebble underneath. No arena memtable, seal/merge, byte ledger or budgets; backlog says prototype timings do not close V3 |
| V4 Read and change API | 10% | Internal bounded ApplicationView reads, history payloads, typed change records. No public query/scan/history API, no access negotiation/statistics, no public change feed; V4 exit needs sigma-tkgd's own pinned `access` contract tests to pass against v5 (PLAN.md:822, :882; the adapter code stays in sigma-tkgd) |
| V5 Integrated distributed access | 2% | No routing, endpoint postings, temporal indexes, distributed scans |
| V6 Migration and operations | 3% | Plan, consumer-migration.json and the pending test only; no importer, backup/restore, retention |
| V7 Release | 0% | No versioned API; only CI/Makefile wiring |

Missing to replace v4: public Graph/Tx API (create, update, delete, retract, queries); read and scan doors with tx-pin semantics; temporal and property indexes (incl. InventoryEpoch and range-count equivalents); compact engine with seal and recovery; v4 importer (retraction, fv=3, re-import lives, lossy chains, ambiguity report); cross-partition transactions; backup and retention; measured space/time parity with v4 and the three vendors.

## 4. Consumer answers (2026-10-11): can we use v5 already?

| Consumer | Answer | Missing from their view |
|---|---|---|
| sigma-tkgd | Not yet; nothing imports rho-tkg/v5, nothing built or tested against it | Public graph API equal to the v4 doors sigma reads (stores, CRUD incl. Retract/SetXVersionIntervalWithTx, NodeAtTx/RelAtTx, ByLabel/ByType ValidAt+TxAt, effective timelines, LatestStamps/HasHistory/MintInstant, column scans + backlog 48, property indexes with range counts and InventoryEpoch, adjacency scans, import path); asks for a v4→v5 door mapping |
| ai-soc | Not yet; imports v4 only via aisoc/internal/graphmgr (pkg/graph, types, temporal, store/tiered), pin v4.49.2 | (1) public v5 graph/store package covering graphmgr, (2) sigma-tkgd on v5 first (ai-soc reaches the graph through sigma's sessions), (3) René's go for a major-version pin |

## 5. v4 → v5 door mapping

Drawn from docs/v5/PLAN.md and the code at origin/v5 fd37fa0, against the v4 doors in sigma-tkgd (HEAD 60539e1, pin v4.49.2) and ai-soc `graphmgr` (HEAD 36752f7, pin v4.49.2), 2026-10-11.
The v5 API is Markus's design: every "v5 plan" cell is the plan's intent, not a promise. PLAN gives no method list beyond its reference surface (PLAN:370-377, "names still subject to API review"), and `v5/pkg` holds only `temporal` and `types`.
"not in plan" = no PLAN line names the door or its contract (input for Markus); "name only" = PLAN names the family without a contract; "requested" = sigma lists the door but calls it nowhere at its HEAD.

Key: `gs` = sigma internal/cypher/source/graphsource, `tr` = sigma internal/server/transport, `cl` = sigma internal/server/cluster, `ty` = sigma internal/tyla/source/graph, `gm` = ai-soc ai-soc-main/aisoc/internal/graphmgr. v5 paths are under `v5/internal/` unless they start with `pkg/`. PLAN line numbers are those of docs/v5/PLAN.md at origin/v5.

| # | v4 door | used by (file:line) | v5 plan | v5 code today | phase |
|---|---|---|---|---|---|
| | **Stores / open** | | | | |
| 1 | `graph.New(Config)`, `Close`; `store/memory.New` as `Config.Store` | both: sigma cmd/tkgd/main.go:358; ai-soc gm/manager.go:272 | PLAN:723 embedded = one partition, optional one replica; PLAN:59 memory, disk and distributed share one engine. No open call named | raftlog/storage.go:100 (`Open`, log store only; no graph-level open) | V3 |
| 2 | `store/tiered` and `Tier().ListShards/ForceRotate/Repair/Archive/Restore` | sigma cmd/tkgd/main.go:165, tr/handler_admin.go:227 (ai-soc: test only, gm/qualify_test.go:819) | PLAN:721 object tiering changes placement only; PLAN:824 V6 "segment tiering". No admin door named | timeblock/block.go:206 (sealed block encode, not tiering) | V6 |
| 3 | `store/sharded`, slot router, `Admin().DecomposeNodeID`, ID-slot lease (`Replication().IDSlotLease/SetIDSlotLease`) | sigma cmd/tkgd/main.go:194, cl/slot_router.go:103, tr/handler_promotion.go:87 | PLAN:523 logical partitions with replicated logs; PLAN:532 leases/epochs fence stale allocators; PLAN:63 IDs from durable allocation blocks; PLAN:892-893 IDs no longer encode placement (slot decompose has no successor by design) | idalloc/allocator.go:203 (`Transfer`); graphstore/partition_routing.go:49 | V2 |
| 4 | `Config.DurableCommit/SyncWrites`, `ErrCommitNotDurable` | ai-soc gm/manager.go:311, gm/manager.go:73 | PLAN:723-725 a buffered submission is not a durable commit receipt. No "visible but not yet durable" state named | raftlog/storage.go:602 (`Persist`) | V2 |
| | **Writes** | | | | |
| 5 | `Nodes()/Rels().Add`, `AddByID`; `GraphTx.AddNode/AddRelationship(ByID)` | both: sigma tr/handler_stream.go:37; ai-soc gm/manager.go:760 | PLAN:793-795 public "graph components, transactions and access"; no method list | graphstate/types.go:284 (`CreateNode`, `CreateRelationship` ops), graphstate/planner.go:234 | V2 |
| 6 | `Rels().AddByIDForeignEnd`, `RecordForeignIncoming` (relationship with a remote endpoint) | sigma tr/handler_rel.go:176, tr/handler_peer.go:75 | PLAN:537 remote endpoint lives add participants; PLAN:587 cross-partition transaction protocol. No foreign-end door | txnproto/machine.go:55 (2-group protocol); graphapply/partition_writes.go:213 | V2 |
| 7 | `Tx().Begin/Run/RunContext/RunWithLSN`; `GraphTx.Commit/Rollback/StartInstant` | both: sigma gs/writer.go:23, cl/bookmark.go:219; ai-soc gm/manager.go:341 | PLAN:587 transaction reference protocol; PLAN:892-893 other readers cannot see a GraphTx's private writes; PLAN:372 `After(Receipt)` takes the place of an LSN | graphapply/graph_read_guard.go:211 (`Request`); txnproto/machine.go:55 | V2 |
| 8 | `Batch().New` (BatchBuilder: `AddNode(s)`, `Execute`) | sigma gs/bulk.go:11 | name only: PLAN:39 "typed batches" (ownership row); PLAN:809 batch interfaces carried forward | none | V4 |
| 9 | `Ingest().NewSession` (`AddNode/AddRelationship/Submit`, `WaitApplied`, `AppliedSeq`) | sigma tr/handler_ingest.go:121 | not in plan | none | unassigned |
| 10 | `Update`, `SetProperty/DeleteProperty`, `AddLabel/RemoveLabel` (node, rel, GraphTx twins) | sigma tr/handler_node.go:120, tr/handler_rel.go:243 | PLAN:119-120 Set/Unset replace exactly a requested region; corrections are new revisions | graphstate/types.go:288-292 (`Correct`, `AddLabel`, `Set`, `Unset`), graphstate/planner.go:120 | V2 |
| 11 | `GetOrCreateByKey` (GraphTx and `Nodes()`) | both: sigma graduation/store.go:81; ai-soc gm/manager.go:729 | not in plan (PLAN:561 has unique claims only) | graphstate/unique.go:191 (unique candidates; no get-or-create) | unassigned |
| 12 | `Delete/DeleteWithTx` (validity end), `GraphTx.DeleteNode/DeleteRelationship` | both: sigma tr/handler_node.go:147; ai-soc gm/changes.go:130 | PLAN:905 Delete keeps its validity-end meaning, distinct from Retract | graphstate/types.go:287 (`Close` op) | V2 |
| 13 | `Retract/RetractWithTx` (+ GraphTx, Batch, Session twins) | sigma: requested, no call at HEAD (CHANGELOG.md:387); ai-soc: none | PLAN:905 belief retraction: pins >= T see no valid time, not a validity close; PLAN:906 `fv=3`/`rx` | assertion/codec.go:103 (`Retracted` flag), assertion/record.go:35; no graph-level op | V6 |
| 14 | `Set{Node,Rel}VersionInterval(+WithTx)`, `CloseVersion`; `*WithTx` twins of Add/Update/Delete (caller-chosen tx instant) | sigma: requested, no call at HEAD (CHANGELOG.md:431) | not in plan for `Set*VersionInterval`/`CloseVersion`; PLAN:804 names only `DeleteWithTx`/`UpdateWithTx` (branch base) | graphstate/types.go:279 ("no automatic time stamping"; ops carry their own Scope) | V6 |
| | **As-of / state reads** | | | | |
| 15 | `Nodes().Get/GetByIDs/Lend`, `Rels().Get` | both: sigma gs/adapter.go:63; ai-soc gm/manager.go:391 | PLAN:339 identity lookups; PLAN:373 `Lookup(snapshot, typed selector, projection)`; PLAN:390 borrowed buffer lifetimes explicit | graphstore/full_readview.go:278 (`Entity`), graphstate/types.go:265 (`ReadView`) | V4 |
| 16 | `Temporal().NodeAtTx/RelAtTx/NodesAtTx/RelsAtTx` (state at ValidAt + TxAt) | sigma gs/adapter.go:71, cypher/analytics/graphview/snapshot.go:60 | PLAN:372 `OpenSnapshot(At(Cut))` then Lookup/Scan; PLAN:907 `*AtTx` resolve valid-time state at a belief pin | graphstate/project.go:16 (`Project` at a Position); raftlog/application_view_history.go:13 (`RootAt`) | V4 |
| 17 | `Temporal().NodeAsOf/NodesAsOf/RelsAsOf`, `TxPin` scans (record door: newest row recorded by the pin) | both: sigma gs/snapshot.go:248; ai-soc gm/recovery.go:386 | PLAN:907 record vs state doors as a consumer obligation; no v5 record door named; PLAN:685 `At(cut)` reproduces retained history | raftlog/application_view_history.go:38 (`ApplicationRecord`) | V4 |
| 18 | `NodesDuringTx/RelsDuringTx`, `Temporal().NodesAt/During/Relating`, `RelateNodes/RelateRels` (Allen) | sigma gs/snapshot.go:265, gs/snapshot.go:313 | PLAN:339 temporal selections; PLAN:482 use the selected relation's predicate; PLAN:246-248 Allen classification as a value operation. No during/relating door | pkg/temporal/allen.go:5; graphstore/full_current_incident.go:153 (`IncidentAt`, exact presence at a position) | V4 |
| 19 | `ByLabel/ForEachByLabel/All/ForEachByType/ForEach` with `QueryOpts` (ValidAt, TxAt, ValidStart/End); TxAt implies valid-now | both: sigma gs/adapter.go:43, gs/snapshot.go:291 (`validNow`); ai-soc gm/recovery.go:182 | PLAN:339 label/type/property scans; PLAN:362-369 a snapshot mode never implies a valid-time coordinate, relationship view named explicitly; PLAN:879 `QueryOpts` field names stay | graphstate/types.go:74 (`Visibility` Declared/Effective); graphstate/types.go:206 (per-owner `KeyPredicate`; no label-to-entity scan found) | V4 |
| 20 | `Temporal().CommittedTx/PeekTx/NowTx/AdvanceClock` | both: sigma gs/snapshot.go:274, gs/snapshot.go:292, tr/handler_peer.go:49; ai-soc gm/manager.go:405 | PLAN:372 `Fresh / Stable / At(Cut) / After(Receipt)`; PLAN:61 opaque certified cuts | raftlog/application_view_history.go:13 (`RootAt` by log index) | V4 |
| 21 | `Temporal().ResolveAsOf/TagAsOf/AsOfTags` (named instants) | sigma ty/sources.go:20 | not in plan | none | unassigned |
| 22 | `Resolve().NodeProperty/RelProperty` (shadow `tkg_*` properties: valid/tx time, hash) | sigma gs/adapter.go:35, ty/values.go:22 | not in plan (PLAN:212 keeps raw temporal fields as legacy evidence; PLAN:274 hashes) | none | unassigned |
| | **Timelines / history** | | | | |
| 23 | `Nodes()/Rels().History`, `VersionAfter/Before` | both: sigma tr/handler_history.go:33; ai-soc gm/manager.go:921 | PLAN:374 `Timeline / Occurrences(snapshot, typed selector)`; PLAN:358 complete histories | raftlog/application_view_history.go:64 (`historyPayload`) | V4 |
| 24 | `Temporal().NodeEffectiveTimeline/RelEffectiveTimeline`, `ForEachNodeEffectiveByLabel/ForEachRelEffectiveByType`, `ErrTxPinTooNew` | sigma: requested, no call at HEAD (CHANGELOG.md:415) | PLAN:907 effective timelines as the state-import oracle; PLAN:374 `Timeline` | graphstate/project.go:16 (Effective visibility, one position only) | V4 |
| 25 | `LatestStamps`, `HasHistory`, `NodeID/RelID.MintInstant` | sigma: requested, no call at HEAD (CHANGELOG.md:404, :438, :418) | not in plan (importer note only); PLAN:63 IDs have no clock dependence, so `MintInstant` cannot exist as is | idalloc/allocator.go:312 (`Cursor.Next`, counter blocks) | unassigned |
| | **Scans / columns** | | | | |
| 26 | `ScanNodeColumns/ScanRelColumns`, `ForEachDocValues(+Multi, AsOf)`, `DocValuesSnapshot` | sigma ty/columns.go:36, gs/columns.go:26 | PLAN:822 V4 "named/generic/row/column parity"; PLAN:339 projected batches. No column-scan door | timeblock/block.go:572 (`Block.Scan`) | V4 |
| 27 | `ScanRelSegments`; `Config.RelSegments/SegmentDir/AllowTxBackfill` | sigma ty/edge_columns.go:71 (ai-soc per PLAN:866-868, not found in graphmgr) | not in plan (PLAN:866-868 lists them as consumer needs only) | timeblock/block.go:206 (`Encode`) | unassigned |
| 28 | `Stats().PropertyTypeClassCounts` (+ Other marker, backlog 48), `PropertyStats`, `NodeCountByLabelAndPropertyKey` | sigma gs/statistics.go:52, :44, :48 | name only: PLAN:377 `Statistics`; PLAN:349 estimates separate from correctness | none | V4 |
| 29 | `Stats().NodeCount/CountByLabel(At)/AllLabelCounts/HistoryCounts/ReadCosts` | sigma gs/statistics.go:35, tr/handler_admin.go:34 | name only: PLAN:377 `Capabilities / Statistics / ExplainAccess` | none | V4 |
| | **Indexes** | | | | |
| 30 | `Index().CreateProperty/CreateRelProperty/CreateComposite/CreateTemporal/CreateRelTemporal` (+ Delete/Has/List) | sigma ty/builtins.go:220, gs/index_maintenance.go | PLAN:477-484 access paths (ordered coordinate postings for points, paged interval structure); property/composite DDL not named | graphstore/component_indexes.go:137 (`initializeIndexes`, genesis-time only) | V5 |
| 31 | Equality lookups `ByLabelAndProperty(ies)`, `ByTypeAndProperty` (+ pinned) | both: sigma gs/adapter.go:201; ai-soc gm/program.go:75 | PLAN:339 property scans; PLAN:341 push down declared exact scalar predicates | graphstore/full_readview.go:427 (`equalityKey`, internal) | V5 |
| 32 | Ordered `ForEachByLabelPropertyRange(Ordered)/Prefix`, `ForEachByTypePropertyRange*`, `RangeCardinality/RelRangeCardinality` | sigma gs/ordered.go:13, gs/ordered.go:67 | PLAN:911 exact vs declined range counts, NaN bound zero (v4.50 delta); no range-scan door named | graphstate/types.go:206 (component-name prefix, not value order) | V5 |
| 33 | `CreatePropertyWithOptions/CreateRelPropertyWithOptions`, `PropertyOptions`, `RangeCounts` capability, `Index().InventoryEpoch` | sigma: requested, no call at HEAD (pin v4.49.2 predates v4.50) | PLAN:909 InventoryEpoch semantics; PLAN:910 options and capability negotiation | none | V5 |
| 34 | `Constraints().CreateUnique/DropUnique/UniqueConstraints/DryRunValidate` | both: sigma gs/bulk.go:36, tr/handler_validate.go:58; ai-soc gm/manager.go:330 | PLAN:561 global unique claim bucket (owner kind, property name, scalar type); DDL and dry-run validate not named | graphstate/unique.go:191, graphstate/types.go:132 (`UniqueMode`) | V2 |
| 35 | `Index().CreateVector/SearchNearestScored` | sigma cypher/runtime/procedure/vector/query.go:33 | not in plan | none | unassigned |
| 36 | `NodeMutationEpoch/NodeLabelMutationEpoch/RelMutationEpoch`, `ScanKeepsOrder`, `MaxOrdinal` | sigma gs/epoch.go:7, gs/adapter.go:191 | not in plan (PLAN:911 covers ordinal adjacency only) | none | unassigned |
| | **Adjacency** | | | | |
| 37 | `Rels().Outgoing/Incoming/ForEachAdjacentEndpoint(At)/ForEachAdjacentRelAt/OutgoingDegree`, batched `OutgoingForNodes(AtPin)` | sigma gs/adjacency.go:166, ty/pushdown.go:57 | PLAN:339 adjacency expansion; PLAN:373 `Expand`; PLAN:486 in/out postings maintained transactionally | graphstore/full_current_incident.go:153 (`IncidentAt`); graphstate/unique.go:128 | V4 (read), V5 (postings) |
| 38 | `ForEachAdjacentEndpointOrdinal`, `RelTypeDegreeStats`, `RelColumns` ordinals | sigma gs/adjacency.go:163, gs/adapter.go:196 | PLAN:911 ordinal adjacency and count-once as obligations; not an API | none | V5 |
| | **Identity / types** | | | | |
| 39 | `types.NodeID/RelID`, `types.Instant`, `Node/Relationship` accessors, `graph.QueryOpts`, `Err*` sentinels | both: sigma gs/adapter.go:63, cypher/core/access/opts.go:37; ai-soc gm/manager.go:391 | PLAN:878-881 IDs stay opaque 64-bit, `Instant` = int64 ms codec, accessor and `QueryOpts` names stay; sentinels not named | pkg/types/instant.go:11 (`Instant` only); no NodeID/RelID in pkg/types, internal `EntityID` at graphstate/types.go:41 | V1 |
| | **Replication / change feed** | | | | |
| 40 | `Replication().Watch` (LSN-ordered tail, `DecodeChangeIdentity`) | sigma cl/mirror_watcher.go:135 | PLAN:375-376 `Changes(cursor, certified boundary)`, `WatchChanges(selector, starting cut)`; PLAN:379 gap-free bootstrap | graphapply/graph_changes.go:338 (`encodeChangeEnvelope`, internal) | V4 |
| 41 | `Replication().ApplyChanges/AppliedLSN/LastCommittedLSN`, read-only replica graph, `SetReplicationSource` | sigma cl/replica_watcher.go:203, cl/bookmark.go:80 | not in plan (asynchronous LSN follower); nearest PLAN:524 three-voter replicated logs, PLAN:372 `After(Receipt)` | raftlog/application_replication.go:15 (voter config) | V2 |
| 42 | `Events().SetAsync`, `NewAsyncEventBus`, `EventHandler` | sigma cmd/tkgd/main.go:450, cl/memgraph.go:155 | not in plan (`WatchChanges` PLAN:376 is the only feed named) | none | V4 |
| | **Import / backup / retention** | | | | |
| 43 | Runtime import with caller IDs: `GraphTx.ImportNodeWithID`, `Nodes().Import`, `/api/v1/import` | sigma tr/handler_import.go:199 | not in plan as a runtime door; PLAN:1013-1019 covers the v4-store importer only | none (no importer) | V6 |
| 44 | `IO().Export/Import`, `BackupTo/BackupDeltaTo`, restore | sigma tr/handler_replication.go:52, tr/handler_admin.go:128 | PLAN:718 backups capture a certified cut, restore on a different topology; PLAN:824 V6 | raftlog/application_snapshot_export.go:44 (`BeginApplicationExport`) | V6 |
| 45 | `Admin().PurgeExpiredNodes`, `CompactHistoryNodes/Rels`, `Admin().Reset` | sigma tr/handler_retention.go:51, tr/handler_admin.go:189 | PLAN:719-721 erasure and retention delimit reproducibility; PLAN:824 V6 retention/erase. Compaction is not a door (PLAN:471) | none | V6 |

Gaps (doors consumers use or request with no PLAN row):
- Write conveniences: `GetOrCreateByKey` (both consumers), `Ingest().NewSession`, `Set*VersionInterval`/`CloseVersion`, runtime import with caller IDs (rows 9, 11, 14, 43).
- Time and identity helpers: `ResolveAsOf` tags, shadow-property `Resolve()`, `LatestStamps`/`HasHistory`/`MintInstant`; PLAN:63 (no clock in IDs) rules `MintInstant` out as it stands (rows 21, 22, 25).
- Index, column and stats surface: property/composite/temporal/vector index DDL, mutation epochs, `ScanRelSegments`; `Statistics` (PLAN:377) is a name with no content for `PropertyTypeClassCounts` (rows 27-30, 35, 36).
- Replication model: sigma's asynchronous LSN follower, `ApplyChanges`/`AppliedLSN` bookmarks, the event bus and ID-slot promotion have no PLAN counterpart; the plan offers raft voters plus `WatchChanges` (rows 3, 41, 42).
- "Used by" caveat: Retract, `Set*VersionInterval`, effective timelines, `LatestStamps`, `HasHistory`, `MintInstant` and the v4.50 doors are requested by sigma but called nowhere at its HEAD (CHANGELOG.md:387-438); `pkg/types` has `Instant` but not `NodeID`/`RelID`, although PLAN:878 says they stay.
