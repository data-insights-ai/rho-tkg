# v4 → v5 analysis and consumer migration mapping

Source audit, 2026-10-11. v4 contracts: main `efbedc28170c6b04aa28a0e957fc16c4424100c6`
(**v4.50.1**). v5 implementation/acceptance: `c6dee9cdae853e86df66412faece76bd8e01e87d`,
then main integrated at `14614553d2ad34f972864c2a0e4392b985b65a0b`. Subsequent internal
certified reads are pinned by the [package-3 acceptance record](../tasks/evidence/v2-certified-reads/acceptance.json).
This document is
an API/semantic inventory, not a release approval or a second roadmap. Live phase
status remains [tasks/backlog.md](../tasks/backlog.md).

The current source extension is main `c23c8b04`/v4.51.5, recorded in
[PLAN §8a](v5/PLAN.md) and the source-delta objects in the consumer references.
Its scoped relationship retention, state column/zero-ordinal contract and
history accelerators add obligations to this historical 45-family inventory.
The [source reconciliation receipt](../tasks/evidence/main4515-integration-20261014/receipt.json)
separates exact upstream CI/source equivalence from broader reader and consumer
qualification. Existing retained-state preflight cases subsequently pass the ten
narrow gates at `da5d32e6` (see [module receipt](../tools/v5-migrate/evidence/main4515-da5d32e6.json));
older inventories and acceptance pins remain unchanged.

**Consumers cannot switch their graph engine to v5 yet.** Only
`github.com/data-insights-ai/rho-tkg/v5/pkg/temporal` and `/pkg/types` are public.
Graph storage, planning, application and replication implementations remain under
`v5/internal/`; exported identifiers there are not callable from consumer modules.
There is no public graph constructor, transaction/read/change surface, or v4
importer. rho remains an embeddable graph library/storage engine; the embedding
application supplies transport/processes, and sigma owns reasoning and its adapter.

## 1. Revision and merge evidence

The earlier analysis compared `fd37fa0` with main `95a1de39` (v4.50.0).
Its fast-forward result, 3,299 passing test executions, LOC totals and phase
percentages describe that earlier snapshot only. They establish neither current
merge readiness nor consumer compatibility. That draft is now committed on main
at `efbedc28`; the HANDOVER claim that it is uncommitted/approval-blocked is stale.

The original audited integration contains main `efbedc28` at `14614553`; its v5 source was
unchanged from the package-2 accepted input (412 files, manifest SHA256
`05a91666717441e100f9bf0617dbbcac418b7f5b7503a2f2cdd7957b1b53d187`).
The [package-2 acceptance record](../tasks/evidence/v2-cross-partition/acceptance.json)
records normal/race/coverage/scanner evidence and its scope. The documentation audit itself ran no builds or tests. The separate
[main integration record](../tasks/evidence/v2-cross-partition/main-4501-integration.json)
records passing root-module build/vet and focused interval/documentation race
tests; [hosted CI at `c61d1124`](../tasks/evidence/v2-cross-partition/hosted-ci-c61d1124.json)
subsequently passed for that integration. Package 3 has separate source, test and
review evidence; the older CI run does not cover its changes. Root `make` dispatches into v5 and graph-compare by default;
`V5_DISPATCH=0 COMPARE_DISPATCH=0` selects root-only gates. The separately moduled
FoundationDB substrate needs its native headers; its build is a separate gate.

## 2. Coverage of the current v4 contracts

Compare semantics and authority, not just v4 method names. Native component
corrections and retained application roots exist under different v5 names, but
neither a private method nor a plan reference is a consumer replacement.

| v4 contract | v5 evidence and remaining obligation |
|---|---|
| Record as-of versus valid-time state/timeline (4.46/4.47) | PLAN §8a preserves the distinction. Internal `graphstate.Project` resolves one position at a supplied view; package 3 supplies certified current/retained graph views. A public record selector, state/timeline API and consumer cut handles remain pending. |
| Caller-instant Add/Update/Delete/Retract (4.44–4.49.2) | PLAN §§2.2/5.3 and `consumer-migration.json` deliberately separate source ingestion from database commit. Import old stamps as legacy evidence; new writes receive new commit order. The public source-time carrier/compatibility policy remains to be specified. |
| **Interval correction with caller instant (4.50.1)** | Shipped on main, not “n/a yet”. `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx`, GraphTx, Batch and ingest twins append every correction/resumption/current-slot row with `TxFrom=UpdatedAt=txFrom`; earlier pins keep the old belief. Positive/nonfuture input, `AllowTxBackfill`, live entity, and ordering after all chain TxFrom/TxTo are required; refusal writes nothing, validFrom may exceed txFrom. Deferred units preflight conflicts including cascades. Internal v5 Correct/Set/Unset provide region-edit machinery, not the caller-stamp contract. The source delta is recorded in PLAN §8a and the importer handover; their next acceptance refresh must cover it. |
| Increasing versions across re-imported lives, lossy old chains (4.46/4.48) | PLAN §8a/§9 require provenance and ambiguity reporting. Internal LifeIDs/Reopen do not decode v4 chains or reconstruct overwritten rows. |
| Delete versus belief Retract; `fv=3`/`rx` (4.49.1/4.49.2) | Internal assertion records retain a Retracted marker; graph operation kinds have Close but no graph-level Retract. PLAN requires the semantic split and marker-aware decoding. There is no v4 wire reader/importer. |
| LatestStamps/HasHistory/MintInstant (4.46/4.47/4.49) | Importer handover §§5–6 describes these. v5 counter-block IDs intentionally have no time bits: a native MintInstant is not an omitted algorithm. Import the effective fallback start and retain the old ID/mint evidence. A public history-presence/stamp metadata contract remains unresolved. |
| InventoryEpoch, index options and range counts (4.50) | PLAN §8a lists the DDL epoch, opt-in options, exact/declined counts, NaN-bound zero and count-once obligations. No public v5 index/statistics implementation exists. Internal component/claim indexes are not substitutes for property-range DDL. |
| One-tick spans, raw types/hashes and transaction provenance | PLAN §9 governs: width or post-2026-06-12 dates cannot classify old sentinel rows. Preserve raw evidence and report ambiguity unless writer/format provenance excludes sentinel encoding. TxFrom sorting cannot recover missing atomic groups; synthetic import cuts must be labelled synthetic. |
| Durable commit and tiered/index behavior | PLAN §§4–5/8 describe durability and placement changes. Internal durable log/prepare/decision paths exist; v4 async-flush results and hot/warm/cold APIs still need an explicit consumer mapping. |

The [importer handover](../tasks/v4-changes-for-v5-importer-20261010.md) and
PLAN §8a now record the 4.50.1 source delta. Their older acceptance evidence stays
pinned at `95a1de39`/v4.50.0; the new obligation is not retroactive importer acceptance.

## 3. Maturity by accepted gates

Percentages are removed: passing a prerequisite is different from accepting a phase.

| Phase | Accepted scope / remaining evidence |
|---|---|
| V0 | Specification accepted at its recorded pins. The 4.50.1 source delta below extends the migration obligations; consumer/importer execution remains V4/V6, and historical runs retain their original pins. |
| V1 | Values/state accepted; public values and internal reducers. This does not accept graph/cut integration or consumer adapters. |
| V2 | Active, not accepted as a whole. Partition-local writes, **package 2 same-graph cross-partition prepare/decision/resolve** and **package 3 certified current/historical reads** accepted internally: one GraphID, two declared partitions, three disk-backed voters each, with retained state, failover/snapshots/reopen. Package 4 remains: serial-history/cut oracle, ownership transfer, identity reuse/fencing, GC, physical-host durability, transactional-KV comparison and engine decision. |
| V3 | Held. Timeblock candidate exists; full compact engine, seal/merge/recovery, paged structures and measured all-in byte/budget ledger are pending. |
| V4 | Held. Public projected reads/history/change API, capability/statistics contracts, leased gap-free bootstrap, named/generic/row/column parity and nonempty pinned sigma adapter tests are pending. |
| V5 | Held. Routing and synchronous endpoint effects already have internal V2 machinery; distributed read/expand/feed parity across 1/2/4/8 partitions, stalls and rebalance are pending. “No routing/postings” is too broad. |
| V6 | Held. Provenance-aware v4 importer/ambiguity report, consumer-repository changes, backup/restore across topology and retention/erase/tiering remain pending. |
| V7 | Held. Versioned public graph contracts, release gates, consumer suites and measured comparative capacity remain pending. |

Package 2's coalesced coverage is 27,153/30,788 statements (88.19%); all 46 changed
production files meet 80%, and all 47 added exported declarations have nonzero
coverage/direct tests. Those are internal implementation acceptance figures,
not evidence of a public consumer API or released v5. Package 3 separately records
its certified-read scope and qualified coverage in its acceptance record.

## 4. Consumer evidence and ownership

| Inventory | Pin and evidence qualification |
|---|---|
| Main's newer sigma report | `60539e1229e3884af9a3e60e433d558104022951`, rho v4.49.2. The pinned object and absence of v5 imports were checked read-only. Representative graphsource signatures/calls were inspected. Plain interval corrections occur in tests; “no call” claims below mean the reported production-call inventory, not all Go files. Current local HEAD is `7918a56b`; this mapping does not claim a fresh full inventory at it. |
| Main's newer ai-soc graphmgr report | `36752f7`, reported rho v4.49.2 and `ai-soc-main/aisoc/internal/graphmgr`. Retained as main's pinned consumer report; that checkout was not located/re-audited here. This is distinct from the historical ai-assisted-soc-v2 engine below. |
| PLAN/reference acceptance fixtures | sigma `6aadc2b3651f68ad43bc2704e7820602651d4403` (rho v4.43.0), ai-assisted-soc-v2 `9a26e689aa3cc8576e7235210a23d85bc8ca6416` (rho v4.40.0/sigma v0.10.1); [consumer-contracts.json](v5/reference/consumer-contracts.json) and [consumer-migration.json](v5/reference/consumer-migration.json) bind source paths/hashes and required tests. These fixtures are not migrated consumers. |

Sigma's required access contracts and adapter live in sigma. ai-soc's major
version switch depends on its graphmgr API and sigma migration as reported in
main. rho ships graph data/storage/access, never either consumer's adapter or
reasoning runtime. No consumer repository was changed by this audit.

## 5. v4 → v5 door mapping

All 45 inventory rows are retained. The v4 contract column is based on the public
APIs at `efbedc28`: `pkg/graph/{nodes,rels,temporal,index,stats,replication,ingest,
admin,tier,resolve,constraints,io,events}/`, `graph.go`, `subapi.go`,
`column_scan.go`, `store/opts.go` and CHANGELOG 4.50.1. Below, v5 implementation
paths start at `v5/`; PLAN references use sections at `14614553`, not drifting line
numbers. Main's reported consumer-call anchors are retained separately below.

Status: **Public implemented** means callable outside the v5 module;
**Internal only** means related machinery exists, not that all v4 semantics are
implemented; **Planned** means specified without a public implementation;
**Decision needed** means API shape, semantic replacement or support still needs
review. **None yet** in the public-successor column means **no public replacement
yet**. Proposed names (`OpenSnapshot`, `Lookup`, `Scan`, `Expand`, `Timeline`,
`Changes`, `WatchChanges`, `Capabilities`, `Statistics`, `ExplainAccess`) are
PLAN §3.2 sketches subject to API review, not callable signatures.

Every acceptance proof below is **required/pending consumer proof**, not a claim
it was executed. For temporal families use node/relationship mutation-then-old-cut
exact-set tests, negative/phantom cases, deleted entities and row/column parity;
exercise the declared/effective relationship view explicitly. Unsupported,
expired/unavailable and exact-empty outcomes must remain distinct. Implementation
acceptance lives in the backlog/evidence records.

### Stores and writes

| # | v4 API and semantic contract | v5 target status and evidence | Exact public successor today | Migration action and acceptance proof |
|---|---|---|---|---|
| 1 | `graph.New(Config)`, `Close`, `memory.New` as Store: embedded graph lifecycle and backend choice. | **Planned**, PLAN §§4/5.4/8; internal raftlog Open is only a log constructor. | None yet. | Select the declared memory/durable deployment once public open/config exists. Prove create/read/close/reopen and lifecycle/config refusals without exposing consensus dependencies. |
| 2 | `tiered.New`; `Tier().ListShards/ForceRotate/Repair/Archive/Restore`: physical routing/tier administration preserves graph answers. | **Planned**, PLAN §5.4/ V6. | None yet. | Map operational intent to placement/tiering rather than porting shard windows mechanically. Prove cold/warm/archive transitions retain IDs, histories and answers. |
| 3 | `sharded.New`, slot router, `Admin().DecomposeNodeID/DecomposeRelID`, `Replication().IDSlotLease/SetIDSlotLease`: Snowflake placement and orchestrator lease hint. | **Internal only** for partition routing/allocation (`internal/graphstore/partition_routing.go`, `internal/idalloc/`); **Decision needed** for public administration. IDs intentionally lose placement/time bits (PLAN §5.1). | None yet; no native bit-decomposition successor by design. | Route via graph-qualified ownership, not ID bits; adapt promotion to fencing. Prove stale allocators/owners refuse, IDs survive transfer and missing local ownership never means global absence. |
| 4 | `Config.DurableCommit/SyncWrites`, `ErrCommitNotDurable`: committed group may be visible while flush failed; exact LSN/result still returned. | **Internal only** durable log/decision paths; public buffered-versus-durable acknowledgement policy **Planned**, PLAN §5.4. | None yet. | Translate visible/non-durable and retry states explicitly; never label queued submission durable. Prove crash/reopen and injected flush/decision failures return truthful, deduplicable outcomes. |
| 5 | `Nodes().Add/AddByIDIfAbsent`, `Rels().Add/AddByID/AddByIDIfAbsent`, GraphTx create twins: identity/endpoints, constraints and created/hit behavior. Node caller-ID import is row 43, not a nonexistent `Nodes().AddByID`. | **Internal only** CreateNode/CreateRelationship and Plan (`internal/graphstate/types.go`, `planner.go`), local/cross-partition apply. | None yet. | Bind explicit graph/axis/life and property schema at the future create API. Prove invalid IDs/endpoints, duplicate keys, parallel-edge identity, nil values and constraint failures; hit returns must match each v4 door's distinct meaning. |
| 6 | `Rels().AddByIDForeignEnd`, `RecordForeignIncoming`: remote END attestation plus separately installed incoming stub, embedding application orchestrates. | **Internal only** routed endpoint effects and prepare/decision/resolve (`internal/dist/graphapply/cross_partition_host.go`), with certified reads in package 3. | None yet. | Replace two independent stub writes with the future atomic cross-participant contract. Prove both endpoint postings/primary apply together; unavailable authority and stale proofs refuse; retry/failover preserves one edge. |
| 7 | `Tx().Begin/Run/RunContext/RunWithLSN`, `GraphTx.Commit/Rollback/StartInstant`: closure/error/cancellation behavior and transaction-specific LSN bookmark; v4 private writes are not globally isolated. | **Internal only** transaction/prepare/decision machinery; public isolated tx and receipts/cuts **Planned**, PLAN §§5.2–5.3. | None yet. | Replace LSN-as-bookmark with an opaque durable receipt and explicit After mode; stop using StartInstant as a wall-clock cut. Prove outside readers see none/all, rollback hides private changes, retries resolve the same outcome and sentinels survive wrappers. |
| 8 | `Batch().New/Run`, BatchBuilder `AddNode(s)/Execute` and mutation twins: queue then execute, per-op result/committed LSN. | **Planned**, PLAN §§3.2/8 carries typed batches; internal Plan over operations is not the public batch builder. | None yet. | Define atomicity, limits, per-op outcome and receipt mapping. Prove refused preflight/cancellation writes nothing, boundary byte/row limits, batch-versus-tx parity and truthful outcomes on failure. |
| 9 | `Ingest().NewSession`, Session Add/Submit, `WaitApplied/AppliedSeq`: watermark means processed, even when rejected; WaitApplied is the apply-outcome channel. | **Decision needed**; no public session/queue capability specified. | None yet. | Decide typed bulk/session support and processed/applied/durable result separation. Prove rejected intent advances processing without false success; close/backpressure/limits and per-token failure propagation. |
| 10 | Update, property Set/Delete/CAS, label Add/Remove and GraphTx twins: changes append versions except explicit UpdateInPlace; nil patch deletes, present-null differs from missing. | **Internal only** Correct/Set/Unset/AddLabel/RemoveLabel (`internal/graphstate/planner.go`); public convenience/in-place policy **Decision needed**. | None yet. | Adapt whole-entity patches to component-region revisions; do not silently preserve UpdateInPlace at retained cuts. Prove patch leaves unrelated regions/properties intact, historical labels return, CAS semantics and null/missing differ. |
| 11 | `Nodes()/GraphTx.GetOrCreateByKey`: atomic among keyed callers; extra props apply only on create; protection against every writer additionally needs active uniqueness. | **Decision needed**; internal unique candidates/claims are not a get-or-create result contract. | None yet. | Define conflict/retry/created semantics at the future public transaction boundary. Prove concurrent keyed misses yield one identity, hits ignore extra props and unconstrained plain writers are not falsely covered. |
| 12 | Delete/DeleteWithTx and GraphTx twins: validity-end close, past valid state stays answerable; node cascade removes attached current relationships. | **Internal only** Close/life coverage, PLAN §8a keeps Delete distinct from Retract. | None yet. | Map to append-only life closure, record caller times separately (row 14). Prove old cut unchanged, past validity remains at later cuts, cascaded relationships and reopen LifeIDs do not rebind old edges. |
| 13 | Retract/RetractWithTx, GraphTx/Batch/Session twins: at/after T, ended life answers at no valid time; pre-T belief survives; re-import can create another life. | **Planned** graph retraction; **Internal only** assertion Retracted flag (`internal/assertion/record.go`, `codec.go`), no graph-level Retract operation kind. | None yet. | Require a public belief-retraction operation and marker-aware v4 import (fv3 and earlier fv2+rx). Prove before/at/after-cut, cascade, reopen and raw marker preservation; hashes alone cannot witness rx. |
| 14 | `Temporal().SetNodeVersionInterval/SetRelVersionInterval`, **WithTx twins shipped 4.50.1**, CloseVersion and caller-instant Add/Update/Delete/Retract: exact interval patch/resumption and old-pin stability; WithTx validates all-chain order, gates and deferred-unit conflicts. | **Internal only** region corrections; **Decision needed** public source-time metadata/compatibility contract. PLAN §§2.2/5.3 deliberately prohibit backdating new database commits; §8a and the importer handover record the 4.50.1 delta. | None yet. | Preserve `[vs,ve)→[vs,ve')` replay meaning and original source knowledge instant as provenance, while committing at a new cut. Prove old cut stays uncorrected, outside-region state survives, future validity is allowed, invalid/order-conflicting units refuse wholly; import raw txFrom exactly without claiming native cut equivalence. |

### Reads, histories, columns and statistics

| # | v4 API and semantic contract | v5 target status and evidence | Exact public successor today | Migration action and acceptance proof |
|---|---|---|---|---|
| 15 | Nodes/Rels Get/GetByIDs/Lend: current row, independent mutable Get copy versus immutable held Lend row; current epochs detect freshness. | **Internal only** ReadView.Entity/Project (`internal/graphstore/full_readview.go`, `internal/graphstate/project.go`); public Lookup/ownership contract **Planned**. | None yet. | Choose explicit snapshot/projection and borrowed/owned lifetime. Prove missing/deleted behavior, Get isolation, lent-row lifetime across later writes and cancellation. |
| 16 | `Temporal().NodeAtTx/RelAtTx/NodesAtTx/RelsAtTx`: valid-time state as believed at txAt; txAt=0 disables TX filter. | **Internal only** certified retained graph views plus Project at explicit Position/Declared-or-Effective visibility; public certified snapshot/state lookup **Planned**. | None yet. | Bind database cut and explicit valid position separately. Prove t0 state after mutation/deletion, generic/named/column exact parity and complete scope; retained local index is never a graph-cut certificate. |
| 17 | NodeAsOf/RelAsOf/NodesAsOf/RelsAsOf and pure TxPin: decisive newest-version row recorded by pin, not state valid-now; correction record may differ from Get. | **Decision needed** public record/history selector; internal ApplicationRecord is an application change/outcome envelope, **not** a v4 entity-record as-of successor. | None yet. | Keep record selection separate from projected state/timeline. Prove bounded correction returns the correction record even when Get differs; deletion/retraction cannot fall through to an older row. |
| 18 | Nodes/Rels At/During/DuringTx/Relating and RelateNodes/RelateRels: half-open overlap and predicate-anywhere historical queries; open end=0 on graph Allen doors. | **Planned** graph selections; **Public implemented** finite proper-interval endpoint classification (`pkg/temporal/allen.go`) only. | No graph replacement yet. Value helper: `temporal.ClassifyAllen(aStart, aEnd, bStart, bEnd temporal.Position, limits temporal.Limits) (temporal.AllenRelation, error)`. | Bind positions to one axis; preserve half-open membership separately from Allen endpoint order. The helper rejects points/infinity and does not scan entities. Prove earlier-only interval predicate, all relation/boundary cases and an explicit policy for open intervals. |
| 19 | ByLabel/ByType/All and ForEach with QueryOpts: historical label/type/property sets, paging; v4 TxAt alone adds valid-now; TxPin conflicts with validity/TxAt. | **Planned** explicit typed Scan, PLAN §3.2; internal per-owner component-prefix readers are not global label scans. | None yet. | Translate pure pin and bitemporal filters separately; request validity only when intended, and select relationship view explicitly. Prove later label/value changes, predicate during an earlier interval part, phantom values, pagination order and depth policy. |
| 20 | CommittedTx/NowTx sound scalar pins (qualified scope), PeekTx observability only, AdvanceClock monotonic floor: milliseconds are not certified distributed closure. | **Internal only** Fresh/Stable/At/After (`internal/dist/graphapply/certified_graph_reader.go`), accepted package 3. Certificates and receipts bind graph/scope/topology/configuration; public modes/handles remain **Planned**, PLAN §5.3. | None yet. | Replace scalar max/clock merge with certified handles and receipt inclusion. Prove older prepared transaction blocks closure despite larger observed rounds; lagged/expired/unavailable scope cannot look exact-empty. |
| 21 | TagAsOf/ResolveAsOf/AsOfTags/RemoveAsOfTag: named persisted knowledge-time marks, registry lifetime/reset behavior. | **Decision needed** named cuts and retention policy; no public registry. | None yet. | Decide library named-cut metadata versus consumer-owned names referring to opaque cuts/leases. Prove overwrite, remove/reset/reopen, unknown/expired distinction and immutable named historical answer. |
| 22 | Resolve().NodeProperty/RelProperty: stored properties plus virtual `tkg_*` validity/transaction/hash/audit fields. | **Decision needed** metadata/shadow compatibility. PLAN §§2.3/9 preserve raw legacy evidence; component revision/cut hashes are a different contract. | None yet. | Classify every exposed shadow as native value, legacy evidence or deliberate semantic change; do not fabricate wall-clock tx stamps/hash predecessors. Prove raw fields/types plus null/missing and node/rel parity. |
| 23 | Nodes/Rels History, VersionAfter/VersionBefore: stored revision chain includes deleted entities; versions identify rows, not array slots. | **Planned** Timeline/complete histories, PLAN §3.2; internal retained roots/change payloads are supporting machinery. | None yet. | Map old entity rows/versions to retained component revisions while preserving legacy identity/provenance. Prove deleted/re-imported entities, gaps/collisions, before/after navigation, complete retained coverage and truthful expiry. |
| 24 | Node/RelEffectiveTimeline and ForEach effective label/type: nonoverlapping half-open segments equal pointwise AtTx; deleted entities retained; rel timeline is declared; ErrTxPinTooNew. | **Planned** timeline access, PLAN §8a; internal Project provides a point oracle only. | None yet. | Use v4 effective timelines as state-import oracle, not raw History order; choose endpoint masking separately. Prove per-point equality at old/new cuts, gaps, deleted lifecycles and distinct equal-content rows; map future/expired cut errors. |
| 25 | LatestStamps folds all rows (max TxFrom, max TxTo/DeletedAt, current deletion); HasHistory equals nonempty History; MintInstant derives v4 effective start when valid-from unset. | **Decision needed** native metadata summaries; MintInstant intentionally absent for counter IDs. Public Instant codec is not an ID-time derivation. | None yet; no native MintInstant successor by design. | Import effective start before losing Snowflake derivation; keep original stamps/IDs as legacy evidence. Define summaries consumers still require. Prove folded equivalence across correction/delete/re-import and exact zero/unset handling. |
| 26 | ScanNodeColumns/ScanRelColumns; ForEachDocValues(+Multi/AsOf), DocValuesSnapshot: row/pin parity, presence/kinds, explicit fallback and reused-buffer lifetime; as-of gen=0 is not a current epoch. | **Planned** projected bounded batches, PLAN §3.2/V4. | None yet. | Bind selector/cut/projection and explicit owned/borrowed batches. Prove historical label/property/deleted rows and exact kinds, Other fallback, byte/row limits, cancellation and no use after callback. |
| 27 | ScanRelSegments; Config.RelSegments/SegmentDir/SegmentMemoryBudget; AllowTxBackfill: declared current segment columns/Row fallback, directory lock/recovery; backfill is a separate semantic gate. | **Decision needed** public schema/config/column lifecycle; **Planned** compact engine V3. A block codec is not a segment directory. | None yet. | Map declaration, budgets/ownership and column fallback; handle backfill via row 14 rather than copying its flag. Prove declared/undeclared results, exact Other values, ID order and seal/reopen/lock/corruption behavior. |
| 28 | PropertyTypeClassCounts/RelPropertyTypeClassCounts: exact Numeric/NaN/String/Bool/Other/Missing partition; PropertyStats NDV is estimate, presence/min/max have their stated scope. | **Planned** Statistics name; exact fields/class markers/access policy **Decision needed**, PLAN §3.2. | None yet. | Retain correctness gates separately from estimates and scoped stats. Prove exact partition including mixed/slices/custom/null/missing, changes and capability refusal; NDV must never justify omission. |
| 29 | Stats NodeCount/RelCount/label/type counts/HistoryCounts/ReadCosts; Nodes.CountByLabelAt/Rels.CountByTypeAt equal scans under opts; counts versus nominal cost differ. | **Planned** Statistics/ExplainAccess; public counters/introspection not implemented. | None yet. | Map exact/scoped counts, estimated costs and optional capability flags explicitly. Prove count=matching scan after update/delete at both cuts; declined stats differ from zero and budgets do not change answers. |

### Indexes, adjacency and types

| # | v4 API and semantic contract | v5 target status and evidence | Exact public successor today | Migration action and acceptance proof |
|---|---|---|---|---|
| 30 | Index Create/Delete/Has/List for property/rel-property/composite/temporal/rel-temporal/HF: declared/persisted inventory and transparent acceleration, backend refusals. | **Planned** access paths PLAN §4.2; public DDL/introspection **Decision needed**. Internal genesis component indexes are different structures. | None yet. | Specify supported DDL/options and durable inventory before porting index maintenance. Prove create/drop/reopen, duplicate/refusal behavior, exact indexed-versus-scan sets and concurrent historical reads. |
| 31 | ByLabelAndProperty(ies), ByTypeAndProperty with temporal opts: typed equality and complete historical matches; composite accelerator must preserve fallback answers. | **Internal only** typed value identity/unique claim machinery; public property Scan **Planned**. Equality key alone does not enumerate matching label members. | None yet. | Map exact scalar predicates and explicitly complete candidates/residuals for unsupported kinds. Prove old value after mutation, historical-only label, phantom values, NaN/type-kind/null rules and indexed/unindexed parity. |
| 32 | Numeric range/ordered/prefix node/rel ForEach and RangeCardinality/RelRangeCardinality: value order plus ascending-ID ties; numeric candidates may overselect/recheck; exact counts or explicit decline, temporal counts decline. | **Planned** access/order/capability contract and §8a range-count obligation; component-name prefix is not ordered property values. | None yet. | Specify candidate-versus-exact order/residual and count contract. Prove inclusivity, ties, NaN bounds, integers beyond float precision, old-cut values, count-once cross partitions and decline/fallback equivalence. |
| 33 | CreatePropertyWithOptions/CreateRelPropertyWithOptions, PropertyOptions/RelPropertyOptions, RangeCounts and InventoryEpoch: opt-in fixed acceleration; epoch changes only for DDL that may alter inventory/reset, nonpersistent and separate from data epochs. | **Planned**, PLAN §8a current-v4 delta; no public option/DDL epoch implementation. | None yet. | Preserve plain versus opt-in options and refusal; define DDL invalidation at future public index boundary. Prove read-epoch-before-inventory/recheck, completed failed DDL vs unchanged refusal, reopen, unsupported option and exact count parity. |
| 34 | Constraints CreateUnique/CreateUniqueForever/DropUnique/UniqueConstraints/ReleaseOwnership/DryRunValidate: current versus permanent ownership, label-scoped nonfloat scalar validation, dry-run no writes/claims. | **Internal only** schema UniqueScalar/UniqueMembers and global claim routing; **Decision needed** compatibility scopes/DDL/dry-run. Temporal uniqueness is not v4 Current/Forever semantics. | None yet. | Declare exact scope/type/label claim mapping and public dry-run rather than equating any claim index with v4 constraints. Prove concurrent conflicts, history/nonoverlap, failed/rolled-back claims, release/reopen and zero mutation/ID/receipt effects in dry-run. |
| 35 | Index CreateVector(+Options)/SearchNearestScored: declared metric/dims, approximate HNSW versus brute force, scored current-vector semantics; TxPin refused. | **Decision needed** vector support; absent from public/reference API. | None yet. | Decide required v5 vector capability and approximation/historical policy before migrating procedures. Prove finite-value/dimension validation, declared engine/metric, score order and explicit unsupported/pin behavior. |
| 36 | NodeMutationEpoch/NodeLabelMutationEpoch/RelMutationEpoch, ScanKeepsOrder, MaxOrdinal: current cache invalidation, order cost and optional dense nonpersistent ordinals; none certifies history. | **Decision needed** public cache/order/ordinal capability; immutable internal view identity is not a data/DDL epoch. | None yet. | Use certified snapshots for correctness, retain separate performance metadata only where offered. Prove stale cache rejected on relevant writes, irrelevant label does not invalidate unnecessarily, ordinal absence and re-open policy are explicit. |
| 37 | Outgoing/Incoming, ForEach adjacent endpoints/rels(+At), degrees and *ForNodes(+AtTx/AtPin): seed/group behavior, rel-ID order, immutable endpoints, history/deletion; AtTx versus AtPin differs. | **Internal only** incident candidates/current incidence and synchronous routed endpoint effects; public Expand **Planned**, PLAN §§3.2/4.2. | None yet. | Bind direction/type, cut/validity/view, seed tolerance, multiplicity and projected fields. Prove remote cross-edge after close/reopen, deleted historical edge/seed, both directions, batch-versus-row parity, ID order and no missing incoming posting. |
| 38 | ForEachAdjacentEndpointOrdinal, RelTypeDegreeStats, RelColumns ordinals: row-aligned IDs/ordinals, 0 when unavailable; degree exactness qualified against concurrent epoch changes. | **Planned** obligation in PLAN §8a; public ordinals/degrees/batch shape **Decision needed**. | None yet. | Negotiate ordinal/degree capabilities instead of using IDs as array indexes. Prove alignment/ID order, cross-partition endpoint numbering, unavailable ordinal=explicit policy, exactness changes under writes and callback isolation. |
| 39 | types NodeID/RelID, Instant, Node/Relationship accessors, graph.QueryOpts and errors.Is sentinels: opaque identities plus whole-entity compatibility shapes; zero Instant is often an unset filter in v4. | **Public implemented** Instant, GraphID, DefaultAxisBinding and temporal values. NodeID/RelID, rows, QueryOpts and graph sentinels **Planned**, PLAN §8a; only internal EntityID exists. | `types.Instant`; `types.InstantPosition(axis temporal.Axis, value types.Instant, limits temporal.Limits) (temporal.Position, error)`; `types.InstantFromPosition(position temporal.Position, axis temporal.Axis, limits temporal.Limits) (types.Instant, error)`; `types.NewDefaultAxisBinding(graph types.GraphID, axisID temporal.AxisID, limits temporal.Limits) (types.DefaultAxisBinding, error)`. **No public graph-shape replacement yet.** | Bind explicit Q/ms/POSIX1970 axis/graph, translate zero/unset/infinity by v4 field contract, preserve exact kinds and fractional values without forcing them through Instant. Prove wrong axis/profile refuses, fractions/overflow decline codec, Point(0)/Unplaced/All/infinity differ; graph contracts must compile before import-path switch. |

### Change feeds and operations

| # | v4 API and semantic contract | v5 target status and evidence | Exact public successor today | Migration action and acceptance proof |
|---|---|---|---|---|
| 40 | Replication Watch/ForEachChange/ChangeFeed, DecodeChangeIdentity: durable mutation-level physical redo in LSN order; Watch fromLSN is inclusive, restart can redeliver. | **Internal only** typed CDC/application envelopes; public Changes/WatchChanges and leased certified boundary **Planned**, PLAN §§3.2–3.3/5.4. | None yet. | Replace scalar cursors with retained transaction envelopes and complete boundary; consumers deduplicate. Prove scan-at-C then changes-after-C gap-free, selector exits include before state, reconnect replay and incomplete cross-partition group cannot advance cut. |
| 41 | ApplyChanges/AppliedLSN/LastCommittedLSN, ReadOnlyReplica, SetReplicationSource: asynchronous physical row reproduction, monotonic durable watermark and registry catch-up. | **Internal only** voter replication; **Decision needed** public asynchronous read-replica/bootstrap topology. Raft voters are not this follower API. | None yet. | Choose advertised replica model and receipt/cut bookmarks; do not import v4 redo wire into consensus. Prove follower waits for certified application, leader loss/restart, registry availability, idempotence and no watermark ahead of durable data. |
| 42 | Events SetAsync/NewAsyncEventBus/EventHandler: process-local notification/backpressure/lifecycle, distinct from durable redo feed. | **Decision needed** event-bus compatibility; WatchChanges is planned durable data, not equivalent local notification. | None yet. | Decide explicit local adapter/public hooks or use a feed with acknowledged changed guarantees. Prove ordering, callback lifetime, failure/backpressure/close and avoid using event delivery as durability proof. |
| 43 | GraphTx.ImportNodeWithID/ImportRelationshipWithID, Nodes/Rels Import; sigma `/api/v1/import`: trusted identity supplied by caller with validated entity/chain semantics. HTTP remains consumer-owned. | **Planned** offline v4 importer (PLAN §9/V6); **Decision needed** runtime caller-ID import/allocation policy. Internal supplied IDs require allocator/ownership proof. | None yet. | Separate runtime import from historical store migration; define ID collision/range claims and source identities. Prove malformed/foreign IDs, endpoints, duplicate/lossy histories, hash/raw-field validation and rollback leave no partial state. |
| 44 | IO Export/Import/ExportSince/ImportMerge, BackupTo/BackupDeltaTo/RestoreInto: framed trusted replay, hashes/chain verification, full base plus durable redo deltas, cursor lineage and idempotence. | **Planned** backup/restore at a certified cut, PLAN §5.4/V6; internal BeginApplicationExport is a partition substrate export. | None yet. | Use versioned graph backup with descriptors, ownership/participant lineage and retained cut; explicitly reject v4 wire as native v5. Prove corrupt/truncated input fails closed, base mismatch, delta replay and restore on different topology preserve retained cuts/CDC. |
| 45 | Admin PurgeExpiredNodes/CompactHistoryNodes/Rels/Reset plus ExactErase: gated destructive removal versus retained history, watermarks/tags/CDC restrictions; expired history errors differ from absent. | **Planned** retention/erase V6, PLAN §§4.1/5.4; physical compaction must preserve retained cuts. Public policy/reset receipt **Decision needed**. | None yet. | Define retention leases, hard erasure closure, reset/feed epoch and logical history loss separately from physical compaction. Prove pinned history cannot silently disappear, erasure removes current/history/index residue, expiry is explicit and retry/reset keeps truthful cursor lineage. |

### Main's pinned consumer-call anchors

These retain the original inventory's evidence locations, rather than asserting
current consumer adoption. `gs` = sigma `internal/cypher/source/graphsource`, `tr` =
`internal/server/transport`, `cl` = `internal/server/cluster`, `ty` =
`internal/tyla/source/graph`; `gm` = reported ai-soc
`ai-soc-main/aisoc/internal/graphmgr`. Lines belong to the pins in §4.

| Rows | Reported anchors |
|---|---|
| 1–4 | sigma `cmd/tkgd/main.go:358,:165,:194`; `tr/handler_admin.go:227`, `cl/slot_router.go:103`, `tr/handler_promotion.go:87`; ai-soc `gm/manager.go:272,:311,:73` (tiered test: `gm/qualify_test.go:819`). |
| 5–12 | sigma `tr/handler_stream.go:37`, `tr/handler_rel.go:176,:243`, `tr/handler_peer.go:75`, `gs/writer.go:23`, `cl/bookmark.go:219`, `gs/bulk.go:11`, `tr/handler_ingest.go:121`, `tr/handler_node.go:120,:147`, `graduation/store.go:81`; ai-soc `gm/manager.go:760,:341,:729`, `gm/changes.go:130`. |
| 13–14 | sigma requests in its `CHANGELOG.md:387,:431`; not reported production callers. Plain interval correction calls are present in sigma tests, including `internal/cypher/testing/suites/contracts/twodriver/shared_expr_qualifiers_test.go:244,:255`. 4.50.1 WithTx is newer than its v4.49.2 pin. |
| 15–22 | sigma `gs/adapter.go:63,:71,:43,:35`, `internal/cypher/analytics/graphview/snapshot.go:60`, `gs/snapshot.go:248,:265,:313,:291,:274,:292`, `tr/handler_peer.go:49`, `ty/sources.go:20`, `ty/values.go:22`; ai-soc `gm/manager.go:391,:405`, `gm/recovery.go:386,:182`. |
| 23–29 | sigma `tr/handler_history.go:33`, requested `CHANGELOG.md:415,:404,:438,:418`, `ty/columns.go:36`, `gs/columns.go:26`, `ty/edge_columns.go:71`, `gs/statistics.go:52,:44,:48,:35`, `tr/handler_admin.go:34`; ai-soc `gm/manager.go:921`. PLAN's older segment/backfill evidence belongs to ai-assisted-soc-v2 `engine/`, not a verified newer graphmgr call. |
| 30–39 | sigma `ty/builtins.go:220`, `gs/index_maintenance.go`, `gs/adapter.go:201,:191,:196,:63`, `gs/ordered.go:13,:67`, `gs/bulk.go:36`, `tr/handler_validate.go:58`, `internal/cypher/runtime/procedure/vector/query.go:33`, `gs/epoch.go:7`, `gs/adjacency.go:166,:163`, `ty/pushdown.go:57`, `internal/cypher/core/access/opts.go:37`; ai-soc `gm/program.go:75`, `gm/manager.go:330,:391`. Row 33 was requested and not callable at that consumer pin. |
| 40–45 | sigma `cl/mirror_watcher.go:135`, `cl/replica_watcher.go:203`, `cl/bookmark.go:80`, `cmd/tkgd/main.go:450`, `cl/memgraph.go:155`, `tr/handler_import.go:199`, `tr/handler_replication.go:52`, `tr/handler_admin.go:128,:189`, `tr/handler_retention.go:51`. |

### Decisions required before consumers implement the switch

1. Review the public graph/config/write/read/history/change signatures and whether
   PLAN §8a's optional v4-shaped compat façade ships. Once shipped publicly in
   v5.0 it must remain compatible throughout v5; the plan is not implementation.
2. Specify record-as-of separately from state/timeline; legacy shadow/stamp
   evidence and caller source knowledge time (including 4.50.1) separately from
   native database cuts. Preserve import ambiguity instead of inventing commits.
3. Decide unresolved convenience/capability surfaces: ingest, get-or-create,
   named cuts, runtime ID import, vector search, event notifications, index DDL,
   exact statistics, cache epochs and ordinals; assign each supported contract or
   explicit decline/semantic retirement in the existing backlog.
4. Keep consumer adapters in their repositories and require their pinned,
   nonempty mutation-then-historical tests. Compile/empty smoke tests, internal
   exported methods and substrate acceptance cannot close that migration gate.

This matrix records requirements and decisions; it does not start a consumer
migration or authorize a dependency upgrade. Refresh implementation status with
pinned acceptance evidence; mark a public successor only after its callable
signature and consumer acceptance evidence exist.
