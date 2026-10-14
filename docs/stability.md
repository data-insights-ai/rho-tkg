# API Stability & Deprecation Policy

## v4 Stability Promise

Within the v4 major version (v4.0.0 through v4.x.x), the following surfaces are guaranteed **stable**: no breaking changes, only additive evolution.

- **Sub-APIs** and their methods: `g.Nodes()`, `g.Rels()`, `g.Temporal()`, `g.Index()`, `g.Events()`, `g.Constraints()`, `g.IO()`, `g.Admin()`, `g.Stats()`, `g.Hash()`, `g.Resolve()`, `g.Ingest()`, `g.Tx()`, `g.Batch()` — plus the read-side change-feed accessors on `g.Replication()` (`ChangeFeed`, `ForEachChange`, `LastCommittedLSN`). The remaining `g.Replication()` methods and all of `g.Tier()` are experimental (see below)
- **Core types** in `pkg/types` (`Node`, `Relationship`, `PropertySlice`, `Instant`, `TemporalMetadata`, `NodeIntegrity`, `RelIntegrity`, and related helpers)
- **Store contract types** in `pkg/graph/store` (`Store` interface, `QueryOpts`, `ShardDepth`, `DistanceMetric`, sentinel errors)
- **Error sentinels** (`ErrNodeNotFound`, `ErrNilGraph`, `ErrReadOnlyReplica`, etc.) — their identities and the guarantees they represent are fixed for the lifetime of v4
- **Type aliases** re-exported from `pkg/graph` (e.g. `Config`, `ValidationLimits`, `GraphStats`)
- **Column segments** (ADR-0011, accepted): `Config.RelSegments` / `SegmentDir` / `SegmentMemoryBudget`, `g.ScanRelSegments`, `g.Admin().SealRelSegments` / `RelSegmentStats`
- **Sub-API `Ops` interfaces** (`temporal.Ops` and the other sub-packages' `Ops`) are wiring seams between the façade and the engine, not for external implementation: they gain methods in patch releases (backlog 34, 35).
- **Doors added in v4.44 and later** are stable additive surface: `Nodes()` / `Rels()` `DeleteWithTx`, `UpdateWithTx`, `HasHistory`, `LatestStamps`, `Retract`, `RetractWithTx` (v4.49.1; the `Retract*` twins on `GraphTx`, `BatchBuilder` and the ingest `Session`; `types.TemporalMetadata.Retracted`); the `*WithTx` twins on `GraphTx`, `BatchBuilder` and the ingest `Session`; ingest `Session.SetNodeVersionInterval` / `SetRelVersionInterval`; `Temporal()` `NodeEffectiveTimeline` / `RelEffectiveTimeline` / `ForEachNodeEffectiveByLabel` / `ForEachRelEffectiveByType` and (v4.51.1) `ForEachNodeEffective` / `ForEachRelEffective` / `ForEachRelEffectiveAtNodes` / `NodesEffectiveByIDs` / `RelsEffectiveByIDs` / `ForEachNodeEffectiveByLabels` with `temporal.Direction` and `ErrInvalidDirection`; `types.NodeID` / `RelID` `.MintInstant()`, `types.Node` / `Relationship` `.TxStamps()`; `Config.DurableCommit`; the sentinels `ErrTxOrder`, `ErrEntityDeleted`, `ErrTxPinTooNew`, `ErrCommitNotDurable`; the optional store capabilities `HistoryPresenceCapability`, `HistoryStampsCapability`, `RelPropertyTxMembershipCapability` / `NodePropertyTxMembershipCapability`, `DurableFlushCapability`; and (v4.51.2) `Temporal()` `ForEachRelEffectiveByTypeFrom` / `ForEachNodeEffectiveByLabelsFrom` (scans bounded by valid end) with the optional store capabilities `store.RelValidEndCapability` / `NodeValidEndCapability` (`store.ValidEndCandidate`, `store.ValidEndOpen` / `ValidEndNone`) and `store.ExAdjacencyCapability` (`ForEachExAdjacentRelID`, `NoteExAdjacentRel`), the generated-ID batch capability `generatedcreate.RelationshipsBatchCapability` (`PutRelationshipsBatchGeneratedID`), and the badger `Store.MetaSetDurable` / `ForEachMetaName`; and (v4.51.3) the optional store capabilities `store.MutationFlushHoldCapability`, `store.RelCreateUndoCapability`, `store.ScopedRelCreateUndoCapability` and `store.ReadSnapshotCapability` / `store.ReadSnapshot`, and the badger `Store.UndoRelationshipCreate` / `UndoRelationshipCreateScoped` and `RemoveRelEntityAndOutPlacement` / `RemoveRelIncomingPlacement`; (v4.51.4) `store.CommitFloorCapability` and badger `Config.OnCommitFloor`; (v4.51.5) `Admin().PurgeExpiredRels` with `admin.RelPurgePolicy` / `RelPurgeReport`, the change tag `store.ChangeRelRangePurge` (14), the optional store capabilities `store.RelRetentionPurgeCapability` / `RelRangePurgeLogCapability` and badger `Store.PurgeRelLegResidue`; (v4.51.8) `Temporal()` `NodesAtTxByIDs` / `RelsAtTxByIDs` (see api.md "Batched point read"); (v4.52.0) `Nodes()` `ForEachDocValuesOrdinal` / `ForEachDocValuesMultiOrdinal`; (v4.52.2) `PropertyIndexOptions.Elements` (see api.md "Elements index") with `ErrElementsIndexUnsupported` and `store.ValidatePropertyIndexOptions`, `types.ForEachIndexableElementKey` / `IndexableValueHasKey`, `types.Node` / `Relationship` `.ForEachIndexKey` / `ForEachPropertyIndexKey` / `HasPropertyIndexKey`, and `store.PropertyIndexOptionsCapability` on the tiered store. Their statistics doors are diagnostics (see Experimental Surfaces); (v4.52.2) `Config.DisableIntegrityHashes` with `ErrIntegrityDisabled` / `ErrIntegrityModeMismatch` (backlog 61), `MetaSetDurable` on the tiered and sharded stores; (v4.54.0) `Temporal()` `ExciseNodeValidity` / `ExciseRelValidity` and their `*WithTx` variants, the `GraphTx` and `BatchBuilder` twins of the same names, and `types.TemporalMetadata.Void`. Their statistics doors are diagnostics (see Experimental Surfaces); (v4.53.0) `Nodes()` / `Rels()` `LendByIDs`, `CountByLabelPropertyValue` / `CountByTypePropertyValue`, `ForEachIDByLabelPropertyRangeOrdered` / `ForEachIDByLabelPropertyPrefix` and the `Rels()` mirrors, `Stats().TrackRelTypeDegrees` / `UntrackRelTypeDegrees`, store capabilities `EntityLendBatchCapability` and `RelTypeDegreeCountsCapability`

Breaking changes are reserved for v5.0.0 or later.

## Deprecation Ritual

Any method or field whose behavior or meaning will change receives a `// Deprecated:` godoc marker **at least one minor release before** the change lands. For example, deprecation in v4.12.0 permits incompatible change in v4.13.0 or later; it does not permit it in the same release.

Version numbers (René 2026-10-14): a release with a really new feature (a new door, option or capability) bumps the MINOR version; fixes, performance work and doc changes are PATCH releases. The ritual reads "release": the marker and a CHANGELOG `Deprecated` entry ship in one release, with sigma-tkgd and ai-soc notified, and the change lands in a later release. Additive, opt-in designs that change nothing for existing callers need no ritual and are preferred.

An on-disk format change follows the same ritual, in two steps: the release that announces it (CHANGELOG `Deprecated`) already READS the new format, and the writer switches one release later, so an upgrade that has to be rolled back can always go back one release. Example, backlog 61: step 1 (the next patch) reads binary integrity hashes and store marker 4 while still writing hex and marker 3; step 2 (one patch later) writes binary hashes and stamps marker 4, which releases before step 1 refuse (`ErrWireFormatVersionUnsupported`).

The deprecation message must state:
- What is deprecated and why
- What the replacement or new behavior is
- Which release will remove or change it (e.g. "removed in v5.0.0" or "incompatible change in v4.13.0+")

Example:
```go
// Deprecated: SomeMethod is superseded by BetterMethod as of v4.11.0.
// This method will be removed in v5.0.0. Use BetterMethod instead.
func (g *Graph) SomeMethod() { ... }
```

Removals and breaking changes land only at major-version boundaries or after a documented, released deprecation period.

A bug fix that makes a door return the answer its documented contract (or the shared resolver it must agree with) already promises is not a behaviour change under this ritual: it ships in a patch, named under CHANGELOG "Fixed". Example: v4.52.2 aligned `NodeMatchesValidTime` / `RelMatchesValidTime` / `NodeInterval` / `RelInterval` and `RelateNodes` / `RelateRels` with `NodeAt` for a later version without a valid-from.

## Documented Exceptions To The Deprecation Ritual

- **v4.46.0, the record doors' answer after a bounded cascade.** `NodeAsOf` / `RelAsOf` / `NodesAsOf` / `RelsAsOf`
  and the `TxPin` scans changed from "the current row while it is current" (v4.44.0) to "the newest row recorded by
  the pin" (pin-stable: a later write can never change an answer at an earlier pin). It is shipped without the
  deprecation period because the old rule was a correctness bug (answers at an earlier pin changed after a later
  write; a Delete after a bounded cascade left the entity valid in declared reads, backlog 18 / 20), it had been
  documented for less than a day (v4.44.0 and v4.46.0 both shipped on 2026-10-09), and both known consumers (sigma-tkgd, ai-soc) were told beforehand and adapted. The
  state doors (`NodeAtTx` / `RelAtTx`, `ByLabel` / `ByType` with `ValidAt` + `TxAt`) did not change meaning.
  Future behaviour changes follow the ritual above.
- **v4.48.0 (backlog 38), the re-import of a deleted ID.** Shipped as an exception to the ritual above
  (additive-only is kept for every other surface), because the old behaviour lost data and the refused input
  has no consistent reading:
  - *Refused input.* `Nodes().Import` / `Rels().Import` / `Nodes().AddByIDIfAbsent` and the `GraphTx` twins
    (`ImportNodeWithID` / `ImportRelationshipWithID`) with a `tkg_tx_from` at or below a `TxFrom` / `TxTo` /
    `DeletedAt` of the ID's history now return `ErrTxOrder` (wraps `ErrInvalidTxFrom`) and write nothing.
    v4.43–v4.47 stored the row; the valid-time doors then ended it at the earlier delete while `Get` and the
    as-of doors answered it, and no stored stamp can place it once a cascade demotes it. The refusal is the
    ordering rule `UpdateWithTx` / `DeleteWithTx` already apply. Consumer that can see it: sigma-tkgd
    `/admin/import` (`tx.ImportNodeWithID` / `ImportRelationshipWithID` with the record's properties, under
    `TKGD_ALLOW_TX_BACKFILL`) — such a record on a deleted ID fails the request and its transaction rolls back;
    sigma-tkgd notified 2026-10-09. Creates of IDs without history are unchanged.
  - *Changed results on the plain doors.* A re-import of a deleted ID (with or without `tkg_tx_from`) returns
    a row whose `Version()` is the earlier life's highest version + 1 (was 0) and whose
    `Integrity().PrevHash` names that row (was empty); the earlier life's history is kept. When the earlier
    life's stamps lie ahead of the clock (a delete of a row whose valid start lies ahead), the re-imported
    row's `TxFrom` is one past them (was the clock).
  Chains stored before keep reading as before; rows that v4.43–v4.47 already overwrote stay lost (no migration).
  Migration for a refused caller: pass an instant after the delete (`DeletedAt` of the newest history row + 1),
  or import without `tkg_tx_from`.
- **v4.51.2 (backlog 50), the badger store marker is raised to 3.** Shipped as an exception to the ritual above
  because an older binary cannot be made to write the new index:
  - *Why.* From v4.51.2 the store keeps the ex-adjacency index (`0x0D`) in the same write as every relationship
    delete. A release before it would delete relationships without writing it, and the pinned adjacency doors
    (`Outgoing/IncomingForNodesAtPin`, `OutgoingRelsAt`, `ForEachRelEffectiveAtNodes`, ...) of v4.51.2 would then
    miss those relationships. Row encodings are unchanged (`storeutil.CurrentWireFormatVersion` stays 2); only the
    store-level marker (`storeutil.StoreFormatVersion`) moves to 3.
  - *Who can see it.* Anyone opening a badger store (also each shard of a tiered or sharded store) that a v4.51.2
    binary wrote with v4.51.1 or older: `New` returns `ErrWireFormatVersionUnsupported`. A store that only v4.51.1
    or older ever wrote is unaffected, and a store that v4.51.2 only opened read-only keeps marker 2.
  - *Refusal path.* v4.51.1 verifies the marker right after `badgerv4.Open`, before any row is decoded and before
    any write, and closes the DB on a mismatch, so the refused open writes no row.
  - *Migration.* Open the store once, writable, with v4.51.2: the marker is raised to 3 and the one-time scan of
    relationship history fills the ex-adjacency (meta marker `rel_exadj_built`). There is no way back to v4.51.1
    afterwards short of restoring a copy made before the upgrade.
  - *Read-only opens.* A v4.51.2 read-only open of a store still at marker 2 works: it neither stamps the marker nor
    scans, and the adjacency doors fold every relationship with history (sound, slower) until a writable open
    has built the index.

## Experimental Surfaces

The `*ForTest` exports on `badger.Store` (e.g. `SyncCountForTest`, `DurableSyncCountForTest`, `FailNextFlushForTest`) are test seams, outside the stability promise: they may change or disappear in any release.

The following surfaces are **not** covered by the v4 stability promise. They are either in active development or pending a future decision and may be removed, changed incompatibly, or stabilized with different semantics at **any time** — including within a minor release.

- **Replication Phase-1 API** (`g.Replication().ApplyChange`, `ApplyChanges`, `AppliedLSN`, `SetAppliedLSN`, `RegistrySnapshot`, `IDSlotLease`, `SetIDSlotLease`, `Watch`, and the replica wiring `g.SetReplicationSource`) — read-replica foundation, available but subject to refinement as horizontal-scaling matures
- **`g.Tier()` sub-API** (tiered-store admin: `Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `Repair`, `VerifyShard`) — tiered-store backend-specific operations, not part of the generic graph contract
- **`sharded.Store`** (`pkg/graph/store/sharded`, ADR-0007) — slot-topology backend over N Badger shards. S1–S5 (mandatory store, batches/cascade, change-log, ingest lanes, index/stats parity) are implemented and tested, but the public surface remains experimental until horizontal multi-machine routing is productized
- **DocValues reader types** (`types.NodeColumnReader` and related) — performance-oriented columnar access, API shape pending use-case feedback from query planners
- **Typed column scans** (`store.NodeColumnScanCapability`, `store.ColumnBatch`, `store.ColumnKind` and the `graph.ColumnBatch`/`ColumnKind`/`ColInt64`.. re-exports, `store.ScanColumnsFromNodes`, `store.NodeColumnSource` / `ScanColumnsFromNodeSource` (backlog 28), `store.ColumnScanBatchRows`, `graph.ErrMixedNumericColumn`) — the typed sibling of the DocValues doors above, experimental for the same reason: the batch shape is pending consumer feedback. The REFUSAL contract is the part most likely to matter to a consumer and the part least likely to change
- **Relationship column doors** (`RelColumnSnapshot`, `RelMutationEpochForType`, and the `tkg_rel_start` / `tkg_rel_end` reserved column keys, on the badger AND memory stores) — the relationship sibling of the DocValues doors above, experimental for the same reason: the shape is pending consumer feedback. The epoch is a freshness token and never a change count, and its granularity is NOT portable — badger stripes per type (coarsened by any mutation site that cannot name its type), while memory uses a single store-wide stamp. Code that relies on a write to type A leaving type B's snapshot valid is correct on badger and wrong on memory.
- **`ScanRelColumns` / `RelColumnBatch`** — the relationship column SCAN (distinct
  from the snapshot doors above): streams typed column batches with `StartIDs` /
  `EndIDs` alongside the property columns. Optional capability, so `ok=false` means
  the backend has none and the caller must fall back to `Rels().ByType`. Experimental
  for the same reason as its node sibling: the batch shape is pending consumer
  feedback. The callback must not retain the batch — its slices are reused between
  calls.
- **Columnar refresh counters** (`ColumnExtendCount` / `ColumnRebuildCount` on the memory and badger stores, `ColumnPatchCount` on badger, v4.51.0) — telemetry for how a label's columnar snapshot was refreshed (append-extended, patched after in-place updates, or fully rebuilt). Diagnostic only; not a stable metric contract
- **RAM-sidecar statistics** (`badger.Store.HistoryPresenceStats()` / `HistoryStampsStats()`, `store.PropertyTxMembershipStatsCapability`) — sizes, build counts and build times of the in-memory sidecars behind `HasHistory`, `LatestStamps` and the pinned property lookups. Diagnostic only, like the counters above; the doors they describe are stable
- **`QueryOpts.IncludeEclipsed`** — reserved no-op, kept for API compatibility; one-tick rows are ordinary spans (no row is skipped as "eclipsed"). Consumers must not set it; it may be removed at the next major version

Do not take a production dependency on experimental surfaces without understanding the risk. If a consumer does rely on one, open a GitHub issue so the dependency is known before the surface changes.

## Release Conventions

- **CHANGELOG.md is the source of truth** for version history. Each release is dated; every release tag (`vX.Y.Z`) has a `## [X.Y.Z] - date` section in CHANGELOG (the module path carries only the major version, `/v4`).
- **Versions batch multiple changes.** A single feature, bug fix, or test improvement may span multiple internal commits (via rebase or squash) but appears as one bullet in the public CHANGELOG under the version it ships in.
- **Docs-consistency check:** `pkg/graph/internal/core/docs_consistency_test.go` pins the current release from the topmost `## [x.y.z]` heading in `CHANGELOG.md` against fixed strings in `README.md`, `AGENTS.md` and `docs/architecture.md`, and pins the Go version from `go.mod` against `README.md` / `AGENTS.md` / `docs/architecture.md`. Stale status lines fail that test.

## See Also

- [`CHANGELOG.md`](../CHANGELOG.md) for the full release history
- [`docs/api.md`](api.md) for the API reference (`go doc` on each `pkg/graph` sub-package is the complete method list)
- [`docs/errors.md`](errors.md) for every public error sentinel
- Lessons in `tasks/lessons.md` document patterns, anti-patterns, and historical context

