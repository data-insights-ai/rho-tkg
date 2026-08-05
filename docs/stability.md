# API Stability & Deprecation Policy

## v4 Stability Promise

Within the v4 major version (v4.0.0 through v4.x.x), the following surfaces are guaranteed **stable**: no breaking changes, only additive evolution.

- **Sub-APIs** and their methods: `g.Nodes()`, `g.Rels()`, `g.Temporal()`, `g.Index()`, `g.Events()`, `g.Constraints()`, `g.IO()`, `g.Admin()`, `g.Stats()`, `g.Hash()`, `g.Resolve()`, `g.Ingest()`, `g.Tx()`, `g.Batch()` — plus the read-side change-feed accessors on `g.Replication()` (`ChangeFeed`, `ForEachChange`, `LastCommittedLSN`). The remaining `g.Replication()` methods and all of `g.Tier()` are experimental (see below)
- **Core types** in `pkg/types` (`Node`, `Relationship`, `PropertySlice`, `Instant`, `TemporalMetadata`, `NodeIntegrity`, `RelIntegrity`, and related helpers)
- **Store contract types** in `pkg/graph/store` (`Store` interface, `QueryOpts`, `ShardDepth`, `DistanceMetric`, sentinel errors)
- **Error sentinels** (`ErrNodeNotFound`, `ErrNilGraph`, `ErrReadOnlyReplica`, etc.) — their identities and the guarantees they represent are fixed for the lifetime of v4
- **Type aliases** re-exported from `pkg/graph` (e.g. `Config`, `ValidationLimits`, `GraphStats`)

Breaking changes are reserved for v5.0.0 or later.

## Deprecation Ritual

Any method or field whose behavior or meaning will change receives a `// Deprecated:` godoc marker **at least one minor release before** the change lands. For example, deprecation in v4.12.0 permits incompatible change in v4.13.0 or later; it does not permit it in the same release.

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

## Experimental Surfaces

The following surfaces are **not** covered by the v4 stability promise. They are either in active development or pending a future decision and may be removed, changed incompatibly, or stabilized with different semantics at **any time** — including within a minor release.

- **Replication Phase-1 API** (`g.Replication().ApplyChange`, `ApplyChanges`, `AppliedLSN`, `SetAppliedLSN`, `RegistrySnapshot`, `IDSlotLease`, `SetIDSlotLease`, `Watch`) — read-replica foundation, available but subject to refinement as horizontal-scaling matures
- **`g.Tier()` sub-API** (tiered-store admin: `Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `Repair`, `VerifyShard`) — tiered-store backend-specific operations, not part of the generic graph contract
- **DocValues reader types** (`types.NodeColumnReader` and related) — performance-oriented columnar access, API shape pending use-case feedback from query planners
- **Typed column scans** (`store.NodeColumnScanCapability`, `store.ColumnBatch`, `store.ColumnKind` and the `graph.ColumnBatch`/`ColumnKind`/`ColInt64`.. re-exports, `store.ScanColumnsFromNodes`, `store.ColumnScanBatchRows`, `graph.ErrMixedNumericColumn`) — the typed sibling of the DocValues doors above, experimental for the same reason: the batch shape is pending consumer feedback. The REFUSAL contract is the part most likely to matter to a consumer and the part least likely to change
- **Relationship column doors** (`RelColumnSnapshot`, `RelMutationEpochForType`, and the `tkg_rel_start` / `tkg_rel_end` reserved column keys, on the badger AND memory stores) — the relationship sibling of the DocValues doors above, experimental for the same reason: the shape is pending consumer feedback. The epoch is a freshness token and never a change count, and its granularity is NOT portable — badger stripes per type (coarsened by any mutation site that cannot name its type), while memory uses a single store-wide stamp. Code that relies on a write to type A leaving type B's snapshot valid is correct on badger and wrong on memory.
- **Columnar refresh counters** (`ColumnExtendCount` / `ColumnRebuildCount` on the memory and badger stores) — telemetry for how a label's columnar snapshot was refreshed (append-extended versus fully rebuilt). Diagnostic only; not a stable metric contract
- **`QueryOpts.IncludeEclipsed`** — reserved field with zero readers; pre-placeholder for a future cascade-edit feature (Phase 3). Consumers must not set it; it will either be implemented or removed at the next major version

Do not take a production dependency on experimental surfaces without understanding the risk. If a consumer does rely on one, open a GitHub issue so the dependency is known before the surface changes.

## Release Conventions

- **CHANGELOG.md is the source of truth** for version history. Each release is dated; a version bumped in `go.mod` must have a corresponding entry in CHANGELOG.
- **Versions batch multiple changes.** A single feature, bug fix, or test improvement may span multiple internal commits (via rebase or squash) but appears as one bullet in the public CHANGELOG under the version it ships in.
- **Docs-consistency check:** The version marker in `docs/` must match the current code's version (`go.mod`). Stale doc version strings are a catch-all blocker.

## See Also

- `CHANGELOG.md` for the full release history
- `docs/api.md` for the complete API reference
- Lessons in `tasks/lessons.md` document patterns, anti-patterns, and historical context

