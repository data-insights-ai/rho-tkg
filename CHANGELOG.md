# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [4.2.2] - 2026-05-15

### Added - Alias ErrNodeExists / ErrRelExists on graph package (2026-05-15)

Follow-on to v4.2.1's consumer-ergonomics aliases. The entity-conflict
sentinels (returned by `Import` and `AddByIDIfAbsent` create paths when
a caller-supplied ID is already present) were not yet aliased on
`pkg/graph`, forcing consumers to retain a `storepkg` import for those
two `errors.Is` checks. Two-line addition; no behavior change.

- **`graph.ErrNodeExists`** — alias for `store.ErrNodeExists`.
- **`graph.ErrRelExists`** — alias for `store.ErrRelExists`.

After upgrading, the conflict-409 paths in downstream DTO mappers no
longer need to import `pkg/graph/store` alongside `pkg/graph`.

## [4.2.1] - 2026-05-15

### Added - Public aliases for QueryOpts + index sentinels (2026-05-15)

Consumer-ergonomics patch — lets the Cypher engine (and other
downstream callers) use `graph.QueryOpts` + the index-sentinel errors
without importing `pkg/graph/store` alongside `pkg/graph`. No
behavior change; purely additive type/var aliases on the `graph`
package.

- **`graph.QueryOpts`** — alias for `store.QueryOpts`. Configures
  pagination, depth, and temporal filtering for read methods on
  `g.Nodes()`, `g.Rels()`, `g.Temporal()`, `g.Index().SearchNearest`.
- **`graph.ShardDepth`** — alias for `store.ShardDepth` (the `Depth`
  field of `QueryOpts`). Use the `storepkg.Depth*` constants —
  they stay where they are since they're tiered-store specific.
- **`graph.DistanceMetric`** — alias for `store.DistanceMetric`,
  accepted by `g.Index().SearchNearest`. Use the `storepkg.Distance*`
  constants for values.
- **`graph.ErrIndexExists`** — alias for `store.ErrIndexExists`
  (property index already exists).
- **`graph.ErrIndexNotFound`** — alias for `store.ErrIndexNotFound`.
- **`graph.ErrTemporalIndexExists`** — alias for `store.ErrTemporalIndexExists`.
- **`graph.ErrTemporalIndexNotFound`** — alias for `store.ErrTemporalIndexNotFound`.

The vector-index sentinels (`ErrVectorIndexExists`,
`ErrVectorIndexNotFound`) were already exported; this patch closes
the parallel gap for property and temporal indexes.

After upgrading, consumer-side import of `storepkg` is no longer
required for the common read-and-error-check pattern; just import
`pkg/graph`.

## [4.2.0] - 2026-05-15

### Changed - Sub-APIs are accessor methods, not exported fields (2026-05-15)

The 14 sub-API field accessors on `*Graph` (`g.Nodes`, `g.Rels`,
`g.Temporal`, `g.Index`, `g.Events`, `g.Constraints`, `g.IO`,
`g.Admin`, `g.Tier`, `g.Stats`, `g.Hash`, `g.Resolve`, `g.Tx`, `g.Batch`)
are now **methods** (`g.Nodes()`, `g.Rels()`, …). The exported fields
are gone; the underlying sub-API pointers live in unexported fields and
the accessor methods return them.

#### Why

- More idiomatic Go. Stdlib uses methods on receivers; exported struct
  fields as the primary API is a Java/C#-style accent. See
  CLAUDE.md "Code Style" notes from the v4.1.0 review thread.
- Centralizes nil-safety. `(*Graph)(nil).Nodes()` returns nil cleanly;
  zero-value `(&Graph{}).Nodes()` returns nil; both yield nil sub-API
  pointers whose methods fail closed with `ErrNilGraph` via the
  existing zero-value contract.
- Removes a class of footgun: customers can no longer accidentally
  assign or zero out `g.Nodes` from outside the package.

#### Migration recipe (mechanical sed)

```
sed -i '' -E 's/\.(Nodes|Rels|Temporal|Index|Events|Constraints|IO|Admin|Tier|Stats|Hash|Resolve|Tx|Batch)\./.\1()./g' **/*.go
```

Run from any consumer repo's root. Catches the dot-prefixed access
pattern (`g.Nodes.Add(...)` → `g.Nodes().Add(...)`). Bare field
references (`var x = g.Nodes`) need a one-line follow-up: replace with
`var x = g.Nodes()`. The compiler will flag every remaining occurrence.

Inside `tkg` itself the migration touched 34 files (~5,218 call-site
updates).

#### Tests

- `pkg/graph/accessor_test.go` (new) — 8 test functions covering:
  - Reflection-driven enumeration: every exported method on `*Graph` is
    checked for nil-receiver safety.
  - Zero-value `Graph{}` returns nil for every accessor.
  - Chained `(*Graph)(nil).Nodes().Add(...)` fails closed with
    `ErrNilGraph`.
  - After `Close()`, accessors still return live sub-API pointers,
    but calls return `ErrGraphClosed`.
  - Pointer stability: `g.Nodes() == g.Nodes()` on consecutive calls.
  - Struct-shape regression: `*Graph` must have **zero** exported
    fields (catches accidental re-introduction).
  - Reflection-driven signature check: all 14 expected accessor methods
    exist and return a single pointer.
- Existing `subapi_zero_value_test.go` (~70+ table-driven cases for
  zero-value sub-API behavior) is unchanged — the field-vs-method
  conversion didn't alter the sub-API zero-value contract.

#### What was NOT changed

- Each sub-API's `ok bool` zero-value guard. Could have been simplified
  to a single nil-check, but that would force `grapherr.IsNil` (reflect)
  on every method call. Keep the field, keep the constant-time check.
- The sub-API package layout under `pkg/graph/{nodes,rels,…}`.
- Internal-core references to `*Core.Nodes`, `*Core.Rels` etc. (those
  are still exported fields on `*Core` because the internal sub-API
  packages need them — that surface is not customer-facing).

## [4.1.0] - 2026-05-14

### Changed - Tx isolation: drop c.mu.Lock from tx lifetime (Path B) (2026-05-14)

**Eliminates the entire `tx-vs-c.mu.RLock` deadlock class.** v4.0.1 and
v4.0.2 added tx-side mirrors for every read accessor that took
`c.mu.RLock` — that worked but only by patching one accessor at a time
and was missable. v4.1.0 attacks the root: `BeginTx` no longer holds
`c.mu.Lock` for the transaction lifetime.

#### What changed

- **New `c.txMu sync.Mutex`** (`core.go`) replaces `c.mu.Lock` as the
  serialization mechanism between concurrent transactions and batches.
  `BeginTx` and `BatchBuilder.Execute` both acquire `c.txMu`.
- **Each tx method takes `c.mu.RLock` briefly** around its body
  (via new `tx.lockActiveCore` / `tx.unlockActiveCore` helpers) so
  the `*Internal` / `*Locked` helpers see the same lock context the
  standalone call paths provide via `runUnderRLock` / `readUnderRLock`.
  `tx.lockActiveCore` checks `tx.done` and `c.closed` together.
- **Commit / Rollback** take `c.mu.Lock` briefly (for registry pointer
  mutation safety), then release `c.txMu`. They no longer hold
  `c.mu.Lock` for the tx duration.
- **Batch execution** also takes `c.txMu` first then `c.mu.Lock` (held
  for the whole batch — batches are short-lived and atomic).

#### What this fixes

- `g.Nodes.ByLabel`, `g.Rels.Outgoing`, `g.Temporal.NodesAt`, every
  `*By*(opts QueryOpts)` / `*At(...)` / `*AsOf(...)` method, `Stats.*`,
  `Index.SearchNearest`, and every other public read accessor now
  **work correctly inside an open `*GraphTx`**. The non-tx accessors
  acquire `c.mu.RLock` — under v4.1.0 nothing holds `c.mu.Lock` during
  the tx, so RLock succeeds immediately.
- The bug class is gone going forward. Future read accessors that take
  `c.mu.RLock` automatically work inside a tx with no tx-side mirror
  required. Lesson 31's "mirror every accessor" audit recipe is
  superseded.

#### Isolation semantics (minor-version-bump price)

- **Before (v3.4 / v4.0.x):** tx holds `c.mu.Lock` for its lifetime →
  ALL concurrent standalone mutations and ALL reads from other
  goroutines block until the tx ends. "Serializable graph-wide."
- **After (v4.1.0):** tx holds `c.txMu` for its lifetime and `c.mu.RLock`
  briefly per call. Concurrent standalone mutations on DIFFERENT
  entities run in parallel; entity-level conflicts still serialize via
  the existing entity-lock manager. Concurrent reads from other
  goroutines proceed in parallel. **"Serializable per touched entity,
  snapshot-isolated elsewhere."**
- **Visible side effect:** a concurrent reader can see an in-progress
  tx's allocated labels / types before the tx commits or rolls back. On
  rollback, those allocations are revoked. If a reader captured a token
  during the tx that the tx later rolled back, the token's name lookup
  will return `(0, false)` after rollback; the token itself is no
  longer mapped. Code that depended on "tx blocks all concurrent
  observation" must take an external lock to preserve that guarantee.

#### Tests that pinned the old semantics

- `TestMutationBlockedDuringTx` → renamed `TestMutationProceedsAlongsideTx`,
  now asserts the standalone mutation completes promptly.
- `TestGraphTx_PublicResolverReadsWaitForRollback` →
  `TestGraphTx_PublicResolverReadsDoNotBlock`, asserts concurrent reads
  complete and post-rollback the in-progress allocations are revoked.
- The three "deadlock regression" tests added in v4.0.1 / v4.0.2 were
  inverted: the non-tx accessors now WORK inside a tx and the tests
  assert that.

#### What was NOT changed

- The tx-side read mirrors added in v4.0.1 and v4.0.2 are kept. They
  no longer prevent deadlock (there's nothing to deadlock against) but
  remain functional and provide a clearer call-site signal that "this
  read is inside the tx I'm holding." They take `c.mu.RLock` via the
  same helper used by tx mutations.
- `Temporal.DiffCallback` still releases `c.mu.RLock` between callbacks
  by design (handlers may re-enter the graph). Under v4.1.0 this no
  longer deadlocks inside a tx, so its v4.0.2-deferred mirror is no
  longer needed.

## [4.0.2] - 2026-05-14

### Fixed - Tx-read deadlock against c.mu (bulk read methods) (2026-05-14)

- **v4.0.1 covered only the metadata-resolution accessors.** The actual
  data-retrieval reads (`Nodes.ByLabel`, `Rels.Outgoing`, `Temporal.NodesAt`,
  every `*By*(opts QueryOpts)` and `*At(...)` method, the `*AsOf` set,
  `Stats.*`, `Index.SearchNearest`, `Tier.ListShards`, `Events.GetSync`,
  `Constraints.Get`) all go through `c.mu.RLock` either directly or via
  `c.readUnderRLock(func(){…})`, and still deadlocked inside an open
  `*GraphTx` for the same reason: `BeginTx` holds `c.mu.Lock` for the tx
  lifetime, `sync.RWMutex` is not reentrant.
- Operationally critical second incident: Cypher
  `CREATE (n:Widget {…}) RETURN n; MATCH (n:Widget) RETURN n.name` hung at
  the `MATCH` because the engine keeps one tx open across all statements
  and the second statement called `g.Nodes.ByLabel` inside it.
- **Fix.** Extracted ~20 `(*Core).fooLocked` helpers from the
  `readUnderRLock(func(){…})` closure bodies in `queries.go`,
  `graph_property_query.go`, `temporal_queries.go`, `txtime.go`,
  `stats.go`. Both the public methods (under `c.mu.RLock`) and the new
  tx mirrors (under `tx.mu` only) now route through these helpers.
- Audit recipe is now in lessons.md #31: any new public read method
  that takes `c.mu.RLock` or `c.readUnderRLock` owes a tx mirror.
  The audit grep is `c\.mu\.RLock\|c\.readUnderRLock` in pkg/graph/internal/core/.
- **Known follow-up.** `Temporal.DiffCallback` retains its multi-segment
  `readUnderRLock` design (lock released between callbacks so handlers
  can re-enter the graph). It is not yet mirrored — its lock-release
  semantics make the tx-side contract non-trivial. Deferred to v4.1.0
  (Path B: drop `c.mu.Lock` from tx lifetime), which eliminates the
  whole bug class.

### Added - Tx-side bulk read accessors (2026-05-14)

- **27 new `(*GraphTx)` methods** in
  `pkg/graph/internal/core/tx_consistent_reads.go`:
  - Nodes: `GetNodesByIDs`, `AllNodes`, `NodesByLabel`,
    `NodesByLabelAndProperty`, `NodeCount`, `NodeCountByLabel`.
  - Rels: `GetRelsByIDs`, `AllRels`, `RelsByType`, `OutgoingRels`,
    `IncomingRels`, `OutgoingRelsForNodes`, `IncomingRelsForNodes`,
    `RelCount`, `RelCountByType`.
  - Temporal: `NodesAt`, `RelsAt`, `NodesByLabelAt`, `NodesDuring`,
    `RelsDuring`, `NodeAt`, `RelAt`, `NeighborsAt`, `OutgoingRelsAt`,
    `IncomingRelsAt`, `NodesByLabelPropertyAt`,
    `NodesByLabelPropertyDuring`, `RelsByTypePropertyAt`,
    `RelsByTypePropertyDuring`.
  - AsOf (bitemporal): `NodeAsOf`, `RelAsOf`, `NodesAsOf`, `RelsAsOf`.
  - Stats: `Stats`, `AllLabelCounts`, `AllRelTypeCounts`.
  - Misc: `SearchNearest`, `IndexProviders`, `ListShards`, `EventBus`,
    `AsyncEventBus`, `Constraints`.
- Each guards only `tx.mu`, dispatches to the lock-free
  `*Locked`/`*Unlocked` helper, and matches the corresponding non-tx
  accessor's zero-value return contract after Commit/Rollback.

## [4.0.1] - 2026-05-14

### Fixed - Tx-read deadlock against c.mu (2026-05-14)

- **`g.Nodes.Labels`, `g.Nodes.HasLabel`, `g.Nodes.PrimaryLabel`,
  `g.Rels.Type`, `g.Rels.HasType`, `g.Resolve.NodeProperty`, and
  `g.Resolve.RelProperty` deadlocked when called from inside an open
  `*GraphTx`.** Every accessor takes `c.mu.RLock`; `BeginTx` holds
  `c.mu.Lock` for the whole tx lifetime; `sync.RWMutex` is not reentrant
  (lesson 9), so the read accessor blocked forever waiting for a lock the
  same goroutine already owns through the tx. Operationally critical:
  every Cypher `CREATE ... RETURN n` flow that resolves labels on the
  returned entity inside the tx hit this.
- **Fix.** Added tx-side mirrors that call the lock-free `*Unlocked`
  helpers directly without re-entering `c.mu`. See "Added".
- The non-tx accessors are unchanged — they remain correct outside a tx.
  Inside a tx, callers must now use the `*GraphTx` mirrors.

### Added - Tx-side resolution and shadow-property accessors (2026-05-14)

- **`(*GraphTx).Labels(n)`, `PrimaryLabel(n)`, `HasLabel(n, name)`,
  `RelType(r)`, `HasType(r, name)`, `NodeProperty(n, key)`,
  `RelProperty(r, key)`.** Mirror the seven `g.{Nodes,Rels,Resolve}.*`
  read accessors that previously deadlocked when called inside a
  transaction. Each guards only `tx.mu` (returns the zero value of its
  return type after Commit/Rollback) and dispatches to the existing
  `nodeLabelsUnlocked`, `nodePrimaryLabelUnlocked`, `nodeHasLabelUnlocked`,
  `relTypeUnlocked`, `relHasTypeUnlocked` helpers plus two new
  `nodePropertyUnlocked` / `relPropertyUnlocked` extracted from the
  inline `ResolveOps.NodeProperty` / `RelProperty` switch bodies.
- Lives in `pkg/graph/internal/core/tx_consistent_reads.go` next to the
  existing `tx.Export` / `tx.Snapshot` / `tx.VerifyShard` consistent-read
  family.

## [4.0.0] - 2026-05-14

### Changed - Module path bump v3 → v4 (2026-05-14)

- **`go.mod` module path** rewritten from `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3` to `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4`. All 372 Go source files updated to the new import path. Historical CHANGELOG entries under `[3.2.0]` and `[3.0.54]` retain their original `/v3` references as accurate snapshots of those releases.
- **Documentation refresh for v4.** README, AGENTS, CLAUDE, and `docs/{architecture,persistence,SPEC}.md` updated to v4 branding: heading, intro prose, ecosystem-table labels, status paragraph. `docs/architecture.md` adds a `g.Tier` row to the sub-API table and corrects the `g.Admin` row to its v4 backend-agnostic surface (`Reset`, `DecomposeNodeID`, `DecomposeRelID`). Sub-API field-accessor count updated `13 → 14` across CLAUDE.md, AGENTS.md, and `docs/architecture.md`.
- **CLAUDE.md consolidation.** Collapsed the duplicated `pkg/graph/<sub-api>/` and `pkg/graph/<types-package>/` tables into a single table; merged the `BadgerStoreConfig` / `Graph.Config` duplication; replaced the inline v4 migration recipe with a pointer to this section. ~30 lines tighter, no rule removed. `git log --follow` still works.

### Changed - API 4.0 surface cleanup (2026-05-14)

Breaking changes — all customer call sites need updating. The shape changes
below are concentrated in `pkg/graph/{nodes,rels,admin,resolve,io,index,temporal,events}`
and the new `pkg/graph/tier` sub-API.

**Migration recipe (sed-driven):**

```
# 1. Collapse *WithContext methods to context-aware base names.
sed -i '' '
s/\.AddWithContext(/.Add(/g
s/\.AddByIDWithContext(/.AddByID(/g
s/\.AddByIDIfAbsentWithContext(/.AddByIDIfAbsent(/g
s/\.GetWithContext(/.Get(/g
s/\.UpdateWithContext(/.Update(/g
s/\.UpdateInPlaceWithContext(/.UpdateInPlace(/g
s/\.DeleteWithContext(/.Delete(/g
s/\.CompareAndSetPropertyWithContext(/.CompareAndSetProperty(/g
' **/*.go

# 2. Insert context.Background() in any remaining no-context call to the
# collapsed methods. Use a context-aware editor or compile-loop driven sed.

# 3. Tiered-only Admin methods moved to a new g.Tier sub-API.
sed -i '' '
s/\.Admin\.Archive(/.Tier.Archive(/g
s/\.Admin\.Restore(/.Tier.Restore(/g
s/\.Admin\.ForceRotate(/.Tier.ForceRotate(/g
s/\.Admin\.ListShards(/.Tier.ListShards(/g
s/\.Admin\.RebuildCatalog(/.Tier.RebuildCatalog(/g
s/\.Admin\.Repair(/.Tier.Repair(/g
s/\.Admin\.VerifyShard(/.Tier.VerifyShard(/g
' **/*.go

# 4. Temporal long-form Relationships* names collapsed to short Rels*.
sed -i '' '
s/\.Temporal\.RelationshipsAt(/.Temporal.RelsAt(/g
s/\.Temporal\.RelationshipsByTypeAt(/.Temporal.RelsByTypeAt(/g
s/\.Temporal\.RelationshipsDuring(/.Temporal.RelsDuring(/g
' **/*.go

# 5. NextVersion/PreviousVersion renamed for unambiguous ordering.
sed -i '' '
s/\.NextVersion(/.VersionAfter(/g
s/\.PreviousVersion(/.VersionBefore(/g
' **/*.go

# 6. io.Import collapsed to single signature.
# For old IO.Import(r) no-options calls: append ", tkgio.ImportOptions{}".

# 7. Admin.DecomposeID(snowflake.ID) split into typed variants.
# Replace .Admin.DecomposeID(nodeID.SnowflakeID()) with .Admin.DecomposeNodeID(nodeID).
# Replace .Admin.DecomposeID(relID.SnowflakeID()) with .Admin.DecomposeRelID(relID).

# 8. Stats.Get now returns (GraphStats, error). Callers that ignored the
# error before need `, _ :=` (or `, _ =` for re-assignment). Inline field
# accessors like `g.Stats.Get().NodesRead` must split into two lines:
#   snap, _ := g.Stats.Get()
#   v := snap.NodesRead
```

**Complete change list:**

- `pkg/graph/nodes`: `Add`, `Get`, `Update`, `UpdateInPlace`, `Delete`,
  `CompareAndSetProperty` now take `ctx context.Context` as first arg.
  `*WithContext` variants removed. `NextVersion` renamed `VersionAfter`,
  `PreviousVersion` renamed `VersionBefore`.
- `pkg/graph/rels`: same collapse for `Add`, `AddByID`, `AddByIDIfAbsent`,
  `Get`, `Update`, `UpdateInPlace`, `Delete`, `CompareAndSetProperty`. Same
  version-helper rename.
- `pkg/graph/temporal`: `RelationshipsAt` → `RelsAt`, `RelationshipsByTypeAt`
  → `RelsByTypeAt`, `RelationshipsDuring` → `RelsDuring` (matches the
  package, ID-type, and sub-API-field name `Rels`).
- `pkg/graph/admin` → split into `admin` (generic: Reset, DecomposeNodeID,
  DecomposeRelID) plus new `pkg/graph/tier` (tiered-only: Archive, Restore,
  ForceRotate, ListShards, RebuildCatalog, Repair, VerifyShard).
  `g.Admin.DecomposeID(snowflake.ID)` replaced with typed
  `g.Admin.DecomposeNodeID(types.NodeID)` and `g.Admin.DecomposeRelID(types.RelID)`.
  Top-level `graph.DecomposeID` package var likewise replaced by typed
  `graph.DecomposeNodeID` / `graph.DecomposeRelID` helpers.
- `pkg/graph/resolve`: token methods `LabelToken`, `RelTypeToken`,
  `LookupLabel`, `LookupRelType` removed (leaked uint16 representation).
  Shadow-property accessors `NodeProperty`, `RelProperty` kept.
- `pkg/graph/index`: `LegacyIndexProvider` interface + `RegisterLegacyProvider`
  removed. External providers must migrate to `IndexProvider` (with optional
  `Initializable` for bulk-load).
- `pkg/graph/io`: `IO.Import(r)` and `IO.ImportWithOptions(r, opts)`
  collapsed into single `IO.Import(r, opts)`. Pass `ImportOptions{}` for
  the previous default behaviour.
- `pkg/graph/subapi.go`: new `BatchAPI.Run(fn) (*BatchResult, error)` —
  parallel to `TxAPI.Run`. `TxAPI.Begin` docstring strengthened to direct
  callers to `Run`.
- `pkg/graph/stats`: `Stats.Get()` now returns `(GraphStats, error)`
  instead of `GraphStats`. Every other `g.Stats.*` method already returned
  an error; collapsing this last odd-one-out lets callers treat the sub-API
  uniformly. Migrate with `, _ :=` (or split into two lines when the result
  was field-accessed inline — `snap, _ := g.Stats.Get(); v := snap.Field`).
  In practice the error path is `ErrNilGraph` (zero-value API) or
  `ErrGraphClosed`; runtime callers can keep ignoring it with `, _`.
- `graph.ErrDepthTemporalUnsupported` removed (legacy sentinel, never
  returned in production).

### Fixed - Temporal adjacency O(history) fold (2026-05-14)

- **`g.Temporal.OutgoingRelsAt`, `IncomingRelsAt`, `NeighborsAt`** no longer
  scan the entire rel-history ID space to catch deleted-but-historically-
  valid edges. The adjacency-at-t fold now uses a new optional store
  capability, `DeletedIterationCapability` (with depth-aware companion
  `DepthDeletedIterationCapability`), that yields ONLY IDs whose history
  exists but whose current row is absent. Cost drops from O(total history)
  to O(deleted_count). Rel endpoints are immutable, so a rel that ever
  pointed at a node still does if alive — the only rels missing from the
  live adjacency index are deleted ones.
- **Store contract additions (`pkg/graph/store/capabilities.go`).**
  - `ForEachDeletedNodeID(fn) error` / `ForEachDeletedRelID(fn) error`
  - `ForEachDeletedNodeIDByDepth(depth, fn) error` /
    `ForEachDeletedRelIDByDepth(depth, fn) error`
  - All three in-tree backends (memory, badger, tiered) implement the
    capability. External stores that omit it transparently fall back to
    the previous history-scan path — correct, just slower.
- Label/property temporal queries (`g.Nodes.ByLabel(opts)`,
  `ByLabelAndProperty(...)`, etc.) keep the full-history candidate fold
  because an entity's CURRENT label/property can differ from its at-t state
  — those queries can match entities whose history overlaps the predicate
  even when the entity is currently live with different labels.

### Fixed - PublishBatch priority ordering under queue saturation (2026-05-14)

- **`events.AsyncEventBus.PublishBatch`** now preserves its documented
  priority guarantee even when a priority queue saturates mid-batch under
  `BackpressureBlock`. Previously, the in-batch wake-up that drains space
  (necessary to avoid deadlock when `QueueSize < per-priority batch size`)
  could let the dispatcher pick up a pre-existing lower-priority event
  between batch enqueues, inverting the "no lower-priority event before all
  higher-priority batch events are visible" contract.
- A per-batch priority ceiling (atomic `batchPriorityCeiling` on the bus)
  is raised for each priority pass and cleared at end-of-batch; the
  dispatcher honours it so the in-batch wake-up drains only same-or-higher
  priorities. Liveness preserved (existing
  `TestAsyncEventBusPublishBatchBlockWakesBeforeFullQueueWait` still passes).

### Added - Temporal directional accessors (2026-05-14)

- **`g.Temporal.OutgoingRelsAt(nodeID, t)` and `g.Temporal.IncomingRelsAt(nodeID, t)`.**
  Return the relationships incident to `nodeID` in the chosen direction that
  were valid at instant `t`. History-aware: includes rels that have since
  been deleted but were valid at `t`, and returns the version-at-`t` (not
  the most recent) of each. Sorted by relationship ID. Returns
  `ErrNodeNotFound` if the node was not valid at `t`. Endpoint immutability
  is exploited — start/end on the returned versions equal the routing
  endpoint by construction.

### Fixed - Hardening sweep correctness items (2026-05-14)

- **Relationship endpoint-lock retry now bounded.** `lockRelationshipCurrentEndpoints`
  in `pkg/graph/internal/core/relationship_update.go` retries at most 10 times
  before returning an "endpoints changed after N retries" error, matching
  `deleteNodeInternal`'s adjacency-retry pattern. The previous unbounded loop
  could deadlock the caller under a hostile concurrent delete+reimport workload.
- **Relationship import event-dispatch parity.** `RelOps.Import` guards its
  `dispatchEvent` call with `ep != nil` to match every other create path.
  Functionally safe before (dispatchEvent is nil-safe) — pure parity fix.
- **Property hash contract documented for NaN.** New `TestAppendPropertyValue_Float{32,64}_NaN_BitsPreserved`
  tests document the intentional design: equal-by-CAS NaN values can hash
  differently because the hash preserves the IEEE-754 bit pattern. Mirrors
  the existing tested `±0` case. `PropertyValueEqual` and the property-hash
  function carry expanded doc comments calling out the cross-system implication.
- **Registry capacity warning fires on import.** `LabelRegistry.ImportNames`
  and `RelTypeRegistry.ImportNames` now trip `warnOnce` when a restore
  pushes the registry past `tokenCapacityWarning`. Before this fix, large
  restores could exceed the threshold without any operator-visible signal.
- **Tiered store doc-only hardening.** `temporalIndexShardRefsLocked`,
  `EndpointIntegrityHashes`, `recordBackgroundError`/`backgroundError`,
  and `DeleteNodeWithHistory` now document — respectively — the
  `ts.mu → archiveMu` lock-ordering contract, the self-loop / same-shard /
  cross-shard branch semantics, the sticky poison-pill behaviour and its
  network-filesystem implication, and the partial-state error semantics
  for the "shard delete failed but node is gone" race.
- **`reconstructTypedValue` documents the trust contract.** The unchecked
  path is intentionally best-effort for legacy data (Type=ptUnknown=0); the
  checked path (`validatePropertyWire`) rejects unknown tags before reaching
  this function.

### Fixed - Index create rollback cleanup (2026-05-14)

- **Graph-level index creates now remove partial indexes after backend failure.**
  `Index.CreateProperty`, `CreateTemporal`, `CreateHighFrequency`, and
  `CreateVector` no longer leave a stale index behind after a failed create,
  including the new-label path where the label token is rolled back and later
  reused. If cleanup itself fails after a new-label allocation, the label token
  is retained and the combined error is returned so the stale index cannot be
  inherited by a different label. Duplicate/conflict create errors preserve the
  existing index instead of treating it as cleanup state.

### Fixed - Relationship create rollback cleanup (2026-05-14)

- **Relationship create paths now remove partial rows after backend failure.**
  `Rels.Add`, `AddByIDWithContext`, `AddByIDIfAbsentWithContext`,
  `Import`, and batch relationship creates delete a row that may have been
  installed before a backend returned an error, then roll back newly allocated
  relationship-type tokens. If row cleanup itself fails after a new reltype was
  allocated, the token is retained and the combined error is returned so the
  stale row cannot be inherited by a later relationship type; transaction
  callers also receive the live partial relationship so rollback can delete it.

### Fixed - Node create rollback cleanup (2026-05-14)

- **Node create and import paths now remove partial rows after backend
  failure.** `Nodes.Add`, `Import`, and batch node creates delete node rows
  that may have been installed before a backend returned an error, then roll
  back newly allocated label tokens. If row cleanup itself fails after a new
  label allocation, the label token is retained and the combined error is
  returned so the stale row cannot be inherited by a later label; transaction
  callers also receive the live partial node so rollback can delete it.
- **Create operation counters now count committed rows even when a trailing
  registry checkpoint fails.** Standalone and transaction create paths now
  update `NodesAdded`/`RelsAdded` at the point a row is known to be live, instead
  of relying on transaction-only compensating increments.
- **Batch create result counters now distinguish live rows from errored
  operations.** `BatchResult.Created` and graph create stats now include rows
  that were actually installed even when the operation also reports a trailing
  checkpoint or cleanup error through `Failed`/`Errors`.
- **Standalone create events now follow committed row state.** `Nodes.Add`,
  `Nodes.Import`, `Rels.Add`, `AddByID`, `AddByIDIfAbsent`, and `Import` now
  publish create events when a row is committed but a trailing registry
  checkpoint error is returned, matching transaction and batch event semantics.

### Fixed - Property query value validation (2026-05-14)

- **Property query APIs now reject malformed query values before empty result
  shortcuts.** `NodesByLabelAndProperty`, `NodesByLabelPropertyAt`,
  `NodesByLabelPropertyDuring`, `RelsByTypePropertyAt`, and
  `RelsByTypePropertyDuring` validate unsupported property values such as
  structs or pointers instead of silently treating them as non-indexable misses;
  valid non-indexable values still return an empty result. Store-level
  `NodesByLabelAndProperty` implementations in MemoryStore, BadgerStore, and
  TieredStore enforce the same boundary.

### Fixed - Typed nil property container preservation (2026-05-14)

- **Property deep-copy and vector-copy boundaries now preserve typed nil
  built-in slices and maps.** `PropertySlice.Set`, `NewPropertySlice`,
  `DeepCopy`, `ToMap`, and the node/relationship `Float32SlicePropertyCopy`
  helpers no longer turn values such as `[]float32(nil)` or
  `map[string]any(nil)` into non-nil empty containers, keeping exact property
  equality and CAS semantics stable. Property wire encoding now carries an
  explicit typed-nil marker for slice/map tags, and integrity hashing now
  distinguishes typed nil containers from empty containers.

### Fixed - Transaction label failed preflight snapshots (2026-05-14)

- **`GraphTx.AddNodeLabel` and `GraphTx.RemoveNodeLabel` now check no-op and
  rejection cases before capturing rollback history.** Idempotent add-label,
  too-many-labels, closed-node, missing-label, and last-label failures no longer
  copy node history or record update snapshots, and unknown remove-label inputs
  now match the standalone error precedence.

### Fixed - Absent-property delete update no-ops (2026-05-14)

- **Deleting a property that is already absent is now a true read-only no-op.**
  Standalone, in-place, transaction, and batch node/relationship update paths no
  longer write rows, append version history, increment update counters, publish
  update events, or count batch updates for absent-property delete requests.
  Metadata-only provenance updates and deletes of existing nil-valued
  properties remain real mutations.

### Fixed - Update/CAS invalid-ID error precedence (2026-05-14)

- **Existing-entity update and property-CAS paths now reject zero/negative IDs
  before validating update payloads.** Standalone, in-place, transaction, and
  batch node/relationship updates now consistently surface the store invalid-ID
  sentinel even when the supplied update map or CAS key/value is also malformed.

### Changed - Property equality helper for CAS (2026-05-13)

- **Property CAS now compares through the type layer without copying stored
  reference values.** `types.PropertyValueEqual`,
  `(*types.Node).PropertyValueEqual`, and
  `(*types.Relationship).PropertyValueEqual` expose the exact-type, NaN-aware
  property equality used by node and relationship CAS for callers that need
  compare-only semantics without receiving a defensive copy.

### Fixed - Tiered cold-shard read fanout descriptor pressure (2026-05-13)

- **Bulk tiered read fanout now processes cold event shards sequentially while
  keeping hot/warm shards parallel.** Depth-all scans still close transient
  cold shard handles after each read, and they no longer open up to 16 cold
  Badger stores at once while walking historical shards.
- **Tiered temporal-index create/drop operations now walk shards one at a time
  instead of pinning every cold shard for the full operation.** Store-wide
  temporal and high-frequency index changes still block `Close` until rollback
  or metadata persistence is complete, but historical cold shard handles are no
  longer accumulated across the whole shard set.

### Fixed - Tiered archive reference-entity contract (2026-05-13)

- **`tiered.Store.ArchiveNode` and `RestoreNode` now explicitly reject
  non-reference nodes with `ErrNotReferenceEntity`.** Event nodes no longer
  surface as missing simply because they are physically stored outside the
  reference shard, and malformed archive/ref-shard rows with event labels are
  rejected without moving data.

### Fixed - Tiered catalog rebuild cold-shard handle pressure (2026-05-13)

- **`tiered.Store.RebuildCatalog` and the shared all-shard admin enumerator now
  close cold event shards that they open only for the active operation.**
  Rebuilding stats, repairing shards, or applying tier-wide index changes
  across historical shards no longer leaves transient Badger handles open after
  release.
- **Tiered point routing now also releases transient cold event shards after the
  routed operation checks in.** Single-node lookups and relationship owner
  resolution no longer keep a lazy-opened cold shard resident until idle close.

### Fixed - Tiered clear catalog consistency (2026-05-13)

- **`tiered.Store.Clear` now keeps the live shard catalog aligned with cleared
  shard data when the final catalog save fails.** The error is still returned,
  but the in-process catalog no longer restores stale pre-clear counts and
  verification flags after the shards have already been wiped.

### Fixed - Hash verification nil history rows (2026-05-13)

- **`g.Hash.VerifyNodeChain` and `g.Hash.VerifyRelChain` now reject nil
  history rows without panicking.** Malformed backend history slices and paged
  history results now return the corresponding nil-entity sentinel instead of
  dereferencing a nil node or relationship while verifying the hash chain.

### Fixed - Export history nil-row fail-closed behavior (2026-05-13)

- **`g.IO.Export` now rejects nil node and relationship history rows without
  panicking.** Corrupt or custom backend history slices with nil entries now
  return the corresponding nil-entity sentinel instead of dereferencing the
  row while formatting the encode error.

### Fixed - Closed graph event getter lifecycle (2026-05-13)

- **`g.Events.GetSync()` and `g.Events.GetAsync()` now fail closed after
  `Graph.Close`.** No-error event-bus getters no longer expose stale
  graph-owned event pointers after the graph lifecycle has ended.

### Fixed - Index provider Close panic isolation (2026-05-13)

- **Graph provider lifecycle paths now isolate index provider `Close` panics.**
  A panicking provider close during `Graph.Close`, `g.Index.UnregisterProvider`,
  or failed-`Init` rollback no longer aborts surrounding cleanup; the panic is
  returned as part of the lifecycle error.

### Fixed - Index provider Init panic rollback (2026-05-13)

- **`g.Index.RegisterProvider` now rolls back providers whose `Init` panics.**
  A panicking initializable index provider no longer remains registered or
  subscribed to future graph events after `RegisterProvider` returns an error.

### Fixed - If-absent relationship row ownership (2026-05-13)

- **`g.Rels.AddByIDIfAbsent` now returns a defensive copy when the existing
  relationship comes from a custom store.** The duplicate-check path no longer
  exposes adjacency rows owned by external store implementations.

### Fixed - Delete-node tombstone row ownership (2026-05-13)

- **Cascade node deletes now build relationship tombstones from deep copies.**
  Failed `DeleteNodeWithHistory` calls no longer mutate adjacency relationship
  rows returned by custom stores while preparing tombstone history.

### Fixed - Registry persistence panic cleanup (2026-05-13)

- **Registry token allocation paths now release the registry mutex if backend
  registry persistence panics.** Callers that recover from a custom store's
  `SaveRegistries` panic no longer leave later label or relationship-type
  operations deadlocked.

### Fixed - Tiered index metadata load validation (2026-05-13)

- **Tiered temporal and vector index metadata loads now reject definitions that
  the save path would reject.** Duplicate temporal labels, duplicate
  high-frequency labels, temporal/HFI conflicts, and duplicate vector
  definitions no longer get silently accepted or de-duplicated on restart.

### Changed - Badger temporal vector search validation order (2026-05-13)

- **Badger vector search with temporal filters now validates query vectors
  before reading candidate node rows.** Invalid query dimensions or non-finite
  query values are no longer masked by unrelated candidate-row corruption, and
  invalid requests avoid the eager temporal-filter candidate scan. Valid
  temporal searches still propagate corrupt candidate rows instead of silently
  dropping them.

### Fixed - Async event batch backpressure wake-up (2026-05-13)

- **`AsyncEventBus.PublishBatch` now wakes the dispatcher before blocking on a
  full priority queue.** Batched async event publishing with
  `BackpressureBlock` no longer deadlocks when one priority group is larger
  than the configured queue size while the dispatcher is asleep.

### Fixed - Import registry corruption sentinels (2026-05-13)

- **Malformed IO import registry records now wrap `ErrCorruptExport`.**
  Invalid reserved registry slots, duplicate label names, duplicate relationship
  type names, and other registry wire-shape failures now match the corrupt
  export sentinel contract instead of surfacing as raw registry validation
  errors. Incompatible but well-formed non-empty destination registries still
  return `ErrIncompatibleRegistry`.

### Changed - Hash verification history paging (2026-05-13)

- **`g.Hash.VerifyNodeChain` and `g.Hash.VerifyRelChain` now stream history
  through wrapper-aware version pages when safe.** Deep hash-chain verification
  no longer has to materialize full per-entity history on native or direct
  pager stores, while wrapper stores that override full-history reads still
  keep those hooks observable.

### Fixed - Badger read-only mutation guard (2026-05-13)

- **Read-only `badger.Store` handles now reject mutating APIs before touching
  in-memory state.** Direct writes, non-empty batch writes, history mutations,
  index definition changes, registry saves, `Clear`, and split relationship
  repair helpers now fail with `ErrInvalidStoreMutation` instead of appearing to
  succeed in memory while being impossible to flush to the read-only Badger DB.
- **Empty tiered put batches no longer rotate shards.** `PutNodesBatch(nil)`,
  `PutNodesBatch([])`, `PutRelationshipsBatch(nil)`, and
  `PutRelationshipsBatch([])` now return as read-only no-ops on an open store,
  even when the hot shard window has expired.
- **Tiered shard catalog accessors now deep-copy slice fields at catalog
  ingress and egress.** `AddShard`, `GetShard`, `HotEventShard`, `EventShards`,
  and `ColdEventShards` no longer let caller-owned or returned `Labels` /
  `RelTypes` slices mutate catalog-owned metadata outside the catalog lock.
- **Tiered shard catalog saves now validate the current topology before
  writing.** `ShardCatalog.Save` no longer persists invalid in-memory shard
  metadata that `Load` would reject on restart, and rejected saves leave the
  existing catalog file unchanged.
- **Tiered shard catalog loads now validate before mutating live catalog state.**
  A corrupt or invalid catalog file no longer leaves rejected shard metadata in
  the in-memory catalog after `Load` returns an error.
- **In-memory tiered shard catalogs now treat `Load` as a no-op, matching
  `Save`.** Direct `NewShardCatalog("")` callers no longer get an OS read error
  for the explicit non-persistent catalog mode.
- **Tiered registry file saves now validate registry names before writing.**
  The flat registry writer no longer persists invalid label or relationship
  type slices that the loader would reject on the next open, and rejected saves
  leave the previous registry file bytes intact.
- **Tiered temporal and vector index metadata saves now validate definitions
  before writing.** Invalid label tokens, conflicting tracked temporal/HFI
  definitions, invalid HFI bucket sizes, invalid vector dimensions/metrics, and
  invalid vector property keys now fail before the metadata files are changed.

### Fixed - Custom property integrity hash type separation (2026-05-13)

- **Custom property integrity hashing now includes the registered custom type
  name and pointer/value shape before the custom `HashBytes()` payload.** Two
  different registered custom property types that intentionally or accidentally
  return identical `HashBytes()` output no longer collapse to the same property
  hash bytes.
- **`RegisterPropertyStructType` now rejects non-struct element types.** Named
  scalar or other non-struct types that implement the custom property
  interfaces can no longer enter the struct registry and then behave
  inconsistently across validation and wire reconstruction.

### Fixed - Badger split relationship delete liveness (2026-05-13)

- **`badger.Store.DeleteRelEntityAndOut` now verifies the relationship is still
  live in the in-memory relationship set before reading or deleting the
  persisted row.** Split cross-shard cleanup no longer lets a stale Badger row
  drive type/outgoing index cleanup or relationship-counter changes after the
  live relationship set has already rejected the ID.
- **`badger.Store.DeleteRelIncoming` now verifies the stored incoming-entry
  relationship type before dropping the in-memory index entry.** A stale or
  mismatched split-delete descriptor now fails with `ErrRelNotFound` instead of
  deleting memory while leaving the persisted incoming key behind.
- **Badger incoming-index repair deletes every pending and persisted key for an
  end-node/relationship pair.** Repair cleanup no longer leaves duplicate
  incoming keys behind when memory can represent only one entry for that pair.
- **Tiered `RunRepair` now validates incoming-index entries against the actual
  relationship row.** Stale incoming entries with the wrong end shard or
  relationship type are removed before missing correct entries are recreated.

### Fixed - Interface-embedded store wrapper capability dispatch (2026-05-13)

- **Graph optional-capability fast paths now distinguish declared wrapper
  methods from methods merely promoted through an embedded native `store.Store`
  interface.** Custom stores that directly implement a capability keep the fast
  path, while wrappers that only inherit native optional methods fall back
  through the graph-layer path so wrapper overrides remain observable.
- **Import rollback history-suffix snapshots now use the same wrapper-aware
  history pager selection as export.** Trim-capable custom wrappers that only
  inherit an in-tree history pager no longer bypass their mandatory
  `Get*History` hooks while preparing rollback state.
- **Depth-scoped temporal history iteration now uses cached, wrapper-aware
  capability selection.** Tiered-store wrappers that only inherit the in-tree
  `DepthHistoryIterationCapability` no longer bypass their mandatory
  `ForEach*HistoryID` hooks during graph-layer depth-filtered temporal scans,
  and Core avoids repeated reflection on this temporal-query path.

### Fixed - IO short-write handling (2026-05-13)

- **Export record writes, import stage spills, and tiered atomic file writes
  now handle short `io.Writer` writes explicitly.** Partial writes with nil
  errors are retried until complete, and zero-progress writers fail closed with
  `io.ErrShortWrite` instead of reporting success after producing truncated
  export or persistence bytes.

### Fixed - Tiered write lifecycle sentinels (2026-05-13)

- **Tiered node, relationship, label-mutation, and history write APIs now check
  store lifecycle before validating malformed mutation payloads.** Nil tiered
  stores return `ErrNilStore` and closed tiered stores return `ErrStoreClosed`
  consistently across these write surfaces instead of leaking payload
  validation errors first.

### Fixed - Relationship rollback endpoint restore (2026-05-13)

- **Transaction rollback now restores relationship type/start/end index tuples
  atomically after delete/import-same-ID sequences.** If a transaction deletes a
  relationship, imports the same relationship ID with the same type but
  different endpoints, and then rolls back, the original current row and
  outgoing/incoming adjacency indexes are restored instead of leaking the
  store-layer indexed-field mutation error.

### Fixed - Tiered clear metadata failure ordering (2026-05-13)

- **Tiered `Store.Clear()` now proves persistent vector and temporal index
  metadata can be cleared before deleting shard data.** If metadata deletion
  fails, `Clear` returns the metadata error without erasing existing entities or
  dropping the in-memory index tracking, avoiding a partially cleared store when
  the failure is known before destructive shard work begins.

### Fixed - Tiered migration destination emptiness (2026-05-13)

- **`MigrateFromBadger` now rejects tiered destinations with node or
  relationship history even when they have no current rows.** Migration no
  longer treats live counts alone as proof of an empty destination, preventing
  stale history from being mixed with newly migrated current rows.

### Added - Async event bus getter parity (2026-05-13)

- **`g.Events.GetAsync()` now mirrors `GetSync()` for asynchronous event buses.**
  The events sub-API can now retrieve either installed bus kind without exposing
  the internal publisher interface.

### Fixed - GraphTx relationship read parity (2026-05-13)

- **`GraphTx.GetRelationship` now mirrors `GraphTx.GetNode` for transaction-scoped
  reads.** Relationship reads inside transaction callbacks no longer need to use
  standalone APIs that wait behind the transaction's write lock.
- **Committed transaction-scoped reads now update read counters.** `GraphTx.GetNode`
  increments `NodesRead` on success, `GraphTx.GetRelationship` increments
  `RelsRead` on success, and rollback restores the pre-transaction counter
  snapshot.
- **Transaction-scoped reads now validate IDs before store lookup.**
  `GraphTx.GetNode` and `GraphTx.GetRelationship` reject zero or negative IDs
  at the graph layer, matching the standalone direct-read APIs.
- **Bulk direct-ID reads now update read counters.** Successful
  `g.Nodes.GetByIDs` and `g.Rels.GetByIDs` calls increment `NodesRead` and
  `RelsRead` by the number of rows returned, matching the scalar direct-read
  APIs.

### Fixed - Admin reset consistency (2026-05-13)

- **`g.Admin.Reset()` now clears graph operation counters and checkpoints the
  preserved registry snapshot after clearing the backing store.** This aligns
  Reset with its API contract on both in-memory and persistent stores.

### Fixed - Checked property wire serialization (2026-05-13)

- **Checked property wire conversion now rejects unsupported non-nil values
  instead of writing them with the legacy unknown type tag.** Normal public
  entity setters already prevent those values, but invariant-broken property
  slices now fail closed at the serialization boundary instead of producing
  ambiguous `ptUnknown` wire data.

### Fixed - Property equality index canonicalization (2026-05-13)

- **Float property equality indexes now canonicalize signed zero and NaN for
  `float32` and `float64` values.** `Nodes.ByLabelAndProperty` now treats
  stored `-0.0` and queried `+0.0` as the same exact-type scalar value, and
  treats NaN payload variants as equal within the same concrete float type, on
  both fallback scans and property-index lookups. `float32` and `float64`
  values remain distinct, matching the compare-and-set equality contract.

### Fixed — Checked integrity hash recovery (2026-05-13)

- **Checked node and relationship integrity hashing now recovers unsupported
  property-value invariant breaks, not only panics from custom `HashableValue`
  implementations.** This keeps graph mutation and verification paths on the
  error-returning checked hash surface when an external or corrupt row carries
  a property shape that normal public setters would have rejected.

### Changed — Tiered read fanout cold-shard handles (2026-05-13)

- **Tiered read fanout now closes cold shards that were opened only for a
  single read scan.** Depth-wide node, relationship, label/property, and history
  reads still include cold event shards, but a scan no longer leaves every
  lazily opened historical Badger handle resident until the idle-close timer.
  Cold shards that were already open keep the normal idle-close behavior.

### Changed — Badger transaction rollback history trim (2026-05-13)

- **Badger-backed transactions now use version-range history trimming during
  rollback instead of snapshotting full per-entity history chains on first
  mutation.** `badger.Store` implements the internal
  `HistoryRollbackTrimCapability`, and Core enables that fast path only for the
  exact native Badger store so wrapper stores still observe their
  `Truncate*History` / `Put*Version` hooks.
- **Badger history maintenance now honors `SyncWrites`.** `TruncateNodeHistory`,
  `TruncateRelHistory`, and the rollback trim helpers flush their queued history
  deletes immediately when sync writes are enabled, matching the rest of the
  Badger write surface.
- **Transaction update rollback preserves pre-existing current-version history.**
  Update snapshots now materialize history instead of using the trim fast path
  when the store already has a history row for the entity's current version,
  matching the delete rollback guard and preserving Node/Relationship parity.
- **Import replay rollback now snapshots only touched history suffixes on
  trim-capable stores.** Non-empty imports that touch a high version no longer
  need to deep-copy an entity's entire pre-existing history chain; rollback
  trims from the first imported version and restores the captured suffix. Stores
  without the trim capability keep the full-history fallback.
- **Export history paging now respects store-wrapper boundaries.** `IO.Export`
  still uses `HistoryVersionPageCapability` for exact native stores and direct
  external implementations, but concrete wrappers that only inherit an in-tree
  pager now fall back through their mandatory `Get*History` methods so wrapper
  faults, validation, and policy hooks remain observable.

### Fixed — Tiered history iterator snapshot bounds (2026-05-13)

- **Tiered node and relationship history iterators no longer let callback-created
  higher IDs extend the active scan.** `ForEachNodeHistoryID*` and
  `ForEachRelHistoryID*` now snapshot the highest eligible history ID before
  invoking user callbacks, matching the Badger iterator contract while keeping
  callbacks outside shard locks and checkouts. The default `DepthAll` path uses
  per-shard max-history probes rather than a full pre-iteration, preserving the
  bounded scan shape for large histories. Badger max-history probes now also
  honor pending history deletes, so async-buffered truncation cannot leave a
  stale deleted upper bound that admits callback-created history IDs.
- **Tiered `DepthAll` history `ForEach` now emits each logical history ID once
  even when the same ID has history split between the reference shard and
  archive.** The iterator now follows the bounded paginated ID merge used by
  `All*HistoryIDsFrom`, keeping callback delivery consistent with the other
  history-ID APIs.
- **Tiered restored reference entities now resolve archive-only history
  versions through `GetNodeVersion` and `GetRelVersion`.** Point version reads
  now match `Get*History` and paged history reads when archive/restore leaves
  older history in `refArchive` while the live row is back on the reference
  shard.

### Fixed — Persistent vector-index rebuild failures (2026-05-13)

- **Badger and Tiered vector-index definitions now fail closed when restart
  rebuild finds stored vectors that violate the persisted index shape.**
  Reopening a store with inconsistent vector-index metadata now returns the
  rebuild error instead of silently dropping mismatched nodes from the in-memory
  k-NN index.
- **Tiered vector-index creation now backfills only nodes carrying the indexed
  label.** This avoids full-row reads for unrelated labels and keeps corrupt
  unrelated rows from blocking creation of an otherwise valid vector index.

### Fixed — Tiered clear lifecycle errors (2026-05-13)

- **Tiered event-shard clear now propagates checkout and close-race errors
  instead of treating skipped shards as successfully cleared.** This prevents
  `Store.Clear` from reporting success after an event shard was not actually
  cleared because the store was closing or had a recorded lifecycle failure.

### Fixed — Relationship update endpoint liveness (2026-05-13)

- **Relationship update and relationship property-CAS now fail closed when an
  endpoint row is missing during endpoint-hash refresh.** These mutation paths
  now propagate `ErrNodeNotFound` instead of committing the property change with
  empty endpoint hashes.

### Fixed — Materialized temporal diff ordering (2026-05-13)

- **`g.Temporal.Diff` now returns each change slice sorted by entity ID.**
  `DiffCallback` remains a streaming API with implementation-defined callback
  order, while the materialized `SnapshotDiff` now matches the deterministic
  ordering used by snapshot and temporal query result slices.

### Fixed — Public stats snapshot alias (2026-05-12)

- **`graph.GraphStats` now aliases the type returned by `g.Stats.Get()`.**
  The top-level public alias previously pointed at the internal core stats
  struct while the sub-API returned `pkg/graph/stats.GraphStats`, so callers
  could not assign `g.Stats.Get()` to a `graph.GraphStats` variable without an
  explicit conversion despite the identical fields.

### Fixed — Public index provider sentinels (2026-05-12)

- **`pkg/graph` now re-exports the index-provider sentinel errors returned by
  `g.Index`.** `ErrIndexProviderExists`, `ErrIndexProviderNotFound`, and
  `ErrIndexProviderEmptyName` were already available from `pkg/graph/index`,
  but unlike the other `g.Index` errors they were missing from the top-level
  graph package.

### Fixed — Constraint lifecycle sentinel ordering (2026-05-12)

- **`g.Constraints.Add` and `Set` now fail closed before validating caller
  input after graph close.** Closed graphs return `ErrGraphClosed` and leave
  the installed constraint set unchanged even when the supplied constraint set
  is malformed.

### Fixed — Temporal Allen lifecycle sentinel ordering (2026-05-12)

- **`g.Temporal.NodeInterval`, `RelInterval`, `RelateNodes`, and `RelateRels`
  now participate in the graph read lifecycle gate.** After graph close these
  error-returning temporal helpers return `ErrGraphClosed` before inspecting
  caller-supplied node or relationship values.

### Fixed — External store query row ownership (2026-05-12)

- **Graph query APIs now defensively copy valid rows returned by untrusted
  external stores before exposing them to callers.** Native memory, badger, and
  tiered stores keep their existing hot-path allocation profile, while custom
  store implementations can no longer leak caller-mutable backing rows through
  validated node, relationship, adjacency, property-query, vector, or history
  read paths.

### Fixed — Strict property wire metadata validation (2026-05-12)

- **Checked store/import wire reads now reject contradictory custom-property
  metadata.** Non-custom property type tags can no longer carry ignored
  `CustomType` or `CustomPointer` fields, so malformed wire records fail closed
  instead of being silently normalized into a different property shape.
- **Tiered shard catalog loads now require canonical relative shard paths.**
  Persisted catalog entries such as `events/../reference` are rejected instead
  of being accepted as aliases for a different physical shard directory.
- **Badger index-definition reloads now de-duplicate property and temporal
  definitions before rebuilding.** Duplicate persisted definitions no longer
  trigger repeated full-row rebuild scans for the same logical index.
- **Badger temporal ID enumeration avoids full entity materialization.**
  `AllNodeIDs` and `AllRelIDs` with temporal filters now inspect temporal
  metadata and return IDs directly instead of allocating full deep-copy result
  slices that are immediately discarded, while still surfacing corrupt rows.
- **Badger index and vector query APIs now check lifecycle before argument
  validation.** Closed or nil stores consistently return the lifecycle
  sentinels on these surfaces even when callers also pass malformed index,
  vector, or query arguments.
- **Badger mutation and history APIs now use the same lifecycle-first
  sentinel ordering.** Entity writes, deletes, split relationship index
  helpers, and history write helpers no longer validate malformed payloads
  before detecting nil or closed stores.
- **Memory store mutation, history, index, and vector APIs now match the same
  lifecycle-first contract.** Typed-nil memory stores fail with `ErrNilStore`
  instead of panicking on these surfaces, and closed stores return
  `ErrStoreClosed` before malformed argument errors.
- **Memory store read and query APIs now also fail closed on nil receivers.**
  Direct reads, bulk reads, adjacency reads, history reads, iteration helpers,
  and transaction-time reads now return `ErrNilStore` instead of panicking
  before the store lifecycle gate.
- **Tiered index, vector, and property-query APIs now also check lifecycle
  first.** Nil or closed tiered stores no longer leak argument-validation
  errors on these public surfaces when the receiver itself is unusable.

### Fixed — Property CAS NaN matching (2026-05-12)

- **Node and relationship property CAS now matches accepted NaN property values.**
  `CompareAndSetProperty` keeps exact-type comparison and nil-as-absent
  semantics, but no longer treats stored `NaN` values as permanently
  unmatchable when they appear as scalar float properties or inside supported
  property containers.

### Fixed — Relationship property API parity (2026-05-11)

- **Relationship properties now support atomic compare-and-set.**
  `g.Rels.CompareAndSetProperty` and `CompareAndSetPropertyWithContext` mirror
  the node CAS contract: exact-type comparison, nil-as-absent semantics,
  reserved shadow-key rejection, final property-count validation, version
  history, endpoint-hash refresh, update stats, and relationship update events.

### Fixed — Monotonic mutation transaction timestamps (2026-05-11)

- **Core mutation timestamps are monotonic per graph instance.**
  `TxFrom`, `TxTo`, `UpdatedAt`, `DeletedAt`, and graph mutation event
  timestamps now advance when the underlying millisecond clock repeats or moves
  backwards, preserving transaction-time version intervals for fast consecutive
  node and relationship mutations without wall-clock sleeps.

### Fixed — Store wrapper native fast paths (2026-05-11)

- **Generated-ID create shortcuts no longer bypass wrapper Store overrides.**
  Node create, relationship create, and batch node create still use the native
  tiered fast path for exact in-tree stores, but embedded wrapper stores now
  route through `PutNode`, `PutRelationship`, and `PutNodesBatch` so
  instrumentation, fault injection, and custom policy hooks are honored.
- **Endpoint-hash shortcuts no longer bypass wrapper Store reads.**
  Relationship create and update paths still use native endpoint-hash reads for
  exact in-tree stores, but wrapper stores now fall back to `GetNode` so
  injected endpoint read failures and custom read policies are preserved.
- **Transaction-time and rollback-trim shortcuts now respect wrapper stores.**
  Concrete wrappers around `memory.Store` now use the mandatory
  `Get*`/history/iteration and truncate+restore rollback paths instead of
  inheriting native shortcuts that could bypass injected read or history faults.
- **Temporal vector search respects concrete Store wrappers.**
  Filtered vector search is still used for exact in-tree stores and direct
  external implementations, but wrappers that merely inherit an in-tree
  `SearchNearestFiltered` now use the graph over-fetch fallback so their
  `SearchNearestNodes` override remains visible.
- **Property equality queries respect concrete Store wrappers.**
  `Nodes.ByLabelAndProperty` still uses exact in-tree and direct external
  property-query capabilities, but wrappers that merely inherit an in-tree
  `NodesByLabelAndProperty` now use the graph fallback so their `NodesByLabel`
  override remains visible.
- **Nested Store wrappers no longer re-enable inherited native shortcuts.**
  Anonymous wrapper layers are inspected recursively before enabling native
  optional capabilities, so promoted in-tree methods do not leak through
  multi-layer instrumentation, fault-injection, or policy wrappers.
- **Tiered Store wrappers now participate in tiered graph integration.**
  Admin operations, injected-store registry rehydration, and transaction/import
  rollback label-registry rewiring now dispatch through narrow tiered capability
  interfaces instead of exact `*tiered.Store` assertions.

### Changed — Count and delete hot paths (2026-05-11)

- **Count reads avoid generic helper overhead on the graph and MemoryStore hot
  paths.**
  `Count`, `CountByLabel`, and `CountByType` still participate in graph
  lifecycle isolation and validate malformed names before empty-result
  shortcuts, but no longer allocate closures or run MemoryStore lazy-init
  checks for pure `len` reads.
- **Delete tombstones reuse caller-owned defensive copies.**
  Graph delete paths now mutate the defensive copies already returned by Store
  reads instead of deep-copying them again before `Delete*WithHistory`; native
  in-tree stores also skip the Phase A node snapshot read for unconnected node
  deletes while wrapper stores keep the conservative two-read behavior.

### Changed — MemoryStore generated relationship create fast path (2026-05-11)

- **MemoryStore relationship creates can capture endpoint hashes during the
  generated-ID write.**
  The graph-generated `AddRelationship` path now uses a MemoryStore capability
  that verifies endpoints, captures their current integrity hashes, and persists
  the relationship under one store lock instead of taking a separate endpoint
  hash read before the write.

### Fixed — Transaction-time query ordering (2026-05-11)

- **`NodesAsOf` and `RelsAsOf` now return deterministic ID-sorted results.**
  The memory-store transaction-time query path and the graph-layer fallback for
  stores without native transaction-time queries now sort by entity ID instead
  of exposing Go map iteration order.

### Fixed — Transaction rollback same-ID replacement (2026-05-11)

- **Rollback preserves pre-transaction entities when caller-specified imports
  reuse a deleted ID inside the same transaction.**
  Transaction create bookkeeping now distinguishes rows that were absent at
  transaction start from replacements of rows deleted earlier in the same
  transaction. Rollback restores original node and relationship rows, labels,
  relationship types, and registry state without deleting the restored row as a
  newly-created entity.

### Fixed — MemoryStore lazy initialization race (2026-05-11)

- **Zero-value MemoryStore concurrent first use is race-free.**
  The lazy initialization marker is now atomic, so concurrent read-path first
  use of a zero-value `memory.Store` cannot race while `sync.Once` initializes
  the backing maps.

### Changed — Read-path performance pass (2026-05-11)

- **Hot read and resolver paths avoid unnecessary helper overhead.**
  Graph read helpers no longer route through mutation/event dispatch setup,
  no-error ID allocators skip the graph lifecycle lock while preserving
  post-close zero results, initialized registries and MemoryStore instances
  skip repeated `sync.Once` checks on normal hot paths, and MemoryStore valid
  read/count calls avoid invoking full validator helpers unless an input is
  malformed.

### Fixed — Transaction-time deleted entity visibility (2026-05-11)

- **Transaction-time queries preserve deleted entities before deletion.**
  Node and relationship delete tombstones now keep the deleted live version's
  original `TxFrom` and set only `TxTo` to the delete time. `g.Temporal.NodeAsOf`,
  `NodesAsOf`, `RelAsOf`, and `RelsAsOf` can now resolve deleted entities at
  transaction times when those entities were still live without exposing future
  delete markers (`TxTo`, `ValidTo`, `DeletedAt`) before the delete transaction,
  including relationships removed by cascade node delete.

### Changed — Maintainability review round 9 (2026-05-09)

- **Allen relation sets ignore invalid relation bits.**
  `AllenRelation.Set`, `AllenRelationSet.Add`, `Union`, `Intersection`,
  `IsEmpty`, and `Len` now mask to the 13 defined Allen relations. Invalid
  relation enum values can no longer create hidden bits where `Len()` reports a
  member but `ToSlice()` and `String()` show an empty set.
- **Query depth rejects invalid enum values.**
  MemoryStore, BadgerStore, and TieredStore query methods that accept
  `QueryOpts` now return `ErrInvalidShardDepth` for unknown
  `QueryOpts.Depth` values instead of silently treating them as `DepthAll`.
  Valid `DepthHot`/`DepthWarm` values remain equivalent to `DepthAll` on
  single-shard stores because all data is in the only shard.
- **Graph-level queries reject invalid depth before empty shortcuts.**
  `g.Nodes.ByLabel`, `ByLabelAndProperty`, `All`, `g.Rels.ByType`, `All`, and
  `g.Index.SearchNearest` now validate `QueryOpts.Depth` before unregistered
  label/type/index shortcuts can return an empty successful result.
- **Graph-level interval filters require positive bounds.**
  Graph temporal query detection now matches the Store contract:
  `ValidStart`/`ValidEnd` form an interval filter only when both bounds are
  greater than zero. Non-positive pairs no longer force history-aware filtering
  that can hide otherwise-visible current entities.
- **Temporal interval ranges fail closed.**
  Active `QueryOpts` intervals now require `ValidStart < ValidEnd`; MemoryStore,
  BadgerStore, TieredStore, and graph-level query methods return
  `ErrInvalidTimeRange` for empty or reversed active intervals. Explicit
  `g.Temporal.*During` APIs also return `ErrInvalidTimeRange` when `start >= end`
  after resolving `end == 0` to the query's open-ended upper bound.
- **Negative query limits fail closed.**
  `QueryOpts.Limit` now accepts only `0` (unbounded) or positive limits.
  MemoryStore, BadgerStore, TieredStore, and graph-level query methods return
  `ErrInvalidQueryLimit` for negative limits before empty label/type/index
  shortcuts can widen the query to all results.
- **Negative query cursors fail closed.**
  `QueryOpts.After` now accepts only `0` (from start) or a non-negative entity
  cursor. MemoryStore, BadgerStore, TieredStore, and graph-level query methods
  return `ErrInvalidQueryCursor` for negative cursors before empty
  label/type/index shortcuts can widen the query to the first page.
- **History cursor scans validate raw pagination parameters.**
  `AllNodeHistoryIDsFrom` and `AllRelHistoryIDsFrom` now share the same
  non-negative cursor/limit contract: `limit == 0` means all remaining,
  positive limits cap the page, negative limits return `ErrInvalidQueryLimit`,
  and negative cursors return `ErrInvalidQueryCursor` before empty-history fast
  paths.
- **History truncation rejects negative retention.**
  `TruncateNodeHistory` and `TruncateRelHistory` still accept
  `keepVersions == 0` as the explicit clear-all request, but negative retention
  values now return `ErrInvalidStoreMutation` instead of deleting all history.
- **Import, Store, and graph mutations reject invalid entity IDs.**
  `g.Nodes.Import`, `g.Rels.Import`, and their transaction wrappers now return
  `ErrInvalidID` for negative caller-supplied IDs. Store mutation invariants now
  reject negative node IDs, relationship IDs, and relationship endpoints with
  `ErrInvalidStoreMutation` before current-row, history, index, delete/cascade,
  archive/restore, or batch mutation begins. Graph standalone, transaction,
  batch update, label mutation, property CAS, close-version, delete, and admin
  archive/restore entry points now reject zero/negative targets before lookup,
  lock, snapshot, capability checks, or queue work.
  Relationship create/import paths that accept endpoint IDs or endpoint node
  pointers now reject zero/negative endpoint IDs before endpoint locks, duplicate
  probes, or relationship-type token allocation.
- **Explicit-ID reads reject invalid IDs.**
  Graph read sub-APIs and MemoryStore, BadgerStore, and TieredStore now reject
  zero or negative IDs with `ErrInvalidStoreMutation` on `GetNode`,
  `GetRelationship`, `GetNodesByIDs`, `GetRelationshipsByIDs`, and adjacency
  read methods before not-found handling or partial batch work. Token `0` still
  means "all types" only for adjacency relationship-type filtering.
- **History reads and truncation reject invalid IDs.**
  Graph `g.Nodes.History` / `g.Rels.History` and direct Store
  `Get*Version`, `Get*History`, and `Truncate*History` methods now reject zero
  or negative entity IDs with `ErrInvalidStoreMutation` before empty-history,
  version-not-found, or no-op truncation handling.
- **Badger repair helpers reject negative IDs.**
  `DeleteRelEntityAndOut`, `PurgeOrphanRelationshipIndexes`,
  `DeleteIncomingByRelID`, and `ScanAndDeleteIncoming` now use the same
  positive-ID validators as other Store mutation boundaries, so negative
  relationship or end-node IDs return `ErrInvalidStoreMutation` before
  not-found, scanner, or no-op repair behavior.
- **Exported entity constructors no longer panic on reserved tokens.**
  `types.NewNode(..., 0, ...)`, `types.NewNode(..., labelsWithZero)`, and
  `types.NewRelationship(..., 0, ...)` now return regular entity values instead
  of panicking. Store write invariants reject those reserved token-0 payloads
  with `ErrInvalidStoreMutation` before persistence, so invalid direct-store
  inputs fail at the write boundary instead of crashing the caller.
- **Registry metadata is persisted when new tokens commit.**
  Successful graph mutations, batch execution, index creation, and IO import
  that introduce new label or relationship-type tokens now save the current
  registries to persistent Badger/Tiered stores before returning. Import and
  transaction rollback paths also persist the restored registry snapshots, so a
  restart that did not pass through `Graph.Close()` no longer reopens durable
  entity rows without their token mappings. Batch post-write registry checkpoint
  failures are reported as batch errors without re-finalizing registry locks or
  rolling caller-visible node/relationship pointers back to pre-commit
  skeletons after their rows have already been written. Transaction create and
  import methods also record created nodes/relationships for rollback whenever
  the Store write committed, even if the trailing registry checkpoint returned
  an error. `GraphTx.Commit` now retries the registry checkpoint before making
  the transaction irreversible; if that checkpoint fails, the transaction
  remains open so callers can retry commit or roll back.
- **Internal registries are zero-value safe.**
  Zero-value `LabelRegistry` and `RelTypeRegistry` now initialize the reserved
  token-0 entry lazily. Direct internal use reports `Len() == 0`, exports
  `[]string{""}`, imports persisted names, and allocates the first new token as
  `1` instead of panicking or returning token `0`.
- **Internal no-error index helpers tolerate nil and zero values.**
  `PropertyIndex` now initializes its value map on first `Add`, and nil
  property-index receivers no-op or return nil. Zero-value
  `HighFrequencyIndex` now initializes its bucket map on first `Add`; nil HFI
  receivers no-op or return zero/nil values from no-error helper methods.
  Nil `TemporalIndex` receivers also no-op or return nil/zero, and nil
  temporal-index map entries are ignored during purge helpers. Nil
  `VectorIndex` cleanup/read helpers no-op or return nil/zero, while
  vector writes/searches fail closed with `ErrInvalidVectorIndexConfig`.
  Zero-value internal LRU caches now lazily initialize as minimum-capacity
  caches instead of panicking on `Put`, `LoadClean`, `MarkDeleted`,
  `CollectDirty`, or `ResetForTest`.
- **By-ID relationship creates capture endpoint hashes.**
  `g.Rels.AddByID`, `AddByIDIfAbsent`, and their transaction variants now fetch
  live endpoints under the endpoint lock even when no graph-level constraints are
  configured. By-ID creation is now only an endpoint input-form variant: it
  verifies live endpoints, captures `FromNodeHash`/`ToNodeHash`, enforces
  configured constraints, and preserves relationship integrity metadata with the
  same semantics as `g.Rels.Add`.
- **Import staging size caps are non-negative.**
  `g.IO.ImportWithOptions` now rejects `ImportOptions.MaxStagedBytes < 0` with
  `ErrImportSizeLimit` before creating a staging file. `0` remains the explicit
  unlimited value. The cap check uses overflow-safe arithmetic, so a very large
  staged byte count cannot wrap the `staged + recordSize` calculation and bypass
  the configured cap.
- **Versioned mutations reject `uint32` version overflow.**
  Node/relationship updates, node label add/remove, node/relationship property
  CAS, transactions, and batch updates now return `ErrVersionOverflow` when the
  current entity version is already `math.MaxUint32`. The mutation fails before
  writing history, wrapping the current version to `0`, or registering a new
  label token for a rejected label add.
- **Version-chain successor lookup does not wrap.**
  `g.Nodes.NextVersion(id, math.MaxUint32)` and the relationship mirror now
  return `nil, nil` instead of wrapping the requested successor to version `0`
  and potentially returning genesis history as a false next version.
- **Version-chain navigation validates explicit IDs.**
  `PreviousVersion` and `NextVersion` for nodes and relationships now return
  `ErrNodeNotFound`/`ErrRelNotFound` when the requested ID is unknown. A missing
  neighboring version still returns `nil, nil` for entities that exist in the
  current row set or in version history.
- **Temporal constraint registration fails closed.**
  `g.Constraints.Add` and `Set` now return an error and reject unknown
  `TemporalConstraint.Kind` values before changing the configured set. Post-close
  `Add`/`Set` return `ErrGraphClosed` instead of silently dropping the mutation.
- **Event bus setters fail closed after graph close.**
  `g.Events.SetSync` and `SetAsync` now return an error. Open graphs preserve the
  same attach/detach behavior, while post-close calls return `ErrGraphClosed` and
  leave the installed publisher unchanged.
- **Public sub-API zero values fail closed.**
  Zero-value or nil public sub-API wrappers (`Nodes`, `Rels`, `Temporal`,
  `Index`, `Events`, `Constraints`, `IO`, `Admin`, `Stats`, `Hash`, and
  `Resolve`) now return `ErrNilGraph` from error-returning methods instead of
  panicking on an unwired or typed-nil `Ops` value. No-error helpers return their
  natural zero values.
- **Public entity nil receivers fail closed.**
  Nil `*types.Node` and `*types.Relationship` accessors now return zero values,
  no-error mutators no-op, and error-returning mutators return
  `ErrNilNode`/`ErrNilRelationship` instead of panicking. `graph.ErrNilNode` and
  `graph.ErrNilRelationship` now alias the type-layer sentinels. Nil
  `*types.PropertySlice` pointer mutators return `ErrNilPropertySlice`.
  Nil `*types.TemporalMetadata` helpers fail closed, and nil integrity
  `DeepCopy` calls return nil.
- **Event bus nil receivers no-op.**
  Nil `*events.EventBus` and `*events.AsyncEventBus` receivers no longer panic
  on no-error methods. `Subscribe` returns a no-op unsubscribe, and publish or
  close calls return without side effects.
- **Integrity hashing returns errors for panicking custom properties.**
  Graph mutation, batch, label, close-version, hash-verification, and checked
  store wire-encoding paths now use checked integrity hash helpers. A registered
  custom property whose `HashBytes` implementation panics is rejected with an
  `ErrUnsupportedValueType`-wrapped error instead of crashing the caller or
  leaving partially committed graph state.
- **Checked wire reconstruction rejects panicking custom deep copies.**
  `ValidatePropertyWireSlice`, `WireToNodeChecked`, and `WireToRelChecked` now
  verify reconstructed property values through the normal property install and
  deep-copy boundary. A persisted or imported custom property whose
  `DeepCopyValue` panics now returns an `ErrUnsupportedValueType`-wrapped error
  instead of escaping through the checked wire path.
- **Ontology mapping nil receivers classify as empty.**
  Nil `*ontology.OntologyMapping` receivers now return `ClassEvent` from
  classification helpers, return nil from `SetLabelRegistry`, and return nil
  reference labels instead of panicking.
- **Store lifecycle nil receivers fail closed.**
  Nil `*memory.Store`, `*badger.Store`, and `*tiered.Store` lifecycle calls now
  return the public `store.ErrNilStore` sentinel from `Close` and `Clear`
  instead of panicking in common deferred cleanup paths. Graph-level
  `ErrNilStore` aliases the same store-layer sentinel.
- **MemoryStore zero value is usable.**
  A zero-value `memory.Store` now lazily initializes its maps at the lifecycle
  gate, so direct callers can read, write nodes and relationships, create
  indexes, and write history without first calling `memory.New()`.
- **Persistent Store zero values fail closed.**
  Zero-value `badger.Store` and `tiered.Store` values now return
  `ErrStoreClosed` from lifecycle and checked read/write entry points instead
  of panicking on nil database handles, stop channels, or reference shards.
- **Resolver reads respect transaction isolation.**
  No-error resolver helpers (`LookupLabel`, `LookupRelType`, node label/type
  resolution, and shadow-property resolution) now take the graph read lock before
  reading registry pointers, so they cannot observe token names created inside an
  active transaction or race rollback/import registry restoration. Once graph
  close is visible, those no-error helpers return zero values instead of exposing
  teardown-owned registry state. Internal mutation and hash paths use explicit
  lock-free resolver helpers when they already hold the graph lock.
- **Provider listing re-checks close under the graph lock.**
  `g.Index.Providers()` now checks the closed flag after acquiring the graph read
  lock, so a call that began before `Close()` but waited behind teardown returns
  an empty list instead of stale provider names.
- **Provider teardown waits for in-flight initialization.**
  `g.Index.RegisterProvider` still wires `Initializable` providers before
  calling `Init`, but `g.Index.UnregisterProvider` and `Graph.Close()` now wait
  for that synchronous `Init` callback to finish before calling provider
  `Close()`. Teardown no longer closes provider resources while bulk-load is
  still running.
- **Temporal diff callbacks run outside graph locks.**
  `g.Temporal.DiffCallback` now collects known IDs and resolves each entity
  version pair under short graph read-lock windows, then invokes caller-supplied
  `DiffHandlers` outside the lock. A diff handler can safely call graph read
  APIs even while a writer is waiting on `g.mu`, avoiding the `sync.RWMutex`
  writer-preference deadlock that occurred when handlers ran under the outer
  diff scan lock.
- **Store iteration callbacks run outside backend locks.**
  MemoryStore, BadgerStore, and TieredStore `ForEach*ID` iterators now invoke
  caller callbacks after releasing backend map locks, Badger transactions, and
  Tiered shard checkouts. Callbacks can call back into Store methods without
  deadlocking on the iterator's internal lock window. Badger history iteration
  keeps memory bounded by paging history IDs through `All*HistoryIDsFrom` and
  caps each direct iteration at the history-ID high-water mark captured at start,
  so callback-created higher IDs cannot extend the same iterator.
- **Badger history ID scans honor pending truncation deletes.**
  `AllNodeHistoryIDsFrom`, `AllRelHistoryIDsFrom`, and the Badger history
  `ForEach*HistoryID` wrappers now overlay pending delete writeOps on top of
  persisted history keys. A `Truncate*History` call is visible to history ID
  scans immediately, even before the async Badger write buffer flushes.
- **Adjacency reads fail closed for missing explicit nodes.**
  `OutgoingRelationships`, `IncomingRelationships`, and their `ForNodes`
  variants now return `ErrNodeNotFound` when any requested node ID is absent.
  Existing isolated nodes still return empty results, and batched reads no
  longer silently drop missing IDs while returning partial maps. History-aware
  `g.Temporal.NeighborsAt` first validates the target at the queried instant,
  then still traverses relationship history when the target has no current row.
- **Graph query names validate before empty shortcuts.**
  `g.Nodes.ByLabel`, `CountByLabel`, `g.Rels.ByType`, `CountByType`, and
  relationship adjacency type filters now reject empty, whitespace-only, or
  overlong names before an unregistered-name or empty-input shortcut can report
  a successful empty result.
- **Authorization tier shadow input accepts all bounded numeric forms.**
  `tkg_auth_level` parsing now accepts every signed/unsigned integer type plus
  whole-number `float32`/`float64` values in `[0, 255]`, and rejects fractional
  or out-of-range values before mutation. This removes the previous mismatch
  where in-range `int8`, `int16`, `uint`, `uint16`, `uint32`, `uint64`, and
  `float32` callers were rejected even though the tier stores as `uint8`.
- **Temporal shadow inputs accept all safe millisecond numeric forms.**
  `tkg_valid_from`, `tkg_valid_to`, and `tkg_created_at` parsing now accepts
  `types.Instant`, every signed integer type, unsigned integer values that fit
  in `int64`, and whole-number `float32`/`float64` values inside each type's
  contiguous exact integer range. Fractional, non-numeric, and unsafe
  out-of-range or precision-losing values still fail before mutation.
- **Mutation names validate before unrelated failures.**
  Node label mutations and relationship type creation now reject empty,
  whitespace-only, or overlong names before property validation, entity lookup,
  registry lookup, or transaction rollback snapshots can return unrelated
  errors.
- **Vector search target validation precedes the zero-k shortcut.**
  `g.Index.SearchNearest` now validates label/property targets and reports
  unknown labels before returning the empty result for `k <= 0`, so malformed
  vector-search targets are not hidden by the non-positive-k fast path.
- **Transaction updates validate inputs before rollback snapshots.**
  `GraphTx.UpdateNode` and `UpdateRelationship` now run the same update-map
  validation as standalone updates before reading the entity snapshot for
  rollback. Malformed update keys or values on a missing entity now return the
  validation sentinel instead of being masked by `ErrNodeNotFound` or
  `ErrRelNotFound`. `BatchBuilder` update queues also share this validation,
  so provenance shadow keys are accepted consistently with standalone updates.
- **Batch empty updates are accounting no-ops.**
  `BatchBuilder.Execute` still checks entity existence for queued empty
  `UpdateNode`/`UpdateRelationship` operations, but successful empty updates no
  longer increment `BatchResult.Updated` or publish update events. This matches
  standalone update semantics, where an empty update is a read-only no-op.
- **Batch create statistics match created results.**
  Successful `BatchBuilder.Execute` node and relationship creates now increment
  `g.Stats.Get().NodesAdded` and `RelsAdded` in the same units as
  `BatchResult.Created`, instead of publishing create events while leaving the
  add counters unchanged.
- **Temporal close statistics count as updates.**
  Successful `g.Nodes.CloseVersion` and `g.Rels.CloseVersion` now increment
  `NodesUpdated`/`RelsUpdated`, matching their store write and update event.
  Rejected repeat-close calls still leave counters unchanged.
- **Transaction rollback restores graph stats.**
  `GraphTx` now snapshots operation counters at `BeginTx` and restores them
  after a successful `Rollback`, so rolled-back create/update/delete writes do
  not leak into `g.Stats.Get()` after the transaction is discarded.
- **Transaction rollback snapshots distinguish entity kind.**
  `GraphTx` now tracks rollback snapshots by node-vs-relationship identity plus
  snowflake ID. Caller-supplied imports can no longer make an updated node and
  relationship with the same underlying ID collapse into one snapshot and leave
  the relationship update committed after rollback.
- **High-frequency indexes backfill and maintain existing nodes.**
  MemoryStore and BadgerStore `CreateHighFrequencyIndex` now populate buckets
  from current matching nodes, and node write/history/delete paths maintain HFI
  buckets after creation. Badger HFI creation uses the same placeholder +
  backfill shape as temporal indexes, so corrupt existing rows fail creation and
  TieredStore rolls back earlier shard-local HFI creates instead of committing a
  partial index.
- **High-frequency index definitions survive restart.**
  BadgerStore now persists HFI label/bucket definitions and rebuilds buckets on
  open; drop and clear remove the definition. TieredStore persists its
  temporal/HFI tracking metadata so reopened stores still propagate active
  indexes to rotated hot shards and lazily opened archives. Store temporal
  index creation now rejects labels that already have an HFI, preserving the
  one-temporal-index-kind-per-label invariant in MemoryStore and BadgerStore.
- **Tracked temporal index apply rolls back partial shard setup.**
  TieredStore now rolls back temporal/HFI indexes that `applyTrackedTemporalIndexes`
  created on a target shard if a later tracked definition conflicts or fails.
  Lazy archive open and hot-shard rotation can no longer return an error after
  leaving a subset of tracked temporal indexes installed on that shard.
- **Index reads ignore in-flight create placeholders.**
  Badger property, temporal, and high-frequency query paths now fall back to
  complete row scans while a phased index create is still backfilling. Badger
  and Tiered vector searches return `ErrVectorIndexNotFound` for an in-flight
  vector placeholder until creation finalizes. Visible placeholders still catch
  concurrent writes, but callers no longer see partial index results.
- **Store-level node row deletes no longer orphan relationships.**
  MemoryStore, BadgerStore, and TieredStore `DeleteNode` and `DeleteNodesBatch`
  now return `ErrInvalidStoreMutation` when any target node still has connected
  relationships. The nodes and relationships remain intact; connected deletes
  must use the cascade or delete-with-history APIs that remove relationship rows
  atomically.
- **Graph node delete tombstones use the fully locked current node.**
  `deleteNodeInternal` now re-reads the node after `LockMany` succeeds and
  builds the node tombstone from that Phase B row. A node update that lands
  between the initial adjacency scan and the final delete can no longer be lost
  from delete history.
- **Tiered node batch deletes roll back prior shard deletes.**
  `DeleteNodesBatch` now keeps preflight node snapshots and restores earlier
  accepted shard deletes when a later shard bucket fails. Store-level vector
  indexes are updated only after every shard bucket delete succeeds.
- **Tiered relationship batch deletes roll back prior shard deletes.**
  `DeleteRelationshipsBatch` now keeps preflight relationship snapshots and
  restores earlier accepted same-shard or cross-shard relationship deletes when
  a later bucket or split-delete fails. Normal correctness no longer depends on
  a follow-up repair scan after this batch path returns an error.
- **Tiered cascade deletes roll back prior relationship deletes.**
  `DeleteNodeCascade` now snapshots connected relationships before mutation and
  restores already-deleted relationship rows when a later relationship delete or
  node row delete fails while the node still exists.
- **Tiered cascade deletes purge orphan adjacency.**
  Plain `DeleteNodeCascade` now skips stale adjacency entries whose relationship
  row is already missing and uses the shard cascade path to remove those
  leftover index entries while deleting the node.
- **Tiered archive/restore purges orphan adjacency.**
  `ArchiveNode` and `RestoreNode` now skip adjacency entries whose relationship
  row is already missing while planning relationship placement and use the
  source shard's cascade delete path for final node removal, so stale
  source adjacency-only entries do not make a reference node unarchivable or
  unrestorable. Destination preflight also purges stale adjacency-only entries
  whose relationship row is already gone before writing the temporary
  destination node. Badger orphan cleanup verifies the relationship row itself
  instead of trusting index-derived state, and scans persisted type/out/in keys
  directly so ignored stale disk keys are removed too.
- **Tiered relationship routing skips stale type-index owners.**
  Relationship ID routing and repair shard probes now verify that a candidate
  shard has the relationship entity row, not only index-derived membership. A
  stale type-index key in an earlier-probed shard no longer hides a live
  relationship row in the timestamp owner or another event/archive shard. Orphan
  incoming repair now calls the Badger orphan-index purge path so stale local
  type/out/in keys and counters are removed together.
- **Badger restart rebuild uses entity rows for node/relationship liveness.**
  `loadIndexes()` now rebuilds `nodeIDs` from node entity rows and `relIDs` from
  relationship entity rows, then derives label/type/outgoing indexes from those
  rows. Stale label/type/outgoing keys without entity rows are ignored instead
  of becoming live IDs; incoming-only relationship keys remain visible for
  TieredStore cross-shard repair.
- **Badger fixed-width key scans require exact key sizes.**
  Restart rebuild, history-ID scans, pending history-ID overlays, per-entity
  history reads, and history truncation now ignore overlong keys even when they
  share a valid fixed-width prefix. Corrupt or future-format keys can no longer
  be truncated into live node, relationship, or history state.
- **Archive/restore preflights destination relationship collisions.**
  Tiered `ArchiveNode` and `RestoreNode` now reject duplicate destination
  relationship placement or live destination adjacency before writing the
  temporary destination node, so a failed move does not leave a duplicate node
  behind when rollback cleanup correctly refuses to row-delete connected nodes.
- **Rejected label additions no longer create registry tokens.**
  `g.Nodes.AddLabel` and `GraphTx.AddNodeLabel` now check the post-addition
  `MaxLabelsPerNode` limit before creating a token for a previously unseen
  label. Failed add-label attempts no longer leave unreachable label names in
  the registry when the transaction commits or when the standalone call returns.
- **Constraint-rejected relationship creates do not register rel types.**
  `g.Rels.Add`, `AddByID`, `AddByIDIfAbsent`, `Import`, and
  `BatchBuilder.AddRelationship`/`Execute` now run endpoint and temporal
  constraint rejection paths before allocating a token for a previously unseen
  relationship type. Rejected relationship creates no longer leave unreachable
  type names in the registry. If the final relationship store write fails after
  a new type token is allocated, the registry snapshot is restored before the
  error returns.
- **Failed index creates do not keep new label tokens.**
  `g.Index.CreateProperty`, `CreateTemporal`, `CreateHighFrequency`, and
  `CreateVector` still create label tokens for successful future-label indexes,
  but now snapshot and restore the label registry when the backend create call
  fails. Failed index creates no longer leave unreachable label names behind.
- **Failed node label writes do not keep new label tokens.**
  `g.Nodes.Add`, `Import`, and `AddLabel` now restore newly allocated label
  registry entries when the final Store write fails. Registry rollback windows
  are also panic-safe, so a panicking backend cannot leave the graph stuck on the
  registry allocation mutex. Multi-label create/import paths also roll back a
  partially allocated label suffix if a later label hits registry capacity
  before any Store write starts.
- **Batch node queueing no longer registers labels early.**
  `BatchBuilder.AddNode` now uses non-zero probe tokens for previously unseen
  labels and allocates real label tokens only during `Execute`. The returned
  node pointer is retokenized in place before persistence, and failed,
  panicking, or capacity-rejected execute-time allocations restore newly
  allocated labels.
- **Failed batch relationship creates restore queue-time relationship state.**
  `BatchBuilder.Execute` now restores the caller-visible relationship's
  `TxFrom`, endpoint hashes, and queued type token when endpoint refresh,
  temporal constraints, type allocation, or the final relationship Store write
  rejects the create. Failed batch relationships no longer look partially
  committed through the pointer returned from `AddRelationship`.
- **Batch event flushes are atomic publisher batches.**
  `BatchBuilder.Execute` now dispatches buffered mutation events with one
  `Publisher.PublishBatch` call after releasing graph and builder locks. Async
  event buses therefore preserve Critical -> High -> Normal -> Low -> Deferred
  priority ordering across all events from one batch execution.
- **Batch update queues snapshot caller update maps.**
  `BatchBuilder.UpdateNode` and `UpdateRelationship` now deep-copy queued
  update maps, including nested property values and provenance signatures.
  Mutating the caller's map or slice values after queueing no longer changes
  what `Execute` applies.
- **CloseVersion preserves existing integrity metadata.**
  `g.Nodes.CloseVersion` and `g.Rels.CloseVersion` still recompute the entity
  content hash after setting `ValidTo`, but now preserve non-hash integrity
  fields. Closing a relationship no longer erases `FromNodeHash`/`ToNodeHash`,
  and closing either entity no longer drops provenance, signature, or
  authorization metadata.
- **CloseVersion rejects the open-ended sentinel.**
  `g.Nodes.CloseVersion(id, 0)` and `g.Rels.CloseVersion(id, 0)` now return
  `ErrInvalidTimeRange`. `ValidTo == 0` means open-ended/current, so a close call
  with zero time cannot mark an entity closed.
- **Explicit temporal creation ranges must be non-empty.**
  Node/relationship create, import, and batch queue paths now reject caller
  payloads where both `tkg_valid_from` and `tkg_valid_to` are set but
  `valid_from >= valid_to`, returning `ErrInvalidTimeRange` before ID generation
  or batch queueing. Checked persisted wire reconstruction applies the same
  invariant to raw durable rows. Direct Store current-row writes, replacements,
  history snapshots, delete tombstones, and node label-token helpers also
  reject explicit finite ranges where `ValidFrom >= ValidTo` with
  `ErrInvalidStoreMutation`.
- **Store history snapshots are validated at the boundary.**
  Direct Store `PutNodeVersion`, `PutRelVersion`, `DeleteNodeWithHistory`, and
  `DeleteRelWithHistory` calls now reject malformed history/tombstone payloads
  before accepting them. Node snapshots must have valid non-zero label tokens,
  relationship snapshots must have valid type/endpoints, and readable node
  delete tombstones must preserve the stored node labels instead of smuggling a
  label mutation into history. Corrupt-row delete cleanup still proceeds when
  the current row cannot be decoded, so stale indexes are purged.
- **Store label/type reads reject reserved token zero.**
  Direct Store `NodesByLabel`, `RelationshipsByType`, `NodeCountByLabel`, and
  `RelCountByType` now reject token `0` with `ErrInvalidStoreMutation` instead
  of returning a successful empty result for the registry sentinel.
- **Metadata-blind rehash paths preserve existing integrity metadata.**
  Node label add/remove, node/relationship property CAS, and node/relationship
  in-place updates now preserve non-hash integrity fields while recomputing
  hash-chain values. These APIs have no provenance shadow-key channel, so they
  no longer silently erase existing provenance, signature, authorization, or
  relationship endpoint-hash metadata.
- **Temporal label queries validate malformed labels.**
  `g.Temporal.NodesByLabelAt` now rejects empty, whitespace-only, and overlong
  label names before registry lookup, matching `g.Nodes.ByLabel` and the
  temporal label+property helpers instead of returning a successful empty
  result for malformed targets.
- **Registry resolution APIs enforce graph name limits.**
  `g.Resolve.LabelToken` / `RelTypeToken` now reject empty, whitespace-only,
  and overlong names before allocating registry tokens. Non-error boolean
  helpers (`LookupLabel`, `LookupRelType`, `Nodes.HasLabel`, `Rels.HasType`)
  fail closed for malformed names instead of consulting the registry.
- **Imported and rehydrated registries enforce graph name limits.**
  `IO.Import` and `graph.New` registry rehydration from Badger or Tiered stores
  now validate loaded label/type names against the active `MaxNameLength` before
  the mappings become graph state. Store-level registry loaders remain primitive
  persistence helpers; the graph layer owns configured name-limit enforcement.
- **Index provider names reject blank input.**
  `g.Index.RegisterProvider`, `RegisterLegacyProvider`, and
  `UnregisterProvider` now reject whitespace-only provider names with
  `ErrIndexProviderEmptyName` before mutating or probing the provider registry.
- **Index provider lists fail closed during graph teardown.**
  `g.Index.Providers()` now returns an empty list as soon as the graph closed
  flag is visible. The no-error list API no longer reads provider registry
  state in the window after `Close()` starts but before provider entries are
  drained under the graph lock.
- **Transaction rollback restores multi-label node mutations.**
  `GraphTx.Rollback` now restores a node's label sequence by transforming the
  current row back to the pre-transaction snapshot through exact one-token Store
  label writes before replacing the remaining node state. Multiple label adds,
  multiple label removes, and mixed remove/add transactions on the same node no
  longer fail rollback or leave label indexes out of sync.
- **`tkg_base_entity` resolves as `types.EntityID`.**
  `g.Resolve.NodeProperty` and `RelProperty` now return the public
  `types.EntityID` wrapper for `tkg_base_entity` instead of leaking the raw
  `snowflake.ID` dependency through the shadow-property surface.
- **Transaction rollback now restores endpoint nodes before relationships.**
  `GraphTx.Rollback` no longer tries to recreate standalone-deleted
  relationships before nodes deleted later in the same transaction. Rollback
  first restores all deleted node rows, then cascade-deleted relationships,
  then standalone-deleted relationships, so referential integrity holds during
  the undo. Regression coverage deletes a relationship, deletes one endpoint,
  rolls back, and verifies both endpoint nodes and the relationship are present.
- **Transaction rollback restores version history too.**
  `GraphTx.Rollback` now snapshots node and relationship history before the
  first transactional update/delete, restores those history rows on rollback,
  and truncates history for entities created and then rolled back. Rolled-back
  updates and deletes no longer leave phantom version entries behind.
- **Import replay errors roll back partial writes.**
  `IO.ImportWithOptions` now snapshots touched current rows, version-history
  rows, and registry mappings while replaying staged records under the graph
  write lock. Corrupt or inconsistent streams that fail after earlier records
  were applied now restore the pre-import graph state, including transient
  history rows and imported tokens.
- **Import rejects malformed entity wire invariants.**
  Snapshot import now rejects zero/negative entity IDs, negative versions,
  non-canonical label token lists, reserved `tkg_` property keys,
  unsorted/duplicate property keys, unknown property type tags, and negative
  base-entity IDs before constructing entities.
- **Badger reads now validate semantic wire invariants.**
  Current and history node/relationship reads now use checked wire
  reconstruction after MsgPack decode. Semantically corrupt rows with invalid
  IDs, token-0, out-of-range tokens, non-canonical label lists, or malformed
  property wire now return read errors instead of panicking in type constructors
  or truncating token values. Badger reads also reject rows whose decoded entity
  ID does not match the key being read, so corrupt key/value pairs cannot return
  an entity under the wrong ID.
- **Float32 wire validation preserves special values.**
  Checked property wire validation now treats `float32` NaN and infinities as
  valid `float32` values for scalar and slice properties, while still rejecting
  finite `float64` values that would overflow or round when reconstructed as
  `float32`. Valid persisted float32 properties no longer fail checked reads
  just because their value is non-finite, but corrupt float32-tagged wire can no
  longer silently narrow finite `float64` payloads.
- **Numeric wire reconstruction preserves tag type.**
  Property wire tagged as `float64` or `[]float64` now reconstructs decoded
  `float32` payloads as `float64` values, and `[]int` tags reconstruct decoded
  `[]int64` payloads as `[]int`. Integer tags also reconstruct decoded `uint`
  values that the validator accepts. Reconstruction now matches the checked
  validation contract instead of returning scalar `float32`, nil slices, or
  zero-filled numeric values.
- **Vector search heap allocation is bounded by index size.**
  `VectorIndex.SearchNearest` now caps its heap capacity at the number of
  indexed entries instead of blindly using caller-provided `k`. A tiny vector
  index searched with a huge `k` no longer attempts a huge allocation or panics;
  it returns all available ranked entries.
- **Temporal vector over-fetch buffers are bounded by the probe ceiling.**
  The graph-level temporal vector fallback now sizes its resolved-candidate
  buffer from the clamped over-fetch probe size, not raw caller `k`. External
  vector backends without `FilteredVectorSearchCapability` no longer make
  `g.Index.SearchNearest(..., k=math.MaxInt, temporalOpts)` panic before the
  bounded backend probe.
- **Badger deletes prefetch old state outside the index write lock.**
  `BadgerStore.DeleteRelationship` now mirrors the node delete path by reading
  the relationship row before taking `idxMu.Lock`, so cache-miss deletes no
  longer hold the global index write lock across Badger I/O. Node and
  relationship deletes re-read the current cached row after the write lock is
  acquired, so index cleanup uses the row that is actually being deleted even
  if a direct Store caller reused the same ID in the prefetch window.
- **Badger sync writes flush index definition metadata too.**
  In `SyncWrites` mode, Badger property, temporal, high-frequency, and vector index
  create/drop operations now flush their persisted definition metadata before
  returning. The flush runs after releasing `idxMu`, preserving the synchronous
  durability contract without deadlocking the flush snapshot path. Badger split
  relationship helper writeOps used for Tiered relationship routing and repair
  now use the same synchronous flush path.
- **Index definition persistence skips in-flight creates.**
  Badger property, temporal, high-frequency, and vector definition persistence and Tiered
  vector definition persistence now exclude indexes whose phased create is
  still backfilling. A concurrent index create/drop can no longer persist an
  unfinished placeholder that later fails or has not merged its backfill.
- **Tiered temporal index DDL rejects cross-kind shard conflicts.**
  `tiered.Store` now distinguishes same-kind retry from a shard that already
  has the other temporal index kind. Retrying a partial temporal or
  high-frequency create can finish only when the HFI bucket size also matches,
  but attempting to create HFI over an untracked interval index, interval
  indexing over an untracked HFI, or HFI over a different shard-local HFI bucket
  returns `ErrTemporalIndexExists` without installing mixed per-shard index
  kinds or mixed per-shard HFI buckets.
  If a later shard fails during temporal/HFI create after earlier shards were
  updated, the earlier shard-local creates are rolled back before the error is
  returned. Drop paths now do the symmetric rollback by recreating indexes that
  were removed from earlier shards when a later shard fails.
- **Checked wire encoders reject nil entities.**
  `storeutil.NodeToWireChecked` and `RelToWireChecked` now return conversion
  errors for nil payloads instead of dereferencing before the checked error
  path. Direct regressions cover both helpers.
- **Bulk ID reads now fail on missing explicit IDs.**
  `g.Nodes.GetByIDs`, `g.Rels.GetByIDs`, and the Store-level `Get*ByIDs`
  methods now return wrapped `ErrNodeNotFound` / `ErrRelNotFound` when any
  requested ID is absent. They no longer report success with partial results
  for explicit caller input.
- **Tiered cold-shard timing config fails closed.**
  `tiered.New` now rejects fractional-millisecond `ShardWindow` values,
  negative `ColdAfter`, negative `IdleTimeout`, and `IdleTimeout` values that
  are not positive whole milliseconds before starting the idle-close goroutine.
  This prevents a zero-duration ticker panic, keeps shard windows aligned with
  millisecond snowflake timestamps, and avoids silently disabling cold-shard
  cleanup through negative durations.
- **Sub-day Tiered shard windows use real duration buckets.**
  Minute/hour `ShardWindow` values now start at the enclosing fixed-duration
  boundary instead of the first day of the month. Accepted sub-day hot shards no
  longer start already expired for most timestamps or rotate on every write.
- **Recurrence validation rejects impossible calendar selectors.**
  `types.RecurrencePattern.Validate` now rejects unsupported `WeekdayMask`
  bits for daily/weekly patterns and invalid `Month` values for yearly
  patterns. `DayStart` and `DayEnd` must be whole milliseconds so expansion
  cannot silently truncate offsets to the `Instant` precision. `Expand` now
  returns a `types.ErrInvalidTimeRange` sentinel for empty or inverted windows.
- **Index creation materializes future-label indexes.**
  `g.Index.CreateProperty`, `CreateTemporal`, `CreateHighFrequency`, and
  `CreateVector` now validate and create the label token before creating the
  backing store index. Calling create before the first matching node no longer
  returns success over a no-op; future matching writes are indexed. Empty labels
  now return `ErrEmptyName`, and property/vector index keys respect
  `MaxPropertyKeyLength`.
- **High-frequency temporal index buckets must be whole milliseconds.**
  `g.Index.CreateHighFrequency` and Store-level `CreateHighFrequencyIndex` now
  reject bucket sizes that are not positive whole milliseconds with
  `ErrInvalidTemporalIndexConfig` before installing the index. A successful HFI
  create therefore has a time-bucket width that matches the millisecond
  `Instant` precision instead of collapsing or truncating buckets.
- **Unknown temporal constraint kinds now fail closed.**
  Relationship writes now return wrapped `ErrTemporalConstraint` and
  `ErrInvalidTemporalConstraint` when the graph's constraint set contains an
  unsupported kind, including the zero-value `TemporalConstraint{}`. Invalid
  constraint configuration can no longer be silently treated as no constraint.
- **Tiered reference labels reject empty names.**
  `tiered.New` now rejects empty or whitespace-only `Config.RefLabels` before
  opening shards, and `ontology.NewOntologyMapping` ignores empty names when
  used directly. Tiered catalog metadata and reference/event routing can no
  longer record an impossible reference label that the registry would reject.
- **Property construction and setters are defensive.**
  `NewPropertySlice`, `PropertySlice.Set`, `Node.SetProperty`, and
  `Relationship.SetProperty` now deep-copy accepted slice/map/custom values
  before storing them, while preserving the pointer shape of registered custom
  property values. Mutating a caller-owned property value after property
  construction or `SetProperty` can no longer rewrite entity state outside the
  graph/store mutation boundary.
- **Store index APIs reject token-0 labels.**
  Direct MemoryStore, BadgerStore, and TieredStore property, temporal,
  high-frequency, and vector index operations now reject label token 0 with
  `ErrInvalidStoreMutation`. Badger and Tiered reopen paths also reject
  persisted index definitions that target token 0.
- **Property/vector index targets reject shadow keys.**
  Graph-level `g.Index.*`, `g.Index.SearchNearest`, and
  stored-property query methods (`g.Nodes.ByLabelAndProperty`,
  `g.Temporal.NodesByLabelProperty*`, and
  `g.Temporal.RelsByTypeProperty*`) now reject reserved `tkg_` property keys
  with `types.ErrReservedPrefix` instead of creating or querying impossible
  stored property targets. Direct Store-level property and vector index APIs
  and persisted index definitions apply the same rule.
- **Badger index-definition metadata fails closed.**
  BadgerStore now returns an open-time error when persisted property,
  temporal, or vector index-definition records cannot be decoded as MsgPack.
  Corrupt metadata no longer silently opens as if no indexes were defined.
- **Badger persisted counters reject invalid values.**
  Reopen now treats negative `node_count` or `rel_count` metadata, and positive
  counters that match neither admitted live rows nor the exact raw entity-row
  count skipped as corrupt during replay, as corrupt store state and returns
  `ErrInvalidStoreMutation`. Public `NodeCount`/`RelationshipCount` results are
  derived from admitted live rows, so stale counters do not over-report damaged
  entities.
- **Index target operations reject invalid or unknown labels.**
  `g.Index.DropProperty`, `DropTemporal`, `DropHighFrequency`, and `DropVector`
  now validate labels before Store delegation and return the matching not-found
  sentinel when the label has never been registered. Property/vector drops also
  enforce `MaxPropertyKeyLength`. `g.Index.SearchNearest` now applies the same
  label/property-key validation and returns `ErrVectorIndexNotFound` for unknown
  labels, so typoed labels and invalid keys no longer report a successful empty
  result.
- **Vector index configuration is validated at every boundary.**
  `CreateVectorIndex` now rejects non-positive dimensions and unsupported
  distance metrics with `ErrInvalidVectorIndexConfig` before installing a
  placeholder or persisting a definition. BadgerStore and TieredStore also
  reject invalid persisted vector-index definitions during open instead of
  rebuilding indexes with disabled dimension checks.
- **Vector definition metadata rejects conflicting duplicates.**
  Badger vector-index metadata and Tiered `vector_indexes.msgpack` are persisted
  map-shaped contracts. Reopen now deduplicates identical repeated definitions
  and rejects conflicting duplicate `(label, property)` entries with
  `ErrVectorIndexExists` instead of silently letting the last corrupt record
  choose the active index configuration.
- **Tiered vector definition persistence is rollback-safe.**
  TieredStore now snapshots the raw `vector_indexes.msgpack` file before
  vector index create/drop persistence, restores or removes it on persisted
  definition write failure, and fsyncs the parent directory after removing the
  last definition so restart cannot resurrect a dropped vector index.
- **Tiered catalog saves restore raw files on write failure.**
  `ShardCatalog.Save` now snapshots the raw catalog JSON before atomic writes
  and restores or removes it if the write reports an error, so caller-level
  in-memory catalog rollback is matched by file-level rollback.
- **Tiered catalog loads validate topology before opening shards.**
  Persisted `shard_catalog.json` now rejects duplicate shard names, multiple
  hot event shards, invalid kind/tier combinations, unsafe relative paths,
  negative stats, and invalid event time windows before `tiered.New` opens
  shard handles from catalog metadata.
- **Tiered registry saves restore raw files on write failure.**
  The flat `registry.msgpack` helper now snapshots the previous raw file and
  restores or removes it when an atomic write reports an error, giving direct
  registry save APIs the same all-or-error file semantics as vector metadata
  and catalog saves.
- **Tiered registry files validate both persisted halves on load.**
  The flat `registry.msgpack` loader now rejects missing token-0 sentinels,
  empty or whitespace-only names, duplicate names, and over-capacity label or
  relationship-type slices before returning metadata to load or deprecated
  single-registry save paths. `SaveLabelRegistry` and `SaveRelTypeRegistry`
  therefore fail rather than preserving a corrupt other half.
- **Tiered rotation aborts on inherited-index setup failure.**
  Hot-shard rotation now closes and removes the newly opened shard and returns
  the index setup error if tracked temporal or high-frequency indexes cannot be
  installed. The catalog and live hot-shard pointer remain unchanged.
- **Tiered temporal tracking load rolls back earlier shard changes.**
  Restart-time replay of `temporal_indexes.msgpack` now records shard-local
  temporal/HFI definitions it creates and removes them from earlier shards if a
  later shard rejects the persisted tracking metadata. A failed open no longer
  leaves newly persisted index definitions behind on shards that replayed first.
- **Store registry APIs reject nil registry pointers.**
  BadgerStore and TieredStore registry save/load methods now return
  `ErrInvalidStoreMutation` for nil label or relationship-type registry
  pointers on open stores, while closed stores still return `ErrStoreClosed`
  first. `tiered.Store.SetLabelRegistry(nil)` is a no-op, so a direct caller
  cannot accidentally clear ontology routing state.
- **Tiered migration validates caller-owned pointers and rolls back failures.**
  `tiered.MigrateFromBadger` now matches the public two-argument signature and
  loads label/relationship-type registries from the source BadgerStore. Nil
  source or destination stores return `ErrInvalidStoreMutation`; missing source
  registry metadata for non-empty data and entity tokens outside the loaded
  source registries also fail closed. The destination must be empty, source
  entities and relationship endpoints are preflighted before destination
  writes, and mid-copy failures roll back inserted entities, restore the
  previous ontology registry, and preserve the prior destination registry file
  if the final metadata save fails after a partial filesystem update. Ontology
  registry swaps now clear the token cache so stale token classifications
  cannot survive a migration rollback. The
  destination lifecycle is checked before registry wiring or iteration, so an
  empty migration to a closed TieredStore returns `ErrStoreClosed` instead of
  success; a closed source BadgerStore also propagates `ErrStoreClosed`.
- **Vector Store sentinels live in the public Store contract.**
  `ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`,
  and `ErrInvalidVectorIndexConfig` are now canonical in `pkg/graph/store`.
  The root `graph` package, `pkg/graph/index`, and concrete backends keep
  aliases, so callers can use stable `errors.Is` checks without importing a
  concrete backend.
- **Restarted warm/cold event shards stay writable.**
  TieredStore now reopens warm event shards and lazy-opened cold event shards
  with writable Badger handles. Warm/cold remains a routing and idle-close tier
  marker, but existing event entities still route updates/deletes to their
  owner shard after rotation and restart. Direct regressions cover a restarted
  warm-shard `ReplaceNode` and `DeleteRelationship` surviving another reopen.
- **Weekly shard windows follow ISO week-one boundaries.**
  TieredStore weekly `ShardWindow` starts now anchor ISO week 1 on the week
  containing January 4. Years where January 1 falls on Friday or Saturday no
  longer route ISO week-one event entities into the previous week's shard.
- **Backfilled event node creates route to their timestamp owner.**
  TieredStore `PutNode` and `PutNodesBatch` now route event nodes by the
  caller-supplied snowflake ID timestamp instead of unconditionally writing to
  the current hot shard. A successful create with an older/imported event ID is
  therefore immediately readable by `GetNode`, which resolves the same ID by
  timestamp.
- **Tiered creates reject IDs already parked in closed cold shards.**
  TieredStore duplicate checks now use cold-shard-aware ID routing before
  accepting caller-supplied node or relationship IDs. A duplicate ID already
  persisted in an idle-closed cold event shard is rejected with `ErrNodeExists`
  or `ErrRelExists` instead of allowing a second entity with the same ID in a
  reference or hot shard. Relationship duplicate checks resolve the actual
  owner shard, not only the relationship ID timestamp, because a current-ID
  relationship can still be owned by an old start-node shard. Graph-generated
  single and batch creates use a new internal generated-ID fast path so
  fresh IDs keep the hot path and do not eagerly open unrelated cold shards.
- **Generated-ID create optimization is internal-only.**
  The TieredStore generated-ID fast path now requires a proof token from
  `pkg/graph/internal/generatedcreate` and is no longer exposed through the
  public Store capability contract. Direct Store callers always get the safe
  duplicate-checking `Put*` methods; graph-generated creates keep the optimized
  path without documenting a caller-side precondition.
- **Cold-shard idle-close errors are surfaced.**
  TieredStore now records and logs errors from background cold-shard idle
  close. A failed idle close blocks later lazy checkouts with the recorded
  error and is joined into the next `Store.Close` result, so a failed final
  Badger flush during idle eviction cannot disappear as an unobservable
  background failure.
- **Tiered close synchronizes event-shard handles.**
  `tiered.Store.Close` now takes each event shard's mutex before closing or
  clearing its `BadgerStore` pointer. Explicit close and background idle-close
  now use the same synchronization before touching `EventShard.store`.
- **Tiered post-close routing fails closed.**
  Checked node/relationship routing and create-time rotation now return
  `ErrStoreClosed` after `tiered.Store.Close` starts. Direct Store callers no
  longer route into closed reference state or nil event shard handles after
  shutdown.
- **Tiered index operations fail closed after close.**
  Tiered property, temporal, high-frequency, and vector index create/drop/search
  paths now return `ErrStoreClosed` for valid requests after `Store.Close`
  starts. Vector drop/search can no longer mutate the store-level index map or
  return stale in-memory vector results after shutdown.
- **Tiered direct reads fail closed after close.**
  Tiered direct Store read/count/bulk/history APIs now return `ErrStoreClosed`
  after `Store.Close` starts, before touching reference shard state or returning
  stale cached data for otherwise valid requests.
- **Tiered admin and metadata APIs fail closed after close.**
  Tiered public admin, archive/restore, repair, clear, and registry
  save/load entry points now return `ErrStoreClosed` after `Store.Close`
  starts instead of touching closed reference state, catalog metadata, or
  registry files.
- **Tiered empty batch and reference-history writes fail closed after close.**
  Tiered empty batch deletes and reference node history writes now check
  `Store.Close` state before returning success or touching `refShard`.
- **Badger public APIs fail closed after close.**
  BadgerStore public read/write/history/index/metadata APIs now return
  `ErrStoreClosed` after `Store.Close`, including cache-hit reads, O(1)
  counters, empty batch/query fast paths, vector search, public `Flush`, and
  split relationship helper operations. No-error diagnostic/routing helpers
  return inert zero values after close instead of exposing stale in-memory
  cache, index, or rebuild state.
- **Memory public APIs fail closed after close.**
  MemoryStore now marks itself closed on `Close` and returns `ErrStoreClosed`
  from public read/write/history/index/count/iteration APIs after shutdown,
  including empty fast paths. Exported test-only tampering helpers return
  no data or no-op after close, so direct callers cannot keep reading or
  mutating process-local maps after lifecycle teardown.
- **Vector searches re-check close after index scans.**
  MemoryStore, BadgerStore, and TieredStore vector search paths now re-check
  the Store close state after the in-memory vector scan/filter callback
  completes and before returning empty or raw-ID results. A close triggered
  during a filtered vector search can no longer race into a successful
  post-close result.
- **AsyncEventBus stops accepting publishes once close starts.**
  `AsyncEventBus.Close` now marks the bus closing, closes `stopCh` to unblock
  any `BackpressureBlock` publisher waiting on a full priority queue, then waits
  for the publish gate and dispatcher drain. `Publish` and `PublishBatch`
  re-check the closing gate under `publishMu`, and `Subscribe` after close
  returns a no-op unsubscribe, so post-close callers cannot enqueue or install
  work into a stopped dispatcher.
- **AsyncEventBus serializes dispatch to preserve batch priority.**
  `AsyncEventBusConfig.Workers` values greater than one are now capped to one
  dispatcher. `Close` can wake every configured worker, and multiple drainers
  let lower-priority batch events run before a higher-priority handler has
  completed. Serialized dispatch keeps `PublishBatch` priority order true
  during normal drain and close.
- **Property ingress validates custom deep-copy output.**
  `PropertySlice.Set`, `NewPropertySlice`, and entity `SetProperties` now
  validate registered custom property values after `DeepCopyValue` runs and
  convert deep-copy panics into errors. A faulty custom `DeepCopier` can no
  longer smuggle an unsupported value into entity state after the original
  value passed validation. The copy path also preserves the caller's
  value-vs-pointer shape for registered custom values.
- **AsyncEventBus invalid backpressure preserves delivery.**
  `NewAsyncEventBus` and lazy async-bus start now normalize unknown
  `BackpressureStrategy` values to `BackpressureBlock`. A bad enum value can no
  longer fall through the enqueue switch and silently drop every published event.
- **Event buses ignore nil subscriptions.**
  `events.EventBus.Subscribe(nil)` and `events.AsyncEventBus.Subscribe(nil)`
  now return no-op unsubscribe functions without installing a nil handler.
  The async zero value also avoids starting workers for a nil subscription.
- **Store iteration rejects nil callbacks instead of panicking.**
  MemoryStore, BadgerStore, and TieredStore `ForEach*ID` iteration methods now
  return `ErrInvalidStoreMutation` for a nil callback on an open store. Closed
  stores still return `ErrStoreClosed` first. Depth-limited Tiered history
  iterators apply the same contract before entering shard callbacks.
- **ConstraintSet iteration rejects nil callbacks consistently.**
  `temporal.ConstraintSet.ForEach(nil)` now returns
  `ErrInvalidTemporalConstraint` for both empty and non-empty sets instead of
  returning nil for empty sets and panicking once a constraint exists.
- **Transaction and context entry points reject nil inputs.**
  `g.Tx.Run(nil)` and `g.Tx.RunContext(ctx, nil)` now return
  `ErrNilTxCallback` before beginning a transaction, and `RunContext(nil, fn)`
  plus graph mutation context helpers now return `ErrNilContext` instead of
  panicking on `ctx.Err()` or `ctx.Done()`.
- **IO entry points reject nil readers and writers.**
  `g.IO.Export(nil)` and `tx.Export(nil)` now return `ErrNilWriter`, while
  `g.IO.Import(nil)` and `ImportWithOptions(nil, opts)` return `ErrNilReader`.
  Typed nil reader/writer values are rejected too, before staging files or
  stream writes are attempted.
- **Index provider registration rejects typed nil providers.**
  `g.Index.RegisterProvider` and deprecated `RegisterLegacyProvider` now reject
  typed nil provider interfaces before calling `Name()`. Invalid extension
  inputs no longer panic during provider-name validation.
- **Configured stores reject typed nils.**
  `graph.New(Config{Store: typedNil})` now returns `ErrNilStore` instead of
  constructing a graph with a nil backend hidden inside the Store interface.
  A nil `Config.Store` still selects the default backend path.
- **Graph façade nil receivers return errors.**
  `(*Graph)(nil).Close()`, zero-value `Graph.Close`, `NewBatchBuilder(nil)`,
  zero-value `TxAPI`, and zero-value `BatchAPI` now return `ErrNilGraph`
  instead of panicking on a nil internal core pointer.
- **Node property CAS enforces final property-count validation.**
  `g.Nodes.CompareAndSetProperty` now rechecks `MaxPropertiesPerEntity` after
  a matched add/delete/set mutation and before version-history persistence. A
  CAS add cannot create a node shape that ordinary update paths would reject.
- **Entity property setters validate and canonicalize bulk input.**
  `types.Node.SetProperties` and `types.Relationship.SetProperties` now return
  an error, reject reserved/unsupported values, sort by key, de-duplicate with
  last-value-wins semantics, and deep-copy values before installation. Direct
  `PropertySlice` literals can no longer bypass the `Set`/`NewPropertySlice`
  safety checks and later break lookup, hashing, or persistence.
- **Registered custom properties have a typed wire form.**
  Persisted/exported property wire now stores registered custom struct values
  as a registered type name plus MsgPack payload, preserving value and pointer
  forms across Badger/Tiered/export round-trips. Wire encoding verifies that the
  MsgPack round-trip preserves the custom value's `HashBytes`; unencodable or
  lossy custom values return an error instead of silently decoding as generic
  maps.
- **Custom property registration rejects untyped nil.**
  `types.RegisterPropertyStructType(nil)` now returns an
  `ErrUnsupportedValueType`-wrapping error and leaves the registry unchanged.
  Untyped nil cannot identify a custom property type, so it is no longer
  reported as a successful registration.
- **Store relationship replacements preserve indexed fields.**
  `ReplaceRelationship`, `ReplaceRelWithHistory`, `DeleteRelWithHistory`, and
  relationship tombstones supplied to `DeleteNodeWithHistory` now reject type or
  endpoint changes with `ErrInvalidStoreMutation` before mutating rows, history,
  or indexes. Store history writes also reject node/relationship snapshots whose
  payload ID does not match the history key.
- **Store node replacements preserve label indexes.**
  `ReplaceNode` and `ReplaceNodeWithHistory` now reject node label-token changes
  with `ErrInvalidStoreMutation`; label changes must go through the dedicated
  label-token mutation paths that maintain label indexes and tier routing.
  Badger `ReplaceNodeWithHistory` also cleans property, temporal, high-frequency, and vector
  indexes from the stored current row rather than trusting the caller-supplied
  previous snapshot.
- **Store label-token helpers validate exact deltas.**
  `AddNodeLabelToken`, `RemoveNodeLabelToken`, and their history variants now
  verify that the supplied current-row payload adds or removes exactly the
  requested token before mutating label indexes, history, caches, or vector
  indexes. Direct Store callers can no longer desynchronize a label index from
  the stored node row by passing an unchanged or differently changed node.
- **Store node delete-with-history validates relationship tombstone coverage.**
  `DeleteNodeWithHistory` now rejects duplicate relationship tombstones,
  tombstones for relationships not connected to the deleted node, and missing
  tombstones for live connected relationships before deleting rows or writing
  history. TieredStore now matches the MemoryStore and BadgerStore trust
  boundary instead of silently deleting connected relationships without their
  tombstone history.
- **Store mutations reject zero entity IDs.**
  `PutNode`, `ReplaceNode`, `PutNodesBatch`, `PutRelationship`,
  `ReplaceRelationship`, `PutRelationshipsBatch`, delete/cascade, and batch
  delete paths now reject zero node or relationship identities with
  `ErrInvalidStoreMutation`. Relationship writes also reject zero start/end node
  IDs before endpoint lookup or partial batch mutation. Badger split
  relationship helpers used by TieredStore cross-shard routing apply the same
  validation to entity/out, incoming-index writes, and repair-only
  incoming-index deletes. Direct incoming-index scan deletes now remove the
  in-memory entry as well as pending/persisted keys.
- **Store history replacements reject nil payloads.**
  `ReplaceNodeWithHistory` and `ReplaceRelWithHistory` now validate current and
  previous snapshot payloads before reading IDs or marshaling data, returning
  `ErrInvalidStoreMutation` instead of panicking on nil input.
- **Import rejects incomplete and unknown-tag streams.**
  Export streams with a format version accepted by this runtime must contain
  a header, a registry, and only the known current-entity/history record tags.
  Missing required records or unknown record tags now return `ErrCorruptExport`
  instead of reporting a successful no-op or partial import.
- **Import enforces header counts and per-stream uniqueness.**
  Current node/relationship record counts must match the export header, and a
  single stream may not repeat singleton header/registry records, the same
  current entity, or the same history-version record. Truncated exports and
  duplicate-record streams now return `ErrCorruptExport` instead of importing a
  partial graph.
- **Import wraps malformed MsgPack records with `ErrCorruptExport`.**
  Header, registry, current-entity, and history-record decode failures now
  satisfy `errors.Is(err, ErrCorruptExport)` instead of surfacing raw decoder
  errors that callers could not classify through the public sentinel.
- **Legacy temporal-depth sentinel no longer carries a stale unsupported contract.**
  `ErrDepthTemporalUnsupported` is retained only for source compatibility with
  older callers. Current query and vector paths compose temporal filters with
  tiered-store `Depth`.
- **Security and vulnerability gates now scan module packages only.**
  `make security` feeds `gosec` the directories from `go list -f '{{.Dir}}'
  ./...`, and `make vulncheck` feeds `govulncheck` the package list from
  `go list ./...`. Hidden agent worktrees and stale scratch copies no longer
  affect CI security signals.
- **Formatting targets now operate only on tracked Go sources.**
  `make fmt` and `make fmt-check` use `git ls-files '*.go'` instead of walking
  the repository directory tree, so hidden worktrees and scratch copies cannot
  be reformatted or break formatting checks.
- **Stale review artifacts removed from live docs.**
  Historical maintainability-review reports and an outdated optimization-target
  note were removed from `docs/` because they described already-fixed bugs and
  speculative caller/implementation workarounds as if they were current
  contracts. Live documentation now carries the current API and architecture
  state without those stale findings.
- **Property size limits now apply recursively.**
  `MaxPropertyValueSize` is enforced for every string nested inside accepted
  property values, including string slices, `[]any`, `map[string]any`,
  `map[string]string`, and nested map keys. Snapshot import applies the
  destination graph's property-count and property-size limits before replaying
  entity records, so direct wire imports cannot bypass normal mutation limits.
- **Property validation now rejects type-erasing aliases and unsupported slices.**
  `types.ValidatePropertyValue` now admits only the exact scalar, slice, and
  map types that integrity hashing, deep-copy, and type-tagged MsgPack support,
  plus explicitly registered custom struct types. Named scalar/container
  aliases and unsupported concrete slices such as `[]uint` or `[]int32` now
  return validation errors instead of reaching the hash path and panicking.
- **Provenance shadow inputs now reject wrong value types.**
  `extractProvenance` now validates `tkg_author_id`, `tkg_signature`, and
  `tkg_authorized_by` before stripping them from props/updates. Non-nil values
  of the wrong type now return an error instead of silently clearing the
  caller-supplied metadata.
- **Tiered high-frequency indexes now inherit across shard topology changes.**
  `tiered.Store` tracks high-frequency index definitions alongside temporal
  index definitions, applies them to rotated hot shards, applies both tracked
  temporal index kinds to lazily opened reference archives, and clears the
  tracking maps on `Clear()`. Creating a high-frequency index for a label with
  a tracked temporal index now returns `ErrTemporalIndexExists` instead of
  silently doing nothing.
- **Tiered delete-with-history rolls back pre-node relationship deletes.**
  `tiered.Store.DeleteNodeWithHistory` snapshots each connected relationship
  and its history before deleting it. If a later relationship delete or the node
  tombstone write fails while the node is still live, the store restores the
  already-deleted relationships and their pre-call history instead of leaving a
  live node with missing edges.
- **Tiered rollback failures now surface with the primary error.**
  `PutNodesBatch`, `PutRelationshipsBatch`, `ArchiveNode`, and `RestoreNode`
  no longer discard errors from cleanup after a partial cross-shard write.
  Rollback attempts continue across completed steps where possible, and any
  rollback failure is included in the returned error alongside the original
  write failure.
- **Cascade deletes purge orphan relationship indexes.**
  Memory and Badger cascade-delete paths now clean stale relationship IDs out
  of type, outgoing, and incoming indexes when adjacency points at a
  relationship entity that is already missing. The delete still completes, but
  indexes no longer retain phantom rel IDs.
- **Vector index creation rejects wrong-dimension backfill rows.**
  Memory, Badger, and Tiered `CreateVectorIndex` now return
  `ErrDimensionMismatch` and remove the placeholder index when an existing
  vector property has a length different from the requested index dimension.
  The method no longer returns success over a partial index that silently
  excludes those nodes.
- **Vector index maintenance rejects wrong-dimension writes.**
  Memory, Badger, and Tiered node create/update/label/history paths now
  validate active vector indexes before mutating store state. Matching vector
  properties with the wrong length return `ErrDimensionMismatch` and leave the
  node row unchanged instead of silently excluding the node from k-NN results.
- **Persistent stores keep vector index definitions across restart.**
  BadgerStore now persists vector index definitions in Badger metadata, and
  TieredStore persists its store-level definitions in `meta/vector_indexes.msgpack`.
  Both rebuild vector entries from node properties on open. `DropVectorIndex`
  and `Clear()` remove those definitions so deleted indexes do not reappear
  after restart.
- **IndexProvider event errors are logged instead of discarded.**
  `g.Index.RegisterProvider` still treats `OnEvent` errors as non-veto
  diagnostics because the graph mutation has already committed, but the
  registration wrapper now logs the provider name, event type, entity ID, and
  error instead of silently dropping the returned error.
- **Phased index create cleanup is placeholder-scoped.**
  Badger property, temporal, high-frequency, and vector index builds, plus Tiered vector index
  builds, now delete only the placeholder created by the failing call and reject
  stale finalization if a concurrent drop or recreate replaced it. A failed
  create can no longer remove a newer index under the same key.
- **Tiered repair scans incoming index entries directly.**
  `RunRepair` Phase 1 now snapshots each shard's incoming adjacency index
  entries instead of first enumerating live node IDs. Orphaned `in/` entries
  whose endpoint node row is already missing are now visible to repair and
  removed. `DeleteIncomingByRelID` also matches the endpoint ID when rewriting
  pending incoming-key writes.
- **Tiered catalog updates are save-or-rollback.**
  Lazy reference-archive creation now persists the archive shard catalog entry
  before publishing `refArchive`; if the save fails, the catalog is restored and
  the newly opened archive store is closed and removed. `RebuildCatalog` and
  immutable-shard `VerifyShard` cache updates now do the same for stats,
  tiers, `Verified`, and count fields, returning the save error instead of
  silently keeping in-memory-only metadata.
- **Tiered clear wipes closed historical shards.**
  `tiered.Store.Clear` now serializes against shard topology changes, clears
  closed cold event shards by opening them for the wipe, handles restarted warm
  shards, and resets catalog verification/stat caches with save-or-rollback
  semantics.
- **Tiered catalog rebuild counts closed cold shards.**
  `tiered.Store.RebuildCatalog` now opens closed cold event shards and derives
  their node/relationship counts from durable shard data instead of preserving
  stale catalog counts for shards whose `store` pointer is nil.
- **Tiered shard listing reports closed-shard counts.**
  `tiered.Store.ListShards` now uses catalog counts for closed cold event
  shards and closed archives instead of reporting `0/0` merely because the
  shard is idle. Open shards still report live counts from their store handle.
- **Transaction rollback restores registries.**
  `GraphTx.Rollback` now restores label and relationship-type registries to
  their `BeginTx` snapshots after undoing entity/index mutations. Rolled-back
  transactions no longer leak tokens created by `AddNode`, `AddNodeLabel`, or
  relationship creation.
- **Delete batches now coalesce duplicate IDs.**
  Memory, Badger, and Tiered `DeleteNodesBatch` / `DeleteRelationshipsBatch`
  normalize duplicate IDs before validation and mutation. A duplicated delete
  target is applied once instead of panicking, double-decrementing Badger
  counters, or routing duplicate work across Tiered shards.
- **Batch partial failures now surface through the error return.**
  `BatchBuilder.Execute` still returns `BatchResult` with per-operation
  details, but any failed queued operation now also returns an error wrapping
  `ErrBatchFailed`. Callers that perform only the normal error check can no
  longer miss partial batch failure.
- **Batch builders now serialize their own lifecycle.**
  `BatchBuilder` queue methods and `Execute` are guarded by a builder mutex,
  and `Execute` marks the builder done as soon as replay begins. Later queue
  calls or repeat `Execute` calls return `ErrBatchDone`, preventing concurrent
  slice mutation and accidental replay of already-applied operations.
- **Event buses are now zero-value ready.**
  `events.EventBus.Subscribe` initializes its handler map lazily, and
  `events.AsyncEventBus` starts its dispatcher and queues lazily with default
  configuration on first use. Plain `var bus events.EventBus` and
  `var bus events.AsyncEventBus` values can subscribe, publish, batch-publish,
  close, and unsubscribe without panicking or blocking forever. Constructors
  remain convenience/configuration helpers, not required initialization steps.
- **Tiered label-removal coverage now exercises the direct backend APIs.**
  `tiered.Store.RemoveNodeLabelToken` and `RemoveNodeLabelTokenWithHistory`
  are covered with regression tests for label-index removal, vector-index
  refresh, history snapshots, and rejection of primary-label reference/event
  class changes.
- **Public transaction and batch wrappers now have direct coverage.**
  `g.Tx.Begin`, top-level `graph.NewBatchBuilder`, and `GraphTx.VerifyShard`
  are exercised directly instead of relying on examples or internal call sites
  to cover the exported wrapper methods.
- **Small forwarding/conversion helpers now have direct tests.**
  `pkg/graph/io.API.ImportWithOptions` is covered with an options-forwarding
  unit test, and `storeutil.ToRelIDs` is covered for nil and ordered conversion.
- **Index-provider graph reader adapters now have direct coverage.**
  `graphReaderView` read methods are exercised against real graph state so
  legacy and initializable providers no longer rely only on partial adapter
  coverage.
- **Badger incoming-index repair scan now has direct coverage.**
  `badger.Store.ScanAndDeleteIncoming` is covered with a persisted raw
  `in/` key deletion test.
- **Dead helper code removed from lock, routing, and history cursor paths.**
  Unused `runUnderLock`, `readUnderRLock`, and `timestampToEventShard` helpers
  were removed. Badger history cursor overflow handling now returns the only
  possible result directly instead of routing through unreachable pagination
  helper branches; max-cursor regression tests pin the behavior.
- **Unused test-export hooks removed from store packages.** Dead `*ForTest`
  accessors in the Badger and Tiered store packages were pruned instead of
  being carried as uncovered normal-package API surface.
- **Manual graph-lock paths now re-check lifecycle under the lock.**
  `IO.ImportWithOptions` now verifies `ErrGraphClosed` under the graph write
  lock before replaying staged records, closing a race where `Close` could
  complete during phase-1 reader I/O and phase 2 would still write to the
  already-closed graph. `IO.Export`, admin lock-taking methods, and provider
  unregister now use the same under-lock closed-state guard. Public read and
  index-management paths now also participate in the graph lifecycle lock, so
  `Close` drains them before closing Badger/tiered backing stores. The audit
  also covers node/relationship history reads, property queries, vector search,
  registry token creation, batch-builder queue methods, and no-error constraint
  mutation APIs, preventing registry/queue state changes from racing graph
  teardown. Remaining no-error ID allocation helpers (`Nodes.NextID`,
  `Rels.NextID`) are post-close zero-value reads instead of mutating closed graph
  state; event-bus setters now return `ErrGraphClosed` after close.
- **Temporal Allen helpers now reject nil entity pointers instead of panicking.**
  `g.Temporal.NodeInterval(nil)` and `RelateNodes` with a nil side return
  `ErrNilNode`; `RelInterval(nil)` and `RelateRels` with a nil side return the
  new `ErrNilRelationship` sentinel. Direct `errors.Is` regression tests cover
  both interval helpers and the relation wrappers.
- **Temporal vector search now surfaces candidate-resolution store errors.**
  Both the in-tree `FilteredVectorSearchCapability` path and the graph-layer
  over-fetch fallback now skip only expected temporal misses
  (`ErrNoVersionValidAt` / `ErrNodeNotFound`). History/store read failures from
  `findNodeVersionForOpts` are returned to the caller instead of being silently
  treated as vector ineligibility.
- **Current vector search now surfaces candidate-resolution store errors.**
  Badger and Tiered `SearchNearestNodes` now skip only `ErrNodeNotFound` while
  resolving ranked vector IDs. Corrupt or otherwise unreadable candidate rows
  are returned to the caller instead of collapsing the result set to `nil`.
- **Store-level vector search honors cursor pagination.**
  MemoryStore, BadgerStore, and TieredStore direct
  `SearchNearestNodes(..., QueryOpts{After, Limit})` calls now apply cursor
  pagination over distance-ranked results. Direct Store vector search also
  applies `ValidAt` / `ValidStart`+`ValidEnd` filters before heap selection, so
  a near-but-temporally-ineligible current node cannot occupy the k-cut and
  hide a farther eligible node. Graph-level `g.Index.SearchNearest` strips
  `After`/`Limit` before backend search and paginates the final
  current/historical result slice once, so direct Store callers and graph
  callers share the same contract without double-pagination.
- **ID-only queries honor temporal filters.**
  Store-level `AllNodeIDs(QueryOpts{ValidAt/ValidStart/ValidEnd})` and
  `AllRelIDs(...)` now return only current-row IDs that match the temporal
  filter before applying cursor pagination. The no-deserialization fast path is
  preserved for non-temporal ID scans; temporal ID scans fetch entity metadata
  because correctness depends on `TemporalMetadata`.
- **Badger temporal pages verify cache misses before consuming `Limit`.**
  BadgerStore `NodesByLabel`, `AllNodes`, `NodesByLabelAndProperty`,
  `RelationshipsByType`, and `AllRelationships` now apply `After` by ID first,
  then fetch cache-miss candidates and consume `Limit` only after the row passes
  the temporal filter. Cold-cache temporal pages after reopen can no longer let
  an expired early ID hide a later valid entity.
- **Tiered corrupt delete-with-history purges Store-level vector entries.**
  If the underlying Badger shard completes corrupt-node cleanup and returns the
  corruption error, `tiered.Store.DeleteNodeWithHistory` now still removes the
  node from the TieredStore-level vector index before returning the error. This
  prevents a deleted near vector from occupying the k-cut and hiding valid
  farther results.
- **Snapshot-style scans now exclude standalone mutations.**
  `g.IO.Export`, `g.Temporal.Snapshot`, and `g.Admin.VerifyShard` now hold the
  graph write lock while composing multi-read snapshots/scans. Standalone
  mutations can no longer interleave between export record groups, snapshot node
  and relationship reads, or shard hash-chain verification reads. The
  `GraphTx` variants remain for code that is already running under a transaction
  and must avoid re-entering `sync.RWMutex`; `VerifyShard` uses lock-free
  hash-chain helpers internally so its store-level scan does not re-enter the
  graph lock.
- **`RemoveLabelTokenRaw` contract now matches behavior.**
  `types.Node.RemoveLabelTokenRaw` has always refused to remove a single-label
  node's last label by returning `false`; the public comment no longer pushes
  that invariant onto callers, and direct `pkg/types` coverage pins the refusal.
- **Tiered node replacement enforces primary-label class immutability.**
  `tiered.Store.ReplaceNode` and `ReplaceNodeWithHistory` now reject
  reference↔event primary-label changes with `ErrPrimaryLabelClassMutation`,
  matching the label-token mutation guard. Tiered node writes also propagate
  old-state read errors before vector-index maintenance instead of treating
  them as a purge-only fallback.
- **Badger node update paths surface corrupt current rows.**
  `badger.Store.ReplaceNode`, label-token writes, and their history variants
  now return old-state read failures after confirming the node still exists,
  instead of silently purging secondary indexes and overwriting the corrupted
  durable row. Cascade delete keeps its explicit cleanup-and-return-corruption
  behavior.

### Changed — Maintainability review round 8 (2026-05-09)

- **Tiered vector depth filtering now covers event shards before the k-cut.**
  `tiered.Store.SearchNearestNodes` no longer treats `DepthHot` and
  `DepthWarm` as archive-only filters. Vector candidates on warm/cold
  event shards are filtered by shard tier before heap selection, so a
  near cold event vector cannot crowd out a farther hot/warm candidate
  when `k` is small. Regression coverage verifies `DepthAll`, `DepthWarm`,
  and `DepthHot` against cold, warm, and hot event-shard candidates.
- **Archive helper regression tests no longer rely on timing for distinct
  IDs.** Missing-endpoint cases now mint related node IDs from one
  snowflake generator instance so same-microsecond test setup cannot
  accidentally create a self-loop and skip the intended error path.
- **Tiered bulk `GetByIDs` now preserves the sorted result contract.**
  `tiered.Store.GetNodesByIDs` and `GetRelationshipsByIDs` sort by
  ascending ID before returning, matching the memory and Badger backends.
  Regression coverage passes reversed input with missing IDs through both
  the store backend and Graph sub-API entry points.
- **Tiered create paths now reject cross-shard duplicate IDs.**
  `PutNode`, `PutRelationship`, `PutNodesBatch`, and
  `PutRelationshipsBatch` probe ref/archive placement, the timestamp-routed
  event shard, and already-open event shards before writing, so the common
  caller-supplied duplicate cases cannot bypass the destination shard check
  just by changing the primary-label class or relationship start shard.
  Batch preflight rejects internal and resident cross-shard duplicates before
  any entity is written without waking unrelated closed cold shards.
- **Archive-close safety now uses stable pinned-snapshot identity.**
  `RunRepair` resolves relationship endpoints from the shard snapshot it
  already pinned instead of re-routing through live archive state. History
  reads and truncation use checked routers that return a stable
  "archive-owned" placement bit, so a concurrent `Close` that temporarily
  nils `refArchive` cannot make archived current entities skip their
  pre-archive history fan-out. Vector depth filtering and duplicate-ID probes
  also pin the archive before dereferencing it.
- **Vector index creation is now failure-atomic.** Badger no longer swallows
  operational `GetNode` errors during vector-index backfill, and Tiered removes
  its Store-level placeholder if the all-shard scan fails. Vector indexes now
  track IDs mutated during backfill, matching the property-index dirty-map
  pattern, so stale backfill cannot resurrect a vector that a concurrent update
  removed.
- **Depth-limited history iteration now includes restored reference entities.**
  `ForEachNodeHistoryIDByDepth` and `ForEachRelHistoryIDByDepth` no longer treat
  old archive history as proof that an ID is still archive-only. If the current
  node or relationship has been restored to `refShard`, `DepthHot` and
  `DepthWarm` include its history IDs while still excluding IDs whose only
  remaining state is archived.

### Changed — Maintainability review round 7 (2026-05-09)

- **Archive/restore now migrates cross-shard relationship placement.**
  `tiered.Store.ArchiveNode` no longer rejects relationships whose
  other endpoint remains live on `refShard` or an event shard. It moves
  the node row to `refArchive`, then moves each touching relationship's
  entity/out leg and incoming leg to the shards implied by the
  endpoints' new locations. `RestoreNode` applies the reverse move.
  Post-archive `PutRelationship`/`g.Rels.Add` with one archived
  endpoint now writes the same split placement instead of returning the
  old guard sentinel. Regression coverage verifies R→E, E→R, and R→R
  archive shapes plus Graph API archive/add/restore traversal.

### Changed — Maintainability review round 6 (2026-05-09)

- **Vector search now composes temporal filters with shard depth.**
  `g.Index.SearchNearest(..., QueryOpts{ValidAt: ..., Depth: DepthHot})`
  no longer returns `ErrDepthTemporalUnsupported`. For `DepthAll`,
  the existing `FilteredVectorSearchCapability` fast path still applies
  temporal eligibility before the k-cut. For depth-limited searches,
  the graph layer now drives the backend vector search with `Depth`
  first and then resolves temporal eligibility through the existing
  over-fetch loop, so archived-but-near candidates cannot crowd out
  farther hot/warm candidates. Regression coverage verifies `DepthAll`
  sees the closer archived node while `DepthHot` returns the eligible
  live node under the same temporal filter.
- **Bulk history-aware queries now compose temporal filters with shard
  depth.** `Nodes.All`, `Rels.All`, `Nodes.ByLabel`,
  `Nodes.ByLabelAndProperty`, and `Rels.ByType` now pass depth through
  both the current-index seed and the history-ID side of their
  candidate union. `tiered.Store` exposes a depth-aware history
  iteration capability so archived/cold history is excluded when a
  caller asks for `DepthHot` or `DepthWarm`; single-shard stores keep
  their existing "Depth is ignored" behavior. Regression coverage uses
  an archived self-loop node and verifies every bulk query includes the
  live entity and excludes the archived entity under `ValidAt+DepthHot`.

### Changed — Maintainability review round 5 (2026-05-09)

- **Post-close protection extended to every public sub-API entry
  point.** `*core.Core.checkOpen()` is the single primitive every
  sub-Core method calls before contacting the store, registries, or
  indexes. 30+ public APIs (Nodes/Rels reads, ByLabel/All/Count
  variants, Temporal point/interval queries, Snapshot, IO Export,
  Hash verification, Index management, Stats, Admin, Constraints,
  BeginTx, Batch.New, every BatchBuilder queue method, and
  Batch.Execute) now uniformly return `ErrGraphClosed` instead of
  racing the lifecycle teardown. `BatchBuilder.DeleteNode` /
  `DeleteRelationship` now return `error` so the close gate has a
  surface to report on (R5-F1, R5-F5).
- **Transaction-scoped snapshot APIs added on `*GraphTx`.**
  `sync.RWMutex` is not reentrant, so code that is already inside
  `g.Tx.Run` cannot call standalone snapshot-style APIs that acquire the
  graph lock. `(*GraphTx).Export(w)`, `(*GraphTx).Snapshot(at)`, and
  `(*GraphTx).VerifyShard(name)` call lock-free internal variants
  (`exportLocked`, `snapshotAt`, `verifyShardLocked`) under the
  transaction's already-held write lock (R5-F2).
- **Import validates label/reltype tokens against the registry.**
  `ImportWithOptions` rejects entity records whose `PrimaryLabel`,
  `ExtraLabels[i]`, or `RelType` token exceeds the imported
  registry's `Len()`. Pre-fix, a corrupt or hostile export carrying
  out-of-range tokens imported successfully and every label/type
  query against the entity silently resolved to "" (R5-F3).
- **History records get the same idempotent / conflict-rejection
  contract as current entities (R4-F12).** `PutNodeVersion` /
  `PutRelVersion` silently overwrite, so the import path now reads
  the existing version first; identical content is skipped, divergent
  content is rejected with `ErrCorruptExport`. Re-importing a graph's
  own export remains the supported idempotent workflow (R5-F4).
- **`AddByID` / `AddByIDIfAbsent` enforce graph-level constraints.**
  When `ConstraintRelWithinEndpoints` (or any endpoint-dependent
  constraint) is configured on the graph, the ByID variants
  transparently fetch the live endpoints under the endpoint lock and
  run the same constraint check that `Rels.Add` runs. Silent constraint
  bypass via the ByID entry point is no longer possible (R5-F7).
  A later round made the live-endpoint fetch unconditional so the ByID
  variants also capture endpoint hashes without requiring constraints.
- **Rel-type token allocation deferred past every endpoint-fetch
  failure path.** Round 4 (R4-F14) deferred allocation past cheap
  validation gates; round 5 pushes it past the operational store
  failures too. `addRelationshipInternal` allocates only after both
  GetNode calls succeed; `importRelWithIDInternal` allocates only
  after the collision probe AND the live-endpoint fetches succeed;
  `addRelationshipByIDIfAbsentInternal` uses `Lookup` to skip the
  duplicate-existence check entirely when the type is unknown,
  allocating only on the create path. The remaining unavoidable
  pollution path (PutRelationship store-write failure) is documented
  inline because the rel object literally needs the token at
  construction time (R5-F6).
- **Async event bus exposes `PublishBatch(events ...Event)`.** A
  single `publishMu`-protected enqueue lets a publisher insert N
  events into the priority-queue array atomically before the dispatcher
  is woken up, restoring strict global priority ordering under
  bursty publishes. Sequential `Publish` calls do not guarantee
  ordering (and never did under burst); the strict-ordering test
  was migrated to the new API and now passes 100/100 iterations
  (R5-F12).
- **`TestExportGraph_HistoryBoundedMemory` is no longer flaky.**
  The export retains nothing past return; the test was misreading
  transient cursor state as retention. Forcing GC after the export
  call (`runtime.GC(); runtime.GC()`) makes the retained-heap delta
  measurement deterministic. 20 consecutive runs pass.
- **`resolveTemporalVectorMatches` moved out of production.** The
  test-only helper now lives in `vector_correctness_test.go` and
  no longer compiles into the production binary (R5-F11).
- **Production files split for review-by-feature (R5-F9).** Eight files
  greater than 568 LOC carved into behavior-aligned siblings without
  changing exported APIs:
  - `pkg/graph/internal/core/batch.go` 727 → `batch.go` 114 +
    `batch_queue.go` 277 + `batch_execute.go` 355.
  - `pkg/graph/internal/core/export.go` 707 → `export.go` 275 +
    `import.go` 449 (wire format / Export path vs. Import +
    per-record validators).
  - `pkg/graph/internal/storeutil/wire.go` 657 → `wire.go` 283 +
    `wire_value.go` 382 (entity converters vs. property type-tag
    reconstruction).
  - `pkg/graph/store/tiered/tieredstore_read_history.go` 568 → 341 +
    `tieredstore_read_history_rel.go` 240 (node-history vs.
    rel-history).
  - `pkg/graph/store/tiered/tieredstore_read_bulk.go` 603 → 317 +
    `tieredstore_read_bulk_rel.go` 297.
  - `pkg/graph/store/badger/badgerstore_rel.go` 761 → `badgerstore_rel.go`
    299 + `badgerstore_rel_query.go` 322 + `badgerstore_rel_batch.go`
    161 (CRUD vs. query vs. batch).
  - `pkg/graph/store/badger/badgerstore_node.go` 964 →
    `badgerstore_node.go` 448 + `badgerstore_node_query.go` 262 +
    `badgerstore_node_batch.go` 279.
  - `pkg/graph/store/badger/badgerstore_history.go` 1109 → 105
    (shared helpers) + `badgerstore_history_node.go` 562 +
    `badgerstore_history_rel.go` 393.
- **Wall-clock sleep migration in tests (R5-F10).** 64 of 121
  `time.Sleep` calls eliminated via three reusable patterns:
  - `useTestClock(t, g)` + `clk.PeekInstant()` for "after-mutation"
    query anchors — works wherever the c.now()-driven UpdatedAt or
    TxFrom is what the test was waiting on.
  - `clk.Advance(d)` to widen the gap between UpdatedAt values so a
    midpoint anchor lands strictly between two versions.
  - Explicit `tkg_valid_from` on Add to pin temporal ordering when
    the test was relying on wall-clock-spread snowflake IDs (vector
    eligibility tests, diff-window tests).

  Files now at 0 sleeps: `temporal_test.go` (22→0),
  `temporal_queries_rel_parity_test.go` (8→0),
  `findings_extra_regression_test.go` (6→0), `txtime_test.go` (3→0),
  `vector_correctness_test.go` (14→0), `diff_test.go` (3→0),
  `graph_temporal_test.go` (2→0), `foreach_test.go` (2→0),
  `diff_callback_test.go` (2→0), `v3061_fixes_test.go` (1→0),
  `bench_production_test.go` (1→0).

  The remaining ~57 sleeps test genuinely wall-clock-scheduled
  behaviour: tiered-store hot/warm/cold shard rotation depends on
  wall-clock shard window boundaries; async event bus tests assert
  worker dispatch latency; badger flush tests wait for the auto-flush
  goroutine; production code in `tieredstore.go` waits on
  `activeReqs` to drain on Close. Migrating these would require
  injecting a clock into the snowflake generator (cross-repo) and the
  shard rotation scheduler (intrusive) — accepted as out of scope for
  R5-F10.
- **Documentation drift corrected.** The `UpdateRelationship` lock
  table in `docs/architecture.md` reflects the actual `LockMany(rel,
  start, end)` triple instead of the obsolete `LockEntity(id)`
  single-lock; `docs/design.md` now describes `NodeID` / `RelID` /
  `EntityID` as the customer-facing exported wrapper types (the
  pre-v3.4.0 unexported aliases are noted as historical). The
  misleading "hold c.mu.RLock for strong consistency" advice on
  `TempOps.Snapshot` was removed; snapshot-style scans now take the graph
  write lock directly, and the `GraphTx` methods are for transaction-scoped
  callers that already hold it (R5-F8).

### Changed — Maintainability review round 2 (2026-05-08)

- **Admin mutators now serialise against tx/batch.** `g.Admin.ForceRotate`,
  `RebuildCatalog`, `Repair`, and `VerifyShard` take `c.mu.Lock`;
  `ListShards` takes `c.mu.RLock`. Previously these bypassed the graph
  write lock, so `Reset` could race a concurrent `ForceRotate` (R2-F1).
- **Hot-shard rotation is transactional around catalog persist.**
  `RotateHotShard` now opens the new badger shard, snapshots the
  catalog, applies tentative catalog mutations, persists, and only then
  switches the live `hotShard` pointer. On `catalog.Save` failure the
  in-memory catalog is restored from the snapshot, the new shard is
  closed and removed, and live topology is unchanged. Previously a
  Save failure left the running process with a switched-over hotShard
  but a durable catalog describing the old topology — split-brain on
  restart (R2-F2). New helpers: `ShardCatalog.snapshotShards()` /
  `restoreShards(snapshot)`.
- **Public mutation surface is panic-safe.** Every public mutation
  entry point on `g.Nodes`, `g.Rels`, `g.Constraints`, the version-chain
  helpers, and the property-CAS helpers is wrapped with a defer-backed
  `RUnlock` via the new `(*Core).runUnderRLock(fn)` /
  `runUnderLock(fn)` helpers. Includes the `BatchBuilder.Execute`
  per-relationship endpoint locks (`LockTwo`/`UnlockTwo`) and the
  two-phase `deleteNodeInternal` (Phase A node lock, Phase B
  `LockMany`) — both now release on every panic path. Previously a
  panic from a custom Store could leak the graph read lock or the
  shard locks for the rest of the process lifetime, deadlocking
  `Close` and any subsequent mutation hashing to the same shard
  (R2-F3, generalised across 20 sites).
- **`g.Nodes.ByLabelAndProperty` works on mandatory-only backends.**
  The capability split previously bundled the correctness-level query
  with optional index management; backends that satisfied
  `MandatoryStore` but not `PropertyIndexCapability` got
  `ErrCapabilityNotSupported`. The graph layer now falls back to a
  label scan + property filter via the mandatory `NodesByLabel` surface
  when the optional capability is absent. Index management
  (`g.Index.CreateProperty` / `DropProperty`) remains optional and
  still surfaces the typed sentinel (R2-F4).
- **Vector search top-k correctness is preserved on backends that
  cannot pre-filter.** `FilteredVectorSearchCapability` is now a
  public optional capability in `pkg/graph/store`. Backends that
  implement it get pre-filter semantics (top-k taken from the
  eligible-only set); backends that do not get iterative over-fetch
  (`k → 2k → 4k …` up to 65536) until either k eligible results are
  accumulated or the backend is exhausted. Previously the same public
  API silently returned fewer than k results when the nearest k
  vectors were all temporally ineligible (R2-F5).
- **`g.IO.ImportWithOptions(r, ImportOptions)` for staging control.**
  New `io.ImportOptions{StagingDir, MaxStagedBytes}` lets callers
  redirect the per-import temp file off the platform default temp
  volume and bound the staged-disk usage. `MaxStagedBytes > 0` causes
  Phase 1 to surface the new `core.ErrImportSizeLimit` sentinel before
  any live mutation (R2-F6). `g.IO.Import(r)` keeps prior behaviour
  via default options.
- **Public docs realigned with the v3.4 sub-API surface.** `docs/api.md`
  and `docs/SPEC.md` rewritten to reference `g.Nodes.*` / `g.Rels.*` /
  `g.IO.*` / `g.Admin.*` / `g.Tx.*` / `g.Batch.*` / `g.Resolve.*` /
  etc. instead of the removed direct `Graph.AddNode` style methods.
  Cypher integration is now correctly described as a downstream
  (`rho/tkgd-v3`) concern, not a feature of this library.
  `docs/architecture.md` "Two-Phase Operations" updated: batch is per-
  operation result reporting, not all-or-nothing (R2-F7).

Lessons logged to `tasks/lessons.md`: B49 (mutating admin must join
exclusion domain), B50 (every secondary lock needs an immediate
defer), B51 (optional capabilities must separate correctness from
acceleration), B52 (public docs must be compiled against the current
public surface).

### Changed — Maintainability review (2026-05-08)

- **`TxAPI.Run` / `RunContext` are now panic-safe.** A panic inside the
  user callback used to leak the graph write lock and silently drop any
  rollback error. Both wrappers now use a `defer` guard that runs
  `Rollback` on every non-success path and joins any rollback error
  with the caller's. Pinned by `pkg/graph/tx_run_test.go`.
- **Property validation now matches what hashing and the wire format
  support.** Maps with non-string keys, concrete `[]any{}`-style
  value-form maps, named scalar/container aliases, and unsupported concrete
  slices are rejected at `Set`/`NewPropertySlice` rather than panicking later
  in the integrity hash. Pointer-only registered custom property types now
  reject value-form storage at validation.
- **Documented isolation contract.** `docs/architecture.md` no longer
  implies snapshot isolation. Read APIs do NOT acquire `g.mu` and
  concurrent reads can observe a transaction's already-applied (and
  possibly rolled-back) mutations.
- **Relationship endpoint hash refresh propagates errors.** The
  standalone update path and the batch path used to swallow every
  non-nil error from the endpoint `GetNode`, writing relationships
  with empty `FromNodeHash`/`ToNodeHash` on operational failures. Both
  paths now distinguish `ErrNodeNotFound` (silent — endpoint was
  cascade-deleted) from any other error (surfaced).
- **`IO.Import` memory is bounded.** Phase 1 streams records to an
  `os.CreateTemp("tkg-import-*.stage")` staging file instead of an
  in-memory buffer. Phase-1 staging memory is `O(maxExportRecordSize)`
  regardless of export size; replay still keeps rollback state for
  touched rows.
- **`Store` decomposed into capability interfaces.** The 65-method
  contract is now an embedded composition of 13 narrower capabilities
  in `pkg/graph/store/capabilities.go`. Four optional capabilities
  (`PropertyIndexCapability`, `TemporalIndexCapability`,
  `VectorIndexCapability`, `HighFrequencyIndexCapability`) can be
  omitted by future backends; the graph layer's internal store field
  is `MandatoryStore` and call sites that need an optional capability
  type-assert and surface a new `ErrCapabilityNotSupported` sentinel
  on miss. Per-backend compile-time assertions live in
  `{memory,badger,tiered}/capabilities.go`. Public contract unchanged
  — every in-tree backend still satisfies the full composition.
- **Backend file splits.** `memorystore.go` 1925 LOC → 119 + 5
  satellites; `tieredstore.go` 1237 → 532 + 3; `tieredstore_read.go`
  1787 → 190 + 4; `tieredstore_write.go` 1545 → base + 5. Moves-only,
  byte-identical bodies.
- **Badger index-rebuild diagnostics.** `loadIndexes` silent skips
  replaced with `IndexRebuildStats().PropertySkipped` /
  `TemporalSkipped` counters and per-skip `Warningf` log entries; new
  `(*badger.Store).IndexRebuildStats()` accessor.
- **Sub-API wrapper coverage.** Every wrapper in the 13 sub-API
  accessor packages is now invoked by
  `pkg/graph/subapi_smoke_test.go` +
  `pkg/graph/subapi_smoke_extra_test.go`. Total `./pkg/...` coverage
  is 86.6%. New `make cover-gate` target enforces a configurable floor
  (default 80%); `make ci` runs it.

Lessons logged to `tasks/lessons.md`: B45 (property validation must
match hash/wire/copy), B46 (transaction wrappers must be panic-safe),
B47 (`err == nil { use }` is not the same as tolerating one sentinel),
B48 (tolerated-on-rebuild errors must be counted, not continued).

### Changed — BREAKING

- **`Graph.Core()` escape hatch removed.** The experimental accessor
  that returned the underlying `*core.Core` is gone. Use the sub-API
  fields (`g.Nodes`, `g.Rels`, `g.Stats`, …) for everything that was
  previously reachable through it. The full `GraphStats` snapshot
  (operation counters + cache metrics) is now reachable as
  `g.Stats.Get()`; the `pkg/graph/stats` package re-declares the
  `GraphStats` struct so the sub-API import does not pull in
  `pkg/graph/internal/core`.
- **All 11 sub-API packages now use short, suffix-free names.** The
  earlier "collapse temporal/events/index" change has been extended to
  the remaining six sub-API packages:
  - `pkg/graph/temporalapi`    -> `pkg/graph/temporal`    (collapsed; both types and API in one package)
  - `pkg/graph/eventsapi`      -> `pkg/graph/events`
  - `pkg/graph/indexapi`       -> `pkg/graph/index`
  - `pkg/graph/adminapi`       -> `pkg/graph/admin`
  - `pkg/graph/constraintsapi` -> `pkg/graph/constraints`
  - `pkg/graph/hashapi`        -> `pkg/graph/hash`        (shadows stdlib `hash`)
  - `pkg/graph/ioapi`          -> `pkg/graph/io`          (shadows stdlib `io`)
  - `pkg/graph/resolveapi`     -> `pkg/graph/resolve`
  - `pkg/graph/statsapi`       -> `pkg/graph/stats`
  Customers that imported any of these packages must update their
  import path and the package selector accordingly (`adminapi.API` ->
  `admin.API`, etc.). The `g.<Field>` accessors are unchanged except
  for `Statistics` (see next entry).
- **`Graph.Statistics` field renamed to `Graph.Stats`.** The
  `Statistics` workaround field name was needed only because the old
  `Graph.Stats() GraphStats` method (removed in v3.4.0) used the same
  identifier. With the package now named `pkg/graph/stats`, the field
  is renamed to match. Migration: `g.Statistics.NodeCount()` ->
  `g.Stats.NodeCount()`. The full `GraphStats` struct (atomic
  operation counters + cache metrics) is reachable via
  `g.Stats.Get()`.

#### Stdlib aliasing for hash and io

The new `pkg/graph/hash` and `pkg/graph/io` packages shadow stdlib
`hash` and `io`. Inside the local packages no aliasing is required —
Go resolves `hash.Hash` / `io.Reader` to the imported stdlib package.
At consumer sites that import BOTH stdlib `"hash"` (or `"io"`) AND the
local sub-API package, alias the local one with a `tkg` prefix:

```go
import (
    "io"
    tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/io"
)

var _ tkgio.API
```

In practice few sites need the alias because customers usually reach
the sub-API via `g.IO` / `g.Hash` (field access — no package import
needed) and only import the package directly when they need the
exported type.

#### Migration cheat sheet

```go
// Before                         // After
g.Statistics.NodeCount()         g.Stats.NodeCount()
g.Statistics.AllLabelCounts()    g.Stats.AllLabelCounts()
g.Admin.Archive(id)              g.Admin.Archive(id)               // unchanged
g.Hash.VerifyNodeChain(id)       g.Hash.VerifyNodeChain(id)        // unchanged
import "...tkg/v3/pkg/graph/adminapi"      ->  "...tkg/v3/pkg/graph/admin"
import "...tkg/v3/pkg/graph/statsapi"      ->  "...tkg/v3/pkg/graph/stats"
import "...tkg/v3/pkg/graph/hashapi"       ->  "...tkg/v3/pkg/graph/hash"
import "...tkg/v3/pkg/graph/ioapi"         ->  "...tkg/v3/pkg/graph/io"
adminapi.API                     admin.API
adminapi.New(c)                  admin.New(c)
```

- **`*core.Core` implementation split into 11 sub-Core types matching the public sub-API surface (internal only).**
  The 130+ method bodies that used to live directly on `*Core` moved
  to dedicated `*NodeOps`, `*RelOps`, `*TempOps`, `*IndexOps`,
  `*EventOps`, `*AdminOps`, `*ConstraintOps`, `*HashOps`, `*IOOps`,
  `*ResolveOps`, `*StatOps` types declared in
  `pkg/graph/internal/core/subops.go`. `*Core` is now a coordinator
  holding these as exported fields wired in `core.New`. Method names
  drop their type prefix (`AddNode → NodeOps.Add`, `GetRelationship →
  RelOps.Get`, `VerifyNodeHashChain → HashOps.VerifyNodeChain`, etc.)
  so the call chain `g.Nodes.Add()` → `nodes.API.Add()` →
  `core.NodeOps.Add()` no longer renames the method across the
  wrapper boundary. Each sub-API wrapper at `pkg/graph/<name>/api.go`
  was restructured from a `Core` interface to an `Ops` interface that
  matches the new sub-Core method shapes 1:1. **No public-API change**
  — `*core.Core` is internal, customers only see the unchanged
  sub-API surface (`g.Nodes.Add`, `g.Stats.Get`, etc.). Internal
  callers (tx, batch, store wrappers) and tests were updated; the
  `Store` / `BatchBuilder` / `GraphTx` types kept their old method
  names because they do not expose the sub-API surface.
- **`pkg/graph/temporalapi`, `pkg/graph/eventsapi`, `pkg/graph/indexapi` collapsed into their types-only siblings.**
  The three sub-API packages were merged into the existing types-only
  packages they used to forward to:
  - `pkg/graph/temporalapi` -> `pkg/graph/temporal`
  - `pkg/graph/eventsapi`   -> `pkg/graph/events`
  - `pkg/graph/indexapi`    -> `pkg/graph/index`
  Customers that imported `temporalapi.API`, `eventsapi.API`, or
  `indexapi.API` must update their import path and rename to
  `temporal.API`, `events.API`, or `index.API` respectively. The
  `Graph.Temporal`, `Graph.Events`, `Graph.Index` field accessors
  themselves are unchanged — only the underlying package names move.
  No semantics change.

### Added

- **`make lint` target and `.golangci.yml` configuration** integrated
  into `make ci` (after `vet`, before `build`). Linters enabled:
  `errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`,
  `revive`, `misspell`, `unconvert`, `unparam` plus `gofmt` /
  `goimports` formatters. Dead helpers, redundant type conversions,
  and missing doc comments fixed inline (no broad suppressions).

### Security

- **`toolchain go1.26.2` directive added to `go.mod`.** govulncheck flagged
  `GO-2026-4865` (JsBraceDepth context-tracking bugs in `html/template`)
  as a stdlib-level concern, fixed in `html/template@go1.26.2`. The
  codebase does not import `html/template` directly — the trace runs
  through `fmt.Sprintf`'s error formatter — so the vulnerability is
  practically unreachable, but bumping the preferred toolchain to
  `go1.26.2` is the documented mitigation. The `go 1.26.1` minimum
  requirement is unchanged.

### Documented

- **`race_off_test.go` / `race_on_test.go` cannot collapse via
  `runtime.RaceEnabled`** — that constant lives in the build-tagged
  `runtime/race` internal package and is not reachable from external
  code without using the same `//go:build race / !race` tags the pair
  already employs. The two files retain a one-line note explaining the
  rationale so future maintainers do not redo the experiment.

### Added

- **Example tests in every sub-API package.** Each of the 11 sub-API
  packages (`nodes`, `rels`, `temporal`, `index`, `events`,
  `constraintsapi`, `ioapi`, `adminapi`, `statsapi`, `hashapi`,
  `resolveapi`) plus the in-package `Tx` and `Batch` sub-APIs now has
  an `example_test.go` demonstrating the most-common operations, so
  godoc renders idiomatic usage hints. Examples are compile-only
  (no `// Output:` blocks because snowflake IDs are nondeterministic).
- **`tutorials_test.go` in every tutorial directory.** Empty
  `TestCompiles` placeholder ensures `go test ./tutorials/...` exercises
  the build of every tutorial, catching public-API breakage immediately
  instead of when a user runs the tutorial.

### Changed

- **`pkg/graph/internal/core/graph_test.go` (4970 LOC) split into themed
  files.** Functions moved byte-identically into `graph_node_test.go`,
  `graph_node_validation_test.go`, `graph_node_history_test.go`,
  `graph_node_hash_test.go`, `graph_rel_test.go`,
  `graph_rel_validation_test.go`, `graph_rel_history_test.go`,
  `graph_rel_hash_test.go`, `graph_temporal_test.go`,
  `graph_index_test.go`, `graph_cas_test.go`, `graph_label_test.go`,
  `graph_lifecycle_test.go`, `graph_tx_test.go`, `graph_misc_test.go`.
  All split files are below 1500 LOC. No test bodies were changed.
- **`pkg/graph/store/badger/badgerstore.go` (4262 LOC) split into themed
  files.** Methods moved byte-identically into `badgerstore.go` (struct,
  lifecycle: `New`, `Close`, `Clear`, `loadIndexes`),
  `badgerstore_node.go`, `badgerstore_rel.go`, `badgerstore_index.go`,
  `badgerstore_history.go`, `badgerstore_temporal.go`,
  `badgerstore_meta.go`, `badgerstore_flush.go`. All split files are
  below 1500 LOC. No method bodies were changed.

- **Doc cleanup post-v3.4.0**:
  - `CLAUDE.md` and `AGENTS.md` Status lines now describe the v3.4.0
    reality (sub-APIs are the only public surface; the 130+ direct
    `*Graph` methods were removed). Previous wording incorrectly
    described the change as "additive".
  - Stale `Graph.Stats()` comments in `pkg/graph/internal/core/stats.go`
    and `pkg/graph/statsapi/api.go` updated to point at
    `g.Core().Stats()` (the public method was removed in v3.4.0; the
    `Core()` escape hatch itself was subsequently removed in the same
    Unreleased cycle, see "Changed — BREAKING" above).
  - `README.md`, `docs/api.md`, `docs/architecture.md`,
    `docs/persistence.md`, and `docs/design.md` updated to use the
    `memory.Store` / `badger.Store` / `tiered.Store` names introduced
    in v3.3.0. Historical "What's new" entries are intentionally left
    untouched since they describe state at the time of those releases.

### Added

- **Streaming `DiffSnapshotsCallback` (resolves the `TODO(v3.1.0)` in
  `pkg/graph/internal/core/temporal.go`).** A new method on `*core.Core`
  surfaces entity diffs between two instants via handler callbacks
  instead of materialising both `*GraphSnapshot` results plus the two
  ID-keyed maps `buildDiff` previously built. The peak working set is
  now O(|distinct entity IDs| × ~24B for the dedup map) plus one
  before/after entity pair at a time, down from O(|entities valid at
  t1| + |entities valid at t2|). Asymptotically — 5M nodes valid at both
  timestamps — this eliminates ~1.5 GB of transient allocation.
  - The new `temporal.DiffHandlers` struct in `pkg/graph/temporal`
    declares one optional callback per change class
    (`OnNodeCreated`, `OnNodeUpdated`, `OnNodeDeleted`,
    `OnRelCreated`, `OnRelUpdated`, `OnRelDeleted`). nil fields are
    skipped. Returning a non-nil error from any callback aborts
    iteration and propagates the error verbatim.
  - The existing `DiffSnapshots(t1, t2)` API stays — it now delegates
    to `DiffSnapshotsCallback` with handlers that accumulate into a
    `*SnapshotDiff`. Callers that need the materialised result are
    unaffected; callers willing to consume changes streaming-style
    avoid the snapshot allocations entirely.
  - The Temporal sub-API exposes `g.Temporal.DiffCallback(t1, t2, h)`
    forwarding to the new method.
  - Relationship endpoint filtering preserves parity with `snapshotAt`:
    a relationship is reported only when both endpoints are valid at
    the queried instant. The streaming path applies this filter
    per-entity rather than via two whole-graph node-validity sets.
- **Cursor-paginated history-ID scans (`AllNodeHistoryIDsFrom` / `AllRelHistoryIDsFrom`).**
  Two new methods on the `Store` interface return the IDs of nodes /
  relationships with version-history entries, sorted ascending, starting
  strictly after a caller-supplied cursor and capped at a caller-supplied
  limit. Implemented in `pkg/graph/store/{memory,badger,tiered}`.
  - **MemoryStore** sorts the in-memory history map and applies
    `storeutil.PaginateNodeIDs` / `PaginateRelIDs` for an O(N log N + N)
    one-shot result.
  - **BadgerStore** seeks the `0x07` (node-history) / `0x08`
    (rel-history) prefix at `[prefix, after+1]` and merges the seek stream
    with the pending write-buffer entries strictly greater than `after`,
    deduplicating same-ID version-suffix repeats. Memory is bounded by
    `limit`, not total history depth.
  - **TieredStore** walks the reference shard, the reference archive, and
    every event shard sequentially via `checkout/checkin` — only one
    shard's iterator is open at a time. Cross-shard duplicates collapse
    via a `seen` set bounded by the IDs returned in the current page.
  - The legacy `AllNodeHistoryIDs()` / `AllRelHistoryIDs()` methods are
    retained for backward compatibility and now delegate to the
    paginated form (`limit == 0` means "all remaining").

### Changed

- **`ExportGraph` history phase is now bounded-RAM** (resolves the
  `TODO(v3.1.0)` in `pkg/graph/internal/core/export.go`). The export
  loop pages history-ID scans at `exportHistoryBatchSize = 4096` IDs per
  call, eliminating the OOM risk at large history depths
  (e.g., 10K nodes × 1K versions = 10M IDs would previously have
  materialised in a single slice).
- **TieredStore parallel-merge transient eliminated.** The previous
  `AllNodeHistoryIDs` / `AllRelHistoryIDs` implementations spun a
  goroutine per event shard and held all per-shard ID slices in RAM
  simultaneously before dedup-mapping at the end (~400MB transient on
  52-shard year-long graphs). The new sequential checkout/checkin walk
  removes both the goroutine fan-out and the global dedup map. The
  legacy methods are now thin wrappers that page the new API at a
  generous in-memory page size.

### Tests

- `pkg/graph/store/memory/history_cursor_test.go` — empty graph, limit=0
  parity, after-past-last, paginated-walk-equals-unpaginated for both
  node and rel history.
- `pkg/graph/store/badger/history_cursor_test.go` — same coverage plus
  pending-only / persisted / mixed buffer scenarios and the
  same-ID-multiple-versions dedup case.
- `pkg/graph/store/tiered/history_cursor_test.go` — empty graph,
  limit=0 parity, paginated walk across ref + event shards, cross-shard
  same-ID dedup.
- `pkg/graph/internal/core/export_history_bounded_test.go` — seeds
  1K nodes × 50 versions = 50K history records and asserts that
  `ExportGraph`'s heap delta stays under 16 MiB. Without the cursor the
  history-ID slice alone would allocate 40+ MiB.
- `pkg/graph/internal/core/diff_callback_test.go` — covers the streaming
  `DiffSnapshotsCallback` path: parity with `DiffSnapshots`, two-phase
  rule-15 lifecycle (create-update-delete with three diff windows
  asserting the correct before/after state in each), handler abort
  propagation, nil-handler safety, empty-graph short-circuit, invalid
  time-range sentinel (`ErrInvalidTimeRange`), relationship endpoint
  filter, and an informational RAM measurement at 20K stable nodes + 10
  updates that logs `TotalAlloc` and peak `HeapInuse` for both paths.
  (The asymptotic snapshot-buffer savings the design intends are
  dominated by per-entity deep-copy traffic at the in-process scale this
  test can exercise; the asymptotic improvement is documented above.)

## [3.4.0] - 2026-05-07

### Sub-API accessors on Graph (Option 3) — BREAKING

The 130+ public methods that previously lived directly on `*Graph` have been
**removed** and reorganized into 13 discoverable sub-API accessors. Customers
MUST migrate every call site to the sub-API form — see the migration table
below. The implementation has been extracted into `pkg/graph/internal/core`;
`*Graph` is now a thin façade with the sub-API field accessors plus `New`,
`Close`, and `Core` (escape hatch).

#### Migration

```go
// REMOVED in v3.4.0                            // Replacement
g.AddNode(labels, props)                        g.Nodes.Add(labels, props)
g.GetNode(id)                                   g.Nodes.Get(id)
g.NodesByLabel("Person", opts)                  g.Nodes.ByLabel("Person", opts)
g.UpdateNode(id, updates)                       g.Nodes.Update(id, updates)
g.DeleteNode(id)                                g.Nodes.Delete(id)
g.AddRelationship(typ, a, b, props)             g.Rels.Add(typ, a, b, props)
g.OutgoingRelationships(id, "")                 g.Rels.Outgoing(id, "")
g.GetNodesValidAt(t)                            g.Temporal.NodesAt(t)
g.GetNodesByLabelValidAt(label, t)              g.Temporal.NodesByLabelAt(label, t)
g.Snapshot(t)                                   g.Temporal.Snapshot(t)
g.DiffSnapshots(t1, t2)                         g.Temporal.Diff(t1, t2)
g.CreatePropertyIndex(label, key)               g.Index.CreateProperty(label, key)
g.SearchNearestNodes(label, key, vec, k, opts)  g.Index.SearchNearest(label, key, vec, k, opts)
g.RegisterIndexProvider(p)                      g.Index.RegisterProvider(p)
g.SetEventBus(eb)                               g.Events.SetSync(eb)
g.SetAsyncEventBus(b)                           g.Events.SetAsync(b)
g.RunRepair()                                   g.Admin.Repair()
g.ListShards()                                  g.Admin.ListShards()
g.ExportGraph(w)                                g.IO.Export(w)
g.ImportGraph(r)                                g.IO.Import(r)
g.VerifyNodeHashChain(id)                       g.Hash.VerifyNodeChain(id)
g.VerifyRelHashChain(id)                        g.Hash.VerifyRelChain(id)
g.SetTemporalConstraints(cs)                    g.Constraints.Set(cs)
g.AddTemporalConstraint(c)                      g.Constraints.Add(c)
g.TemporalConstraints()                         g.Constraints.Get()
g.NodeCount()                                   g.Stats.NodeCount()
g.RelationshipCount()                           g.Stats.RelCount()
g.NodeCountByLabel("Person")                    g.Stats.NodeCountByLabel("Person")
g.AllLabelCounts()                              g.Stats.AllLabelCounts()
g.ResolveNodeProperty(n, "tkg_hash")            g.Resolve.NodeProperty(n, "tkg_hash")
g.GetOrCreateLabel("Person")                    g.Resolve.LabelToken("Person")
g.LookupLabel("Person")                         g.Resolve.LookupLabel("Person")
g.BeginTx()                                     g.Tx.Begin()
                                                g.Tx.Run(func(tx) error { ... })
NewBatchBuilder(g)                              g.Batch.New()
```

The 13 sub-API accessors:

| Field          | Package                              | Methods | Purpose                                                                |
|----------------|--------------------------------------|---------|------------------------------------------------------------------------|
| `g.Nodes`      | `pkg/graph/nodes`                    | 31      | Node CRUD, label, property, version chain                              |
| `g.Rels`       | `pkg/graph/rels`                     | 30      | Relationship CRUD, adjacency, property, version chain                  |
| `g.Temporal`   | `pkg/graph/temporal`                 | 24      | Point-in-time, interval, bitemporal, snapshot/diff, Allen relations    |
| `g.Index`      | `pkg/graph/index`                    | 13      | Property/vector/high-frequency index management + IndexProvider        |
| `g.Events`     | `pkg/graph/events`                   | 3       | Sync/async EventBus management                                         |
| `g.Constraints`| `pkg/graph/constraints`              | 3       | Temporal-constraint set management                                     |
| `g.IO`         | `pkg/graph/io`                       | 2       | Export / Import                                                        |
| `g.Admin`      | `pkg/graph/admin`                    | 9       | Tiered-store admin (archive, repair, shards, rotate, reset)            |
| `g.Stats`      | `pkg/graph/stats`                    | 6       | Count helpers                                                          |
| `g.Hash`       | `pkg/graph/hash`                     | 2       | Hash-chain verification (shadows stdlib `hash` — alias as `tkghash` if needed) |
| `g.Resolve`    | `pkg/graph/resolve`                  | 6       | Shadow-property + registry resolution                                  |
| `g.Tx`         | `pkg/graph` (TxAPI in subapi.go)     | 3       | Transaction begin / Run / RunContext                                   |
| `g.Batch`      | `pkg/graph` (BatchAPI in subapi.go)  | 1       | New BatchBuilder                                                       |

#### Why

Discoverability for 130+ methods on a single type was poor. After tab-completing
`g.` in any LSP-aware editor, customers saw an alphabetic dump of every method.
Now customers see 13 categories. The pattern matches Kubernetes client-go
(`clientset.CoreV1()`), GitHub Go SDK (`client.Issues`), and AWS SDK v2 service
clients.

#### Implementation note

Each sub-API package declares a local `Core` interface listing only the methods
its wrappers forward to. The interface is satisfied implicitly by
`*core.Core` (the new internal type holding all state and method bodies).
Sub-API constructors take a `*core.Core` directly. Wrappers are 1-2 lines
each, single indirect dispatch — no devirtualization, but the benchmark
gate measured no regression vs v3.3.0.

#### Performance

`benchstat` over n=3 iterations on every Reads/Writes sub-benchmark
(MemoryStore, Apple M4 Max). Geomean Δ +0.44% on time, +0.01% on bytes/op,
+0.00% on allocs/op — well within the 2% tolerance gate. Allocations per op
are byte-for-byte identical for every existing-method benchmark. The sub-API
additions are effectively free at runtime — only customers who actually call
`g.Nodes.X(...)` pay the indirect-dispatch cost, and only once per call.

#### Public surface on `*Graph` after this MR

- `New(cfg Config) (*Graph, error)`
- `(*Graph).Close() error`
- `(*Graph).Core() *core.Core` (escape-hatch, not part of the stable customer surface)
- The 13 sub-API fields listed in the table above
- `NewBatchBuilder(g *Graph) *BatchBuilder` (free function, equivalent to `g.Batch.New()`)
- The type aliases re-exported for convenience: `Config`, `ValidationLimits`,
  `GraphTx`, `BatchBuilder`, `BatchResult`, `BatchError`, `GraphStats`,
  `StoreStats`, `IDComponents`, `ConstraintSet`.

The implementation moved into a new internal package
`pkg/graph/internal/core` (the `Core` type holds all the heavy state, locks,
generators, and method bodies). `*Graph` is now a thin façade that holds a
`*core.Core` and the 13 sub-API field accessors. The package layout reflects
this — `pkg/graph/graph.go` is 118 LOC (from 380), and the entire
implementation lives in `pkg/graph/internal/core/` (≈7.5K LOC across the
27 implementation files plus the 53 test files).

### Documentation

- `CLAUDE.md`, `AGENTS.md`, `docs/architecture.md` — file map updated to list
  the 11 new sub-API package directories under `pkg/graph/` plus the two
  in-package sub-API types (`TxAPI`, `BatchAPI`) in `pkg/graph/subapi.go`.
- Status lines bumped to v3.4.0.

## [3.3.0] - 2026-05-07

### Public API reorganization (Option A — audience-based sub-packages)

This is a major API restructure. External customers (notably `tkgd-v3`) must update import paths.

#### New sub-packages

- `pkg/graph/store` — `Store` interface, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, the `Depth*` and `Distance*` constants, and 12 store-layer sentinel errors (was `pkg/graph.{Store,QueryOpts,...}`).
- `pkg/graph/store/memory` — `memory.Store`, `memory.New()` (was `graph.MemoryStore`, `graph.NewMemoryStore`).
- `pkg/graph/store/badger` — `badger.Store`, `badger.Config`, `badger.New()` (was `graph.BadgerStore`, `graph.BadgerStoreConfig`, `graph.NewBadgerStore`).
- `pkg/graph/store/tiered` — `tiered.Store`, `tiered.Config`, `tiered.New()`, `tiered.MigrateFromBadger()`, `tiered.ShardInfo`, `tiered.VerifyResult`, `tiered.RepairResult`, plus the four TieredStore sentinels (`ErrEventPropertyIndex`, `ErrPrimaryLabelClassMutation`, `ErrNotReferenceEntity`, `ErrCrossShardArchiveRel`).
- `pkg/graph/events` — `Event`, `EventBus`, `AsyncEventBus`, `EventType`, `EventPriority`, `BackpressureStrategy`, `Publisher`, plus constructors and constants.
- `pkg/graph/index` — `IndexProvider`, `Initializable`, `GraphReader`, `LegacyIndexProvider`, plus the three `ErrIndexProvider*` sentinels.
- `pkg/graph/temporal` — `GraphSnapshot`, `SnapshotDiff`, `NodeUpdate`, `RelUpdate`, `TemporalConstraint`, `TemporalConstraintKind`, `ConstraintSet`, `NewConstraintSet`, `ConstraintRelWithinEndpoints`, plus seven temporal-constraint sentinels.
- `pkg/graph/ontology` — `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, `ClassEvent`, `ClassReference`.

#### Breaking changes

- **`LegacyIndexProvider.OnEvent` signature** — was `OnEvent(events.Event, *Graph)`, now `OnEvent(events.Event, index.GraphReader)`. The interface had to move out of `pkg/graph` to break a cycle with `pkg/graph/index`; the read-only contract is now enforced at the type level.
- **All public re-exports for store backends, events, index providers, temporal constraints, and ontology classification are removed from `pkg/graph`**. Customers must import the new sub-packages directly.

#### Migration guide

```go
// Before (v3.2.0)                              // After (v3.3.0)
graph.QueryOpts{...}                            store.QueryOpts{...}
                                                // import .../pkg/graph/store

graph.NewMemoryStore()                          memory.New()
                                                // import .../pkg/graph/store/memory

graph.NewBadgerStore(graph.BadgerStoreConfig{}) badger.New(badger.Config{})
                                                // import .../pkg/graph/store/badger

graph.NewTieredStore(graph.TieredStoreConfig{}) tiered.New(tiered.Config{})
                                                // import .../pkg/graph/store/tiered

graph.MigrateFromBadger(src, dst)               tiered.MigrateFromBadger(src, dst)

graph.NewEventBus()                             events.NewEventBus()
graph.SetEventBus(...)                          g.SetEventBus(events.NewEventBus())
graph.PriorityHigh                              events.PriorityHigh
                                                // import .../pkg/graph/events

graph.IndexProvider                             index.IndexProvider
graph.LegacyIndexProvider                       index.LegacyIndexProvider
graph.GraphReader                               index.GraphReader
                                                // import .../pkg/graph/index

graph.GraphSnapshot                             temporal.GraphSnapshot
graph.SnapshotDiff                              temporal.SnapshotDiff
graph.NewConstraintSet(...)                     temporal.NewConstraintSet(...)
graph.ConstraintRelWithinEndpoints              temporal.ConstraintRelWithinEndpoints
                                                // import .../pkg/graph/temporal

graph.NewOntologyMapping([]string{"Case"})      ontology.NewOntologyMapping([]string{"Case"})
graph.ClassReference                            ontology.ClassReference
                                                // import .../pkg/graph/ontology

errors.Is(err, graph.ErrNodeNotFound)           errors.Is(err, store.ErrNodeNotFound)
errors.Is(err, graph.ErrEventPropertyIndex)     errors.Is(err, tiered.ErrEventPropertyIndex)
errors.Is(err, graph.ErrTemporalConstraint)     errors.Is(err, temporal.ErrTemporalConstraint)
```

#### Internal restructure

- `internal/events` deleted (contents promoted to `pkg/graph/events`).
- `internal/temporal` deleted (contents promoted to `pkg/graph/temporal`).
- `internal/memorystore` deleted (contents promoted to `pkg/graph/store/memory`).
- `internal/badgerstore` deleted (contents promoted to `pkg/graph/store/badger`).
- `internal/tieredstore` deleted (contents promoted to `pkg/graph/store/tiered`).
- `internal/store` renamed to `internal/storeutil` (helpers only — key encoding, msgpack wire types, pagination, temporal-filter push-down). The public `Store` contract now lives in `pkg/graph/store`.

#### File consolidation in pkg/graph

Pre-restructure consolidation merged 11 over-split per-operation files into 4 cohesive files organized by domain:

- `context_node_{add,update,read_delete}.go` (3 files) → `node.go` (~750 LOC)
- `context_relationship_{add,update,read_delete,import}.go` (4 files) → `relationship.go` (~900 LOC)
- `temporal_{snapshot,diff}.go` → folded into `temporal.go` (~660 LOC)
- `graph.go` + `lifecycle.go` + `config.go` → `graph.go` (~330 LOC)

## [3.2.0] - 2026-05-07

### Breaking changes

- **`MigrateFromBadger(src, dst, labels)` → `MigrateFromBadger(src, dst)`**. The `*LabelRegistry` parameter is dropped; the function now loads the registry from the source `*BadgerStore` directly via `LoadLabelRegistry`. Callers no longer have to allocate and populate a registry before invoking the migrator.
- **`graph.ComputeNodeHash` and `graph.ComputeRelHash` are removed from the public API**. They were thin re-exports of the hash primitives in `pkg/graph/internal/integrity`. Use `Graph.VerifyNodeHashChain` / `Graph.VerifyRelHashChain` for chain verification; the primitives themselves remain available inside `pkg/graph/internal/integrity` for internal use.
- **`graph.LabelRegistry` and `graph.RelTypeRegistry` are removed from the public API**. The registries live in `pkg/graph/internal/registry/` and are managed entirely through the `*Graph` (creating, resolving, persisting through `Close`/`MigrateFromBadger`). No public function references them after the `MigrateFromBadger` signature change above.
- **TieredStore catalog types removed from the public API**: `graph.ShardCatalog`, `graph.ShardEntry`, `graph.ShardKind`, `graph.ShardTier`, `graph.NewShardCatalog`, `graph.EventShard`, plus the constants `graph.ShardReference`, `graph.ShardEvent`, `graph.TierHot`, `graph.TierWarm`, `graph.TierCold`. They had been re-exported from `internal/tieredstore` "for tests" but were never customer-facing — `tkgd-v3` does not reference them. Tests that used them now import `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/tieredstore` directly.
- **`graph.RelDeleteInfo` removed from the public API**. The struct is a `BadgerStore` cascade-delete payload; tests that need it import `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/badgerstore` directly.

### API consolidation (no behaviour change)

- `pkg/graph/aliases.go` (382 lines, 50 declarations) is split into themed files at the `pkg/graph/` top level. The public API surface for the kept symbols is unchanged — every alias still resolves to the same canonical `internal/*` declaration as before. The new layout is:
  - `pkg/graph/store.go` — `Store`, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric` aliases plus the 12 store-layer sentinel errors (`ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrVersionNotFound`, `ErrNoVersionValidAt`, `ErrIndexExists`, `ErrIndexNotFound`, `ErrTemporalIndexExists`, `ErrTemporalIndexNotFound`, `ErrTxDone`, `ErrStoreClosed`).
  - `pkg/graph/events.go` — `Event`, `EventType`, `EventPriority`, `EventHandler`, `EventBus`, `AsyncEventBus`, `AsyncEventBusConfig`, `BackpressureStrategy`, plus the six `EventNode*`/`EventRel*` constants, the five `Priority*` constants, the three `Backpressure*` constants, and the `NewEventBus` / `NewAsyncEventBus` constructors.
  - `pkg/graph/ontology.go` — `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, plus the `ClassEvent` / `ClassReference` constants.
  - `pkg/graph/snowflake.go` — `IDComponents`, `DecomposeID`, plus the package-level `snowflakeEpoch` / `snowflakeLayout` helpers used internally by `lifecycle.go` and ID-decomposition code.
  - `pkg/graph/errors.go` — vector-index sentinel errors (`ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`) and the registry sentinels (`ErrEmptyName`, `ErrRegistryNotEmpty`).
  - `pkg/graph/backends.go` — concrete `Store` impls: `MemoryStore` + `NewMemoryStore`, `BadgerStore` + `BadgerStoreConfig` + `NewBadgerStore`, `TieredStore` + `TieredStoreConfig` + `NewTieredStore`, the admin return types `ShardInfo` / `VerifyResult` / `RepairResult`, the four TieredStore-specific sentinels (`ErrEventPropertyIndex`, `ErrPrimaryLabelClassMutation`, `ErrNotReferenceEntity`, `ErrCrossShardArchiveRel`), and the new-signature `MigrateFromBadger(src, dst)`.
  - `pkg/graph/temporal_constraint.go` — temporal-constraint aliases (`TemporalConstraintKind`, `TemporalConstraint`, `ConstraintSet`, `ConstraintRelWithinEndpoints`, `NewConstraintSet`, the seven sentinel errors) sit alongside the Graph-coupled enforcement methods (`checkTemporalConstraints` and helpers).
- `pkg/graph/pagination.go` is deleted. The lowercase `paginateNodes` / `paginateRels` / `sortNodesByID` / `sortRelsByID` wrappers were one-line forwarders; the three call sites (`queries.go`, `graph_property_query.go`, `temporal_queries.go`) now invoke `internal/store.PaginateNodes` / `PaginateRels` / `SortNodesByID` / `SortRelsByID` directly. The other wrappers (`paginateIDs`, `paginateNodeIDs`, `paginateRelIDs`, `toNodeIDs`, `toRelIDs`) had no remaining callers and are gone.

### Internal

- `Store`, `QueryOpts`, `RelTombstone`, the 12 store sentinels, and the in-memory index types (`EntityClass`, `OntologyMapping`, vector-index errors, registry errors) keep their canonical declarations inside `pkg/graph/internal/{store,index}` due to the import-cycle constraint with the helpers (`PaginateIDs`, `EntityValidFrom`, `MatchesTemporalFilter`) that depend on them. The public type aliases at `pkg/graph/*.go` preserve the customer-facing names.

### Internal cleanup (Phase 7a follow-up)

- Renamed local `integrity` variables in `pkg/graph/batch.go` to `nodeIntegrity`/`relIntegrity` so the `internal/integrity` package can be imported under its bare name. The `integritypkg` import alias is gone.
- All `pkg/graph/*_test.go` files unified to `package graph` (no more `package graph_test`). Eliminates the !45 caveat where tests landing in `findings_regression_test.go` should have been in `remove_label_test.go` but couldn't due to package boundary.

## [3.1.23] - 2026-05-07

### Added

- `Graph.GetRelationshipsByTypeValidAt(relType, at)` — point-in-time relationship convenience query (parity with the Node side's `GetNodesByLabelValidAt`).
- `Graph.RelationshipsByTypePropertyAndTime(relType, key, value, at)` — point-in-time predicate query for relationships.
- `Graph.RelationshipsByTypePropertyDuring(relType, key, value, start, end)` — interval predicate query with predicate-during-interval semantics (matches any version whose property held during any portion of the requested interval, not just the most-recent overlap).
- `BadgerStore.SaveRegistries(labelReg, relTypeReg)` — atomic single-transaction registry persistence (parity with `TieredStore.SaveRegistries`); the previous `SaveLabelRegistry` + `SaveRelTypeRegistry` pair persisted across two separate transactions and could leave the on-disk state inconsistent on crash.
- IndexProvider redesign (Phase 6): the `IndexProvider` interface no longer takes a `*Graph` parameter and now returns `error`; new optional `Initializable` interface lets providers participate in bulk-load on startup; new narrow `GraphReader` interface gives providers a stable read surface without coupling to the full `*Graph`; AsyncEventBus subscription is now supported. Direct test coverage added in `index_provider_test.go`.

### Changed (backward-compatible)

- IndexProvider migration: the pre-Phase-6 interface shape is preserved as `LegacyIndexProvider`. Existing customers must change their registration call from `RegisterIndexProvider` to `RegisterLegacyIndexProvider`. The provider implementation itself is identical — only the registration call site changes.

### Changed (structural, no behaviour change)

- **Restructure phase 6 — `internal/snowflake` extracted from `internal/store`** (`pkg/graph/internal/store/id_decompose.go` → `pkg/graph/internal/snowflake/id_decompose.go`, package `snowflake`). The shared `SnowflakeEpoch`, `SnowflakeLayout`, `IDComponents`, and `DecomposeID` symbols moved to a new dedicated package so locks, tieredstore, and badgerstore stop depending on `internal/store` purely for layout decoding. Public surface unchanged — `pkg/graph/aliases.go` still re-exports `DecomposeID`, `IDComponents`, `snowflakeEpoch`, `snowflakeLayout`. The `internal/store` symbols `SnowflakeLayout` and `SnowflakeEpoch` are gone (no caller outside the same package referenced them after the move).
- **`internal/registry` extracted from `internal/index`** (`label_registry.go`, `label_registry_test.go`, `reltype_registry.go`, `reltype_registry_test.go` → `pkg/graph/internal/registry/`, package `registry`). `internal/index/aliases.go` re-exports `LabelRegistry`, `RelTypeRegistry`, `NewLabelRegistry`, `NewRelTypeRegistry`, `ErrEmptyName`, `ErrRegistryNotEmpty`, and `TokenCapacityMax` so existing callers (graph layer, badgerstore, tieredstore, tests) keep compiling unchanged.
- **`internal/temporal` package**: pure constraint types (`TemporalConstraintKind`, `TemporalConstraint`, `ConstraintSet`, the seven sentinel errors, `NewConstraintSet`) extracted from `pkg/graph/temporal_constraint.go` into a dedicated subpackage; the Graph-coupled enforcement method `checkTemporalConstraints` stays in `pkg/graph/temporal_constraint.go`. Direct unit tests added — `internal/temporal/` is now at 100% direct coverage (was 0% direct, only exercised through Graph integration tests).
- **`pkg/graph/graph.go` split into 14 files** (graph.go 1880L → 48L). Concerns split into `config.go`, `lifecycle.go`, `validation.go`, `resolution.go`, `crud.go`, `property_cas.go`, `queries.go`, `graph_indexes.go`, `vector_search.go`, `graph_property_query.go`, `admin.go`, `events_dispatch.go`, `node_label.go`, `version_chain.go`. Function bodies are byte-identical.
- **`pkg/graph/context.go` split per entity**: `context.go` (helpers) + `context_node_add.go`, `context_node_update.go`, `context_node_read_delete.go`, `context_relationship_add.go`, `context_relationship_read_delete.go`, `context_relationship_update.go`, `context_relationship_import.go`. Function bodies are byte-identical.
- **`pkg/graph/temporal.go` split by feature**: `temporal.go` (internal helpers, 446L) + `temporal_queries.go`, `temporal_snapshot.go`, `temporal_diff.go`. Function bodies are byte-identical.
- **Test relocation across the restructure**: 11 test-relocation MRs and the Phase-6 follow-ups moved tests next to their backend code in `internal/{memorystore, badgerstore, tieredstore, events, store, index, integrity}`. Some Graph-integration tests were intentionally kept in `pkg/graph/` where they thread `New(Config{})` through their scenarios — splitting along that seam would have required pulling Graph machinery into the internal test fixtures (B42 moves-only contract). `tieredstore_test.go` and `tieredstore_history_routing_test.go` remain in `pkg/graph/` for the same reason; deferred to a follow-up MR per the project's "if unsure, defer" convention.
- `pkg/graph/lifecycle.go` uses a unified `registriesPersister` interface for both `BadgerStore` and `TieredStore` — neither backend's atomic registry-persistence call is referenced by name, so adding a third backend in the future requires only implementing `SaveRegistries(labelReg, relTypeReg)`.

### Test coverage

- `internal/temporal/` — 100% direct coverage (was 0% direct).
- `internal/integrity/` — 100% direct coverage with five SHA-256 fixed-vector anchors locking the on-disk hash format (was 32.9% direct, only exercised through Graph integration tests).

### Documentation

- `tasks/lessons.md` — B42 deduped (renamed to B44), B22-B29 reordered ascending, B31-B35 reordered ascending. Block bodies are byte-identical; only their position changed.
- File-map sync in `CLAUDE.md` and `docs/architecture.md` reflects the post-restructure `pkg/graph/` layout (graph.go split + internal-package extractions).
- This MR adds the Phase-6 entry above to the long-running restructure narrative; previous restructure phases are described in their own version sections (3.1.17 — phase 1, 3.1.18 — phase 2, 3.1.19 — phase 3, 3.1.21 — phase 4, 3.1.22 — phase 5).

## [3.1.22] - 2026-05-07

### Changed (structural, no behaviour change)

- **Restructure phase 5 — split `pkg/graph/integrity.go` (helpers → `pkg/graph/internal/integrity`)** (`pkg/graph/integrity.go` → `pkg/graph/integrity.go` + `pkg/graph/internal/integrity/integrity.go`, package `integrity`). The two `*Graph` methods (`VerifyNodeHashChain`, `VerifyRelHashChain`) stay in `pkg/graph/integrity.go` because they have a `*Graph` receiver and call `g.store.GetNode` / `g.NodeLabels` / `g.RelationshipType`; the four pure helpers (`ComputeNodeHash`, `ComputeRelHash`, `appendProperties`, `appendPropertyValue`) plus `hashBufPool` move into the new `internal/integrity` subpackage. Cross-boundary exports: `ComputeNodeHash` and `ComputeRelHash` were already exported and stay so under the new `integrity.` qualifier; `hashBufPool`, `appendProperties`, and `appendPropertyValue` stay unexported because the only callers are now same-package. Function bodies are byte-identical to the pre-split implementation — hash output for the same input is unchanged (verified by the existing `integrity_test.go` table of `TestComputeNodeHash*` / `TestComputeRelHash*` tests, all green). **Judgement call**: `integrity_test.go` stayed in `pkg/graph/` rather than splitting along the helper / Graph-method seam because the file mixes pure-helper unit tests (`TestComputeNodeHashDeterministic`, `TestComputeRelHashChangesWithType`, etc.) with `*Graph` integration tests (`TestVerifyNodeHashChain_GenesisOnly`, `TestVerifyRelHashChain_*`) that thread `New(Config{})` through their scenarios. Splitting the file would have required either pulling Graph machinery into `internal/integrity` test fixtures or fragmenting the test helpers, both violating the moves-only contract from B42 — same precedent applied in Phase 4 to `events_test.go`. The pure-helper tests remain green by relying on the `var ComputeNodeHash = integrity.ComputeNodeHash` / `var ComputeRelHash = integrity.ComputeRelHash` re-exports added to `pkg/graph/aliases.go`. The same alias keeps the ~19 internal `pkg/graph/` call sites in `batch.go` (2), `graph.go` (5), `context.go` (12) compiling unchanged — no qualified-name updates were needed at the call sites, keeping the diff to a verifiable copy-paste split. The `pkg/graph/integrity.go` retained file imports `pkg/graph/internal/integrity` directly and uses the qualified `integrity.ComputeNodeHash` / `integrity.ComputeRelHash` calls inside its two methods.
- **Dependency arrows** after the move: `pkg/graph` → `internal/integrity`, `internal/events`, `internal/store`, `internal/locks`, `internal/index`, `internal/memorystore`, `internal/badgerstore`, `internal/tieredstore`; `internal/integrity` → `internal/store`, `pkg/types`. No `internal/*` package imports `pkg/graph` (verified by grep). `go build ./...`, `go vet ./...`, `gofmt -l .` empty, and `go test -race -short -count=1 -timeout 600s ./...` all green.

## [3.1.21] - 2026-05-07

### Changed (structural, no behaviour change)

- **Restructure phase 4 — extract `pkg/graph/internal/events`** (`pkg/graph/events.go` → `pkg/graph/internal/events/events.go`, package `events`). Cross-boundary exports: the package-private `eventPublisher` interface became `events.Publisher` because it is the type of `Graph.events`, which is set by the public `SetEventBus`/`SetAsyncEventBus` methods and consumed by the publish path that crosses the boundary; the corresponding interface method `publish(Event)` was renamed to `Publish(Event)` so both `*EventBus` and `*AsyncEventBus` satisfy it from the new package. All other identifiers in `events.go` were already exported (`EventType` + the six `EventNode*`/`EventRel*` constants, `EventPriority` + the five `Priority*` constants, `Event`, `EventHandler`, `EventBus`, `NewEventBus`, `AsyncEventBus`, `AsyncEventBusConfig`, `NewAsyncEventBus`, `BackpressureStrategy` + the three `Backpressure*` constants); package-private helpers used cross-file inside `events.go` only (`safeInvoke`, `numPriorityLevels`, `priorityOrder`, the `dispatch`/`drainAll`/`worker` methods on `*AsyncEventBus`) stayed lowercase. **Judgement call**: `events_test.go` and `async_eventbus_test.go` stayed in `pkg/graph/` rather than moving with the source, mirroring Phase 3's treatment of the BadgerStore/TieredStore integration tests — both files mix pure-bus tests (`TestEventBus_Subscribe_Unsubscribe`, `TestEventBus_PanicHandler`, `TestAsyncEventBus_BackpressureBlock`, etc.) with `*Graph` integration tests (`TestGraph_NodeCreate_Event`, `TestPriority_GraphCreateIsHigh`, `TestSetAsyncEventBus_GraphIntegration`, etc.) that thread `New(Config{})` and `g.AddNode`/`g.AddRelationship` through their scenarios. Splitting the files would have required either pulling Graph machinery into the `internal/events` test files or fragmenting the test helpers, both violating the moves-only contract from B42. The remaining call sites inside `pkg/graph/` (the `Graph.events` field declaration in `graph.go`, `publishEvent` and `dispatchEvent` in `graph.go`, the `ep.publish(e)` lines in `tx.go` Commit and `batch.go` Run, plus one in-test `bus.publish` in `v3056_fixes_test.go`) were updated to call `Publish` on the new `events.Publisher` interface; the doc-comment reference to `eventPublisher` in `index_provider.go` was updated to `events.Publisher`. `pkg/graph/aliases.go` re-exports `Event`, `EventType` (+ constants), `EventPriority` (+ constants), `EventHandler`, `EventBus`, `AsyncEventBus`, `AsyncEventBusConfig`, `BackpressureStrategy` (+ constants), and the `NewEventBus`/`NewAsyncEventBus` constructors so the public API surface used by `tkgd-v3` (`graph.EventBus`, `graph.NewEventBus`, `graph.SetEventBus`, the `EventNode*` constants, etc.) is unchanged.
- **Dependency arrows** after the move: `pkg/graph` → `internal/events`, `internal/store`, `internal/locks`, `internal/index`, `internal/memorystore`, `internal/badgerstore`, `internal/tieredstore`; `internal/events` → `pkg/types` only. No `internal/*` package imports `pkg/graph` (verified by grep). `go build ./...`, `go vet ./...`, `gofmt -l .` empty, and `go test -race -short -count=1 -timeout 600s ./...` all green.

## [3.1.20] - 2026-05-07

### Fixed

- **Vector index stale after `UpdateNode`** (`pkg/graph/internal/memorystore`, `pkg/graph/internal/badgerstore`, `pkg/graph/internal/tieredstore`): `ReplaceNodeWithHistory` — the path called by every `UpdateNode` — did not update in-memory vector indexes. The old vector entry was never removed; the new one was never inserted. After any node update, `SearchNearestNodes` returned pre-update distances for the modified node. Fixed in all three store backends.
- **Batch operations missing temporal and vector index maintenance** (same packages): `PutNodesBatch` and `DeleteNodesBatch` did not maintain temporal or vector indexes. Batch-inserted nodes were invisible to temporal queries and vector searches; batch-deleted nodes remained as phantom candidates. Fixed in `MemoryStore`, `BadgerStore`, and `TieredStore` (six locations total). `TieredStore.ReplaceNodeWithHistory` also fixed — it delegated to a shard but did not update the TieredStore-level vector index map.
- **Dead code `shardForRelID` (unchecked variant) removed** (`pkg/graph/internal/tieredstore`): the function was never called — all callers use `shardForRelIDChecked`. It contained a checkout-without-pin bug in its cold-shard probe loop. Deleted.

## [3.1.19] - 2026-05-07

### Changed (structural, no behaviour change)

- **Restructure phase 3 — extract `pkg/graph/internal/memorystore`, `pkg/graph/internal/badgerstore`, and `pkg/graph/internal/tieredstore`**: three sequential moves landed as their own commits, each leaving the tree green.
  - **Step A — `internal/memorystore`** (`pkg/graph/memorystore.go` → `pkg/graph/internal/memorystore/memorystore.go`). Cross-boundary exports: `searchNearestFiltered` → `SearchNearestFiltered` on all three Store impls (renamed in this commit on `BadgerStore` and `TieredStore` too because Go interface satisfaction matches by method name; the package-internal `filteredVectorSearchStore` interface in `graph.go` now declares `SearchNearestFiltered`). Pagination/sort helpers (`paginateIDs`, `paginateNodes`, `paginateRels`, `paginateNodeIDs`, `paginateRelIDs`, `toNodeIDs`, `toRelIDs`, `sortNodesByID`, `sortRelsByID`) moved into `pkg/graph/internal/store/pagination.go` with capitalised names; `pkg/graph/aliases.go` retains lowercase wrappers so existing call sites and tests are untouched. New `MemoryStore` test-export helpers (`GetNodeHistoryEntry`, `SetNodeHistoryEntryForTest`, `SetNodeForTest`) added so the one tampering test that previously poked unexported maps (`findings_regression_test.go`) keeps working through a narrow surface.
  - **Step B — `internal/badgerstore`** (`pkg/graph/badgerstore.go` and `pkg/graph/badgerstore_partial.go` → `pkg/graph/internal/badgerstore/`). Tests that exercise BadgerStore in isolation (`badgerstore_test.go`, `badgerstore_partial_test.go`, `badgerstore_temporal_test.go`) move with the source; integration tests using BadgerStore as one component of a Graph stay in `pkg/graph/`. Cross-boundary exports for the partial-write helpers TieredStore reaches into: `hasNodeID/HasRelID/IncomingRelIDs/OutgoingRelIDs/PutRelEntityAndOut/PutRelIncoming/DeleteRelIncoming/DeleteRelEntityAndOut/DeleteIncomingByRelID/ScanAndDeleteIncoming` and the `RelDeleteInfo` struct (with `ID`, `RelType`, `StartID`, `EndID` fields exported). Default constants exported as `DefaultCacheCapacity`/`DefaultFlushInterval`/`DefaultGCInterval`/`DefaultGCDiscardRatio`. Test-only exports added on `BadgerStore`: `SetDBClosedForTest`, `SetNodeCountForTest`, `LockFlushMuForTest`/`UnlockFlushMuForTest`, `DBForTest`, `NodeCacheForTest`/`RelCacheForTest`, `LabelIndexForTest`, `HasNodeIDForTest`/`HasRelIDForTest`, `HasTemporalIndexForTest`/`HasHFIndexForTest`, `SyncWritesForTest`/`FlushIntervalForTest`, `LockIdxMuRForTest`/`UnlockIdxMuRForTest`, `ReadOnlyForTest`, `FlushDoneForTest`/`GCDoneForTest`.
  - **Step C — `internal/tieredstore`** (8 source files: `tieredstore.go`, `tieredstore_write.go`, `tieredstore_read.go`, `tieredstore_admin.go`, `tieredstore_repair.go`, `tieredstore_migrate.go`, `shard_catalog.go`, `registry_file.go` → `pkg/graph/internal/tieredstore/`). Standalone tests `shard_catalog_test.go` and `registry_file_test.go` move with the source; the integration tests `tieredstore_test.go` and `tieredstore_history_routing_test.go` stay in `pkg/graph/` because they thread `*Graph` from `newTestTieredGraph` through their scenarios — splitting them would have required pulling >1000 LOC of helpers across the boundary. Cross-boundary exports: `eventShard` → `EventShard`, `registryFileData` → `RegistryFileData`. **Dependency-inversion for `VerifyShard`**: the signature changed from `VerifyShard(g *Graph, ...)` to `VerifyShard(g HashChainVerifier, ...)`, where the new `HashChainVerifier` interface declares `VerifyNodeHashChain` and `VerifyRelHashChain`; `*Graph` satisfies it implicitly. This is the only API-shape change in Phase 3 and it preserves backward compatibility — every existing call site passes a `*Graph` value. A comprehensive `*ForTest` accessor surface was added on `TieredStore` and `EventShard` so the integration tests can keep poking at unexported state without exporting the underlying fields. The `pkg/graph/aliases.go` extension covers `TieredStore`, `TieredStoreConfig`, `ShardInfo`, `VerifyResult`, `RepairResult`, `ShardEntry`, `ShardCatalog`, `ShardKind`, `ShardTier`, `EventShard`, `NewTieredStore`, `NewShardCatalog`, `MigrateFromBadger`, the `ShardReference`/`ShardEvent`/`TierHot`/`TierWarm`/`TierCold` constants, and the four TieredStore-specific sentinels (`ErrEventPropertyIndex`, `ErrPrimaryLabelClassMutation`, `ErrNotReferenceEntity`, `ErrCrossShardArchiveRel`).
  - **Dependency arrows** after the moves: `pkg/graph` → `internal/tieredstore` → `internal/badgerstore` → {`internal/store`, `internal/index`, `internal/locks`}; `internal/tieredstore` also depends on `internal/store` and `internal/index` directly; `pkg/graph` also depends on `internal/memorystore`, `internal/badgerstore`, `internal/store`, `internal/index`, and `internal/locks`. No `internal/*` package imports `pkg/graph` (verified by grep). `go build ./...`, `go vet ./...`, `gofmt -l .` empty, and `go test -race -short -count=1 -timeout 600s ./...` all green.
- **Phase 4 deferred**: there is no Phase 4 in the original restructure plan — Phase 3 completes the structural extraction. Future work (identifier cleanup, file splits within each subpackage) was always slated for follow-up MRs per B42.

## [3.1.18] - 2026-05-06

### Changed (structural, no behaviour change)

- **Restructure phase 2 — extract `pkg/graph/internal/locks` + `pkg/graph/internal/index`, plus relocate the temporal-filter helpers into `internal/store`**: three sequential moves landed as their own commits, each leaving the tree green.
  - **Step A — temporal-filter helpers into `internal/store`** (`pkg/graph/temporal_filter.go` → `pkg/graph/internal/store/temporal_filter.go`). `entityValidFrom` → `EntityValidFrom`, `matchesTemporalFilter` → `MatchesTemporalFilter`. `matchesPointInTime`/`matchesInterval` stayed unexported (only same-file callers). The move pre-empts a cycle that would have appeared once `internal/index` referenced `EntityValidFrom`/`MatchesTemporalFilter`, since Phase 3 will move backends out of `pkg/graph` too.
  - **Step B — entity locks into `internal/locks`** (`pkg/graph/entity_locks.go` → `pkg/graph/internal/locks/entity_locks.go`, package `locks`). `entityLockManager` → `Manager`, `newEntityLockManager` → `NewManager`. Public methods (`LockEntity`, `LockTwo`, `LockMany`) and constants stayed exported. Callers in `pkg/graph/` qualify as `locks.Manager` / `locks.NewManager()`.
  - **Step C — index types into `internal/index`** (`pkg/graph/lru.go`, `label_registry.go`, `reltype_registry.go`, `ontology.go`, `property_index.go`, `temporal_index.go`, `hf_index.go`, `vector_index.go` → `pkg/graph/internal/index/`, package `index`). Cross-package symbols exported: `entityLRU` → `Cache`, `lruEntry` → `Entry`, the cache-status constants → `CacheHit`/`CacheMiss`/`CacheDeleted`; `labelRegistry` → `LabelRegistry`, `relTypeRegistry` → `RelTypeRegistry`, both with their `New*` constructors and `Err*` sentinels; `propertyIndex` → `PropertyIndex`, `temporalIndex` → `TemporalIndex`, `highFrequencyIndex` → `HighFrequencyIndex`, `vectorIndex` → `VectorIndex` (plus their constructors and `Err*` sentinels). `OntologyMapping` and `EntityClass` were already exported; their internal `labelReg *labelRegistry` field tracked the registry rename to `*LabelRegistry`. `internal/index/vector_index.go` references `DistanceMetric` from `internal/store` (Phase 1) qualified as `storepkg.DistanceMetric`. **Judgement call**: `index_provider.go` stays in `pkg/graph` — the `IndexProvider` interface depends tightly on `Graph`, `Event`, `EventBus`, `eventPublisher`, and the `g.indexProviders`/`g.events` fields, and moving it would have required pulling in chunks of `events.go` and `graph.go` that violate the moves-only contract.
  - **`pkg/graph/aliases.go`** extended with `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, the registry+vector-index sentinels (`ErrEmptyName`, `ErrRegistryNotEmpty`, `ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`) so the public API surface stays unchanged.
  - **Dependency arrows** after the move: `pkg/graph` → `internal/store`, `internal/locks`, `internal/index`; `internal/index` → `internal/store`, `pkg/types`; `internal/locks` → `pkg/types`; `internal/store` → `pkg/types`. No `internal/*` package imports `pkg/graph` (verified by grep). `go build ./...`, `go vet ./...`, `gofmt -l .` empty, and `go test -race -short -count=1 -timeout 600s ./...` all green.
- **Phase 3-4 deferred**: extracting `memorystore.go`, `badgerstore.go`, and `tieredstore*.go` into `internal/memorystore`/`internal/badgerstore`/`internal/tieredstore` is left for follow-up MRs.

## [3.1.17] - 2026-05-06

### Changed (structural, no behaviour change)

- **Restructure phase 1 — extract `pkg/graph/internal/store`**: the persistence-contract types (`Store` interface, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, the `Err*` sentinels), the binary key encoding (`keys.go`), the msgpack wire format (`wire.go`), the `id_decompose.go` helpers, and the package-level `snowflakeEpoch`/`snowflakeLayout` are now in `pkg/graph/internal/store`. Identifiers that cross the new package boundary were exported (`nodeKey` → `NodeKey`, `nodeWire` → `NodeWire`, `propertyTypeTag` → `PropertyTypeTag`, etc.); identifiers that stay package-private inside `pkg/graph/internal/store` (e.g. `propertiesToWire`, `toInt64`) keep their original lowercase names. `pkg/graph/aliases.go` re-exports `Store`, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, the depth and metric constants, the sentinel errors, `IDComponents`, and `DecomposeID` so the public API surface (`graph.Store`, `graph.ErrNodeNotFound`, etc.) is unchanged. `(QueryOpts).hasTemporalFilter` became a free function `hasTemporalFilter(opts)` because Go forbids methods on a non-local aliased type. No semantic changes; `go test -race -short ./...` is green.
- **Phase 2-4 deferred**: the `internal/index`, `internal/locks`, `internal/memorystore`, `internal/badgerstore`, and `internal/tieredstore` extractions described in the restructure plan remain in `pkg/graph` for now and will land in subsequent MRs to keep individual diffs reviewable.

## [3.1.16] - 2026-05-06

### Fixed

- **`BadgerStore.Clear` flush race** (`pkg/graph/badgerstore.go`): a flush goroutine that snapshotted pending writes under `idxMu.RLock` but had not yet submitted its `WriteBatch` could race ahead of `DropAll()` and resurrect pre-Clear entities after a restart. `Clear` now acquires `flushMu` first (same ordering as `flush()`), serialising the two paths end-to-end.
- **`BadgerStore.Clear` `sync.Map` field-replacement race** (`pkg/graph/badgerstore.go`): `bs.labelCounts = sync.Map{}` replaced the struct field while concurrent `NodeCountByLabel` calls read from it without holding `idxMu` — a data race on the field itself. Fixed with `Range+Delete` (concurrency-safe by contract).
- **Missing secondary-index resets in `Clear`** (all three stores): `BadgerStore`, `MemoryStore`, and `TieredStore.Clear` left `temporalIndexes`, `hfIndexes`, and `vectorIndexes` populated. Subsequent `CreateTemporalIndex`/`CreateHighFrequencyIndex`/`CreateVectorIndex` returned "already exists" on a logically empty store; stale vector entries occupied top-k slots in `SearchNearestNodes`. `TieredStore.Clear` also left `tempIdxLabels` set, causing the next rotation to re-install temporal indexes for already-dropped labels.

## [3.1.15] - 2026-05-06

### Fixed

- **`SearchNearestNodes` k ≤ 0 panic** (`pkg/graph/vector_index.go`, `pkg/graph/graph.go`): negative or zero k could reach `make(knnHeap, 0, k)` and panic. Both the Graph layer and `vectorIndex.searchNearest` now return `nil, nil` for k ≤ 0.
- **`SearchNearestNodes` ignores `QueryOpts`** (`pkg/graph/graph.go`, `pkg/graph/tieredstore_write.go`): `ValidAt`/`ValidStart`/`ValidEnd` temporal filters, `After`/`Limit` cursor pagination, and `Depth` gating were silently dropped. Temporal filtering now applies an eligibility predicate **before** the k-cut via `filteredVectorSearchStore`; distance-order cursor pagination applies after the k-cut; `Depth != DepthAll` + temporal filter returns `ErrDepthTemporalUnsupported`; TieredStore `depthFilter` excludes archive-resident nodes from `DepthHot`/`DepthWarm` before heap selection.

## [3.1.14] - 2026-05-06

### Fixed

- **`ImportGraph` panic safety** (`pkg/graph/export.go`): `wireToNode`/`wireToRel` panic on token 0 (reserved). `ImportGraph` reads from an untrusted `io.Reader`; a corrupt or malicious export becomes a process crash. New `validateNodeWire`/`validateRelWire` validate all four record types (node, nodeHist, rel, relHist) before constructing, returning the new `ErrCorruptExport` sentinel on token-0 or out-of-uint16-range values.
- **`RunRepair` silent operational error swallow** (`pkg/graph/tieredstore_repair.go`): Phase 2 conflated `ErrRelNotFound` (legitimate TOCTOU skip) with I/O failures, routing failures, and closed-shard errors — returning "Repair succeeded" while needed `in/` repairs were missed. Now `errors.Is(err, ErrRelNotFound)` continues; all other errors propagate.

## [3.1.13] - 2026-05-06

### Added

- **`DeepCopier` interface** (`pkg/types/property_registry.go`): custom property struct types registered via `RegisterPropertyStructType` must now implement `DeepCopyValue() any`. Enforced at registration; the interface is dispatched in `deepCopyValue` before the generic type switch so registered types with nested mutable state (slices, maps, pointers) get a proper deep copy at the store boundary instead of a shallow struct copy.

### Changed

- **`RegisterPropertyStructType` now returns `error`** (`pkg/types/property_registry.go`): registration validates that the type implements both `HashableValue` (new: `ErrTypeNotHashable`) and `DeepCopier` (new: `ErrTypeNotDeepCopyable`). The check uses the form actually passed — registering a value form when methods are on the pointer receiver only is rejected, preventing a non-addressable runtime type-assert failure in the hash and deep-copy paths.

## [3.1.12] - 2026-05-06

### Fixed

- **Admin-path event-shard pinning** (`tieredstore_admin.go`, `tieredstore.go`, `tieredstore_write.go`): five latent Close-race surfaces closed — `ListShards`, `RebuildCatalog`, and `Clear` called `es.store.NodeCount/RelCount/Clear()` without a `checkoutStore` pin; a concurrent `Close` could free the BadgerStore mid-call. `CreateTemporalIndex`, `DropTemporalIndex`, `CreateHighFrequencyIndex`, `DropHighFrequencyIndex` used `allActiveShards` (unpinned pointers); all four now use `allShardStoresWithLazyOpen`. `findRelInAnyShardStore` changed to accept the caller's pre-pinned snapshot rather than re-resolving via a fresh `checkoutArchive`, closing the window where `Close` nil'd `refArchive` between resolution and scan.
- **`ArchiveNode` / `RestoreNode` graph-level mutex** (`graph.go`): both now acquire `g.mu.Lock()` — same exclusion class as a transaction — preventing a concurrent `AddRelationship` from sneaking past the adjacency pre-scan between archive and cascade.
- **Cross-shard archive guard in `PutRelationship`** (`tieredstore_write.go`): this release returned `ErrCrossShardArchiveRel` when one endpoint was on `refArchive` and the other was not. The Unreleased archive-placement migration supersedes this guard.

## [3.1.11] - 2026-05-06

### Fixed (refArchive parity follow-up — MR !6 + audit)

MR !6 (Markus Nissl) closes refArchive parity gaps left over after MR !4. Pre-MR, an archived reference entity stayed `GetNode`-addressable but silently disappeared from indexed/bulk reads, while a concurrent `Close` could free the archive while a query was still using it.

- **Indexed and bulk reads now see archived entities** (`pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore.go`):
  - `NodesByLabel` (reference label): refShard ∪ refArchive (was refShard only).
  - `NodesByLabelAndProperty` (reference label): refShard ∪ refArchive.
  - `NodeCountByLabel` (reference label): refShard + refArchive.
  - `RelationshipsByType`: refShard ∪ refArchive ∪ events.
  - `RelCountByType`: refShard + refArchive + events.
  - `AllNodes` / `AllRelationships`: refShard ∪ refArchive ∪ events at `DepthAll`.
  - `AllNodeIDs` / `AllRelIDs`: refShard ∪ refArchive ∪ events at `DepthAll`.
  - Archive merge is gated on `opts.Depth == DepthAll`. `DepthHot` and `DepthWarm` exclude archive — caller explicitly asked to exclude colder tiers. Mirrors event-shard depth handling.
- **Point-lookup ID routing now pins the archive** (`pkg/graph/tieredstore.go`): `shardForNodeIDChecked` and `shardForRelIDChecked` previously resolved archived IDs via raw `refArchive.Load()` and returned a no-op checkin. A concurrent `Close` could free the archive while a public `GetNode`/`UpdateNode`/`DeleteNode` was still holding it, because `archiveActiveReqs` was never incremented. Both routers now go through `checkoutArchive`, mirroring the `activeReqs` discipline already used for event shards.
- **`forEachHistoryShard` now pins the archive** (`pkg/graph/tieredstore.go`): same Close-race risk on the history fan-out path. Switched to `checkoutArchive` with a checkin scoped around the callback.
- **`ArchiveNode` rejected cross-shard relationships** (`pkg/graph/tieredstore_write.go`, new `ErrCrossShardArchiveRel`): this release failed loud rather than silently fragmenting relationship placement when an archived node still had live partners. The Unreleased archive-placement migration supersedes this rejection by moving the entity/out and incoming legs to the correct shards.
- **Admin & repair paths**: `ListShards`, `RebuildCatalog`, `resolveShardStore`, `allShardStoresWithLazyOpen` all use `checkoutArchive`. `Clear` skips lazy-open when no archive exists in the catalog. `CreateTemporalIndex` and `CreateHighFrequencyIndex` now also propagate the index to refArchive (otherwise archived entities are absent from the temporal index).

### Fixed (audit-found refArchive sites missed by MR !6)

Post-merge audit caught two remaining sites with the same Close-race pattern that MR !6 fixed elsewhere:

- **`findRelInAnyShardStore`** (`pkg/graph/tieredstore_admin.go`): probe used raw `ts.refArchive.Load()` then `archive.hasRelID(relID)` without `checkoutArchive`. Used by `RunRepair` Phase 1 — the function's own doc comment notes that missing the archive probe causes silent data loss, but the implementation didn't pin the probe. Fixed: archive probe now runs under `checkoutArchive`; the returned pointer is used only for identity comparison.
- **`ArchiveNode`** and **`RestoreNode`** (`pkg/graph/tieredstore_write.go`): both called `ensureRefArchive()` then `ts.refArchive.Load()` and dereferenced the result for `PutNode`/`PutRelationship`/`DeleteNodeCascade` calls. Concurrent `Close` racing between the Load and the writes could free the archive `BadgerStore` under the operation. Fixed: both paths now call `checkoutArchive()` after `ensureRefArchive()` and `defer archiveCheckin()` for the duration of the cross-store moves. `archiveActiveReqs` makes Close wait.

### Tests Added (MR !6)

- **`pkg/graph/tieredstore_history_routing_test.go`**: 15 new regression tests covering each fix above. Notable: pinning tests assert `archiveActiveReqs > 0` *during* the callback / resolve, proving the pin is held across the boundary. Tests:
  - `TestTieredStore_IndexedPublicQueries_IncludeArchive`
  - `TestTieredStore_BulkQueries_DepthGatesArchive`
  - `TestTieredStore_IndexedQueries_DepthGatesArchive`
  - `TestTieredStore_AllCurrentIDAPIs_IncludeArchiveAtDepthAll`
  - `TestTieredStore_ShardForNodeIDChecked_PinsArchive`
  - `TestTieredStore_ForEachHistoryShard_PinsArchive`
  - `TestTieredStore_ArchiveNode_RejectsCrossShardRel_REtoE`
  - `TestTieredStore_ArchiveNode_RejectsCrossShardRel_EtoR`
  - `TestTieredStore_ArchiveNode_RejectsRefRefRel`
  - `TestTieredStore_Clear_NoArchive_SkipsLazyOpen`
  - `TestTieredStore_TemporalIndexCreate_CoversArchive`
  - `TestTieredStore_ResolveShardStore_PinsArchive`
  - `TestTieredStore_FindRelInAnyShardStore_ProbesArchive`
  - `TestTieredStore_AllShardStoresWithLazyOpen_IncludesArchive`
  - `TestTieredStore_HighFrequencyIndexCreate_CoversArchive`

## [3.1.10] - 2026-05-05

### Fixed (history-aware regressions and batch hardening — MR !5)

- **History-aware indexed candidate planning** (`pkg/graph/graph.go`, `pkg/graph/temporal.go`): `NodesByLabel`, `NodesByLabelAndProperty`, `RelationshipsByType`, `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` now derive candidates from the appropriate index (label / property / type / adjacency) and merge them with history IDs. Previously they fell back to `Store.ForEachNodeID` / `ForEachRelID` over every entity even when a narrow index was available — O(N) where O(matches+history) was achievable. New helpers `forEachNodeCandidateID` / `forEachRelCandidateID` (typed callbacks `func(types.NodeID) error` / `func(types.RelID) error`) capture the merge.
- **`AllNodes` / `AllRelationships` history-aware temporal opts** (`pkg/graph/graph.go`): when the caller passes a temporal `QueryOpts` (`ValidAt` or `ValidStart` / `ValidEnd`), the union of current and history IDs is now resolved through `findNodeVersionForOpts` / `findRelVersionForOpts` instead of a current-only scan. Closes a hole in v3.1.7's history-aware sweep.
- **`hasTemporalFilter` rejects one-sided `ValidStart` / `ValidEnd`** (`pkg/graph/temporal_filter.go`): half-set ranges (only `ValidStart` set, or only `ValidEnd` set) used to be treated as "no filter"; now correctly classified as a temporal filter so the history-aware path runs.
- **Combined property+temporal queries seeded from the property index** (`pkg/graph/temporal.go`): `NodesByLabelPropertyAndTime` and `NodesByLabelPropertyDuring` now seed candidates from the property index when one exists, instead of scanning every entity with the label.
- **`BatchBuilder.AddRelationship` enforces `AllowSelfLoops`** (`pkg/graph/batch.go`): the batch path used to bypass the `ValidationLimits.AllowSelfLoops` check that the non-batch `Graph.AddRelationship` enforces. Self-loops in batches with `AllowSelfLoops: false` now fail with `ErrSelfLoop` at validation time.
- **Batch metadata stamped at execute time, not queue time** (`pkg/graph/batch.go`): `TxFrom` / `CreatedAt` / `UpdatedAt` are now set when `Execute()` runs, not when the operation is queued. Queue-time stamping let `Execute()` see entities with `TxFrom > now` if the queue was held open across the boundary.
- **Batch rels with failed-create endpoints are skipped with diagnostics** (`pkg/graph/batch.go`): if `AddNode` failed earlier in the batch, subsequent `AddRelationship` calls referencing that node used to silently produce orphans. Now a `BatchError` is recorded with the offending rel and the failed endpoint's name.
- **Endpoint integrity hashes captured under endpoint lock** (`pkg/graph/batch.go`): `RelIntegrity.FromNodeHash` / `ToNodeHash` are now refreshed via `GetNode` while holding the endpoint locks, mirroring the non-batch path. Pre-fix code captured the hash before the lock and could miss a concurrent label/property mutation.
- **Cross-shard rel rollback on batch failure rolls back `TxFrom`** (`pkg/graph/tieredstore_write.go`): the rollback path that restores the entity+out write also restores the original `TxFrom` so a re-read sees the rel's original transaction window, not the half-applied batch's. Open-end resolution is hoisted out of the inner loop so it computes once per operation.
- **`hasTemporalFilter` open-end resolution hoisted** (`pkg/graph/temporal.go`): `findNodeVersionMatchingDuring` / `findRelVersionMatchingDuring` now resolve `end == 0` (open-ended interval) once at function entry instead of per-iteration. Documented inline.
- **Empty tx update kept inside store boundary** (`pkg/graph/tx.go`): a tx with no actual mutations no longer leaks to event publish — the empty-update guard now lives at the store boundary, not above it.

### Tests Added (MR !5)

- **`pkg/graph/findings_extra_regression_test.go`**: 14 regression tests paired 1:1 with the fixes above. Multi-entity scenarios covering history-aware candidate planning across all 7 entry points; one-sided ValidStart/ValidEnd; combined property+temporal queries; batch self-loop rejection; batch metadata stamping; failed-endpoint diagnostics; cross-shard rel rollback under failure injection; empty-tx-update boundary; open-end resolution hoist correctness.

## [3.1.9] - 2026-05-05

### Added (out-of-tree extension points — MR !1)

- **`graph.IndexProvider` interface** (`pkg/graph/index_provider.go`): plugin contract for auxiliary indexes that live outside Store's built-in index types (property, temporal, high-frequency, vector). Providers register on the Graph, receive lifecycle events through the existing `EventBus`, and own their persistence and query routing. Designed for tkgd's spatial R-tree.
  - `Graph.RegisterIndexProvider(p IndexProvider) error` — auto-creates a synchronous `EventBus` if none is attached; rejects `AsyncEventBus` (providers need the sync `Subscribe` API).
  - `Graph.UnregisterIndexProvider(name string) error` — detaches and calls `Close`.
  - `Graph.IndexProviders() []string` — lexicographic list for admin / snapshot tests.
  - `Graph.Close()` closes all registered providers before the store; errors joined.
  - 3 new sentinel errors: `ErrIndexProviderExists`, `ErrIndexProviderNotFound`, `ErrIndexProviderEmptyName`.
- **`types.HashableValue` interface** (`pkg/types/property_registry.go`): contract that lets external packages register custom property struct types whose values participate in node/relationship integrity hashing. Values that implement `HashableValue` and whose type is registered via `RegisterPropertyStructType` are accepted by `PropertySlice.Set` (previously rejected as unsupported).
  - `types.RegisterPropertyStructType(v any)` — declares that values of `v`'s type (and pointer-to-that-type) are valid property values. Idempotent. Both value and pointer forms accepted.
  - `types.RegisteredPropertyStructTypes() []string` — admin / diagnostic listing.
  - `pkg/graph/integrity.go appendPropertyValue` now dispatches custom types via `HashableValue.HashBytes()` instead of panicking.
  - **HashableValue is treated as a wire format** — output bytes feed the hash chain; once written, you cannot change the encoding without breaking every existing chain that contains the value. Doc comment in `property_registry.go` spells out the determinism / stability requirements.

### Fixed (MR !1)

- **TOCTOU race in `Graph.RegisterIndexProvider`** (`pkg/graph/index_provider.go`): the original implementation unlocked `g.mu` between the duplicate-name check and the entry insertion, allowing concurrent goroutines registering the same `Name()` to all pass the dup check, all subscribe to the bus, and overwrite each other's map entries — leaving N-1 orphaned subscriptions whose unsubscribe closures were lost. Fixed by holding `g.mu` through the entire critical section (dup check → auto-bus creation → type assertion → `Subscribe` → entry insertion). `EventBus.Subscribe` is non-reentrant w.r.t. graph mutations, so holding the lock through it is deadlock-safe.
- **Nil property values in hash computation** (`pkg/graph/integrity.go`): `appendPropertyValue` previously panicked in its default switch arm when called with `v == nil`. Common case from loaders that map SQL NULL to Go nil. Now nil hashes to its type tag alone (deterministic, stable).

### Changed (MR !1)

- **`PropertySlice.Set` accepts registered struct/pointer types** (`pkg/types/propertyslice.go`): `reflect.Ptr` and `reflect.Struct` previously rejected wholesale; now accepted when the type has been registered via `RegisterPropertyStructType`. Backwards-compatible: unregistered structs still rejected with `ErrUnsupportedValueType`.
- **`graph.IndexProvider.OnEvent` doc comment** clarifies that the `Event.EntityID` is `types.EntityID` and lookups should go through `g.GetNode(types.NodeID(ev.EntityID))` or `g.GetRelationship(types.RelID(ev.EntityID))` (corrects a stale doc reference to non-existent `g.Node` / `g.Relationship` methods).

### Tests Added (MR !1)

- **IndexProvider regression suite** (`pkg/graph/index_provider_test.go`): 12 tests covering Register/Unregister, duplicate/empty/nil name rejection, event fan-out, auto-bus-creation, Close propagation from `Graph.Close`, error joining, async-bus incompatibility, and a concurrent-registration race-safety test that pre-fix code would have failed (50 goroutines register the same `Name()`; exactly 1 succeeds, exactly 1 receives events).
- **Property-registry suite** (`pkg/types/property_registry_test.go`): 7 tests covering value/pointer registration, registering pointer also accepts value, unregistered rejection, nil-pointer rejection, idempotent re-registration, lexicographic listing.

### Documentation (post-v3.1.8 polish)

- **`pkg/types/temporal.go`**: explicit `EntityID` zero-value semantics — `0` is the universal sentinel for "unset" across `Event.EntityID`, `BatchError.ID`, `QueryOpts.After`, and `TemporalMetadata.baseEntityID`. Go's untyped-constant rule keeps `if id == 0` and `if opts.After != 0` working unchanged.
- **`CLAUDE.md` TieredStore section**: added "Primary-label class is immutable" rule documenting that the `ErrPrimaryLabelClassMutation` guard is enforced at the `TieredStore` Store-impl boundary only — `MemoryStore` and `BadgerStore` are single-shard and don't care; if you add another sharded backend, replicate the guard there.
- **`tasks/lessons.md`**: renumbered the duplicate `B26 Performance Tests Need Production Shape` to `B34` (collision with `B26 Lock Acquisition Without Defer Leaks on Panic`).
- **`pkg/graph/store_contract_test.go`**: dropped the `nodeIDsToSnowflake` / `relIDsToSnowflake` adapter functions left over from MR !4's typed-ID merge. Replaced the assertion helpers with Go-generic versions (`assertIDSet[T orderedID]` / `assertIDSetPreserveOrder[T orderedID]`) so callers can pass `[]types.NodeID`, `[]types.RelID`, `[]types.EntityID`, or `[]snowflake.ID` directly.

## [3.1.8] - 2026-05-05

### Changed (typed entity IDs)

This release pushes typed entity wrappers (`types.NodeID`, `types.RelID`, `types.EntityID`) through every public method signature, struct field, and internal storage map in `pkg/graph`. The wrappers were already public-shaped via the `SnowflakeID()` accessor; this release exports them and makes them the lingua franca of the package.

**Architecture invariant**: only `keys.go` (binary key encoding), `wire.go` (msgpack on-disk format), the `snowflake.Node` library boundary, the LRU cache (`pkg/graph/lru.go`, type-agnostic infrastructure), and a small set of deliberately type-agnostic surfaces (`entityLockManager`, `collectDeleteIDs`, `sameIDSet`, `Graph.DecomposeID`) see raw `snowflake.ID`. Everything else flows typed.

#### Public type surface

- **Exported entity ID wrappers** (`pkg/types/node.go`, `pkg/types/relationship.go`, `pkg/types/temporal.go`): `nodeID` → `NodeID`, `relID` → `RelID`, `entityID` → `EntityID`. The wrappers and their `SnowflakeID()` accessor were already public-shaped — only the type names became exported.
- **`ID()` accessors** (`pkg/types/node.go`, `pkg/types/relationship.go`): `func (n *Node) ID() NodeID` and `func (r *Relationship) ID() RelID`. `InternalID()` retained as a deprecated alias (scheduled for removal in this release).
- **`TemporalMetadata.SetBaseEntityID`** (`pkg/types/temporal.go`): now takes `types.EntityID` instead of `snowflake.ID`. Symmetric with the existing `BaseEntityID() EntityID` getter.

#### Public method signatures

- **`Graph` methods** (`pkg/graph/graph.go`, `pkg/graph/context.go`, `pkg/graph/temporal.go`, `pkg/graph/txtime.go`, `pkg/graph/integrity.go`, `pkg/graph/tx.go`, `pkg/graph/batch.go`): 56+ methods now take `types.NodeID` / `types.RelID` parameters and return typed values. ~950 `n.InternalID().SnowflakeID()` patterns at callsites collapse to `n.ID()`.
- **`Store` interface** (`pkg/graph/store.go`): all 35+ methods now use typed IDs. `MemoryStore`, `BadgerStore`, and `TieredStore` implementations updated.
- **`GraphTx` and `BatchBuilder`**: typed mirrors of the `Graph` API.

#### Public struct fields (Tier A — public-API leaks closed)

- **`Event.EntityID`** (`pkg/graph/events.go`): `snowflake.ID` → `types.EntityID`. ~30 `.SnowflakeID()` unwraps dropped at event-publication sites in `tx.go`, `batch.go`, `context.go`, `graph.go`.
- **`BatchError.ID`** (`pkg/graph/batch.go`): `snowflake.ID` → `types.EntityID`. 6 producer sites updated; `snowflake` import dropped from `batch.go` and `batch_test.go`.
- **`QueryOpts.After`** (`pkg/graph/store.go`): `snowflake.ID` → `types.EntityID`. The 5 paginate helpers (`paginateIDs`, `paginateNodes`, `paginateRels`, `paginateNodeIDs`, `paginateRelIDs`) also take typed cursors now; each extracts `afterRaw := after.SnowflakeID()` once at the top.

#### Internal helpers (Tier C — chokepoint consolidation)

- **`highFrequencyIndex`** (`pkg/graph/hf_index.go`): bucket storage + `add`/`remove`/`pointQuery`/`rangeQuery` typed.
- **`Graph.forEachKnownNodeID/RelID`** (`pkg/graph/temporal.go`): callback parameter types migrated; 11 caller closures + 6 in-scope helpers updated.
- **`TieredStore.shardForNodeID/RelID/Checked`** (`pkg/graph/tieredstore.go`): ~43 callers across 5 tieredstore files updated.
- **`Graph.publishEvent`** (`pkg/graph/graph.go`): parameter type migrated; 18 callsites updated as part of the `Event.EntityID` change.
- **BadgerStore unexported helpers** (`pkg/graph/badgerstore.go`): `prefetchNode`, `getNodeLocked`, `getRelLocked`, `cascadeDeleteInner`, `cascadeDeleteLocked`, `filterNodeIDsByTemporalPeek`, `filterRelIDsByTemporalPeek`, `fetchNodesWithTemporalFilter`, `fetchRelsWithTemporalFilter` all take typed parameters.
- **MemoryStore storage maps** (`pkg/graph/memorystore.go`): 8 maps (`nodes`, `rels`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx`, `nodeHistory`, `relHistory`) keyed by typed IDs. ~25 top-of-method `id := nid.SnowflakeID()` shims dropped entirely. 5 internal helpers retyped (`deleteRelLocked`, `filterNodeIDsByTemporal`, `filterRelIDsByTemporal`, `sortNodesByID`, `sortRelsByID`). `.SnowflakeID()` count: 46 → 17 (-63%).
- **BadgerStore storage maps** (`pkg/graph/badgerstore.go`): 6 maps (`nodeIDs`, `relIDs`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx`) keyed by typed IDs. Map type-safety prevents accidental cross-kind lookups (compiler now catches `bs.nodeIDs[someRelID]` mistakes).
- **Pagination helpers** (`pkg/graph/pagination.go`): added `paginateNodeIDs`, `paginateRelIDs`, `toNodeIDs`, `toRelIDs` typed equivalents alongside the existing raw `paginateIDs`.

#### Audit snapshot post-migration

- 207 raw `snowflake.ID` references remain in production code (down from ~600+ pre-migration); all at deliberate Tier D boundaries (LRU cache, `keys.go`, `wire.go` format, `snowflake.Node` library calls, type-agnostic helpers).
- 17 references in chokepoint files (`keys.go` + `wire.go`) — all justified.
- Public API surface is fully typed; no exported field, parameter, or return type uses raw `snowflake.ID` except `Graph.DecomposeID` / `DecomposeID` (deliberately type-agnostic — accepts either node or relationship IDs).

#### Migration notes for downstream consumers

`engram` is the only known consumer (currently pinned at `v3.1.1`). Migration steps when bumping:

1. `n.InternalID().SnowflakeID()` → `n.ID().SnowflakeID()` (still works during the alias window) or just `n.ID()` if passing into a Graph method that has already been migrated.
2. Anywhere a raw `snowflake.ID` was passed to `g.GetNode`, `g.AddNodeLabel`, `g.DeleteNode`, etc.: wrap as `types.NodeID(id)` — or, preferred, switch the variable's type to `types.NodeID` at its declaration.
3. Same pattern for relationships with `types.RelID`.
4. `EntityID` is now public if you store base-entity references from `TemporalMetadata.BaseEntityID()`.
5. `Event.EntityID`, `BatchError.ID`, `QueryOpts.After` are now `types.EntityID` — wrap raw IDs at the construction site or pass typed values directly.
6. `TemporalMetadata.SetBaseEntityID` now takes `types.EntityID`; wrap with `types.EntityID(rawID)` at callsites.

### Documentation (Tier D chokepoint invariant)

- **`pkg/graph/keys.go`**: package-doc-style preamble stating the chokepoint invariant — only this file (binary key encoding), `wire.go` (msgpack on-disk format), `lru.go` (type-agnostic LRU infrastructure), `entity_locks.go` (type-agnostic lock pool), and direct `snowflake.Node` library calls legitimately consume raw `snowflake.ID`. Everything else flows typed.
- **`pkg/graph/wire.go`**: explicit doc explaining why `nodeWire.ID int64` / `relWire.ID int64` cannot become typed — these are the on-disk msgpack format, and changing the field type would break unmarshalling of every existing Badger db file. Graph layer wraps these int64 values into typed IDs at the deserialization boundary (`wireToNode` / `wireToRel`).
- **`pkg/graph/entity_locks.go`**: doc on `entityLockManager` explaining its type-agnostic design — the same 256-shard pool serves both node and rel IDs by hashing the snowflake bits. A typed wrapper would imply distinct lock domains; they aren't.

### Fixed (TieredStore cross-shard hardening — MR !4)

- **TieredStore cross-shard rel reachable after start shard cold demotion** (`pkg/graph/tieredstore.go`, `pkg/graph/tieredstore_read.go`): `shardForRelID` and `shardForRelIDChecked` now probe cold event shards in addition to hot/warm during fallback resolution. Previous "skip cold shards" fast-path silently lost a live cross-shard rel once its start-node shard aged warm→cold — the rel's entity stayed on the original shard but the lookup excluded it. `GetRelationship`, `IncomingRelationships`, and `IncomingRelationshipsForNodes` now resolve via `shardForRelIDChecked` so the cold shard remains pinned for the duration of the read.
- **TieredStore empty-history fan-out no longer wakes cold shards** (`pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`): `GetNodeHistory`, `GetRelHistory`, `GetNodeVersion`, `GetRelVersion`, `TruncateNodeHistory`, and `TruncateRelHistory` skip the cross-shard fan-out when the live entity is present on its home shard. The fan-out is only needed to recover history of a deleted entity whose home shard no longer holds the live index — for an alive entity with no history, the empty result is authoritative locally. Exception: when the home shard is `refArchive`, the fan-out still runs because `ArchiveNode` only migrates the live entity and pre-archive history versions remain on `refShard`.
- **TieredStore checkout pinning on rel write paths and node-keyed adjacency reads** (`pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`): `ReplaceRelationship`, `DeleteRelationship`, `DeleteRelWithHistory`, `OutgoingRelationships`, `OutgoingRelationshipsForNodes`, `IncomingRelationships`, and `IncomingRelationshipsForNodes` now resolve their owner shards via `shardForRelIDChecked`/`shardForNodeIDChecked` and `defer checkin()`. Previously they used the unchecked variants and dereferenced the returned `*BadgerStore` pointer — once the rel-fallback probe started opening cold shards, that pointer could be closed by `closeIdleShards` mid-operation.
- **TieredStore checkout pinning on remaining write paths** (`pkg/graph/tieredstore_write.go`): `PutRelationship`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteNodeCascade`, `DeleteNodeWithHistory`, `ReplaceNodeWithHistory`, `ReplaceRelWithHistory`, `PutNodeVersion`, and `PutRelVersion` now also resolve their owner shards via the checked resolvers and pin the cold owner for the duration of the write.
- **TieredStore relationship resolver probes refArchive** (`pkg/graph/tieredstore.go`): `shardForRelID` and `shardForRelIDChecked` now check `refArchive` (between `refShard` and the timestamp candidate) so any rel that ends up on the archive — e.g. when both endpoints get archived together by `ArchiveNode` — remains reachable through every public read/write path. Mirrors the equivalent `shardForNodeID(Checked)` archive probe.
- **`ReplaceRelWithHistory` routes by relationship ID, not start-node ID** (`pkg/graph/tieredstore_write.go`): every other rel-write path resolves the owner via `shardForRelIDChecked(rel ID)`; this method previously used `shardForNodeIDChecked(StartNodeID)`. For rels whose entity has been migrated independently of the start node the start-node-keyed lookup picks the wrong shard and skips the `refArchive` probe.
- **`DeleteRelationship` cross-shard rollback parity** (`pkg/graph/tieredstore_write.go`): the cross-shard delete previously deleted the entity+out from the entity shard then attempted the in/ delete on the end-node shard with no rollback — a failure of the second leg would leave a phantom incoming-index entry. Now mirrors the rollback already in place on the create path: on second-leg failure the entity+out write is restored via `putRelEntityAndOut(r)`.
- **`ensureRefArchive` ↔ `Close` race** (`pkg/graph/tieredstore.go`, new `ErrStoreClosed` sentinel): a reader that observed `refArchive==nil` could lazy-open a fresh archive after `Close` had already closed the store, leaking the freshly opened DB handle. `Close` now sets a `closed atomic.Bool` under `archiveMu` before tearing the archive down; `ensureRefArchive` consults the flag (also under `archiveMu`) and returns `ErrStoreClosed` instead of opening.
- **TieredStore deleted entity history routing** (`pkg/graph/tieredstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`): node and relationship history reads/truncation now fall back to probing history-owning shards when live indexes no longer identify the owner after delete. Node history writes route reference snapshots to the reference shard; relationship history writes route by the relationship start-node shard, matching cross-shard entity ownership. This restores parity with `MemoryStore` and `BadgerStore` for tombstones after deleting reference nodes and `Case → Signal` relationships.
- **TieredStore relationship history cold-shard race** (`pkg/graph/tieredstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`): added `shardForRelIDChecked` paralleling `shardForNodeIDChecked` — increments `activeReqs` on event shards so `closeIdleShards` cannot close the DB while a relationship history read is in flight. `GetRelVersion`, `GetRelHistory`, and `TruncateRelHistory` now use the checked variant.
- **TieredStore primary-label class invariant** (`pkg/graph/tieredstore_write.go`, new `ErrPrimaryLabelClassMutation`): `AddNodeLabelToken{,WithHistory}` and `RemoveNodeLabelToken{,WithHistory}` now reject mutations that would change the primary label's ontology class (reference ↔ event). Such mutations would leave the live entity on its original shard while subsequent history snapshots routed to a different shard, fragmenting the version chain.
- **TieredStore `refArchive` data race** (`pkg/graph/tieredstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`, `pkg/graph/tieredstore_admin.go`): `TieredStore.refArchive` is now `atomic.Pointer[BadgerStore]`. Concurrent reads from `shardForNodeID`, `shardForNodeIDChecked`, the `ForEach*ID` iterators, and admin helpers no longer race with `ensureRefArchive`'s lazy-open write. `archiveMu` is retained as a single-flight guard for the open operation.
- **`forEachHistoryShard` cold-shard probe** restored to `DepthAll` from a previous narrowing to `DepthWarm` that would have lost history of cross-shard rels whose start-node shard aged to cold post-rotation.

### Tests Added (TieredStore hardening)

- **Shared store contract suite** (`pkg/graph/store_contract_test.go`): reusable behaviour tests run against `MemoryStore`, `BadgerStore`, and `TieredStore` for current visibility, version history visibility, delete tombstones/history IDs, cursor pagination, temporal filters, synchronous events through the public `Graph` API, and graph stats/cache metrics where supported.
- **TieredStore history-routing regression suite** (`pkg/graph/tieredstore_history_routing_test.go`): direct coverage for `shardForRelIDChecked` paths, primary-label-class rejection, deleted ref-node history, deleted cross-shard rel history, no-op truncate, post-rotation rel on cold start-node shard, public read paths after cold demotion (`GetRelationship`, `OutgoingRelationships`, `IncomingRelationships`, `IncomingRelationshipsForNodes`), empty-history lookups not lazy-opening cold shards, archived-node history survival, cross-shard delete checkout pinning, and update/delete after start-shard cold demotion.

### Fixed

- **TieredStore cross-shard relationship rollback** (`pkg/graph/tieredstore_write.go`): `PutRelationship` and `DeleteRelationship` now reverse the partial cross-shard write when the second step fails. Previously a duplicate `PutRelationship` (or `deleteRelIncoming` failure on delete) could leave orphaned `in/` entries on the end-node shard or an out-of-sync entity/out side. Closes the B7 partial-cross-shard-write window for the create and delete paths.
- **Batch creation shadow-key handling** (`pkg/graph/batch.go`): `BatchBuilder.AddNode` and `BatchBuilder.AddRelationship` now extract `tkg_author_id`, `tkg_signature`, `tkg_authorized_by`, `tkg_auth_level` (provenance) and populate `Integrity` accordingly, mirroring `AddNodeWithContext`/`AddRelationshipWithContext`. `BatchBuilder.AddRelationship` also records `FromNodeHash`/`ToNodeHash` matching the standalone path. Batch-created entities now carry `TxFrom` on `TemporalMetadata`. Previously batch creation rejected provenance shadow keys with an `ErrReservedPrefix` validation error, never set `TxFrom`, and dropped the endpoint hashes.
- **Batch metadata stamped at commit time, not queue time** (`pkg/graph/batch.go`): `TxFrom` for both batch-created nodes and relationships, and `FromNodeHash`/`ToNodeHash` for batch-created relationships, are now populated inside `Execute()` rather than at queue time. `TxFrom` reflects when the batch actually commits — a builder assembled at T0 and executed minutes later records the execute-time clock, not T0. Endpoint hashes are re-read from the live store under the per-rel endpoint locks, so an `UpdateNode` that fires between `AddRelationship` and `Execute` is reflected. Without this, a queued rel could record stale endpoint hashes that never matched the committed endpoint state.
- **Batch rels short-circuit on failed node dependencies** (`pkg/graph/batch.go`): when `PutNodesBatch` fails (all-or-nothing), step 2 now skips relationships referencing those failed nodes and reports a `"skipped — start/end node N failed to create in this batch"` diagnostic instead of letting the rel write surface a generic `"node not found"` that hides the real cause. Self-loops use a single `GetNode` for endpoint-hash refresh instead of two.
- **`GraphTx.UpdateNode`/`UpdateRelationship` empty-update boundary** (`pkg/graph/tx.go`): the empty-update fast path now reads via `tx.g.store.GetNode`/`GetRelationship` instead of the exported `*WithContext` wrappers. The tx already holds `g.mu.Lock()`, so any future `g.mu.RLock()` on the exported wrappers would deadlock the tx — keeping the tx/internal boundary clean preserves the convention that tx code never crosses through exported entry points.
- **`AllNodes`/`AllRelationships` history-aware temporal opts** (`pkg/graph/graph.go`): when the caller passes a temporal `QueryOpts` (`ValidAt` or `ValidStart`/`ValidEnd`), the union of current and history IDs is now resolved through `findNodeVersionForOpts`/`findRelVersionForOpts` so deleted entities that were valid at the query time appear in the result. Non-temporal calls retain the existing fast store-pushdown path.
- **History-aware indexed query candidate planning** (`pkg/graph/graph.go`, `pkg/graph/temporal.go`): `NodesByLabel`, `NodesByLabelAndProperty`, `RelationshipsByType`, `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` now derive their candidate set from the appropriate index (label / property / type / adjacency) and merge it with history IDs, instead of running `Store.ForEachNodeID`/`ForEachRelID` over every entity. New helpers `forEachNodeCandidateID` and `forEachRelCandidateID` capture the merge pattern. The combined label+property paths (`NodesByLabelAndProperty`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`) seed candidates from `Store.NodesByLabelAndProperty` so an installed property index narrows the current set before history merge — previously they did a label-wide scan and filtered the property in Go.

### Tests Added

- **Un-fixed regression coverage** (`pkg/graph/findings_extra_regression_test.go`): rebased subset of the original history-aware regression test suite, kept only for bugs that were not yet fixed on `main` after MR !2 / v3.1.7. Each of `TestHistoryAwareIndexedNodeQueries_DoNotScanAllCurrentIDs`, `TestHistoryAwareNeighborQuery_DoesNotScanAllCurrentRelIDs`, `TestTieredStore_PutRelationshipRollsBackIncomingOnEntityFailure`, `TestGenericAllTemporalOpts_UseHistoricalDeletedEntities`, and `TestBatchCreation_UsesSharedMetadataPreparation` is now paired with a focused fix in this MR. `TestHistoryAwarePropertyTemporalQueries_UsePropertyIndexCandidates` additionally guards the property-index pushdown on the combined label+property temporal paths — counts on `NodesByLabel` vs `NodesByLabelAndProperty` show the new code seeds from the tighter property index.

### Benchmarks Added

- **Graph performance baseline suite** (`pkg/graph/bench_baseline_test.go`, `pkg/graph/bench_production_test.go`, `Makefile`): added `BenchmarkGraphBaseline/...` coverage for memory-store reads/writes, temporal queries, batch and transaction operations, Badger async/sync writes, Badger indexed reads, and TieredStore reference/event/cross-shard writes. Added `BenchmarkGraphProduction/...` scenarios for public `Graph` APIs covering large graph reads, high-degree traversal, temporal and bitemporal queries, node and relationship history chains, public method surface checks, export/import, sync/async event buses, TieredStore multi-shard queries, and batch/transaction write shapes. Added small and large production profiles: `make bench-graph-production-small` keeps the routine 10K-node/30-version suite, while `make bench-graph-production-large` raises the stress profile to 100K nodes, 1M regular relationships, a 10K-degree hub, 3,000-version node and relationship history chains, larger export/import, TieredStore, batch, and public-surface fixtures. `make bench-graph-production` remains an alias for the small profile; `make bench-graph-all` and `make bench-graph-all-large` run the baseline with the respective production profile for `benchstat` comparisons against `main`.

## [3.1.7] - 2026-05-05

### Fixed

- **History-aware graph semantics** (`pkg/graph/integrity.go`, `pkg/graph/graph.go`, `pkg/graph/temporal.go`, `pkg/graph/context.go`, `pkg/graph/tx.go`):
  - `VerifyNodeHashChain` now recomputes each node version with that version's own labels instead of reusing the current tip's labels, so hash-chain verification remains valid after label add/remove mutations.
  - `AddNodeLabel` / `RemoveNodeLabel` now set `TxTo` on the previous version and `TxFrom` on the new version, and compute the new hash after the version bump.
  - `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` now resolve historical versions instead of relying only on current label/property/adjacency indexes.
  - `NodesByLabel`, `NodesByLabelAndProperty`, and `RelationshipsByType` are now history-aware when called with a temporal `QueryOpts` (`ValidAt` or `ValidStart`/`ValidEnd`). Previously these generic entry points routed temporal queries through store-side pushdown that consults only current indexes, so a label/type membership that held at `t` but no longer holds was invisible. Non-temporal calls keep the existing fast pushdown path.
  - No-op mutations no longer publish update events for idempotent label adds, empty property updates, empty in-place updates, or successful no-op compare-and-delete operations.
  - `ImportNodeWithID` and `ImportRelationshipWithID` now extract temporal/provenance shadow properties, populate transaction time, increment stats, and publish create events consistently with generated-ID creation paths.

### Changed

- **Documentation metadata alignment** (`README.md`, `AGENTS.md`, `docs/architecture.md`, `docs/api.md`): updated documented Go/version metadata to match `go.mod` and the latest changelog entry, and corrected combined temporal query docs to describe history-aware behavior.
- **Behavior change** (`pkg/graph/graph.go`): `NodesByLabel(label, opts)`, `NodesByLabelAndProperty(label, key, value, opts)`, and `RelationshipsByType(typeName, opts)` now scan history when called with a temporal `QueryOpts` (`ValidAt` and/or `ValidStart`/`ValidEnd`). Callers who relied on the previous (incorrect) current-only behavior will see different results: nodes/rels that matched the predicate at the requested time but no longer do are now included, and entities that match now but did not match at the requested time are excluded. Non-temporal calls retain the existing fast pushdown path. The during-interval semantic is "predicate held on any version overlapping [start, end)" — implementations that need only the most-recent version overlapping should call `getNodeVersionDuring`/`getRelVersionDuring` directly.

### Tests Added

- `TestVerifyNodeHashChain_LabelMutations` — regression coverage for hash-chain verification after label add/remove. Three sub-tests: a 3-distinct-label-set chain (every version is a witness), a discriminating history tamper that pre-fix code would have accepted but per-entry-label code rejects, and a deleted-entity path that exercises the `chain[len(chain)-1]` fallback.
- `TestNodeHashChain_InspectsHashValues` — probes actual hash bytes (not just the boolean from `VerifyNodeHashChain`) and walks the persisted chain to verify per-version Hash/PrevHash linkage independently.
- `TestGetNodesByLabelValidAt_UsesHistoricalLabelVersion` — verifies label point-in-time queries use historical label sets.
- `TestNodesByLabelPropertyTemporalQueries_UseHistoricalPropertyVersion` — verifies combined label/property temporal queries use historical property values.
- `TestGetNeighborsValidAt_UsesHistoricalRelationships` — verifies temporal neighbor traversal sees deleted historical relationships.
- `TestLabelMutations_UpdateTransactionTimeBounds` — verifies label mutations update bitemporal transaction bounds.
- `TestNoOpMutations_DoNotPublishUpdateEvents` — verifies no-op mutation paths do not publish update events.
- `TestImportNodeWithID_MatchesAddNodeMetadataEventsAndStats` / `TestImportRelationshipWithID_MatchesAddRelationshipMetadataEventsAndStats` — verifies import-by-ID public methods match creation semantics for metadata, stats, and events.
- `TestGraphTx_PropertyConvenienceMethods`, store-level label-add tests, `TestDocsMetadataMatchesSourceOfTruth`, and `TestRecurrence_Monthly_LastDay` — close direct coverage gaps and prevent docs/version drift.
- `TestNodesByLabel_TemporalOpts_Adversarial`, `TestNodesByLabelAndProperty_TemporalOpts_Adversarial`, `TestRelationshipsByType_TemporalOpts_Adversarial` — verify the generic `*By*(opts QueryOpts)` entry points are history-aware when temporal filters are set. Multi-entity scenarios with diverging lifecycles, exact-set assertions (`assertNodeSet`/`assertRelSet`) catching over-reporting and omission, the decisive "predicate-anywhere-in-interval" case (a node whose label held during part of the interval but not on the most-recent version), and pagination on the temporal path.

## [3.1.6] - 2026-04-10

### Added

- **Node label mutation after creation** (`pkg/types/node.go`, `pkg/graph/graph.go`, `pkg/graph/store.go`, `pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, `pkg/graph/tieredstore_write.go`, `pkg/graph/tx.go`):
  - `Graph.AddNodeLabel(id snowflake.ID, label string) error` — mirror of `RemoveNodeLabel`. Validates label name length, enforces `MaxLabelsPerNode`, advances the hash chain, writes a version history entry, updates the label index, and publishes `EventNodeUpdate`. Idempotent: a no-op (no version bump, no history) when the node already has the label. Returns `ErrNodeNotFound` if the node does not exist, `ErrTooManyLabels` if adding would exceed the configured maximum, and `ErrNameTooLong` if the label name exceeds `MaxNameLength`.
  - `GraphTx.AddNodeLabel` / `GraphTx.RemoveNodeLabel` — transactional wrappers that snapshot the node for rollback, call the lock-free internal implementations under `g.mu.Lock`, and track label deltas so `Rollback()` can restore label-mutated rows through Store label-token helpers.
  - `types.Node.AddLabelTokenRaw(tok uint16) bool` — counterpart to `RemoveLabelTokenRaw`. Appends `tok` as an extra label; returns `false` if `tok == 0` or already present.
  - `Store.AddNodeLabelTokenWithHistory(id, tok, updatedNode, prevVersion, prevState)` — atomic label-add + history + persist, mirroring `RemoveNodeLabelTokenWithHistory`. Implemented in `MemoryStore`, `BadgerStore`, `TieredStore`.
  - `Store.AddNodeLabelToken(id, tok, updatedNode)` — non-history variant used by `GraphTx.Rollback` to reverse label deltas without polluting version history.

### Fixed

- **Transaction rollback label index consistency** (`pkg/graph/tx.go`): `ReplaceNode` deliberately leaves the label index alone (labels are immutable on that path), so a rollback after `GraphTx.AddNodeLabel` / `RemoveNodeLabel` must restore label-mutated rows through `Store.AddNodeLabelToken` / `RemoveNodeLabelToken`. `NodesByLabel` queries could otherwise return a node that no longer had the label (or miss one that did). `GraphTx` tracks label deltas separately and applies them while restoring the pre-transaction node snapshot. Exposed by two regression tests before the fix.

### Tests Added

- `TestAddNodeLabel_AddsExtraLabel` — basic add path
- `TestAddNodeLabel_IdempotentIfAlreadyPresent` — no version bump when label already present
- `TestAddNodeLabel_EmptyNameRejected` — empty label rejected
- `TestAddNodeLabel_NameTooLong` — `ErrNameTooLong` sentinel
- `TestAddNodeLabel_TooManyLabelsRejected` — `ErrTooManyLabels` sentinel when crossing `MaxLabelsPerNode`
- `TestAddNodeLabel_NodeNotFound` — `ErrNodeNotFound` for unknown ID
- `TestAddNodeLabel_HashChainAdvances` — new hash linked via `PrevHash` to previous hash
- `TestAddNodeLabel_WritesHistoryEntry` — pre-mutation snapshot written to history at version 0, current bumped to version 1
- `TestAddNodeLabel_NodesByLabelUpdated` — new label index entry visible via `NodesByLabel`
- `TestAddNodeLabel_PublishesEvent` — `EventNodeUpdate` published after commit
- `TestGraphTx_AddNodeLabel_Commit` — transactional commit persists
- `TestGraphTx_AddNodeLabel_Rollback` — rollback restores node state
- `TestGraphTx_AddNodeLabel_RollbackRestoresLabelIndex` — rollback also restores the label index (regression)
- `TestGraphTx_RemoveNodeLabel_RollbackRestoresLabelIndex` — remove-then-rollback restores the label index (regression)
- `TestGraphTx_AddNodeLabel_AfterCommitReturnsTxDone` — `ErrTxDone` after commit
- `TestGraphTx_RemoveNodeLabel_Commit` — transactional remove + commit
- `TestGraphTx_RemoveNodeLabel_Rollback` — rollback restores the removed label
- `TestGraphTx_RemoveNodeLabel_LastLabelError` — `ErrLastLabel` sentinel inside tx
- `TestGraphTx_RemoveNodeLabel_AfterRollbackReturnsTxDone` — `ErrTxDone` after rollback

### Benchmarks Added

- `BenchmarkAddNodeLabel` — ~1.2µs/op, 22 allocs/op (MemoryStore, Apple M4 Max) — parity with `BenchmarkRemoveNodeLabel`
- `BenchmarkAddNodeLabelIdempotent` — ~112ns/op, 4 allocs/op — idempotent fast path
- `BenchmarkRemoveNodeLabel` — ~1.2µs/op, 24 allocs/op

## [3.1.5] - 2026-04-02

### Added

- **Batch incoming adjacency query** (`pkg/graph/store.go`, `pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/graph.go`): `IncomingRelationshipsForNodes(nodeIDs, typeToken)` returns incoming relationships for multiple nodes in a single batched operation. Symmetric counterpart to `OutgoingRelationshipsForNodes`. BadgerStore leverages early type filtering from `inIdx` (stores relID -> typeToken). TieredStore handles cross-shard entity fetches (relIDs from node's shard, entities resolved via `shardForRelID`). Same return contract: `map[snowflake.ID][]*types.Relationship`, per-node sorted, absent for zero incoming.

### Tests Added

- `TestMemoryStoreIncomingRelationshipsForNodes` — basic, type filter, empty input, no-match
- `TestMemoryStoreIncomingForNodesDuplicateInput` — duplicate nodeIDs in input
- `TestMemoryStoreIncomingForNodesSorted` — per-node sort order
- `TestBadgerStoreIncomingForNodesAll` — all types, multiple nodes
- `TestBadgerStoreIncomingForNodesFiltered` — type filter
- `TestBadgerStoreIncomingForNodesEmpty` — nil and empty input
- `TestBadgerStoreIncomingForNodesSorted` — per-node sort order
- `TestBadgerStoreOutgoingForNodesCorruptionError` — corruption error propagation (outgoing)
- `TestBadgerStoreIncomingForNodesCorruptionError` — corruption error propagation (incoming)
- `TestBadgerStoreOutgoingForNodesOrphanSkipped` — index orphan silently skipped (outgoing)
- `TestBadgerStoreIncomingForNodesOrphanSkipped` — index orphan silently skipped (incoming)
- `TestBadgerStoreOutgoingForNodesNonexistentNode` — nonexistent node returns nil
- `TestBadgerStoreIncomingForNodesNonexistentNode` — nonexistent node returns nil
- `TestGraphIncomingRelationshipsForNodes` — Graph layer integration
- `TestGraphIncomingForNodesUnregisteredType` — unregistered type returns nil
- `TestTieredStore_IncomingRelationshipsForNodes` — cross-shard incoming, mixed ref+event nodes

## [3.1.4] - 2026-04-02

### Added

- **Batch outgoing adjacency query** (`pkg/graph/store.go`, `pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/graph.go`): `OutgoingRelationshipsForNodes(nodeIDs, typeToken)` returns outgoing relationships for multiple nodes in a single batched operation. Amortizes lock acquisition (one `idxMu.RLock` instead of N) and shard resolution (groups nodeIDs by shard in TieredStore). Returns `map[snowflake.ID][]*types.Relationship` — per-node slices sorted by ID; nodes with zero outgoing rels absent from map. Graph layer accepts `typeName string` with single token resolution.

### Tests Added

- `TestMemoryStoreOutgoingRelationshipsForNodes` — basic, type filter, empty input, no-match
- `TestMemoryStoreOutgoingForNodesPartialResults` — mixed nodes with/without rels
- `TestMemoryStoreOutgoingForNodesDuplicateInput` — duplicate nodeIDs in input
- `TestMemoryStoreOutgoingForNodesSorted` — per-node sort order
- `TestBadgerStoreOutgoingForNodesAll` — all types, multiple nodes
- `TestBadgerStoreOutgoingForNodesFiltered` — type filter
- `TestBadgerStoreOutgoingForNodesEmpty` — nil and empty input
- `TestBadgerStoreOutgoingForNodesSorted` — per-node sort order
- `TestGraphOutgoingRelationshipsForNodes` — Graph layer integration
- `TestGraphOutgoingForNodesUnregisteredType` — unregistered type returns nil
- `TestTieredStore_OutgoingRelationshipsForNodes` — cross-shard grouping, mixed ref+event nodes

## [3.1.3] - 2026-04-02

### Improved

- **Property index fallback observability** (`pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`): `NodesByLabelAndProperty` now emits a `slog.Debug` log when using the full label-scan fallback path instead of a property index. Helps operators identify missing property indexes that degrade query performance from O(1) to O(N). The hint includes `labelToken` and `propertyKey` for targeted `CreatePropertyIndex` calls.

## [3.1.2] - 2026-03-28

### Fixed

- **WAL corruption recovery for warm/cold shards** (`pkg/graph/tieredstore.go`): Shards opened in read-only mode (warm at startup, cold on lazy-open) now auto-recover from corrupt WAL files left by unclean shutdowns (SIGKILL, Ctrl-C, OOM kill). Previously, a partially written `.mem` file caused `ErrTruncateNeeded` — a fatal startup error requiring manual intervention. The new `openBadgerStoreWithRecovery` detects the truncation error, opens the shard read-write (Badger auto-truncates the corrupt WAL tail), closes it, and reopens read-only. At most one flush window (~100ms) of buffered writes is lost. Applied at all three read-only open sites (L1 pattern): warm shard startup, cold shard `getStore`, cold shard `checkoutStore`. Includes `isTruncateNeeded` fallback for Badger v4's broken `errors.Is` chain (`y.Wrap` uses `%+v` not `%w`).

### Tests Added

- `TestTieredStore_WarmShard_WALCorruptionRecovery` — warm shard with corrupt WAL recovers on startup, data survives, shard remains read-only
- `TestTieredStore_ColdShard_WALCorruptionRecovery` — cold shard with corrupt WAL recovers on lazy-open (L1 pattern)
- `TestTieredStore_WALCorruption_NonTruncateError` — real errors (permission denied) are not masked by recovery path
- `TestTieredStore_WALCorruption_DataIntegrity` — 10 nodes written before crash all survive truncation recovery
- `TestTieredStore_WALCorruption_ConcurrentColdAccess` — 50 concurrent goroutines on corrupt cold shard, no panics or deadlocks

## [3.1.1] - 2026-03-15

### Added

- **Compression configuration** (`pkg/graph/badgerstore.go`, `pkg/graph/graph.go`, `pkg/graph/tieredstore.go`): `BadgerStoreConfig.Compression` (`options.None`/`options.Snappy`/`options.ZSTD`) and `BadgerStoreConfig.ZSTDCompressionLevel` (1-15) control Badger SSTable compression. Zero values keep Badger defaults (Snappy, level 1). Threaded through `Graph.Config` (convenience BadgerStore path) and `TieredStoreConfig` (applied to all shards via `openBadgerStore`).

### Tests Added

- `TestCompression_BadgerStore_None` — store with compression disabled
- `TestCompression_BadgerStore_Snappy` — store with explicit Snappy
- `TestCompression_BadgerStore_ZSTD` — store with ZSTD level 3
- `TestCompression_BadgerStore_ZeroKeepsDefault` — zero value keeps Badger default
- `TestCompression_BadgerStore_InMemory` — compression with InMemory mode
- `TestCompression_Graph_ConfigPassthrough` — Config.Compression flows to BadgerStore
- `TestCompression_TieredStore_Passthrough` — TieredStoreConfig fields stored and passed through
- `TestCompression_ZSTD_DataSurvivesReopen` — ZSTD-compressed data persists across reopen

## [3.1.0] - 2026-03-14

### Changed

- **Key function signatures** (`pkg/graph/keys.go`): Refactored 10 key functions (`nodeKey`, `relKey`, `labelIndexKey`, `relTypeIndexKey`, `outKey`, `inKey`, `histNodeKey`, `histRelKey`, `histNodePrefix`, `histRelPrefix`) from `int64` to `snowflake.ID` for entity ID parameters. Parser functions (`parseIDFromKey`, `parseRelIDFromAdjKey`) now return `snowflake.ID`. Eliminates ~35 `int64(id)` conversions in `badgerstore.go`/`badgerstore_partial.go` and ~14 `snowflake.ID(parseIDFromKey(...))` wrappings. `putUint64` remains `int64` (generic binary helper). Wire format unchanged — same bytes on disk.
- **Test-only key helpers** (`pkg/graph/keys_helpers_test.go`): Same `int64` → `snowflake.ID` refactoring for 4 prefix functions, 2 temporal key functions, and 2 parser functions.

## [3.0.68] - 2026-03-14

### Added

- **CompareAndSetProperty / CompareAndSetPropertyWithContext** (`pkg/graph/graph.go`): Atomic compare-and-swap on a single node property for optimistic locking patterns. Returns `(true, nil)` on match+update, `(false, nil)` on mismatch, `(false, error)` on real error. `expected == nil` means "property must not exist"; `newVal == nil` means "delete the property". Value comparison uses exact-type property equality — type must match exactly (`int(42) != int64(42)`). Current releases also match accepted `NaN` property values. Follows the `UpdateNode` pattern: entity lock serialization, pre-mutation snapshot, version bump, temporal metadata, hash chain, `ReplaceNodeWithHistory`.

### Tests Added

- `TestCAS_Match` — match → update → verify persisted
- `TestCAS_Mismatch` — wrong expected → `(false, nil)`, unchanged
- `TestCAS_NilExpected_Absent` — absent prop + nil expected → sets value
- `TestCAS_NilExpected_Present` — existing prop + nil expected → `(false, nil)`
- `TestCAS_DeleteOnMatch` — `newVal=nil` → deletes property
- `TestCAS_NilBoth_Absent` — both nil, absent → `(true, nil)` no-op
- `TestCAS_ShadowKey` — `tkg_` key → error
- `TestCAS_NodeNotFound` — non-existent ID → `ErrNodeNotFound`
- `TestCAS_VersionBump` — successful CAS increments version
- `TestCAS_NoVersionBumpOnMismatch` — mismatch doesn't bump
- `TestCAS_History` — successful CAS adds history entry
- `TestCAS_TypeMismatch` — `int` vs `int64` → no swap
- `TestCAS_DeleteMismatch` — delete with wrong expected → no swap

### Changed

- Bump Go version to 1.26.1
- Bump rho-snowflake-2026 dependency from v1.3.0 to v1.3.2
- Bump rho-mclock dependency from v0.2.0 to v0.2.1
- Tighten TieredStore data directory permissions from 0755 to 0750

### Fixed

- Resolve all 46 gosec findings: annotate G115 integer conversions (integrity hashing, snowflake IDs, registry imports), G703 path traversal false positives, G304 file inclusion false positives, G404 weak RNG in tutorial, G301 directory permissions

## [3.0.67] - 2026-03-13

### Fixed

- **Cross-shard incoming relationship type filter** (`pkg/graph/badgerstore.go`, `pkg/graph/badgerstore_partial.go`): `IncomingRelationships(nodeID, typeToken)` returned empty results for cross-shard relationships in TieredStore. The in-memory `inIdx` stored relationship IDs as a bare set (`struct{}`). When filtering by type, `incomingRelIDs` called `GetRelationship(relID)` on its own shard to read the type token — but for cross-shard relationships (e.g., Signal in event shard → Case in reference shard), the entity lives in the event shard while the incoming index is on the reference shard. `GetRelationship` returned `ErrRelNotFound`, silently skipping the relationship. Changed `inIdx` from `map[snowflake.ID]struct{}` to `map[snowflake.ID]uint16` (relID → typeToken). The type token is already available at index write time — filtering now happens directly on the index without fetching the entity.

## [3.0.66] - 2026-03-12

### Added

- **GraphTx.GetNode(id)** (`pkg/graph/tx.go`): Read a node by snowflake ID within a transaction. Safe because the tx holds the write lock — no concurrent modifications possible. Used by callers that need to inspect node state mid-transaction.

- **GraphTx.AddRelationshipByID(typeName, startID, endID, props)** (`pkg/graph/tx.go`): Create a relationship using endpoint snowflake IDs within a transaction. Mirrors the standalone `Graph.AddRelationshipByID` but participates in tx rollback tracking. The relationship ID is tracked for rollback on `tx.Rollback()`.

- **GraphTx.AddRelationshipByIDIfAbsent(typeName, startID, endID, props)** (`pkg/graph/tx.go`): Atomic check-then-create for relationships within a transaction. Returns `(rel, created, err)` where `created=true` if a new relationship was created, `false` if one already existed. Only tracks for rollback when `created=true`. Enables idempotent relationship creation patterns (e.g., TECHNIQUE_OBSERVED) inside atomic transactions.

### Tests Added

- `TestTxGetNode` — read node within tx, verify properties visible.
- `TestTxGetNode_AfterDone` — GetNode returns `ErrTxDone` after commit.
- `TestTxAddRelationshipByID` — create by ID in tx, commit, verify persisted with properties.
- `TestTxAddRelationshipByID_Rollback` — create by ID, rollback, verify absent.
- `TestTxAddRelationshipByIDIfAbsent` — first call creates, second returns `created=false`, count stays 1.
- `TestTxAddRelationshipByIDIfAbsent_Rollback` — create, rollback, verify absent.

### Documentation

- Updated `docs/api.md` Transactions section with `tx.GetNode`, `tx.AddRelationshipByID`, and `tx.AddRelationshipByIDIfAbsent`.

## [3.0.65] - 2026-03-12

### Added

- **AddRelationshipByIDIfAbsent / AddRelationshipByIDIfAbsentWithContext** (`pkg/graph/context.go`, `pkg/graph/graph.go`): Atomic check-then-create for relationships. Prevents TOCTOU race where concurrent callers both see "absent" and create duplicate relationships. The existence check and creation are serialized under entity locks, guaranteeing exactly one relationship per (type, from, to). Returns `(rel, created, err)` where `created=true` if a new relationship was created, `false` if one already existed.

### Tests Added

- `TestGraphAddRelationshipByIDIfAbsent` — first call creates, second returns `created=false`.
- `TestGraphAddRelationshipByIDIfAbsent_Concurrent` — concurrent callers produce exactly one relationship.

## [3.0.64] - 2026-03-09

### Added

- **AddRelationshipByID / AddRelationshipByIDWithContext** (`pkg/graph/context.go`, `pkg/graph/graph.go`): Relationship creation using endpoint `snowflake.ID` values directly, introduced as an ID-form alternative to passing endpoint node objects. At introduction this path skipped endpoint fetches; current `Unreleased` behavior fetches live endpoints, captures `FromNodeHash`/`ToNodeHash`, and enforces configured constraints.

### Tests Added

- `TestGraphAddRelationshipByID` — happy path: type, endpoints, properties, store retrieval, integrity metadata, adjacency query.
- `TestGraphAddRelationshipByID_SelfLoop` — self-loop rejected with `ErrSelfLoop`.
- `TestGraphAddRelationshipByID_TemporalProps` — `tkg_valid_from` and `tkg_created_at` propagated correctly.

### Documentation

- Updated `docs/api.md` with `AddRelationshipByID` / `AddRelationshipByIDWithContext` documentation.

## [3.0.63] - 2026-03-09

### Added

- **Caller-provided temporal metadata via `tkg_` props** (`pkg/graph/context.go`, `pkg/graph/batch.go`): `AddNode`/`AddRelationship` (and `BatchBuilder` equivalents) now accept `tkg_valid_from`, `tkg_valid_to`, `tkg_created_at` in the props map. Values are extracted before validation (same pattern as provenance), then merged into `TemporalMetadata` alongside auto-set `TxFrom`. Zero API signature changes — fully backward compatible.

### Tests Added

- `TestAddNodeWithTemporal` — `tkg_valid_from`, `tkg_valid_to`, `tkg_created_at` propagated to `TemporalMetadata`; keys not stored as regular properties.
- `TestAddNodeWithoutTemporal` — default behavior unchanged when no temporal props provided.
- `TestAddRelWithTemporal` — temporal props propagated on relationship creation.
- `TestTemporalProps_InvalidType` — non-`int64` values rejected with error.

### Documentation

- Updated tutorial 002 with props-based temporal metadata section demonstrating `tkg_valid_from`, `tkg_valid_to`, `tkg_created_at` usage.

## [3.0.62] - 2026-03-07

### Fixed (5 Defects — production readiness)

- **Fix U — Batch lock-leak on panic** (`pkg/graph/batch.go`, MAJOR): `BatchBuilder.Execute()` acquired `g.mu.Lock()` without defer. A panic between lock acquisition and unlock (e.g., in a Store implementation) would leak the lock and leave `txEventBuffer` non-nil, permanently deadlocking the Graph. Fixed by adding deferred cleanup with an `unlocked` flag.

- **Fix V — Graph validation accepts negative limits** (`pkg/graph/graph.go`, MAJOR): `New()` resolved zero `ValidationLimits` fields to defaults but accepted negative values (e.g., `MaxLabelsPerNode: -1`), which passed through silently and broke downstream comparisons. Fixed by rejecting negative values after zero-to-default resolution.

- **Fix W — BadgerStore accepts invalid config** (`pkg/graph/badgerstore.go`, MAJOR): `NewBadgerStore()` accepted negative `FlushInterval`/`GCInterval`, out-of-range `GCDiscardRatio`, and empty `Dir` when `InMemory` is false. Fixed by adding upfront validation before opening Badger.

- **Fix X — Auth level silently truncates fractional float64** (`pkg/graph/context.go`, MAJOR): `extractProvenance` accepted `tkg_auth_level: 5.9` and silently truncated to `uint8(5)`. Fixed by adding `math.Trunc` check before range check — fractional values now return an error.

- **Fix Y — RemoveNodeLabel crash-consistency gap** (`pkg/graph/graph.go`, `pkg/graph/store.go`, all 3 Store implementations, MAJOR): `removeNodeLabelInternal` performed two separate writes: `PutNodeVersion` then `RemoveNodeLabelToken`. A crash between them would leave a phantom history entry. Fixed by adding `RemoveNodeLabelTokenWithHistory` to the Store interface, implemented atomically in MemoryStore (single lock), BadgerStore (single `appendOps` call), and TieredStore (delegate to shard).

### Tests Added

- `pkg/graph/v3062_fixes_test.go` — 19 tests:
  - `TestNew_NegativeValidationLimits` — each field at -1 rejected; zero/positive accepted.
  - `TestNewBadgerStore_InvalidConfig` — table-driven: negative flush, negative gc, bad ratio, empty dir.
  - `TestExtractProvenance_FractionalAuthLevel` — 5.9 rejected, 5.0 accepted, 0.1 rejected.
  - `TestBatchExecute_PanicRecovery` — inject panicking store, verify lock released.
  - `TestBatchExecute_ConcurrentAccess` — verify lock released after normal batch.
  - `TestBadgerStore_CreateTemporalIndex` — create, duplicate error.
  - `TestBadgerStore_DropTemporalIndex` — drop, double-drop error.
  - `TestBadgerStore_CreateHighFrequencyIndex` — success, duplicate, temporal conflict.
  - `TestBadgerStore_DropHighFrequencyIndex` — drop, double-drop, re-create after drop.
  - `TestBadgerStore_RemoveNodeLabelTokenWithHistory` — atomic label removal + history entry.
  - `TestTieredStore_CreateTemporalIndex_Store` — create across shards, verify query.
  - `TestTieredStore_DropTemporalIndex_Store` — drop, double-drop.
  - `TestTieredStore_CreateHighFrequencyIndex_Store` — create across shards.
  - `TestTieredStore_DropHighFrequencyIndex_Store` — drop, double-drop.
  - `TestTieredStore_SaveLabelRegistry_Deprecated` — in-memory no-op path.
  - `TestTieredStore_SaveRelTypeRegistry_Deprecated` — in-memory no-op path.
  - `TestTieredStore_RemoveNodeLabelTokenWithHistory` — atomic path on TieredStore.
  - `TestRemoveNodeLabel_AtomicHistory` — verifies history + version via Graph API.

### Historical Known Limitations (resolved later)

- **Cursor-based history ID queries** (`export.go:124`, `badgerstore.go:3050`): At this release point, `AllNodeHistoryIDs` and `AllRelHistoryIDs` returned all IDs at once and could exceed memory on large graphs. This was resolved later by `AllNodeHistoryIDsFrom` / `AllRelHistoryIDsFrom` and the bounded-RAM export history loop.
- **Streaming DiffSnapshots** (`temporal.go:618`): At this release point, `DiffSnapshots` materialized all nodes into RAM before computing the diff. This was resolved later by `DiffSnapshotsCallback`; `DiffSnapshots` now delegates to the streaming path and materializes only for callers that request the `SnapshotDiff`.
- **Cross-shard relationship batching** (`tieredstore_write.go:313`): `PutRelationshipsBatch` iterates relationships one-by-one across shards. Partitioning by shard would enable store-level batching for same-shard relationships.

### Documentation

- Refactored `README.md` by moving detailed technical documentation into the `docs/` directory (`api.md`, `persistence.md`, `design.md`), producing a cleaner high-level overview.
- Added updated Snowflake configuration details (microsecond precision, 5-bit node IDs, max ID 15) to both `README.md` and `CLAUDE.md`.

## [3.0.61] - 2026-03-07

### Fixed (6 Defects — pre-release hardening)

- **Fix O — Signature aliasing in DeepCopy** (`pkg/types/node.go`, `pkg/types/relationship.go`, MAJOR): `Node.DeepCopy()` and `Relationship.DeepCopy()` shallow-copied integrity structs — `Signature []byte` shared the same backing array between original and copy. Caller mutation of the copy corrupted the original. Fixed by adding `CloneBytes`, `NodeIntegrity.DeepCopy()`, and `RelIntegrity.DeepCopy()` in `pkg/types/integrity.go`.

- **Fix P — Signature aliasing at input boundary** (`pkg/graph/context.go`, MAJOR): `extractProvenance` assigned `tkg_signature` from the props map without copying. Caller mutation after `AddNode`/`AddRelationship` corrupted stored integrity. Fixed by cloning with `types.CloneBytes`.

- **Fix Q — Signature aliasing in wire encode/decode** (`pkg/graph/wire.go`, MAJOR): `nodeToWire`, `wireToNode`, `relToWire`, `wireToRel` all assigned Signature directly. After msgpack deserialization, Signature could alias Badger's internal value buffer. Fixed by wrapping all 4 assignments with `types.CloneBytes`.

- **Fix R — Data race in SetEventBus/SetAsyncEventBus** (`pkg/graph/graph.go`, MAJOR): `SetEventBus`, `SetAsyncEventBus`, and `GetEventBus` read/wrote `g.events` without synchronization. Fixed by wrapping writes in `g.mu.Lock()` and reads in `g.mu.RLock()`. Post-lock dispatch uses captured `eventPublisher` reference (`dispatchEvent` helper) to avoid reading `g.events` outside the lock.

- **Fix S — Data race in SetTemporalConstraints/AddTemporalConstraint** (`pkg/graph/graph.go`, MAJOR): `SetTemporalConstraints` and `AddTemporalConstraint` wrote `g.constraints` without synchronization. `checkTemporalConstraints` read it under `g.mu.RLock`. Fixed by wrapping writes in `g.mu.Lock()` and the `TemporalConstraints()` getter in `g.mu.RLock()`.

- **Fix T — Synchronous event handler deadlock** (`pkg/graph/context.go`, `pkg/graph/graph.go`, MAJOR): All `publishEvent` calls executed under `g.mu.RLock` (via defer). A synchronous event handler calling graph write methods would deadlock (RLock held, write needs Lock). Fixed by moving event dispatch outside mutation locks: `*WithContext` wrappers capture `g.events` under lock, release lock, then dispatch via `dispatchEvent`. Same pattern applied to `RemoveNodeLabel`, `CloseNodeVersion`, `CloseRelVersion`, `BatchBuilder.Execute`, and `GraphTx.Commit`.

### Changed

- **Config validation**: `TieredStoreConfig.ShardWindow` now rejects values below `time.Minute` (previously accepted any positive duration including sub-millisecond).
- **Doc fix**: `Config.SnowflakeNodeID` comment corrected from "0-511" to "0-15" to match actual validation (5-bit node ID).
- Retagged stale TODOs: `TODO(v3.0.57)` and `TODO(v3.0.58)` → `TODO(v3.1.0)` across `export.go`, `badgerstore.go`, `temporal.go`, `graph.go`, `tieredstore_write.go`.

### Tests Added

- `pkg/types/integrity_test.go` — `CloneBytes` (nil, empty, isolation), `NodeIntegrity.DeepCopy`, `RelIntegrity.DeepCopy`, `Node.DeepCopy` and `Relationship.DeepCopy` signature isolation.
- `pkg/graph/v3061_fixes_test.go` — 13 tests:
  - `TestExtractProvenance_SignatureIsolation` — caller mutation after AddNode cannot corrupt stored signature.
  - `TestWireRoundTrip_NodeSignatureIsolation`, `TestWireRoundTrip_RelSignatureIsolation` — wire encode/decode signature isolation.
  - `TestSetEventBus_NoRace`, `TestSetTemporalConstraints_NoRace` — concurrent config toggles with mutations under `-race`.
  - `TestSyncEventHandler_GraphRead_NoDeadlock` — sync handler calling `GetNode` during AddNode callback.
  - `TestNew_SnowflakeNodeID_Bounds` — boundary validation for 5-bit node ID (15 accepted, 16 rejected, -1 rejected).
  - `TestNewTieredStore_ShardWindow_Invalid` — negative and sub-minute windows rejected.
  - `TestTx_ImportRelationshipWithID` — tx import, commit, duplicate error, rollback.
  - `TestGetRelsAsOf` — temporal query returns correct version at each transaction time.
  - `TestCreateDropTemporalIndex`, `TestDropHighFrequencyIndex` — create/drop lifecycle, idempotency error.
  - `TestToFloat32SliceWire` — wire helper coverage for `[]any`, `[]float32`, nil, unsupported.

## [3.0.60] - 2026-03-06

### Fixed (2 Defects — post-v3.0.59 audit)

- **Fix M — Temporal index concurrent sort race** (`pkg/graph/temporal_index.go`, MAJOR): `sortIfDirty` was called under `idxMu.RLock` (shared) but `sort.Slice` mutates the slice in place. Two concurrent readers calling `queryAt`/`queryOverlap` could race on the sort. Fixed by adding `sortMu sync.Mutex` which serializes `sortIfDirty` — callers still enter under `RLock` but the sort itself is single-threaded.

- **Fix N — NodesByLabel temporal fast path nil fallthrough** (`pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, MINOR): When a temporal index existed but `queryAt`/`queryOverlap` returned a nil result (no matches), the nil fell through the `if ids != nil` guard and proceeded to the O(N) label scan — defeating the index. Fixed by tracking `temporalQuery bool` separately from the result slice, so an empty-but-valid temporal result correctly short-circuits the label scan.

### Tests Added

- `pkg/graph/temporal_index_stress_test.go` — 11 stress tests covering concurrent sort races, interleaved reads/writes, and high-contention temporal index operations (Fix M).

### Changed

- Tutorial `005_performance`: added sections 8-11 (temporal index benchmarks, high-frequency index comparison, vector index throughput, TieredStore shard rotation overhead).
- **Snowflake Layout Upgrade**: Upgraded `github.com/bds421/rho-snowflake-2026` to `v1.3.0`. Switched to microsecond precision (`snowflake.WithMicroseconds()`) and adjusted bit layout: node bits reduced from 10 to 5 (max `SnowflakeNodeID` is now 15), step bits reduced from 12 to 10.
- **Removed hardcoded bit-shifting**: Replaced manual bitwise operations via native `snowflakeLayout.CreatedAt(id)` and `snowflakeLayout.Decompose(id)` APIs across `id_decompose.go`, `shadow.go`, `temporal.go`, `temporal_filter.go`, `entity_locks.go`, and `tieredstore.go`.

### Added

- `AGENTS.md` — comprehensive guidelines, project overview, testing rules, and architecture map for AI agents assisting with the codebase.

### Documentation

- Updated `CLAUDE.md` to include a new lesson on using library APIs instead of reimplementing internals.
- Updated `CLAUDE.md`, `README.md`, `docs/architecture.md`, `docs/SPEC.md`, `pkg/types/shadow.go` to reflect v3.0.60 changes.

## [3.0.59] - 2026-03-04

### Fixed (4 Defects — external audit)

- **Fix I — Transaction isolation gap** (`pkg/graph/context.go`, `pkg/graph/graph.go`, BLOCKER): `BeginTx()` holds `g.mu.Lock()` but individual mutations (`AddNode`, `UpdateNode`, etc.) did not acquire `g.mu`, so concurrent standalone mutations bypassed tx isolation — torn reads were possible during snapshot operations. Fixed via internal/external method split: all 13 exported mutation methods (`*WithContext`, `RemoveNodeLabel`, `CloseNodeVersion`, `CloseRelVersion`) now acquire `g.mu.RLock()` at entry. New unexported `*Internal` variants (lock-free) are called directly by `GraphTx` and `BatchBuilder` (which already hold `g.mu.Lock()`). Lock ordering: `g.mu` → entity locks (safe: entity locks never acquire `g.mu`).

- **Fix J — Rollback event desync** (`pkg/graph/graph.go`, `pkg/graph/tx.go`, MAJOR): Events were emitted immediately during tx mutations via `publishEvent`. On rollback, state was restored but no compensating events were published, leaving EventBus subscribers inconsistent. Fixed by adding `txEventBuffer *[]Event` field to `Graph`. During a transaction, `publishEvent` appends to the buffer instead of dispatching. On `Commit`, events are published after `g.mu.Unlock()` (so handlers can safely call Graph read methods). On `Rollback`, the buffer is discarded.

- **Fix K — Missing directory fsync** (`pkg/graph/registry_file.go`, MINOR): `atomicWriteFile` did `tmp.Sync()` + `os.Rename()` but omitted the directory `fsync`. On crash, the rename could be lost on some filesystems (ext4 with delayed allocation). Fixed by opening the parent directory and calling `Sync()` after rename.

- **Fix L — Dead code removal** (`pkg/graph/temporal.go`, `pkg/graph/tieredstore.go`, TRIVIAL): Removed 3 unused functions: `isNodeValidDuring`, `isRelValidDuring` (temporal.go), `classifyNodeID` (tieredstore.go). Zero callers confirmed via grep.

### Changed

- `pkg/graph/batch.go`: `Execute` now calls `*Internal` variants directly (was calling exported methods that would deadlock under `g.mu.Lock`). Added `"context"` import.
- `pkg/graph/export.go`: Updated `ExportGraph` doc comment to reflect that individual mutations are now also blocked by `g.mu`.

### Tests Added

- `TestMutationBlockedDuringTx` — verifies standalone `AddNode` blocks while a tx holds `g.mu.Lock`.
- `TestSnapshotConsistencyDuringMutation` — concurrent reads and writes under race detector.
- `TestTxCommitPublishesBufferedEvents` — 3 events buffered during tx, all published on Commit in order.
- `TestTxRollbackNoEvents` — zero events published after Rollback.
- `TestTxCommitHandlerCanReadGraph` — event handler successfully calls `GetNode` on Commit (proves events fire after `g.mu.Unlock`).
- `TestBatchEventsNotBuffered` — standalone mutations still emit events immediately.

## [3.0.58] - 2026-03-04

### Fixed (2 Defects — post-v3.0.57 code review triage)

- **Fix G — temporalIndex O(N²) memmove under store write lock** (`pkg/graph/temporal_index.go`, MAJOR): `add()` used binary search + `copy()` shift to maintain sorted order on every insert. For N batch inserts (e.g. bulk node import, N temporal index adds per label) this was O(N²) total memmove, all performed while holding the store's write lock. Fixed via lazy sort: `add()` now appends unsorted and sets `dirty=true`; new `sortIfDirty()` runs `sort.Slice` once at the start of `queryAt()` and `queryOverlap()`. Complexity: N inserts → O(N) appends + O(N log N) sort at first query (vs. O(N²) sorted insertions). The `dirty bool` field adds 1 byte to `temporalIndex`; no external API change.

- **Fix H — RemoveLabelTokenRaw leaves non-nil empty extraLabels** (`pkg/types/node.go`, MINOR): Two removal paths in `RemoveLabelTokenRaw` left `n.extraLabels` as an empty slice with non-zero capacity, violating the convention that an absent label set is `nil`. Case 1 (removing the only extra label via `append([:i], [i+1:]...)`) and Case 2 (promoting the only extra to primary via `extraLabels[1:]`) both produced `[]labelToken{}` with cap > 0. Since `DeepCopy` only copies when `len(n.extraLabels) > 0`, and `ExtraLabelTokens()` already guards with the same check, a non-nil empty slice was functionally harmless but inconsistent and retained the backing array unnecessarily. Fixed by adding an explicit `n.extraLabels = nil` after each removal path that empties the slice.

### Documentation

- `pkg/types/propertyslice.go` (`Set`): added note that `NewPropertySlice` (O(N log N)) is preferred over repeated `Set` calls for bulk construction.
- `pkg/types/recurrence.go` (`RecurrencePattern`): strengthened UTC-only note — DST transitions are invisible to this type; expanded instants stay in UTC and local-time presentation happens after expansion.
- `pkg/graph/vector_index.go` (`toFloat32Slice`): added slow-path note on the `[]any` branch; `[]float32` remains the fast path for high-frequency vector nodes.

### Tests Added

- `TestTemporalIndex_LazySort_BatchInsert` — 100 out-of-order inserts followed by a single `queryAt`; verifies all IDs returned and `dirty` transitions correctly (Fix G).
- `TestTemporalIndex_LazySort_InterleavedReadsWrites` — interleaved `add`/`queryAt`/`queryOverlap` calls; verifies correct results at each step and `dirty` flag transitions (Fix G).
- `TestRemoveLabelTokenRaw_ExtraLabelsNilAfterLastRemoval` — three sub-tests: remove only extra, promote only extra to primary, remove one of two extras (last case verifies non-nil is preserved) (Fix H).

## [3.0.57] - 2026-03-03

### Fixed (6 Production Defects — v3.0.57)

- **Fix A — Rollback/Commit panic leaks graph write lock** (`pkg/graph/tx.go`, CRITICAL): `tx.g.mu.Unlock()` was the last explicit statement in `Rollback()`. Any panic in one of the six rollback phases (store `PutRelationship`, `PutNode`, `ReplaceRelationship`, `ReplaceNode`, `DeleteRelationship`, `DeleteNodeCascade`) left the graph write lock permanently held, deadlocking all subsequent `BeginTx`/`Batch`/`Reset` callers. Fixed by replacing the explicit unlock with `defer tx.g.mu.Unlock()` placed immediately after `tx.done = true`, ensuring the lock is released on both normal and panic paths. Same fix applied to `Commit()` for forward safety.

- **Fix B — RemoveNodeLabel violates temporal guarantees** (`pkg/graph/graph.go`, BLOCKER): `RemoveNodeLabel` overwrote the node in-place via `RemoveNodeLabelToken` with no version bump and no history entry (explicit "No version bump; no history entry" comment). Past-time queries (`GetNodeAt`, `GetNodesValidAt`, `DiffSnapshots`) could not observe that the node ever had the removed label. Fixed following the `UpdateNodeWithContext` version-bump pattern: capture `prevVersion`/`prevState` before mutation, call `PutNodeVersion(id, prevVersion, prevState)` to save the old state, bump `copy.SetVersion(prevVersion + 1)`, then call `RemoveNodeLabelToken`. Also corrected the hash chain to use `current.Integrity().Hash` (not `PrevHash`) as the new `PrevHash`, matching the chain in all other update paths. Stale comment removed from `badgerstore.go:RemoveNodeLabelToken`. The crash window was later closed by the atomic `RemoveNodeLabelTokenWithHistory` Store method.

- **Fix C — DiffSnapshots holds g.mu.RLock during O(N) materialization** (`pkg/graph/temporal.go`, BLOCKER): `DiffSnapshots` held `g.mu.RLock()` across two calls to `snapshotLocked`, each materialising ALL valid nodes/rels into RAM. All `g.mu.Lock()` callers (`BeginTx`, `Batch`, `Reset`) were blocked for the full O(N) duration. `GetNodesValidAt` uses `forEachKnownNodeID` which does its own store-level locking — `g.mu` is not required for correctness of individual snapshot reads. Fixed by removing `g.mu.RLock()` from `DiffSnapshots`. Renamed `snapshotLocked` → `snapshotAt` (no longer requires the caller to hold `g.mu`). `Snapshot()` continues to hold `g.mu.RLock()` for strong consistency. Trade-off documented: a concurrent backdated write that commits between the two reads may appear as a spurious Created/Deleted entry. The RAM concern was later addressed by `DiffSnapshotsCallback`; `DiffSnapshots` now delegates to that streaming path.

- **Fix D — searchNearest O(N log N) sort under RLock** (`pkg/graph/vector_index.go`, MAJOR): `searchNearest` allocated a `[]scored` of all N entries, sorted it in O(N log N) while holding `vi.mu.RLock()`, blocking concurrent vector insertions. Replaced with a max-heap of size k (`knnHeap` via `container/heap`) that runs in O(N log k) time. For k ≪ N the lock is held significantly shorter; for k=N behaviour is equivalent. The heap drains in ascending distance order (closest first) matching the previous sort contract. Removed `"sort"` import; added `"container/heap"`.

- **Fix E — checkTemporalConstraints allocates on every relationship write** (`pkg/graph/temporal_constraint.go`, MINOR): `g.constraints.Items()` copies the constraint slice on every relationship write (even with a single constraint). Added unexported `forEach(fn func(TemporalConstraint) error) error` method on `ConstraintSet` that iterates `cs.items` directly with zero allocation. `checkTemporalConstraints` updated to use `forEach`. Exported `Items()` retained for external callers.

- **Fix F — TOCTOU retry loop has no backoff** (`pkg/graph/context.go`, MINOR): The TOCTOU retry in `DeleteNodeWithContext` called `continue` immediately after `UnlockMany` on a failed attempt. Under sustained concurrent rel-add/remove to the same node, all 10 retries could exhaust without the competing goroutine making progress. Fixed by adding `runtime.Gosched()` after `UnlockMany`, yielding the processor to let the competing rel-writer commit. Added `"runtime"` to import block.

### Tests Added

- `TestRemoveNodeLabel_PreservesHistory` — verifies `GetNodeHistory` returns 1 entry after `RemoveNodeLabel`, version 0 retains the removed label (Fix B).
- `TestGraphTx_RollbackPanicSafe` — injects a store panic during rollback via `deleteRelPanicStore`; verifies `BeginTx` completes within 2s after recovery, confirming the graph write lock was released by the deferred unlock (Fix A).
- `TestDiffSnapshots_DoesNotBlockWrites` — runs 8 concurrent `DiffSnapshots` goroutines alongside 8 `BeginTx` goroutines; verifies all complete within 10s without deadlock (Fix C).
- `TestSearchNearest_HeapCorrectness` — verifies top-k results from the heap implementation match brute-force distances for k=1 (exact nearest), k=3 (set equality + non-decreasing order), and k=N (set equality + non-decreasing order, allowing ties within equal-distance groups) (Fix D).

## [3.0.56] - 2026-03-03

### Fixed (7 Production Defects — v3.0.56)

- **Fix #1 — Cold shard use-after-close** (`pkg/graph/tieredstore.go`): `shardForNodeID` returned a `*BadgerStore` pointer without incrementing `activeReqs`, creating a race with `closeIdleShards` that could panic on cold-shard Badger access. Fixed by adding `shardForNodeIDChecked(id snowflake.ID) (*BadgerStore, func(), error)` which wraps the shard resolution with `checkoutStore`/`checkinStore`. The returned `checkin` function is deferred at every callsite. All six read/write callsites updated: `GetNode`, `GetNodeHistory` (`tieredstore_read.go`), `DeleteNode`, `ReplaceNode`, `RemoveNodeLabelToken` (`tieredstore_write.go`). The refShard and refArchive return a no-op checkin (they are never closed by `closeIdleShards`).

- **Fix #2 — stripDepth drops pagination** (`pkg/graph/tieredstore_read.go`): `stripDepth` was returning `QueryOpts{Depth: stripped}` — a fresh struct that dropped `Limit`, `After`, and all temporal fields. Every per-shard call from `NodesByLabel`, `AllNodes`, and `AllRelationships` received `Limit=0` (unbounded), causing full-shard scans and potential OOM materialisation. Fixed by modifying `opts` in-place (`opts.Depth = 0; return opts`) so all other fields are preserved.

- **Fix #3 — PutNodesBatch / PutRelationshipsBatch no rollback** (`pkg/graph/tieredstore_write.go`): When the second-shard write in a batch failed, first-shard writes were committed, leaving orphaned entities in the store. Fixed by adding rollback: on hot-shard `PutNodesBatch` failure, each successfully written refShard node is removed via `DeleteNode`; on `PutRelationshipsBatch` failure, each completed shard batch is rolled back. Rollback errors are surfaced with the primary failure.

- **Fix #4 — idxMu held over disk I/O** (`pkg/graph/badgerstore.go`): `DeleteNode`, `ReplaceNode`, and `RemoveNodeLabelToken` called `idxMu.Lock()` then immediately `getNodeLocked(id)`, which on cache miss issued a synchronous Badger `db.View()` read under the global write lock — blocking all concurrent readers (including `NodesByLabel`, `ForEach`) for up to 5ms per operation under `SyncWrites=true`. Fixed by adding `prefetchNode(id snowflake.ID) (*types.Node, error)` helper that checks the cache under `RLock`, then reads from Badger without any lock, then re-verifies under `RLock`. All three methods call `prefetchNode` before acquiring `idxMu.Lock()`; the TOCTOU window is guarded by re-checking `nodeIDs[id]` under the write lock.

- **Fix #5 — ImportGraph holds lock during streaming I/O** (`pkg/graph/export.go`): `ImportGraph` previously acquired `g.mu.Lock()` at function entry and held it for the entire `io.Reader` streaming loop, freezing all graph operations (reads, writes, queries) for potentially minutes during large or networked imports. Fixed with a two-phase implementation: Phase 1 (no lock) buffers all records from the `io.Reader` into a `[]importRecord` slice; Phase 2 (under `g.mu.Lock()`) deserialises the buffer and applies store writes. I/O latency no longer extends the lock scope.

- **Fix #6 — AllNodeHistoryIDs / AllRelHistoryIDs OOM** (`pkg/graph/export.go`, `badgerstore.go`): Deferred at this point because fixing it required cursor-based history-ID scans across all Store implementations. This was later resolved by `AllNodeHistoryIDsFrom` / `AllRelHistoryIDsFrom` and the bounded-RAM export history loop.

- **Fix #7 — BackpressureDropOldest CPU livelock** (`pkg/graph/events.go`): The `BackpressureDropOldest` inner `for {}` loop had an empty `default:` branch — tight spinning under contention saturated a CPU core and starved the worker goroutine (livelock). Fixed by adding `runtime.Gosched()` in the default case. No cost on the uncontended path; yields the scheduler slot on contention.

### Tests Added

- `TestTieredStore_NodesByLabel_PaginationBounded` — verifies Limit/After are honoured across a multi-shard TieredStore query (fix #2).
- `TestAsyncEventBus_DropOldest_TerminatesQuickly` — 50×20 concurrent publishes on a full channel complete within 2s (fix #7).
- `TestTieredStore_PutNodesBatch_RollbackOnHotShardError` — verifies ref shard has no orphan node after hot-shard failure (fix #3).
- `TestBadgerStore_DeleteNode_NoDiskIOUnderWriteLock` — DeleteNode on a cache-miss node does not hold idxMu during db.View (fix #4).
- `TestImportGraph_DoesNotBlockReadsWhileStreaming` — concurrent GetNode succeeds during ImportGraph Phase 1 (fix #5).

All in `pkg/graph/v3056_fixes_test.go`.

## [3.0.55] - 2026-03-03

### Added (Atomic Delete with Tombstone History — Phase 4.23)

- **`RelTombstone`** — new struct in `pkg/graph/store.go` packaging a relationship's tombstone data for atomic delete operations: `ID snowflake.ID`, `PrevVersion uint32`, `Tombstone *types.Relationship` (pre-built deep copy with `DeletedAt`/`ValidTo`/`TxFrom`/`TxTo` set by the caller).
- **`Store.DeleteNodeWithHistory(id snowflake.ID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error`** — new interface method. Atomically combines `PutRelVersion×N` + `PutNodeVersion` + `DeleteNodeCascade` into a single storage transaction. Eliminates orphaned tombstone history entries that could result from a crash between the previous N+2 separate store calls. `relTombstones` may be nil when the node has no connected relationships.
- **`Store.DeleteRelWithHistory(id snowflake.ID, prevVersion uint32, tombstone *types.Relationship) error`** — new interface method. Atomically combines `PutRelVersion` + `DeleteRelationship` into a single storage transaction. Eliminates the crash window between the previous two separate calls.
- **`MemoryStore.DeleteNodeWithHistory` / `DeleteRelWithHistory`** — implemented under a single `ms.mu.Lock()`, ensuring history write and entity deletion are atomic with respect to concurrent readers.
- **`BadgerStore.DeleteRelWithHistory`** — serializes tombstone outside `idxMu`, acquires `idxMu.Lock()` once, calls `deleteRelByInfo` (queues cascade delete ops into `pending`), appends tombstone history op to the same `pending` map, releases lock, then flushes. All ops land in the same Badger `WriteBatch.Flush()` call — atomic.
- **`BadgerStore.DeleteNodeWithHistory`** — serializes all tombstones outside `idxMu`, acquires `idxMu.Lock()` once, calls `cascadeDeleteInner` (queues all cascade delete ops into `pending`), appends node + rel tombstone history ops to the same `pending` map before releasing the lock. Single `WriteBatch.Flush()` commits cascade + tombstone history atomically.
- **`cascadeDeleteInner`** — unexported helper extracted from `cascadeDeleteLocked`. Same body, but without `idxMu.Lock()/Unlock()`; it runs only inside paths that already hold the lock. Enables `DeleteNodeWithHistory` to extend the lock scope across cascade + tombstone append without a second lock acquisition.
- **`marshalNodeToBytes` / `marshalRelToBytes`** — unexported helpers in `badgerstore.go` wrapping `msgpack.Marshal(nodeToWire(n))` / `msgpack.Marshal(relToWire(r))`. Used by both existing `PutNodeVersion`/`PutRelVersion` paths and the new tombstone serialization in `DeleteNodeWithHistory`/`DeleteRelWithHistory`.
- **`TieredStore.DeleteRelWithHistory`** — delegates to the relationship's entity shard for tombstone + entity/typeIdx/outIdx ops. Cross-shard inIdx cleanup follows the same split-write pattern as `DeleteRelationship`; if the entity-shard tombstone/delete fails after the incoming leg was removed, the incoming leg is restored.
- **`TieredStore.DeleteNodeWithHistory`** — deletes connected relationships through `DeleteRelWithHistory`, then delegates the node tombstone to `shard.DeleteNodeWithHistory(id, prevNodeVersion, nodeTombstone, nil)`. Current code snapshots pre-call relationship state and restores already-deleted relationships if a later relationship delete or node tombstone write fails before the node row is removed.
- **`context.go:deleteNodeLocked`** rewritten — builds `[]RelTombstone` in one pass (dedup via `seen` map), builds node tombstone, calls single `g.store.DeleteNodeWithHistory`. Replaces the previous loop of `PutRelVersion` calls + `PutNodeVersion` + `DeleteNodeCascade` (N+2 separate store calls).
- **`context.go:DeleteRelationshipWithContext`** rewritten — calls single `g.store.DeleteRelWithHistory`. Replaces the previous `PutRelVersion` + `DeleteRelationship` (2 separate store calls).
- 6 tests in `pkg/graph/atomic_delete_test.go`: `TestDeleteRelWithHistory_HistoryAndLiveConsistent`, `TestDeleteRelWithHistory_NotFound`, `TestDeleteNodeWithHistory_HistoryAndLiveConsistent`, `TestDeleteNodeWithHistory_EmptyRelTombstones`, `TestDeleteNodeWithHistory_BadgerStore`, `TestDeleteNodeWithHistory_TieredStore`.

### Design Notes

- Atomicity guarantee (BadgerStore): holding `idxMu.Lock()` across both `cascadeDeleteInner` and tombstone `appendOps` prevents the background flush goroutine (which acquires `idxMu.RLock()`) from draining `pending` between the two phases. All ops land in one `WriteBatch.Flush()`.
- TieredStore keeps per-shard atomic batches and wraps cross-shard split writes in local rollback where the inverse operation is available; `RunRepair` remains the reconciliation path for externally corrupted or unrecoverable states.
- `DeleteNodeCascade`, `PutNodeVersion`, `PutRelVersion` are unchanged — they are still used by repair/migration tools (`tieredstore_repair.go`, `tieredstore_migrate.go`) which do not write tombstones.
- After deletion, `TieredStore.GetNodeVersion`/`GetRelVersion` cannot resolve the shard via the high-level routing (relies on live in-memory presence). Access the underlying shard directly (`ts.refShard.GetNodeVersion(...)`) for post-delete history verification — as demonstrated in `TestDeleteNodeWithHistory_TieredStore`.

## [3.0.54] - 2026-03-03

### Added (AllowSelfLoops Validation — Phase 4.22)

- **`ValidationLimits.AllowSelfLoops bool`** — new field on `ValidationLimits`. Default zero value (`false`) rejects self-loop relationships (where `startNode == endNode`). Set to `true` to permit them. This aligns rho/tkg/v3 with the tkg-2025-v2 and tkg-2026-v3 reference implementations.
- **`ErrSelfLoop`** — new graph-layer sentinel error: `"graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit"`. Returned by `AddRelationshipWithContext` and `ImportRelationshipWithID` when `startID == endID && !g.validation.AllowSelfLoops`.
- **`context.go:AddRelationshipWithContext`** — self-loop guard added after `endID` extraction, before `LockTwo`: rejects when `startID == endID && !g.validation.AllowSelfLoops`.
- **`context.go:ImportRelationshipWithID`** — same self-loop guard added at the equivalent position.
- 5 tests in `pkg/graph/self_loop_test.go`: `TestSelfLoop_ZeroValueRejects`, `TestSelfLoop_ExplicitFalseRejects`, `TestSelfLoop_AllowedByConfig`, `TestSelfLoop_ImportRejected`, `TestSelfLoop_DifferentNodesStillWork`.

### Fixed

- **`TestGraphDeleteNodeSelfLoopCascade`** (`pkg/graph/graph_test.go`) — updated to use `Config{Validation: ValidationLimits{AllowSelfLoops: true}}` since the default now rejects self-loops.
- **`TestEndpointHashSelfLoop`** (`pkg/graph/rel_endpoint_hash_test.go`) — same fix.

## [3.0.53] - 2026-03-03

### Fixed (Code Review Bugs — Phases 4.13–4.16)

- **`extractProvenance` silent truncation** (`pkg/graph/context.go`): Integer values of `tkg_auth_level` outside `[0, 255]` previously silently wrapped via modulo cast to `uint8`, corrupting the stored `AuthorizationLevel`. Unrecognised non-nil types (e.g. `string("5")`) silently stored 0. Fixed by adding bounds checks for all integer/float64 cases (`int`, `int32`, `int64`, `float64`) and a `default` case that returns an explicit error. The function signature gains a 6th `error` return; all 4 callers in `context.go` now propagate the error. All `#nosec G115` comments on the cast sites removed — bounds are now checked explicitly. 5 new tests: `TestExtractProvenance_OutOfBoundsInt`, `TestExtractProvenance_NegativeInt`, `TestExtractProvenance_OutOfBoundsFloat`, `TestExtractProvenance_InvalidType`, `TestExtractProvenance_ValidBoundary`.

- **`ImportGraph` registry mismatch** (`pkg/graph/export.go`): When importing into a graph whose label/reltype registry was already populated with different token mappings, `ErrRegistryNotEmpty` was swallowed and the import continued, silently assigning wrong labels and relationship types to all imported entities. Fixed by comparing the existing registry with the incoming one via `reflect.DeepEqual` when `ErrRegistryNotEmpty` is returned from `ImportNames`. Identical registries (idempotent re-import) continue without error; conflicting registries return the new sentinel `ErrIncompatibleRegistry`. 2 new tests: `TestImportGraph_IncompatibleLabelRegistry`, `TestImportGraph_CompatibleRegistryIdempotent`. Existing `TestImport_IdempotentRegistry` continues to pass.

- **`readExportRecord` DoS / OOM** (`pkg/graph/export.go`): The 4-byte length header in the export stream was trusted unconditionally. A crafted export file with `length = 4 GiB` caused an immediate OOM allocation before any data was read. Fixed by adding `maxExportRecordSize = 128 MiB` constant and a guard that returns an error before the `make([]byte, length)` call. 1 new test: `TestReadExportRecord_OversizeRecord`.

## [3.0.52] - 2026-03-03

### Added (HighFrequencyIndex — Phase 4.21)

- **`highFrequencyIndex`** — new time-bucketed index providing O(1) amortized insertion versus the sorted-slice `temporalIndex`'s O(log n). Designed for high-write-rate scenarios (thousands of event writes/sec into TieredStore event shards).
- **`newHighFrequencyIndex(bucketSize time.Duration, origin types.Instant) *highFrequencyIndex`** — constructor. `bucketSize` controls the time width of each bucket (e.g., `time.Hour`). `origin` sets the baseline for bucket 0.
- **`(*highFrequencyIndex).add(id snowflake.ID, validFrom types.Instant)`** — O(1) amortized insertion; bucket index = `(validFrom - origin) / bucketSize`. Thread-safe via internal `sync.RWMutex`.
- **`(*highFrequencyIndex).remove(id snowflake.ID, validFrom types.Instant)`** — O(n/num_buckets) amortized removal from a single bucket.
- **`(*highFrequencyIndex).pointQuery(t types.Instant) []snowflake.ID`** — returns all IDs in the bucket containing `t`. These are candidates for the graph/query filtering layer that checks full interval semantics.
- **`(*highFrequencyIndex).rangeQuery(start, end types.Instant) []snowflake.ID`** — returns all IDs in buckets overlapping `[start, end)`. Candidates — same re-filter note as `pointQuery`.
- **`(*highFrequencyIndex).bucketFor(validFrom types.Instant) int64`** — unexported helper; instants before `origin` map to negative bucket indices using correct floor division.
- **`Store.CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error`** — new method on the `Store` interface. Returns `ErrTemporalIndexExists` if any temporal index (sorted-slice or bucket) already exists for this label — only one type per label at a time.
- **`Store.DropHighFrequencyIndex(labelToken uint16) error`** — new method on the `Store` interface. Returns `ErrTemporalIndexNotFound` if no high-frequency index exists.
- **`MemoryStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — implemented; stored in a new `hfIndexes map[uint16]*highFrequencyIndex` field (separate from `temporalIndexes`).
- **`BadgerStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — implemented; stored in a new `hfIndexes map[uint16]*highFrequencyIndex` field under `idxMu`. The index is in-memory and `CreateHighFrequencyIndex` builds it from current store state.
- **`TieredStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — delegates across all active shards (ref + event) following the same pattern as `CreateTemporalIndex`. Current code tracks HFI definitions so rotated hot shards and lazily opened archives inherit them.
- **`Graph.CreateHighFrequencyIndex(label string, bucketSize time.Duration) error`** — public Graph API. Current behavior creates the label token if needed so the index applies to future matching nodes. Returns `ErrTemporalIndexExists` on conflict.
- **`Graph.DropHighFrequencyIndex(label string) error`** — public Graph API. Current behavior returns `ErrTemporalIndexNotFound` when the label or HFI is absent.
- 12 tests in `pkg/graph/hf_index_test.go`: `TestHFIndex_Add_PointQuery`, `TestHFIndex_RangeQuery`, `TestHFIndex_Remove`, `TestHFIndex_HighWriteRate` (10k concurrent adds, race-clean), `TestCreateHighFrequencyIndex_Graph`, `TestHFIndex_ReplacesTemporalIndex`, `TestHFIndex_DuplicateCreate`, `TestHFIndex_DropNotFound`, `TestHFIndex_ConflictsWithTemporalIndex`, `TestHFIndex_UnknownLabel`, `TestHFIndex_BucketFor_BeforeOrigin`, `TestHFIndex_RangeQuery_EmptyResult`.

### Design Notes

- HFI stores `validFrom` buckets and returns candidates for a filtering layer that checks full interval semantics.
- HFI entries are rebuilt in memory; BadgerStore persists label/bucket definitions, and TieredStore persists temporal/HFI tracking so restarted stores, rotated hot shards, and lazily opened archives keep the active HFI contract.
- Only one temporal index type can exist per label at a time: a `temporalIndex` and a `highFrequencyIndex` cannot coexist. Drop one before creating the other.
- `time.Duration` added to `store.go` imports (used by new interface methods).

## [3.0.51] - 2026-03-03

### Added (Event Priority Levels — Phase 4.20)

- **`EventPriority uint8`** — new type controlling delivery queue routing in `AsyncEventBus`. Five named constants (zero value = `PriorityNormal` for backward compatibility):
  - `PriorityNormal` (0) — default; all existing `Event{}` literals remain valid without change.
  - `PriorityHigh` (1) — create events (`EventNodeCreate`, `EventRelCreate`).
  - `PriorityCritical` (2) — delete/cascade events (`EventNodeDelete`, `EventRelDelete`).
  - `PriorityLow` (3) — available for caller-side lower-priority events.
  - `PriorityDeferred` (4) — available for caller-side background/analytics events.
- **`numPriorityLevels = 5`** — unexported constant sizing the per-priority queue array.
- **`Event.Priority EventPriority`** — new field on `Event`. Zero value is `PriorityNormal`; all existing `Event{}` struct literals compile unchanged (backward-compatible).
- **`AsyncEventBus.queues [numPriorityLevels]chan Event`** — replaces the single `queue chan Event` with one buffered channel per priority level. `QueueSize` is applied uniformly to each channel.
- **`priorityOrder [numPriorityLevels]EventPriority`** — package-level drain order array: `[Critical, High, Normal, Low, Deferred]`. Used by workers to implement best-effort priority ordering.
- **Per-priority `publish` routing** — `AsyncEventBus.publish` routes each event to `queues[e.Priority]`. Out-of-range priority values fall back to `PriorityNormal`. Backpressure strategies (`BackpressureBlock`, `BackpressureDropOldest`, `BackpressureDropLatest`) apply per-queue.
- **Priority-ordered worker drain** — workers perform a non-blocking check through `priorityOrder` before blocking on a multi-channel select. When multiple queues have events, higher-priority events are served first (best-effort; Go scheduler is non-deterministic).
- **`drainAll()`** — helper draining all per-priority queues in priority order on stop signal. Called from worker on `stopCh` closure (replaces the previous inline drain loop).
- **`Graph.publishEvent` signature updated** — fourth parameter `priority EventPriority` added. All 11 internal call sites updated with appropriate priorities:
  - `EventNodeCreate` / `EventRelCreate` → `PriorityHigh`
  - `EventNodeDelete` / `EventRelDelete` → `PriorityCritical`
  - `EventNodeUpdate` / `EventRelUpdate` (all paths including in-place, RemoveLabel, CloseVersion) → `PriorityNormal`
- 4 new tests in `pkg/graph/async_eventbus_test.go`: `TestPriority_ZeroValueIsNormal`, `TestPriority_GraphDeleteIsCritical`, `TestPriority_GraphCreateIsHigh`, `TestPriority_CriticalBeforeNormal`.

## [3.0.50] - 2026-03-03

### Added (Async EventBus with Worker Pool + Backpressure — Phase 4.19)

- **`eventPublisher` interface** — unexported interface with a single `publish(Event)` method. Both `*EventBus` (sync) and `*AsyncEventBus` (async) implement it. `Graph.events` field changed from `*EventBus` to `eventPublisher`, enabling transparent substitution.
- **`AsyncEventBus`** — asynchronous event bus that decouples handler latency from graph write latency. Handlers are invoked in a bounded worker pool, not on the caller's goroutine. A slow handler no longer stalls mutations.
- **`NewAsyncEventBus(cfg AsyncEventBusConfig) *AsyncEventBus`** — creates and starts the bus. Workers begin consuming immediately. Defaults: `Workers=1`, `QueueSize=256`.
- **`AsyncEventBusConfig`** — configuration struct: `Workers int` (goroutine count), `QueueSize int` (channel buffer), `Backpressure BackpressureStrategy` (full-queue behavior).
- **`BackpressureStrategy`** — enum with three values:
  - `BackpressureBlock` — blocks the caller until queue space is available (zero event loss, max back-pressure to writers).
  - `BackpressureDropOldest` — evicts the oldest queued event and enqueues the new one (preserves newest events under load).
  - `BackpressureDropLatest` — discards the incoming event when the queue is full (zero blocking, may lose events).
- **`AsyncEventBus.Subscribe(h EventHandler) func()`** — registers a handler; returns an idempotent unsubscribe closure (B11, `sync.Once`). Safe for concurrent use.
- **`AsyncEventBus.Close()`** — signals workers to stop, drains all pending queue entries before returning, then waits for all workers to exit. Safe to call multiple times (B11, `sync.Once`). Guarantees at-most-once delivery of all events enqueued before `Close()`.
- **`Graph.SetAsyncEventBus(bus *AsyncEventBus)`** — attaches an `AsyncEventBus`. Nil-safe (typed-nil guard prevents interface-wrapping a nil pointer from defeating the `g.events == nil` check in `publishEvent`).
- **`Graph.SetEventBus(bus *EventBus)`** — updated with explicit nil-guard (same typed-nil safety fix).
- **`Graph.GetEventBus() *EventBus`** — updated to type-assert against `eventPublisher` interface; returns `nil` when an `AsyncEventBus` is attached.
- **`dispatch` (internal)** — copies handler slice under `RLock` before invoking (B15 copy-outside-lock pattern). Uses `safeInvoke` — panics inside handlers are recovered and logged, never crashing the worker goroutine.
- 8 new tests in `pkg/graph/async_eventbus_test.go`: `TestAsyncEventBus_HandlerReceivesEvent`, `TestAsyncEventBus_SlowHandlerDoesNotBlockPublish`, `TestAsyncEventBus_BackpressureBlock`, `TestAsyncEventBus_BackpressureDropOldest`, `TestAsyncEventBus_BackpressureDropLatest`, `TestAsyncEventBus_Close_DrainsQueue`, `TestAsyncEventBus_MultipleWorkers`, `TestSetAsyncEventBus_GraphIntegration`.

## [3.0.49] - 2026-03-03

### Added (Transaction Time / Bitemporality — Phase 4.18)

- **`TemporalMetadata.TxFrom Instant`** is now populated on every write path. `AddNodeWithContext` and `AddRelationshipWithContext` set `TxFrom = nowInstant()` after hash computation (TxFrom/TxTo are NOT fed into `ComputeNodeHash`/`ComputeRelHash`). `UpdateNodeWithContext` and `UpdateRelationshipWithContext` set `TxFrom = now` on the new version (the same `now` used for `UpdatedAt`) and `TxTo = now` on the prevState deep-copy before it is written to history.
- **`TemporalMetadata.TxTo Instant`** is set to `now` on the tombstone created by `deleteNodeLocked` (node + connected rels) and `DeleteRelationshipWithContext`. Tombstones have both `TxFrom = now` and `TxTo = now` (committed and immediately superseded at the same instant, matching the deleted-entity contract).
- **`Graph.GetNodeAsOf(id, txTime)`** — returns the node version whose transaction time window covered `txTime`. Checks the current tip first (`TxFrom <= txTime && TxTo == 0`), then scans history for the highest-`TxFrom` version satisfying `TxFrom <= txTime && (TxTo == 0 || TxTo > txTime)`. Returns `ErrNoVersionAsOf` if no version was recorded at that time.
- **`Graph.GetRelAsOf(id, txTime)`** — mirrors `GetNodeAsOf` for relationships.
- **`Graph.GetNodesAsOf(txTime)`** — returns all nodes that existed in the graph at the given transaction time. Uses the two-phase ForEach pattern: Phase 1 collects all current + history node IDs through Store iteration; Phase 2 calls `GetNodeAsOf` per ID after the iterator returns. Skips `ErrNoVersionAsOf`.
- **`Graph.GetRelsAsOf(txTime)`** — mirrors `GetNodesAsOf` for relationships.
- **`ErrNoVersionAsOf`** — new sentinel: `"graph: no entity version recorded at the given transaction time"`.
- **`Config.SyncWrites bool`** — also now wired through to `BadgerStoreConfig.SyncWrites` in `New()` (pre-existing gap fixed alongside Phase 4.18).
- **Shadow resolver tests updated** — `TestResolveNodePropertyNilTemporal` and `TestResolveRelPropertyNilTemporal` now construct entities directly (bypassing `Add*`) to test the nil-temporal code path. `AddNode`/`AddRelationship` always set `TxFrom` after Phase 4.18, so the test must use manually constructed entities to exercise the nil branch.
- 8 new tests in `pkg/graph/txtime_test.go`: `TestTxFromSetOnAdd`, `TestTxToSetOnUpdate`, `TestTxToSetOnDelete`, `TestGetNodeAsOf_BeforeCreate`, `TestGetNodeAsOf_CurrentVersion`, `TestGetNodeAsOf_HistoricalVersion`, `TestGetNodesAsOf_FiltersCorrectly`, `TestGetRelAsOf`.

## [3.0.48] - 2026-03-03

### Added (Sync-Write Config Flag — Phase 4.17)

- **`BadgerStoreConfig.SyncWrites bool`** — when true, opens Badger with `WithSyncWrites(true)` (fsync-on-every-write at the disk level) and forces `FlushInterval=0` so the background flush goroutine is never started. Every mutating method calls `bs.flush()` immediately after releasing `idxMu`, persisting writes to stable storage before returning. Eliminates the 100ms async flush window at the cost of higher write latency. Ignored in ReadOnly mode.
- **`Config.SyncWrites bool`** — propagated to `BadgerStoreConfig.SyncWrites` when `New()` creates a `BadgerStore`. No-op when using `MemoryStore` or an explicitly injected `Store`.
- **`BadgerStore.syncWrites bool`** — unexported field on BadgerStore; checked after every `appendOps` call in all 16 mutating methods: `PutNode`, `DeleteNode`, `ReplaceNode`, `RemoveNodeLabelToken`, `PutRelationship`, `ReplaceRelationship`, `DeleteRelationship`, `ReplaceNodeWithHistory`, `ReplaceRelWithHistory`, `PutNodeVersion`, `PutRelVersion`, `DeleteNodeCascade`, `PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`.
- **Lock ordering preserved** — all `defer bs.idxMu.Unlock()` patterns in mutating methods were converted to explicit unlock before the sync flush call, maintaining the invariant: `idxMu` is released before `flush()` acquires `wbMu` then `idxMu.RLock()` internally (B15, lock-ordering rule).
- **B22 compliance** — flush path already guards with `bs.dbClosed.Load()`, so sync writes cannot hang on closed DB.
- 5 new tests in `pkg/graph/sync_write_test.go`: `TestSyncWrite_ConfigPassthrough`, `TestSyncWrite_DataSurvivesWithoutClose`, `TestSyncWrite_FlushIntervalIgnored_WhenSyncWrites`, `TestSyncWrite_ReadOnly_SyncWritesIgnored`, `TestSyncWrite_Graph_ConfigPassthrough`.

## [3.0.47] - 2026-03-03

### Added (AuthorizedBy + AuthorizationLevel — Phase 4.16)

- **`NodeIntegrity.AuthorizedBy string`** / **`NodeIntegrity.AuthorizationLevel uint8`** — optional caller-supplied authorization fields. Set by passing `"tkg_authorized_by"` (string) and `"tkg_auth_level"` (uint8 or int for JSON round-trip safety) in the `props`/`updates` map of any Add or Update call. Stripped before `PropertySlice.Set` (never stored in PropertySlice).
- **`RelIntegrity.AuthorizedBy string`** / **`RelIntegrity.AuthorizationLevel uint8`** — same pattern on relationship integrity.
- **`ShadowAuthorizedBy = "tkg_authorized_by"`** / **`ShadowAuthLevel = "tkg_auth_level"`** — new shadow constants in `pkg/types/shadow.go`. Both accessible via `ResolveNodeProperty` and `ResolveRelProperty`.
- **`extractProvenance(props)`** extended — now also extracts `tkg_authorized_by` and `tkg_auth_level` from any props/updates map. Zero-allocation fast path preserved (B23 compliant): no allocation when none of the 4 reserved keys are present. Accepts `int`, `int32`, `int64`, `float64` for `tkg_auth_level` for JSON round-trip compat.
- **Wire persistence** — `AuthorizedBy` (`msgpack:"aby"`) and `AuthorizationLevel` (`msgpack:"al"`) added to both `nodeWire` and `relWire`. Backward-compatible (`omitempty`).
- **Layout test updated** — `NodeIntegrity` size: 72 → 96 bytes; `RelIntegrity` size: 104 → 128 bytes.
- 10 new tests in `pkg/graph/integrity_authz_test.go`: `TestAuthorizedBySetOnAdd_Node`, `TestAuthorizedBySetOnAdd_Rel`, `TestAuthLevelSetOnAdd_Node`, `TestAuthLevelSetOnAdd_Rel`, `TestAuthLevelAcceptsInt_Node`, `TestAuthzPreservedOnUpdate`, `TestAuthzViaShadow_Node`, `TestAuthzViaShadow_Rel`, `TestAuthLevelViaShadow_Node`, `TestNoAuthz_DefaultsZero`.

## [3.0.46] - 2026-03-03

### Added (Portable Export/Import — Phase 4.15)

- **`Graph.ExportGraph(w io.Writer) error`** — writes a portable format-independent snapshot of the entire graph to `w`. Snapshot includes: header, label/reltype registries, all current nodes and rels, and their full version history. Holds `g.mu.RLock` for the duration (consistent snapshot).
- **`Graph.ImportGraph(r io.Reader) error`** — reads an export stream and restores it into the graph. Registries are imported if empty; if already populated, the existing registry is kept (idempotent). Holds `g.mu.Lock` for the duration (serialised restore).
- **Wire format** — length-prefixed msgpack record stream with 1-byte type tags: `0x01` header, `0x02` registry, `0x03` node, `0x04` node history, `0x05` rel, `0x06` rel history. Each record is `[tag(1)] [len(4BE)] [msgpack body]`. Unknown tags are rejected with `ErrCorruptExport`.
- **`ErrIncompatibleExport`** — returned when the export stream version is not supported by this binary.
- **Two-phase ForEach pattern (C4)** — collect IDs in ForEachNodeID/ForEachRelID/ForEachNodeHistoryID/ForEachRelHistoryID callbacks; fetch entities after callback returns. Current Store implementations invoke callbacks outside backend locks and shard checkouts. OOM-safe on large graphs.
- 12 new tests in `pkg/graph/export_test.go`: `TestExportImport_RoundTrip_MemoryStore`, `TestExportImport_RoundTrip_BadgerStore`, `TestExport_Empty_Graph`, `TestExport_WithNodeHistory`, `TestExport_RelHistory`, `TestImport_IdempotentRegistry`, `TestExport_Writer_Error`, `TestImport_InvalidHeader`, `TestExportImport_IntegrityPreserved`, `TestExportImport_EndpointHashesPreserved`, `TestExportImport_AuthorIDPreserved`, `TestExport_ShadowProperty_Survives`.

## [3.0.45] - 2026-03-03

### Added (AuthorID + Signature on Integrity — Phase 4.14)

- **`NodeIntegrity.AuthorID string`** / **`NodeIntegrity.Signature []byte`** — caller-supplied provenance fields. Set by passing `"tkg_author_id"` (string) and `"tkg_signature"` ([]byte) in the `props`/`updates` map of any Add or Update call. Stripped before `PropertySlice.Set` (never stored in PropertySlice).
- **`RelIntegrity.AuthorID string`** / **`RelIntegrity.Signature []byte`** — same pattern on relationship integrity.
- **`ShadowAuthorID = "tkg_author_id"`** / **`ShadowSignature = "tkg_signature"`** — new shadow constants in `pkg/types/shadow.go`. Both accessible via `ResolveNodeProperty` and `ResolveRelProperty`.
- **`extractProvenance(props)`** (unexported) — helper in `context.go` that extracts `tkg_author_id` and `tkg_signature` from any props/updates map without mutating the caller's map. Zero-allocation fast path when neither key is present.
- **Wire persistence** — `AuthorID` (`msgpack:"aid"`) and `Signature` (`msgpack:"sig"`) added to both `nodeWire` and `relWire`. Backward-compatible (`omitempty`); old data reads as zero values.
- **Layout test updated** — `NodeIntegrity` size: 32 → 72 bytes; `RelIntegrity` size: 32 → 104 bytes.
- 11 new tests in `pkg/graph/integrity_author_test.go`: SetOnAdd (node + rel), SignatureSetOnAdd (node + rel), PreservedOnUpdate, ViaShadow (node + rel, both fields), DefaultsEmpty, DoesNotAffectHash.

## [3.0.44] - 2026-03-03

### Added (RelIntegrity Endpoint Hashes — Phase 4.13)

- **`RelIntegrity.FromNodeHash string`** — hash of the start node at the time this relationship version was written. NOT fed into `ComputeRelHash` (prevents cascading hash invalidation on node updates). Used for cross-validation.
- **`RelIntegrity.ToNodeHash string`** — hash of the end node at write time.
- **`ShadowFromHash = "tkg_from_hash"`** / **`ShadowToHash = "tkg_to_hash"`** — new shadow constants. Accessible via `ResolveRelProperty`; return `(nil, false)` on nodes (rel-only).
- **`AddRelationshipWithContext`** — captures `startNode.Integrity().Hash` → `ig.FromNodeHash` and `endNode.Integrity().Hash` → `ig.ToNodeHash` under the endpoint lock. Empty string if endpoint has no integrity.
- **`UpdateRelationshipWithContext`** — refreshes `FromNodeHash`/`ToNodeHash` from the store on each update, capturing the current endpoint hashes at write time.
- **Wire persistence** — `FromNodeHash` (`msgpack:"fnh"`) and `ToNodeHash` (`msgpack:"tnh"`) added to `relWire`. Backward-compatible (`omitempty`).
- 5 new tests in `pkg/graph/rel_endpoint_hash_test.go`: `TestFromNodeHashStoredOnAdd`, `TestEndpointHashFromShadow`, `TestEndpointHashPreservedOnUpdate`, `TestEndpointHashSelfLoop`, `TestEndpointHashNotOnNode`.

## [3.0.43] - 2026-03-02

### Added (VectorField Index — Phase 4.9)

- **`DistanceMetric uint8`** — `DistanceCosine` and `DistanceEuclidean` constants.
- **`vectorIndex`** (unexported) — in-memory brute-force k-NN index: `add`, `remove`, `searchNearest`. O(n × dims) per query. Thread-safe via `sync.RWMutex`. `add` replaces existing entry for same ID (upsert).
- **`[]float32` property support** — added to `wire.go` (`ptSliceF32 = 24`), `integrity.go` (hash computation), and `propertyslice.go` (deep copy). `[]float32` values are now fully round-trip serializable.
- **`Store.CreateVectorIndex(labelToken, propertyKey, dims, metric)`** — creates in-memory k-NN index on nodes with the given label. Scans existing nodes to populate. Returns `ErrVectorIndexExists` on duplicate and `ErrInvalidVectorIndexConfig` for `dims <= 0` or unsupported distance metrics. Implemented in MemoryStore, BadgerStore, TieredStore.
- **`Store.DropVectorIndex(labelToken, propertyKey)`** — removes the index. Returns `ErrVectorIndexNotFound` if not present.
- **`Store.SearchNearestNodes(labelToken, propertyKey, query, k, opts)`** — returns the k closest nodes by vector distance in ranked order. Returns `ErrVectorIndexNotFound` / `ErrDimensionMismatch` on error; nil slice (no error) if index is empty.
- **`Graph.CreateVectorIndex(label, propertyKey, dims, metric)`** / **`DropVectorIndex`** / **`SearchNearestNodes`** — Graph-layer API resolving label string to token. Current `CreateVectorIndex` creates the label token if needed so the index applies to future matching nodes.
- **Auto-maintenance** — all mutation paths (PutNode, ReplaceNode, DeleteNode, RemoveNodeLabelToken) update vector indexes in MemoryStore, BadgerStore, and TieredStore.
- **`ErrVectorIndexExists`** / **`ErrVectorIndexNotFound`** / **`ErrDimensionMismatch`** / **`ErrInvalidVectorIndexConfig`** — vector sentinel errors canonical in `pkg/graph/store` and aliased by the `graph` and `pkg/graph/index` packages.
- **TieredStore** holds vector indexes at the store level (not per-shard) with its own `vectorIdxMu sync.RWMutex`.
- **Persistence shape** — vector entries are in-memory; current BadgerStore and TieredStore behavior persists index definitions and rebuilds entries from node properties on open.
- Internal-package tests in `vector_badger_test.go` covering BadgerStore and TieredStore implementations directly (12 tests). External-package tests in `vector_index_test.go` (12 tests).

## [3.0.42] - 2026-03-02

### Added (Recurrence Patterns — Phase 4.7)

- **`RecurrenceFrequency uint8`** — `RecurrenceDaily`, `RecurrenceWeekly`, `RecurrenceMonthly`, `RecurrenceYearly`.
- **`WeekdayMask uint8`** — bit-per-weekday bitmask: `MaskMonday` (bit 0) through `MaskSunday` (bit 6), plus `MaskWeekdays`, `MaskWeekend`, `MaskAllDays` composites.
- **`Interval`** — `{Start, End Instant}` — closed-open `[Start, End)` temporal interval.
- **`RecurrencePattern`** — struct with `Frequency`, `Days` (WeekdayMask), `DayOfMonth` (1–28; 0 = last day of month), `Month` (time.Month for Yearly), `DayStart`/`DayEnd` (whole-millisecond time.Duration from UTC midnight).
- **`RecurrencePattern.Validate()`** — validates frequency, non-empty Days for Daily/Weekly, supported weekday-mask bits only, DayStart < DayEnd with whole-millisecond offsets, DayOfMonth ∈ [0, 28], and Yearly Month ∈ [January, December] or 0.
- **`RecurrencePattern.Expand(from, to)`** — walks days from `TruncateInstant(from, GranDay)` to `TruncateInstant(to, GranDay)`, checks day-of-week / day-of-month / month match, emits `[day+DayStart, day+DayEnd)` clipped to `[from, to)`. All calculations UTC. Returns `types.ErrInvalidTimeRange` if `from >= to`.
- 8 direct tests in `pkg/types/recurrence_test.go`: Daily_Weekdays, Weekly_Monday, Monthly_NthDay, Yearly, Clipped, EmptyResult, Validate_Errors (7 sub-cases), Expand_InvalidRange.

## [3.0.41] - 2026-03-02

### Added (Remove Label from Node — Phase 4.10)

- **`Node.RemoveLabelTokenRaw(tok uint16) bool`** — removes a label token from the node's label set. If `tok` is the primary label, promotes `extraLabels[0]` to primary. Returns false if `tok == 0`, not present, or would remove the last remaining label.
- **`Store.RemoveNodeLabelToken(id, tok, updatedNode)`** — removes `tok` from the label index for `id` and persists `updatedNode` (no version bump, no history entry). Implemented in MemoryStore, BadgerStore, and TieredStore.
- **`Graph.RemoveNodeLabel(id snowflake.ID, label string)`** — resolves label string to token, locks the entity, validates the node has the label and has more than one label, deep-copies + mutates, recomputes hash (preserving PrevHash), delegates to `store.RemoveNodeLabelToken`, publishes `EventNodeUpdate`, increments `opNodeUpdates`.
- **`ErrLabelNotFound`** — new sentinel: `"graph: node does not have the specified label"`.
- **`ErrLastLabel`** — new sentinel: `"graph: cannot remove the last label from a node"`.
- 8 new tests in `pkg/graph/remove_label_test.go`: ExtraLabel, PrimaryPromotesExtra, LastLabelError, LabelNotFoundError, NodeNotFoundError, HashUpdated, NodesByLabelUpdated, PublishesEvent.
- Internal-package tests in `vector_badger_test.go` covering BadgerStore and TieredStore `RemoveNodeLabelToken` directly.

## [3.0.40] - 2026-03-02

### Added (Time Granularity + In-Place Update + Graph Stats — Phases 4.8, 4.11, 4.12)

**Time Granularity (4.8)**
- **`TimeGranularity uint8`** — 8 levels: `GranMillisecond` (1) through `GranYear` (8).
- **`TruncateInstant(t, g)`** — floors `t` to the nearest `g` boundary (UTC). Week truncation floors to Monday midnight.
- **`RoundInstant(t, g)`** — rounds to nearest boundary (ties ceil).
- **`CeilInstant(t, g)`** — smallest boundary ≥ `t`.
- Table-driven tests in `pkg/types/granularity_test.go` covering all 3 functions × 8 granularities, plus on-boundary and week-day edge cases.

**In-Place Update (4.11)**
- **`Graph.UpdateNodeInPlace(id, updates)`** / **`UpdateNodeInPlaceWithContext`** — updates node properties without bumping the version or writing a history entry. Uses `store.ReplaceNode` (not `ReplaceNodeWithHistory`). Preserves existing `PrevHash`. Publishes `EventNodeUpdate`. Increments `opNodeUpdates`.
- **`Graph.UpdateRelInPlace(id, updates)`** / **`UpdateRelInPlaceWithContext`** — rel mirror.
- 12 new tests in `pkg/graph/inplace_test.go`: NoHistoryEntry, VersionUnchanged, PropertiesUpdated, NoOp, PublishesEvent (× Node/Rel), WithContext_Cancelled, CountedAsUpdate.

**Graph Stats (4.12)**
- **`GraphStats`** — struct with 8 operation counters (`NodesAdded`, `NodesRead`, `NodesUpdated`, `NodesDeleted`, `RelsAdded`, `RelsRead`, `RelsUpdated`, `RelsDeleted`) and 4 cache metrics (`NodeCacheHits`, `NodeCacheMisses`, `RelCacheHits`, `RelCacheMisses`).
- **`StoreStats`** (unexported interface) — optional interface type-asserted in `Graph.Stats()`. Avoids polluting the `Store` interface. `BadgerStore` implements it.
- **8 `atomic.Int64` fields** on `Graph` struct: `opNode{Adds,Reads,Updates,Deletes}` + rel mirrors. Incremented after every successful store write in `context.go`.
- **LRU hit/miss tracking** — `entityLRU` gains `hits` and `misses atomic.Int64`. `Get()` increments on cacheMiss (miss) and on cacheHit/cacheDeleted (hit). `Hits()`/`Misses()` accessors.
- **`BadgerStore` implements `StoreStats`** via `nodeCache.Hits()`/`Misses()` + rel mirrors.
- 8 new tests in `pkg/graph/stats_test.go`: InitialState, NodeCounters, RelCounters, EmptyUpdate_NoUpdateIncrement, CacheMetrics_MemoryStore_Zero, CacheMetrics_BadgerStore, UpdateNodeInPlace_CountsAsUpdate, UpdateRelInPlace_CountsAsUpdate.

## [3.0.39] - 2026-03-02

### Added (CRUD Diff Exporter — Phase 4.6)

- **`NodeUpdate`** / **`RelUpdate`** — pair structs holding `Before` and `After` snapshots of a changed entity.
- **`SnapshotDiff`** — result type with `T1`, `T2`, `NodesCreated`, `NodesUpdated`, `NodesDeleted`, `RelsCreated`, `RelsUpdated`, `RelsDeleted`.
- **`Graph.DiffSnapshots(t1, t2)`** — compares two temporal snapshots under a single `g.mu.RLock` (prevents torn reads). Returns `*SnapshotDiff` classifying each entity as Created (present only at t2), Deleted (present only at t1), or Updated (hash changed). Unchanged entities are omitted. Returns `ErrInvalidTimeRange` if `t1 >= t2` or either is zero.
- **`ErrInvalidTimeRange`** — new sentinel error in `graph` package.
- **`snapshotLocked`** (unexported) — inner body of `Snapshot(t)` extracted without the lock, allowing `DiffSnapshots` to hold the RLock across both snapshot reads (B15 compliance: no nested RLock).
- 15 new tests in `pkg/graph/diff_test.go`: invalid range, empty graph, created/deleted/updated/unchanged for nodes and rels, mixed scenario, nil integrity branches.

## [3.0.38] - 2026-03-02

### Added (Event / Notification System — Phase 4.5)

- **`EventType uint8`** — 6 constants: `EventNodeCreate`, `EventNodeUpdate`, `EventNodeDelete`, `EventRelCreate`, `EventRelUpdate`, `EventRelDelete`.
- **`Event`** — struct with `Type EventType`, `EntityID snowflake.ID`, `Timestamp types.Instant`.
- **`EventHandler`** — type alias for `func(Event)`.
- **`EventBus`** — dispatcher with `Subscribe(handler) func()` (returns idempotent unsubscribe via `sync.Once`) and unexported `publish(e Event)`. Handlers are copied under `RLock`, then invoked outside the lock to prevent deadlocks when handlers re-enter the Graph.
- **`NewEventBus()`** — constructor.
- **`Graph.SetEventBus(bus)`** / **`Graph.GetEventBus()`** — attach/retrieve the event bus. Nil by default (zero overhead for callers not using events).
- 6 hook points wired in `context.go` after each successful store write: `AddNode`→`EventNodeCreate`, `AddRelationship`→`EventRelCreate`, `UpdateNode`→`EventNodeUpdate`, `UpdateRelationship`→`EventRelUpdate`, `DeleteNode`→`EventNodeDelete`, `DeleteRelationship`→`EventRelDelete`.
- `CloseNodeVersion` / `CloseRelVersion` also publish `EventNodeUpdate` / `EventRelUpdate`.
- 13 new tests in `pkg/graph/events_test.go`: subscribe/unsubscribe, idempotent unsubscribe, multiple handlers, nil-default graph, all 6 CRUD event types, async handler, CloseNodeVersion/CloseRelVersion events.

## [3.0.37] - 2026-03-02

### Added (Version Chain Navigation — Phase 4.4)

- **`Graph.GetPreviousNodeVersion(id, version)`** — returns the version immediately before `version`. Returns `nil, nil` if `version == 0` (genesis has no predecessor) or the predecessor does not exist in history.
- **`Graph.GetNextNodeVersion(id, version)`** — returns the version immediately after `version`. Checks the history store first for `version+1`, then falls back to the current entity (which may itself be `version+1`). Returns `nil, nil` if no newer version exists (current tip or deleted node with a version gap).
- **`Graph.CloseNodeVersion(id, t)`** — sets `ValidTo = t` on the current node in-place via `ReplaceNode` (no new version, no history entry). Recomputes the integrity hash preserving `PrevHash`. Returns `ErrAlreadyClosed` if `ValidTo` is already non-zero; returns `ErrNodeNotFound` if the node does not exist. Updates temporal indexes via `ReplaceNode`.
- **`GetPreviousRelVersion`** / **`GetNextRelVersion`** / **`CloseRelVersion`** — exact mirrors of the node methods for relationships.
- **`ErrAlreadyClosed`** — new sentinel error in `graph` package.
- 18 new tests in `pkg/graph/version_chain_test.go`: genesis/tip boundaries, normal prev/next traversal, through-history path, deleted node/rel edge cases, version gap after truncation, CloseNodeVersion sets ValidTo, ErrAlreadyClosed on second close, ErrNodeNotFound on missing entity, rel mirrors.

## [3.0.36] - 2026-03-02

### Added (Temporal Constraints + Advanced Temporal Indexes — Phases 4.2 + 4.3)

**Temporal Constraints (4.2)**

- **`TemporalConstraintKind`** — enum type for constraint kinds. Initial kind: `ConstraintRelWithinEndpoints`.
- **`TemporalConstraint`** — struct binding a `TemporalConstraintKind` to optional parameters.
- **`ConstraintSet`** — value type (zero value = no constraints). Holds a slice of `TemporalConstraint`. Passed via `Graph.Config`; zero overhead when unused.
- **`ConstraintRelWithinEndpoints`** — enforces that a relationship's validity interval (`[ValidFrom, ValidTo)`) is a subset of the intersection of both endpoint nodes' validity intervals. Evaluated in `AddRelationshipWithContext` and `ImportRelationshipWithID` before the store write.
- **`ErrTemporalConstraint`** — base sentinel error; 6 specific leaf errors wrap it so callers can use `errors.Is` on either the outer or the specific leaf: `ErrConstraintStartNodeOpen`, `ErrConstraintEndNodeOpen`, `ErrConstraintRelBeforeStart`, `ErrConstraintRelAfterEnd`, `ErrConstraintRelStartTooEarly`, `ErrConstraintRelEndTooLate`.
- 13 new tests in `pkg/graph/temporal_constraint_test.go`: no-op on zero ConstraintSet, valid rel within both endpoints, rel starts before node, rel ends after node, open-ended node (no ValidTo), import path enforcement, errors.Is wrapping for each leaf error, interaction with existing temporal filters.

**Advanced Temporal Indexes (4.3)**

- **`temporalIndex`** (unexported) — sorted-slice interval index on `[ValidFrom, ValidTo)`. Binary search insertion (O(log n)); point-in-time and interval queries are O(n) scan with early exit. Thread-safe via `sync.RWMutex`. Stored per label token on the Store.
- **`Store.CreateTemporalIndex(labelToken)`** — installs a temporal index for the given label; scans existing nodes to populate. Returns `ErrTemporalIndexExists` on duplicate. Implemented in MemoryStore, BadgerStore, and TieredStore.
- **`Store.DropTemporalIndex(labelToken)`** — removes the temporal index. Returns `ErrTemporalIndexNotFound` if not present.
- **`Graph.CreateTemporalIndex(label)`** / **`Graph.DropTemporalIndex(label)`** — Graph-layer API resolving label string to token before delegation.
- **`NodesByLabel` temporal fast path** — when a temporal index is active for the queried label and `QueryOpts` carries a `ValidAt` or `ValidStart`/`ValidEnd` filter, `NodesByLabel` uses the index to narrow candidate IDs before fetching entities, avoiding a full label scan.
- **BadgerStore persistence** — temporal index label tokens are persisted to Badger under a meta key (same 3-phase creation pattern as property indexes). On startup `loadIndexes()` reads persisted token set and rebuilds index data by scanning matching nodes; indexes survive restart.
- **TieredStore delegation** — `CreateTemporalIndex`/`DropTemporalIndex` delegate to all currently open shards. `tempIndexLabels` set on `TieredStore` ensures each new hot shard created during rotation inherits all active temporal indexes immediately.
- **MemoryStore** — full integration: index created/dropped/maintained across all node mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`).
- **`ErrTemporalIndexExists`** / **`ErrTemporalIndexNotFound`** — new sentinel errors in `graph` package.
- 11 new unit tests in `pkg/graph/temporal_index_test.go` + helper tests: create/duplicate/drop/not-found, NodesByLabel fast path (point-in-time, interval), BadgerStore persistence round-trip, TieredStore delegation, new hot shard inherits index on rotation, MemoryStore mutation maintenance.

### Fixed

- **B22: Badger `WriteBatch.Flush` blocks on closed DB** — `flush()` in `badgerstore.go` now checks `bs.closed` (atomic flag) before calling `WriteBatch.Flush()`. Previously, a flush triggered after `Close()` would block indefinitely because Badger's `WriteBatch.Flush` waits on an internal channel that is never drained once the DB is closed. Fix: early-return with `ErrDBClosed` when the closed flag is set; re-queue is skipped. `flushLoop` goroutine exit path updated accordingly.

## [3.0.35] - 2026-03-02

### Fixed (Carry-Forward Coverage Gaps)

- **`ImportNames` validation** (`label_registry.go`, `reltype_registry.go`) — added rejection of whitespace-only and empty strings at positions > 0, and duplicate names. Uses `strings.TrimSpace` to match the invariant enforced by `GetOrCreate`. Previously, importing `["", "Foo", "Foo"]` silently corrupted the reverse map; importing `["", " ", "Foo"]` mapped token 1 to a whitespace-only string that `GetOrCreate` would refuse to produce, creating an inconsistent registry.
- **`propertyTypeTag` coverage** — direct unit tests covering all 24 branches: `int8`, `int16`, `int32`, `uint`, `uint8`–`uint32`, `uint64`, `float32`, and the `default` unknown-type branch (64% → 100%).
- **`toInt64` / `toUint64` coverage** — direct unit tests covering all 9 type cases plus the `default` zero-return branch each (54.5% → 100%).
- **`normalizeIntegersRecursive` coverage** — direct unit tests for `int8`, `int16`, `int32`, `uint8`, `uint16`, `uint32`, `[]any` recursion, `map[string]any` recursion, and the `default` passthrough (40% → 100%).
- **`flush()` WriteBatch error path** (`badgerstore.go`) — test closes the underlying Badger DB before `WriteBatch.Flush()`, exercising the requeue-on-error path with correct goroutine lifecycle via `t.Cleanup` (73% → covered).

## [3.0.34] - 2026-03-02

### Added (Allen's 13 Interval Relations — Gap A1)

- **`types.AllenRelation`** — `uint8` iota (1..13) representing Allen's 13 interval relations: Before, After, Meets, MetBy, Overlaps, OverlappedBy, Starts, StartedBy, During, Contains, Finishes, FinishedBy, Equals. Zero value is invalid (catches uninitialized usage). Methods: `String()`, `Symbol()`, `Inverse()`, `Set()`.
- **`types.AllenRelationSet`** — compact `uint16` bitset for sets of relations. Methods: `Contains`, `Add`, `Union`, `Intersection`, `IsEmpty`, `Len`, `ToSlice`, `String`, `InverseSet`. `AllRelations()` returns the full 13-element set.
- **`types.Relate(aStart, aEnd, bStart, bEnd)`** — classifies the Allen relation between two `[start, end)` intervals. Returns `ErrOpenInterval` for zero endpoints, `ErrInvalidInterval` for start >= end.
- **`types.Compose(r1, r2)`** / **`types.ComposeSets(a, b)`** — composition table (computed at `init()` via exhaustive enumeration of 21 intervals over a 7-point timeline). Returns the set of possible relations when chaining two known relations.
- **`graph.NodeInterval(n)`** / **`graph.RelInterval(r)`** — extracts effective `[start, end)` from a node/relationship, deriving start from snowflake ID timestamp when no explicit `ValidFrom` is set. Returns `ErrOpenInterval` if `ValidTo == 0`.
- **`graph.RelateNodes(a, b)`** / **`graph.RelateRels(a, b)`** — classifies Allen relation between two entities via their effective intervals.
- **`ErrOpenInterval`** / **`ErrInvalidInterval`** — new sentinel errors in `pkg/types`.
- 38 new tests in `pkg/types/allen_test.go`: error cases, all 13 relations, inverse symmetry, string/symbol, set operations, composition identity/known results/never-empty/singleton consistency.
- 10 new tests in `pkg/graph/temporal_allen_test.go`: open-ended error propagation, resolved intervals, snowflake-derived start, all 13 relations through Graph, inverse consistency, relationship intervals.

### Notes

- Purely additive — no existing files modified, no existing tests affected.
- Foundation for temporal constraints (A2) and advanced temporal indexes (A7).

## [3.0.33] - 2026-03-02

### Fixed (Pre-Release Code Review — 1 BLOCKER + 10 MAJORs + 16 MINORs)

**BLOCKER:**
- **checkoutStore TOCTOU race** — `activeReqs` increment moved inside `shardMu` for cold shards, eliminating race window between `getStore` return and ref-count increment that allowed `closeIdleShards` to close an in-use store.

**MAJORs:**
- **Atomic file persistence** — `atomicWriteFile` helper with `fsync` before rename in `shard_catalog.go` and `registry_file.go`. Prevents crash-induced corruption of catalog and registry files.
- **Registry save race** — new `SaveRegistries(labels, relTypes)` writes both registries atomically in a single file operation, eliminating read-modify-write race between `SaveLabelRegistry` and `SaveRelTypeRegistry`.
- **ShardCatalog thread safety** — added `sync.RWMutex` to `ShardCatalog`. All read methods take RLock, all write methods take Lock. `GetShard`/`HotEventShard` return copies instead of internal pointers.
- **append backing array corruption** — replaced `append(outRels, inRels...)` with explicit allocation in `context.go` and `tx.go` to prevent writing into spare capacity of the `outRels` slice.
- **ReplaceNode property index cleanup** — added `purgeNodeFromAllPropertyIndexes` fallback in `badgerstore.go` when `getNodeLocked` fails during Replace, preventing orphaned property index entries.
- **Float formatting precision** — replaced `fmt.Sprintf("%v")` with `strconv.FormatFloat` for float32/float64 in `property_index.go`, ensuring deterministic round-trip-safe index keys.
- **NodesByLabel event label fan-out** — `NodesByLabel`, `NodesByLabelAndProperty`, and `NodeCountByLabel` now fan out across all event shards (respecting `opts.Depth`), not just the hot shard.
- **Constructor warm shard leak** — added cleanup loop in `NewTieredStore` to close already-opened warm shard stores when a subsequent warm shard fails to open.
- **TieredStore.Close() error accumulation** — replaced `closeErr = fmt.Errorf(...)` with `errors.Join` so all close errors are reported, not just the last one.

**MINORs:**
- Removed unused `_ = attempt` variable in `context.go` retry loop
- `result.Failed += 1` → `result.Failed++` in `batch.go`
- Documented `Execute` return pattern and thread-safety in `batch.go`
- Defined `ErrNotTieredStore` sentinel, replaced 7 ad-hoc `fmt.Errorf` strings in `graph.go`
- Added `slog.Error` in `persistPropertyIndexDefs` for marshal failures
- Distinguished `ErrNodeNotFound` from real errors in property scan fallback
- Moved test-only `contains()` from `property_index.go` to `property_index_test.go`
- Removed redundant `archiveWritten`/`refWritten` guard variables in `tieredstore_write.go`
- Fixed `tkg_version` comment: `int` → `uint32` in `pkg/types/shadow.go`
- Documented `ValidStart+ValidEnd` both-required for interval filtering in `temporal_filter.go`
- Added `panic("unreachable")` in `writePropertyValue` default case in `integrity.go`
- Rewrote `MigrateFromBadger` to use `ForEachNodeID`/`ForEachRelID` pagination instead of materializing all entities

## [3.0.32] - 2026-03-02

### Added

- **`ImportNodeWithID(ctx, id, labels, props)`** — creates a node with a caller-specified snowflake ID. Returns `ErrNodeExists` if the ID is already in use, `ErrZeroID` if id == 0. Used for backup/restore where ID preservation is required.
- **`ImportRelationshipWithID(ctx, id, typeName, startNode, endNode, props)`** — creates a relationship with a caller-specified snowflake ID. Returns `ErrRelExists` on collision.
- **`GraphTx.ImportNodeWithID` / `GraphTx.ImportRelationshipWithID`** — transaction wrappers for both import methods, tracked for rollback.
- **`ErrZeroID`** — new sentinel error for zero ID validation in import methods.
- 8 new tests in `import_test.go`: basic, collision, zero ID, validation, rel import, tx commit, tx rollback.

## [3.0.31] - 2026-03-02

### Added (OOM Fix — Lazy ForEach Iterators for Temporal Pipeline)

- **`ForEachNodeID(fn)` / `ForEachRelID(fn)`** — lazy iterator over all current entity IDs. Callback returns `true` to continue, `false` to stop. Implemented on MemoryStore (map iteration under RLock), BadgerStore (index map iteration under `idxMu.RLock`), and TieredStore (sequential shard iteration with checkout/checkin — one shard open at a time).
- **`ForEachNodeHistoryID(fn)` / `ForEachRelHistoryID(fn)`** — lazy iterator over all entity IDs with version history entries. BadgerStore: pending buffer scan + Badger prefix scan with dedup. TieredStore: sequential shard iteration.
- **`forEachKnownNodeID` / `forEachKnownRelID`** — two-phase temporal helpers replacing `allKnownNodeIDs`/`allKnownRelIDs`. Phase 1: collect unique IDs via ForEach callbacks into `seen`. Phase 2: process IDs after Store iteration returns.

### Changed

- **`GetNodesValidAt`**, **`GetRelationshipsValidAt`**, **`GetNodesValidDuring`**, **`GetRelationshipsValidDuring`** — rewritten to use `forEachKnownNodeID`/`forEachKnownRelID` instead of materializing all IDs into slices.
- **`Snapshot`** — benefits transitively (calls `GetNodesValidAt` + `GetRelationshipsValidAt`).

### Removed

- **`allKnownNodeIDs`** / **`allKnownRelIDs`** — replaced by `forEachKnownNodeID`/`forEachKnownRelID`.

### Memory Impact

- Eliminates per-shard `[]snowflake.ID` slices and `mergeIDSlices` allocations in the temporal query pipeline. For 10M nodes across 12 shards, reduces peak memory from ~928 MB to ~160 MB (~83% reduction for the ID collection phase).

### Tests

- 20 new tests: MemoryStore ForEach (node IDs, early stop, empty, rel IDs, node history, rel history), BadgerStore ForEach (node IDs, early stop, rel IDs, node history with dedup, node history early stop, rel history), TieredStore ForEach (all shards, early stop, with rotation, rel IDs, rel ID early stop, node history, rel history), Graph-level temporal query integration.

## [3.0.30] - 2026-03-02

### Fixed (5 Concurrency & Data Consistency Bugs)

- **`idleCloseLoop` race condition** — `getStore()` returned a `*BadgerStore` pointer and released `shardMu`, allowing `closeIdleShards()` to close the store while callers were still using it. Added `checkoutStore()` / `checkinStore()` with `atomic.Int64` `activeReqs` per shard. `closeIdleShards()` now skips shards with `activeReqs > 0`. Applied to all 10 parallel merge goroutines and 3 sequential count methods.
- **`shardForRelID` unnecessary cold shard probing** — fallback probe opened ALL event shards including cold ones. Cross-shard rels are only created on hot/warm shards, so cold probing is unnecessary and expensive. Fallback now skips `TierCold` shards.
- **`ArchiveNode`/`RestoreNode` missing rollback** — if step 5 (write to destination) partially succeeded then failed, or step 6 (delete from source) failed, data was duplicated across both shards. This release added destination-shard cleanup on failure; current Unreleased code supersedes it with relationship-placement rollback and surfaced rollback errors.
- **`CreatePropertyIndex` concurrent delete resurrection** — Phase 3 used `liveIdx.contains(id)` which returned false after a concurrent delete (ID removed from all value buckets), causing Phase 3 to re-add the stale backfill value. Added `propertyIndex.mutated` dirty-map: `add()` and `remove()` track all mutated IDs during Phase 2. Phase 3 checks `mutated[id]` instead of `contains(id)`.
- **`BatchBuilder.AddNode` hash mismatch** — hashed raw user-supplied labels (potentially with duplicates) instead of canonical deduplicated labels from the registry. `VerifyNodeHashChain` later used canonical labels, causing permanent verification failure. Now uses `b.g.NodeLabels(n)` for hash computation.

### Changed

- **`eventShard` struct** — new field `activeReqs atomic.Int64` for reference counting cold shard access. New methods `checkoutStore()` / `checkinStore()`.
- **`propertyIndex` struct** — new field `mutated map[snowflake.ID]struct{}` (non-nil only during index creation Phase 2).

### Tests

- 12 new tests: idle-close blocked by active request, concurrent checkout/checkin during idle-close, shardForRelID skips cold shards, shardForRelID finds in warm shard, ArchiveNode/RestoreNode rollback (2), CreatePropertyIndex concurrent delete/update (2), BatchBuilder duplicate label hash, BatchBuilder hash chain verification.

## [3.0.29] - 2026-03-02

### Added (Phase 3e — Repair + Tooling)

- **`DecomposeID(snowflake.ID)`** — extracts `IDComponents{CreatedAt, NodeID, Sequence}` from snowflake ID using `snowflakeLayout.Decompose()`. Package-level function, also accessible via `Graph.DecomposeID`.
- **`TieredStore.ForceRotate()`** — safe hot-shard rotation with internal locking (unlike `RotateHotShard()` which expects the caller to hold `ts.mu.Lock`). Accessible via `Graph.ForceRotate()`.
- **`TieredStore.ListShards()`** — returns `[]ShardInfo` for all shards (reference, archive, event), enriched with live node/rel counts from open stores. Accessible via `Graph.ListShards()`.
- **`TieredStore.RebuildCatalog()`** — reconstructs the shard catalog from live in-memory state, updating node/rel counts and tier info for all open shards. Accessible via `Graph.RebuildCatalog()`.
- **`TieredStore.VerifyShard(g, shardName)`** — runs hash chain verification on all entities in a named shard. For immutable shards (warm/cold) that have already passed verification, returns the cached result without re-scanning. Caches successful results in the catalog. Accessible via `Graph.VerifyShard(name)`.
- **`TieredStore.RunRepair()`** — cross-shard split-write consistency repair. Phase 1: detects orphaned in/ entries (entity missing from all shards) and deletes them. Phase 2: detects missing in/ entries (entity exists but in/ missing in end shard) and re-creates them. Returns `RepairResult` with counts. Accessible via `Graph.RunRepair()`.
- **`MigrateFromBadger(src, dst, labels)`** — copies all nodes and relationships from a single BadgerStore into a TieredStore with automatic ontology-based routing. No history migration (hash chains would need re-creation).
- **`ErrEventPropertyIndex`** — sentinel error returned when `CreatePropertyIndex` is called for an event label in TieredStore. Property indexes are only supported for reference entities.
- **`ShardInfo`** struct — describes a shard for admin queries: `Name`, `Kind`, `Tier`, `TimeStart`, `TimeEnd`, `Nodes`, `Rels`, `Open`, `Verified`.
- **`VerifyResult`** struct — holds per-shard hash chain verification outcome: `ShardName`, `NodesOK`, `RelsOK`, `NodesFailed`, `RelsFailed`, `Cached`.
- **`RepairResult`** struct — holds repair scan outcome: `OrphanedInEntries`, `MissingInEntries`, `ShardsScanned`, `CrossShardRelsChecked`.
- **`deleteIncomingByRelID`** on BadgerStore — removes an orphaned in/ entry by scanning for matching relID when relType and startID are unknown (entity is gone). Scans pending buffer first, falls back to Badger prefix scan.
- **`UpdateShardVerified`** / **`UpdateShardStats`** on ShardCatalog — field updates for verification caching and catalog rebuild.
- **~29 new tests** — ID decomposition (known values, time precision, node field, temporal filter consistency), property index restriction (ref label, event rejected, errors.Is), catalog extensions, admin API (ForceRotate, ListShards initial/after rotation/with cold/live stats, RebuildCatalog, admin not tiered), per-shard verification (hot, immutable cached, unknown shard), repair (no orphans, orphaned incoming, missing incoming, via Graph), migration (empty, nodes only, with rels, cross-shard rel).

## [3.0.28] - 2026-03-02

### Added (Phase 3d — Cold Shard Lifecycle, Parallel Queries, Reference Archive)

- **Cold shard lazy-open** — `eventShard.getStore(ts)` opens cold shards on first access with per-shard `shardMu` mutex and `atomic.Int64` `lastAccess` tracking. Cold shards are NOT opened on startup (recovered from catalog with `store=nil`).
- **Idle-close goroutine** — `idleCloseLoop()` periodically checks cold shards and closes those idle longer than `IdleTimeout` (default 5min when `ColdAfter > 0`), reclaiming memory.
- **`ColdAfter`** / **`IdleTimeout`** config — `ColdAfter` sets the warm→cold demotion threshold (0=never, non-negative). `IdleTimeout` sets cold shard auto-close delay (0=disabled/defaulted when cold is enabled; positive values must be >=1ms).
- **Parallel shard queries** — 10 merge query methods (`AllNodes`, `AllRelationships`, `AllNodeIDs`, `AllRelIDs`, `NodeCount`, `RelationshipCount`, `NodeCountByLabel`, `RelCountByType`, `AllNodeHistoryIDs`, `AllRelHistoryIDs`) launch concurrent goroutines per event shard via `sync.WaitGroup`. Reference shard runs sequentially first.
- **Reference archive** (`refArchive`) — lazy-opened BadgerStore at `data/archive/` for archiving closed/inactive reference entities.
- **`Graph.ArchiveNode(id)`** — moves a reference node and all connected relationships from `refShard` to `refArchive`. Returns `ErrNotReferenceEntity` for event entities.
- **`Graph.RestoreNode(id)`** — moves an archived reference node and relationships back to `refShard`.
- **`shardForNodeID` archive fallback** — node lookup probes `refArchive` when `refShard` misses, enabling transparent reads of archived entities.

### Changed

- **`TieredStore.Close()`** — now closes `refArchive` if open, signals `closeCh` to stop idle-close goroutine.
- **`eventShardSnapshot(depth)`** — returns depth-filtered `[]*eventShard` under `mu.RLock` for merge queries.

## [3.0.27] - 2026-03-02

### Added (Phase 3a — TieredStore Infrastructure)

- **`TieredStore`** — new `Store` implementation routing entities across multiple BadgerStore instances by ontology classification. Reference entities (configured via `RefLabels`) go to a single reference shard; event entities go to time-windowed event shards. Shard resolution is O(1) via snowflake ID timestamp extraction (bits 22–62).
- **`TieredStoreConfig`** — `DataDir`, `InMemory`, `RefLabels`, `ShardWindow` (default 1 week), `CacheCapacity` (default 10K), `FlushInterval` (default 100ms), `ColdAfter` (warm→cold demotion threshold, non-negative), `IdleTimeout` (default 5min when `ColdAfter > 0`; positive values must be >=1ms).
- **`OntologyMapping`** / **`EntityClass`** — classifies labels as `ClassReference` (long-lived) or `ClassEvent` (time-windowed, default). Lazy token cache backed by label registry. Token 0 returns `ClassEvent`.
- **`ShardCatalog`** / **`ShardEntry`** / **`ShardKind`** / **`ShardTier`** — JSON-persisted catalog tracking all shards with atomic write (write-tmp + rename). Tracks time windows, labels, rel types, tier (hot/warm/cold), kind (reference/event/archive). `UpdateShardTier`, `UpdateShardTimeEnd`, `HotEventShard`, `EventShards`, `ColdEventShards`.
- **`ShardDepth`** type — `DepthAll` (0, default), `DepthHot` (1), `DepthWarm` (2). Controls which shard tiers are included in TieredStore merge queries. `QueryOpts.Depth` field. Ignored by MemoryStore/BadgerStore (backward-compatible).
- **`BadgerStoreConfig.ReadOnly`** — opens Badger with `WithReadOnly(true)`, skips flushLoop and gcLoop. Used by TieredStore for warm/cold shards.
- **`registry_file.go`** — flat msgpack registry file save/load with atomic rename (write-tmp + rename). Used by TieredStore for label/reltype registry persistence separate from BadgerStore's in-DB persistence.
- **`badgerstore_partial.go`** — unexported helpers on `*BadgerStore` for TieredStore cross-shard relationship routing: `putRelEntityAndOut` (entity+typeIdx+outIdx), `putRelIncoming` (inIdx only), `deleteRelEntityAndOut`/`deleteRelIncoming` (split delete), `hasNodeID`/`hasRelID` (O(1) existence), `incomingRelIDs`/`outgoingRelIDs` (sorted ID snapshots).
- **`Graph.ArchiveNode(id)`** / **`Graph.RestoreNode(id)`** — move reference nodes and connected relationships between ref shard and ref archive. TieredStore only.
- **`ErrNotReferenceEntity`** sentinel error.

### Added (Phase 3b+3c — Shard Rotation, Warm Recovery, Cold Tier)

- **`TieredStore.RotateHotShard()`** — demotes current hot shard to warm (flush, mark read-only, ms-aligned boundary), creates new hot shard with contiguous window. Handles same-window collision via disambiguating suffix.
- **`checkRotation()`** on all new-entity write paths — fast-path time comparison (~1ns), slow-path Lock + double-check + rotate.
- **Warm shard recovery** — constructor reopens warm event shards from catalog as ReadOnly BadgerStore on restart. Mid-window restart via catalog `HotEventShard` resolution.
- **Cold shard support** — shards older than `ColdAfter` are demoted to `TierCold` during rotation. Cold shards are NOT opened on startup (lazy-open on first access via `eventShard.getStore()`). Idle-close goroutine reclaims memory by closing cold shards after `IdleTimeout`.
- **Depth-aware reads** — merge queries (`AllNodes`, `AllRelationships`, `AllNodeIDs`, `AllRelIDs`, `RelationshipsByType`, counts, history IDs) use `eventShardSnapshot(opts.Depth)` under `mu.RLock` to filter shard tiers.
- **Cross-shard relationships** — `PutRelationship` split-write with shard-based routing (`shardForNodeID`, not class-based) for correct E→E cross-shard after rotation. E→R: ref-first in/ per §12. `DeleteRelationship` split-delete. `DeleteNodeCascade` cross-shard aware. `IncomingRelationships` fetches each rel entity via `shardForRelID`.
- **Parallel shard queries** — merge queries launch concurrent goroutines per event shard.
- **~50 new tests** — ontology classification, shard catalog CRUD + persistence, registry file save/load/atomic, badgerstore partial helpers (split write/delete/existence/adjacency), TieredStore end-to-end (ref+event routing, rotation, warm recovery, cross-shard rels, counts, archive/restore, depth filtering, cold shards, idle-close).

### Changed (Phase 3a — GraphTx Full CRUD)

- **`GraphTx`** — upgraded from create-only to full CRUD with snapshot-based rollback. New methods: `UpdateNode`, `UpdateRelationship`, `SetNodeProperty`, `DeleteNodeProperty`, `SetRelationshipProperty`, `DeleteRelationshipProperty`, `DeleteNode` (cascade), `DeleteRelationship`. Rollback restores pre-mutation state in reverse order: deleted rels → deleted nodes → updated rels → updated nodes → created rels → created nodes. Current Unreleased code also restores pre-transaction history snapshots and truncates history for entities created then rolled back.
- **`Graph.Close()`** — type switch now handles `*TieredStore` in addition to `*BadgerStore` for registry persistence.
- **`Graph.New()`** — wires `TieredStore.SetLabelRegistry` and loads registries when Store is `*TieredStore`.

## [3.0.26] - 2026-03-02

### Fixed (Concurrency & Integrity — 3 Bugs)

- **`CreatePropertyIndex` concurrent data loss** — rewrote 3-phase approach: Phase 1 now installs an empty live index under write Lock (not RLock) so concurrent `PutNode`/`ReplaceNode` writes are captured immediately via `addNodeToPropertyIndexes`. Phase 2 builds backfill outside lock. Phase 3 merges backfill into live index, skipping IDs already handled by concurrent writes and deleted nodes. Previously, writes between Phase 1 (RLock) and Phase 3 (Lock) were silently dropped.
- **`ComputeNodeHash` hashed raw user labels** — `AddNodeWithContext` now calls `ComputeNodeHash(n, g.NodeLabels(n))` using canonical deduplicated labels from the node's internal tokens. Previously hashed the raw `labels` slice which could contain duplicates (e.g., `["Person", "Person"]`), causing `VerifyNodeHashChain` to fail because verification resolves canonical labels `["Person"]`.
- **`VerifyNodeHashChain`/`VerifyRelHashChain` failed on deleted entities** — both methods now tolerate `ErrNodeNotFound`/`ErrRelNotFound` for the current entity. When current is nil but history exists, the chain is built from history alone and labels/type name are extracted from the last history entry (tombstone). Returns `ErrNodeNotFound`/`ErrRelNotFound` only when neither current nor history exists.

### Added

- **`propertyIndex.contains(id)`** — O(V) check if a node ID exists in any value bucket. Used during `CreatePropertyIndex` merge phase to avoid overwriting concurrent writes.
- **6 new tests** — duplicate-label hash verification, duplicate-label deduplication, deleted entity hash chain verification (node + rel), never-existed hash chain (node + rel), concurrent `CreatePropertyIndex` with simultaneous writes, `propertyIndex.contains` unit test.

## [3.0.25] - 2026-03-02

### Added (Phase 2i — Temporal Query Push-Down + Graph Transactions)

- **Temporal push-down to Store layer** — `QueryOpts` gains `ValidAt types.Instant` (point-in-time filter) and `ValidStart`/`ValidEnd types.Instant` (interval filter). Zero values = no filter (backward-compatible). Both MemoryStore and BadgerStore filter entities at the persistence layer before deep-copy, dramatically reducing allocations for temporal queries.
- **`temporal_filter.go`** — package-level helpers `entityValidFrom(id, tm)` (derives valid-from from explicit `ValidFrom` or snowflake ID bit extraction) and `matchesTemporalFilter(id, tm, opts)` (evaluates point-in-time or interval overlap). Used by both Store implementations.
- **`entityLRU.Peek(key)`** — returns cached value and status without deep-copy or MRU promotion. Used by BadgerStore temporal pre-filter for zero-allocation cache-hit checks.
- **`entityLRU.Cap()`** — returns capacity. Used by `BadgerStore.Clear()` to recreate caches.
- **`GraphTx`** — mutation transaction holding graph write lock for duration. `Graph.BeginTx()` acquires write lock. `AddNode`/`AddRelationship` delegate to Graph and track IDs. `Commit()` releases lock. `Rollback()` deletes created entities in reverse order via `store.Delete*` (no tombstones — rolled-back creates vanish). `CreatedNodeIDs()`/`CreatedRelIDs()` for inspection. All methods return `ErrTxDone` after Commit/Rollback. (Later upgraded to full CRUD in v3.0.27.)
- **`Graph.Reset()`** — acquires write lock, calls `store.Clear()`, preserves registries. For atomic graph clearing.
- **`Store.Clear()`** — removes all entities, indexes, history, counters. MemoryStore reinitializes all maps. BadgerStore resets indexes, counters, caches, pending buffer, then calls `db.DropAll()`.
- **`ErrTxDone`** sentinel error — returned by GraphTx methods after Commit/Rollback.
- **~45 new tests** — 9 temporal filter unit tests, 4 LRU Peek/Cap tests, 12 MemoryStore/BadgerStore temporal push-down tests (ValidAt, pagination, AllNodes, NodesByLabelAndProperty, RelationshipsByType, interval), 12 GraphTx tests (commit/rollback/double-commit/double-rollback/add-after-done/concurrent/empty), 4 Reset tests (empty/clears entities/preserves registries/clears history), 4 Store.Clear tests.

### Changed

- **`GetNodesByLabelValidAt`** — now pushes `ValidAt` into `store.NodesByLabel(tok, QueryOpts{ValidAt: t})` instead of materializing all label matches and filtering in Go.
- **`NodesByLabelPropertyAndTime`** / **`NodesByLabelPropertyDuring`** — push temporal filters into `store.NodesByLabelAndProperty` via QueryOpts.
- **MemoryStore** — 5 paginated methods (`NodesByLabel`, `RelationshipsByType`, `AllNodes`, `AllRelationships`, `NodesByLabelAndProperty`) now apply temporal filtering before deep-copy using `matchesTemporalFilter` on in-memory entity pointers.
- **BadgerStore** — 5 paginated methods use two-stage filtering: `Peek` pre-filter for zero-allocation cache-hit temporal checks, then post-filter cache-miss candidates after `GetNode`/`GetRelationship`.

## [3.0.24] - 2026-03-02

### Added (Phase 2f — Cursor-Based Pagination)

- **`QueryOpts` struct** — `Limit int` (max results, 0 = no limit) and `After snowflake.ID` (keyset cursor, 0 = from start). Zero values mean "return all" for backward compatibility.
- **`paginateIDs` helper** (`pagination.go`) — shared binary-search cursor over sorted `snowflake.ID` slices, used by both MemoryStore and BadgerStore.
- **~27 pagination tests** — 8 `paginateIDs` unit tests, 8 MemoryStore integration (limit, multi-page walk, zero opts, indexed/fallback property query), 8 BadgerStore integration (mirrored), 3 Graph-layer passthroughs. All pass with race detector.

### Added (Phase 2g — Combined Label+Property+Temporal Queries)

- **`NodesByLabelPropertyAndTime(label, key, value, t)`** — intersects property index results with point-in-time temporal filter in a single call. Returns nodes matching all three axes.
- **`NodesByLabelPropertyDuring(label, key, value, start, end)`** — intersects property index results with interval temporal filter. Returns nodes valid during the given range.
- **7 combined query tests** — found (all axes match), property mismatch, temporal mismatch, unregistered label, with property index, interval overlap, no overlap.

### Changed

- **`Store` interface** — 5 unbounded query methods now accept `QueryOpts` parameter: `NodesByLabel(token, opts)`, `RelationshipsByType(token, opts)`, `AllNodes(opts)`, `AllRelationships(opts)`, `NodesByLabelAndProperty(token, key, value, opts)`.
- **`MemoryStore`** — 5 methods refactored to sort IDs before fetch, paginate via `paginateIDs`, then deep-copy only the paginated subset. For `Limit=100` on a 1M-node label, goes from 1M deep copies to 100.
- **`BadgerStore`** — 5 methods refactored with pagination. `NodesByLabelAndProperty` lock scope fixed: changed from holding `idxMu.RLock` during entity I/O to snapshot-and-release pattern. Fallback scan path applies cursor skip first, then early-stops when limit reached.
- **`Graph` layer** — 5 passthrough methods gain `QueryOpts` parameter: `NodesByLabel`, `RelationshipsByType`, `AllNodes`, `AllRelationships`, `NodesByLabelAndProperty`.
- **Internal callers** — `temporal.go` internal methods pass `QueryOpts{}` (zero = return all). All tutorials updated.

## [3.0.23] - 2026-03-02

### Fixed (Phase 2 Review — 6 Issues)

- **Hash chain verification truncation resilience** — `VerifyNodeHashChain`/`VerifyRelHashChain` now detect genesis by `entry.Version() == 0` instead of `i == 0`. After `TruncateNodeHistory`, the oldest chain entry may not be genesis; the old `i == 0` check caused verification to permanently return false. Non-genesis entries at chain position 0 (truncated history) now skip the PrevHash link check while still verifying content hash integrity.
- **`GetNodeAt` truncation resilience** — version start time derivation now checks `entry.Version() == 0` instead of `i == 0`. After truncation, the first entry in the chain may be a non-genesis version whose validity should start at `UpdatedAt`, not at snowflake creation time.
- **`CreatePropertyIndex` lock scope** — rewrote to 3-phase approach: (1) RLock to check existence + snapshot IDs, (2) fetch node data outside any lock via public `GetNode`, (3) write Lock to install index with double-check for concurrent creation. Previously held `idxMu.Lock` during Badger I/O, blocking all concurrent reads/writes. Non-`ErrNodeNotFound` errors are now propagated instead of silently swallowed.
- **Property index persistence** — index definitions now survive BadgerStore restart. Definitions are serialized to `0x0F/prop_indexes` meta key via msgpack on create/drop. `loadIndexes()` reads definitions back and rebuilds index data by scanning matching nodes. Previously, indexes were lost on restart, silently degrading `NodesByLabelAndProperty` to O(N) scan.
- **Delete preserves version history** — all delete paths (`DeleteNode`, `DeleteRelationship`, `DeleteNodeCascade`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`) no longer erase 0x07/0x08 history entries. `DeleteNodeWithContext`/`DeleteRelationshipWithContext` now save tombstone versions (with `DeletedAt`/`ValidTo` set) for all affected entities before deletion. This preserves the temporal history tape for past-time queries. Removed `deleteHistoryByPrefix` function entirely.
- **Temporal queries are history-aware** — `GetNodesValidAt`, `GetRelationshipsValidAt`, `GetNodesValidDuring`, `GetRelationshipsValidDuring`, and `Snapshot` now include deleted entities that were valid at the queried time. Previously, these methods only scanned current tip versions via `AllNodes()`/`AllRelationships()`, making deleted nodes invisible to temporal queries.

### Added

- **`GetRelAt(id, t)`** — returns the version of a relationship valid at instant `t`. Mirrors `GetNodeAt` for relationships. Handles deleted entities via history chain reconstruction.
- **`AllNodeHistoryIDs()`** / **`AllRelHistoryIDs()`** — new Store interface methods returning IDs of all entities with version history entries (including deleted entities whose history was preserved). Implemented in both MemoryStore and BadgerStore (BadgerStore scans both pending buffer and persisted 0x07/0x08 keys).
- **~19 new tests** — hash chain verification after truncation (node + rel), GetNodeAt after truncation, deleted entity temporal queries (GetNodeAt deleted/after-deletion, GetNodesValidAt deleted/updated, GetRelAt basic/deleted/not-found, GetRelationshipsValidAt deleted, Snapshot includes deleted, GetNodesValidDuring deleted, GetRelationshipsValidDuring deleted), BadgerStore AllHistoryIDs (node/rel with pending buffer tests), Badger-backed temporal query integration tests.

### Changed

- **`Store` interface** — added `AllNodeHistoryIDs() ([]snowflake.ID, error)` and `AllRelHistoryIDs() ([]snowflake.ID, error)`.
- **`GetNodeAt`** — now handles deleted entities (tolerates `ErrNodeNotFound`, builds chain from history only). Refactored version resolution into `resolveNodeVersionAt`/`nodeVersionBounds` helpers.
- **`DeleteNodeCascade`** — simplified to single-phase (preflight + apply). Removed Phase 3 (history cleanup) since history is now preserved.
- **`context.go`** — `DeleteNodeWithContext` saves tombstone versions for all connected relationships and the node before cascade delete. `DeleteRelationshipWithContext` saves tombstone version before delete.
- **`keys.go`** — added `propIndexDefsKey` meta key for property index definition persistence.

## [3.0.22] - 2026-03-02

### Added (Phase 2e — Configurable Validation Limits)

- **`ValidationLimits` struct** on `Graph.Config` — configurable limits for graph operations: `MaxLabelsPerNode` (default 50), `MaxPropertiesPerEntity` (default 1000), `MaxPropertyKeyLength` (default 256), `MaxPropertyValueSize` (default 65536, strings nested inside property values), `MaxNameLength` (default 256, label and reltype names). Zero values resolve to defaults in `New()`.
- **5 sentinel errors** — `ErrTooManyLabels`, `ErrTooManyProperties`, `ErrKeyTooLong`, `ErrValueTooLarge`, `ErrNameTooLong` (all in `graph.go`).
- **Validation enforcement** in all 4 `WithContext` mutation methods (`AddNodeWithContext`, `AddRelationshipWithContext`, `UpdateNodeWithContext`, `UpdateRelationshipWithContext`) and all 4 `BatchBuilder` mutation methods (`AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`). Update methods use two-phase validation: pre-lock entry checks + post-mutation `MaxPropertiesPerEntity` under entity lock.
- **`PropertyCount()`** on `Node` and `Relationship` — returns `properties.Len()` without deep copy. Used by update-path post-mutation property count validation.
- **~30 new tests** — defaults, zero-uses-defaults, custom limits, boundary tests (at-limit succeeds, one-over fails) for all 5 limits across AddNode/AddRelationship/UpdateNode/UpdateRelationship, batch mirroring. All sentinel errors tested with `errors.Is`.

### Changed (Phase 2d upgrade — Per-Label/Per-Type Statistics O(1))

- **`Store` interface** — added `NodeCountByLabel(token uint16) (int, error)` and `RelCountByType(token uint16) (int, error)` for O(1) per-label/per-type counting at the Store level.
- **`MemoryStore`** — O(1) via `len(labelIdx[token])` / `len(typeIdx[token])` under existing RWMutex.
- **`BadgerStore`** — `sync.Map` of `*atomic.Int64` counters, maintained incrementally in 9 mutation sites (`PutNode`, `DeleteNode`, `PutRelationship`, `deleteRelByInfo`, `PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `cascadeDeleteLocked` normal + corruption paths). Counters rebuilt from index sizes in `loadIndexes()` — no new Badger keys.
- **Graph layer** — `NodeCountByLabel`, `RelCountByType`, `AllLabelCounts`, `AllRelTypeCounts` now delegate to Store-level O(1) methods instead of materializing all entities via `NodesByLabel`/`RelationshipsByType`. `AllLabelCounts`/`AllRelTypeCounts` use `uint16(i)` as token directly.
- **18 new tests** — 8 MemoryStore counter tests, 8 BadgerStore counter tests (including persistence round-trip verifying counters rebuilt from indexes), 2 graph-level integration tests (batch add, cascade delete).

### Added (Phase 2c — Property Indexes)

- **`CreatePropertyIndex(label, propertyKey)`** — creates an in-memory index on a property for a given label. Scans existing nodes to populate the index. Returns `ErrIndexExists` if already defined.
- **`DropPropertyIndex(label, propertyKey)`** — removes a property index. Returns `ErrIndexNotFound` if missing.
- **`NodesByLabelAndProperty(label, key, value)`** — O(1) indexed lookup of nodes matching a label+property value. Falls back to scan if no index is defined.
- **`propertyValueKey(v any)`** — type-prefixed canonical string for safe cross-type value comparison (`"s:Alice"`, `"i:42"`, `"f64:3.14"`, `"b:true"`). Only primitives are indexed; complex types (maps, slices) return `""`.
- **Auto-update hooks** — property indexes are automatically maintained across all 7 node mutation paths: `PutNode`, `DeleteNode`, `ReplaceNode`, `ReplaceNodeWithHistory`, `PutNodesBatch`, `DeleteNodesBatch`, `DeleteNodeCascade`.
- **24 new tests** — 8 MemoryStore (create/duplicate/drop/not-found/hit/miss/no-index-fallback/auto-update), 8 BadgerStore (mirrored), 8 Graph-layer (end-to-end: create/drop/found/not-found/unregistered-label/multiple-values/update-reflected/delete-removes). Plus `TestPropertyValueKey_AllTypes` (table-driven, all 14 type branches + 3 non-indexed types).
- **`ErrIndexExists`** / **`ErrIndexNotFound`** sentinel errors in `store.go`.

### Changed

- **`Store` interface** — added 3 property index methods (`CreatePropertyIndex`, `DropPropertyIndex`, `NodesByLabelAndProperty`). Both MemoryStore and BadgerStore implement them.

## [3.0.21] - 2026-03-02

### Added (Phase 2a — Temporal Queries)

- **`GraphSnapshot`** struct — represents the complete graph state at a point in time (`Timestamp`, `Nodes`, `Relationships`, `NodeCount`, `RelCount`).
- **`GetNodesValidAt(t)`** — returns all nodes valid at instant `t`. Nodes without explicit temporal metadata derive valid-from from snowflake ID timestamp and are treated as open-ended.
- **`GetRelationshipsValidAt(t)`** — returns all relationships valid at instant `t`.
- **`GetNodesByLabelValidAt(label, t)`** — returns nodes with the given label that are valid at `t`.
- **`GetNodesValidDuring(start, end)`** — returns nodes whose validity overlaps `[start, end)`.
- **`GetRelationshipsValidDuring(start, end)`** — returns relationships whose validity overlaps `[start, end)`.
- **`GetNodeAt(id, t)`** — returns the version of a node that was valid at `t`. Builds the full version chain (history + current), computes validity periods from `UpdatedAt` timestamps, with explicit `ValidFrom`/`ValidTo` overrides. Returns `ErrNoVersionValidAt` if no version covers `t`.
- **`GetNeighborsValidAt(nodeID, t)`** — returns neighbor nodes reachable via relationships valid at `t`, where the neighbors themselves are also valid at `t`.
- **`Snapshot(t)`** — returns a `GraphSnapshot` at instant `t`. Relationships are only included if both endpoints are in the valid node set (no dangling rels).
- **31 new tests** — 12 point-in-time, 6 interval (including open-ended rels), 5 version-specific, 3 neighbor, 5 snapshot.
- **`ErrNoVersionValidAt`** sentinel error in `store.go`.

### Changed

- **No Store interface change** — all temporal queries are Graph-layer filters over existing `AllNodes`/`AllRelationships`/`GetNodeHistory` methods.

## [3.0.20] - 2026-03-02

### Added (Phase 2d — Per-Label / Per-Type Statistics)

- **`NodeCountByLabel(label)`** — returns the count of nodes with the given label. Returns 0 for unregistered labels.
- **`RelCountByType(typeName)`** — returns the count of relationships with the given type. Returns 0 for unregistered types.
- **`AllLabelCounts()`** — returns a map of label name to node count for all registered labels. Skips token 0 (reserved).
- **`AllRelTypeCounts()`** — returns a map of relationship type name to count for all registered types. Skips token 0 (reserved).
- **12 new tests** — empty/unregistered/single/multiple/after-delete for both labels and types, plus AllLabelCounts and AllRelTypeCounts with mixed counts.

### Changed

- **No Store interface change** — statistics are scan-based, delegating to existing `NodesByLabel`/`RelationshipsByType` methods.

## [3.0.19] - 2026-03-02

### Added (Phase 2b — Hash Chain Verification)

- **`VerifyNodeHashChain(id)`** — verifies the full hash chain for a node. Retrieves history + current version, validates genesis `PrevHash == ""`, verifies each version's `PrevHash` links to the previous version's `Hash`, and recomputes each hash via `ComputeNodeHash` to detect tampering. Returns `(true, nil)` if valid, `(false, nil)` on any mismatch, `(false, err)` on I/O failure.
- **`VerifyRelHashChain(id)`** — mirrors `VerifyNodeHashChain` for relationships using `ComputeRelHash`.
- **14 new tests** — 7 node (genesis-only, multiple updates, tampered hash, broken PrevHash, non-existent, nil integrity, property change) + 7 mirrored relationship tests.

### Changed

- **No Store or API changes** — verification methods are pure reads over existing `GetNodeHistory`/`GetRelHistory` + `ComputeNodeHash`/`ComputeRelHash`.

## [3.0.18] - 2026-03-01

### Fixed (Pre-Release Code Review)

- **BLOCKER: Update atomicity** — `UpdateNode`/`UpdateRelationship` now use `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` to atomically write version history and entity data in a single store call. Prevents orphaned history entries on crash between `PutNodeVersion` and `ReplaceNode`. New `Store` interface methods: `ReplaceNodeWithHistory(current, prevVersion, prevState)` and `ReplaceRelWithHistory(current, prevVersion, prevState)`.
- **BLOCKER: Hash serialization** — `writeProperties` in `integrity.go` replaced `fmt.Sprintf("%v")` with typed binary serialization using wire.go type tags. Maps now sort keys before hashing (deterministic). Type-distinct: `int(1)` vs `string("1")` produce different hashes. Breaking change for computed hashes (pre-release, no production data).
- **MAJOR: Cascade lock scope** — `DeleteNodeCascade` in BadgerStore releases `idxMu.Lock` before Phase 3 history cleanup. `deleteHistoryByPrefix` does Badger `db.View()` iterator scans — these no longer block concurrent reads/writes.
- **MINOR: Hash error handling** — Added `mustWrite`/`mustWriteString` helpers in `integrity.go`. All `_ = binary.Write()` and `_, _ = io.WriteString()` calls replaced with panicking wrappers. hash.Hash.Write never errors, but errors are no longer silently discarded.
- **MINOR: BatchBuilder docstring** — Changed "persisted atomically" to "executed sequentially; partial success is possible" to accurately describe behavior.
- **MINOR: MemoryStore RLock comment** — Added comment to `AllNodes`/`AllRelationships` documenting the RLock-for-iteration design choice.
- **MINOR: Tutorial 005 resource leak** — `bsQuery` is now explicitly closed if `graph.New` fails.

### Added (Phase 1g — Context-Aware Operations)

- **`AddNodeWithContext(ctx, labels, props)`** — creates a node with context support. Checks context at entry (pre-flight) and before the store write. Returns `context.Canceled` or `context.DeadlineExceeded` on cancellation.
- **`AddRelationshipWithContext(ctx, typeName, startNode, endNode, props)`** — creates a relationship with context support. Checks context at entry, before acquiring endpoint locks, and before the store write.
- **`UpdateNodeWithContext(ctx, id, updates)`** — updates a node with context support. 5 context checks: entry, before entity lock, before store read, before version history write, before final store write.
- **`UpdateRelationshipWithContext(ctx, id, updates)`** — mirrors `UpdateNodeWithContext` for relationships.
- **`DeleteNodeWithContext(ctx, id)`** — cascade-deletes a node with context support. Checks context at entry and under the entity lock before cascade.
- **`DeleteRelationshipWithContext(ctx, id)`** — deletes a relationship with context support. Checks context at entry before the store call.
- **`GetNodeWithContext(ctx, id)`** — retrieves a node with context support. Single pre-flight check.
- **`GetRelationshipWithContext(ctx, id)`** — retrieves a relationship with context support. Single pre-flight check.
- **`checkCtx(ctx)` helper** — non-blocking `select` with `default` branch. Zero overhead when context is not cancelled.
- **28 new tests** — 8 pre-flight cancellation (all methods return `context.Canceled`), 8 happy path (identical behavior to non-context methods), 4 deadline exceeded, 4 delegation regression (non-context methods still work), 4 edge cases (empty updates no-op, validation priority, checkCtx on Background). All pass with race detector.

### Changed

- **8 Graph methods refactored to thin wrappers** — `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `GetNode`, `GetRelationship` now delegate to their `WithContext` variants with `context.Background()`. Backward-compatible — existing callers require no changes.
- **No Store interface change** — Badger v4 does not support `context.Context` in its core API (`View`/`Update`/`Txn`). Context checks are best-effort at the Graph layer: pre-flight and between phases (before locks, before store calls). In-memory CPU-bound steps complete in microseconds and are not interrupted.

## [3.0.17] - 2026-03-01

### Added (Phase 1f — Batch Operations)

- **`PutNodesBatch(nodes []*types.Node)`** on Store interface — two-phase (validate then apply) atomic batch create. Phase 1 checks for duplicates vs existing store AND within the batch. Phase 2 deep-copies each, stores, and updates indexes. Any duplicate returns `ErrNodeExists` with zero mutations. Empty/nil input returns nil error. MemoryStore holds `mu.Lock()` for entire operation; BadgerStore holds `idxMu.Lock()` with pre-serialization outside the lock.
- **`PutRelationshipsBatch(rels []*types.Relationship)`** on Store interface — two-phase atomic batch create. Phase 1 verifies endpoints exist and checks for duplicate rel IDs. Phase 2 deep-copies, stores, updates type + adjacency indexes. MemoryStore and BadgerStore both use single lock for atomicity.
- **`DeleteNodesBatch(ids []snowflake.ID)`** on Store interface — two-phase atomic batch delete. Phase 1 verifies all IDs exist. Phase 2 removes entities, cleans label indexes, removes history. Missing ID returns `ErrNodeNotFound` with zero mutations.
- **`DeleteRelationshipsBatch(ids []snowflake.ID)`** on Store interface — two-phase atomic batch delete. Phase 1 verifies all IDs exist and pre-reads metadata. Phase 2 deletes via mutation-only helpers (type/adjacency/history cleanup). Missing ID returns `ErrRelNotFound` with zero mutations.
- **`BatchBuilder` fluent API** — `NewBatchBuilder(g)` creates a builder that queues operations with eager validation and deferred persistence. `AddNode(labels, props)` validates and creates fully-formed nodes (with hash + integrity) but doesn't persist. `AddRelationship(typeName, startNode, endNode, props)` validates type and properties. `UpdateNode(id, updates)` / `UpdateRelationship(id, updates)` pre-validate shadow key rejection and property types. `DeleteNode(id)` / `DeleteRelationship(id)` queue deletes.
- **`BatchResult`** — reports batch outcome with `Created`, `Updated`, `Deleted`, `Failed` counts, per-operation `Errors` slice, and `Duration`. Execute order: create nodes → create rels → update nodes → update rels → delete rels → delete nodes.
- **`BatchError`** — describes a single operation failure with `Op` (operation name), `ID` (entity ID), and `Err` (underlying error). Implements `error` interface.
- **41 new tests** — 12 MemoryStore batch tests (empty/happy/duplicate/internal-duplicate for Put, empty/happy/missing for Delete — node and rel parity), 12 BadgerStore batch tests (mirrored), 17 BatchBuilder tests (AddNode validation, AddRelationship validation, UpdateNode/UpdateRelationship validation, Execute empty/nodes/nodes+rels/updates/deletes/mixed/1000-nodes/partial-failure/update-rel, BatchError.Error). All pass with race detector.

### Changed

- **`Store` interface** — added 4 batch methods (`PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`). Both MemoryStore and BadgerStore implement them.

## [3.0.16] - 2026-03-01

### Fixed (Phase 1e — FlushInterval Policy + LRU evictClean Fix)

- **FlushInterval defaulting for InMemory mode** — `NewBadgerStore` now defaults `FlushInterval` to 100ms for both InMemory and OnDisk modes. Previously, InMemory mode had no periodic flushing by default.
- **LRU `evictClean()` O(N²) degradation** — added `cleanCount` field for O(1) early exit when all entries are dirty. Prevents repeated O(N) scans of a fully-dirty cache.

## [3.0.15] - 2026-03-01

### Added (Phase 1d — Bulk Query Methods)

- **`AllNodes()`** on Store interface — returns all stored nodes, sorted by snowflake.ID. MemoryStore iterates the node map under `RLock` with DeepCopy. BadgerStore snapshots `nodeIDs` under `idxMu.RLock`, then fetches each via `GetNode` (cache + existence check + Badger fallback + DeepCopy). Returns `nil, nil` for empty stores.
- **`AllRelationships()`** on Store interface — returns all stored relationships, sorted by snowflake.ID. Same patterns as `AllNodes`.
- **`GetNodesByIDs(ids []snowflake.ID)`** on Store interface — originally returned nodes matching the given IDs, sorted by snowflake.ID, and omitted missing IDs. Current Unreleased behavior supersedes this by returning `ErrNodeNotFound` for any missing explicit node ID.
- **`GetRelationshipsByIDs(ids []snowflake.ID)`** on Store interface — originally returned relationships matching the given IDs, sorted by snowflake.ID, and omitted missing IDs. Current Unreleased behavior supersedes this by returning `ErrRelNotFound` for any missing explicit relationship ID.
- **Graph-layer passthroughs** — `Graph.AllNodes()`, `Graph.AllRelationships()`, `Graph.GetNodesByIDs(ids)`, `Graph.GetRelationshipsByIDs(ids)`. Pure delegation to the store (no string resolution needed).
- **32 new tests** — 12 MemoryStore (empty/count/sorted for each method), 12 BadgerStore (mirrored), 8 graph-layer (empty + populated for each method, including skip-missing verification). All pass with race detector.

### Added (Phase 1c — Hash Chain Computation)

- **`ComputeNodeHash(n, labels)`** — computes a SHA-256 hash of a node's content (id, version, sorted labels, sorted properties). Returns a 64-character hex string.
- **`ComputeRelHash(r, typeName)`** — computes a SHA-256 hash of a relationship's content (id, version, type name, start/end IDs, sorted properties). Returns a 64-character hex string.
- **Integrity hooks in `AddNode`/`AddRelationship`** — newly created entities now have `Integrity()` populated with a computed `Hash` and empty `PrevHash` (genesis).
- **Integrity hooks in `UpdateNode`/`UpdateRelationship`** — updated entities get a new `Hash` computed on their final state, with `PrevHash` set to the previous version's hash. Forms a verifiable hash chain across the entity's version history.
- **22 new tests** — 10 unit tests for hash functions (determinism, property/version/label/type/endpoint sensitivity, label order independence), 12 graph-layer integration tests (integrity set on create, hash determinism, hash chain linking across updates, multiple-update chain verification, genesis zero PrevHash — all with node/relationship parity).

### Added

- **`UpdateNode(id, updates)`** — graph-layer read-modify-write under entity lock. Pre-validates all keys (`tkg_` prefix rejected) and values (`ValidatePropertyValue`) before acquiring the lock. Under the lock: reads current state, applies property updates (nil value = delete key), bumps version, sets `temporal.UpdatedAt`, persists via `ReplaceNode`. Empty updates map is a no-op (no lock, no version bump).
- **`UpdateRelationship(id, updates)`** — same pattern as `UpdateNode`. Entity lock on the relationship ID only — property changes don't affect adjacency, so endpoint locking is unnecessary.
- **`SetNodeProperty(id, key, value)`** / **`DeleteNodeProperty(id, key)`** — convenience wrappers around `UpdateNode`.
- **`SetRelationshipProperty(id, key, value)`** / **`DeleteRelationshipProperty(id, key)`** — convenience wrappers around `UpdateRelationship`.
- **`ReplaceNode(n)`** / **`ReplaceRelationship(r)`** on Store interface — overwrite existing entities. Returns `ErrNodeNotFound`/`ErrRelNotFound` if the entity does not exist. Deep-copies at the store boundary. No index changes (labels and type/endpoints are immutable after creation).
- **`ValidatePropertyValue` exported** — renamed from `validatePropertyValue` in `pkg/types/propertyslice.go` for use in graph-layer update pre-validation paths.
- **44 new tests** — 6 MemoryStore Replace, 6 BadgerStore Replace, 28 graph-layer (13 UpdateNode + 11 UpdateRelationship + 4 convenience), 4 Badger integration (including persistence round-trip). All pass with race detector.
- **Version history** — pre-mutation entity state saved automatically on every `UpdateNode`/`UpdateRelationship`. Queryable by version or as a full ascending history. Truncatable. Cleaned up on entity deletion (including cascade).
- **`PutNodeVersion(id, version, n)`** / **`PutRelVersion(id, version, r)`** on Store interface — persist a versioned snapshot. Deep-copies at the store boundary. Initial creation (AddNode/AddRelationship) does NOT write history; first update saves version 0.
- **`GetNodeVersion(id, version)`** / **`GetRelVersion(id, version)`** — retrieve a specific historical version. Returns `ErrVersionNotFound` when the version doesn't exist.
- **`GetNodeHistory(id)`** / **`GetRelHistory(id)`** — return all saved versions in ascending order. Empty slice for never-updated entities.
- **`TruncateNodeHistory(id, keepVersions)`** / **`TruncateRelHistory(id, keepVersions)`** — keep only the N most recent versions. `keepVersions == 0` clears all history.
- **`Graph.GetNodeHistory(id)`** / **`Graph.GetRelHistory(id)`** — graph-layer passthrough to the Store.
- **`ErrVersionNotFound`** sentinel error — returned by `GetNodeVersion`/`GetRelVersion` for non-existent versions.
- **History key promotion** — `keyHistNode` (0x07) and `keyHistRel` (0x08) promoted from test-only stubs to production keys in `keys.go`. Added `histNodePrefix`/`histRelPrefix` for prefix scanning.
- **~50 new history tests** — 17 MemoryStore (8 node + 9 rel), 19 BadgerStore (mirrored + 2 restart persistence), 14 graph-layer (5 node + 5 rel + 4 Badger persistence). All pass with race detector.

### Changed

- **`Store` interface** — added `Close() error` (resource cleanup contract). All Store implementations must satisfy it. `MemoryStore.Close()` is a no-op (returns nil). `BadgerStore.Close()` was already implemented. `Graph.Close()` now calls `store.Close()` universally instead of the previous `closeFn` indirection — custom stores injected via `Config.Store` are now properly closed.
- **`Store` interface** — added `ReplaceNode(n *types.Node) error` and `ReplaceRelationship(r *types.Relationship) error`. Replace semantics are the opposite of Put: Put rejects duplicates (`ErrNodeExists`/`ErrRelExists`), Replace requires existence (`ErrNodeNotFound`/`ErrRelNotFound`).
- **`Store` interface** — added 8 version history methods (`PutNodeVersion`, `GetNodeVersion`, `GetNodeHistory`, `TruncateNodeHistory` + relationship mirrors). Both MemoryStore and BadgerStore implement them.
- **`UpdateNode`/`UpdateRelationship`** — now save pre-mutation state to version history via `PutNodeVersion`/`PutRelVersion` before applying mutations.
- **`DeleteNode`/`DeleteNodeCascade`/`DeleteRelationship`** — all delete paths now clean up associated version history entries. BadgerStore uses a three-phase cascade: (1) preflight, (2) rel mutations, (3) history cleanup.
- **`Graph.Close()` error handling** — replaced `&& closeErr == nil` guards with `errors.Join`, preserving all errors (registry save + store close) instead of dropping subsequent ones.
- **`Graph` struct** — removed `closeFn func() error` field. No longer needed since `Store.Close()` is called directly.

### Fixed

- **`BadgerDir` whitespace silent fallback** — `New(Config{BadgerDir: "   "})` previously fell through to MemoryStore silently (whitespace-only string is non-empty, passes `!= ""` check, but Badger would fail). Now rejects whitespace-only `BadgerDir` with an explicit error message. Empty string `""` still correctly defaults to MemoryStore.

## [3.0.14] - 2026-03-01

### Fixed

- **`flushLoop` silently discards errors** — `_ = bs.flush()` in the background flush loop now logs failures via `slog.Error` instead of silently discarding them. Removed `#nosec G104` annotations. Persistent Badger failures (disk full, corruption) are now observable.
- **Shared entity pointers between cache and caller** — `PutNode`/`PutRelationship` and `GetNode`/`GetRelationship` in both `BadgerStore` and `MemoryStore` now deep-copy entities at the store boundary. Previously, caller and cache shared the same pointer; mutations via `SetProperty` on the returned entity would silently corrupt cached state.
- **`DeleteNodeCascade` partial mutation on mid-loop error** — refactored to a two-phase approach: (1) preflight reads all relationship metadata, aborting with zero state changes on any read failure; (2) applies all deletions atomically via the new `deleteRelByInfo` helper. Previously, a corrupted relationship mid-cascade left indexes in a permanently split state.
- **`Close()` masks `db.Close()` error** — replaced `if e != nil && err == nil { err = e }` with `errors.Join(err, e)` to preserve both flush and database close errors.
- **`DeleteNodeCascade` returns nil on node data corruption** — now returns `fmt.Errorf("graph: cascade completed with corrupt node data: %w", err)` while still completing index cleanup. Callers can detect and log corruption.
- **Entity lock shard index uses step bits** — `shardIndex()` was masking bits 0-7 (step counter), which resets to 0 every millisecond. All entities created in separate milliseconds mapped to shard 0, reducing 256 shards to a single global mutex. Now shifts right by 22 to extract the low 8 bits of the timestamp field. Entities created >256ms apart land in different shards.
- **Node and rel generators share snowflake node ID** — both `nodeIDGen` and `relIDGen` used the same `SnowflakeNodeID`, allowing value-level ID collisions within the same millisecond. Now mapped to an even/odd pair (`ID*2` for nodes, `ID*2+1` for rels). Valid `SnowflakeNodeID` range reduced from 0-1023 to 0-511 (512 concurrent instances). **Breaking**: existing databases with IDs generated under the old scheme remain readable (key prefixes distinguish entity types), but new IDs will have different node fields.
- **`ImportNames` integer overflow on corrupted data** — both `labelRegistry.ImportNames` and `relTypeRegistry.ImportNames` cast slice indices to `uint16` without bounds checking. If persisted data exceeded 65,535 entries, the cast silently truncated, causing token collisions. Now returns an error if `len(names)-1 > tokenCapacityMax`. Removed `#nosec G115` annotations.

### Added

- **`Node.DeepCopy()`** — returns a fully independent clone (extraLabels, properties, temporal, integrity all deep-copied).
- **`Relationship.DeepCopy()`** — returns a fully independent clone (properties, temporal, integrity all deep-copied).
- **`tkg_created_at` derived from snowflake ID** — when `TemporalMetadata` is nil or `CreatedAt` is zero, `ResolveNodeProperty`/`ResolveRelProperty` derive the creation timestamp from the entity's snowflake ID via `Decompose()`. Explicit non-zero `CreatedAt` takes priority (historical import). Every entity now has an automatic, accurate creation timestamp without requiring `SetTemporal()`.

### Changed

- **`Config.SnowflakeNodeID` range** — valid range is now 0-511 (was 0-1023). Mapped to even/odd generator pair for value-level ID uniqueness.
- **Sort comments corrected** — `sortNodesByID`/`sortRelsByID` and query method doc comments no longer claim "chronological" order. Sort is time-dominant (ms timestamp in high bits) with node field and step as tiebreakers.

## [3.0.13] - 2026-02-28

### Fixed

- **Graph.Close() data race** — `closeFn` was read/written without synchronization; two concurrent `Close()` calls would race on the nil check. Replaced with `sync.Once` (`closeOnce`) for race-free idempotency.
- **Query methods swallow corruption errors** — `NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, `IncomingRelationships` used bare `continue` on all errors, silently eating I/O and corruption errors. Now only `ErrNodeNotFound` / `ErrRelNotFound` (index orphans) are skipped; all other errors propagate to the caller.
- **Dead variable `relDeleteCount`** — written but never read in `DeleteNodeCascade`. Removed.

### Changed

- **Test-only code relocated** — 12 functions and 6 constants (`histNodeKey`, `histRelKey`, `tempNodeKey`, `tempRelKey`, `labelIndexPrefix`, `relTypeIndexPrefix`, `outPrefix`, `outTypedPrefix`, `inPrefix`, `inTypedPrefix`, `parseNodeIDFromLabelIdx`, `parseRelIDFromTypeIdx`) moved from `keys.go` to `keys_helpers_test.go`. Reduces production binary size.
- **`toIntSlice` test coverage** — added `TestWireRoundTripIntSlice` exercising `[]int` property wire round-trip. No longer at 0% coverage.

## [3.0.12] - 2026-02-28

### Fixed

- **Atomic counter persistence** — counter keys (`meta/node_count`, `meta/rel_count`) are now written in the same `WriteBatch` as entity data. Previously, `persistCounters()` was a separate transaction, creating a TOCTOU window where counters could drift from actual entity count on crash recovery.
- **O(N) → O(1) evictClean** — LRU `evictClean()` was O(N²) due to restarting the inner scan from `Back()` after each eviction. Now a single-pass backward scan with `prev` pointer — O(N) worst case.
- **Cascade delete error propagation** — `DeleteNodeCascade` now checks `errors.Is(err, ErrRelNotFound)` and propagates non-sentinel errors (data corruption). Previously all `deleteRelLocked` errors were silently swallowed.
- **Close() InMemory flush** — `Close()` now calls `bs.flush()` unconditionally, ensuring pending ops are persisted even when `flushLoop` was never spawned (InMemory mode or zero FlushInterval).

### Changed

- **O(1) existence checks** — added `relIDs` map (mirrors `nodeIDs`) for O(1) relationship existence lookups. `GetRelationship()` and `PutRelationship()` now short-circuit via `relIDs` instead of scanning `typeIdx`. Removed dead `relExistsInIndex()` O(N) scan.
- **`GetNode()` / `GetRelationship()` bloom filter** — on cache miss, both methods check `nodeIDs`/`relIDs` (O(1)) before opening a Badger `db.View()` transaction, avoiding disk I/O for non-existent entities.
- **`persistCounters()` removed** — counter persistence is now inlined into `flush()` as part of the atomic WriteBatch.

## [3.0.11] - 2026-02-28

### Fixed

- **Version-aware dirty tracking** — LRU `CollectDirty()` is now read-only; `MarkFlushed()` only clears entries matching the collected `dirtyVer`. Prevents data loss when new writes land between `CollectDirty()` and `MarkFlushed()`.
- **Map-based pending write buffer** — replaced `[]writeOp` with `map[string]writeOp` for last-write-wins deduplication. `requeueOps()` preserves newer writes over failed ops. Prevents chronological write inversion on flush retry.
- **Cascade index scrub** — `DeleteNodeCascade` now scrubs `labelIdx` by scanning all label sets when entity data is unreadable, preventing ghost index entries.

## [3.0.10] - 2026-02-28

### Added

- **LRU entity caches** (`pkg/graph/lru.go`) — generic `entityLRU[V]` with dirty tracking, tombstone support, and soft capacity (dirty entries never evicted until flushed). BadgerStore maintains separate caches for nodes and relationships. Configurable via `BadgerStoreConfig.CacheCapacity` (default 10,000 per cache).
- **Entity lock manager** (`pkg/graph/entity_locks.go`) — 256-shard `sync.Mutex` array for write-skew prevention. `LockTwo(a, b)` acquires shards in ascending order (deadlock-free). Self-loops and same-shard IDs handled correctly with single lock acquisition.
- **Async batch persistence** — write operations update in-memory state immediately and queue `writeOp` structs. Background flush loop drains the buffer via Badger `WriteBatch` (blind writes = zero OCC conflicts) every `FlushInterval` (default 100ms). Failed ops are re-queued for retry.
- **Background value log GC** — periodic `RunValueLogGC()` loop (default 5min interval, configurable via `GCInterval`). Skipped entirely in in-memory mode.
- **In-memory indexes** — `nodeIDs`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx` maps rebuilt from Badger on startup via `loadIndexes()`. In-memory state is the source of truth while running. Badger is the durable backing store.
- **`BadgerStoreConfig`** new fields: `CacheCapacity`, `FlushInterval`, `GCInterval`, `GCDiscardRatio`.
- **`BadgerStore.Flush()`** — explicit flush for tests that need to verify durable persistence.
- **Write-skew regression test** (`TestGraphAddRelDeleteNodeConcurrency`) — 100 iterations of concurrent `AddRelationship` + `DeleteNode` on overlapping entities, verifying no dangling edges.

### Changed

- **BadgerStore architecture** — replaced synchronous per-operation Badger transactions with cache-first reads + async batch writes. Read path: LRU cache hit → return; cache miss → `db.View()` → populate cache; tombstone → `ErrNotFound`. Write path: update cache (dirty) + update indexes + queue writeOps.
- **Counter implementation** — replaced `incrCounter()` inside Badger transactions with `atomic.Int64` fields on the BadgerStore struct. Counters are persisted by the flush loop piggyback. Eliminates all OCC contention on concurrent writes.
- **`Graph.AddRelationship`** — now acquires entity locks on both endpoints via `LockTwo(startID, endID)` before ID generation. Prevents write-skew where concurrent `AddRelationship(→X)` + `DeleteNodeCascade(X)` both commit, producing a dangling edge.
- **`Graph.DeleteNode`** — now acquires entity lock on the target via `LockEntity(id)` before cascade.
- **`DeleteNodeCascade` (BadgerStore)** — atomic in-memory under `idxMu` write lock with async Badger writes, replacing single `db.Update()` transaction.
- **`Close()`** — idempotent via `sync.Once`. Stops background goroutines, performs final flush, persists counters, closes Badger.

### Removed

- `incrCounter()` — replaced by atomic counters.
- `initCounters()` — replaced by `loadIndexes()` + counter loading from meta keys.

## [3.0.9] - 2026-02-27

### Fixed

- **Close() file handle leak** — `Graph.Close()` now always calls `closeFn()` even if registry saves fail. Previously, a failed `SaveLabelRegistry` or `SaveRelTypeRegistry` would exit early, leaving the Badger file handle open. `closeFn()` now runs unconditionally; the first error is collected and returned.
- **Type erasure in wire format** — msgpack serialization destroyed Go type fidelity (`[]string` → `[]any`, `int64` → `int8`). Added a `Type byte` tag to `propertyWire` that records the concrete Go type during serialization and reconstructs it during deserialization. 24 type tags cover all allowlisted property types. Backward compatible: old data without the tag (decoded as `Type: 0`) falls through to integer normalization.
- **Silent data loss on query errors** — Store query methods (`NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, `IncomingRelationships`, `NodeCount`, `RelationshipCount`) now return `error`. `BadgerStore` propagates I/O and corruption errors instead of swallowing them. `MemoryStore` always returns `nil`.
- **TOCTOU cascade-delete** — `Graph.DeleteNode` was executing N+2 separate lock acquisitions/transactions, creating a window where concurrent `AddRelationship` could produce dangling edges. Both `MemoryStore.DeleteNodeCascade` and `BadgerStore.DeleteNodeCascade` now execute the entire cascade atomically — single write lock / single `db.Update()` transaction.
- **O(N) counts** — `BadgerStore.NodeCount()` and `RelationshipCount()` previously did full prefix scans. Now maintained as atomic metadata counters (`meta/node_count`, `meta/rel_count`) incremented/decremented within each mutating transaction — O(1) reads. Counter initialization scans on first open for backward compatibility with existing databases.

### Changed

- **`Store` interface** — all 6 query methods now return `error`. Added `DeleteNodeCascade(id snowflake.ID) error`. `Graph.DeleteNode` delegates to `DeleteNodeCascade`.
- **`propertyWire`** struct gained `Type byte` field (msgpack tag `"t"`).

## [3.0.8] - 2026-02-27

### Added

- **Badger persistence** (`pkg/graph/badgerstore.go`) — `BadgerStore` implementing the `Store` interface using [Badger v4](https://github.com/dgraph-io/badger) as the storage backend. Supports in-memory mode for testing and on-disk persistence. Includes type index, label index, and adjacency index maintenance.
- **Msgpack wire formats** (`pkg/graph/wire.go`) — `nodeWire`, `relWire`, `propertyWire` structs with conversion functions for serialization boundary. Handles temporal metadata, integrity hashes, and type normalization (msgpack compact integer encoding).
- **Binary key encoding** (`pkg/graph/keys.go`) — fixed-width binary keys with single-byte prefix tags for correct sort order. All snowflake IDs stored as big-endian uint64; tokens as big-endian uint16. 10 key types covering entities, indexes, adjacency, history, temporal, and metadata.
- **Registry persistence** — `ExportNames()` / `ImportNames(names)` on both `labelRegistry` and `relTypeRegistry`. `BadgerStore` persists registries as msgpack `[]string` under `meta/label_tokens` and `meta/reltype_tokens`.
- **`Graph.Close()`** — saves registries to Badger (if applicable), then closes the database. Idempotent. No-op for `MemoryStore`.
- **`Config.BadgerDir`** / **`Config.BadgerInMemory`** — creates a `BadgerStore` automatically when `Config.Store` is nil. Loads persisted registries on startup; fail-fast on corrupt data.
- **`ErrRegistryNotEmpty`** sentinel error — returned when `ImportNames` is called on a non-empty registry.

### Dependencies

- Added `github.com/vmihailenco/msgpack/v5` v5.4.1
- Added `github.com/dgraph-io/badger/v4` v4.9.1

## [3.0.7] - 2026-02-27

### Changed

- **Snowflake dependency migrated** from `gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026 v0.1.3` to [`github.com/bds421/rho-snowflake-2026 v1.0.1`](https://github.com/bds421/rho-snowflake-2026). All import paths updated across 14 `.go` files, `go.mod`, `SPEC.md`, and documentation.

### Fixed

- **Deterministic query results** — `NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, and `IncomingRelationships` now sort results by snowflake.ID for deterministic output. Previously, Go map iteration randomized the output on every call.
- **Cascade-delete outgoing tolerance** — `Graph.DeleteNode` now skips `ErrRelNotFound` in both the outgoing and incoming loops. Previously, a concurrently-deleted outgoing relationship would abort the cascade, leaving a partially severed node.
- **TOCTOU documentation** — `Graph.DeleteNode` documents the per-call locking limitation: without a transactional store API, a concurrent `AddRelationship` can create a dangling edge during cascade. The Badger implementation must wrap the entire cascade in a single `Update()` transaction.

## [3.0.6] - 2026-02-27

### Added

- **`Store` interface** (`pkg/graph/store.go`) — pure persistence contract with `PutNode`/`GetNode`/`DeleteNode`, `PutRelationship`/`GetRelationship`/`DeleteRelationship`, index queries (`NodesByLabel`, `RelationshipsByType`), adjacency queries (`OutgoingRelationships`, `IncomingRelationships`), and counts. Keys are `snowflake.ID`.
- **`MemoryStore`** (`pkg/graph/memorystore.go`) — thread-safe in-memory `Store` implementation. Uses nested hash-set adjacency indexes (`map[snowflake.ID]map[snowflake.ID]struct{}`) for O(1) insert/delete. `PutRelationship` validates start/end nodes exist.
- **`Graph.AddNode(labels, props)`** — creates a node with auto-generated snowflake ID, resolves labels to tokens, and bulk-loads properties via `NewPropertySlice`. Validates input before generating IDs to prevent snowflake waste.
- **`Graph.AddRelationship(typeName, startNode, endNode, props)`** — creates a directed relationship with auto-generated snowflake ID, resolves type to token, validates endpoints are non-nil, and bulk-loads properties.
- **`Graph.DeleteNode(id)`** — cascade-deletes all outgoing and incoming relationships before removing the node. Handles self-loops correctly by skipping `ErrRelNotFound` on the incoming pass.
- **`Graph.DeleteRelationship(id)`** — passthrough to store.
- **Store passthrough queries on Graph**: `GetNode`, `GetRelationship`, `NodesByLabel` (string-based, resolves to token), `RelationshipsByType` (string-based), `NodeCount`, `RelationshipCount`.
- **Shadow property resolution** (`pkg/graph/shadow.go`): `ResolveNodeProperty(n, key)` and `ResolveRelProperty(r, key)` dispatch all 15 `tkg_*` shadow keys. Non-`tkg_` keys delegate to `GetProperty`. Nil-guards on `Temporal()` and `Integrity()` prevent nil-pointer panics on new entities.
- **`NewPropertySlice(map[string]any)`** — O(N log N) bulk loader. Allocates once, validates all values (reserved-prefix + recursive allowlist), sorts once. Replaces the O(N²) per-property `SetProperty` loop for bulk construction.
- **`Node.SetProperties(ps)`** / **`Relationship.SetProperties(ps)`** — assign a pre-built `PropertySlice` directly, bypassing per-property validation (already done by `NewPropertySlice`).
- **SnowflakeID bridge methods**: `nodeID.SnowflakeID()`, `relID.SnowflakeID()`, `entityID.SnowflakeID()` — exported methods on unexported wrapper types for cross-package persistence key extraction.
- **Sentinel errors**: `ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrNoLabels`, `ErrNilNode`.
- **`Config.Store`** field — pluggable persistence backend. Defaults to `NewMemoryStore()` when nil.

### Changed

- `Graph` struct now holds a `store Store` field alongside registries and generators.
- `New(Config)` initializes a `MemoryStore` when `Config.Store` is nil.
- Implementation phases updated: Phase 2A (Store, MemoryStore, entity management, shadow resolution) complete.

## [3.0.5] - 2026-02-27

### Added

- **`HasLabelTokenRaw(uint16)`** on Node — zero-allocation label check for the graph layer. Accepts raw `uint16` instead of opaque `labelToken`, avoiding the heap allocation from `AllLabelTokens()`.
- **`HasTypeTokenRaw(uint16)`** on Relationship — zero-allocation type check for the graph layer. Mirrors `Node.HasLabelTokenRaw`. Token 0 always returns false.
- **`ErrMaxDepthExceeded` sentinel error.** `PropertySlice.Set()` returns this when a value exceeds 32 levels of nesting, preventing stack overflow from self-referential or deeply nested structures.
- **`ErrEmptyName` sentinel error.** Both `labelRegistry.GetOrCreate` and `relTypeRegistry.GetOrCreate` reject empty strings, preventing ambiguous token resolution.

### Changed

- **Explicit snowflake configuration.** Both snowflake generators now pass `WithEpoch(2026-01-01)`, `WithNodeBits(10)`, `WithStepBits(12)` explicitly instead of relying on defaults.
- **Allowlist property validation.** `validateReflectValue` switched from denylist (reject Ptr/Struct) to allowlist (accept only primitives + safe containers). Arrays, channels, functions, and unsafe pointers are now rejected at any nesting depth.
- **Registry capacity: token 65535 is now assignable.** Fixed off-by-one in `GetOrCreate` that blocked the final token. Capacity check uses `len(toLabel/toName) > 65535` instead of `nextToken >= 65535`.
- **Recursion depth limit.** `validateReflectValue`, `deepCopyValue`, and `reflectCopyValue` now thread a `depth` counter and stop at `maxPropertyDepth` (32). Validation returns an error; copy functions fall back to shallow return.
- `NodeHasLabel` and `RelationshipHasType` use `HasLabelTokenRaw`/`HasTypeTokenRaw` for zero-allocation matching with token-0 defense-in-depth.
- **`deepCopyValue` nil short-circuit.** Nil values return immediately without entering the type switch or reflect path.
- **`Temporal()`/`Integrity()` doc comments** on both Node and Relationship now document the shared-pointer intent (no defensive copy).
- **`ErrUnsupportedValueType` message updated** from "pointer and struct values are not supported" to the generic "unsupported property value type" to reflect the allowlist model.
- `Set()` doc comment updated to describe the allowlist approach and depth limit.

### Fixed

- Empty-string labels and relationship types no longer silently assigned tokens.
- Registry capacity boundary corrected (65535 tokens, not 65534).
- `pkg/graph/doc.go` updated to reflect current state (snowflake generators exist, not "will hold").

## [3.0.3] - 2026-02-27

### Added

- **Snowflake ID generators.** `Graph` now holds two independent `snowflake.Node` generators. `NextNodeID()` and `NextRelID()` produce unique `snowflake.ID` values. `Config.SnowflakeNodeID` is validated; out-of-range values return an error from `New()`. *(Range changed from 0-1023 to 0-511 in v3.0.14.)*
- **Recursive property validation.** `PropertySlice.Set()` traverses slices, maps, and `any`/interface wrappers to reject pointers and structs at any nesting depth (`validatePropertyValue` + `validateReflectValue`).
- **`Instant` type** (`pkg/types`): semantic wrapper for Unix-millisecond timestamps used by all temporal fields.
- **`nodeID` / `relID` opaque types** (`pkg/types`): unexported wrappers around `snowflake.ID`. `InternalID()`, `StartNodeID()`, `EndNodeID()` return these instead of `snowflake.ID` directly.
- **`TemporalMetadata` fields**: `ValidFrom`, `ValidTo`, `TxFrom`, `TxTo`, `CreatedAt`, `UpdatedAt`, `DeletedAt` (all `Instant`), `CreatedBy`, `UpdatedBy` (`string`), `BaseEntityID` (`snowflake.ID`).
- **`NodeIntegrity` / `RelIntegrity` fields**: `Hash`, `PrevHash` (`string`).

### Changed

- `reflectCopyValue` nil-value handling: map keys with nil values are preserved using `reflect.Zero()` instead of silently deleted.
- Registry capacity warning fires exactly once via `sync.Once` (was per-token for 60000-65534).

## [3.0.2] - 2026-02-27

### Added

- **`pkg/graph` package**: Graph layer with label and relationship type registries (Phase 1).
  - `Graph` struct with `Config`, registry ownership, and string resolution methods.
  - `labelRegistry` / `relTypeRegistry`: thread-safe bidirectional string ↔ uint16 token mappings (RWMutex, double-check on write miss). Independent token namespaces.
  - Resolution: `NodeLabels`, `NodePrimaryLabel`, `NodeHasLabel`, `RelationshipType`, `RelationshipHasType`.
  - Registry passthrough: `GetOrCreateLabel`, `GetOrCreateRelType`, `LookupLabel`, `LookupRelType`.
  - Capacity: warning at 60K tokens, error at 65535.
- `labelToken.Value()` and `relTypeToken.Value()` bridge methods for cross-package token access.
- Pointer/struct rejection in `PropertySlice.Set()` with `ErrUnsupportedValueType` sentinel.
- `PropertySlice.Delete(key)` method with `tkg_` prefix guard.
- `Node.DeleteProperty(key)` and `Relationship.DeleteProperty(key)`.

### Changed

- `deepCopyValue` expanded to all common slice/map types plus reflect-based fallback for exotic types.
- `ToMap()` now deep-copies all values.
- Sentinel errors (`ErrReservedPrefix`) properly wrapped for `errors.Is` discrimination.

## [3.0.1] - 2026-02-27

### Added

- `PropertiesMap()` on Node and Relationship.
- `TemporalMetadata` stub struct with `Temporal()`/`SetTemporal()` accessors.
- `NodeIntegrity` and `RelIntegrity` stub structs with `Integrity()`/`SetIntegrity()` accessors.
- Comprehensive `PropertySlice` test suite.

### Changed

- Shadow properties aligned to spec (final 15 `tkg_*` keys).
- Opaque token types (`labelToken`/`relTypeToken`) replace raw `uint16` in public API.
- Token 0 validation: constructors panic, `Has*Token(0)` returns false.
- Extra label deduplication in `NewNode`.

## [3.0.0] - 2026-02-16

### Added

- Initial implementation of `pkg/types`: `Node`, `Relationship`, `PropertySlice`, shadow constants.
- Snowflake ID integration via `github.com/bds421/rho-snowflake-2026`.
- Token interning with `labelToken` and `relTypeToken` (uint16).
- Shadow property protection (`tkg_` prefix rejection).
- Defensive copying on all slice accessors.
