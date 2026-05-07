# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Session Protocol

- **Session start**: Read `tasks/lessons.md` and `CHANGELOG.md` (source of truth for version history) before doing any work
- **Before planning**: Read the full API of ALL files involved in the change — not snippets around suspected bug locations. Understanding the complete API prevents plans based on wrong assumptions
- **Challenge the plan**: Before implementing any method, ask: "Does this algorithm deliver what the method name promises?" and "Does this interact with an existing feature that could break it?"
- **After each implementation step**: Update all affected documentation (CLAUDE.md, README.md, CHANGELOG.md, docs/architecture.md, docs/SPEC.md). If not specified whether changes belong to the current version or a new version in CHANGELOG.md, ask the user before writing
- **After corrections**: Update `tasks/lessons.md` with the pattern and a rule to prevent recurrence
- **Session end**: Update `tasks/lessons.md` with new lessons

## MR Review Protocol

When asked to analyse or review a merge request, execute these three phases in order:

### Phase 1 — Correctness of the MR itself

For each changed or added file:

1. **Read the full diff** with `git diff origin/main...origin/<branch>`.
2. **Verify the implementation against the spec** — does every new method/test deliver exactly what its name promises? Check edge cases the author may have assumed away.
3. **Lessons and CHANGELOG hygiene**:
   - Confirm any new lesson entry has the correct next sequential number (grep `^## B` in `tasks/lessons.md`).
   - Confirm the lesson body is not a duplicate of an existing entry (same title or same code pattern).
   - Confirm the CHANGELOG section (`[Unreleased]` or explicit version) is placed above the current latest release, not above an older one — rebase issues leave the context pointing at a stale anchor.
4. **Test quality** (apply all 17 testing rules from "Testing Rules"):
   - Two-phase tests for every temporal/history-aware method (rule 15).
   - Adversarial scenarios with exact-set assertions, not just happy-path (rule 16).
   - Negative assertions: "must NOT contain Y" and phantom-value returns-empty cases.
   - For interval queries: the "predicate held during part of interval but not on most-recent version" case must be asserted.
   - Sentinel errors tested with `errors.Is` at every call layer (rule 4).
5. **Run the tests**: `make test-race` on the branch. A test suite that fails for one backend is not mergeable.

### Phase 2 — Is the addressed issue also present elsewhere?

After understanding what problem the MR fixes or tests:

1. **Grep for the same pattern** across the whole codebase — the same bug often hides behind multiple doors (see lessons A1, Code Review Lessons section). Use the audit checklists in "Audit Checklists".
2. **Check symmetric types**: Node and Relationship are structural mirrors; fixes to one without the other are incomplete. Same for `Get*ValidAt` (named temporal) vs `*By*(opts QueryOpts)` (generic temporal) — rule 17.
3. **Check all Store implementations**: If a fix touches `MemoryStore`, verify `BadgerStore` and `TieredStore` are consistent, and vice versa.
4. **Check batch paths**: Any fix to standalone mutation paths must be verified against `BatchBuilder` paths (same logic often duplicated — see lesson A1).

### Phase 3 — Are the MR's tests sufficient?

After confirming the implementation is correct and the issue isn't duplicated elsewhere:

1. **Coverage**: Run `make cover`. Every new public method must appear in coverage. No new code below 80%.
2. **Missing scenarios checklist**:
   - Cross-shard relationships (for TieredStore) — not just same-shard.
   - Deleted entities: history must be queryable after deletion (B32).
   - Concurrent access: if the MR touches shared state, confirm a `test-race` run exists.
   - Cold→warm shard transitions (for TieredStore): use `demoteToCold` helper, not sub-second `ShardWindow`.
3. **Confirm tests would have caught the original bug**: For each test, ask "if the implementation silently returned current state instead of historical state, would my assertions fail?" If not, the test is happy-path regardless of coverage (see Code Review Lessons).
4. **State whether the MR is mergeable**: List blocking issues (tests that fail, wrong numbering, missing parity) and non-blocking issues separately.

## Project Overview

**Temporal Knowledge Graph v3** — an internal Go library providing the core graph engine for temporal knowledge graphs. Pure library (no main binary, no HTTP server, no query language).

Module: `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3`
Go: 1.26.1 | License: Apache-2.0
Dependencies: `rho-snowflake-2026` (IDs), `msgpack/v5` (serialization), `badger/v4` (persistence)
Status: v3.2.0 | Phases: 1a-1g, 2a-2i, 3a-3e, 4.1-4.23 (complete) + typed entity IDs (v3.1.8) + IndexProvider/HashableValue extension points (v3.1.9) + history-aware indexed candidate planning + batch hardening (v3.1.10) + refArchive parity in indexed/bulk reads + Close-race protection (v3.1.11) + admin-path event-shard pinning + ArchiveNode/RestoreNode g.mu.Lock (v3.1.12) + DeepCopier interface + RegisterPropertyStructType safety (v3.1.13) + ImportGraph panic safety + RunRepair error propagation (v3.1.14) + SearchNearestNodes QueryOpts + k≤0 panic (v3.1.15) + Clear flush race + sync.Map race + missing index resets (v3.1.16) + restructure phase 1 (Store contract / keys / wire / snowflake layout in `pkg/graph/internal/store`) (v3.1.17) + restructure phase 2 (`internal/locks`, `internal/index`, temporal-filter helpers in `internal/store`) (v3.1.18) + restructure phase 3 (`internal/memorystore`, `internal/badgerstore`, `internal/tieredstore`) (v3.1.19) + vector index stale after UpdateNode + batch index maintenance + dead code removal (v3.1.20) + restructure phase 4 (`internal/events`) (v3.1.21) + restructure phase 5 (split integrity.go: helpers in internal/integrity, methods on Graph) (v3.1.22) + restructure phase 6 (`internal/snowflake`, `internal/registry`, `internal/temporal`) + graph.go/context.go/temporal.go file-splits + IndexProvider redesign + Relationship temporal-query parity + BadgerStore.SaveRegistries (v3.1.23) + phase 7a public-API consolidation: `aliases.go` split into themed files (`store.go`, `events.go`, `ontology.go`, `snowflake.go`, `errors.go`, `backends.go`), `MigrateFromBadger` signature simplified, `ComputeNodeHash`/`ComputeRelHash`/`LabelRegistry`/`RelTypeRegistry`/TieredStore catalog types/`RelDeleteInfo` demoted (v3.2.0). See CHANGELOG.md for version history.

## Build & Test Commands

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector — always run for concurrent code
make test-integration  # integration tests (long-running)
make cover          # coverage report -> coverage.html
make check          # pre-commit: vet + build + test
make ci             # full pipeline: fmt-check + vet + build + test-race + security + vulncheck
make fmt            # format code
make security       # gosec static analysis
make vulncheck      # govulncheck for known CVEs
```

Single test: `go test -run TestFoo ./pkg/types/`
Coverage check: `go tool cover -func=coverage.out` (after `make cover`)

## Testing Rules (hard requirements)

These rules exist because every single one was violated at least once. Do not skip them.

1. **Every public method gets a direct test.** Indirect coverage via delegation does NOT count. Any public method at 0% is a blocker.
2. **Node and Relationship must have test parity.** These types are structural mirrors.
3. **Every type-switch branch gets its own test.** If a branch shows 0% in `cover`, add a test.
4. **Sentinel errors are tested with `errors.Is`, not just `err != nil`.** At every call layer.
5. **Fallback/reflect paths must be tested or removed.**
6. **Deep copy means deep copy.** Must truly clone all nested reference types.
7. **Run `make cover` before marking any step complete.** Any public method at 0% or new code below 80% is a blocker.
8. **Validation must be recursive/adversarial.** `[]any{&myStruct{}}` must be rejected.
9. **Test nil values in reflect-based code.** `reflect.ValueOf(nil)` returns zero `reflect.Value`. `SetMapIndex` with zero Value deletes the key.
10. **One-time warnings must use `sync.Once`.** Never use `>=` for one-shot warnings.
11. **No empty stubs when the spec defines the fields.**
12. **Public method return types must not leak dependencies.** Use `type nodeID snowflake.ID`, NOT `type nodeID int64`.
13. **Config fields must be used or removed.**
14. **DO NOT use sub-millisecond or millisecond `ShardWindow` in tests.** ShardWindow boundaries are truncated to millisecond precision for alignment with snowflake timestamp extraction, so sub-second windows create boundary gaps. Use 1-week window (`newTestTieredStore`) and test cold/warm via manual rotation + `demoteToCold` helper.
15. **History-aware code needs two-phase tests.** Any method that answers a question about the past, a different version, or a comparative state (`*ValidAt`, `*ValidDuring`, `*AsOf`, `Verify*Chain`, `Snapshot`, `Diff`, `*At`, or any `*ByLabel`/`*ByType` accepting temporal `QueryOpts`) requires at least one test that (1) creates an entity in state X at t0, (2) mutates it after t0, (3) queries with t = t0 and asserts the result reflects X, not the post-mutation state. Single-mutation tests verify the API exists; only mutation-then-query tests verify it remembers. See `tasks/lessons.md` B30 (per-version metadata) and B31 (two-phase tests).
16. **Adversarial test shape, not happy-path.** Multi-entity scenarios with diverging lifecycles. Exact-set assertions (`assertNodeSet`/`assertRelSet`) catch over-reporting and omission. Negative assertions ("must NOT contain Y", "phantom value returns empty"). For interval queries, the "predicate-anywhere-in-interval" case (a node whose label held during part of the interval but not on the most-recent version) MUST be one of the asserted scenarios.
17. **When fixing a temporal-flavored named method, audit the generic equivalent.** Two doors, same shape: a fix in `Get*ValidAt(...)` (named method) without the matching fix in `*By*(opts QueryOpts)` (generic with temporal opts) leaves the bug behind a different door. Grep for `g\.store\.\w+ByLabel\|g\.store\.\w+ByType` in any file you touch — that's the generic-API door.

## Architecture

### `pkg/types`

| File | Purpose |
|---|---|
| `node.go` | Node (graph vertex, 80B) — `nodeID` wrapping `snowflake.ID`, labels as `labelToken`, properties, version, temporal, integrity |
| `relationship.go` | Relationship (directed edge, 72B) — `relID`, `relTypeToken`, start/end as `nodeID`, properties, version, temporal, integrity |
| `propertyslice.go` | Sorted key-value store with binary search; recursive allowlist validation; depth-limited to 32 levels; `[]float32` support; `deepCopyValue` dispatches to `DeepCopier.DeepCopyValue()` for registered types before the generic type switch |
| `property_registry.go` | `RegisterPropertyStructType(v any) error` — validates `HashableValue` + `DeepCopier` at registration; `DeepCopier` interface (`DeepCopyValue() any`); `ErrTypeNotHashable`, `ErrTypeNotDeepCopyable` sentinels |
| `shadow.go` | Constants for virtual read-only `tkg_*` properties |
| `temporal.go` | `Instant` type (Unix ms), `entityID`, `TemporalMetadata` struct |
| `integrity.go` | `NodeIntegrity` / `RelIntegrity` — hash chain (`Hash`, `PrevHash`) |
| `allen.go` | Allen's 13 interval relations — `AllenRelation`, `AllenRelationSet`, `Relate()`, `Compose()`, `ComposeSets()`, composition table |
| `granularity.go` | `TimeGranularity` (8 levels), `TruncateInstant`, `RoundInstant`, `CeilInstant` — ISO 8601 week truncation |
| `recurrence.go` | `RecurrencePattern`, `RecurrenceFrequency`, `WeekdayMask`, `Interval` — `Validate()` + `Expand(from, to)` |

### `pkg/graph`

After the v3.1.22-v3.1.23 restructure and the v3.2.0 public-API consolidation, `pkg/graph/` is a thin orchestration layer on top of the `internal/*` subpackages. The Graph struct lives in a 48-line file; the rest is split by concern. Persistence, indexes, and registries live entirely under `pkg/graph/internal/`. The single public `aliases.go` is gone — every public re-export now lives in a themed file (`store.go`, `events.go`, `ontology.go`, `snowflake.go`, `errors.go`, `backends.go`, `temporal_constraint.go`).

| File | Purpose |
|---|---|
| `graph.go` | `Graph` struct only (48 LOC). The struct holds dual snowflake generators, registry pointers, entity-lock manager, validation limits, async/sync event bus, and the package-private `txEventBuffer`. |
| `config.go` | `Config` and `ValidationLimits` types. |
| `lifecycle.go` | `New`, `Close`, registry persistence wiring (`registriesPersister` interface for both `BadgerStore` and `TieredStore`). |
| `validation.go` | Name + property validation helpers (`validateName`, `validateProperties`, `validatePropertyEntry`). |
| `resolution.go` | `NodeLabels`, `RelationshipType`, label/reltype string resolution helpers. |
| `crud.go` | Exported short-form `AddNode`/`AddRelationship`/`UpdateNode`/`DeleteNode`/`GetNode`/`GetRelationship` wrappers (delegate to `*WithContext`). |
| `property_cas.go` | `CompareAndSetProperty` — atomic CAS on a node property using `reflect.DeepEqual`. |
| `queries.go` | `Outgoing*`/`Incoming*` adjacency queries; `AllNodes`, `AllRelationships`, `NodesByLabel`, `RelationshipsByType`. |
| `graph_indexes.go` | Public index management — `CreatePropertyIndex`, `DropPropertyIndex`, `ListPropertyIndexes`, `CreateHighFrequencyIndex`, `DropHighFrequencyIndex`. |
| `vector_search.go` | `CreateVectorIndex`, `DropVectorIndex`, `SearchNearestNodes`. |
| `graph_property_query.go` | Property-indexed reads (`NodesByLabelAndProperty` and time variants). |
| `admin.go` | Tiered admin pass-throughs: `ListShards`, `ForceRotate`, `ArchiveNode`, `RestoreNode`, `RunRepair`, `VerifyShard`, `MigrateFromBadger`, `DecomposeID`. `ArchiveNode`/`RestoreNode` acquire `g.mu.Lock()` for tx-class exclusion. |
| `events_dispatch.go` | `dispatchEvent`, `publishEvent`, `SetEventBus`, `SetAsyncEventBus`. |
| `node_label.go` | `AddNodeLabel`, `RemoveNodeLabel` — post-creation label-set mutation with history + hash chain. |
| `version_chain.go` | `CloseNodeVersion`, `CloseRelVersion` — close the open-ended `ValidTo` of the current version without writing a new version. |
| `store.go` | Public re-exports for the persistence contract: `Store`, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric` plus the 12 store-layer sentinel errors and the `DepthAll`/`DepthHot`/`DepthWarm` + `DistanceCosine`/`DistanceEuclidean` constants. |
| `events.go` | Public re-exports for the lifecycle bus: `Event`, `EventType`, `EventPriority`, `EventHandler`, `EventBus`, `AsyncEventBus`, `AsyncEventBusConfig`, `BackpressureStrategy` + the six `EventNode*`/`EventRel*`, five `Priority*`, three `Backpressure*` constants, and the `NewEventBus`/`NewAsyncEventBus` constructors. |
| `ontology.go` | Public re-exports for the label-class taxonomy: `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, `ClassEvent`/`ClassReference`. |
| `snowflake.go` | Public re-export `IDComponents`/`DecomposeID` plus the package-level `snowflakeEpoch`/`snowflakeLayout` helpers used inside `pkg/graph/`. |
| `errors.go` | Vector-index sentinels (`ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`) and registry sentinels (`ErrEmptyName`, `ErrRegistryNotEmpty`). |
| `backends.go` | Concrete `Store` implementations: `MemoryStore`/`NewMemoryStore`, `BadgerStore`/`BadgerStoreConfig`/`NewBadgerStore`, `TieredStore`/`TieredStoreConfig`/`NewTieredStore`, the admin return types `ShardInfo`/`VerifyResult`/`RepairResult`, the four TieredStore-specific sentinels, and the `MigrateFromBadger(src, dst)` helper. |
| `context.go` | `*WithContext` helpers (`checkCtx`, `extractProvenance`, `extractTemporal`, `nowInstant`, etc.). 145 LOC; entity-mutation methods live in the eight `context_node_*.go` / `context_relationship_*.go` files. |
| `context_node_add.go` | `AddNodeWithContext`, `addNodeInternal`, `ImportNodeWithID`, `importNodeWithIDInternal`. |
| `context_node_update.go` | `UpdateNodeWithContext`, `updateNodeInternal`, `UpdateNodeInPlace`, `UpdateNodeInPlaceWithContext`, `updateNodeInPlaceInternal`. |
| `context_node_read_delete.go` | `GetNodeWithContext`, `DeleteNodeWithContext`, `deleteNodeInternal`, `collectDeleteIDs`, `sameIDSet`, `deleteNodeLocked`. |
| `context_relationship_add.go` | `AddRelationshipWithContext`, `addRelationshipInternal`. |
| `context_relationship_read_delete.go` | `GetRelationshipWithContext`, `DeleteRelationshipWithContext`, `deleteRelationshipInternal`. |
| `context_relationship_update.go` | `UpdateRelationshipWithContext`, `updateRelationshipInternal`. |
| `context_relationship_import.go` | `ImportRelationshipWithID`, `importRelWithIDInternal`. |
| `batch.go` | `BatchBuilder` — fluent API with eager validation and deferred persistence. |
| `tx.go` | `GraphTx` — full CRUD transaction holding the graph write lock; snapshot-based rollback; `labelDeltas` tracker for label-index rollback consistency. |
| `export.go` | `ExportGraph`/`ImportGraph` — length-prefixed msgpack record stream; `validateNodeWire`/`validateRelWire` guard the untrusted boundary. |
| `integrity.go` | `Graph.VerifyNodeHashChain` / `Graph.VerifyRelHashChain` — hash chain verification. Pure hash primitives live in `pkg/graph/internal/integrity/`. |
| `temporal.go` | Internal helpers shared by the temporal-query files (`mergeIDs`, history-aware merging). 446 LOC. |
| `temporal_queries.go` | Temporal point-in-time and interval query methods (`GetNode*ValidAt`, `Nodes*During`, etc.). |
| `temporal_snapshot.go` | `Snapshot`, `GraphSnapshot`, snapshot helpers. |
| `temporal_diff.go` | `Diff`, snapshot diffing. |
| `temporal_allen.go` | Allen's-algebra graph integration — `NodeInterval`, `RelInterval`, `RelateNodes`, `RelateRels`. |
| `temporal_constraint.go` | Public re-exports for the temporal-constraint vocabulary (`TemporalConstraintKind`, `TemporalConstraint`, `ConstraintSet`, `ConstraintRelWithinEndpoints`, `NewConstraintSet`, the seven sentinel errors) plus the Graph-coupled enforcement methods (`checkTemporalConstraints`, `checkRelWithinEndpoints`, `checkRelAgainstEndpoint`). The pure types' canonical declaration lives in `internal/temporal/`. |
| `temporal_index.go` | In-memory interval index — `temporalIndex` (sorted slice with lazy sort), `addNodeToTemporalIndexes`, `removeNodeFromTemporalIndexes`. |
| `txtime.go` | Bitemporality — `GetNodeAsOf`, `GetRelAsOf`, `Get*sAsOf`, `ErrNoVersionAsOf`. |
| `stats.go` | `GraphStats` (8 atomic operation counters + 4 cache metrics), `StoreStats` optional interface. |
| `shadow.go` | `ResolveNodeProperty` / `ResolveRelProperty` — dispatches 21 `tkg_*` shadow keys. |
| `index_provider.go` | Public `IndexProvider` interface (Phase 6 redesign) + optional `Initializable` hook + narrow `GraphReader` read surface; `LegacyIndexProvider` adapter for backward compat. |
| `doc.go` | Package documentation. |

### `pkg/graph/internal/*` subpackages

| Package | Purpose |
|---|---|
| `internal/snowflake` | Package-level snowflake `Epoch` + `Layout` + `IDComponents` + `DecomposeID`. Imported by `internal/locks`, `internal/store`, `internal/tieredstore`, `internal/badgerstore` (via test fixtures). Avoids `pkg/graph` import cycles. |
| `internal/store` | `Store` interface contract, sentinel errors, `QueryOpts`, `ShardDepth`, `RelTombstone`, key encoding (`NodeKey`, `RelKey`, `LabelIndexKey`, etc.), msgpack wire types (`NodeWire`, `RelWire`), pagination helpers (`PaginateIDs`, `PaginateNodes`, etc.), temporal-filter push-down (`EntityValidFrom`, `MatchesTemporalFilter`). |
| `internal/locks` | `Manager` — 256-shard entity-lock manager, `LockEntity`/`LockTwo`/`LockMany` in ascending order. |
| `internal/registry` | `LabelRegistry` and `RelTypeRegistry` — thread-safe string-to-uint16 token registries. `ErrEmptyName`, `ErrRegistryNotEmpty`, `TokenCapacityMax`. Re-exported from `internal/index` for backward compat. |
| `internal/index` | In-memory indexes: property index, vector index, high-frequency temporal index, `OntologyMapping` (label-class classification). Re-exports `LabelRegistry` and `RelTypeRegistry` from `internal/registry`. |
| `internal/integrity` | Pure SHA-256 hash primitives — `ComputeNodeHash`, `ComputeRelHash`, `appendProperties`, `appendPropertyValue`, `hashBufPool`. Five fixed-vector hash anchors lock the on-disk hash format. |
| `internal/events` | Lifecycle event delivery — `EventType`/`Event`/`EventPriority`, sync `EventBus`, async `AsyncEventBus` (worker pool, per-priority queues, `BackpressureStrategy`), `Publisher` interface. |
| `internal/temporal` | Pure constraint types — `TemporalConstraintKind`, `TemporalConstraint`, `ConstraintSet`, seven sentinel errors, `NewConstraintSet`. |
| `internal/memorystore` | Thread-safe in-memory `Store` implementation with hash-set indexes. |
| `internal/badgerstore` | Persistent `Store` implementation backed by Badger v4. LRU caches with dirty tracking, async `WriteBatch` flush, background GC, `SaveRegistries` for atomic single-txn registry persistence. |
| `internal/tieredstore` | Multi-shard `Store` implementation routing across reference shard + time-windowed event shards + optional reference archive. `checkoutStore`/`checkinStore` for cold-shard ref-counting, sequential ForEach, cross-shard split-write ordering, archive/restore, repair, migration. |

### Configuration

- **`Graph.Config`**: `SnowflakeNodeID` (0-15), `Store`, `BadgerDir`, `BadgerInMemory`, `Validation` (ValidationLimits). Whitespace-only `BadgerDir` rejected.
- **`ValidationLimits`**: `MaxLabelsPerNode` (50), `MaxPropertiesPerEntity` (1000), `MaxPropertyKeyLength` (256), `MaxPropertyValueSize` (64K strings), `MaxNameLength` (256). `AllowSelfLoops` (default `false` — reject self-loop relationships where start == end; set `true` to permit). Zero = default for numeric limits.
- **`BadgerStoreConfig`**: `Dir`, `InMemory`, `Logger` (Badger logger, nil uses default), `CacheCapacity` (10K), `FlushInterval` (100ms), `GCInterval` (5min), `GCDiscardRatio` (0.5), `ReadOnly` (for warm/cold shards), `SyncWrites` (fsync after every write — disables async buffer, forces FlushInterval=0), `Compression` (`options.None`/`Snappy`/`ZSTD`, zero = Badger default Snappy), `ZSTDCompressionLevel` (1-15, zero = Badger default 1).
- **`Graph.Config`**: also accepts `SyncWrites bool`, `Compression`, `ZSTDCompressionLevel` which pass through to `BadgerStoreConfig`.
- **`TieredStoreConfig`**: `DataDir`, `InMemory`, `RefLabels`, `ShardWindow` (1 week), `CacheCapacity` (10K), `FlushInterval` (100ms), `ColdAfter` (0=never), `IdleTimeout` (5min when cold enabled), `Compression` (applied to all shards, zero = Badger default Snappy), `ZSTDCompressionLevel` (1-15, zero = Badger default 1).

### Snowflake Configuration

Both generator sets (nodes and relationships) are initialized with explicit parameters matching the v1.3.0 microsecond precision layout:

```text
+---------------------------------------------------------------+
|  1 bit  |       48 bits        |   5 bits   |     10 bits     |
|  zero   |     time (usec)      |   node ID  |    sequence     |
+---------------------------------------------------------------+
```

| Parameter | Value |
|-----------|-------|
| Epoch | `2026-01-01 00:00:00 UTC` |
| Precision | Microseconds (`snowflake.WithMicroseconds()`) |
| Node bits | 5 (max `SnowflakeNodeID` is 15 since it maps to `id*2` and `id*2+1`) |
| Step bits | 10 (1024 unique IDs per microsecond) |

Each concurrent graph instance **must** use a different `Config.SnowflakeNodeID` (0-15).

## Design Rules

### Data Model

- **Pure-data structs**: Node/Relationship never hold references to Graph, registries, or resolvers. String resolution is always the Graph layer's responsibility.
- **snowflake.ID everywhere**: All IDs are `snowflake.ID` wrapped in opaque types (`nodeID`, `relID`, `entityID`). Never use `int64` or `string` for entity IDs.
- **Dual generators**: Nodes use even node field (`SnowflakeNodeID*2`), rels use odd (`*2+1`). Guarantees value-level uniqueness. Range: 0-15 (16 instances). Epoch: `2026-01-01`.
- **Strict encapsulation**: All fields unexported. Access through methods only.
- **Struct alignment**: Node (80B), Relationship (72B) packed by descending alignment. Verify with `unsafe.Sizeof`.
- **Token 0 reserved**: `HasLabelToken(0)` and `HasTypeToken(0)` always return false.
- **Validate before generating IDs**: `AddNode`/`AddRelationship` validate before `NextNodeID()`/`NextRelID()`.

### Defensive Copying

- **Accessors**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, `PropertiesMap()`, `ToMap()`, `DeepCopy()` always return independent copies.
- **Store boundary**: `Put*` deep-copy before caching; `Get*` deep-copy on return. Callers and store never share pointers.
- **Exception**: `Temporal()` and `Integrity()` return internal pointer (graph layer needs mutation access).

### Properties

- **Allowlist validation**: Recursive at insertion time. Primitives, slices, maps with safe elements only. Depth-limited to 32 (`ErrMaxDepthExceeded`).
- **`tkg_` prefix reserved**: `PropertySlice.Set()` rejects any key starting with `tkg_`.
- **Sorted invariant**: Always use `Set()` — never modify the slice directly.
- **Bulk construction**: `NewPropertySlice(map)` is O(N log N). `SetProperties(ps)` assigns directly. `AddNode`/`AddRelationship` use this path.

### Concurrency

- **Entity locks**: 256-shard `entityLockManager` for write-skew prevention. `shardIndex` uses low 8 bits of snowflake timestamp via `snowflakeLayout.Decompose(id).Time`.
- **Lock ordering**: entity locks -> idxMu. Always.
- **Two-phase delete with TOCTOU retry**: Phase A reads adjacency under node lock. Phase B locks all entities via `LockMany`, re-verifies adjacency, retries if changed (max 10).
- **Ascending shard order**: `LockTwo` normalizes. `LockMany` deduplicates + sorts. Deadlock-free.
- **Transaction isolation via g.mu**: `Graph.mu` serializes tx/batch (Lock) vs standalone mutations and reads (RLock). All exported mutation methods (`*WithContext`, `RemoveNodeLabel`, `CloseNodeVersion`, `CloseRelVersion`) acquire `g.mu.RLock()`. Tx/batch call unexported `*Internal` variants directly under `g.mu.Lock()`. Admin ops that read adjacency and cascade (`ArchiveNode`, `RestoreNode`) also acquire `g.mu.Lock()` to prevent concurrent writers from interleaving with the pre-scan. Individual temporal query methods do NOT acquire `mu` (avoids reentrancy deadlock).
- **sync.RWMutex is NOT reentrant**: If A holds RLock and calls B which RLocks, and a writer waits between them, deadlock. Inner methods must be lock-free.
- **sync.Once for idempotent Close()**: Never nil-guard a function pointer across goroutines.

### Persistence

- **Store is pure persistence**: Shadow and string resolution are Graph-layer responsibilities. All queries return `error` and sort by ID.
- **Async batch**: Writes update in-memory immediately, queue `writeOp` structs (last-write-wins dedup). Background flush via `WriteBatch` every `FlushInterval`.
- **LRU dirty tracking**: Dirty entries never evicted (soft capacity). `CollectDirty()` is read-only. `MarkFlushed()` is CAS on `dirtyVer`. `Peek()` for zero-alloc cache hits.
- **Tombstones in cache-first**: A cache miss must not fall through to stale Badger data.
- **Close() must flush unconditionally**: Even when `flushLoop` was never started (InMemory mode).
- **In-memory state must survive restart**: If it's in memory and it matters, it needs a persistence path and a rebuild path. If `loadIndexes()` doesn't rebuild it, it doesn't survive restart.
- **Counters in same WriteBatch as data**: Separate transactions = crash inconsistency.
- **Badger WriteBatch.Flush() blocks forever on closed DB**: Badger v4 uses `context.Background()` in `oracle.readTs()` → `WaitForMark()`, so closing the DB stops oracle goroutines and any in-flight `WriteBatch.Flush()` blocks forever. Fix: `BadgerStore.dbClosed atomic.Bool` is set to `true` in `Close()` BEFORE `db.Close()`. `flush()` checks it and returns `ErrDBClosed` immediately. Any test that calls `bs.db.Close()` directly MUST set `bs.dbClosed.Store(true)` first. Never call `WriteBatch.Flush()` after `db.Close()`.

### Version History

- **Pre-mutation snapshots**: `Update*` captures deep copy, applies mutations, writes both via `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` atomically.
- **Append-only**: Delete paths save tombstone versions (with `DeletedAt`/`ValidTo`) before deletion. History is never erased on delete.
- **Replace vs Put**: Put rejects duplicates. Replace requires existence. Replace overwrites data only — no index changes (labels/type/endpoints are immutable).
- **Genesis detection**: Use `entry.Version() == 0`, not array position. Array position changes after truncation.

### Temporal Queries

- **Effective valid-from**: Derived from explicit `ValidFrom` or snowflake ID timestamp. Every entity is queryable temporally without `SetTemporal()`.
- **Point-in-time**: `effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t)`.
- **Interval overlap**: `effectiveValidFrom < end AND (ValidTo == 0 OR ValidTo > start)`.
- **History-aware merging**: Temporal queries merge current + history IDs via lazy ForEach iterators (two-phase: collect IDs under store locks, process after release).
- **ForEach for OOM-safe iteration**: Never materialize all per-shard slices + merge. Use `ForEach*ID` callbacks. Constraint: callback must NOT call store methods (deadlock via B15). Two-phase: collect IDs, then process. ~83% memory reduction.
- **Deleted entity verification**: Any verification reading entity state must tolerate deletion — if entity has history but no current state, proceed using history alone.

### TieredStore

- **Timestamp routing**: Resolve the actual shard via snowflake ID timestamp or ref probe — never by `EntityClass`. Class tells you where new entities go; shard tells you where existing ones live.
- **Cross-shard split-write ordering (section 12)**: E->R: write ref shard inIdx first. R->E: write entity shard first. Both endpoints verified before any writes.
- **Checkout/checkin for cold shards**: `getStore()` returning a pointer without ref counting races with `closeIdleShards()`. Use `checkoutStore()`/`checkinStore()` to increment/decrement `activeReqs`.
- **Rotation boundary alignment**: Truncate to millisecond + add one unit. Nanosecond precision creates gaps with ms-resolution snowflake IDs.
- **Catalog sync on rotation**: Both in-memory `eventShard.timeEnd` AND catalog `ShardEntry.TimeEnd` must be updated on hot->warm rotation. Catalog is persisted — warm shard recovery depends on it.
- **Rollback on partial failure**: Cross-shard moves (archive/restore) must undo completed steps when a later step fails. Otherwise partial failure leaves data duplicated or orphaned.
- **Sequential ForEach**: One shard open at a time via checkout/checkin. No goroutines, no `mergeIDSlices`. Trades parallelism for memory safety.
- **Property indexes on reference entities only**: `CreatePropertyIndex` rejects event labels (`ErrEventPropertyIndex`).
- **Primary-label class is immutable**: `AddNodeLabelToken{,WithHistory}` and `RemoveNodeLabelToken{,WithHistory}` reject any mutation that would change the primary label's ontology class (reference ↔ event) and return `ErrPrimaryLabelClassMutation`. The check is enforced at the `TieredStore` Store-impl boundary only — `MemoryStore` and `BadgerStore` are single-shard and don't care. If you add another sharded backend, replicate the guard there. The reason: routing decisions depend on primary-label class; flipping the class would leave the live entity on its original shard while subsequent history snapshots route to a different shard, fragmenting the version chain. See lessons.md B33.

### Integrity & Indexes

- **Canonical hash inputs**: Hash canonical internal state, not raw user input. Token deduplication/normalization happens during construction — hash must reflect the canonical state.
- **Index cleanup on corruption**: When a corruption fallback skips entity data, it must still clean ALL indexes (label, property, adjacency). Leaving stale entries causes phantom results.
- **3-phase index creation**: (1) Install empty placeholder under Lock (visibility for concurrent writes), (2) unlocked I/O to build, (3) Lock to install with dirty-map tracking (`mutated[id]` not `contains(id)` — prevents re-adding concurrently deleted values).
- **API design rules**: Config fields must be used or removed. Opaque wrappers must wrap the real type. Graph is sole external API. Doc comments must match behavior.

### Events & Stats

- **EventBus is opt-in**: `Graph.SetEventBus(bus)` — nil by default (zero overhead). Handlers are copied under RLock, invoked outside the lock (prevents deadlocks from re-entrant Graph calls in handlers).
- **Tx event buffering**: During a transaction (`txEventBuffer != nil`), `publishEvent` appends to a buffer instead of dispatching. On `Commit`, events are published after `g.mu.Unlock()` so handlers can safely call Graph read methods. On `Rollback`, buffered events are discarded — subscribers never see rolled-back mutations.
- **AsyncEventBus for async delivery**: `Graph.SetAsyncEventBus(bus)` — worker pool with per-priority `[5]chan Event` queues. `BackpressureStrategy` controls full-queue behavior (Block/DropOldest/DropLatest). `Close()` drains all pending events before stopping workers. `Graph.events` is typed as `eventPublisher` interface (unexported) — allows either bus type without breaking the external API.
- **EventPriority**: 5 levels — `PriorityNormal` (0, zero value), `PriorityHigh` (1), `PriorityCritical` (2), `PriorityLow` (3), `PriorityDeferred` (4). Graph assigns internally: creates→High, deletes→Critical, updates→Normal. Backward-compatible: existing `Event{}` literals default to PriorityNormal. Priority ordering in `AsyncEventBus` worker uses non-blocking drain per level (Critical first) before blocking select.
- **StoreStats opt-in**: Type-asserted in `Graph.Stats()` — avoids polluting the `Store` interface.
- **Atomic operation counters**: 8 `atomic.Int64` fields on Graph — incremented after every successful store write.

### Vector Indexes

- **Not persisted**: Vector indexes are rebuilt from node properties on restart. This is a documented limitation — acceptable for brute-force k-NN.
- **Store-level scope in TieredStore**: Vector indexes live at the `TieredStore` level (not per-shard) with their own `vectorIdxMu sync.RWMutex`.
- **Auto-maintenance**: All mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`) update vector indexes.

### Code Review Lessons

- **Use library APIs, never reimplement internals**: If a dependency provides an API (e.g. `snowflake.Node.Decompose()`), use it. Never duplicate internal knowledge like bit layouts or hardcoded shifts in the consumer. When the library changes its layout, hardcoded shifts break silently across dozens of call sites. The library's API encapsulates the layout — use it.
- **Every fix needs a grep audit**: When fixing a pattern in one call site, grep for the same pattern across all files. The canonical hash bug (A1) was fixed in `context.go` but missed in `batch.go`, requiring a second review round.
- **Fix descriptions must include exact signatures**: Telling a developer to "use lazy iterators" without specifying the callback shape led to 5 rounds of partial fixes for the OOM issue (C4). Specify the exact interface.
- **Review by feature, not by file**: Single-file reviews missed cross-file interactions. The `batch.go` hash bug was only caught when reviewing `batch.go`, not when reviewing `integrity.go` where the hash function lives.
- **Repair tools complement but don't replace correctness**: The cross-shard rollback issue (B7) was accepted as "mitigated" by `RunRepair` when it should have been fixed inline.
- **High coverage ≠ correct tests**: Phase 1c shipped "22 new tests, 100% coverage on all new functions" and the hash chain was still wrong for label mutations. Coverage measures whether lines executed, not whether the test would have caught the bug. After writing a test, ask: "If the implementation silently returned current state instead of historical state, would my assertions fail?" If not, the test is happy-path regardless of coverage.
- **Most-recent-overlap is wrong for predicate-during-interval**: a "during [start,end)" query that checks the predicate only on the most-recent overlapping version misses entities whose label/property held earlier in the interval. Use `findNodeVersionMatchingDuring(id, start, end, pred)` which scans all overlapping versions. Bug introduced in MR !2 for `NodesByLabelPropertyDuring`, fixed in the same code path that added `NodesByLabel(opts)`/`NodesByLabelAndProperty(opts)` history-aware temporal handling.

## Audit Checklists

Run these after any change to the relevant subsystem.

**Hash computation**: `grep -rn 'ComputeNodeHash\|ComputeRelHash' pkg/` — every call site must pass canonical (registry-resolved) labels/type, not raw input.

**Entity locks**: `grep -rn 'store\.Put\|store\.Delete\|store\.Replace' pkg/graph/` — every Store write must have `LockEntity`/`LockTwo`/`LockMany` before it.

**Index cleanup**: Every delete path handling missing entities must purge label, property, AND adjacency indexes. Brute-force purge is acceptable for corruption paths.

**Index creation visibility**: `CreatePropertyIndex` must install empty placeholder BEFORE I/O phase. Phase 3 must check `mutated[id]`, not `contains(id)`.

## Registries

Two independent registries with independent token namespaces. Methods: `GetOrCreate`, `Resolve`, `ResolveAll`, `Lookup`. `GetOrCreate` rejects empty strings (`ErrEmptyName`), warns at 60K tokens, errors at 65535. Persisted in Badger as `meta/label_tokens`/`meta/reltype_tokens`, or in TieredStore as `data/meta/registry.msgpack` (atomic write).

## Shadow Properties (21)

| Key | Type | Applies To | Category |
|---|---|---|---|
| `tkg_labels` | `[]string` | Node | Structural |
| `tkg_type` | `string` | Relationship | Structural |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both | Temporal |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both | Temporal |
| `tkg_created_at` | `Instant` | Both | Temporal (auto-derived from snowflake ID when unset) |
| `tkg_updated_at`, `tkg_deleted_at` | `Instant` | Both | Temporal |
| `tkg_created_by`, `tkg_updated_by` | `string` | Both | Provenance |
| `tkg_version` | `uint32` | Both | Provenance |
| `tkg_hash`, `tkg_prev_hash` | `string` | Both | Integrity |
| `tkg_base_entity` | `entityID` | Both | Version chain |
| `tkg_from_hash` | `string` | Relationship | Integrity — start-node hash at write time |
| `tkg_to_hash` | `string` | Relationship | Integrity — end-node hash at write time |
| `tkg_author_id` | `string` | Both | Provenance — caller-supplied author identifier |
| `tkg_signature` | `[]byte` | Both | Integrity — caller-supplied cryptographic signature |
| `tkg_authorized_by` | `string` | Both | Authorization — caller-supplied authorizing entity |
| `tkg_auth_level` | `uint8` | Both | Authorization — caller-supplied authorization tier |

## Badger Key Layout

| Key pattern | Purpose | Key size |
|---|---|---|
| `0x01/<8B nodeID>` | Node entity | 9B |
| `0x02/<8B relID>` | Relationship entity | 9B |
| `0x03/<2B labelToken>/<8B nodeID>` | Label index | 11B |
| `0x04/<2B relTypeToken>/<8B relID>` | Type index | 11B |
| `0x05/<8B startID>/<2B relType>/<8B endID>/<8B relID>` | Outgoing adjacency | 27B |
| `0x06/<8B endID>/<2B relType>/<8B startID>/<8B relID>` | Incoming adjacency | 27B |
| `0x07/<8B nodeID>/<8B version>` | Node version history | 17B |
| `0x08/<8B relID>/<8B version>` | Rel version history | 17B |
| `0x0F/*` | Metadata (registries, counters, prop index defs) | varies |

## Ecosystem

| Module | Role |
|---|---|
| `rho/tkg/v3` | Internal library — graph types, persistence, registries (this repo) |
| `rho/tkgd-v3` | Full product — Cypher engine, Vadalog reasoning, HTTP/gRPC server |
| `rho/kit` | Service toolkit — app builder, logging, tracing, resilience, database |

tkg/v3 does **not** depend on kit. tkgd-v3 depends on both.
