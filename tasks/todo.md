# tkg-v3 — Roadmap

## Status

Library at v3.0.35. Core engine complete (Phases 1-3). Now closing v2 feature gaps.

---

## Completed Work

| Version | Phase | What |
|---------|-------|------|
| v3.0.15 | 1a-1d | UpdateNode/Rel, version history, hash chains, bulk queries |
| v3.0.16 | 1e | FlushInterval fix, LRU evictClean O(1) |
| v3.0.17 | 1f | Batch operations (PutNodesBatch, BatchBuilder) |
| v3.0.18 | 1g | Context-aware operations (8 WithContext methods) |
| v3.0.19 | 2b | Hash chain verification |
| v3.0.20 | 2d | Per-label/type statistics (O(1) atomic counters) |
| v3.0.21 | 2a | Temporal queries (point-in-time, interval, snapshot) |
| v3.0.22 | 2c | Property indexes (CreatePropertyIndex, auto-maintenance) |
| — | 2e | Configurable ValidationLimits (5 limits, 5 sentinel errors) |
| v3.0.23 | 2-review | 6 fixes (hash chain truncation, history-aware queries, index persistence) |
| — | 2f | Cursor-based pagination (QueryOpts{Limit, After}) |
| — | 2g | Combined label+property+temporal queries |
| v3.0.25 | 2h | 5 architectural fixes (entity locks, TOCTOU retry, snapshot isolation) |
| — | 2h-ext | Temporal query push-down to Store layer |
| — | 2i | GraphTx (create-only transactions), Graph.Reset() |
| v3.0.26 | 3a | TieredStore: ref/event split, cross-shard rels, registry file |
| v3.0.27 | 3b+3c | Shard rotation (hot→warm), depth-aware reads, warm recovery |
| v3.0.28 | 3d | Cold shard tier, parallel queries, reference archive |
| v3.0.29 | 3e | Repair, admin API, verification caching, migration tool |
| v3.0.30 | bugfix | 5 fixes (idle-close race, shardForRelID, archive rollback, dirty-map, batch hash) |
| v3.0.31 | bugfix | OOM fix — lazy ForEach iterators (~83% memory reduction) |
| v3.0.32 | feat | ImportNodeWithID/ImportRelationshipWithID + GraphTx wrappers |
| v3.0.33 | review | Pre-release code review (1 BLOCKER + 10 MAJORs + 16 MINORs) |
| v3.0.34 | 4.1 | Allen's 13 interval relations |
| v3.0.35 | coverage | Carry-forward test gaps: ImportNames validation, wire.go helpers, flush() error path |

---

## Phase 4 — v2 Feature Parity

Engine-layer features present in tkg-2025-v2 but missing from rho/tkg-v3.

### 4.1. Allen's Interval Algebra ✓ (v3.0.34)

Complete. `types.AllenRelation` (13 relations), `AllenRelationSet` (uint16 bitset),
`Relate()`, `Compose()`/`ComposeSets()` (composition table), `NodeInterval`/`RelInterval`,
`RelateNodes`/`RelateRels`. 48 tests. No `EnforcePathConsistency` yet (deferred until
tkgd-v3 needs constraint networks).

### 4.2. Temporal Constraints — High

**v2:** `types.TemporalConstraint` and `ConstraintSet` — enforce rules like "relationship R
must exist within the validity of its endpoints".

**v3:** None. Temporal metadata stored but no constraint enforcement.

- [ ] `TemporalConstraint` type (relationship validity ⊆ endpoint validity)
- [ ] `ConstraintSet` for composing multiple constraints
- [ ] Enforcement hooks in AddRelationship / UpdateRelationship
- [ ] Constraint violation sentinel errors

### 4.3. Advanced Temporal Indexes — High

**v2:** `AdvancedTemporalIndex` (interval tree), `HighFrequencyIndex`, `TimeWindowIndex`,
`IndexManager` — specialized index structures for temporal queries.

**v3:** Property indexes only (`CreatePropertyIndex` / `NodesByLabelAndProperty`).
Temporal queries do full-scan with ForEach iterators.

- [ ] Interval tree data structure for `[ValidFrom, ValidTo)` ranges
- [ ] `CreateTemporalIndex(labelToken)` on Store interface
- [ ] Temporal push-down uses index when available, falls back to scan
- [ ] Integration with TieredStore per-shard indexes

### 4.4. Version Chain Navigation — Medium

**v2:** `GetPreviousNodeVersion()`, `GetNextNodeVersion()`, `CloseNodeVersion()`,
`VersionChain`, `RelationshipVersionChain` — navigate version history as a linked list.

**v3:** `GetNodeHistory(id)` returns flat `[]*Node` slice. No chain navigation, no CloseNodeVersion.

- [ ] `GetPreviousNodeVersion(id, version)` / `GetNextNodeVersion(id, version)`
- [ ] `CloseNodeVersion(id)` — set ValidTo on current version
- [ ] Rel mirrors

### 4.5. Event / Notification System — Medium

**v2:** Full `events` package — `Type` constants (NodeCreate, NodeUpdate, NodeDelete,
RelCreate, RelUpdate, RelDelete, etc.), `Event`, `Queue`, `Dispatcher`, `EventBus`,
`AsyncUpdater`, `Worker`, `WorkerPool`.

**v3:** None. No publish/subscribe, no lifecycle hooks.

- [ ] `EventType` iota (NodeCreate, NodeUpdate, NodeDelete, RelCreate, RelUpdate, RelDelete)
- [ ] `Event` struct with Type, EntityID, Timestamp
- [ ] `EventBus` with Subscribe/Publish
- [ ] Hook points in Graph CRUD methods

### 4.6. CRUD Diff Exporter — Medium

**v2:** `CRUDDiffExporter` — export a stream of create/update/delete diffs between two
points in time.

**v3:** `Snapshot(t)` gives full state at a point. No diff between snapshots.

- [ ] `DiffSnapshots(t1, t2)` — returns created/updated/deleted entity lists
- [ ] Efficient implementation via version history scan (not dual snapshot)

### 4.7. Recurrence Patterns — Low

**v2:** `types.RecurrencePattern` — express recurring temporal validity (e.g., "every Monday 9-17").

**v3:** None.

- [ ] `RecurrencePattern` type + expansion to concrete intervals

### 4.8. Time Granularity — Low

**v2:** `types.TimeGranularity` with levels from Millisecond through Year. Allows
coercion/rounding of temporal values.

**v3:** Raw `types.Instant` (int64 unix millis) only. No granularity abstraction.

- [ ] `TimeGranularity` enum + coercion/rounding of `Instant` values

### 4.9. VectorField Support — Low

**v2:** `types.VectorField` — store vector embeddings as entity properties for similarity search.

**v3:** Property values support basic Go types (`string`, `int64`, `float64`, `bool`,
`[]any`, `map[string]any`) but not vectors.

- [ ] New property type for float32/float64 vectors
- [ ] Similarity index (cosine, euclidean)

### 4.10. Remove Label from Node — Low

**v2:** `RemoveNodeLabel()` — remove a label from an existing node.

**v3:** Nodes are created with labels; no API to add or remove labels after creation.

- [ ] `Graph.RemoveNodeLabel(id, label)` — remove label, update indexes + hash chain

### 4.11. In-Place Update (No History) — Low

**v2:** `UpdateNodeInPlace()` — update a node without creating a version history entry.

**v3:** Every `UpdateNode()` creates a new version and stores the previous state in history.
No bypass.

- [ ] `UpdateNodeInPlace(id, updates)` — skip history write path
- [ ] Clear use-case justification (perf-critical counters, etc.)

### 4.12. Graph Stats (Cache Metrics) — Low

**v2:** `GraphStats` with cache hit/miss tracking, operation counters.

**v3:** Count methods (`NodeCount`, `RelationshipCount`, `AllLabelCounts`, `AllRelTypeCounts`)
but no cache hit/miss metrics or operation counters.

- [ ] Operation counters (AddNode, GetNode, UpdateNode, DeleteNode, etc.)
- [ ] Cache hit/miss metrics on BadgerStore LRU
- [ ] `Graph.Stats()` accessor

### Phase 4 Summary

| # | Feature | Priority | Effort |
|---|---------|----------|--------|
| ~~4.1~~ | ~~Allen's interval algebra~~ | ~~High~~ | ✓ Complete (v3.0.34) |
| 4.2 | Temporal constraints | High | Medium — constraint types + enforcement hooks |
| 4.3 | Advanced temporal indexes | High | Large — interval tree data structure |
| 4.4 | Version chain navigation | Medium | Small — add Next/Prev/Close methods |
| 4.5 | Event system | Medium | Medium — dispatcher + subscriber pattern |
| 4.6 | CRUD diff exporter | Medium | Medium — compare two snapshots |
| 4.7 | Recurrence patterns | Low | Small — type + expansion logic |
| 4.8 | Time granularity | Low | Small — enum + rounding functions |
| 4.9 | VectorField | Low | Medium — new property type + index |
| 4.10 | Remove label | Low | Small — single method |
| 4.11 | In-place update | Low | Small — skip history write path |
| 4.12 | Graph stats (cache metrics) | Low | Small — counters + exposure methods |

---

## Application Layer (Out of Scope for tkg-v3)

These v2 features belong in `tkgd-v3/` (server layer), NOT the core graph library.

| # | Feature | v2 Location | Notes |
|---|---------|-------------|-------|
| S1 | Cypher query engine (v1) | `cypher/` | Parser, AST, query plans, plan cache |
| S2 | Cypher query engine (v2) | `cypher2/` | Pratt parser, full AST, GraphV2Ops integration |
| S3 | Vadalog reasoning engine | `vadalog/` | Chase engine, stratified negation, query builder |
| S4 | HTTP REST server | `server/` | Endpoints for nodes, rels, cypher, history, stats, export/import, streaming |
| S5 | gRPC services | `services/ruleintelligence/` | Detection rule intelligence service |
| S6 | Authorization / RBAC | `types.SystemsTable`, `UserRecord`, `RolePermissions` | Access control |
| S7 | Memory management (exposed) | `memory.Manager` | Cleanup callbacks, manual GC triggers |
| S8 | API-level TKG wrapper | `api.TKG` with `Config` | High-level facade over graph + storage + server |

---

## Redesigned (v2 → v3)

Features present in both versions with different designs. Not missing — intentionally rearchitected.

| Feature | v2 | v3 |
|---------|----|----|
| Entity IDs | `string` | `snowflake.ID` (uint64, timestamp-embedded) |
| Hybrid persistence | `HybridStore` (RAM + disk + eviction) | `TieredStore` (ref/event shards, hot→warm→cold lifecycle) |
| Validation | Separate `validation` package | `ValidationLimits` struct in `graph` package |
| Errors | Separate `errors` package with codes | Sentinel `var` errors in `graph` package |
| Batch operations | Separate `batch` package | `BatchBuilder` in `graph` package |
| Entity types | `NodeV2` / `RelationshipV2` + `PropertyMap` | `Node` / `Relationship` + `PropertySlice` (sorted, binary search) |
| Transactions | `transaction.Transaction` with WAL | `GraphTx` with snapshot-based rollback (no WAL) |
| Persistence contract | `Store` + `StoreV2` + `Persister` (Sync/Async/Null) | Single `Store` interface (47 methods), persistence integrated |
| Deep copy | `GetNodeCopy()` / `GetRelationshipCopy()` | `node.DeepCopy()` / `rel.DeepCopy()` |
| Property removal | `RemoveNodeProperty()` | `DeleteNodeProperty()` (same semantics, different name) |
| Export/Import | Server-level endpoints | `ImportNodeWithID()` / `ImportRelationshipWithID()` (import only, at graph layer) |
| Query types | Separate `query` package (AST, Plan, Cost) | No query AST — direct Go method calls |

---

## Carry-Forward Coverage Gaps

- [x] wire.go `propertyTypeTag` — direct unit test covers all 24 branches (64% → 100%)
- [x] wire.go `toInt64`/`toUint64` — direct unit tests cover all 9 type cases each (54.5% → 100%)
- [x] wire.go `normalizeIntegersRecursive` — direct unit tests for int16/int32/uint8-32/default (40% → 100%)
- [x] badgerstore.go `flush()` — WriteBatch error path tested by closing DB before Flush() (73% → covered)
- [x] ImportNames — validation added for empty entries (index > 0) and duplicates; 4 new tests (label + reltype)
