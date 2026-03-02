# tkg-v3 — Roadmap

## Status

Library at v3.0.22. Phases 1a-1g and 2a-2d complete.

## Gap Analysis: tkg-2025-v2 vs rho/tkg-v3

Comprehensive comparison of v2's pkg/graph + pkg/storage against v3's current feature set.
v3 is architecturally superior (80B nodes, token interning, compound adjacency keys,
snowflake IDs, async batch persistence) but missing several v2 features needed before
the tiered persistence layer can be built.

### Key Schema Alignment (verified)

The current key schema already supports the tiered persistence spec:
- `0x05` (out): compound key supports basic/typed/counterpart prefix scans in 1 key
- `0x06` (in): same, 3 scan levels in 1 key
- History keys (`0x07`/`0x08`) promoted to production in `keys.go` (Phase 1b). Temporal keys (`0x09`/`0x0A`) remain forward-planned in `keys_helpers_test.go`
- **No key schema changes needed** for tiered persistence

### Existing Coverage Gaps (carry-forward)

- [ ] MINOR: wire.go `propertyTypeTag` at 56% — uint, uint8-32, float32 branches untested
- [ ] MINOR: wire.go `toInt64`/`toUint64` at ~50% — integer conversion branches untested
- [ ] MINOR: wire.go `normalizeIntegersRecursive` at 40% — backward-compat fallback untested
- [ ] MINOR: badgerstore.go `flush()` at 73% — WriteBatch error recovery paths untested
- [ ] MINOR: ImportNames — no validation for empty/duplicate entries in persisted data

---

## Phase 1 — Store Interface Completion (must-have before tiering)

Make tkg-v3 a fully usable graph engine. These operations are foundational —
the tiered store must implement them all.

### 1a. UpdateNode / UpdateRelationship ✓

Complete. Implemented in v3.0.15.

**Store interface:** `ReplaceNode(n)`, `ReplaceRelationship(r)` — overwrite existing entities.
**Graph layer:** `UpdateNode(id, updates)`, `UpdateRelationship(id, updates)` — read-modify-write with entity lock, version bump, UpdatedAt.
**Convenience:** `SetNodeProperty`, `DeleteNodeProperty`, `SetRelationshipProperty`, `DeleteRelationshipProperty`.

**44 tests total:** 6 MemoryStore Replace, 6 BadgerStore Replace, 28 graph-layer (13 UpdateNode + 11 UpdateRelationship + 4 convenience), 4 Badger integration (including persistence round-trip).
All pass with race detector. Coverage ≥89% on all new methods.

### 1b. Version History ✓

Complete. Implemented in v3.0.15.

**Store interface:** `PutNodeVersion`, `GetNodeVersion`, `GetNodeHistory`, `TruncateNodeHistory` + relationship mirrors. `ErrVersionNotFound` sentinel for missing versions.
**Graph layer:** `UpdateNode`/`UpdateRelationship` save pre-mutation state to history. `GetNodeHistory`/`GetRelHistory` passthroughs.
**Key promotion:** `keyHistNode` (0x07) and `keyHistRel` (0x08) promoted from test-only stubs to production. Added `histNodePrefix`/`histRelPrefix` for prefix scanning.
**Delete cleanup:** All delete paths (DeleteNode, DeleteNodeCascade, DeleteRelationship) clean up associated history entries. BadgerStore cascade uses three-phase approach.

**~50 tests total:** 17 MemoryStore (8 node + 9 rel), 19 BadgerStore (mirrored + 2 restart persistence), 14 graph-layer (5 node + 5 rel + 4 Badger persistence).
All pass with race detector.

### 1c. Hash Chain Computation ✓

Complete. Implemented in v3.0.15.

**Implementation:**
- [x] `ComputeNodeHash(n *types.Node, labels []string) string` — SHA-256 of id + version + sorted labels + sorted properties, hex-encoded (64 chars)
- [x] `ComputeRelHash(r *types.Relationship, typeName string) string` — SHA-256 of id + version + type + startID + endID + sorted properties
- [x] AddNode: compute hash, set integrity (PrevHash = "" for genesis)
- [x] UpdateNode: capture PrevHash from current integrity, compute new hash on final state
- [x] AddRelationship: compute hash, set integrity (PrevHash = "" for genesis)
- [x] UpdateRelationship: capture PrevHash from current integrity, compute new hash on final state

**22 tests total:** 10 unit tests (determinism, property/version/label/type/endpoint sensitivity, label order independence), 12 graph-layer integration tests (integrity set on create, hash chain linking, multiple-update chain, genesis zero PrevHash — node/rel parity).
All pass with race detector. 100% coverage on all new functions.

### 1d. Bulk Query Methods (AllNodes / AllRelationships / GetByIDs) ✓

Complete. Implemented in v3.0.15.

**Store interface additions:**
- [x] `AllNodes() ([]*types.Node, error)`
- [x] `AllRelationships() ([]*types.Relationship, error)`
- [x] `GetNodesByIDs(ids []snowflake.ID) ([]*types.Node, error)`
- [x] `GetRelationshipsByIDs(ids []snowflake.ID) ([]*types.Relationship, error)`

**Graph-layer passthrough:**
- [x] `Graph.AllNodes()`, `Graph.AllRelationships()`
- [x] `Graph.GetNodesByIDs(ids)`, `Graph.GetRelationshipsByIDs(ids)`

**Design decisions:**
- Missing IDs silently skipped (matches `NodesByLabel` orphan-skip pattern)
- `nil, nil` for empty results (consistent with all existing query methods)
- Pure Graph passthroughs (no string resolution needed)
- BadgerStore: snapshot IDs under `idxMu.RLock()`, fetch via public `GetNode`/`GetRelationship`
- MemoryStore: single `RLock`, iterate map, DeepCopy, sort

**32 tests total:** 12 MemoryStore (AllNodes empty/count/sorted, AllRels empty/count/sorted, GetNodesByIDs empty/found/sorted, GetRelsByIDs empty/found/sorted), 12 BadgerStore (mirrored), 8 graph-layer (AllNodes/AllRels empty + populated, GetNodesByIDs/GetRelsByIDs empty + skip-missing).
All pass with race detector. Coverage 82-100% on all new methods.

### 1e. FlushInterval Policy + LRU evictClean Fix ✓

Complete. Implemented in v3.0.16.

- [x] Fix `NewBadgerStore` FlushInterval defaulting — remove `!cfg.InMemory` condition
- [x] Add `cleanCount` field to `entityLRU` for O(1) early exit in `evictClean()`
- [x] Update all LRU mutation methods to maintain `cleanCount`
- [x] Add `CleanCount()` accessor for test verification
- [x] Add tests: `TestLRUEvictCleanSkipsWhenAllDirty`, `TestLRUCleanCountAccuracy`
- [x] Fix `TestBadgerStoreDirtyNotEvictedUnderPressure` — add large FlushInterval
- [x] Update `newTestBadgerStore` comment
- [x] Tutorial 005 — fair comparison via library fix + tutorial fixes (Close in bench timing, rels in memory measurement, log.Fatal→Printf in defer, distinct SnowflakeNodeIDs)

### 1f. Batch Operations ✓

Complete. Implemented in v3.0.17.

**Store interface additions:**
- [x] `PutNodesBatch(nodes []*types.Node) error` — two-phase atomic, all-or-nothing
- [x] `PutRelationshipsBatch(rels []*types.Relationship) error` — two-phase atomic
- [x] `DeleteNodesBatch(ids []snowflake.ID) error` — two-phase atomic
- [x] `DeleteRelationshipsBatch(ids []snowflake.ID) error` — two-phase atomic

**Graph-layer batch builder:**
- [x] `BatchBuilder` fluent API (`batch.go`)
  - `NewBatchBuilder(g *Graph) *BatchBuilder`
  - `.AddNode(labels, props)` — eager validation, deferred persistence
  - `.AddRelationship(typeName, startNode, endNode, props)` — eager validation
  - `.UpdateNode(id, updates)` / `.UpdateRelationship(id, updates)` — pre-validate
  - `.DeleteNode(id)` / `.DeleteRelationship(id)` — queue deletes
  - `.Execute() (*BatchResult, error)` — create → update → delete order
- [x] `BatchResult` with Created, Updated, Deleted, Failed, Errors, Duration
- [x] `BatchError` with Op, ID, Err

**41 tests total:** 12 MemoryStore, 12 BadgerStore, 17 BatchBuilder.
All pass with race detector. Coverage ≥80% on all new methods.

### 1g. Context-Aware Operations ✓

Complete. Implemented in v3.0.18.

- [x] `AddNodeWithContext(ctx, labels, props)` — 2 context checks (entry + before store write)
- [x] `AddRelationshipWithContext(ctx, typeName, startNode, endNode, props)` — 3 context checks (entry + before lock + before store write)
- [x] `UpdateNodeWithContext(ctx, id, updates)` — 5 context checks (entry + before lock + before read + before history + before write)
- [x] `UpdateRelationshipWithContext(ctx, id, updates)` — 5 context checks (mirror of UpdateNode)
- [x] `DeleteNodeWithContext(ctx, id)` — 2 context checks (entry + under lock before cascade)
- [x] `DeleteRelationshipWithContext(ctx, id)` — 1 context check (entry)
- [x] `GetNodeWithContext(ctx, id)` — 1 context check (entry)
- [x] `GetRelationshipWithContext(ctx, id)` — 1 context check (entry)
- [x] Existing methods refactored to delegate to WithContext with `context.Background()`
- [x] `checkCtx` helper — non-blocking select, zero overhead when context is not cancelled
- [x] 28 tests in `context_test.go` — all pass with race detector
- [x] No Store interface change — Badger v4 doesn't support context in its core API

---

## Phase 2 — Temporal Query Layer

Make tkg-v3 a proper temporal graph. The "T" in TKG.

### 2b. Hash Chain Verification ✓

Complete. Implemented in v3.0.19.

- [x] `VerifyNodeHashChain(id)` — verifies genesis PrevHash="", PrevHash chain links, recomputes hashes
- [x] `VerifyRelHashChain(id)` — mirrors node verification for relationships
- [x] 14 tests (7 node + 7 rel): genesis-only, multiple updates, tampered hash, broken PrevHash, non-existent, nil integrity, property change

### 2d. Per-Label / Per-Type Statistics ✓

Complete. Implemented in v3.0.20.

- [x] `NodeCountByLabel(label)`, `RelCountByType(typeName)` — scan-based cardinality counts
- [x] `AllLabelCounts()`, `AllRelTypeCounts()` — aggregated counts for all registered labels/types
- [x] 12 tests: empty/unregistered/single/multiple/after-delete for both, plus AllLabelCounts and AllRelTypeCounts
- [x] No Store interface change — delegates to existing NodesByLabel/RelationshipsByType

### 2a. Temporal Queries ✓

Complete. Implemented in v3.0.21.

- [x] `GetNodesValidAt(t)`, `GetRelationshipsValidAt(t)`, `GetNodesByLabelValidAt(label, t)` — point-in-time queries
- [x] `GetNodesValidDuring(start, end)`, `GetRelationshipsValidDuring(start, end)` — interval queries
- [x] `GetNodeAt(id, t)` — version-specific query with version chain derivation
- [x] `GetNeighborsValidAt(nodeID, t)` — temporal neighbor traversal
- [x] `Snapshot(t)` — full graph state at time t (endpoint-filtered relationships)
- [x] `GraphSnapshot` struct, `ErrNoVersionValidAt` sentinel, 6 internal helpers
- [x] 31 tests: 12 point-in-time, 6 interval, 5 version-specific, 3 neighbor, 5 snapshot
- [x] No Store interface change — scan-based filtering over existing methods

### 2c. Property Indexes ✓

Complete. Implemented in v3.0.22.

**Store interface additions:**
- [x] `CreatePropertyIndex(labelToken, propertyKey)` — creates in-memory index, scans existing nodes
- [x] `DropPropertyIndex(labelToken, propertyKey)` — removes index
- [x] `NodesByLabelAndProperty(labelToken, key, value)` — O(1) indexed lookup with fallback scan

**Implementation:**
- [x] `propertyValueKey` — type-prefixed canonical string for 14 primitive types
- [x] Auto-update hooks in all 7 node mutation paths (both MemoryStore and BadgerStore)
- [x] `ErrIndexExists` / `ErrIndexNotFound` sentinel errors
- [x] 25 tests: 8 MemoryStore, 8 BadgerStore, 8 Graph-layer, 1 propertyValueKey type coverage

---

## Phase 3 — Tiered Persistence

The multi-shard architecture from the Tiered Temporal Storage spec.
Built behind the Store interface — single-instance deployments unaffected.

### 3a. Snowflake + Registry + Reference/Event Split

- [ ] Ontology mapping (label → reference/event classification)
- [ ] TieredStore skeleton implementing Store interface
- [ ] Registry as flat msgpack file (tokens only, ~2KB)
- [ ] Reference shard (single BadgerStore, always hot)
- [ ] Hot event shard (single BadgerStore, current time window)
- [ ] Shard catalog (JSON, time windows, tier tracking)
- [ ] Entity routing by label classification
- [ ] Relationship key routing (in/ keys to reference shard, out/ keys to event shard)
- [ ] All 4 relationship patterns routed correctly (E→R, E→E, R→R, R→E)

### 3b. Timestamp Resolution + Split Writes + Read Fan-Out

- [ ] Timestamp extraction from snowflake IDs for shard resolution
- [ ] Full CRUD through TieredStore
- [ ] Cross-shard split-write with reference-first ordering
- [ ] Depth-aware reads (hot/warm/cold/all)
- [ ] Cache-miss routing: extract timestamp → target shard

### 3c. Rotation + Demotion

- [ ] Background rotation task (hot → warm on time window expiry)
- [ ] Warm → cold demotion (reopen with cheaper Badger options)
- [ ] Lazy-open cold shards on first access
- [ ] Idle-close after configurable timeout
- [ ] Parallel shard open within a query
- [ ] Mid-window restart handling (reopen existing hot shard from catalog)

### 3d. DEPTH Clause + Reference Archive

- [ ] Cypher parser extension (terminal clause)
- [ ] DEPTH applied at MATCH time (shard pruning before materialization)
- [ ] HTTP `?depth=` parameter override
- [ ] COUNT push-down returns total (no DEPTH filtering on count)
- [ ] Reference archive with case archival/restore
- [ ] Lazy-open archive on DEPTH=all queries

### 3e. Repair + Tooling

- [ ] Split-write repair scan (startup + background)
- [ ] Per-shard verification caching (immutable shards verified once)
- [ ] Property indexes for reference entities only
- [ ] Admin API: force rotation, list shards, archive/restore, rebuild catalog
- [ ] ID decomposition endpoint (creation time + generator + sequence)
- [ ] Migration tool for single-Badger → tiered deployments

---

## Implementation Order Rationale

Phase 1 before Phase 3 because:
1. The Store interface must be stable and complete before building the TieredStore
2. Every Store method becomes a routing decision in the tiered layer — adding methods later means reopening TieredStore
3. History storage uses keys `0x07`/`0x08` that need to be routed to the right shard
4. The tiered spec explicitly references version history, property indexes, and bulk ops

Phase 2 before Phase 3 because:
1. Temporal queries define how temporal index keys (`0x09`/`0x0A`) are written and scanned
2. The tiered spec's DEPTH clause filters by temporal data — the query layer must exist first
3. Property indexes are built "for reference entities only" — needs the index system in place

Key schema is already aligned — no changes needed for tiering.
