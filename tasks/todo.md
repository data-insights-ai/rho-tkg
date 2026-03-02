# tkg-v3 — Roadmap

## Status

Library at v3.0.28. Phases 1a-1g, 2a-2i, 3a, 3b+3c, and 3d complete. Phase 2 review (6 issues) resolved. Phase 2h (5 architectural fixes) complete. Store interface extensions (temporal query push-down + graph transactions) implemented. TieredStore with reference/event split, shard rotation (hot→warm→cold), warm recovery, depth-aware reads, E→E cross-shard fix, cold shard lifecycle (lazy-open, idle-close), parallel shard queries, and reference archive (archive/restore) implemented.

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
**Delete preserves history:** All delete paths preserve version history (append-only). `DeleteNodeWithContext`/`DeleteRelationshipWithContext` save tombstone versions with `DeletedAt`/`ValidTo` before deletion.

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

Complete. Initially implemented in v3.0.20 (scan-based O(N)). Upgraded to O(1) atomic counters.

**O(1) upgrade (post-v3.0.22):**
- [x] Store interface: `NodeCountByLabel(token uint16)`, `RelCountByType(token uint16)` — O(1) delegation
- [x] MemoryStore: `len(labelIdx[token])` / `len(typeIdx[token])` — trivial O(1) via existing index sizes
- [x] BadgerStore: `sync.Map` + `atomic.Int64` counters — maintained in 9 mutation sites, rebuilt from index sizes in `loadIndexes()`
- [x] Graph layer: O(1) delegation to Store (no longer materializes entities to count)
- [x] `AllLabelCounts()` / `AllRelTypeCounts()` use `uint16(i)` as token directly (avoids redundant registry lookups)
- [x] 8 MemoryStore counter tests, 8 BadgerStore counter tests (including persistence round-trip), 2 graph-level integration tests
- [x] All pass with race detector. 100% coverage on all new methods

### 2a. Temporal Queries ✓

Complete. Initially implemented in v3.0.21. Made history-aware in v3.0.23 (Phase 2 review).

- [x] `GetNodesValidAt(t)`, `GetRelationshipsValidAt(t)`, `GetNodesByLabelValidAt(label, t)` — point-in-time queries (history-aware: includes deleted entities)
- [x] `GetNodesValidDuring(start, end)`, `GetRelationshipsValidDuring(start, end)` — interval queries (history-aware)
- [x] `GetNodeAt(id, t)` — version-specific query with version chain derivation (handles deleted entities)
- [x] `GetRelAt(id, t)` — mirrors `GetNodeAt` for relationships (added in v3.0.23)
- [x] `GetNeighborsValidAt(nodeID, t)` — temporal neighbor traversal
- [x] `Snapshot(t)` — full graph state at time t (endpoint-filtered relationships, includes deleted entities)
- [x] `GraphSnapshot` struct, `ErrNoVersionValidAt` sentinel
- [x] Store interface: `AllNodeHistoryIDs()`, `AllRelHistoryIDs()` for history-aware queries (added in v3.0.23)
- [x] ~50 tests total: point-in-time, interval, version-specific, neighbor, snapshot, deleted entity queries, truncation resilience

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

### 2e. Configurable Validation Limits ✓

Complete. Implemented post-v3.0.22.

**ValidationLimits struct** on `Graph.Config` with generous defaults:
- `MaxLabelsPerNode` (50), `MaxPropertiesPerEntity` (1000), `MaxPropertyKeyLength` (256), `MaxPropertyValueSize` (65536), `MaxNameLength` (256)
- Zero values resolve to defaults in `New()`

**Sentinel errors:** `ErrTooManyLabels`, `ErrTooManyProperties`, `ErrKeyTooLong`, `ErrValueTooLarge`, `ErrNameTooLong`

**Enforcement:**
- [x] `AddNodeWithContext` — MaxLabelsPerNode, MaxNameLength (each label), validateProperties
- [x] `AddRelationshipWithContext` — MaxNameLength (type), validateProperties
- [x] `UpdateNodeWithContext` — pre-lock entry validation + post-mutation MaxPropertiesPerEntity check under lock
- [x] `UpdateRelationshipWithContext` — same pattern as UpdateNode
- [x] Batch builder mirrors all graph-layer checks (`AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`)

**Prerequisites:**
- [x] `PropertyCount()` on Node and Relationship — `properties.Len()` without deep copy

**~30 tests:** defaults, zero-uses-defaults, custom limits, boundary tests (at-limit succeeds, one-over fails) for all 5 limits on AddNode/AddRelationship/UpdateNode/UpdateRelationship, batch mirroring, all sentinel errors tested with `errors.Is`

### Phase 2 Review — 6 Issues ✓

Complete. Implemented in v3.0.23.

6 issues found during Phase 2 code review, resolved in order 5 → 6 → 4 → 3 → 2 → 1:

- [x] **Fix 5**: Hash chain verification — truncation resilience (`i==0` → `entry.Version()==0`)
- [x] **Fix 6**: GetNodeAt — truncation resilience (`i==0` → `entry.Version()==0`)
- [x] **Fix 4**: CreatePropertyIndex — 3-phase lock scope (RLock→unlocked I/O→Lock)
- [x] **Fix 3**: Property index persistence — definitions survive BadgerStore restart via `0x0F/prop_indexes` meta key
- [x] **Fix 2**: Delete preserves history — removed `deleteHistoryByPrefix`, added tombstone versions with `DeletedAt`/`ValidTo`
- [x] **Fix 1**: Temporal queries history-aware — `GetNodesValidAt`/`GetRelationshipsValidAt`/`GetNodesValidDuring`/`GetRelationshipsValidDuring`/`Snapshot` now include deleted entities; added `GetRelAt`, `AllNodeHistoryIDs`/`AllRelHistoryIDs` Store methods

**~19 new tests**, all pass with race detector. Coverage 89.7%.

### 2f. Cursor-Based Pagination ✓

Complete. Addresses tkgd-v3 Issue 6 — `NodesByLabel("Person")` on a million-node graph allocates 1M deep copies.

**Design:** `QueryOpts{Limit, After}` parameter added to 5 unbounded Store methods. Zero values mean "return all" (backward-compatible). Keyset cursor using `snowflake.ID`.

**Store interface changes:**
- [x] `NodesByLabel(token, opts QueryOpts)` — paginated
- [x] `RelationshipsByType(token, opts QueryOpts)` — paginated
- [x] `AllNodes(opts QueryOpts)` — paginated
- [x] `AllRelationships(opts QueryOpts)` — paginated
- [x] `NodesByLabelAndProperty(token, key, value, opts QueryOpts)` — paginated

**Implementation:**
- [x] `QueryOpts` struct in `store.go`
- [x] `paginateIDs` shared helper (`pagination.go`) — binary search cursor, sort-before-fetch
- [x] MemoryStore: 5 methods refactored to sort IDs → paginate → deep-copy subset only
- [x] BadgerStore: 5 methods refactored with sort+paginate; `NodesByLabelAndProperty` lock scope fixed (snapshot-and-release pattern, early-stop on fallback scan)
- [x] Graph layer: 5 passthrough methods gain `QueryOpts` parameter
- [x] All internal callers pass `QueryOpts{}` (temporal.go, tutorials)

**~30 tests total:** 8 paginateIDs unit tests, 8 MemoryStore integration (limit, multi-page walk, zero opts, indexed/fallback property query), 8 BadgerStore integration (mirrored), 3 Graph-layer. All pass with race detector.

### 2g. Combined Label+Property+Temporal Queries ✓

Complete. Addresses tkgd-v3 Issue 7 — finding "Person nodes named Alice valid at time T" required two calls plus Go-side filtering.

**New Graph methods:**
- [x] `NodesByLabelPropertyAndTime(label, key, value, t)` — intersects property index with point-in-time filter
- [x] `NodesByLabelPropertyDuring(label, key, value, start, end)` — intersects property index with interval filter

**7 tests:** found (all axes match), property mismatch, temporal mismatch, unregistered label, with property index, interval overlap, no overlap.

### 2h. Architectural Fixes (5 Issues) ✓

Complete. Implemented in v3.0.25.

5 issues found during post-Phase-2 review, resolved in order 3 → 1 → 2 → 4 → 5:

- [x] **Fix 1 (Issue #3)**: `DeleteRelationshipWithContext` missing entity lock — added `LockEntity`/`UnlockEntity` (2-line fix in `context.go`)
- [x] **Fix 2 (Issue #1)**: `allKnownNodeIDs` O(N) deep-copy waste — added `AllNodeIDs`/`AllRelIDs` to Store interface (MemoryStore + BadgerStore), rewrote temporal helpers to use ID-only queries
- [x] **Fix 3 (Issue #2)**: `DeleteNodeWithContext` missing relationship locks — added `LockMany`/`UnlockMany` to entity lock manager, rewrote delete with two-phase locking + TOCTOU retry
- [x] **Fix 4 (Issue #4)**: Cascade corruption fallback skips property index cleanup — added `purgeNodeFromAllPropertyIndexes` brute-force purge helper, called in BadgerStore corruption path
- [x] **Fix 5 (Issue #5)**: Snapshot vs Batch torn reads — added graph-level `sync.RWMutex`, Batch acquires write lock, Snapshot acquires read lock

**25 new tests**, all pass with race detector. Coverage 89.6%.

---

## Store Interface Extensions (from tkgd-v3 integration)

Gaps identified during tkgd-v3 development. Not blockers for Phase 3 tiering,
but required for correct multi-step mutations and efficient temporal queries
at the server layer.

### 2h. Temporal Query Push-Down ✓

Complete. Temporal filtering pushed down to Store layer, eliminating O(N) deep-copy waste.

**QueryOpts extension:** `ValidAt types.Instant` (point-in-time), `ValidStart/ValidEnd types.Instant` (interval). Zero values = no filter (backward-compatible).

**Implementation:**
- [x] `temporal_filter.go` — `entityValidFrom` (explicit ValidFrom or snowflake ID bit extraction), `matchesTemporalFilter` (point-in-time: `from <= t AND (to == 0 OR to > t)`, interval overlap: `from < end AND (to == 0 OR to > start)`)
- [x] `lru.go` — `Peek(key)` for zero-allocation cache lookup (no deep-copy, no MRU promotion), `Cap()` for cache recreation
- [x] MemoryStore: 5 paginated methods filter before deep-copy via `filterNodeIDsByTemporal`/`filterRelIDsByTemporal` (reads in-memory entity pointers directly)
- [x] BadgerStore: two-stage filtering — `filterNodeIDsByTemporalPeek` pre-filters cache hits (zero allocation), `fetchNodesWithTemporalFilter` post-filters cache misses after GetNode
- [x] Graph-layer refactor: `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring` now push temporal filters into Store calls via QueryOpts

**~30 tests:** 9 temporal_filter, 6 LRU (Peek/Cap), 8 MemoryStore temporal, 7 BadgerStore temporal. All pass with race detector.

### 2i. Graph Transactions + Reset ✓

Complete. Create-only transaction with full rollback + atomic graph reset.

**Implementation:**
- [x] `Store.Clear()` — removes all entities, indexes, history, counters. MemoryStore reinitializes 9 maps. BadgerStore clears indexes + counters + LRU caches + pending buffer + `db.DropAll()`
- [x] `ErrTxDone` sentinel error for post-commit/rollback method calls
- [x] `GraphTx` (`tx.go`) — create-only transaction holding graph write lock. `BeginTx()`, `AddNode`, `AddRelationship`, `Commit`, `Rollback`, `CreatedNodeIDs`, `CreatedRelIDs`
- [x] Rollback uses `store.Delete*` directly (no tombstone versions — rolled-back entities vanish completely). Reverse creation order. Best-effort: continues on error, returns first error
- [x] `Graph.Reset()` — acquires graph write lock, calls `store.Clear()`. Preserves registries (Graph-layer concern, not cleared by Store)

**~25 tests:** 8 Store.Clear (4 MemoryStore + 4 BadgerStore), 12 GraphTx (commit/rollback/double-commit/ErrTxDone/concurrent access/rollback-leaves-no-history), 4 Graph.Reset (empty/clears entities/preserves registries/clears history). All pass with race detector.

---

## Phase 3 — Tiered Persistence

The multi-shard architecture from the Tiered Temporal Storage spec.
Built behind the Store interface — single-instance deployments unaffected.

### 3a. Snowflake + Registry + Reference/Event Split ✓

Complete. Implemented in v3.0.26.

**New files (12):**
- `ontology.go` + `ontology_test.go` — EntityClass, OntologyMapping (label → ref/event classification, lazy token cache)
- `shard_catalog.go` + `shard_catalog_test.go` — ShardCatalog, ShardEntry, ShardKind, ShardTier (JSON persistence, atomic write)
- `registry_file.go` + `registry_file_test.go` — Flat msgpack registry save/load (write-tmp+rename)
- `badgerstore_partial.go` + `badgerstore_partial_test.go` — Split rel write/delete helpers (putRelEntityAndOut, putRelIncoming, deleteRelEntityAndOut, deleteRelIncoming, hasNodeID, hasRelID, incomingRelIDs, outgoingRelIDs)
- `tieredstore.go` — TieredStoreConfig, TieredStore, eventShard, constructor, Close, Clear, shard routing, timestamp-to-shard resolution, registry file integration
- `tieredstore_write.go` — Node/Rel Put/Delete/Replace/Batch, cross-shard relationship routing (E→R ref-first, R→E entity-first), DeleteNodeCascade, history writes, property indexes
- `tieredstore_read.go` — Get/Query/Count/Adjacency/History reads, merge helpers (k-way merge of sorted slices), pagination helpers, IncomingRelationships cross-shard fetch
- `tieredstore_test.go` — 40 integration tests covering ontology routing, CRUD, cross-shard rels, merge queries, cascade, history, batch, property indexes, lifecycle, registry round-trip, pagination, disk-backed, mid-window restart

**Modified files (1):**
- `graph.go` — TieredStore type switch in Close() for registry saves, TieredStore setup in New() (SetLabelRegistry, LoadLabelRegistry, LoadRelTypeRegistry)

**Key design decisions:**
- [x] Ontology mapping (label → reference/event classification)
- [x] TieredStore skeleton implementing Store interface (all 43 methods)
- [x] Registry as flat msgpack file (tokens only, ~2KB)
- [x] Reference shard (single BadgerStore, always hot)
- [x] Hot event shard (single BadgerStore, current time window)
- [x] Shard catalog (JSON, time windows, tier tracking)
- [x] Entity routing by label classification
- [x] Relationship key routing (in/ keys to endpoint shard, entity+out/ to start shard)
- [x] All 4 relationship patterns routed correctly (E→R, E→E, R→R, R→E)
- [x] Snowflake ID timestamp extraction for shard resolution (O(1), no fan-out)
- [x] Cross-shard split-write with reference-first ordering (§12)
- [x] Cross-shard DeleteRelationship and DeleteNodeCascade
- [x] Mid-window restart (reopen existing hot shard from catalog)

**52 tests total.** All pass with race detector. Coverage ≥80% on all public methods.

### 3b+3c. Shard Rotation + Warm Tier + Depth-Aware Reads ✓

Complete. Implemented in v3.0.27.

**New/modified files:**
- `store.go` — `ShardDepth` type (DepthAll/DepthHot/DepthWarm), `Depth` field on `QueryOpts`
- `badgerstore.go` — `ReadOnly` config, opens Badger read-only, skips flushLoop/gcLoop
- `shard_catalog.go` — `UpdateShardTier`, `UpdateShardTimeEnd` methods
- `tieredstore.go` — `mu sync.RWMutex`, `RotateHotShard()`, `checkRotation()`, `eventShardSnapshot(depth)`, warm shard recovery in constructor, `shardForRelID` probe-all-shards fallback
- `tieredstore_write.go` — shard-based routing (replaces class-based), `checkRotation()` on write paths, batch partitioning by `*BadgerStore` pointer
- `tieredstore_read.go` — all merge queries use `eventShardSnapshot(opts.Depth)` with `mu.RLock` snapshot pattern

**Key design decisions:**
- [x] ShardDepth type — zero=all tiers (backward-compatible)
- [x] BadgerStore ReadOnly mode (warm/cold shards)
- [x] Shard rotation: hot→warm on time window expiry via `checkRotation()`
- [x] Millisecond-aligned shard boundaries (snowflake ID ms resolution)
- [x] Warm shard recovery from catalog on restart
- [x] E→E cross-shard fix: shard-based routing via `shardForNodeID` (not class-based)
- [x] `shardForRelID` probe-all-shards fallback for cross-shard relationship entities
- [x] Depth-aware merge queries (hot/warm/all)
- [x] sync.RWMutex snapshot pattern for rotation safety

**~25 new tests** (rotation, E→E cross-shard, depth-aware reads, warm recovery, ReadOnly BadgerStore, catalog). All pass with race detector. Coverage ≥80%.

**Deferred to later phases:**
- [x] Warm → cold demotion → Phase 3d ✓
- [x] Lazy-open cold shards on first access → Phase 3d ✓
- [x] Idle-close after configurable timeout → Phase 3d ✓
- [x] Parallel shard queries → Phase 3d ✓

### 3d. Cold Shard Tier + Parallel Query + Reference Archive ✓

Complete. Implemented in v3.0.28.

**New/modified files:**
- `tieredstore.go` — `eventShard` gains `path`, `shardMu`, `lastAccess` fields + `getStore()` lazy-open method. Config gains `ColdAfter`, `IdleTimeout`. Constructor: cold shard recovery from catalog (store=nil), archive dir, `closeCh` channel, `idleCloseLoop()` goroutine. `eventShardSnapshot` returns `[]*eventShard`. `RotateHotShard()` gains warm→cold demotion. `shardForNodeID` falls back to refArchive. `openRefArchive()`, `ensureRefArchive()`, `hasArchiveShard()`. `Close()` handles nil stores + closeCh + refArchive. `Clear()` skips nil stores + clears archive.
- `tieredstore_write.go` — Error propagation from shard routing. `ArchiveNode(id)` moves ref node + rels to archive (skips rels where other endpoint not archived). `RestoreNode(id)` reverse of archive. `ErrNotReferenceEntity` sentinel.
- `tieredstore_read.go` — All 10 merge queries rewritten with `sync.WaitGroup` parallel pattern (ref shard sequential, event shards concurrent via goroutines). `stripDepth` helper. Error propagation from `es.getStore(ts)`.
- `shard_catalog.go` — `ColdEventShards()` helper.
- `graph.go` — `ArchiveNode`/`RestoreNode` passthroughs via type switch on `*TieredStore`.
- `tieredstore_test.go` — ~30 new tests.

**Key design decisions:**
- [x] Cold shard lifecycle: warm→cold demotion during rotation when `ColdAfter > 0`
- [x] Per-shard `sync.Mutex` (`shardMu`) protects lazy open/close of individual cold shards
- [x] `atomic.Int64` `lastAccess` tracks idle time for cold shard auto-close
- [x] `idleCloseLoop()` goroutine checks every `IdleTimeout/2`, stopped via `closeCh`
- [x] Cold shards recovered from catalog on restart with `store=nil` (not opened until queried)
- [x] `getStore(ts)` zero overhead for hot/warm (direct pointer return), lock+open for cold
- [x] Parallel shard queries via `sync.WaitGroup` for all 10 merge methods
- [x] Reference archive lazy-opened on first `ArchiveNode`/`RestoreNode` or DepthAll with catalog entry
- [x] `shardForNodeID` falls back to refArchive (lazy-open if catalog says archive exists)
- [x] `ArchiveNode`: read node+rels from refShard → write to archive → `DeleteNodeCascade` from refShard
- [x] Cross-node rels skipped during archive if other endpoint not in archive (cascade deletes them)
- [x] Zero defaults = backward-compatible (ColdAfter=0: no demotion, IdleTimeout=0: no idle-close)

**~30 new tests** (8 cold shard, 4 parallel query, 8 archive, 3 routing error, 2 graph passthrough, 1 catalog). All pass with race detector.

### 3e. Repair + Tooling

- [ ] Split-write repair scan (startup + background)
- [ ] Per-shard verification caching (immutable shards verified once)
- [ ] Property indexes for reference entities only
- [ ] Admin API: force rotation, list shards, archive/restore, rebuild catalog
- [ ] ID decomposition endpoint (creation time + generator + sequence)
- [ ] Migration tool for single-Badger → tiered deployments


