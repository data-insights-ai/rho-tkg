# tkg-v3 — Roadmap

## Status

Library at v3.0.15. Phase 1a (UpdateNode/UpdateRelationship) complete.

## Gap Analysis: tkg-2025-v2 vs rho/tkg-v3

Comprehensive comparison of v2's pkg/graph + pkg/storage against v3's current feature set.
v3 is architecturally superior (80B nodes, token interning, compound adjacency keys,
snowflake IDs, async batch persistence) but missing several v2 features needed before
the tiered persistence layer can be built.

### Key Schema Alignment (verified)

The current key schema already supports the tiered persistence spec:
- `0x05` (out): compound key supports basic/typed/counterpart prefix scans in 1 key
- `0x06` (in): same, 3 scan levels in 1 key
- History keys (`0x07`/`0x08`) and temporal keys (`0x09`/`0x0A`) are forward-planned in `keys_helpers_test.go`
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

### 1b. Version History

Store previous versions of entities for temporal queries and audit trails.

**Store interface additions:**
- [ ] `PutNodeVersion(id snowflake.ID, version uint32, n *types.Node) error`
- [ ] `GetNodeVersion(id snowflake.ID, version uint32) (*types.Node, error)`
- [ ] `GetNodeHistory(id snowflake.ID) ([]*types.Node, error)` — all versions, ascending
- [ ] `TruncateNodeHistory(id snowflake.ID, keepVersions int) error`
- [ ] `PutRelVersion(id snowflake.ID, version uint32, r *types.Relationship) error`
- [ ] `GetRelVersion(id snowflake.ID, version uint32) (*types.Relationship, error)`
- [ ] `GetRelHistory(id snowflake.ID) ([]*types.Relationship, error)`
- [ ] `TruncateRelHistory(id snowflake.ID, keepVersions int) error`

**Badger key layout (already planned):**
- `0x07/<8B nodeID>/<8B version>` — node history entry (17B)
- `0x08/<8B relID>/<8B version>` — relationship history entry (17B)
- Prefix scan `0x07/<nodeID>` returns all versions in order

**Graph-layer integration:**
- [ ] UpdateNode saves old version to history before overwriting
- [ ] UpdateRelationship saves old version to history before overwriting
- [ ] `Graph.GetNodeHistory(id)` passthrough
- [ ] `Graph.GetRelHistory(id)` passthrough

**Tests:**
- [ ] History grows on each update
- [ ] History returns versions in ascending order
- [ ] Truncate keeps only N most recent versions
- [ ] History survives BadgerStore restart
- [ ] DeleteNode cleans up history
- [ ] Mirror for Relationship

### 1c. Hash Chain Computation

Make the integrity fields real — compute SHA-256 hashes on create/update.

**Implementation:**
- [ ] `ComputeNodeHash(n *types.Node, labels []string) [32]byte`
  - Hash: id + sorted labels + sorted properties + version
  - SHA-256
- [ ] `ComputeRelHash(r *types.Relationship, typeName string) [32]byte`
  - Hash: id + type + startID + endID + sorted properties + version
- [ ] AddNode: compute hash, set integrity (PrevHash = zero for v1)
- [ ] UpdateNode: compute new hash, set PrevHash = old hash
- [ ] AddRelationship: compute hash, set integrity
- [ ] UpdateRelationship: compute new hash, set PrevHash = old hash

**Tests:**
- [ ] Hash changes when properties change
- [ ] Hash is deterministic (same input = same hash)
- [ ] PrevHash chain links correctly across updates
- [ ] Genesis version has zero PrevHash

### 1d. GetAllNodes / GetAllRelationships / GetByIDs

Bulk query methods needed by export, snapshot, and the tiered store layer.

**Store interface additions:**
- [ ] `AllNodes() ([]*types.Node, error)`
- [ ] `AllRelationships() ([]*types.Relationship, error)`
- [ ] `GetNodesByIDs(ids []snowflake.ID) ([]*types.Node, error)`
- [ ] `GetRelationshipsByIDs(ids []snowflake.ID) ([]*types.Relationship, error)`

**Graph-layer passthrough:**
- [ ] `Graph.AllNodes()`, `Graph.AllRelationships()`
- [ ] `Graph.GetNodesByIDs(ids)`, `Graph.GetRelationshipsByIDs(ids)`

**Tests:**
- [ ] AllNodes returns all stored nodes
- [ ] AllNodes returns empty slice when empty
- [ ] GetNodesByIDs returns found nodes, skips missing (or returns error per ID)
- [ ] Results sorted by snowflake.ID (deterministic)
- [ ] Both MemoryStore and BadgerStore

### 1e. Batch Operations

Bulk create/update/delete in single operations. Critical for import and the tiered
store's split-write.

**Store interface additions:**
- [ ] `PutNodesBatch(nodes []*types.Node) error`
- [ ] `PutRelationshipsBatch(rels []*types.Relationship) error`
- [ ] `DeleteNodesBatch(ids []snowflake.ID) error`
- [ ] `DeleteRelationshipsBatch(ids []snowflake.ID) error`

**Graph-layer batch builder:**
- [ ] `BatchBuilder` fluent API
  - `NewBatchBuilder(g *Graph) *BatchBuilder`
  - `.AddNode(labels, props)` — queues node creation
  - `.AddRelationship(typeName, startNode, endNode, props)` — queues rel creation
  - `.UpdateNode(id, updates)` — queues update
  - `.DeleteNode(id)` — queues delete
  - `.Execute() (*BatchResult, error)` — runs all ops
- [ ] `BatchResult` with Success, Failed, Errors, Duration

**Tests:**
- [ ] Batch add 1000 nodes
- [ ] Batch mixed operations
- [ ] Partial failure tracking
- [ ] Both MemoryStore and BadgerStore

### 1f. Context-Aware Operations (optional for Phase 1)

Timeout and cancellation support. Lower priority — can be deferred.

- [ ] `AddNodeWithContext(ctx, labels, props)`
- [ ] `UpdateNodeWithContext(ctx, id, updates)`
- [ ] `DeleteNodeWithContext(ctx, id)`
- [ ] `GetNodeWithContext(ctx, id)`
- [ ] Mirror for Relationship

---

## Phase 2 — Temporal Query Layer

Make tkg-v3 a proper temporal graph. The "T" in TKG.

### 2a. Temporal Queries

Point-in-time and interval queries over valid time.

**Graph methods:**
- [ ] `GetNodesValidAt(t types.Instant) ([]*types.Node, error)`
- [ ] `GetRelationshipsValidAt(t types.Instant) ([]*types.Relationship, error)`
- [ ] `GetNodesByLabelValidAt(label string, t types.Instant) ([]*types.Node, error)`
- [ ] `GetNodesValidDuring(start, end types.Instant) ([]*types.Node, error)`
- [ ] `GetRelationshipsValidDuring(start, end types.Instant) ([]*types.Relationship, error)`
- [ ] `GetNodeAt(id snowflake.ID, t types.Instant) (*types.Node, error)` — version valid at t
- [ ] `GetNeighborsValidAt(nodeID snowflake.ID, t types.Instant) ([]*types.Node, error)`
- [ ] `Snapshot(t types.Instant) (*GraphSnapshot, error)` — full graph state at time t

**Implementation options:**
- Simple: scan all nodes/history, filter by ValidFrom/ValidTo interval
- Indexed: use temporal index keys (`0x09`/`0x0A`) for O(log N) queries
- Start simple, add indexes when performance requires it

### 2b. Hash Chain Verification

Prove integrity of version chains.

- [ ] `VerifyNodeHashChain(id snowflake.ID) (bool, error)`
  - Get full history
  - Verify each version's ContentHash matches computed hash
  - Verify PreviousHash chain links correctly
  - Genesis version must have zero PreviousHash
- [ ] `VerifyRelHashChain(id snowflake.ID) (bool, error)`

### 2c. Property Indexes

O(1) property lookups for Cypher WHERE clauses.

**Store interface:**
- [ ] `CreatePropertyIndex(labelToken uint16, propertyKey string) error`
- [ ] `DropPropertyIndex(labelToken uint16, propertyKey string) error`
- [ ] `NodesByLabelAndProperty(labelToken uint16, key string, value any) ([]*types.Node, error)`

**Implementation:**
- In-memory index maintained alongside label/type indexes
- Persisted to Badger for restart recovery
- Auto-updated on PutNode/DeleteNode

### 2d. Per-Label / Per-Type Statistics

Cardinality statistics for query optimization (Cypher join ordering).

- [ ] `NodeCountByLabel(label string) (int, error)`
- [ ] `RelCountByType(typeName string) (int, error)`
- [ ] `AllLabelCounts() (map[string]int, error)`
- [ ] `AllRelTypeCounts() (map[string]int, error)`

**Implementation:**
- Atomic counters maintained in-memory, persisted in Badger flush
- Updated on Add/Delete/Update (label changes)

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
