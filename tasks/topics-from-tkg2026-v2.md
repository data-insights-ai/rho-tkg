# Improvements from tkg-2026-v3 (v2 Architecture)

Analysis of mature features in `tkg-2026-v3` that are worth porting to `rho/tkg-v3`.

## Already Present in rho/tkg-v3 (no action needed)

- Shadow properties (15 keys, identical design)
- Token interning + registry persistence (snapshot/restore)
- Binary key layout for Badger (big-endian, prefix-scannable)
- 4 index types (label, reltype, outgoing, incoming)
- Version history API + hash chain integrity (SHA-256)
- Batch operations (BatchBuilder with ordered execution)
- Context-aware operations (Phase 1g, 8 WithContext methods)
- Entity locks (256-shard mutex array)
- LRU caches with dirty tracking + async WriteBatch flush
- Defensive copying at store boundaries

---

## Port Now (low risk, high value) — COMPLETE

### 1. LabelStats — Per-Label Cardinality Counters ✓

- [x] Implement O(1) per-label/per-type counters at the Store level (`NodeCountByLabel(token uint16)`, `RelCountByType(token uint16)`)
- [x] MemoryStore: `len(labelIdx[token])` — trivial O(1) via existing index sizes
- [x] BadgerStore: `sync.Map` + `atomic.Int64` counters, maintained in 9 mutation sites, rebuilt from index sizes in `loadIndexes()`
- [x] Graph layer: `NodeCountByLabel(label)`, `RelCountByType(typeName)`, `AllLabelCounts()`, `AllRelTypeCounts()` delegate to Store-level O(1) methods
- [x] 18 tests: 8 MemoryStore, 8 BadgerStore (including persistence round-trip), 2 graph-level integration

**Implemented**: Phase 2d (initially scan-based in v3.0.20, upgraded to O(1) post-v3.0.22)

### 2. Validation Framework — Configurable Limits ✓

- [x] `ValidationLimits` struct on `Graph.Config` with 5 configurable limits (MaxLabelsPerNode, MaxPropertiesPerEntity, MaxPropertyKeyLength, MaxPropertyValueSize, MaxNameLength) — zero values resolve to defaults
- [x] 5 sentinel errors: `ErrTooManyLabels`, `ErrTooManyProperties`, `ErrKeyTooLong`, `ErrValueTooLarge`, `ErrNameTooLong`
- [x] Enforced at all graph entry points: 4 WithContext methods + 4 BatchBuilder methods
- [x] Two-phase update validation: pre-lock entry checks + post-mutation `MaxPropertiesPerEntity` under entity lock
- [x] `PropertyCount()` on Node and Relationship for efficient count checking
- [x] ~30 tests: defaults, boundary tests, batch mirroring, all sentinel errors tested with `errors.Is`

**Implemented**: Phase 2e (post-v3.0.22). No separate `pkg/validation/` package — limits are inline on Graph (library is too small to justify a separate package). Regex format validation and AllowSelfLoops deferred — format validation belongs at the HTTP layer (tkgd-v3).

---

## Port with Phase 2 (temporal query layer)

### 3. Temporal Indexing — Allen's Interval Algebra

- [ ] Port interval algebra core: 13 Allen relations (before, after, meets, overlaps, during, starts, finishes, equals, and inverses)
- [ ] Implement interval tree data structure for efficient temporal range queries
- [ ] Add `GetNodesValidAt(t)`, `GetRelationshipsValidAt(t)` using interval index
- [ ] Add `GetNodesValidDuring(start, end)` for interval overlap queries
- [ ] Integrate with existing `TemporalMetadata` on Node/Relationship types
- [ ] Port test suite (9500+ LOC in tkg-2026-v3 — covers all 13 relations exhaustively)

**Source**: `tkg-2026-v3/services/tkg/pkg/index/temporal.go` (~420 LOC implementation, ~9500 LOC tests)
**Effort**: Large | **Impact**: High (core Phase 2 enabler)
**Why**: This is the foundation for `Snapshot(t)`, temporal Cypher queries, and point-in-time graph reconstruction. The exhaustive test suite alone saves weeks.

### 4. Property Index — Label+Property+Value Lookups

- [ ] Implement dual-layer index: RAM nested map (hot path, ~10ns) + Badger prefix scan (persistence)
- [ ] Key schema: binary prefix `0x09/<labelToken>/<propertyHash>/<valueHash>/<nodeID>`
- [ ] Add `FindByLabelAndProperty(label, property, value) []snowflake.ID` to Store interface
- [ ] Add lifecycle hooks: `OnNodePut`, `OnNodeDelete`, `OnNodeUpdate` to maintain index
- [ ] Support `CreateIndex(label, property)` / `DropIndex(label, property)` for selective indexing
- [ ] Add `RebuildIndex(label, property)` for cold-start population
- [ ] Write tests for: creation, deletion, update (old value removed, new value added), rebuild, concurrent access

**Source**: `tkg-2026-v3/services/tkg/pkg/storage/property_index.go` (~400 LOC)
**Effort**: Medium | **Impact**: High
**Why**: Without it, finding a node by property requires full label scan + filter (O(n)). With it, O(1).
Already on rho/tkg-v3 roadmap as Phase 2c.

---

## Defer (no consumers yet)

### 5. Event System — Pub/Sub Dispatcher

- [ ] Implement `Dispatcher` with type-specific and catch-all subscriptions
- [ ] Implement bounded `Queue` with configurable overflow strategies (block, drop-old, drop-new, reject)
- [ ] Implement `WorkerPool` for async event processing with backpressure
- [ ] Define event types: NodeCreate, NodeUpdate, NodeDelete, RelCreate, RelUpdate, RelDelete
- [ ] Add priority levels (High, Normal, Low)
- [ ] Wire into Graph layer mutation methods

**Source**: `tkg-2026-v3/services/tkg/pkg/events/` (~600 LOC across 5 files)
**Effort**: Medium | **Impact**: Medium-Low
**Why**: Enables reactive patterns (audit logging, cascading updates, real-time notifications, index maintenance hooks). But rho/tkg-v3 has no consumers for events yet. Add when Phase 2/3 creates demand.

---

## Decision Log

| # | Feature | Priority | When | Rationale |
|---|---------|----------|------|-----------|
| 1 | LabelStats | P0 | **Done** | O(1) counters at Store level, 18 tests |
| 2 | Validation limits | P0 | **Done** | 5 configurable limits, enforced at all entry points, ~30 tests |
| 3 | Temporal queries | P1 | **Done** (scan-based) | Phase 2a (v3.0.21), 31 tests. Allen interval indexing deferred (perf opt) |
| 4 | Property index | P1 | **Done** | Phase 2c (v3.0.22), in-memory with auto-maintenance, 25 tests |
| 5 | Event system | P2 | Defer | No consumers yet, add on demand |
