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

## Ported with Phase 2 (temporal query layer) — COMPLETE

### 3. Temporal Indexing — Scan-Based ✓

- [x] `GetNodesValidAt(t)`, `GetRelationshipsValidAt(t)` — Phase 2a (v3.0.21), history-aware since v3.0.23
- [x] `GetNodesValidDuring(start, end)`, `GetRelationshipsValidDuring(start, end)` — Phase 2a, history-aware
- [x] Integrate with `TemporalMetadata` — `ValidFrom`/`ValidTo`/`UpdatedAt`/`DeletedAt` all used
- [x] ~50 temporal tests (point-in-time, interval, version-specific, neighbor, snapshot, deleted entity queries, truncation resilience)
- Not ported: Allen's 13 interval relations — basic overlap semantics (`start < end AND (ValidTo == 0 OR ValidTo > start)`) covers all current use cases. Allen's algebra formalizes 11 additional named relations beyond overlap; not needed for the query API
- Not ported: Interval tree data structure — performance optimization for temporal range queries. Current O(N) scan works. Add interval tree + `0x09`/`0x0A` keys if temporal range queries become a bottleneck on large datasets
- Not ported: v2 test suite (9500+ LOC) — tests Allen's algebra edge cases which aren't relevant. Own test suite written (~50 tests)

**Implemented**: Phase 2a (v3.0.21), history-aware since v3.0.23

### 4. Property Index — Label+Property+Value Lookups ✓

- [x] `NodesByLabelAndProperty(labelToken, key, value, opts)` — O(1) indexed lookup with fallback scan, paginated via `QueryOpts`
- [x] `CreatePropertyIndex(labelToken, propertyKey)` / `DropPropertyIndex(labelToken, propertyKey)` — with `ErrIndexExists`/`ErrIndexNotFound`
- [x] Lifecycle hooks in all 7 node mutation paths (both MemoryStore and BadgerStore) via `addNodeToPropertyIndexes`/`removeNodeFromPropertyIndexes`
- [x] Rebuild on startup via `loadIndexes()` — reads persisted definitions from `0x0F/prop_indexes`, scans matching nodes
- [x] 25 tests: 8 MemoryStore, 8 BadgerStore, 8 Graph-layer, 1 propertyValueKey type coverage
- Different approach: In-memory index with definition persistence to `0x0F/prop_indexes`. No Badger prefix scan — index is always in RAM. No `0x09` key schema; using `propertyValueKey` canonical strings with type prefixes (`"s:"`, `"i:"`, `"f64:"`, `"b:"` etc.)

**Implemented**: Phase 2c (v3.0.22), persistence fixed in v3.0.23 (Phase 2 review)

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
| 3 | Temporal queries | P1 | **Done** (scan-based) | Phase 2a (v3.0.21), history-aware since v3.0.23. ~50 tests. Allen algebra + interval tree deferred (perf opt, not needed for current API) |
| 4 | Property index | P1 | **Done** | Phase 2c (v3.0.22), persistence in v3.0.23. In-memory index with `0x0F` definition persistence, 25 tests. Badger prefix scan keys deferred |
| 5 | Event system | P2 | Defer | No consumers yet, add on demand |
