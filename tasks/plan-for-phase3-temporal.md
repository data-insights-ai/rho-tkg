# Phase 3a: TieredStore — Reference/Event Split

## Context

Phase 3a introduces the foundational tiered storage architecture: a `TieredStore` implementing the `Store` interface that routes entities across multiple BadgerStore instances based on an ontology mapping. This eliminates cross-shard fan-out for the dominant SOC query pattern (`Case ← Signal`) by co-locating all incoming adjacency for reference entities in a single shard.

Phases 1–2 are complete (v3.0.25). The Store interface is stable at ~43 methods. Phase 3a builds the full N-shard data model but only populates the reference shard + one hot event shard. Warm/cold/archive come in Phases 3b–3e.

---

## Architecture

```
              Graph (unchanged)
                │
           TieredStore (new Store impl)
       ┌────────┼────────────────────┐
  refShard   refArchive     eventShards map[string]*eventShard
 (BadgerStore) (nil in 3a)    ├── "2026-W09" (hot)    ← Phase 3a: only this one
                              ├── "2026-W08" (warm)   ← Phase 3c
                              └── "2026-W07" (cold)   ← Phase 3c
```

### Shard Model

```go
type TieredStore struct {
    refShard    *BadgerStore            // reference shard (always hot, always open)
    refArchive  *BadgerStore            // nil until Phase 3d (lazy-opened for closed cases)
    eventShards map[string]*eventShard  // name → event shard (e.g., "2026-W09")
    hotShard    *eventShard             // pointer to the currently hot shard (convenience)
    ontology    *OntologyMapping
    catalog     *ShardCatalog
    ...
}

type eventShard struct {
    name      string
    store     *BadgerStore
    tier      ShardTier     // hot, warm, cold
    timeStart time.Time     // shard window start (inclusive)
    timeEnd   time.Time     // shard window end (exclusive)
    readOnly  bool          // warm/cold shards are read-only
}
```

Phase 3a: `eventShards` has exactly one entry (the hot shard). Reads iterate all event shards (trivially one). Writes always go to `hotShard`. Phase 3c adds rotation/demotion.

### Relationship Routing (7 keys per relationship)

| Pattern | Entity + out/ keys | in/ keys | Split? |
|---------|-------------------|----------|--------|
| E→E     | event shard       | event shard | No |
| R→R     | ref shard         | ref shard   | No |
| E→R     | event shard       | ref shard   | Yes (ref first §12) |
| R→E     | ref shard         | event shard | Yes |

### Snowflake ID–Based Shard Resolution (§11)

Every snowflake ID carries its creation timestamp in bits 22–62:

```
Snowflake ID → uint64(id) >> 22 → time_ms since epoch (2026-01-01)
              → epochMs + time_ms → absolute creation time
              → map to shard window → target event shard
```

Existing infrastructure in `temporal_filter.go`:
```go
epochMs := snowflakeEpoch.UnixMilli()
timeMs := int64(uint64(id) >> 22)
creationTime := time.UnixMilli(epochMs + timeMs)
```

The node field (bits 12–21) distinguishes entity IDs (even) from relationship IDs (odd).

**Shard resolution strategy:**
1. **Writes**: Route by ontology classification (label → ref/event), then event entities go to `hotShard`
2. **Reads (known label)**: Route by label classification → single shard
3. **Reads (ID only, unknown label)**: Check `refShard.hasNodeID` (O(1)). If miss → extract timestamp from ID → `findEventShardByTimestamp` → O(1) shard resolution
4. **Reads (relID)**: Check `refShard.hasRelID` (O(1)). If miss → extract timestamp → target event shard
5. **After in/ prefix scan**: Each relID from the scan carries its own timestamp → O(1) per entity

No fan-out. No range tables. The ID IS the shard address.

### Query Routing

| Operation | Strategy |
|-----------|----------|
| `GetNode(id)` | ref probe (O(1)), miss → timestamp extraction → target event shard |
| `NodesByLabel(tok)` | Single-shard by label classification |
| `OutgoingRels(nodeID)` | Delegate to node's shard (entity + out/ co-located) |
| `IncomingRels(nodeID)` | Get relIDs from node's shard inIdx, fetch each by timestamp extraction |
| `AllNodes()` | Merge ref + all event shards |
| `NodeCount()` | Sum ref + all event shards |
| `GetRelationship(id)` | ref probe (O(1)), miss → timestamp extraction → target event shard |
| `RelationshipsByType(tok)` | Merge ref + all event shards |

---

## Files (all in `pkg/graph/`)

### New Implementation Files

| # | File | Lines | Purpose |
|---|------|-------|---------|
| 1 | `ontology.go` | ~100 | `EntityClass`, `OntologyMapping`, classification by token and name |
| 2 | `shard_catalog.go` | ~200 | `ShardCatalog`, `ShardEntry`, `ShardKind`, `ShardTier`, JSON persistence |
| 3 | `registry_file.go` | ~100 | Flat msgpack registry load/save (write-tmp+rename) |
| 4 | `tieredstore.go` | ~450 | `TieredStoreConfig`, `TieredStore`, `eventShard`, constructor, Close, Clear, shard routing helpers |
| 5 | `tieredstore_write.go` | ~450 | Node/Rel Put/Delete/Replace, Batch ops, History writes |
| 6 | `tieredstore_read.go` | ~500 | Get, Query, Count, Adjacency, History reads, Property indexes, Cascade |
| 7 | `badgerstore_partial.go` | ~150 | Unexported helpers on `*BadgerStore` for partial rel writes/deletes |

### New Test Files

| # | File | Lines | Purpose |
|---|------|-------|---------|
| 8 | `ontology_test.go` | ~80 | Classification unit tests |
| 9 | `shard_catalog_test.go` | ~120 | Catalog JSON round-trip tests |
| 10 | `registry_file_test.go` | ~80 | Registry file round-trip + atomic rename tests |
| 11 | `tieredstore_test.go` | ~900 | Full Store interface tests + cross-shard scenarios |
| 12 | `badgerstore_partial_test.go` | ~150 | Partial write/delete helper unit tests |

### Modified Files

| File | Change |
|------|--------|
| `graph.go` | Add `*TieredStore` type switch in `Close()` and registry loading in `New()` (~25 lines) |

**Total**: ~1950 impl + ~1330 tests ≈ 3280 lines across 12 new files + 1 modified file.

---

## Implementation Steps

### Step 1: `ontology.go` + `ontology_test.go`

```go
type EntityClass byte
const (
    ClassEvent     EntityClass = 0 // default for unknown labels
    ClassReference EntityClass = 1
)

type OntologyMapping struct {
    byName   map[string]EntityClass  // label name → class (from config)
    byToken  map[uint16]EntityClass  // cached token → class (populated lazily)
    mu       sync.RWMutex            // protects byToken lazy cache
    labelReg *labelRegistry          // shared ref, set after Graph.New()
}
```

- `NewOntologyMapping(refLabels []string)` — builds `byName` map with all entries as ClassReference
- `ClassifyByName(name string) EntityClass` — O(1) lookup, unknown → `ClassEvent`
- `ClassifyByToken(token uint16) EntityClass` — checks `byToken` cache under RLock, resolves via `labelReg` on miss, caches under Lock
- `SetLabelRegistry(reg *labelRegistry)` — called by Graph.New() after construction

Tests: known ref label, known event label, unknown defaults to event, token-based classification after registry set.

### Step 2: `shard_catalog.go` + `shard_catalog_test.go`

```go
type ShardKind string
const (
    ShardReference ShardKind = "reference"
    ShardEvent     ShardKind = "event"
    ShardArchive   ShardKind = "archive"
)

type ShardTier string
const (
    TierHot  ShardTier = "hot"
    TierWarm ShardTier = "warm"
    TierCold ShardTier = "cold"
)

type ShardEntry struct {
    Name        string    `json:"name"`
    Kind        ShardKind `json:"kind"`
    Tier        ShardTier `json:"tier"`
    Path        string    `json:"path"`
    TimeStart   time.Time `json:"time_start,omitempty"`
    TimeEnd     time.Time `json:"time_end,omitempty"`
    Labels      []string  `json:"labels"`
    RelTypes    []string  `json:"rel_types"`
    ApproxNodes int       `json:"approx_nodes"`
    ApproxRels  int       `json:"approx_rels"`
    Verified    bool      `json:"verified"`
}

type ShardCatalog struct {
    Shards []ShardEntry `json:"shards"`
    path   string
}
```

- `NewShardCatalog(path string) *ShardCatalog`
- `Load() error` / `Save() error` — JSON read/write
- `AddShard(entry ShardEntry)` — append new shard
- `GetShard(name string) (*ShardEntry, bool)` — lookup by name
- `EventShards() []ShardEntry` — all event shards (for iteration)
- `HotEventShard() (*ShardEntry, bool)` — find the hot event shard
- `AddLabel(shardName, label string)` — append-only label tracking
- `AddRelType(shardName, relType string)` — append-only type tracking

Tests: create, save, load round-trip, add shard, add label idempotent, event shard listing.

### Step 3: `registry_file.go` + `registry_file_test.go`

```go
type registryFileData struct {
    Labels   []string `msgpack:"labels"`
    RelTypes []string `msgpack:"reltypes"`
}
```

- `saveRegistryFile(path string, labels, relTypes []string) error` — marshal msgpack, write to tmp, atomic rename
- `loadRegistryFile(path string) (labels, relTypes []string, err error)` — read file, unmarshal; missing file returns nil, nil, nil

Tests: save+load round-trip, empty registries, missing file returns empty without error.

### Step 4: `badgerstore_partial.go` + `badgerstore_partial_test.go`

Unexported methods on `*BadgerStore` for TieredStore's cross-shard relationship routing:

```go
// putRelEntityAndOut writes rel entity (0x02) + typeIdx (0x04) + outIdx (0x05).
// Does NOT write inIdx (0x06). Does NOT verify endpoints (caller's responsibility).
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) putRelEntityAndOut(r *types.Relationship) error

// putRelIncoming writes only inIdx (0x06) entry for a cross-shard relationship.
// The relationship entity is NOT stored in this shard.
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) putRelIncoming(endID, startID snowflake.ID, relType uint16, relID snowflake.ID) error

// deleteRelEntityAndOut removes rel entity + typeIdx + outIdx.
// Does NOT touch inIdx. Returns relDeleteInfo for the companion in-shard deletion.
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) deleteRelEntityAndOut(id snowflake.ID) (relDeleteInfo, error)

// deleteRelIncoming removes only inIdx entry for a cross-shard relationship.
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) deleteRelIncoming(info relDeleteInfo) error

// incomingRelIDs returns relIDs from inIdx for the given node (snapshot under RLock).
// typeToken 0 = all types. Returns sorted slice.
func (bs *BadgerStore) incomingRelIDs(nodeID snowflake.ID, typeToken uint16) []snowflake.ID

// outgoingRelIDs returns relIDs from outIdx for the given node (snapshot under RLock).
func (bs *BadgerStore) outgoingRelIDs(nodeID snowflake.ID) []snowflake.ID

// hasNodeID checks nodeIDs under RLock. O(1).
func (bs *BadgerStore) hasNodeID(id snowflake.ID) bool

// hasRelID checks relIDs under RLock. O(1).
func (bs *BadgerStore) hasRelID(id snowflake.ID) bool
```

These follow the exact patterns of existing `deleteRelByInfo` and `PutRelationship` but split the 7-key write/delete into entity+out vs in components.

Tests: putRelEntityAndOut creates entity but not inIdx, putRelIncoming creates inIdx but not entity, delete variants, hasNodeID/hasRelID.

### Step 5: `tieredstore.go` (core + lifecycle)

```go
type TieredStoreConfig struct {
    DataDir       string            // Root data directory (required unless InMemory)
    InMemory      bool              // In-memory shards (testing only)
    RefLabels     []string          // Labels classified as reference
    ShardWindow   time.Duration     // Event shard time window (default: 1 week)
    CacheCapacity int               // Per-shard LRU capacity (default 10K)
    FlushInterval time.Duration     // Per-shard flush interval (default 100ms)
}

type eventShard struct {
    name      string
    store     *BadgerStore
    tier      ShardTier
    timeStart time.Time
    timeEnd   time.Time
    readOnly  bool
}

type TieredStore struct {
    refShard    *BadgerStore
    refArchive  *BadgerStore            // nil in Phase 3a
    eventShards map[string]*eventShard  // name → shard
    hotShard    *eventShard             // convenience pointer to the currently hot shard
    ontology    *OntologyMapping
    catalog     *ShardCatalog
    regFile     string                  // path to registry.msgpack
    dataDir     string
    closeOnce   sync.Once
}
```

**Constructor flow:**
1. Validate config (DataDir required unless InMemory)
2. Create directory layout: `data/meta/`, `data/reference/`, `data/events/`
3. Load or create shard catalog from `data/meta/shard_catalog.json`
4. Create `OntologyMapping` from `RefLabels`
5. Open reference shard (`NewBadgerStore` at `data/reference/`)
6. Compute current time window name (e.g., "2026-W09")
7. Open hot event shard (`NewBadgerStore` at `data/events/2026-W09/`)
8. Register both shards in catalog
9. If restarting: reopen existing hot shard from catalog (mid-window restart)

**Key methods:**
- `Close() error` — closes all shards (ref + archive + all event shards), saves catalog
- `Clear() error` — clears all open shards
- `shardForNode(n *types.Node) *BadgerStore` — route by primary label token classification
- `shardForNodeID(id snowflake.ID) *BadgerStore` — try ref (hasNodeID O(1)), miss → `timestampToEventShard(id)`
- `shardForRelID(id snowflake.ID) *BadgerStore` — try ref (hasRelID O(1)), miss → `timestampToEventShard(id)`
- `timestampToEventShard(id snowflake.ID) *BadgerStore` — extract timestamp from snowflake ID bits 22–62, find event shard whose `[timeStart, timeEnd)` contains it. Reuses `snowflakeEpoch` and `uint64(id) >> 22` pattern from `temporal_filter.go`
- `classifyEndpoints(r *types.Relationship) (startClass, endClass EntityClass)` — classify both endpoints by probing ref nodeIDs + timestamp fallback
- `allEventStores() []*BadgerStore` — collect all open event shard stores (for merge queries)
- `SaveLabelRegistry` / `LoadLabelRegistry` / `SaveRelTypeRegistry` / `LoadRelTypeRegistry` — delegates to registry_file.go
- `SetLabelRegistry(reg *labelRegistry)` — wires ontology to registry for token resolution

**Timestamp extraction** (reuses existing pattern from `temporal_filter.go:14-20`):
```go
func (ts *TieredStore) timestampToEventShard(id snowflake.ID) *BadgerStore {
    epochMs := snowflakeEpoch.UnixMilli()
    timeMs := int64(uint64(id) >> 22)
    created := time.UnixMilli(epochMs + timeMs)
    for _, es := range ts.eventShards {
        if !created.Before(es.timeStart) && created.Before(es.timeEnd) {
            return es.store
        }
    }
    return ts.hotShard.store // fallback: newest shard
}
```

**Time window naming:**
```go
func shardWindowName(t time.Time, window time.Duration) string
// Weekly: "2026-W09"
// Daily: "2026-03-02"
// Monthly: "2026-03"
```

### Step 6: `tieredstore_write.go` (all mutations)

**Node operations** — single-shard, route by primary label:
- `PutNode(n)` → `ts.shardForNode(n).PutNode(n)`
- `DeleteNode(id)` → `ts.shardForNodeID(id).DeleteNode(id)`
- `ReplaceNode(n)` → `ts.shardForNode(n).ReplaceNode(n)`
- `PutNodesBatch(nodes)` → partition by shard, delegate each partition
- `DeleteNodesBatch(ids)` → partition by shard, delegate each partition

**Relationship operations** — routing by endpoint classification:
```go
func (ts *TieredStore) PutRelationship(r *types.Relationship) error {
    startClass, endClass := ts.classifyEndpoints(r)

    if startClass == endClass {
        // Same shard: delegate entirely (single atomic transaction)
        shard := ts.shardForClass(startClass)
        // Verify both endpoints exist in this shard
        return shard.PutRelationship(r)
    }

    // Cross-shard: verify endpoints exist in their respective shards
    entityShard := ts.shardForClass(startClass)  // entity + out/
    inShard := ts.shardForClass(endClass)         // in/

    // Endpoint verification (both must exist)
    if !entityShard.hasNodeID(r.StartNodeID().SnowflakeID()) { return ErrNodeNotFound }
    if !inShard.hasNodeID(r.EndNodeID().SnowflakeID()) { return ErrNodeNotFound }

    // Split-write ordering per spec §12
    if startClass == ClassEvent {
        // E→R: reference shard (in/) first — critical path
        if err := inShard.putRelIncoming(endID, startID, relType, relID); err != nil { return err }
        if err := entityShard.putRelEntityAndOut(r); err != nil { return err }
    } else {
        // R→E: entity shard (ref) first — entity is the critical path
        if err := entityShard.putRelEntityAndOut(r); err != nil { return err }
        if err := inShard.putRelIncoming(endID, startID, relType, relID); err != nil { return err }
    }
    return nil
}
```

- `DeleteRelationship(id)` — find entity shard via probe, read metadata, cross-shard delete of in/ entries
- `ReplaceRelationship(r)` → `shardForRelID(id).ReplaceRelationship(r)`
- `PutRelationshipsBatch(rels)` → partition same-shard for batch, individual put for cross-shard
- `DeleteRelationshipsBatch(ids)` → per-id delete (cross-shard aware)

**History writes** — route to entity's shard:
- `PutNodeVersion`, `TruncateNodeHistory`, `ReplaceNodeWithHistory` → probe to find shard
- `PutRelVersion`, `TruncateRelHistory`, `ReplaceRelWithHistory` → probe to find shard

### Step 7: `tieredstore_read.go` (all reads)

**Entity reads** — ref probe + timestamp resolution (O(1), no fan-out):
- `GetNode(id)` → try ref (hasNodeID O(1)), miss → `timestampToEventShard(id)` → fetch from target shard
- `GetRelationship(id)` → try ref (hasRelID O(1)), miss → `timestampToEventShard(id)` → fetch
- `GetNodesByIDs(ids)` → per-ID: ref probe + timestamp fallback
- `GetRelationshipsByIDs(ids)` → same

**Label/type queries** — single-shard or merge:
- `NodesByLabel(tok, opts)` → single-shard by label classification
- `RelationshipsByType(tok, opts)` → merge from ref + all event shards
- `AllNodes(opts)` → merge ref + all event shards, interleave by ID
- `AllRelationships(opts)` → same

**Merge helper:**
```go
func mergeNodeSlices(slices [][]*types.Node, limit int) []*types.Node
func mergeRelSlices(slices [][]*types.Relationship, limit int) []*types.Relationship
func mergeIDSlices(slices [][]snowflake.ID, limit int) []snowflake.ID
```
Standard k-way merge of sorted slices with limit. For Phase 3a with 2 shards, this is a simple 2-way merge.

**Adjacency queries:**
- `OutgoingRelationships(nodeID, typeTok)` → delegate to node's shard (entity + out/ co-located)
- `IncomingRelationships(nodeID, typeTok)` → get relIDs from node's shard via `incomingRelIDs`, fetch each entity via timestamp extraction (relID → `timestampToEventShard` → O(1) per entity, zero fan-out)

**Counts** — sum or route:
- `NodeCount()` → ref + sum(event shards)
- `RelationshipCount()` → ref + sum(event shards)
- `NodeCountByLabel(tok)` → single-shard by label
- `RelCountByType(tok)` → ref + sum(event shards)

**Property indexes** — route by label:
- `CreatePropertyIndex(labelTok, key)` → route to label's shard
- `DropPropertyIndex(labelTok, key)` → same
- `NodesByLabelAndProperty(labelTok, key, val, opts)` → same

**History reads** — ref probe + timestamp resolution:
- `GetNodeVersion`, `GetNodeHistory` → ref probe + timestamp fallback (O(1))
- `GetRelVersion`, `GetRelHistory` → same
- `AllNodeHistoryIDs()`, `AllRelHistoryIDs()` → merge from all shards

**Cascade delete:**
```go
func (ts *TieredStore) DeleteNodeCascade(id snowflake.ID) error {
    shard := ts.shardForNodeID(id)
    // Collect all connected relIDs from shard's outIdx + inIdx
    outRels := shard.outgoingRelIDs(id)
    inRels := shard.incomingRelIDs(id, 0) // 0 = all types
    // Delete each relationship (cross-shard aware)
    for _, relID := range dedupIDs(outRels, inRels) {
        ts.DeleteRelationship(relID)
    }
    // Delete the node
    return shard.DeleteNode(id)
}
```

**ID-only queries:**
- `AllNodeIDs(opts)` → merge from all shards
- `AllRelIDs(opts)` → merge from all shards

### Step 8: `graph.go` modifications

```go
// In Close():
switch s := g.store.(type) {
case *BadgerStore:
    // existing registry save logic (unchanged)
case *TieredStore:
    if err := s.SaveLabelRegistry(g.labels); err != nil {
        closeErr = fmt.Errorf("graph: save label registry: %w", err)
    }
    if err := s.SaveRelTypeRegistry(g.relTypes); err != nil {
        closeErr = errors.Join(closeErr, fmt.Errorf("graph: save reltype registry: %w", err))
    }
}

// In New(), after store is set:
if ts, ok := store.(*TieredStore); ok {
    ts.SetLabelRegistry(g.labels)
    if _, err := ts.LoadLabelRegistry(g.labels); err != nil {
        _ = ts.Close()
        return nil, fmt.Errorf("graph: load label registry: %w", err)
    }
    if _, err := ts.LoadRelTypeRegistry(g.relTypes); err != nil {
        _ = ts.Close()
        return nil, fmt.Errorf("graph: load reltype registry: %w", err)
    }
}
```

### Step 9: `tieredstore_test.go` (integration tests)

**Test categories** (~90 tests):

1. **Ontology routing** (8): ref node → ref shard, event node → event shard, unknown → event, multi-label
2. **Node CRUD** (8): put/get/replace/delete for both ref and event nodes
3. **Same-shard relationships** (8): E→E, R→R create/query/delete
4. **Cross-shard relationships** (12): E→R, R→E create/query/delete, verify keys land in correct shards
5. **IncomingRelationships cross-shard** (4): ref node with event rels, event node with ref rels
6. **OutgoingRelationships** (4): delegation to correct shard
7. **Merge queries** (8): AllNodes, AllRels, AllNodeIDs, AllRelIDs, NodeCount, RelCount
8. **DeleteNodeCascade cross-shard** (4): ref node with cross-shard rels, event node with cross-shard rels
9. **History routing** (6): version history follows entity's shard
10. **Batch operations** (6): mixed ref/event in same batch
11. **Property indexes** (4): routed by label classification
12. **Registry file** (4): save/load round-trip through Graph
13. **Lifecycle** (4): close idempotent, clear all shards
14. **Multi-shard model** (6): verify eventShards map, catalog entries, shard listing
15. **Mid-window restart** (4): close and reopen preserves hot shard identity

---

## Verification

1. `make test` — all existing tests pass (TieredStore is additive, no existing code changes except graph.go type switch)
2. `make test-race` — no new race conditions (each BadgerStore handles its own locking)
3. `make cover` — all new public methods ≥80% coverage
4. Cross-shard E→R test: verify `IncomingRelationships(caseID)` returns signals from event shard
5. Cross-shard delete: verify `DeleteRelationship` cleans up both shards
6. Cascade delete: verify `DeleteNodeCascade` handles cross-shard relationships
7. Registry round-trip: create Graph with TieredStore, add entities, close, reopen, verify registries loaded
8. Catalog persistence: verify shard_catalog.json reflects all shards

---

## Risk Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Split-write partial failure | Orphaned in/ entries | Ref-first ordering (§12). Repair scan in Phase 3e. |
| Shard resolution accuracy | Wrong shard on clock skew | Snowflake timestamp is generator-local; shard windows are coarse (weekly). Clock skew within a week has no effect. |
| Cascade not atomic | Partial cleanup on crash | Graph-layer entity locks serialize. Repair in Phase 3e. |
| Counter accuracy | Eventual consistency | Atomic within each shard; sum at read time |
| Endpoint verification for cross-shard rels | E→R: event shard can't verify ref endpoint | TieredStore verifies both endpoints exist before delegating partial writes |
| ID not in any event shard window | Entity created before oldest shard | `timestampToEventShard` falls back to hot shard; Phase 3c adds cold/archive handling |
