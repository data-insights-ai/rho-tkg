# tkg-v3

**Temporal Knowledge Graph v3** — the internal Go library powering the core graph engine for temporal knowledge graphs.

tkg-v3 is a **pure library** (no main binary, no HTTP server, no query language). It provides the low-level graph types, persistence layer, and entity management that higher-level products build on.

For the full product with Cypher queries, Vadalog reasoning, and an HTTP/gRPC server, see **tkgd-v3**.

| Layer | Repository | What it provides |
|---|---|---|
| **tkg-v3** (this repo) | `rho/tkg-v3` | Graph types, registries, MemoryStore, BadgerStore, TieredStore, entity locks |
| **tkgd-v3** | `rho/tkgd-v3` | Cypher engine, Vadalog reasoning, HTTP/gRPC server, REST API |

## Module

```
gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3
```

**Go:** 1.26.0
**License:** Apache-2.0
**Dependencies:** [`rho-snowflake-2026`](https://github.com/bds421/rho-snowflake-2026) (IDs), [`msgpack/v5`](https://github.com/vmihailenco/msgpack) (serialization), [`badger/v4`](https://github.com/dgraph-io/badger) (persistence)

## Architecture

### Core Types (`pkg/types`)

| Type | Purpose |
|------|---------|
| `Node` | Graph vertex with opaque `nodeID` (wraps `snowflake.ID`), primary + extra labels (`labelToken`), properties, version, temporal, integrity |
| `Relationship` | Directed edge with opaque `relID` (wraps `snowflake.ID`), type token (`relTypeToken`), start/end `nodeID`, properties, version, temporal, integrity |
| `PropertySlice` | Sorted key-value store with binary search; recursive validation rejects `tkg_` prefix keys and non-allowlisted types (pointers, structs, arrays, channels, functions, unsafe pointers) at any nesting depth; depth-limited to 32 levels |
| `Instant` | Semantic wrapper for Unix-millisecond timestamps used by all temporal fields |
| `TemporalMetadata` | Temporal lifecycle metadata: `ValidFrom`, `ValidTo`, `TxFrom`, `TxTo`, `CreatedAt`, `UpdatedAt`, `DeletedAt` (all `Instant`), `CreatedBy`, `UpdatedBy` (`string`), `BaseEntityID` (`snowflake.ID`) |
| `NodeIntegrity` / `RelIntegrity` | Hash-chain integrity: `Hash`, `PrevHash` (`string`); `AuthorID` (`string`), `Signature` (`[]byte`) for caller-supplied provenance. `RelIntegrity` also carries `FromNodeHash` / `ToNodeHash` — hashes of the endpoint nodes at write time (not fed into `ComputeRelHash`) |
| `TimeGranularity` | 8-level granularity enum: `GranMillisecond`, `GranSecond`, `GranMinute`, `GranHour`, `GranDay`, `GranWeek`, `GranMonth`, `GranYear`. Functions: `TruncateInstant(t, gran)`, `RoundInstant(t, gran)`, `CeilInstant(t, gran)` |
| `AllenRelation` | Full 13-relation Allen's interval algebra: `Relate(a, b Interval) AllenRelation`, `ComposeSets(rs1, rs2 AllenSet) AllenSet`. Constants: `Before`, `Meets`, `Overlaps`, `Starts`, `During`, `Finishes`, `Equal` and their converses |
| `RecurrencePattern` | Recurring time pattern: `Frequency` (`Daily`/`Weekly`/`Monthly`/`Yearly`), `Days` (`WeekdayMask`), `DayOfMonth` (1–28; 0 = last), `Month`, `DayStart`/`DayEnd` (duration from UTC midnight). `Validate()` checks invariants; `Expand(from, to Instant) []Interval` emits concrete intervals within the window. Returns `ErrInvalidTimeRange` if `from >= to` |

### Graph Layer (`pkg/graph`)

| Type | Purpose |
|------|---------|
| `Graph` | Central graph layer — owns registries, dual snowflake generators, store, entity management (`AddNode`/`AddRelationship`/`UpdateNode`/`UpdateRelationship`/`DeleteNode`), convenience property methods, shadow resolution, string resolution |
| `Store` | Persistence interface — CRUD, index/adjacency/bulk queries with cursor-based pagination (`QueryOpts`), lazy ForEach iterators (`ForEachNodeID`, `ForEachRelID`, `ForEachNodeHistoryID`, `ForEachRelHistoryID`), batch operations (`PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`), counts, `Close()` |
| `BatchBuilder` | Fluent API for queuing graph operations with eager validation and deferred persistence — `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `Execute` |
| `MemoryStore` | Thread-safe in-memory `Store` with hash-set adjacency indexes for O(1) insert/delete, no-op `Close()` |
| `BadgerStore` | Persistent `Store` using Badger v4 with msgpack serialization, fixed-width binary keys, label/type/adjacency indexes, LRU caches with dirty tracking. Optional `ReadOnly` mode for warm/cold shards |
| `TieredStore` | Multi-shard `Store` routing entities across ref shard + time-windowed event shards by ontology classification. Hot→warm→cold shard rotation, warm recovery on restart, lazy-open cold shards, depth-aware reads, cross-shard relationships |
| `GraphTx` | Mutation transaction — holds graph write lock, tracks created/updated/deleted entities, supports commit and snapshot-based rollback (full CRUD) |
| `OntologyMapping` | Classifies labels as `ClassReference` (long-lived) or `ClassEvent` (time-windowed, default). Lazy token cache backed by label registry |
| `ShardCatalog` | JSON-persisted catalog of all shards — tracks time windows, tiers, labels, rel types, verification status. Atomic write via write-tmp + rename |
| `labelRegistry` | Thread-safe bidirectional label string ↔ uint16 token mapping (persisted to Badger or registry file on `Close()`) |
| `relTypeRegistry` | Thread-safe bidirectional relationship type string ↔ uint16 token mapping (persisted to Badger or registry file on `Close()`) |

Entity management: `AddNode(labels, props)`, `AddRelationship(typeName, start, end, props)`, `UpdateNode(id, updates)`, `UpdateRelationship(id, updates)`, `UpdateNodeInPlace(id, updates)`, `UpdateRelInPlace(id, updates)`, `DeleteNode(id)` (cascade), `DeleteRelationship(id)`, `RemoveNodeLabel(id, label)`. `UpdateNodeInPlace` / `UpdateRelInPlace` mutate the current entity in-place (no version bump, no history entry) — for correcting metadata without creating a new version. `RemoveNodeLabel` removes a label from a node's label set; returns `ErrLastLabel` if it is the only label, `ErrLabelNotFound` if not present.

Convenience methods: `SetNodeProperty(id, key, value)`, `DeleteNodeProperty(id, key)`, `SetRelationshipProperty(id, key, value)`, `DeleteRelationshipProperty(id, key)`.

Resolution methods: `NodeLabels(n)`, `NodePrimaryLabel(n)`, `NodeHasLabel(n, label)`, `RelationshipType(r)`, `RelationshipHasType(r, typ)`.

Shadow resolution: `ResolveNodeProperty(n, key)`, `ResolveRelProperty(r, key)` — dispatches all 21 `tkg_*` keys with nil-guards.

Registry methods: `GetOrCreateLabel(name)`, `GetOrCreateRelType(name)`, `LookupLabel(name)`, `LookupRelType(name)`.

Store queries: `GetNode(id)`, `GetRelationship(id)`, `NodesByLabel(label, opts)`, `RelationshipsByType(typeName, opts)`, `OutgoingRelationships(nodeID, typeName)`, `IncomingRelationships(nodeID, typeName)`, `NodeCount()`, `RelationshipCount()`. Five unbounded query methods accept `QueryOpts{Limit, After, ValidAt, ValidStart, ValidEnd, Depth}` for cursor-based pagination, temporal push-down, and shard-depth filtering; zero values mean "return all / no filter / all tiers". `Depth` controls which TieredStore shard tiers to include: `DepthAll` (default), `DepthHot`, `DepthWarm`; ignored by MemoryStore/BadgerStore.

Bulk queries: `AllNodes(opts)`, `AllRelationships(opts)`, `GetNodesByIDs(ids)`, `GetRelationshipsByIDs(ids)` — all return results sorted by snowflake.ID; missing IDs are silently skipped.

Batch operations: `NewBatchBuilder(g)` creates a builder that queues operations with eager validation. Call `AddNode(labels, props)`, `AddRelationship(typeName, start, end, props)`, `UpdateNode(id, updates)`, `UpdateRelationship(id, updates)`, `DeleteNode(id)`, `DeleteRelationship(id)` to queue operations. Call `Execute()` to persist all operations in order (creates → updates → deletes). Returns a `BatchResult` with counts and per-operation errors. Store-level batch methods (`PutNodesBatch`, `DeleteNodesBatch`, etc.) use two-phase validate-then-apply for atomicity.

Temporal queries: `GetNodesValidAt(t)`, `GetRelationshipsValidAt(t)`, `GetNodesByLabelValidAt(label, t)` — point-in-time queries. `GetNodesValidDuring(start, end)`, `GetRelationshipsValidDuring(start, end)` — interval queries. `GetNodeAt(id, t)`, `GetRelAt(id, t)` — version-specific queries. `GetNeighborsValidAt(nodeID, t)` — temporal neighbor traversal. `Snapshot(t)` — full graph state at a point in time (endpoints-filtered). All temporal queries are history-aware (include deleted entities via lazy ForEach iterators — no slice materialization, ~83% memory reduction vs pre-v3.0.31). Nodes without explicit temporal metadata derive valid-from from their snowflake ID timestamp.

Version chain navigation: `GetPreviousNodeVersion(id, version)` / `GetNextNodeVersion(id, version)` — navigate the version chain by stepping one version backward or forward. Return nil, nil when the requested version does not exist (genesis has no predecessor; the current tip has no successor). `GetNextNodeVersion` checks the history store first, then falls back to the current entity (which may itself be the next version). `CloseNodeVersion(id, t)` / `CloseRelVersion(id, t)` — set `ValidTo` on the current entity in-place without incrementing its version or creating a history entry. Returns `ErrAlreadyClosed` if `ValidTo` is already set. Rel mirrors: `GetPreviousRelVersion`, `GetNextRelVersion`, `CloseRelVersion`.

Event system: `NewEventBus()` creates a synchronous dispatcher. `bus.Subscribe(handler)` registers an `EventHandler` (type `func(Event)`) and returns an idempotent unsubscribe function. `Event` carries `Type EventType`, `EntityID snowflake.ID`, `Timestamp types.Instant`, and `Priority EventPriority`. Six event types: `EventNodeCreate`, `EventNodeUpdate`, `EventNodeDelete`, `EventRelCreate`, `EventRelUpdate`, `EventRelDelete`. Internal priorities: creates → `PriorityHigh`, deletes → `PriorityCritical`, updates → `PriorityNormal`. Handlers are invoked synchronously after each successful store write, outside the EventBus lock (prevents deadlocks when handlers re-enter the Graph). Attach to a graph with `g.SetEventBus(bus)`. No-op when no bus is set. Hook points: `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `CloseNodeVersion`, `CloseRelVersion`.

For async delivery, use `NewAsyncEventBus(AsyncEventBusConfig{Workers, QueueSize, Backpressure})` instead. Workers drain a per-priority queue pool (one channel per `EventPriority` level), decoupling slow handler latency from write latency. Three backpressure strategies: `BackpressureBlock` (caller blocks until queue drains), `BackpressureDropOldest` (evict oldest event), `BackpressureDropLatest` (discard new event). Attach with `g.SetAsyncEventBus(bus)`. Call `bus.Close()` to drain all remaining events and stop workers. Five `EventPriority` levels: `PriorityNormal` (0, zero value), `PriorityHigh` (1), `PriorityCritical` (2), `PriorityLow` (3), `PriorityDeferred` (4). Workers drain in Critical→High→Normal→Low→Deferred order.

Snapshot diff: `DiffSnapshots(t1, t2)` compares two temporal snapshots and returns a `*SnapshotDiff` with `NodesCreated`, `NodesUpdated` (as `[]NodeUpdate{Before, After}`), `NodesDeleted`, `RelsCreated`, `RelsUpdated` (as `[]RelUpdate{Before, After}`), `RelsDeleted`. Classification: entity present only at t2 → Created; present only at t1 → Deleted; present at both with a different integrity hash → Updated; same hash → Unchanged (omitted). Does not hold `g.mu.RLock` — trades strong isolation for non-blocking writes (a concurrent backdated write between the two snapshot reads may appear as a spurious Created/Deleted entry). Returns `ErrInvalidTimeRange` if t1 ≥ t2 or either is zero.

Combined queries: `NodesByLabelPropertyAndTime(label, key, value, t)` — intersects property index with point-in-time filter. `NodesByLabelPropertyDuring(label, key, value, start, end)` — intersects property index with interval filter.

Hash chain verification: `VerifyNodeHashChain(id)`, `VerifyRelHashChain(id)` — verify the full hash chain for an entity's version history. Handles deleted entities (verifies history chain alone when current entity is gone). Returns `(true, nil)` if valid.

Transactions: `BeginTx()` starts a mutation transaction holding the graph write lock. `tx.AddNode(labels, props)`, `tx.AddRelationship(typeName, start, end, props)` — create entities and track IDs. `tx.UpdateNode(id, updates)`, `tx.UpdateRelationship(id, updates)` — snapshot pre-mutation state then apply. `tx.SetNodeProperty(id, key, value)`, `tx.DeleteNodeProperty(id, key)`, `tx.SetRelationshipProperty(id, key, value)`, `tx.DeleteRelationshipProperty(id, key)` — convenience wrappers. `tx.DeleteNode(id)` (cascade), `tx.DeleteRelationship(id)` — snapshot before deletion. `tx.Commit()` releases the lock. `tx.Rollback()` restores all mutations in reverse order: deleted entities are re-created, updates are reverted to snapshots, created entities are deleted. `tx.CreatedNodeIDs()`, `tx.CreatedRelIDs()` for inspection.

Reset: `Reset()` atomically clears all entities, indexes, history, and counters while preserving label and relationship type registries.

Export/Import: `ExportGraph(w io.Writer)` writes a portable format-independent snapshot to `w` — header, label/reltype registries, all current nodes and relationships, and their full version history. Wire format: length-prefixed msgpack record stream with 1-byte type tags; forward-compatible (unknown tags are skipped on import). Holds `g.mu.RLock` for a consistent snapshot. `ImportGraph(r io.Reader)` reads the stream and restores into the graph, holding `g.mu.Lock`. Registry import is idempotent when the existing token mappings are identical to the export (safe re-import). Returns `ErrIncompatibleExport` on format version mismatch; returns `ErrIncompatibleRegistry` when the existing registry maps tokens differently from the export (importing would corrupt all entity labels or relationship types). Per-record allocations are capped at 128 MiB. Use for backup, migration across store versions, or seeding test fixtures.

Statistics: `NodeCountByLabel(label)`, `RelCountByType(typeName)`, `AllLabelCounts()`, `AllRelTypeCounts()` — O(1) cardinality statistics for all labels and relationship types. MemoryStore uses existing index sizes; BadgerStore maintains `sync.Map` + `atomic.Int64` counters.

Property indexes: `CreatePropertyIndex(label, propertyKey)`, `DropPropertyIndex(label, propertyKey)` — create/drop in-memory property indexes. `NodesByLabelAndProperty(label, key, value, opts)` — O(1) indexed lookup with cursor-based pagination. Indexes are automatically maintained across all node mutation paths and persist across BadgerStore restarts. In TieredStore, property indexes are restricted to reference entities (`ErrEventPropertyIndex` for event labels).

Temporal indexes: `CreateTemporalIndex(label)`, `DropTemporalIndex(label)` — create/drop in-memory interval indexes accelerating `NodesByLabel` when a temporal filter (`ValidAt` or `ValidStart`/`ValidEnd`) is set. Uses a sorted-slice interval index with lazy sort (`sortIfDirty` + `sortMu` for concurrent-read safety) and O(log n) range filtering. When a temporal index exists and a filter is active, `NodesByLabel` uses the index fast path instead of scanning all label entries. Indexes are automatically maintained across all node mutation paths and persist across BadgerStore restarts (label tokens stored, index data rebuilt from nodes on startup). TieredStore creates the index on all active shards and propagates it to new hot shards on rotation.

High-frequency temporal indexes: `CreateHighFrequencyIndex(label, bucketSize)`, `DropHighFrequencyIndex(label)` — an alternative to the sorted-slice temporal index for labels under high write rates (thousands of events/sec). Uses time-bucketed storage: bucket index = `(validFrom - origin) / bucketSize`. Insertion is O(1) amortized; range queries visit O(buckets_in_range) buckets. Only one temporal index type can exist per label at a time — drop the existing index before switching. Not persisted; must be re-created after restart. TieredStore fans out to all active shards.

Transaction-time queries (bitemporality): `GetNodeAsOf(id, txTime)`, `GetRelAsOf(id, txTime)` — return the entity version that was committed at or before `txTime` (based on `TxFrom`/`TxTo`, which are populated automatically on all mutations). `GetNodesAsOf(txTime)`, `GetRelsAsOf(txTime)` — scan all known entity IDs (current + history) and return those with a version active at `txTime`. Returns `ErrNoVersionAsOf` when no version was recorded at the given transaction time.

Vector indexes: `CreateVectorIndex(label, propertyKey, dims, metric)`, `DropVectorIndex(label, propertyKey)` — create/drop in-memory brute-force k-NN indexes on nodes with the given label and a `[]float32` property. `metric` is `DistanceCosine` or `DistanceEuclidean`. `SearchNearestNodes(label, propertyKey, query []float32, k int, opts)` returns the `k` closest nodes in ranked order. Returns `ErrVectorIndexExists` / `ErrVectorIndexNotFound` / `ErrDimensionMismatch` on error. Indexes are maintained automatically across all node mutation paths; not persisted across restarts (rebuilt from properties on startup is not automatic — recreate the index after reopening).

Temporal constraints: `AddTemporalConstraint(c)`, `SetTemporalConstraints(cs)`, `TemporalConstraints()` — configure write-time enforcement rules. `TemporalConstraint{Kind: ConstraintRelWithinEndpoints}` enforces that a relationship's validity interval is contained within the intersection of both endpoint nodes' validity. Checked during `AddRelationship` and `ImportRelationshipWithID`. Violations return errors wrapping `ErrTemporalConstraint`; the specific cause (e.g., `ErrRelBeforeStartNode`, `ErrRelAfterEndNode`, `ErrRelExceedsStartNodeValidity`) is accessible via `errors.Is`. Zero value `ConstraintSet` means no constraints.

Validation limits: `Config.Validation` accepts a `ValidationLimits` struct with configurable maximums: `MaxLabelsPerNode` (default 50), `MaxPropertiesPerEntity` (default 1000), `MaxPropertyKeyLength` (default 256), `MaxPropertyValueSize` (default 65536), `MaxNameLength` (default 256). `AllowSelfLoops bool` (default `false`) — when false, `AddRelationship` and `ImportRelationshipWithID` reject self-loop relationships (start == end) with `ErrSelfLoop`; set to `true` to permit them. Enforced at all graph entry points. Zero values use defaults.

Archive: `ArchiveNode(id)` moves a reference node and its relationships from the reference shard to the archive (TieredStore only). `RestoreNode(id)` moves it back.

Admin & repair (TieredStore only): `ForceRotate()` triggers a safe hot-shard rotation with internal locking. `ListShards()` returns `[]ShardInfo` with live counts from open stores. `RebuildCatalog()` reconstructs the shard catalog from live state. `VerifyShard(name)` runs hash chain verification with immutable-shard caching. `RunRepair()` scans for cross-shard split-write inconsistencies and fixes orphaned/missing in/ entries.

ID decomposition: `DecomposeID(id)` extracts `IDComponents{CreatedAt, NodeID, Sequence}` from any snowflake ID (works with all store types).

Migration: `MigrateFromBadger(src, dst, labels)` copies all entities from a single BadgerStore to a TieredStore with automatic ontology-based routing.

Lifecycle: `Close()` saves registries (BadgerStore or TieredStore), then calls `store.Close()` on every Store implementation. MemoryStore.Close() returns nil. TieredStore.Close() saves catalog, closes all event shards, reference shard, and archive. Always call `Close()` when done — it is safe to call multiple times.

### Persistence (Badger)

Configure with `Config.BadgerDir` (on-disk) or `Config.BadgerInMemory: true` (testing):

```go
g, err := graph.New(graph.Config{
    SnowflakeNodeID: 1,
    BadgerDir:       "/path/to/data",
})
// ... use graph ...
g.Close() // saves registries + closes DB
```

Data is serialized using msgpack. Keys use fixed-width binary encoding with single-byte prefix tags for correct sort order. Registries are persisted on `Close()` and restored on startup.

Sync writes: set `Config.SyncWrites: true` to eliminate the 100ms async flush window — each write is flushed to disk synchronously (Badger `WithSyncWrites(true)` + immediate `flush()` after every store call). This removes the in-memory buffer vulnerability at the cost of higher write latency. `FlushInterval` is forced to 0 and the background flush goroutine is not started when `SyncWrites` is true.

### Tiered Persistence (TieredStore)

For workloads with distinct reference data (Case, User) and high-volume events (Signal, Alert):

```go
ts, err := graph.NewTieredStore(graph.TieredStoreConfig{
    DataDir:     "/path/to/data",
    RefLabels:   []string{"Case", "Organization", "User"},
    ShardWindow: 7 * 24 * time.Hour, // weekly event shards
    ColdAfter:   30 * 24 * time.Hour, // demote warm→cold after 30 days
})
g, err := graph.New(graph.Config{
    SnowflakeNodeID: 1,
    Store:           ts,
})
```

Directory layout: `data/meta/` (catalog + registry), `data/reference/` (ref shard), `data/events/<window>/` (event shards), `data/archive/` (archived reference entities). Hot shard receives all new event writes. On window expiry, `RotateHotShard()` demotes hot→warm (read-only) and creates a new hot shard. Warm shards are recovered from catalog on restart. Cold shards are lazy-opened on first access and auto-closed after idle timeout.

### Snowflake Configuration

Both generators are initialized with explicit parameters matching the spec:

| Parameter | Value |
|-----------|-------|
| Epoch | `2026-01-01 00:00:00 UTC` |
| Node bits | 10 (1024 instances) |
| Step bits | 12 (4096 IDs/ms) |

Each concurrent graph instance **must** use a different `Config.SnowflakeNodeID` (0-511). Generators are stateless — no counter persistence, no crash recovery.

### Design Invariants

- **Pure-data structs**: Node and Relationship hold no references to Graph, registries, or resolvers. They are self-contained data containers.
- **snowflake.ID everywhere**: All entity and reference IDs are `snowflake.ID`. Opaque wrapper types (`nodeID`/`relID`) prevent external construction or comparison.
- **Strict encapsulation**: All struct fields are unexported. Access through methods only.
- **Defensive copying**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, `DeepCopy()`, `ToMap()`, and `PropertiesMap()` always return independent copies.
- **Token 0 reserved**: Token 0 is invalid. `HasLabelToken(0)`, `HasTypeToken(0)`, `HasLabelTokenRaw(0)`, and `HasTypeTokenRaw(0)` always return false. Constructors panic on token 0 (both primary and extra labels).
- **Extra label deduplication**: `NewNode` deduplicates extra labels and removes the primary label from extras.
- **Allowlist property validation**: `PropertySlice.Set()` recursively validates values using an allowlist. Only primitives (`bool`, `int*`, `uint*`, `float*`, `string`), slices, and maps with safe element types are accepted. Pointers, structs, arrays, channels, functions, and unsafe pointers are rejected at any nesting depth (`ErrUnsupportedValueType`).
- **Shadow property protection**: The `tkg_` prefix is reserved. `PropertySlice.Set()` and `Delete()` reject any key starting with `tkg_`. Errors wrap `ErrReservedPrefix` for programmatic discrimination via `errors.Is`.
- **Opaque token types**: Label and relationship type tokens use unexported `labelToken` and `relTypeToken` types, preventing accidental misuse of raw `uint16` values.
- **Zero-allocation token checks**: `HasLabelTokenRaw(uint16)` on Node and `HasTypeTokenRaw(uint16)` on Relationship for graph-layer hot paths. Token 0 returns false.
- **Depth-limited validation**: Recursive validation and deep-copy stop at `maxPropertyDepth` (32). `Set()` returns `ErrMaxDepthExceeded` for deeper structures.
- **Registry input validation**: `GetOrCreate("")` returns `ErrEmptyName`. Empty strings are never assigned tokens.
- **Shared-pointer accessors**: `Temporal()` and `Integrity()` return the internal pointer — no defensive copy. The graph layer needs mutation access; external callers should treat as read-only.
- **Bulk property construction**: `NewPropertySlice(map[string]any)` is O(N log N) — allocate once, validate all, sort once. Avoids the O(N²) per-property `SetProperty` loop for entity creation.
- **Store is pure persistence**: The `Store` interface handles entity storage, index maintenance, and resource cleanup (`Close()`). Shadow resolution, referential integrity (cascade-delete), and string resolution live on Graph. Three implementations: MemoryStore (in-memory), BadgerStore (persistent), TieredStore (multi-shard).
- **TieredStore shard routing**: Reference entities go to `refShard`; event entities go to time-windowed event shards. Shard resolution for existing entities is O(1) via `shardForNodeID` (ref probe + snowflake timestamp extraction). Cross-shard relationships use split writes: entity+out/ in start shard, in/ in end shard. `shardForRelID` probes all event shards for cross-shard entities. Merge queries use parallel goroutines per shard.
- **Update operations**: `UpdateNode(id, updates)` / `UpdateRelationship(id, updates)` perform read-modify-write under entity lock. Property keys with `nil` values are deleted; non-nil values are set/overwritten. Each update bumps the version counter and sets `temporal.UpdatedAt`. Empty updates map is a no-op (no lock, no version bump). Pre-validates all keys (`tkg_` prefix rejected) and values (`ValidatePropertyValue`) before acquiring the lock.
- **Replace vs Put semantics**: `ReplaceNode`/`ReplaceRelationship` (Store interface) require existence — return `ErrNodeNotFound`/`ErrRelNotFound` if missing. `PutNode`/`PutRelationship` reject duplicates — return `ErrNodeExists`/`ErrRelExists` if present. Replace overwrites entity data only; labels and relationship type/endpoints are immutable after creation — no index changes.
- **Cascade-delete on node removal**: `Graph.DeleteNode` removes all outgoing and incoming relationships before the node. Self-loops are handled by skipping `ErrRelNotFound` in the incoming pass. The tombstone history entries for all cascaded relationships and the node itself are written atomically in a single store call (`DeleteNodeWithHistory`) — no orphaned history entries on crash.
- **SnowflakeID bridges**: `nodeID.SnowflakeID()`, `relID.SnowflakeID()`, `entityID.SnowflakeID()` — exported methods on unexported wrapper types allow cross-package persistence key extraction without leaking the `snowflake.ID` dependency into entity method signatures.
- **Shadow resolution nil-guards**: `ResolveNodeProperty` / `ResolveRelProperty` check `Temporal() != nil` and `Integrity() != nil` before accessing fields. New entities without metadata return `(nil, false)` instead of panicking.

### Shadow Properties (21)

Read-only virtual properties managed by the graph layer:

| Key | Type | Applies To |
|-----|------|------------|
| `tkg_labels` | `[]string` | Node |
| `tkg_type` | `string` | Relationship |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both |
| `tkg_created_at`, `tkg_updated_at`, `tkg_deleted_at` | `Instant` | Both |
| `tkg_created_by`, `tkg_updated_by` | `string` | Both |
| `tkg_version` | `uint32` | Both |
| `tkg_hash`, `tkg_prev_hash` | `string` | Both |
| `tkg_base_entity` | `snowflake.ID` | Both |
| `tkg_from_hash`, `tkg_to_hash` | `string` | Relationship only |
| `tkg_author_id` | `string` | Both |
| `tkg_signature` | `[]byte` | Both |
| `tkg_authorized_by` | `string` | Both |
| `tkg_auth_level` | `uint8` | Both |

`tkg_author_id`, `tkg_signature`, `tkg_authorized_by`, and `tkg_auth_level` are write-path shadow keys: pass them in the `props`/`updates` map of any Add or Update call to store provenance/authorization on the integrity struct. They are stripped before `PropertySlice` construction (never stored as real properties) and readable back via `ResolveNodeProperty` / `ResolveRelProperty`. `tkg_auth_level` accepts `uint8`, `int`, `int32`, `int64`, or `float64`; values outside `[0, 255]` or non-numeric types return an error.

## Build & Test

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector enabled
make cover          # coverage report -> coverage.html
make check          # pre-commit: vet + build + test
make ci             # full pipeline: fmt-check + vet + build + test-race + security + vulncheck
make fmt            # format code
make security       # gosec static analysis
make vulncheck      # govulncheck for known CVEs
```

Run a single test:

```bash
go test -run TestFoo ./pkg/types/
```

## Tutorials

Progressive tutorials in `tutorials/`, each a standalone `main.go`:

| Tutorial | Topic |
|----------|-------|
| `001_basic_graph` | Create nodes, relationships, and query the graph |
| `002_temporal` | Temporal metadata (ValidFrom/ValidTo, CreatedAt, UpdatedAt) |
| `003_badger_persistence` | On-disk BadgerStore, close/reopen, registry persistence |
| `004_full_features` | Update operations, version history, hash chain integrity |
| `005_performance` | Benchmark MemoryStore vs BadgerStore (throughput, memory, storage) |

Run any tutorial: `go run ./tutorials/001_basic_graph/`

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
