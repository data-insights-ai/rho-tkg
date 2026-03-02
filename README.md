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
| `NodeIntegrity` / `RelIntegrity` | Hash-chain integrity: `Hash`, `PrevHash` (`string`) |

### Graph Layer (`pkg/graph`)

| Type | Purpose |
|------|---------|
| `Graph` | Central graph layer — owns registries, dual snowflake generators, store, entity management (`AddNode`/`AddRelationship`/`UpdateNode`/`UpdateRelationship`/`DeleteNode`), convenience property methods, shadow resolution, string resolution |
| `Store` | Persistence interface — CRUD, index/adjacency/bulk queries with cursor-based pagination (`QueryOpts`), batch operations (`PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`), counts, `Close()` |
| `BatchBuilder` | Fluent API for queuing graph operations with eager validation and deferred persistence — `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `Execute` |
| `MemoryStore` | Thread-safe in-memory `Store` with hash-set adjacency indexes for O(1) insert/delete, no-op `Close()` |
| `BadgerStore` | Persistent `Store` using Badger v4 with msgpack serialization, fixed-width binary keys, label/type/adjacency indexes, LRU caches with dirty tracking. Optional `ReadOnly` mode for warm/cold shards |
| `TieredStore` | Multi-shard `Store` routing entities across ref shard + time-windowed event shards by ontology classification. Hot→warm→cold shard rotation, warm recovery on restart, lazy-open cold shards, depth-aware reads, cross-shard relationships |
| `GraphTx` | Mutation transaction — holds graph write lock, tracks created/updated/deleted entities, supports commit and snapshot-based rollback (full CRUD) |
| `OntologyMapping` | Classifies labels as `ClassReference` (long-lived) or `ClassEvent` (time-windowed, default). Lazy token cache backed by label registry |
| `ShardCatalog` | JSON-persisted catalog of all shards — tracks time windows, tiers, labels, rel types, verification status. Atomic write via write-tmp + rename |
| `labelRegistry` | Thread-safe bidirectional label string ↔ uint16 token mapping (persisted to Badger or registry file on `Close()`) |
| `relTypeRegistry` | Thread-safe bidirectional relationship type string ↔ uint16 token mapping (persisted to Badger or registry file on `Close()`) |

Entity management: `AddNode(labels, props)`, `AddRelationship(typeName, start, end, props)`, `UpdateNode(id, updates)`, `UpdateRelationship(id, updates)`, `DeleteNode(id)` (cascade), `DeleteRelationship(id)`.

Convenience methods: `SetNodeProperty(id, key, value)`, `DeleteNodeProperty(id, key)`, `SetRelationshipProperty(id, key, value)`, `DeleteRelationshipProperty(id, key)`.

Resolution methods: `NodeLabels(n)`, `NodePrimaryLabel(n)`, `NodeHasLabel(n, label)`, `RelationshipType(r)`, `RelationshipHasType(r, typ)`.

Shadow resolution: `ResolveNodeProperty(n, key)`, `ResolveRelProperty(r, key)` — dispatches all 15 `tkg_*` keys with nil-guards.

Registry methods: `GetOrCreateLabel(name)`, `GetOrCreateRelType(name)`, `LookupLabel(name)`, `LookupRelType(name)`.

Store queries: `GetNode(id)`, `GetRelationship(id)`, `NodesByLabel(label, opts)`, `RelationshipsByType(typeName, opts)`, `OutgoingRelationships(nodeID, typeName)`, `IncomingRelationships(nodeID, typeName)`, `NodeCount()`, `RelationshipCount()`. Five unbounded query methods accept `QueryOpts{Limit, After, ValidAt, ValidStart, ValidEnd, Depth}` for cursor-based pagination, temporal push-down, and shard-depth filtering; zero values mean "return all / no filter / all tiers". `Depth` controls which TieredStore shard tiers to include: `DepthAll` (default), `DepthHot`, `DepthWarm`; ignored by MemoryStore/BadgerStore.

Bulk queries: `AllNodes(opts)`, `AllRelationships(opts)`, `GetNodesByIDs(ids)`, `GetRelationshipsByIDs(ids)` — all return results sorted by snowflake.ID; missing IDs are silently skipped.

Batch operations: `NewBatchBuilder(g)` creates a builder that queues operations with eager validation. Call `AddNode(labels, props)`, `AddRelationship(typeName, start, end, props)`, `UpdateNode(id, updates)`, `UpdateRelationship(id, updates)`, `DeleteNode(id)`, `DeleteRelationship(id)` to queue operations. Call `Execute()` to persist all operations in order (creates → updates → deletes). Returns a `BatchResult` with counts and per-operation errors. Store-level batch methods (`PutNodesBatch`, `DeleteNodesBatch`, etc.) use two-phase validate-then-apply for atomicity.

Temporal queries: `GetNodesValidAt(t)`, `GetRelationshipsValidAt(t)`, `GetNodesByLabelValidAt(label, t)` — point-in-time queries. `GetNodesValidDuring(start, end)`, `GetRelationshipsValidDuring(start, end)` — interval queries. `GetNodeAt(id, t)`, `GetRelAt(id, t)` — version-specific queries. `GetNeighborsValidAt(nodeID, t)` — temporal neighbor traversal. `Snapshot(t)` — full graph state at a point in time (endpoints-filtered). All temporal queries are history-aware (include deleted entities). Nodes without explicit temporal metadata derive valid-from from their snowflake ID timestamp.

Combined queries: `NodesByLabelPropertyAndTime(label, key, value, t)` — intersects property index with point-in-time filter. `NodesByLabelPropertyDuring(label, key, value, start, end)` — intersects property index with interval filter.

Hash chain verification: `VerifyNodeHashChain(id)`, `VerifyRelHashChain(id)` — verify the full hash chain for an entity's version history. Handles deleted entities (verifies history chain alone when current entity is gone). Returns `(true, nil)` if valid.

Transactions: `BeginTx()` starts a mutation transaction holding the graph write lock. `tx.AddNode(labels, props)`, `tx.AddRelationship(typeName, start, end, props)` — create entities and track IDs. `tx.UpdateNode(id, updates)`, `tx.UpdateRelationship(id, updates)` — snapshot pre-mutation state then apply. `tx.SetNodeProperty(id, key, value)`, `tx.DeleteNodeProperty(id, key)`, `tx.SetRelationshipProperty(id, key, value)`, `tx.DeleteRelationshipProperty(id, key)` — convenience wrappers. `tx.DeleteNode(id)` (cascade), `tx.DeleteRelationship(id)` — snapshot before deletion. `tx.Commit()` releases the lock. `tx.Rollback()` restores all mutations in reverse order: deleted entities are re-created, updates are reverted to snapshots, created entities are deleted. `tx.CreatedNodeIDs()`, `tx.CreatedRelIDs()` for inspection.

Reset: `Reset()` atomically clears all entities, indexes, history, and counters while preserving label and relationship type registries.

Statistics: `NodeCountByLabel(label)`, `RelCountByType(typeName)`, `AllLabelCounts()`, `AllRelTypeCounts()` — O(1) cardinality statistics for all labels and relationship types. MemoryStore uses existing index sizes; BadgerStore maintains `sync.Map` + `atomic.Int64` counters.

Property indexes: `CreatePropertyIndex(label, propertyKey)`, `DropPropertyIndex(label, propertyKey)` — create/drop in-memory property indexes. `NodesByLabelAndProperty(label, key, value, opts)` — O(1) indexed lookup with cursor-based pagination. Indexes are automatically maintained across all node mutation paths and persist across BadgerStore restarts. In TieredStore, property indexes are restricted to reference entities (`ErrEventPropertyIndex` for event labels).

Validation limits: `Config.Validation` accepts a `ValidationLimits` struct with configurable maximums: `MaxLabelsPerNode` (default 50), `MaxPropertiesPerEntity` (default 1000), `MaxPropertyKeyLength` (default 256), `MaxPropertyValueSize` (default 65536), `MaxNameLength` (default 256). Enforced at all graph entry points. Zero values use defaults.

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
- **Cascade-delete on node removal**: `Graph.DeleteNode` removes all outgoing and incoming relationships before the node. Self-loops are handled by skipping `ErrRelNotFound` in the incoming pass.
- **SnowflakeID bridges**: `nodeID.SnowflakeID()`, `relID.SnowflakeID()`, `entityID.SnowflakeID()` — exported methods on unexported wrapper types allow cross-package persistence key extraction without leaking the `snowflake.ID` dependency into entity method signatures.
- **Shadow resolution nil-guards**: `ResolveNodeProperty` / `ResolveRelProperty` check `Temporal() != nil` and `Integrity() != nil` before accessing fields. New entities without metadata return `(nil, false)` instead of panicking.

### Shadow Properties (15)

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
