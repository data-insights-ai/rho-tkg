# tkg-v3

**Temporal Knowledge Graph v3** — the internal Go library powering the core graph engine for temporal knowledge graphs.

tkg-v3 is a **pure library** (no main binary, no HTTP server, no query language). It provides the low-level graph types, persistence layer, and entity management that higher-level products build on.

For the full product with Cypher queries, Vadalog reasoning, and an HTTP/gRPC server, see **tkgd-v3**.

| Layer | Repository | What it provides |
|---|---|---|
| **tkg-v3** (this repo) | `rho/tkg-v3` | Graph types, registries, MemoryStore, BadgerStore, entity locks |
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
| `Store` | Persistence interface — CRUD, index/adjacency/bulk queries, batch operations (`PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`), counts, `Close()` |
| `BatchBuilder` | Fluent API for queuing graph operations with eager validation and deferred persistence — `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `Execute` |
| `MemoryStore` | Thread-safe in-memory `Store` with hash-set adjacency indexes for O(1) insert/delete, no-op `Close()` |
| `BadgerStore` | Persistent `Store` using Badger v4 with msgpack serialization, fixed-width binary keys, and label/type/adjacency indexes |
| `labelRegistry` | Thread-safe bidirectional label string ↔ uint16 token mapping (persisted to Badger on `Close()`) |
| `relTypeRegistry` | Thread-safe bidirectional relationship type string ↔ uint16 token mapping (persisted to Badger on `Close()`) |

Entity management: `AddNode(labels, props)`, `AddRelationship(typeName, start, end, props)`, `UpdateNode(id, updates)`, `UpdateRelationship(id, updates)`, `DeleteNode(id)` (cascade), `DeleteRelationship(id)`.

Convenience methods: `SetNodeProperty(id, key, value)`, `DeleteNodeProperty(id, key)`, `SetRelationshipProperty(id, key, value)`, `DeleteRelationshipProperty(id, key)`.

Resolution methods: `NodeLabels(n)`, `NodePrimaryLabel(n)`, `NodeHasLabel(n, label)`, `RelationshipType(r)`, `RelationshipHasType(r, typ)`.

Shadow resolution: `ResolveNodeProperty(n, key)`, `ResolveRelProperty(r, key)` — dispatches all 15 `tkg_*` keys with nil-guards.

Registry methods: `GetOrCreateLabel(name)`, `GetOrCreateRelType(name)`, `LookupLabel(name)`, `LookupRelType(name)`.

Store queries: `GetNode(id)`, `GetRelationship(id)`, `NodesByLabel(label)`, `RelationshipsByType(typeName)`, `OutgoingRelationships(nodeID, typeName)`, `IncomingRelationships(nodeID, typeName)`, `NodeCount()`, `RelationshipCount()`.

Bulk queries: `AllNodes()`, `AllRelationships()`, `GetNodesByIDs(ids)`, `GetRelationshipsByIDs(ids)` — all return results sorted by snowflake.ID; missing IDs are silently skipped.

Batch operations: `NewBatchBuilder(g)` creates a builder that queues operations with eager validation. Call `AddNode(labels, props)`, `AddRelationship(typeName, start, end, props)`, `UpdateNode(id, updates)`, `UpdateRelationship(id, updates)`, `DeleteNode(id)`, `DeleteRelationship(id)` to queue operations. Call `Execute()` to persist all operations in order (creates → updates → deletes). Returns a `BatchResult` with counts and per-operation errors. Store-level batch methods (`PutNodesBatch`, `DeleteNodesBatch`, etc.) use two-phase validate-then-apply for atomicity.

Lifecycle: `Close()` saves registries (Badger only), then calls `store.Close()` on every Store implementation. MemoryStore.Close() returns nil. Always call `Close()` when done — it is safe to call multiple times.

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
- **Store is pure persistence**: The `Store` interface handles entity storage, index maintenance, and resource cleanup (`Close()`). Shadow resolution, referential integrity (cascade-delete), and string resolution live on Graph.
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
