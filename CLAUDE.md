# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Session Protocol

- **Session start**: Read `tasks/lessons.md` and `tasks/todo.md` before doing any work
- **After corrections**: Update `tasks/lessons.md` with the pattern and a rule to prevent recurrence
- **Session end**: Update `tasks/lessons.md` with new lessons, clean up `tasks/todo.md`

## Project Overview

**Temporal Knowledge Graph v3** — an internal Go library providing the core graph engine for temporal knowledge graphs. This is the low-level storage and type layer. It is **not** the end-user-facing product.

**tkg-v3** is a pure library (no main binary, no HTTP server, no query language). It provides:
- Graph entity types (Node, Relationship) with token interning and snowflake IDs
- Pluggable persistence (MemoryStore, BadgerStore)
- Thread-safe registries, entity locks, async batch persistence

**tkgd-v3** (separate repository) is the full product built on top of tkg-v3, providing:
- Cypher query engine
- Vadalog reasoning engine
- HTTP/gRPC server
- REST API

Module: `gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3`
License: Apache-2.0 (open source)
Go: 1.26.0
Dependencies: `github.com/bds421/rho-snowflake-2026` (IDs), `github.com/vmihailenco/msgpack/v5` (serialization), `github.com/dgraph-io/badger/v4` (persistence)

Status: v3.0.15 — Update operations and version history added (Phase 1a+1b complete).

## Build & Test Commands

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector — always run for concurrent code
make test-integration  # integration tests (long-running)
make cover          # coverage report -> coverage.html
make check          # pre-commit: vet + build + test
make ci             # full pipeline: fmt-check + vet + build + test-race + security + vulncheck
make fmt            # format code
make security       # gosec static analysis
make vulncheck      # govulncheck for known CVEs
```

Single test: `go test -run TestFoo ./pkg/types/`
Coverage check: `go tool cover -func=coverage.out` (after `make cover`)

## Testing Rules (hard requirements)

These rules exist because every single one was violated at least once. Do not skip them.

1. **Every public method gets a direct test.** Indirect coverage via delegation does NOT count. Run `go tool cover -func=coverage.out` — any public method at 0% is a blocker.

2. **Node and Relationship must have test parity.** These types are structural mirrors. When a test exists for Node (e.g., `TestNodeVersion`), the equivalent MUST exist for Relationship (`TestRelVersion`).

3. **Every type-switch branch gets its own test.** If a branch shows 0% in `cover`, add a test. No exceptions.

4. **Sentinel errors are tested with `errors.Is`, not just `err != nil`.** This applies at every call layer that propagates the error.

5. **Fallback/reflect paths must be tested or removed.** Any `default:` branch that uses reflection must have at least one test exercising it.

6. **Deep copy means deep copy.** Must truly clone all nested reference types. If implementation is shallow, document it as "shallow element copy."

7. **Run `make cover` before marking any step complete.** Any public method at 0% or new code path below 80% is a blocker.

8. **Validation must be recursive/adversarial.** Traverse containers (slices, maps, `any` interfaces) to check nested values. `[]any{&myStruct{}}` must be rejected. Write tests with nested prohibited values.

9. **Test nil values in reflect-based code.** `reflect.ValueOf(nil)` returns zero `reflect.Value`. `SetMapIndex` with zero Value **deletes the key** — silent data loss.

10. **One-time warnings must use `sync.Once`.** Never use `>=` for a warning that should be one-shot.

11. **No empty stubs when the spec defines the fields.** If the spec defines it, implement it.

12. **Public method return types must not leak dependencies.** Use unexported wrapper types (`type nodeID snowflake.ID`, NOT `type nodeID int64`). Never substitute `int64` for `snowflake.ID`.

13. **Config fields must be used or removed.** Never accept a config field that does nothing.

## Architecture

### `pkg/types`

| File | Purpose |
|---|---|
| `node.go` | Node (graph vertex, 80B) — `nodeID` (wraps `snowflake.ID`), primary + extra labels as `labelToken`, properties, `uint32` version, temporal, integrity |
| `relationship.go` | Relationship (directed edge, 72B) — `relID` (wraps `snowflake.ID`), `relTypeToken`, start/end as `nodeID`, properties, `uint32` version, temporal, integrity |
| `propertyslice.go` | Sorted key-value store with binary search; recursive validation rejects `tkg_` prefix keys and non-allowlisted types at any nesting depth; depth-limited to 32 levels (`ErrMaxDepthExceeded`); `ValidatePropertyValue` exported for pre-validation in graph-layer update paths |
| `shadow.go` | Constants for virtual read-only properties (`tkg_*`) managed by the graph layer |
| `temporal.go` | `Instant` type (Unix ms), `entityID` (opaque cross-entity ref wrapping `snowflake.ID`), `TemporalMetadata` struct (validity, transaction, audit, provenance, version chain via `baseEntityID entityID`) |
| `integrity.go` | `NodeIntegrity` / `RelIntegrity` structs (hash chain: `Hash`, `PrevHash`) |

### `pkg/graph`

| File | Purpose |
|---|---|
| `graph.go` | Graph struct with Config, Store, dual snowflake generators, registries, entity lock manager, `AddNode`/`AddRelationship` (with entity locks)/`DeleteNode` (with entity lock + cascade)/`DeleteRelationship`, `UpdateNode`/`UpdateRelationship` (saves pre-mutation state to version history before mutations), `GetNodeHistory`/`GetRelHistory` passthroughs, passthrough queries (including `OutgoingRelationships`/`IncomingRelationships` with string type name resolution), string resolution, `Close()` lifecycle (calls `store.Close()` universally, saves Badger registries via type assertion), BadgerDir whitespace validation in `New()` |
| `store.go` | `Store` interface (pure persistence contract with error-returning query methods, `DeleteNodeCascade`, `Close()` for resource cleanup, 8 version history methods) + sentinel errors (`ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrVersionNotFound`) |
| `memorystore.go` | `MemoryStore` — thread-safe in-memory `Store` with hash-set adjacency indexes for O(1) insert/delete, atomic `DeleteNodeCascade` under single write lock, version history maps (`nodeHistory`/`relHistory`) with deep-copy at boundary, no-op `Close()` |
| `badgerstore.go` | `BadgerStore` — persistent `Store` using Badger v4 with LRU entity caches (dirty tracking + tombstones), in-memory indexes as source of truth, async WriteBatch flush loop, background value log GC, atomic `int64` counters (never in transactions), `loadIndexes()` startup rebuild, version history via 0x07/0x08 keys (prefix scan + pending buffer merge, no in-memory index), three-phase cascade delete with history cleanup, `Close()` with `sync.Once` idempotence, registry persistence |
| `lru.go` | `entityLRU[V]` — generic LRU cache with dirty tracking, tombstone support, soft capacity (dirty entries never evicted) |
| `entity_locks.go` | `entityLockManager` — 256-shard mutex array for write-skew prevention. `LockTwo` acquires in ascending shard order (deadlock-free) |
| `keys.go` | Binary key encoding — single-byte prefix tags, big-endian IDs/tokens, fixed-width keys for entities, indexes, adjacency, history (0x07/0x08), metadata. `histNodeKey`/`histRelKey` for exact keys, `histNodePrefix`/`histRelPrefix` for prefix scans |
| `wire.go` | Msgpack wire format types (`nodeWire`/`relWire`/`propertyWire` with `Type byte` tag for Go type fidelity) and conversion functions for serialization boundary |
| `shadow.go` | `ResolveNodeProperty` / `ResolveRelProperty` — dispatches all 15 `tkg_*` shadow keys with nil-guards on `Temporal()`/`Integrity()`; `tkg_created_at` derives from snowflake ID via `Decompose()` when `CreatedAt` is zero/unset |
| `label_registry.go` | Thread-safe label string <-> uint16 token registry (RWMutex, double-check, `sync.Once` capacity warning, `ExportNames`/`ImportNames` for persistence) |
| `reltype_registry.go` | Thread-safe relationship type string <-> uint16 token registry (with `ExportNames`/`ImportNames`) |
| `doc.go` | Package documentation |

### Configuration

**`Graph.Config`** (in `graph.go`): `SnowflakeNodeID` (int64, 0-511), `Store` (Store interface), `BadgerDir` (string), `BadgerInMemory` (bool). If `Store` is nil and `BadgerDir` or `BadgerInMemory` is set, a `BadgerStore` is auto-created with default settings. Whitespace-only `BadgerDir` (e.g. `"   "`) is rejected — prevents silent fallback to MemoryStore.

**`BadgerStoreConfig`** (in `badgerstore.go`): `Dir`, `InMemory`, `Logger`, `CacheCapacity` (default 10K), `FlushInterval` (default 100ms), `GCInterval` (default 5min), `GCDiscardRatio` (default 0.5). To customize these, create a `BadgerStore` manually via `NewBadgerStore(cfg)` and pass it as `Config.Store`.

## Critical Design Invariants

**Pure-data structs (core architectural rule)**: Node and Relationship **never** hold references to the Graph, registries, or any resolver. They are self-contained data containers that hold tokens internally. String resolution is **always** the responsibility of the Graph layer, Cypher engine, or serialization layer — never on entities. No `SetGraph()`, no injected resolvers.

**snowflake.ID everywhere**: All entity IDs are backed by `snowflake.ID`. Internally, `Node.id` is `nodeID` (wraps `snowflake.ID`), `Relationship.id` is `relID` (wraps `snowflake.ID`), `startID`/`endID` are `nodeID`, and `TemporalMetadata.baseEntityID` is `entityID` (wraps `snowflake.ID`). These opaque wrappers prevent external packages from constructing or comparing IDs directly. Constructors accept `snowflake.ID`; the graph layer generates IDs via `NextNodeID()`/`NextRelID()`. Never use plain `int64` or `string` for entity IDs.

**Dual snowflake generators with even/odd node IDs**: Graph holds two separate generators — one for nodes (`SnowflakeNodeID*2`, even), one for relationships (`SnowflakeNodeID*2+1`, odd). This guarantees **value-level uniqueness** across entity types: no two snowflake IDs from different generators can ever collide, because the embedded node field always differs. Epoch: `2026-01-01`. 10-bit node field, 12-bit step (4096 IDs/ms). Valid `SnowflakeNodeID` range: 0-511 (512 concurrent graph instances). Generators are stateless — no counter persistence, no crash recovery.

**Strict encapsulation**: All struct fields are unexported. Access is through methods only. Constructors are `NewNode(id, primaryLabel, extraLabels)` and `NewRelationship(id, relType, startID, endID)`.

**Struct alignment packing**: Node (80B) and Relationship (72B) are packed by descending field alignment. When adding fields, maintain descending-alignment order and verify with `unsafe.Sizeof`.

**Defensive copying**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, `PropertiesMap()`, `ToMap()`, and `DeepCopy()` always return fully independent copies — never internal references. When adding a new accessor that returns reference types, always deep-copy and always add a mutation-independence test. **Store boundary isolation**: `PutNode`/`PutRelationship` deep-copy entities before caching; `GetNode`/`GetRelationship` and query methods deep-copy on return. Callers and the store never share pointers. Internal methods (`getNodeLocked`, `getRelLocked`) used under the write lock do not copy.

**Token interning**: Labels (`labelToken`) and relationship types (`relTypeToken`) are `uint16`. **Token 0 is reserved** as zero/invalid and must never be assigned — `HasLabelToken(0)` and `HasTypeToken(0)` always return false.

**Allowlist property validation**: `PropertySlice.Set()` recursively validates values at insertion time using an allowlist. Only primitives (`bool`, `int*`, `uint*`, `float*`, `string`), slices, and maps with safe element types are accepted. All other kinds are rejected at any nesting depth (`ErrUnsupportedValueType`). Recursion is depth-limited to `maxPropertyDepth` (32); deeper structures return `ErrMaxDepthExceeded`.

**Shadow property protection**: The `tkg_` prefix is reserved for graph-layer virtual properties. `PropertySlice.Set()` rejects any key starting with `tkg_`.

**PropertySlice sorted invariant**: Properties are maintained in sorted-by-key order. Always use `Set()` to add/update — never modify the slice directly.

**Shared-pointer accessors**: `Temporal()` and `Integrity()` return the internal pointer — no defensive copy. The graph layer needs mutation access; external callers should treat as read-only.

**Zero-allocation token checks**: `HasLabelTokenRaw(uint16)` on Node and `HasTypeTokenRaw(uint16)` on Relationship for hot-path graph traversal. Token 0 returns false.

**Bulk property construction**: `NewPropertySlice(map[string]any)` is O(N log N) — allocate once, validate, sort once. `SetProperties(ps)` on Node/Relationship assigns the pre-built slice directly. `AddNode`/`AddRelationship` use this path.

**Store is pure persistence**: The `Store` interface handles entity CRUD, index maintenance, atomic cascade operations, and resource cleanup via `Close()`. All Store implementations must satisfy `Close() error` (no-op for MemoryStore, stops goroutines + flushes + closes Badger for BadgerStore). `Graph.Close()` always calls `store.Close()` — no `closeFn` indirection. Shadow resolution and string resolution are Graph-layer responsibilities. `MemoryStore` uses nested hash-sets for O(1) adjacency insert/delete. All query methods return `error` and sort results by snowflake.ID for deterministic output. `BadgerStore` maintains atomic `int64` counters (persisted in the flush WriteBatch) for O(1) `NodeCount`/`RelationshipCount`. In-memory indexes (`nodeIDs`, `relIDs`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx`) are rebuilt from Badger on startup via `loadIndexes()`. `nodeIDs` and `relIDs` are O(1) existence maps used as bloom filters to short-circuit `GetNode`/`GetRelationship` for non-existent entities.

**LRU caches with version-aware dirty tracking**: `BadgerStore` maintains `entityLRU[*types.Node]` and `entityLRU[*types.Relationship]` with configurable capacity (default 10K per cache). Entries are marked dirty on write (monotonic `dirtyVer` counter) and tombstoned on delete. Dirty entries are never evicted (soft capacity). `CollectDirty()` is read-only — returns snapshots with version stamps. `MarkFlushed()` only clears entries matching the collected `dirtyVer`, preventing data loss when new writes land between collect and flush. `evictClean()` is O(N) single-pass backward scan.

**Async batch persistence**: Write operations update in-memory state immediately and queue `writeOp` structs into a `map[string]writeOp` buffer (last-write-wins deduplication). A background flush loop drains the buffer via Badger `WriteBatch` every `FlushInterval` (default 100ms). Counter keys (`meta/node_count`, `meta/rel_count`) are included in the same WriteBatch for atomic crash recovery. Failed ops are re-queued via `requeueOps()` (preserves newer writes). `Close()` stops goroutines, calls `flush()` unconditionally (handles InMemory mode where flushLoop was never spawned), then closes Badger. Idempotent via `sync.Once`.

**Entity locks for write-skew prevention**: `entityLockManager` (256 shards) serializes operations on overlapping entities. `shardIndex` extracts the low 8 bits of the snowflake timestamp field (`>> 22 & 0xFF`), cycling every 256ms — entities created at different times distribute across shards. `Graph.AddRelationship` acquires locks on both endpoints via `LockTwo(startID, endID)` before ID generation. `Graph.DeleteNode` acquires lock on the target via `LockEntity(id)` before cascade. `LockTwo` normalizes to ascending shard order (deadlock-free). Lock ordering: entity locks -> idxMu. Always.

**Atomic cascade-delete on node removal**: `Graph.DeleteNode` acquires the entity lock, then delegates to `Store.DeleteNodeCascade`, which atomically removes the node and all connected relationships. Self-loops are deduplicated via a map. No TOCTOU window. `BadgerStore.DeleteNodeCascade` uses a three-phase approach: (1) preflight reads all relationship metadata via `getRelLocked`, aborting with zero mutations on any read failure; (2) applies all deletions via `deleteRelByInfo` (mutation-only, cannot fail); (3) cleans up version history entries for all deleted relationships and the node. This prevents partial state corruption when a relationship has corrupt data on disk.

**Version history — pre-mutation snapshots**: `UpdateNode`/`UpdateRelationship` save the entity's current state to version history (via `PutNodeVersion`/`PutRelVersion`) before applying mutations. History is keyed by `(entityID, version)` — version comes from `entity.Version()` at the time of the snapshot. Initial creation (AddNode/AddRelationship) does NOT write history; the first update saves version 0. `GetNodeHistory`/`GetRelHistory` return all saved versions in ascending order. `TruncateNodeHistory`/`TruncateRelHistory` keep only the N most recent versions (`keepVersions <= 0` clears all). All delete paths (DeleteNode, DeleteNodeCascade, DeleteRelationship) clean up associated history entries. BadgerStore stores history directly in Badger via 0x07/0x08 prefix keys — no in-memory index (low frequency, bounded cardinality). `GetNodeVersion`/`GetRelVersion` check the pending buffer first for unflushed writes before falling through to Badger.

**Validate before generating IDs**: `AddNode`/`AddRelationship` run `NewPropertySlice(props)` and registry lookups before `NextNodeID()`/`NextRelID()`. Validation failures return early with no wasted snowflake IDs.

**Update operations — read-modify-write with entity lock**: `UpdateNode(id, updates)` and `UpdateRelationship(id, updates)` pre-validate all keys (reject `tkg_` prefix) and values (`ValidatePropertyValue`) before acquiring the entity lock. Under the lock: read current state from store, apply property updates (nil value = delete), bump version, set `UpdatedAt`, persist via `ReplaceNode`/`ReplaceRelationship`. Empty updates map is a no-op (no version bump, no lock). `UpdateRelationship` locks on the rel ID only — property changes don't affect adjacency, so endpoint locking is unnecessary.

**`ReplaceNode`/`ReplaceRelationship` are separate from Put**: Put rejects duplicates (`ErrNodeExists`/`ErrRelExists`). Replace requires existence (`ErrNodeNotFound`/`ErrRelNotFound`). Replace overwrites the entity data blob only — no index changes, because labels (Node) and type/endpoints (Relationship) are immutable after creation. Both deep-copy at the store boundary.

## Registries (pkg/graph)

Two independent registries with independent token namespaces. A label `"KNOWS"` and a relationship type `"KNOWS"` get independent tokens.

- **labelRegistry**: `map[string]labelToken` + `[]string` reverse lookup. Thread-safe (RWMutex, double-check on write miss).
- **relTypeRegistry**: Same structure with `relTypeToken`.
- Methods: `GetOrCreate(string)`, `Resolve(token)`, `ResolveAll([]token)`, `Lookup(string) (token, bool)`
- `GetOrCreate` rejects empty strings with `ErrEmptyName`.
- Growth warning logged at 60K tokens (92% of uint16). `GetOrCreate` returns error at 65535.
- Persisted to Badger as `meta/label_tokens` and `meta/reltype_tokens` (msgpack `[]string`).

## String Resolution Ownership

The Graph layer is the **sole owner** of string resolution:

| Consumer | Resolution methods |
|---|---|
| Graph layer | `NodeLabels(n)`, `NodePrimaryLabel(n)`, `RelationshipType(r)`, `ResolveNodeProperty(n, key)`, `ResolveRelProperty(r, key)`, `OutgoingRelationships(id, typeName)`, `IncomingRelationships(id, typeName)`, `NodesByLabel(label)`, `RelationshipsByType(typeName)` |
| Cypher engine | Resolves label/type tokens once per query via `Lookup()`, then matches with integer comparison |
| REST/gRPC API | Calls Graph resolution methods before JSON encoding |

All internal operations (index lookups, label matching, adjacency traversal) work with tokens directly.

## Shadow Properties (15)

All resolve to user-meaningful data via the Graph layer. No internal IDs exposed.

| Key | Type | Applies To | Category |
|---|---|---|---|
| `tkg_labels` | `[]string` | Node | Structural |
| `tkg_type` | `string` | Relationship | Structural |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both | Temporal |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both | Temporal |
| `tkg_created_at` | `Instant` | Both | Temporal (auto-derived) |
| `tkg_updated_at`, `tkg_deleted_at` | `Instant` | Both | Temporal |
| `tkg_created_by`, `tkg_updated_by` | `string` | Both | Provenance |
| `tkg_version` | `uint32` | Both | Provenance |
| `tkg_hash`, `tkg_prev_hash` | `string` | Both | Integrity |
| `tkg_base_entity` | `entityID` | Both | Version chain |

**`tkg_created_at` auto-derivation**: When `TemporalMetadata` is nil or `CreatedAt` is zero, the resolution falls back to extracting the creation timestamp from the entity's snowflake ID via `Decompose()`. This means every entity has an accurate creation timestamp without requiring `SetTemporal()`. Explicit non-zero `CreatedAt` takes priority (for historical data import).

## Badger Key Layout

All keys use fixed-width binary encoding with single-byte prefix tags. Snowflake IDs stored as big-endian uint64 (cast from int64) for correct sort order and temporal clustering.

| Key pattern | Purpose | Key size |
|---|---|---|
| `0x01/<8B nodeID>` | Node entity | 9B |
| `0x02/<8B relID>` | Relationship entity | 9B |
| `0x03/<2B labelToken>/<8B nodeID>` | Label index | 11B |
| `0x04/<2B relTypeToken>/<8B relID>` | Type index | 11B |
| `0x05/<8B startID>/<2B relType>/<8B endID>/<8B relID>` | Outgoing adjacency | 27B |
| `0x06/<8B endID>/<2B relType>/<8B startID>/<8B relID>` | Incoming adjacency | 27B |
| `0x0F/label_tokens`, `0x0F/reltype_tokens` | Registry persistence | varies |
| `0x0F/node_count`, `0x0F/rel_count` | Atomic entity counters (big-endian int64) | varies |

| `0x07/<8B nodeID>/<8B version>` | Node version history | 17B |
| `0x08/<8B relID>/<8B version>` | Relationship version history | 17B |

Temporal index keys (`0x09`/`0x0A`) exist as test-only stubs in `keys_helpers_test.go` — not yet implemented in any Store.

No `meta/next_node_id` or `meta/next_rel_id` — snowflake generation is stateless.

## Ecosystem

tkg-v3 is the internal graph engine. It lives in the `rho/` umbrella alongside:

| Module | Role |
|---|---|
| `rho/tkg-v3` | Internal library — graph types, persistence, registries (this repo) |
| `rho/tkgd-v3` | Full product — Cypher engine, Vadalog reasoning, HTTP/gRPC server (separate repo) |
| `rho/kit` | Service toolkit — app builder, logging, tracing, resilience, database |

tkg-v3 does **not** depend on kit. tkgd-v3 depends on both tkg-v3 and kit.

When tkgd-v3 needs kit conventions:
- **Error Handling**: `kit/apperror` — `NewNotFound`, `NewValidation`, `NewConflict`, `NewPermanent`.
- **Service Bootstrap**: `kit/app.Builder` fluent pattern.
- **Logging**: `kit/logging` (slog + JSON). Use `logging.FromContext(ctx)`.
- **Observability**: `kit/tracing` (OpenTelemetry), Prometheus via `kit/health`.
- **Private registry**: `go env -w GOPRIVATE="gitlab2024.bds421-cloud.com/*"`
