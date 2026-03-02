# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Session Protocol

- **Session start**: Read `tasks/lessons.md` and `tasks/todo.md` before doing any work
- **Before planning**: Read the full API of ALL files involved in the change — not snippets around suspected bug locations. Understanding the complete API prevents plans based on wrong assumptions.
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

Status: v3.0.29 — Phases 1a-1g, 2a-2i, 3a, 3b+3c, 3d, and 3e complete, Phase 2 review (6 issues) resolved, Phase 2h (5 architectural fixes) resolved, 3 concurrency/integrity bugs fixed (CreatePropertyIndex concurrent data loss, ComputeNodeHash label dedup, VerifyHashChain deleted entities). History preserved on delete (tombstones), temporal queries history-aware, property indexes persist across restarts, hash chain verification truncation-resilient + handles deleted entities, cursor-based pagination on 5 unbounded queries, combined label+property+temporal queries, entity locks on all delete paths, LockMany for multi-entity locking, AllNodeIDs/AllRelIDs ID-only queries, property index purge on corruption, graph-level RWMutex for Snapshot vs Batch isolation, temporal query push-down to Store layer, create-only GraphTx with rollback, atomic Graph.Reset. TieredStore shard rotation (hot→warm→cold), warm shard recovery on restart, depth-aware reads (ShardDepth), E→E cross-shard routing fix, BadgerStore ReadOnly mode. Cold shard lifecycle (lazy-open via `getStore`, idle-close goroutine, `ColdAfter`/`IdleTimeout` config). Parallel shard queries (WaitGroup on 10 merge methods). Reference archive (lazy-open `refArchive`, `ArchiveNode`/`RestoreNode`, `shardForNodeID` archive fallback). Repair + Tooling (cross-shard repair scan, per-shard verification caching, property index restriction for event labels, admin API: `ForceRotate`/`ListShards`/`RebuildCatalog`/`VerifyShard`/`RunRepair`, ID decomposition via `DecomposeID`, `MigrateFromBadger` migration tool).

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

14. **DO NOT use sub-millisecond or millisecond `ShardWindow` in tests.** Snowflake IDs encode creation time at millisecond resolution (timestamp bits `>> 22`). A `ShardWindow` of `time.Millisecond` causes the shard window to expire before or during the first write — `checkRotation()` auto-rotates, the node lands in a new shard, but its snowflake timestamp resolves to the old (now-expired) shard. This silently breaks all shard routing. Instead, use the standard 1-week window (`newTestTieredStore`) and test cold/warm behavior via manual rotation (`ts.mu.Lock(); ts.RotateHotShard(); ts.mu.Unlock()`) + the `demoteToCold` test helper.

## Architecture

### `pkg/types`

| File | Purpose |
|---|---|
| `node.go` | Node (graph vertex, 80B) — `nodeID` (wraps `snowflake.ID`), primary + extra labels as `labelToken`, properties, `uint32` version, temporal, integrity. `PropertyCount()` returns count without deep copy |
| `relationship.go` | Relationship (directed edge, 72B) — `relID` (wraps `snowflake.ID`), `relTypeToken`, start/end as `nodeID`, properties, `uint32` version, temporal, integrity. `PropertyCount()` returns count without deep copy |
| `propertyslice.go` | Sorted key-value store with binary search; recursive validation rejects `tkg_` prefix keys and non-allowlisted types at any nesting depth; depth-limited to 32 levels (`ErrMaxDepthExceeded`); `ValidatePropertyValue` exported for pre-validation in graph-layer update paths |
| `shadow.go` | Constants for virtual read-only properties (`tkg_*`) managed by the graph layer |
| `temporal.go` | `Instant` type (Unix ms), `entityID` (opaque cross-entity ref wrapping `snowflake.ID`), `TemporalMetadata` struct (validity, transaction, audit, provenance, version chain via `baseEntityID entityID`) |
| `integrity.go` | `NodeIntegrity` / `RelIntegrity` structs (hash chain: `Hash`, `PrevHash`) |

### `pkg/graph`

| File | Purpose |
|---|---|
| `graph.go` | Graph struct with Config, Store, dual snowflake generators, registries, entity lock manager, `mu sync.RWMutex` (batch vs snapshot isolation), `ValidationLimits` struct (5 configurable limits with defaults), sentinel errors (`ErrTooManyLabels`, `ErrTooManyProperties`, `ErrKeyTooLong`, `ErrValueTooLarge`, `ErrNameTooLong`), private validation helpers (`validateName`, `validatePropertyEntry`, `validateProperties`), `AddNode`/`AddRelationship` (with entity locks)/`DeleteNode` (with entity lock + cascade)/`DeleteRelationship`, `UpdateNode`/`UpdateRelationship` (saves pre-mutation state to version history before mutations), `GetNodeHistory`/`GetRelHistory` passthroughs, passthrough queries (including `OutgoingRelationships`/`IncomingRelationships` with string type name resolution), bulk query passthroughs (`AllNodes`, `AllRelationships`, `GetNodesByIDs`, `GetRelationshipsByIDs`), per-label/per-type O(1) statistics (`NodeCountByLabel`, `RelCountByType`, `AllLabelCounts`, `AllRelTypeCounts`) delegating to Store-level token-based counters, property index methods (`CreatePropertyIndex`, `DropPropertyIndex`, `NodesByLabelAndProperty`), `Reset()` (acquires write lock, calls `store.Clear()`, preserves registries), string resolution, `Close()` lifecycle (calls `store.Close()` universally, saves registries via type switch for `*BadgerStore` and `*TieredStore`), `New()` wires `TieredStore.SetLabelRegistry` and loads registries when Store is `*TieredStore`, BadgerDir whitespace validation. TieredStore admin passthroughs: `DecomposeID`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `RunRepair`, `VerifyShard` — all via type switch on `*TieredStore` (except `DecomposeID` which calls the package-level function) |
| `store.go` | `QueryOpts` struct (`Limit int`, `After snowflake.ID`, `ValidAt types.Instant`, `ValidStart/ValidEnd types.Instant` — zero=disabled, backward-compatible, `Depth ShardDepth` — shard tier filter, zero=all tiers), `ShardDepth` type (`DepthAll`=0, `DepthHot`=1, `DepthWarm`=2 — controls which shard tiers are included in TieredStore merge queries; ignored by MemoryStore/BadgerStore), `Store` interface (pure persistence contract with error-returning query methods, 5 paginated queries (`NodesByLabel`, `RelationshipsByType`, `AllNodes`, `AllRelationships`, `NodesByLabelAndProperty` — all accept `QueryOpts` with temporal push-down), ID-only queries (`AllNodeIDs`, `AllRelIDs` — paginated, avoid deep-copy), bulk queries, batch operations (`PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`), atomic replace+history (`ReplaceNodeWithHistory`, `ReplaceRelWithHistory`), `DeleteNodeCascade`, `Clear()` (removes all entities, indexes, history, counters), O(1) per-label/per-type counters (`NodeCountByLabel(token uint16)`, `RelCountByType(token uint16)`), `Close()` for resource cleanup, 8 version history methods, property index methods (`CreatePropertyIndex`, `DropPropertyIndex`), history ID enumeration (`AllNodeHistoryIDs`, `AllRelHistoryIDs`)) + sentinel errors (`ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrVersionNotFound`, `ErrNoVersionValidAt`, `ErrIndexExists`, `ErrIndexNotFound`, `ErrTxDone`) |
| `memorystore.go` | `MemoryStore` — thread-safe in-memory `Store` with hash-set adjacency indexes for O(1) insert/delete, O(1) per-label/per-type counts via `len(labelIdx[token])` / `len(typeIdx[token])`, atomic `DeleteNodeCascade` under single write lock, version history maps (`nodeHistory`/`relHistory`) with deep-copy at boundary, temporal push-down via `filterNodeIDsByTemporal`/`filterRelIDsByTemporal` (reads in-memory entity pointers without deep-copy), `Clear()` reinitializes all maps, no-op `Close()` |
| `badgerstore.go` | `BadgerStore` — persistent `Store` using Badger v4 with LRU entity caches (dirty tracking + tombstones), in-memory indexes as source of truth, async WriteBatch flush loop, background value log GC, atomic `int64` counters (never in transactions), per-label/per-type O(1) counters (`sync.Map` of `*atomic.Int64`, maintained in 9 mutation sites, rebuilt from index sizes in `loadIndexes()`), `loadIndexes()` startup rebuild (including property index definition restore from `0x0F/prop_indexes`), version history via 0x07/0x08 keys (prefix scan + pending buffer merge, no in-memory index), cascade delete (preflight + apply under idxMu, history preserved), atomic `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` (single `appendOps` call), 3-phase `CreatePropertyIndex` (RLock→unlocked I/O→Lock), `AllNodeHistoryIDs`/`AllRelHistoryIDs` (pending buffer + Badger scan), two-stage temporal push-down (`Peek` pre-filter for zero-allocation cache hits + post-filter for cache misses), `Clear()` resets all indexes/counters/caches + `db.DropAll()`, `Close()` with `sync.Once` idempotence, registry persistence. `ReadOnly` mode: `BadgerStoreConfig.ReadOnly` opens Badger with `WithReadOnly(true)`, skips flushLoop and gcLoop — used by TieredStore for warm/cold shards |
| `lru.go` | `entityLRU[V]` — generic LRU cache with dirty tracking, tombstone support, soft capacity (dirty entries never evicted), `Peek(key)` for zero-allocation lookup (no deep-copy, no MRU promotion — used by BadgerStore temporal pre-filter), `Cap()` returns capacity (used by `Clear()` to recreate caches) |
| `entity_locks.go` | `entityLockManager` — 256-shard mutex array for write-skew prevention. `LockTwo` acquires in ascending shard order (deadlock-free). `LockMany`/`UnlockMany` for N-entity locking (sorted, deduplicated shards) |
| `keys.go` | Binary key encoding — single-byte prefix tags, big-endian IDs/tokens, fixed-width keys for entities, indexes, adjacency, history (0x07/0x08), metadata (`propIndexDefsKey` for property index persistence). `histNodeKey`/`histRelKey` for exact keys, `histNodePrefix`/`histRelPrefix` for prefix scans |
| `integrity.go` | `ComputeNodeHash(n, labels)` / `ComputeRelHash(r, typeName)` — SHA-256 content hashing via typed binary serialization (`writePropertyValue` with type tags from wire.go, sorted map keys). `mustWrite`/`mustWriteString` helpers panic on error (hash.Hash never errors). Called by `AddNode`/`AddRelationship` (genesis, PrevHash="") and `UpdateNode`/`UpdateRelationship` (chain linking, PrevHash=previous hash). `VerifyNodeHashChain(id)` / `VerifyRelHashChain(id)` — full hash chain verification: retrieves history + current, validates genesis PrevHash="", verifies PrevHash chain links, recomputes each hash |
| `wire.go` | Msgpack wire format types (`nodeWire`/`relWire`/`propertyWire` with `Type byte` tag for Go type fidelity) and conversion functions for serialization boundary |
| `shadow.go` | `ResolveNodeProperty` / `ResolveRelProperty` — dispatches all 15 `tkg_*` shadow keys with nil-guards on `Temporal()`/`Integrity()`; `tkg_created_at` derives from snowflake ID via `Decompose()` when `CreatedAt` is zero/unset |
| `label_registry.go` | Thread-safe label string <-> uint16 token registry (RWMutex, double-check, `sync.Once` capacity warning, `ExportNames`/`ImportNames` for persistence) |
| `reltype_registry.go` | Thread-safe relationship type string <-> uint16 token registry (with `ExportNames`/`ImportNames`) |
| `batch.go` | `BatchBuilder` — fluent API for queuing graph operations with eager validation (including all `ValidationLimits` checks) and deferred persistence. `BatchResult` / `BatchError` types. Execute order: create nodes → create rels → update nodes → update rels → delete rels → delete nodes. Node creates use `store.PutNodesBatch` for efficiency; rel creates lock endpoints per-rel; updates and deletes use existing Graph methods |
| `context.go` | `checkCtx` helper + 8 `WithContext` methods (`AddNodeWithContext`, `AddRelationshipWithContext`, `UpdateNodeWithContext`, `UpdateRelationshipWithContext`, `DeleteNodeWithContext`, `DeleteRelationshipWithContext`, `GetNodeWithContext`, `GetRelationshipWithContext`). All mutation methods enforce `ValidationLimits` (label count, property count/size, key/value length, name length). Update methods use two-phase validation: pre-lock entry checks + post-mutation `MaxPropertiesPerEntity` under entity lock. `DeleteNodeWithContext` uses two-phase locking with TOCTOU retry: Phase A reads adjacency under node lock, Phase B locks node + all connected rels via `LockMany`, re-verifies adjacency, retries if changed. `DeleteRelationshipWithContext` acquires `LockEntity(id)`. Delete methods save tombstone versions (DeletedAt/ValidTo) before deletion to preserve temporal history. Helpers: `collectDeleteIDs`, `sameIDSet`, `deleteNodeLocked`. Non-context methods delegate to `WithContext` with `context.Background()`. No Store interface change — context checks are Graph-layer best-effort (pre-flight + before locks + before I/O) |
| `temporal.go` | `GraphSnapshot` struct + temporal query methods: `GetNodesValidAt`, `GetRelationshipsValidAt`, `GetNodesByLabelValidAt`, `GetNodesValidDuring`, `GetRelationshipsValidDuring`, `GetNodeAt`, `GetRelAt`, `GetNeighborsValidAt`, `Snapshot`. Combined queries: `NodesByLabelPropertyAndTime(label, key, value, t)`, `NodesByLabelPropertyDuring(label, key, value, start, end)` — push temporal filters into Store via `QueryOpts`. History-aware: merges current + historical entity IDs via `AllNodeHistoryIDs`/`AllRelHistoryIDs`, handles deleted entities (nil current). Internal helpers: `nodeValidFrom`/`relValidFrom`, `nodeVersionBounds`/`relVersionBounds`, `resolveNodeVersionAt`/`resolveRelVersionAt`, `allKnownNodeIDs`/`allKnownRelIDs`, `getNodeVersionDuring`/`getRelVersionDuring` |
| `temporal_filter.go` | Package-level temporal filter helpers (no `*Graph` dependency): `entityValidFrom(id, tm)` derives effective valid-from from explicit `ValidFrom` or snowflake ID bit extraction (`uint64(id) >> 22`), `matchesTemporalFilter(id, tm, opts)` evaluates point-in-time or interval overlap. Used by both MemoryStore and BadgerStore for Store-level temporal push-down |
| `tx.go` | `GraphTx` — create-only transaction holding graph write lock for duration. `BeginTx()` acquires `g.mu.Lock()`. `AddNode`/`AddRelationship` delegate to `Graph` methods and track IDs. `Commit()` releases lock. `Rollback()` deletes entities in reverse creation order via `store.Delete*` (no tombstones — rolled-back entities vanish completely). `CreatedNodeIDs()`/`CreatedRelIDs()` for inspection. All methods return `ErrTxDone` after Commit/Rollback |
| `property_index.go` | `propertyIndexKey` (label token + property key), `propertyIndex` (reverse mapping from canonical value keys to node ID sets), `propertyValueKey(v)` (type-prefixed canonical string for 14 primitive types), `addNodeToPropertyIndexes`/`removeNodeFromPropertyIndexes` shared helpers used by all mutation paths in both MemoryStore and BadgerStore |
| `pagination.go` | `paginateIDs(ids, after, limit)` — shared cursor-based pagination helper using binary search on sorted `snowflake.ID` slices. Used by both MemoryStore and BadgerStore for the 5 paginated query methods |
| `ontology.go` | `EntityClass` (ClassEvent/ClassReference), `OntologyMapping` — classifies labels as reference or event with lazy token cache via label registry. Used by TieredStore for shard routing |
| `shard_catalog.go` | `ShardCatalog`, `ShardEntry`, `ShardKind`, `ShardTier` — JSON-persisted catalog of all shards with atomic write. Tracks time windows, labels, rel types per shard. `UpdateShardTier(name, tier)` and `UpdateShardTimeEnd(name, timeEnd)` for rotation state changes |
| `registry_file.go` | Flat msgpack registry file save/load with atomic rename. Used by TieredStore for label/reltype registry persistence (separate from BadgerStore's in-DB persistence) |
| `badgerstore_partial.go` | Unexported helpers on `*BadgerStore` for TieredStore cross-shard relationship routing: `putRelEntityAndOut` (entity+typeIdx+outIdx), `putRelIncoming` (inIdx only), `deleteRelEntityAndOut`/`deleteRelIncoming` (split delete), `deleteIncomingByRelID` (repair: remove orphaned in/ entry by relID scan), `hasNodeID`/`hasRelID` (O(1) existence), `incomingRelIDs`/`outgoingRelIDs` (sorted ID snapshots) |
| `tieredstore.go` | `TieredStoreConfig`, `TieredStore` (Store impl), `eventShard` — routes entities across ref shard + time-windowed event shards by ontology classification. `mu sync.RWMutex` protects hotShard pointer and eventShards map during rotation. Snowflake ID timestamp extraction for O(1) shard resolution. `RotateHotShard()` demotes hot→warm (flush, mark read-only, create new hot shard with ms-aligned boundaries), warm→cold demotion when `ColdAfter > 0`. `checkRotation()` fast-path time check + slow-path Lock+double-check+rotate. `eventShardSnapshot(depth)` returns depth-filtered `[]*eventShard`. `eventShard.getStore(ts)` lazy-opens cold shards (per-shard `shardMu`, `atomic.Int64` `lastAccess`). `idleCloseLoop()` goroutine closes idle cold shards. Warm/cold shard recovery in constructor (warm: reopened as ReadOnly, cold: `store=nil` lazy-open). `refArchive` lazy-opened on first archive/restore or DepthAll with catalog entry. `shardForNodeID` falls back to refArchive. `shardForRelID` probes all event shards for cross-shard relationship entities. Directory layout: `data/meta/`, `data/reference/`, `data/events/<window>/`, `data/archive/`. Mid-window restart via catalog. `Close()` with `sync.Once`, signals `closeCh`, handles nil stores |
| `tieredstore_write.go` | TieredStore write operations — nodes single-shard by label, relationships cross-shard aware via shard-based routing (`shardForNodeID` not class-based). E→R: ref-first in/ per §12. R→E and E→E cross-shard: entity-first. `checkRotation()` on all new-entity write paths. `PutRelationship` split-write, `DeleteRelationship` split-delete, `DeleteNodeCascade` cross-shard, batch partitioning by `*BadgerStore` pointer. `ArchiveNode`/`RestoreNode` (ref node + rels, lazy-open archive, `ErrNotReferenceEntity` sentinel). `ErrEventPropertyIndex` — `CreatePropertyIndex` rejects event labels (only reference entities support property indexes). Error propagation from shard routing |
| `tieredstore_read.go` | TieredStore read operations — ref probe + archive fallback + timestamp fallback (O(1), no fan-out). Merge queries (AllNodes, AllRels, AllNodeIDs, AllRelIDs, counts, history IDs) use `eventShardSnapshot(opts.Depth)` under `mu.RLock`, parallel via `sync.WaitGroup` (ref shard sequential, event shards concurrent). `es.getStore(ts)` for lazy-open of cold shards. `stripDepth` helper. k-way merge of sorted slices. `IncomingRelationships` cross-shard: get relIDs from node's shard inIdx, fetch each entity via `shardForRelID`. Pagination on merged results |
| `tieredstore_admin.go` | Admin API: `ShardInfo`/`VerifyResult` types, `ForceRotate` (safe wrapper with internal locking), `ListShards` (catalog + live counts), `RebuildCatalog` (reconstruct from live state), `VerifyShard` (hash chain verification with immutable-shard caching), `resolveShardStore`, `allShardStoresWithLazyOpen`, `findRelInAnyShardStore` |
| `tieredstore_repair.go` | `RepairResult`, `RunRepair` — cross-shard split-write consistency repair: Phase 1 detects orphaned in/ entries (entity missing) and deletes them, Phase 2 detects missing in/ entries (entity exists) and re-creates them via `putRelIncoming` |
| `tieredstore_migrate.go` | `MigrateFromBadger(src, dst, labels)` — copies all nodes and relationships from a single BadgerStore to a TieredStore with automatic ontology-based routing |
| `id_decompose.go` | `IDComponents` struct, `DecomposeID(snowflake.ID)` — extracts creation time, node ID (0-1023), sequence (0-4095) from snowflake ID bits |
| `doc.go` | Package documentation |

### Configuration

**`Graph.Config`** (in `graph.go`): `SnowflakeNodeID` (int64, 0-511), `Store` (Store interface), `BadgerDir` (string), `BadgerInMemory` (bool), `Validation` (ValidationLimits). If `Store` is nil and `BadgerDir` or `BadgerInMemory` is set, a `BadgerStore` is auto-created with default settings. Whitespace-only `BadgerDir` (e.g. `"   "`) is rejected — prevents silent fallback to MemoryStore.

**`ValidationLimits`** (in `graph.go`): `MaxLabelsPerNode` (default 50), `MaxPropertiesPerEntity` (default 1000), `MaxPropertyKeyLength` (default 256), `MaxPropertyValueSize` (default 65536, string values only), `MaxNameLength` (default 256, label and reltype names). Zero values resolve to defaults in `New()`. Enforced at all graph entry points (context.go + batch.go). Sentinel errors: `ErrTooManyLabels`, `ErrTooManyProperties`, `ErrKeyTooLong`, `ErrValueTooLarge`, `ErrNameTooLong`.

**`BadgerStoreConfig`** (in `badgerstore.go`): `Dir`, `InMemory`, `Logger`, `CacheCapacity` (default 10K), `FlushInterval` (default 100ms for both InMemory and OnDisk), `GCInterval` (default 5min, disk-only), `GCDiscardRatio` (default 0.5), `ReadOnly` (opens Badger in read-only mode, skips flushLoop/gcLoop — used for warm/cold shards in TieredStore). To customize these, create a `BadgerStore` manually via `NewBadgerStore(cfg)` and pass it as `Config.Store`.

**`TieredStoreConfig`** (in `tieredstore.go`): `DataDir` (root data directory, required unless InMemory), `InMemory` (in-memory shards for testing), `RefLabels` (entity labels classified as reference — all others default to event), `ShardWindow` (event shard time window, default 1 week), `CacheCapacity` (per-shard LRU capacity, default 10K), `FlushInterval` (per-shard flush interval, default 100ms), `ColdAfter` (demote warm→cold after this duration, 0=never), `IdleTimeout` (close idle cold shards after this, default 5min when ColdAfter>0, 0=never close). Create via `NewTieredStore(cfg)` and pass as `Config.Store`.

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

**Store is pure persistence**: The `Store` interface handles entity CRUD, index maintenance, atomic cascade operations, per-label/per-type counting (`NodeCountByLabel(token)`, `RelCountByType(token)`), `Clear()` for atomic removal of all entities/indexes/history/counters, and resource cleanup via `Close()`. All Store implementations must satisfy `Close() error` (no-op for MemoryStore, stops goroutines + flushes + closes Badger for BadgerStore, closes all shards + saves catalog for TieredStore). `Graph.Close()` always calls `store.Close()` — no `closeFn` indirection. Shadow resolution and string resolution are Graph-layer responsibilities. `MemoryStore` uses nested hash-sets for O(1) adjacency insert/delete; per-label/per-type counts are O(1) via `len(labelIdx[token])`. All query methods return `error` and sort results by snowflake.ID for deterministic output. Five unbounded query methods (`NodesByLabel`, `RelationshipsByType`, `AllNodes`, `AllRelationships`, `NodesByLabelAndProperty`) accept `QueryOpts` for cursor-based pagination and temporal push-down; zero values mean "return all / no filter" (backward-compatible). `BadgerStore` maintains atomic `int64` counters (persisted in the flush WriteBatch) for O(1) `NodeCount`/`RelationshipCount`, plus `sync.Map` of `*atomic.Int64` for O(1) per-label/per-type counts (maintained in 9 mutation sites, rebuilt from index sizes in `loadIndexes()`). In-memory indexes (`nodeIDs`, `relIDs`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx`) are rebuilt from Badger on startup via `loadIndexes()`. `nodeIDs` and `relIDs` are O(1) existence maps used as bloom filters to short-circuit `GetNode`/`GetRelationship` for non-existent entities. `TieredStore` routes entities across multiple BadgerStore instances by ontology classification: reference entities (configured via `RefLabels`) go to a single reference shard, event entities go to time-windowed event shards. Shard resolution is O(1) via snowflake ID timestamp extraction (bits 22–62). Cross-shard relationships use split writes (entity+out/ in start shard, in/ in end shard) with shard-based routing (`shardForNodeID`, not class-based) to correctly handle E→E cross-shard after rotation. Merge queries (AllNodes, AllRels, counts, IDs) combine results from depth-filtered shards via k-way merge, parallelized with `sync.WaitGroup` (ref shard sequential, event shards concurrent). `ShardDepth` (DepthAll/DepthHot/DepthWarm) controls tier inclusion. `sync.RWMutex` protects hotShard pointer and eventShards map: reads take RLock to snapshot pointers then release, rotation takes Lock. `checkRotation()` on write paths: fast-path time check (~1ns), slow-path Lock+double-check+rotate. Warm shards recovered from catalog on restart (reopened as ReadOnly BadgerStore). Cold shards recovered with `store=nil` (lazy-opened on first access via `getStore`). `idleCloseLoop()` goroutine closes cold shards idle longer than `IdleTimeout`. `RotateHotShard()` demotes warm→cold when `ColdAfter > 0`. Reference archive (`refArchive`): lazy-opened BadgerStore for archiving/restoring closed cases. `ArchiveNode`/`RestoreNode` move ref nodes + rels between refShard and refArchive. `shardForNodeID` falls back to refArchive when refShard misses.

**LRU caches with version-aware dirty tracking**: `BadgerStore` maintains `entityLRU[*types.Node]` and `entityLRU[*types.Relationship]` with configurable capacity (default 10K per cache). Entries are marked dirty on write (monotonic `dirtyVer` counter) and tombstoned on delete. Dirty entries are never evicted (soft capacity). `CollectDirty()` is read-only — returns snapshots with version stamps. `MarkFlushed()` only clears entries matching the collected `dirtyVer`, preventing data loss when new writes land between collect and flush. `evictClean()` maintains `cleanCount` for O(1) early exit when no clean entries exist; otherwise O(N) single-pass backward scan. This prevents O(N²) degradation when the cache is full of dirty entries. `Peek(key)` returns cached value and status without deep-copy or MRU promotion — used by BadgerStore temporal pre-filter for zero-allocation cache-hit checks. `Cap()` returns capacity — used by `Clear()` to recreate caches with the same size.

**Async batch persistence**: Write operations update in-memory state immediately and queue `writeOp` structs into a `map[string]writeOp` buffer (last-write-wins deduplication). A background flush loop drains the buffer via Badger `WriteBatch` every `FlushInterval` (default 100ms). Counter keys (`meta/node_count`, `meta/rel_count`) are included in the same WriteBatch for atomic crash recovery. Failed ops are re-queued via `requeueOps()` (preserves newer writes). `Close()` stops goroutines, calls `flush()` unconditionally (handles InMemory mode where flushLoop was never spawned), then closes Badger. Idempotent via `sync.Once`.

**Entity locks for write-skew prevention**: `entityLockManager` (256 shards) serializes operations on overlapping entities. `shardIndex` extracts the low 8 bits of the snowflake timestamp field (`>> 22 & 0xFF`), cycling every 256ms — entities created at different times distribute across shards. `Graph.AddRelationship` acquires locks on both endpoints via `LockTwo(startID, endID)` before ID generation. `Graph.DeleteNode` acquires locks on the node AND all connected relationships via two-phase `LockMany` (TOCTOU retry if adjacency changes between phases). `Graph.DeleteRelationship` acquires `LockEntity(id)` before deletion. `LockTwo` normalizes to ascending shard order (deadlock-free). `LockMany`/`UnlockMany` deduplicate shard indices, sort ascending, lock/unlock in order (deadlock-free for N entities). Lock ordering: entity locks -> idxMu. Always.

**Atomic cascade-delete on node removal**: `Graph.DeleteNodeWithContext` uses two-phase locking with TOCTOU retry: Phase A reads adjacency under node lock only, Phase B locks ALL entities (node + connected rels) via `LockMany`, re-verifies adjacency hasn't changed, then saves tombstone versions (with `DeletedAt`/`ValidTo`) for all connected relationships and the node itself before delegating to `Store.DeleteNodeCascade`. If adjacency changed between phases (concurrent `AddRelationship`), it retries from Phase A (max 10 retries). Tombstones preserve temporal history — past-time queries can reconstruct deleted entities. `BadgerStore.DeleteNodeCascade` uses a two-phase approach: (1) preflight reads all relationship metadata via `getRelLocked`, aborting with zero mutations on any read failure; (2) applies all deletions via `deleteRelByInfo` (mutation-only, cannot fail). Both phases run under `idxMu.Lock`. Version history (0x07/0x08 keys) is intentionally preserved — never deleted.

**Snapshot vs Batch isolation**: `Graph.mu` (`sync.RWMutex`) serializes batch writes against whole-graph temporal reads. `BatchBuilder.Execute` acquires `mu.Lock`; `Snapshot` acquires `mu.RLock`. Individual temporal methods (`GetNodesValidAt`, etc.) do NOT acquire `mu` to avoid reentrancy deadlock (Snapshot holds RLock → calls temporal methods). Guarantee: a Snapshot sees either the complete pre-batch or complete post-batch state, never partial.

**Create-only GraphTx**: `Graph.BeginTx()` acquires `mu.Lock()` (graph write lock), blocking concurrent Batch/Snapshot operations for the transaction duration. `AddNode`/`AddRelationship` delegate to Graph methods and track created IDs. `Commit()` releases the lock; `Rollback()` deletes entities in reverse creation order via `store.DeleteRelationship`/`store.DeleteNodeCascade` (bypasses `Graph.Delete*WithContext` to avoid tombstone versions — rolled-back entities vanish completely). Best-effort rollback: continues on error, returns first error. All methods return `ErrTxDone` after Commit/Rollback. Not suitable for long-running transactions (holds write lock), but acceptable for create-only transactions (fast). `Graph.Reset()` acquires write lock and calls `store.Clear()` — preserves registries (Graph-layer concern, not cleared by Store).

**Version history — pre-mutation snapshots**: `UpdateNode`/`UpdateRelationship` capture a deep copy of the entity's current state, apply mutations, then atomically write both the history snapshot and the updated entity via `ReplaceNodeWithHistory`/`ReplaceRelWithHistory`. History is keyed by `(entityID, version)` — version comes from `entity.Version()` at the time of the snapshot. Initial creation (AddNode/AddRelationship) does NOT write history; the first update saves version 0. `GetNodeHistory`/`GetRelHistory` return all saved versions in ascending order. `TruncateNodeHistory`/`TruncateRelHistory` keep only the N most recent versions (`keepVersions <= 0` clears all). **Delete paths preserve history** — `DeleteNodeWithContext`/`DeleteRelationshipWithContext` save tombstone versions (with `DeletedAt`/`ValidTo`) before deletion. History is never erased on delete (append-only temporal data). `AllNodeHistoryIDs`/`AllRelHistoryIDs` enumerate all entities with history entries (including deleted ones). BadgerStore stores history directly in Badger via 0x07/0x08 prefix keys — no in-memory index (low frequency, bounded cardinality). `GetNodeVersion`/`GetRelVersion` check the pending buffer first for unflushed writes before falling through to Badger.

**Temporal query semantics**: `nodeValidFrom`/`relValidFrom` derive effective valid-from from explicit `ValidFrom` (if set) or snowflake ID timestamp (always available). This means every entity is queryable temporally without requiring `SetTemporal()`. Point-in-time match: `effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t)`. Interval overlap: `effectiveValidFrom < end AND (ValidTo == 0 OR ValidTo > start)`. **Temporal push-down**: `QueryOpts.ValidAt` and `QueryOpts.ValidStart/ValidEnd` push temporal filtering into the Store layer. `entityValidFrom` (in `temporal_filter.go`) derives valid-from from explicit `ValidFrom` or snowflake ID bit extraction (`uint64(id) >> 22`). MemoryStore checks in-memory entity pointers without deep-copy; BadgerStore uses `Peek` for zero-allocation cache-hit checks then post-filters cache-miss candidates. `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring` push filters via QueryOpts. **History-aware**: `GetNodesValidAt`/`GetRelationshipsValidAt`/`GetNodesValidDuring`/`GetRelationshipsValidDuring` merge current entity IDs with `AllNodeHistoryIDs()`/`AllRelHistoryIDs()` to include deleted entities — these still use full version chain resolution, not push-down. `GetNodeAt`/`GetRelAt` build the full version chain (history + current, handling nil current for deleted entities), use `UpdatedAt` timestamps to compute version validity periods, with explicit `ValidFrom`/`ValidTo` overrides. Genesis detection uses `entry.Version() == 0` (not array position) for truncation resilience. `Snapshot` filters relationships to only include those with both endpoints present in the valid node set.

**Property indexes are in-memory with auto-maintenance and persistence**: `propertyIndex` stores a reverse mapping from `propertyValueKey(value)` → set of node IDs. Indexes are maintained automatically across all 7 node mutation paths in both MemoryStore and BadgerStore via shared helpers `addNodeToPropertyIndexes`/`removeNodeFromPropertyIndexes`. When no indexes are defined, the helpers are no-ops (zero overhead on existing code paths). `propertyValueKey` uses type-prefixed canonical strings (`"s:"`, `"i:"`, `"f64:"`, `"b:"` etc.) to prevent cross-type collisions. Only primitive types (string, int*, uint*, float*, bool) are indexed; complex types return `""` and are silently skipped. **BadgerStore persistence**: index definitions (label token + property key pairs) are serialized to `0x0F/prop_indexes` via msgpack on create/drop. `loadIndexes()` reads definitions back and rebuilds index data by scanning matching nodes via `loadNodeFromBadger()`. `CreatePropertyIndex` uses 3-phase locking: RLock to check existence + snapshot IDs, unlocked I/O via `GetNode`, Lock to install with double-check.

**Cross-shard split-write ordering (§12)**: When a relationship spans two shards (E→R or R→E), the 7-key write is split across shards: entity+typeIdx+outIdx in the start node's shard, inIdx in the end node's shard. Ordering rule: for E→R (event→reference), write the reference shard's inIdx first (critical path for the dominant `Case ← Signal` query pattern); for R→E (reference→event), write the entity shard first. On partial failure, the reference shard's inIdx may exist without a corresponding entity — repair scan in Phase 3e. Both endpoints are verified to exist before any writes begin.

**Validate before generating IDs**: `AddNode`/`AddRelationship` run `NewPropertySlice(props)` and registry lookups before `NextNodeID()`/`NextRelID()`. Validation failures return early with no wasted snowflake IDs.

**Update operations — read-modify-write with entity lock**: `UpdateNode(id, updates)` and `UpdateRelationship(id, updates)` pre-validate all keys (reject `tkg_` prefix) and values (`ValidatePropertyValue`) before acquiring the entity lock. Under the lock: read current state from store, deep-copy pre-mutation state, apply property updates (nil value = delete), bump version, set `UpdatedAt`, persist via `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` (atomic replace + history in a single store call). Empty updates map is a no-op (no version bump, no lock). `UpdateRelationship` locks on the rel ID only — property changes don't affect adjacency, so endpoint locking is unnecessary.

**`ReplaceNode`/`ReplaceRelationship` are separate from Put**: Put rejects duplicates (`ErrNodeExists`/`ErrRelExists`). Replace requires existence (`ErrNodeNotFound`/`ErrRelNotFound`). Replace overwrites the entity data blob only — no index changes, because labels (Node) and type/endpoints (Relationship) are immutable after creation. Both deep-copy at the store boundary. `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` atomically write both the new entity data and a version history entry in a single `appendOps` call (BadgerStore) or single lock acquisition (MemoryStore), preventing orphaned history entries on crash or error.

## Registries (pkg/graph)

Two independent registries with independent token namespaces. A label `"KNOWS"` and a relationship type `"KNOWS"` get independent tokens.

- **labelRegistry**: `map[string]labelToken` + `[]string` reverse lookup. Thread-safe (RWMutex, double-check on write miss).
- **relTypeRegistry**: Same structure with `relTypeToken`.
- Methods: `GetOrCreate(string)`, `Resolve(token)`, `ResolveAll([]token)`, `Lookup(string) (token, bool)`
- `GetOrCreate` rejects empty strings with `ErrEmptyName`.
- Growth warning logged at 60K tokens (92% of uint16). `GetOrCreate` returns error at 65535.
- **BadgerStore persistence**: Persisted inside Badger as `meta/label_tokens` and `meta/reltype_tokens` (msgpack `[]string`).
- **TieredStore persistence**: Persisted as a flat msgpack file at `data/meta/registry.msgpack` (via `registry_file.go`). Contains both label and reltype token arrays. Atomic write via write-tmp+rename. Loaded on `Graph.New()` via `TieredStore.LoadLabelRegistry`/`LoadRelTypeRegistry`; saved on `Graph.Close()` via `TieredStore.SaveLabelRegistry`/`SaveRelTypeRegistry`.

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
