# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [3.0.22] - 2026-03-02

### Added (Phase 2c — Property Indexes)

- **`CreatePropertyIndex(label, propertyKey)`** — creates an in-memory index on a property for a given label. Scans existing nodes to populate the index. Returns `ErrIndexExists` if already defined.
- **`DropPropertyIndex(label, propertyKey)`** — removes a property index. Returns `ErrIndexNotFound` if missing.
- **`NodesByLabelAndProperty(label, key, value)`** — O(1) indexed lookup of nodes matching a label+property value. Falls back to scan if no index is defined.
- **`propertyValueKey(v any)`** — type-prefixed canonical string for safe cross-type value comparison (`"s:Alice"`, `"i:42"`, `"f64:3.14"`, `"b:true"`). Only primitives are indexed; complex types (maps, slices) return `""`.
- **Auto-update hooks** — property indexes are automatically maintained across all 7 node mutation paths: `PutNode`, `DeleteNode`, `ReplaceNode`, `ReplaceNodeWithHistory`, `PutNodesBatch`, `DeleteNodesBatch`, `DeleteNodeCascade`.
- **24 new tests** — 8 MemoryStore (create/duplicate/drop/not-found/hit/miss/no-index-fallback/auto-update), 8 BadgerStore (mirrored), 8 Graph-layer (end-to-end: create/drop/found/not-found/unregistered-label/multiple-values/update-reflected/delete-removes). Plus `TestPropertyValueKey_AllTypes` (table-driven, all 14 type branches + 3 non-indexed types).
- **`ErrIndexExists`** / **`ErrIndexNotFound`** sentinel errors in `store.go`.

### Changed

- **`Store` interface** — added 3 property index methods (`CreatePropertyIndex`, `DropPropertyIndex`, `NodesByLabelAndProperty`). Both MemoryStore and BadgerStore implement them.

## [3.0.21] - 2026-03-02

### Added (Phase 2a — Temporal Queries)

- **`GraphSnapshot`** struct — represents the complete graph state at a point in time (`Timestamp`, `Nodes`, `Relationships`, `NodeCount`, `RelCount`).
- **`GetNodesValidAt(t)`** — returns all nodes valid at instant `t`. Nodes without explicit temporal metadata derive valid-from from snowflake ID timestamp and are treated as open-ended.
- **`GetRelationshipsValidAt(t)`** — returns all relationships valid at instant `t`.
- **`GetNodesByLabelValidAt(label, t)`** — returns nodes with the given label that are valid at `t`.
- **`GetNodesValidDuring(start, end)`** — returns nodes whose validity overlaps `[start, end)`.
- **`GetRelationshipsValidDuring(start, end)`** — returns relationships whose validity overlaps `[start, end)`.
- **`GetNodeAt(id, t)`** — returns the version of a node that was valid at `t`. Builds the full version chain (history + current), computes validity periods from `UpdatedAt` timestamps, with explicit `ValidFrom`/`ValidTo` overrides. Returns `ErrNoVersionValidAt` if no version covers `t`.
- **`GetNeighborsValidAt(nodeID, t)`** — returns neighbor nodes reachable via relationships valid at `t`, where the neighbors themselves are also valid at `t`.
- **`Snapshot(t)`** — returns a `GraphSnapshot` at instant `t`. Relationships are only included if both endpoints are in the valid node set (no dangling rels).
- **31 new tests** — 12 point-in-time, 6 interval (including open-ended rels), 5 version-specific, 3 neighbor, 5 snapshot.
- **`ErrNoVersionValidAt`** sentinel error in `store.go`.

### Changed

- **No Store interface change** — all temporal queries are Graph-layer filters over existing `AllNodes`/`AllRelationships`/`GetNodeHistory` methods.

## [3.0.20] - 2026-03-02

### Added (Phase 2d — Per-Label / Per-Type Statistics)

- **`NodeCountByLabel(label)`** — returns the count of nodes with the given label. Returns 0 for unregistered labels.
- **`RelCountByType(typeName)`** — returns the count of relationships with the given type. Returns 0 for unregistered types.
- **`AllLabelCounts()`** — returns a map of label name to node count for all registered labels. Skips token 0 (reserved).
- **`AllRelTypeCounts()`** — returns a map of relationship type name to count for all registered types. Skips token 0 (reserved).
- **12 new tests** — empty/unregistered/single/multiple/after-delete for both labels and types, plus AllLabelCounts and AllRelTypeCounts with mixed counts.

### Changed

- **No Store interface change** — statistics are scan-based, delegating to existing `NodesByLabel`/`RelationshipsByType` methods.

## [3.0.19] - 2026-03-02

### Added (Phase 2b — Hash Chain Verification)

- **`VerifyNodeHashChain(id)`** — verifies the full hash chain for a node. Retrieves history + current version, validates genesis `PrevHash == ""`, verifies each version's `PrevHash` links to the previous version's `Hash`, and recomputes each hash via `ComputeNodeHash` to detect tampering. Returns `(true, nil)` if valid, `(false, nil)` on any mismatch, `(false, err)` on I/O failure.
- **`VerifyRelHashChain(id)`** — mirrors `VerifyNodeHashChain` for relationships using `ComputeRelHash`.
- **14 new tests** — 7 node (genesis-only, multiple updates, tampered hash, broken PrevHash, non-existent, nil integrity, property change) + 7 mirrored relationship tests.

### Changed

- **No Store or API changes** — verification methods are pure reads over existing `GetNodeHistory`/`GetRelHistory` + `ComputeNodeHash`/`ComputeRelHash`.

## [3.0.18] - 2026-03-01

### Fixed (Pre-Release Code Review)

- **BLOCKER: Update atomicity** — `UpdateNode`/`UpdateRelationship` now use `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` to atomically write version history and entity data in a single store call. Prevents orphaned history entries on crash between `PutNodeVersion` and `ReplaceNode`. New `Store` interface methods: `ReplaceNodeWithHistory(current, prevVersion, prevState)` and `ReplaceRelWithHistory(current, prevVersion, prevState)`.
- **BLOCKER: Hash serialization** — `writeProperties` in `integrity.go` replaced `fmt.Sprintf("%v")` with typed binary serialization using wire.go type tags. Maps now sort keys before hashing (deterministic). Type-distinct: `int(1)` vs `string("1")` produce different hashes. Breaking change for computed hashes (pre-release, no production data).
- **MAJOR: Cascade lock scope** — `DeleteNodeCascade` in BadgerStore releases `idxMu.Lock` before Phase 3 history cleanup. `deleteHistoryByPrefix` does Badger `db.View()` iterator scans — these no longer block concurrent reads/writes.
- **MINOR: Hash error handling** — Added `mustWrite`/`mustWriteString` helpers in `integrity.go`. All `_ = binary.Write()` and `_, _ = io.WriteString()` calls replaced with panicking wrappers. hash.Hash.Write never errors, but errors are no longer silently discarded.
- **MINOR: BatchBuilder docstring** — Changed "persisted atomically" to "executed sequentially; partial success is possible" to accurately describe behavior.
- **MINOR: MemoryStore RLock comment** — Added comment to `AllNodes`/`AllRelationships` documenting the RLock-for-iteration design choice.
- **MINOR: Tutorial 005 resource leak** — `bsQuery` is now explicitly closed if `graph.New` fails.

### Added (Phase 1g — Context-Aware Operations)

- **`AddNodeWithContext(ctx, labels, props)`** — creates a node with context support. Checks context at entry (pre-flight) and before the store write. Returns `context.Canceled` or `context.DeadlineExceeded` on cancellation.
- **`AddRelationshipWithContext(ctx, typeName, startNode, endNode, props)`** — creates a relationship with context support. Checks context at entry, before acquiring endpoint locks, and before the store write.
- **`UpdateNodeWithContext(ctx, id, updates)`** — updates a node with context support. 5 context checks: entry, before entity lock, before store read, before version history write, before final store write.
- **`UpdateRelationshipWithContext(ctx, id, updates)`** — mirrors `UpdateNodeWithContext` for relationships.
- **`DeleteNodeWithContext(ctx, id)`** — cascade-deletes a node with context support. Checks context at entry and under the entity lock before cascade.
- **`DeleteRelationshipWithContext(ctx, id)`** — deletes a relationship with context support. Checks context at entry before the store call.
- **`GetNodeWithContext(ctx, id)`** — retrieves a node with context support. Single pre-flight check.
- **`GetRelationshipWithContext(ctx, id)`** — retrieves a relationship with context support. Single pre-flight check.
- **`checkCtx(ctx)` helper** — non-blocking `select` with `default` branch. Zero overhead when context is not cancelled.
- **28 new tests** — 8 pre-flight cancellation (all methods return `context.Canceled`), 8 happy path (identical behavior to non-context methods), 4 deadline exceeded, 4 delegation regression (non-context methods still work), 4 edge cases (empty updates no-op, validation priority, checkCtx on Background). All pass with race detector.

### Changed

- **8 Graph methods refactored to thin wrappers** — `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `GetNode`, `GetRelationship` now delegate to their `WithContext` variants with `context.Background()`. Backward-compatible — existing callers require no changes.
- **No Store interface change** — Badger v4 does not support `context.Context` in its core API (`View`/`Update`/`Txn`). Context checks are best-effort at the Graph layer: pre-flight and between phases (before locks, before store calls). In-memory CPU-bound steps complete in microseconds and are not interrupted.

## [3.0.17] - 2026-03-01

### Added (Phase 1f — Batch Operations)

- **`PutNodesBatch(nodes []*types.Node)`** on Store interface — two-phase (validate then apply) atomic batch create. Phase 1 checks for duplicates vs existing store AND within the batch. Phase 2 deep-copies each, stores, and updates indexes. Any duplicate returns `ErrNodeExists` with zero mutations. Empty/nil input returns nil error. MemoryStore holds `mu.Lock()` for entire operation; BadgerStore holds `idxMu.Lock()` with pre-serialization outside the lock.
- **`PutRelationshipsBatch(rels []*types.Relationship)`** on Store interface — two-phase atomic batch create. Phase 1 verifies endpoints exist and checks for duplicate rel IDs. Phase 2 deep-copies, stores, updates type + adjacency indexes. MemoryStore and BadgerStore both use single lock for atomicity.
- **`DeleteNodesBatch(ids []snowflake.ID)`** on Store interface — two-phase atomic batch delete. Phase 1 verifies all IDs exist. Phase 2 removes entities, cleans label indexes, removes history. Missing ID returns `ErrNodeNotFound` with zero mutations.
- **`DeleteRelationshipsBatch(ids []snowflake.ID)`** on Store interface — two-phase atomic batch delete. Phase 1 verifies all IDs exist and pre-reads metadata. Phase 2 deletes via mutation-only helpers (type/adjacency/history cleanup). Missing ID returns `ErrRelNotFound` with zero mutations.
- **`BatchBuilder` fluent API** — `NewBatchBuilder(g)` creates a builder that queues operations with eager validation and deferred persistence. `AddNode(labels, props)` validates and creates fully-formed nodes (with hash + integrity) but doesn't persist. `AddRelationship(typeName, startNode, endNode, props)` validates type and properties. `UpdateNode(id, updates)` / `UpdateRelationship(id, updates)` pre-validate shadow key rejection and property types. `DeleteNode(id)` / `DeleteRelationship(id)` queue deletes.
- **`BatchResult`** — reports batch outcome with `Created`, `Updated`, `Deleted`, `Failed` counts, per-operation `Errors` slice, and `Duration`. Execute order: create nodes → create rels → update nodes → update rels → delete rels → delete nodes.
- **`BatchError`** — describes a single operation failure with `Op` (operation name), `ID` (entity ID), and `Err` (underlying error). Implements `error` interface.
- **41 new tests** — 12 MemoryStore batch tests (empty/happy/duplicate/internal-duplicate for Put, empty/happy/missing for Delete — node and rel parity), 12 BadgerStore batch tests (mirrored), 17 BatchBuilder tests (AddNode validation, AddRelationship validation, UpdateNode/UpdateRelationship validation, Execute empty/nodes/nodes+rels/updates/deletes/mixed/1000-nodes/partial-failure/update-rel, BatchError.Error). All pass with race detector.

### Changed

- **`Store` interface** — added 4 batch methods (`PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`). Both MemoryStore and BadgerStore implement them.

## [3.0.16] - 2026-03-01

### Fixed (Phase 1e — FlushInterval Policy + LRU evictClean Fix)

- **FlushInterval defaulting for InMemory mode** — `NewBadgerStore` now defaults `FlushInterval` to 100ms for both InMemory and OnDisk modes. Previously, InMemory mode had no periodic flushing by default.
- **LRU `evictClean()` O(N²) degradation** — added `cleanCount` field for O(1) early exit when all entries are dirty. Prevents repeated O(N) scans of a fully-dirty cache.

## [3.0.15] - 2026-03-01

### Added (Phase 1d — Bulk Query Methods)

- **`AllNodes()`** on Store interface — returns all stored nodes, sorted by snowflake.ID. MemoryStore iterates the node map under `RLock` with DeepCopy. BadgerStore snapshots `nodeIDs` under `idxMu.RLock`, then fetches each via `GetNode` (cache + existence check + Badger fallback + DeepCopy). Returns `nil, nil` for empty stores.
- **`AllRelationships()`** on Store interface — returns all stored relationships, sorted by snowflake.ID. Same patterns as `AllNodes`.
- **`GetNodesByIDs(ids []snowflake.ID)`** on Store interface — returns nodes matching the given IDs, sorted by snowflake.ID. Missing IDs are silently skipped (matches `NodesByLabel` orphan-skip pattern). Returns `nil, nil` for empty/nil input.
- **`GetRelationshipsByIDs(ids []snowflake.ID)`** on Store interface — returns relationships matching the given IDs, sorted by snowflake.ID. Missing IDs are silently skipped.
- **Graph-layer passthroughs** — `Graph.AllNodes()`, `Graph.AllRelationships()`, `Graph.GetNodesByIDs(ids)`, `Graph.GetRelationshipsByIDs(ids)`. Pure delegation to the store (no string resolution needed).
- **32 new tests** — 12 MemoryStore (empty/count/sorted for each method), 12 BadgerStore (mirrored), 8 graph-layer (empty + populated for each method, including skip-missing verification). All pass with race detector.

### Added (Phase 1c — Hash Chain Computation)

- **`ComputeNodeHash(n, labels)`** — computes a SHA-256 hash of a node's content (id, version, sorted labels, sorted properties). Returns a 64-character hex string.
- **`ComputeRelHash(r, typeName)`** — computes a SHA-256 hash of a relationship's content (id, version, type name, start/end IDs, sorted properties). Returns a 64-character hex string.
- **Integrity hooks in `AddNode`/`AddRelationship`** — newly created entities now have `Integrity()` populated with a computed `Hash` and empty `PrevHash` (genesis).
- **Integrity hooks in `UpdateNode`/`UpdateRelationship`** — updated entities get a new `Hash` computed on their final state, with `PrevHash` set to the previous version's hash. Forms a verifiable hash chain across the entity's version history.
- **22 new tests** — 10 unit tests for hash functions (determinism, property/version/label/type/endpoint sensitivity, label order independence), 12 graph-layer integration tests (integrity set on create, hash determinism, hash chain linking across updates, multiple-update chain verification, genesis zero PrevHash — all with node/relationship parity).

### Added

- **`UpdateNode(id, updates)`** — graph-layer read-modify-write under entity lock. Pre-validates all keys (`tkg_` prefix rejected) and values (`ValidatePropertyValue`) before acquiring the lock. Under the lock: reads current state, applies property updates (nil value = delete key), bumps version, sets `temporal.UpdatedAt`, persists via `ReplaceNode`. Empty updates map is a no-op (no lock, no version bump).
- **`UpdateRelationship(id, updates)`** — same pattern as `UpdateNode`. Entity lock on the relationship ID only — property changes don't affect adjacency, so endpoint locking is unnecessary.
- **`SetNodeProperty(id, key, value)`** / **`DeleteNodeProperty(id, key)`** — convenience wrappers around `UpdateNode`.
- **`SetRelationshipProperty(id, key, value)`** / **`DeleteRelationshipProperty(id, key)`** — convenience wrappers around `UpdateRelationship`.
- **`ReplaceNode(n)`** / **`ReplaceRelationship(r)`** on Store interface — overwrite existing entities. Returns `ErrNodeNotFound`/`ErrRelNotFound` if the entity does not exist. Deep-copies at the store boundary. No index changes (labels and type/endpoints are immutable after creation).
- **`ValidatePropertyValue` exported** — renamed from `validatePropertyValue` in `pkg/types/propertyslice.go` for use in graph-layer update pre-validation paths.
- **44 new tests** — 6 MemoryStore Replace, 6 BadgerStore Replace, 28 graph-layer (13 UpdateNode + 11 UpdateRelationship + 4 convenience), 4 Badger integration (including persistence round-trip). All pass with race detector.
- **Version history** — pre-mutation entity state saved automatically on every `UpdateNode`/`UpdateRelationship`. Queryable by version or as a full ascending history. Truncatable. Cleaned up on entity deletion (including cascade).
- **`PutNodeVersion(id, version, n)`** / **`PutRelVersion(id, version, r)`** on Store interface — persist a versioned snapshot. Deep-copies at the store boundary. Initial creation (AddNode/AddRelationship) does NOT write history; first update saves version 0.
- **`GetNodeVersion(id, version)`** / **`GetRelVersion(id, version)`** — retrieve a specific historical version. Returns `ErrVersionNotFound` when the version doesn't exist.
- **`GetNodeHistory(id)`** / **`GetRelHistory(id)`** — return all saved versions in ascending order. Empty slice for never-updated entities.
- **`TruncateNodeHistory(id, keepVersions)`** / **`TruncateRelHistory(id, keepVersions)`** — keep only the N most recent versions. `keepVersions <= 0` clears all history.
- **`Graph.GetNodeHistory(id)`** / **`Graph.GetRelHistory(id)`** — graph-layer passthrough to the Store.
- **`ErrVersionNotFound`** sentinel error — returned by `GetNodeVersion`/`GetRelVersion` for non-existent versions.
- **History key promotion** — `keyHistNode` (0x07) and `keyHistRel` (0x08) promoted from test-only stubs to production keys in `keys.go`. Added `histNodePrefix`/`histRelPrefix` for prefix scanning.
- **~50 new history tests** — 17 MemoryStore (8 node + 9 rel), 19 BadgerStore (mirrored + 2 restart persistence), 14 graph-layer (5 node + 5 rel + 4 Badger persistence). All pass with race detector.

### Changed

- **`Store` interface** — added `Close() error` (resource cleanup contract). All Store implementations must satisfy it. `MemoryStore.Close()` is a no-op (returns nil). `BadgerStore.Close()` was already implemented. `Graph.Close()` now calls `store.Close()` universally instead of the previous `closeFn` indirection — custom stores injected via `Config.Store` are now properly closed.
- **`Store` interface** — added `ReplaceNode(n *types.Node) error` and `ReplaceRelationship(r *types.Relationship) error`. Replace semantics are the opposite of Put: Put rejects duplicates (`ErrNodeExists`/`ErrRelExists`), Replace requires existence (`ErrNodeNotFound`/`ErrRelNotFound`).
- **`Store` interface** — added 8 version history methods (`PutNodeVersion`, `GetNodeVersion`, `GetNodeHistory`, `TruncateNodeHistory` + relationship mirrors). Both MemoryStore and BadgerStore implement them.
- **`UpdateNode`/`UpdateRelationship`** — now save pre-mutation state to version history via `PutNodeVersion`/`PutRelVersion` before applying mutations.
- **`DeleteNode`/`DeleteNodeCascade`/`DeleteRelationship`** — all delete paths now clean up associated version history entries. BadgerStore uses a three-phase cascade: (1) preflight, (2) rel mutations, (3) history cleanup.
- **`Graph.Close()` error handling** — replaced `&& closeErr == nil` guards with `errors.Join`, preserving all errors (registry save + store close) instead of dropping subsequent ones.
- **`Graph` struct** — removed `closeFn func() error` field. No longer needed since `Store.Close()` is called directly.

### Fixed

- **`BadgerDir` whitespace silent fallback** — `New(Config{BadgerDir: "   "})` previously fell through to MemoryStore silently (whitespace-only string is non-empty, passes `!= ""` check, but Badger would fail). Now rejects whitespace-only `BadgerDir` with an explicit error message. Empty string `""` still correctly defaults to MemoryStore.

## [3.0.14] - 2026-03-01

### Fixed

- **`flushLoop` silently discards errors** — `_ = bs.flush()` in the background flush loop now logs failures via `slog.Error` instead of silently discarding them. Removed `#nosec G104` annotations. Persistent Badger failures (disk full, corruption) are now observable.
- **Shared entity pointers between cache and caller** — `PutNode`/`PutRelationship` and `GetNode`/`GetRelationship` in both `BadgerStore` and `MemoryStore` now deep-copy entities at the store boundary. Previously, caller and cache shared the same pointer; mutations via `SetProperty` on the returned entity would silently corrupt cached state.
- **`DeleteNodeCascade` partial mutation on mid-loop error** — refactored to a two-phase approach: (1) preflight reads all relationship metadata, aborting with zero state changes on any read failure; (2) applies all deletions atomically via the new `deleteRelByInfo` helper. Previously, a corrupted relationship mid-cascade left indexes in a permanently split state.
- **`Close()` masks `db.Close()` error** — replaced `if e != nil && err == nil { err = e }` with `errors.Join(err, e)` to preserve both flush and database close errors.
- **`DeleteNodeCascade` returns nil on node data corruption** — now returns `fmt.Errorf("graph: cascade completed with corrupt node data: %w", err)` while still completing index cleanup. Callers can detect and log corruption.
- **Entity lock shard index uses step bits** — `shardIndex()` was masking bits 0-7 (step counter), which resets to 0 every millisecond. All entities created in separate milliseconds mapped to shard 0, reducing 256 shards to a single global mutex. Now shifts right by 22 to extract the low 8 bits of the timestamp field. Entities created >256ms apart land in different shards.
- **Node and rel generators share snowflake node ID** — both `nodeIDGen` and `relIDGen` used the same `SnowflakeNodeID`, allowing value-level ID collisions within the same millisecond. Now mapped to an even/odd pair (`ID*2` for nodes, `ID*2+1` for rels). Valid `SnowflakeNodeID` range reduced from 0-1023 to 0-511 (512 concurrent instances). **Breaking**: existing databases with IDs generated under the old scheme remain readable (key prefixes distinguish entity types), but new IDs will have different node fields.
- **`ImportNames` integer overflow on corrupted data** — both `labelRegistry.ImportNames` and `relTypeRegistry.ImportNames` cast slice indices to `uint16` without bounds checking. If persisted data exceeded 65,535 entries, the cast silently truncated, causing token collisions. Now returns an error if `len(names)-1 > tokenCapacityMax`. Removed `#nosec G115` annotations.

### Added

- **`Node.DeepCopy()`** — returns a fully independent clone (extraLabels, properties, temporal, integrity all deep-copied).
- **`Relationship.DeepCopy()`** — returns a fully independent clone (properties, temporal, integrity all deep-copied).
- **`tkg_created_at` derived from snowflake ID** — when `TemporalMetadata` is nil or `CreatedAt` is zero, `ResolveNodeProperty`/`ResolveRelProperty` derive the creation timestamp from the entity's snowflake ID via `Decompose()`. Explicit non-zero `CreatedAt` takes priority (historical import). Every entity now has an automatic, accurate creation timestamp without requiring `SetTemporal()`.

### Changed

- **`Config.SnowflakeNodeID` range** — valid range is now 0-511 (was 0-1023). Mapped to even/odd generator pair for value-level ID uniqueness.
- **Sort comments corrected** — `sortNodesByID`/`sortRelsByID` and query method doc comments no longer claim "chronological" order. Sort is time-dominant (ms timestamp in high bits) with node field and step as tiebreakers.

## [3.0.13] - 2026-02-28

### Fixed

- **Graph.Close() data race** — `closeFn` was read/written without synchronization; two concurrent `Close()` calls would race on the nil check. Replaced with `sync.Once` (`closeOnce`) for race-free idempotency.
- **Query methods swallow corruption errors** — `NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, `IncomingRelationships` used bare `continue` on all errors, silently eating I/O and corruption errors. Now only `ErrNodeNotFound` / `ErrRelNotFound` (index orphans) are skipped; all other errors propagate to the caller.
- **Dead variable `relDeleteCount`** — written but never read in `DeleteNodeCascade`. Removed.

### Changed

- **Test-only code relocated** — 12 functions and 6 constants (`histNodeKey`, `histRelKey`, `tempNodeKey`, `tempRelKey`, `labelIndexPrefix`, `relTypeIndexPrefix`, `outPrefix`, `outTypedPrefix`, `inPrefix`, `inTypedPrefix`, `parseNodeIDFromLabelIdx`, `parseRelIDFromTypeIdx`) moved from `keys.go` to `keys_helpers_test.go`. Reduces production binary size.
- **`toIntSlice` test coverage** — added `TestWireRoundTripIntSlice` exercising `[]int` property wire round-trip. No longer at 0% coverage.

## [3.0.12] - 2026-02-28

### Fixed

- **Atomic counter persistence** — counter keys (`meta/node_count`, `meta/rel_count`) are now written in the same `WriteBatch` as entity data. Previously, `persistCounters()` was a separate transaction, creating a TOCTOU window where counters could drift from actual entity count on crash recovery.
- **O(N) → O(1) evictClean** — LRU `evictClean()` was O(N²) due to restarting the inner scan from `Back()` after each eviction. Now a single-pass backward scan with `prev` pointer — O(N) worst case.
- **Cascade delete error propagation** — `DeleteNodeCascade` now checks `errors.Is(err, ErrRelNotFound)` and propagates non-sentinel errors (data corruption). Previously all `deleteRelLocked` errors were silently swallowed.
- **Close() InMemory flush** — `Close()` now calls `bs.flush()` unconditionally, ensuring pending ops are persisted even when `flushLoop` was never spawned (InMemory mode or zero FlushInterval).

### Changed

- **O(1) existence checks** — added `relIDs` map (mirrors `nodeIDs`) for O(1) relationship existence lookups. `GetRelationship()` and `PutRelationship()` now short-circuit via `relIDs` instead of scanning `typeIdx`. Removed dead `relExistsInIndex()` O(N) scan.
- **`GetNode()` / `GetRelationship()` bloom filter** — on cache miss, both methods check `nodeIDs`/`relIDs` (O(1)) before opening a Badger `db.View()` transaction, avoiding disk I/O for non-existent entities.
- **`persistCounters()` removed** — counter persistence is now inlined into `flush()` as part of the atomic WriteBatch.

## [3.0.11] - 2026-02-28

### Fixed

- **Version-aware dirty tracking** — LRU `CollectDirty()` is now read-only; `MarkFlushed()` only clears entries matching the collected `dirtyVer`. Prevents data loss when new writes land between `CollectDirty()` and `MarkFlushed()`.
- **Map-based pending write buffer** — replaced `[]writeOp` with `map[string]writeOp` for last-write-wins deduplication. `requeueOps()` preserves newer writes over failed ops. Prevents chronological write inversion on flush retry.
- **Cascade index scrub** — `DeleteNodeCascade` now scrubs `labelIdx` by scanning all label sets when entity data is unreadable, preventing ghost index entries.

## [3.0.10] - 2026-02-28

### Added

- **LRU entity caches** (`pkg/graph/lru.go`) — generic `entityLRU[V]` with dirty tracking, tombstone support, and soft capacity (dirty entries never evicted until flushed). BadgerStore maintains separate caches for nodes and relationships. Configurable via `BadgerStoreConfig.CacheCapacity` (default 10,000 per cache).
- **Entity lock manager** (`pkg/graph/entity_locks.go`) — 256-shard `sync.Mutex` array for write-skew prevention. `LockTwo(a, b)` acquires shards in ascending order (deadlock-free). Self-loops and same-shard IDs handled correctly with single lock acquisition.
- **Async batch persistence** — write operations update in-memory state immediately and queue `writeOp` structs. Background flush loop drains the buffer via Badger `WriteBatch` (blind writes = zero OCC conflicts) every `FlushInterval` (default 100ms). Failed ops are re-queued for retry.
- **Background value log GC** — periodic `RunValueLogGC()` loop (default 5min interval, configurable via `GCInterval`). Skipped entirely in in-memory mode.
- **In-memory indexes** — `nodeIDs`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx` maps rebuilt from Badger on startup via `loadIndexes()`. In-memory state is the source of truth while running. Badger is the durable backing store.
- **`BadgerStoreConfig`** new fields: `CacheCapacity`, `FlushInterval`, `GCInterval`, `GCDiscardRatio`.
- **`BadgerStore.Flush()`** — explicit flush for tests that need to verify durable persistence.
- **Write-skew regression test** (`TestGraphAddRelDeleteNodeConcurrency`) — 100 iterations of concurrent `AddRelationship` + `DeleteNode` on overlapping entities, verifying no dangling edges.

### Changed

- **BadgerStore architecture** — replaced synchronous per-operation Badger transactions with cache-first reads + async batch writes. Read path: LRU cache hit → return; cache miss → `db.View()` → populate cache; tombstone → `ErrNotFound`. Write path: update cache (dirty) + update indexes + queue writeOps.
- **Counter implementation** — replaced `incrCounter()` inside Badger transactions with `atomic.Int64` fields on the BadgerStore struct. Counters are persisted by the flush loop piggyback. Eliminates all OCC contention on concurrent writes.
- **`Graph.AddRelationship`** — now acquires entity locks on both endpoints via `LockTwo(startID, endID)` before ID generation. Prevents write-skew where concurrent `AddRelationship(→X)` + `DeleteNodeCascade(X)` both commit, producing a dangling edge.
- **`Graph.DeleteNode`** — now acquires entity lock on the target via `LockEntity(id)` before cascade.
- **`DeleteNodeCascade` (BadgerStore)** — atomic in-memory under `idxMu` write lock with async Badger writes, replacing single `db.Update()` transaction.
- **`Close()`** — idempotent via `sync.Once`. Stops background goroutines, performs final flush, persists counters, closes Badger.

### Removed

- `incrCounter()` — replaced by atomic counters.
- `initCounters()` — replaced by `loadIndexes()` + counter loading from meta keys.

## [3.0.9] - 2026-02-27

### Fixed

- **Close() file handle leak** — `Graph.Close()` now always calls `closeFn()` even if registry saves fail. Previously, a failed `SaveLabelRegistry` or `SaveRelTypeRegistry` would exit early, leaving the Badger file handle open. `closeFn()` now runs unconditionally; the first error is collected and returned.
- **Type erasure in wire format** — msgpack serialization destroyed Go type fidelity (`[]string` → `[]any`, `int64` → `int8`). Added a `Type byte` tag to `propertyWire` that records the concrete Go type during serialization and reconstructs it during deserialization. 24 type tags cover all allowlisted property types. Backward compatible: old data without the tag (decoded as `Type: 0`) falls through to integer normalization.
- **Silent data loss on query errors** — Store query methods (`NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, `IncomingRelationships`, `NodeCount`, `RelationshipCount`) now return `error`. `BadgerStore` propagates I/O and corruption errors instead of swallowing them. `MemoryStore` always returns `nil`.
- **TOCTOU cascade-delete** — `Graph.DeleteNode` was executing N+2 separate lock acquisitions/transactions, creating a window where concurrent `AddRelationship` could produce dangling edges. Both `MemoryStore.DeleteNodeCascade` and `BadgerStore.DeleteNodeCascade` now execute the entire cascade atomically — single write lock / single `db.Update()` transaction.
- **O(N) counts** — `BadgerStore.NodeCount()` and `RelationshipCount()` previously did full prefix scans. Now maintained as atomic metadata counters (`meta/node_count`, `meta/rel_count`) incremented/decremented within each mutating transaction — O(1) reads. Counter initialization scans on first open for backward compatibility with existing databases.

### Changed

- **`Store` interface** — all 6 query methods now return `error`. Added `DeleteNodeCascade(id snowflake.ID) error`. `Graph.DeleteNode` delegates to `DeleteNodeCascade`.
- **`propertyWire`** struct gained `Type byte` field (msgpack tag `"t"`).

## [3.0.8] - 2026-02-27

### Added

- **Badger persistence** (`pkg/graph/badgerstore.go`) — `BadgerStore` implementing the `Store` interface using [Badger v4](https://github.com/dgraph-io/badger) as the storage backend. Supports in-memory mode for testing and on-disk persistence. Includes type index, label index, and adjacency index maintenance.
- **Msgpack wire formats** (`pkg/graph/wire.go`) — `nodeWire`, `relWire`, `propertyWire` structs with conversion functions for serialization boundary. Handles temporal metadata, integrity hashes, and type normalization (msgpack compact integer encoding).
- **Binary key encoding** (`pkg/graph/keys.go`) — fixed-width binary keys with single-byte prefix tags for correct sort order. All snowflake IDs stored as big-endian uint64; tokens as big-endian uint16. 10 key types covering entities, indexes, adjacency, history, temporal, and metadata.
- **Registry persistence** — `ExportNames()` / `ImportNames(names)` on both `labelRegistry` and `relTypeRegistry`. `BadgerStore` persists registries as msgpack `[]string` under `meta/label_tokens` and `meta/reltype_tokens`.
- **`Graph.Close()`** — saves registries to Badger (if applicable), then closes the database. Idempotent. No-op for `MemoryStore`.
- **`Config.BadgerDir`** / **`Config.BadgerInMemory`** — creates a `BadgerStore` automatically when `Config.Store` is nil. Loads persisted registries on startup; fail-fast on corrupt data.
- **`ErrRegistryNotEmpty`** sentinel error — returned when `ImportNames` is called on a non-empty registry.

### Dependencies

- Added `github.com/vmihailenco/msgpack/v5` v5.4.1
- Added `github.com/dgraph-io/badger/v4` v4.9.1

## [3.0.7] - 2026-02-27

### Changed

- **Snowflake dependency migrated** from `gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026 v0.1.3` to [`github.com/bds421/rho-snowflake-2026 v1.0.1`](https://github.com/bds421/rho-snowflake-2026). All import paths updated across 14 `.go` files, `go.mod`, `SPEC.md`, and documentation.

### Fixed

- **Deterministic query results** — `NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, and `IncomingRelationships` now sort results by snowflake.ID for deterministic output. Previously, Go map iteration randomized the output on every call.
- **Cascade-delete outgoing tolerance** — `Graph.DeleteNode` now skips `ErrRelNotFound` in both the outgoing and incoming loops. Previously, a concurrently-deleted outgoing relationship would abort the cascade, leaving a partially severed node.
- **TOCTOU documentation** — `Graph.DeleteNode` documents the per-call locking limitation: without a transactional store API, a concurrent `AddRelationship` can create a dangling edge during cascade. The Badger implementation must wrap the entire cascade in a single `Update()` transaction.

## [3.0.6] - 2026-02-27

### Added

- **`Store` interface** (`pkg/graph/store.go`) — pure persistence contract with `PutNode`/`GetNode`/`DeleteNode`, `PutRelationship`/`GetRelationship`/`DeleteRelationship`, index queries (`NodesByLabel`, `RelationshipsByType`), adjacency queries (`OutgoingRelationships`, `IncomingRelationships`), and counts. Keys are `snowflake.ID`.
- **`MemoryStore`** (`pkg/graph/memorystore.go`) — thread-safe in-memory `Store` implementation. Uses nested hash-set adjacency indexes (`map[snowflake.ID]map[snowflake.ID]struct{}`) for O(1) insert/delete. `PutRelationship` validates start/end nodes exist.
- **`Graph.AddNode(labels, props)`** — creates a node with auto-generated snowflake ID, resolves labels to tokens, and bulk-loads properties via `NewPropertySlice`. Validates input before generating IDs to prevent snowflake waste.
- **`Graph.AddRelationship(typeName, startNode, endNode, props)`** — creates a directed relationship with auto-generated snowflake ID, resolves type to token, validates endpoints are non-nil, and bulk-loads properties.
- **`Graph.DeleteNode(id)`** — cascade-deletes all outgoing and incoming relationships before removing the node. Handles self-loops correctly by skipping `ErrRelNotFound` on the incoming pass.
- **`Graph.DeleteRelationship(id)`** — passthrough to store.
- **Store passthrough queries on Graph**: `GetNode`, `GetRelationship`, `NodesByLabel` (string-based, resolves to token), `RelationshipsByType` (string-based), `NodeCount`, `RelationshipCount`.
- **Shadow property resolution** (`pkg/graph/shadow.go`): `ResolveNodeProperty(n, key)` and `ResolveRelProperty(r, key)` dispatch all 15 `tkg_*` shadow keys. Non-`tkg_` keys delegate to `GetProperty`. Nil-guards on `Temporal()` and `Integrity()` prevent nil-pointer panics on new entities.
- **`NewPropertySlice(map[string]any)`** — O(N log N) bulk loader. Allocates once, validates all values (reserved-prefix + recursive allowlist), sorts once. Replaces the O(N²) per-property `SetProperty` loop for bulk construction.
- **`Node.SetProperties(ps)`** / **`Relationship.SetProperties(ps)`** — assign a pre-built `PropertySlice` directly, bypassing per-property validation (already done by `NewPropertySlice`).
- **SnowflakeID bridge methods**: `nodeID.SnowflakeID()`, `relID.SnowflakeID()`, `entityID.SnowflakeID()` — exported methods on unexported wrapper types for cross-package persistence key extraction.
- **Sentinel errors**: `ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrNoLabels`, `ErrNilNode`.
- **`Config.Store`** field — pluggable persistence backend. Defaults to `NewMemoryStore()` when nil.

### Changed

- `Graph` struct now holds a `store Store` field alongside registries and generators.
- `New(Config)` initializes a `MemoryStore` when `Config.Store` is nil.
- Implementation phases updated: Phase 2A (Store, MemoryStore, entity management, shadow resolution) complete.

## [3.0.5] - 2026-02-27

### Added

- **`HasLabelTokenRaw(uint16)`** on Node — zero-allocation label check for the graph layer. Accepts raw `uint16` instead of opaque `labelToken`, avoiding the heap allocation from `AllLabelTokens()`.
- **`HasTypeTokenRaw(uint16)`** on Relationship — zero-allocation type check for the graph layer. Mirrors `Node.HasLabelTokenRaw`. Token 0 always returns false.
- **`ErrMaxDepthExceeded` sentinel error.** `PropertySlice.Set()` returns this when a value exceeds 32 levels of nesting, preventing stack overflow from self-referential or deeply nested structures.
- **`ErrEmptyName` sentinel error.** Both `labelRegistry.GetOrCreate` and `relTypeRegistry.GetOrCreate` reject empty strings, preventing ambiguous token resolution.

### Changed

- **Explicit snowflake configuration.** Both snowflake generators now pass `WithEpoch(2026-01-01)`, `WithNodeBits(10)`, `WithStepBits(12)` explicitly instead of relying on defaults.
- **Allowlist property validation.** `validateReflectValue` switched from denylist (reject Ptr/Struct) to allowlist (accept only primitives + safe containers). Arrays, channels, functions, and unsafe pointers are now rejected at any nesting depth.
- **Registry capacity: token 65535 is now assignable.** Fixed off-by-one in `GetOrCreate` that blocked the final token. Capacity check uses `len(toLabel/toName) > 65535` instead of `nextToken >= 65535`.
- **Recursion depth limit.** `validateReflectValue`, `deepCopyValue`, and `reflectCopyValue` now thread a `depth` counter and stop at `maxPropertyDepth` (32). Validation returns an error; copy functions fall back to shallow return.
- `NodeHasLabel` and `RelationshipHasType` use `HasLabelTokenRaw`/`HasTypeTokenRaw` for zero-allocation matching with token-0 defense-in-depth.
- **`deepCopyValue` nil short-circuit.** Nil values return immediately without entering the type switch or reflect path.
- **`Temporal()`/`Integrity()` doc comments** on both Node and Relationship now document the shared-pointer intent (no defensive copy).
- **`ErrUnsupportedValueType` message updated** from "pointer and struct values are not supported" to the generic "unsupported property value type" to reflect the allowlist model.
- `Set()` doc comment updated to describe the allowlist approach and depth limit.

### Fixed

- Empty-string labels and relationship types no longer silently assigned tokens.
- Registry capacity boundary corrected (65535 tokens, not 65534).
- `pkg/graph/doc.go` updated to reflect current state (snowflake generators exist, not "will hold").

## [3.0.3] - 2026-02-27

### Added

- **Snowflake ID generators.** `Graph` now holds two independent `snowflake.Node` generators. `NextNodeID()` and `NextRelID()` produce unique `snowflake.ID` values. `Config.SnowflakeNodeID` is validated; out-of-range values return an error from `New()`. *(Range changed from 0-1023 to 0-511 in v3.0.14.)*
- **Recursive property validation.** `PropertySlice.Set()` traverses slices, maps, and `any`/interface wrappers to reject pointers and structs at any nesting depth (`validatePropertyValue` + `validateReflectValue`).
- **`Instant` type** (`pkg/types`): semantic wrapper for Unix-millisecond timestamps used by all temporal fields.
- **`nodeID` / `relID` opaque types** (`pkg/types`): unexported wrappers around `snowflake.ID`. `InternalID()`, `StartNodeID()`, `EndNodeID()` return these instead of `snowflake.ID` directly.
- **`TemporalMetadata` fields**: `ValidFrom`, `ValidTo`, `TxFrom`, `TxTo`, `CreatedAt`, `UpdatedAt`, `DeletedAt` (all `Instant`), `CreatedBy`, `UpdatedBy` (`string`), `BaseEntityID` (`snowflake.ID`).
- **`NodeIntegrity` / `RelIntegrity` fields**: `Hash`, `PrevHash` (`string`).

### Changed

- `reflectCopyValue` nil-value handling: map keys with nil values are preserved using `reflect.Zero()` instead of silently deleted.
- Registry capacity warning fires exactly once via `sync.Once` (was per-token for 60000-65534).

## [3.0.2] - 2026-02-27

### Added

- **`pkg/graph` package**: Graph layer with label and relationship type registries (Phase 1).
  - `Graph` struct with `Config`, registry ownership, and string resolution methods.
  - `labelRegistry` / `relTypeRegistry`: thread-safe bidirectional string ↔ uint16 token mappings (RWMutex, double-check on write miss). Independent token namespaces.
  - Resolution: `NodeLabels`, `NodePrimaryLabel`, `NodeHasLabel`, `RelationshipType`, `RelationshipHasType`.
  - Registry passthrough: `GetOrCreateLabel`, `GetOrCreateRelType`, `LookupLabel`, `LookupRelType`.
  - Capacity: warning at 60K tokens, error at 65535.
- `labelToken.Value()` and `relTypeToken.Value()` bridge methods for cross-package token access.
- Pointer/struct rejection in `PropertySlice.Set()` with `ErrUnsupportedValueType` sentinel.
- `PropertySlice.Delete(key)` method with `tkg_` prefix guard.
- `Node.DeleteProperty(key)` and `Relationship.DeleteProperty(key)`.

### Changed

- `deepCopyValue` expanded to all common slice/map types plus reflect-based fallback for exotic types.
- `ToMap()` now deep-copies all values.
- Sentinel errors (`ErrReservedPrefix`) properly wrapped for `errors.Is` discrimination.

## [3.0.1] - 2026-02-27

### Added

- `PropertiesMap()` on Node and Relationship.
- `TemporalMetadata` stub struct with `Temporal()`/`SetTemporal()` accessors.
- `NodeIntegrity` and `RelIntegrity` stub structs with `Integrity()`/`SetIntegrity()` accessors.
- Comprehensive `PropertySlice` test suite.

### Changed

- Shadow properties aligned to spec (final 15 `tkg_*` keys).
- Opaque token types (`labelToken`/`relTypeToken`) replace raw `uint16` in public API.
- Token 0 validation: constructors panic, `Has*Token(0)` returns false.
- Extra label deduplication in `NewNode`.

## [3.0.0] - 2026-02-16

### Added

- Initial implementation of `pkg/types`: `Node`, `Relationship`, `PropertySlice`, shadow constants.
- Snowflake ID integration via `github.com/bds421/rho-snowflake-2026`.
- Token interning with `labelToken` and `relTypeToken` (uint16).
- Shadow property protection (`tkg_` prefix rejection).
- Defensive copying on all slice accessors.
