# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [3.0.33] - 2026-03-02

### Fixed (Pre-Release Code Review — 1 BLOCKER + 10 MAJORs + 16 MINORs)

**BLOCKER:**
- **checkoutStore TOCTOU race** — `activeReqs` increment moved inside `shardMu` for cold shards, eliminating race window between `getStore` return and ref-count increment that allowed `closeIdleShards` to close an in-use store.

**MAJORs:**
- **Atomic file persistence** — `atomicWriteFile` helper with `fsync` before rename in `shard_catalog.go` and `registry_file.go`. Prevents crash-induced corruption of catalog and registry files.
- **Registry save race** — new `SaveRegistries(labels, relTypes)` writes both registries atomically in a single file operation, eliminating read-modify-write race between `SaveLabelRegistry` and `SaveRelTypeRegistry`.
- **ShardCatalog thread safety** — added `sync.RWMutex` to `ShardCatalog`. All read methods take RLock, all write methods take Lock. `GetShard`/`HotEventShard` return copies instead of internal pointers.
- **append backing array corruption** — replaced `append(outRels, inRels...)` with explicit allocation in `context.go` and `tx.go` to prevent writing into spare capacity of the `outRels` slice.
- **ReplaceNode property index cleanup** — added `purgeNodeFromAllPropertyIndexes` fallback in `badgerstore.go` when `getNodeLocked` fails during Replace, preventing orphaned property index entries.
- **Float formatting precision** — replaced `fmt.Sprintf("%v")` with `strconv.FormatFloat` for float32/float64 in `property_index.go`, ensuring deterministic round-trip-safe index keys.
- **NodesByLabel event label fan-out** — `NodesByLabel`, `NodesByLabelAndProperty`, and `NodeCountByLabel` now fan out across all event shards (respecting `opts.Depth`), not just the hot shard.
- **Constructor warm shard leak** — added cleanup loop in `NewTieredStore` to close already-opened warm shard stores when a subsequent warm shard fails to open.
- **TieredStore.Close() error accumulation** — replaced `closeErr = fmt.Errorf(...)` with `errors.Join` so all close errors are reported, not just the last one.

**MINORs:**
- Removed unused `_ = attempt` variable in `context.go` retry loop
- `result.Failed += 1` → `result.Failed++` in `batch.go`
- Documented `Execute` return pattern and thread-safety in `batch.go`
- Defined `ErrNotTieredStore` sentinel, replaced 7 ad-hoc `fmt.Errorf` strings in `graph.go`
- Added `slog.Error` in `persistPropertyIndexDefs` for marshal failures
- Distinguished `ErrNodeNotFound` from real errors in property scan fallback
- Moved test-only `contains()` from `property_index.go` to `property_index_test.go`
- Removed redundant `archiveWritten`/`refWritten` guard variables in `tieredstore_write.go`
- Fixed `tkg_version` comment: `int` → `uint32` in `pkg/types/shadow.go`
- Documented `ValidStart+ValidEnd` both-required for interval filtering in `temporal_filter.go`
- Added `panic("unreachable")` in `writePropertyValue` default case in `integrity.go`
- Rewrote `MigrateFromBadger` to use `ForEachNodeID`/`ForEachRelID` pagination instead of materializing all entities

## [3.0.32] - 2026-03-02

### Added

- **`ImportNodeWithID(ctx, id, labels, props)`** — creates a node with a caller-specified snowflake ID. Returns `ErrNodeExists` if the ID is already in use, `ErrZeroID` if id == 0. Used for backup/restore where ID preservation is required.
- **`ImportRelationshipWithID(ctx, id, typeName, startNode, endNode, props)`** — creates a relationship with a caller-specified snowflake ID. Returns `ErrRelExists` on collision.
- **`GraphTx.ImportNodeWithID` / `GraphTx.ImportRelationshipWithID`** — transaction wrappers for both import methods, tracked for rollback.
- **`ErrZeroID`** — new sentinel error for zero ID validation in import methods.
- 8 new tests in `import_test.go`: basic, collision, zero ID, validation, rel import, tx commit, tx rollback.

## [3.0.31] - 2026-03-02

### Added (OOM Fix — Lazy ForEach Iterators for Temporal Pipeline)

- **`ForEachNodeID(fn)` / `ForEachRelID(fn)`** — lazy iterator over all current entity IDs. Callback returns `true` to continue, `false` to stop. Implemented on MemoryStore (map iteration under RLock), BadgerStore (index map iteration under `idxMu.RLock`), and TieredStore (sequential shard iteration with checkout/checkin — one shard open at a time).
- **`ForEachNodeHistoryID(fn)` / `ForEachRelHistoryID(fn)`** — lazy iterator over all entity IDs with version history entries. BadgerStore: pending buffer scan + Badger prefix scan with dedup. TieredStore: sequential shard iteration.
- **`forEachKnownNodeID` / `forEachKnownRelID`** — two-phase temporal helpers replacing `allKnownNodeIDs`/`allKnownRelIDs`. Phase 1: collect unique IDs via ForEach (callbacks only insert into `seen` map — no store method calls, safe with RWMutex). Phase 2: process IDs after store locks released.

### Changed

- **`GetNodesValidAt`**, **`GetRelationshipsValidAt`**, **`GetNodesValidDuring`**, **`GetRelationshipsValidDuring`** — rewritten to use `forEachKnownNodeID`/`forEachKnownRelID` instead of materializing all IDs into slices.
- **`Snapshot`** — benefits transitively (calls `GetNodesValidAt` + `GetRelationshipsValidAt`).

### Removed

- **`allKnownNodeIDs`** / **`allKnownRelIDs`** — replaced by `forEachKnownNodeID`/`forEachKnownRelID`.

### Memory Impact

- Eliminates per-shard `[]snowflake.ID` slices and `mergeIDSlices` allocations in the temporal query pipeline. For 10M nodes across 12 shards, reduces peak memory from ~928 MB to ~160 MB (~83% reduction for the ID collection phase).

### Tests

- 20 new tests: MemoryStore ForEach (node IDs, early stop, empty, rel IDs, node history, rel history), BadgerStore ForEach (node IDs, early stop, rel IDs, node history with dedup, node history early stop, rel history), TieredStore ForEach (all shards, early stop, with rotation, rel IDs, rel ID early stop, node history, rel history), Graph-level temporal query integration.

## [3.0.30] - 2026-03-02

### Fixed (5 Concurrency & Data Consistency Bugs)

- **`idleCloseLoop` race condition** — `getStore()` returned a `*BadgerStore` pointer and released `shardMu`, allowing `closeIdleShards()` to close the store while callers were still using it. Added `checkoutStore()` / `checkinStore()` with `atomic.Int64` `activeReqs` per shard. `closeIdleShards()` now skips shards with `activeReqs > 0`. Applied to all 10 parallel merge goroutines and 3 sequential count methods.
- **`shardForRelID` unnecessary cold shard probing** — fallback probe opened ALL event shards including cold ones. Cross-shard rels are only created on hot/warm shards, so cold probing is unnecessary and expensive. Fallback now skips `TierCold` shards.
- **`ArchiveNode`/`RestoreNode` missing rollback** — if step 5 (write to destination) partially succeeded then failed, or step 6 (delete from source) failed, data was duplicated across both shards. Added best-effort rollback via `DeleteNodeCascade` on the destination shard on failure.
- **`CreatePropertyIndex` concurrent delete resurrection** — Phase 3 used `liveIdx.contains(id)` which returned false after a concurrent delete (ID removed from all value buckets), causing Phase 3 to re-add the stale backfill value. Added `propertyIndex.mutated` dirty-map: `add()` and `remove()` track all mutated IDs during Phase 2. Phase 3 checks `mutated[id]` instead of `contains(id)`.
- **`BatchBuilder.AddNode` hash mismatch** — hashed raw user-supplied labels (potentially with duplicates) instead of canonical deduplicated labels from the registry. `VerifyNodeHashChain` later used canonical labels, causing permanent verification failure. Now uses `b.g.NodeLabels(n)` for hash computation.

### Changed

- **`eventShard` struct** — new field `activeReqs atomic.Int64` for reference counting cold shard access. New methods `checkoutStore()` / `checkinStore()`.
- **`propertyIndex` struct** — new field `mutated map[snowflake.ID]struct{}` (non-nil only during index creation Phase 2).

### Tests

- 12 new tests: idle-close blocked by active request, concurrent checkout/checkin during idle-close, shardForRelID skips cold shards, shardForRelID finds in warm shard, ArchiveNode/RestoreNode rollback (2), CreatePropertyIndex concurrent delete/update (2), BatchBuilder duplicate label hash, BatchBuilder hash chain verification.

## [3.0.29] - 2026-03-02

### Added (Phase 3e — Repair + Tooling)

- **`DecomposeID(snowflake.ID)`** — extracts `IDComponents{CreatedAt, NodeID, Sequence}` from snowflake ID bits. `time = id >> 22`, `node = (id >> 12) & 0x3FF`, `seq = id & 0xFFF`. Package-level function, also accessible via `Graph.DecomposeID`.
- **`TieredStore.ForceRotate()`** — safe hot-shard rotation with internal locking (unlike `RotateHotShard()` which expects the caller to hold `ts.mu.Lock`). Accessible via `Graph.ForceRotate()`.
- **`TieredStore.ListShards()`** — returns `[]ShardInfo` for all shards (reference, archive, event), enriched with live node/rel counts from open stores. Accessible via `Graph.ListShards()`.
- **`TieredStore.RebuildCatalog()`** — reconstructs the shard catalog from live in-memory state, updating node/rel counts and tier info for all open shards. Accessible via `Graph.RebuildCatalog()`.
- **`TieredStore.VerifyShard(g, shardName)`** — runs hash chain verification on all entities in a named shard. For immutable shards (warm/cold) that have already passed verification, returns the cached result without re-scanning. Caches successful results in the catalog. Accessible via `Graph.VerifyShard(name)`.
- **`TieredStore.RunRepair()`** — cross-shard split-write consistency repair. Phase 1: detects orphaned in/ entries (entity missing from all shards) and deletes them. Phase 2: detects missing in/ entries (entity exists but in/ missing in end shard) and re-creates them. Returns `RepairResult` with counts. Accessible via `Graph.RunRepair()`.
- **`MigrateFromBadger(src, dst, labels)`** — copies all nodes and relationships from a single BadgerStore into a TieredStore with automatic ontology-based routing. No history migration (hash chains would need re-creation).
- **`ErrEventPropertyIndex`** — sentinel error returned when `CreatePropertyIndex` is called for an event label in TieredStore. Property indexes are only supported for reference entities.
- **`ShardInfo`** struct — describes a shard for admin queries: `Name`, `Kind`, `Tier`, `TimeStart`, `TimeEnd`, `Nodes`, `Rels`, `Open`, `Verified`.
- **`VerifyResult`** struct — holds per-shard hash chain verification outcome: `ShardName`, `NodesOK`, `RelsOK`, `NodesFailed`, `RelsFailed`, `Cached`.
- **`RepairResult`** struct — holds repair scan outcome: `OrphanedInEntries`, `MissingInEntries`, `ShardsScanned`, `CrossShardRelsChecked`.
- **`deleteIncomingByRelID`** on BadgerStore — removes an orphaned in/ entry by scanning for matching relID when relType and startID are unknown (entity is gone). Scans pending buffer first, falls back to Badger prefix scan.
- **`UpdateShardVerified`** / **`UpdateShardStats`** on ShardCatalog — field updates for verification caching and catalog rebuild.
- **~29 new tests** — ID decomposition (known values, time precision, node field, temporal filter consistency), property index restriction (ref label, event rejected, errors.Is), catalog extensions, admin API (ForceRotate, ListShards initial/after rotation/with cold/live stats, RebuildCatalog, admin not tiered), per-shard verification (hot, immutable cached, unknown shard), repair (no orphans, orphaned incoming, missing incoming, via Graph), migration (empty, nodes only, with rels, cross-shard rel).

## [3.0.28] - 2026-03-02

### Added (Phase 3d — Cold Shard Lifecycle, Parallel Queries, Reference Archive)

- **Cold shard lazy-open** — `eventShard.getStore(ts)` opens cold shards on first access with per-shard `shardMu` mutex and `atomic.Int64` `lastAccess` tracking. Cold shards are NOT opened on startup (recovered from catalog with `store=nil`).
- **Idle-close goroutine** — `idleCloseLoop()` periodically checks cold shards and closes those idle longer than `IdleTimeout` (default 5min when `ColdAfter > 0`), reclaiming memory.
- **`ColdAfter`** / **`IdleTimeout`** config — `ColdAfter` sets the warm→cold demotion threshold (0=never). `IdleTimeout` sets cold shard auto-close delay.
- **Parallel shard queries** — 10 merge query methods (`AllNodes`, `AllRelationships`, `AllNodeIDs`, `AllRelIDs`, `NodeCount`, `RelationshipCount`, `NodeCountByLabel`, `RelCountByType`, `AllNodeHistoryIDs`, `AllRelHistoryIDs`) launch concurrent goroutines per event shard via `sync.WaitGroup`. Reference shard runs sequentially first.
- **Reference archive** (`refArchive`) — lazy-opened BadgerStore at `data/archive/` for archiving closed/inactive reference entities.
- **`Graph.ArchiveNode(id)`** — moves a reference node and all connected relationships from `refShard` to `refArchive`. Returns `ErrNotReferenceEntity` for event entities.
- **`Graph.RestoreNode(id)`** — moves an archived reference node and relationships back to `refShard`.
- **`shardForNodeID` archive fallback** — node lookup probes `refArchive` when `refShard` misses, enabling transparent reads of archived entities.

### Changed

- **`TieredStore.Close()`** — now closes `refArchive` if open, signals `closeCh` to stop idle-close goroutine.
- **`eventShardSnapshot(depth)`** — returns depth-filtered `[]*eventShard` under `mu.RLock` for merge queries.

## [3.0.27] - 2026-03-02

### Added (Phase 3a — TieredStore Infrastructure)

- **`TieredStore`** — new `Store` implementation routing entities across multiple BadgerStore instances by ontology classification. Reference entities (configured via `RefLabels`) go to a single reference shard; event entities go to time-windowed event shards. Shard resolution is O(1) via snowflake ID timestamp extraction (bits 22–62).
- **`TieredStoreConfig`** — `DataDir`, `InMemory`, `RefLabels`, `ShardWindow` (default 1 week), `CacheCapacity` (default 10K), `FlushInterval` (default 100ms), `ColdAfter` (warm→cold demotion threshold), `IdleTimeout` (default 5min when `ColdAfter > 0`).
- **`OntologyMapping`** / **`EntityClass`** — classifies labels as `ClassReference` (long-lived) or `ClassEvent` (time-windowed, default). Lazy token cache backed by label registry. Token 0 returns `ClassEvent`.
- **`ShardCatalog`** / **`ShardEntry`** / **`ShardKind`** / **`ShardTier`** — JSON-persisted catalog tracking all shards with atomic write (write-tmp + rename). Tracks time windows, labels, rel types, tier (hot/warm/cold), kind (reference/event/archive). `UpdateShardTier`, `UpdateShardTimeEnd`, `HotEventShard`, `EventShards`, `ColdEventShards`.
- **`ShardDepth`** type — `DepthAll` (0, default), `DepthHot` (1), `DepthWarm` (2). Controls which shard tiers are included in TieredStore merge queries. `QueryOpts.Depth` field. Ignored by MemoryStore/BadgerStore (backward-compatible).
- **`BadgerStoreConfig.ReadOnly`** — opens Badger with `WithReadOnly(true)`, skips flushLoop and gcLoop. Used by TieredStore for warm/cold shards.
- **`registry_file.go`** — flat msgpack registry file save/load with atomic rename (write-tmp + rename). Used by TieredStore for label/reltype registry persistence separate from BadgerStore's in-DB persistence.
- **`badgerstore_partial.go`** — unexported helpers on `*BadgerStore` for TieredStore cross-shard relationship routing: `putRelEntityAndOut` (entity+typeIdx+outIdx), `putRelIncoming` (inIdx only), `deleteRelEntityAndOut`/`deleteRelIncoming` (split delete), `hasNodeID`/`hasRelID` (O(1) existence), `incomingRelIDs`/`outgoingRelIDs` (sorted ID snapshots).
- **`Graph.ArchiveNode(id)`** / **`Graph.RestoreNode(id)`** — move reference nodes and connected relationships between ref shard and ref archive. TieredStore only.
- **`ErrNotReferenceEntity`** sentinel error.

### Added (Phase 3b+3c — Shard Rotation, Warm Recovery, Cold Tier)

- **`TieredStore.RotateHotShard()`** — demotes current hot shard to warm (flush, mark read-only, ms-aligned boundary), creates new hot shard with contiguous window. Handles same-window collision via disambiguating suffix.
- **`checkRotation()`** on all new-entity write paths — fast-path time comparison (~1ns), slow-path Lock + double-check + rotate.
- **Warm shard recovery** — constructor reopens warm event shards from catalog as ReadOnly BadgerStore on restart. Mid-window restart via catalog `HotEventShard` resolution.
- **Cold shard support** — shards older than `ColdAfter` are demoted to `TierCold` during rotation. Cold shards are NOT opened on startup (lazy-open on first access via `eventShard.getStore()`). Idle-close goroutine reclaims memory by closing cold shards after `IdleTimeout`.
- **Depth-aware reads** — merge queries (`AllNodes`, `AllRelationships`, `AllNodeIDs`, `AllRelIDs`, `RelationshipsByType`, counts, history IDs) use `eventShardSnapshot(opts.Depth)` under `mu.RLock` to filter shard tiers.
- **Cross-shard relationships** — `PutRelationship` split-write with shard-based routing (`shardForNodeID`, not class-based) for correct E→E cross-shard after rotation. E→R: ref-first in/ per §12. `DeleteRelationship` split-delete. `DeleteNodeCascade` cross-shard aware. `IncomingRelationships` fetches each rel entity via `shardForRelID`.
- **Parallel shard queries** — merge queries launch concurrent goroutines per event shard.
- **~50 new tests** — ontology classification, shard catalog CRUD + persistence, registry file save/load/atomic, badgerstore partial helpers (split write/delete/existence/adjacency), TieredStore end-to-end (ref+event routing, rotation, warm recovery, cross-shard rels, counts, archive/restore, depth filtering, cold shards, idle-close).

### Changed (Phase 3a — GraphTx Full CRUD)

- **`GraphTx`** — upgraded from create-only to full CRUD with snapshot-based rollback. New methods: `UpdateNode`, `UpdateRelationship`, `SetNodeProperty`, `DeleteNodeProperty`, `SetRelationshipProperty`, `DeleteRelationshipProperty`, `DeleteNode` (cascade), `DeleteRelationship`. Rollback restores pre-mutation state in reverse order: deleted rels → deleted nodes → updated rels → updated nodes → created rels → created nodes. Known limitation: phantom version history entries may remain after rollback.
- **`Graph.Close()`** — type switch now handles `*TieredStore` in addition to `*BadgerStore` for registry persistence.
- **`Graph.New()`** — wires `TieredStore.SetLabelRegistry` and loads registries when Store is `*TieredStore`.

## [3.0.26] - 2026-03-02

### Fixed (Concurrency & Integrity — 3 Bugs)

- **`CreatePropertyIndex` concurrent data loss** — rewrote 3-phase approach: Phase 1 now installs an empty live index under write Lock (not RLock) so concurrent `PutNode`/`ReplaceNode` writes are captured immediately via `addNodeToPropertyIndexes`. Phase 2 builds backfill outside lock. Phase 3 merges backfill into live index, skipping IDs already handled by concurrent writes and deleted nodes. Previously, writes between Phase 1 (RLock) and Phase 3 (Lock) were silently dropped.
- **`ComputeNodeHash` hashed raw user labels** — `AddNodeWithContext` now calls `ComputeNodeHash(n, g.NodeLabels(n))` using canonical deduplicated labels from the node's internal tokens. Previously hashed the raw `labels` slice which could contain duplicates (e.g., `["Person", "Person"]`), causing `VerifyNodeHashChain` to fail because verification resolves canonical labels `["Person"]`.
- **`VerifyNodeHashChain`/`VerifyRelHashChain` failed on deleted entities** — both methods now tolerate `ErrNodeNotFound`/`ErrRelNotFound` for the current entity. When current is nil but history exists, the chain is built from history alone and labels/type name are extracted from the last history entry (tombstone). Returns `ErrNodeNotFound`/`ErrRelNotFound` only when neither current nor history exists.

### Added

- **`propertyIndex.contains(id)`** — O(V) check if a node ID exists in any value bucket. Used during `CreatePropertyIndex` merge phase to avoid overwriting concurrent writes.
- **6 new tests** — duplicate-label hash verification, duplicate-label deduplication, deleted entity hash chain verification (node + rel), never-existed hash chain (node + rel), concurrent `CreatePropertyIndex` with simultaneous writes, `propertyIndex.contains` unit test.

## [3.0.25] - 2026-03-02

### Added (Phase 2i — Temporal Query Push-Down + Graph Transactions)

- **Temporal push-down to Store layer** — `QueryOpts` gains `ValidAt types.Instant` (point-in-time filter) and `ValidStart`/`ValidEnd types.Instant` (interval filter). Zero values = no filter (backward-compatible). Both MemoryStore and BadgerStore filter entities at the persistence layer before deep-copy, dramatically reducing allocations for temporal queries.
- **`temporal_filter.go`** — package-level helpers `entityValidFrom(id, tm)` (derives valid-from from explicit `ValidFrom` or snowflake ID bit extraction) and `matchesTemporalFilter(id, tm, opts)` (evaluates point-in-time or interval overlap). Used by both Store implementations.
- **`entityLRU.Peek(key)`** — returns cached value and status without deep-copy or MRU promotion. Used by BadgerStore temporal pre-filter for zero-allocation cache-hit checks.
- **`entityLRU.Cap()`** — returns capacity. Used by `BadgerStore.Clear()` to recreate caches.
- **`GraphTx`** — mutation transaction holding graph write lock for duration. `Graph.BeginTx()` acquires write lock. `AddNode`/`AddRelationship` delegate to Graph and track IDs. `Commit()` releases lock. `Rollback()` deletes created entities in reverse order via `store.Delete*` (no tombstones — rolled-back creates vanish). `CreatedNodeIDs()`/`CreatedRelIDs()` for inspection. All methods return `ErrTxDone` after Commit/Rollback. (Later upgraded to full CRUD in v3.0.27.)
- **`Graph.Reset()`** — acquires write lock, calls `store.Clear()`, preserves registries. For atomic graph clearing.
- **`Store.Clear()`** — removes all entities, indexes, history, counters. MemoryStore reinitializes all maps. BadgerStore resets indexes, counters, caches, pending buffer, then calls `db.DropAll()`.
- **`ErrTxDone`** sentinel error — returned by GraphTx methods after Commit/Rollback.
- **~45 new tests** — 9 temporal filter unit tests, 4 LRU Peek/Cap tests, 12 MemoryStore/BadgerStore temporal push-down tests (ValidAt, pagination, AllNodes, NodesByLabelAndProperty, RelationshipsByType, interval), 12 GraphTx tests (commit/rollback/double-commit/double-rollback/add-after-done/concurrent/empty), 4 Reset tests (empty/clears entities/preserves registries/clears history), 4 Store.Clear tests.

### Changed

- **`GetNodesByLabelValidAt`** — now pushes `ValidAt` into `store.NodesByLabel(tok, QueryOpts{ValidAt: t})` instead of materializing all label matches and filtering in Go.
- **`NodesByLabelPropertyAndTime`** / **`NodesByLabelPropertyDuring`** — push temporal filters into `store.NodesByLabelAndProperty` via QueryOpts.
- **MemoryStore** — 5 paginated methods (`NodesByLabel`, `RelationshipsByType`, `AllNodes`, `AllRelationships`, `NodesByLabelAndProperty`) now apply temporal filtering before deep-copy using `matchesTemporalFilter` on in-memory entity pointers.
- **BadgerStore** — 5 paginated methods use two-stage filtering: `Peek` pre-filter for zero-allocation cache-hit temporal checks, then post-filter cache-miss candidates after `GetNode`/`GetRelationship`.

## [3.0.24] - 2026-03-02

### Added (Phase 2f — Cursor-Based Pagination)

- **`QueryOpts` struct** — `Limit int` (max results, 0 = no limit) and `After snowflake.ID` (keyset cursor, 0 = from start). Zero values mean "return all" for backward compatibility.
- **`paginateIDs` helper** (`pagination.go`) — shared binary-search cursor over sorted `snowflake.ID` slices, used by both MemoryStore and BadgerStore.
- **~27 pagination tests** — 8 `paginateIDs` unit tests, 8 MemoryStore integration (limit, multi-page walk, zero opts, indexed/fallback property query), 8 BadgerStore integration (mirrored), 3 Graph-layer passthroughs. All pass with race detector.

### Added (Phase 2g — Combined Label+Property+Temporal Queries)

- **`NodesByLabelPropertyAndTime(label, key, value, t)`** — intersects property index results with point-in-time temporal filter in a single call. Returns nodes matching all three axes.
- **`NodesByLabelPropertyDuring(label, key, value, start, end)`** — intersects property index results with interval temporal filter. Returns nodes valid during the given range.
- **7 combined query tests** — found (all axes match), property mismatch, temporal mismatch, unregistered label, with property index, interval overlap, no overlap.

### Changed

- **`Store` interface** — 5 unbounded query methods now accept `QueryOpts` parameter: `NodesByLabel(token, opts)`, `RelationshipsByType(token, opts)`, `AllNodes(opts)`, `AllRelationships(opts)`, `NodesByLabelAndProperty(token, key, value, opts)`.
- **`MemoryStore`** — 5 methods refactored to sort IDs before fetch, paginate via `paginateIDs`, then deep-copy only the paginated subset. For `Limit=100` on a 1M-node label, goes from 1M deep copies to 100.
- **`BadgerStore`** — 5 methods refactored with pagination. `NodesByLabelAndProperty` lock scope fixed: changed from holding `idxMu.RLock` during entity I/O to snapshot-and-release pattern. Fallback scan path applies cursor skip first, then early-stops when limit reached.
- **`Graph` layer** — 5 passthrough methods gain `QueryOpts` parameter: `NodesByLabel`, `RelationshipsByType`, `AllNodes`, `AllRelationships`, `NodesByLabelAndProperty`.
- **Internal callers** — `temporal.go` internal methods pass `QueryOpts{}` (zero = return all). All tutorials updated.

## [3.0.23] - 2026-03-02

### Fixed (Phase 2 Review — 6 Issues)

- **Hash chain verification truncation resilience** — `VerifyNodeHashChain`/`VerifyRelHashChain` now detect genesis by `entry.Version() == 0` instead of `i == 0`. After `TruncateNodeHistory`, the oldest chain entry may not be genesis; the old `i == 0` check caused verification to permanently return false. Non-genesis entries at chain position 0 (truncated history) now skip the PrevHash link check while still verifying content hash integrity.
- **`GetNodeAt` truncation resilience** — version start time derivation now checks `entry.Version() == 0` instead of `i == 0`. After truncation, the first entry in the chain may be a non-genesis version whose validity should start at `UpdatedAt`, not at snowflake creation time.
- **`CreatePropertyIndex` lock scope** — rewrote to 3-phase approach: (1) RLock to check existence + snapshot IDs, (2) fetch node data outside any lock via public `GetNode`, (3) write Lock to install index with double-check for concurrent creation. Previously held `idxMu.Lock` during Badger I/O, blocking all concurrent reads/writes. Non-`ErrNodeNotFound` errors are now propagated instead of silently swallowed.
- **Property index persistence** — index definitions now survive BadgerStore restart. Definitions are serialized to `0x0F/prop_indexes` meta key via msgpack on create/drop. `loadIndexes()` reads definitions back and rebuilds index data by scanning matching nodes. Previously, indexes were lost on restart, silently degrading `NodesByLabelAndProperty` to O(N) scan.
- **Delete preserves version history** — all delete paths (`DeleteNode`, `DeleteRelationship`, `DeleteNodeCascade`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`) no longer erase 0x07/0x08 history entries. `DeleteNodeWithContext`/`DeleteRelationshipWithContext` now save tombstone versions (with `DeletedAt`/`ValidTo` set) for all affected entities before deletion. This preserves the temporal history tape for past-time queries. Removed `deleteHistoryByPrefix` function entirely.
- **Temporal queries are history-aware** — `GetNodesValidAt`, `GetRelationshipsValidAt`, `GetNodesValidDuring`, `GetRelationshipsValidDuring`, and `Snapshot` now include deleted entities that were valid at the queried time. Previously, these methods only scanned current tip versions via `AllNodes()`/`AllRelationships()`, making deleted nodes invisible to temporal queries.

### Added

- **`GetRelAt(id, t)`** — returns the version of a relationship valid at instant `t`. Mirrors `GetNodeAt` for relationships. Handles deleted entities via history chain reconstruction.
- **`AllNodeHistoryIDs()`** / **`AllRelHistoryIDs()`** — new Store interface methods returning IDs of all entities with version history entries (including deleted entities whose history was preserved). Implemented in both MemoryStore and BadgerStore (BadgerStore scans both pending buffer and persisted 0x07/0x08 keys).
- **~19 new tests** — hash chain verification after truncation (node + rel), GetNodeAt after truncation, deleted entity temporal queries (GetNodeAt deleted/after-deletion, GetNodesValidAt deleted/updated, GetRelAt basic/deleted/not-found, GetRelationshipsValidAt deleted, Snapshot includes deleted, GetNodesValidDuring deleted, GetRelationshipsValidDuring deleted), BadgerStore AllHistoryIDs (node/rel with pending buffer tests), Badger-backed temporal query integration tests.

### Changed

- **`Store` interface** — added `AllNodeHistoryIDs() ([]snowflake.ID, error)` and `AllRelHistoryIDs() ([]snowflake.ID, error)`.
- **`GetNodeAt`** — now handles deleted entities (tolerates `ErrNodeNotFound`, builds chain from history only). Refactored version resolution into `resolveNodeVersionAt`/`nodeVersionBounds` helpers.
- **`DeleteNodeCascade`** — simplified to single-phase (preflight + apply). Removed Phase 3 (history cleanup) since history is now preserved.
- **`context.go`** — `DeleteNodeWithContext` saves tombstone versions for all connected relationships and the node before cascade delete. `DeleteRelationshipWithContext` saves tombstone version before delete.
- **`keys.go`** — added `propIndexDefsKey` meta key for property index definition persistence.

## [3.0.22] - 2026-03-02

### Added (Phase 2e — Configurable Validation Limits)

- **`ValidationLimits` struct** on `Graph.Config` — configurable limits for graph operations: `MaxLabelsPerNode` (default 50), `MaxPropertiesPerEntity` (default 1000), `MaxPropertyKeyLength` (default 256), `MaxPropertyValueSize` (default 65536, string values only), `MaxNameLength` (default 256, label and reltype names). Zero values resolve to defaults in `New()`.
- **5 sentinel errors** — `ErrTooManyLabels`, `ErrTooManyProperties`, `ErrKeyTooLong`, `ErrValueTooLarge`, `ErrNameTooLong` (all in `graph.go`).
- **Validation enforcement** in all 4 `WithContext` mutation methods (`AddNodeWithContext`, `AddRelationshipWithContext`, `UpdateNodeWithContext`, `UpdateRelationshipWithContext`) and all 4 `BatchBuilder` mutation methods (`AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`). Update methods use two-phase validation: pre-lock entry checks + post-mutation `MaxPropertiesPerEntity` under entity lock.
- **`PropertyCount()`** on `Node` and `Relationship` — returns `properties.Len()` without deep copy. Used by update-path post-mutation property count validation.
- **~30 new tests** — defaults, zero-uses-defaults, custom limits, boundary tests (at-limit succeeds, one-over fails) for all 5 limits across AddNode/AddRelationship/UpdateNode/UpdateRelationship, batch mirroring. All sentinel errors tested with `errors.Is`.

### Changed (Phase 2d upgrade — Per-Label/Per-Type Statistics O(1))

- **`Store` interface** — added `NodeCountByLabel(token uint16) (int, error)` and `RelCountByType(token uint16) (int, error)` for O(1) per-label/per-type counting at the Store level.
- **`MemoryStore`** — O(1) via `len(labelIdx[token])` / `len(typeIdx[token])` under existing RWMutex.
- **`BadgerStore`** — `sync.Map` of `*atomic.Int64` counters, maintained incrementally in 9 mutation sites (`PutNode`, `DeleteNode`, `PutRelationship`, `deleteRelByInfo`, `PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `cascadeDeleteLocked` normal + corruption paths). Counters rebuilt from index sizes in `loadIndexes()` — no new Badger keys.
- **Graph layer** — `NodeCountByLabel`, `RelCountByType`, `AllLabelCounts`, `AllRelTypeCounts` now delegate to Store-level O(1) methods instead of materializing all entities via `NodesByLabel`/`RelationshipsByType`. `AllLabelCounts`/`AllRelTypeCounts` use `uint16(i)` as token directly.
- **18 new tests** — 8 MemoryStore counter tests, 8 BadgerStore counter tests (including persistence round-trip verifying counters rebuilt from indexes), 2 graph-level integration tests (batch add, cascade delete).

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
