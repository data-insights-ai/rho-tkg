# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [3.0.60] - 2026-03-06

### Fixed (2 Defects — post-v3.0.59 audit)

- **Fix M — Temporal index concurrent sort race** (`pkg/graph/temporal_index.go`, MAJOR): `sortIfDirty` was called under `idxMu.RLock` (shared) but `sort.Slice` mutates the slice in place. Two concurrent readers calling `queryAt`/`queryOverlap` could race on the sort. Fixed by adding `sortMu sync.Mutex` which serializes `sortIfDirty` — callers still enter under `RLock` but the sort itself is single-threaded.

- **Fix N — NodesByLabel temporal fast path nil fallthrough** (`pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, MINOR): When a temporal index existed but `queryAt`/`queryOverlap` returned a nil result (no matches), the nil fell through the `if ids != nil` guard and proceeded to the O(N) label scan — defeating the index. Fixed by tracking `temporalQuery bool` separately from the result slice, so an empty-but-valid temporal result correctly short-circuits the label scan.

### Tests Added

- `pkg/graph/temporal_index_stress_test.go` — 11 stress tests covering concurrent sort races, interleaved reads/writes, and high-contention temporal index operations (Fix M).

### Changed

- Tutorial `005_performance`: added sections 8-11 (temporal index benchmarks, high-frequency index comparison, vector index throughput, TieredStore shard rotation overhead).
- **Snowflake Layout Upgrade**: Upgraded `github.com/bds421/rho-snowflake-2026` to `v1.3.0`. Switched to microsecond precision (`snowflake.WithMicroseconds()`) and adjusted bit layout: node bits reduced from 10 to 5 (max `SnowflakeNodeID` is now 15), step bits reduced from 12 to 10.
- **Removed hardcoded bit-shifting**: Replaced manual bitwise operations via native `snowflakeLayout.CreatedAt(id)` and `snowflakeLayout.Decompose(id)` APIs across `id_decompose.go`, `shadow.go`, `temporal.go`, `temporal_filter.go`, `entity_locks.go`, and `tieredstore.go`.

### Added

- `AGENTS.md` — comprehensive guidelines, project overview, testing rules, and architecture map for AI agents assisting with the codebase.

### Documentation

- Updated `CLAUDE.md` to include a new lesson on using library APIs instead of reimplementing internals.
- Updated `CLAUDE.md`, `README.md`, `docs/architecture.md`, `docs/SPEC.md`, `pkg/types/shadow.go` to reflect v3.0.60 changes.

## [3.0.59] - 2026-03-04

### Fixed (4 Defects — external audit)

- **Fix I — Transaction isolation gap** (`pkg/graph/context.go`, `pkg/graph/graph.go`, BLOCKER): `BeginTx()` holds `g.mu.Lock()` but individual mutations (`AddNode`, `UpdateNode`, etc.) did not acquire `g.mu`, so concurrent standalone mutations bypassed tx isolation — torn reads were possible during snapshot operations. Fixed via internal/external method split: all 13 exported mutation methods (`*WithContext`, `RemoveNodeLabel`, `CloseNodeVersion`, `CloseRelVersion`) now acquire `g.mu.RLock()` at entry. New unexported `*Internal` variants (lock-free) are called directly by `GraphTx` and `BatchBuilder` (which already hold `g.mu.Lock()`). Lock ordering: `g.mu` → entity locks (safe: entity locks never acquire `g.mu`).

- **Fix J — Rollback event desync** (`pkg/graph/graph.go`, `pkg/graph/tx.go`, MAJOR): Events were emitted immediately during tx mutations via `publishEvent`. On rollback, state was restored but no compensating events were published, leaving EventBus subscribers inconsistent. Fixed by adding `txEventBuffer *[]Event` field to `Graph`. During a transaction, `publishEvent` appends to the buffer instead of dispatching. On `Commit`, events are published after `g.mu.Unlock()` (so handlers can safely call Graph read methods). On `Rollback`, the buffer is discarded.

- **Fix K — Missing directory fsync** (`pkg/graph/registry_file.go`, MINOR): `atomicWriteFile` did `tmp.Sync()` + `os.Rename()` but omitted the directory `fsync`. On crash, the rename could be lost on some filesystems (ext4 with delayed allocation). Fixed by opening the parent directory and calling `Sync()` after rename.

- **Fix L — Dead code removal** (`pkg/graph/temporal.go`, `pkg/graph/tieredstore.go`, TRIVIAL): Removed 3 unused functions: `isNodeValidDuring`, `isRelValidDuring` (temporal.go), `classifyNodeID` (tieredstore.go). Zero callers confirmed via grep.

### Changed

- `pkg/graph/batch.go`: `Execute` now calls `*Internal` variants directly (was calling exported methods that would deadlock under `g.mu.Lock`). Added `"context"` import.
- `pkg/graph/export.go`: Updated `ExportGraph` doc comment to reflect that individual mutations are now also blocked by `g.mu`.

### Tests Added

- `TestMutationBlockedDuringTx` — verifies standalone `AddNode` blocks while a tx holds `g.mu.Lock`.
- `TestSnapshotConsistencyDuringMutation` — concurrent reads and writes under race detector.
- `TestTxCommitPublishesBufferedEvents` — 3 events buffered during tx, all published on Commit in order.
- `TestTxRollbackNoEvents` — zero events published after Rollback.
- `TestTxCommitHandlerCanReadGraph` — event handler successfully calls `GetNode` on Commit (proves events fire after `g.mu.Unlock`).
- `TestBatchEventsNotBuffered` — standalone mutations still emit events immediately.

## [3.0.58] - 2026-03-04

### Fixed (2 Defects — post-v3.0.57 code review triage)

- **Fix G — temporalIndex O(N²) memmove under store write lock** (`pkg/graph/temporal_index.go`, MAJOR): `add()` used binary search + `copy()` shift to maintain sorted order on every insert. For N batch inserts (e.g. bulk node import, N temporal index adds per label) this was O(N²) total memmove, all performed while holding the store's write lock. Fixed via lazy sort: `add()` now appends unsorted and sets `dirty=true`; new `sortIfDirty()` runs `sort.Slice` once at the start of `queryAt()` and `queryOverlap()`. Complexity: N inserts → O(N) appends + O(N log N) sort at first query (vs. O(N²) sorted insertions). The `dirty bool` field adds 1 byte to `temporalIndex`; no external API change.

- **Fix H — RemoveLabelTokenRaw leaves non-nil empty extraLabels** (`pkg/types/node.go`, MINOR): Two removal paths in `RemoveLabelTokenRaw` left `n.extraLabels` as an empty slice with non-zero capacity, violating the convention that an absent label set is `nil`. Case 1 (removing the only extra label via `append([:i], [i+1:]...)`) and Case 2 (promoting the only extra to primary via `extraLabels[1:]`) both produced `[]labelToken{}` with cap > 0. Since `DeepCopy` only copies when `len(n.extraLabels) > 0`, and `ExtraLabelTokens()` already guards with the same check, a non-nil empty slice was functionally harmless but inconsistent and retained the backing array unnecessarily. Fixed by adding an explicit `n.extraLabels = nil` after each removal path that empties the slice.

### Documentation

- `pkg/types/propertyslice.go` (`Set`): added note that `NewPropertySlice` (O(N log N)) is preferred over repeated `Set` calls for bulk construction.
- `pkg/types/recurrence.go` (`RecurrencePattern`): strengthened UTC-only note — DST transitions are invisible to this type; callers must convert expanded instants to local time.
- `pkg/graph/vector_index.go` (`toFloat32Slice`): added slow-path note on the `[]any` branch; callers should prefer `[]float32` property values for high-frequency vector nodes.

### Tests Added

- `TestTemporalIndex_LazySort_BatchInsert` — 100 out-of-order inserts followed by a single `queryAt`; verifies all IDs returned and `dirty` transitions correctly (Fix G).
- `TestTemporalIndex_LazySort_InterleavedReadsWrites` — interleaved `add`/`queryAt`/`queryOverlap` calls; verifies correct results at each step and `dirty` flag transitions (Fix G).
- `TestRemoveLabelTokenRaw_ExtraLabelsNilAfterLastRemoval` — three sub-tests: remove only extra, promote only extra to primary, remove one of two extras (last case verifies non-nil is preserved) (Fix H).

## [3.0.57] - 2026-03-03

### Fixed (6 Production Defects — v3.0.57)

- **Fix A — Rollback/Commit panic leaks graph write lock** (`pkg/graph/tx.go`, CRITICAL): `tx.g.mu.Unlock()` was the last explicit statement in `Rollback()`. Any panic in one of the six rollback phases (store `PutRelationship`, `PutNode`, `ReplaceRelationship`, `ReplaceNode`, `DeleteRelationship`, `DeleteNodeCascade`) left the graph write lock permanently held, deadlocking all subsequent `BeginTx`/`Batch`/`Reset` callers. Fixed by replacing the explicit unlock with `defer tx.g.mu.Unlock()` placed immediately after `tx.done = true`, ensuring the lock is released on both normal and panic paths. Same fix applied to `Commit()` for forward safety.

- **Fix B — RemoveNodeLabel violates temporal guarantees** (`pkg/graph/graph.go`, BLOCKER): `RemoveNodeLabel` overwrote the node in-place via `RemoveNodeLabelToken` with no version bump and no history entry (explicit "No version bump; no history entry" comment). Past-time queries (`GetNodeAt`, `GetNodesValidAt`, `DiffSnapshots`) could not observe that the node ever had the removed label. Fixed following the `UpdateNodeWithContext` version-bump pattern: capture `prevVersion`/`prevState` before mutation, call `PutNodeVersion(id, prevVersion, prevState)` to save the old state, bump `copy.SetVersion(prevVersion + 1)`, then call `RemoveNodeLabelToken`. Also corrected the hash chain to use `current.Integrity().Hash` (not `PrevHash`) as the new `PrevHash`, matching the chain in all other update paths. Stale comment removed from `badgerstore.go:RemoveNodeLabelToken`. Crash window: phantom history entry possible if crash between `PutNodeVersion` and `RemoveNodeLabelToken` — same documented limitation as `Rollback`; entity state remains correct. TODO(v3.0.58): atomic `RemoveNodeLabelTokenWithHistory`.

- **Fix C — DiffSnapshots holds g.mu.RLock during O(N) materialization** (`pkg/graph/temporal.go`, BLOCKER): `DiffSnapshots` held `g.mu.RLock()` across two calls to `snapshotLocked`, each materialising ALL valid nodes/rels into RAM. All `g.mu.Lock()` callers (`BeginTx`, `Batch`, `Reset`) were blocked for the full O(N) duration. `GetNodesValidAt` uses `forEachKnownNodeID` which does its own store-level locking — `g.mu` is not required for correctness of individual snapshot reads. Fixed by removing `g.mu.RLock()` from `DiffSnapshots`. Renamed `snapshotLocked` → `snapshotAt` (no longer requires the caller to hold `g.mu`). `Snapshot()` continues to hold `g.mu.RLock()` for strong consistency. Trade-off documented: a concurrent backdated write that commits between the two reads may appear as a spurious Created/Deleted entry. TODO(v3.0.58): streaming `DiffSnapshots` to avoid O(N) RAM.

- **Fix D — searchNearest O(N log N) sort under RLock** (`pkg/graph/vector_index.go`, MAJOR): `searchNearest` allocated a `[]scored` of all N entries, sorted it in O(N log N) while holding `vi.mu.RLock()`, blocking concurrent vector insertions. Replaced with a max-heap of size k (`knnHeap` via `container/heap`) that runs in O(N log k) time. For k ≪ N the lock is held significantly shorter; for k=N behaviour is equivalent. The heap drains in ascending distance order (closest first) matching the previous sort contract. Removed `"sort"` import; added `"container/heap"`.

- **Fix E — checkTemporalConstraints allocates on every relationship write** (`pkg/graph/temporal_constraint.go`, MINOR): `g.constraints.Items()` copies the constraint slice on every relationship write (even with a single constraint). Added unexported `forEach(fn func(TemporalConstraint) error) error` method on `ConstraintSet` that iterates `cs.items` directly with zero allocation. `checkTemporalConstraints` updated to use `forEach`. Exported `Items()` retained for external callers.

- **Fix F — TOCTOU retry loop has no backoff** (`pkg/graph/context.go`, MINOR): The TOCTOU retry in `DeleteNodeWithContext` called `continue` immediately after `UnlockMany` on a failed attempt. Under sustained concurrent rel-add/remove to the same node, all 10 retries could exhaust without the competing goroutine making progress. Fixed by adding `runtime.Gosched()` after `UnlockMany`, yielding the processor to let the competing rel-writer commit. Added `"runtime"` to import block.

### Tests Added

- `TestRemoveNodeLabel_PreservesHistory` — verifies `GetNodeHistory` returns 1 entry after `RemoveNodeLabel`, version 0 retains the removed label (Fix B).
- `TestGraphTx_RollbackPanicSafe` — injects a store panic during rollback via `deleteRelPanicStore`; verifies `BeginTx` completes within 2s after recovery, confirming the graph write lock was released by the deferred unlock (Fix A).
- `TestDiffSnapshots_DoesNotBlockWrites` — runs 8 concurrent `DiffSnapshots` goroutines alongside 8 `BeginTx` goroutines; verifies all complete within 10s without deadlock (Fix C).
- `TestSearchNearest_HeapCorrectness` — verifies top-k results from the heap implementation match brute-force distances for k=1 (exact nearest), k=3 (set equality + non-decreasing order), and k=N (set equality + non-decreasing order, allowing ties within equal-distance groups) (Fix D).

## [3.0.56] - 2026-03-03

### Fixed (7 Production Defects — v3.0.56)

- **Fix #1 — Cold shard use-after-close** (`pkg/graph/tieredstore.go`): `shardForNodeID` returned a `*BadgerStore` pointer without incrementing `activeReqs`, creating a race with `closeIdleShards` that could panic on cold-shard Badger access. Fixed by adding `shardForNodeIDChecked(id snowflake.ID) (*BadgerStore, func(), error)` which wraps the shard resolution with `checkoutStore`/`checkinStore`. The returned `checkin` function is deferred at every callsite. All six read/write callsites updated: `GetNode`, `GetNodeHistory` (`tieredstore_read.go`), `DeleteNode`, `ReplaceNode`, `RemoveNodeLabelToken` (`tieredstore_write.go`). The refShard and refArchive return a no-op checkin (they are never closed by `closeIdleShards`).

- **Fix #2 — stripDepth drops pagination** (`pkg/graph/tieredstore_read.go`): `stripDepth` was returning `QueryOpts{Depth: stripped}` — a fresh struct that dropped `Limit`, `After`, and all temporal fields. Every per-shard call from `NodesByLabel`, `AllNodes`, and `AllRelationships` received `Limit=0` (unbounded), causing full-shard scans and potential OOM materialisation. Fixed by modifying `opts` in-place (`opts.Depth = 0; return opts`) so all other fields are preserved.

- **Fix #3 — PutNodesBatch / PutRelationshipsBatch no rollback** (`pkg/graph/tieredstore_write.go`): When the second-shard write in a batch failed, first-shard writes were committed, leaving orphaned entities in the store. Fixed by adding best-effort rollback: on hot-shard `PutNodesBatch` failure, each successfully written refShard node is removed via `DeleteNode`; on `PutRelationshipsBatch` failure, each completed shard batch is rolled back. Best-effort (second-shard rollback failure is logged as the same documented B7 limitation).

- **Fix #4 — idxMu held over disk I/O** (`pkg/graph/badgerstore.go`): `DeleteNode`, `ReplaceNode`, and `RemoveNodeLabelToken` called `idxMu.Lock()` then immediately `getNodeLocked(id)`, which on cache miss issued a synchronous Badger `db.View()` read under the global write lock — blocking all concurrent readers (including `NodesByLabel`, `ForEach`) for up to 5ms per operation under `SyncWrites=true`. Fixed by adding `prefetchNode(id snowflake.ID) (*types.Node, error)` helper that checks the cache under `RLock`, then reads from Badger without any lock, then re-verifies under `RLock`. All three methods call `prefetchNode` before acquiring `idxMu.Lock()`; the TOCTOU window is guarded by re-checking `nodeIDs[id]` under the write lock.

- **Fix #5 — ImportGraph holds lock during streaming I/O** (`pkg/graph/export.go`): `ImportGraph` previously acquired `g.mu.Lock()` at function entry and held it for the entire `io.Reader` streaming loop, freezing all graph operations (reads, writes, queries) for potentially minutes during large or networked imports. Fixed with a two-phase implementation: Phase 1 (no lock) buffers all records from the `io.Reader` into a `[]importRecord` slice; Phase 2 (under `g.mu.Lock()`) deserialises the buffer and applies store writes. I/O latency no longer extends the lock scope.

- **Fix #6 — AllNodeHistoryIDs / AllRelHistoryIDs OOM** (`pkg/graph/export.go`, `badgerstore.go`): Deferred to v3.0.57 — fixing requires a cursor-based `AllNodeHistoryIDs(QueryOpts)` on the `Store` interface, which breaks all three implementations. TODO comments added to `export.go` and `badgerstore.go`'s `AllNodeHistoryIDs` implementation.

- **Fix #7 — BackpressureDropOldest CPU livelock** (`pkg/graph/events.go`): The `BackpressureDropOldest` inner `for {}` loop had an empty `default:` branch — tight spinning under contention saturated a CPU core and starved the worker goroutine (livelock). Fixed by adding `runtime.Gosched()` in the default case. No cost on the uncontended path; yields the scheduler slot on contention.

### Tests Added

- `TestTieredStore_NodesByLabel_PaginationBounded` — verifies Limit/After are honoured across a multi-shard TieredStore query (fix #2).
- `TestAsyncEventBus_DropOldest_TerminatesQuickly` — 50×20 concurrent publishes on a full channel complete within 2s (fix #7).
- `TestTieredStore_PutNodesBatch_RollbackOnHotShardError` — verifies ref shard has no orphan node after hot-shard failure (fix #3).
- `TestBadgerStore_DeleteNode_NoDiskIOUnderWriteLock` — DeleteNode on a cache-miss node does not hold idxMu during db.View (fix #4).
- `TestImportGraph_DoesNotBlockReadsWhileStreaming` — concurrent GetNode succeeds during ImportGraph Phase 1 (fix #5).

All in `pkg/graph/v3056_fixes_test.go`.

## [3.0.55] - 2026-03-03

### Added (Atomic Delete with Tombstone History — Phase 4.23)

- **`RelTombstone`** — new struct in `pkg/graph/store.go` packaging a relationship's tombstone data for atomic delete operations: `ID snowflake.ID`, `PrevVersion uint32`, `Tombstone *types.Relationship` (pre-built deep copy with `DeletedAt`/`ValidTo`/`TxFrom`/`TxTo` set by the caller).
- **`Store.DeleteNodeWithHistory(id snowflake.ID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error`** — new interface method. Atomically combines `PutRelVersion×N` + `PutNodeVersion` + `DeleteNodeCascade` into a single storage transaction. Eliminates orphaned tombstone history entries that could result from a crash between the previous N+2 separate store calls. `relTombstones` may be nil when the node has no connected relationships.
- **`Store.DeleteRelWithHistory(id snowflake.ID, prevVersion uint32, tombstone *types.Relationship) error`** — new interface method. Atomically combines `PutRelVersion` + `DeleteRelationship` into a single storage transaction. Eliminates the crash window between the previous two separate calls.
- **`MemoryStore.DeleteNodeWithHistory` / `DeleteRelWithHistory`** — implemented under a single `ms.mu.Lock()`, ensuring history write and entity deletion are atomic with respect to concurrent readers.
- **`BadgerStore.DeleteRelWithHistory`** — serializes tombstone outside `idxMu`, acquires `idxMu.Lock()` once, calls `deleteRelByInfo` (queues cascade delete ops into `pending`), appends tombstone history op to the same `pending` map, releases lock, then flushes. All ops land in the same Badger `WriteBatch.Flush()` call — atomic.
- **`BadgerStore.DeleteNodeWithHistory`** — serializes all tombstones outside `idxMu`, acquires `idxMu.Lock()` once, calls `cascadeDeleteInner` (queues all cascade delete ops into `pending`), appends node + rel tombstone history ops to the same `pending` map before releasing the lock. Single `WriteBatch.Flush()` commits cascade + tombstone history atomically.
- **`cascadeDeleteInner`** — unexported helper extracted from `cascadeDeleteLocked`. Same body, but without `idxMu.Lock()/Unlock()` — caller must hold the lock. Enables `DeleteNodeWithHistory` to extend the lock scope across cascade + tombstone append without a second lock acquisition.
- **`marshalNodeToBytes` / `marshalRelToBytes`** — unexported helpers in `badgerstore.go` wrapping `msgpack.Marshal(nodeToWire(n))` / `msgpack.Marshal(relToWire(r))`. Used by both existing `PutNodeVersion`/`PutRelVersion` paths and the new tombstone serialization in `DeleteNodeWithHistory`/`DeleteRelWithHistory`.
- **`TieredStore.DeleteRelWithHistory`** — delegates to the relationship's entity shard for tombstone + entity/typeIdx/outIdx ops. Cross-shard inIdx cleanup follows the same split-write pattern as `DeleteRelationship`. Per-shard atomic only (B7 limitation, same as `DeleteNodeCascade`).
- **`TieredStore.DeleteNodeWithHistory`** — delegates rel tombstones via `DeleteRelWithHistory`, then delegates node tombstone via `shard.DeleteNodeWithHistory(id, prevNodeVersion, nodeTombstone, nil)`. Per-shard atomic only.
- **`context.go:deleteNodeLocked`** rewritten — builds `[]RelTombstone` in one pass (dedup via `seen` map), builds node tombstone, calls single `g.store.DeleteNodeWithHistory`. Replaces the previous loop of `PutRelVersion` calls + `PutNodeVersion` + `DeleteNodeCascade` (N+2 separate store calls).
- **`context.go:DeleteRelationshipWithContext`** rewritten — calls single `g.store.DeleteRelWithHistory`. Replaces the previous `PutRelVersion` + `DeleteRelationship` (2 separate store calls).
- 6 tests in `pkg/graph/atomic_delete_test.go`: `TestDeleteRelWithHistory_HistoryAndLiveConsistent`, `TestDeleteRelWithHistory_NotFound`, `TestDeleteNodeWithHistory_HistoryAndLiveConsistent`, `TestDeleteNodeWithHistory_EmptyRelTombstones`, `TestDeleteNodeWithHistory_BadgerStore`, `TestDeleteNodeWithHistory_TieredStore`.

### Design Notes

- Atomicity guarantee (BadgerStore): holding `idxMu.Lock()` across both `cascadeDeleteInner` and tombstone `appendOps` prevents the background flush goroutine (which acquires `idxMu.RLock()`) from draining `pending` between the two phases. All ops land in one `WriteBatch.Flush()`.
- TieredStore cross-shard atomicity is per-shard only — the same documented B7 limitation as `DeleteNodeCascade`. Cross-shard rels are handled shard-by-shard.
- `DeleteNodeCascade`, `PutNodeVersion`, `PutRelVersion` are unchanged — they are still used by repair/migration tools (`tieredstore_repair.go`, `tieredstore_migrate.go`) which do not write tombstones.
- After deletion, `TieredStore.GetNodeVersion`/`GetRelVersion` cannot resolve the shard via the high-level routing (relies on live in-memory presence). Access the underlying shard directly (`ts.refShard.GetNodeVersion(...)`) for post-delete history verification — as demonstrated in `TestDeleteNodeWithHistory_TieredStore`.

## [3.0.54] - 2026-03-03

### Added (AllowSelfLoops Validation — Phase 4.22)

- **`ValidationLimits.AllowSelfLoops bool`** — new field on `ValidationLimits`. Default zero value (`false`) rejects self-loop relationships (where `startNode == endNode`). Set to `true` to permit them. This aligns rho/tkg-v3 with the tkg-2025-v2 and tkg-2026-v3 reference implementations.
- **`ErrSelfLoop`** — new graph-layer sentinel error: `"graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit"`. Returned by `AddRelationshipWithContext` and `ImportRelationshipWithID` when `startID == endID && !g.validation.AllowSelfLoops`.
- **`context.go:AddRelationshipWithContext`** — self-loop guard added after `endID` extraction, before `LockTwo`: rejects when `startID == endID && !g.validation.AllowSelfLoops`.
- **`context.go:ImportRelationshipWithID`** — same self-loop guard added at the equivalent position.
- 5 tests in `pkg/graph/self_loop_test.go`: `TestSelfLoop_ZeroValueRejects`, `TestSelfLoop_ExplicitFalseRejects`, `TestSelfLoop_AllowedByConfig`, `TestSelfLoop_ImportRejected`, `TestSelfLoop_DifferentNodesStillWork`.

### Fixed

- **`TestGraphDeleteNodeSelfLoopCascade`** (`pkg/graph/graph_test.go`) — updated to use `Config{Validation: ValidationLimits{AllowSelfLoops: true}}` since the default now rejects self-loops.
- **`TestEndpointHashSelfLoop`** (`pkg/graph/rel_endpoint_hash_test.go`) — same fix.

## [3.0.53] - 2026-03-03

### Fixed (Code Review Bugs — Phases 4.13–4.16)

- **`extractProvenance` silent truncation** (`pkg/graph/context.go`): Integer values of `tkg_auth_level` outside `[0, 255]` previously silently wrapped via modulo cast to `uint8`, corrupting the stored `AuthorizationLevel`. Unrecognised non-nil types (e.g. `string("5")`) silently stored 0. Fixed by adding bounds checks for all integer/float64 cases (`int`, `int32`, `int64`, `float64`) and a `default` case that returns an explicit error. The function signature gains a 6th `error` return; all 4 callers in `context.go` now propagate the error. All `#nosec G115` comments on the cast sites removed — bounds are now checked explicitly. 5 new tests: `TestExtractProvenance_OutOfBoundsInt`, `TestExtractProvenance_NegativeInt`, `TestExtractProvenance_OutOfBoundsFloat`, `TestExtractProvenance_InvalidType`, `TestExtractProvenance_ValidBoundary`.

- **`ImportGraph` registry mismatch** (`pkg/graph/export.go`): When importing into a graph whose label/reltype registry was already populated with different token mappings, `ErrRegistryNotEmpty` was swallowed and the import continued, silently assigning wrong labels and relationship types to all imported entities. Fixed by comparing the existing registry with the incoming one via `reflect.DeepEqual` when `ErrRegistryNotEmpty` is returned from `ImportNames`. Identical registries (idempotent re-import) continue without error; conflicting registries return the new sentinel `ErrIncompatibleRegistry`. 2 new tests: `TestImportGraph_IncompatibleLabelRegistry`, `TestImportGraph_CompatibleRegistryIdempotent`. Existing `TestImport_IdempotentRegistry` continues to pass.

- **`readExportRecord` DoS / OOM** (`pkg/graph/export.go`): The 4-byte length header in the export stream was trusted unconditionally. A crafted export file with `length = 4 GiB` caused an immediate OOM allocation before any data was read. Fixed by adding `maxExportRecordSize = 128 MiB` constant and a guard that returns an error before the `make([]byte, length)` call. 1 new test: `TestReadExportRecord_OversizeRecord`.

## [3.0.52] - 2026-03-03

### Added (HighFrequencyIndex — Phase 4.21)

- **`highFrequencyIndex`** — new time-bucketed index providing O(1) amortized insertion versus the sorted-slice `temporalIndex`'s O(log n). Designed for high-write-rate scenarios (thousands of event writes/sec into TieredStore event shards).
- **`newHighFrequencyIndex(bucketSize time.Duration, origin types.Instant) *highFrequencyIndex`** — constructor. `bucketSize` controls the time width of each bucket (e.g., `time.Hour`). `origin` sets the baseline for bucket 0.
- **`(*highFrequencyIndex).add(id snowflake.ID, validFrom types.Instant)`** — O(1) amortized insertion; bucket index = `(validFrom - origin) / bucketSize`. Thread-safe via internal `sync.RWMutex`.
- **`(*highFrequencyIndex).remove(id snowflake.ID, validFrom types.Instant)`** — O(n/num_buckets) amortized removal from a single bucket.
- **`(*highFrequencyIndex).pointQuery(t types.Instant) []snowflake.ID`** — returns all IDs in the bucket containing `t`. Returns candidates — callers must re-filter by `ValidTo` if exact interval matching is needed.
- **`(*highFrequencyIndex).rangeQuery(start, end types.Instant) []snowflake.ID`** — returns all IDs in buckets overlapping `[start, end)`. Candidates — same re-filter note as `pointQuery`.
- **`(*highFrequencyIndex).bucketFor(validFrom types.Instant) int64`** — unexported helper; instants before `origin` map to negative bucket indices using correct floor division.
- **`Store.CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error`** — new method on the `Store` interface. Returns `ErrTemporalIndexExists` if any temporal index (sorted-slice or bucket) already exists for this label — only one type per label at a time.
- **`Store.DropHighFrequencyIndex(labelToken uint16) error`** — new method on the `Store` interface. Returns `ErrTemporalIndexNotFound` if no high-frequency index exists.
- **`MemoryStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — implemented; stored in a new `hfIndexes map[uint16]*highFrequencyIndex` field (separate from `temporalIndexes`).
- **`BadgerStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — implemented; stored in a new `hfIndexes map[uint16]*highFrequencyIndex` field under `idxMu`. Not persisted — must be rebuilt via `CreateHighFrequencyIndex` after restart.
- **`TieredStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — delegates across all active shards (ref + event) following the same pattern as `CreateTemporalIndex`. New hot shards created via rotation do NOT inherit HFI automatically.
- **`Graph.CreateHighFrequencyIndex(label string, bucketSize time.Duration) error`** — public Graph API. Returns nil if the label has never been registered. Returns `ErrTemporalIndexExists` on conflict.
- **`Graph.DropHighFrequencyIndex(label string) error`** — public Graph API. Returns nil if the label has never been registered. Returns `ErrTemporalIndexNotFound` if no HFI exists.
- 12 tests in `pkg/graph/hf_index_test.go`: `TestHFIndex_Add_PointQuery`, `TestHFIndex_RangeQuery`, `TestHFIndex_Remove`, `TestHFIndex_HighWriteRate` (10k concurrent adds, race-clean), `TestCreateHighFrequencyIndex_Graph`, `TestHFIndex_ReplacesTemporalIndex`, `TestHFIndex_DuplicateCreate`, `TestHFIndex_DropNotFound`, `TestHFIndex_ConflictsWithTemporalIndex`, `TestHFIndex_UnknownLabel`, `TestHFIndex_BucketFor_BeforeOrigin`, `TestHFIndex_RangeQuery_EmptyResult`.

### Design Notes

- HFI does NOT store `ValidTo` — it indexes `validFrom` only. Callers needing precise interval filtering must re-filter results.
- HFI is NOT persisted (like vector indexes). Must be rebuilt on restart via `CreateHighFrequencyIndex`. Document this in your service bootstrap code.
- Only one temporal index type can exist per label at a time: a `temporalIndex` and a `highFrequencyIndex` cannot coexist. Drop one before creating the other.
- `time.Duration` added to `store.go` imports (used by new interface methods).

## [3.0.51] - 2026-03-03

### Added (Event Priority Levels — Phase 4.20)

- **`EventPriority uint8`** — new type controlling delivery queue routing in `AsyncEventBus`. Five named constants (zero value = `PriorityNormal` for backward compatibility):
  - `PriorityNormal` (0) — default; all existing `Event{}` literals remain valid without change.
  - `PriorityHigh` (1) — create events (`EventNodeCreate`, `EventRelCreate`).
  - `PriorityCritical` (2) — delete/cascade events (`EventNodeDelete`, `EventRelDelete`).
  - `PriorityLow` (3) — available for caller-side lower-priority events.
  - `PriorityDeferred` (4) — available for caller-side background/analytics events.
- **`numPriorityLevels = 5`** — unexported constant sizing the per-priority queue array.
- **`Event.Priority EventPriority`** — new field on `Event`. Zero value is `PriorityNormal`; all existing `Event{}` struct literals compile unchanged (backward-compatible).
- **`AsyncEventBus.queues [numPriorityLevels]chan Event`** — replaces the single `queue chan Event` with one buffered channel per priority level. `QueueSize` is applied uniformly to each channel.
- **`priorityOrder [numPriorityLevels]EventPriority`** — package-level drain order array: `[Critical, High, Normal, Low, Deferred]`. Used by workers to implement best-effort priority ordering.
- **Per-priority `publish` routing** — `AsyncEventBus.publish` routes each event to `queues[e.Priority]`. Out-of-range priority values fall back to `PriorityNormal`. Backpressure strategies (`BackpressureBlock`, `BackpressureDropOldest`, `BackpressureDropLatest`) apply per-queue.
- **Priority-ordered worker drain** — workers perform a non-blocking check through `priorityOrder` before blocking on a multi-channel select. When multiple queues have events, higher-priority events are served first (best-effort; Go scheduler is non-deterministic).
- **`drainAll()`** — helper draining all per-priority queues in priority order on stop signal. Called from worker on `stopCh` closure (replaces the previous inline drain loop).
- **`Graph.publishEvent` signature updated** — fourth parameter `priority EventPriority` added. All 11 internal call sites updated with appropriate priorities:
  - `EventNodeCreate` / `EventRelCreate` → `PriorityHigh`
  - `EventNodeDelete` / `EventRelDelete` → `PriorityCritical`
  - `EventNodeUpdate` / `EventRelUpdate` (all paths including in-place, RemoveLabel, CloseVersion) → `PriorityNormal`
- 4 new tests in `pkg/graph/async_eventbus_test.go`: `TestPriority_ZeroValueIsNormal`, `TestPriority_GraphDeleteIsCritical`, `TestPriority_GraphCreateIsHigh`, `TestPriority_CriticalBeforeNormal`.

## [3.0.50] - 2026-03-03

### Added (Async EventBus with Worker Pool + Backpressure — Phase 4.19)

- **`eventPublisher` interface** — unexported interface with a single `publish(Event)` method. Both `*EventBus` (sync) and `*AsyncEventBus` (async) implement it. `Graph.events` field changed from `*EventBus` to `eventPublisher`, enabling transparent substitution.
- **`AsyncEventBus`** — asynchronous event bus that decouples handler latency from graph write latency. Handlers are invoked in a bounded worker pool, not on the caller's goroutine. A slow handler no longer stalls mutations.
- **`NewAsyncEventBus(cfg AsyncEventBusConfig) *AsyncEventBus`** — creates and starts the bus. Workers begin consuming immediately. Defaults: `Workers=1`, `QueueSize=256`.
- **`AsyncEventBusConfig`** — configuration struct: `Workers int` (goroutine count), `QueueSize int` (channel buffer), `Backpressure BackpressureStrategy` (full-queue behavior).
- **`BackpressureStrategy`** — enum with three values:
  - `BackpressureBlock` — blocks the caller until queue space is available (zero event loss, max back-pressure to writers).
  - `BackpressureDropOldest` — evicts the oldest queued event and enqueues the new one (preserves newest events under load).
  - `BackpressureDropLatest` — discards the incoming event when the queue is full (zero blocking, may lose events).
- **`AsyncEventBus.Subscribe(h EventHandler) func()`** — registers a handler; returns an idempotent unsubscribe closure (B11, `sync.Once`). Safe for concurrent use.
- **`AsyncEventBus.Close()`** — signals workers to stop, drains all pending queue entries before returning, then waits for all workers to exit. Safe to call multiple times (B11, `sync.Once`). Guarantees at-most-once delivery of all events enqueued before `Close()`.
- **`Graph.SetAsyncEventBus(bus *AsyncEventBus)`** — attaches an `AsyncEventBus`. Nil-safe (typed-nil guard prevents interface-wrapping a nil pointer from defeating the `g.events == nil` check in `publishEvent`).
- **`Graph.SetEventBus(bus *EventBus)`** — updated with explicit nil-guard (same typed-nil safety fix).
- **`Graph.GetEventBus() *EventBus`** — updated to type-assert against `eventPublisher` interface; returns `nil` when an `AsyncEventBus` is attached.
- **`dispatch` (internal)** — copies handler slice under `RLock` before invoking (B15 copy-outside-lock pattern). Uses `safeInvoke` — panics inside handlers are recovered and logged, never crashing the worker goroutine.
- 8 new tests in `pkg/graph/async_eventbus_test.go`: `TestAsyncEventBus_HandlerReceivesEvent`, `TestAsyncEventBus_SlowHandlerDoesNotBlockPublish`, `TestAsyncEventBus_BackpressureBlock`, `TestAsyncEventBus_BackpressureDropOldest`, `TestAsyncEventBus_BackpressureDropLatest`, `TestAsyncEventBus_Close_DrainsQueue`, `TestAsyncEventBus_MultipleWorkers`, `TestSetAsyncEventBus_GraphIntegration`.

## [3.0.49] - 2026-03-03

### Added (Transaction Time / Bitemporality — Phase 4.18)

- **`TemporalMetadata.TxFrom Instant`** is now populated on every write path. `AddNodeWithContext` and `AddRelationshipWithContext` set `TxFrom = nowInstant()` after hash computation (TxFrom/TxTo are NOT fed into `ComputeNodeHash`/`ComputeRelHash`). `UpdateNodeWithContext` and `UpdateRelationshipWithContext` set `TxFrom = now` on the new version (the same `now` used for `UpdatedAt`) and `TxTo = now` on the prevState deep-copy before it is written to history.
- **`TemporalMetadata.TxTo Instant`** is set to `now` on the tombstone created by `deleteNodeLocked` (node + connected rels) and `DeleteRelationshipWithContext`. Tombstones have both `TxFrom = now` and `TxTo = now` (committed and immediately superseded at the same instant, matching the deleted-entity contract).
- **`Graph.GetNodeAsOf(id, txTime)`** — returns the node version whose transaction time window covered `txTime`. Checks the current tip first (`TxFrom <= txTime && TxTo == 0`), then scans history for the highest-`TxFrom` version satisfying `TxFrom <= txTime && (TxTo == 0 || TxTo > txTime)`. Returns `ErrNoVersionAsOf` if no version was recorded at that time.
- **`Graph.GetRelAsOf(id, txTime)`** — mirrors `GetNodeAsOf` for relationships.
- **`Graph.GetNodesAsOf(txTime)`** — returns all nodes that existed in the graph at the given transaction time. Uses the two-phase ForEach pattern (B15-compliant): Phase 1 collects all current + history node IDs under store locks; Phase 2 calls `GetNodeAsOf` per ID with locks released. Skips `ErrNoVersionAsOf`.
- **`Graph.GetRelsAsOf(txTime)`** — mirrors `GetNodesAsOf` for relationships.
- **`ErrNoVersionAsOf`** — new sentinel: `"graph: no entity version recorded at the given transaction time"`.
- **`Config.SyncWrites bool`** — also now wired through to `BadgerStoreConfig.SyncWrites` in `New()` (pre-existing gap fixed alongside Phase 4.18).
- **Shadow resolver tests updated** — `TestResolveNodePropertyNilTemporal` and `TestResolveRelPropertyNilTemporal` now construct entities directly (bypassing `Add*`) to test the nil-temporal code path. `AddNode`/`AddRelationship` always set `TxFrom` after Phase 4.18, so the test must use manually constructed entities to exercise the nil branch.
- 8 new tests in `pkg/graph/txtime_test.go`: `TestTxFromSetOnAdd`, `TestTxToSetOnUpdate`, `TestTxToSetOnDelete`, `TestGetNodeAsOf_BeforeCreate`, `TestGetNodeAsOf_CurrentVersion`, `TestGetNodeAsOf_HistoricalVersion`, `TestGetNodesAsOf_FiltersCorrectly`, `TestGetRelAsOf`.

## [3.0.48] - 2026-03-03

### Added (Sync-Write Config Flag — Phase 4.17)

- **`BadgerStoreConfig.SyncWrites bool`** — when true, opens Badger with `WithSyncWrites(true)` (fsync-on-every-write at the disk level) and forces `FlushInterval=0` so the background flush goroutine is never started. Every mutating method calls `bs.flush()` immediately after releasing `idxMu`, persisting writes to stable storage before returning. Eliminates the 100ms async flush window at the cost of higher write latency. Ignored in ReadOnly mode.
- **`Config.SyncWrites bool`** — propagated to `BadgerStoreConfig.SyncWrites` when `New()` creates a `BadgerStore`. No-op when using `MemoryStore` or an explicitly injected `Store`.
- **`BadgerStore.syncWrites bool`** — unexported field on BadgerStore; checked after every `appendOps` call in all 16 mutating methods: `PutNode`, `DeleteNode`, `ReplaceNode`, `RemoveNodeLabelToken`, `PutRelationship`, `ReplaceRelationship`, `DeleteRelationship`, `ReplaceNodeWithHistory`, `ReplaceRelWithHistory`, `PutNodeVersion`, `PutRelVersion`, `DeleteNodeCascade`, `PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`.
- **Lock ordering preserved** — all `defer bs.idxMu.Unlock()` patterns in mutating methods were converted to explicit unlock before the sync flush call, maintaining the invariant: `idxMu` is released before `flush()` acquires `wbMu` then `idxMu.RLock()` internally (B15, lock-ordering rule).
- **B22 compliance** — flush path already guards with `bs.dbClosed.Load()`, so sync writes cannot hang on closed DB.
- 5 new tests in `pkg/graph/sync_write_test.go`: `TestSyncWrite_ConfigPassthrough`, `TestSyncWrite_DataSurvivesWithoutClose`, `TestSyncWrite_FlushIntervalIgnored_WhenSyncWrites`, `TestSyncWrite_ReadOnly_SyncWritesIgnored`, `TestSyncWrite_Graph_ConfigPassthrough`.

## [3.0.47] - 2026-03-03

### Added (AuthorizedBy + AuthorizationLevel — Phase 4.16)

- **`NodeIntegrity.AuthorizedBy string`** / **`NodeIntegrity.AuthorizationLevel uint8`** — optional caller-supplied authorization fields. Set by passing `"tkg_authorized_by"` (string) and `"tkg_auth_level"` (uint8 or int for JSON round-trip safety) in the `props`/`updates` map of any Add or Update call. Stripped before `PropertySlice.Set` (never stored in PropertySlice).
- **`RelIntegrity.AuthorizedBy string`** / **`RelIntegrity.AuthorizationLevel uint8`** — same pattern on relationship integrity.
- **`ShadowAuthorizedBy = "tkg_authorized_by"`** / **`ShadowAuthLevel = "tkg_auth_level"`** — new shadow constants in `pkg/types/shadow.go`. Both accessible via `ResolveNodeProperty` and `ResolveRelProperty`.
- **`extractProvenance(props)`** extended — now also extracts `tkg_authorized_by` and `tkg_auth_level` from any props/updates map. Zero-allocation fast path preserved (B23 compliant): no allocation when none of the 4 reserved keys are present. Accepts `int`, `int32`, `int64`, `float64` for `tkg_auth_level` for JSON round-trip compat.
- **Wire persistence** — `AuthorizedBy` (`msgpack:"aby"`) and `AuthorizationLevel` (`msgpack:"al"`) added to both `nodeWire` and `relWire`. Backward-compatible (`omitempty`).
- **Layout test updated** — `NodeIntegrity` size: 72 → 96 bytes; `RelIntegrity` size: 104 → 128 bytes.
- 10 new tests in `pkg/graph/integrity_authz_test.go`: `TestAuthorizedBySetOnAdd_Node`, `TestAuthorizedBySetOnAdd_Rel`, `TestAuthLevelSetOnAdd_Node`, `TestAuthLevelSetOnAdd_Rel`, `TestAuthLevelAcceptsInt_Node`, `TestAuthzPreservedOnUpdate`, `TestAuthzViaShadow_Node`, `TestAuthzViaShadow_Rel`, `TestAuthLevelViaShadow_Node`, `TestNoAuthz_DefaultsZero`.

## [3.0.46] - 2026-03-03

### Added (Portable Export/Import — Phase 4.15)

- **`Graph.ExportGraph(w io.Writer) error`** — writes a portable format-independent snapshot of the entire graph to `w`. Snapshot includes: header, label/reltype registries, all current nodes and rels, and their full version history. Holds `g.mu.RLock` for the duration (consistent snapshot).
- **`Graph.ImportGraph(r io.Reader) error`** — reads an export stream and restores it into the graph. Registries are imported if empty; if already populated, the existing registry is kept (idempotent). Holds `g.mu.Lock` for the duration (serialised restore).
- **Wire format** — length-prefixed msgpack record stream with 1-byte type tags: `0x01` header, `0x02` registry, `0x03` node, `0x04` node history, `0x05` rel, `0x06` rel history. Each record is `[tag(1)] [len(4BE)] [msgpack body]`. Forward-compatible: unknown tags are skipped on import.
- **`ErrIncompatibleExport`** — returned when the export stream version is not supported by this binary.
- **Two-phase ForEach pattern (C4)** — collect IDs in ForEachNodeID/ForEachRelID/ForEachNodeHistoryID/ForEachRelHistoryID callbacks (store lock held); fetch entities after callback returns (lock released). OOM-safe on large graphs.
- 12 new tests in `pkg/graph/export_test.go`: `TestExportImport_RoundTrip_MemoryStore`, `TestExportImport_RoundTrip_BadgerStore`, `TestExport_Empty_Graph`, `TestExport_WithNodeHistory`, `TestExport_RelHistory`, `TestImport_IdempotentRegistry`, `TestExport_Writer_Error`, `TestImport_InvalidHeader`, `TestExportImport_IntegrityPreserved`, `TestExportImport_EndpointHashesPreserved`, `TestExportImport_AuthorIDPreserved`, `TestExport_ShadowProperty_Survives`.

## [3.0.45] - 2026-03-03

### Added (AuthorID + Signature on Integrity — Phase 4.14)

- **`NodeIntegrity.AuthorID string`** / **`NodeIntegrity.Signature []byte`** — caller-supplied provenance fields. Set by passing `"tkg_author_id"` (string) and `"tkg_signature"` ([]byte) in the `props`/`updates` map of any Add or Update call. Stripped before `PropertySlice.Set` (never stored in PropertySlice).
- **`RelIntegrity.AuthorID string`** / **`RelIntegrity.Signature []byte`** — same pattern on relationship integrity.
- **`ShadowAuthorID = "tkg_author_id"`** / **`ShadowSignature = "tkg_signature"`** — new shadow constants in `pkg/types/shadow.go`. Both accessible via `ResolveNodeProperty` and `ResolveRelProperty`.
- **`extractProvenance(props)`** (unexported) — helper in `context.go` that extracts `tkg_author_id` and `tkg_signature` from any props/updates map without mutating the caller's map. Zero-allocation fast path when neither key is present.
- **Wire persistence** — `AuthorID` (`msgpack:"aid"`) and `Signature` (`msgpack:"sig"`) added to both `nodeWire` and `relWire`. Backward-compatible (`omitempty`); old data reads as zero values.
- **Layout test updated** — `NodeIntegrity` size: 32 → 72 bytes; `RelIntegrity` size: 32 → 104 bytes.
- 11 new tests in `pkg/graph/integrity_author_test.go`: SetOnAdd (node + rel), SignatureSetOnAdd (node + rel), PreservedOnUpdate, ViaShadow (node + rel, both fields), DefaultsEmpty, DoesNotAffectHash.

## [3.0.44] - 2026-03-03

### Added (RelIntegrity Endpoint Hashes — Phase 4.13)

- **`RelIntegrity.FromNodeHash string`** — hash of the start node at the time this relationship version was written. NOT fed into `ComputeRelHash` (prevents cascading hash invalidation on node updates). Used for cross-validation.
- **`RelIntegrity.ToNodeHash string`** — hash of the end node at write time.
- **`ShadowFromHash = "tkg_from_hash"`** / **`ShadowToHash = "tkg_to_hash"`** — new shadow constants. Accessible via `ResolveRelProperty`; return `(nil, false)` on nodes (rel-only).
- **`AddRelationshipWithContext`** — captures `startNode.Integrity().Hash` → `ig.FromNodeHash` and `endNode.Integrity().Hash` → `ig.ToNodeHash` under the endpoint lock. Empty string if endpoint has no integrity.
- **`UpdateRelationshipWithContext`** — refreshes `FromNodeHash`/`ToNodeHash` from the store on each update, capturing the current endpoint hashes at write time.
- **Wire persistence** — `FromNodeHash` (`msgpack:"fnh"`) and `ToNodeHash` (`msgpack:"tnh"`) added to `relWire`. Backward-compatible (`omitempty`).
- 5 new tests in `pkg/graph/rel_endpoint_hash_test.go`: `TestFromNodeHashStoredOnAdd`, `TestEndpointHashFromShadow`, `TestEndpointHashPreservedOnUpdate`, `TestEndpointHashSelfLoop`, `TestEndpointHashNotOnNode`.

## [3.0.43] - 2026-03-02

### Added (VectorField Index — Phase 4.9)

- **`DistanceMetric uint8`** — `DistanceCosine` and `DistanceEuclidean` constants.
- **`vectorIndex`** (unexported) — in-memory brute-force k-NN index: `add`, `remove`, `searchNearest`. O(n × dims) per query. Thread-safe via `sync.RWMutex`. `add` replaces existing entry for same ID (upsert).
- **`[]float32` property support** — added to `wire.go` (`ptSliceF32 = 24`), `integrity.go` (hash computation), and `propertyslice.go` (deep copy). `[]float32` values are now fully round-trip serializable.
- **`Store.CreateVectorIndex(labelToken, propertyKey, dims, metric)`** — creates in-memory k-NN index on nodes with the given label. Scans existing nodes to populate. Returns `ErrVectorIndexExists` on duplicate. Implemented in MemoryStore, BadgerStore, TieredStore.
- **`Store.DropVectorIndex(labelToken, propertyKey)`** — removes the index. Returns `ErrVectorIndexNotFound` if not present.
- **`Store.SearchNearestNodes(labelToken, propertyKey, query, k, opts)`** — returns the k closest nodes by vector distance in ranked order. Returns `ErrVectorIndexNotFound` / `ErrDimensionMismatch` on error; nil slice (no error) if index is empty.
- **`Graph.CreateVectorIndex(label, propertyKey, dims, metric)`** / **`DropVectorIndex`** / **`SearchNearestNodes`** — Graph-layer API resolving label string to token; returns nil for unregistered labels.
- **Auto-maintenance** — all mutation paths (PutNode, ReplaceNode, DeleteNode, RemoveNodeLabelToken) update vector indexes in MemoryStore, BadgerStore, and TieredStore.
- **`ErrVectorIndexExists`** / **`ErrVectorIndexNotFound`** / **`ErrDimensionMismatch`** — new sentinel errors in `graph` package.
- **TieredStore** holds vector indexes at the store level (not per-shard) with its own `vectorIdxMu sync.RWMutex`.
- **Not persisted** — vector indexes are rebuilt from node properties after restart (documented limitation).
- Internal-package tests in `vector_badger_test.go` covering BadgerStore and TieredStore implementations directly (12 tests). External-package tests in `vector_index_test.go` (12 tests).

## [3.0.42] - 2026-03-02

### Added (Recurrence Patterns — Phase 4.7)

- **`RecurrenceFrequency uint8`** — `RecurrenceDaily`, `RecurrenceWeekly`, `RecurrenceMonthly`, `RecurrenceYearly`.
- **`WeekdayMask uint8`** — bit-per-weekday bitmask: `MaskMonday` (bit 0) through `MaskSunday` (bit 6), plus `MaskWeekdays`, `MaskWeekend`, `MaskAllDays` composites.
- **`Interval`** — `{Start, End Instant}` — closed-open `[Start, End)` temporal interval.
- **`RecurrencePattern`** — struct with `Frequency`, `Days` (WeekdayMask), `DayOfMonth` (1–28; 0 = last day of month), `Month` (time.Month for Yearly), `DayStart`/`DayEnd` (time.Duration from UTC midnight).
- **`RecurrencePattern.Validate()`** — validates frequency, non-empty Days for Daily/Weekly, DayStart < DayEnd, DayOfMonth ∈ [0, 28].
- **`RecurrencePattern.Expand(from, to)`** — walks days from `TruncateInstant(from, GranDay)` to `TruncateInstant(to, GranDay)`, checks day-of-week / day-of-month / month match, emits `[day+DayStart, day+DayEnd)` clipped to `[from, to)`. All calculations UTC. Returns `ErrInvalidTimeRange` if `from >= to`.
- 8 new tests in `pkg/types/recurrence_test.go`: Daily_Weekdays, Weekly_Monday, Monthly_NthDay, Yearly, Clipped, EmptyResult, Validate_Errors (5 sub-cases), Expand_InvalidRange.

## [3.0.41] - 2026-03-02

### Added (Remove Label from Node — Phase 4.10)

- **`Node.RemoveLabelTokenRaw(tok uint16) bool`** — removes a label token from the node's label set. If `tok` is the primary label, promotes `extraLabels[0]` to primary. Returns false if `tok == 0` or not present. Caller must ensure `LabelTokenCount() > 1`.
- **`Store.RemoveNodeLabelToken(id, tok, updatedNode)`** — removes `tok` from the label index for `id` and persists `updatedNode` (no version bump, no history entry). Implemented in MemoryStore, BadgerStore, and TieredStore.
- **`Graph.RemoveNodeLabel(id snowflake.ID, label string)`** — resolves label string to token, locks the entity, validates the node has the label and has more than one label, deep-copies + mutates, recomputes hash (preserving PrevHash), delegates to `store.RemoveNodeLabelToken`, publishes `EventNodeUpdate`, increments `opNodeUpdates`.
- **`ErrLabelNotFound`** — new sentinel: `"graph: node does not have the specified label"`.
- **`ErrLastLabel`** — new sentinel: `"graph: cannot remove the last label from a node"`.
- 8 new tests in `pkg/graph/remove_label_test.go`: ExtraLabel, PrimaryPromotesExtra, LastLabelError, LabelNotFoundError, NodeNotFoundError, HashUpdated, NodesByLabelUpdated, PublishesEvent.
- Internal-package tests in `vector_badger_test.go` covering BadgerStore and TieredStore `RemoveNodeLabelToken` directly.

## [3.0.40] - 2026-03-02

### Added (Time Granularity + In-Place Update + Graph Stats — Phases 4.8, 4.11, 4.12)

**Time Granularity (4.8)**
- **`TimeGranularity uint8`** — 8 levels: `GranMillisecond` (1) through `GranYear` (8).
- **`TruncateInstant(t, g)`** — floors `t` to the nearest `g` boundary (UTC). Week truncation floors to Monday midnight.
- **`RoundInstant(t, g)`** — rounds to nearest boundary (ties ceil).
- **`CeilInstant(t, g)`** — smallest boundary ≥ `t`.
- Table-driven tests in `pkg/types/granularity_test.go` covering all 3 functions × 8 granularities, plus on-boundary and week-day edge cases.

**In-Place Update (4.11)**
- **`Graph.UpdateNodeInPlace(id, updates)`** / **`UpdateNodeInPlaceWithContext`** — updates node properties without bumping the version or writing a history entry. Uses `store.ReplaceNode` (not `ReplaceNodeWithHistory`). Preserves existing `PrevHash`. Publishes `EventNodeUpdate`. Increments `opNodeUpdates`.
- **`Graph.UpdateRelInPlace(id, updates)`** / **`UpdateRelInPlaceWithContext`** — rel mirror.
- 12 new tests in `pkg/graph/inplace_test.go`: NoHistoryEntry, VersionUnchanged, PropertiesUpdated, NoOp, PublishesEvent (× Node/Rel), WithContext_Cancelled, CountedAsUpdate.

**Graph Stats (4.12)**
- **`GraphStats`** — struct with 8 operation counters (`NodesAdded`, `NodesRead`, `NodesUpdated`, `NodesDeleted`, `RelsAdded`, `RelsRead`, `RelsUpdated`, `RelsDeleted`) and 4 cache metrics (`NodeCacheHits`, `NodeCacheMisses`, `RelCacheHits`, `RelCacheMisses`).
- **`StoreStats`** (unexported interface) — optional interface type-asserted in `Graph.Stats()`. Avoids polluting the `Store` interface. `BadgerStore` implements it.
- **8 `atomic.Int64` fields** on `Graph` struct: `opNode{Adds,Reads,Updates,Deletes}` + rel mirrors. Incremented after every successful store write in `context.go`.
- **LRU hit/miss tracking** — `entityLRU` gains `hits` and `misses atomic.Int64`. `Get()` increments on cacheMiss (miss) and on cacheHit/cacheDeleted (hit). `Hits()`/`Misses()` accessors.
- **`BadgerStore` implements `StoreStats`** via `nodeCache.Hits()`/`Misses()` + rel mirrors.
- 8 new tests in `pkg/graph/stats_test.go`: InitialState, NodeCounters, RelCounters, EmptyUpdate_NoUpdateIncrement, CacheMetrics_MemoryStore_Zero, CacheMetrics_BadgerStore, UpdateNodeInPlace_CountsAsUpdate, UpdateRelInPlace_CountsAsUpdate.

## [3.0.39] - 2026-03-02

### Added (CRUD Diff Exporter — Phase 4.6)

- **`NodeUpdate`** / **`RelUpdate`** — pair structs holding `Before` and `After` snapshots of a changed entity.
- **`SnapshotDiff`** — result type with `T1`, `T2`, `NodesCreated`, `NodesUpdated`, `NodesDeleted`, `RelsCreated`, `RelsUpdated`, `RelsDeleted`.
- **`Graph.DiffSnapshots(t1, t2)`** — compares two temporal snapshots under a single `g.mu.RLock` (prevents torn reads). Returns `*SnapshotDiff` classifying each entity as Created (present only at t2), Deleted (present only at t1), or Updated (hash changed). Unchanged entities are omitted. Returns `ErrInvalidTimeRange` if `t1 >= t2` or either is zero.
- **`ErrInvalidTimeRange`** — new sentinel error in `graph` package.
- **`snapshotLocked`** (unexported) — inner body of `Snapshot(t)` extracted without the lock, allowing `DiffSnapshots` to hold the RLock across both snapshot reads (B15 compliance: no nested RLock).
- 15 new tests in `pkg/graph/diff_test.go`: invalid range, empty graph, created/deleted/updated/unchanged for nodes and rels, mixed scenario, nil integrity branches.

## [3.0.38] - 2026-03-02

### Added (Event / Notification System — Phase 4.5)

- **`EventType uint8`** — 6 constants: `EventNodeCreate`, `EventNodeUpdate`, `EventNodeDelete`, `EventRelCreate`, `EventRelUpdate`, `EventRelDelete`.
- **`Event`** — struct with `Type EventType`, `EntityID snowflake.ID`, `Timestamp types.Instant`.
- **`EventHandler`** — type alias for `func(Event)`.
- **`EventBus`** — dispatcher with `Subscribe(handler) func()` (returns idempotent unsubscribe via `sync.Once`) and unexported `publish(e Event)`. Handlers are copied under `RLock`, then invoked outside the lock to prevent deadlocks when handlers re-enter the Graph.
- **`NewEventBus()`** — constructor.
- **`Graph.SetEventBus(bus)`** / **`Graph.GetEventBus()`** — attach/retrieve the event bus. Nil by default (zero overhead for callers not using events).
- 6 hook points wired in `context.go` after each successful store write: `AddNode`→`EventNodeCreate`, `AddRelationship`→`EventRelCreate`, `UpdateNode`→`EventNodeUpdate`, `UpdateRelationship`→`EventRelUpdate`, `DeleteNode`→`EventNodeDelete`, `DeleteRelationship`→`EventRelDelete`.
- `CloseNodeVersion` / `CloseRelVersion` also publish `EventNodeUpdate` / `EventRelUpdate`.
- 13 new tests in `pkg/graph/events_test.go`: subscribe/unsubscribe, idempotent unsubscribe, multiple handlers, nil-default graph, all 6 CRUD event types, async handler, CloseNodeVersion/CloseRelVersion events.

## [3.0.37] - 2026-03-02

### Added (Version Chain Navigation — Phase 4.4)

- **`Graph.GetPreviousNodeVersion(id, version)`** — returns the version immediately before `version`. Returns `nil, nil` if `version == 0` (genesis has no predecessor) or the predecessor does not exist in history.
- **`Graph.GetNextNodeVersion(id, version)`** — returns the version immediately after `version`. Checks the history store first for `version+1`, then falls back to the current entity (which may itself be `version+1`). Returns `nil, nil` if no newer version exists (current tip or deleted node with a version gap).
- **`Graph.CloseNodeVersion(id, t)`** — sets `ValidTo = t` on the current node in-place via `ReplaceNode` (no new version, no history entry). Recomputes the integrity hash preserving `PrevHash`. Returns `ErrAlreadyClosed` if `ValidTo` is already non-zero; returns `ErrNodeNotFound` if the node does not exist. Updates temporal indexes via `ReplaceNode`.
- **`GetPreviousRelVersion`** / **`GetNextRelVersion`** / **`CloseRelVersion`** — exact mirrors of the node methods for relationships.
- **`ErrAlreadyClosed`** — new sentinel error in `graph` package.
- 18 new tests in `pkg/graph/version_chain_test.go`: genesis/tip boundaries, normal prev/next traversal, through-history path, deleted node/rel edge cases, version gap after truncation, CloseNodeVersion sets ValidTo, ErrAlreadyClosed on second close, ErrNodeNotFound on missing entity, rel mirrors.

## [3.0.36] - 2026-03-02

### Added (Temporal Constraints + Advanced Temporal Indexes — Phases 4.2 + 4.3)

**Temporal Constraints (4.2)**

- **`TemporalConstraintKind`** — enum type for constraint kinds. Initial kind: `ConstraintRelWithinEndpoints`.
- **`TemporalConstraint`** — struct binding a `TemporalConstraintKind` to optional parameters.
- **`ConstraintSet`** — value type (zero value = no constraints). Holds a slice of `TemporalConstraint`. Passed via `Graph.Config`; zero overhead when unused.
- **`ConstraintRelWithinEndpoints`** — enforces that a relationship's validity interval (`[ValidFrom, ValidTo)`) is a subset of the intersection of both endpoint nodes' validity intervals. Evaluated in `AddRelationshipWithContext` and `ImportRelationshipWithID` before the store write.
- **`ErrTemporalConstraint`** — base sentinel error; 6 specific leaf errors wrap it so callers can use `errors.Is` on either the outer or the specific leaf: `ErrConstraintStartNodeOpen`, `ErrConstraintEndNodeOpen`, `ErrConstraintRelBeforeStart`, `ErrConstraintRelAfterEnd`, `ErrConstraintRelStartTooEarly`, `ErrConstraintRelEndTooLate`.
- 13 new tests in `pkg/graph/temporal_constraint_test.go`: no-op on zero ConstraintSet, valid rel within both endpoints, rel starts before node, rel ends after node, open-ended node (no ValidTo), import path enforcement, errors.Is wrapping for each leaf error, interaction with existing temporal filters.

**Advanced Temporal Indexes (4.3)**

- **`temporalIndex`** (unexported) — sorted-slice interval index on `[ValidFrom, ValidTo)`. Binary search insertion (O(log n)); point-in-time and interval queries are O(n) scan with early exit. Thread-safe via `sync.RWMutex`. Stored per label token on the Store.
- **`Store.CreateTemporalIndex(labelToken)`** — installs a temporal index for the given label; scans existing nodes to populate. Returns `ErrTemporalIndexExists` on duplicate. Implemented in MemoryStore, BadgerStore, and TieredStore.
- **`Store.DropTemporalIndex(labelToken)`** — removes the temporal index. Returns `ErrTemporalIndexNotFound` if not present.
- **`Graph.CreateTemporalIndex(label)`** / **`Graph.DropTemporalIndex(label)`** — Graph-layer API resolving label string to token before delegation.
- **`NodesByLabel` temporal fast path** — when a temporal index is active for the queried label and `QueryOpts` carries a `ValidAt` or `ValidStart`/`ValidEnd` filter, `NodesByLabel` uses the index to narrow candidate IDs before fetching entities, avoiding a full label scan.
- **BadgerStore persistence** — temporal index label tokens are persisted to Badger under a meta key (same 3-phase creation pattern as property indexes). On startup `loadIndexes()` reads persisted token set and rebuilds index data by scanning matching nodes; indexes survive restart.
- **TieredStore delegation** — `CreateTemporalIndex`/`DropTemporalIndex` delegate to all currently open shards. `tempIndexLabels` set on `TieredStore` ensures each new hot shard created during rotation inherits all active temporal indexes immediately.
- **MemoryStore** — full integration: index created/dropped/maintained across all node mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`).
- **`ErrTemporalIndexExists`** / **`ErrTemporalIndexNotFound`** — new sentinel errors in `graph` package.
- 11 new unit tests in `pkg/graph/temporal_index_test.go` + helper tests: create/duplicate/drop/not-found, NodesByLabel fast path (point-in-time, interval), BadgerStore persistence round-trip, TieredStore delegation, new hot shard inherits index on rotation, MemoryStore mutation maintenance.

### Fixed

- **B22: Badger `WriteBatch.Flush` blocks on closed DB** — `flush()` in `badgerstore.go` now checks `bs.closed` (atomic flag) before calling `WriteBatch.Flush()`. Previously, a flush triggered after `Close()` would block indefinitely because Badger's `WriteBatch.Flush` waits on an internal channel that is never drained once the DB is closed. Fix: early-return with `ErrDBClosed` when the closed flag is set; re-queue is skipped. `flushLoop` goroutine exit path updated accordingly.

## [3.0.35] - 2026-03-02

### Fixed (Carry-Forward Coverage Gaps)

- **`ImportNames` validation** (`label_registry.go`, `reltype_registry.go`) — added rejection of whitespace-only and empty strings at positions > 0, and duplicate names. Uses `strings.TrimSpace` to match the invariant enforced by `GetOrCreate`. Previously, importing `["", "Foo", "Foo"]` silently corrupted the reverse map; importing `["", " ", "Foo"]` mapped token 1 to a whitespace-only string that `GetOrCreate` would refuse to produce, creating an inconsistent registry.
- **`propertyTypeTag` coverage** — direct unit tests covering all 24 branches: `int8`, `int16`, `int32`, `uint`, `uint8`–`uint32`, `uint64`, `float32`, and the `default` unknown-type branch (64% → 100%).
- **`toInt64` / `toUint64` coverage** — direct unit tests covering all 9 type cases plus the `default` zero-return branch each (54.5% → 100%).
- **`normalizeIntegersRecursive` coverage** — direct unit tests for `int8`, `int16`, `int32`, `uint8`, `uint16`, `uint32`, `[]any` recursion, `map[string]any` recursion, and the `default` passthrough (40% → 100%).
- **`flush()` WriteBatch error path** (`badgerstore.go`) — test closes the underlying Badger DB before `WriteBatch.Flush()`, exercising the requeue-on-error path with correct goroutine lifecycle via `t.Cleanup` (73% → covered).

## [3.0.34] - 2026-03-02

### Added (Allen's 13 Interval Relations — Gap A1)

- **`types.AllenRelation`** — `uint8` iota (1..13) representing Allen's 13 interval relations: Before, After, Meets, MetBy, Overlaps, OverlappedBy, Starts, StartedBy, During, Contains, Finishes, FinishedBy, Equals. Zero value is invalid (catches uninitialized usage). Methods: `String()`, `Symbol()`, `Inverse()`, `Set()`.
- **`types.AllenRelationSet`** — compact `uint16` bitset for sets of relations. Methods: `Contains`, `Add`, `Union`, `Intersection`, `IsEmpty`, `Len`, `ToSlice`, `String`, `InverseSet`. `AllRelations()` returns the full 13-element set.
- **`types.Relate(aStart, aEnd, bStart, bEnd)`** — classifies the Allen relation between two `[start, end)` intervals. Returns `ErrOpenInterval` for zero endpoints, `ErrInvalidInterval` for start >= end.
- **`types.Compose(r1, r2)`** / **`types.ComposeSets(a, b)`** — composition table (computed at `init()` via exhaustive enumeration of 21 intervals over a 7-point timeline). Returns the set of possible relations when chaining two known relations.
- **`graph.NodeInterval(n)`** / **`graph.RelInterval(r)`** — extracts effective `[start, end)` from a node/relationship, deriving start from snowflake ID timestamp when no explicit `ValidFrom` is set. Returns `ErrOpenInterval` if `ValidTo == 0`.
- **`graph.RelateNodes(a, b)`** / **`graph.RelateRels(a, b)`** — classifies Allen relation between two entities via their effective intervals.
- **`ErrOpenInterval`** / **`ErrInvalidInterval`** — new sentinel errors in `pkg/types`.
- 38 new tests in `pkg/types/allen_test.go`: error cases, all 13 relations, inverse symmetry, string/symbol, set operations, composition identity/known results/never-empty/singleton consistency.
- 10 new tests in `pkg/graph/temporal_allen_test.go`: open-ended error propagation, resolved intervals, snowflake-derived start, all 13 relations through Graph, inverse consistency, relationship intervals.

### Notes

- Purely additive — no existing files modified, no existing tests affected.
- Foundation for temporal constraints (A2) and advanced temporal indexes (A7).

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
