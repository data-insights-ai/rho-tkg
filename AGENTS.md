# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Session Protocol

- **Session start**: Read `tasks/lessons.md` and `CHANGELOG.md` (source of truth for version history) before doing any work
- **Before planning**: Read the full API of ALL files involved in the change — not snippets around suspected bug locations. Understanding the complete API prevents plans based on wrong assumptions
- **Challenge the plan**: Before implementing any method, ask: "Does this algorithm deliver what the method name promises?" and "Does this interact with an existing feature that could break it?"
- **After each implementation step**: Update all affected documentation (AGENTS.md, README.md, CHANGELOG.md, docs/architecture.md, docs/SPEC.md). If not specified whether changes belong to the current version or a new version in CHANGELOG.md, ask the user before writing
- **After corrections**: Update `tasks/lessons.md` with the pattern and a rule to prevent recurrence
- **Session end**: Update `tasks/lessons.md` with new lessons

## Project Overview

**Temporal Knowledge Graph v3** — an internal Go library providing the core graph engine for temporal knowledge graphs. Pure library (no main binary, no HTTP server, no query language).

Module: `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3`
Go: 1.26.0 | License: Apache-2.0
Dependencies: `rho-snowflake-2026` (IDs), `msgpack/v5` (serialization), `badger/v4` (persistence)
Status: v3.0.62 | Phases: 1a-1g, 2a-2i, 3a-3e, 4.1-4.23 (complete). v3.0.62: batch lock-leak fix, negative config validation, fractional auth level rejection, atomic RemoveNodeLabelTokenWithHistory, store API test coverage.

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

1. **Every public method gets a direct test.** Indirect coverage via delegation does NOT count. Any public method at 0% is a blocker.
2. **Node and Relationship must have test parity.** These types are structural mirrors.
3. **Every type-switch branch gets its own test.** If a branch shows 0% in `cover`, add a test.
4. **Sentinel errors are tested with `errors.Is`, not just `err != nil`.** At every call layer.
5. **Fallback/reflect paths must be tested or removed.**
6. **Deep copy means deep copy.** Must truly clone all nested reference types.
7. **Run `make cover` before marking any step complete.** Any public method at 0% or new code below 80% is a blocker.
8. **Validation must be recursive/adversarial.** `[]any{&myStruct{}}` must be rejected.
9. **Test nil values in reflect-based code.** `reflect.ValueOf(nil)` returns zero `reflect.Value`. `SetMapIndex` with zero Value deletes the key.
10. **One-time warnings must use `sync.Once`.** Never use `>=` for one-shot warnings.
11. **No empty stubs when the spec defines the fields.**
12. **Public method return types must not leak dependencies.** Use `type nodeID snowflake.ID`, NOT `type nodeID int64`.
13. **Config fields must be used or removed.**
14. **DO NOT use sub-millisecond or millisecond `ShardWindow` in tests.** Snowflake IDs encode time at ms resolution. Use 1-week window (`newTestTieredStore`) and test cold/warm via manual rotation + `demoteToCold` helper.

## Architecture

### `pkg/types`

| File | Purpose |
|---|---|
| `node.go` | Node (graph vertex, 80B) — `nodeID` wrapping `snowflake.ID`, labels as `labelToken`, properties, version, temporal, integrity |
| `relationship.go` | Relationship (directed edge, 72B) — `relID`, `relTypeToken`, start/end as `nodeID`, properties, version, temporal, integrity |
| `propertyslice.go` | Sorted key-value store with binary search; recursive allowlist validation; depth-limited to 32 levels; `[]float32` support |
| `shadow.go` | Constants for virtual read-only `tkg_*` properties |
| `temporal.go` | `Instant` type (Unix ms), `entityID`, `TemporalMetadata` struct |
| `integrity.go` | `NodeIntegrity` / `RelIntegrity` — hash chain (`Hash`, `PrevHash`) |
| `allen.go` | Allen's 13 interval relations — `AllenRelation`, `AllenRelationSet`, `Relate()`, `Compose()`, `ComposeSets()`, composition table |
| `granularity.go` | `TimeGranularity` (8 levels), `TruncateInstant`, `RoundInstant`, `CeilInstant` — ISO 8601 week truncation |
| `recurrence.go` | `RecurrencePattern`, `RecurrenceFrequency`, `WeekdayMask`, `Interval` — `Validate()` + `Expand(from, to)` |

### `pkg/graph`

| File | Purpose |
|---|---|
| `graph.go` | Graph struct, Config, dual snowflake generators, registries, entity locks, `ValidationLimits`, CRUD operations (exported wrappers acquire `g.mu.RLock`), `txEventBuffer` for tx event buffering, string resolution, `Close()` lifecycle, `ErrNotTieredStore` sentinel |
| `store.go` | `Store` interface (persistence contract), `QueryOpts`, `ShardDepth`, sentinel errors, `RelTombstone` struct, `DeleteNodeWithHistory`/`DeleteRelWithHistory` atomic compound delete methods |
| `memorystore.go` | Thread-safe in-memory Store with hash-set indexes, O(1) counts, temporal push-down |
| `badgerstore.go` | Persistent Store — Badger v4, LRU caches with dirty tracking, async WriteBatch flush, background GC |
| `lru.go` | Generic LRU cache with dirty tracking, tombstones, soft capacity, `Peek()` for zero-alloc lookups |
| `entity_locks.go` | 256-shard mutex array for write-skew prevention; `LockTwo`/`LockMany` in ascending order |
| `keys.go` | Binary key encoding — single-byte prefix tags, big-endian IDs |
| `integrity.go` | SHA-256 content hashing, hash chain verification |
| `wire.go` | Msgpack wire format types for serialization boundary |
| `shadow.go` | `ResolveNodeProperty` / `ResolveRelProperty` — dispatches 21 `tkg_*` shadow keys |
| `label_registry.go` | Thread-safe label string <-> uint16 token registry |
| `reltype_registry.go` | Thread-safe relationship type string <-> uint16 token registry |
| `batch.go` | `BatchBuilder` — fluent API with eager validation and deferred persistence |
| `context.go` | `*WithContext` exported wrappers (acquire `g.mu.RLock`) + `*Internal` unexported implementations (lock-free), `ValidationLimits` enforcement (incl. `ErrSelfLoop` check), two-phase delete with TOCTOU retry; `deleteNodeLocked` and `deleteRelationshipInternal` use atomic store calls (`DeleteNodeWithHistory`/`DeleteRelWithHistory`) |
| `export.go` | `ExportGraph`/`ImportGraph` — length-prefixed msgpack record stream with 1-byte type tags; format-versioned, forward-compatible; `ErrIncompatibleExport`, `ErrIncompatibleRegistry` |
| `temporal.go` | `GraphSnapshot`, temporal queries, history-aware ID merging via ForEach iterators |
| `temporal_filter.go` | Store-level temporal push-down helpers (`entityValidFrom`, `matchesTemporalFilter`) |
| `temporal_allen.go` | Allen's interval algebra graph integration — `NodeInterval`, `RelInterval`, `RelateNodes`, `RelateRels` |
| `temporal_constraint.go` | Temporal constraint types — `TemporalConstraintKind`, `TemporalConstraint`, `ConstraintSet`, 6 sentinel errors, `checkTemporalConstraints` enforcement |
| `temporal_index.go` | In-memory interval index — `temporalIndex` (sorted slice, lazy sort via `sortIfDirty` + `sortMu sync.Mutex`), `addNodeToTemporalIndexes`, `removeNodeFromTemporalIndexes`, `purgeNodeFromAllTemporalIndexes`, `nodeTemporalBounds` |
| `tx.go` | `GraphTx` — full CRUD transaction holding graph write lock, snapshot-based rollback |
| `property_index.go` | In-memory property indexes with auto-maintenance across all mutation paths |
| `pagination.go` | Cursor-based pagination via binary search on sorted ID slices |
| `events.go` | `EventType` (6 constants), `Event` (with `Priority EventPriority`), `EventBus` (sync), `AsyncEventBus` (worker pool + `BackpressureStrategy`), `EventPriority` (5 levels), `eventPublisher` interface |
| `txtime.go` | Bitemporality — `GetNodeAsOf`, `GetRelAsOf`, `GetNodesAsOf`, `GetRelsAsOf`, `ErrNoVersionAsOf`; TxFrom/TxTo populated on all mutation paths |
| `hf_index.go` | High-frequency temporal index — time-bucketed `highFrequencyIndex`, O(1) amortized insertion, `CreateHighFrequencyIndex`/`DropHighFrequencyIndex` |
| `stats.go` | `GraphStats` (8 operation counters + 4 cache metrics), `StoreStats` optional interface |
| `vector_index.go` | In-memory brute-force k-NN `vectorIndex` — `DistanceMetric` (Cosine/Euclidean), `CreateVectorIndex`/`DropVectorIndex`/`SearchNearestNodes` |
| `ontology.go` | `EntityClass` — classifies labels as reference or event for shard routing |
| `shard_catalog.go` | JSON-persisted catalog of all shards with `sync.RWMutex`, atomic write via `atomicWriteFile` |
| `registry_file.go` | Flat msgpack registry file save/load with `atomicWriteFile` (fsync before rename) |
| `badgerstore_partial.go` | Unexported helpers for TieredStore cross-shard relationship routing |
| `tieredstore.go` | Routes entities across ref shard + time-windowed event shards; atomic `checkoutStore` for cold shards; `SaveRegistries` for atomic label+reltype persistence |
| `tieredstore_write.go` | TieredStore writes — cross-shard split-write, archive/restore, batch partitioning |
| `tieredstore_read.go` | TieredStore reads — ref probe + timestamp fallback, parallel merge queries, event label fan-out for NodesByLabel, sequential ForEach |
| `tieredstore_admin.go` | Admin API: `ForceRotate`, `ListShards`, `RebuildCatalog`, `VerifyShard` |
| `tieredstore_repair.go` | Cross-shard split-write consistency repair (orphaned/missing in/ entries) |
| `tieredstore_migrate.go` | `MigrateFromBadger` — copy from single BadgerStore to TieredStore |
| `id_decompose.go` | Extract creation time, node ID, sequence from snowflake ID bits |
| `doc.go` | Package documentation |

### Configuration

- **`Graph.Config`**: `SnowflakeNodeID` (0-511), `Store`, `BadgerDir`, `BadgerInMemory`, `Validation` (ValidationLimits). Whitespace-only `BadgerDir` rejected.
- **`ValidationLimits`**: `MaxLabelsPerNode` (50), `MaxPropertiesPerEntity` (1000), `MaxPropertyKeyLength` (256), `MaxPropertyValueSize` (64K strings), `MaxNameLength` (256). `AllowSelfLoops` (default `false` — reject self-loop relationships where start == end; set `true` to permit). Zero = default for numeric limits.
- **`BadgerStoreConfig`**: `Dir`, `InMemory`, `Logger` (Badger logger, nil uses default), `CacheCapacity` (10K), `FlushInterval` (100ms), `GCInterval` (5min), `GCDiscardRatio` (0.5), `ReadOnly` (for warm/cold shards), `SyncWrites` (fsync after every write — disables async buffer, forces FlushInterval=0).
- **`Graph.Config`**: also accepts `SyncWrites bool` which passes through to `BadgerStoreConfig`.
- **`TieredStoreConfig`**: `DataDir`, `InMemory`, `RefLabels`, `ShardWindow` (1 week), `CacheCapacity` (10K), `FlushInterval` (100ms), `ColdAfter` (0=never), `IdleTimeout` (5min when cold enabled).

## Design Rules

### Data Model

- **Pure-data structs**: Node/Relationship never hold references to Graph, registries, or resolvers. String resolution is always the Graph layer's responsibility.
- **snowflake.ID everywhere**: All IDs are `snowflake.ID` wrapped in opaque types (`nodeID`, `relID`, `entityID`). Never use `int64` or `string` for entity IDs.
- **Dual generators**: Nodes use even node field (`SnowflakeNodeID*2`), rels use odd (`*2+1`). Guarantees value-level uniqueness. Range: 0-511 (512 instances). Epoch: `2026-01-01`.
- **Strict encapsulation**: All fields unexported. Access through methods only.
- **Struct alignment**: Node (80B), Relationship (72B) packed by descending alignment. Verify with `unsafe.Sizeof`.
- **Token 0 reserved**: `HasLabelToken(0)` and `HasTypeToken(0)` always return false.
- **Validate before generating IDs**: `AddNode`/`AddRelationship` validate before `NextNodeID()`/`NextRelID()`.

### Defensive Copying

- **Accessors**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, `PropertiesMap()`, `ToMap()`, `DeepCopy()` always return independent copies.
- **Store boundary**: `Put*` deep-copy before caching; `Get*` deep-copy on return. Callers and store never share pointers.
- **Exception**: `Temporal()` and `Integrity()` return internal pointer (graph layer needs mutation access).

### Properties

- **Allowlist validation**: Recursive at insertion time. Primitives, slices, maps with safe elements only. Depth-limited to 32 (`ErrMaxDepthExceeded`).
- **`tkg_` prefix reserved**: `PropertySlice.Set()` rejects any key starting with `tkg_`.
- **Sorted invariant**: Always use `Set()` — never modify the slice directly.
- **Bulk construction**: `NewPropertySlice(map)` is O(N log N). `SetProperties(ps)` assigns directly. `AddNode`/`AddRelationship` use this path.

### Concurrency

- **Entity locks**: 256-shard `entityLockManager` for write-skew prevention. `shardIndex` uses low 8 bits of snowflake timestamp (`>> 22 & 0xFF`).
- **Lock ordering**: entity locks -> idxMu. Always.
- **Two-phase delete with TOCTOU retry**: Phase A reads adjacency under node lock. Phase B locks all entities via `LockMany`, re-verifies adjacency, retries if changed (max 10).
- **Ascending shard order**: `LockTwo` normalizes. `LockMany` deduplicates + sorts. Deadlock-free.
- **Transaction isolation via g.mu**: `Graph.mu` serializes tx/batch (Lock) vs standalone mutations and reads (RLock). All exported mutation methods (`*WithContext`, `RemoveNodeLabel`, `CloseNodeVersion`, `CloseRelVersion`) acquire `g.mu.RLock()`. Tx/batch call unexported `*Internal` variants directly under `g.mu.Lock()`. Individual temporal query methods do NOT acquire `mu` (avoids reentrancy deadlock).
- **sync.RWMutex is NOT reentrant**: If A holds RLock and calls B which RLocks, and a writer waits between them, deadlock. Inner methods must be lock-free.
- **sync.Once for idempotent Close()**: Never nil-guard a function pointer across goroutines.

### Persistence

- **Store is pure persistence**: Shadow and string resolution are Graph-layer responsibilities. All queries return `error` and sort by ID.
- **Async batch**: Writes update in-memory immediately, queue `writeOp` structs (last-write-wins dedup). Background flush via `WriteBatch` every `FlushInterval`.
- **LRU dirty tracking**: Dirty entries never evicted (soft capacity). `CollectDirty()` is read-only. `MarkFlushed()` is CAS on `dirtyVer`. `Peek()` for zero-alloc cache hits.
- **Tombstones in cache-first**: A cache miss must not fall through to stale Badger data.
- **Close() must flush unconditionally**: Even when `flushLoop` was never started (InMemory mode).
- **In-memory state must survive restart**: If it's in memory and it matters, it needs a persistence path and a rebuild path. If `loadIndexes()` doesn't rebuild it, it doesn't survive restart.
- **Counters in same WriteBatch as data**: Separate transactions = crash inconsistency.
- **Badger WriteBatch.Flush() blocks forever on closed DB**: Badger v4 uses `context.Background()` in `oracle.readTs()` → `WaitForMark()`, so closing the DB stops oracle goroutines and any in-flight `WriteBatch.Flush()` blocks forever. Fix: `BadgerStore.dbClosed atomic.Bool` is set to `true` in `Close()` BEFORE `db.Close()`. `flush()` checks it and returns `ErrDBClosed` immediately. Any test that calls `bs.db.Close()` directly MUST set `bs.dbClosed.Store(true)` first. Never call `WriteBatch.Flush()` after `db.Close()`.

### Version History

- **Pre-mutation snapshots**: `Update*` captures deep copy, applies mutations, writes both via `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` atomically.
- **Append-only**: Delete paths save tombstone versions (with `DeletedAt`/`ValidTo`) before deletion. History is never erased on delete.
- **Replace vs Put**: Put rejects duplicates. Replace requires existence. Replace overwrites data only — no index changes (labels/type/endpoints are immutable).
- **Genesis detection**: Use `entry.Version() == 0`, not array position. Array position changes after truncation.

### Temporal Queries

- **Effective valid-from**: Derived from explicit `ValidFrom` or snowflake ID timestamp. Every entity is queryable temporally without `SetTemporal()`.
- **Point-in-time**: `effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t)`.
- **Interval overlap**: `effectiveValidFrom < end AND (ValidTo == 0 OR ValidTo > start)`.
- **History-aware merging**: Temporal queries merge current + history IDs via lazy ForEach iterators (two-phase: collect IDs under store locks, process after release).
- **ForEach for OOM-safe iteration**: Never materialize all per-shard slices + merge. Use `ForEach*ID` callbacks. Constraint: callback must NOT call store methods (deadlock via B15). Two-phase: collect IDs, then process. ~83% memory reduction.
- **Deleted entity verification**: Any verification reading entity state must tolerate deletion — if entity has history but no current state, proceed using history alone.

### TieredStore

- **Timestamp routing**: Resolve the actual shard via snowflake ID timestamp or ref probe — never by `EntityClass`. Class tells you where new entities go; shard tells you where existing ones live.
- **Cross-shard split-write ordering (section 12)**: E->R: write ref shard inIdx first. R->E: write entity shard first. Both endpoints verified before any writes.
- **Checkout/checkin for cold shards**: `getStore()` returning a pointer without ref counting races with `closeIdleShards()`. Use `checkoutStore()`/`checkinStore()` to increment/decrement `activeReqs`.
- **Rotation boundary alignment**: Truncate to millisecond + add one unit. Nanosecond precision creates gaps with ms-resolution snowflake IDs.
- **Catalog sync on rotation**: Both in-memory `eventShard.timeEnd` AND catalog `ShardEntry.TimeEnd` must be updated on hot->warm rotation. Catalog is persisted — warm shard recovery depends on it.
- **Rollback on partial failure**: Cross-shard moves (archive/restore) must undo completed steps when a later step fails. Otherwise partial failure leaves data duplicated or orphaned.
- **Sequential ForEach**: One shard open at a time via checkout/checkin. No goroutines, no `mergeIDSlices`. Trades parallelism for memory safety.
- **Property indexes on reference entities only**: `CreatePropertyIndex` rejects event labels (`ErrEventPropertyIndex`).

### Integrity & Indexes

- **Canonical hash inputs**: Hash canonical internal state, not raw user input. Token deduplication/normalization happens during construction — hash must reflect the canonical state.
- **Index cleanup on corruption**: When a corruption fallback skips entity data, it must still clean ALL indexes (label, property, adjacency). Leaving stale entries causes phantom results.
- **3-phase index creation**: (1) Install empty placeholder under Lock (visibility for concurrent writes), (2) unlocked I/O to build, (3) Lock to install with dirty-map tracking (`mutated[id]` not `contains(id)` — prevents re-adding concurrently deleted values).
- **API design rules**: Config fields must be used or removed. Opaque wrappers must wrap the real type. Graph is sole external API. Doc comments must match behavior.

### Events & Stats

- **EventBus is opt-in**: `Graph.SetEventBus(bus)` — nil by default (zero overhead). Handlers are copied under RLock, invoked outside the lock (prevents deadlocks from re-entrant Graph calls in handlers).
- **Tx event buffering**: During a transaction (`txEventBuffer != nil`), `publishEvent` appends to a buffer instead of dispatching. On `Commit`, events are published after `g.mu.Unlock()` so handlers can safely call Graph read methods. On `Rollback`, buffered events are discarded — subscribers never see rolled-back mutations.
- **AsyncEventBus for async delivery**: `Graph.SetAsyncEventBus(bus)` — worker pool with per-priority `[5]chan Event` queues. `BackpressureStrategy` controls full-queue behavior (Block/DropOldest/DropLatest). `Close()` drains all pending events before stopping workers. `Graph.events` is typed as `eventPublisher` interface (unexported) — allows either bus type without breaking the external API.
- **EventPriority**: 5 levels — `PriorityNormal` (0, zero value), `PriorityHigh` (1), `PriorityCritical` (2), `PriorityLow` (3), `PriorityDeferred` (4). Graph assigns internally: creates→High, deletes→Critical, updates→Normal. Backward-compatible: existing `Event{}` literals default to PriorityNormal. Priority ordering in `AsyncEventBus` worker uses non-blocking drain per level (Critical first) before blocking select.
- **StoreStats opt-in**: Type-asserted in `Graph.Stats()` — avoids polluting the `Store` interface.
- **Atomic operation counters**: 8 `atomic.Int64` fields on Graph — incremented after every successful store write.

### Vector Indexes

- **Not persisted**: Vector indexes are rebuilt from node properties on restart. This is a documented limitation — acceptable for brute-force k-NN.
- **Store-level scope in TieredStore**: Vector indexes live at the `TieredStore` level (not per-shard) with their own `vectorIdxMu sync.RWMutex`.
- **Auto-maintenance**: All mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`) update vector indexes.

### Code Review Lessons

- **Use library APIs, never reimplement internals**: If a dependency provides an API (e.g. `snowflake.Node.Decompose()`), use it. Never duplicate internal knowledge like bit layouts (`>> 22`, `& 0x3FF`) in the consumer. When the library changes its layout, hardcoded shifts break silently across dozens of call sites. The library's API encapsulates the layout — use it.
- **Every fix needs a grep audit**: When fixing a pattern in one call site, grep for the same pattern across all files. The canonical hash bug (A1) was fixed in `context.go` but missed in `batch.go`, requiring a second review round.
- **Fix descriptions must include exact signatures**: Telling a developer to "use lazy iterators" without specifying the callback shape led to 5 rounds of partial fixes for the OOM issue (C4). Specify the exact interface.
- **Review by feature, not by file**: Single-file reviews missed cross-file interactions. The `batch.go` hash bug was only caught when reviewing `batch.go`, not when reviewing `integrity.go` where the hash function lives.
- **Repair tools complement but don't replace correctness**: The cross-shard rollback issue (B7) was accepted as "mitigated" by `RunRepair` when it should have been fixed inline.

## Audit Checklists

Run these after any change to the relevant subsystem.

**Hash computation**: `grep -rn 'ComputeNodeHash\|ComputeRelHash' pkg/` — every call site must pass canonical (registry-resolved) labels/type, not raw input.

**Entity locks**: `grep -rn 'store\.Put\|store\.Delete\|store\.Replace' pkg/graph/` — every Store write must have `LockEntity`/`LockTwo`/`LockMany` before it.

**Index cleanup**: Every delete path handling missing entities must purge label, property, AND adjacency indexes. Brute-force purge is acceptable for corruption paths.

**Index creation visibility**: `CreatePropertyIndex` must install empty placeholder BEFORE I/O phase. Phase 3 must check `mutated[id]`, not `contains(id)`.

## Registries

Two independent registries with independent token namespaces. Methods: `GetOrCreate`, `Resolve`, `ResolveAll`, `Lookup`. `GetOrCreate` rejects empty strings (`ErrEmptyName`), warns at 60K tokens, errors at 65535. Persisted in Badger as `meta/label_tokens`/`meta/reltype_tokens`, or in TieredStore as `data/meta/registry.msgpack` (atomic write).

## Shadow Properties (21)

| Key | Type | Applies To | Category |
|---|---|---|---|
| `tkg_labels` | `[]string` | Node | Structural |
| `tkg_type` | `string` | Relationship | Structural |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both | Temporal |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both | Temporal |
| `tkg_created_at` | `Instant` | Both | Temporal (auto-derived from snowflake ID when unset) |
| `tkg_updated_at`, `tkg_deleted_at` | `Instant` | Both | Temporal |
| `tkg_created_by`, `tkg_updated_by` | `string` | Both | Provenance |
| `tkg_version` | `uint32` | Both | Provenance |
| `tkg_hash`, `tkg_prev_hash` | `string` | Both | Integrity |
| `tkg_base_entity` | `entityID` | Both | Version chain |
| `tkg_from_hash` | `string` | Relationship | Integrity — start-node hash at write time |
| `tkg_to_hash` | `string` | Relationship | Integrity — end-node hash at write time |
| `tkg_author_id` | `string` | Both | Provenance — caller-supplied author identifier |
| `tkg_signature` | `[]byte` | Both | Integrity — caller-supplied cryptographic signature |
| `tkg_authorized_by` | `string` | Both | Authorization — caller-supplied authorizing entity |
| `tkg_auth_level` | `uint8` | Both | Authorization — caller-supplied authorization tier |

## Badger Key Layout

| Key pattern | Purpose | Key size |
|---|---|---|
| `0x01/<8B nodeID>` | Node entity | 9B |
| `0x02/<8B relID>` | Relationship entity | 9B |
| `0x03/<2B labelToken>/<8B nodeID>` | Label index | 11B |
| `0x04/<2B relTypeToken>/<8B relID>` | Type index | 11B |
| `0x05/<8B startID>/<2B relType>/<8B endID>/<8B relID>` | Outgoing adjacency | 27B |
| `0x06/<8B endID>/<2B relType>/<8B startID>/<8B relID>` | Incoming adjacency | 27B |
| `0x07/<8B nodeID>/<8B version>` | Node version history | 17B |
| `0x08/<8B relID>/<8B version>` | Rel version history | 17B |
| `0x0F/*` | Metadata (registries, counters, prop index defs) | varies |

## Ecosystem

| Module | Role |
|---|---|
| `rho/tkg/v3` | Internal library — graph types, persistence, registries (this repo) |
| `rho/tkgd-v3` | Full product — Cypher engine, Vadalog reasoning, HTTP/gRPC server |
| `rho/kit` | Service toolkit — app builder, logging, tracing, resilience, database |

tkg/v3 does **not** depend on kit. tkgd-v3 depends on both.
