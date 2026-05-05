# Architecture — tkg/v3 (v3.1.9)

Temporal Knowledge Graph v3 is a pure Go library providing the core graph engine for temporal knowledge graphs. It is the low-level storage and type layer — no main binary, no HTTP server, no query language.

```
Module:  gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3
Go:      1.26.1
License: Apache-2.0
```

---

## System Overview

```
                        Graph
                          |
          +---------------+---------------+
          |               |               |
    labelRegistry   relTypeRegistry   entityLockManager
                          |                (256 shards)
                        Store
                          |
            +-------------+-------------+
            |             |             |
       MemoryStore   BadgerStore   TieredStore
                                       |
                     +-----------------+-----------------+
                     |                 |                 |
                  refShard        refArchive      eventShards
                (BadgerStore)   (lazy-open)    map[string]*eventShard
                                                 +-- hot  (read-write)
                                                 +-- warm (read-only)
                                                 +-- cold (lazy-open, idle-close)
```

**Graph** is the sole external API. It owns the registries, dual snowflake generators, the Store, and an entity lock manager. All string resolution, referential integrity, and temporal query logic live here.

**Store** is a pure persistence interface (59 methods). Three implementations: MemoryStore (testing), BadgerStore (single-instance persistent), TieredStore (multi-shard persistent).

**Vector indexes** live at the Store level (per-Store in MemoryStore/BadgerStore, per-TieredStore -- not per-shard). In-memory brute-force k-NN with Cosine and Euclidean distance metrics. Protected by a dedicated `vectorIdxMu sync.RWMutex`. Not persisted -- rebuilt from node properties on restart. Auto-maintained across all mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`).

---

## Core Types (`pkg/types`)

| Type | Size | Purpose |
|------|------|---------|
| `Node` | 80B | Graph vertex. `nodeID` (wraps `snowflake.ID`), primary + extra labels as `labelToken`, properties, `uint32` version, temporal, integrity |
| `Relationship` | 72B | Directed edge. `relID` (wraps `snowflake.ID`), `relTypeToken`, start/end as `nodeID`, properties, `uint32` version, temporal, integrity |
| `PropertySlice` | var | Sorted key-value store with binary search. Recursive allowlist validation, depth-limited to 32 levels. `tkg_` prefix reserved |
| `Instant` | 8B | Unix-millisecond timestamp. All temporal fields use this type |
| `TemporalMetadata` | var | ValidFrom, ValidTo, TxFrom, TxTo, CreatedAt, UpdatedAt, DeletedAt, CreatedBy, UpdatedBy, BaseEntityID |
| `NodeIntegrity` / `RelIntegrity` | var | SHA-256 hash chain + provenance/authorization: `Hash`, `PrevHash`, `AuthorID string`, `Signature []byte`, `AuthorizedBy string`, `AuthorizationLevel uint8`. `RelIntegrity` additionally holds `FromNodeHash`, `ToNodeHash` (endpoint cross-references at write time) |

**Key invariants:**
- All struct fields unexported. Access through methods only.
- Packed by descending field alignment (verified with `unsafe.Sizeof`).
- `ExtraLabelTokens()`, `Properties()`, `DeepCopy()`, `ToMap()` always return independent copies.
- `Temporal()` and `Integrity()` return internal pointers (no copy).
- Token 0 is reserved/invalid. `HasLabelToken(0)` always returns false.

### Shadow Properties (21)

Read-only virtual properties dispatched by the graph layer from internal metadata. Never stored in `PropertySlice`; `PropertySlice.Set()` rejects any key with the `tkg_` prefix.

| Key | Type | Applies To | Category |
|-----|------|------------|----------|
| `tkg_labels` | `[]string` | Node | Structural |
| `tkg_type` | `string` | Relationship | Structural |
| `tkg_valid_from` | `Instant` | Both | Temporal |
| `tkg_valid_to` | `Instant` | Both | Temporal |
| `tkg_tx_from` | `Instant` | Both | Temporal |
| `tkg_tx_to` | `Instant` | Both | Temporal |
| `tkg_created_at` | `Instant` | Both | Temporal (auto-derived from snowflake ID when unset) |
| `tkg_updated_at` | `Instant` | Both | Temporal |
| `tkg_deleted_at` | `Instant` | Both | Temporal |
| `tkg_created_by` | `string` | Both | Provenance |
| `tkg_updated_by` | `string` | Both | Provenance |
| `tkg_version` | `uint32` | Both | Provenance |
| `tkg_hash` | `string` | Both | Integrity |
| `tkg_prev_hash` | `string` | Both | Integrity |
| `tkg_from_hash` | `string` | Relationship | Integrity -- start-node hash at write time |
| `tkg_to_hash` | `string` | Relationship | Integrity -- end-node hash at write time |
| `tkg_author_id` | `string` | Both | Provenance -- caller-supplied author identifier |
| `tkg_signature` | `[]byte` | Both | Integrity -- caller-supplied cryptographic signature |
| `tkg_authorized_by` | `string` | Both | Authorization -- caller-supplied authorizing entity |
| `tkg_auth_level` | `uint8` | Both | Authorization -- caller-supplied authorization tier |
| `tkg_base_entity` | `entityID` | Both | Version chain |

---

## Snowflake IDs

```
+---------------------------------------------------------------+
|  1 bit  |       48 bits        |   5 bits   |     10 bits     |
|  zero   |     time (µsec)      |   node ID  |    sequence     |
+---------------------------------------------------------------+
```

- Precision: **microseconds** (`snowflake.WithMicroseconds()`)
- Epoch: `2026-01-01 00:00:00 UTC`
- Two generators per Graph: nodes (`SnowflakeNodeID*2`), relationships (`SnowflakeNodeID*2+1`)
- Valid `SnowflakeNodeID` range: 0-15 (16 concurrent graph instances)
- 1024 unique IDs per microsecond per generator
- Stateless — no counter persistence, no crash recovery
- Creation timestamp extractable via `snowflakeLayout.Decompose(id).Time` or `DecomposeID(id).CreatedAt`

---

## Graph Layer (`pkg/graph`)

### Entity Management

| Method | Locks | Description |
|--------|-------|-------------|
| `AddNode(labels, props)` | `g.mu.RLock()` | Validate, generate ID, compute hash (genesis), store |
| `AddRelationship(type, start, end, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Validate endpoints exist; reject self-loops when `AllowSelfLoops=false` (`ErrSelfLoop`); generate ID, hash, store |
| `UpdateNode(id, updates)` | `g.mu.RLock()` + `LockEntity(id)` | Deep-copy pre-mutation, apply updates, bump version, `ReplaceNodeWithHistory` |
| `UpdateRelationship(id, updates)` | `g.mu.RLock()` + `LockEntity(id)` | Same pattern as UpdateNode |
| `DeleteNode(id)` | `LockMany(node + all rels)` | Two-phase TOCTOU: read adjacency, lock all, re-verify, build `[]RelTombstone`, single atomic `DeleteNodeWithHistory` call |
| `DeleteRelationship(id)` | `LockEntity(id)` | Build tombstone, single atomic `DeleteRelWithHistory` call |
| `AddRelationshipByID(type, startID, endID, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Create relationship using endpoint snowflake IDs directly. Skips endpoint fetch, endpoint hash capture, and temporal constraint checks against endpoints. High-throughput path |
| `AddRelationshipByIDIfAbsent(type, startID, endID, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Atomic check-then-create: returns existing relationship if same type+endpoints already connected, otherwise creates. Same trade-offs as `AddRelationshipByID`. Returns `(rel, created, err)` |
| `GetNode(id)` | none (read-only via store) | Retrieve a single node by snowflake ID |

All mutations enforce `ValidationLimits` (5 configurable limits with defaults). Context-aware variants (`*WithContext`) add cancellation checks at critical points. Non-context methods delegate with `context.Background()`.

### Temporal Queries

Point-in-time: `GetNodesValidAt(t)`, `GetRelationshipsValidAt(t)`, `GetNodesByLabelValidAt(label, t)`
Interval: `GetNodesValidDuring(start, end)`, `GetRelationshipsValidDuring(start, end)`
Version-specific: `GetNodeAt(id, t)`, `GetRelAt(id, t)`
Traversal: `GetNeighborsValidAt(nodeID, t)`
Snapshot: `Snapshot(t)` -- full graph state at time t (endpoint-filtered)
Combined: `NodesByLabelPropertyAndTime(label, key, value, t)`, `NodesByLabelPropertyDuring(label, key, value, start, end)`
Bitemporal (transaction time): `GetNodeAsOf(id, txTime)`, `GetRelAsOf(id, txTime)`, `GetNodesAsOf(txTime)`, `GetRelsAsOf(txTime)`

**History-aware queries** include deleted entities. They use two-phase ForEach iteration:
1. **Collect** -- `ForEachNodeID` + `ForEachNodeHistoryID` insert unique IDs into a `seen` map (callback must NOT call store methods -- RWMutex non-reentrancy)
2. **Process** -- iterate `seen` map after ForEach returns (store locks released, safe to call `GetNodeAt` etc.)

This replaced the pre-v3.0.31 approach of materializing all IDs into slices (`allKnownNodeIDs`/`mergeIDSlices`), reducing peak memory by ~83% for 10M nodes.

**Temporal semantics:**
- `effectiveValidFrom` derives from explicit `ValidFrom` (if set) or snowflake ID timestamp (always available)
- Point-in-time match: `effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t)`
- Interval overlap: `effectiveValidFrom < end AND (ValidTo == 0 OR ValidTo > start)`

### Batch Operations

`BatchBuilder` -- fluent API with eager validation and deferred persistence. Execute order: create nodes, create rels, update nodes, update rels, delete rels, delete nodes. Returns `BatchResult` with counts and per-operation errors.

### Transactions

`GraphTx` -- full CRUD transaction holding the graph write lock (`mu.Lock`). Supports `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode` (cascade), `DeleteRelationship`, and convenience property methods. `Commit()` releases the lock and publishes buffered events. `Rollback()` restores all mutations in reverse order via snapshot-based rollback: deleted entities are re-created, updates are reverted to pre-mutation snapshots, created entities are deleted. Not suitable for long-running operations.

### Transaction and Mutation Isolation

`Graph.mu` (`sync.RWMutex`) serializes tx/batch (Lock) vs standalone mutations and reads (RLock). All 13 exported mutation methods (`*WithContext`, `RemoveNodeLabel`, `CloseNodeVersion`, `CloseRelVersion`) acquire `g.mu.RLock()` at entry. `BeginTx` and `BatchBuilder.Execute` acquire `g.mu.Lock()`, blocking all standalone mutations. `GraphTx` and `BatchBuilder` call unexported `*Internal` variants (lock-free) directly under `g.mu.Lock()`. Individual temporal query methods do NOT acquire `mu` (avoids reentrancy deadlock when Snapshot calls them).

### Hash Chain Integrity

`ComputeNodeHash(n, labels)` / `ComputeRelHash(r, typeName)` -- SHA-256 via typed binary serialization with sorted map keys. Genesis: `PrevHash=""`. Updates: `PrevHash=previous.Hash`. `VerifyNodeHashChain(id)` / `VerifyRelHashChain(id)` verify the full chain (handles deleted entities and truncated history).

---

## Store Interface (`pkg/graph/store.go`)

59 methods. Pure persistence contract -- no string resolution, no referential integrity, no shadow properties.

### Query Control

```go
type QueryOpts struct {
    Limit      int            // 0 = no limit
    After      snowflake.ID   // Cursor: return IDs > After (0 = from start)
    ValidAt    types.Instant  // Point-in-time filter (0 = disabled)
    ValidStart types.Instant  // Interval filter start (0 = disabled)
    ValidEnd   types.Instant  // Interval filter end (0 = disabled)
    Depth      ShardDepth     // Shard tier filter (0 = all tiers)
}

type ShardDepth byte
const (
    DepthAll  ShardDepth = 0  // all tiers (default, backward-compatible)
    DepthHot  ShardDepth = 1  // hot shard only
    DepthWarm ShardDepth = 2  // hot + warm shards
)
```

### Method Categories

| Category | Methods | Notes |
|----------|---------|-------|
| Node CRUD | `PutNode`, `GetNode`, `ReplaceNode`, `DeleteNode` | Deep-copy at boundary |
| Rel CRUD | `PutRelationship`, `GetRelationship`, `ReplaceRelationship`, `DeleteRelationship` | Deep-copy at boundary |
| Index queries | `NodesByLabel`, `RelationshipsByType` | Paginated, temporal push-down |
| Adjacency | `OutgoingRelationships`, `IncomingRelationships` | typeToken 0 = all types |
| Bulk queries | `AllNodes`, `AllRelationships`, `GetNodesByIDs`, `GetRelationshipsByIDs` | Paginated, sorted by ID |
| Batch ops | `PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch` | Two-phase validate-then-apply |
| Atomic replace | `ReplaceNodeWithHistory`, `ReplaceRelWithHistory` | Entity + history in one call |
| Version history | `PutNodeVersion`, `GetNodeVersion`, `GetNodeHistory`, `TruncateNodeHistory` + rel mirrors | 8 methods total |
| Cascade | `DeleteNodeCascade` | Node + all connected rels |
| Atomic delete | `DeleteNodeWithHistory`, `DeleteRelWithHistory` | Tombstone history + entity delete in one batch (crash-safe) |
| Counts | `NodeCount`, `RelationshipCount`, `NodeCountByLabel`, `RelCountByType` | O(1) |
| Property indexes | `CreatePropertyIndex`, `DropPropertyIndex`, `NodesByLabelAndProperty` | In-memory, auto-maintained |
| Temporal indexes | `CreateTemporalIndex`, `DropTemporalIndex` | Sorted-slice interval index, O(log n + k) overlap queries |
| HF indexes | `CreateHighFrequencyIndex`, `DropHighFrequencyIndex` | Time-bucketed, O(1) amortized insert. One temporal index type per label |
| Vector indexes | `CreateVectorIndex`, `DropVectorIndex`, `SearchNearestNodes` | In-memory brute-force k-NN. Not persisted |
| Label management | `RemoveNodeLabelToken`, `RemoveNodeLabelTokenWithHistory` | Atomic label removal with optional history |
| ID-only queries | `AllNodeIDs`, `AllRelIDs` | No deserialization |
| History IDs | `AllNodeHistoryIDs`, `AllRelHistoryIDs` | Includes deleted entities |
| ForEach iterators | `ForEachNodeID`, `ForEachRelID`, `ForEachNodeHistoryID`, `ForEachRelHistoryID` | Callback-based, no slice materialization |
| Lifecycle | `Clear`, `Close` | `Close` idempotent via `sync.Once` |

### Sentinel Errors

`ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrVersionNotFound`, `ErrNoVersionValidAt`, `ErrIndexExists`, `ErrIndexNotFound`, `ErrTxDone`

**Graph-layer sentinel errors** (not in Store interface): `ErrSelfLoop` — returned by `AddRelationshipWithContext` / `ImportRelationshipWithID` when `startID == endID && !g.validation.AllowSelfLoops`.

---

## MemoryStore (`pkg/graph/memorystore.go`)

Thread-safe in-memory Store. Single `sync.RWMutex` protects all maps.

```
nodes        map[snowflake.ID]*types.Node
rels         map[snowflake.ID]*types.Relationship
labelIdx     map[uint16]map[snowflake.ID]struct{}     // label -> node IDs
typeIdx      map[uint16]map[snowflake.ID]struct{}     // relType -> rel IDs
outIdx       map[snowflake.ID]map[snowflake.ID]struct{}  // startID -> rel IDs
inIdx        map[snowflake.ID]map[snowflake.ID]struct{}  // endID -> rel IDs
nodeHistory  map[snowflake.ID]map[uint32]*types.Node
relHistory   map[snowflake.ID]map[uint32]*types.Relationship
```

- O(1) per-label/per-type counts via `len(labelIdx[token])`
- Hash-set adjacency indexes for O(1) insert/delete
- Deep-copy at store boundary (both Put and Get)
- Temporal push-down: filters in-memory entity pointers without deep-copy
- ForEach: map iteration under RLock
- `Clear()` reinitializes all maps. `Close()` is a no-op.

---

## BadgerStore (`pkg/graph/badgerstore.go`)

Persistent Store using Badger v4 with async batch persistence.

### Key Architecture

```
+-- In-memory indexes (source of truth) --+     +-- Entity caches --+
|  nodeIDs   map[ID]struct{}               |     |  nodeCache  LRU   |
|  relIDs    map[ID]struct{}               |     |  relCache   LRU   |
|  labelIdx  map[token]map[ID]struct{}     |     +-------------------+
|  typeIdx   map[token]map[ID]struct{}     |
|  outIdx    map[ID]map[ID]struct{}        |     +-- Write buffer --+
|  inIdx     map[ID]map[ID]uint16         |     |  pending map     |
|  (all under idxMu RWMutex)              |     |  (under wbMu)    |
+------------------------------------------+     +------------------+
                    |                                     |
                    v                                     v
              +-- Badger DB (on-disk or in-memory) --+
              |  entity data, indexes, history, meta  |
              +---------------------------------------+
```

**Concurrency primitives:**
- `idxMu sync.RWMutex` -- protects all in-memory indexes
- `wbMu sync.Mutex` -- protects pending write buffer
- `atomic.Int64` counters for node/rel counts
- `sync.Map` for per-label/per-type counters
- `sync.Once` for idempotent Close

**Async persistence flow:**
1. Write operations update in-memory state immediately
2. Queue `writeOp` structs into `map[string]writeOp` (last-write-wins dedup)
3. Background flush loop drains buffer via Badger `WriteBatch` every `FlushInterval` (default 100ms)
4. Counter keys (`meta/node_count`, `meta/rel_count`) included in same WriteBatch for atomic crash recovery
5. Failed ops re-queued via `requeueOps()`

**LRU caches (`entityLRU[V]`):**
- Dirty tracking with monotonic `dirtyVer` counter
- Tombstone support for deletions
- Dirty entries never evicted (soft capacity)
- `Peek(key)` for zero-allocation lookup (no deep-copy, no MRU promotion) -- used by temporal pre-filter
- `MarkFlushed()` only clears entries matching the collected `dirtyVer` (prevents data loss from concurrent writes)

**Key layout (9 prefixes):**

| Prefix | Key pattern | Size | Purpose |
|--------|-------------|------|---------|
| `0x01` | `/<8B nodeID>` | 9B | Node entity |
| `0x02` | `/<8B relID>` | 9B | Relationship entity |
| `0x03` | `/<2B labelToken>/<8B nodeID>` | 11B | Label index |
| `0x04` | `/<2B relTypeToken>/<8B relID>` | 11B | Type index |
| `0x05` | `/<8B startID>/<2B relType>/<8B endID>/<8B relID>` | 27B | Outgoing adjacency |
| `0x06` | `/<8B endID>/<2B relType>/<8B startID>/<8B relID>` | 27B | Incoming adjacency |
| `0x07` | `/<8B nodeID>/<8B version>` | 17B | Node version history |
| `0x08` | `/<8B relID>/<8B version>` | 17B | Rel version history |
| `0x0F` | `/meta/*` | var | Counters, registry tokens, property index defs |

All IDs stored as big-endian uint64 for correct sort order and temporal clustering.

**ReadOnly mode:** `BadgerStoreConfig.ReadOnly` opens Badger read-only, skips flush/GC goroutines. Used by TieredStore for warm/cold shards.

**ForEach iterators:**
- `ForEachNodeID` / `ForEachRelID` -- iterate in-memory index maps under `idxMu.RLock`
- `ForEachNodeHistoryID` / `ForEachRelHistoryID` -- Phase 1: scan pending buffer under `wbMu.Lock`, emit unique IDs. Phase 2: Badger prefix scan with dedup via `seen` map

---

## TieredStore (`pkg/graph/tieredstore.go` + `_read.go` + `_write.go`)

Multi-shard Store routing entities across a reference shard and time-windowed event shards.

### Shard Model

```
data/
  meta/
    registry.msgpack        # Label + reltype registries (msgpack, atomic rename)
    shard_catalog.json      # Shard metadata (JSON, atomic rename)
  reference/                # Reference shard (always open, read-write)
  events/
    2026-W10/               # Hot event shard (read-write)
    2026-W09/               # Warm event shard (read-only)
    2026-W08/               # Cold event shard (store=nil, lazy-open)
    ...
  archive/                  # Reference archive (lazy-created on first ArchiveNode)
```

**TieredStore struct (key fields):**

```
mu            sync.RWMutex           -- protects hotShard + eventShards during rotation
refShard      *BadgerStore           -- always open
refArchive    *BadgerStore           -- nil until first archive/restore
archiveMu     sync.Mutex             -- protects lazy-open of refArchive
eventShards   map[string]*eventShard -- name -> shard
hotShard      *eventShard            -- convenience pointer to current hot shard
ontology      *OntologyMapping       -- classifies labels (reference vs event)
catalog       *ShardCatalog          -- persistent shard metadata
closeCh       chan struct{}           -- signals idle-close goroutine to stop
closeOnce     sync.Once              -- idempotent Close
```

**eventShard struct:**

```
name        string         -- shard name (e.g., "2026-W10")
store       *BadgerStore   -- nil when cold + closed
tier        ShardTier      -- TierHot / TierWarm / TierCold
timeStart   time.Time      -- window start (inclusive)
timeEnd     time.Time      -- window end (exclusive)
readOnly    bool           -- warm/cold shards
path        string         -- for lazy-open
shardMu     sync.Mutex     -- protects lazy open/close
activeReqs  atomic.Int64   -- outstanding reads (blocks idle-close)
lastAccess  atomic.Int64   -- unix ms (idle-close tracking)
```

### Ontology Classification

`OntologyMapping` classifies labels as `ClassReference` (long-lived: Case, Organization, User) or `ClassEvent` (time-windowed: Signal, Alert -- the default for unknown labels). Lazy token cache backed by label registry.

Reference entities go to `refShard`. Event entities go to the hot event shard.

### Shard Resolution (O(1) for nodes, O(1)->O(N) for rels)

**Nodes (`shardForNodeID`):**
1. Check `refShard.hasNodeID(id)` -- O(1)
2. If miss, check `refArchive` (if open) -- O(1)
3. Extract timestamp from snowflake ID via `snowflakeLayout.Decompose(id).Time` -- O(1) shard window lookup

**Relationships (`shardForRelID`):**
1. Check `refShard.hasRelID(id)` -- O(1)
2. Timestamp extraction -> target event shard -- O(1)
3. Fallback: probe hot+warm shards for cross-shard rels -- O(N), skips cold

**New entities:** Route by ontology classification (label -> ref/event), event entities go to hotShard.

### Relationship Routing (7 keys per relationship)

| Pattern | Entity + out/ keys | in/ keys | Split? | Write order |
|---------|-------------------|----------|--------|-------------|
| E->E | event shard | event shard | No | single shard |
| R->R | ref shard | ref shard | No | single shard |
| E->R | event shard | ref shard | Yes | ref in/ first |
| R->E | ref shard | event shard | Yes | entity first |

Cross-shard split writes use `badgerstore_partial.go` helpers: `putRelEntityAndOut` (entity + typeIdx + outIdx) and `putRelIncoming` (inIdx only). Both endpoints verified to exist before any writes begin.

**Incoming index structure:** BadgerStore's `inIdx` uses `map[snowflake.ID]map[snowflake.ID]uint16` (endNodeID -> relID -> relTypeToken), not `map[snowflake.ID]struct{}`. The typeToken value enables efficient cross-shard type filtering in `IncomingRelationships` without fetching the relationship entity from a remote shard. MemoryStore retains the simpler `map[snowflake.ID]struct{}` since all entities are local.

### Shard Lifecycle

```
                  checkRotation()
                       |
    hot ----[window expires]----> warm ----[ColdAfter]----> cold
  (read-write)                  (read-only)              (store=nil)
                                                            |
                                                    [first access]
                                                            |
                                                     lazy-open (read-only)
                                                            |
                                                    [IdleTimeout]
                                                            |
                                                     idle-close (store=nil)
```

**Rotation (`RotateHotShard`):**
1. Flush old hot shard
2. Align `timeEnd` boundary to `now.Truncate(time.Millisecond).Add(time.Millisecond)` (snowflake ms resolution)
3. Mark warm (read-only via `BadgerStoreConfig.ReadOnly`)
4. Create new hot shard with ms-aligned boundaries
5. If `ColdAfter > 0` and eligible warm shards exist, demote to cold (close store, set `store=nil`)
6. Update catalog

**Cold shard access (`checkoutStore`/`checkinStore`):**
- `checkoutStore` increments `activeReqs`, lazy-opens if `store==nil`
- `checkinStore` decrements `activeReqs`
- `closeIdleShards()` skips shards with `activeReqs > 0`
- Background goroutine `idleCloseLoop()` ticks every `IdleTimeout/2`, stopped via `closeCh`

**Warm recovery on restart:** Constructor reads catalog, reopens warm shards as ReadOnly, cold shards recovered with `store=nil`.

### Read Operations

**Single-entity reads:** ref probe + archive fallback + timestamp extraction. O(1), no fan-out.

**Merge queries (AllNodes, AllRels, AllNodeIDs, AllRelIDs, counts, history IDs):**
- `eventShardSnapshot(opts.Depth)` under `mu.RLock` -- snapshot shard list
- Parallel via `sync.WaitGroup`: ref shard sequential, event shards concurrent
- k-way merge of sorted slices

**ForEach iterators (ForEachNodeID, ForEachRelID, ForEachNodeHistoryID, ForEachRelHistoryID):**
- Sequential shard iteration with checkout/checkin -- one shard open at a time
- No goroutines, no `mergeIDSlices` -- trades parallelism for ~83% memory reduction
- refShard -> archive (if open) -> event shards one at a time

**Cross-shard IncomingRelationships:** Get relIDs from node's shard inIdx, fetch each entity via `shardForRelID` (O(1) per entity via timestamp extraction).

### Write Operations

**Nodes:** Single-shard by primary label classification. `checkRotation()` on all new-entity write paths.

**Relationships:** Shard-based routing via `shardForNodeID` (not class-based -- class only tells you where *new* entities go, not where existing ones live). Batch operations partition by `*BadgerStore` pointer.

**Archive/Restore:** `ArchiveNode(id)` moves ref node + rels to archive shard. `RestoreNode(id)` reverses. Rollback on partial failure (Lesson B7).

**Property indexes:** Restricted to reference entities only (`ErrEventPropertyIndex` for event labels).

### Admin & Repair (`tieredstore_admin.go`, `tieredstore_repair.go`)

- `ForceRotate()` -- safe wrapper with internal locking
- `ListShards()` -- returns `[]ShardInfo` with live counts
- `RebuildCatalog()` -- reconstructs catalog from live state
- `VerifyShard(name)` -- hash chain verification with immutable-shard caching
- `RunRepair()` -- Phase 1: detect+delete orphaned in/ entries. Phase 2: detect+re-create missing in/ entries
- `MigrateFromBadger(src, dst, labels)` -- copies all entities with automatic ontology routing

---

## Entity Lock Manager (`pkg/graph/entity_locks.go`)

256-shard mutex array for write-skew prevention. 2KB total.

```go
type entityLockManager struct {
    shards [256]sync.Mutex
}

func shardIndex(id snowflake.ID) uint8 {
    return uint8(snowflakeLayout.Decompose(id).Time) & (entityLockShards - 1)
}
```

Extracts low 8 bits of the snowflake timestamp via `snowflakeLayout.Decompose()`. Entities created >256µs apart distribute across shards.

| Method | Use case | Deadlock prevention |
|--------|----------|---------------------|
| `LockEntity(id)` | Single-entity mutations | N/A |
| `LockTwo(a, b)` | Relationship creation | Ascending shard order |
| `LockMany(ids)` | Cascade delete | Deduplicate + sort ascending |

Lock ordering: entity locks -> `idxMu`. Always.

---

## Registries (`pkg/graph`)

Two independent registries with independent token namespaces.

```
labelRegistry:    map[string]labelToken   + []string reverse lookup
relTypeRegistry:  map[string]relTypeToken + []string reverse lookup
```

- Thread-safe: `sync.RWMutex`, double-check on write miss
- `GetOrCreate(string)` rejects empty strings (`ErrEmptyName`)
- Growth warning at 60K tokens (92% of uint16), error at 65535
- **BadgerStore persistence:** Inside Badger as `meta/label_tokens` and `meta/reltype_tokens` (msgpack)
- **TieredStore persistence:** Flat msgpack file at `data/meta/registry.msgpack` (atomic write via write-tmp+rename)
- Loaded on `Graph.New()`, saved on `Graph.Close()`

---

## Key Architectural Patterns

Distilled from real bugs across 13+ review rounds. Full details in `tasks/lessons.md`.

### Two-Phase Operations

Multi-step mutations are all-or-nothing. Phase 1: read everything, fail fast. Phase 2: apply everything, no error exits. Used by: `DeleteNodeCascade`, batch operations, `CreatePropertyIndex`.

### Async Persistence with Last-Write-Wins

Write operations update in-memory state immediately, queue write ops into `map[string]writeOp` (dedup by key). Background flush loop drains via Badger WriteBatch. Counters in the same WriteBatch for atomic crash recovery. `Close()` calls `flush()` unconditionally (even if flushLoop never started).

### Deep-Copy at Store Boundary

Entities deep-copied on both Put and Get. Cache and caller never share pointers. Internal locked methods (`getNodeLocked`) skip the copy when the caller already holds the write lock.

### Temporal Data Is Append-Only

History is never physically deleted. Delete paths save tombstone versions (with `DeletedAt`/`ValidTo`) before deletion. Past-time queries reconstruct deleted entities from history.

The tombstone write and the entity deletion are combined in a single atomic Store call (`DeleteNodeWithHistory` / `DeleteRelWithHistory`). All ops land in one Badger `WriteBatch.Flush()` under the same `idxMu.Lock()`, eliminating the crash window that previously existed between N+2 separate store calls.

### ForEach for OOM-Safe Iteration

Callback-based iteration (`fn(snowflake.ID) bool` -- return true to continue, false to stop) replaces slice materialization. Two constraints: (1) callbacks must NOT call store methods (RWMutex non-reentrancy); (2) TieredStore iterates shards sequentially, one at a time via checkout/checkin.

### Cold Shard Checkout/Checkin

Returning a `*BadgerStore` pointer and releasing the lock creates a race with idle-close. Solution: `checkoutStore` increments `activeReqs`, caller defers `checkinStore`, `closeIdleShards` skips shards with `activeReqs > 0`.

### Cross-Shard Split-Write Ordering

E->R: write ref shard in/ first (critical path for `Case <- Signal` queries). R->E: write entity shard first. Both endpoints verified before any partial writes. Repair tool (`RunRepair`) fixes inconsistencies from partial failures.

### Shard-Based Routing (Not Class-Based)

Class tells you where *new* entities go. Shard tells you where *existing* entities live. After rotation, two event entities in different shards both classify as `ClassEvent`, but `shardForClass(ClassEvent)` returns only the hot shard. Always resolve the actual shard via snowflake ID timestamp or ref probe.

### Canonical State for Hash Computation

Hash inputs must come from the internal canonical representation (deduplicated, registry-resolved), never from raw user input. Audit: `grep -rn 'ComputeNodeHash\|ComputeRelHash' pkg/` -- every call site must pass canonical labels/type.

### Event Bus (Copy-Then-Invoke + Safe Recovery)

The `EventBus` publishes lifecycle events (`EventNodeCreate`, `EventNodeUpdate`, etc.) synchronously. To prevent deadlocks when an event handler re-enters the Graph (e.g., to query the mutated entity), the bus copies the handler slice under `RLock` and invokes handlers *outside* the lock. Handlers are executed via `safeInvoke(h, e)` which defers `recover()` to isolate panics and logs them via `slog` without crashing the mutation caller.

**Tx event buffering:** During a transaction (`txEventBuffer != nil`), `publishEvent` appends to a buffer instead of dispatching. On `Commit`, events are published after `g.mu.Unlock()` so handlers can safely call Graph read methods. On `Rollback`, the buffer is discarded.

**AsyncEventBus:** `NewAsyncEventBus(config)` provides async delivery via a worker pool with per-priority `[5]chan Event` queues. `BackpressureStrategy` controls full-queue behavior (Block/DropOldest/DropLatest). Workers drain in Critical->High->Normal->Low->Deferred order.

**EventPriority:** 5 levels -- `PriorityNormal` (0, zero value), `PriorityHigh` (1), `PriorityCritical` (2), `PriorityLow` (3), `PriorityDeferred` (4). Graph assigns internally: creates->High, deletes->Critical, updates->Normal.

### Type-Tagged MsgPack Serialization

To reverse MsgPack's type-destructive behavior (e.g., `int64` downcast to `int8`, `[]string` to `[]any`), `wire.go` stores a 1-byte type tag alongside every property value. This ensures absolute Go type fidelity across the persistence boundary, which is critical for deterministic hashing and schema validation.

---

## Version History

| Version | What changed |
|---------|-------------|
| v3.0.15 | Phase 1a-1d: UpdateNode/UpdateRel, version history, hash chains, bulk queries |
| v3.0.16 | Phase 1e: FlushInterval policy, LRU evictClean O(1) early exit |
| v3.0.17 | Phase 1f: Batch operations (PutNodesBatch, BatchBuilder) |
| v3.0.18 | Phase 1g: Context-aware operations (8 WithContext methods) |
| v3.0.19 | Phase 2b: Hash chain verification (VerifyNodeHashChain, VerifyRelHashChain) |
| v3.0.20 | Phase 2d: Per-label/per-type statistics (initially O(N), later upgraded to O(1)) |
| v3.0.21 | Phase 2a: Temporal queries (GetNodesValidAt, Snapshot, etc.) |
| v3.0.22 | Phase 2c: Property indexes (CreatePropertyIndex, auto-maintenance) |
| v3.0.23 | Phase 2 review: 6 fixes (truncation resilience, 3-phase index lock, delete preserves history, history-aware temporal queries) |
| v3.0.25 | Phase 2h: 5 architectural fixes (entity locks, ID-only queries, LockMany, cascade index cleanup, Snapshot vs Batch isolation) |
| v3.0.26 | Phase 3a: TieredStore foundation (ontology, shard catalog, ref+hot event, cross-shard rels, 52 tests) |
| v3.0.27 | Phase 3b+3c: Shard rotation, warm tier, depth-aware reads, E->E cross-shard fix |
| v3.0.28 | Phase 3d: Cold shard lifecycle, parallel queries, reference archive, idle-close |
| v3.0.29 | Phase 3e: Admin API, repair tool, verification caching, ID decomposition, migration |
| v3.0.30 | 5 bug fixes: checkout/checkin race, cold shard skip, archive rollback, dirty-map tracking, canonical hash |
| v3.0.31 | OOM fix: ForEach iterators for temporal pipeline (~83% memory reduction) |
| v3.0.54 | Phase 4.22: `AllowSelfLoops bool` in `ValidationLimits`, `ErrSelfLoop` sentinel, guard in `AddRelationshipWithContext`/`ImportRelationshipWithID` |
| v3.0.55 | Phase 4.23: `DeleteNodeWithHistory`/`DeleteRelWithHistory` atomic compound store methods; `deleteNodeLocked`/`DeleteRelationshipWithContext` rewritten to single store call |
| v3.0.56 | 7 production defects: cold shard race, pagination, batch rollback, disk I/O under lock, ImportGraph lock, DropOldest livelock |
| v3.0.57 | 6 production defects (A-F): rollback panic leak, RemoveNodeLabel history, DiffSnapshots lock removal, searchNearest heap, constraint alloc, TOCTOU backoff |
| v3.0.58 | Temporal index lazy sort (Fix G), extraLabels nil invariant (Fix H) |
| v3.0.59 | Tx isolation (internal/external method split), event buffering in tx, directory fsync, dead code removal |
| v3.0.60 | Temporal index concurrent sort race fix (sortMu), NodesByLabel temporal fast path nil fallthrough fix, tutorial 005 upgrade |
| v3.0.61 | 6 defects: batch lock-leak on panic, negative config validation, BadgerStore invalid config, auth level float truncation, RemoveNodeLabel crash-consistency, store API test coverage |
| v3.0.62 | 5 production defects (U-Y), module rename tkg-v3 → tkg/v3, documentation refactor to docs/ |
| v3.0.63 | Caller-provided temporal metadata via `tkg_` props (`tkg_valid_from`, `tkg_valid_to`, `tkg_created_at` in AddNode/AddRelationship props map), tutorial 002 update |
| v3.0.64 | `AddRelationshipByID` / `AddRelationshipByIDWithContext` — high-throughput relationship creation by endpoint snowflake IDs |
| v3.0.65 | `AddRelationshipByIDIfAbsent` / `AddRelationshipByIDIfAbsentWithContext` — atomic check-then-create for relationships |
| v3.0.66 | `GraphTx.GetNode`, `GraphTx.AddRelationshipByID`, `GraphTx.AddRelationshipByIDIfAbsent` for transactional graph writes |
| v3.0.67 | Cross-shard incoming relationship type filter fix — `inIdx` changed from `map[ID]struct{}` to `map[ID]uint16` (relID → typeToken) |

---

## File Map (`pkg/graph/`)

| File | Purpose |
|------|---------|
| `graph.go` | Graph struct, Config, entity management, registries, entity locks, `ValidationLimits` (incl. `AllowSelfLoops`), `ErrSelfLoop`, lifecycle |
| `store.go` | Store interface (59 methods), QueryOpts, ShardDepth, sentinel errors, `RelTombstone`, `DeleteNodeWithHistory`/`DeleteRelWithHistory` |
| `memorystore.go` | In-memory Store with hash-set indexes, O(1) counts, ForEach iterators |
| `badgerstore.go` | Persistent Store: Badger v4, LRU caches, async flush, in-memory indexes, ForEach iterators |
| `lru.go` | Generic LRU cache with dirty tracking, tombstones, Peek |
| `entity_locks.go` | 256-shard mutex array, LockTwo, LockMany |
| `keys.go` | Binary key encoding (9 prefix tags, big-endian IDs) |
| `integrity.go` | SHA-256 hash chain computation and verification |
| `wire.go` | Type-tagged Msgpack serialization preserving Go type fidelity |
| `shadow.go` | 21 `tkg_*` virtual property resolvers |
| `label_registry.go` | Thread-safe label string <-> uint16 token registry |
| `reltype_registry.go` | Thread-safe reltype string <-> uint16 token registry |
| `batch.go` | BatchBuilder fluent API |
| `context.go` | `*WithContext` exported wrappers (acquire `g.mu.RLock`) + `*Internal` unexported implementations (lock-free), `ValidationLimits` enforcement (incl. `ErrSelfLoop`), two-phase delete with TOCTOU retry |
| `export.go` | `ExportGraph`/`ImportGraph` — length-prefixed msgpack record stream with 1-byte type tags; format-versioned, forward-compatible |
| `events.go` | `EventType` (6 constants), `Event` (with `Priority EventPriority`), `EventBus` (sync), `AsyncEventBus` (worker pool + `BackpressureStrategy`), `EventPriority` (5 levels), `eventPublisher` interface, tx event buffering |
| `temporal.go` | Temporal queries, GraphSnapshot, forEachKnownNodeID/forEachKnownRelID |
| `temporal_allen.go` | Allen's interval algebra graph integration — `NodeInterval`, `RelInterval`, `RelateNodes`, `RelateRels` |
| `temporal_constraint.go` | Write-time temporal boundary enforcement (e.g., endpoints must outlive relationships) |
| `temporal_filter.go` | Store-level temporal push-down helpers (entityValidFrom, matchesTemporalFilter) |
| `temporal_index.go` | In-memory interval index — sorted-slice with lazy sort (`sortIfDirty` + `sortMu`), temporal overlap queries |
| `tx.go` | GraphTx — full CRUD transaction holding graph write lock, snapshot-based rollback |
| `property_index.go` | In-memory property indexes with auto-maintenance |
| `txtime.go` | Bitemporality — `GetNodeAsOf`, `GetRelAsOf`, `GetNodesAsOf`, `GetRelsAsOf`, `ErrNoVersionAsOf` |
| `hf_index.go` | High-frequency temporal index — time-bucketed `highFrequencyIndex`, O(1) amortized insertion |
| `stats.go` | `GraphStats` (8 operation counters + 4 cache metrics), `StoreStats` optional interface |
| `vector_index.go` | In-memory brute-force k-NN vector indexing via Cosine/Euclidean distance |
| `pagination.go` | Cursor-based pagination helper (binary search on sorted ID slices) |
| `ontology.go` | EntityClass, OntologyMapping (label -> ref/event classification) |
| `shard_catalog.go` | ShardCatalog, ShardEntry (JSON persistence, atomic write) |
| `registry_file.go` | Flat msgpack registry save/load (atomic rename) |
| `badgerstore_partial.go` | Split rel write/delete helpers for TieredStore cross-shard routing |
| `tieredstore.go` | TieredStore core: config, shard model, routing, rotation, lifecycle |
| `tieredstore_write.go` | TieredStore writes: cross-shard rels, archive/restore, property index restriction |
| `tieredstore_read.go` | TieredStore reads: merge queries, parallel shards, ForEach sequential iteration |
| `tieredstore_admin.go` | Admin API: ForceRotate, ListShards, RebuildCatalog, VerifyShard |
| `tieredstore_repair.go` | RunRepair: cross-shard split-write consistency repair |
| `tieredstore_migrate.go` | MigrateFromBadger: single-store to tiered migration |
| `id_decompose.go` | DecomposeID: extract creation time, node ID, sequence from snowflake bits |
| `doc.go` | Package documentation |
