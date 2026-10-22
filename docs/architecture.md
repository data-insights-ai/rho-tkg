# Architecture — tkg/v4 (v4.70.0)

Temporal Knowledge Graph v4 is a pure Go library providing the core graph engine for temporal knowledge graphs. It is the low-level storage and type layer — no main binary, no HTTP server, no query language.

```
Module:  github.com/data-insights-ai/rho-tkg/v4
Go:      1.26.9
License: Apache-2.0
```

---

## System Overview

```
                        Graph
          +---------------+----------------+----------------------+-------------------+
          |               |                |                      |
    labelRegistry   relTypeRegistry  propertyKeyRegistry   entityLockManager
                          |                                       (256 shards)
                        Store
                          |
            +-------------+-------------+-------------+
            |             |             |             |
       memory.Store  badger.Store  tiered.Store  sharded.Store
                                       |          (EXPERIMENTAL,
                                       |           N badger slots)
                     +-----------------+-----------------+
                     |                 |                 |
                  refShard        refArchive      eventShards
                (badger.Store)  (lazy-open)    map[string]*EventShard
                                                 +-- hot  (read-write)
                                                 +-- warm (writable owner)
                                                 +-- cold (lazy-open, idle-close)
```

**Graph** is the sole external API. It owns the registries, dual snowflake generators, the Store, and an entity lock manager. All string resolution, referential integrity, and temporal query logic live here. When a successful graph operation creates new label or relationship-type tokens, the graph layer saves the updated registries to persistent stores before the method returns; import and transaction rollback save restored registry snapshots as part of rollback. Batch execution treats registry checkpoint failures after a row write as committed-row errors: the error is surfaced, but returned entity pointers keep the finalized state that matches the written rows. Transaction create/import paths log committed rows for rollback even when the trailing registry checkpoint fails, and Commit retries the checkpoint before publishing events and releasing the transaction.

**Registry zero values preserve token invariants.** Internal label and relationship-type registries lazily initialize the reserved token-0 entry, so direct zero-value use reports an empty registry, exports/imports the reserved-name shape, and allocates token 1 first.

**Store** is a pure persistence interface composed from capability sub-interfaces in `pkg/graph/store/capabilities.go`. The mandatory composition (`MandatoryStore` — Lifecycle, NodeCRUD, RelationshipCRUD, Adjacency, BulkRead, Batch, History, Stats, Iteration) is what the graph layer depends on. Optional capabilities are type-asserted at the call sites that need them; the list below is illustrative — the `*Capability` interfaces in `capabilities.go` and its siblings (`changefeed.go`, `history_stamps.go`, `current_stamps.go`, `member_ids.go`, `property_stats.go`, `rel_segments.go`, …) are the authority: DepthHistoryIteration, DeletedIteration, DepthDeletedIteration, ExAdjacency, PropertyIndex, TemporalIndex, VectorIndex, VectorIndexOptions, FilteredVectorSearch, HighFrequencyIndex, CompositePropertyIndex, RelPropertyIndex, MetaKV, ChangeFeed, TransactionTimeQuery, HistoryCompaction, RetentionPurge, PreEncodedPut, Degree, BeliefWatermark, TemporalCandidate, DurableFlush, HistoryPresence, HistoryStamps, CurrentStamps, MemberIDs, Rel/NodePropertyTxMembership, PropertyTxMembershipStats, RelSegment, …. `ExAdjacencyCapability` yields, per node and direction, the relationships that named the node in a row its live adjacency no longer holds (deleted ones, earlier lives of re-imported ones), kept by the store with every delete and every history row with other endpoints; the graph layer uses it for the adjacency coverage fold (`g.Temporal().OutgoingRelsAt`/`IncomingRelsAt`/`NeighborsAt`/`ForEachRelEffectiveAtNodes`, the pinned adjacency doors) so cost is O(the seed's ever-degree) instead of O(total history). `DeletedIterationCapability` (and the depth variant) yields IDs with history rows but no current row. Four in-tree implementations satisfy the full composition: `memory.Store` (testing), `badger.Store` (single-instance persistent), `tiered.Store` (multi-shard persistent), and `sharded.Store` (slot-topology persistent — EXPERIMENTAL, ADR-0007; ADR texts are in git history, `git log --all -- docs/adr/`; ADR-0011 is summarized in its own section below). Nil concrete in-tree Store receivers return `ErrNilStore` from lifecycle `Close` and `Clear` calls. The `memory.Store` zero value is usable; persistent Store zero values fail closed with `ErrStoreClosed`.

**Store registry inputs are lifecycle-checked.** BadgerStore and TieredStore registry save/load APIs reject nil label or relationship-type registry pointers with `ErrInvalidStoreMutation` before dereference on open stores. Closed stores still return `ErrStoreClosed` first. `tiered.Store.SetLabelRegistry(nil)` is a no-op so direct Store callers cannot accidentally clear ontology routing state. Tiered `registry.msgpack` loads validate both label and relationship-type slices before returning metadata to startup/load or deprecated single-registry save paths.

**Store indexed identity is enforced at mutation boundaries.** Current-row put/replace, delete/cascade, archive/restore, batch write/delete, and relationship endpoint writes reject zero or negative node IDs, relationship IDs, relationship endpoints, zero node-label tokens, and zero relationship type tokens with `ErrInvalidStoreMutation` before routing, endpoint lookup, or mutation. Graph update, label mutation, property CAS, close-version, delete, admin archive/restore, and tx/batch variants validate zero/negative target IDs before entity lookup, locks, snapshots, capability checks, or queueing, so invalid targets do not collapse into not-found results. Graph relationship create/import paths validate endpoint IDs before endpoint locks, duplicate probes, constraints, or relationship-type token allocation. Badger split relationship helpers enforce the same positive-ID index tuple before cross-shard entity/out or incoming-index writes and deletes, and repair-only incoming-index deletion and orphan-purge helpers reject invalid end-node or relationship IDs before scanning. Repair incoming-index cleanup uses the Badger orphan-index purge path, so local `inIdx`, `outIdx`, `typeIdx`, pending/persisted keys, `relIDs`, and counters are cleaned together once the relationship row is absent everywhere. Atomic history replacements validate nil current/history payloads before dereference or marshaling. Node replacements and node history replacements must keep the original label-token sequence; label changes use the dedicated label-token mutation APIs that update label indexes and tier routing. Plain Store `DeleteNode` and `DeleteNodesBatch` remove only unconnected node rows and return `ErrInvalidStoreMutation` without mutation if relationships are still attached. Tiered plain cascade skips stale adjacency entries whose relationship row is already missing and purges those local indexes during final shard-local node removal. Badger orphan relationship cleanup, tiered relationship routing, and repair shard probes check the relationship row itself, not only index-derived membership. Those label-token APIs also verify the supplied current row adds or removes exactly the requested token before touching label indexes, history, caches, or vector indexes. Relationship replacements, relationship history replacements, relationship delete tombstones, and relationship tombstones supplied to node cascade deletes must keep the original relationship ID, type token, start node, and end node. Node cascade tombstone sets must also cover every live connected relationship exactly once and must not include unrelated relationships. The Store returns `ErrInvalidStoreMutation` before touching current rows, history, or indexes if those fields change. History snapshot payload IDs must also match their storage keys.

**Explicit adjacency targets are all-or-error.** `OutgoingRelationships`, `IncomingRelationships`, and the batched `ForNodes` variants validate that every requested node row exists before using adjacency indexes as result sources. Existing nodes with no matching relationships return empty results, but missing requested nodes return `ErrNodeNotFound` rather than being collapsed into an empty neighborhood or omitted from a partial batch map. `g.Temporal().NeighborsAt` is the history-aware wrapper: it validates target existence at the queried instant first, then treats current-adjacency `ErrNodeNotFound` as no current candidates so relationship history can still answer for deleted target nodes.

**History read targets are explicit IDs.** Entity-specific history reads and truncation (`Get*Version`, `Get*History`, `Truncate*History`) reject zero or negative node/relationship IDs with `ErrInvalidStoreMutation` before version-not-found, empty-history, or no-op retention handling. Graph `g.Nodes().History` and `g.Rels().History` enforce the same positive-ID boundary before delegating to the Store.

**Store index definitions require real targets.** Store-level property, temporal, high-frequency, and vector index create/drop/search operations reject label token 0 with `ErrInvalidStoreMutation`. Property/vector index definitions, graph-level stored-property queries (`NodesByLabelAndProperty`, `NodesByLabelProperty*`, `RelsByTypeProperty*`), and temporal label lookups reject malformed label/type/property targets before treating them as empty namespaces. Property/vector queries reject reserved `tkg_` property keys with `types.ErrReservedPrefix`, because shadow properties are graph-resolved metadata and are not stored in `PropertySlice`. Query methods validate limits, cursors, and active temporal intervals before empty namespace shortcuts; negative `Limit` returns `ErrInvalidQueryLimit`, negative `After` returns `ErrInvalidQueryCursor`, and `ValidStart >= ValidEnd` returns `ErrInvalidTimeRange` instead of widening to all data or matching open-ended entities. BadgerStore and TieredStore also validate persisted index definitions on open, so a durable token-0 or shadow-key definition cannot become an active empty namespace after restart. BadgerStore fails open-time loading if persisted property, temporal, high-frequency, or vector index-definition MsgPack cannot be decoded. MemoryStore and BadgerStore public read/write/history/index APIs return `ErrStoreClosed` after close rather than serving stale in-memory state; Badger no-error diagnostics return zero values, and MemoryStore exported test-only tampering helpers return no data or no-op. TieredStore index create/drop/search calls, direct read/count/bulk/history APIs, empty batch/history write edges, and public admin/metadata operations also fail with `ErrStoreClosed` after close starts before touching shard handles, store-level vector metadata, reference-shard read state, catalog metadata, or registry files. Vector search paths re-check close state after in-memory vector scans/filter callbacks before returning empty or raw-ID results.

**Tiered temporal index kind is a store-wide invariant.** `tiered.Store` serializes temporal and high-frequency index create/drop against shard rotation, preflights shard-local index kind before mutating, persists store-level temporal/HFI tracking metadata, and treats `ErrTemporalIndexExists` as retryable only when the existing shard index is the same kind and HFI bucket size. HFI bucket sizes must be positive whole milliseconds because buckets are stored in `Instant` precision. A partial same-kind create can be completed; a partial interval/HFI cross-kind or different-bucket HFI leftover fails closed instead of creating mixed per-shard temporal index state. If a later shard fails during create, drop, tracking-file persistence, tracked-index application to a newly opened shard, or persisted tracking replay on open, earlier shard-local changes are undone before returning the error.

**Vector indexes** live at the Store level (per-Store in `memory.Store`/`badger.Store`, per-`tiered.Store` -- not per-shard). In-memory k-NN with Cosine and Euclidean distance metrics. Two engines: `CreateVectorIndex` defaults to the approximate HNSW engine (`pkg/graph/internal/index/hnsw.go`, `M=16`, `EfConstruction=200`, `EfSearch=64`), and `CreateVectorIndexWithOptions` (`VectorIndexOptionsCapability`) selects the tuning or switches to the exact linear-scan engine via `VectorIndexOptions.UseBruteForce` — the escape hatch for exact-recall requirements. Brute force also remains the correctness fallback for filtered searches. Protected by a dedicated `vectorIdxMu sync.RWMutex` in tiered stores and the store index mutex in single-shard stores. BadgerStore and TieredStore persist index definitions and rebuild entries from node properties on open; MemoryStore remains process-local. Definition serializers skip phased-create placeholders until backfill finalization clears their build marker, so concurrent metadata writes cannot durably publish unfinished indexes. Tiered vector definition create/drop snapshots the raw definition file before persisting, restores/removes it on write failure, and fsyncs the metadata directory after deleting the last definition. Tiered catalog saves also snapshot and restore/remove the raw JSON file on write failure, matching caller-level in-memory catalog rollback. Auto-maintained across all mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`), including delete paths that mutate state and return a corruption error after cleanup. Creation and config validation: "Vector Index Creation" below. Search applies Store-level temporal/depth eligibility before heap selection, resolves ranked IDs back to nodes, applies cursor pagination over distance order, and skips only `ErrNodeNotFound` from concurrent deletes; other read errors propagate. Heap allocation is capped by indexed entry count, so oversized `k` values do not allocate proportional memory on tiny indexes. Tiered vector search applies `DepthHot`/`DepthWarm` before heap selection by combining reference/archive residency with event-shard tier snapshots.

**Internal no-error index helpers fail closed on nil or zero values.** Property indexes lazily initialize value maps on first `Add`; high-frequency indexes lazily initialize buckets on first `Add`; zero-value LRU caches lazily initialize as minimum-capacity caches. Nil property, temporal, high-frequency, and vector cleanup/read helpers no-op or return zero/nil values instead of panicking. Nil vector writes/searches return `ErrInvalidVectorIndexConfig` because they have an error channel.

**Tiered primary-label class is immutable.** Any TieredStore write that replaces a node or changes label tokens must preserve the node's reference/event class. `ErrPrimaryLabelClassMutation` prevents live rows and history snapshots from being routed to different shard families.

---

## Core Types (`pkg/types`)

| Type | Size | Purpose |
|------|------|---------|
| `Node` | 96B | Graph vertex. `nodeID` (wraps `snowflake.ID`), primary + extra labels as `labelToken`, properties, `uint32` version, temporal, integrity (or, on a store's cached first version, one compact metadata object — `CompactFrozenCopy`) |
| `Relationship` | 96B | Directed edge. `relID` (wraps `snowflake.ID`), `relTypeToken`, start/end as `nodeID`, properties, `uint32` version, temporal, integrity (or, on a store's cached first version, one compact metadata object — `CompactFrozenCopy`) |
| `PropertySlice` | var | Sorted key-value store with binary search. Recursive exact-type allowlist validation aligned with hash/copy/wire support, depth-limited to 32 levels. `tkg_` prefix reserved. `Set` and entity `SetProperty` methods deep-copy accepted reference values and reject invalid post-copy custom values; `SetProperties` methods canonicalize and deep-copy direct slices before installing them |
| `Instant` | 8B | Unix-millisecond timestamp. All temporal fields use this type |
| `TemporalMetadata` | var | ValidFrom, ValidTo, TxFrom, TxTo, CreatedAt, UpdatedAt, DeletedAt, CreatedBy, UpdatedBy, BaseEntityID |
| `NodeIntegrity` / `RelIntegrity` | var | SHA-256 hash chain + provenance/authorization: `Hash`, `PrevHash`, `AuthorID string`, `Signature []byte`, `AuthorizedBy string`, `AuthorizationLevel uint8`. `RelIntegrity` additionally holds `FromNodeHash`, `ToNodeHash` (endpoint cross-references at write time) |

**Key invariants:**
- All struct fields unexported. Access through methods only.
- Packed by descending field alignment (verified with `unsafe.Sizeof`).
- `ExtraLabelTokens()`, `Properties()`, `DeepCopy()`, `ToMap()` always return independent copies; `NewPropertySlice` and single-property setters also copy accepted reference values before storage and validate registered custom values after `DeepCopyValue`.
- `Temporal()` and `Integrity()` return internal pointers (no copy) on unfrozen entities and independent copies on frozen (store-cached) rows.
- Token 0 is reserved/invalid. `HasLabelToken(0)` always returns false.
- Graph-level registry resolution, import, and persistence rehydration enforce the same empty/whitespace and `MaxNameLength` checks as mutation/query paths before accepting label or relationship-type names; non-error lookup helpers fail closed for malformed names.

### Shadow Properties (21)

Read-only virtual properties dispatched by the graph layer from internal metadata. Never stored in `PropertySlice`; `PropertySlice.Set()` rejects any key with the `tkg_` prefix.

| Key | Type | Applies To | Category |
|-----|------|------------|----------|
| `tkg_labels` | `[]string` | Node | Structural |
| `tkg_type` | `string` | Relationship | Structural |
| `tkg_valid_from` | `Instant` | Both | Temporal — world-time (VT) assertion, caller-only, NO fallback: resolves to `(Instant(0), ok=true)` when never asserted |
| `tkg_valid_to` | `Instant` | Both | Temporal — world-time (VT) assertion, caller-only, NO fallback |
| `tkg_tx_from` | `Instant` | Both | Temporal — transaction time (TX), stamped by the system on every Add; caller-settable as a property on CREATE doors only under `Config.AllowTxBackfill` (backfill, §4.1); under the same gate `UpdateWithTx`/`DeleteWithTx` take a caller instant as an argument for a supersession or an ending (`ErrTxOrder` when it does not follow the chain) |
| `tkg_tx_to` | `Instant` | Both | Temporal — transaction time (TX) |
| `tkg_created_at` | `Instant` | Both | Temporal (auto-derived from snowflake ID when unset — the only temporal shadow key with a resolver fallback) |
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

`tkg_valid_from` / `tkg_valid_to` are also accepted by `UpdateInPlace`, which rewrites the current row's TemporalMetadata directly (no history entry, no TxFrom bump): a bitemporal read pinned between the original write and an in-place correction sees the corrected value; callers needing the pre-correction belief use `Update`. Resolved shadow values keep their public wrapper types (`tkg_base_entity` returns `types.EntityID`, not raw `snowflake.ID`).

Caller-supplied provenance/authorization shadow inputs are validated before they are stripped from the property map: `tkg_author_id` and `tkg_authorized_by` require `string`, `tkg_signature` requires `[]byte`, and `tkg_auth_level` requires an integer value in `[0, 255]` (integer or whole-number float types).

Caller-supplied temporal shadow inputs are also stripped before property validation. `tkg_valid_from`, `tkg_valid_to`, and `tkg_created_at` accept millisecond values as `types.Instant`, safe integer types, or whole-number floats inside each float type's contiguous exact integer range.

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
- Epoch and layout live once, in `pkg/internal/idlayout` (shared by `pkg/types` and `pkg/graph/internal/**`; `internal/snowflake` re-exports them)
- Never derive an ID-based Instant anywhere except `idlayout.MintInstantMillis` (the resolver `storeutil.EntityValidFrom` / `SnowflakeInstant` and the `tkg_created_at` fallback call it); store routing by time window and lock sharding read the time bits for placement, not for an Instant
- Creation timestamp extractable via `snowflakepkg.Layout.Decompose(id).Time` or `DecomposeID(id).CreatedAt`; the public millisecond form is `types.NodeID.MintInstant()` / `types.RelID.MintInstant()`, the last fallback of a row's valid-from (api.md "Valid-time start (Config.UntimedValidStart)")

---

## Graph Layer (`pkg/graph`)

### Entity Management

| Method | Locks | Description |
|--------|-------|-------------|
| `g.Nodes().Add(ctx, labels, props)` | `g.mu.RLock()` | Validate, generate ID, compute hash (genesis), store |
| `g.Rels().Add(ctx, type, start, end, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Validate endpoints exist; reject self-loops when `AllowSelfLoops=false` (`ErrSelfLoop`); generate ID, hash, store |
| `g.Nodes().Update(ctx, id, updates)` | `g.mu.RLock()` + `LockEntity(id)` | Validate update map, deep-copy pre-mutation, apply updates, checked version bump, `ReplaceNodeWithHistory` |
| `g.Rels().Update(ctx, id, updates)` | `g.mu.RLock()` + `LockThree(relID, startID, endID)` | Validate update map, deep-copy pre-mutation, apply updates, checked version bump. Locks both endpoints (in addition to the rel itself) so the FromNodeHash/ToNodeHash refresh cannot interleave with a concurrent endpoint node update (R4-F7) |
| `g.Nodes().Delete(ctx, id)` | `LockMany(node + all rels)` | Two-phase TOCTOU: read adjacency, lock all, re-read node and adjacency, build tombstones from the locked Phase B rows, single atomic `DeleteNodeWithHistory` call |
| `g.Rels().Delete(ctx, id)` | `LockEntity(id)` | Build tombstone, single atomic `DeleteRelWithHistory` call |
| `g.Rels().AddByID(ctx, type, startID, endID, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Create relationship using endpoint snowflake IDs directly. Live endpoints are fetched under the endpoint lock, `FromNodeHash`/`ToNodeHash` are captured, and graph-level constraints are enforced with the same semantics as `g.Rels().Add` |
| `g.Rels().AddByIDIfAbsent(ctx, type, startID, endID, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Atomic check-then-create: returns existing relationship if same type+endpoints already connected, otherwise creates. Same constraint behaviour as `AddByID`. Returns `(rel, created, err)` |
| `g.Nodes().Get(ctx, id)` | none (read-only via store) | Retrieve a single node by snowflake ID |

`UpdateWithTx(ctx, id, updates, txFrom)` / `DeleteWithTx(ctx, id, txTo)` (nodes and rels, plus the `GraphTx`, `BatchBuilder` and ingest `Session` twins) take the same locks as `Update` / `Delete` and stamp a caller transaction instant (gate `Config.AllowTxBackfill`). Under the entity lock the instant must lie after every `TxFrom` / `TxTo` of the chain and after the current version's start, else `ErrTxOrder` (wraps `ErrInvalidTxFrom`); batch and ingest units run that check as one pre-flight over the unit before any write. Every appended row takes its version from one allocator (`internal/core/version_alloc.go`: one above the highest version stored for the entity, current row and history). `Retract` / `RetractWithTx` (and the twins) are the delete doors with one more bit: the tombstones they write carry `TemporalMetadata.Retracted`, and the chain resolver drops every row of a life a retraction recorded by the pin ended before it selects anything (`dropRetractedLives`, `chain_resolver.go`; the effective-timeline sweep does the same), so that life answers at no valid time and shapes no other life — `Delete` ends validity, `Retract` ends belief. `g.Nodes().HasHistory` / `LatestStamps` (and the rel mirrors) answer from the optional `store.HistoryPresenceCapability` / `store.HistoryStampsCapability` sidecars without reading history rows (`history_presence.go`, `latest_stamps.go`; a store without them is answered from `History`).

All mutations enforce `ValidationLimits` (5 configurable limits with defaults). Update-style paths extract provenance/authorization shadow keys, reject other reserved `tkg_` keys, and validate update values before entity locks, transaction rollback snapshots, or batch queueing. They also recheck final property count after nil deletes/adds/sets are applied and before persistence. Versioned mutations use a checked next-version helper and return `ErrVersionOverflow` at `math.MaxUint32` before history writes, wrapped version `0`, or label-token allocation for rejected add-label calls; version-chain successor lookup also treats `math.MaxUint32` as having no successor instead of wrapping to genesis, and version-chain navigation validates explicit IDs before returning `nil, nil` for a missing neighbor version. Batch update queues deep-copy caller maps after validation so later caller mutations cannot change `Execute`. Add-label paths check node existence, idempotence, and `MaxLabelsPerNode` before creating a token for an unseen label. Node create/import/add-label write failures restore any newly allocated label tokens, and multi-label allocation failures restore partial suffixes before returning. Batch node queueing uses non-zero probe tokens for unseen labels and allocates real tokens only during `Execute`, retokenizing returned node pointers in place before persistence. Batch relationship create failures restore queue-time `TxFrom`, endpoint hashes, and type-token state on the returned relationship pointer. Direct relationship create paths run temporal constraints before allocating a token for an unseen relationship type and restore newly allocated type tokens on final write failure. `CloseVersion`, node label add/remove, node/relationship property CAS, and node/relationship in-place updates recompute hash-chain fields while preserving existing provenance, signature, authorization, and relationship endpoint-hash metadata because those APIs have no provenance shadow-key channel. Registry rollback windows release their allocation mutex via `defer`, so backend panics do not strand future registry writes. Every mutation door is ctx-first and adds cancellation checks at critical points — v4.0 collapsed the old `*WithContext` / non-context pairs into single `ctx`-taking methods.

### Sub-API Accessors

The implementation methods on `*core.Core` are reachable through 16 nil-safe sub-API accessor methods on `*Graph` (`(*Graph)(nil).Nodes()` returns nil and chained calls fail closed with `ErrNilGraph`). The thin `*Graph` façade (`pkg/graph/graph.go`) exposes only `New`, `Close`, `SetReplicationSource`, the 16 accessors below, and the columnar scans `ScanNodeColumns` / `ScanRelColumns` / `ScanRelSegments` (`pkg/graph/column_scan.go`); the package-level `Open`, `OpenInMemory`, `RestoreInto`, `NewBatchBuilder`, `DecomposeNodeID` and `DecomposeRelID` helpers live alongside it. `*core.Core` is strictly internal.

| Accessor | Package | Exported methods (v4.70.0) | Wraps |
|---|---|---|---|
| `g.Nodes` | `pkg/graph/nodes` | 73 | Node CRUD (incl. `AddWithTx` / `UpdateWithTx` / `DeleteWithTx`), label, property, version chain, `HasHistory` / `LatestStamps`, DocValues, streaming ForEach/Iter |
| `g.Rels` | `pkg/graph/rels` | 75 | Relationship CRUD (incl. the `WithTx` doors), adjacency (incl. pinned `*ForNodesAtTx` / `*AtPin`), property, version chain, `HasHistory` / `LatestStamps`, streaming ForEach |
| `g.Temporal` | `pkg/graph/temporal` | 83 | Point-in-time, interval, bitemporal (record and state doors), effective timeline (+ scan forms), version-interval corrections, commit clock (`NowTx` / `CommittedTx` / `PeekTx` / `AdvanceClock`), snapshot/diff, Allen relations, named as-of tags |
| `g.Index` | `pkg/graph/index` | 34 | Property / rel-property / composite / temporal / rel-temporal / high-frequency / vector index management, `InventoryEpoch`, IndexProvider registry, scored search |
| `g.Events` | `pkg/graph/events` | 4 | Sync/async EventBus install + retrieval; setters return `ErrGraphClosed` after graph close |
| `g.Constraints` | `pkg/graph/constraints` | 9 | Temporal-constraint set (`Set`/`Add`/`Get`, `DryRunValidate`; `Add`/`Set` reject unknown kinds before changing the set, and relationship writes fail closed with `ErrInvalidTemporalConstraint` if an invalid kind reaches enforcement) and the unique-property constraints (ADR-0002): `CreateUnique`, `CreateUniqueForever`, `ReleaseOwnership`, `DropUnique`, `UniqueConstraints` — six `ErrUnique*` sentinels re-exported from `pkg/graph` |
| `g.IO` | `pkg/graph/io` | 7 | Export / Import, deltas (`Watermark`, `ExportSince`, `ImportMerge`), backups (`BackupTo` / `BackupDeltaTo`); package-level `HeaderOf`. Shadows stdlib `io` — alias as `tkgio` |
| `g.Admin` | `pkg/graph/admin` | 15 | `Reset` (`Config.AllowReset`, else `ErrResetDisabled`), `SetUntimedValidStart` / `UntimedValidStart` / `ValidAxis`, `IsReferenceLabel`, `DecomposeNodeID` / `DecomposeRelID`, `CompactHistoryNodes` / `Rels` (ADR-0001), `PurgeExpiredNodes` / `Rels` (ADR-0008, `Config.AllowRetentionPurge`, else `ErrRetentionPurgeDisabled`), `ExactErase` (`Config.AllowExactErasure`; preview `ResolveExactErasure`), `SealRelSegments` / `RelSegmentStats` (ADR-0011) |
| `g.Tier` | `pkg/graph/tier` | 7 | Tiered-store admin: `Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `Repair`, `VerifyShard` (reuses `core.AdminOps`) |
| `g.Stats` | `pkg/graph/stats` | 30 | Counts and planner statistics (owner: `docs/query-planners.md`) |
| `g.Hash` | `pkg/graph/hash` | 2 | `VerifyNodeChain` / `VerifyRelChain`. Shadows stdlib `hash` — alias as `tkghash` |
| `g.Replication` | `pkg/graph/replication` | 11 | Change-log / op-log (`ChangeFeed`, `ForEachChange`, `LastCommittedLSN`, `Watch`), replica apply (`ApplyChange`/`ApplyChanges`, `AppliedLSN`/`SetAppliedLSN`), `RegistrySnapshot`, `IDSlotLease`/`SetIDSlotLease`; package-level `DecodeChangeIdentity` / `ChangeOpOf` |
| `g.Ingest` | `pkg/graph/ingest` | 3 | ADR-0006 prepare-parallel / apply-sequential write door: `NewSession`, `AppliedSeq`, `WaitApplied` |
| `g.Resolve` | `pkg/graph/resolve` | 2 | Shadow-property accessors (`NodeProperty`, `RelProperty`) |
| `g.Tx` | `pkg/graph/subapi.go` (`TxAPI`) | 4 | `Begin` / `Run` / `RunContext` / `RunWithLSN` |
| `g.Batch` | `pkg/graph/subapi.go` (`BatchAPI`) | 3 | `New` (a `*BatchBuilder`), `Run` / `RunContext` |

Each sub-API package declares its own local `Core` interface listing only the methods it forwards to. `*core.Core` satisfies each `Core` implicitly (no explicit declaration required), which lets sub-API packages stay independent of `pkg/graph/internal/core` while still type-checking the wiring at compile time. Wrappers are 1-2 lines, single indirect dispatch each. Nil, zero-value, or typed-nil wrappers fail closed: error-returning methods return `ErrNilGraph`, while no-error helpers return zero values. The `temporal`, `index`, and `events` sub-APIs share their package with the types-only declarations of the same name.

`g.Index` provider registration rejects nil and typed nil providers before
calling `Name()`, rejects empty or whitespace-only provider names before
mutating or probing the provider registry, and logs `OnEvent` errors as
diagnostics without vetoing already-committed mutations. Provider teardown waits
for in-flight `Initializable.Init` callbacks before invoking provider `Close`.

`g.Tx` and `g.Batch` are in-package types because their wrapped values (`*GraphTx`, `*BatchBuilder`) live in `pkg/graph/internal/core` and can't be moved into a sibling sub-API package without either re-homing the underlying types or accepting an import cycle.

### Temporal Queries

**Three timestamps, three claims (VT vs TX).** The bitemporal model rests on keeping these distinct:

| Timestamp | Claim | Who can assert it | State on an unstamped entity |
|---|---|---|---|
| `tkg_tx_from` | "the DB recorded this fact at T" | the system — automatically (or a privileged backfill under `Config.AllowTxBackfill`, §4.1) | always stamped: every Add allocates `TemporalMetadata` and sets `TxFrom` |
| `tkg_created_at` | "the entity record came into existence at T" | system-derived (snowflake ID timestamp); caller may override at Add | always derivable — the shadow resolver applies the snowflake fallback |
| `tkg_valid_from` | "the fact holds **in the world** from T" | only the domain — a recorder/curator with actual knowledge | no world-time claim made (stored `0` on the legacy valid axis; api.md "Valid-time start (Config.UntimedValidStart)") |

Two doors expose two deliberate views of valid-time. The shadow resolver (`g.Resolve().NodeProperty(n, "tkg_valid_from")`) returns the RAW asserted value — `(Instant(0), ok=true)` when never asserted (`ok` is true because `TemporalMetadata` always exists; check the zero value, not `ok`). Temporal queries use the EFFECTIVE valid-from (explicit `ValidFrom`, else the row's `TxFrom`; api.md "Valid-time start (Config.UntimedValidStart)"), so an entity with unset `ValidFrom` is "eternal" through the shadow door but time-bounded through the query door. Which start an untimed write stores, and whether a stored 0 is a claim, is the store's persisted `Config.UntimedValidStart` mode (`UntimedFromTxFrom`, `UntimedUnbounded` — the default for new stores —, or `UntimedWholeAxis` on the literal valid axis); owner: api.md "Valid-time start (Config.UntimedValidStart)". Shadow props report *stored/asserted* state; temporal queries report *effective* state. Writers must never default `tkg_valid_from := now()` without domain knowledge — that conflates TX with VT; consumers wanting "unstamped ⇒ valid since recorded" should implement it as an explicit, flagged heuristic on their side.

**Canonical temporal predicates.** The effective-valid-from derivation and the point/interval predicates are defined ONCE, in `storeutil` (`EntityValidFrom`, `MatchesPointInTime`, `MatchesInterval`); the core graph layer delegates its `nodeValidFrom`/`relValidFrom`/validity helpers there and the store backends use the same functions for query push-down. Never redefine these semantics elsewhere — the cross-door equivalence test (`TestTemporalTwoDoorsAgreeOnLabelQueries`) asserts the named door, the generic `QueryOpts` door, and the per-ID resolver return identical sets.

**Valid-time inheritance is cleared on every version boundary.** Every mutation that creates a new version by deep-copying the current one (Update, AddLabel/RemoveLabel, property mutations) clears the inherited `ValidFrom`/`ValidTo` before stamping, so `ValidFrom != 0` on a non-genesis version always means "caller-supplied" (lessons 33 and 42). Delete tombstones and `CloseVersion` set `ValidTo` deliberately (and `CloseVersion` stamps the version's start as `ValidFrom`) — closing semantics, not inheritance.

**Cascade interval edits (`SetNodeVersionInterval`) are append-only and not wire-atomic.** The cascade never rewrites a stored row (lesson 46). It appends, at one new `TxFrom`, one row per piece of `[validFrom, validTo)` where the pre-correction state differs (each piece = the version valid there, as believed before the call, plus the `props` patch; a piece with no valid version uses the current row, the most recent version), plus a resumption row that re-asserts the state at `validTo`. On overlap the newer belief wins at read time. Appended rows take versions above every stored row, carry no `TxTo`/`DeletedAt`, and the newest row whose own interval is open takes the current slot. Unique constraints judge the built rows (`unique_cascade.go`); a hard-deleted entity is refused with `ErrEntityDeleted` (wraps `ErrNodeNotFound` / `ErrRelNotFound`). The rows are written sequentially under the entity lock, the new current row last. A crash mid-cascade leaves some appended rows without the rest; each row write is atomic and there is no in-progress marker.

**TX visibility: superseded is not retracted (lesson 43).** `QueryOpts.TxAt` / `NodeAtTx` filter the version chain to versions RECORDED by txAt (`TxFrom <= txAt`). `TxTo` deliberately does not bound visibility: it marks when a version stopped being the current record, and the row remains the authority for its valid-time slot in every later belief state. (The previous `< TxTo` clause treated every supersession as a retraction, so `NodeAtTx(oldVT, now)` returned nothing after any update — including the flagship explicit-VT tiling scenario.) Belief reconstruction falls out of the vEnd derivation over the TxFrom-filtered chain: versions recorded after txAt are absent, so the then-latest version is open-ended exactly as believed at txAt. All of this lives in one funnel, `internal/core/chain_resolver.go`, which every named and generic temporal door uses. Two read-time caps complete it: a replacing write (Update, `CloseVersion`, a label change, a property CAS, `UpdateWithTx`) ends every older belief that started at or before its row's start, also on a chain a cascade made non-monotonic (`chain_supersession.go`); and a hard delete ends every row of its life (`lifeEnds`). A chain is ordered by life (a row's life = the deletes recorded before it), then version, so a re-imported ID's rows follow the earlier life's; a re-import continues the chain (version above every stored row, `PrevHash` linked to that row, stamped after every stamp of the chain; a backfilled `tkg_tx_from` at or below one returns `ErrTxOrder`; `lifeStart` in `version_alloc.go`).

**Record doors vs state doors.** `NodeAsOf` / `RelAsOf` / `NodesAsOf` / `RelsAsOf` and `QueryOpts.TxPin` are RECORD doors: they answer the newest row recorded by the pin, whatever valid time it covers (pin-stable: one rule, `storeutil.SelectAsOfWithCurrent`, in memory, the badger native scan and the core fallback; a row recorded later never changes the answer at an earlier pin). `NodeAtTx` / `RelAtTx`, `ByLabel` / `ByType` with `ValidAt` + `TxAt`, and the effective timeline are STATE doors: the row valid at `t` as believed at the pin. After a bounded correction `[2000,3000)` the record door answers the correction row at every valid time, the state door only inside `[2000,3000)`. The effective timeline (`effective_timeline.go`) is the state door's interval form: half-open segments, each holding the row `NodeAtTx(id, t, pin)` returns for every `t` in it, computed from one chain load with one max-heap sweep (O(n log n)) under the entity's lock; a pin above the commit clock (`PeekTx`) returns `ErrTxPinTooNew`.

**Bitemporal back-compat shims and their sunset.** Two shims remain for pre-4.3.0 data: the inherited-valid-from detector (`nodeInheritedValidFrom`, bypassed once the post-open migration has run — `bitemporalMigrated`) and the `UpdatedAt`-as-`vEnd` fallback for rows without explicit valid-time. Retirement condition: the detector can be deleted once every supported backend requires `MetaKVCapability` (so the migration always runs) — revisit at the next major version. The `UpdatedAt` fallback is permanent API surface: it is what gives unstamped entities TX-as-VT semantics. It lives in the one start rule `storeutil.EntityValidFrom` (backlog 62; `temporal_filter.go:19` over `types.ValidAxis.Start`) — a claimed `ValidFrom`, else `TxFrom`, else (legacy rows) `UpdatedAt`, else the mint instant — for creates and later versions alike; the resolver, the store push-down, the temporal index envelopes and the column filters all read it. The persisted envelopes keyed by the older mint start are rebuilt on the first open (open image format 2, 0x0B marker `temporal_index_on_disk_built_v2`); persisted column blobs are format 2 (an effective-start column).

All of the following are methods on `g.Temporal()` (`pkg/graph/temporal`):

Point-in-time: `NodesAt(t)`, `RelsAt(t)`, `NodesByLabelAt(label, t)`, `RelsByTypeAt(relType, t)`
Interval: `NodesDuring(start, end)`, `RelsDuring(start, end)`
Version-specific: `NodeAt(id, t)`, `RelAt(id, t)`
Traversal: `NeighborsAt(nodeID, t)`, `OutgoingRelsAt(nodeID, t)`, `IncomingRelsAt(nodeID, t)`
Snapshot: `Snapshot(t)` -- full graph state at time t (endpoint-filtered)
Combined: `NodesByLabelPropertyAt(label, key, value, t)`, `NodesByLabelPropertyDuring(label, key, value, start, end)`
Bitemporal (transaction time): `NodeAsOf(id, txTime)`, `RelAsOf(id, txTime)`, `NodesAsOf(txTime)`, `RelsAsOf(txTime)`, `NodeAtTx(id, validAt, txAt)`, `NodesAtTx(validAt, txAt)`, `NodesDuringTx(from, to, txAt)` (+ rel mirrors)
Effective timeline: `NodeEffectiveTimeline(id, pin)`, `RelEffectiveTimeline(id, pin)`, scan forms `ForEachNodeEffectiveByLabel(label, pin, fn)`, `ForEachRelEffectiveByType(type, pin, fn)`, `ForEachNodeEffectiveByLabels(labels, pin, fn)`, `ForEachNodeEffective(pin, fn)` / `ForEachRelEffective(pin, fn)`, `ForEachRelEffectiveAtNodes(nodeIDs, dir, relTypes, pin, fn)`, `NodesEffectiveByIDs` / `RelsEffectiveByIDs(ids, pin, fn)` (include entities deleted before the pin)
Corrections: `SetNodeVersionInterval`, `SetRelVersionInterval`

**Head-pin effective timeline (`internal/core/effective_head.go`, `store.HistoryEpochCapability`, backlog 68):** at a pin at or above every `TxFrom` of a chain without deletes the effective timeline does not depend on the pin, so the point doors keep it per entity as pieces naming their winner by version, valid while the store's per-entity history token (`storeutil.HistoryEpochs`: a watch table noted at the store's one history-write funnel, count-checked on memory, `noteHistoryKey` on badger, the slot on sharded, on tiered the largest of the walked shards' tokens and a shard-set token drawn anew at every shard-set change; tokens come from one increasing process-wide counter) and the current row's version and temporal block are unchanged. A correction (`cascadeNode` / `cascadeRel`) that finds a valid entry extends it by its appended rows when they cannot change how older rows resolve (`headOverlay`: newest beliefs, no TxTo, no replaced row, the resolver arm known — kept, or a monotonic chain of bounded rows turning non-monotonic; the appended rows paint over the pieces in one pass), else drops it. A head read then costs the current row, the token and one point read per segment. The cache and the watch tables are striped under read-write locks (a hit takes read locks only); the cache is bounded by pieces (1M per graph over 64 stripes; one entry keeps at most 2,048 pieces and 4,096 bounds, a share of its stripe — above that the newest ones, row 85). A miss gathers the entry's facts inside the fold's own walk (`effectivePiecesFacts`): no extra copy, sort or classification. Scans read through one snapshot and never use it. The differential oracle (`effective_head_test.go`) checks every answer against the fold.

**Corrections from the head entry (`internal/core/effective_head_write.go`, backlog 68b):** a correction (`cascadeNode` / `cascadeRel`: every `SetVersionInterval` and excise door) of an entity whose head entry is valid when the write starts reads no history: the entry holds what the correction needs of the chain — the highest version, the newest `TxFrom` / `TxTo` (a caller instant at or below them reads the history, so the refusal names the stamp), the belief-winner at every instant (its pieces), every row's own valid bounds (the resumption's end; at most 4,096 kept, else a correction below the kept ones reads the history) and the newest open, non-void belief (the current-slot choice) — plus one point read per base row and one for the resumption source. The GraphTx rollback snapshot of such an entity is its current row and the entry's highest version: the transaction's rows are the ones above it and the current row moved into history at its own version (a valid entry proves that key was free). Rollback trims above the highest version (memory, badger) and, only when the transaction moved the current row into history below kept rows, rewrites those rows around a trim at the current version; sharded and tiered (no rollback trim) rewrite the history from the rows read at rollback. `effective_head_write_twin_test.go` runs both paths on twin graphs (byte-identical rows on all four backends).

**Valid-end index (`internal/index/valid_end.go`, `store.RelValidEndCapability` / `NodeValidEndCapability`, backlog 51):** `ForEachRelEffectiveByTypeFrom` / `ForEachNodeEffectiveByLabelsFrom` stream the effective segments of the entities whose valid end is open or at or after `validFrom`. The memory and badger stores keep a RAM index per queried type or label that gives the scan a sound superset of candidates, so the entities read follow the answer and not the type's history (5 read with 10,000 retired entities). The bound per entity is folded from its rows only (each row's `ValidTo`, a delete tombstone recorded after the earlier stamps), never from the chain resolver: the scan reads each candidate's timeline and the resolver decides. A removed or overwritten row, or a write to an entity the scan refined (`RefineRelValidEnd` / `RefineNodeValidEnd`, keyed by the candidate's generation), makes the entity re-read on the next call. The index is per queried token: the first call tracks the type or label from the K1 membership sidecar, and writes of untracked tokens cost one map lookup. It is not persisted (opening a badger store already walks every current row). RAM about 96-133 B per tracked entity; first call after a reopen with 100,000 retired relationships about 1 s (later calls microseconds). A node is bounded by its whole timeline, so one that dropped the labels but lives on is read and skipped. An entity ended only by a valid-time close (`CloseVersion`, an update setting `tkg_valid_to`) keeps its open predecessor in the fold: it is read once and its exact end recorded until its next write or a reopen. Tiered unions its shards' indexes (backlog 58, below); sharded does not implement it and answers through the unbounded candidates.

**History-aware queries** include deleted entities. They use two-phase ForEach iteration:
1. **Collect** -- `ForEachNodeID` + `ForEachNodeHistoryID` insert unique IDs into a `seen` map. Store implementations invoke callbacks outside their internal locks and Tiered shard checkouts, so callbacks may re-enter Store methods.
2. **Process** -- iterate `seen` map after ForEach returns.

No ID slice is materialized (peak memory ~83% below the slice approach at 10M nodes).

`DiffCallback` applies the same lock-boundary rule at the graph layer: it collects known IDs and resolves each before/after entity pair under short `g.mu.RLock()` windows, then invokes caller `DiffHandlers` outside the graph lock. Handlers may call graph read APIs without deadlocking behind a waiting writer.

**Temporal semantics:**
- Start: `storeutil.EntityValidFrom` (above); "open end" is the store's valid axis (`ValidAxis.Open`: `ValidTo == 0` on the legacy axis, `types.InfiniteValidTo` on the literal one)
- Point-in-time match (`MatchesPointInTime`): `start <= t AND (open end OR ValidTo > t)`
- Interval overlap (`MatchesInterval`): `start < end AND (open end OR ValidTo > start)`
- Explicit finite ranges are half-open and non-empty: when both `ValidFrom` and `ValidTo` are set, `ValidFrom < ValidTo` is required at graph input, durable wire, and direct Store write boundaries.

### Batch Operations

`BatchBuilder` -- fluent API with eager validation and deferred persistence. Queue methods are serialized by the builder, and update queues use the same update-map validation and provenance shadow-key extraction as standalone updates. Empty queued updates read at execute time so missing entities still fail, but successful empty updates do not count as `Updated` and do not publish update events. Successful batch creates increment graph add stats in the same units reported by `BatchResult.Created`. New relationship-type tokens are deferred until execute-time endpoint and temporal constraint checks pass; the queued relationship pointer is retokenized in place before persistence, and a final relationship write failure restores any newly allocated type token. Execute order: create nodes, create rels, update nodes, update rels, delete rels, delete nodes. Mutation events are buffered during replay and emitted after graph and builder locks are released via one `Publisher.PublishBatch` call, so async priority ordering is applied across the whole batch result. Returns `BatchResult` with counts and per-operation errors; any operation failure also makes `Execute` return an error wrapping `ErrBatchFailed`. Execute is one-shot and marks the builder done when replay starts; later queue calls or repeat Execute calls return `ErrBatchDone`.

`CloseVersion` writes a closing version that keeps the version's start (history row plus new current row, api.md "Closing a version"). It is a successful mutation write, so it increments `NodesUpdated`/`RelsUpdated` and publishes the corresponding update event. `t == 0` or `t` at or before the start returns `ErrInvalidTimeRange`, and already-closed entities return `ErrAlreadyClosed` without changing update counters. An update that sets only `tkg_valid_to` takes the same path.

Direct Store history entry points are input boundaries, not trusted internals.
`PutNodeVersion`, `PutRelVersion`, `DeleteNodeWithHistory`, and
`DeleteRelWithHistory` validate snapshot shape before accepting history rows:
node labels, relationship type/endpoints, key/payload ID agreement, and explicit
finite temporal ranges must be valid. When the current node row is readable,
node delete tombstones must preserve its labels; corrupt unreadable current rows
still proceed through the cleanup path so stale indexes are purged.

Direct Store label/type reads are also input boundaries. `NodesByLabel`,
`RelationshipsByType`, `NodeCountByLabel`, and `RelCountByType` reject token `0`
because registry token zero is reserved and never names a real label or
relationship type.

Graph-level index create APIs can create label tokens for labels that have no current nodes so future matching nodes are indexed. The graph snapshots the label registry before that allocation and restores it if the backend index create fails, keeping failed index definitions from leaving unreachable label names. The rollback guard is panic-safe around backend create calls.

### Transactions

`GraphTx` -- full CRUD transaction serialized against other transactions and batches by `txMu` (it does NOT hold the graph write lock for its lifetime; see "Transaction isolation — what v4 actually guarantees" below). Supports `GetNode`, `GetRelationship`, `AddNode` / `AddNodes`, `AddRelationship` / `AddRelationships` (+ `ByID` / `ByIDIfAbsent`), `ImportNodeWithID` / `ImportRelationshipWithID`, `GetOrCreateByKey`, `UpdateNode`, `UpdateRelationship`, the caller-instant twins `UpdateNodeWithTx` / `DeleteNodeWithTx` / `UpdateRelationshipWithTx` / `DeleteRelationshipWithTx`, `SetNodeVersionInterval` / `SetRelVersionInterval`, node label/property helpers, `DeleteNode` (cascade), `DeleteRelationship`, and in-tx reads. Update methods validate update maps before reading rollback snapshots, so malformed input is reported before missing-entity snapshot errors. First-mutation snapshots are keyed by entity kind plus snowflake ID so a node and relationship with the same caller-supplied underlying value roll back independently. `Commit()` releases `txMu` and publishes buffered events. `Rollback()` restores all deleted node rows before restoring deleted relationships, restores pre-transaction version history, reverts node label changes by exact one-token Store writes until the pre-transaction label sequence is restored, reverts other updates (cascade rows stored above the current version before the transaction are kept), deletes created entities (a re-imported ID loses only the rows from its created version on — `TrimNodeHistoryFrom` / `TrimRelHistoryFrom`, or a rewrite on stores without them — so the earlier life stays), restores label and relationship-type registries to their `BeginTx` snapshots, then restores the `BeginTx` operation-counter snapshot. Relationship recreation always sees live endpoints, rolled-back history rows do not survive, restored label indexes match restored node labels, tokens created inside a rolled-back transaction do not leak, and discarded writes or reads do not leak through `g.Stats().Get()`. Committed transaction reads increment the same read counters as standalone reads. `TxAPI.Run` / `RunContext` reject nil callbacks before opening a transaction, and `RunContext` rejects nil contexts before dereference. Not suitable for long-running operations.

### Transaction and Mutation Isolation

`Core.mu` (a striped `shardedRWMutex` with exactly `sync.RWMutex` semantics: readers spread over lane-keyed stripes, a writer takes every stripe) serializes **writes** against tx/batch and protects long snapshot-style scans. All exported mutation methods (`g.Nodes().Add`/`Update`/`Delete`/`AddLabel`/`RemoveLabel`/`CloseVersion`, `g.Rels().Add`/`AddByID`/`Update`/`Delete`/`CloseVersion`, …) acquire `g.mu.RLock()` at entry. `BatchBuilder.Execute`, `IO.Export`, `Temporal.Snapshot`, and `Admin.VerifyShard` acquire `g.mu.Lock()`, blocking standalone mutations. `BeginTx` acquires only `txMu` (v4.1.0+); each `GraphTx` method takes a brief `g.mu.RLock()` around its body (a mutation takes `g.mu.Lock()` instead when the graph has a change-log and the store lacks the per-tx scoped log, `store.ScopedTxCapability`, which memory and badger implement) and calls the unexported `*Internal` variants, which take and release their entity locks per call. `GraphTx.Commit` takes `g.mu.Lock()` briefly to checkpoint registries and mint the tx's change-log LSNs, releases `g.mu` and `txMu`, then runs the `DurableCommit` flush (when on) and publishes buffered events outside all locks. `GraphTx.Rollback` takes `g.mu.Lock()` for the restore.

Most point and query reads acquire `g.mu.RLock()`: they are blocked while a tx/batch or snapshot-style write-lock scan is active, but they can run concurrently with standalone mutations that also hold `RLock()`. No-error resolver helpers also acquire `g.mu.RLock()` before reading registry pointers and return zero values after graph close is visible; internal mutation/hash code that already holds the graph lock uses explicit lock-free resolver helpers to avoid recursive read locks. `IO.Export`, `Temporal.Snapshot`, and `Admin.VerifyShard` take the write lock because they compose multiple store reads and must not observe a graph changing mid-scan. `IO.Export` rejects nil and typed nil writers before taking the write lock. `IO.ImportWithOptions` rejects nil and typed nil readers before creating a staging file, stages reader I/O before the lock, validates wire invariants before entity construction, then replays under the write lock with rollback snapshots for touched current rows, history rows, and registries.

#### Transaction isolation — what v4 actually guarantees

`GraphTx` is a **write-through transaction with compensating rollback**, not an isolated one:

- **Tx vs tx / batch:** serialized (`txMu`). Two transactions never interleave.
- **Tx vs standalone reads:** every tx mutation is applied to the Store immediately. A concurrent standalone reader sees the transaction's uncommitted rows before `Commit` (dirty read), and sees them disappear again after `Rollback`.
- **Tx vs standalone writes:** entity locks are taken and released per call, not held until commit. A standalone write can land on an entity between two calls of an open transaction. `Rollback` restores the transaction's pre-transaction snapshot of that entity and therefore **overwrites the standalone write** (lost update).
- **Crash:** the tx's rows reach disk through the normal async flush (or per call under `SyncWrites`, except inside a `DurableCommit` group, which syncs at its commit); a crash before `Commit` can leave part of the transaction persisted. With `Config.DurableCommit`, `Commit` (and `Batch.Execute`, the strong ingest applier) does not return success before one durable flush — the pending buffer as one WriteBatch plus a WAL fsync on every open shard that took writes — so a crash after the return keeps the whole group; see "Durable-on-return commit" below. The change-log is scoped: a rolled-back or uncommitted transaction emits no records, so replicas never see its rows.

This is the v4.1.0 performance trade-off (standalone mutations and reads on disjoint entities run in parallel with an open tx). Use `g.Tx()` when no standalone writer touches the same entities concurrently, or serialize those writers yourself; use `g.Batch()` when readers must never observe a partial group (it holds `g.mu.Lock()` for the whole execution). Isolated transactions (private write set applied atomically at commit, locks held to commit) are a v5 item.

### Hash Chain Integrity

`ComputeNodeHash(n, labels)` / `ComputeRelHash(r, typeName)` -- SHA-256 via typed binary serialization with sorted map keys. Genesis: `PrevHash=""`. Updates: `PrevHash=previous.Hash`. A cascade row links to its base row's hash (the pre-correction winner over its piece); a re-import of a deleted ID links to the highest stored row, so the chain verifies across lives. Transaction-time stamps are not hashed. `g.Hash().VerifyNodeChain(id)` / `g.Hash().VerifyRelChain(id)` verify the full chain (handles deleted entities, truncated history and compaction stubs).

---

## Store Interface (`pkg/graph/store/store.go`)

Pure persistence contract — no string resolution, no referential integrity, no shadow properties. The interface is composed from capability sub-interfaces in `pkg/graph/store/capabilities.go`; consult `pkg/graph/store/store.go` for the full embedding and method count. The graph layer depends on `MandatoryStore` (the always-required subset); optional capabilities are type-asserted on demand.

### Query Control

```go
type QueryOpts struct {
    Limit           int            // 0 = no limit
    After           types.EntityID // Cursor: return IDs > After (0 = from start)
    ValidAt         types.Instant  // Point-in-time filter (0 = disabled)
    ValidStart      types.Instant  // Interval filter start (0 = disabled)
    ValidEnd        types.Instant  // Interval filter end (0 = disabled)
    ValidAtSet, ValidWindowSet, TxAtSet, TxPinSet bool // explicit presence: the filter is on whatever its instants (0 included)
    TxAt            types.Instant  // Bitemporal: restrict chain to TxFrom <= TxAt (0 = no TX filter)
    TxPin           types.Instant  // Belief state: pure knowledge-time resolution, NO valid-time filter (0 = disabled)
    IncludeEclipsed bool           // Reserved no-op, kept for API compatibility
    Depth           ShardDepth     // Shard tier filter (0 = all tiers)
    NoSort          bool           // Skip the label-scan ID sort (honoured only when After == 0)
}

type ShardDepth byte
const (
    DepthAll  ShardDepth = 0  // all tiers (default, backward-compatible)
    DepthHot  ShardDepth = 1  // hot shard only
    DepthWarm ShardDepth = 2  // hot + warm shards
)
```

Store query methods reject any other `ShardDepth` value with
`ErrInvalidShardDepth` instead of widening the query to `DepthAll`. Single-
shard stores accept `DepthAll`, `DepthHot`, and `DepthWarm`, but all three
valid selectors read the full single shard because there are no colder tiers.
Graph-level query methods perform the same depth validation before unregistered
label/type/index shortcuts or non-positive-k vector shortcuts can return an
empty success.
`Limit` accepts `0` for unbounded queries or a positive maximum; negative
values return `ErrInvalidQueryLimit`. `After` accepts `0` for the first page or
a non-negative entity cursor; negative values return `ErrInvalidQueryCursor`.
Without its presence flag, `ValidAt != 0` activates point-in-time filtering and
`ValidStart`/`ValidEnd` activate interval filtering only when both are non-zero
(0 is the only unset value, a negative bound is a real bound); a `*Set` flag
makes the filter on whatever its instants, 0 included. Every door reads
presence through the `Has*` methods (`pkg/graph/store/opts.go`).

`TxAt` and `TxPin` are different pins and are mutually exclusive. `TxAt` is the
BITEMPORAL coordinate: it restricts the version chain to `TxFrom <= TxAt`, and
the generic scan doors (`ByLabel` / `ByType` / `All`) still apply an implicit
valid-at-now filter on top, so `TxAt` alone silently drops entities whose fact
was only valid in the past — pair it with a valid-time coordinate. `TxPin` is
the BELIEF-STATE pin: pure knowledge-time resolution with NO valid-time
filtering, the same semantics as `g.Temporal().NodesAsOf`/`RelsAsOf` reached
through the generic door. Setting `TxPin` together with `TxAt` or any valid-time
filter returns `ErrConflictingTemporalOpts` rather than mis-resolving silently.

### Method Categories

| Category | Methods | Notes |
|----------|---------|-------|
| Node CRUD | `PutNode`, `GetNode`, `ReplaceNode`, `DeleteNode` | Deep-copy at boundary; plain `DeleteNode` rejects connected nodes with `ErrInvalidStoreMutation` |
| Rel CRUD | `PutRelationship`, `GetRelationship`, `ReplaceRelationship`, `DeleteRelationship` | Deep-copy at boundary |
| Index queries | `NodesByLabel`, `RelationshipsByType` | Paginated, temporal push-down; label/type names are validated before unregistered-name empty shortcuts |
| Adjacency | `OutgoingRelationships`, `IncomingRelationships` | Node IDs must be positive; typeToken 0 = all types |
| Bulk queries | `AllNodes`, `AllRelationships`, `GetNodesByIDs`, `GetRelationshipsByIDs` | Paginated/sorted scans; direct ID lists reject malformed IDs, then sort and all-or-error on missing positive explicit IDs |
| Batch ops | `PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch` | Two-phase validate-then-apply; delete batches coalesce duplicate IDs before validation; tiered delete batches restore prior accepted shard deletes on later node/relationship bucket failures; `DeleteNodesBatch` rejects connected nodes before mutating any bucket |
| Atomic replace | `ReplaceNodeWithHistory`, `ReplaceRelWithHistory` | Entity + history in one call |
| Version history | `PutNodeVersion`, `GetNodeVersion`, `GetNodeHistory`, `TruncateNodeHistory` + rel mirrors | 8 methods total; entity IDs must be positive, `keepVersions == 0` is explicit clear-all, negative retention returns `ErrInvalidStoreMutation` |
| Cascade | `DeleteNodeCascade` | Node + all connected rels |
| Atomic delete | `DeleteNodeWithHistory`, `DeleteRelWithHistory` | Tombstone history + entity delete in one batch (crash-safe) |
| Counts | `NodeCount`, `RelationshipCount`, `NodeCountByLabel`, `RelCountByType` | O(1) |
| Property indexes | `CreatePropertyIndex`, `DropPropertyIndex`, `NodesByLabelAndProperty` | In-memory, auto-maintained. Store-level create/drop/query reject label token 0 and reserved `tkg_` keys; scalar float keys canonicalize signed zero and NaN payload variants within the same concrete type; graph-level create allocates future-label tokens and rolls them back if backend create fails; graph-level drop validates targets and returns `ErrIndexNotFound` for missing labels/indexes |
| Temporal indexes | `CreateTemporalIndex`, `DropTemporalIndex` | Sorted-slice interval index, O(log n + k) overlap queries. Store-level create/drop reject label token 0; graph-level create allocates future-label tokens and rolls them back if backend create fails; graph-level drop validates targets and returns `ErrTemporalIndexNotFound` for missing labels/indexes |
| Temporal bulk reads | `NodesByLabel`, `AllNodes`, `NodesByLabelAndProperty`, `RelationshipsByType`, `AllRelationships` with temporal `QueryOpts` | Filter by entity temporal metadata before `Limit`; Badger cache misses are fetched before they can consume a page slot |
| HF indexes | `CreateHighFrequencyIndex`, `DropHighFrequencyIndex` | Time-bucketed, O(1) amortized insert. Store-level create/drop reject label token 0; graph-level create allocates future-label tokens and rolls them back if backend create fails; create rejects bucket sizes that are not positive whole milliseconds with `ErrInvalidTemporalIndexConfig`; MemoryStore and BadgerStore backfill matching current nodes at create time and maintain buckets on later node writes; Badger persists HFI definitions and rebuilds buckets on open; Badger create fails closed on corrupt existing rows; one temporal index type per label; tiered stores persist tracking definitions for restart, rotated hot shards, and lazy archives and roll back earlier shard creates if later-shard backfill or metadata persistence fails; graph-level drop validates targets |
| Vector indexes | `CreateVectorIndex`, `CreateVectorIndexWithOptions`, `DropVectorIndex`, `SearchNearestNodes` | In-memory k-NN — approximate HNSW by default, exact brute force via `VectorIndexOptions.UseBruteForce`. Store-level create/drop/search reject label token 0 and reserved `tkg_` keys; Store-level search applies temporal/depth filters before heap selection and `QueryOpts.After`/`Limit` over distance order; graph-level temporal over-fetch caps resolved-candidate buffers by the bounded probe ceiling; graph-level create allocates future-label tokens and rolls them back if backend create fails; graph-level drop/search validate targets and paginate after temporal/history resolution. BadgerStore and TieredStore persist definitions and rebuild entries on open; Tiered definition file writes restore prior raw metadata on failure |
| Label management | `RemoveNodeLabelToken`, `RemoveNodeLabelTokenWithHistory` | Atomic label removal with optional history |
| ID-only queries | `AllNodeIDs`, `AllRelIDs` | No deserialization for non-temporal scans; temporal `QueryOpts` fetch metadata and filter before cursor pagination |
| History IDs | `AllNodeHistoryIDs`, `AllRelHistoryIDs`, `AllNodeHistoryIDsFrom`, `AllRelHistoryIDsFrom` | Includes deleted entities. The `*From` variants are cursor-paginated (after, limit) for bounded-RAM walks; `limit == 0` means all remaining, negative limits/cursors fail closed with `ErrInvalidQueryLimit` / `ErrInvalidQueryCursor`, and the legacy non-paginated methods now delegate to the paginated form |
| ForEach iterators | `ForEachNodeID`, `ForEachRelID`, `ForEachNodeHistoryID`, `ForEachRelHistoryID` | Callback-based, no slice materialization; nil callbacks return `ErrInvalidStoreMutation` on open stores |
| Lifecycle | `Clear`, `Close` | `Close` idempotent via `sync.Once` |

### Sentinel Errors

`ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrVersionNotFound`, `ErrNoVersionValidAt`, `ErrIndexExists`, `ErrIndexNotFound`, `ErrTxDone`

**Graph-layer sentinel errors** (not in Store interface unless noted): `ErrSelfLoop` — returned by the shared relationship-create kernel (`g.Rels().Add` / `AddByID` / `AddByIDIfAbsent`, the batch queue) and the import paths (`g.Rels().Import`, `GraphTx.ImportRelationshipWithID`) when `startID == endID && !g.validation.AllowSelfLoops`; `ErrInvalidID` — returned by import-by-ID APIs for negative caller-supplied IDs; `ErrVersionOverflow` — returned by versioned mutations when the current entity version is already `math.MaxUint32`; `ErrNilNode` and `ErrNilRelationship` — aliases of the type-layer nil entity sentinels, returned by graph methods and entity methods with error channels; `ErrNilGraph` — returned by nil, zero-value, or typed-nil graph façade and sub-API entry points with error returns; `ErrNilContext` and `ErrNilTxCallback` — returned by public context and transaction-helper boundary checks; `ErrNilReader` and `ErrNilWriter` — returned by IO import/export boundary checks; `ErrNilStore` — aliases the Store-layer sentinel returned by `graph.New` for typed nil `Config.Store` values and by nil concrete in-tree Store lifecycle receivers; `ErrReadOnlyReplica` — returned by the core-layer `checkWritable()` gate on every user mutation door (and `Tx().Begin` / `Batch.Execute` / `Admin().Reset`) when the graph was opened with `Config.ReadOnlyReplica` (reads, bootstrap import, and `ApplyChange` stay open). Temporal-write sentinels: `ErrTxOrder` (wraps `ErrInvalidTxFrom`; a caller instant that does not follow the chain), `ErrEntityDeleted` (a correction on a hard-deleted entity; returned wrapped together with `ErrNodeNotFound` / `ErrRelNotFound`), `ErrTxPinTooNew` (effective-timeline pin above the commit clock), `ErrCommitNotDurable` (`DurableCommit` flush failed after the group committed).

---

## memory.Store (`pkg/graph/store/memory/memorystore.go`)

Thread-safe in-memory Store (`memorystore*.go`). One `sync.RWMutex` protects the maps (the `LatestStamps` cache has its own `histStampsMu`). Core maps:

```
nodes        map[types.NodeID]*types.Node
rels         map[types.RelID]*types.Relationship
labelIdx     map[uint16]map[types.NodeID]struct{}        // labelToken -> node IDs
typeIdx      map[uint16]map[types.RelID]struct{}         // relTypeToken -> rel IDs
outIdx       map[types.NodeID]*adjSet                    // startNodeID -> rel IDs (slice up to 64, then hash set)
inIdx        map[types.NodeID]*adjSet                    // endNodeID -> rel IDs
nodeHistory  map[types.NodeID]map[uint32]*types.Node
relHistory   map[types.RelID]map[uint32]*types.Relationship
```

Alongside them: the index maps (property, rel-property, composite, temporal, rel temporal, high-frequency, vector), the transaction-time membership sidecars (`labelTxMembers` / `relTypeTxMembers`, and per declared property index `nodePropTxMembers` / `relPropTxMembers`, recorded at the four row seams `storedNode` / `historyNode` / `storedRel` / `historyRel`), the per-ID history-stamps cache (`memorystore_history_stamps.go`), and the ADR-0011 column segments of declared relationship types (`memorystore_segments.go`; with `SegmentDir` sealed segments are files, `memorystore_segments_dir.go` over `internal/segdir`). `HasHistory` reads the history maps directly.

- O(1) per-label/per-type counts via `len(labelIdx[token])`
- Adjacency sets (`adjSet`, `memorystore_adjset.go`): an unordered slice up to 64 members (~10 B each), a hash set above
- Deep-copy at store boundary (both Put and Get)
- Temporal push-down: filters in-memory entity pointers without deep-copy
- ForEach: snapshot IDs under RLock, release the lock, then invoke callbacks
- The zero value is a usable empty store; maps initialize lazily at the lifecycle gate.
- `Clear()` reinitializes all maps. `Close()` marks the store closed; public operations return `ErrStoreClosed` after close, and test-only tampering helpers are inert.

---

## badger.Store (`pkg/graph/store/badger/`)

The implementation is split across themed production files; the principal ones:

- `badgerstore.go` — Store struct, sentinel-error aliases, `New`, `Close`, `Clear`, `loadIndexes`.
- `badgerstore_node.go` — node CRUD (`PutNode`, `GetNode`, `DeleteNode`, `ReplaceNode`, label-token mutations) plus node queries and node-batch ops.
- `badgerstore_rel.go` — relationship CRUD plus rel queries (`Outgoing*`, `Incoming*`) and rel-batch ops.
- `badgerstore_index.go` — property / temporal / high-frequency / vector index management.
- `badgerstore_history.go` (+ `badgerstore_history_node.go` / `badgerstore_history_rel.go`) — version history methods, including the cursor-paginated `AllNodeHistoryIDsFrom` / `AllRelHistoryIDsFrom`.
- `badgerstore_temporal.go` — temporal-filter helpers (`filter*ByTemporalPeek`, `fetch*WithTemporalFilter`).
- `badgerstore_meta.go` — counts, registry persistence, cache hit/miss accessors.
- `badgerstore_flush.go` — async write batch + flush loop + dirty tracking + write-pressure backpressure + `publishMoveLocked` (a with-history write appends its history row to the pending buffer before it changes the entity cache, so a reader at a fixed pin that reads the current row and then history never misses the moved row).
- `badgerstore_durable.go` — `DurableFlush` (`Config.DurableCommit`, below).
- `badgerstore_flush_hold.go` — `store.MutationFlushHoldCapability`: the per-mutation `SyncWrites` flush held inside a durable group (below).
- `badgerstore_history_presence.go`, `badgerstore_history_stamps.go`, `badgerstore_history_epoch.go`, `badgerstore_propertytxmembers.go`, `badgerstore_labeltxmembers.go` — the RAM sidecars below.
- `badgerstore_format.go` — on-disk wire-format version marker (verify/stamp at open).
- `badgerstore_changelog.go`, `badgerstore_composite_index.go`, `badgerstore_docvalues.go`, `badgerstore_property_disk.go` (0x0A), `badgerstore_temporal_disk.go` (0x0B), `badgerstore_retention_purge.go`, `badgerstore_history_delta.go` — the later capability layers.


Persistent Store using Badger v4 with async batch persistence.

### On-Disk Format Versioning

Two layers, per row (`fv`) and per store (the `wire_format_version` marker, verified at open before any row is decoded; tiered shards check it per shard). One tracker raises the marker; a newer row or marker fails closed with `store.ErrWireFormatVersionUnsupported`. Home: `docs/persistence.md` "Format versions and markers", upgrade path "Upgrading a store"; row layout SPEC 9.1a.

- `loadIndexes` still tolerates corrupt rows (skip + warn + counter
  reconcile), but a FUTURE per-row version is not damage — it fails the open
  instead of silently dropping the row.
- A clean `Close` writes the open image (`badgerstore_open_image.go`, backlog 54);
  an open that trusts it (its header is the newest badger commit) reads keys and
  the image instead of decoding rows; planner statistics travel in the image
  too, and only an image without them defers them to their first read
  (`badgerstore_planner_stats_deferred.go`).
- Every flush that writes rows puts the commit floor first in its WriteBatch
  (`badgerstore_commit_floor.go`, backlog 44): the newest TxFrom/TxTo/DeletedAt
  of every entity and history row the store has written, decoded from the
  batch. The open loads it and the graph raises its commit clock to it
  (`store.CommitFloorCapability`), so a crash reopen stays above every stored
  stamp without the Close-time watermark; tiered shards other than the
  reference shard hand it to `Config.OnCommitFloor`, which keeps it on the
  reference shard ahead of their batch.


### Write-Pressure Bound

`badger.Config.MaxPendingWrites` bounds the async write buffer (dirty cache entries are
never evicted); at the bound the writing call flushes synchronously. Defaults,
failure behaviour and `PendingWriteCount()`: `docs/persistence.md`.

### Durable-on-return commit

`graph.Config.DurableCommit` (off by default; backlog 11, ai-soc request 9)
makes `GraphTx.Commit`, `Tx().Run*`, `Batch.Execute` and the strong ingest
applier call `store.DurableFlushCapability.DurableFlush` once per group, after
the group is applied and the graph locks are released (readers and the next
writer do not wait for the fsync). Badger's `DurableFlush` holds `flushMu`
across the normal flush (the whole pending buffer, counters and change-log
records in one WriteBatch) and a `db.Sync()` of the WAL; the sync runs only
when a WriteBatch reached the WAL without an fsync since the last one (an
`unsynced` flag set by every flush, background ones included), and never under
`SyncWrites`. Tiered folds it over the reference shard, the open archive and
the open event shards without lazy-opening a closed cold shard (it has nothing
pending); sharded over every slot. A store without stable storage (memory,
`BadgerInMemory`, in-memory tiered/sharded) fails `New` with
`ErrCapabilityNotSupported`.

What a caller may assume: after success, a crash keeps the whole group. Before
the return, a crash can keep any subset — the background flush may already
have written part of it, and the flush snapshot is a map, so Badger can split a
buffer above its transaction size limit into several transactions in arbitrary
order. A consumer that writes its cut record LAST in the group and recovers by
"rows above the last durable cut are unfinished" is therefore safe while the
flush that carries the cut fits one Badger transaction (about 15 % of
`MemTableSize`, ≈ 9.6 MB at the 64 MB default — the flush carries the group
plus any other write still pending); all-or-nothing on disk for larger flushes
is v5. On a flush failure the door returns `ErrCommitNotDurable`: the group is
committed in memory, its operations stay pending (requeued), and the next
successful flush persists them; until then a crash can lose them. `RunWithLSN`
still returns the group's LSN and `Batch.Execute` its result (with a
`durable-commit` `BatchError`), so a caller never has to re-apply a committed
group. Rollback never flushes (under `SyncWrites` it ends with one durable flush, below). Standalone mutations and concurrent-mode ingest
`Submit` keep the async flush.

Under `SyncWrites` (strict power-loss durability: Badger fsyncs the WAL after
every write and syncs a value-log file it retires, so no memtable or value-log
switch leaves a gap) the store flushes every mutation. Inside a durable group
that cost one Badger sync per mutation (~1 s per 530-op group, backlog 52), so
the graph holds it (`store.MutationFlushHoldCapability`, badger and tiered): each
`GraphTx` mutation, the whole `Batch.Execute` window and a `Rollback` run under
the exclusive graph lock with the per-mutation flush held, and the group's one
`DurableFlush` commits the held writes as one WriteBatch that Badger syncs
(Badger may split it into several synced commits). The exclusive lock keeps
every other mutation out of the hold, so a standalone write still returns
synced. A held buffer that reaches `MaxPendingWrites` flushes (synced) in
steps; a `Rollback` ends with one durable flush, because part of the group can
already be on disk. Without `DurableCommit`, `SyncWrites` still syncs every
mutation; sharded keeps per-mutation syncs. Side effect: with
both flags each `GraphTx` mutation takes the exclusive `c.mu.Lock` instead of
the shared one, so standalone writers, readers and concurrent ingest lanes wait
for each tx mutation (in-memory work only; the sync runs at the commit, outside
the lock). The hold is released exactly once, under the holder's lock.

"Crash" above means a process crash. Power loss is narrower (Badger's
`db.Sync()` covers only the active memtable WAL and the current value-log file,
so a group whose flush crossed a memtable or value-log switch can be unsynced);
it needs `DurableCommit` + `SyncWrites` on a badger, sharded or tiered store.
Mode table and measured cost: `docs/persistence.md`. On tiered,
`tiered.Config.SyncWrites` reaches every writable shard, and one shared flush
hold covers every shard, a shard opened during the hold included.

`DurableFlush` re-checks the store's closed state under `flushMu`: `Close`
sets `closing` before its final flush, which needs `flushMu`, so a durable
flush never syncs a DB that `Close` already closed (a commit can race
`Graph.Close`, since it flushes after releasing the graph locks; it then gets
`ErrCommitNotDurable` wrapping `ErrStoreClosed`, while `Close`'s own final
flush writes the group). Sharded holds its store lock across the fold.

### Key Architecture

```
+-- In-memory indexes (source of truth) ---------+   +-- Entity caches --+
|  nodeIDs   map[NodeID]struct{}                  |   |  nodeCache  LRU   |
|  relIDs    map[RelID]struct{}                   |   |  relCache   LRU   |
|  labelIdx  map[token]map[NodeID]struct{}        |   +-------------------+
|  typeIdx   map[token]map[RelID]struct{}         |
|  outIdx    map[NodeID]map[RelID]NodeID          |   +-- Write buffer --+
|  inIdx     map[NodeID]map[RelID]inEdge          |   |  pending map     |
|  (all under idxMu RWMutex)                      |   |  (under wbMu)    |
+-------------------------------------------------+   +------------------+
                    |                                     |
                    v                                     v
              +-- Badger DB (on-disk or in-memory) --+
              |  entity data, indexes, history, meta  |
              +---------------------------------------+
```

**Concurrency primitives:**
- `idxMu sync.RWMutex` -- protects all in-memory indexes
- `wbMu sync.Mutex` -- protects pending write buffer
- `flushMu sync.Mutex` -- serializes flushes (background, backpressure, `DurableFlush`, `Close`)
- sidecar locks: the history presence / stamps sets each have a `buildMu` and an `RWMutex` taken inside `wbMu` by `noteHistoryKey`; the property tx-membership sidecars are claimed by one build pass per kind under `idxMu` (`propTxPass`), and a lookup waits for the claiming pass with no lock held
- `atomic.Int64` counters for node/rel counts
- `sync.Map` for per-label/per-type counters
- `sync.Once` for idempotent Close

**Async persistence flow:**
1. Write operations update in-memory state immediately
2. Queue `writeOp` structs into `map[string]writeOp` (last-write-wins dedup)
3. Background flush loop drains buffer via Badger `WriteBatch` every `FlushInterval` (default 100ms)
4. Counter keys (`meta/node_count`, `meta/rel_count`) included in same WriteBatch for atomic crash recovery
5. Failed ops re-queued via `requeueOps()`
6. `Close()` calls `flush()` unconditionally (even if the flush loop never started)

With `SyncWrites`, `FlushInterval` is forced to zero and every Badger writeOp
enqueuer (index-definition create/drop, split relationship helpers) flushes
before returning, after releasing `idxMu` because `flush()` takes
`idxMu.RLock()` for its snapshot. Contract: `docs/persistence.md`.

**Old-state reads for Badger mutations:** `ReplaceNode`, label-token mutation
paths, and their history variants prefetch the current node outside `idxMu` to
avoid Badger I/O under the write lock. Cache-miss `DeleteNode` and
`DeleteRelationship` do the same prefetch, then re-read the current cached row
under `idxMu` and use that row for label/type/adjacency cleanup. After acquiring
`idxMu`, a missing `nodeIDs`/`relIDs` entry is a concurrent delete and returns
`ErrNodeNotFound`/`ErrRelNotFound`; any prefetch error while the ID still exists
is corruption or an operational read failure and is returned. Only cascade
delete has a cleanup-and-return-corruption fallback.

**Entity caches (`index.EntityCache[V]`: an N-way `ShardedCache` over `Cache[V]` LRUs, `internal/index/lru.go` / `sharded.go`; byte-budgeted with `CacheBudgetBytes`):**
- Dirty tracking with monotonic `dirtyVer` counter
- Tombstone support for deletions
- Dirty entries never evicted (soft capacity)
- `Peek(key)` for zero-allocation lookup (no deep-copy, no MRU promotion) -- used by temporal pre-filter
- `MarkFlushed()` only clears entries matching the collected `dirtyVer` (prevents data loss from concurrent writes)

`outIdx`'s value is the relationship's END node ID and `inIdx`'s value is an
`inEdge{startNodeID, typeToken}` — both let adjacency answer type-filtered and
endpoint-resolving traversals without fetching the relationship row.

**RAM sidecars (no on-disk keyspace; built lazily, rebuilt after reopen, dropped by `Clear`):**
- *History presence* (`HasNodeHistory` / `HasRelHistory`): the IDs with history rows and their highest history version, built by one key-only scan of `0x07`/`0x08` (write buffer included) and maintained in `noteHistoryKey` under `wbMu`, the one seam every history key passes; a delete marks the ID for a per-ID key probe installed under a write-generation stamp. `Config.HistoryPresenceProbeOnly` (tiered cold shards) answers by probe without building.
- *History stamps* (`NodeHistoryStamps` / `RelHistoryStamps`, behind `LatestStamps`): the fold of each ID's history-row `TxFrom` / `TxTo` / `DeletedAt` (`store.FoldTxStamps`), built from the temporal block of each history value, same maintenance and probe protocol.
- *Transaction-time membership*: per label / rel type (`ByLabel` / `ByType` with `TxAt` / `TxPin`) and per declared property index (`ForEach{Node,Rel}PropertyTxMember`: value -> every entity whose rows ever carried it, with the earliest `TxFrom`, merged by `index.MergeFirstTx` so an unstamped 0 is never raised). Append-only sound supersets: the chain resolver stays the authority. Recorded at the current-row index seam and every history-row door. The property ones are the exception to "rebuilt after reopen": one scan per kind builds every declared one, and the open image carries the built ones across a clean close (backlog 74).

**Key layout (13 prefixes, `internal/storeutil/keys.go:26-36`):**

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
| `0x09` | `/<8B LSN>` | 9B | Change-log (op-log) record — opt-in (`ChangeLog`); value = `tag(1B) ‖ msgpack(body)` |
| `0x0A` | `/<2B propKeyToken>/<payload>/<8B nodeID>` | var | Persisted property-index entry — opt-in (`Config.PropertyIndexOnDisk`) |
| `0x0B` | `/<2B labelToken>/<8B from>/<8B nodeID>` | 19B | Persisted temporal-index raw-entry row — opt-in (`Config.TemporalIndexOnDisk`) |
| `0x0D` | `/<8B nodeID>/<1B dir>/<8B relID>` | 18B | Ex-adjacency (`ExAdjacencyCapability`, backlog 50) |
| `0x0F` | `/meta/*` | var | Counters, registry tokens, property index defs, `last_lsn` watermark |

All IDs stored as big-endian uint64 for correct sort order and temporal clustering.

**Index-residency config knobs** (all badger-backed, ignored when `Store` is
supplied explicitly): `Config.LabelIndexOnDisk` and `Config.AdjacencyIndexOnDisk`
answer label / adjacency snapshots from their persisted keyspaces instead of the
in-memory maps; `Config.PropertyIndexOnDisk` moves property-index entries into
the `0x0A` keyspace; `Config.TemporalIndexOnDisk` maintains the compact `0x0B`
per-entity row so open streams the temporal index instead of rebuilding it with
a full node fetch+decode per entity (the index itself stays RAM-resident at
runtime). The two new keyspaces are backfilled from current node state exactly
once, the first time their flag is turned on.

Sizing figures behind these knobs (order of magnitude, measured): a label-index
entry costs about 50-100 B of RAM (the ceiling at hundreds of millions of
nodes) and `AdjacencyIndexOnDisk` saves about two map entries per relationship;
a label scan over more nodes than `Config.CacheCapacity` holds decodes every
miss from badger (about 3 µs per row) instead of hitting the cache (about
0.1 µs per row), so size the cache to the scan working set; an 8-byte numeric
DocValues row costs no per-row heap object, and a 10M-row numeric column
(`index.MaxDocValuesNodes`, the cap) takes about 165 MB.

### Temporal envelope indexes (node label, relationship type)

`g.Index().CreateTemporal(label)` and `CreateRelTemporal(type)` keep a RAM index of one envelope per entity: the earliest valid-from and latest valid-to over ALL its rows, current and history. A valid-time `ByLabel` / `ByType` read, and a paged effective door with a valid filter (`RelsEffectivePage` / `NodesEffectivePage`, backlog 75), prunes an entity whose envelope misses the query instant without reading it (one seam: `pruneRelTypeCandidates` / `pruneNodeLabelCandidates`). The prune is sound only if no row lies outside its entity's envelope, so the contract is: an entity the index does not cover is never pruned, and every write that adds a row to a covered entity extends the envelope in the same step.

- **Build (three phases, `CreateTemporalIndex`, `CreateRelTemporalIndex`).** (1) Install the index as Building: writes fold into it and record their IDs, prunes decline it, it is not persisted or imaged. (2) Build unlocked from the members taken at the start; the relationship build reads one transaction with two forward walks (current rows, history rows) in ID order and folds a delta history row from its temporal block (reopen rebuild and tiered cold-to-warm promotion use the same build; 200,000 relationships with history reopen at 0.3 M rels/s, 4x the former per-ID reads). (3) Re-read every recorded ID, merge and publish under the lock. An ID first written after the re-read has its whole history folded at the install. A create that fails leaves no index behind.
- **Doors that add rows to a covered or uncovered ID fold its history first**: the create doors for a re-created ID (`PutNode`, `PutRelationship` and their batch and co-located forms), the add-label doors for a label added back to a live node, and `PutNodeVersion` (it extends the envelope of each of the node's labels; an uncovered ID stays uncovered). Memory's create reads a node's history rows under its lock.
- **On disk (`TemporalIndexOnDisk`).** The `0x0B` rows are written for a build member unless a write since the build started added or removed that row; a failed or crashed create purges the label's rows, and an open purges the rows of every label with no definition (see persistence.md).

Tests: `TestRelTemporalIndexRebuildSequential`, `TestCreateRelTemporalIndex_*`, `TestRelTemporalIndex_Recreate*`, `TestNodeTemporalEnvelope_*`, `TestMemoryNodeTemporalEnvelope_*`.

### Change-log / op-log (`ChangeFeedCapability`, opt-in)

With `ChangeLog` enabled, every committed mutation appends a framed record under
`0x09/<LSN>` (big-endian LSN, so a prefix scan yields ascending commit order),
in the SAME `WriteBatch` as the data and counters, plus a `last_lsn` watermark —
so a record and its mutation commit atomically and the LSN allocator reseeds
crash-consistently at open. The log is the topology-agnostic foundation for
horizontal scaling (CDC / audit / PITR today; read-replica streaming next) and
is surfaced through `g.Replication()` (`ChangeFeed` / `ForEachChange` /
`LastCommittedLSN`). It is emitted IN-BACKEND (badger, memory, tiered, sharded) rather than via a
`Store` decorator, because crash-safety requires co-committing the record in the
data batch and a decorator would lose native-store trust. The log alone does not
converge a replica from empty — bootstrap from a full export snapshot (registry
included), then tail the feed. The feed is scoped per `GraphTx`: its records are
buffered (`store.TxChangeLogScope`) and get their LSNs at `Commit`, so a rolled-back
or uncommitted transaction emits no records and burns no LSN; an unscoped door (a
store without the capability, the standalone doors, the concurrent ingest session)
appends its record eagerly, one per store mutation. See `tasks/todo.md` "Backlog (open)".

### Read replicas: apply engine + read-only gate (Phase 1, opt-in)

A graph opened with `graph.Config.ReadOnlyReplica` is a log-shipped read replica.
It is seeded by `g.IO().Import` from a primary export, records the snapshot's
`LastCommittedLSN` via `SetAppliedLSN`, then loops `ForEachChange(AppliedLSN())`
→ `ApplyChange(rec)` to tail the primary. `ApplyChange` (`apply_record.go`,
under `c.mu.Lock`) reproduces the primary's rows VERBATIM: it runs the same
import-trust pipeline per record (`WireTo*Checked` rebuilds version / `TxFrom` /
temporal / integrity exactly → token-in-registry validation → property-limit
validation → hash recompute-**and-compare**) and writes through a foreign-ID
store door (`PutNode` / `ReplaceNode` / `ReplaceNodeWithHistory` / `Delete*` /
`PutNodeVersion` / `Truncate*` / `Trim*` / label-token doors) that persists the
supplied entity byte-for-byte — never `NodeOps.Add/Update`, which would re-mint
IDs and re-stamp metadata. The `ChangeNodePut` / `ChangeRelPut` record carries a
`WithHistory` bit so the replica knows whether to write a history row; create vs
in-place vs label-mutation is inferred from local state + a label-token diff.
Apply is idempotent (identical row = no-op; missing-entity delete = no-op), so the
applied-LSN watermark (`meta/replica_applied_lsn`) can advance via a separate
`MetaSet` after each door commits — a crash in that window simply replays. Writes
fail closed with `ErrReadOnlyReplica` via a core-layer `checkWritable()` gate on
every user mutation door; reads, the bootstrap importer, and `ApplyChange`
remain open.

Three further primitives complete the Phase-1 base layer. **Gapless handoff:** the
export header (v2 for a full export, v3 for an `ExportSince` delta; importers accept v1–v3) carries `SnapshotLSN`, captured via
`LastCommittedLSN()` under the same `c.mu.Lock` as the entity snapshot; import
records it as the replica's initial applied watermark, so a bootstrap needs no
separate post-export LSN read. **Token-registry refetch:** when an applied record
references a label/rel-type token the primary registered after the bootstrap,
`g.Replication().RegistrySnapshot()` (a `store.ReplicationSource` injected via
`Config.ReplicationSource` / `g.SetReplicationSource` — a primary's
`g.Replication()` satisfies it in-process) returns the primary's registries plus
the LSN they are complete as-of; the apply path guards `CapturedAtLSN >= rec.LSN`,
append-only-extends the replica's registries (`AppendNames`, prefix-guarded,
persist-then-rollback on failure), and re-validates. Property keys are tokenized
locally (records carry string keys), so only labels/rel-types are synced.
**Failover lease:** `g.Replication().IDSlotLease()` / `SetIDSlotLease()` persist a
durable snowflake-slot hint (MetaKV; `SafeUnmarshal` on read; last-writer-wins,
not CAS); promotion is by reopen (`Close()` + `New()` under the leased
`SnowflakeNodeID`, since the generators are built only in `New`). The
network/orchestration half (Bolt routing, read-your-writes bookmarks, slot
assignment) lives in consumer — rho-tkg exposes the primitives.

**ReadOnly mode:** `BadgerStoreConfig.ReadOnly` opens Badger read-only and skips flush/GC goroutines. TieredStore does not use read-only Badger handles for warm/cold owner shards: existing event entities still update/delete on their original shard after rotation and restart.

**ForEach iterators:**
- `ForEachNodeID` / `ForEachRelID` -- snapshot in-memory index-map IDs under `idxMu.RLock`, release the lock, then invoke callbacks
- `ForEachNodeHistoryID` / `ForEachRelHistoryID` -- page unique history IDs from pending writes plus Badger prefix scans, overlay pending delete writeOps from history truncation, release Badger transactions before invoking callbacks, and stop at the history-ID high-water mark captured before callbacks start

---

## tiered.Store (`pkg/graph/store/tiered/`)

Multi-shard Store routing entities across a reference shard and time-windowed event shards.

`tieredstore.go` plus the
`tieredstore_read_*` / `tieredstore_write_*` splits carry the core, with
`tieredstore_admin.go`, `_catalog.go`, `_changelog.go`, `_compaction.go`,
`_docvalues.go`, `_durable.go`, `_history_presence.go`, `_history_stamps.go`, `_history_epoch.go`,
`_lifecycle.go`, `_migrate.go`, `_property_stats.go`, `_repair.go`,
`_routing.go`, `shard_index_fanout.go` and `retention_purge.go` alongside them.

### Shard Model

```
data/
  meta/
    registry.msgpack        # Label + reltype registries (msgpack, atomic rename)
    shard_catalog.json      # Shard metadata (JSON, atomic rename)
  reference/                # Reference shard (always open, read-write)
  events/
    2026-W10/               # Hot event shard (read-write)
    2026-W09/               # Warm event shard (writable owner shard)
    2026-W08/               # Cold event shard (store=nil, lazy-open)
    ...
  archive/                  # Reference archive (lazy-created on first ArchiveNode)
```

**TieredStore struct (key fields):**

```
mu                   sync.RWMutex                  -- protects hotShard + eventShards during rotation
refShard             *BadgerStore                  -- always open
refArchive           atomic.Pointer[BadgerStore]   -- nil until first archive/restore (atomic.Pointer for race-free Load)
archiveMu            sync.Mutex                    -- protects lazy-open of refArchive
archiveActiveReqs    atomic.Int64                  -- outstanding refArchive checkouts; Close drains before close
closed               atomic.Bool                   -- set by Close; ensureRefArchive / checkout* return ErrStoreClosed
eventShards          map[string]*EventShard        -- name -> shard
hotShard             *EventShard                   -- convenience pointer to current hot shard
ontology             *OntologyMapping              -- classifies labels (reference vs event)
catalog              *ShardCatalog                 -- persistent shard metadata
closeCh              chan struct{}                 -- signals idle-close goroutine to stop
closeOnce            sync.Once                     -- idempotent Close
```

**refArchive checkout discipline:** every refArchive read or write
goes through `checkoutArchive() (*BadgerStore, func(), error)`, mirroring the
event-shard `checkoutStore`. Returns nil with a no-op checkin when no archive
exists in the catalog. Increments `archiveActiveReqs` so a concurrent `Close`
that drains active requests cannot free the store mid-call. Cold-start safe:
opens the archive on demand if the catalog has it. Used by every API that
reads or mutates archive state — point lookups (`shardForNodeIDChecked` /
`shardForRelIDChecked`), indexed/bulk reads at `DepthAll`, history fan-out
(`forEachHistoryShard`), `ArchiveNode` / `RestoreNode` cross-store moves, and
admin paths (`ListShards`, `RebuildCatalog`, `findRelInAnyShardStore`,
`allShardStoresWithLazyOpen`). Checked ID routers also return stable
archive-placement identity for history read/truncate callers; those callers do
not compare against a fresh `refArchive.Load()` after the archive has been
pinned.

**EventShard checkout discipline for admin paths:** admin methods
that iterate event shards (`ListShards`, `RebuildCatalog`, `Clear`,
`CreateTemporalIndex`, `DropTemporalIndex`, `CreateHighFrequencyIndex`,
`DropHighFrequencyIndex`) must pin each shard via `checkoutStore` /
`checkinStore` before calling any BadgerStore method on it. `Close` does not
take `ts.mu` — it only spin-waits on `activeReqs` — so holding `ts.mu.RLock`
or `ts.mu.Lock` is not sufficient protection against a concurrent `Close`
freeing the underlying DB. The rule: `es.store.<anything>` without a preceding
`checkoutStore` is a Close race. See lesson B36.

`Clear` has one extra case: closed cold shards must be opened before they can
be wiped. Warm/cold event shard handles are writable when open because existing
entities keep routing owner-shard mutations there after rotation. After data is
cleared, catalog verification and count caches are reset and persisted or
rolled back.

**EventShard struct:**

```
name        string         -- shard name (e.g., "2026-W10")
store       *BadgerStore   -- nil when cold + closed
tier        ShardTier      -- TierHot / TierWarm / TierCold
timeStart   time.Time      -- window start (inclusive)
timeEnd     time.Time      -- window end (exclusive)
readOnly    bool           -- warm/cold tier marker, not Badger read-only mode
path        string         -- for lazy-open
shardMu     sync.Mutex     -- protects lazy open/close
activeReqs  atomic.Int64   -- outstanding reads (blocks idle-close)
lastAccess  atomic.Int64   -- unix ms (idle-close tracking)
```

### Ontology Classification

`OntologyMapping` classifies labels as `ClassReference` (long-lived: Case, Organization, User) or `ClassEvent` (time-windowed: Signal, Alert -- the default for unknown labels). Lazy token cache backed by label registry; registry swaps clear the token cache so old token meanings do not survive rollback or migration wiring. Nil mapping receivers behave like an empty mapping and classify everything as `ClassEvent`.

Reference entities go to `refShard`. Graph-generated current event entities go
to the hot event shard. Event node creates with caller-supplied/backfilled
snowflake IDs go to the event shard selected by that ID's timestamp, so
subsequent `shardForNodeID` reads resolve to the same owner.

Weekly event windows use ISO week boundaries. For a 1-week `ShardWindow`,
`timeStart` is Monday for the window timestamp's ISO week-year; ISO week 1 is
the week containing January 4, not necessarily the week containing January 1.
Sub-day event windows use fixed-duration boundaries that contain the timestamp;
`ShardWindow` is rejected unless it is at least 1 minute and a whole millisecond.

### Shard Resolution (O(1) for nodes, O(1)->O(N) for rels)

**Nodes (`shardForNodeIDChecked`):**
1. Check `refShard.hasNodeID(id)` -- O(1)
2. If miss, check `refArchive` (if open) -- O(1)
3. Extract timestamp from snowflake ID via `snowflakepkg.Layout.Decompose(id).Time` -- window lookup by binary search, O(log shards) (see "Routing and the move lock")

**Relationships (`shardForRelIDChecked`):**
1. Check `refShard.hasRelID(id)` -- O(1)
2. Check `refArchive` with a checkout pin when present -- O(1)
3. Timestamp extraction -> target event shard -- O(log shards)
4. Fallback: probe every other event shard, including cold shards, for cross-shard rels -- O(N)

**New entities:** Route by ontology classification (label -> ref/event), with a timestamp-owner exception for caller-supplied event node IDs. Graph-generated current event IDs land in `hotShard`; imported/backfilled event node IDs are written to the event shard selected by their snowflake timestamp. Public Store create paths use the same cold-shard-aware ID resolution as reads before accepting caller-supplied IDs, so changing the label class or relationship start shard cannot bypass duplicate detection for IDs already parked in closed cold shards. Relationship duplicate checks resolve actual owner placement, not only the relationship ID timestamp, because a current-ID relationship can still be stored on an old start-node shard. Core-generated fresh IDs use an internal generated-ID fast path, including batch node execution, to keep normal creates on the hot path without opening unrelated cold shards.

### Relationship Routing (7 keys per relationship)

| Pattern | Entity + out/ keys | in/ keys | Split? | Write order |
|---------|-------------------|----------|--------|-------------|
| E->E | event shard | event shard | No | single shard |
| R->R | ref shard | ref shard | No | single shard |
| E->R | event shard | ref shard | Yes | ref in/ first |
| R->E | ref shard | event shard | Yes | entity first |

Cross-shard split writes use `badgerstore_partial.go` helpers: `PutRelEntityAndOut` (entity + typeIdx + outIdx) and `PutRelIncoming` (inIdx only). Both endpoints verified to exist before any writes begin.

`PutRelationship` and `PutRelationshipsBatch` serialize duplicate-ID probes with the write path. Batch create preflight rejects internal duplicates and resident cross-shard duplicates before any relationship row or adjacency leg is written.

**Incoming index structure:** BadgerStore's `inIdx` uses `map[types.NodeID]map[types.RelID]inEdge` (endNodeID -> relID -> `{startNodeID, typeToken}`), not a bare set. The typeToken value enables efficient cross-shard type filtering in `IncomingRelationships` without fetching the relationship entity from a remote shard, and the startNodeID resolves the far endpoint in the same lookup. MemoryStore keeps a plain per-node relationship-ID set (`map[types.NodeID]*adjSet`) since all entities are local.

### Shard Lifecycle

```
                  checkRotation()
                       |
    hot ----[window expires]----> warm ----[ColdAfter]----> cold
  (read-write)                  (writable)               (store=nil)
                                                            |
                                                    [first access]
                                                            |
                                                     lazy-open (writable)
                                                            |
                                                    [IdleTimeout]
                                                            |
                                                     idle-close (store=nil)
```

**Rotation (`RotateHotShard`):**
1. Flush old hot shard
2. Align `timeEnd` boundary to `now.Truncate(time.Millisecond).Add(time.Millisecond)` (snowflake ms resolution)
3. Mark warm tier; keep the owner-shard Badger handle writable
4. Create new hot shard with ms-aligned boundaries
5. Install tracked temporal/high-frequency indexes on the new shard; on failure, close/remove the new shard and leave the old topology unchanged
6. If `ColdAfter > 0` and eligible warm shards exist, demote to cold (close store, set `store=nil`)
7. Update catalog

**Cold shard access (`checkoutStore`/`checkinStore`; returning a `*BadgerStore` and releasing the lock would race idle-close):**
- `checkoutStore` increments `activeReqs`, lazy-opens if `store==nil`
- `checkinStore` decrements `activeReqs`
- `closeIdleShards()` skips shards with `activeReqs > 0`
- idle-close `BadgerStore.Close` errors are recorded, logged, block later lazy checkout, and are joined into `Store.Close`
- explicit `Store.Close` takes the same event-shard mutex before closing or clearing `store`, so close and idle-close cannot race on the shard handle
- after close starts, routing, create-time rotation and every public door return `ErrStoreClosed` (System Overview, "Store index definitions require real targets")
- Background goroutine `idleCloseLoop()` ticks every `IdleTimeout/2`, stopped via `closeCh`; positive `IdleTimeout` is validated as a whole millisecond, and `ColdAfter`/`IdleTimeout` reject negative durations.

**Warm recovery on restart:** Constructor reads catalog, reopens warm shards writable, and recovers cold shards with `store=nil` for lazy writable open on first access.

### Read Operations

**Single-entity reads:** node reads use ref probe + archive fallback + timestamp extraction. Relationship reads use the same first three probes, then fan out across other event shards when the timestamp candidate does not own the relationship.

**Merge queries (AllNodes, AllRels, AllNodeIDs, AllRelIDs, counts, history IDs):**
- `eventShardSnapshot(opts.Depth)` under `mu.RLock` -- snapshot shard list
- Parallel via `sync.WaitGroup`: ref shard sequential, event shards concurrent
- k-way merge of sorted slices

**ForEach iterators (ForEachNodeID, ForEachRelID, ForEachNodeHistoryID, ForEachRelHistoryID):**
- Sequential shard iteration with checkout/checkin -- one shard open at a time
- No goroutines, no `mergeIDSlices` -- trades parallelism for ~83% memory reduction
- Nil callbacks return `ErrInvalidStoreMutation` before any shard callback is entered
- refShard -> archive (if open) -> event shards one at a time
- Each shard's IDs are collected, the shard is checked in, and only then is the caller callback invoked
- Depth-limited history iterators gate archived IDs by current owner first:
  a restored ref-shard entity remains eligible for `DepthHot`/`DepthWarm`
  even if old history records still exist in `refArchive`.

**Cross-shard IncomingRelationships:** Get relIDs from the node shard's inIdx, then fetch each entity through `shardForRelIDChecked` so relationships stored on another event shard, including a cold start-node owner, remain visible. Relationship routing verifies the candidate shard has the entity row, so a stale type-index key in an earlier-probed shard cannot hide a live row elsewhere.

### Write Operations

**Nodes:** Single-shard by primary label classification. `checkRotation()` on all new-entity write paths.

**Relationships:** Shard-based routing via `shardForNodeID` (not class-based -- class only tells you where *new* entities go, not where existing ones live). Batch operations partition by `*BadgerStore` pointer.

**Archive/Restore:** `ArchiveNode(id)` moves a reference node to the archive shard and migrates each live touching relationship's entity/out leg and incoming leg to match the endpoints' new shard locations. `RestoreNode(id)` reverses the placement. Stale source adjacency entries whose relationship row is already missing are skipped during placement planning and purged during final source-node removal. Destination preflight rejects live node/relationship collisions, purges orphan destination adjacency-only entries, and runs before the temporary destination node is written. Rollback on later partial failure undoes completed placement moves (Lessons B7, B72, B224, B231).

**Routing and the move lock (backlog 59, 60).**
- *Event-shard routing.* A snowflake ID maps to the event shard whose time window holds its timestamp. The store keeps `shardIndex`, the event shards sorted by `timeStart` (windows do not overlap; zero-length shards left out), rebuilt under `ts.mu` on every insert or delete of an event shard (open, rotation, purge drop, re-link), and routes with `sort.Search`: O(log shards), 700 shards 3,301 ns to 33 ns per lookup. An ID in a gap or outside every window falls back to the hot shard.
- *Move lock.* `ArchiveNode` / `RestoreNode` move a node and its relationships between the reference shard and the archive in several writes. They hold `moveMu` for writing for the whole move; every read that looks at the reference shard and then the archive, or routes an ID and then reads it, holds the read side across both steps (`refAndArchive`, `lockMoveRead`). Without it a scan returned a node twice (archive copy written before the reference delete) or not at all (reference write lands after the reference read, archive delete before the archive read), and a point read of a live entity answered not-found. Covered: the label, type, property and `All*` scans at `DepthAll`, the ID and count doors, point reads and adjacency (`GetNode`, `GetRelationship`, `Lend*`, `Outgoing*`/`Incoming*`, `*Degree`, `*CurrentStamps`, the integrity hashes), and the tx-membership fan-out below. `ForEachNodeID` / `ForEachRelID` collect both shards\' IDs under the lock and call `fn` after releasing it. Not covered, by design: history reads (history rows stay on their shard and the walks fall back to every shard, so a move leaves their answer unchanged), the DocValues folds (a move bumps `NodeMutationEpoch`, so the caller\'s Gate-2 re-check discards the answer). Lock order: `moveMu` before `ts.mu` and `archiveMu`; a door holding it must not call back into a door that takes it again. Cost: one uncontended `RLock` per call, no allocation (`GetNode` 160 ns to 175 ns).
- *Writes are not fenced.* The write doors route without the lock: a graph is safe (`g.Tier().Archive` / `Restore` hold the graph\'s exclusive lock), but a direct `tiered.Store` user must not write the moving node or its relationships during a move.

**Tiered delete-with-history:** `DeleteNodeWithHistory` first validates that the supplied relationship tombstones cover every live connected relationship exactly once. It then deletes connected relationships before deleting the node so endpoint checks remain valid. It snapshots each relationship and history tape first; if a later relationship delete or node tombstone write fails while the node still exists, the already-deleted relationships are restored.

**Tiered plain cascade:** `DeleteNodeCascade` snapshots live connected relationships before mutation, skips adjacency entries whose relationship row is already missing, and uses the shard-local cascade path for final node removal so stale adjacency-only entries are purged with the node. If a later live relationship delete or node row delete fails while the node still exists, previously deleted live relationships are restored from the preflight snapshots.

**Composite, property and relationship temporal indexes (`shard_index_fanout.go`, backlog 10, WP-31):** per shard, as on sharded. Each covered shard builds and maintains its own badger index over its own rows; nothing moves on rotation, and a reopened shard rebuilds its entries from the persisted definition. Composites and single-key node (every label) and relationship property indexes cover every shard (reference, archive, every event shard — cold ones opened for the DDL — and the hot shard a later rotation opens); a property lookup checks each shard's copy against the anchor's options and scans a shard whose copy is missing or stale. The relationship temporal index is bounded to the reference, hot and warm shards: a cold demotion frees it, a cold shard opens with its definitions discarded (`badger.Config.DropRelTemporalIndexesAtOpen`, no rebuild), the DDL skips cold shards and the archive, and `PromoteColdShardsAtOpen` rebuilds it at open; a cold shard's rows are never pruned, so answers are unchanged. The reference shard anchors the definitions: create reaches it last, drop leaves it last, a failed fan-out undoes the shards it changed, `syncAnchoredIndexes` copies them onto a new hot shard / the archive and repairs every open shard at store open. Composite lookups fold the shards like `NodesByLabelAndProperty` (event labels allowed). The rel temporal candidate prune asks the already-open event shards in the query's depth and the reference shard only while no archive exists (`ArchiveNode` splits a relationship's row and history between reference and archive). Cost: ~156 B per indexed relationship on hot + warm shards (≈ 5.8–9.2 GB per indexed type at ai-soc's rate with `ColdAfter` = 1 day) and ~310 B per indexed node per open shard; a warm shard rebuilds its relationship index when it opens (build: "Temporal envelope indexes").

**History presence and stamps (`tieredstore_history_presence.go`, `tieredstore_history_stamps.go`):** `HasNodeHistory` / `NodeHistoryStamps` (+ rel) walk exactly the shards `GetNodeHistory` / `GetRelHistory` read (one walk shared by both); a cold shard is opened as `History` opens it and answers by a per-ID key probe (`HistoryPresenceProbeOnly`). When two or more shards hold rows of one ID, the stamps are folded from `History`, which keeps one copy per version. `DurableFlush` folds over the reference shard, the open archive and the open event shards.

**Tx-membership sidecars and valid-end indexes (`tieredstore_tx_members.go`, backlog 58):** tiered implements `LabelTxMembership` / `RelTypeTxMembership` (and the point forms `LabelTxMembersOf` / `RelTypeTxMembersOf`), `NodePropertyTxMembership`, `RelPropertyTxMembership` and `RelValidEnd` / `NodeValidEnd`, so the temporal doors that use them (`ForEachRelEffectiveByTypeFrom`, `RelHistoryCountByType`, the DepthAll history folds) are accelerated as on badger. Each fans out to the reference shard, the archive and every event shard (a closed cold shard is opened, as the DepthAll history fold opens it) and unions the per-shard badger answers: each entity once with the lowest first transaction time; a valid-end candidate listed by several shards takes the highest stamp and bound. The reference and archive reads run under the move lock, so a node moved by `RestoreNode` / `ArchiveNode` mid-read is neither missed nor listed twice. The core uses the membership sidecars for DepthAll reads only; a depth-scoped read keeps the depth fold. `RefineRelValidEnd` / `RefineNodeValidEnd` are no-ops (an entity's rows can span shards), so an entity ended only by a valid-time close is read on every call. `NodePropertyTxMembership` / `RelPropertyTxMembership` union every shard's sidecar and decline (`ErrIndexNotFound`) unless the anchor declares the index and every shard carries a finished copy with the anchor's options (`unionPropertyTxMembers`). Cost: `ForEachRelEffectiveByTypeFrom`, 2,000 relationships over two event shards, 10 live: 2,000 to 10 timelines read, about 490 ms to 11.5 µs per scan (with 7 of 8 event shards cold: 141 s to 0.59 s). Tests: `TestTieredTxMembershipAndValidEndMatchBadger`, `TestTxMembersFanoutListsNodeRestoredMidRead`.

**Earlier lives on other shards (`tieredstore_history_lives.go`, backlog 49):** a row's history is written where its row lives, and a history read of a live row goes to that shard (plus the archive for the reference shard and the reference shard for the archive). A deleted ID re-imported onto another shard — a relationship whose new start node lives elsewhere, a node whose primary label changed class — leaves its earlier lives' rows where they were. Every caller-ID create checks whether a shard its new row's reads do not reach holds history of the ID; if so it records the ID durably in the reference shard's meta (`tiered_history_lives/rel|node/<id>`, fsynced before the row is written) and in an in-memory set loaded at open (a malformed entry fails the open). Relationships: the duplicate probe's walk over every shard (`relCreateProbe`) also asks each shard outside the new row's read set `HasRelHistory` — a RAM presence lookup (the first ask builds that shard's presence set, as the core's re-import check already does) or a key probe on a cold shard; no second walk. Nodes: one more probe of the other side (reference + archive, or the ID's event shard). Generated-ID creates (`AddByID`, `GraphTx.AddRelationships` through `PutRelationshipsBatchGeneratedID`) and rollback restores skip the check. Every history read of a recorded ID (`Get*History`, `Get*Version`, `*HistoryVersionsFrom`, `Truncate*History`, `Has*History`, `*HistoryStamps`) walks every history shard, as the read of a deleted ID does. Re-imports written before v4.51.2 are found once, at the first open (`backfillHistoryLives`, marker `tiered_history_lives_backfilled`): every shard, cold ones included, is opened up to twice — once to read its history IDs, once to look the IDs with no live row on their own shard up in its RAM live-ID index — holding 8 B per such ID during the walk. A record is dropped only by `Clear` (which also sets the marker), never when a purge or a truncation to 0 removes the earlier rows — the reads then walk every shard and find nothing more — so the set is bounded by the number of cross-shard re-imports ever made. The history is not moved with the row: a move is a history write plus a history truncate in the change log, which a replica replays as a truncate of the whole chain. Cost: one set entry per recorded ID; any other read pays one atomic load.

### Admin & Repair (`tieredstore_admin.go`, `tieredstore_repair.go`)

- `ForceRotate()` -- safe wrapper with internal locking
- `ListShards()` -- returns `[]ShardInfo` with live counts for open stores and catalog counts for closed cold shards
- `RebuildCatalog()` -- reconstructs catalog from backing stores, including closed cold event shards opened for counting; save failures restore the pre-rebuild in-memory catalog
- `VerifyShard(name)` -- hash chain verification with immutable-shard caching; cache updates persist the catalog or roll back the in-memory cache fields and return an error
- `Clear()` -- clears every shard, including closed cold event shards and restarted warm shards, then resets persisted verification/count cache fields
- `RunRepair()` -- Phase 1: detect+delete orphaned in/ entries by scanning each shard's incoming-index entries directly, including entries whose end node row is missing. Phase 2: detect+re-create missing in/ entries. Endpoint resolution uses the already-pinned shard snapshot, not fresh live routing. Logs a warning with exact counts whenever it fixed anything — repaired residue from a crash window is operator-visible.
- `RecoverBackgroundError()` -- operator-driven recovery from the sticky background error (recorded on idle/transient cold-shard close failures). Re-probes persistence via an atomic catalog save; success clears the gate in place (no close/re-open), failure retains the original cause joined with the probe failure. Clears the lifecycle gate only — run `VerifyShard`/`RunRepair` for data confidence.
- `MigrateFromBadger(src, dst)` -- copies all entities into an empty destination with automatic ontology routing, loading registries from the source BadgerStore, preflighting entity tokens and relationship endpoints, and saving registries to the destination TieredStore only after success; failures roll back inserted destination entities, restore ontology routing, and restore/remove the destination registry file to match its pre-migration state; nil stores/non-empty destinations/missing source registry metadata for non-empty data return `ErrInvalidStoreMutation`, and closed stores return `ErrStoreClosed`

---

## sharded.Store (`pkg/graph/store/sharded/`) — EXPERIMENTAL (ADR-0007)

Slot-topology Store: N `badger.Store` shards, one per snowflake SLOT, routing every entity by the slot carried in its ID's node field — never by ontology class (that is tiered's job). The design target is the horizontal stage where `ErrSlotNotLocal` becomes "route to the owning machine." Stages S1–S5 (mandatory store, batches/cascade, store-global change-log, ingest lanes, index/stats parity) are implemented and tested; the surface remains **EXPERIMENTAL** in `docs/stability.md` until multi-machine routing is productized.

### Slot Model

```
Dir/
  shard-00/                 # anchor shard (BaseSlot) — MetaKV, registries,
                            #   catalog, change-log watermark, vector-index defs
  shard-01/
  ...
  shard-<SlotCount-1>/
```

`Config{Dir, InMemory, BaseSlot, SlotCount (1..32, BaseSlot+SlotCount<=32), ChangeLog, IngestLanes-agnostic per-shard badger passthroughs}`. A slot CATALOG (claimed range, slot→shard map, ID-discipline marker, format version) is persisted on the anchor shard (`shards[0]` == BaseSlot). Opens FAIL CLOSED: `ErrCatalogConflict` on a config/catalog mismatch (wrong `SlotCount`, missing mapped shard dir), `ErrCatalogCorrupt` on a tampered blob. Every shard is always an open local badger — there is no cold/lazy tier and no checkout/checkin discipline, so the tiered `activeReqs` machinery has no analogue here.

### Slot Routing (O(1) pure function)

`shardFor(id) = shards[ decompose(id).Node - BaseSlot ]` — an immutable O(1) map of the ID, no probe, no ref-shard lookup. A door reached with an ID whose slot is unclaimed fails closed with `ErrSlotNotLocal` (the future "not on this machine" signal). Because slots are assigned at mint time by the per-lane UNIFIED generators (S4, `Config.IngestLanes` on the graph layer), a concurrent-ingest session pins lane→slot and mints BOTH its nodes and rels from one generator, so a whole commit group lands in one slot → one shard → one batched door call.

### Relationship Co-location

A relationship row AND both its adjacency legs (`outIdx`, `inIdx`) live on the REL ID's shard — so `GetRelationship`/`DeleteRelationship` are O(1) single-shard. Adjacency READS (`OutgoingRelationships`/`IncomingRelationships`) are PARALLEL FOLDS across all shards (foreign-ID puts spread rel slots, so rel-slot is never assumed to equal start-slot); endpoint-existence checks read the endpoint's own shard. `PutRelationshipCoLocated` (S3) writes the rel entity + both legs + the co-committed `ChangeRelPut` record in ONE `WriteBatch`, skipping only the same-shard endpoint check (the sharded layer validates endpoints cross-shard first).

### Read / Write Operations

- **Point ops** route by entity-ID slot — single shard, frozen-row/mutable-point-read semantics inherited verbatim from badger.
- **Scans / counts / stats / iteration** fold across shards in parallel with an ID-sorted k-way merge; pagination (`Limit`/`After`) is applied AFTER the merge so it straddles shard boundaries correctly.
- **Batched doors** (`PutNodesBatch`/`…PreEncoded`/`…PreEncodedLog`/`PutRelationshipsBatch`/`DeleteNodesBatch`/`DeleteRelationshipsBatch`) are ATOMIC PER SHARD GROUP — no cross-shard `WriteBatch` exists. Each validates the WHOLE input first (structure, slot-locality, no duplicate IDs, creates-not-present, rel endpoints live, node deletes unconnected across ALL shards), then applies per shard group in ascending shard order; a mid-sequence I/O error returns a typed `*PartialBatchError{Op, CommittedShards, FailedShard, Err}` (fail-LOUD, never a silent cross-shard partial). The `wireBodies[j]`/`logBodies[j]` pre-encoded arrays are sliced per shard group with INDEX ALIGNMENT PRESERVED (an off-by-one is the silent-wrong-answer class).
- **Cross-shard cascade delete** is crash-RECOVERABLE, not crash-atomic: fold-collect every connected rel across all shards, delete each rel on ITS OWN shard in deterministic ascending rel-ID order (each a single-shard atomic `WriteBatch`), then delete the NODE row LAST — a crash mid-cascade leaves dangling RELS but never a ghost-edged node, so recovery always finds a live node to re-drive from.

### Store-global Change-log (S3, `Config.ChangeLog`)

One store-global LSN allocator (`changeLogAllocator`) is injected into every shard via `badger.Config.ChangeLogSeqSource`, so each shard co-commits its own `0x09` records + `LastLSNKey` in its own `WriteBatch` but all records draw from ONE monotonic sequence — a single total commit order across shards. Reseed at open needs none of tiered's persisted-watermark/poison machinery: each shard's `badger.New` folds its durable `LastLSNKey` into the shared allocator via `ChangeLogSeqSource.Observe`. `sharded.Store` satisfies `ChangeFeedCapability` (barrier-first, W-bounded paged k-way min-heap merge over all shards' logs — `Flush` makes every allocated LSN durable, `W = LastCommittedLSN` bounds emission so records allocated mid-drain defer to the next poll), `ChangeLogStatusCapability`, and `TxChangeLogScope` (per-tx buffer folded over every shard; LSNs minted at commit so a rolled-back tx burns none). The feed is topology-independent: a 2-shard sharded primary converges BYTE-EXACT onto a single-badger replica AND a 4-shard sharded replica (records carry entities verbatim; each replica routes by its own catalog).

### Capability Parity (S5)

A label's entities are distributed across slots, so every index/stats capability fans its DDL out to EVERY shard (each badger shard maintains its own index over its local entities, in lockstep via `fanOutUniform`) and folds the per-shard results on read. Implemented: PropertyIndex, RelPropertyIndex, Composite + CompositeIntrospection, TemporalIndex, HighFrequencyIndex, NodePropertyKeyStats, NodePropertyTypeClassCounts, `NodeRangeCardinality` / `RelRangeCardinality` (per-shard exact sum; exact only if every shard is exact), `NodePropertyBounds` / `RelPropertyBounds`, the ordered range and prefix walks (k-way merge, `ordered_scan.go`; full list: `docs/query-planners.md` "Backend coverage"), and Vector (VectorIndex + Options + FilteredVectorSearch). Vector indexes keep ONE index PER SHARD (reusing badger's per-write maintenance — no store-level write-path hooks, avoiding tiered's silent-staleness surface) and merge per-shard top-k globally by `index.VectorDistance` — EXACT for brute-force, sound-approximate for HNSW; the store persists only per-index def metadata (dims+metric) to anchor MetaKV. Also the history sidecars (`HasNodeHistory` / `*HistoryStamps` ask the owning slot), the property tx-membership sidecars (union of the slots, `property_tx_members.go`) and `DurableFlush` (every slot, `durable.go`). Also the DocValues columns (`docvalues.go`: each slot's badger builds and caches its own snapshot, the store streams the slots in slot order, bounds each slot by its exact label count so a slot with members that cannot build declines the whole call, routes a snapshot's `Row` to the ID's slot, and states gen as the slots' per-label epochs summed — `NodeLabelMutationEpoch`) and `RelPropertyTypeClassCounts` (the slots' counters summed; a relationship's row lives on one slot). The typed column scans (`ScanNodeColumns` / `ScanRelColumns`) are not implemented: their batches promise ascending ID order across the whole label, which slot-by-slot streaming does not give.

### Declined-with-reason

Not implemented on sharded: `TransactionTimeQuery`, `HistoryRollbackTrim`, `LabelTxMembership`/`RelTypeTxMembership` (the full-history fold is the correct sharded path; tiered unions its shards' sidecars since backlog 58), and the two depth-iteration accelerators. `OwnedPreEncodedPutCapability` and the pre-encoded put/log capabilities ARE implemented (routed to the owning shard's badger door).

---

## Entity Lock Manager (`pkg/graph/internal/locks/entity_locks.go`)

256-shard mutex array for write-skew prevention. 2KB total.

```go
type Manager struct {
    shards [ShardCount]sync.Mutex // ShardCount == 256
}

func ShardIndex(id snowflake.ID) uint8 {
    return uint8(snowflakepkg.Layout.Decompose(id).Time) & (ShardCount - 1)
}
```

Takes the low 8 bits of the snowflake microsecond timestamp, so consecutive mints spread over the stripes (the pattern repeats every 256 µs).

| Method | Use case | Deadlock prevention |
|--------|----------|---------------------|
| `LockEntity(id)` | Single-entity mutations | N/A |
| `LockTwo(a, b)` | Relationship creation | Ascending shard order |
| `LockThree(a, b, c)` | Relationship update / import (rel + both endpoints) | Deduplicate + sort ascending |
| `LockMany(ids)` | Cascade delete, bulk relationship add | Deduplicate + sort ascending |

Unique-property constraints add a second 256-stripe pool keyed by (label, key, value) (`value_locks.go`, `ValueManager`); a claiming door holds its stripes across its store write and the withdrawal of a failed claim (`uniqueHold`, `internal/core/unique_hold.go`).

Lock order (one place): `txMu` -> `Core.mu` -> entity locks -> value locks -> store locks (`retention_purge.go:196`, `core.go:350`). Inside badger: `flushMu` -> `idxMu` -> sidecar `buildMu` -> `wbMu` (`badgerstore.go:2039`); tiered: `moveMu` -> `ts.mu` / `archiveMu` ("Routing and the move lock").

---

## Registries (`pkg/graph/internal/registry`)

Three independent registries with independent token namespaces (label, rel-type, property-key). Types are internal (`LabelRegistry`, `RelTypeRegistry`, `PropertyKeyRegistry`); the Graph layer owns them and is the only public surface for name↔token resolution.

```
LabelRegistry:       map[string]labelToken   + []string reverse lookup
RelTypeRegistry:     map[string]relTypeToken + []string reverse lookup
PropertyKeyRegistry: map[string]propKeyToken + []string reverse lookup
```

- Thread-safe: `sync.RWMutex`, double-check on write miss
- `GetOrCreate(string)` rejects empty strings (`ErrEmptyName`)
- Growth warning at 60K tokens (92% of uint16), error at 65535
- **BadgerStore persistence:** Inside Badger as `meta/label_tokens`, `meta/reltype_tokens`, and `meta/property_keys` (msgpack)
- **TieredStore persistence:** Flat msgpack file at `data/meta/registry.msgpack` (atomic write via write-tmp+rename with raw-file rollback on write failure)
- Loaded on `Graph.New()`, saved on `Graph.Close()`
- Registry-creating public APIs run under the graph lifecycle lock and re-check `closed` after acquiring it, so token allocation cannot race `Graph.Close()` registry persistence.
- Public mutators that can return an error must report lifecycle failures instead of silently no-oping after close. No-error ID generators return zero after close and use lock-free internal helpers inside already-locked write paths. No-error list/read surfaces over graph-owned maps, such as `g.Index().Providers()`, must check the closed flag while holding the graph read lock before returning shared state.

---

## Design invariants

Invariants not already stated in the sections above (those are not repeated here).

- **Pure-data entities**: `Node` and `Relationship` hold no references to Graph, registries or resolvers.
- **Defined ID types**: `types.NodeID` / `RelID` / `EntityID` are defined types over `snowflake.ID` (`type NodeID snowflake.ID`, not an alias); `SnowflakeID()` is the one bridge, so entity method signatures do not leak `snowflake.ID`.
- **Copies and tokens**: besides the accessors under Core Types, `AllLabelTokens()` and `PropertiesMap()` return copies; `PropertyValueEqual` compares without handing out stored reference values. Label/type tokens are unexported types (`labelToken`, `relTypeToken`); `HasLabelTokenRaw(uint16)` / `HasTypeTokenRaw(uint16)` are the zero-allocation hot-path checks, and token 0 is false everywhere. Constructors keep token-0 payloads representable; Store writes reject them (`ErrInvalidStoreMutation`). `NewNode` deduplicates extra labels and removes the primary from them.
- **Property allowlist**: accepted built-ins are the concrete scalars (`bool`, `int*`, `uint*`, `float*`, `string`), `[]string`, `[]int`, `[]int64`, `[]float32`, `[]float64`, `[]byte`, `[]bool`, `[]any`, `map[string]any`, `map[string]string`, and registered custom structs; named aliases, pointers, unregistered structs, arrays, channels, functions and unsafe pointers are rejected at any depth (`ErrUnsupportedValueType`); deeper than 32 levels returns `ErrMaxDepthExceeded`. `RegisterPropertyStructType(nil)` is refused (pass a zero value or typed pointer). `PropertySlice.Set` and `Delete` reject `tkg_` keys (`ErrReservedPrefix`). `NewPropertySlice(map)` validates, copies and sorts once (O(N log N)).
- **Nil receivers fail closed**: nil `*types.Node` / `*types.Relationship` accessors return zero values, no-error mutators no-op, error-returning ones return `ErrNilNode` / `ErrNilRelationship`; nil `*PropertySlice` mutators return `ErrNilPropertySlice`; `ConstraintSet.ForEach(nil)` returns `ErrInvalidTemporalConstraint`. `g.Resolve()` returns `(nil, false)` for an entity without temporal or integrity metadata.
- **Names validate before work**: label/type query and count APIs reject empty, whitespace-only or overlong names before registry lookup (adjacency's empty `typeName` means all types); label mutations and relationship-type creation reject them before property validation, lookup, snapshots, ID generation or writes.
- **Update maps**: a `nil` value deletes the key; an empty map is a no-op without lock or version bump (`UpdateWithTx` with an empty map returns `ErrTxOrder`).
- **Put vs Replace**: `Put*` rejects an existing ID (`ErrNodeExists` / `ErrRelExists`), `Replace*` a missing one (`ErrNodeNotFound` / `ErrRelNotFound`); labels, type and endpoints are immutable through Replace.
- **Cascade delete** removes outgoing then incoming relationships (a self-loop's second visit skips `ErrRelNotFound`) and writes every tombstone in one `DeleteNodeWithHistory` call.
- **Absence over an interval is a void row**, not a delete or retraction (`ExciseNodeValidity` / `ExciseRelValidity`; history readers skip voids); api.md "Excise (valid-time)".
- **History sidecars equal their fold**: `HasHistory(id) == len(History(id)) > 0` and `LatestStamps(id)` equals the fold of `Get` + `History` at every moment (pending writes, flush, reopen, compaction, purge, erasure, `Clear`, replica apply).

---

## Key Architectural Patterns

Distilled from real bugs; the rules are the lessons in [CONTRIBUTING.md](CONTRIBUTING.md#lessons).

### Two-Phase Operations

Multi-step mutations split work into a read/validate phase and an apply phase. The two phases differ in their error contract:

- **All-or-nothing**: `DeleteNodeCascade` (preflight reads adjacency, skips stale adjacency-only entries, and restores completed relationship deletes if a later step fails before the node row is removed) and `CreatePropertyIndex` (placeholder install + dirty-map tracking + atomic install — lesson 11).
- **Per-operation error reporting**: `BatchBuilder.Execute` runs Phase 2 against each queued operation; failures are recorded as `BatchError` entries on the returned `BatchResult` rather than aborting the batch. If any operation fails, `Execute` also returns an error wrapping `ErrBatchFailed`, so a normal error check cannot miss partial failure.

Used by: `DeleteNodeCascade`, `CreatePropertyIndex` (all-or-nothing); `BatchBuilder.Execute` (per-op result reporting).

### Deep-Copy / Frozen Rows at Store Boundary

`Put*` deep-copies before caching and freezes the cached entry (`types.Node.Freeze()` / `types.Relationship.Freeze()`). Point reads (`GetNode`/`GetRelationship`) deep-copy on return, so callers get mutable, independent copies. Plural/scan reads (`*ByLabel*`, `All*`, `Get*ByIDs`, adjacency traversals, temporal/index scans) return the shared frozen pointer directly — zero-copy. Frozen entities reject mutation: error-returning mutators return `ErrFrozenNode`/`ErrFrozenRelationship`, void/bool mutators panic, and `DeepCopy()` is the thaw operation. Rows for duplicate requested IDs may alias the same frozen pointer. Internal locked methods (`getNodeLocked`) skip the copy when the caller already holds the write lock. The frozen guard also covers the pointer escape hatches: on a FROZEN entity, `Temporal()` and `Integrity()` return independent copies (Signature bytes cloned) instead of the shared internal pointer — their exported fields would otherwise let a scan consumer silently corrupt the canonical cached row (the poisoning tests in `pkg/graph/frozen_poisoning_test.go` pin this). Unfrozen entities keep the shared-pointer contract the graph layer relies on. `AddByIDIfAbsent` deep-copies on the found branch so both branches return mutable results.

### Temporal Data Is Append-Only

No mutation door physically deletes history (only the opt-in admin doors — `CompactHistory*`, retention purge, exact erasure — and the `GraphTx` rollback trim of rows the transaction wrote remove history rows). Delete paths save tombstone versions (with `DeletedAt`/`ValidTo`) before deletion. Past-time queries reconstruct deleted entities from history — including transaction-time reads: a delete is a transaction-time tombstone, so any `TxAt`/as-of read pinned BEFORE the delete returns the entity in its pre-delete belief state (the post-pin `DeletedAt`/`ValidTo`/`TxTo` stamps are normalized away on a copy), and any pin at or after the delete excludes it (lesson 60).

For single-shard stores, the tombstone write and the entity deletion are combined in a single atomic Store call (`DeleteNodeWithHistory` / `DeleteRelWithHistory`). All ops land in one Badger `WriteBatch.Flush()` under the same `idxMu.Lock()`, eliminating the crash window that previously existed between N+2 separate store calls. Tiered stores preserve that per-shard atomicity, preflight relationship tombstone coverage before the first delete, and add rollback around plain and history-carrying pre-node relationship deletes when a later step fails before the node is removed. Rollback failures are surfaced with the primary error.

Delete tombstone payloads are validated before deletion. A relationship tombstone must describe the same relationship ID, type token, and endpoints as the live row it closes; otherwise the Store returns `ErrInvalidStoreMutation` and leaves the live row and history unchanged.

### ForEach for OOM-Safe Iteration

Callback-based iteration (`fn(snowflake.ID) bool` -- return true to continue, false to stop) avoids materialising all per-shard slices and preserves early stop. Store implementations collect a bounded shard/page snapshot, release backend locks or check in the Tiered shard, then invoke the caller callback. Callbacks may call Store methods. Badger history iterators overlay pending truncation deletes and stop at the start high-water mark so callback-created higher history IDs do not extend the same iterator. TieredStore still iterates shards sequentially, one shard snapshot at a time.

### Cross-Shard Split-Write Ordering

E->R: write ref shard in/ first (critical path for `Case <- Signal` queries). R->E: write entity shard first. Both endpoints verified before any partial writes. Mutation paths roll back completed split writes where the inverse operation is available, keep undo logs for completed placement moves, and include rollback failures in the returned error. `RunRepair` is a diagnostic/reconciliation layer for corruption or failures that outlive the write path.

### Shard-Based Routing (Not Class-Based)

Class tells you where *new* entities go. Shard tells you where *existing* entities live. After rotation, two event entities in different shards both classify as `ClassEvent`, but `shardForClass(ClassEvent)` returns only the hot shard. Always resolve the actual shard via snowflake ID timestamp or ref probe.

### Corruption Cleanup

Delete and repair paths that tolerate missing entity rows must still clean the
secondary indexes that referenced those rows. In particular, an adjacency entry
whose relationship entity is already gone must have its relationship ID purged
from type, outgoing, and incoming indexes before the delete path returns
success. Relationship entity rows are the liveness source for `relIDs`; restart
rebuild must not let stale type/outgoing keys create live relationship IDs, while
incoming-only keys remain visible so tiered cross-shard repair can reconcile or
purge them.

Badger restart rebuild treats node and relationship entity rows as the only
liveness source for `nodeIDs` and `relIDs`. Label, type, and outgoing index keys
without entity rows are ignored instead of creating live IDs; decoded entity rows
rebuild missing label/type/outgoing membership from canonical row state.
Every fixed-width Badger prefix scan must also require the exact expected key
length before parsing IDs; overlong keys are corrupt or future-format input, not
valid rows with trailing bytes.

Tiered catalog JSON is validated before shard handles are reopened. Duplicate
names, multiple hot event shards, invalid kind/tier combinations, unsafe paths,
negative stats, and zero or inverted event windows are rejected at load time.

### Canonical State for Hash Computation

Hash inputs must come from the internal canonical representation (deduplicated, registry-resolved), never from raw user input. Audit: `grep -rn 'ComputeNodeHash\|ComputeRelHash' pkg/` -- every call site must pass canonical labels/type.

### Event Bus (Copy-Then-Invoke + Safe Recovery)

The zero-value `EventBus` publishes lifecycle events (`EventNodeCreate`, `EventNodeUpdate`, etc.) synchronously. `Subscribe` lazily initializes the handler map, so `events.NewEventBus` is a convenience constructor rather than a required initialization step. Nil subscriptions and nil bus receivers are ignored and return no-op behavior. To prevent deadlocks when an event handler re-enters the Graph (e.g., to query the mutated entity), the bus copies the handler slice under `RLock` and invokes handlers *outside* the lock. Handlers are executed via `safeInvoke(h, e)` which defers `recover()` to isolate panics and logs them via `slog` without crashing the mutation caller.

**Tx/batch event buffering:** During a transaction or batch (`txEventBuffer != nil`), `publishEvent` appends to a buffer instead of dispatching. On transaction `Commit` and successful batch `Execute`, events are published after graph locks are released so handlers can safely call Graph read methods. `GraphTx.Rollback` discards its buffer. Both tx and batch flush paths use `Publisher.PublishBatch` so an `AsyncEventBus` sees the complete mutation burst before applying priority ordering.

**AsyncEventBus:** `NewAsyncEventBus(config)` provides async delivery via a serialized dispatcher with per-priority `[5]chan Event` queues. A zero-value `AsyncEventBus` lazily starts with default config on first non-nil subscribe/publish/close; nil subscriptions are ignored and do not start the dispatcher, and nil bus receivers no-op. The constructor accepts worker count for compatibility plus queue size and backpressure; worker counts greater than one are capped at one dispatcher because lower-priority handlers cannot run concurrently with higher-priority batch handlers without breaking strict priority. `BackpressureStrategy` controls full-queue behavior (Block/DropOldest/DropLatest); unknown strategy values normalize to `BackpressureBlock`. The dispatcher drains in Critical->High->Normal->Low->Deferred order. `PublishBatch` raises a per-batch priority ceiling and clears it at end-of-batch so a saturating in-batch wake-up cannot dispatch a pre-existing lower-priority event before later same-batch higher-priority events have been enqueued — even under BackpressureBlock with QueueSize less than the per-priority batch size. `Close` marks the bus closing, closes `stopCh` to unblock `BackpressureBlock` publishers waiting on full queues, waits for the publish gate, then drains accepted events before the dispatcher exits. Post-close `Publish`/`PublishBatch` cannot enqueue, and post-close `Subscribe` returns a no-op unsubscribe.

**EventPriority:** 5 levels -- `PriorityNormal` (0, zero value), `PriorityHigh` (1), `PriorityCritical` (2), `PriorityLow` (3), `PriorityDeferred` (4). Graph assigns internally: creates->High, deletes->Critical, updates->Normal.

### Vector Index Creation

`CreateVectorIndex` installs a placeholder before scanning so concurrent writes
can maintain the index during backfill. If the scan encounters an operational
read error or a matching vector property with the wrong dimension, creation
removes the placeholder and returns the error. A successful create means every
eligible existing vector row was either indexed or superseded by a concurrent
mutation tracked by the placeholder.

Vector index configuration is validated before placeholder installation:
`dims` must be positive and `metric` must be `DistanceCosine` or
`DistanceEuclidean`. BadgerStore and TieredStore apply the same validation when
loading persisted vector-index definitions, so restart cannot revive a
definition that would disable dimension checks.
Tiered vector definition files are treated as map-shaped metadata: conflicting
duplicate `(label, property)` records are invalid and identical duplicates are
collapsed before rebuilding entries.
Persisted property/vector index definitions also reject reserved `tkg_` property
keys because shadow properties are graph-resolved metadata, not stored vector or
property-index targets.
The vector sentinels are canonical in the public `pkg/graph/store` contract
because the distance metric and vector capability interfaces live there; root
`pkg/graph`, `pkg/graph/index`, and concrete backends expose aliases for
compatibility.

Graph-level index create methods validate label/property-key inputs and create
the label token before calling the Store capability. Creating an index before
the first matching node is therefore a real future-label index, not a successful
no-op.

After creation, node writes validate active vector indexes before changing
store state. A matching vector property with the wrong dimension returns
`ErrDimensionMismatch`; the write is rejected instead of committing a node that
would be absent from the maintained index.

Badger property, temporal, high-frequency, and vector indexes, plus Tiered vector indexes, are
created in phases. Failure cleanup is scoped to the placeholder pointer
installed by that create call, and finalization first verifies that the map
still points at the same placeholder. A concurrent drop or drop+recreate
therefore cannot make an older failing create delete or finalize a newer index.
Those placeholders are not query sources: Badger property, temporal, and
high-frequency fast paths ignore building placeholders and use the complete
scan path, while Badger and Tiered vector search return `ErrVectorIndexNotFound`
until a vector create finalizes.

### Type-Tagged MsgPack Serialization

To reverse MsgPack's type-destructive behavior (e.g., `int64` downcast to `int8`, `[]string` to `[]any`), `wire.go` stores a 1-byte type tag alongside every property value. This ensures absolute Go type fidelity across the persistence boundary, which is critical for deterministic hashing and schema validation.

Registered custom property structs use the custom property type tag with a registered element type name and a MsgPack payload. Registration rejects untyped nil because it carries no type to register; callers must pass a zero value or typed pointer. Property ingress validates the `DeepCopyValue` result and preserves whether the original form was `T` or `*T`; decode reconstructs the value through the type registry with the same shape. Encode rejects custom values that cannot be MsgPack-encoded or whose MsgPack round-trip changes `HashBytes`, preventing silent conversion into generic maps.

Badger current-entity and history reads treat persisted bytes as a trust boundary. After MsgPack decode, `WireToNodeChecked` / `WireToRelChecked` validate IDs, token ranges, label-list canonicality, base-entity IDs, and property-wire invariants before constructing entities, preventing token truncation or invalid token-0 entities on semantically corrupt rows. The checked encode side rejects nil entity pointers before reading IDs or properties.

Import treats record streams as untrusted input. `ImportOptions.MaxStagedBytes == 0` is unlimited, positive values cap the staging file, and negative values return `ErrImportSizeLimit` before staging-file creation. Replay requires one header and one registry record, validates current node/relationship counts against the header, rejects duplicate singleton/current/history-version records inside one stream, and before it constructs an entity it rejects malformed MsgPack bodies, invalid entity IDs, lossy version values, non-canonical label token lists, unknown property type tags, unknown record tags, reserved `tkg_` property keys, unsorted/duplicate property keys, invalid property values, and negative base-entity IDs with `ErrCorruptExport`. It also applies the destination graph's property-count and recursive string-size limits to imported property records before installing them.

---

## Configuration (canonical field list)

The full, current field list of the five config structs; a config field's contract lives here.

- **`Graph.Config`**: `SnowflakeNodeID` (0-15), `Store`, `BadgerDir`, `BadgerInMemory`, `Validation` (ValidationLimits). Whitespace-only `BadgerDir` rejected. Omitted `Store` selects the default backend path; typed nil Store interfaces are rejected with `ErrNilStore`. Core-level (NOT store pass-throughs) — four fail-closed gates, all off by default: `AllowTxBackfill` (a create door honoring a caller-supplied `tkg_tx_from` / `AddWithTx(…, txFrom)`, and the caller-instant `DeleteWithTx` / `UpdateWithTx` doors with their GraphTx, batch and ingest twins; otherwise `ErrTxBackfillDisabled`), `AllowRetentionPurge` (`g.Admin().PurgeExpiredNodes`; otherwise `ErrRetentionPurgeDisabled`), `AllowReset` (`g.Admin().Reset()`; otherwise `ErrResetDisabled`), `AllowExactErasure` (`g.Admin().ExactErase`, bounded hard removal of declared node/rel sets incl. history and index residue; refused while change-log material is retained; otherwise `ErrExactErasureDisabled`) — plus `ChangeLog`, `ReadOnlyReplica`, `ReplicationSource`, and `IngestLanes uint8` (0 = off; extra per-lane unified ID generators for concurrent ingest, requires `2+IngestLanes ≤ 32`). `UntimedValidStart` (backlog 73: where an untimed first version's valid time starts; zero value `UntimedDefault` = `UntimedUnbounded` for new stores, existing stores keep their persisted mode; `UntimedWholeAxis` since v4.65.0; [persistence.md](persistence.md)). `DisableIntegrityHashes bool` (backlog 61, default false = hashes on; off computes and stores no integrity hash, `Verify*Chain` / `VerifyShard` return `ErrIntegrityDisabled`, `RelSegments` refused; persisted with the store, a reopen under the other setting fails with `ErrIntegrityModeMismatch`; see [persistence.md](persistence.md)). `DurableCommit bool` (backlog 11, off = today: no flush on commit): `GraphTx.Commit` (so `Tx().Run*`), `Batch.Execute` and the strong ingest applier call `store.DurableFlushCapability.DurableFlush` once after the group is applied and the graph locks are released — the whole pending buffer as one WriteBatch plus a WAL fsync, on every open shard that took writes (tiered: reference shard, open archive, open event shards, never lazy-opening a cold one; sharded: every slot; a shard with nothing unsynced is not fsynced again; under `SyncWrites` no extra fsync). A crash after the door returned keeps the whole group; before it, any subset (not promised). Rollback never flushes (under `SyncWrites` it ends with one durable flush, below). A flush failure returns `ErrCommitNotDurable` wrapping the store error: the group is committed in memory (tx done, change-log records minted, not rolled back), its ops stay pending, and the next successful flush (next durable commit, an empty `Tx().Run`, the background tick, `Close`) persists it; `RunWithLSN` still returns the committed LSN, and `Batch.Execute` returns its result with a `durable-commit` `BatchError` (error wraps `ErrBatchFailed` and `ErrCommitNotDurable`), so every ingest submitter of the group fails. `DurableFlush` re-checks closed under `flushMu` (a commit can race `Close`; sharded holds `s.mu` across the fold). Power loss: Badger's `Sync` covers only the active memtable WAL and current value-log file, so a flush that filled the memtable can leave rows unsynced — process-crash safe, not power-loss safe; `SyncWrites` for strict durability (backlog 15). With both, the group's mutations run under the exclusive graph lock with the per-mutation `SyncWrites` flush held (`store.MutationFlushHoldCapability`, badger), and the commit's `DurableFlush` syncs them as one WriteBatch (backlog 52; `MaxPendingWrites` bounds the held buffer; `Rollback` ends with one durable flush; side effect: with both flags each GraphTx mutation takes the exclusive `c.mu.Lock`, so standalone writers, readers and concurrent ingest lanes wait per tx mutation). Not covered: standalone mutations and concurrent-mode ingest `Submit`. A buffer above Badger's transaction size limit is split by Badger (no all-or-nothing on disk; that is v5). `New` declines with `ErrCapabilityNotSupported` for a store without stable storage (memory, `BadgerInMemory`, in-memory tiered/sharded). ADR-0011: `RelSegments []RelSegmentSpec` (declared bulk relationship types — `Type`, `Columns` of `{Name, Kind}`, `IntegrityBlockRows` 0 = 64 or a power of two ≤ 4,096; empty = no segments, today's behavior; each type's token is created at `New`; a backend without `store.RelSegmentCapability` — every backend but memory in S2 — fails `New` with `ErrCapabilityNotSupported`, a malformed or duplicate spec with `ErrRelSegmentDeclaration`) and `SegmentMemoryBudget int64` (unsealed bytes of all declared types by `ApproxHeapBytes` before a seal; 0 = 256 MiB, negative fails `New`). `g.Admin().SealRelSegments(type)` seals now; `g.Admin().RelSegmentStats(type)` reports the layout (`Sealing`: the background sealer is running). Budget seals run on one background sealer per store; a writer twice the budget ahead seals itself and blocks; `Close` waits for a seal in flight. `g.ScanRelSegments(type, props, fn)` is the segment-column door (S5; ok=false for an undeclared type or a non-declared property). `SegmentDir string` (S3; empty = in-RAM segments): sealed segments become self-contained files there, listed in its manifest and read memory-mapped; rows leave the memtable only after the manifest is durable; `New` takes the directory's lock (`ErrRelSegmentDirLocked`), recovers it (the manifest is the truth; every listed type must be declared identically, else `ErrRelSegmentDeclaration`; every listed file is mapped and verified, else `ErrRelSegmentFileInvalid`, a bad manifest `ErrRelSegmentManifestInvalid` — both fail closed and touch nothing; then .tmp and unlisted segment files are removed) and serves the listed segments' rows as sealed rows. Durable are the sealed rows only: nodes, memtable, overlay, row-store history and indexes stay process memory. Requires `RelSegments` and `store.RelSegmentDirCapability` (memory); whitespace-only rejected. `Clear` resets the directory.
- **`ValidationLimits`**: `MaxLabelsPerNode` (50), `MaxPropertiesPerEntity` (1000), `MaxPropertyKeyLength` (256), `MaxPropertyValueSize` (64K strings nested inside property values), `MaxPropertyContainerLength` (100000 — the aggregate element/entry count of a slice- or map-typed property value, or the byte length of a `[]byte`; a SEPARATE knob from `MaxPropertyValueSize` so a legitimate large numeric container, e.g. a vector-index embedding `[]float32`, is bounded on its own natural scale), `MaxNameLength` (256). `AllowSelfLoops` (default `false` — reject self-loop relationships where start == end; set `true` to permit). Zero = default for numeric limits.
- **`badger.Config`** (aliased `tiered.BadgerStoreConfig`): `Dir`, `ValidAxis` (owner-injected: the valid axis of a tiered/sharded shard whose mode lives on the reference/anchor shard; zero = legacy), `InMemory`, `Logger` (Badger logger, nil uses default), `CacheCapacity` (10K), `FlushInterval` (100ms), `GCInterval` (5min), `GCDiscardRatio` (0.5), `ReadOnly` (explicit read-only opens only; Tiered warm/cold owner shards use writable handles), `SyncWrites` (fsync after every write — disables async buffer, forces FlushInterval=0), `MaxPendingWrites` (100K ops — async write-buffer bound; at the bound the writer flushes synchronously; negative disables; under SyncWrites it bounds only the writes a DurableCommit group holds). Since v4.5 it has also grown `Compression` / `ZSTDCompressionLevel`, `CacheBudgetBytes`, `ResidentCache`, `LabelIndexOnDisk` / `AdjacencyIndexOnDisk` / `PropertyIndexOnDisk` / `TemporalIndexOnDisk`, `DisablePlannerStats`, `HistoryDeltaEncoding` / `HistoryAnchorInterval`, `ValueLogFileSize` / `MemTableSize` / `BlockCacheSize` / `IndexCacheSize` / `NumCompactors`, `EncryptionKey` / `EncryptionKeyRotation`, `ChangeLog`, `DropRelTemporalIndexesAtOpen` / `HistoryPresenceProbeOnly` / `ColumnsOnDisk`, and the owner-injected `SharedMutationEpochs` (one node/relationship epoch pair every shard of a tiered store advances), `SharedFlushHold` (tiered: one `SyncWrites` flush-hold flag, so a shard opened during a DurableCommit group starts held), `OnCommitFloor` (the commit-floor persistence hook, backlog 44), `SharedOrdinals` (one dense-ordinal allocator for the sharded store's slots), `DisableOrdinals` (no dense ordinals; tiered sets it), `ForeignNodeOrdinal` (sharded: a cross-slot endpoint's ordinal), `ChangeLogSeqSource` / `OnChangeLogFlush` (tiered/sharded: one store-global LSN allocator and its watermark persistence) and `PropertyKeyRegistry` / `OnPropertyKeyGrow` (tiered: one canonical property-key registry, persisted write-ahead to the reference shard) — several behavior-changing or fail-closed (compression `options.None`/`Snappy`/`ZSTD`, zero = Badger default Snappy; `ZSTDCompressionLevel` 1–15, zero = Badger default 1; `CacheBudgetBytes` 0 = off — byte-budgeted entity caches via `types.ApproxHeapBytes`, soft limit, dirty entries never evicted; `LabelIndexOnDisk`/`AdjacencyIndexOnDisk` 0/false = RAM maps, opt-in answers label/adjacency snapshots from the persisted keyspaces via prefix iteration with a pending-write overlay, no migration for existing dirs; `PropertyIndexOnDisk` false = RAM maps, opt-in answers equality/range reads from the persisted `0x0A` keyspace via prefix/range iteration with the same overlay, backfills an existing directory exactly once when turned on, requires a wired property-key registry, `NodeRangeCardinality` always declines `exact=false` in this mode; `TemporalIndexOnDisk` false = rebuild-at-open via full fetch+decode, true persists a compact raw-entry log under `0x0B` so open streams a prefix iteration instead — a rebuild-at-open accelerator only, the maxTo-augmented index itself always stays fully RAM-resident at runtime, backfilled once via a `temporal_index_on_disk_built` marker; `ValueLogFileSize`/`MemTableSize`/`BlockCacheSize`/`IndexCacheSize`/`NumCompactors` 0 = Badger stock, validated vlog `[1MB,2GB)`, memtable `[8MB,1GB]`, block/index cache `>=0`, compactors `0` or `>=2`, one shared `buildBadgerOptions` path, shrinking `MemTableSize` on a live-WAL dir triggers `MigrateOversizedWAL`; `EncryptionKey` length must be 0/16/24/32 bytes else `ErrInvalidEncryptionKeyLength`, `EncryptionKeyRotation` 0 = Badger stock 10-day rotation, encryption REQUIRES both `BlockCacheSize > 0` and `IndexCacheSize > 0` (Badger would panic at `Open` / the first encrypted flush) — `New` fails closed with `ErrEncryptionRequiresBlockCache` / `ErrEncryptionRequiresIndexCache`, a wrong key or a key applied to a previously-plaintext dir fails `New` with a wrapped `badgerv4.ErrEncryptionKeyMismatch`; `DisablePlannerStats` false = query-planner stats maintained every write via `adjustNodePropertyKeyCounts`, true skips the sweep and the open-time rebuild and makes `NodeCountByLabelAndPropertyKey`/`NodePropertyStats`/`NodePropertyStatsSketch`/`NodePropertyTypeClassCounts` fail closed with `ErrCapabilityNotSupported` at runtime — not a correctness knob, unique constraints and `NodeRangeCardinality` use the property index so are unaffected, ~36% faster `PutNode` on wide entities; `HistoryDeltaEncoding` false = full-snapshot history rows as always, true = anchor+delta storage, see "Anchor+delta history storage" under Version history; `DropRelTemporalIndexesAtOpen` false = rebuild the persisted relationship-type temporal index definitions at open, true deletes them instead (tiered cold shards keep none); `ColumnsOnDisk` false = columnar snapshots in RAM only, true persists built columns as a rebuild accelerator, never a read authority; `HistoryPresenceProbeOnly` false = `HasNodeHistory`/`HasRelHistory` answer from a RAM set of the IDs with history rows built by one key-only scan on the first call (measured 16-26 B per ID, observable via `Store.HistoryPresenceStats()`), true answers each call by a per-ID key probe and never builds the set (tiered sets it on cold shards).)
- **`Graph.Config` badger pass-throughs**: `SyncWrites`, `Compression`, `ZSTDCompressionLevel`, `CacheCapacity`, `CacheBudgetBytes`, `ResidentCache`, `LabelIndexOnDisk`, `AdjacencyIndexOnDisk`, `PropertyIndexOnDisk`, `TemporalIndexOnDisk`, `DisablePlannerStats`, `HistoryDeltaEncoding`, `HistoryAnchorInterval`, `ValueLogFileSize`, `MemTableSize`, `BlockCacheSize`, `IndexCacheSize`, `NumCompactors`, `EncryptionKey`, `EncryptionKeyRotation` and `ChangeLog` are forwarded to the store built from `BadgerDir`/`BadgerInMemory` (ignored when `Store` is supplied explicitly; `DisablePlannerStats` also reaches the default memory store). The other `badger.Config` fields (`FlushInterval`, `GCInterval`, `MaxPendingWrites`, `ReadOnly`, the three open-time knobs, the owner-injected ones) need an explicit `Store`.
- **`tiered.Config`**: `DataDir`, `InMemory`, `RefLabels`, `ShardWindow` (1 week; must be at least 1 minute and a whole millisecond), `CacheCapacity` (10K), `FlushInterval` (100ms), `ColdAfter` (0=never; must not be negative), `PromoteColdShardsAtOpen` (promote every cold event shard back to warm at open and persist it — the only way back from `ColdAfter`; set for one start), `MaxOpenColdShards` (0 = a cold shard opened by a read closes when the read ends; N = up to N stay open, LRU-reclaimed on the idle sweep), `IdleTimeout` (5min when cold enabled; if set, must be a positive whole millisecond), `RelsByMint` (v4.67.0: a rel between reference-class endpoints lives with both legs on its ID's mint event shard; a cold mint shard falls back to the reference shard, counted by `Store.RelsByMintFallbacks()`), `SyncWrites` (v4.67.0: every writable shard, group-synced under `DurableCommit` via the shared flush hold), `AdjacencyIndexOnDisk` (v4.67.0), plus the per-shard badger knobs (compression, `ValueLogFileSize` / `MemTableSize` / `BlockCacheSize` / `IndexCacheSize` / `NumCompactors` — each opened shard owns its own block/index cache, no store-wide budget —, cache budget, encryption, `PropertyIndexOnDisk` / `TemporalIndexOnDisk`, `DisablePlannerStats`, `HistoryDeltaEncoding`) and `ChangeLog`, all passed through `badgerCfg` to every shard (cold shards additionally open with `DropRelTemporalIndexesAtOpen` and `HistoryPresenceProbeOnly`). `RefLabels` entries must be non-empty after trimming whitespace because they share the registry's label-name invariant.
- **`sharded.Config`** (EXPERIMENTAL, ADR-0007): `Dir` (each slot under `Dir/slot-NN/`), `InMemory`, `BaseSlot` (0..31), `SlotCount` (1..32, `BaseSlot+SlotCount ≤ 32`; one `badger.Store` per claimed slot), `ChangeLog` (one injected store-global LSN allocator, k-way-merged feed), `DisablePlannerStats`, `HistoryDeltaEncoding`, and the per-shard badger pass-throughs `Compression`, `ZSTDCompressionLevel`, `CacheCapacity`, `CacheBudgetBytes`, `SyncWrites`, `FlushInterval`, `MemTableSize`, `ValueLogFileSize`, `BlockCacheSize`, `IndexCacheSize`, `NumCompactors`, `EncryptionKey`, `EncryptionKeyRotation`.

## Design rules

Store and graph contracts, one rule per bullet; each bullet's bold title is the citation key (code comments cite them). "lesson N" = [CONTRIBUTING.md](CONTRIBUTING.md) `### N.`; "rule N" = its testing rule N. Rules already stated in the sections above are not repeated.

### Data model

- **snowflake.ID everywhere**: All IDs are `snowflake.ID` wrapped in opaque types (`nodeID`, `relID`, `entityID`). Never use `int64` or `string` for entity IDs.
- **Dual generators**: Nodes use even node field (`SnowflakeNodeID*2`), rels use odd (`*2+1`). Guarantees value-level uniqueness. Range: 0-15 (16 instances). Epoch: `2026-01-01`.
- **Struct alignment**: Node (96B), Relationship (96B) packed by descending alignment. Verify with `unsafe.Sizeof`. The dense ordinal (v4.41) fills Node's 96 B size class at no cost and moves Relationship from the 80 B to the 96 B class (+16 B per row); the endpoints' ordinals fill the rest of that class (88 → 96 B, no allocation change).
- **Validate before generating IDs**: `AddNode`/`AddRelationship` validate before `NextNodeID()`/`NextRelID()`.
- **Validate query/index targets before empty shortcuts**: malformed label/property targets and unknown label/type/index namespaces must not be hidden by empty-result fast paths such as non-positive vector `k` or unregistered temporal label lookups.
- **Validate update maps before snapshots/queues**: transaction updates must validate malformed keys/values before rollback snapshot lookup; batch update queues must use the same validation and provenance shadow-key extraction as standalone updates.
- **Batch empty updates are read-only**: queued empty updates still check entity existence during `Execute`, but successful empty updates must not increment `BatchResult.Updated` or publish update events.
- **Batch creates update graph stats**: successful batch node and relationship creates must increment `NodesAdded`/`RelsAdded` in `g.Stats().Get()` in the same units that they contribute to `BatchResult.Created`.
- **Temporal closes update graph stats**: successful node/relationship `CloseVersion` calls persist state, publish update events, and must increment `NodesUpdated`/`RelsUpdated`; rejected repeat closes and zero-time closes must not.
- **Transaction rollback restores graph stats**: transaction methods may touch the store before commit, but a successful rollback must restore the operation-counter snapshot captured at `BeginTx` so discarded writes do not appear in `g.Stats().Get()`.
- **Transaction rollback snapshot keys include entity kind**: first-mutation snapshots must key by node-vs-relationship identity plus snowflake ID. Caller-supplied imports can produce a `NodeID` and `RelID` with the same underlying value, and rollback must restore both independently.
- **Create registry tokens only after rejecting validation passes**: add-label paths must check node existence, idempotence, and `MaxLabelsPerNode` before creating a new label token. Node create/import/add-label final write failures must restore newly allocated label tokens. Failed mutations must not leave unreachable registry names behind.
- **Batch node labels allocate at execute time**: `BatchBuilder.AddNode` must use non-zero probe tokens for unseen labels while queueing, allocate real label tokens only during `Execute`, retokenize returned node pointers in place, and roll new labels back if persistence fails or panics.
- **Create relationship type tokens after rejection paths**: direct, import, and batch relationship create paths must check endpoints and temporal constraints before creating a token for a previously unseen relationship type. If the final relationship store write fails after allocation, restore the pre-allocation registry snapshot. Failed writes must not leave unreachable registry names behind.
- **Registry rollback windows must be panic-safe**: any path holding the registry allocation mutex across backend Store or index code must release it via deferred cleanup so a custom backend panic cannot deadlock later registry writes. Multi-token allocations must also roll back partial suffixes when allocation itself fails midway, such as token N succeeding and token N+1 hitting capacity.
- **Committed registry tokens must be durable before return**: successful graph writes, batch execution, index creation, and IO import that create label or relationship-type tokens must persist the current registries to persistent stores before returning. Import and transaction rollback must persist restored registry snapshots, not only replace in-memory pointers. If batch registry persistence fails after Store rows were written, surface the error but keep caller-visible entity pointers in the committed shape. If a transaction create/import write commits before the registry checkpoint fails, record the created row for rollback before returning the error. `GraphTx.Commit` must retry the registry checkpoint before making the transaction irreversible.
- **Batch relationship failure rollback**: relationship pointers returned by `BatchBuilder.AddRelationship` are queue-time skeletons until Execute commits. If endpoint refresh, constraints, type allocation, or the final Store write fails, restore `TxFrom`, endpoint hashes, and queued type token before reporting the batch error.
- **Batch queue inputs must be owned**: queue methods must not retain caller-owned mutable maps, slices, or byte slices. Validate first, then deep-copy the queued shape so later caller mutations cannot change what `Execute` applies.
- **Index create label tokens roll back on backend failure**: graph-level index create APIs may create a label token for successful future-node indexes, but must restore the pre-allocation label registry snapshot if the backend create call fails.

### Defensive copying

- **Entity nil guards**: public `types.Node`, `types.Relationship`, pointer-receiver `PropertySlice`, metadata, and integrity helper methods must not panic on nil receivers. Error-returning methods return type-layer nil sentinels; no-error accessors return zero values, no-error mutators no-op, nil `PropertySlice` pointer mutators return `ErrNilPropertySlice`, and nil integrity `DeepCopy` returns nil.
- **Accessors/setters**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, `PropertiesMap()`, `ToMap()`, `DeepCopy()` always return independent copies. `NewPropertySlice`, `PropertySlice.Set()`, `Node.SetProperty()`, and `Relationship.SetProperty()` must deep-copy accepted reference values before storing them, validate the post-copy value, and preserve custom value-vs-pointer shape.
- **Store boundary (since v4.5.0 — frozen rows)**: contract in "Deep-Copy / Frozen Rows at Store Boundary" above. The memory store caches `CompactFrozenCopy` (first versions keep temporal + integrity metadata in one compact object); a new metadata or entity field must be added to `compact.go`'s fit check and rebuild or copy constructors, or it is dropped from cached first versions — `compact_fields_test.go` enumerates the fields by reflection and fails naming it.

### Properties

- **Sorted invariant**: Always use `Set()` — never modify the slice directly. `Set()` validates and deep-copies accepted values before insertion.
- **Bulk construction**: `NewPropertySlice(map)` is O(N log N), validates all values, sorts once, deep-copies accepted reference values, and rejects invalid post-copy values. `SetProperties(ps)` validates, sorts by key, de-duplicates with last-value-wins semantics, and deep-copies before installing. `AddNode`/`AddRelationship` use this path and must check the returned error.
- **Final-shape validation**: Any read-modify-write path that can add/delete/set properties must recheck `MaxPropertiesPerEntity` after the final mutation shape is applied and before persistence. This includes node property CAS, not only bulk update paths.
- **Custom property wire**: Registered custom struct values persist/export as a registered element type name plus MsgPack payload. The wire path must reconstruct through the registry and reject unencodable or lossy custom values whose MsgPack round-trip changes `HashBytes`. `RegisterPropertyStructType(nil)` must reject untyped nil because it carries no type to register; use a zero value or typed pointer.

### Concurrency

- **Lock ordering**: entity locks -> value locks -> idxMu, always; the full chain (txMu, Core.mu, badger flushMu/idxMu/buildMu/wbMu, tiered moveMu) is owned by "Entity Lock Manager" above. Value locks are the separate `internal/locks.ValueManager` class (256 stripes keyed by `hash(labelToken, keyToken, canonical value bytes)`) guarding unique-property-constraint enforcement: the stripe is held across BOTH the index check AND the store write, so concurrent same-value writers serialize to exactly one winner. An update that changes a constrained value takes BOTH the old and the new stripe in ascending order (`LockStripes` sorts + dedups). A caller holding value locks must never take an entity lock afterwards. The order is documented canonically in `internal/locks/value_locks.go`.
- **Two-phase delete with TOCTOU retry**: Phase A reads adjacency under node lock. Phase B locks all entities via `LockMany`, re-reads the node under that full lock, re-verifies adjacency, retries if adjacency changed (max 10), then builds tombstones from the Phase B rows. Never build a node tombstone from the Phase A snapshot.
- **Ascending shard order**: `LockTwo` normalizes. `LockThree` (a relationship's own ID + its start/end node IDs — 3 entities) and `LockMany` deduplicate + sort. Deadlock-free.
- **Transaction isolation via `c.txMu` (v4.1.0+ Path B)**: `Core.txMu` serializes tx-vs-tx and tx-vs-batch; `Core.mu` (an RWMutex) is taken with a brief `RLock` by each tx method around its body (`tx.lockActiveCore` / `tx.unlockActiveCore`) and is **no longer held for the tx lifetime**. Concurrent standalone mutations and reads on disjoint entities run in parallel with an open tx — only entity-level conflicts block, via the 256-shard entity-lock manager. Snapshot-style scans (`IO.Export`, `Temporal.Snapshot`, `Admin.VerifyShard`) and admin cascades (`ArchiveNode` / `RestoreNode`) take `c.mu.Lock()` to fence writers; tx Rollback briefly takes `c.mu.Lock` to swap registry pointers. Isolation level: tx-vs-tx serialized, NOT isolated from standalone operations — tx mutations are write-through (a concurrent reader sees uncommitted tx rows and tx-allocated labels/types before commit), entity locks are per call (a standalone write can land between two tx calls), and `Rollback` restores pre-tx snapshots, overwriting any such intervening standalone write (lost update). This is the deliberate v4.1.0 trade-off, documented in "Transaction isolation — what v4 actually guarantees" above; never describe `g.Tx()` as serializable or isolated. Code needing isolation must serialize its writers externally or use `g.Batch()` (atomic against readers).
- **Inside a tx, both forms work (v4.1.0+)**: because the tx no longer holds `c.mu.Lock` for its lifetime, both `g.Nodes().ByLabel(...)` and the tx-side mirror `tx.NodesByLabel(...)` work inside an open tx. The tx-side read mirrors in `internal/core/tx_consistent_reads.go` remain for call-site clarity but are no longer required for correctness (lesson 31, superseded).
- **No-error resolver reads still need isolation**: `LookupLabel`, `LookupRelType`, node label/type resolution, and shadow-property resolution must take `c.mu.RLock()` before reading registry pointers and return zero values once graph close is visible. Internal code already under `c.mu` must call explicit lock-free resolver helpers instead of recursively taking `RLock`.
- **Graph façade and sub-API nil guards**: public graph façade entry points and sub-API wrappers must not dereference nil internals. Error-returning methods return `ErrNilGraph` for nil, zero-value, or typed-nil graph/core/ops receivers; no-error helpers return zero values.
- **Transaction and context entry validation**: `TxAPI.Run` / `RunContext` must reject nil callbacks with `ErrNilTxCallback` before opening a transaction. `RunContext` and graph mutation context helpers must reject nil contexts with `ErrNilContext` before calling `ctx.Err()` or `ctx.Done()`.
- **IO stream endpoint validation**: `IO.Export` and `GraphTx.Export` must reject nil and typed nil writers with `ErrNilWriter` before stream writes; `IO.Import` (which takes `(io.Reader, ImportOptions)` — the v4.0 merge of the old `Import` + `ImportWithOptions`) must reject nil and typed nil readers with `ErrNilReader` before creating staging files or reading records.
- **Lifecycle isolation via `c.mu`**: any public method that touches store, registries, indexes, or batch queues must participate in `c.mu` (`Core.mu`) and re-check `closed` after acquiring the lock. A pre-lock `checkOpen()` alone is not sufficient because `Close()` sets `closed`, drains `c.mu`, then closes the backing store.
- **Store lifecycle nil receivers fail closed**: nil concrete `*memory.Store`, `*badger.Store`, and `*tiered.Store` receivers return the public Store-layer `ErrNilStore` sentinel from `Close` and `Clear`. `graph.ErrNilStore` aliases that sentinel, so configured typed-nil Store rejection and direct Store lifecycle nil calls are testable with the same `errors.Is` check.
- **MemoryStore zero value is usable**: `var ms memory.Store` must behave like an empty in-memory store. Reads return empty/not-found results, and writes, indexes, and history paths lazily initialize maps before mutation.
- **Persistent Store zero values fail closed**: `var bs badger.Store` and `var ts tiered.Store` are not operational backends, but lifecycle and checked read/write entry points must return `ErrStoreClosed` instead of panicking on nil internal handles.
- **Lifecycle surfaces fail closed when possible**: public mutators with an error return must return `ErrGraphClosed` after close and leave graph-owned state unchanged. Only true no-error surfaces may use post-close no-ops or empty/zero results (zero IDs for `NextID`, empty provider lists). Keep internal call sites on lock-free helpers when the caller already holds `c.mu`. If the method reads a graph-owned map or pointer, check `closed` under `c.mu.RLock`, not only before acquiring it.
- **Transaction rollback dependency order**: restore deleted node rows before restoring any deleted relationships. A relationship deleted earlier in a tx may reference an endpoint node deleted later in the same tx; recreating the relationship first fails referential integrity. Restore pre-transaction history snapshots too, truncate history for created entities that rollback deletes, and restore label/reltype registries to their `BeginTx` snapshots after entity/index rollback.
- **Transaction label rollback**: do not replay multiple label deltas by passing the final snapshot to each Store helper. Label helpers require exact one-token changes. Rollback must compare the current node label sequence to the pre-transaction snapshot, apply exact add/remove helper writes until the target label sequence is live, then call `ReplaceNode` for the remaining non-label state.
- **Batch partial failure is an error**: `BatchBuilder.Execute` may continue after individual operation failures, but the final return must wrap `ErrBatchFailed` whenever `BatchResult.Failed > 0`. Do not rely on callers inspecting result fields to discover failure.
- **Batch builders own queue serialization**: `BatchBuilder` queue methods and `Execute` must take the builder mutex before touching queued slices. `Execute` is one-shot and sets the done flag before replay; later queue calls or repeat `Execute` calls return `ErrBatchDone`.
- **Batch event flushes use PublishBatch**: `BatchBuilder.Execute` buffers mutation events while holding the graph write lock, then dispatches them with one `Publisher.PublishBatch` call after releasing graph and builder locks. Do not loop over `Publish`; async priority ordering depends on seeing the whole batch at once.
- **sync.RWMutex is NOT reentrant**: If A holds RLock and calls B which RLocks, and a writer waits between them, deadlock. Inner methods must be lock-free.
- **sync.Once for idempotent Close()**: Never nil-guard a function pointer across goroutines.

### Persistence

- **Store is pure persistence**: Shadow and string resolution are Graph-layer responsibilities. All queries return `error` and sort by ID.
- **Async batch**: Writes update in-memory immediately, queue `writeOp` structs (last-write-wins dedup). Background flush via `WriteBatch` every `FlushInterval`.
- **LRU dirty tracking**: Dirty entries never evicted (soft capacity). `CollectDirty()` is read-only. `MarkFlushed()` is CAS on `dirtyVer`. `Peek()` for zero-alloc cache hits.
- **Tombstones in cache-first**: A cache miss must not fall through to stale Badger data.
- **Close() must flush unconditionally**: Even when `flushLoop` was never started (InMemory mode).
- **In-memory state must survive restart**: If it's in memory and it matters, it needs a persistence path and a rebuild path. If `loadIndexes()` doesn't rebuild it, it doesn't survive restart.
- **Clean reopen reads keys, not rows (backlog 54)**: a writable badger `Close` that flushed cleanly writes the open image (index contents, `0x0F tkg_open_image*`, header last); `New` trusts it only while the header is the newest badger commit (`db.MaxVersion()`), then rebuilds liveness, label / type / adjacency maps from keys, installs index contents and planner stats from the image (an image without stats defers them to their first read: writes skip them meanwhile, `ensurePlannerStats` in every stats reader). `Close` holds flushMu from the image's drained check until `dbClosed` is set. Anything else — a later commit, a missing chunk, a CRC mismatch — is the full row rebuild below. New RAM state an open derives from rows must go into the image or be built on first use, and `IndexStateDigestForTest` must cover it (`TestCleanReopenMatchesFullRebuild`).
- **Entity rows are liveness sources on restart**: Badger `loadIndexes()` must rebuild `nodeIDs` from node rows and `relIDs` from relationship rows. Label/type/outgoing index keys may supplement decoded corrupt-row diagnostics, but they must not create live entity IDs without entity rows. Incoming-only relationship keys are preserved for TieredStore cross-shard repair without becoming local relationship entities.
- **Fixed-width Badger scans require exact key length**: Prefix scans over entity, secondary-index, adjacency, and history keys must check `len(key) == Size*`, not only `len(key) >= Size*`, before parsing IDs. Overlong keys with valid prefixes are corrupt/future-format input and must not be truncated into live IDs.
- **Relationship existence checks must verify the entity row when correctness depends on liveness**: Relationship entity rows, not type/outgoing index keys, are the liveness source. Cleanup paths, tiered relationship routing, and repair shard probes deciding whether an index entry is live must read the relationship row itself, not trust index-derived membership alone.
- **Repair must purge the whole orphan relationship index set**: When repair proves a relationship entity is absent everywhere, do not delete only the incoming key that revealed the orphan. Use the Badger orphan-index purge path so local type, outgoing, incoming, `relIDs`, counters, and persisted type/out/in keys are cleaned together.
- **Counters in same WriteBatch as data**: Separate transactions = crash inconsistency.
- **Delete batches coalesce duplicates before mutation**: `DeleteNodesBatch` and `DeleteRelationshipsBatch` must normalize duplicate IDs before validation. Otherwise a repeated ID can double-decrement counters, panic after the first delete, or route duplicate work across shards.
- **Batch node deletes must fail closed on connected nodes and roll back shard failures**: `DeleteNodesBatch` is a row delete for unconnected nodes only, like `DeleteNode`. MemoryStore, BadgerStore, and TieredStore must return `ErrInvalidStoreMutation` without mutating any target row when any target node still has outgoing or incoming relationships. TieredStore must preflight every shard bucket before deleting any bucket, and restore earlier accepted node deletes if a later shard bucket fails.
- **Tiered relationship batch deletes must roll back prior shards**: `DeleteRelationshipsBatch` may partition same-shard relationships and replay cross-shard deletes, but a later shard or split-delete failure must restore earlier accepted relationship deletes before returning. `RunRepair` is a diagnostic backstop, not the normal correctness path.
- **Tiered plain cascade deletes need a relationship rollback log and orphan-adjacency cleanup**: `DeleteNodeCascade` deletes connected relationships before deleting the node row. Snapshot connected relationships before mutation, ignore adjacency entries whose relationship row is already missing, let the shard cascade purge those stale index entries, and restore already-deleted relationship rows if a later relationship delete or the node row delete fails while the node still exists. `RunRepair` is not the normal rollback path.
- **Tiered archive/restore must not get stuck on orphan adjacency**: `ArchiveNode` and `RestoreNode` move live connected relationships. If an adjacency entry points at a missing relationship row, skip it during placement planning, purge orphan destination adjacency during preflight, and use shard-local cascade deletion for the old node row so stale source indexes are purged. Do not require a separate repair pass before archive/restore can proceed.
- **Import replay rollback**: `IO.Import` stages reader I/O before taking `c.mu`, validates wire invariants and destination graph property limits before entity construction, then replays records under the write lock. Missing header/registry records, header count mismatches, duplicate records inside one stream, and unknown record tags are corrupt input, not successful no-ops or forward-compatible skips. Any replay path that writes current rows, history rows, or registries must snapshot the pre-import state first and restore it on error; never document a caller-side "import fresh graph" workaround for partial replay.
- **Persisted entity wire is a trust boundary**: Badger current/history reads must use checked wire reconstruction and verify decoded entity IDs against the storage key. Valid MsgPack can still contain invalid IDs, token-0, out-of-range tokens, non-canonical label lists, invalid explicit temporal ranges, unknown property tags, reserved property keys, key/value ID mismatches, or custom values whose `DeepCopyValue` panics; those must return read errors instead of panicking in `types.NewNode`/`types.NewRelationship`, `SetProperties`, returning an entity under the wrong ID, or integer conversion.
- **Wire validation must preserve valid property values and tag type**: Checked wire rejects lossy reconstruction, not valid values the property layer supports. For `float32`, NaN and infinities are valid scalar/slice values; finite `float64` payloads that overflow or round when narrowed to `float32` are corrupt wire. If validation accepts a decoded shape for a typed tag, reconstruction must return that tagged type, not the raw decoded type or a zero fallback. Values nested in `[]any` / `map[string]any` keep their exact Go kind through the nested wire envelopes (`storeutil/wire_nested_temporal.go`): never normalize a nested value at write time, write raw only the kinds msgpack preserves, accept an envelope only in the form the encoder writes, and apply the property depth limit to the reconstructed value, not the raw wire value.
- **Checked wire encoders must check nil first**: `NodeToWireChecked` and `RelToWireChecked` must return an error for nil entity pointers before reading IDs, tokens, properties, temporal metadata, or integrity metadata.
- **Badger update prefetch errors are not repair paths**: `ReplaceNode` and label-token writes prefetch current state outside `idxMu`; after the write lock confirms the node still exists, any prefetch error must be returned. Only cascade delete may scrub indexes and return a corruption error.
- **Badger delete prefetch must re-read under lock**: `DeleteNode` and `DeleteRelationship` prefetch cache-miss rows outside `idxMu`, then re-read the current cached row after `idxMu.Lock()` and use that row's labels/type/endpoints for index cleanup. This keeps normal Badger I/O outside the global write lock without letting Store-level ID reuse make cleanup use stale indexed metadata.
- **Badger SyncWrites includes metadata and helper writes**: Any path that queues Badger writeOps outside the normal entity CRUD methods, including property/temporal/high-frequency/vector index definition create/drop and split relationship helper writes/deletes, must call the synchronous flush path before returning when `SyncWrites` is enabled. Do not call `flush()` while holding `idxMu`; release the write lock first because `flush()` snapshots under `idxMu.RLock()`.
- **Badger reads re-pin across a flush; cache fills carry the flush epoch** (4.37.1): a multi-row read uses `scanSnapshot` (read `FlushEpoch` before opening the transaction; after a cache miss, re-pin when the epoch moved), and every cache fill goes through `LoadCleanAt(key, value, epoch)` with the epoch read before the badger read. A snapshot plus a cache miss does not prove the snapshot sees the latest version: a flush that committed after the snapshot and then let the cache evict the row leaves the read dropped or stale.
- **Badger overlay before snapshot, lazy builds included; built-flags read under `idxMu`** (lessons 64/74): every reader that merges the write buffer (`rangePending` / `pendingHistoryVersionOverlay`) with a Badger view captures the buffer FIRST — `idxMu.Lock` does not exclude an already-parked flush's commit. A lazy one-shot build (K1 membership sidecars, belief watermarks) that misses a row misses it for the store's lifetime, and a door that enqueues without `idxMu` must read the build's "built" flag and enqueue under `idxMu.RLock` (`enqueueVersionAgainstLazyBuilds`), else its row lands behind a running build unrecorded.
- **Badger split and repair helpers use shared ID validators**: Partial relationship helpers and orphan/incoming-index repair helpers must call the shared Store `Validate*ID`/index-entry validators, not only `== 0` checks. Negative IDs are malformed input and must return `ErrInvalidStoreMutation` before not-found, scan, or no-op cleanup behavior.
- **Explicit-ID reads reject malformed IDs before lookup**: `GetNode`, `GetRelationship`, `GetNodesByIDs`, `GetRelationshipsByIDs`, `OutgoingRelationships`, `IncomingRelationships`, and the ForNodes adjacency variants must reject zero or negative caller-provided entity IDs with `ErrInvalidStoreMutation` before returning not-found or doing partial batch work. Adjacency `typeToken == 0` remains the only Store read sentinel for "all relationship types".
- **Bulk ID reads are all-or-error for explicit IDs**: `GetNodesByIDs` and `GetRelationshipsByIDs` must return the typed not-found sentinel when any requested positive ID is absent. Query/index scans may tolerate orphaned IDs at their own boundary, but direct caller-provided ID lists must not report success with partial results.
- **Store label/type reads require real tokens**: direct Store `NodesByLabel`, `RelationshipsByType`, `NodeCountByLabel`, and `RelCountByType` must reject token `0` with `ErrInvalidStoreMutation`. Token `0` is the registry sentinel, not a query target; do not silently return empty results for it.
- **Store writes enforce explicit temporal range shape**: direct Store current-row writes, replacements, history snapshots, delete tombstones, and node label-token helper payloads must reject malformed finite temporal intervals where both `ValidFrom` and `ValidTo` are set but `ValidFrom >= ValidTo`. Open-ended `ValidTo == 0` and derived-start `ValidFrom == 0` remain valid. Use `ErrInvalidStoreMutation`.
- **Memory public APIs fail closed after Close**: every public MemoryStore method with an error return must check the closed flag under `mu` before reading or mutating process-local maps, including empty fast paths, counters, iteration, history, and index APIs. Exported test-only tampering helpers cannot return an error, so they must return no data or no-op after close.
- **Badger public APIs fail closed after Close**: every public BadgerStore method with an error return must check `dbClosed` before serving cache hits, O(1) counters, empty fast-path success, index metadata, history scans, registry data, public `Flush`, or split relationship helper mutations. No-error Badger diagnostics/routing helpers return zero/nil after close. Keep private `flush()` on the Badger `ErrDBClosed` path because it exists to avoid `WriteBatch.Flush()` deadlock after the DB closes.
- **Badger persisted counters are metadata, not arbitrary integers**: `node_count` and `rel_count` must decode as eight-byte, non-negative signed counts. Missing/zero legacy counters are rebuilt from admitted live rows. Positive counters must either match admitted live rows or match the exact raw entity-row count when corrupt rows were skipped for rebuild diagnostics; public counts still use admitted live rows. Negative values and unexplained positive mismatches are corrupt metadata and must fail closed with `ErrInvalidStoreMutation`; do not expose impossible or stale `NodeCount` or `RelationshipCount` results.
- **Badger history ID scans must overlay pending deletes**: `AllNodeHistoryIDsFrom` and `AllRelHistoryIDsFrom` merge pending writes with persisted history keys. They must also mask pending delete writeOps from `TruncateNodeHistory` / `TruncateRelHistory`; otherwise truncated persisted history remains visible until async flush.
- **Ontology mapping nil receivers fail closed**: nil `*ontology.OntologyMapping` receivers behave like an empty mapping. Classification returns `ClassEvent`, registry swaps return nil, and `RefLabels` returns nil.
- **Migration helpers own source registries**: `tiered.MigrateFromBadger(src, dst)` must require an empty destination, load label and relationship-type registries from the source BadgerStore, preflight migrated entity tokens and relationship endpoints against source data, wire destination ontology from the loaded label registry, and save both registries to the destination after a successful copy. Failures must roll back inserted destination entities, restore the previous ontology registry, and restore or remove the destination registry file to match its pre-migration state. Nil stores, non-empty destinations, and missing registry metadata for non-empty data return `ErrInvalidStoreMutation`; closed source/destination stores return `ErrStoreClosed` before an empty migration can report success.
- **Vector searches re-check close and apply QueryOpts before rank loss**: Store vector search implementations release Store-level locks while scanning vector entries and invoking temporal/depth filters. Re-check `Close` state after `VectorIndex.SearchNearest` returns and before returning empty or raw-ID results, otherwise a filter-triggered close can race into a successful post-close response. Direct Store `SearchNearestNodes` must apply temporal/depth eligibility before heap selection and `QueryOpts.After`/`Limit` over distance order; graph-level search must strip cursor opts from backend calls and paginate its resolved current/history slice once.
- **Badger temporal pagination verifies cache misses before limiting**: Badger query methods that use index snapshots plus cache peeks (`NodesByLabel`, `AllNodes`, `NodesByLabelAndProperty`, `RelationshipsByType`, `AllRelationships`) may skip known-invalid cached rows early, but cache-miss candidates are only unknown. Apply `After` by sorted ID first, fetch and check temporal metadata, then consume `Limit` on rows that actually match.
- **Interval QueryOpts require valid active ranges**: `ValidStart` and `ValidEnd` activate interval filtering only when both values are non-zero (0 is the only unset value, a negative bound is a real bound), and active intervals must satisfy `ValidStart < ValidEnd`. Keep graph-level history resolution and Store-level pushdown on the same predicate.
- **Graph query targets validate before lookup**: label/type query and count APIs must reject empty, whitespace-only, or overlong names before treating a target as unregistered. Relationship adjacency helpers use empty `typeName` for "all types" but must validate every non-empty type filter before empty-node shortcuts.
- **On-disk wire format is versioned**: `NodeWire`/`RelWire` carry `FormatVersion` (`fv`; absent = legacy = v1) and badger verifies/stamps a `wire_format_version` meta marker at open. Data written by a newer release fails closed with `ErrWireFormatVersionUnsupported` (store-level at open; per-row at checked decode and during `loadIndexes`). Bump protocol on `storeutil.CurrentWireFormatVersion`: encoders (lesson 39) + decode + marker together. **v2 (current)** moved the transaction-time tail (`tf`/`tt`) from omitempty mid-map fields to a FIXED-WIDTH, always-present TRAILING slot — the last two map entries, full-width int64 (`0xd3` form) — so the ingest pipeline can pre-encode a row on the producer thread with a ZERO tail and the single applier can `storeutil.PatchWireTemporalTail(buf, txFrom, txTo)` in place (marker-validated, fail-closed with `ErrCorruptWire`) instead of a second msgpack pass (ADR-0006 §4.5). The equivalence `Patch(PreEncode(E,0),T) == Encode(E,T)` holds byte-for-byte by construction (the tail is the only difference; TxFrom/TxTo are not hashed, so a patched buffer still passes `Verify*Chain` and replicates verbatim). Decoders accept BOTH v1 (omitempty tail) and v2 (fixed tail) transparently — msgpack keys are self-describing, no read-side version branch. `wire_temporal_tail.go` owns the slot layout, `PatchWireTemporalTail`, `HasWireTemporalTail`, `PeekWireTemporalTail` (read-side: marker-validated tf/tt read with NO unmarshal — badger's as-of reverse walk classifies full rows per version via this peek instead of a decode; delta/v1 rows fall back to the decode), `PreEncode{Node,Rel}WireV2` (untokenized — matches the change-log put body) and `PreEncode{Node,Rel}WireV2WithKeys` (property-key TOKENIZED — matches the persisted entity row). Apply-side CONSUMPTION is WIRED: the optional `store.PreEncodedPutCapability` (`PutNodesBatchPreEncoded`, native memory/badger only — routed for badger, since memory holds live objects and never encodes a row) lets the ingest applier hand the store the producer-thread-encoded row with its tail patched, skipping the second msgpack pass. Threat model: the tail is deliberately OUTSIDE the content hash (TxFrom/TxTo were never hashed), so a patched buffer replicates verbatim and passes `Verify*Chain` — the corollary is tail integrity relies on transport/storage integrity, not the entity hash (see persistence.md). Optional keys a reader may skip are added without a bump (SPEC §9.1a: the nested-kind envelopes). The retraction marker `rx` is a key an older reader must NOT skip, so a retraction tombstone is written at row `fv=3` (`storeutil.RetractedWireFormatVersion`, raised in the encoder by `emittedRowFormatVersion`; only those rows, the store marker stays 2) and every decoder accepts `fv <= 3` via `checkRowFormatVersion`; a 4.49.0 reader fails closed on it, while v4.49.1's `fv=2` + `rx` rows still decode as retractions (and stay unprotected against old readers). An excise's void piece (`vd`, backlog 64) is likewise written at row `fv=5` (`storeutil.VoidWireFormatVersion`; 4 is backlog 61's) and raises the badger store marker; the marker table, the one tracker and the reader limit live in persistence.md "Format versions and markers".
- **Canonical temporal predicates live in storeutil**: `EntityValidFrom` / `MatchesPointInTime` / `MatchesInterval` are the ONLY definitions of effective-valid-from and the point/interval predicates; core delegates, stores push down the same functions. `TestTemporalTwoDoorsAgreeOnLabelQueries` enforces cross-door agreement. `SelectAsOfWithCurrent[T TemporalRow](history, current, hasCurrent, pin)` (`asof_select.go`; `SelectAsOf` is its history-only form) is the layer ABOVE them — the ONE definition of the as-of (transaction-time belief-state) SELECTION rule, PIN-STABLE (the answer at a pin never depends on later writes): newest version by VERSION order with `0 < TxFrom <= pin`; a live current row answers unless a history row above its version was recorded at or after it and by the pin (the rows a bounded `SetVersionInterval` appends while the current row keeps the slot); among the rows of one write the highest whose own valid interval is open answers; ABSENT when none exists, when that decisive newest belief was retracted (`TxTo != 0 && TxTo <= pin`) or hard-deleted (`DeletedAt != 0 && DeletedAt <= pin`), or when its life ended in a tombstone below it — never fall through to an older still-open row (lesson 62). The core resolver (`resolveNodeChainAsOf` / `resolveRelChainAsOf`) and the memory backend (`nodeAsOfLocked` / `relAsOfLocked`) DELEGATE to it; the badger native reverse-scan (`NodeAsOf` / `RelAsOf`) is an early-stopping optimization proven equivalent to it by randomized-chain tests (`badgerstore_asof_equivalence_test.go`), so no backend re-implements the rule (killing the cross-backend-divergence bug class: the as-of version-order divergence and the commit-window drop). `SelectAsOf` is pure selection — normalization to the then-visible state stays with the caller.
- **Decode untrusted msgpack only through `storeutil.SafeUnmarshal`**: Raw `msgpack.Unmarshal` of persisted/imported bytes is unsafe at the trust boundary — the vmihailenco decoder PANICS via reflect on a duplicate map key bound to an interface field (`PropertyWire.Value any` → `SetString/SetInt using unaddressable value`) and FATALLY stack-overflows (unrecoverable) on deep nesting. `SafeUnmarshal` runs `guardMsgpackDepth` (non-recursive, rejects nesting > `maxWireDecodeDepth`=64 before the decoder runs) then recovers any panic, returning `store.ErrCorruptWire`. Every `NodeWire`/`RelWire`/meta/custom-property decode in badger + core import routes through it (`msgpack.Marshal` is panic-free, no wrapper). When adding a new decode-from-bytes site (or a sharded backend), use `SafeUnmarshal`, never raw `msgpack.Unmarshal`. Audit recipe: `grep -rn 'msgpack.Unmarshal(' pkg/ | grep -v _test`. ONE sanctioned non-SafeUnmarshal read path exists: `scanWireTemporalMeta` (`wire_temporal_scan.go`) — a single-pass, non-recursive, panic-free-by-construction token walk (the `guardMsgpackDepth` machinery extended to capture the ten temporal-selection fields) behind `DecodeWireTemporalMeta`. It is FAIL-OPEN: any structural surprise declines to the SafeUnmarshal partial decode, which stays the authority for both decoding and malformed-input error classification; equivalence is locked by a randomized+adversarial battery (`wire_temporal_scan_test.go`). Do not add further raw-byte readers without the same decline-to-SafeUnmarshal shape. See lesson 47.
- **Change-log (op-log) is IN-BACKEND and co-committed, never a Store decorator**: when `ChangeLog` is enabled, each backend appends a framed record under `0x09/<LSN>` in the SAME `WriteBatch` as the data + counters + a `last_lsn` watermark — crash-consistent, atomic with the mutation. A decorator over `Store` cannot reach the inner `WriteBatch` (committed-but-unlogged window) and would be treated as an untrusted store (loses frozen-pointer scans), so the log lives inside badger/memory as the optional `store.ChangeFeedCapability`, type-asserted by core (`changeFeedCapability` guards against a wrapper promoting a shard's per-shard feed). The LSN is assigned + the record buffered under ONE `wbMu` window with the entity ops (doors lacking `idxMu` use `appendOpsLogged`); every early-return-after-snapshot in `flush()` requeues logs; the empty guard is `len(ops)==0 && len(logs)==0`. Both backends share the `storeutil` body builders so feeds are byte-identical (cross-backend parity test). The log ALONE does not converge a replica — bootstrap from an export snapshot (registry included), then tail. When adding a mutation door, emit exactly ONE logical record from the PUBLIC door (shared helpers like `deleteRelByInfo`/`cascadeDeleteInner` stay record-free). The `ChangeNodePut`/`ChangeRelPut` body is `storeutil.NodePutBody`/`RelPutBody` (wire + `WithHistory` bit); the put-record wire is built UNTOKENIZED via `NodeToWireChecked` (property keys as strings) in BOTH backends — so feeds are byte-identical even for property-bearing entities, and a put record carries no property-key registry dependency. A new put-door must set `WithHistory` to true IFF it wrote a version-history row (`ReplaceNodeWithHistory` / `*LabelTokenWithHistory`), false for create / in-place `ReplaceNode` / no-history label doors; badger and memory MUST agree (the replica reproduces history depth from this bit). See lessons 49–50 and the change-log section above.
- **Tiered change-log is a store-global feed over per-shard co-committed logs (ADR-0005 §2)**: `tiered.Store` now satisfies `ChangeFeedCapability`/`ChangeLogStatusCapability`/`TxChangeLogScope` (core's `changeFeedCapability` switch admits `*tiered.Store`), enabled by `tiered.Config.ChangeLog`. There is no single `WriteBatch` on tiered, so a store-global `atomic.Uint64` allocator (`changeLogAllocator`) is INJECTED into every shard via `badger.Config.ChangeLogSeqSource` (nil = badger self-owned counter, standalone byte-for-byte identical); each shard still co-commits its own records + `LastLSNKey` in its own batch, but LSNs form ONE total order across shards. Reseed at open reads ONLY the monotonic `changelog_lsn_watermark` MetaKV key on the always-present refShard (persisted after every log-bearing flush via `badger.Config.OnChangeLogFlush`, plus a belt-and-braces fold of the open ref/hot `LastLSNKey`s) — NEVER a full cold-shard scan; an unreadable watermark FAILS CLOSED (poisons the allocator so it refuses LSNs, sticky error on every feed door, `RecoverChangeLog` clears in place). The feed (`ForEachChange`/`ChangeFeed`) is barrier-first + W-bounded: `Flush()` folds `shard.Flush()` over every open shard (also satisfies the core `storeFlusher`), then captures `W = LastCommittedLSN` and runs a paged k-way min-heap merge over every catalog shard emitting ONLY `LSN <= W` (one shard checked out at a time; cold-shard log segments paged read-only, no flush on a closed shard; heads > W deferred to the next poll). The barrier + W-bound TOGETHER close the cross-shard flush-reordering silent-loss (ADR Finding-1); the barrier alone is insufficient under a concurrent writer. `TxChangeLogScope` is store-level (`SetLogDivert` is the ONE divert seam, marked for the scope-tagged-routing redesign — measurements 2026-07-11) buffering per shard, LSNs minted at commit so a rolled-back tx burns none. A cascade whose whole neighborhood is LOCAL to the node's shard delegates to `shard.DeleteNodeCascade` (ONE cascade record, byte-identical to single-shard backends); a cross-shard neighborhood emits one `ChangeRelDelete` per edge (ADR §2.4). Reseed/reorder/rotation/parity/replica-convergence tests live under `pkg/graph/store/tiered/tieredstore_changelog*_test.go` and the three-way `changefeed_parity_test.go`.
- **Replica apply reproduces the primary's rows VERBATIM, never re-stamps**: `g.Replication().ApplyChange` (in `apply_record.go`, under `c.mu.Lock`, bypassing `checkWritable`) decodes each record, runs the SAME import-trust pipeline per record (`WireTo*Checked` reconstructs version/`TxFrom`/temporal/integrity verbatim → `validate*TokensInRegistry` → `validatePropertySliceLimits` → `verifyImported*Hash` recompute-AND-COMPARE), then writes through a FOREIGN-ID store door (`PutNode`/`ReplaceNode`/`ReplaceNodeWithHistory`/`Delete*`/`PutNodeVersion`/`Truncate*`/`Trim*`/label-token doors) that persists the supplied entity byte-for-byte. NEVER route apply through `NodeOps.Add/Update` (they re-mint IDs, re-stamp `TxFrom`, recompute hashes). Create-vs-update is inferred from local existence; a `NodePut` whose label set differs from the local row by one token is a label mutation → routed to the matching label-token door. Apply is IDEMPOTENT (identical row = no-op via `nodeWireMatches`/`relWireMatches`; delete tolerates `ErrNodeNotFound`/`ErrRelNotFound`) because the watermark advances via a SEPARATE `MetaSet` after the door commits — the crash window is closed by re-apply, not co-commit. Per-record delete `PrevVersion`s are derived from the replica's OWN local current rows (correct because LSN total-ordering guarantees the replica's local current == the primary's pre-mutation state). The read-only gate is `c.checkWritable()` on user doors; apply and `IOOps.Import` use `checkOpen` only. See lesson 50.
- **Token-registry refetch is append-only and prefix-guarded; property keys are never synced**: a label/rel-type the primary allocates AFTER a replica's bootstrap is resolved by `g.Replication().RegistrySnapshot()` (capture the LSN BEFORE the names so `CapturedAtLSN ≤ names coverage`) + the apply-path hook (`validate*TokensWithRefetch` in `apply_record.go`), which guards `CapturedAtLSN ≥ rec.LSN`, then `AppendNames(prefix, suffix)` (the registries' append-only grow — `ImportNames` is load-only; prefix must DeepEqual current, else `(false,nil)`), persists, and re-validates. The refetch runs UNDER `c.mu.Lock` (held by apply) and takes ONLY `c.registryMu` (order `c.mu → registryMu`; the source is a DIFFERENT/primary Core taking its own locks — never re-take `c.mu`). On a persist failure it `RollbackNames` the in-memory grow so the registry never runs ahead of disk (else a crash leaves an applied entity referencing an unpersisted token). Property keys are NOT refetched — records carry UNTOKENIZED string keys, so the replica tokenizes them locally in its own independent registry; appending the primary's propkeys would misalign it. See lesson 51.
- **Bootstrap LSN is captured under the export lock; the failover lease is a durable HINT not CAS**: the export header (v2; importers accept v1 OR v2) carries `SnapshotLSN`, read via `c.changeFeed.LastCommittedLSN()` UNDER the same `c.mu.Lock` as the entity snapshot (gapless), and import records it as the replica's initial applied watermark (flush-before-watermark). The ID-slot lease (`MetaKey("id_slot_lease")`, `SafeUnmarshal` on read, slot 0-15 validated) is last-writer-wins (`DetectConflicts=false`) — rho-tkg persists/reads it; the EXTERNAL orchestrator serializes writes, picks slots, and triggers promotion = `Close()`+`New()` with the leased `SnowflakeNodeID` (generators are built only in `New`). A promoted node and the node it replaces MUST hold different slots so minted IDs never collide. See lesson 51.
- **Relationship creation goes through one kernel**: all create doors (Add / AddByID / AddByIDIfAbsent / batch Execute) share `relationship_create_kernel.go` (`prepareRelCreate` → `relEndpointHashLadder` → `createRelWithTypeRollback`). Add creation invariants THERE, not in a door.
- **Badger WriteBatch.Flush() blocks forever on closed DB**: Badger v4 uses `context.Background()` in `oracle.readTs()` → `WaitForMark()`, so closing the DB stops oracle goroutines and any in-flight `WriteBatch.Flush()` blocks forever. Fix: `BadgerStore.dbClosed atomic.Bool` is set to `true` in `Close()` BEFORE `db.Close()`. `flush()` checks it and returns `ErrDBClosed` immediately. Any test that calls `bs.db.Close()` directly MUST set `bs.dbClosed.Store(true)` first. Never call `WriteBatch.Flush()` after `db.Close()`.

### Version history

- **Pre-mutation snapshots**: `Update*` captures deep copy, applies mutations, writes both via `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` atomically.
- **A move publishes history first; a chain read reads the current row first** (backlog 32): the per-entity resolvers (`nodeAtLockedTx`, `nodeAsOfLocked`, `find*VersionMatchingDuringTx`, `find*VersionRelating`, rel mirrors) and badger's own `NodeAsOf`/`RelAsOf` read the current row and then the history in two reads, with no snapshot and no entity lock (badger `GetNode` takes no `idxMu` on a cache hit), so every door that moves a current row into history must make the moved row readable in history no later than the current slot changes: memory holds one lock over both; badger appends the batch to the pending buffer before it changes the entity cache (`publishMoveLocked`, also for the node delete's tombstones ahead of the cascade's removals); the core cascade writes the demoted row (`PutNodeVersion`) before `ReplaceNode`. A reader then sees the moved row at least once (twice is tolerated by the resolver). Tests: `TestWithHistoryDoorsPublishHistoryBeforeCurrent` (badger, `moveTestHook` between the writer's two halves and after every current-slot change), `TestPointDoorRace_MoveBetweenChainReads` and `TestNativeAsOfReadsCurrentBeforeHistory` (a whole move between the reader's two reads: `chainReadHook`, `asOfAfterCurrentTestHook`), `TestPointDoorRace_UnderMovingWriters` (every door, every backend), `TestCascadeDeleteFatalPreflightAppliesNothing` (the cascade delete reads every fallible input before its first mutation).
- **Append-only**: Delete paths save tombstone versions (with `DeletedAt`/`ValidTo`; a retraction's tombstone also carries `Retracted`) before deletion. History is never erased on delete or retraction.
- **Metadata-blind rehashes preserve integrity metadata**: when an API recomputes `Hash`/`PrevHash` but has no provenance shadow-key channel (`CloseVersion`, label add/remove, CAS, in-place update), preserve provenance, signature, authorization, and relationship endpoint hashes already present on the integrity struct.
- **CloseVersion must actually close**: `ValidTo == 0` is the open-ended/current sentinel. `CloseVersion(id, 0)` must return `ErrInvalidTimeRange` before locking or mutating; otherwise the call records an update that is still open-ended and can be repeated forever.
- **Version increments are checked**: use `nextEntityVersion` for versioned mutations. `math.MaxUint32` returns `ErrVersionOverflow` before history writes, wrapped version `0`, or label-token allocation for rejected add-label calls.
- **One version allocator, dense versions**: every appending door (cascade, Update, CloseVersion, label change, CAS) takes one above the chain's highest version (`nextNodeVersion` / `nextRelVersion`, `version_alloc.go`); the cascade numbers its slot row last. The fast paths (allocator, as-of current arm on memory, badger and the core fallback, tx rollback snapshot) decide "a row above the current version exists" by version current+1 alone, so they assume no gap above the current row. A re-import of a deleted ID (`Import`, `AddByIDIfAbsent`, GraphTx twins) continues the chain (backlog 38, `lifeStart`): one above the highest stored version, `PrevHash` = that row's hash, recorded after every stamp of the chain (plain door: `validInstantAfter(now, maxStamp)` on its own row, never a commit-clock floor raise — valid time must not pull the floor; a `tkg_tx_from` at or below one → `ErrTxOrder`; rows purged after a compaction: continue above the stub), so (life, version) order is version order; GraphTx rollback of a created ID trims from its created version, never the earlier life. A gap (a direct `Store.TruncateNodeHistory` / `TrimNodeHistoryFrom`, or a re-import over a compacted earlier life stored before backlog 38) makes them answer "none above": the allocator then reuses current+1 (unique, but below the higher row), and the as-of door answers the current row while `SelectAsOfWithCurrent` over the full chain would answer the higher row.
- **Version-chain navigation is boundary-checked**: `VersionAfter(..., math.MaxUint32)` has no successor and must return `nil, nil` for a known entity; never compute a successor with raw `version + 1` before checking the max boundary. `VersionBefore`/`VersionAfter` must still validate explicit IDs and return `ErrNodeNotFound`/`ErrRelNotFound` for unknown entities.
- **Replace vs Put**: Put rejects duplicates. Replace requires existence. Put/replace/batch/split-helper writes/deletes reject zero or negative entity IDs before mutation. History replacements reject nil current/history payloads before dereference or marshaling. Replace overwrites data only — no index changes (labels/type/endpoints are immutable; node Store replacements enforce labels, label-token helpers enforce exact one-token deltas, and relationship Store replacements enforce type/endpoints).
- **Genesis detection**: Use `entry.Version() == 0`, not array position. Array position changes after truncation.
- **Anchor+delta history storage (ADR-0009 / B6, opt-in `badger.Config.HistoryDeltaEncoding`)**: version-history rows are stored as a FULL anchor every `badger.Config.HistoryAnchorInterval` versions (`storeutil.DefaultHistoryAnchorInterval` = 16; configurable in [2,4096] and pinned by an open-time marker — a mismatched reopen fails closed with `ErrHistoryAnchorIntervalMismatch`) and DELTAS in between (`storeutil.DiffNodeHistory`/`ApplyNodeHistory` + rel mirror) — the delta carries only properties that changed vs the interval anchor, plus every non-property field (both hashes, full temporal block) verbatim. Framing is self-describing: an anchor is the raw full marshal, a delta is 1-byte `'D'`-tagged (0x44 — never a msgpack map header), so reads (`storeutil.HistoryValueKindOf`) accept both forms and a legacy pre-B6 row is transparently an anchor (zero migration, no `fv` bump). Reconstruction (`badgerstore_history_delta.go`): `GetNodeVersion` ≤2 point reads, whole-chain scans use an in-scan anchor cache, `NodeAsOf` classifies on the delta Meta (temporal verbatim — no anchor read) and reconstructs only the winner. WRITE routes all history-write doors through `historyNodeValue`/`historyRelValue` (falls back to full when the anchor is unavailable or a delta would not be smaller). Truncate AND compaction (`truncateHistoryByPrefix` / `compactHistoryByPrefix`) re-anchor ONLY orphaned kept deltas (`rematerializeOrphanedDeltas` — a kept delta whose interval anchor is being trimmed is rewritten to full in the same batch, exact retention count preserved); trim-from is inherently safe (kept deltas' anchors are lower and survive). Replication byte-exactness is preserved because the delta NEVER enters the change-log feed (a put-with-history logs the current row + history bit; the replica reproduces history via its own write path) and `DiffNodeHistory` is deterministic — proven by a delta-primary→full-replica convergence test. `MemoryStore` stays full-snapshot (the differential oracle). Downgrade safety: enabling delta does NOT bump `wire_format_version` (entity rows stay v2), so a downgraded delta-unaware binary opens the store but FAILS CLOSED (`ErrCorruptWire`, never a silent misread) if it reads a `'D'`-tagged history row — the tag byte 0x44 is a msgpack fixint, not a map header, so the struct decode rejects it (locked by a test). A clean open-time refusal for old readers is coupled to a `wire_format_version` bump, deferred to the default-ON flip (avoids global golden-vector churn for an opt-in feature). Default OFF while it soaks.
- **History compaction (ADR-0001)**: `g.Admin().CompactHistoryNodes/Rels(ctx, RetentionPolicy{KeepVersions, KeepSince})` trims OLDEST history rows (current row + newest history version never trimmable) and writes a per-entity detached-anchor STUB `{TrimmedThroughVersion, LastTrimmedHash, LastTrimmedTxTo, CompactedAtTx, StubHash(self)}` in MetaKV — per-entity trim + stub in ONE WriteBatch via `store.HistoryCompactionCapability` (memory + badger commit them atomically; tiered routes the trim to the owning shard and the stub to the reference shard — see `tieredstore_compaction.go`), with the graph watermark routed ONCE via the store-level MetaSet (`advanceCompactionWatermark`). NEVER mutates a stored row (append-only). `Verify*Chain` is stub-aware: oldest kept version's `PrevHash` must equal `LastTrimmedHash` (virtual predecessor). A temporal read pinned before compacted knowledge returns `ErrHistoryCompacted` (point doors: per-entity stub boundary; scan doors: graph `CompactedThroughTx` watermark, fail whole scan) — never silently incomplete. REFUSES while a change-log is enabled (`ErrCompactionChangeLogEnabled` — replication interplay lands later), on a protected as-of tag (`ErrCompactionProtectedTag`), and on an empty policy (`ErrInvalidRetentionPolicy`). Sentinels canonical in `internal/core`, re-exported from `pkg/graph`. The ADR file was never committed; the design record is CHANGELOG (compaction entries).

### Ingest pipeline (ADR-0006)

- **Two write doors, one core**: `g.Ingest()` is the prepare-parallel / apply-sequential write door for insert-dominated throughput, BESIDE the interactive `g.Tx()` door. Producer sessions (`g.Ingest().NewSession(IngestOptions)`) do ALL validation / property-slice construction / content-hash precompute / snowflake-ID mint ON THE CALLER THREAD — the batch queue methods (`batch_queue.go`) are reused verbatim as the prepare step, so each session's `AddNode`/`AddRelationship`/… builds a `pendingNode`/`pendingRel` (already an ADR-6 prepared intent) on its own thread, fully parallel across sessions.
- **Single applier, group commit, REUSE the batch apply path**: one applier goroutine per Core (`ingestApplier`, lazily started on the first session via `ensureIngestApplier`, drained+stopped at the START of `Close` BEFORE `c.closed` is set so no accepted intent is dropped). It drains submitted `ingestGroup`s, COALESCES up to `defaultIngestGroupSize` intents into one `BatchBuilder`, and calls `Execute()` — do NOT build a second write path.
- **Enqueue↔`Close` linearization (C1)**: `enqueue` and `stop()` share ONE fence — `enqueueMu` (the seq-order serializer). `enqueue` reads `stopping` and performs an UNCONDITIONAL channel send (NO `select` whose random choice could send-after-close) while holding `enqueueMu`; `stop()` acquires `enqueueMu` EXCLUSIVELY to set `stopping` BEFORE closing `quit`. So any send that won held the lock strictly before the fence and is already in the channel when `drainRemaining` runs (applied, never accepted-then-dropped), and a post-fence enqueue rejects cleanly with `ErrIngestClosed` WITHOUT minting a seq. Holding the lock across the (backpressure-blocking) send is safe because the applier keeps draining until `stop()` has fenced, so a blocked producer is always woken. The applier-startup path is guarded by `ingestClosing` (set under `ingestMu` by `stopIngestApplier`, checked by `ensureIngestApplier`) so a session racing `Close` cannot orphan a fresh applier behind the shutdown sweep. `Session.Submit` clears its builder's slices IN PLACE (never `NewBatchBuilder`, whose close-check would surface `ErrGraphClosed` and drop the captured group before `enqueue` — the single applied-or-`ErrIngestClosed` decision point) — `ErrIngestClosed` re-exported as `graph.ErrIngestClosed`.
- **`WaitApplied` is the async failure truth channel (C2)**: `AppliedSeq() ≥ token.Seq` means PROCESSED, NOT necessarily committed — the watermark advances even for a REJECTED group so it never wedges later waiters. `WaitApplied(token)` returns nil on success or the intent's real apply error (e.g. `ErrUniqueViolation`, a flaky-flush failure) if it was rejected. Per-intent granularity: `applyCommitGroup` maps each coalesced-batch `BatchError` (by entity ID) back to its owning group's seq (`groupApplyError`), so a survivor gets nil while its sibling gets the error; a group-level / whole-batch failure fails every group. Failures are keyed by token seq under `a.mu`, pruned on read (a second `WaitApplied` returns nil), capped at `ingestFailureCap` (oldest evicted, `failureDrops` counted) so a flood of never-read async failures stays bounded. A sync `Submit` returns its OWN group's error via `g.result`, not the batch-level `ErrBatchFailed`. `Batch.Execute` already IS the Lanes:1 strong-mode applier: `c.txMu` then `c.mu.Lock` per group (group-atomic against readers; serializes against the interactive door at GROUP granularity, not per insert), `TxFrom` stamped once from `c.now()`, `CommitLogScope` mints contiguous LSNs co-committed with the data, one buffered `PublishBatch`, one `flush()` per group (via `putGeneratedNodesBatch`). A whole commit group shares one `TxFrom` (distinct fresh-ID entities, each its own genesis chain — per-entity monotonicity is unaffected, lessons 20/62); `TxFrom` is strictly increasing ACROSS groups.
- **Submit modes = the §4.6 freshness contract**: `IngestOptions.Sync=true` → `Submit` blocks until the group's flush returns (ack ⇒ applied AND overlay-visible ⇒ read-your-writes on ANY goroutine); `Sync=false` → `Submit` returns a `SubmitToken{Lane, Seq}` immediately, and a reader waits via `WaitApplied(token)` / compares `AppliedSeq()`. Seq is assigned under `enqueueMu` held across the channel send so seq order == the single applier's apply order == LSN order (so `appliedSeq ≥ token.Seq` ⇒ visible). `QueueBound` bounds the prepare→apply channel; a full queue BLOCKS the producer (synchronous stall — entity writes never dropped, §4.8). `DeclareLabels`/`DeclareRelTypes` pre-allocate+persist vocabulary (`predeclareVocabulary`) so prepare is lookup-only; undeclared names fall back to probe tokens re-stamped at apply (safe: the content hash keys on label STRINGS, so re-tokenization does not invalidate it — §4.4).
- **Two throughput doors, one applier path**: at Lanes:1 the strong-mode pipeline (single applier, `Batch.Execute`) is APPLIER-BOUND — p8 gains ~nothing over p1 on fast storage. Its niche is durable `SyncWrites` ingest (a single applier amortizes one group-flush fsync across the group). For raw throughput use **concurrent mode** (`IngestOptions.Concurrent`, below): N sessions self-apply in parallel, no applier/handoff. (A former serializable `IntentRecord` wire format — a day-one substrate for a distributed Lanes:N applier that never shipped, verified inert in-process — was removed 2026-07-17; the design lives in ADR-0006 + git history and can be reconstructed if distributed ingest is built.)
- **§4.5 prepare-side pre-encode (WIRED, ingest path only)**: gated by `BatchBuilder.preEncode` (set by `Ingest.NewSession` on native badger). Producers pre-encode node-create rows on their own thread (`pendingNode.wireBody = storeutil.PreEncodeNodeWireV2WithKeys`, zero temporal tail, property keys tokenized via the shared registry); the applier `PatchWireTemporalTail`s each and hands it to `store.PreEncodedPutCapability`, skipping the second msgpack pass. **INVARIANT: the buffer is used ONLY when provably byte-identical to the store's own encode.** For a genesis create the sole possible divergence is label tokens (a probe token re-stamped, §4.4), so when finalized tokens differ from queued the buffer is stale → the applier leaves `wireBodies[i]` nil and the store re-encodes that row. A wrong buffer is the silent-wrong-answer class, so re-encode when in doubt. The change-log put body is likewise pre-encoded for creates (`pendingNode.logBody` → `PutNodesBatchPreEncodedLog`) under the SAME token-equality gate. Plain `g.Batch()` never sets `preEncode`.
- **Concurrent mode (`IngestOptions.Concurrent`, §14 — the Lanes:N write door)**: the session SELF-APPLIES each submitted group on the caller thread under the STANDALONE concurrency discipline — `c.mu.RLock` + entity locks + value stripes, via `applyIngestGroupConcurrent` (`ingest_concurrent.go`) — so N sessions apply genuinely in parallel with no applier/handoff. NO second write path: creates go through `putGeneratedNodesBatch(PreEncoded)` (one batched store door per group when no unique constraint exists; per-node under value stripes otherwise — sequential per-node stripe acquisition is the deadlock-free discipline), rels through the shared create kernel under ordered `LockTwo`, updates/deletes/cascades through the same `*Internal` doors the strong-mode batch calls. DECLARE-ON-PREPARE: an unseen label/rel-type is registered+persisted during `AddNode`/`AddRelationship` (steady state is lookup-only), so queued tokens are always REAL — there is no probe-restamp step, and the §4.5 pre-encoded buffer stays valid (patched with the group's TxFrom at apply; `concurrentTokensResolvable` is the defense-in-depth guard, rejecting any probe token that slips through instead of persisting it). Change-log records emit EAGERLY per store door (gapless, crash-consistent, byte-exact replica convergence from an N-session primary — `TestIngestConcurrentReplicaByteExact`); the per-tx scope is NEVER touched under the read lock. Events are collected under the lock and dispatched AFTER RUnlock (dispatching under a held RLock could deadlock a re-entrant handler against a waiting writer). Semantics (deliberate, §14): `Submit` is always synchronous and returns the group's apply outcome (`Sync` implied; token carries a nonzero per-session `Lane`; `WaitApplied` on it returns nil); a group is NOT atomic against concurrent readers (per-entity only); cross-session TxFrom is ±ε ordered (per-entity monotonicity via entity locks); `QueueBound` ignored. This is the throughput door; durable `SyncWrites` ingest should PREFER strong mode (single-applier group-flush fsync amortization).
- **Invariant ownership (§9)**: apply OWNS TxFrom (one shared monotonic clock), LSN (co-commit), PrevHash (chain-ordered splice), event publication, unique arbitration; prepare's answers are advisory. Because the applier is `Batch.Execute`, every invariant the batch path already preserves (LSN gaplessness, byte-exact replica reproduction, unique pre-check under the exclusive lock, buffered-event ordering, frozen-row publication, bitemporal resolver reuse) is inherited unchanged. See api.md "Ingest pipeline", `pkg/graph/internal/core/ingest.go`.

### Temporal query rules

- **Three timestamps, three claims (VT vs TX)**: Do not conflate them.

  | Timestamp | Claim | Who can assert it | State on an unstamped entity |
  |---|---|---|---|
  | `tkg_tx_from` | "the DB recorded this fact at T" | the system — automatically | always stamped: every Add allocates `TemporalMetadata` and sets `TxFrom` |
  | `tkg_created_at` | "the entity record came into existence at T" | system-derived (snowflake ID timestamp); caller may override at Add | always derivable — the shadow resolver applies the snowflake fallback |
  | `tkg_valid_from` | "the fact holds **in the world** from T" | only the domain — a recorder/curator with actual knowledge | `0` = no world-time claim made |

  Writers must NEVER default `tkg_valid_from := now()` without domain knowledge — that silently conflates TX with VT: recording latency leaks into world-time semantics and backfilled facts become indistinguishable from real assertions. If a consumer wants "unstamped ⇒ valid since recorded", implement it as an explicit, flagged heuristic on the consumer side, not by stamping at write time and not by changing the shadow resolver.
- **Two doors, two views (asserted vs effective)**: The shadow resolver (`g.Resolve().NodeProperty(n, "tkg_valid_from")`, `shadow.go`) returns the RAW asserted value — `(Instant(0), ok=true)` when never asserted. `ok` is true because `TemporalMetadata` always exists (TxFrom stamping); consumers must check the zero value, not `ok`. Temporal queries instead use the EFFECTIVE valid-from (`nodeValidFrom`/`relValidFrom`: explicit `ValidFrom`, else snowflake fallback) — so an entity with unset `ValidFrom` is "eternal" through the shadow door but time-bounded through the query door. This asymmetry is deliberate: shadow props report *stored/asserted* state, temporal queries report *effective* state. `tkg_created_at` is the only temporal shadow key with a fallback in the resolver.
- **Effective valid-from / point / overlap predicates**: owner "Temporal Queries" above (effectiveValidFrom: explicit `ValidFrom`, else TxFrom / UpdatedAt / mint instant; open end per valid axis, `ValidTo == 0` only on the legacy axis). Every entity is queryable temporally without `SetTemporal()`.
- **Explicit ranges are half-open and non-empty**: when caller payloads provide both `tkg_valid_from` and `tkg_valid_to`, require `valid_from < valid_to` before ID generation or batch queueing. Equal or reversed explicit ranges return `ErrInvalidTimeRange` at graph create/import/batch boundaries and `ErrInvalidStoreMutation` at direct Store write boundaries.
- **Bitemporal (since v4.3.0)**: `QueryOpts.TxAt` filters the chain to versions RECORDED by the given TX time (`TxFrom <= TxAt` — recorded-by-then). TxTo deliberately does NOT bound visibility: superseded is not retracted, and a superseded version remains the authority for its valid-time slot at every later txAt (lesson 43; the old `< TxTo` clause made `NodeAtTx(oldVT, now)` return nothing after any update). `TxAt == 0` keeps "no TX filter" backward-compat. Bitemporal point queries: `g.Temporal().NodeAtTx(id, validAt, txAt)` and counterparts (batched: `NodesAtTxByIDs` / `RelsAtTxByIDs`, per ID the same gates and body, `batched_attx.go`, backlog 57); bitemporal valid-time INTERVAL queries: `g.Temporal().NodesDuringTx(from, to, txAt)` / `RelsDuringTx(from, to, txAt)` (overlap `[from,to)` as known at `txAt`; `txAt == 0` == `NodesDuring`; same `probeInterval` seam + generic `QueryOpts{ValidStart,ValidEnd,TxAt}` door). The resolver computes `vEnd` from `next.ValidFrom` (falls back to `next.UpdatedAt`), so adjacent versions auto-tile once the caller supplies explicit `tkg_valid_from` on Update. See lessons 32–34. `QueryOpts.TxPin` is the pure knowledge-time BELIEF-STATE pin (NO valid-time filter): the generic doors (`ByLabel`/`ByType`/`All`) route each candidate through the SAME as-of resolver as `NodesAsOf`/`RelsAsOf` (`findNodeVersionForOpts`→`nodeAsOfLocked`), so `ByLabel{TxPin:T}` equals `NodesAsOf(T)` filtered by label — closing the `TxAt`-alone footgun (that door still valid-filters at now and drops past-valid facts). `TxPin` is mutually exclusive with `ValidAt`/`ValidStart`/`ValidEnd`/`TxAt` (`ErrConflictingTemporalOpts`).
- **Single resolution seam**: `resolveNodeChain(chain, probe, pred)` / `resolveRelChain(...)` in `chain_resolver.go` are the ONE ENTRY POINT all core-layer temporal reads select a version from a `(history ‖ current)` chain through — every named door (`NodeAt`/`NodeAtTx`/`NodesDuring`/`NodesAsOf`/`NodesRelating` + rel mirrors) and every generic `QueryOpts` door (`findNodeVersionForOpts`, `nodeAtLockedTx`, `find*VersionMatchingDuringTx`, `nodeAsOfLocked`) funnels here via a `chainProbe{kind: probePoint|probeInterval|probeAsOf|probeRelate, ...}`, so TX visibility (lesson 43), pre-delete tombstone normalization (lesson 60), `[vStart,vEnd)` derivation (lessons 32/33/42), newest-belief selection (lessons 46/62), predicate-anywhere interval matching (rule 16), and the as-of retraction rule (lesson 62) cannot drift between the two doors (rule 17). This is a CALL-GRAPH guarantee, not a file-boundary: `resolveNodeChain`'s `probePoint` arm delegates straight to `resolveNodeVersionAtCapped` (and `resolveRelChain` to `resolveRelVersionAtCapped`), both defined in `temporal.go` alongside their `nodeVersionBounds`/`relVersionBounds` and belief-newer-than helpers, with the cascade-specific pieces in `temporal_cascade.go` (`correctionChain`: the resolver prepared once per correction, row 85) — so a lesson-17/58 drift audit on this seam must inspect `temporal.go`/`temporal_cascade.go` too, not just `chain_resolver.go`. The canonical entity-level predicates in `internal/storeutil` (store push-down) are separate and unchanged.
- **Record doors vs state doors**: RECORD doors (`NodeAsOf` / `RelAsOf`, `NodesAsOf` / `RelsAsOf`, `QueryOpts.TxPin` and its counts) answer the newest ROW recorded by the pin (`SelectAsOfWithCurrent`, pin-stable). STATE doors answer "the state at valid time `t` as believed at pin `p`": `NodeAtTx` / `RelAtTx`, `ByLabel` / `ByType` with `QueryOpts{ValidAt, TxAt}`, and the effective timelines. A consumer asking for a valid instant uses a state door, never `NodeAsOf`. The effective-timeline doors (`g.Temporal().NodeEffectiveTimeline` / `RelEffectiveTimeline(id, pin)`, the many-pin forms `NodeEffectiveTimelines` / `RelEffectiveTimelines(id, pins)` (one chain walk for all pins, `effective_timelines.go`), scan forms `ForEachNodeEffectiveByLabel` / `ForEachRelEffectiveByType` and the broader scans `ForEachNodeEffective` / `ForEachRelEffective` / `ForEachRelEffectiveAtNodes` / `NodesEffectiveByIDs` / `RelsEffectiveByIDs` / `ForEachNodeEffectiveByLabels`, `effective_timeline.go`, `effective_scans.go`; every scan streams through `streamNodeTimelines` / `streamRelTimelines`, the per-entity body) are the state door's interval form: ascending, non-overlapping half-open segments whose row at every `t` equals `NodeAtTx(id, t, pin)`; they include entities deleted before the pin, refuse a pin above the commit clock with `ErrTxPinTooNew`, and share the point resolver's input preparation and bounds functions (never a second rule).
- **Read-time supersession and life order** (`chain_supersession.go`, `chainWriteOrder` in `chain_resolver.go`): a replacing row `s` (recorded at the `TxTo` of a row it replaced) ends, at its start, every older belief (`TxFrom`, then version) that started at or before that start — also on chains a `SetVersionInterval` made non-monotonic; nothing changes at pins before `s` was recorded. The resolver orders a chain by LIFE (deletes recorded before the row), then version, so a re-imported ID never interleaves its lives. Both are read-seam rules that apply to stored chains as they are.
- **Caller-instant endings** (`DeleteWithTx` / `UpdateWithTx` on `Nodes()` / `Rels()` and the GraphTx, batch and ingest twins; gate `AllowTxBackfill`): the instant `t` must lie after every `TxFrom` / `TxTo` of the chain (history and current — `TxFrom` is not co-monotonic with version) and after the current version's start, on the node and every cascaded relationship, checked under the entity lock; otherwise `ErrTxOrder` (wraps `ErrInvalidTxFrom`), also for a no-op update, the same `t` twice, or a delete over a recorded close at or after `t`. Batch and ingest units pre-flight every caller-instant op before any write and refuse as a whole; such an op must be the only op of its unit on its entity. The instant never advances the commit clock. A valid-time correction (`SetNodeVersionInterval` and twins) on a hard-deleted entity is refused with `ErrEntityDeleted` (wraps the not-found sentinel). `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx` (and their GraphTx, batch and ingest twins) stamp every appended row at the caller instant `t`; `t` must lie after every `TxFrom` / `TxTo` of the whole chain (`checkTxOrder` with no version start, `interval_withtx.go`), else `ErrTxOrder`.
- **Retraction ends belief, Delete ends validity** (backlog 43; `Retract` / `RetractWithTx` on `Nodes()` / `Rels()` and the `GraphTx`, batch and ingest twins): a retraction is the delete seam (`deleteNodeInternal` / `deleteRelationshipInternal` with a `tombstoneSpec`) writing Delete's tombstone marked `TemporalMetadata.Retracted` (wire `rx`, row `fv=3`; refused by checked decode and Store writes without `DeletedAt`). The read side is ONE rule at the resolver seam: `dropRetractedLives` (`chain_resolver.go`) removes, from the TxAt-filtered and normalized chain, every row of a life a retraction known at the pin ended (the life rule of `chainLifeEnds`), before any bound, supersession or life cap is derived — the effective-timeline sweep applies the same function. Dropping, not capping: a retracted life must not keep superseding or bounding an earlier life. The record doors need no change (the tombstone is the newest row recorded by the pin: absent, as after a delete). The pinned-read normalizers clear the marker with a post-pin delete, so a pre-T read is byte-identical to before. A retraction of an ID without a current row: `ErrNodeNotFound` / `ErrRelNotFound` for an unknown ID, `ErrEntityDeleted` + not-found for a deleted or retracted one (classified in the seam, the GraphTx snapshot and the batch pre-flight alike). Chains without a marker answer exactly as before (`TestRetraction_OldChainsAnswerAsBefore`, golden from the pre-retraction code); the meaning of the marker is pinned by `TestRetractionOracle_NeverRecordedWorld` (the graph must answer like one where the retracted lives were never recorded).
- **Excise voids a span** (backlog 64; `ExciseNodeValidity` / `ExciseRelValidity` and twins): the cascade kernel (`cascadeNode` / `cascadeRel` with `void`) appends void pieces where a version was valid in `[a, b)` plus the resumption from `b`. Read side: the point resolver answers `ErrNoVersionValidAt` where a void row wins (`resolveNodeVersionAtCapped`), the interval and Allen kinds test rows minus the void spans (`voidMask`, `chain_resolver.go`), the effective sweep drops void winners (`effectiveSweep`), and the as-of rule never answers a void row (`storeutil.SelectAsOfWithCurrent`, badger `selectAsOfScan`). A void row is always bounded, never current. Semantics: api.md.
- **`tkg_valid_from = 0` is unset in every door of a legacy-axis store** (`UntimedFromTxFrom`, `UntimedUnbounded`): an explicit 0 stores the same row as an omitted key and reads from the ID's mint instant everywhere (point, scan, generic, record, timeline doors); a claim of the instant 0 (`types.ExplicitInstant(0)`) is `ErrZeroInstantLegacyAxis`. On an `UntimedWholeAxis` store (the literal axis, round 11) every stored value is the instant it names, 0 included.
- **The valid axis (round 11)**: how rows record valid time is the store's, fixed at open from its persisted mode (`types.ValidAxis`, `store.ValidAxisCapability`), never a per-row flag. Every reader of a stored `ValidFrom` / `ValidTo` goes through the axis (`ValidAxis.Start` / `Claimed` / `Open` / `End` / `OpenEnd`; core `c.axis()`, stores `validAxis`): never compare a stored value with 0 directly, and write the open end as `OpenEnd()`. Envelope and upper-bound structures (temporal index, valid-end index, zone maps) keep the legacy convention (0 = open) internally: a literal end at 0 widens to open, still a sound superset (`index.EnvelopeAxis`). `UntimedWholeAxis` (the literal axis): an untimed write claims the whole axis and no path derives a valid time from `TxFrom`: an untimed update is the cascade kernel over every valid segment (`cascadeExisting`, `whole_axis_write.go`; SQL:2011 UPDATE without FOR PORTION OF), an untimed delete a retraction (`deleteRetracts`).
- **Newest stamps and history presence without reading history**: `Nodes()/Rels().LatestStamps(id)` (largest `TxFrom`, largest `TxTo`/`DeletedAt`, `deleted`) and `HasHistory(id)` read the current row's stamps through the optional `store.CurrentStampsCapability` (backlog 42: badger answers an entity-cache miss from the row's temporal block only, `storeutil.DecodeWireRowStamps`; tiered asks the one shard the lend door reads and its history walk reuses the route's row probe; core admits it like every cached capability handle, a wrapper that only inherits it keeps its `GetNode` override) plus the optional `store.HistoryStampsCapability` / `HistoryPresenceCapability` (memory: its history maps, stamps cached per ID against the row count; badger: RAM sidecars built lazily and maintained where every history key enters the write buffer, overlay before the badger view; sharded: the slot; tiered: the shards `History` reads, cold shards per ID). A store without the capability is folded from `History`; the door must equal the fold of `Get` + `History` at every moment (compaction, purge, erasure included). The effective scans (`ForEachRelEffectiveByType` / `ForEachNodeEffectiveByLabel`) gather their candidates' current members through the optional `store.MemberIDsCapability` (badger, sharded: the label/type index, no row read; backlog 33; tiered since row 106: every shard's index merged, asked for `DepthAll` reads only), so each candidate's row is read once, by its own timeline.
- **Resolve the open-ended "now" ONCE per scan, never per candidate**: a multi-candidate scan carrying a TxAt-ONLY `QueryOpts` (no `TxPin`/`ValidAt`/`ValidStart`/`ValidEnd`) must pass its opts through `c.normalizeTxAtOnlyOpts(opts)` (`temporal.go`) BEFORE the loop — it pins the implicit "now" into `opts.ValidAt` so every candidate is evaluated against the same valid-time bound. Otherwise each `findNodeVersionForOpts`/`findRelVersionForOpts` call resolves its own fresh "now", and an entity whose `ValidTo`/`DeletedAt` boundary falls between two per-candidate reads is included by one probe and excluded by another inside ONE logical query. Every top-level scan door that loops the per-ID resolver applies it (13 call sites).
- **A read's implicit "now" is `c.readNow()`, never the bare wall clock**: every implicit "now" of a read — the TxAt-only valid-time probe and the open end (`end == 0`) of an interval read (`c.resolveOpenEndInstant` / `c.normalizeDuringRange`) — is `readNow()` = max(wall clock, transaction-clock floor) (`context.go`). A version without an explicit `tkg_valid_from` starts its valid time at its `UpdatedAt`, stamped by `c.now()`, whose floor runs ahead of the wall after a >1 write/ms burst, after `AdvanceClock`, and on a replica that applied a primary's stamps (lesson 71); probing at the wall put the newest versions "in the future", so a TxAt-only door returned an older superseded version, a different one per call as the wall caught up, and primary and replica disagreed. Pinned by `TestTxAtDoorsWhenTxClockIsAheadOfWall` (every such door, memory + badger, primary + replica).
- **Allen-predicate interval doors (OPT10)**: `g.Temporal().NodesRelating(from, to, rels)` / `RelsRelating(...)` return entities having a version whose valid-interval `[vStart,vEnd)` has an Allen relation (in the `types.AllenRelationSet` `rels`) to the query interval `[from,to)`. `probeRelate` classifies per-version via `types.RelateOpen` (open ends → +∞), predicate-anywhere/most-recent-first — so the ONLY temporal doors that surface NON-overlapping relations (`Before`/`After`/`Meets`/`MetBy`). CRITICAL: the B4 envelope-overlap prune is OVERLAP-sound and MUST NOT be applied to these doors (it would drop a `Before`/`After` match); it is wired only into the overlap-based `During` doors. The query end is passed RAW (0 = open) — never pre-resolved to a "now+" bound, which would corrupt the Before/After boundary. `from==0` (unset; a negative `from` is a real start) or `from>=to` (closed) → `ErrInvalidTimeRange`; empty `rels` → empty result.
- **Point-door historical resolution runs on selection skeletons when the store can enumerate temporal metadata cheaply**: the optional `store.TemporalMetaHistoryCapability` (native badger only; memory/tiered/sharded decline) lets `nodeAtLockedTx`/`relAtLockedTx` select the winning version on per-version SKELETONS (version + numeric temporal instants, via `storeutil.DecodeWireTemporalMeta`'s partial wire decode) and fully decode ONLY the winner (`nodeAtViaTemporalMeta`/`relAtViaTemporalMeta` in `temporal_queries.go`). The chain resolver stays the single selection authority — the SAME `resolveNodeChain` runs on skeletons and again after winner hydration; a skeleton never leaves the seam (hydration failure falls back to the full-chain arm — `nodeAtFullChain`, inlined in `relAtLockedTx` for relationships — never a wrong answer). CRITICAL resolver input contract (documented in `chain_resolver.go`): the chain must arrive in ASCENDING-VERSION order and the resolver may SORT it in place, so a double-resolving caller hands EACH run its own pristine copy — feeding a sorted output back in flips the monotonic-vs-cascade branch (lesson 73). Equivalence locked by `temporal_skeleton_test.go` (A/B fast-vs-full grids + randomized lifecycles) and the bitemporal oracle harness.
- **Pinned label/type scans are output-sensitive (K1)**: a history-aware `ByLabel`/`ByType` (one carrying a temporal filter) scopes its candidate set to the label/type's transaction-time EVER-MEMBERS via the optional `store.LabelTxMembershipCapability` / `RelTypeTxMembershipCapability` sidecar (`c.labelTxMembers` / `c.relTypeTxMembers`; memory, badger and tiered since v4.52.1, wrappers that only embed a native store decline) instead of folding ALL node/rel history — O(matches) not O(everything ever). A member whose earliest-acquisition transaction time post-dates the effective pin (`TxPin` else `TxAt`) is pruned WITHOUT loading its chain. The sidecar is an APPEND-ONLY SOUND SUPERSET (removal/delete never drops a member; the chain resolver stays the correctness authority and rejects over-included candidates), lazily built on first use and rebuilt after reopen (mirrors the OPT15 `relValidIdx`), maintained at every label-acquisition door; removal/delete are no-ops. A store without the capability takes the correct full-history fold (`forEachNodeHistoryIDByDepth`). The seam is `nodeLabelCandidatesSorted` / `relTypeCandidatesSorted` in `queries.go`; both doors funnel through the SAME `findNodeVersionForOpts`/`findRelVersionForOpts` resolver, so pruned and unpruned are equivalent by construction (pinned divergence battery). NOTE the `TxAt` door remains the documented wall-clock footgun; the equivalence guarantee is on the well-defined `TxPin`/`ValidAt` doors.
- **Temporal property lookups are output-sensitive (backlog 8)**: `ByTypeAndProperty` / `ByLabelAndProperty(ies)` with a temporal filter and the named `*PropertyAt` / `*PropertyDuring` doors take their candidates from `forEachRelPropertyCandidateID` / `forEachNodePropertyCandidateID` (`temporal.go`): current index matches united with the value's ever-members from the optional `store.RelPropertyTxMembershipCapability` / `NodePropertyTxMembershipCapability` sidecar of a DECLARED property index (memory, badger; sharded and tiered = union of shards, tiered since v4.58.0 with paged members since v4.69.2; wrappers decline), pruned by `firstTx > effective pin`; no sidecar or no declared index (`ErrIndexNotFound`) = the history fold scoped to the label's (type's) ever-members by the K1 point lookup (`store.LabelTxMemberLookupCapability` / `RelTypeTxMemberLookupCapability`, `c.labelTxLookup` / `c.relTypeTxLookup`, exact native memory/badger only; `forEachNodeHistoryCandidateOfLabel` / `forEachRelHistoryCandidateOfType`), else the full-history fold, same answers. The sidecar is keyed like the index ((type, key) / (label, key), node rows must carry the label and value in the same row), append-only, RAM-only, lazy per index, recorded where rows enter the store (memory: `storedRel` / `historyRel` / `storedNode` / `historyNode`; badger: `maintain*PropertyIndexesAdd` for current rows, next to the belief-watermark bump for history rows, `enqueueVersionAgainstLazyBuilds` for `Put*Version`), and dropped by Clear, index drop, retention purge and exact erasure. firstTx 0 (unstamped row) is sticky (never prunes). The badger build does NOT hold `idxMu`: tracking on under `idxMu`, overlay captured, view scanned, scan installed only if the drop generation `propTxGen` did not move (lessons 63/64/74).
- **Adjacency-at-t fold uses the seed's ex-adjacency**: `g.Temporal().OutgoingRelsAt` / `IncomingRelsAt` / `NeighborsAt`, the pinned doors `g.Rels().Outgoing/IncomingForNodesAtPin` / `*ForNodesAtTx`, `g.Temporal().ForEachRelEffectiveAtNodes` AND the scan doors `g.Rels().ForEachAdjacentRelAt` / `ForEachAdjacentEndpointAt` under a valid-time filter (`forEachAdjacentRelVersionLocked` — each adjacent rel resolves through `findRelVersionForOpts`, so the yielded row is the version valid under opts, never a live row the filter rejects) go through `forEachRelAdjacencyCandidateID` (the scan doors: `relAdjacencyCandidateIDsSorted`; under a pure valid-time filter a live rel without history rows is judged from its live row, `singleVersionAdjacency`): candidates = each seed's live adjacency ∪ its ex-adjacency (store `ExAdjacencyCapability`, backlog 50) — every rel that named the seed in a row the live adjacency no longer ties to it. Endpoints never change within one life, but a rel deleted, or deleted and re-imported with another start or end, keeps rows naming the old endpoints; the store writes both legs of the dropped row into the ex-adjacency in the same batch as every delete (not a tiered move between shards, `Remove*Placement`, nor an undone create, which core routes through `RelCreateUndoCapability` / `undoRelationshipCreate`), and both legs of every history row whose endpoints are not the live row's (`PutRelVersion`: replica history-version apply, `IO.Import`, `ImportMerge`, cascades), so every write door is covered without core-side noting. Cost O(the seeds' ever-degree), never O(deleted) or O(history). The core trusts the index only once the one-time open scan of a store written before it finished (`backfillExAdjacencyBestEffort`, meta `rel_exadj_built`; a read-only open does not scan); until then, and on a store without the capability, it folds every rel with history (sound). Badger stamps store marker 3 so ≤ v4.51.1 (which would delete without the index) refuses to open it. Label/property temporal queries must keep using the full-history `forEachNodeCandidateID` / `forEachRelCandidateID` because entities can have their CURRENT label/property differ from their at-t state.
- **Declared vs effective relationship visibility**: EFFECTIVE doors (`Snapshot`, `Diff`, `NeighborsAt`, `OutgoingRelsAt`, `IncomingRelsAt`, tx mirrors included) show an edge at t only if its row AND both endpoints are valid at t (`nodeAtLocked` semantics); they must agree edge-for-edge (`TestEffectiveDoorsAgree`). DECLARED doors (`RelsAt[Tx]`, `RelsDuring[Tx]`, `RelsAsOf`, `RelsRelating`, `RelsByType*`, generic `Rels()` doors with `QueryOpts`) report the relationship's own asserted validity and are never endpoint-masked; a bulk consumer relies on this. A new relationship door must name its class in its doc comment and join the matching test.
- **History-aware merging**: Temporal queries merge current + history IDs via lazy ForEach iterators (two-phase: collect IDs through Store iteration, process after return).
- **Temporal neighbors validate target time first**: `NeighborsAt(nodeID, t)` must prove the target node exists at `t` using `NodeAt` semantics, then tolerate `ErrNodeNotFound` from current adjacency reads as an empty current candidate set so deleted-but-historical nodes still traverse relationship history.
- **ForEach for OOM-safe iteration**: Never materialize all per-shard slices + merge. Use `ForEach*ID` callbacks. Store implementations must invoke caller callbacks outside backend locks, Badger transactions, and Tiered shard checkouts so callbacks can call Store methods without deadlock. Nil callbacks return `ErrInvalidStoreMutation` on open stores; closed stores return `ErrStoreClosed` first. Two-phase: collect a shard/page of IDs, release locks/checkouts, then invoke callbacks. Badger history iterators must cap direct iteration at the start high-water mark so callback-created higher IDs cannot extend the same iterator. ~83% memory reduction.
- **Deleted entity verification**: Any verification reading entity state must tolerate deletion — if entity has history but no current state, proceed using history alone.
- **Temporal diff callbacks do not run under graph locks**: `g.Temporal().DiffCallback` may hold `c.mu.RLock()` while collecting IDs or resolving one entity pair, but it must invoke caller `DiffHandlers` only after releasing that lock. A handler that reads the graph while a writer is waiting must not deadlock on `sync.RWMutex` writer preference.

### Temporal constraints

- **Unknown constraint kinds fail closed**: `g.Constraints().Add` and `Set` must reject unsupported `TemporalConstraint.Kind` values before changing graph state. `checkTemporalConstraints` must also return wrapped `ErrTemporalConstraint` and `ErrInvalidTemporalConstraint` for unsupported kinds, including the zero value, if one reaches enforcement. An invalid configured constraint must never be silently ignored as if the graph had no constraints. `ConstraintSet.ForEach(nil)` must return `ErrInvalidTemporalConstraint` before checking set length.

### TieredStore rules

- **Caller-supplied event node IDs create on their timestamp owner**: graph-generated current event IDs normally resolve to hot, but imported/backfilled event node IDs must be written to the same event shard that `shardForNodeID` will later choose.
- **Create-time duplicate checks must include cold owners**: caller-supplied node and relationship IDs are global even when the existing row lives in an idle-closed cold event shard. Public Store duplicate probes must use cold-shard-aware ID routing before any write, not only already-open shard maps. Relationship probes must resolve actual owner placement, not only the relationship ID timestamp, because a current-ID relationship can still be stored on an old start-node shard. Core-generated fresh IDs may use an internal generated-ID fast path, including batch node execution, to avoid opening unrelated cold shards; do not expose that weaker duplicate-probe contract as a public Store capability.
- **Cross-shard split-write ordering (section 12)**: E->R: write ref shard inIdx first. R->E: write entity shard first. Both endpoints verified before any writes. Under `Config.RelsByMint` a ref->ref rel is no split write: entity + both legs go in one batch to its ID's mint event shard (`PutRelationshipCoLocated`; a cold mint shard falls back to the reference shard, counted). Such a rel's row is never on its start node's shard, a split-written one's always is: delete and repair tell the two apart by that (`relLegsColocated`), and repair never regrows its in/ leg on the reference shard. Reference/archive adjacency reads fan out over the event shards `QueryOpts.Depth` selects once the sticky `rels_by_mint` meta marker is set; node deletes check every tier.
- **Checkout/checkin for cold shards**: a raw shard-store pointer read without ref counting races with `closeIdleShards()`. Use `checkoutStore()`/`checkinStore()` to increment/decrement `activeReqs`.
- **RefArchive uses the same pinning discipline**: use `checkoutArchive()` before dereferencing archive state. If a caller already owns a pinned shard snapshot, resolve ownership from that snapshot or carry an explicit `isArchive` bit; never re-compare against a fresh `refArchive.Load()` after checkout because `Close()` nils the pointer before active archive checkouts drain.
- **Idle-close errors are durable state**: `closeIdleShards()` calls `BadgerStore.Close()`, which performs a final flush. Any idle-close error must be recorded, logged, block later lazy checkouts, and be joined into `Store.Close`; never discard it as unreachable background cleanup.
- **Close and idle-close share the shard mutex**: any path that closes or clears `EventShard.store` must hold `EventShard.shardMu`. `Store.Close` must use the same lock as `closeIdleShards`, not only rely on `activeReqs`.
- **Post-close routing fails closed**: checked node/relationship routing, create-time rotation, TieredStore index create/drop/search operations, direct read/count/bulk/history APIs, empty batch/history write edges, and public admin/metadata operations must return `ErrStoreClosed` after `Store.Close` starts, before probing `refShard`, event shard handles, store-level index maps, cached read state, catalog metadata, or registry files.
- **Rotation boundary alignment**: Truncate to millisecond + add one unit. Nanosecond precision creates gaps with ms-resolution snowflake IDs.
- **Weekly shard windows are ISO week windows**: for 1-week `ShardWindow`, compute ISO week 1 from the week containing January 4, not January 1. Years where January 1 belongs to the previous ISO week must not route week-one entities into the previous shard.
- **Sub-day shard windows are fixed-duration windows**: for accepted minute/hour `ShardWindow` values, compute the start with the duration boundary containing the timestamp; do not fall back to month start. `ShardWindow` must be a whole millisecond so window comparisons align with snowflake timestamp precision.
- **Catalog sync on rotation**: Both in-memory `eventShard.timeEnd` AND catalog `ShardEntry.TimeEnd` must be updated on hot->warm rotation. Catalog is persisted — warm shard recovery depends on it.
- **Catalog-visible changes are save-or-rollback**: any operation that adds a shard entry, rebuilds catalog stats/tiers, or marks verification/stats in the catalog must either persist the catalog or restore the prior in-memory catalog before returning. Do not keep process-local catalog state that restart cannot recover.
- **RebuildCatalog counts durable shards**: closed cold event shards are not exempt from rebuild. Open and count their backing store; carrying forward old catalog counts is stale metadata, not a rebuild.
- **ListShards reports useful closed-shard counts**: do not lazy-open cold shards just to list them, but do use catalog `ApproxNodes`/`ApproxRels` for closed cold shards and archives. Reporting zero because `store == nil` misleads admin callers.
- **Clear covers every shard**: `tiered.Store.Clear` must wipe ref, archive, hot/warm event shards, closed cold event shards, and restarted warm shards. It must also reset catalog verification/stat caches; stale `Verified` or count fields after Clear are wrong state, not harmless metadata.
- **Warm/cold tiers are not Badger read-only**: Hot/warm/cold controls create routing and idle-close. Existing event entities keep updating/deleting on their owner shard after rotation and restart, so reopened warm and lazy-opened cold Badger handles must be writable.
- **Rollback on partial failure**: Cross-shard moves (archive/restore) must undo completed steps when a later step fails. Otherwise partial failure leaves data duplicated or orphaned.
- **Archive/restore preflights destination placement**: `ArchiveNode` and `RestoreNode` must reject destination node duplicates, duplicate destination relationship entity/out placement, and live destination adjacency for the moving node before writing the temporary destination node. Orphan destination adjacency whose relationship row is already missing must be purged during preflight. Do not rely on rollback to row-delete a connected temporary node.
- **Rollback errors are part of the failure**: rollback helpers must attempt all completed undo steps they own, ignore only expected idempotent not-found sentinels, and include rollback failures in the returned error with the primary failure.
- **DeleteNodeWithHistory relationship rollback**: tiered node deletes remove relationships before the node tombstone write; snapshot each relationship and its history first, and restore committed relationship deletes if a later rel delete or node tombstone write fails while the node row still exists.
- **DeleteNodeWithHistory tombstone coverage**: validate missing, duplicate, and unrelated relationship tombstones before the first relationship delete. A rollback path cannot recover history that was never supplied.
- **Repair scans index entries, not only live entity rows**: `RunRepair` orphan cleanup must enumerate `in/` index entries directly. A stale incoming entry can be keyed by an end node that no longer has a node row, so `AllNodeIDs` + `IncomingRelIDs(nodeID)` is not a complete repair scan.
- **Sequential ForEach**: One shard open at a time via checkout/checkin. No goroutines, no `mergeIDSlices`. Trades parallelism for memory safety.
- **History reads reach every life (backlog 49)**: a re-import of a deleted ID can put its new row on another shard than its earlier lives' history (a relationship's new start node elsewhere, a node's class changed). Every caller-ID create records such an ID (`relCreateProbe`, the duplicate probe and the note in one walk / `noteNodeEarlierLives`; generated-ID creates and a rollback restore of a deleted row skip it, `restoreDeletedRelationship`; reference-shard meta, fsynced before the row, plus an in-memory set loaded at open; pre-v4.51.2 re-imports found once by `backfillHistoryLives` at open; only `Clear` drops a record) and every per-ID history door routes a recorded ID through `relLivesRoute` / `nodeLivesRoute` to every history shard. A new history door must take that route; never move history between shards (the change log would carry a truncate that a replica replays as the loss of the chain).
- **Valid-end bounded scans read the index's candidates, never the whole history (backlog 51)**: `g.Temporal().ForEachRelEffectiveByTypeFrom` / `ForEachNodeEffectiveByLabelsFrom` take their candidates from the store's `RelValidEndCapability` / `NodeValidEndCapability` (RAM `index.ValidEndIndex`, memory and badger; tiered since v4.52.1), then read each candidate's `*EffectiveTimeline` and decide; the index is only a sound superset. The valid-end bound is folded from ROWS only (each row's `ValidTo`, a delete tombstone recorded after the entity's earlier stamps), never from the chain resolver: a bound that modelled `findRelVersionForOpts` would drift from it, and the resolver stays the authority. A removal or an overwrite of a row, and any write to a refined entity (`RefineRelValidEnd` / `RefineNodeValidEnd`, keyed by the generation the candidate carried), moves the entity back to a re-read on the next call, so a store hook that removes or rewrites rows must note it. Tiered and sharded do not implement the capability and answer through the unbounded candidates (same answers, not accelerated). The index is per QUERIED type or label: the first call tracks it (ever-members from the K1 membership sidecar), only tracked tokens are held and maintained, and a write of an untracked token costs one map lookup (badger decodes no row for it).
- **Depth-limited history uses current ownership first**: archive history alone can exclude an ID from `DepthHot`/`DepthWarm`, but a restored entity that currently lives on `refShard` must remain eligible even if old history records still exist in `refArchive`.
- **Depth enums fail closed**: every Store query method that accepts `QueryOpts.Depth` must accept only `DepthAll`, `DepthHot`, and `DepthWarm`. Unknown values return `ErrInvalidShardDepth`; never silently widen an invalid depth to `DepthAll`. Single-shard stores may treat the three valid selectors equivalently.
- **Graph QueryOpts validate before empty shortcuts**: graph-level query APIs that accept `QueryOpts` must validate `Depth`, `Limit`, `After`, and active interval ranges before checking whether a label/type/index exists or whether `k <= 0`. Invalid options are caller input errors, not successful empty result sets.
- **Query limits are non-negative**: `QueryOpts.Limit` accepts `0` for unbounded queries or a positive maximum. Negative limits must return `ErrInvalidQueryLimit`, not behave like `0`.
- **Query cursors are non-negative**: `QueryOpts.After` accepts `0` for the first page or a non-negative entity cursor. Negative cursors must return `ErrInvalidQueryCursor`, not behave like the first page.
- **Raw history cursors share pagination validation**: `AllNodeHistoryIDsFrom` and `AllRelHistoryIDsFrom` must reject negative limits with `ErrInvalidQueryLimit` and negative cursors with `ErrInvalidQueryCursor`, even when there are no history rows.
- **History retention is non-negative**: `TruncateNodeHistory` and `TruncateRelHistory` may use `keepVersions == 0` for explicit clear-all, but negative retention must return `ErrInvalidStoreMutation` before deleting history.
- **History target IDs are positive**: entity-specific Store history reads/truncation and graph history sub-APIs reject zero or negative IDs with `ErrInvalidStoreMutation` before empty-history or version-not-found semantics.
- **Persisted entity IDs are positive**: graph import-by-ID APIs reject negative caller-supplied IDs with `ErrInvalidID`; graph update, label mutation, property CAS, close-version, delete, admin archive/restore, and tx/batch variants reject zero or negative target IDs with `ErrInvalidStoreMutation` before lookup, snapshot, capability checks, or queueing; graph relationship create/import paths reject zero or negative endpoint IDs before endpoint work; Store current-row, history snapshot, relationship index, delete/cascade, archive/restore, and batch write/delete mutations reject zero or negative entity IDs with `ErrInvalidStoreMutation` before mutation.
- **Import staging caps are non-negative**: `ImportOptions.MaxStagedBytes == 0` is unlimited, positive values cap the staging file, and negative values must return `ErrImportSizeLimit` before creating a staging file.
- **ID-only QueryOpts still mean temporal filters**: `AllNodeIDs` and `AllRelIDs` may avoid deserialization only when no temporal filter is present. With `ValidAt` or a complete interval, they must filter by entity temporal metadata before applying `After`/`Limit`.
- **Per-shard indexes**: node property (every label), relationship property, composite and relationship-type temporal indexes are per shard (`shard_index_fanout.go`, as sharded; `ErrEventPropertyIndex` is deprecated and never returned): every shard builds and maintains its own over its own rows, the reference shard anchors the definitions (create reaches it last, drop leaves it last, a failed fan-out undoes the shards it changed), and a new hot shard, the archive and every open shard at store open are synced to the anchor. The rel temporal index is bounded to the reference, hot and warm shards: a cold demotion frees it, cold shards open with `badger.Config.DropRelTemporalIndexesAtOpen` (no rebuild), the DDL never opens a cold shard or touches the archive for it, a promotion (`PromoteColdShardsAtOpen`) rebuilds it at open; cold rows are never pruned, so answers do not change. Resident ≈ 156 B per relationship on hot + warm shards (index structure alone, live heap after GC over 1 M and 4 M rows, `internal/index/index_budget_test.go`) (≈ 5.8–9.2 GB per indexed type at ai-soc's rate with `ColdAfter` = 1 day); a reopened or promoted warm shard rebuilds at ~0.02 M rels/s (29–54 min for a day of ai-soc rows). The prune never opens a shard and skips the reference shard once an archive exists. A property lookup trusts a shard's copy only when its options equal the anchor's (else it scans that shard); the property membership sidecars answer only when every shard carries the anchor's index (else `ErrIndexNotFound`, the history fold).
- **Background-error recovery**: idle/transient cold-shard close failures set a sticky background error (fail closed on every path). `tiered.Store.RecoverBackgroundError()` re-probes persistence via an atomic catalog save and clears the gate in place on success — no close/re-open. Lifecycle gate only; run `VerifyShard`/`RunRepair` for data confidence.

### Sharded backend re-sharding (ADR-0007)

- **Slot routing is a pure, immutable function of the ID**: `shardFor(id) = catalog[decompose(id).Node]`. Unlike `TieredStore` (routes by time window + ontology class, and can relocate data), a `sharded.Store` entity's shard is fixed at mint time and can never change without changing the entity's own ID — which would break every relationship pointing at it, its hash chain, and external references. There is no in-place "move this entity to another shard" primitive, and none is planned.
- **Grow and empty-slot shrink are the supported re-sharding path, via export/import — TESTED, not incidental**: widening `Config.SlotCount` (or narrowing it to a range that still covers every slot actually in use) needs zero new routing code. `g.IO().Export` from the old topology followed by `g.IO().Import` into a freshly-opened store under the new `Config` reconstructs every entity verbatim through the same store doors (`PutNode`/`PutRelationship`/`PutNodeVersion`/`PutRelVersion`/…) any other write uses — doors that route by the entity's own already-fixed slot, identically to a normal write. Proven by `TestShardedResharding_*` in `pkg/graph/sharded_resharding_test.go`: exact-set live/history parity (rule 16), rule-15 two-phase temporal survival across the migration, graph-level hash-chain re-verification (`g.Hash().VerifyNodeChain`/`VerifyRelChain`), a zero-`ShardMismatch` `VerifyConsistency()` report, and change-log-source watermark handoff.
- **Unsafe shrink/rebase fails closed by design, not by accident**: importing into a topology whose claimed range does not cover a slot some migrated entity actually lives on hits the same `ErrSlotNotLocal` any other misrouted write would, and `Import`'s existing rollback machinery undoes every record already replayed in that migration — the target ends up fully empty (not partially migrated), and the source (read-only `Export`) is untouched. This is a correctness feature: a silent partial migration would be far worse than a loud refusal.
- **Out of scope, deliberately**: live/online topology changes, and a non-identity `SlotShard` rebalance (relocating an already-minted entity's row without changing its ID). Neither has a primitive today; both would require either a large new consistency layer or physically merging badger stores under a routing table the ID itself no longer determines — and the immutable-ID-routing argument above means neither can be expressed without changing entity identity. Not deferred-but-planned — architecturally out of scope for this backend.
- **Growing `SlotCount` does not by itself mint anything into the new slots**: pair a grow with widening the consuming `Graph`'s ID-minting configuration (`Config.SnowflakeNodeID` for the interactive pair, and/or `Config.IngestLanes` for additional per-lane generators) — otherwise the new slots stay permanently empty.

### Integrity and indexes

- **By-ID relationship creates are not weaker relationship creates**: `AddRelationshipByID` / `AddRelationshipByIDIfAbsent` only change the endpoint input form. They must still fetch live endpoints under endpoint locks, capture `FromNodeHash`/`ToNodeHash`, enforce configured constraints, and defer rel-type token allocation until after endpoint fetches succeed.
- **Orphan relationship IDs are index data too**: cascade delete paths that tolerate a missing relationship entity must purge that relationship ID from type and adjacency indexes before returning success.
- **3-phase index creation**: (1) Install empty placeholder under Lock (visibility for concurrent writes), (2) unlocked I/O to build, (3) Lock to install with dirty-map tracking (`mutated[id]` not `contains(id)` — prevents re-adding concurrently deleted values).
- **Graph-level index create is not a no-op for missing labels**: `g.Index().Create*` must validate the label, allocate the label token when needed, then create the Store index so future matching writes are maintained.
- **Graph-level index target operations are not no-ops for missing labels**: `g.Index().Delete*` and `g.Index().SearchNearest` must validate the label, validate property-key length where applicable, and return the matching not-found sentinel for unknown labels.
- **Vector index creation follows the same dirty-map rule**: placeholders must be removed on operational scan errors, and backfill must skip IDs touched by concurrent vector maintenance. Presence checks are not enough because a concurrent update can intentionally remove the vector entry.

### Unique property constraints (ADR-0002)

- **Enforce at the finalized-state seam, not per door**: `enforceUniqueForNode`/`enforceUniqueForNodeHeld` (`unique_constraints.go`) is consulted by every standalone node door that can introduce or change a constrained `(label, value)` pair — `Add`/`AddWithTx`/`AddByIDIfAbsent`, `Update`/`UpdateInPlace`/`CompareAndSetProperty`, AND `AddLabel` (the door everyone forgets: a node carrying an offending value acquiring the constrained label is rejected). `UpdateInPlace` passes its pre-mutation state so a value CHANGED in place also holds the freed old-value stripe (ADR Decision 3). Batch/tx enforce too: `GraphTx.AddNode`/`UpdateNode` flow through the same internal doors (a tx applies mutations immediately, so a same-value create inside one tx sees the earlier one); `BatchBuilder.AddNode` creates are pre-checked (`partitionBatchNodesByUnique`, `unique_enforce_batch.go`) under the batch's exclusive `c.mu.Lock` against committed state, earlier same-batch creates, AND the durable `UniqueForever` ownership registry (a fresh batch create claims ownership durably; the exclusive lock fences concurrent standalone writers, so no value stripe is needed). `SetNodeVersionInterval` (all four doors) enforces in the cascade kernel under the entity lock on the built rows, after every kernel check and right before the first store write; a failed store write withdraws the forever claims no already-written row carries (`enforceUniqueForCascade`, `uniqueHold.writeFailed`): an open-ended patch is judged as the new current row (both scopes, old+new stripes), a bounded patch against `UniqueForever` only; stripes held until the kernel returns. `g.IO().Import` validates the replayed current state post-replay (`validateUniqueAfterImportLocked`) and rolls back with `ErrUniqueViolation`; `ImportOptions.SkipUniqueValidation` opts a trusted restore out. Replica apply does NOT enforce (verbatim reproduction — lesson 50).
- **3-phase create, two live scopes**: `CreateUnique` installs a PENDING entry under `uniqueMu` (concurrent writers enforce immediately — by design), validates existing data unlocked via the property index (auto-ensured), then activates + persists — or uninstalls + returns `ErrUniqueViolationExisting` (≤5 offenders). `UniqueCurrent` (default) consults CURRENT state only (history duplicates legal; a freed value is reusable). `UniqueForever` (`CreateUniqueForever`, `unique_forever.go`) adds a durable value→owner registry: the first entity to hold a value owns it forever (barred from every other node across supersession, hard delete, reopen). The registry is its own self-hashed MetaKV blob (`SafeUnmarshal`, fail-closed on tamper, reaped by `Reset`); the kernel consults it under the value stripe (hit+different entity→violation; same entity→pass; miss→claim+persist). Every claiming door (node doors, batch pre-check, ingest, cascade) shares `uniqueHold` (`unique_hold.go`): it records the claims its call made (registry misses only, so a value owned before the call is never withdrawn), and a call that fails — its store write or a later claim — withdraws them BEFORE releasing the stripe, keeping a claim only when the node's stored row carries the value (`storeWriteFailed` re-reads it; an unreadable row keeps every claim). A crash between the claim's persist and the row write still leaves the claim (atomic MetaKV + store commit: v5 PLAN §5.2). `CreateUniqueForever` seeds owners from current values; `DropUnique` releases a forever constraint's claims; `Constraints().ReleaseOwnership(label, key, value)` is the operator free door (idempotent; `ErrUniqueConstraintNotFound` without a forever constraint). A forever claim made in a rolled-back tx stays barred (durable claim not part of the tx snapshot — `ReleaseOwnership` remedies). `UniqueValidOverlap` still returns not-yet-supported. Float keys/values → `ErrUniqueUnsupportedType`.
- **`GetOrCreateByKey`**: `g.Nodes().GetOrCreateByKey(ctx, label, key, value, extraProps)` takes the value stripe, returns a property-index hit as a mutable copy, else creates under the same held stripe (passed through `addNodeInternal`'s `heldStripes` so the create's own kernel does not re-lock it). Works WITHOUT a constraint (the value lock alone makes it atomic). Float/non-scalar value → `ErrUniqueUnsupportedType`.
- **Lock discipline**: hold the value stripe(s) across the index check + store write (see "Lock ordering" under Concurrency). Sentinels are canonical in `internal/core/core.go`, re-exported in `pkg/graph/errors.go`.

### Events and stats

- **EventBus is opt-in**: `g.Events().SetSync(bus)` — nil by default (zero overhead). Handlers are copied under RLock, invoked outside the lock (prevents deadlocks from re-entrant Graph calls in handlers).
- **Event bus setters fail closed**: `g.Events().SetSync` and `SetAsync` return `ErrGraphClosed` after graph close and must not detach or replace the installed publisher in that state.
- **IndexProvider errors are diagnostics, not silence**: `IndexProvider.OnEvent` errors do not abort already-committed mutations, but must be logged with provider name and event context. Do not drop them with `_ = p.OnEvent(ev)`.
- **IndexProvider registration validates inputs before methods/lookup**: `RegisterProvider` must reject nil and typed nil provider interfaces before calling `Name()`, and registration/unregistration must reject empty or whitespace-only provider names before registry mutation or lookup.
- **IndexProvider teardown waits for Init**: `RegisterProvider` stores `Initializable` providers before running `Init`, so `Graph.Close()` and `UnregisterProvider` must wait for any in-flight `Init` callback before calling provider `Close()`.
- **StoreStats opt-in**: Type-asserted in `(*Core).Stats()` (reachable via `g.Stats().Get()`) — avoids polluting the `Store` interface.
- **Atomic operation counters**: 8 `atomic.Int64` fields on `Core` (adds / reads / updates / deletes × node / relationship) — the write counters are incremented after every successful public mutation write, including batch-created nodes/relationships and temporal close-version updates.

### Temporal indexes

- **High-frequency buckets must be whole milliseconds**: `CreateHighFrequency` and Store-level `CreateHighFrequencyIndex` reject bucket sizes that are not positive whole milliseconds with `ErrInvalidTemporalIndexConfig` before installing an index. A created HFI must not collapse or truncate buckets relative to `Instant`'s millisecond precision.
- **High-frequency indexes must be populated and maintained**: Store-level `CreateHighFrequencyIndex` must backfill matching current nodes, and all node write/history/delete paths must update HFI buckets after creation. Badger HFI creation must use a placeholder scoped to that create, fail closed on corrupt existing rows, and delete only that placeholder so tiered create rollback can undo earlier shards.

### Vector indexes

- **Persistent definitions, in-memory entries**: BadgerStore persists property, temporal, high-frequency, and vector index definitions, then rebuilds entries on open. TieredStore persists temporal/HFI tracking metadata and vector definitions; composite and rel temporal definitions persist in each shard, anchored by the reference shard. MemoryStore remains process-local. Drop and `Clear()` paths must remove persisted definitions so restart cannot resurrect removed indexes.
- **Persisted vector definitions are map-shaped**: BadgerStore vector metadata and TieredStore vector definition files may contain at most one effective definition per `(label, property)`. Identical duplicates can be deduplicated, but conflicting duplicates must return `ErrVectorIndexExists`; never let the last persisted record silently choose the active dimensions or metric.
- **Never persist in-flight index placeholders**: Badger property, temporal, high-frequency, and vector definition serializers, plus Tiered vector definition serializers, must skip indexes whose phased create is still backfilling. A placeholder is visible to concurrent writes for maintenance, but it is not a durable index definition until finalization clears the build marker.
- **Never query from in-flight index placeholders**: Badger property, temporal, and high-frequency query fast paths must ignore building placeholders and use the complete scan path. Badger/Tiered vector search must return `ErrVectorIndexNotFound` while the vector placeholder is still backfilling. Placeholder visibility is only for concurrent write maintenance.
- **Tiered vector definition writes are all-or-error**: `CreateVectorIndex` and `DropVectorIndex` must snapshot the raw `vector_indexes.msgpack` file before persisting, restore or remove it on write failure, and fsync the metadata directory when removing the last definition.
- **Tiered catalog writes are all-or-error**: `ShardCatalog.Save` must snapshot the raw catalog file before writing and restore or remove it on write failure so caller-level in-memory catalog rollback cannot diverge from disk.
- **Tiered registry writes are all-or-error**: the flat `registry.msgpack` save helper must snapshot the raw file before writing and restore or remove it on failure so direct registry save APIs do not return an error after silently changing durable metadata.
- **Tiered registry loads validate both halves**: `registry.msgpack` load must reject malformed label or relationship-type slices before returning metadata to startup/load code or deprecated single-registry save paths. `SaveLabelRegistry` and `SaveRelTypeRegistry` must not preserve a corrupt other half.
- **Store-level scope in TieredStore**: Vector indexes live at the `TieredStore` level (not per-shard) with their own `vectorIdxMu sync.RWMutex`.
- **Auto-maintenance**: All mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`) update vector indexes, including paths that mutate state and return a corruption error after cleanup.
- **Validate vector index config before install/load**: `dims` must be > 0 and metric must be `DistanceCosine` or `DistanceEuclidean`. Return `ErrInvalidVectorIndexConfig` before installing placeholders or accepting persisted definitions.
- **Vector sentinels belong to the Store contract**: vector capability errors are canonical in `pkg/graph/store`, with aliases in root `graph`, `pkg/graph/index`, and concrete backends. Do not force Store-interface users to import a backend for `errors.Is`.
- **Candidate resolution errors propagate**: `SearchNearestNodes` may skip `ErrNodeNotFound` from concurrent deletes, but corruption or storage read errors while resolving ranked IDs must be returned.
- **Vector QueryOpts are part of the Store contract**: Direct Store `SearchNearestNodes` callers receive temporal filters before heap selection and `QueryOpts.After`/`Limit` applied over the distance-ranked slice. Graph-level `SearchNearest` still owns history-version resolution and therefore strips cursor opts before backend search, then paginates the final resolved slice exactly once.
- **Vector heap capacity is bounded by available entries**: caller `k` is a result limit, not an allocation size. `VectorIndex.SearchNearest` must cap heap capacity by indexed entry count so a huge `k` cannot allocate huge memory on a tiny index.
- **Graph vector over-fetch buffers are bounded by the probe ceiling**: external backends without `FilteredVectorSearchCapability` use the graph-level temporal over-fetch loop. Size result buffers from the clamped probe size, not raw caller `k`, so a huge `k` cannot panic before the bounded backend call.
- **CreateVectorIndex is all-or-error for matching vector rows**: if an existing matching node has a vector property with the wrong dimension, return `ErrDimensionMismatch` and remove the placeholder index instead of silently building a partial index.
- **Vector-maintained writes are all-or-error**: once a vector index exists, validate matching node vector dimensions before mutating store state. Return `ErrDimensionMismatch` rather than committing a node that the active index silently omits.
- **Phased index create cleanup is placeholder-scoped**: on Badger/Tiered phased create failures, delete only the placeholder pointer created by that call and reject stale finalization if a concurrent drop/recreate replaced it.
- **High-frequency index tracking in TieredStore**: HFI definitions are tracked and persisted with temporal index definitions so restarted stores, rotated hot shards, and lazily opened refArchive inherit them. `Clear()` must reset both tracking sets and remove their persisted metadata.
- **Shard rotation inherits indexes before topology changes**: `RotateHotShard` must install tracked temporal and high-frequency indexes on the newly opened shard before mutating the catalog or live topology. If inherited-index setup fails, close/remove the new shard and leave `hotShard`, `eventShards`, and the catalog unchanged.
- **Tracked index application is all-or-error**: `applyTrackedTemporalIndexes` must remember which temporal/HFI indexes it created on the target shard and drop those if a later tracked definition fails. Persisted tracking load must also roll back earlier shard creations if a later shard fails. Pre-existing shard indexes must be left intact.


---

## File Map (`pkg/graph/`)

`pkg/graph/` is a thin façade: the `Graph` type holds a `*core.Core` plus 16 unexported sub-API pointers exposed via nil-safe accessor methods (`g.Nodes().Add`, `g.Temporal().NodesAt`, …). All implementation lives in `pkg/graph/internal/core/`. Version history: `CHANGELOG.md`.

#### `pkg/graph/` (7 production files; tests alongside)

| File | Purpose |
|------|---------|
| `graph.go` | `Graph` thin façade: `core *core.Core` + 16 unexported sub-API pointers. Public methods: `New`, `Close`, `SetReplicationSource`, and 16 nil-safe accessor methods (`Nodes() *nodes.API`, etc.). Package-level `NewBatchBuilder`, `DecomposeNodeID`, `DecomposeRelID`. Plus `Config`, `ValidationLimits`, `IDComponents`, `ConstraintSet`, `QueryOpts`, `ShardDepth`, `DistanceMetric` type aliases re-exported. |
| `open.go` | Convenience constructors `Open(dir, opts…)` / `OpenInMemory(opts…)` plus the `Option` functions `WithSnowflakeNodeID`, `WithValidation`, `WithProfileSmall`/`WithProfileServer`/`WithProfileBulkLoad`. |
| `restore.go` | `RestoreInto(cfg, dir)` — validates a full+delta backup chain, then replays it into a fresh graph. |
| `column_scan.go` | `ScanNodeColumns` / `ScanRelColumns` / `ScanRelSegments` on `*Graph` plus the re-exported `ColumnBatch` / `RelColumnBatch` / `ColumnKind` types, column-kind constants and `ErrMixedNumericColumn`. |
| `subapi.go` | `TxAPI` and `BatchAPI` — sub-API accessors for `g.Tx` and `g.Batch`, kept in-package because they wrap `*GraphTx` / `*BatchBuilder` declared in `internal/core`. |
| `errors.go` | Public sentinel re-exports (the canonical consumer surface for `errors.Is`): store sentinels (including the replica/change-log set — `ErrCapabilityNotSupported`, `ErrPrimaryRegistryStale`, `ErrRegistryDiverged`, `ErrWireFormatVersionUnsupported`), vector-index sentinels, registry sentinels, IndexProvider sentinels, IO sentinels, and the `internal/core` graph-layer sentinels (including `ErrReadOnlyReplica`). Each alias points at its one canonical declaration (store / index / registry / io / core). |
| `doc.go` | Package documentation. |
| `subapi_smoke_test.go` | `TestSubAPISmoke` — exercises every sub-API accessor end-to-end (one of the package's tests). |

#### `pkg/graph/<types-package>/` (types-only public packages)

| Package | Purpose |
|---------|---------|
| `pkg/graph/store` | `Store` interface, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, `VectorIndexOptions`, `ChangeFeedCapability` + `ChangeRecord`/`ChangeTag`, `ReplicationSource` + `RegistrySnapshot`/`IDSlotLeaseRecord`, the `*Capability` interfaces (`capabilities.go` and siblings), the store sentinels (`errors.go` and siblings). |
| `pkg/graph/store/memory` | `memory.Store`, `memory.New()`. |
| `pkg/graph/store/badger` | `badger.Store`, `badger.Config`, `badger.New()`. |
| `pkg/graph/store/tiered` | `tiered.Store`, `tiered.Config`, `tiered.New()`, `MigrateFromBadger`, `EventShard`, `ShardInfo`, `VerifyResult`, `RepairResult`. |
| `pkg/graph/store/sharded` | `sharded.Store`, `sharded.Config`, `sharded.New()` — EXPERIMENTAL (ADR-0007) slot-topology backend. |
| `pkg/graph/ingest` | `ingest.API`, `Session`, `IngestOptions`, `SubmitToken` — the ADR-0006 write door behind `g.Ingest()`. |
| `pkg/graph/events` | `Event`, `EventType`, `EventPriority`, `EventBus`, `AsyncEventBus`, `BackpressureStrategy`, constructors, constants. |
| `pkg/graph/index` | `IndexProvider`, `Initializable`, `GraphReader`, `VectorHit`, sentinels. |
| `pkg/graph/temporal` | `GraphSnapshot`, `SnapshotDiff`, `NodeUpdate`, `RelUpdate`, `NodeSegment` / `RelSegment` (effective timeline), `TemporalConstraint`, `ConstraintSet`, sentinels. |
| `pkg/graph/ontology` | `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, class constants. |

### `pkg/types` files

| File | Purpose |
|---|---|
| `node.go` | Node (graph vertex, 96B) — `nodeID` wrapping `snowflake.ID`, labels as `labelToken`, properties, version, temporal, integrity, compact frozen metadata, store-assigned dense ordinal (`ordinal.go`) |
| `relationship.go` | Relationship (directed edge, 96B) — `relID`, `relTypeToken`, start/end as `nodeID`, properties, version, temporal, integrity, compact frozen metadata, store-assigned dense ordinal and its endpoints' ordinals |
| `compact.go` | `CompactFrozenCopy` (P7) — the compact frozen form stores cache: a first version's temporal + integrity metadata in one object (rel 96 B with the 32-byte hash as raw bytes or none when hashes are off, node 48 B), every accessor rebuilding the public structs exactly; any other shape keeps the ordinary objects |
| `propertyslice.go` | Sorted key-value store with binary search; recursive exact-type allowlist validation aligned with hash/copy/wire support; depth-limited to 32 levels; `[]float32` support |
| `shadow.go` | Constants for virtual read-only `tkg_*` properties |
| `temporal.go` | `Instant` type (Unix ms), `entityID`, `TemporalMetadata` struct (incl. `Retracted`, the retraction-tombstone marker, and `Void`, an excise's void piece; 104 B) |
| `mint_instant.go` | `NodeID.MintInstant()` / `RelID.MintInstant()` — the public mint instant of an ID (Unix ms, the derived valid-from of a row with `ValidFrom == 0`), forwarding to `pkg/internal/idlayout.MintInstantMillis`; 0 for a non-positive ID |
| `temporal_time.go` | `InstantFromTime`/`Instant.Time()` — lossless-to-the-millisecond `time.Time` <-> `Instant` conversion (UTC, truncated) |
| `temporal_value.go` | `TemporalValue` — a storage-typed temporal property value (KIND + canonical ISO-8601 rendering) so a query engine can round-trip temporal-ness through storage exactly, instead of guessing from a string's shape |
| `integrity.go` | `NodeIntegrity` / `RelIntegrity` — hash chain (`Hash`, `PrevHash`) |
| `property_hash.go` | Property hash type tags mirroring the persisted wire tags (`propertyHashType*` constants) — part of the integrity-hash byte layout; the per-type-tag property-value hashing switch (`appendPropertyValueHashBytes`, exported as `AppendPropertyValueHashBytes`, which `internal/integrity.appendPropertyValue` forwards to) |
| `allen.go` | Allen's 13 interval relations — `AllenRelation`, masked `AllenRelationSet`, `Relate()` (closed intervals, rejects any zero endpoint), `RelateOpen()` (treats `end==0` as +∞ for open version-chain intervals — backs the `NodesRelating` / `RelsRelating` doors), `Compose()`, `ComposeSets()`, composition table |
| `granularity.go` | `TimeGranularity` (8 levels), `TruncateInstant`, `RoundInstant`, `CeilInstant` — ISO 8601 week truncation |
| `recurrence.go` | `RecurrencePattern`, `RecurrenceFrequency`, `WeekdayMask`, `Interval` — strict calendar selector and millisecond offset validation + `Expand(from, to)` |
| `property_registry.go` | `RegisterPropertyStructType(v any) error` — validates `HashableValue` + `DeepCopier` at registration; `DeepCopier` interface (`DeepCopyValue() any`); `ErrTypeNotHashable`, `ErrTypeNotDeepCopyable` sentinels |
| `property_type_class.go` | `PropertyTypeClass` (Numeric/NaN/String/Bool/Other — the total classification rule for storable values; ±Inf is Numeric, NaN split out as unorderable) + `Node.ForEachPropertyTypeClass` / Relationship mirror (key + class only, never the value) — backs the exact type-class counters |
| `column_reader.go` | `NodeColumnReader` — random-access point-lookup interface over a cached columnar snapshot of one label's nodes (X5 DocValues), exposed across the store boundary so the internal column type does not leak; `Row` fills caller buffers by node ID, `Epoch` pairs with the mutation-epoch staleness re-check |
| `heapsize.go` | Approximate Go heap footprint estimation for cache byte budgets — O(properties), zero-allocation, conservative-cheap; unknown registered custom types fall back to a fixed pessimistic constant rather than walking user structs |

### `pkg/graph/internal/*` subpackages

| Package | Purpose |
|---|---|
| `internal/core` | `Core` type holding all unexported state and every graph-layer method body (one package: ~57K LOC of implementation, ~133K LOC of tests at v4.70.0). Temporal reads funnel through `chain_resolver.go` (+ `chain_supersession.go`, `effective_timeline.go`); `version_alloc.go` allocates every appended row's version; `history_presence.go` / `latest_stamps.go` back `HasHistory` / `LatestStamps`; `unique_hold.go` / `unique_cascade.go` hold and withdraw unique claims; `durable_commit.go` runs the `DurableCommit` flush. |
| `internal/snowflake` | `IDComponents`, `DecomposeID`, and `Epoch` / `Layout` re-exported from `pkg/internal/idlayout`. |
| `pkg/internal/idlayout` | (outside `pkg/graph`) The single home of the snowflake epoch and bit layout and `MintInstantMillis`, shared by `pkg/types` (`MintInstant`) and `pkg/graph/internal/**`. |
| `pkg/internal/hash32` | (outside `pkg/graph`) The single conversion between an integrity hash's 64-hex public form and its 32 raw bytes: compact frozen rows, row wire (binary hashes, backlog 61), segments. |
| `internal/storeutil` | Store-internal helpers: key encoding, msgpack wire types, pagination helpers, temporal-filter push-down, the shared as-of selection rule `SelectAsOfWithCurrent`. The public Store contract lives in `pkg/graph/store`. |
| `internal/locks` | 256-shard entity-lock `Manager` (`LockEntity`/`LockTwo`/`LockThree`/`LockMany` in ascending order) and the 256-stripe unique-value `ValueManager`. |
| `internal/registry` | `LabelRegistry`, `RelTypeRegistry`, `PropertyKeyRegistry`. Internal types — not part of public API. |
| `internal/index` | In-memory indexes only: property (incl. range / prefix), composite, rel-property, temporal interval, high-frequency, vector (HNSW + brute force), DocValues / columnar helpers, the property tx-membership sidecar (`property_tx_members.go`, `MergeFirstTx`). (`OntologyMapping` lives in `pkg/graph/ontology`.) |
| `internal/integrity` | Pure SHA-256 hash primitives — `ComputeNodeHash`, `ComputeRelHash`. Five fixed-vector anchors lock the on-disk hash format. |
| `internal/grapherr` | `ErrNilGraph` / `ErrNilCallback` + the `IsNil` typed-nil detection every sub-API `ready()` uses to fail closed. |
| `internal/apiutil` | Generic helpers shared by the sub-API wrapper packages (`CloneSlice`, `CloneMap`, `IterateForEach`) — de-duplicated from nodes/rels/index/tier/stats. |
| `internal/generatedcreate` | `Proof` / `FreshGraphID()` — the unforgeable internal token that marks a create as carrying a freshly minted graph ID, so the duplicate-check fast path cannot be reached from outside `pkg/graph`. |
| `internal/segment` | ADR-0011 column-segment codec (step S1): an immutable, versioned, CRC32C-checked column format for one declared relationship type, with ID index, out/in CSR and per-group SHA-256 integrity roots; the per-row hash is recomputed from the columns. Used by the memory store for declared relationship types (S2: in-RAM segments; other backends fail `New` with `ErrCapabilityNotSupported`). `Batch` is the one page decode behind both the row door (`Batch.Row`) and a columnar consumer; `Dicts` (`NodeDict`, `StringDict`) are store-level append-only dictionaries the in-RAM segments of one store share. With `SegmentDir` (S3) segments are memory-mapped and reference-counted (`memSeg.refs`: lock-free scans pin, the last holder unmaps). See "ADR-0011" below. |
| `internal/segdir` | ADR-0011 S3: the segment directory — `LOCK`, a versioned CRC-checked `MANIFEST`, immutable `seg-t<token>-<seq>.tkgs` files read memory-mapped, the seal crash protocol and `.tmp` cleanup (`Config.SegmentDir`). |
| `internal/synthhop` | (module root) Deterministic synthday-shaped workload for the ADR-0011 baseline (`bench/segment_baseline_test.go`) and scale tests; test support only. |

### `pkg/graph/<sub-api>/` packages

One package per accessor; table: "Sub-API Accessors" above.

---

## ADR-0011 — Column segments for bulk relationship types (decision record)

Accepted by René 2026-09-24; folded here from `docs/adr/0011-column-segments.md` (full text, including the
design-time rationale, Q1-Q10 and the sigma/ai-soc integration notes: `git show 238e2644:docs/adr/0011-column-segments.md`).
Code comments cite the section numbers below.

**Decision.** Rows of a *declared* relationship type (`Config.RelSegments`) move from the row store into
immutable, versioned, CRC32C-checked column segments (one type per segment), so resident memory stops growing
with the day. The row API stays a view over memtable + overlay + segments; `ScanRelSegments` is the bulk door.
Integrity block size is a setting (`IntegrityBlockRows`, default 64, a power of two 1-4,096, written in the
footer). Every step is gated by a measurement at the three synthday sizes (790 K, 3.15 M, 12.6 M rows).

**Status vs code (v4.70.0).** No segment step since v4.49.0.

| Step | What | State |
|---|---|---|
| S0, S1 | baseline harness (`bench/segment_baseline_test.go`); codec `internal/segment` | done (v4.37.0) |
| S2 | memory store seals declared types into in-RAM segments (union view, differential oracle) | done rho-tkg side (v4.39.0): 24.0 / 24.8 / 25.7 B/HOP beyond the memtable (gate ≤ 60); ai-soc parity gate open (`tasks/backlog.md` item 0) |
| S3 | `Config.SegmentDir`: files, manifest, crash recovery, mmap (`internal/segdir`) | done rho-tkg side (v4.40.0): heap growth 0.3-0.7 B/HOP (gate ≤ 1); BA day on Flux open |
| S4 | compaction, overlay folding, time partitions, cold tier | open |
| S5 | `ScanRelSegments`, `ScanRelColumns` from segment columns | done rho-tkg side (v4.39.0; ID order since v4.39.1) |
| S6 | `AppendColumns` bulk append + ingest routing | open |
| S7 | badger / tiered / sharded segments | open: only the memory store implements `store.RelSegmentCapability` / `RelSegmentDirCapability`; other backends fail `New` with `ErrCapabilityNotSupported` (`internal/core/rel_segments.go:36,81`) |

Facts the code relies on:

- **§2.2 Format.** Header with magic and format version; endpoint node table and out-CSR; column-major pages of
  4,096 rows (constant / FOR / plain per page); in-CSR, sorted ID index, SHA-256 group roots, footer with type
  name, declared schema, sorted dictionaries, per-column stats and page index. A string column whose distinct
  count exceeds a threshold is stored plain, not as a dictionary, so dictionaries never grow with the rows.
  Unknown versions fail closed; decode never allocates from an untrusted count.
- **§3.1 Seal.** The memtable is the row store's unsealed tail (same doors, locks, constraints, change-log).
  Seal: snapshot the type's unsealed rows; encode and verify (§4.3); write `.tmp`, fsync, rename, dir fsync; map
  and open; commit the manifest; install; remove the sealed rows from the row store without delete records.
  Files are `seg-t<token>-<seq>.tkgs`, self-contained. Open verifies every listed file fully, then deletes
  `.tmp` and unlisted files; a row the row store holds with the same ID, version and hash leaves it, another
  version shadows the sealed copy. On the memory store only the segments are durable.
- **§3.2 Mapped segments.** Read-only mmap; dictionaries decoded into the heap (no caller string points into a
  mapping); a segment is pinned by reference count and unmapped at zero.
- **§3.4 Compaction (S4, not built).** A merge never produces more than 64 M rows (`segment.MaxRows`).
- **§3.5 Budget.** `SegmentMemoryBudget` (default 256 MiB) bounds the memtable; over it the type seals, and if
  sealing cannot keep up, appends block.
- **§4.1 / §4.5 Overlay.** A sealed row is never rewritten: an update writes a new row, a write to a sealed row
  faults it back into the memtable and marks the sealed copy dead; every reader door resolves set-vs-delete per
  key across memtable, overlay and segments (lesson 57). Bitemporal predicates are the storeutil ones.
- **§4.3 Integrity.** The per-row hash is not stored: it is recomputed from the columns (ID, version, type name,
  endpoints, properties with their exact Go kind; a value of another kind goes to the fallback column with its
  wire tag). The witness is a SHA-256 root per `IntegrityBlockRows` group built from the rows' stored hashes at
  seal time; sealing refuses on any mismatch.
- **§4.6 Generated IDs.** A per-type maximum sealed ID makes the duplicate probe for a generated ID one compare.
- **§4.8 Replication.** A seal is a physical move, not a mutation: no change-log records; replicas seal on their
  own schedule.
- **§4.9 Declared schema.** Properties outside the declared columns go to the per-row fallback column.
- **§5.3 `ScanRelSegments`.** `(relType, props, fn) (ok, err)`, no `QueryOpts`: every current row of a declared
  type in ascending ID order (identical on every call), string columns as dictionary codes; `ok=false` when the
  type is undeclared, the backend has no segments, or a property is not a declared column.
- **§6** is the step table above; **§7**: a declared bulk type is never routed through the DocValues builder
  (`MaxDocValuesNodes` caps it, segments do not).
- **§8 Provenance.** Synthday sizes 790 K / 3.15 M / 12.6 M rows (107,113 / 408,282 / 1,584,150 HOP); analysis
  numbers from ai-soc `tasks/todo.md:23-29` heap profiles; `internal/synthhop` reproduces the workload shape.

---

## Deferred Architectural Decisions (2026-06-10 review)

Recorded so future readers know these are conscious choices, not oversights:

- **Core sub-packaging**: `internal/core` is one large package (size: File
  Map); the sub-Ops decomposition is
  namespacing, not separation. Splitting into `core/tx`, `core/temporal`,
  `core/mutate` is a multi-week, behavior-neutral restructure — planned as its
  own effort. The shared relationship-create kernel removed the most acute
  intra-core duplication.
- **`pkg/graph/hash` / `pkg/graph/io` renames**: both shadow stdlib package
  names and force `tkghash`/`tkgio` aliasing on consumers. Renaming is
  breaking — v5 item.
- **Iteration-capability matrix**: the history × deleted × depth × paged
  optional-interface grid grows combinatorially; a parameterized iteration
  interface is a breaking store-contract redesign — v5 item.
- **`tier` package returning `tiered.*` types** (forces the tiered import on
  consumers) and `RecoverBackgroundError` exposure via `g.Tier()`: both are
  additive-but-coupled API changes — bundle with the next planned API pass.
- **StoreStats interface extension** (pending-write/dirty counts): widening
  the type-asserted optional interface breaks out-of-tree implementers;
  pressure visibility shipped as `badger.Store.PendingWriteCount()` instead.
