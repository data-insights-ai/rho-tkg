# Architecture — tkg/v4 (v4.27.0)

Temporal Knowledge Graph v4 is a pure Go library providing the core graph engine for temporal knowledge graphs. It is the low-level storage and type layer — no main binary, no HTTP server, no query language.

```
Module:  github.com/data-insights-ai/rho-tkg/v4
Go:      1.26.1
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

**Store** is a pure persistence interface composed from capability sub-interfaces in `pkg/graph/store/capabilities.go`. The mandatory composition (`MandatoryStore` — Lifecycle, NodeCRUD, RelationshipCRUD, Adjacency, BulkRead, Batch, History, Stats, Iteration) is what the graph layer depends on. Optional capabilities are type-asserted at the call sites that need them; the list below is illustrative — `capabilities.go` declares ~48 of them, and it is the authority: DepthHistoryIteration, DeletedIteration, DepthDeletedIteration, PropertyIndex, TemporalIndex, VectorIndex, VectorIndexOptions, FilteredVectorSearch, HighFrequencyIndex, CompositePropertyIndex, RelPropertyIndex, MetaKV, ChangeFeed, TransactionTimeQuery, HistoryCompaction, RetentionPurge, PreEncodedPut, Degree, BeliefWatermark, TemporalCandidate, …. `DeletedIterationCapability` (and the depth variant) yields IDs with history rows but no current row; the graph layer uses it for the deleted-rel coverage fold in `g.Temporal().OutgoingRelsAt`/`IncomingRelsAt`/`NeighborsAt` so cost is O(deleted_count) instead of O(total history). Four in-tree implementations satisfy the full composition: `memory.Store` (testing), `badger.Store` (single-instance persistent), `tiered.Store` (multi-shard persistent), and `sharded.Store` (slot-topology persistent — EXPERIMENTAL, ADR-0007). Nil concrete in-tree Store receivers return `ErrNilStore` from lifecycle `Close` and `Clear` calls. The `memory.Store` zero value is usable; persistent Store zero values fail closed with `ErrStoreClosed`.

**Store registry inputs are lifecycle-checked.** BadgerStore and TieredStore registry save/load APIs reject nil label or relationship-type registry pointers with `ErrInvalidStoreMutation` before dereference on open stores. Closed stores still return `ErrStoreClosed` first. `tiered.Store.SetLabelRegistry(nil)` is a no-op so direct Store callers cannot accidentally clear ontology routing state. Tiered `registry.msgpack` loads validate both label and relationship-type slices before returning metadata to startup/load or deprecated single-registry save paths.

**Store indexed identity is enforced at mutation boundaries.** Current-row put/replace, delete/cascade, archive/restore, batch write/delete, and relationship endpoint writes reject zero or negative node IDs, relationship IDs, relationship endpoints, zero node-label tokens, and zero relationship type tokens with `ErrInvalidStoreMutation` before routing, endpoint lookup, or mutation. Graph update, label mutation, property CAS, close-version, delete, admin archive/restore, and tx/batch variants validate zero/negative target IDs before entity lookup, locks, snapshots, capability checks, or queueing, so invalid targets do not collapse into not-found results. Graph relationship create/import paths validate endpoint IDs before endpoint locks, duplicate probes, constraints, or relationship-type token allocation. Badger split relationship helpers enforce the same positive-ID index tuple before cross-shard entity/out or incoming-index writes and deletes, and repair-only incoming-index deletion and orphan-purge helpers reject invalid end-node or relationship IDs before scanning. Repair incoming-index cleanup uses the Badger orphan-index purge path, so local `inIdx`, `outIdx`, `typeIdx`, pending/persisted keys, `relIDs`, and counters are cleaned together once the relationship row is absent everywhere. Atomic history replacements validate nil current/history payloads before dereference or marshaling. Node replacements and node history replacements must keep the original label-token sequence; label changes use the dedicated label-token mutation APIs that update label indexes and tier routing. Plain Store `DeleteNode` and `DeleteNodesBatch` remove only unconnected node rows and return `ErrInvalidStoreMutation` without mutation if relationships are still attached. Tiered plain cascade skips stale adjacency entries whose relationship row is already missing and purges those local indexes during final shard-local node removal. Badger orphan relationship cleanup, tiered relationship routing, and repair shard probes check the relationship row itself, not only index-derived membership. Those label-token APIs also verify the supplied current row adds or removes exactly the requested token before touching label indexes, history, caches, or vector indexes. Relationship replacements, relationship history replacements, relationship delete tombstones, and relationship tombstones supplied to node cascade deletes must keep the original relationship ID, type token, start node, and end node. Node cascade tombstone sets must also cover every live connected relationship exactly once and must not include unrelated relationships. The Store returns `ErrInvalidStoreMutation` before touching current rows, history, or indexes if those fields change. History snapshot payload IDs must also match their storage keys.

**Explicit adjacency targets are all-or-error.** `OutgoingRelationships`, `IncomingRelationships`, and the batched `ForNodes` variants validate that every requested node row exists before using adjacency indexes as result sources. Existing nodes with no matching relationships return empty results, but missing requested nodes return `ErrNodeNotFound` rather than being collapsed into an empty neighborhood or omitted from a partial batch map. `g.Temporal().NeighborsAt` is the history-aware wrapper: it validates target existence at the queried instant first, then treats current-adjacency `ErrNodeNotFound` as no current candidates so relationship history can still answer for deleted target nodes.

**History read targets are explicit IDs.** Entity-specific history reads and truncation (`Get*Version`, `Get*History`, `Truncate*History`) reject zero or negative node/relationship IDs with `ErrInvalidStoreMutation` before version-not-found, empty-history, or no-op retention handling. Graph `g.Nodes().History` and `g.Rels().History` enforce the same positive-ID boundary before delegating to the Store.

**Store index definitions require real targets.** Store-level property, temporal, high-frequency, and vector index create/drop/search operations reject label token 0 with `ErrInvalidStoreMutation`. Property/vector index definitions, graph-level stored-property queries (`NodesByLabelAndProperty`, `NodesByLabelProperty*`, `RelsByTypeProperty*`), and temporal label lookups reject malformed label/type/property targets before treating them as empty namespaces. Property/vector queries reject reserved `tkg_` property keys with `types.ErrReservedPrefix`, because shadow properties are graph-resolved metadata and are not stored in `PropertySlice`. Query methods validate limits, cursors, and active temporal intervals before empty namespace shortcuts; negative `Limit` returns `ErrInvalidQueryLimit`, negative `After` returns `ErrInvalidQueryCursor`, and `ValidStart >= ValidEnd` returns `ErrInvalidTimeRange` instead of widening to all data or matching open-ended entities. BadgerStore and TieredStore also validate persisted index definitions on open, so a durable token-0 or shadow-key definition cannot become an active empty namespace after restart. BadgerStore fails open-time loading if persisted property, temporal, high-frequency, or vector index-definition MsgPack cannot be decoded. MemoryStore and BadgerStore public read/write/history/index APIs return `ErrStoreClosed` after close rather than serving stale in-memory state; Badger no-error diagnostics return zero values, and MemoryStore exported test-only tampering helpers return no data or no-op. TieredStore index create/drop/search calls, direct read/count/bulk/history APIs, empty batch/history write edges, and public admin/metadata operations also fail with `ErrStoreClosed` after close starts before touching shard handles, store-level vector metadata, reference-shard read state, catalog metadata, or registry files. Vector search paths re-check close state after in-memory vector scans/filter callbacks before returning empty or raw-ID results.

**Tiered temporal index kind is a store-wide invariant.** `tiered.Store` serializes temporal and high-frequency index create/drop against shard rotation, preflights shard-local index kind before mutating, persists store-level temporal/HFI tracking metadata, and treats `ErrTemporalIndexExists` as retryable only when the existing shard index is the same kind and HFI bucket size. HFI bucket sizes must be positive whole milliseconds because buckets are stored in `Instant` precision. A partial same-kind create can be completed; a partial interval/HFI cross-kind or different-bucket HFI leftover fails closed instead of creating mixed per-shard temporal index state. If a later shard fails during create, drop, tracking-file persistence, tracked-index application to a newly opened shard, or persisted tracking replay on open, earlier shard-local changes are undone before returning the error.

**Vector indexes** live at the Store level (per-Store in `memory.Store`/`badger.Store`, per-`tiered.Store` -- not per-shard). In-memory k-NN with Cosine and Euclidean distance metrics. Two engines: `CreateVectorIndex` defaults to the approximate HNSW engine (`pkg/graph/internal/index/hnsw.go`, `M=16`, `EfConstruction=200`, `EfSearch=64`), and `CreateVectorIndexWithOptions` (`VectorIndexOptionsCapability`) selects the tuning or switches to the exact linear-scan engine via `VectorIndexOptions.UseBruteForce` — the escape hatch for exact-recall requirements. Brute force also remains the correctness fallback for filtered searches. Dimensions must be positive and metrics must be one of the supported values before any placeholder or persisted definition is accepted. Protected by a dedicated `vectorIdxMu sync.RWMutex` in tiered stores and the store index mutex in single-shard stores. BadgerStore and TieredStore persist index definitions and rebuild entries from node properties on open; MemoryStore remains process-local. Definition serializers skip phased-create placeholders until backfill finalization clears their build marker, so concurrent metadata writes cannot durably publish unfinished indexes. Tiered vector definition create/drop snapshots the raw definition file before persisting, restores/removes it on write failure, and fsyncs the metadata directory after deleting the last definition. Tiered catalog saves also snapshot and restore/remove the raw JSON file on write failure, matching caller-level in-memory catalog rollback. Auto-maintained across all mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`), including delete paths that mutate state and return a corruption error after cleanup. Creation installs a visible placeholder, scans existing nodes, then merges backfill while skipping IDs touched by concurrent mutations; scan failures remove the placeholder. Search applies Store-level temporal/depth eligibility before heap selection, resolves ranked IDs back to nodes, applies cursor pagination over distance order, and skips only `ErrNodeNotFound` from concurrent deletes; other read errors propagate. Heap allocation is capped by indexed entry count, so oversized `k` values do not allocate proportional memory on tiny indexes. Tiered vector search applies `DepthHot`/`DepthWarm` before heap selection by combining reference/archive residency with event-shard tier snapshots.

**Internal no-error index helpers fail closed on nil or zero values.** Property indexes lazily initialize value maps on first `Add`; high-frequency indexes lazily initialize buckets on first `Add`; zero-value LRU caches lazily initialize as minimum-capacity caches. Nil property, temporal, high-frequency, and vector cleanup/read helpers no-op or return zero/nil values instead of panicking. Nil vector writes/searches return `ErrInvalidVectorIndexConfig` because they have an error channel.

**Tiered primary-label class is immutable.** Any TieredStore write that replaces a node or changes label tokens must preserve the node's reference/event class. `ErrPrimaryLabelClassMutation` prevents live rows and history snapshots from being routed to different shard families.

---

## Core Types (`pkg/types`)

| Type | Size | Purpose |
|------|------|---------|
| `Node` | 80B | Graph vertex. `nodeID` (wraps `snowflake.ID`), primary + extra labels as `labelToken`, properties, `uint32` version, temporal, integrity |
| `Relationship` | 72B | Directed edge. `relID` (wraps `snowflake.ID`), `relTypeToken`, start/end as `nodeID`, properties, `uint32` version, temporal, integrity |
| `PropertySlice` | var | Sorted key-value store with binary search. Recursive exact-type allowlist validation aligned with hash/copy/wire support, depth-limited to 32 levels. `tkg_` prefix reserved. `Set` and entity `SetProperty` methods deep-copy accepted reference values and reject invalid post-copy custom values; `SetProperties` methods canonicalize and deep-copy direct slices before installing them |
| `Instant` | 8B | Unix-millisecond timestamp. All temporal fields use this type |
| `TemporalMetadata` | var | ValidFrom, ValidTo, TxFrom, TxTo, CreatedAt, UpdatedAt, DeletedAt, CreatedBy, UpdatedBy, BaseEntityID |
| `NodeIntegrity` / `RelIntegrity` | var | SHA-256 hash chain + provenance/authorization: `Hash`, `PrevHash`, `AuthorID string`, `Signature []byte`, `AuthorizedBy string`, `AuthorizationLevel uint8`. `RelIntegrity` additionally holds `FromNodeHash`, `ToNodeHash` (endpoint cross-references at write time) |

**Key invariants:**
- All struct fields unexported. Access through methods only.
- Packed by descending field alignment (verified with `unsafe.Sizeof`).
- `ExtraLabelTokens()`, `Properties()`, `DeepCopy()`, `ToMap()` always return independent copies; `NewPropertySlice` and single-property setters also copy accepted reference values before storage and validate registered custom values after `DeepCopyValue`.
- `Temporal()` and `Integrity()` return internal pointers (no copy).
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
| `tkg_tx_from` | `Instant` | Both | Temporal — transaction time (TX), stamped by the system on every Add; caller-settable on CREATE doors only under `Config.AllowTxBackfill` (backfill, §4.1) |
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

Caller-supplied provenance/authorization shadow inputs are validated before they are stripped from the property map: `tkg_author_id` and `tkg_authorized_by` require `string`, `tkg_signature` requires `[]byte`, and `tkg_auth_level` requires a bounded numeric tier with an integer value.

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
- Creation timestamp extractable via `snowflakepkg.Layout.Decompose(id).Time` or `DecomposeID(id).CreatedAt`

---

## Graph Layer (`pkg/graph`)

### Entity Management

| Method | Locks | Description |
|--------|-------|-------------|
| `g.Nodes().Add(ctx, labels, props)` | `g.mu.RLock()` | Validate, generate ID, compute hash (genesis), store |
| `g.Rels().Add(ctx, type, start, end, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Validate endpoints exist; reject self-loops when `AllowSelfLoops=false` (`ErrSelfLoop`); generate ID, hash, store |
| `g.Nodes().Update(ctx, id, updates)` | `g.mu.RLock()` + `LockEntity(id)` | Validate update map, deep-copy pre-mutation, apply updates, checked version bump, `ReplaceNodeWithHistory` |
| `g.Rels().Update(ctx, id, updates)` | `g.mu.RLock()` + `LockMany(relID, startID, endID)` | Validate update map, deep-copy pre-mutation, apply updates, checked version bump. Locks both endpoints (in addition to the rel itself) so the FromNodeHash/ToNodeHash refresh cannot interleave with a concurrent endpoint node update (R4-F7) |
| `g.Nodes().Delete(ctx, id)` | `LockMany(node + all rels)` | Two-phase TOCTOU: read adjacency, lock all, re-read node and adjacency, build tombstones from the locked Phase B rows, single atomic `DeleteNodeWithHistory` call |
| `g.Rels().Delete(ctx, id)` | `LockEntity(id)` | Build tombstone, single atomic `DeleteRelWithHistory` call |
| `g.Rels().AddByID(ctx, type, startID, endID, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Create relationship using endpoint snowflake IDs directly. Live endpoints are fetched under the endpoint lock, `FromNodeHash`/`ToNodeHash` are captured, and graph-level constraints are enforced with the same semantics as `g.Rels().Add` |
| `g.Rels().AddByIDIfAbsent(ctx, type, startID, endID, props)` | `g.mu.RLock()` + `LockTwo(start, end)` | Atomic check-then-create: returns existing relationship if same type+endpoints already connected, otherwise creates. Same constraint behaviour as `AddByID`. Returns `(rel, created, err)` |
| `g.Nodes().Get(ctx, id)` | none (read-only via store) | Retrieve a single node by snowflake ID |

All mutations enforce `ValidationLimits` (5 configurable limits with defaults). Update-style paths extract provenance/authorization shadow keys, reject other reserved `tkg_` keys, and validate update values before entity locks, transaction rollback snapshots, or batch queueing. They also recheck final property count after nil deletes/adds/sets are applied and before persistence. Versioned mutations use a checked next-version helper and return `ErrVersionOverflow` at `math.MaxUint32` before history writes, wrapped version `0`, or label-token allocation for rejected add-label calls; version-chain successor lookup also treats `math.MaxUint32` as having no successor instead of wrapping to genesis, and version-chain navigation validates explicit IDs before returning `nil, nil` for a missing neighbor version. Batch update queues deep-copy caller maps after validation so later caller mutations cannot change `Execute`. Add-label paths check node existence, idempotence, and `MaxLabelsPerNode` before creating a token for an unseen label. Node create/import/add-label write failures restore any newly allocated label tokens, and multi-label allocation failures restore partial suffixes before returning. Batch node queueing uses non-zero probe tokens for unseen labels and allocates real tokens only during `Execute`, retokenizing returned node pointers in place before persistence. Batch relationship create failures restore queue-time `TxFrom`, endpoint hashes, and type-token state on the returned relationship pointer. Direct relationship create paths run temporal constraints before allocating a token for an unseen relationship type and restore newly allocated type tokens on final write failure. `CloseVersion`, node label add/remove, node/relationship property CAS, and node/relationship in-place updates recompute hash-chain fields while preserving existing provenance, signature, authorization, and relationship endpoint-hash metadata because those APIs have no provenance shadow-key channel. Registry rollback windows release their allocation mutex via `defer`, so backend panics do not strand future registry writes. Every mutation door is ctx-first and adds cancellation checks at critical points — v4.0 collapsed the old `*WithContext` / non-context pairs into single `ctx`-taking methods.

### Sub-API Accessors (v3.4.0 introduced; v4.2.0 converted to methods)

The 130+ implementation methods on `*core.Core` are reachable through 16 sub-API accessor methods on `*Graph`. The thin `*Graph` façade itself (in `pkg/graph/graph.go`) only exposes `New`, `Close`, `SetReplicationSource`, plus the 16 accessor methods listed below (the package-level `Open`, `OpenInMemory`, `RestoreInto`, `NewBatchBuilder`, `DecomposeNodeID` and `DecomposeRelID` helpers live alongside it); the old form `g.AddNode(...)` was removed in v3.4.0, and the supported public form is `g.Nodes().Add(...)`. The earlier `Graph.Core()` escape hatch was removed during the post-v3.4.0 cleanup; `*core.Core` is again strictly internal. Until v4.2.0 these accessors were exported fields (`g.Nodes` etc.); v4.2.0 converted them to nil-safe methods so `(*Graph)(nil).Nodes()` returns nil and chained calls fail closed with `ErrNilGraph`.

| Field | Package | Wraps |
|-------|---------|-------|
| `g.Nodes` | `pkg/graph/nodes` | Node CRUD + label/property/version helpers |
| `g.Rels` | `pkg/graph/rels` | Relationship CRUD + adjacency/property/version helpers |
| `g.Temporal` | `pkg/graph/temporal` | Point-in-time, interval, bitemporal, snapshot/diff, Allen relations |
| `g.Index` | `pkg/graph/index` | Property/vector/high-frequency index management + IndexProvider |
| `g.Events` | `pkg/graph/events` | Sync/async EventBus install + retrieval; setters return `ErrGraphClosed` after graph close |
| `g.Constraints` | `pkg/graph/constraints` | Temporal-constraint set management (`Set`/`Add`/`Get`, `DryRunValidate`); `Add`/`Set` reject unknown kinds before changing the set, and relationship writes fail closed with `ErrInvalidTemporalConstraint` if an invalid kind reaches enforcement. Also owns the unique-property constraints (ADR-0002): `CreateUnique`, `CreateUniqueForever`, `ReleaseOwnership`, `DropUnique`, `UniqueConstraints` — six `ErrUnique*` sentinels re-exported from `pkg/graph` |
| `g.IO` | `pkg/graph/io` | Export / Import (shadows stdlib `io` — alias as `tkgio` if both are imported) |
| `g.Admin` | `pkg/graph/admin` | Backend-agnostic admin: `Reset` (gated by `Config.AllowReset`, else `ErrResetDisabled`), `DecomposeNodeID`, `DecomposeRelID`, `CompactHistoryNodes` / `CompactHistoryRels` (ADR-0001 history retention), `PurgeExpiredNodes` (ADR-0008 R2 hard purge, gated by `Config.AllowRetentionPurge`, else `ErrRetentionPurgeDisabled`) |
| `g.Tier` | `pkg/graph/tier` | Tiered-store admin: `Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `Repair`, `VerifyShard` (reuses `core.AdminOps`) |
| `g.Stats` | `pkg/graph/stats` | Count helpers |
| `g.Hash` | `pkg/graph/hash` | Hash-chain verification (shadows stdlib `hash` — alias as `tkghash` if both are imported) |
| `g.Replication` | `pkg/graph/replication` | Change-log / op-log: `ChangeFeed`, `ForEachChange`, `LastCommittedLSN`; replica apply (`ApplyChange`/`ApplyChanges`, `AppliedLSN`/`SetAppliedLSN`), `RegistrySnapshot`, `IDSlotLease`/`SetIDSlotLease` |
| `g.Ingest` | `pkg/graph/ingest` | ADR-0006 prepare-parallel / apply-sequential write door: `NewSession`, `AppliedSeq`, `WaitApplied` |
| `g.Resolve` | `pkg/graph/resolve` | Shadow-property accessors (`NodeProperty`, `RelProperty`) |
| `g.Tx` | `pkg/graph/subapi.go` (in-package `TxAPI`) | `Begin()` / `Run(fn)` / `RunContext(ctx, fn)` |
| `g.Batch` | `pkg/graph/subapi.go` (in-package `BatchAPI`) | `New()` returns a `*BatchBuilder` |

Each sub-API package declares its own local `Core` interface listing only the methods it forwards to. `*core.Core` satisfies each `Core` implicitly (no explicit declaration required), which lets sub-API packages stay independent of `pkg/graph/internal/core` while still type-checking the wiring at compile time. Wrappers are 1-2 lines, single indirect dispatch each. Nil, zero-value, or typed-nil wrappers fail closed: error-returning methods return `ErrNilGraph`, while no-error helpers return zero values. The `temporal`, `index`, and `events` sub-APIs were collapsed into their existing types-only sibling packages in v3.4.0 post-cleanup (previously `pkg/graph/{temporalapi,indexapi,eventsapi}`).

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
| `tkg_valid_from` | "the fact holds **in the world** from T" | only the domain — a recorder/curator with actual knowledge | `0` = no world-time claim made |

Two doors expose two deliberate views of valid-time. The shadow resolver (`g.Resolve().NodeProperty(n, "tkg_valid_from")`) returns the RAW asserted value — `(Instant(0), ok=true)` when never asserted (`ok` is true because `TemporalMetadata` always exists; check the zero value, not `ok`). Temporal queries use the EFFECTIVE valid-from (explicit `ValidFrom`, else snowflake ID timestamp), so an entity with unset `ValidFrom` is "eternal" through the shadow door but time-bounded through the query door. Shadow props report *stored/asserted* state; temporal queries report *effective* state. Writers must never default `tkg_valid_from := now()` without domain knowledge — that conflates TX with VT; consumers wanting "unstamped ⇒ valid since recorded" should implement it as an explicit, flagged heuristic on their side.

**Canonical temporal predicates.** The effective-valid-from derivation and the point/interval predicates are defined ONCE, in `storeutil` (`EntityValidFrom`, `MatchesPointInTime`, `MatchesInterval`); the core graph layer delegates its `nodeValidFrom`/`relValidFrom`/validity helpers there and the store backends use the same functions for query push-down. Never redefine these semantics elsewhere — the cross-door equivalence test (`TestTemporalTwoDoorsAgreeOnLabelQueries`) asserts the named door, the generic `QueryOpts` door, and the per-ID resolver return identical sets.

**Valid-time inheritance is cleared on every version boundary.** Every mutation that creates a new version by deep-copying the current one (Update, AddLabel/RemoveLabel, property mutations) clears the inherited `ValidFrom`/`ValidTo` before stamping, so `ValidFrom != 0` on a non-genesis version always means "caller-supplied" (lessons 33 and 42). Delete tombstones and `CloseVersion` set `ValidTo` deliberately — closing semantics, not inheritance.

**Cascade interval edits (`SetNodeVersionInterval`) are not wire-atomic — by design.** The cascade classifies overlapping history rows (keep/close/open/eclipse/split) and rewrites them sequentially under the entity lock. A crash mid-cascade leaves a partially rewritten timeline, which is tolerated because (a) each row write is itself atomic, (b) eclipsed rows (`ValidTo == ValidFrom+1`) are unconditionally invisible to the resolver, and (c) the new current row is installed last, atomically. There is no in-progress marker; do not "fix" the missing atomicity without accounting for these three invariants.

**TX visibility: superseded is not retracted (lesson 43).** `QueryOpts.TxAt` / `NodeAtTx` filter the version chain to versions RECORDED by txAt (`TxFrom <= txAt`). `TxTo` deliberately does not bound visibility: it marks when a version stopped being the current record, and the row remains the authority for its valid-time slot in every later belief state. (The previous `< TxTo` clause treated every supersession as a retraction, so `NodeAtTx(oldVT, now)` returned nothing after any update — including the flagship explicit-VT tiling scenario.) Belief reconstruction falls out of the vEnd derivation over the TxFrom-filtered chain: versions recorded after txAt are absent, so the then-latest version is open-ended exactly as believed at txAt.

**Bitemporal back-compat shims and their sunset.** Two shims remain for pre-4.3.0 data: the inherited-valid-from detector (`nodeInheritedValidFrom`, bypassed once the post-open migration has run — `bitemporalMigrated`) and the `UpdatedAt`-as-`vEnd` fallback for rows without explicit valid-time. Retirement condition: the detector can be deleted once every supported backend requires `MetaKVCapability` (so the migration always runs) — revisit at the next major version. The `UpdatedAt` fallback is permanent API surface: it is what gives unstamped entities TX-as-VT semantics.

All of the following are methods on `g.Temporal()` (`pkg/graph/temporal`):

Point-in-time: `NodesAt(t)`, `RelsAt(t)`, `NodesByLabelAt(label, t)`, `RelsByTypeAt(relType, t)`
Interval: `NodesDuring(start, end)`, `RelsDuring(start, end)`
Version-specific: `NodeAt(id, t)`, `RelAt(id, t)`
Traversal: `NeighborsAt(nodeID, t)`, `OutgoingRelsAt(nodeID, t)`, `IncomingRelsAt(nodeID, t)`
Snapshot: `Snapshot(t)` -- full graph state at time t (endpoint-filtered)
Combined: `NodesByLabelPropertyAt(label, key, value, t)`, `NodesByLabelPropertyDuring(label, key, value, start, end)`
Bitemporal (transaction time): `NodeAsOf(id, txTime)`, `RelAsOf(id, txTime)`, `NodesAsOf(txTime)`, `RelsAsOf(txTime)`, `NodeAtTx(id, validAt, txAt)`, `NodesAtTx(validAt, txAt)`, `NodesDuringTx(from, to, txAt)` (+ rel mirrors)

**History-aware queries** include deleted entities. They use two-phase ForEach iteration:
1. **Collect** -- `ForEachNodeID` + `ForEachNodeHistoryID` insert unique IDs into a `seen` map. Store implementations invoke callbacks outside their internal locks and Tiered shard checkouts, so callbacks may re-enter Store methods.
2. **Process** -- iterate `seen` map after ForEach returns.

This replaced the pre-v3.0.31 approach of materializing all IDs into slices (`allKnownNodeIDs`/`mergeIDSlices`), reducing peak memory by ~83% for 10M nodes.

`DiffCallback` applies the same lock-boundary rule at the graph layer: it collects known IDs and resolves each before/after entity pair under short `g.mu.RLock()` windows, then invokes caller `DiffHandlers` outside the graph lock. Handlers may call graph read APIs without deadlocking behind a waiting writer.

**Temporal semantics:**
- `effectiveValidFrom` derives from explicit `ValidFrom` (if set) or snowflake ID timestamp (always available)
- Point-in-time match: `effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t)`
- Interval overlap: `effectiveValidFrom < end AND (ValidTo == 0 OR ValidTo > start)`
- Explicit finite ranges are half-open and non-empty: when both `ValidFrom` and `ValidTo` are set, `ValidFrom < ValidTo` is required at graph input, durable wire, and direct Store write boundaries.

### Batch Operations

`BatchBuilder` -- fluent API with eager validation and deferred persistence. Queue methods are serialized by the builder, and update queues use the same update-map validation and provenance shadow-key extraction as standalone updates. Empty queued updates read at execute time so missing entities still fail, but successful empty updates do not count as `Updated` and do not publish update events. Successful batch creates increment graph add stats in the same units reported by `BatchResult.Created`. New relationship-type tokens are deferred until execute-time endpoint and temporal constraint checks pass; the queued relationship pointer is retokenized in place before persistence, and a final relationship write failure restores any newly allocated type token. Execute order: create nodes, create rels, update nodes, update rels, delete rels, delete nodes. Mutation events are buffered during replay and emitted after graph and builder locks are released via one `Publisher.PublishBatch` call, so async priority ordering is applied across the whole batch result. Returns `BatchResult` with counts and per-operation errors; any operation failure also makes `Execute` return an error wrapping `ErrBatchFailed`. Execute is one-shot and marks the builder done when replay starts; later queue calls or repeat Execute calls return `ErrBatchDone`.

`CloseVersion` updates temporal metadata in-place without adding history. It is still a successful mutation write when it changes a node or relationship, so it increments `NodesUpdated`/`RelsUpdated` and publishes the corresponding update event. `t == 0` returns `ErrInvalidTimeRange`, and already-closed entities return `ErrAlreadyClosed` without changing update counters.

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

`GraphTx` -- full CRUD transaction holding the graph write lock (`mu.Lock`). Supports `GetNode`, `GetRelationship`, `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, node label/property helpers, `DeleteNode` (cascade), and `DeleteRelationship`. Update methods validate update maps before reading rollback snapshots, so malformed input is reported before missing-entity snapshot errors. First-mutation snapshots are keyed by entity kind plus snowflake ID so a node and relationship with the same caller-supplied underlying value roll back independently. `Commit()` releases the lock and publishes buffered events. `Rollback()` restores all deleted node rows before restoring deleted relationships, restores pre-transaction version history, reverts node label changes by exact one-token Store writes until the pre-transaction label sequence is restored, reverts other updates, deletes created entities, restores label and relationship-type registries to their `BeginTx` snapshots, then restores the `BeginTx` operation-counter snapshot. Relationship recreation always sees live endpoints, rolled-back history rows do not survive, restored label indexes match restored node labels, tokens created inside a rolled-back transaction do not leak, and discarded writes or reads do not leak through `g.Stats().Get()`. Committed transaction reads increment the same read counters as standalone reads. `TxAPI.Run` / `RunContext` reject nil callbacks before opening a transaction, and `RunContext` rejects nil contexts before dereference. Not suitable for long-running operations.

### Transaction and Mutation Isolation

`Graph.mu` (`sync.RWMutex`) serializes **writes** against tx/batch and protects long snapshot-style scans. All exported mutation methods (`g.Nodes().Add`/`Update`/`Delete`/`AddLabel`/`RemoveLabel`/`CloseVersion`, `g.Rels().Add`/`AddByID`/`Update`/`Delete`/`CloseVersion`, …) acquire `g.mu.RLock()` at entry. `BeginTx`, `BatchBuilder.Execute`, `IO.Export`, `Temporal.Snapshot`, and `Admin.VerifyShard` acquire `g.mu.Lock()`, blocking standalone mutations. `GraphTx` and `BatchBuilder` call unexported `*Internal` variants (lock-free) directly under `g.mu.Lock()`.

Most point and query reads acquire `g.mu.RLock()`: they are blocked while a tx/batch or snapshot-style write-lock scan is active, but they can run concurrently with standalone mutations that also hold `RLock()`. No-error resolver helpers also acquire `g.mu.RLock()` before reading registry pointers and return zero values after graph close is visible; internal mutation/hash code that already holds the graph lock uses explicit lock-free resolver helpers to avoid recursive read locks. `IO.Export`, `Temporal.Snapshot`, and `Admin.VerifyShard` take the write lock because they compose multiple store reads and must not observe a graph changing mid-scan. `IO.Export` rejects nil and typed nil writers before taking the write lock. `IO.ImportWithOptions` rejects nil and typed nil readers before creating a staging file, stages reader I/O before the lock, validates wire invariants before entity construction, then replays under the write lock with rollback snapshots for touched current rows, history rows, and registries.

### Hash Chain Integrity

`ComputeNodeHash(n, labels)` / `ComputeRelHash(r, typeName)` -- SHA-256 via typed binary serialization with sorted map keys. Genesis: `PrevHash=""`. Updates: `PrevHash=previous.Hash`. `g.Hash().VerifyNodeChain(id)` / `g.Hash().VerifyRelChain(id)` verify the full chain (handles deleted entities and truncated history).

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
    TxAt            types.Instant  // Bitemporal: restrict chain to TxFrom <= TxAt (0 = no TX filter)
    TxPin           types.Instant  // Belief state: pure knowledge-time resolution, NO valid-time filter (0 = disabled)
    IncludeEclipsed bool           // Include cascade-superseded history rows (reserved; default false)
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
`ValidAt != 0` activates point-in-time filtering. `ValidStart`/`ValidEnd`
activate interval filtering only when both bounds are greater than zero; a
non-positive bound is treated as no interval filter.

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

**Graph-layer sentinel errors** (not in Store interface unless noted): `ErrSelfLoop` — returned by the shared relationship-create kernel (`g.Rels().Add` / `AddByID` / `AddByIDIfAbsent`, the batch queue) and the import paths (`g.Rels().Import`, `GraphTx.ImportRelationshipWithID`) when `startID == endID && !g.validation.AllowSelfLoops`; `ErrInvalidID` — returned by import-by-ID APIs for negative caller-supplied IDs; `ErrVersionOverflow` — returned by versioned mutations when the current entity version is already `math.MaxUint32`; `ErrNilNode` and `ErrNilRelationship` — aliases of the type-layer nil entity sentinels, returned by graph methods and entity methods with error channels; `ErrNilGraph` — returned by nil, zero-value, or typed-nil graph façade and sub-API entry points with error returns; `ErrNilContext` and `ErrNilTxCallback` — returned by public context and transaction-helper boundary checks; `ErrNilReader` and `ErrNilWriter` — returned by IO import/export boundary checks; `ErrNilStore` — aliases the Store-layer sentinel returned by `graph.New` for typed nil `Config.Store` values and by nil concrete in-tree Store lifecycle receivers; `ErrReadOnlyReplica` — returned by the core-layer `checkWritable()` gate on every user mutation door (and `Tx().Begin` / `Batch.Execute` / `Admin().Reset`) when the graph was opened with `Config.ReadOnlyReplica` (reads, bootstrap import, and `ApplyChange` stay open).

---

## memory.Store (`pkg/graph/store/memory/memorystore.go`)

Thread-safe in-memory Store. Single `sync.RWMutex` protects all maps.

```
nodes        map[types.NodeID]*types.Node
rels         map[types.RelID]*types.Relationship
labelIdx     map[uint16]map[types.NodeID]struct{}        // labelToken -> node IDs
typeIdx      map[uint16]map[types.RelID]struct{}         // relTypeToken -> rel IDs
outIdx       map[types.NodeID]map[types.RelID]struct{}   // startNodeID -> rel IDs
inIdx        map[types.NodeID]map[types.RelID]struct{}   // endNodeID -> rel IDs
nodeHistory  map[types.NodeID]map[uint32]*types.Node
relHistory   map[types.RelID]map[uint32]*types.Relationship
```

- O(1) per-label/per-type counts via `len(labelIdx[token])`
- Hash-set adjacency indexes for O(1) insert/delete
- Deep-copy at store boundary (both Put and Get)
- Temporal push-down: filters in-memory entity pointers without deep-copy
- ForEach: snapshot IDs under RLock, release the lock, then invoke callbacks
- The zero value is a usable empty store; maps initialize lazily at the lifecycle gate.
- `Clear()` reinitializes all maps. `Close()` marks the store closed; public operations return `ErrStoreClosed` after close, and test-only tampering helpers are inert.

---

## badger.Store (`pkg/graph/store/badger/`)

The implementation is split across ~54 themed files; the principal ones:

- `badgerstore.go` — Store struct, sentinel-error aliases, `New`, `Close`, `Clear`, `loadIndexes`.
- `badgerstore_node.go` — node CRUD (`PutNode`, `GetNode`, `DeleteNode`, `ReplaceNode`, label-token mutations) plus node queries and node-batch ops.
- `badgerstore_rel.go` — relationship CRUD plus rel queries (`Outgoing*`, `Incoming*`) and rel-batch ops.
- `badgerstore_index.go` — property / temporal / high-frequency / vector index management.
- `badgerstore_history.go` (+ `badgerstore_history_node.go` / `badgerstore_history_rel.go`) — version history methods, including the cursor-paginated `AllNodeHistoryIDsFrom` / `AllRelHistoryIDsFrom`.
- `badgerstore_temporal.go` — temporal-filter helpers (`filter*ByTemporalPeek`, `fetch*WithTemporalFilter`).
- `badgerstore_meta.go` — counts, registry persistence, cache hit/miss accessors.
- `badgerstore_flush.go` — async write batch + flush loop + dirty tracking + write-pressure backpressure.
- `badgerstore_format.go` — on-disk wire-format version marker (verify/stamp at open).
- `badgerstore_changelog.go`, `badgerstore_composite_index.go`, `badgerstore_docvalues.go`, `badgerstore_property_disk.go` (0x0A), `badgerstore_temporal_disk.go` (0x0B), `badgerstore_retention_purge.go`, `badgerstore_history_delta.go` — the later capability layers.


Persistent Store using Badger v4 with async batch persistence.

### On-Disk Format Versioning

Two layers, both backward compatible with pre-versioning directories:

- **Per-row version**: `NodeWire`/`RelWire` carry `FormatVersion` (`fv`).
  Absent (legacy rows) decodes as 0 and is treated as version 1. A checked
  decode of a row with a version newer than
  `storeutil.CurrentWireFormatVersion` fails closed with
  `store.ErrWireFormatVersionUnsupported` — never zero-filled misdecoding.
- **Store-level marker**: the meta key `wire_format_version` is verified at
  open BEFORE any row is decoded. Newer marker → `New()` fails closed with
  the same sentinel; absent → stamped (read-write opens only); lower →
  raised. A present-but-unparsable marker is corruption and fails the open.
  Tiered shards inherit the check per shard.
- `loadIndexes` still tolerates corrupt rows (skip + warn + counter
  reconcile), but a FUTURE per-row version is not damage — it fails the open
  instead of silently dropping the row.

Bump protocol (documented on `CurrentWireFormatVersion`): a version bump must
update the custom msgpack encoders (lesson 39), the decode path, and the
marker logic together.

### Write-Pressure Bound

`Config.MaxPendingWrites` (default 100,000 ops; negative disables; moot under
`SyncWrites`) bounds the async write buffer: dirty cache entries are never
evicted by design, so without a bound a sustained burst faster than
`FlushInterval` grows memory without limit. At the bound, the writing call
flushes synchronously (backpressure); a failing backpressure flush surfaces
its error to the writer and requeues the ops. `Store.PendingWriteCount()`
exposes the pressure signal.

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
- `atomic.Int64` counters for node/rel counts
- `sync.Map` for per-label/per-type counters
- `sync.Once` for idempotent Close

**Async persistence flow:**
1. Write operations update in-memory state immediately
2. Queue `writeOp` structs into `map[string]writeOp` (last-write-wins dedup)
3. Background flush loop drains buffer via Badger `WriteBatch` every `FlushInterval` (default 100ms)
4. Counter keys (`meta/node_count`, `meta/rel_count`) included in same WriteBatch for atomic crash recovery
5. Failed ops re-queued via `requeueOps()`

With `SyncWrites`, `FlushInterval` is forced to zero and callers that enqueue
Badger writeOps flush before returning. That includes property, temporal,
high-frequency, and vector index definition metadata create/drop paths plus split relationship
helper write/delete paths used by Tiered routing and repair; those methods
release `idxMu` before flushing because `flush()` takes `idxMu.RLock()` for its
snapshot.

**Old-state reads for Badger mutations:** `ReplaceNode`, label-token mutation
paths, and their history variants prefetch the current node outside `idxMu` to
avoid Badger I/O under the write lock. Cache-miss `DeleteNode` and
`DeleteRelationship` do the same prefetch, then re-read the current cached row
under `idxMu` and use that row for label/type/adjacency cleanup. After acquiring
`idxMu`, a missing `nodeIDs`/`relIDs` entry is a concurrent delete and returns
`ErrNodeNotFound`/`ErrRelNotFound`; any prefetch error while the ID still exists
is corruption or an operational read failure and is returned. Only cascade
delete has a cleanup-and-return-corruption fallback.

**LRU caches (`entityLRU[V]`):**
- Dirty tracking with monotonic `dirtyVer` counter
- Tombstone support for deletions
- Dirty entries never evicted (soft capacity)
- `Peek(key)` for zero-allocation lookup (no deep-copy, no MRU promotion) -- used by temporal pre-filter
- `MarkFlushed()` only clears entries matching the collected `dirtyVer` (prevents data loss from concurrent writes)

`outIdx`'s value is the relationship's END node ID and `inIdx`'s value is an
`inEdge{startNodeID, typeToken}` — both let adjacency answer type-filtered and
endpoint-resolving traversals without fetching the relationship row.

**Key layout (11 prefixes):**

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
included), then tail the feed. See `tasks/backlog.md`.

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
export header (v2; importers accept v1 and v2) carries `SnapshotLSN`, captured via
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
assignment) lives in sigma — rho-tkg exposes the primitives.

**ReadOnly mode:** `BadgerStoreConfig.ReadOnly` opens Badger read-only and skips flush/GC goroutines. TieredStore does not use read-only Badger handles for warm/cold owner shards: existing event entities still update/delete on their original shard after rotation and restart.

**ForEach iterators:**
- `ForEachNodeID` / `ForEachRelID` -- snapshot in-memory index-map IDs under `idxMu.RLock`, release the lock, then invoke callbacks
- `ForEachNodeHistoryID` / `ForEachRelHistoryID` -- page unique history IDs from pending writes plus Badger prefix scans, overlay pending delete writeOps from history truncation, release Badger transactions before invoking callbacks, and stop at the history-ID high-water mark captured before callbacks start

---

## tiered.Store (`pkg/graph/store/tiered/`)

Multi-shard Store routing entities across a reference shard and time-windowed event shards.

The package is ~36 production files; `tieredstore.go` plus the
`tieredstore_read_*` / `tieredstore_write_*` splits carry the core, with
`tieredstore_admin.go`, `_catalog.go`, `_changelog.go`, `_compaction.go`,
`_docvalues.go`, `_lifecycle.go`, `_migrate.go`, `_property_stats.go`,
`_repair.go`, `_routing.go` and `retention_purge.go` alongside them.

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

**refArchive checkout discipline (v3.1.11):** every refArchive read or write
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

**EventShard checkout discipline for admin paths (v3.1.16):** admin methods
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
3. Extract timestamp from snowflake ID via `snowflakepkg.Layout.Decompose(id).Time` -- O(1) shard window lookup

**Relationships (`shardForRelIDChecked`):**
1. Check `refShard.hasRelID(id)` -- O(1)
2. Check `refArchive` with a checkout pin when present -- O(1)
3. Timestamp extraction -> target event shard -- O(1)
4. Fallback: probe every other event shard, including cold shards, for cross-shard rels -- O(N)

**New entities:** Route by ontology classification (label -> ref/event), with a timestamp-owner exception for caller-supplied event node IDs. Graph-generated current event IDs land in `hotShard`; imported/backfilled event node IDs are written to the event shard selected by their snowflake timestamp. Public Store create paths use the same cold-shard-aware ID resolution as reads before accepting caller-supplied IDs, so changing the label class or relationship start shard cannot bypass duplicate detection for IDs already parked in closed cold shards. Relationship duplicate checks resolve actual owner placement, not only the relationship ID timestamp, because a current-ID relationship can still be stored on an old start-node shard. Core-generated fresh IDs use an internal generated-ID fast path, including batch node execution, to keep normal creates on the hot path without opening unrelated cold shards.

### Relationship Routing (7 keys per relationship)

| Pattern | Entity + out/ keys | in/ keys | Split? | Write order |
|---------|-------------------|----------|--------|-------------|
| E->E | event shard | event shard | No | single shard |
| R->R | ref shard | ref shard | No | single shard |
| E->R | event shard | ref shard | Yes | ref in/ first |
| R->E | ref shard | event shard | Yes | entity first |

Cross-shard split writes use `badgerstore_partial.go` helpers: `putRelEntityAndOut` (entity + typeIdx + outIdx) and `putRelIncoming` (inIdx only). Both endpoints verified to exist before any writes begin.

`PutRelationship` and `PutRelationshipsBatch` serialize duplicate-ID probes with the write path. Batch create preflight rejects internal duplicates and resident cross-shard duplicates before any relationship row or adjacency leg is written.

**Incoming index structure:** BadgerStore's `inIdx` uses `map[types.NodeID]map[types.RelID]inEdge` (endNodeID -> relID -> `{startNodeID, typeToken}`), not a bare set. The typeToken value enables efficient cross-shard type filtering in `IncomingRelationships` without fetching the relationship entity from a remote shard, and the startNodeID resolves the far endpoint in the same lookup. MemoryStore retains the simpler `map[types.NodeID]map[types.RelID]struct{}` since all entities are local.

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

**Cold shard access (`checkoutStore`/`checkinStore`):**
- `checkoutStore` increments `activeReqs`, lazy-opens if `store==nil`
- `checkinStore` decrements `activeReqs`
- `closeIdleShards()` skips shards with `activeReqs > 0`
- idle-close `BadgerStore.Close` errors are recorded, logged, block later lazy checkout, and are joined into `Store.Close`
- explicit `Store.Close` takes the same event-shard mutex before closing or clearing `store`, so close and idle-close cannot race on the shard handle
- MemoryStore public APIs return `ErrStoreClosed` after close before serving or mutating process-local maps; exported test-only tampering helpers return no data or no-op
- BadgerStore public APIs return `ErrStoreClosed` after close before serving cache hits, O(1) counts, empty fast-path success, vector search, history scans, metadata, or split relationship helper mutations; no-error diagnostics return zero values
- checked node/relationship routing, create-time rotation, valid index create/drop/search calls, direct read/count/bulk/history APIs, empty batch/history write edges, and public admin/metadata operations return `ErrStoreClosed` after close starts, before reading reference state, event shard handles, store-level index maps, catalog metadata, or registry files
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

**Tiered delete-with-history:** `DeleteNodeWithHistory` first validates that the supplied relationship tombstones cover every live connected relationship exactly once. It then deletes connected relationships before deleting the node so endpoint checks remain valid. It snapshots each relationship and history tape first; if a later relationship delete or node tombstone write fails while the node still exists, the already-deleted relationships are restored.

**Tiered plain cascade:** `DeleteNodeCascade` snapshots live connected relationships before mutation, skips adjacency entries whose relationship row is already missing, and uses the shard-local cascade path for final node removal so stale adjacency-only entries are purged with the node. If a later live relationship delete or node row delete fails while the node still exists, previously deleted live relationships are restored from the preflight snapshots.

**Property indexes:** Restricted to reference entities only (`ErrEventPropertyIndex` for event labels).

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

Slot-topology Store: N `badger.Store` shards, one per snowflake SLOT, routing every entity by the slot carried in its ID's node field — never by ontology class (that is tiered's job). The design target is the horizontal stage where `ErrSlotNotLocal` becomes "route to the owning machine." Integration-branch WIP; declared EXPERIMENTAL until the S4 throughput bar and S5 parity land in a numbered release.

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

A label's entities are distributed across slots, so — unlike tiered, which routes property indexes to its ontology reference shard — every index/stats capability fans its DDL out to EVERY shard (each badger shard maintains its own index over its local entities, in lockstep via `fanOutUniform`) and folds the per-shard results on read. Implemented: PropertyIndex, RelPropertyIndex (accelerated — sharded shards are static and all-open, so a per-shard rel-value index is foldable, where tiered declines), Composite + CompositeIntrospection, TemporalIndex, HighFrequencyIndex, NodePropertyKeyStats, NodePropertyTypeClassCounts, inline `NodeRangeCardinality` (per-shard exact sum; exact only if every shard is exact), and Vector (VectorIndex + Options + FilteredVectorSearch). Vector indexes keep ONE index PER SHARD (reusing badger's per-write maintenance — no store-level write-path hooks, avoiding tiered's silent-staleness surface) and merge per-shard top-k globally by `index.VectorDistance` — EXACT for brute-force, sound-approximate for HNSW; the store persists only per-index def metadata (dims+metric) to anchor MetaKV.

### Declined-with-reason

Mirroring tiered: `TransactionTimeQuery`, `HistoryRollbackTrim`, `LabelTxMembership`/`RelTypeTxMembership` (the full-history fold is the correct sharded path), and the two depth-iteration accelerators. `OwnedPreEncodedPutCapability` and the pre-encoded put/log capabilities ARE implemented (routed to the owning shard's badger door).

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

Extracts low 8 bits of the snowflake timestamp via `Layout.Decompose()`. Entities created >256µs apart distribute across shards.

| Method | Use case | Deadlock prevention |
|--------|----------|---------------------|
| `LockEntity(id)` | Single-entity mutations | N/A |
| `LockTwo(a, b)` | Relationship creation | Ascending shard order |
| `LockMany(ids)` | Cascade delete | Deduplicate + sort ascending |

Lock ordering: entity locks -> `idxMu`. Always.

---

## Registries (`pkg/graph`)

Three independent registries with independent token namespaces (label, rel-type, property-key).

```
labelRegistry:       map[string]labelToken   + []string reverse lookup
relTypeRegistry:     map[string]relTypeToken + []string reverse lookup
propertyKeyRegistry: map[string]propKeyToken + []string reverse lookup
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

## Key Architectural Patterns

Distilled from real bugs into the compact checklist in `tasks/lessons.md`.

### Two-Phase Operations

Multi-step mutations split work into a read/validate phase and an apply phase. The two phases differ in their error contract:

- **All-or-nothing**: `DeleteNodeCascade` (preflight reads adjacency, skips stale adjacency-only entries, and restores completed relationship deletes if a later step fails before the node row is removed) and `CreatePropertyIndex` (placeholder install + dirty-map tracking + atomic install — see `tasks/lessons.md` A4).
- **Per-operation error reporting**: `BatchBuilder.Execute` runs Phase 2 against each queued operation; failures are recorded as `BatchError` entries on the returned `BatchResult` rather than aborting the batch. If any operation fails, `Execute` also returns an error wrapping `ErrBatchFailed`, so a normal error check cannot miss partial failure.

Used by: `DeleteNodeCascade`, `CreatePropertyIndex` (all-or-nothing); `BatchBuilder.Execute` (per-op result reporting).

### Async Persistence with Last-Write-Wins

Write operations update in-memory state immediately, queue write ops into `map[string]writeOp` (dedup by key). Background flush loop drains via Badger WriteBatch. Counters in the same WriteBatch for atomic crash recovery. `Close()` calls `flush()` unconditionally (even if flushLoop never started).

### Deep-Copy / Frozen Rows at Store Boundary (since v4.5.0)

`Put*` deep-copies before caching and freezes the cached entry (`types.Node.Freeze()` / `types.Relationship.Freeze()`). Point reads (`GetNode`/`GetRelationship`) deep-copy on return, so callers get mutable, independent copies. Plural/scan reads (`*ByLabel*`, `All*`, `Get*ByIDs`, adjacency traversals, temporal/index scans) return the shared frozen pointer directly — zero-copy. Frozen entities reject mutation: error-returning mutators return `ErrFrozenNode`/`ErrFrozenRelationship`, void/bool mutators panic, and `DeepCopy()` is the thaw operation. Rows for duplicate requested IDs may alias the same frozen pointer. Internal locked methods (`getNodeLocked`) skip the copy when the caller already holds the write lock. The frozen guard also covers the pointer escape hatches: on a FROZEN entity, `Temporal()` and `Integrity()` return independent copies (Signature bytes cloned) instead of the shared internal pointer — their exported fields would otherwise let a scan consumer silently corrupt the canonical cached row (the poisoning tests in `pkg/graph/frozen_poisoning_test.go` pin this). Unfrozen entities keep the shared-pointer contract the graph layer relies on. `AddByIDIfAbsent` deep-copies on the found branch so both branches return mutable results.

### Temporal Data Is Append-Only

History is never physically deleted. Delete paths save tombstone versions (with `DeletedAt`/`ValidTo`) before deletion. Past-time queries reconstruct deleted entities from history — including transaction-time reads: a delete is a transaction-time tombstone, so any `TxAt`/as-of read pinned BEFORE the delete returns the entity in its pre-delete belief state (the post-pin `DeletedAt`/`ValidTo`/`TxTo` stamps are normalized away on a copy), and any pin at or after the delete excludes it (v4.11.1, lesson 60).

For single-shard stores, the tombstone write and the entity deletion are combined in a single atomic Store call (`DeleteNodeWithHistory` / `DeleteRelWithHistory`). All ops land in one Badger `WriteBatch.Flush()` under the same `idxMu.Lock()`, eliminating the crash window that previously existed between N+2 separate store calls. Tiered stores preserve that per-shard atomicity, preflight relationship tombstone coverage before the first delete, and add rollback around plain and history-carrying pre-node relationship deletes when a later step fails before the node is removed. Rollback failures are surfaced with the primary error.

Delete tombstone payloads are validated before deletion. A relationship tombstone must describe the same relationship ID, type token, and endpoints as the live row it closes; otherwise the Store returns `ErrInvalidStoreMutation` and leaves the live row and history unchanged.

### ForEach for OOM-Safe Iteration

Callback-based iteration (`fn(snowflake.ID) bool` -- return true to continue, false to stop) avoids materialising all per-shard slices and preserves early stop. Store implementations collect a bounded shard/page snapshot, release backend locks or check in the Tiered shard, then invoke the caller callback. Callbacks may call Store methods. Badger history iterators overlay pending truncation deletes and stop at the start high-water mark so callback-created higher history IDs do not extend the same iterator. TieredStore still iterates shards sequentially, one shard snapshot at a time.

### Cold Shard Checkout/Checkin

Returning a `*BadgerStore` pointer and releasing the lock creates a race with idle-close. Solution: `checkoutStore` increments `activeReqs`, caller defers `checkinStore`, `closeIdleShards` skips shards with `activeReqs > 0`.

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

After v3.4.0 (Option 3) and v4.2.0 (field→method), `pkg/graph/` is a thin façade: the `Graph` type holds a `*core.Core` plus 16 unexported sub-API pointers exposed via nil-safe accessor methods. All implementation lives in `pkg/graph/internal/core/`. The 130+ public methods that used to live directly on `*Graph` were removed — customers use the sub-APIs (`g.Nodes().Add`, `g.Temporal().NodesAt`, etc.).

#### `pkg/graph/` (6 production files + 1 smoke test)

| File | Purpose |
|------|---------|
| `graph.go` | `Graph` thin façade: `core *core.Core` + 16 unexported sub-API pointers. Public methods: `New`, `Close`, `SetReplicationSource`, and 16 nil-safe accessor methods (`Nodes() *nodes.API`, etc.). Package-level `NewBatchBuilder`, `DecomposeNodeID`, `DecomposeRelID`. Plus `Config`, `ValidationLimits`, `IDComponents`, `ConstraintSet`, `QueryOpts`, `ShardDepth`, `DistanceMetric` type aliases re-exported. |
| `open.go` | Convenience constructors `Open(dir, opts…)` / `OpenInMemory(opts…)` plus the `Option` functions `WithSnowflakeNodeID`, `WithValidation`, `WithProfileSmall`/`WithProfileServer`/`WithProfileBulkLoad`. |
| `restore.go` | `RestoreInto(cfg, dir)` — validates a full+delta backup chain, then replays it into a fresh graph. |
| `subapi.go` | `TxAPI` and `BatchAPI` — sub-API accessors for `g.Tx` and `g.Batch`, kept in-package because they wrap `*GraphTx` / `*BatchBuilder` declared in `internal/core`. |
| `errors.go` | Public sentinel re-exports (the canonical consumer surface for `errors.Is`): store sentinels (including the replica/change-log set — `ErrCapabilityNotSupported`, `ErrPrimaryRegistryStale`, `ErrRegistryDiverged`, `ErrWireFormatVersionUnsupported`), vector-index sentinels, registry sentinels, IndexProvider sentinels, IO sentinels, and the `internal/core` graph-layer sentinels (including `ErrReadOnlyReplica`). Each alias points at its one canonical declaration (store / index / registry / io / core). |
| `subapi_smoke_test.go` | `TestSubAPISmoke` — exercises every sub-API accessor end-to-end. |
| `doc.go` | Package documentation. |

#### `pkg/graph/<types-package>/` (types-only public packages, v3.3.0)

| Package | Purpose |
|---------|---------|
| `pkg/graph/store` | `Store` interface, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, `VectorIndexOptions`, `ChangeFeedCapability` + `ChangeRecord`/`ChangeTag`, `ReplicationSource` + `RegistrySnapshot`/`IDSlotLeaseRecord`, ~48 capability interfaces, 34 store sentinels. |
| `pkg/graph/store/memory` | `memory.Store`, `memory.New()`. |
| `pkg/graph/store/badger` | `badger.Store`, `badger.Config`, `badger.New()`. |
| `pkg/graph/store/tiered` | `tiered.Store`, `tiered.Config`, `tiered.New()`, `MigrateFromBadger`, `EventShard`, `ShardInfo`, `VerifyResult`, `RepairResult`. |
| `pkg/graph/store/sharded` | `sharded.Store`, `sharded.Config`, `sharded.New()` — EXPERIMENTAL (ADR-0007) slot-topology backend. |
| `pkg/graph/ingest` | `ingest.API`, `Session`, `IngestOptions`, `SubmitToken` — the ADR-0006 write door behind `g.Ingest()`. |
| `pkg/graph/events` | `Event`, `EventType`, `EventPriority`, `EventBus`, `AsyncEventBus`, `BackpressureStrategy`, constructors, constants. |
| `pkg/graph/index` | `IndexProvider`, `Initializable`, `GraphReader`, `LegacyIndexProvider`, sentinels. |
| `pkg/graph/temporal` | `GraphSnapshot`, `SnapshotDiff`, `NodeUpdate`, `RelUpdate`, `TemporalConstraint`, `ConstraintSet`, sentinels. |
| `pkg/graph/ontology` | `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, class constants. |

### `pkg/graph/internal/*` subpackages

| Package | Purpose |
|---|---|
| `internal/core` | (v3.4.0) `Core` type holding all unexported state and ~130 method bodies that previously lived on `*Graph`. ~34K LOC of implementation across 83 files; ~88K LOC of internal tests across 232 test files. |
| `internal/snowflake` | Snowflake `Epoch`, `Layout`, `IDComponents`, `DecomposeID`. Single source of truth for ID-bit decomposition. |
| `internal/storeutil` | (renamed from `internal/store` in v3.3.0) Store-internal helpers: key encoding, msgpack wire types, pagination helpers, temporal-filter push-down. The public Store contract lives in `pkg/graph/store`. |
| `internal/locks` | 256-shard entity-lock `Manager`, `LockEntity`/`LockTwo`/`LockMany` in ascending order. |
| `internal/registry` | `LabelRegistry`, `RelTypeRegistry`, `PropertyKeyRegistry`. Internal types — not part of public API. |
| `internal/index` | In-memory indexes only: property index, vector index, high-frequency temporal index, `OntologyMapping`. |
| `internal/integrity` | Pure SHA-256 hash primitives — `ComputeNodeHash`, `ComputeRelHash`. Five fixed-vector anchors lock the on-disk hash format. |
| `internal/grapherr` | `ErrNilGraph` / `ErrNilCallback` + the `IsNil` typed-nil detection every sub-API `ready()` uses to fail closed. |
| `internal/apiutil` | Generic helpers shared by the sub-API wrapper packages (`CloneSlice`, `CloneMap`, `iterateForEach`) — de-duplicated from nodes/rels/index/tier/stats. |
| `internal/generatedcreate` | `Proof` / `FreshGraphID()` — the unforgeable internal token that marks a create as carrying a freshly minted graph ID, so the duplicate-check fast path cannot be reached from outside `pkg/graph`. |

### `pkg/graph/<sub-api>/` packages (v3.4.0)

| Package | Field on Graph | Methods |
|---------|----------------|---------|
| `pkg/graph/nodes` | `g.Nodes` | ~31 wrappers — node CRUD, label, property, version chain. |
| `pkg/graph/rels` | `g.Rels` | ~30 wrappers — relationship CRUD, adjacency, property, version chain. |
| `pkg/graph/temporal` | `g.Temporal` | ~24 wrappers — point-in-time, interval, bitemporal, snapshot/diff, Allen relations. Coexists with the temporal types (`GraphSnapshot`, `SnapshotDiff`, …) in the same package. |
| `pkg/graph/index` | `g.Index` | ~13 wrappers — property/vector/high-frequency index management + IndexProvider. Coexists with `IndexProvider`, `Initializable`, `GraphReader` in the same package. |
| `pkg/graph/events` | `g.Events` | ~3 wrappers — sync/async EventBus management. Coexists with `EventBus`, `AsyncEventBus`, `Event`, … in the same package. |
| `pkg/graph/constraints` | `g.Constraints` | ~4 wrappers — temporal-constraint set management (`Set`, `Add`, `Get`, `DryRunValidate`) — plus the 5 unique-property-constraint doors (ADR-0002) in `unique.go`. |
| `pkg/graph/io` | `g.IO` | ~2 wrappers — Export / Import. Shadows stdlib `io`; alias as `tkgio` at consumer sites that also need stdlib `io`. |
| `pkg/graph/admin` | `g.Admin` | 6 wrappers — backend-agnostic admin (`Reset`, `DecomposeNodeID`, `DecomposeRelID`, `CompactHistoryNodes`, `CompactHistoryRels`, `PurgeExpiredNodes`). `Reset` and `PurgeExpiredNodes` are opt-in via `Config.AllowReset` / `Config.AllowRetentionPurge`. |
| `pkg/graph/tier` | `g.Tier` | ~7 wrappers — tiered-store admin (archive, restore, rotate, shards, rebuild-catalog, repair, verify-shard). Reuses `core.AdminOps`. |
| `pkg/graph/replication` | `g.Replication` | Change-log / op-log + replica apply: `ChangeFeed`, `ForEachChange`, `LastCommittedLSN`, `ApplyChange`/`ApplyChanges`, `AppliedLSN`/`SetAppliedLSN`, `RegistrySnapshot`, `IDSlotLease`/`SetIDSlotLease`. |
| `pkg/graph/ingest` | `g.Ingest` | 3 wrappers — the ADR-0006 prepare-parallel / apply-sequential write door (`NewSession`, `AppliedSeq`, `WaitApplied`). |
| `pkg/graph/stats` | `g.Stats` | ~7 wrappers — count helpers (including `NodeCountByLabelAndPropertyKey`). |
| `pkg/graph/hash` | `g.Hash` | ~2 wrappers — hash-chain verification. Shadows stdlib `hash`; alias as `tkghash` at consumer sites that also need stdlib `hash`. |
| `pkg/graph/resolve` | `g.Resolve` | 2 wrappers — shadow-property accessors (`NodeProperty`, `RelProperty`). |

`g.Tx` (`TxAPI` in `subapi.go`) and `g.Batch` (`BatchAPI`) live in the `pkg/graph` package itself because they wrap the pkg/graph-private `*GraphTx` / `*BatchBuilder` types. `TxAPI.Run` / `TxAPI.RunContext` add closure-style transaction helpers on top of `Begin`.

---

## Deferred Architectural Decisions (2026-06-10 review)

Recorded so future readers know these are conscious choices, not oversights:

- **Core sub-packaging**: `internal/core` is one ~20K-LOC package; the
  sub-Ops decomposition is namespacing, not separation. Splitting into
  `core/tx`, `core/temporal`, `core/mutate` is a multi-week, behavior-neutral
  restructure — planned as its own effort. The shared relationship-create
  kernel removed the most acute intra-core duplication.
- **`pkg/graph/hash` / `pkg/graph/io` renames**: both shadow stdlib package
  names and force `tkghash`/`tkgio` aliasing on consumers. Renaming is
  breaking — v5 item.
- **Iteration-capability matrix**: the history × deleted × depth × paged
  optional-interface grid grows combinatorially; a parameterized iteration
  interface is a breaking store-contract redesign — v5 item.
- **Eclipsed-row explicit wire flag**: the zero-width `ValidTo==ValidFrom+1`
  sentinel works but is an in-band magic value; an explicit flag becomes
  cheap at the next wire-format version bump (the versioning machinery now
  exists) — schedule together.
- **`tier` package returning `tiered.*` types** (forces the tiered import on
  consumers) and `RecoverBackgroundError` exposure via `g.Tier()`: both are
  additive-but-coupled API changes — bundle with the next planned API pass.
- **StoreStats interface extension** (pending-write/dirty counts): widening
  the type-asserted optional interface breaks out-of-tree implementers;
  pressure visibility shipped as `badger.Store.PendingWriteCount()` instead.
