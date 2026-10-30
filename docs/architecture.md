# Architecture — tkg/v4 (v4.97.3)

rho-tkg is a pure Go library for temporal knowledge graphs: typed nodes and relationships whose every version is
kept, stamped in two time dimensions and hash-chained. It is a library, not a database: no binary, no server, no
query language. Consumers (sigma-tkgd, ai-soc) embed it and build their own query and serving layers on top.

```
Module:  github.com/data-insights-ai/rho-tkg/v4
Go:      1.26.9
License: Apache-2.0
```

This document explains how rho is built and which invariants hold. It does not list the API (`docs/api.md`), the
on-disk format and durability (`docs/persistence.md`), the query planners (`docs/query-planners.md`) or the error
sentinels (`docs/errors.md`). Working rules for contributors are in `docs/CONTRIBUTING.md`.

---

## 1. Layers

```
  consumer code
       │
  graph.Graph ── 16 sub-APIs: Nodes Rels Temporal Index Constraints Batch Tx Ingest IO Admin Tier Hash
       │          Resolve Events Stats Replication   (thin façade, pkg/graph)
       │
  internal/core.Core ── validation, string↔token resolution, temporal resolution, locking,
       │                version chains, hash chains, rollback, events, change-log scope
       │
  store.Store ── MandatoryStore + optional *Capability interfaces (pkg/graph/store)
       │
       ├── memory.Store    in-process maps (tests, ephemeral graphs, column segments)
       ├── badger.Store    one Badger DB: the persistent single-node backend
       ├── tiered.Store    many badger.Stores: a reference shard plus time-windowed event shards
       └── sharded.Store   N badger.Stores routed by the ID's slot (EXPERIMENTAL)
```

**Graph** is the only public entry point. It is a thin façade (`graph.go`) over `internal/core.Core`; each sub-API
package declares a small `Ops` interface for the methods it forwards, so the packages stay independent of the
core. A nil or zero-value graph or sub-API fails closed with `ErrNilGraph`.

**Core** owns everything that needs strings or cross-entity reasoning: the three token registries (labels,
relationship types, property keys), the two snowflake ID generators, the entity lock manager, temporal resolution,
hash chains, transactions and rollback. It talks to the store only through the `Store` interface.

**Store** is pure persistence over tokens and IDs. It never sees a label string and makes no temporal decision of
its own beyond the shared predicates in `internal/storeutil`. `MandatoryStore` (lifecycle, CRUD, adjacency, bulk
reads, batch, history, stats, iteration) is what core requires; about 90 optional `*Capability` interfaces
(indexes, change feed, history compaction, retention purge, durable flush, …) are type-asserted where needed, and a
door whose capability is missing fails with `ErrCapabilityNotSupported` or takes a correct generic path.
`pkg/graph/store/capabilities.go` and its siblings are the authority for the capability list.

---

## 2. Data model

- **Node**: an ID, a primary label plus extra labels, properties, a version number, temporal metadata and
  integrity metadata (hash, previous hash, provenance).
- **Relationship**: an ID, a type, start and end node IDs, properties, version, temporal and integrity metadata,
  plus the hashes of both endpoints at write time.
- **Entities are pure data.** `types.Node` / `types.Relationship` hold tokens, not strings, and no reference to the
  graph; core resolves names (`g.Nodes().Labels`, `g.Rels().Type`, `g.Resolve()`). No public door returns a token.
  Rows a store hands to scans are frozen and shared (mutating one fails with `ErrFrozenNode`); point reads return
  mutable copies.
- **Properties** are an allowlist: scalars, string/number/byte/bool slices, `map[string]any`,
  `map[string]string`, `types.TemporalValue` (and `time.Time`, stored as one), and registered structs.
  `ValidationLimits` bounds labels, properties, key and value sizes.
- **Shadow properties** are 22 read-only `tkg_*` keys resolved from metadata: `tkg_id` (the entity's own ID, since
  v4.80.0), `tkg_labels`, `tkg_type`,
  `tkg_valid_from`, `tkg_valid_to`, `tkg_tx_from`, `tkg_tx_to`, `tkg_created_at`, `tkg_updated_at`,
  `tkg_deleted_at`, `tkg_created_by`, `tkg_updated_by`, `tkg_version`, `tkg_hash`, `tkg_prev_hash`,
  `tkg_from_hash`, `tkg_to_hash`, `tkg_author_id`, `tkg_signature`, `tkg_authorized_by`, `tkg_auth_level`,
  `tkg_base_entity`. User properties may not start with `tkg_`.
- **Tokens.** Labels and relationship types are `uint16` tokens (max 65,535, token 0 reserved); a full label or
  type registry fails the write. A full property-key registry falls back to the raw key on the wire, so a property
  write never fails on registry exhaustion.

### Snowflake IDs

```
| 1 bit zero | 48 bits time (µs since 2026-01-01 UTC) | 5 bits node field | 10 bits step |
```

IDs are snowflakes, not counters: unique without coordination, time-ordered, decomposable, and stateless (no
counter to persist or recover). Nodes mint on node field `SnowflakeNodeID*2`, relationships on `*2+1`
(`SnowflakeNodeID` 0–15); `IngestLanes` adds lane generators on the remaining fields (`2 + IngestLanes ≤ 32`).
The time field is used for placement (tiered time windows, lock striping) and, through `idlayout.MintInstantMillis`
only, as the last fallback of an entity's start time. The time field runs out on 2034-12-02 (parked backlog 4).

---

## 3. Time model

rho is bitemporal. Every version carries two independent time dimensions:

| | Claim | Who sets it |
|---|---|---|
| **Valid time** `[ValidFrom, ValidTo)` | "this holds in the world during …" | the caller, from domain knowledge |
| **Transaction time** `TxFrom` (and `TxTo`) | "the graph recorded this belief at …" | the system, from a monotonic commit clock |

A writer must never default valid-from to "now": that leaks recording latency into world time.

**Versions are append-only.** An update writes a new version and moves the old one to history; a delete writes a
tombstone; a correction of the past (`SetNodeVersionInterval`, excise, `UpdateWithTx` / `DeleteWithTx` with a
caller instant under `AllowTxBackfill`) appends rows at a new `TxFrom`. No ordinary door rewrites or removes a
stored row. Only opt-in admin doors remove history: `CompactHistory*`, `PurgeExpiredNodes/Rels`, `ExactErase` and
`Reset`; a read pinned before compacted knowledge fails with `ErrHistoryCompacted` instead of answering partially.

**The valid axis and untimed writes.** A write without a valid time stores a start chosen by the store's persisted
`UntimedValidStart` mode (api.md "Valid-time start"):

- `UntimedUnbounded` (default for new stores): the first version holds from −∞; later versions start at their
  `TxFrom`.
- `UntimedFromTxFrom` (stores before v4.57): every untimed version starts at its `TxFrom`.
- `UntimedWholeAxis`: no claim means the whole axis; rows live on the *literal* axis, where 0 is an ordinary
  instant and the open end is `InfiniteValidTo`.

On the two legacy-axis modes a stored 0 means "unset". Code compares valid times only through `types.ValidAxis`,
never against a literal 0.

**One start rule.** The effective valid-from of a row is its explicit `ValidFrom`, else its `TxFrom`, else (legacy
rows) `UpdatedAt`, else the ID's mint instant: `storeutil.EntityValidFrom`, shared by core, every store push-down,
the temporal indexes and the column filters. A version ends where the next version starts, so versions tile.

**Two kinds of temporal read.**

- *State doors* answer "what was valid at valid time t, as believed at transaction time p": `NodeAtTx`, `ByLabel`
  with `ValidAt` + `TxAt`, snapshots, the effective timeline.
- *Record doors* answer "what was the newest row recorded by p, whatever valid time it covers": `NodeAsOf`,
  `NodesAsOf`, `QueryOpts.TxPin`.
- Transaction time filters by `TxFrom ≤ p`; `TxTo` does not hide a row: superseded is not retracted.
- *Retraction* ends belief: a retracted life disappears from every read pinned after the retraction, while a
  delete only ends validity.
- *Excise* writes a void row: absence over an interval, not a delete.
- *Effective* relationship doors (snapshot, diff, neighbours, adjacency at t) show an edge only while it and both
  endpoints are valid; *declared* doors report the relationship's own validity.

**One resolution seam.** Every core temporal read selects its version through `resolveNodeChain` /
`resolveRelChain` (`internal/core/chain_resolver.go`), so visibility, tombstones, supersession, voids and retraction
cannot drift between doors. The Allen-relation doors (`NodesRelating`, `RelsRelating`) use the same seam.

**The commit clock.** Transaction time comes from a per-graph monotonic clock (`NowTx`) with a persisted floor, so
`TxFrom` never repeats or goes backwards, across restarts, replicas and resets, even when the wall clock does.

---

## 4. Writes, transactions and concurrency

Four write doors share one validation and storage path:

| Door | Unit | Atomic against readers | Use for |
|---|---|---|---|
| standalone (`g.Nodes().Add`, …) | one entity | per entity | interactive writes |
| `g.Tx()` | many calls | no (write-through, compensating rollback) | multi-step logic that can roll back |
| `g.Batch()` | one queued group | yes (holds the graph write lock) | groups readers must never see partially |
| `g.Ingest()` | streamed groups | per group | high-throughput insert (prepare in parallel, apply in order) |

**Every write validates before any side effect**: before an ID is minted, a token interned, a snapshot taken or an
op queued. Relationship creation ends in one kernel (`relationship_create_kernel.go`); constraints and unique
properties are enforced at the finalized-state seam, not per door.

#### Transaction isolation — what v4 actually guarantees

`GraphTx` is a **write-through transaction with compensating rollback**, not an isolated one:

- Transactions and batches are serialized against each other (`txMu`).
- Each tx mutation reaches the store immediately: a concurrent standalone reader sees uncommitted rows (dirty read)
  and sees them vanish after `Rollback`.
- Entity locks are taken per call, not held to commit: a standalone write between two tx calls can be overwritten
  by `Rollback` (lost update).
- A crash before `Commit` can leave part of a transaction on disk, unless `Config.DurableCommit` is on; then
  `Commit` returns only after one durable flush of the whole group.
- The change-log is scoped: a rolled-back transaction emits no records.

Use `g.Batch()` when readers must never see a partial group; serialize writers yourself when a tx must not lose
standalone writes. Isolated transactions are a v5 item.

**Locking.** One lock order, everywhere:

```
txMu → Core.mu → registryMu → entity locks → value locks → store locks
badger:  flushMu → idxMu → buildMu → wbMu
tiered:  lifecycleMu → ts.mu → shardIdxMu;   moveMu → ts.mu / archiveMu
```

`Core.mu` is a striped read/write lock: standalone writes and reads take it shared; `Batch`, export, snapshots and
admin cascades take it exclusive. Entity locks are 256 stripes over the ID's time bits, always taken in ascending
order; unique constraints add 256 value stripes keyed by (label, key, value). A holder of value locks never takes
an entity lock.

**Integrity.** Each version's SHA-256 hash covers its canonical content and the previous version's hash, so a
version chain verifies end to end (`g.Hash().Verify*Chain`), across deletes, re-imports and compaction stubs.
Relationships also record their endpoints' hashes at write time. Transaction-time stamps are not hashed. Hashing
can be switched off per store (`DisableIntegrityHashes`, persisted).

**Events.** Mutations publish events after their locks are released, through a synchronous bus or an async bus
with five strict priorities and a chosen backpressure strategy.

---

## 5. Storage backends

### memory.Store (`pkg/graph/store/memory`)

Maps of frozen rows plus the same index and sidecar structures as badger, process-local. It is the reference
backend for tests and the home of column segments (ADR-0011).

### badger.Store (`pkg/graph/store/badger`)

One Badger LSM. Writes land in an in-memory write buffer and are flushed in one `WriteBatch` every `FlushInterval`,
together with the counters and the change-log records; `SyncWrites` flushes and fsyncs per write, and
`DurableCommit` groups a whole commit into one fsync. Readers see the buffer through an overlay, so a write is
visible before it is durable. Indexes (labels, adjacency, properties, temporal, vector) live in RAM by default and
optionally on disk; a clean `Close` writes an *open image* so the next open skips the rebuild scan, and an open after a crash
applies that older image and decodes only the entities written since it (OPENIMAGE); with `OpenImageInterval`
the store also writes one periodically while it runs, so a store that never closes cleanly reopens from the last
one. What stays in RAM
per entity is bounded (EXISTMAP): the live ID sets and the rel-type index are sorted chunks of IDs (~8 B each;
OPENWALK: ~16 B per rel in all), the write revisions a fixed striped table (128 KB per kind), the integrity-hash
cache at most 64 K entries, the label and rel-type tx-membership sidecars only the entities that left a label or type
(TXMEMBERS). With the label and adjacency indexes on disk a clean open walks no key per entity. A property
index kept on disk (nodes: `PropertyIndexOnDisk`, `PropertyIndexOnDiskLabels`; relationships:
`RelPropertyIndexOnDiskTypes`) answers its pinned reads from member entries on disk too, so it builds no
membership sidecar in RAM (RELDISK); a composite node index of such a label keeps its current entries on disk
(COMPDISK).
`docs/persistence.md` owns the key layout, the wire format, the durability guarantees and recovery.

### tiered.Store (`pkg/graph/store/tiered/`)

A reference shard plus a series of event shards, each a `badger.Store`, under one `DataDir`:

```
DataDir/
  meta/        shard catalog, registries, index definitions
  reference/   reference-class nodes (RefLabels), always open
  events/      one shard per time window (ShardWindow, default 1 week)
               hot  = the current window, written by new events
               warm = older windows, open and writable
               cold = older than ColdAfter, closed by the idle sweep, reopened (writable) on demand
  archive/     archived reference nodes
```

Tiers are catalog states, not places. Each catalog entry names its shard's volume: hot (`DataDir`, fast NVMe), warm
(`WarmDir`, SSD) or cold (`ColdDir`, block devices); a new store makes its reference shard on `ReferenceVolume` (hot or
warm) and its archive on `ArchiveVolume`, everything else starts on hot. Moving shards between volumes and a global
index on the fast volume are being built: ADR-0012.

- **Routing.** A node's label decides its *class* (reference or event) at creation. An event entity lives in the
  shard whose time window contains its ID's mint time. Lookup goes reference shard → archive → event shard by ID
  time. A relationship lives with its start node; when its end node is on another shard, its incoming leg is
  split-written there (the incoming leg first for event→reference, the entity first otherwise).
- **`RelsByMint`** puts a relationship between reference nodes on its mint shard instead, so reference-heavy graphs
  do not pile every edge into the reference shard.
- **Rotation** opens the next hot shard, saves the catalog, and only then flips the old hot shard to warm.
  Shards older than `ColdAfter` become cold; a demoted shard keeps its handle until the idle sweep closes it after
  `IdleTimeout` unused, counted from its last cold access (a demoted shard no cold read touched closes at the next
  sweep; hot and warm reads stamp no access). A read reopens a cold shard read-only (COLDRO: 1.3 ms against 27.6 ms writable), a write or a
  checkout that may write reopens it writable (replacing a read-only handle once its readers finish), and at most
  `MaxOpenColdShards` stay open.
- **No RAM tier.** `DurableCommit` / `SyncWrites` need every write on a disk with a write-ahead log; RAM is each
  shard's cache (`MemTableSize`, `BlockCacheSize`, `CacheBudgetBytes`).
- **Shard pinning.** Every access pins a shard (`checkoutStore` / `checkinStore`); `Close` and the idle sweep wait
  for pins to drain, so no reader ever holds a closed database.
- **Old entities stay writable** on their own shard after rotation; there is no read-only tier.
- **Retention** drops whole expired shards where possible and purges rows otherwise.
- **The reference shard** also holds the change-log watermark, the commit floor and the property-key registry.

### sharded.Store (`pkg/graph/store/sharded`, EXPERIMENTAL, ADR-0007)

N always-open badger shards, one per claimed snowflake slot (the ID's node field), in `slot-NN` directories. An
entity's shard is a pure function of its ID, fixed at mint, so a point operation touches one shard and there is no
move primitive. A relationship and both its adjacency legs live on the relationship's shard (a foreign-start stub
on the end node's shard is the ADR-0010 exception). Scans fold all shards in parallel. Batches are atomic per shard
group (per relationship for relationship batches) and fail loudly with `PartialBatchError`; a cascade delete
removes relationships first and the node last, so a crash leaves dangling relationships, never a ghost-edged node.
All shards draw change-log LSNs from one allocator, giving one total commit order.

---

## 6. Indexes, change-log and replication

**Indexes** are maintained by the store on every write path, created in phases so concurrent writes are never lost
(install a building index, build without the lock, merge and publish under it), and rebuilt on open from persisted
definitions. Property, composite, relationship-property, temporal-interval, high-frequency, unique and vector (HNSW
or brute force) indexes exist; `docs/query-planners.md` describes how reads choose them.

**Change-log** (opt-in, `ChangeLog`): each backend writes one record per public mutation under a monotonic LSN in
the same `WriteBatch` as the data, so log and data cannot diverge on a crash. `ChangeFeed` / `ExportSince` read it.

**Replication.** A replica tails the primary's feed through `ApplyChange` / `ApplyChanges`, which reproduce the
primary's rows byte for byte (never re-minting IDs or re-stamping metadata), verify every hash, flush, and only then
advance the applied-LSN watermark. Replaying a record at or below the watermark is a no-op. A replica refuses
ordinary writes (`ErrReadOnlyReplica`).

**Import and export** treat their input as untrusted: every record is decoded through `storeutil.SafeUnmarshal`,
validated against the destination's limits and registries, and hash-verified before an entity is built.

---

## Design invariants

The contracts below hold in every backend. Each names the test that pins it; a change that breaks one must fail
that test.

| # | Invariant | Pinned by |
|---|---|---|
| 1 | History is append-only; no ordinary door rewrites or deletes a stored row. | `TestBitemporalOracleHarness` |
| 2 | Every temporal read resolves through one seam; named, generic and per-ID doors agree. | `TestTemporalTwoDoorsAgreeOnLabelQueries` |
| 3 | Superseded is not retracted: a `TxAt` pin sees every row recorded by then. | `bitemporal_supersession_test.go` |
| 4 | A retracted life vanishes from every later pin; earlier pins answer as before. | `TestRetraction_OldChainsAnswerAsBefore`, `TestRetractionOracle_NeverRecordedWorld` |
| 5 | Effective relationship doors agree edge for edge. | `TestEffectiveDoorsAgree` |
| 6 | Valid times compare through `ValidAxis`; an explicit legacy 0 is unset in every door. | `TestZeroValidFromIsUnsetInEveryDoor` |
| 7 | Transaction time is strictly monotonic, across reopen and replicas. | `TestNowTx_ReopenSafe`, `TestTxAtDoorsWhenTxClockIsAheadOfWall` |
| 8 | Validation happens before any side effect (ID, token, snapshot, queue). | `TestGraphTx_UpdateNodeValidatesUpdatesBeforeSnapshot` |
| 9 | Rollback restores the exact pre-transaction state, counters and registries included. | `TestTxRollbackRestoresExactPreTxState`, `TestGraphTx_RollbackRestoresStatsCounters` |
| 10 | A constraint holds through every write door. | `TestConstraintRelWithinEndpointsAllDoors` |
| 11 | A version's history row is published before its new current row. | `TestWithHistoryDoorsPublishHistoryBeforeCurrent` |
| 12 | Readers see buffered and in-flight writes consistently across doors. | `TestFlushingOverlay_PendingDeleteMasksMax_NodeDoorsAgree` |
| 13 | Every in-memory structure is rebuilt identically on reopen. | `TestCleanReopenMatchesFullRebuild` |
| 14 | Every persisted meta key has a reap policy. | `TestMetaKVReapPolicyCoversEveryKey` |
| 15 | Persisted counters must match the rows (an undercount heals, anything else fails closed). | `TestBadgerStoreLoadRejectsMismatchedPersistedCounters` |
| 16 | Persisted and imported bytes are untrusted: no decode panics or unbounded allocation. | `FuzzImport`, `FuzzWireToNodeChecked`, `TestSafeUnmarshalRejectsDeepNesting` |
| 17 | The change-log has exactly one record per public mutation door, in every backend. | `TestMemoryChangeLog_AllDoorsEmit` |
| 18 | A replica converges, re-applies idempotently and resumes after a restart. | `TestReplicaConvergence_IdempotentReapply`, `TestReplicaConvergence_DurableRestartResume` |
| 19 | A cascade delete applies nothing if its preflight fails. | `TestCascadeDeleteFatalPreflightAppliesNothing` |
| 20 | Tiered rotation persists the catalog before changing the topology. | `TestRotateHotShard_CatalogSaveFailure_RollsBackInMemory` |
| 21 | No tiered shard is closed under a pinned reader. | `TestTieredStore_ColdShard_IdleCloseBlockedByActiveRequest` |
| 22 | An entity's history follows it across shards and lives. | `TestHistoryLives_RelReimportOntoNewEventShard` |
| 23 | Index answers equal a brute-force scan. | `TestPropertyIndexAgreesWithBruteForceTwin` |
| 24 | Ingest pre-encoding is byte-identical to the store's own encode. | `TestIngestPreEncodedVsDisabledEquivalence` |
| 25 | A store zero value or closed store fails closed with `ErrStoreClosed`. | `TestBadgerStoreZeroValueLifecycleReturnsStoreClosed` |
| 26 | A store read snapshot, kept across many entities in any order, answers every read as the store's method (badger, sharded, tiered). | `TestReadSnapshotParityAcrossEntities` |
| 27 | The listing page doors hand `fn` exactly `ByLabelAndProperties`' matches (the value-list door: `ByLabelAndPropertyValues`' matches carrying every fixed pair) with `NodeEffectiveTimelines` at the pin, in any page size; a candidate is only a candidate. | `TestForEachNodeEffectiveByLabelAndPropertiesMatchesComposition`, `TestForEachNodeEffectiveByLabelAndPropertiesChecksTheRow`, `TestForEachNodeEffectiveByLabelAndPropertyValuesMatchesComposition` |
| 28 | A node's stored-size read lists exactly the rows `History` reads (plus the current row), with the stored value lengths. | `TestNodeStoredSizesMatchHistory`, `TestNodeStoredSizesBytesAreTheStoredValues` |

---

## Decision records

- **ADR-0006 Ingest pipeline.** Prepare (validation, encoding, ID mint) runs in parallel on producer threads; one
  applier per graph applies groups in order through the ordinary batch path, so every batch invariant holds.
  Apply owns `TxFrom`, LSNs, hashes, events and unique arbitration. Concurrent mode applies on the caller thread
  under the standalone locking discipline.
- **ADR-0007 Sharded store.** Slot topology routed by the ID's node field (section 5).
- **ADR-0008 Retention.** Whole expired event shards are dropped as files when they hold only the purged label;
  a shard with other labels is purged row by row.
- **ADR-0010 Foreign endpoints.** A relationship may name an end node on another machine (`AddByIDForeignEnd`,
  sharded store only).
- **ADR-0011 Column segments** (memory store, `Config.RelSegments`, shipped v4.37–v4.40). Bulk relationship types
  declared by the consumer are stored as immutable column segments (CSR adjacency, bit-packed integer pages,
  dictionary or plain string columns, SHA-256 group roots) instead of one row per relationship, within a RAM
  budget (default 256 MiB) and optionally as sealed files (`SegmentDir`). `ScanRelSegments` reads them column-wise.
- **ADR-0012 Three storage volumes and a global index** (tiered; designed 2026-10-23, Fable; built in the steps of
  `tasks/todo.md` SHARD, each marked here when released: steps 1 in v4.76.1, 2-8 in v4.78.0).
  - *Volumes.* A volume is a directory on a device: `hot` = `DataDir` (fast NVMe), `warm` = `Config.WarmDir` (SSD),
    `cold` = `Config.ColdDir` (block devices). Tier (catalog state) and volume are separate: rotation flips tiers at
    once, as today; a shard's volume follows its tier later, through the mover. Placement: `meta/` and the hot shard
    only on hot; the reference shard on hot or warm (`Config.ReferenceVolume`; never cold: every commit and every
    unique check reads it); the archive on any (`Config.ArchiveVolume`); event shards on any; a future global index
    only on hot. The catalog entry carries `Volume` (`""` = hot, `"warm"`, `"cold"`), resolved through the config at
    open, so a remount needs no catalog edit and a fourth volume needs no format change.
  - *A cataloged shard whose directory is missing fails the open* (`fs.ErrNotExist`); badger would otherwise create
    an empty shard and reads would answer "not found" (step 1). `ListShards` reports each shard's volume, directory
    and bytes on disk (step 2). *The config maps the volumes* (`WarmDir`, `ColdDir`, each its own directory;
    `ReferenceVolume`, `ArchiveVolume` for a new store's reference shard and archive), `New` fails when a cataloged
    shard's volume is not mapped, and no shard is made under a missing volume root (an unmounted device) (step 3).
    `tiered.Store.MoveShard(ctx, name, volume)` moves an event shard (step 4 closed, step 5 open): the copy keeps
    sparse files sparse (an all-zero 1 MB chunk is skipped), the shard's checkouts wait on its move gate, an open
    shard closes once its pins drain (flushing and syncing what a durable group holds: durable earlier, never later)
    and a warm one reopens from the new volume (demoted to cold if it cannot). `DurableFlush` and the listing never
    wait for a move: they pin the shard while it is open and skip it once closed. `ctx` or `Close` stops a move
    before its catalog save; a retention drop of a moved shard removes it on its volume. Each idle-sweep tick moves
    one shard a step down to its tier's volume (or the next faster one mapped), the oldest first, a shard whose last
    move failed after the others; a shard on a slower volume stays; `IdleTimeout` defaults to 5 minutes when a
    volume is mapped (step 6). `tiered.MoveShardOffline` moves the reference shard or the archive of a closed store,
    holding badger's directory lock on the reference shard for the move (step 7). `ShardInfo.ColdOpens` counts each
    lazy open of a closed cold shard, so a read's cold opens show as the difference around it (step 8). During
    the copy, reads go on from a read-only handle on the source; writes wait (MOVEREAD, v4.78.2).
  - *The mover* (one, for any event shard and any volume pair; one move per idle-sweep tick, or `tier.API.MoveShard`):
    a closed shard is copied file by file (byte-exact, fsync of files and directory) into
    `<volume>/events/<name>.moving-<nonce>`, renamed, the catalog saved with the new `Volume` (the commit point), then
    the source removed. An open (warm) shard is first quiesced: a `moving` gate makes its checkouts wait, pins drain,
    the shard closes (flushing), moves as above and reopens. MOVEREAD: once the shard is closed the mover opens a
    read-only handle on the source, and every checkout that only reads (`checkoutStoreReadOnly`, the read routings
    `shardForNodeIDRead`, `shardForRelIDReadWithArchive`) pins it instead of waiting; checkouts that may write
    (`checkoutStore`, `checkoutStoreForRead`) still wait. Before the source is removed (after the catalog save) or
    reopened writable (a stopped move), new reads wait again, the pins drain and the handle closes. A `DurableCommit` group needs no exclusion: the close
    syncs the writes it holds, and its `DurableFlush` pins or skips the shard instead of waiting.
    At open, orphans are removed by the catalog's word (`*.moving-*`; a copy on a volume the catalog does not name).
    The mover never fails the store: errors are logged and retried. The reference shard and the archive move only
    offline (`MoveShardOffline`).
  - *One-way door.* A catalog with a shard off the hot volume (the first move, or a new store with its reference
    shard or archive off hot) carries the entry `__format__` (kind `catalog/2`, no path); every v4 release before the
    volume minor refuses such a store at open ("invalid path") and writes nothing (checked with v4.77.2). A store
    that never left the hot volume stays byte-identical.
  - *Failure per volume.* SSD down: warm shards' reads and writes fail, hot writes and the reference shard (on NVMe)
    continue. Block device down: cold reads fail per read. `New` fails closed when a warm shard cannot open.
  - *Global index* (`DataDir/index/`, a badger on the hot volume; keys only, never rows): unique-key postings
    `(label, key, value) → ID`, label/property postings, and an owner map for relationships off their mint shard. A
    move changes nothing in it (postings name IDs, the catalog names volumes). Write order: posting first, row
    second, every read checks the row, so a crash leaves at most a dangling candidate, never a lost key. At open it
    replays each open shard's change-log above its watermark (requires `ChangeLog`); a full rebuild is an admin door.
    About 55 B per posting on NVMe and one more fsync per commit group. Its unique-key slice lets a keyed label be an
    event label (the key stays on NVMe, the row moves with its day), which shrinks the reference shard; built only on
    a measured need after ai-soc's data cleanup. Slice 1 built (UNIQIDX, v4.81.0; René 26 Oct, shaped by ai-soc): the
    unique-key owner index, `(label, key, value) -> ID` under `DataDir/index`, kept by core under its value locks
    (claim before row; readers check the claimant's row, so no replay and no change log needed); claims are synced
    once per durable shard flush (`badger.Config.BeforeFlushRows`), not per claim. Measured: about 18 B a key on
    disk; a hit 2.3-3.1 us however many shards; a keyed create in a transaction 19 us against 12 us on the reference
    shard without SyncWrites (`tasks/evidence/UNIQIDX/`).

- **ADR-0013 Valid-time placement** (TIERCLOCK; designed 2026-10-29 for ai-soc's full BA archive, 33 valid days,
  revised the same day after an independent review; not built, each step marked here when released). *Problem.*
  Placement and tiering follow the wall clock: core mints every ID from its generators before any store call, an
  event entity lives in the shard whose window holds its ID's time (no window: the hot shard), rotation and the
  `ColdAfter` demotion compare `time.Now()`, and `New` opens the hot shard for the wall-clock window. A replay of 33
  days in a few wall hours therefore lands in one hot shard that never rotates (k10291501: 122 M hot checkouts,
  none warm or cold), so every per-shard RAM structure (existence sets, TemporalIndex, value dictionaries, history
  idTables) grows with the whole archive instead of the open days; late data (ai-soc: 24 h) lands in today's shard.
  Live data hides it: wall time is close to valid time. Everything below is behind one option, `MintAtValidFrom`
  (tiered; default off), plus `Config.Clock`; with both off, behaviour and bytes are unchanged.
  - *Decision: the ID carries the valid day* (ai-soc's option a). Rejected, option b (route by a shard key beside
    the ID): a read by ID carries no key, so it needs an ID -> shard map and one more lookup per read.
  - *The store mints, not core.* Core mints before it knows a shard, and the catalog's windows are not calendar
    windows today (rotation cuts the old hot shard at the moment it runs). So with the option on, core asks the
    tiered store for event IDs (a store capability, as `PutNodeGeneratedID` already carries a minted ID): the store
    classifies the label (reference nodes keep today's generator), maps the instant to its catalog window and mints
    there. An event node's instant is its `ValidFrom`; an untimed node (no `ValidFrom`, or the untimed axis's
    literal 0) takes `Clock()`. A relationship lives with its start node, and a lookup by rel ID probes the
    reference shard, the archive, the ID-time shard, then every other shard (cold ones too), so an event
    relationship is minted inside its start node's window: at its `ValidFrom` when that falls there, else at the
    window's next free ID; a relationship on its mint shard (reference start node, `RelsByMint`) at its
    `ValidFrom`. Minting needs only the catalog, never a shard open.
  - *Calendar windows and rotation.* With the option on, windows are calendar windows (`ShardWindow` aligned in
    `ShardLocation`) and never overlap: rotation makes the window holding `Clock()` hot (creating its shard, or
    promoting it when data reached it first) and ends the old hot shard at its calendar end, not at the rotation
    instant; a clock jump over several windows leaves them without a shard until data reaches them; `New` picks
    the hot window by `Clock()` too (after a crash mid-replay the host's clock, not the wall's). A manual rotation
    inside a window (`RotateHotShard`, `ForceRotate`) is refused with the option on: it would cut a window under
    rows already minted past the cut. `Clock` must be monotonic; a step back is ignored. Idle close and access
    stamps keep the wall clock: they measure real idleness.
  - *Uniqueness.* The valid-time IDs use one node field reserved for them, the highest (31): with the option
    `graph.New` fails closed when a generator of its own would draw it (`SnowflakeNodeID` 15, whose pair is 30
    and 31, or `IngestLanes` above 29), and `sharded.Store`, whose slots must cover every minting field, opens
    badger shards only, so it has no such option. Per window the store keeps one high cursor for nodes and relationships alike (a
    value is never handed out twice, to either kind) and mints `max(instant, high + 1 step)`;
    when that would leave the window (instants crowding its last microsecond, or relationships placed at the next
    free ID) a low cursor walks down from the window's lowest ID; a window fills only when both meet (86.4 G
    microseconds x 1,024 steps a day). One mutex per window serializes its mints (producer lanes share it; a mint
    is a few comparisons). IDs are unique per store, as today's are per generator field.
  - *Restart and crash.* No ID may be handed out twice, also not one the host received before a crash lost its row
    (the wall-time generator never reuses one: its clock moved on). The cursors are leased, like badger's
    `Sequence`: the lease ends live in the reference shard's meta (always open and durable, beside the change-log
    watermark and the commit floor), keyed by window start, synced before any ID of a lease is returned; a lease
    spans 1 M steps, one synced write per million IDs. A restart starts each cursor at its lease end; a crash skips
    at most one lease. A window's lease keys are reaped with its shard (retention drop) and kept by `Clear`.
  - *A window without a shard is made on demand* (with the option on only; off, a concrete ID with no window still
    goes to the hot shard, as imports and replicas do today). The write path makes it under the lifecycle gate and
    `ts.mu.Lock`, saving the catalog before the topology changes (rotation's discipline); reads use a non-creating
    lookup and answer not-found. Its tier follows the clock (warm if it ends after `Clock() - ColdAfter`, else
    cold); it is made on the hot volume and the mover takes it to its tier's volume. Late data needs `ColdAfter`
    above the lateness plus one window (ai-soc: 24 h, day windows: 2 days or more): its day is then still warm.
  - *Relationship writes into cold days.* A relationship write pins its endpoints through the read checkout, which
    opens a cold shard writable and closes it again at check-in unless `MaxOpenColdShards` has room: every edge
    from an older-than-`ColdAfter` start node pays an open (~28 ms) and a close. The option requires
    `MaxOpenColdShards > 0`, and the endpoint checkout of a write keeps the handle for the idle sweep instead.
    Question for ai-soc: how often a new edge starts at a node older than `ColdAfter`.
  - *Retention.* With the option, `ByAge` purges by valid day: the ID's time is the valid day, by design the
    same immutable field it reads today. The catalog keeps a watermark, the largest `before` of any `ByAge` run
    (shard drops and row purges alike), set in memory and saved before a drop unlinks its shard; a write whose ID
    falls below it is refused with `ErrWindowDropped` (named, so the host can count its too-late records), checked
    under `ts.mu` where on-demand creation runs, so a write cannot remake a shard a drop is removing. The field
    raises the catalog's format sentinel (an older release refuses the catalog rather than dropping the field).
  - *Consequences.* The ID's time lies within the entity's valid day; it equals `ValidFrom` only for data minted in
    valid order (a record behind the window's cursor takes the cursor). It is also `idlayout.MintInstantMillis`,
    the last fallback of an entity's start time and of `tkg_created_at`: that fallback becomes the valid day.
    ID order within a window is mint order on the cursor, not arrival order across windows; nothing orders by ID
    as arrival (the change log orders by LSN, `TxFrom` comes from the commit clock). The snowflake epoch (2026-01-01
    UTC) and the end of the 48-bit microsecond field (2034-12-02 19:29 UTC) bound it: a `ValidFrom` outside is
    refused with `ErrValidFromOutsideIDRange` (ai-soc's BA, BO and XE data lie after the epoch; a customer
    backfill before 2026 is a later decision). `RelsByMint` falls back to the reference
    shard when the mint shard is cold; with the option it uses the window's shard, made or reopened.
  - *Steps.* 1 calendar windows and `Config.Clock` (rotation, demotion, `New`), manual rotation refused under the
    option (built, unreleased); 2 the store's minting capability with the reserved field, windowed cursors and leases in the
    reference shard, used by every generated-ID create in core (built, unreleased); 3 on-demand window shards, the retention watermark and `ErrWindowDropped`; 4 a replay test
    of 33 synthetic days (shards = days, open shards bounded by `ColdAfter`, resident sets flat) and ai-soc's BA
    replay as the measurement.