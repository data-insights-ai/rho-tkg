# API Documentation

## Core Types (`pkg/types`)

| Type | Purpose |
|------|---------|
| `Node` | Graph vertex with opaque `nodeID` (wraps `snowflake.ID`), primary + extra labels (`labelToken`), properties, version, temporal, integrity |
| `Relationship` | Directed edge with opaque `relID` (wraps `snowflake.ID`), type token (`relTypeToken`), start/end `nodeID`, properties, version, temporal, integrity |
| `PropertySlice` | Sorted key-value store with binary search; recursive validation rejects `tkg_` prefix keys and non-allowlisted types (pointers, structs, arrays, channels, functions, unsafe pointers) at any nesting depth; depth-limited to 32 levels |
| `Instant` | Semantic wrapper for Unix-millisecond timestamps used by all temporal fields |
| `TemporalMetadata` | Temporal lifecycle metadata: `ValidFrom`, `ValidTo`, `TxFrom`, `TxTo`, `CreatedAt`, `UpdatedAt`, `DeletedAt` (all `Instant`), `CreatedBy`, `UpdatedBy` (`string`), `BaseEntityID` (`snowflake.ID`) |
| `NodeIntegrity` / `RelIntegrity` | Hash-chain integrity: `Hash`, `PrevHash` (`string`); `AuthorID` (`string`), `Signature` (`[]byte`) for caller-supplied provenance. `RelIntegrity` also carries `FromNodeHash` / `ToNodeHash` — hashes of the endpoint nodes at write time (not fed into `ComputeRelHash`) |
| `TimeGranularity` | 8-level granularity enum: `GranMillisecond`, `GranSecond`, `GranMinute`, `GranHour`, `GranDay`, `GranWeek`, `GranMonth`, `GranYear`. Functions: `TruncateInstant(t, gran)`, `RoundInstant(t, gran)`, `CeilInstant(t, gran)` |
| `AllenRelation` | Full 13-relation Allen's interval algebra: `Relate(a, b Interval) AllenRelation`, `ComposeSets(rs1, rs2 AllenSet) AllenSet`. Constants: `Before`, `Meets`, `Overlaps`, `Starts`, `During`, `Finishes`, `Equal` and their converses |
| `RecurrencePattern` | Recurring time pattern: `Frequency` (`Daily`/`Weekly`/`Monthly`/`Yearly`), `Days` (`WeekdayMask`), `DayOfMonth` (1–28; 0 = last), `Month`, `DayStart`/`DayEnd` (duration from UTC midnight). `Validate()` checks invariants; `Expand(from, to Instant) []Interval` emits concrete intervals within the window. Returns `ErrInvalidTimeRange` if `from >= to` |
| `HashableValue` | Interface (`HashBytes() []byte`) that custom property struct types implement to participate in node/relationship integrity hashing. Register the type via `RegisterPropertyStructType(v any)`. Treat `HashBytes` like a wire format — once any property of the type has been written, you cannot change the encoding without breaking every existing hash chain that contains the value. List registered types via `RegisteredPropertyStructTypes() []string`. |
| `NodeID` / `RelID` / `EntityID` | Typed wrappers around `snowflake.ID` (`type NodeID snowflake.ID`, etc.). Boundary accessor: `(NodeID).SnowflakeID() snowflake.ID`. `EntityID` is the type-agnostic wrapper used by `Event.EntityID`, `BatchError.ID`, `QueryOpts.After`, and `TemporalMetadata.BaseEntityID`. Zero-value semantics: literal `0` is the universal sentinel for "unset" across the API. |

## Graph Layer (`pkg/graph`) — v3.4 sub-API surface

`*graph.Graph` is a thin façade. Its only direct methods are `New(Config) (*Graph, error)` and `Close() error`. Everything else is reached through 13 sub-API field accessors on the Graph value:

| Field | Package | Purpose |
|-------|---------|---------|
| `g.Nodes` | `pkg/graph/nodes` | Node CRUD, property/label mutation, version chain, queries |
| `g.Rels` | `pkg/graph/rels` | Relationship CRUD, adjacency, property mutation, version chain |
| `g.Temporal` | `pkg/graph/temporal` | Point-in-time, interval, bitemporal queries; snapshot/diff; Allen relations |
| `g.Index` | `pkg/graph/index` | Property / temporal / high-frequency / vector index management; vector search; IndexProvider registration |
| `g.Events` | `pkg/graph/events` | Sync / async EventBus management |
| `g.Constraints` | `pkg/graph/constraints` | Temporal-constraint set management |
| `g.IO` | `pkg/graph/io` | Export, Import, ImportWithOptions (StagingDir, MaxStagedBytes) |
| `g.Admin` | `pkg/graph/admin` | Archive/Restore, ForceRotate, ListShards, RebuildCatalog, Repair, VerifyShard, Reset, DecomposeID |
| `g.Stats` | `pkg/graph/stats` | NodeCount, RelCount, NodeCountByLabel, RelCountByType, AllLabelCounts, AllRelTypeCounts, full GraphStats snapshot |
| `g.Hash` | `pkg/graph/hash` | VerifyNodeChain, VerifyRelChain (shadows stdlib `hash` — alias as `tkghash` at consumer sites that also import stdlib `hash`) |
| `g.Resolve` | `pkg/graph/resolve` | NodeProperty / RelProperty (shadow keys), LabelToken / RelTypeToken (GetOrCreate), LookupLabel / LookupRelType |
| `g.Tx` | `pkg/graph` (in-package) | BeginTx, Run(fn), RunContext(ctx, fn) — panic-safe |
| `g.Batch` | `pkg/graph` (in-package) | New() returns a *BatchBuilder for queue-then-Execute |

`Store` / `memory.Store` / `badger.Store` / `tiered.Store` types live in `pkg/graph/store{,/memory,/badger,/tiered}`. The Store interface is composed from capability sub-interfaces in `pkg/graph/store/capabilities.go` — the graph layer depends on `MandatoryStore` (Lifecycle, NodeCRUD, RelCRUD, Adjacency, BulkRead, Batch, History, Stats, Iteration). Optional capabilities (`PropertyIndexCapability`, `TemporalIndexCapability`, `VectorIndexCapability`, `HighFrequencyIndexCapability`, `FilteredVectorSearchCapability`) are type-asserted at the call sites that need them and surface `ErrCapabilityNotSupported` when a backend omits them. The graph layer falls back to label-scan + property filter for `g.Nodes.ByLabelAndProperty` when `PropertyIndexCapability` is absent (correctness, not just acceleration).

### Entity management

- `g.Nodes.Add(labels, props)` / `g.Nodes.AddWithContext(ctx, labels, props)` / `g.Nodes.Import(ctx, id, labels, props)` (caller-supplied ID).
- `g.Rels.Add(typeName, start, end, props)` / `AddWithContext(...)` / `AddByID(typeName, startID, endID, props)` / `AddByIDWithContext(...)` / `AddByIDIfAbsent(typeName, startID, endID, props)` / `AddByIDIfAbsentWithContext(...)` / `Import(ctx, id, typeName, start, end, props)`.
- `g.Nodes.Update(id, updates)` / `UpdateWithContext(ctx, ...)` / `g.Rels.Update(...)` and `UpdateWithContext`.
- `g.Nodes.UpdateInPlace(id, updates)` / `UpdateInPlaceWithContext(...)` and rel mirrors. In-place updates mutate without a version bump or history entry — for correcting metadata.
- `g.Nodes.Delete(id)` (cascade) / `DeleteWithContext(ctx, id)`. `g.Rels.Delete(id)` / `DeleteWithContext`.
- `g.Nodes.AddLabel(id, label)` / `RemoveLabel(id, label)` (`ErrLastLabel` if it is the only label, `ErrLabelNotFound` if not present).
- High-throughput rel creation: `g.Rels.AddByID*` skips the two endpoint `GetNode` + `DeepCopy` calls. Trade-off: `FromNodeHash`/`ToNodeHash` are empty (no endpoint integrity capture), and temporal constraints against endpoint nodes are not checked.

### Properties

- `g.Nodes.SetProperty(id, key, value)` / `DeleteProperty(id, key)` / `g.Rels.SetProperty(...)` / `DeleteProperty`.
- `g.Nodes.CompareAndSetProperty(id, key, expected, newVal)` / `CompareAndSetPropertyWithContext(...)` — atomic CAS. Returns `(true, nil)` on match+update, `(false, nil)` on mismatch, `(false, error)` on real error. `expected == nil` means "must not exist"; `newVal == nil` means "delete". Comparison is `reflect.DeepEqual` — types must match exactly. Successful CAS bumps version, writes history, updates temporal metadata.

### Resolution

- Label/type helpers: `g.Nodes.Labels(n)`, `g.Nodes.PrimaryLabel(n)`, `g.Nodes.HasLabel(n, label)`, `g.Rels.Type(r)`, `g.Rels.HasType(r, typ)`.
- Shadow-property resolution (21 `tkg_*` keys): `g.Resolve.NodeProperty(n, key)`, `g.Resolve.RelProperty(r, key)`.
- Registry: `g.Resolve.LabelToken(name)` / `RelTypeToken(name)` (GetOrCreate), `g.Resolve.LookupLabel(name)` / `LookupRelType(name)` (no creation).

### Reads

- Single-entity: `g.Nodes.Get(id)` / `GetWithContext(ctx, id)` / `g.Rels.Get(id)` / `GetWithContext`.
- Bulk: `g.Nodes.GetByIDs(ids)` / `g.Rels.GetByIDs(ids)` (sorted ascending, missing IDs silently skipped).
- Label/type: `g.Nodes.ByLabel(label, opts)` / `g.Rels.ByType(typeName, opts)`.
- Adjacency: `g.Rels.Outgoing(nodeID, typeName)` / `Incoming(...)` / `OutgoingForNodes(nodeIDs, typeName)` / `IncomingForNodes(...)`. Empty `typeName` matches all types.
- All: `g.Nodes.All(opts)` / `g.Rels.All(opts)`.
- Property: `g.Nodes.ByLabelAndProperty(label, key, value, opts)` — uses the property index when present (R2-F4 graph-layer fallback scans label set + filters property when `PropertyIndexCapability` is absent).
- Counts: `g.Nodes.Count()` / `g.Rels.Count()` / `g.Nodes.CountByLabel(label)` / `g.Rels.CountByType(typeName)`. Statistics surface: `g.Stats.NodeCount()`, `RelCount()`, `NodeCountByLabel`, `RelCountByType`, `AllLabelCounts()`, `AllRelTypeCounts()`, full `g.Stats.Get()` snapshot (counters + Badger cache metrics).
- `QueryOpts{Limit, After, ValidAt, ValidStart, ValidEnd, Depth}`: cursor pagination, temporal push-down, and shard-depth filtering. Zero values mean "all". `Depth` (`tiered.Store` only) is `DepthAll`/`DepthHot`/`DepthWarm`; ignored by `memory.Store`/`badger.Store`. Combining a temporal filter with `Depth != DepthAll` returns `ErrDepthTemporalUnsupported`.

### Batch operations

`g.Batch.New()` returns a `*BatchBuilder` that queues operations with eager validation. Queue methods: `AddNode(labels, props)`, `AddRelationship(typeName, start, end, props)`, `UpdateNode(id, updates)`, `UpdateRelationship(id, updates)`, `DeleteNode(id)`, `DeleteRelationship(id)`. Call `Execute()` to persist all queued operations in order (creates → updates → deletes).

`Execute()` is **per-operation reporting, not all-or-nothing**: it returns `*BatchResult{Created, Updated, Deleted, Failed, Errors []BatchError}` and a typically-`nil` error (only structural failures bubble up; per-op failures are recorded in `Errors`). Callers MUST inspect `result.Failed` and `result.Errors`. The endpoint-lock window for relationship creation is panic-safe via a defer-backed UnlockTwo (R2-F3).

### Temporal queries (`g.Temporal`)

- Point-in-time: `g.Temporal.NodeAt(id, t)`, `RelAt(id, t)`, `NodesAt(t)`, `RelationshipsAt(t)`, `NodesByLabelAt(label, t)`, `RelationshipsByTypeAt(relType, t)`, `NeighborsAt(nodeID, t)`, `NodesByLabelPropertyAt(label, key, value, t)`, `RelsByTypePropertyAt(relType, key, value, t)`.
- Interval: `NodesDuring(start, end)`, `RelationshipsDuring(start, end)`, `NodesByLabelPropertyDuring(label, key, value, start, end)`, `RelsByTypePropertyDuring(relType, key, value, start, end)`.
- Bitemporal (transaction time): `NodeAsOf(id, txTime)`, `RelAsOf(id, txTime)`, `NodesAsOf(txTime)`, `RelsAsOf(txTime)`. Returns `ErrNoVersionAsOf` if no version was committed at or before `txTime`.
- Snapshot/Diff: `Snapshot(t) (*GraphSnapshot, error)` (full graph state at `t`, endpoints-filtered). `Diff(t1, t2) (*SnapshotDiff, error)` returns `NodesCreated`, `NodesUpdated [{Before, After}]`, `NodesDeleted`, `RelsCreated`, `RelsUpdated`, `RelsDeleted`. `DiffCallback(t1, t2, handlers)` streams the same diff via `DiffHandlers` (each handler optional).
- Allen relations: `NodeInterval(n)`, `RelInterval(r)`, `RelateNodes(a, b)`, `RelateRels(a, b)` — return `(start, end Instant, err error)` for the interval helpers and `AllenRelation` for the relation helpers. Allen relations require finite intervals (ValidTo != 0).

All temporal queries are history-aware (include deleted entities via lazy ForEach iterators). Nodes without explicit temporal metadata derive valid-from from their snowflake ID timestamp.

### Version chain navigation

- `g.Nodes.History(id)` / `g.Rels.History(id)` — full version chain.
- `g.Nodes.PreviousVersion(id, version)` / `NextVersion(id, version)` and rel mirrors. Return `nil, nil` when the requested version does not exist.
- `g.Nodes.CloseVersion(id, t)` / `g.Rels.CloseVersion(id, t)` — set `ValidTo` on the current entity in-place without incrementing its version or creating a history entry. Returns `ErrAlreadyClosed` if `ValidTo` is already set.

### Events (`g.Events`)

- Sync bus: `bus := events.NewEventBus()`, `g.Events.SetSync(bus)`, `g.Events.GetSync()`. `bus.Subscribe(handler)` returns an idempotent unsubscribe. Handlers run synchronously after each successful store write, outside the bus lock.
- Async bus: `bus := events.NewAsyncEventBus(AsyncEventBusConfig{Workers, QueueSize, Backpressure})`, `g.Events.SetAsync(bus)`. Three backpressure strategies: `BackpressureBlock`, `BackpressureDropOldest`, `BackpressureDropLatest`. Worker drains in Critical→High→Normal→Low→Deferred priority order. `bus.Close()` drains pending events and stops workers.
- Six event types: `EventNodeCreate`, `EventNodeUpdate`, `EventNodeDelete`, `EventRelCreate`, `EventRelUpdate`, `EventRelDelete`. Internal priorities: creates → `PriorityHigh`, deletes → `PriorityCritical`, updates → `PriorityNormal`. Five priority levels (zero value = `PriorityNormal`).

### Hash chain verification (`g.Hash`)

- `g.Hash.VerifyNodeChain(id) (bool, error)` / `VerifyRelChain(id) (bool, error)`. Tolerates deleted entities (verifies history alone when current entity is gone).

### Transactions (`g.Tx`) and the imperative tx surface

- Functional form: `g.Tx.Run(func(tx *graph.GraphTx) error { ... })` / `g.Tx.RunContext(ctx, fn)`. Panic-safe — a panic inside fn rolls the tx back via defer and propagates the panic; rollback errors are joined with the caller's error via `errors.Join`.
- Imperative form: `tx := g.Tx.Begin()`. Methods on `*GraphTx`:
  - Mutations: `AddNode`, `AddRelationship`, `AddRelationshipByID`, `AddRelationshipByIDIfAbsent` (returns `(rel, created, err)`), `UpdateNode`, `UpdateRelationship`, `SetNodeProperty`, `DeleteNodeProperty`, `SetRelationshipProperty`, `DeleteRelationshipProperty`, `DeleteNode` (cascade), `DeleteRelationship`.
  - Reads: `GetNode(id)` (safe inside tx — tx holds the write lock).
  - Lifecycle: `Commit()` releases the lock and dispatches buffered events. `Rollback()` restores all mutations in reverse order (deleted entities re-created from snapshots, updates reverted, created entities deleted) and discards buffered events. Sentinel: `ErrTxDone`.
  - Inspection: `CreatedNodeIDs()`, `CreatedRelIDs()`.

### Indexes (`g.Index`)

- Property: `g.Index.CreateProperty(label, propertyKey)` / `DropProperty(label, propertyKey)`. `g.Nodes.ByLabelAndProperty(label, key, value, opts)` uses the index when present, falls back to label scan + property filter otherwise (R2-F4).
- Temporal interval: `g.Index.CreateTemporal(label)` / `DropTemporal(label)`. Accelerates `g.Nodes.ByLabel(label, opts)` when a temporal filter is set.
- High-frequency temporal: `g.Index.CreateHighFrequency(label, bucketSize)` / `DropHighFrequency(label)`. Time-bucketed alternative for high-write-rate event labels. Only one temporal index type per label at a time. Not persisted.
- Vector: `g.Index.CreateVector(label, propertyKey, dims, metric)` / `DropVector(label, propertyKey)`. `g.Index.SearchNearest(label, propertyKey, query []float32, k int, opts)` returns the top-k nearest nodes. Under temporal filters, the graph pre-filters when the backend implements `FilteredVectorSearchCapability`; otherwise iterative over-fetch escalates k until k eligible results are accumulated or the backend is exhausted (R2-F5). `metric` is `DistanceCosine` or `DistanceEuclidean`. Errors: `ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`.
- IndexProvider plugin: `g.Index.RegisterProvider(p)` / `RegisterLegacyProvider(lp)` (deprecated) / `UnregisterProvider(name)` / `Providers() []string`. Auto-creates a sync `events.EventBus` if none attached. Sentinels: `ErrIndexProviderExists`, `ErrIndexProviderNotFound`, `ErrIndexProviderEmptyName`.

### Constraints (`g.Constraints`)

- `g.Constraints.Add(c)`, `Set(cs)`, `Get()`. `TemporalConstraint{Kind: ConstraintRelWithinEndpoints}` enforces that a relationship's validity is contained within both endpoint nodes' validity. Checked during relationship creation and import. Errors wrap `ErrTemporalConstraint` (sub-sentinels: `ErrRelBeforeStartNode`, `ErrRelAfterEndNode`, `ErrRelExceedsStartNodeValidity`, etc.).

### IO (`g.IO`)

- `g.IO.Export(w io.Writer) error` — length-prefixed msgpack record stream with 1-byte type tags. Forward-compatible (unknown tags skipped on import). Holds `g.mu.RLock` for a consistent snapshot.
- `g.IO.Import(r io.Reader) error` — defaults to platform temp dir for staging, no size cap.
- `g.IO.ImportWithOptions(r io.Reader, opts io.ImportOptions) error` — `ImportOptions{StagingDir, MaxStagedBytes}`. Memory is `O(maxExportRecordSize)` regardless of export size; staging file is sized to match the export and removed via defer at exit. Phase-1 errors (read, staging-disk write, MaxStagedBytes exceeded) leave graph state unchanged. Phase-2 (replay) errors may leave a partially populated graph — for transactional restore, import into a fresh graph and swap stores on success. Sentinels: `ErrIncompatibleExport`, `ErrIncompatibleRegistry`, `ErrImportSizeLimit`, `ErrCorruptExport`. Per-record allocations capped at 128 MiB.

### Admin (`g.Admin`, `tiered.Store`-only unless noted)

- `g.Admin.Archive(id)` / `Restore(id)` — move a reference node and its rels between the reference shard and the archive (under `g.mu.Lock`).
- `g.Admin.ForceRotate()` — transactional hot-shard rotation: opens the new shard + temporal indexes, snapshots the catalog, mutates catalog in-memory, persists; on Save failure, restores the catalog snapshot, closes + removes the new shard, returns the error (R2-F2).
- `g.Admin.ListShards()` — `[]ShardInfo` with live counts (under `g.mu.RLock`).
- `g.Admin.RebuildCatalog()` — reconstruct the shard catalog from live state (under `g.mu.Lock`).
- `g.Admin.Repair()` — scan + fix cross-shard split-write inconsistencies (under `g.mu.Lock`, R2-F1).
- `g.Admin.VerifyShard(name)` — hash chain verification with immutable-shard caching (under `g.mu.RLock`).
- `g.Admin.Reset()` — clears all entities, indexes, history, counters; preserves registries. Works on every backend (forwards to `store.Clear`).
- `g.Admin.DecomposeID(id snowflake.ID) IDComponents{CreatedAt, NodeID, Sequence}` — works with any store type.

Non-tiered backends return `ErrNotTieredStore` from the seven tiered-only methods.

### Migration

- `tiered.MigrateFromBadger(src *badger.Store, dst *tiered.Store, labels OntologyMapping) error` — copy all entities from a single badger.Store to a tiered.Store with automatic ontology-based routing.

### Validation limits

- `Config.Validation` accepts `ValidationLimits{MaxLabelsPerNode, MaxPropertiesPerEntity, MaxPropertyKeyLength, MaxPropertyValueSize, MaxNameLength, AllowSelfLoops}`. Defaults: 50 / 1000 / 256 / 65536 / 256 / false. `AllowSelfLoops=false` causes `g.Rels.Add` and `g.Rels.Import` to reject self-loops with `ErrSelfLoop`. Zero values use defaults.

### Lifecycle

- `g.Close() error` — saves registries (badger.Store / tiered.Store), then calls `store.Close()`. `tiered.Store.Close()` saves catalog and closes all event shards plus the reference shard and archive. Idempotent — safe to call multiple times.
