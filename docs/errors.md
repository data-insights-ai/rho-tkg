# Error Sentinels Reference

This page documents public error sentinels returned by the temporal knowledge graph API. Each sentinel is a unique identity usable with `errors.Is()` to classify errors by type.

Coverage spans three sources:

- **`pkg/graph` re-exports** — sentinels aliased into `pkg/graph/errors.go` so a single `import "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"` reaches them via `graph.ErrXxx`. Most sections below are this source.
- **`pkg/graph/store`-only sentinels** — declared in `pkg/graph/store/errors.go` but NOT aliased into `pkg/graph`. A handful of these leak through public `Graph` methods anyway (documented per-row below); callers must `import "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"` and check `errors.Is(err, store.ErrXxx)` — `errors.Is(err, graph.ErrXxx)` will not match. See "Store-Internal Sentinels" below.
- **`pkg/types` sentinels** — declared in `pkg/types/*.go`. Some are aliased verbatim into `pkg/graph/internal/core` and therefore reachable as `graph.ErrXxx` too (documented both places, cross-referenced); the rest are `pkg/types`-only, surfaced by direct `pkg/types` API calls (`Node`/`Relationship` methods, `PropertySlice`, `RegisterPropertyStructType`, Allen-relation helpers, temporal-value validation) that a caller can reach without going through the Graph façade at all. See "pkg/types Sentinels" below.

## Store — Entity Queries

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrNodeNotFound` | store | Node does not exist in the graph | `g.Nodes().Get()`, `g.Temporal().NodeAt*()`, all rel mutations requiring node validation |
| `ErrRelNotFound` | store | Relationship does not exist | `g.Rels().Get()`, `g.Temporal().RelAt*()` |
| `ErrNodeExists` | store | Node with the caller-supplied ID already exists | `g.Nodes().Import()`, `g.IO().Import()` (when ID collision occurs) |
| `ErrRelExists` | store | Relationship with the caller-supplied ID already exists | `g.Rels().Import()`, `g.IO().Import()` (when ID collision occurs) |

## Store — Indexes

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrIndexExists` | store | Property index already exists at the given name (node OR relationship) | `g.Index().CreateProperty()`, `g.Index().CreateRelProperty()` |
| `ErrIndexNotFound` | store | Property index not found (also returned by the range doors when no usable index exists, so callers fall back to a scan) | `g.Index().SearchNearest()`, `g.Index().DeleteRelProperty()`, `g.Rels().ForEachByTypePropertyRange()`, index mutation/removal doors |
| `ErrRelPropertyIndexUnsupported` | store | The backend recognizes relationship property indexes but declines to CREATE them (the tiered store — rel values are scattered across timestamp-routed event shards). Query still works via the type-scan fallback. Distinct from `ErrCapabilityNotSupported` | `g.Index().CreateRelProperty()` on a tiered-backed graph |
| `ErrTemporalIndexExists` | store | Temporal index already exists | `g.Index().CreateTemporal()`, `g.Index().CreateHighFrequency()` |
| `ErrTemporalIndexNotFound` | store | Temporal index not found | `g.Index().DeleteTemporal()`, `g.Index().DeleteHighFrequency()` |
| `ErrVectorIndexExists` | store | Vector index already exists | `g.Index().CreateVector()` |
| `ErrVectorIndexNotFound` | store | Vector index not found | `g.Index().SearchNearest()` |
| `ErrDimensionMismatch` | store | Vector dimension does not match the index | `g.Index().SearchNearest()` |
| `ErrInvalidTemporalIndexConfig` | store | Temporal index configuration is invalid (non-positive or fractional-millisecond high-frequency bucket size) | `g.Index().CreateHighFrequency()` |
| `ErrInvalidVectorIndexConfig` | store | Vector index configuration is invalid | `g.Index().CreateVector()` |
| `ErrInvalidVectorValue` | store | Vector value is invalid (NaN, Inf, wrong dimension) | `g.Index().SearchNearest()`, vector index query paths |
| `ErrInvalidShardDepth` | store | Shard depth is out of valid range | Any query door with an unknown `QueryOpts.Depth` (`g.Nodes().ByLabel()` / `All()`, `g.Rels().ByType()` / `All()`, index searches) |
| `ErrInvalidQueryLimit` | store | Query limit is negative (`0` = unbounded; there is no upper cap) | Scan/query paths with limits |
| `ErrInvalidQueryCursor` | store | Pagination cursor is invalid | Paginated query paths |

## Indexes — Provider Registration

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrIndexProviderExists` | index | An index provider is already registered with that name | `g.Index().RegisterProvider()` |
| `ErrIndexProviderNotFound` | index | No index provider registered with that name | `g.Index().SearchNearest()` when provider not found |
| `ErrIndexProviderEmptyName` | index | Index provider name cannot be empty | `g.Index().RegisterProvider()` |
| `ErrOrderedScanTemporal` | store | **LEGACY — no longer returned by anything.** The ordered / prefix range doors used to decline temporal `QueryOpts` with it; they now SERVE those opts via a sound full fold (values resolved at the pin, then sorted — see query-planners.md). Kept exported so an `errors.Is` match still compiles | none (historically `g.Nodes().ForEachByLabelPropertyRangeOrdered()`) |
| `ErrRelPropertyIndexUnsupported` | store | Backend lacks relationship property-index capability | `g.Index().CreateRelProperty()`, `g.Rels().ByTypeAndProperty()` |

## Registry

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrEmptyName` | registry | Label, relationship type, or property key name cannot be empty | `g.Nodes().Add*()` with blank label, `g.Rels().Add*()` with blank type |
| `ErrRegistryNotEmpty` | registry | Registry cannot be cleared while entities exist | `g.Admin().Reset()` when data remains |

## Temporal & Constraints

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrValidFromBeforePrevious` | core | `tkg_valid_from` must be strictly greater than the previous version's valid-from | `g.Nodes().Update()`, `g.Rels().Update()` with explicit `tkg_valid_from` |
| `ErrNoVersionAsOf` | core | No entity version recorded at the given transaction time | `g.Temporal().NodeAsOf()`, `g.Temporal().RelAsOf()` |
| `ErrNoVersionValidAt` | store | The entity is known (current or historical rows exist) but no version's effective valid-time interval covers the requested instant. Aliases `store.ErrNoVersionValidAt` — previously leaked raw from these four doors with no `pkg/graph` alias | `g.Temporal().NodeAt()`, `g.Temporal().RelAt()`, `g.Temporal().NodeAtTx()`, `g.Temporal().RelAtTx()` |
| `ErrConflictingTemporalOpts` | core | `QueryOpts.TxPin` (the belief-state / knowledge-time pin) was set together with a valid-time filter (`ValidAt` / `ValidStart` / `ValidEnd`) or with `TxAt`; TxPin resolves like `NodesAsOf` with NO valid-time filtering, so combining it with any other temporal filter is contradictory and rejected rather than silently mis-resolved | `g.Nodes().All()` / `ByLabel()`, `g.Rels().All()` / `ByType()` with a conflicting `QueryOpts` |
| `ErrVectorSearchTxPinUnsupported` | core | `QueryOpts.TxPin` was set on a vector search. The vector index holds only the LATEST vector per node and drops deleted nodes entirely, so a belief-state (`AS OF SYSTEM TIME`) nearest-neighbor ranking is ill-defined — a node hard-deleted after the pin would be silently missing and distances would rank by post-pin vectors; the door refuses loudly instead | `g.Index().SearchNearest()` (and the tx-side mirror) with `QueryOpts.TxPin` set |
| `ErrInvalidClockAdvance` | core | The `AdvanceClock` target lands implausibly far ahead of wall-clock (more than the ~10-year skew tolerance) — guards against a unit/scale mixup (e.g. microseconds passed where milliseconds are expected) permanently poisoning the transaction clock | `g.Temporal().AdvanceClock()` |
| `ErrTemporalConstraint` | temporal | A temporal constraint was violated (interval predicate failed) | `g.Rels().Add*()` / `g.Rels().Update()` / `g.IO().Import()` with a configured constraint set |
| `ErrInvalidTemporalConstraint` | temporal | Temporal constraint definition is invalid (unknown kind, including the zero-value `TemporalConstraint{}`; also a nil `ConstraintSet.ForEach` callback) | `g.Constraints().Add()`, `g.Constraints().Set()` |
| `ErrRelBeforeStartNode` | temporal | Relationship begins before its start node is valid | `g.Rels().Add*()` with temporal validation enabled |
| `ErrRelBeforeEndNode` | temporal | Relationship begins before its end node is valid | `g.Rels().Add*()` with temporal validation enabled |
| `ErrRelAfterStartNode` | temporal | Relationship begins after its start node expires | `g.Rels().Add*()` with temporal validation enabled |
| `ErrRelAfterEndNode` | temporal | Relationship begins after its end node expires | `g.Rels().Add*()` with temporal validation enabled |
| `ErrRelExceedsStartNodeValidity` | temporal | Relationship validity extends beyond start node validity | `g.Rels().Add*()`, `g.Rels().Update()` with temporal validation |
| `ErrRelExceedsEndNodeValidity` | temporal | Relationship validity extends beyond end node validity | `g.Rels().Add*()`, `g.Rels().Update()` with temporal validation |

## Entity Validation

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrNoLabels` | core | Node requires at least one label | `g.Nodes().Add*()`, `g.Nodes().RemoveLabel()` (on last label) |
| `ErrNilNode` | core | Pointer to Node is nil (canonically declared in `pkg/types` as `ErrNilNode` — same identity; see pkg/types Sentinels below) | All paths accepting `*Node` values |
| `ErrNilRelationship` | core | Pointer to Relationship is nil (canonically declared in `pkg/types` as `ErrNilRelationship` — same identity; see pkg/types Sentinels below) | All paths accepting `*Relationship` values |
| `ErrZeroID` | core | Entity ID is zero (invalid for import/creation) | `g.Nodes().Import()`, `g.Nodes().AddByIDIfAbsent()`, `g.Rels().Import()`, `g.IO().Import()` |
| `ErrInvalidID` | core | Entity ID is invalid | `g.IO().Import()` with corrupted IDs |
| `ErrVersionOverflow` | core | Entity version counter overflowed | `g.Nodes().Update()`, `g.Rels().Update()` after 2^32 mutations |
| `ErrLabelNotFound` | core | Node does not have the specified label | `g.Nodes().RemoveLabel()` with non-existent label |
| `ErrLastLabel` | core | Cannot remove the last label from a node | `g.Nodes().RemoveLabel()` on sole label |
| `ErrSelfLoop` | core | Relationship cannot start and end at the same node (when disallowed) | `g.Rels().Add*()` with `start == end` and `AllowSelfLoops=false` |
| `ErrForeignEndpointUnsupported` | core | Cross-machine (foreign-endpoint) edges require a partitioned (sharded) store | `g.Rels().AddByIDForeignEnd()` on a non-sharded store |
| `ErrForeignEndpointConstraint` | core | Temporal constraints cannot be enforced on a cross-machine edge (foreign end not locally available) | `g.Rels().AddByIDForeignEnd()` with temporal constraints configured |
| `ErrInvalidForeignEndpoint` | store | Malformed `ForeignEndpoint` descriptor (zero node ID, empty attested hash, or zero attest-time) | `g.Rels().AddByIDForeignEnd()` with an invalid descriptor |
| `ErrTooManyLabels` | core | Node exceeds max label count (default 50) | `g.Nodes().Add*()` with excess labels |
| `ErrTooManyProperties` | core | Entity exceeds max property count (default 1000) | `g.Nodes().Add*()`, `g.Nodes().Update()`, `g.Rels().Add*()`, `g.Rels().Update()` |
| `ErrKeyTooLong` | core | Property key exceeds max length (default 256) | `g.Nodes().Add*()`, `g.Nodes().Update()`, `g.Rels().Add*()`, `g.Rels().Update()` |
| `ErrValueTooLarge` | core | Property value exceeds max size (default 64 KiB) | `g.Nodes().Add*()`, `g.Nodes().Update()`, `g.Rels().Add*()`, `g.Rels().Update()` |
| `ErrNameTooLong` | core | Label, relationship type, or property key name exceeds max length (default 256) | `g.Nodes().Add*()`, `g.Rels().Add*()`, index names |

## Graph Lifecycle

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrGraphClosed` | core | Graph is closed (no operations permitted) | All API calls after `g.Close()` |
| `ErrAlreadyClosed` | core | Entity is already closed | Direct entity-level close calls (uncommon) |
| `ErrNotTieredStore` | core | Operation requires a tiered store but a different backend is configured | `g.Tier().Archive()`, `g.Tier().Restore()` on non-tiered backends |
| `ErrReadOnlyReplica` | core | Write operation on a read-only replica | `g.Nodes().Add*()`, mutations on `ReadOnlyReplica=true` graph |
| `ErrNilGraph` | core | Pointer to Graph is nil | Chained accessor calls on nil graph |
| `ErrNilStore` | core | Store pointer is nil | Graph construction with nil store |

## Context & Transactions

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrNilContext` | core | Context is nil; context.Background() required | All methods requiring `context.Context` |
| `ErrNilTxCallback` | core | Transaction callback is nil | `g.Tx().Run()`, `g.Tx().RunContext()` with nil callback |
| `ErrBatchFailed` | core | Batch had one or more failed operations | `g.Batch().Execute()` |
| `ErrBatchDone` | core | Batch already executed (cannot reuse) | Multiple `g.Batch().Execute()` calls on the same batch |
| `ErrInvalidTimeRange` | core | Supplied time range is invalid (start >= end or negative bounds). Aliases `store.ErrInvalidTimeRange`. Distinct identity from `types.ErrInvalidTimeRange` (see pkg/types Sentinels below) despite the identical name | `g.Temporal().NodesDuring()`, `g.Temporal().RelsDuring()`, `QueryOpts.ValidStart`/`ValidEnd` validation, `g.Nodes().CloseVersion()` with `t == 0` |

## Ingest Pipeline (ADR-0006)

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrIngestClosed` | core | The ingest pipeline (graph + applier) is closed before the work could be applied — an enqueue racing `Close` is rejected cleanly with this sentinel (never accepted-then-dropped, never hung) | `g.Ingest()` `Session.Submit()` / `Session.Close()`, `g.Ingest().WaitApplied()` |
| `ErrNilSession` | core | An `ingest.Session` method was called on a nil `*Session`. `ingest.Session` is a type alias for `core.Session` (not a wrapper), so this is reachable directly through the public surface | Any `*ingest.Session` method (`AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `Submit`, `Close`) called on a nil receiver |

## I/O Operations

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrNilReader` | io | Import reader is nil | `g.IO().Import()` |
| `ErrNilWriter` | io | Export writer is nil | `g.IO().Export()` |
| `ErrIncompatibleExport` | io | Export stream format version is not supported by this binary | `g.IO().Import()` with data from newer release |
| `ErrIncompatibleRegistry` | io | Imported registry conflicts with existing registry (e.g., diverged label tokens) | `g.IO().Import()` when registries are out of sync |
| `ErrCorruptExport` | io | Export record is corrupted or malformed | `g.IO().Import()` with truncated or corrupted stream. Wraps `store.ErrCorruptWire` when the underlying cause is a decode-boundary rejection (see Integrity & Wire below) |
| `ErrImportSizeLimit` | io | Import staging buffer exceeds maximum allowed size | `g.IO().Import()` with very large export streams |

## Delta Export & Merge

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrCursorUnknown` | io | Export cursor is from another graph, ahead of the change-log, or predates retained history | `g.IO().ExportSince()` with invalid/stale cursor |
| `ErrDeltaBaseMismatch` | io | Delta does not match the base it is applied to (different snapshot state or epoch) | `g.IO().ImportMerge()` with mismatched base |

## Backup Ergonomics

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrBackupExists` | io | The deterministic target filename (content-addressed by change-log cursor) already exists in the backup directory — `BackupTo`/`BackupDeltaTo` never silently overwrite a prior backup or pick for the caller | `g.IO().BackupTo()`, `g.IO().BackupDeltaTo()` |

## Replication & Change-Log

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrPrimaryRegistryStale` | store | Primary's registry snapshot has not yet caught up to the change record (retryable) | `g.Replication().ApplyChange()` during token refetch |
| `ErrRegistryDiverged` | store | Replica's registry is not a prefix of primary's (fatal, re-bootstrap required) | `g.Replication().ApplyChange()` during token refetch |

## Replication Status

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrTxDone` | store | Transaction already committed or rolled back (no further operations) | `g.Tx()` methods after `Commit()` or `Rollback()` |

## Integrity & Wire

Sentinels guarding the on-disk / on-wire trust boundary: format compatibility and adversarial-or-corrupt-byte decoding. (Hash-chain verification itself — `g.Hash().VerifyNodeChain()` / `VerifyRelChain()` — returns a plain `(bool, error)`; the `error` is `ErrNodeNotFound`/`ErrRelNotFound` for a missing entity, documented under Store — Entity Queries above, not a dedicated hash-chain sentinel.)

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrWireFormatVersionUnsupported` | store | On-disk wire format is newer than this binary supports | `g.New()` with data directory created by newer release; per-row checked decode during `loadIndexes` |
| `ErrHistoryAnchorIntervalMismatch` | store | Configured `HistoryAnchorInterval` differs from the interval the store's existing delta history was written at (baked into the on-disk delta layout; a mismatch would silently reconstruct against the wrong anchor) | `g.New()` reopening a delta store at a different `Config.HistoryAnchorInterval` |
| `ErrCorruptWire` | store | msgpack decoding of a persisted, imported, or replicated row failed in a way indicating corrupt or adversarial bytes — including a recovered decoder panic (`guardMsgpackDepth` / `SafeUnmarshal`, lesson 47). The store trust boundary fails closed rather than crashing. **NOT re-exported through `pkg/graph`** under its own name — only the `ErrCorruptExport` wrapping alias is (see I/O Operations above) | `g.Replication().ChangeFeed()` / `ForEachChange()` on a corrupt `0x09` change-log record; `g.Temporal().AsOfTags()` / `ResolveAsOf()` / `TagAsOf()` on a corrupted `asof_tags` MetaKV blob; wrapped as `ErrCorruptExport` at the `g.IO().Import()` trust boundary |

## Capabilities

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrCapabilityNotSupported` | store | Required optional Store capability is not implemented by the configured backend | `g.Replication().ApplyChange()` on memory store, `g.IO().ExportSince()` on tiered store without change-log, `g.Replication().Watch()` on its very first pull — either no change-feed capability at all (e.g. tiered) or a badger/memory store whose change-log is present but disabled (`store.ChangeLogStatusCapability.ChangeLogEnabled() == false`), mirroring the same fail-closed check as `Watermark`/`ExportSince` |

## TieredStore Reference/Event Ontology

Sentinels enforcing `tiered.Store`'s reference-vs-event primary-label class boundary (see CLAUDE.md's TieredStore section). Declared in `pkg/graph/store/tiered` and reachable through three DIFFERENT sub-APIs depending on which door the caller used, so they are re-exported centrally in `pkg/graph/errors.go` rather than in a single sub-API package's own `errors.go`.

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrNotReferenceEntity` | tiered | The target entity is not a reference entity — event entities cannot be archived | `g.Tier().Archive()` / `Restore()` on an event-classed node |
| `ErrEventPropertyIndex` | tiered | Property indexes are reference-entities-only on a tiered store | `g.Index().CreateProperty()` on an event-classed label |
| `ErrPrimaryLabelClassMutation` | tiered | A label mutation would change the primary label's reference↔event ontology class (routing depends on this class, so flipping it mid-flight would fragment the version chain across shards) | `g.Nodes().AddLabel()` / `RemoveLabel()` — surfaced from the store-level label-token doors (`AddNodeLabelToken` / `RemoveNodeLabelToken` and their `WithHistory` variants) |

## Store-Internal Sentinels (Not Re-Exported Through pkg/graph)

These sentinels are declared in `pkg/graph/store/errors.go` and have **no alias in `pkg/graph/errors.go`**. Some are purely internal (never surfaced past the store implementation); others leak through a specific public `Graph` method (noted per row) because that door has no conversion step. To classify these, import the store package directly: `storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"` and check `errors.Is(err, storepkg.ErrXxx)` — `errors.Is(err, graph.ErrXxx)` will never match them. (`ErrCorruptWire`, also store-only and not re-exported, is documented above under Integrity & Wire alongside its wire-format sibling rather than repeated here.)

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrStoreClosed` | store | The backing store instance has been closed; returned directly by store-implementation methods, independent of the graph-level `ErrGraphClosed` check | Any `Store` interface method called on a closed `badger.Store` / `memory.Store` / `tiered.Store`. Most graph-façade doors intercept with `ErrGraphClosed` first (via `core.checkOpen()`), so this is chiefly visible to a caller driving a `store.Store` implementation directly (custom wiring, store-level tests) |
| `ErrVersionNotFound` | store | The requested history version *number* does not exist for the entity (version-number lookup, distinct from a time-based query) | Store-level `GetNodeVersion(id, version)` / `GetRelVersion(id, version)`. Consumed and converted internally by most public callers — `g.Nodes().VersionBefore()` / `VersionAfter()` fold it into `(nil, nil)` or `ErrNodeNotFound`; `g.Temporal().NodeAsOf()` / `RelAsOf()` fold it into `ErrNoVersionAsOf` — but a default-branch conflict check in `g.IO().Import()` can still surface it raw on an unexpected store error |
| `ErrInvalidStoreMutation` | store | A `Store` implementation returned a result violating its documented contract (mismatched ID, non-ascending order, wrong row count, dangling adjacency reference), or a backend-specific mutation guard rejected the write (deleting a node that still has live relationships via a raw store call, a nil iteration callback, a write attempted against a read-only badger store) | Internal `store_validation.go` invariant checks that wrap a misbehaving custom `Store`; direct `memory.Store` / `badger.Store` / `tiered.Store` method calls that bypass the graph façade's cascade-safe doors |
| `ErrChangesNotAscending` | store | A batch passed to `ApplyChanges` is not in strictly ascending LSN order | `g.Replication().ApplyChanges(recs)` — the successful ascending prefix before the out-of-order record is still applied and watermarked; this sentinel itself is not re-exported through `pkg/graph` or `pkg/graph/replication` |
| `ErrSlotNotLocal` | store | A PARTITIONED store was handed an entity whose snowflake slot it does not own (its authority lives on another partition — ADR-0007/0010). Re-exported as `sharded.ErrSlotNotLocal` (same value) | A point read or write routed to the wrong `sharded.Store` partition (an ID whose slot is unclaimed fails closed — see api.md's Sharded store section); also raised inside `g.IO().Import()` when a re-shard would drop a non-empty slot, where it drives the import rollback |

## Transaction-Time Backfill (§4.1)

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrTxBackfillDisabled` | core | Transaction-time backfill is disabled (gate not set) | `g.Nodes().AddWithTx()`, `g.Rels().AddWithTx()`, `tkg_tx_from` property on Add when `Config.AllowTxBackfill=false` |
| `ErrInvalidTxFrom` | core | Backfilled `tkg_tx_from` is invalid (non-positive or in the future) | `g.Nodes().AddWithTx()`, `g.Rels().AddWithTx()` with invalid timestamp |

## Named As-Of Tags (§4.2)

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrInvalidAsOfTag` | core | As-of tag name is blank or the instant is non-positive | `g.Temporal().TagAsOf()` with invalid inputs |
| `ErrTooManyAsOfTags` | core | Graph exceeds maximum as-of tags (4096) | `g.Temporal().TagAsOf()` after limit reached |

## Unique Property Constraints (ADR-0002)

Unique property constraints (`g.Constraints().CreateUnique(...)`) forbid two current nodes carrying the same value for a constrained `(label, property)`. Enforcement covers the standalone node doors (Add / AddWithTx / AddByIDIfAbsent / Update / UpdateInPlace / CompareAndSetProperty / AddLabel), `BatchBuilder.AddNode`/`UpdateNode`, `GraphTx.AddNode`/`UpdateNode`, and `g.IO().Import` (default-strict — a duplicate rolls the whole import back; `ImportOptions.SkipUniqueValidation` opts a trusted restore out). Replica apply reproduces rows verbatim and does NOT enforce.

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrUniqueViolation` | core | A write would make two current nodes hold the same value for a constrained `(label, property)`, a `UniqueForever` value already owned by another entity was claimed, or an import stream carried a duplicate. Wrapped with the label, key, and winning/owning entity ID | `g.Nodes().Add()` / `AddWithTx()` / `AddByIDIfAbsent()` / `Update()` / `UpdateInPlace()` / `CompareAndSetProperty()` / `AddLabel()`, `BatchBuilder.AddNode()`/`UpdateNode()`, `GraphTx.AddNode()`/`UpdateNode()`, and `g.IO().Import()` into a duplicate value |
| `ErrUniqueViolationExisting` | core | `CreateUnique` found existing duplicate values; the constraint is NOT installed. Wrapped with up to five offender IDs | `g.Constraints().CreateUnique()` over duplicated data |
| `ErrUniqueConstraintExists` | core | A unique constraint already exists for the `(label, property)`, or the registry is at capacity | `g.Constraints().CreateUnique()` |
| `ErrUniqueConstraintNotFound` | core | No unique constraint exists for the `(label, property)`, or `ReleaseOwnership` was called without a `UniqueForever` constraint on the pair | `g.Constraints().DropUnique()` / `ReleaseOwnership()` |
| `ErrUniqueUnsupportedType` | core | A constrained key or an existing/incoming value is a floating-point type (bit-pattern equality is user-hostile), or an unimplemented `UniqueScope` was requested | `g.Constraints().CreateUnique()`/`CreateUniqueForever()` over float data; a float write on a constrained key; `g.Nodes().GetOrCreateByKey()` / `Constraints().ReleaseOwnership()` with a non-scalar or float value |
| `ErrUniqueEventLabelUnsupported` | core | On the tiered store, a unique constraint was requested on an event-class label. Reference labels enforce globally on the reference shard, but event values span unbounded time shards with no global value index, so uniqueness cannot be enforced there — a permanent correctness boundary (ADR-0005 §3.5), distinct from the tiered store's `ErrEventPropertyIndex` | `g.Constraints().CreateUnique()` / `CreateUniqueForever()` on an event label of a tiered graph |

## History Retention & Compaction (ADR-0001)

History compaction (`g.Admin().CompactHistoryNodes(...)` / `CompactHistoryRels(...)`) trims an entity's oldest version-history rows per a `RetentionPolicy`, keeping the newest belief and recording a per-entity detached-anchor stub. Compaction removes the ability to answer transaction-time pins older than the trimmed knowledge; that loss is explicit, never silent.

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrHistoryCompacted` | core | A temporal read's transaction-time pin falls before compacted knowledge, so the answer would require a trimmed version. Point doors (`NodeAsOf` / `NodeAtTx` / rel mirrors) check per entity; scan doors (`NodesAsOf` / `ByLabel` with `TxAt` / `TxPin`) check the graph watermark and fail the whole scan | `g.Temporal().NodeAsOf()` / `NodeAtTx()` / `NodesAsOf()`, `g.Nodes().ByLabel()` / `g.Rels().ByType()` with a pin below compacted knowledge |
| `ErrCompactionProtectedTag` | core | A registered named as-of tag (§4.2) pins knowledge the policy would trim; a tag is a promise the state stays addressable. No history is trimmed. Remove the tag first | `g.Admin().CompactHistoryNodes()` / `CompactHistoryRels()` |
| `ErrInvalidRetentionPolicy` | core | The `RetentionPolicy` has no bound set (both `KeepVersions` and `KeepSince` zero) or a negative bound | `g.Admin().CompactHistoryNodes()` / `CompactHistoryRels()` |
| `ErrCompactionChangeLogEnabled` | core | Compaction is refused while the change-log is enabled (compaction records / replica apply / delta interplay land later), so no replica can silently diverge from a compacted primary | `g.Admin().CompactHistoryNodes()` / `CompactHistoryRels()` |
| `ErrRetentionExpired` | core | A temporal read's pin falls before a relevant label's retention watermark (ADR-0008 — whole entities purged below a policy boundary with no tombstone). Point doors (`NodeAtTx` / `NodeAsOf` / rel mirrors) check the queried entity's label watermark(s); scan doors (`NodesAsOf` / `ByLabel` / `All` with a temporal pin) fail the whole scan against the graph max watermark. Fail-closed guard shipped in R1 before any purge exists | `g.Temporal().NodeAtTx()` / `NodesAsOf()`, `g.Nodes().All()` / `ByLabel()` with a pin below a retention watermark |
| `ErrRetentionPurgeDisabled` | core | The retention-purge admin door was called but the graph was not opened with `Config.AllowRetentionPurge`. A no-tombstone hard removal must be explicitly enabled | `g.Admin().PurgeExpiredNodes()` |
| `ErrRetentionPurgeChangeLogEnabled` | core | The change-log is enabled but the store cannot emit the `ChangeRangePurge` predicate record (no `RangePurgeLogCapability`), so a purge would remove data locally without telling a replica — a silent divergence, refused. Defensive: no in-tree backend hits it (the native purge stores also implement `RangePurgeLogCapability`); it guards a future/partial backend | `g.Admin().PurgeExpiredNodes()` |
| `ErrInvalidPurgePolicy` | core | The `PurgePolicy` is missing its `Label`, carries a non-positive `Before`, or names an unsupported `Mode` | `g.Admin().PurgeExpiredNodes()` |
| `ErrResetDisabled` | core | `g.Admin().Reset()` (a whole-graph destructive wipe — every entity, index, history row, named as-of tag, and unique-constraint definition) was called but the graph was not opened with `Config.AllowReset`. Mirrors `ErrRetentionPurgeDisabled`'s safety-valve pattern (BACKLOG 13d) | `g.Admin().Reset()` |
| `ErrExactErasureDisabled` | core | The bounded legal-erasure door was called without the explicit `Config.AllowExactErasure` safety opt-in | `g.Admin().ExactErase()` |
| `ErrInvalidExactErasureRequest` | core | The exact-erasure request is empty, contains a non-positive node/relationship ID, or omits positive relationship-closure bounds for node erasure | `g.Admin().ResolveExactErasure()` / `g.Admin().ExactErase()` |
| `ErrExactErasureRelationshipEscape` | store | A current or historical relationship version touching a declared node has an identity absent from the declared relationship set. The operation refuses before writing and never widens caller scope implicitly | `g.Admin().ExactErase()` / direct `store.ExactErasureCapability` |
| `ErrExactErasureClosureLimit` | store | Current-plus-history relationship closure exhausted its caller-declared relationship-identity, scanned-version, or endpoint-node-identity bound. Planning or execution refuses before mutation | `g.Admin().ResolveExactErasure()` / `g.Admin().ExactErase()` / direct `store.ExactErasureCapability` |
| `ErrExactErasureChangeLogRetained` | store | Exact erasure found enabled, buffered, scoped, or persisted change-log material. Change records contain full entity payloads, so leaving them behind would retain erased data; the operation refuses before writing | `g.Admin().ExactErase()` / direct `store.ExactErasureCapability` |

Compaction also declines with `ErrCapabilityNotSupported` on the tiered backend (per-shard trim + catalog counters are out of scope for v1). Retention purge (R2) likewise declines with `ErrCapabilityNotSupported` on the tiered/sharded backends until R4 wires their per-shard mapping.

## pkg/types Sentinels

`pkg/types` is directly importable (`import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"`). A handful of its sentinels are aliased verbatim into `pkg/graph/internal/core` and are therefore ALSO reachable as `graph.ErrXxx` (cross-referenced in their row below and in the section that documents the graph-side alias); the rest are `pkg/types`-only and never pass through the Graph façade at all — they surface from direct `pkg/types` API calls (`Node`/`Relationship` mutators, `PropertySlice`, `RegisterPropertyStructType`, Allen-relation helpers, `TemporalValue` validation, `RecurrencePattern.Expand`).

| Sentinel | Package | Meaning | Typical Doors |
|----------|---------|---------|---------------|
| `ErrNilNode` | types | Pointer to Node is nil. Same identity as `graph.ErrNilNode` / `core.ErrNilNode` — `pkg/types` is the canonical declaration (see Entity Validation above) | `(*types.Node)` mutator methods called on a nil receiver (`SetProperty`, `AddLabel`, `RemoveLabel`, `Freeze`, ...) |
| `ErrNilRelationship` | types | Pointer to Relationship is nil. Same identity as `graph.ErrNilRelationship` / `core.ErrNilRelationship` — `pkg/types` is the canonical declaration (see Entity Validation above) | `(*types.Relationship)` mutator methods called on a nil receiver |
| `ErrNilPropertySlice` | types | Pointer to PropertySlice is nil. Not re-exported through `pkg/graph` | `(*types.PropertySlice)` methods (`Set`, `Delete`, ...) called on a nil receiver |
| `ErrFrozenNode` | types | Node is frozen (returned by a scan/plural read); mutation requires `DeepCopy()` first. Not re-exported through `pkg/graph` | Error-returning `types.Node` mutators (`SetProperty`, `AddLabel`, ...) called on a frozen node; void/bool mutators panic instead |
| `ErrFrozenRelationship` | types | Relationship is frozen; mutation requires `DeepCopy()` first. Not re-exported through `pkg/graph` | Error-returning `types.Relationship` mutators called on a frozen relationship; void/bool mutators panic instead |
| `ErrTypeNotHashable` | types | A type registered via `RegisterPropertyStructType` does not implement `HashableValue`. Not re-exported through `pkg/graph` | `types.RegisterPropertyStructType(v)` |
| `ErrTypeNotDeepCopyable` | types | A type registered via `RegisterPropertyStructType` does not implement `DeepCopier`. Not re-exported through `pkg/graph` | `types.RegisterPropertyStructType(v)` |
| `ErrPropertyTypeNameCollision` | types | Two registered custom property struct types share the same type name from different packages. Not re-exported through `pkg/graph` | `types.RegisterPropertyStructType(v)` |
| `ErrOpenInterval` | types | An Allen-relation interval endpoint is zero (intervals must be finite). Not re-exported through `pkg/graph` | `types.Relate(a, b)`, `types.Compose(...)` and other `pkg/types/allen.go` helpers |
| `ErrInvalidInterval` | types | An Allen-relation interval has start >= end. Not re-exported through `pkg/graph` | `types.Relate(a, b)` and other Allen-relation helpers |
| `ErrReservedPrefix` | types | A property key uses the reserved `tkg_` prefix. Not re-exported through `pkg/graph` | `types.PropertySlice.Set()`, `types.NewPropertySlice(map)` |
| `ErrEmptyPropertyKey` | types | A property key is the empty string. Not re-exported through `pkg/graph` | `types.PropertySlice.Set()`, `types.NewPropertySlice(map)` |
| `ErrUnsupportedValueType` | types | A property value is not on the recursive allowlist (or a deep-copy round-trip changed its shape — an internal invariant break). Not re-exported through `pkg/graph` | `types.PropertySlice.Set()`, `types.NewPropertySlice(map)`, `DeepCopy()` |
| `ErrUnsupportedMapType` | types | A property value is a map type other than `map[string]any` / `map[string]string`. Not re-exported through `pkg/graph` | `types.PropertySlice.Set()` with an unsupported map value |
| `ErrMaxDepthExceeded` | types | A property value's nesting exceeds the 32-level depth limit. Not re-exported through `pkg/graph` | `types.PropertySlice.Set()`, `types.NewPropertySlice(map)` |
| `ErrInvalidTemporalValue` | types | A `TemporalValue` fails shape validation (unknown kind, empty rendering, oversized rendering). Not re-exported through `pkg/graph` | `pkg/types/temporal_value.go` validation helpers |
| `ErrInvalidTimeRange` | types | **DISTINCT IDENTITY** from the `core`/`store`/`graph` sentinel of the same name (see Context & Transactions above) — `pkg/types/recurrence.go` declares its own `errors.New("types: invalid time range")`, never aliased to or from the core/store one. Returned when a `RecurrencePattern.Expand(from, to)` window is empty or inverted. Not re-exported through `pkg/graph` | `types.RecurrencePattern.Expand(from, to)` |
| `ErrRecurrenceSpanTooLarge` | types | A `RecurrencePattern.Expand(from, to)` window exceeds the maximum expansion span (`to - from` too large). Not re-exported through `pkg/graph` | `types.RecurrencePattern.Expand(from, to)` |

---

## Using errors.Is

To classify errors returned by graph API calls, use the `errors.Is()` function from the `errors` standard library:

```go
import (
	"context"
	"errors"
	"log"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func main() {
	g, _ := graph.New(graph.Config{SnowflakeNodeID: 4})
	defer g.Close()

	ctx := context.Background()

	// Query a node that doesn't exist
	_, err := g.Nodes().Get(ctx, types.NodeID(999999))
	if err != nil {
		// Classify the error by type
		if errors.Is(err, graph.ErrNodeNotFound) {
			log.Println("Node does not exist — create it or handle gracefully")
		} else if errors.Is(err, graph.ErrGraphClosed) {
			log.Println("Graph is closed — reinitialize")
		} else {
			log.Printf("Unexpected error: %v", err)
		}
	}

	// Batch operations report accumulated failures
	bb, err := g.Batch().New()
	if err != nil {
		log.Fatal(err)
	}
	bb.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	bb.AddNode([]string{"Person"}, nil) // missing name
	if _, err := bb.Execute(); err != nil {
		if errors.Is(err, graph.ErrBatchFailed) {
			log.Println("Batch had failures — check individual operation results")
		}
	}
}
```

All sentinels documented under a `graph` package label above are re-exported from `pkg/graph` for consistency, so a single import reaches every one of them:

```go
import "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"

// Check with a single qualifier
errors.Is(err, graph.ErrNodeNotFound)
errors.Is(err, graph.ErrTxBackfillDisabled)
errors.Is(err, graph.ErrPrimaryRegistryStale)
```

Sentinels documented under "Store-Internal Sentinels" (and `ErrCorruptWire` under "Integrity & Wire") have no `graph.ErrXxx` alias — reach them via `pkg/graph/store` directly:

```go
import storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

// GetNodeVersion(id, version) returns storepkg.ErrVersionNotFound raw at the
// store layer — there is no graph.ErrVersionNotFound.
if _, err := someStore.GetNodeVersion(id, version); errors.Is(err, storepkg.ErrVersionNotFound) {
	log.Println("that version number does not exist for this entity")
}
```

`ErrNoVersionValidAt` DOES have a `graph.ErrXxx` alias (added alongside this
example — it used to be store-only, hence the historical footgun this
section used to warn about):

```go
// g.Temporal().NodeAt(id, at) returns graph.ErrNoVersionValidAt (identical
// to storepkg.ErrNoVersionValidAt — pick whichever import you already have).
if _, err := g.Temporal().NodeAt(id, at); errors.Is(err, graph.ErrNoVersionValidAt) {
	log.Println("no version covers that instant")
}
```
