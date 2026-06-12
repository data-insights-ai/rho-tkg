# tkg/v4

**Temporal Knowledge Graph v4** — the internal Go library powering the core graph engine for temporal knowledge graphs.

tkg/v4 is a **pure library** (no main binary, no HTTP server, no query language). It provides the low-level graph types, persistence layer, and entity management that higher-level products build on.

For the full product with Cypher queries, Vadalog reasoning, and an HTTP server, see **tkgd-v3**.

| Layer | Repository | What it provides |
|---|---|---|
| **tkg/v4** (this repo) | `rho/tkg/v4` | Graph types, registries, `memory.Store`, `badger.Store`, `tiered.Store`, entity locks |
| **tkgd-v3** | `rho/tkgd-v3` | Cypher engine, Vadalog reasoning, HTTP server, REST API |

## Module

```
github.com/data-insights-ai/rho-tkg/v4
```

**Go:** 1.26.1
**License:** Apache-2.0
**Dependencies:** [`rho-snowflake-2026`](https://github.com/bds421/rho-snowflake-2026) (IDs), [`msgpack/v5`](https://github.com/vmihailenco/msgpack) (serialization), [`badger/v4`](https://github.com/dgraph-io/badger) (persistence)

## Documentation

Detailed documentation has been split into the `docs/` directory:

- [API & Core Types](docs/api.md) — Graph layer, registries, validation limits, temporal queries, transactions, and shadow properties.
- [Architecture & Concurrency](docs/architecture.md) — System boundaries, entity lock managers, multi-phase iteration, and thread safety.
- [Persistence](docs/persistence.md) — Storage interfaces, `badger.Store`, and `tiered.Store` multi-shard persistence.
- [Design Invariants](docs/design.md) — Protocol guarantees, referential integrity, defensive copying, and error sentinels.
- [Specifications](docs/SPEC.md) — Formal specifications and algorithms.

### What's new in v4 (v4.0.0 → v4.9.0)

**v4.9.0 — read/write performance and bitemporal cascade correctness.** A cross-engine benchmark round drove a sequence of optimizations, all equivalence-gated against the pre-existing decode/scan paths. Reads: a scan-resistant non-promoting entity cache (`GetNoPromote` — a warm label scan no longer evicts the working set it is about to revisit, and concurrent scanners stop serializing on the LRU mutex); an endpoint-carrying adjacency index with streaming `ForEachAdjacentEndpoint` that skips the relationship decode on one-hop expansion; inline valid-time adjacency stamps (`ForEachAdjacentEndpointAt` / `ForEachAdjacentRelAt` + the canonical `g.Temporal().RelMatchesValidTime`) that reject expired edges in temporal traversal with no decode (OPT15); `RangeCardinality` — an O(buckets) range-count over the sorted numeric index that replaces an over-select-then-re-fetch count path (R1); native badger reverse-scan `AsOf` (O(versions newer than the query) instead of materializing the whole chain); a maxto-augmented temporal interval index (output-sensitive stabbing queries); single-allocation property index keys; and an opt-in `QueryOpts.NoSort` scan lever. Writes: `badger DetectConflicts=false` (the store owns conflict semantics above Badger). Fixed: the append-only cascade — `SetNodeVersionInterval` / `SetRelVersionInterval` corrections no longer mutate stored rows (which left holes and leaks in `NodeAtTx` and diverged the native `NodeAsOf` from the other backends); selection is now deterministic across all three backends (lesson 46). See `CHANGELOG.md` `[4.9.0]`.

**v4.8.0 — per-shard Badger footprint tuning.** One Badger instance opens per shard, and Badger's stock per-instance sizes (≈2 GB apparent vlog, ≈64 MB memtable arena allocated upfront, 256 MB block-cache bound, 4 compactors) multiply by shard count — tens of GB of apparent disk and gigabytes of heap across a tiered deployment that holds little data. New knobs `ValueLogFileSize` / `MemTableSize` / `BlockCacheSize` / `NumCompactors` on `badger.Config`, `tiered.Config`, and `graph.Config` bound that; **zero keeps Badger's stock defaults** (a deliberate library decision — the owner opts in explicitly), validated at `New` and applied through one shared options path. `tiered.Config` also gains `CacheBudgetBytes` — the per-shard entity-cache byte budget that bounds heap across many open shards where the entry-count `CacheCapacity` cannot. **Fixed a process-kill bug:** shrinking `MemTableSize` on a data dir that still holds live WALs (copied from a running server, or left by a crash) made Badger replay them into a too-small arena and `log.Fatal`/`os.Exit` the whole process — not a recoverable error or panic. `badger.MigrateOversizedWAL` flushes such WALs first (`New` runs it; the tiered recovery path migrates *before* its read-only probe, which would crash identically), and a read-only or above-1GB-cap open fails closed with `ErrOversizedWAL` instead of crashing. Every tuning test is mutation-validated and the WAL-replay crash is proven with a subprocess test. Badger bumped to v4.9.2.

**v4.7.0 — scan-resistant cache + streaming scans.** Full-cardinality scans no longer fill the badger LRU (100% steady-state miss past the cache); `core.Config.CacheCapacity` is now configurable; an ordered numeric index view backs range predicates; streaming `ForEachByLabel`/`ForEachByType`/`ForEachOutgoing`/`ForEachIncoming`/`ForEachByLabelPropertyRange` give O(1)-in-cardinality scans; byte-budgeted caches (`CacheBudgetBytes`); and the flush cycle is O(dirty) via a dirty-set index (fixing an ingestion stall at large cache capacities).

**v4.6.1 — adversarial-campaign fixes.** Three break-the-system rounds against the v4.6.0 tree. Fixed: (1) frozen scan rows could silently poison the store's canonical cache through the `Temporal()`/`Integrity()` shared-pointer escape (exported fields bypass the frozen guard) — on frozen entities those accessors now return independent copies; (2) bitemporal point queries treated supersession as retraction — `NodeAtTx(oldValidTime, now)` returned nothing after any update, including the flagship explicit-VT tiling scenario; the TX-visibility predicate is now recorded-by-then (`TxFrom <= txAt`, lesson 43); (3) `AddByIDIfAbsent`'s found branch returned a frozen shared row since v4.5.0 — it deep-copies again; (4) import is a real trust boundary — truncated streams classify as `ErrCorruptExport`, and import recomputes per-row content hashes plus a full post-replay chain verification, rolling back on mismatch (previously a bit-flipped export imported cleanly and the graph failed its own `Verify*Chain`, lesson 44). A fourth round (index-vs-brute-force twins, constraint door-parity, pagination exactness, hostile event handlers, tiered rotation/archive/repair) found no defects; all batteries are permanent regression detectors.

**v4.6.0 — architecture-review remediation.** Every issue from the full 2026-06-10 architecture review, fixed and adversarially tested: (1) the on-disk wire format is now versioned — per-row `FormatVersion` plus a store-level `wire_format_version` marker verified at open; data written by a newer release fails closed with the new `ErrWireFormatVersionUnsupported` instead of misdecoding (backward compatible with every existing directory). (2) Temporal predicates have a single canonical definition in `storeutil`, shared by the core resolver and the store push-down; the new cross-door equivalence test found and fixed a real bug — `AddLabel`/`RemoveLabel` and property mutations inherited the previous version's `tkg_valid_from`, so historical label/property queries resolved to the post-mutation state (lesson 42). (3) Relationship creation goes through one shared kernel for all four doors (Add/AddByID/AddByIDIfAbsent/batch). (4) The badger async write buffer is bounded (`Config.MaxPendingWrites`, default 100K ops, writer-side backpressure, `PendingWriteCount()` visibility). (5) `tiered.Store.RecoverBackgroundError()` clears the previously-permanent sticky background error after a successful persistence probe; `RunRepair` warns with counts when it fixed anything. (6) A sentinel anti-drift test locks every exported error surface to its canonical declaration; `types.ErrNilNode`/`ErrNilRelationship` messages now carry the `types:` prefix (identities unchanged).

**v4.5.0 — performance: frozen rows, zero-copy scan reads.** Store query/scan paths no longer deep-copy every returned row. Cache and canonical-map entries are frozen (`types.Node.Freeze()` / `types.Relationship.Freeze()`) when published, and plural/scan reads (`*ByLabel*`, `All*`, `Get*ByIDs`, adjacency traversals, temporal/index scans) on all three store backends return the shared frozen pointer directly. Point reads (`GetNode`/`GetRelationship`) still return mutable deep copies. Frozen entities reject mutation — error-returning mutators return `types.ErrFrozenNode`/`types.ErrFrozenRelationship`, void/bool mutators panic, and `DeepCopy()` is the thaw operation. On a read-heavy workload (5k nodes / 25k rels, BadgerInMemory): label-scan aggregation −19% time / −35% allocations, 2-hop traversal −14%, var-length −10%. Callers that mutated scan results must `DeepCopy()` first.

**v4.4.2 — documentation: valid-time vs transaction-time semantics pinned down.** The docs (CLAUDE.md, AGENTS.md, docs/architecture.md) now state the bitemporal contract explicitly: `tkg_tx_from` is a system claim ("recorded at T", stamped on every Add), `tkg_created_at` is system-derived (snowflake fallback), and `tkg_valid_from` is a world-time domain assertion — `0` means no claim was made. The shadow resolver returns the raw asserted value (`(Instant(0), ok=true)` when unset) while temporal queries use the effective valid-from with snowflake fallback — a deliberate asymmetry, not a missing fallback. No code or public-API changes.

**v4.4.1 — documentation-consistency patch.** Synced two version strings (`AGENTS.md` `Status:` line and the `docs/architecture.md` title) that were left at `v4.3.2` in the 4.4.0 release, which had broken the docs-consistency test. No code or public-API changes.

**v4.4.0 — repository moved to GitHub, Go module path renamed.** The canonical repository is now [`github.com/data-insights-ai/rho-tkg`](https://github.com/data-insights-ai/rho-tkg) and the Go module path is `github.com/data-insights-ai/rho-tkg/v4` (was `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4`). The `/v4` major-version suffix is unchanged — this is a host move, not a major bump. Consumers must replace the import path and re-run `go mod tidy`; the public API is otherwise unchanged.

**v4.3.2 — heal an undercounted entity counter on reopen.** A hard crash can leave a shard's persisted node/rel counter below the number of clean rows actually present (lost increments). With all rows decoding cleanly (`liveRows == rawEntityRows`) no data is missing, so reopen now heals the counter up to the live row count (with a warning) instead of fataling. The opposite direction — counter > live rows (rows actually missing) — stays fatal. Together with 4.3.1 this fully recovers tiered stores left inconsistent by an unclean shutdown.

**v4.3.1 — property-key registry crash fix (tiered reload).** Fixes a `node counter does not match N live rows` fatal on reopening a tiered store that used the 4.3.0 property-key tokenization. Two causes: event shards loaded their own (empty) registry instead of the single canonical one (now injected into every shard at open), and new tokens were only persisted at `Close` (now write-ahead `fsync`ed in `flush()` before the row batch). No on-disk change; 4.3.0 DBs recover on first open. Adds `badger.Config.PropertyKeyRegistry`, `badger.Config.OnPropertyKeyGrow`, `badger.Store.PropertyKeyRegistry()`.

**v4.3.0 — O(1) relationship degree + bitemporal queries.** `DegreeCapability` (`IncomingDegree` / `OutgoingDegree`) counts a node's relationships of a type from index entries without resolving entities; `RelOps` uses it with a traversal fallback. Plus the five-phase bitemporal rollout closing the bitemporal gap:

- `QueryOpts.TxAt` filters the version chain to entries visible at a transaction time. New bitemporal point queries `g.Temporal().NodeAtTx(id, validAt, txAt)` (plus `RelAtTx`, `NodesAtTx`, `RelsAtTx`). `TxAt == 0` keeps current backward-compatible "no TX filter" behaviour.
- `g.Nodes().Update(id, {tkg_valid_from: t, ...})` and the Rel counterpart now accept caller-supplied valid time. Same for batch (`BatchBuilder.UpdateNode`) and tx (`GraphTx.UpdateNode`). New sentinel `graph.ErrValidFromBeforePrevious` rejects backwards-in-time on the immediate predecessor.
- Resolver no longer conflates transaction time with valid time when computing version bounds: `vEnd` derives from `next.ValidFrom` when explicit (falls back to `next.UpdatedAt`). Adjacent versions auto-tile once the caller supplies explicit `tkg_valid_from` on Update.
- New convenience `g.Temporal().SetNodeVersionInterval(id, vF, vT, props)` / `SetRelVersionInterval` for tile-clean timeline edits.
- No on-disk schema change; existing Badger / Tiered DBs work unchanged.

**v4.2.2 — consumer-ergonomics alias additions.** `graph.ErrNodeExists` and `graph.ErrRelExists` aliased so consumers can run `errors.Is(err, graph.ErrNodeExists)` without importing `pkg/graph/store` for those two sentinels.

**v4.2.1 — consumer-ergonomics aliases.** `graph.QueryOpts`, `graph.ShardDepth`, `graph.DistanceMetric`, `graph.ErrIndexExists`, `graph.ErrIndexNotFound`, `graph.ErrTemporalIndexExists`, `graph.ErrTemporalIndexNotFound` aliased on `pkg/graph` for the same reason — keep downstream import lists short.

**v4.2.0 — sub-APIs are accessor methods.** The 14 sub-API exported fields on `*Graph` (`g.Nodes`, `g.Rels`, …) are now methods (`g.Nodes()`, `g.Rels()`, …). The accessors are nil-safe: `(*Graph)(nil).Nodes()` returns nil and chained calls fail closed with `ErrNilGraph` instead of panicking. Mechanical sed migration in CHANGELOG `[4.2.0]`.

**v4.1.0 — tx isolation: drop `c.mu.Lock` from tx lifetime (Path B).** Read accessors called inside an open `*GraphTx` no longer deadlock. `BeginTx` now uses a separate `c.txMu` for tx-vs-tx serialization; each tx method takes a brief `c.mu.RLock` around its body. Isolation semantics shift from "serializable graph-wide" to "serializable per touched entity, snapshot-isolated elsewhere" — concurrent standalone mutations on disjoint entities run in parallel. The tx-side read mirrors added in v4.0.1/4.0.2 remain for call-site clarity but are no longer required for correctness.

**v4.0.x — tx-read deadlock fixes.** v4.0.1 mirrored the metadata-resolution read accessors on `*GraphTx`; v4.0.2 added 27 more mirrors covering bulk reads (ByLabel, Outgoing, NodesAt, …). Superseded by v4.1.0 Path B, kept as a clearer call-site signal "this read is inside the tx I'm holding".

**v4.0.0 API cleanup (breaking).** The public surface was simplified — see CHANGELOG `[4.0.0]` for the full migration recipe. Highlights:

- `*WithContext` method pairs collapsed: every `g.Nodes().Add(labels, props)` becomes `g.Nodes().Add(ctx, labels, props)`. Same for `Get`, `Update`, `UpdateInPlace`, `Delete`, `CompareAndSetProperty`, and the Rels counterparts (`Add`, `AddByID`, `AddByIDIfAbsent`, `Get`, `Update`, `UpdateInPlace`, `Delete`, `CompareAndSetProperty`). The `*WithContext` siblings no longer exist.
- `g.Admin` split: tiered-only methods (`Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `Repair`, `VerifyShard`) moved to a new `g.Tier` sub-API. `g.Admin` now exposes only `Reset` + `DecomposeNodeID` + `DecomposeRelID`, which work on every backend.
- `g.Admin().DecomposeID(snowflake.ID)` replaced with typed `g.Admin().DecomposeNodeID(types.NodeID)` / `g.Admin().DecomposeRelID(types.RelID)`.
- `temporal.RelationshipsAt` → `temporal.RelsAt`, `RelationshipsByTypeAt` → `RelsByTypeAt`, `RelationshipsDuring` → `RelsDuring`.
- `Nodes.NextVersion` / `PreviousVersion` renamed `VersionAfter` / `VersionBefore` (same on Rels).
- `g.Resolve().LabelToken` / `RelTypeToken` / `LookupLabel` / `LookupRelType` removed. Shadow-property accessors kept.
- `index.LegacyIndexProvider` interface + `Index.RegisterLegacyProvider` removed. External providers must migrate to `IndexProvider` + optional `Initializable`.
- `IO.Import(r)` + `IO.ImportWithOptions(r, opts)` collapsed into `IO.Import(r, opts)`. Pass `ImportOptions{}` for the previous defaults.
- New `g.Batch().Run(fn)` parallel to `g.Tx().Run` for the closure-style batch idiom. `g.Tx().RunContext` and `g.Batch().RunContext` also accept a context for cancellation parity with the explicit-context update methods.
- `g.Nodes().AddByIDIfAbsent(ctx, id, labels, props)` parallels the existing `g.Rels().AddByIDIfAbsent` — both sub-APIs now have an idempotent "ensure exists" verb.
- `g.Index().DropProperty` / `DropTemporal` / `DropVector` / `DropHighFrequency` renamed `DeleteProperty` / `DeleteTemporal` / `DeleteVector` / `DeleteHighFrequency` for verb consistency with the rest of the API surface.

**History-aware adjacency queries cost O(deleted) instead of O(total history).** `g.Temporal().OutgoingRelsAt`, `IncomingRelsAt`, and `NeighborsAt` previously folded every relationship history ID onto the narrow indexed candidate set to catch deleted-but-historically-valid rels. The fold now goes through a new optional store capability (`DeletedIterationCapability`) which yields only IDs whose history exists but whose current row is absent — every in-tree backend (memory, badger, tiered) implements it. External stores that omit the capability transparently fall back to the wider history scan with identical correctness.

**PublishBatch priority ordering is preserved under queue saturation.** `AsyncEventBus.PublishBatch` previously promised "no lower-priority event before all higher-priority batch events have been made visible", but the in-batch wake-up (added so a saturating batch under `BackpressureBlock` does not deadlock) could let a pre-existing lower-priority event slip in between batch enqueues. The bus now raises a per-batch priority ceiling for each priority pass; the dispatcher honours it so the in-batch wake-up drains only same-or-higher priorities and the ordering guarantee holds even with `QueueSize` smaller than the per-priority batch slice.

**Transaction rollback restores endpoints before relationships.** `GraphTx.Rollback` now restores all deleted node rows before recreating deleted relationships, so a rollback after `DeleteRelationship` followed by `DeleteNode(endpoint)` restores the edge instead of failing on a missing endpoint.

**Transaction rollback restores version history.** Rolled-back updates and deletes no longer leave phantom node/relationship history rows; rollback restores pre-transaction history snapshots and clears history for created entities it deletes.

**Metadata-blind rehashes preserve integrity metadata.** `CloseVersion`, node label add/remove, node/relationship property CAS, and in-place updates recompute hash-chain fields without erasing existing provenance, signature, authorization, or relationship endpoint-hash metadata.

**By-ID relationship creates preserve endpoint integrity.** `g.Rels().AddByID`, `AddByIDIfAbsent`, and transaction variants now fetch live endpoints under endpoint locks, capture `FromNodeHash`/`ToNodeHash`, and enforce configured constraints with the same relationship-state semantics as `g.Rels().Add`.

**Authorization tiers accept bounded numeric inputs.** `tkg_auth_level` now accepts every signed/unsigned integer type plus whole-number `float32`/`float64` values in `[0,255]`, and still rejects fractional or out-of-range values before mutation.

**Temporal shadow inputs accept safe numeric milliseconds.** `tkg_valid_from`, `tkg_valid_to`, and `tkg_created_at` accept `types.Instant`, signed integers, unsigned values that fit in `int64`, and whole-number floats inside each float type's contiguous exact integer range.

**Transaction rollback restores multi-label node changes.** Multiple label adds, multiple label removes, and mixed label remove/add operations on the same node roll back to the pre-transaction label sequence and label indexes.

**Resolver reads respect active transactions.** `Nodes.HasLabel`, `Rels.HasType`, and shadow-property resolution (`g.Resolve().NodeProperty` / `RelProperty`) now wait behind the graph read lock, so they cannot expose labels or relationship types created inside an uncommitted transaction. After `Close`, these no-error helpers return zero values rather than registry state.

**Versioned mutations fail before `uint32` wraparound.** Updates, node label add/remove, node/relationship property CAS, transaction updates, and batch updates return `ErrVersionOverflow` when the current entity version is already `math.MaxUint32`; they do not write history, wrap to version `0`, or allocate labels for rejected add-label calls.

**Version-chain successor lookup does not wrap.** `g.Nodes().NextVersion(id, math.MaxUint32)` and the relationship mirror return `nil, nil` instead of treating genesis version `0` as the successor.

**Version-chain navigation validates explicit IDs.** `PreviousVersion` and `NextVersion` return `ErrNodeNotFound`/`ErrRelNotFound` for unknown node or relationship IDs, while still returning `nil, nil` for missing neighboring versions on entities that exist now or in history.

**Event bus setters fail closed.** `g.Events().SetSync` and `SetAsync` now return an error; after graph close they return `ErrGraphClosed` and leave the installed publisher unchanged.

**Event bus getters have sync/async parity.** `g.Events().GetSync()` returns the installed synchronous bus only, and `g.Events().GetAsync()` returns the installed asynchronous bus only; both return nil for nil or zero-value API wrappers.

**Transaction helpers reject nil inputs.** `g.Tx().Run(nil)` and `RunContext(ctx, nil)` return `ErrNilTxCallback` before opening a transaction; `RunContext(nil, fn)` and graph mutation context helpers return `ErrNilContext` instead of panicking on nil contexts.

**IO helpers reject nil stream endpoints.** `g.IO().Export(nil)` and `tx.Export(nil)` return `ErrNilWriter`; `g.IO().Import(nil, opts)` returns `ErrNilReader`, including typed nil reader/writer values.

**Configured Store rejects typed nils.** `graph.New(graph.Config{Store: typedNil})` returns `ErrNilStore`; omit `Store` to request the default backend.

**Graph façade nils fail closed.** Nil or zero-value `*graph.Graph`, `TxAPI`, and `BatchAPI` entry points with error returns now return `ErrNilGraph` instead of panicking on an uninitialized core pointer.

**Sub-API zero values fail closed.** Nil, zero-value, or typed-nil public sub-API wrappers return `ErrNilGraph` from error-returning methods; no-error helpers return zero values instead of dereferencing an unwired `Ops`.

**Import replay errors roll back partial writes.** `g.IO().Import` now snapshots touched current rows, history rows, and registries during replay. If a corrupt or inconsistent stream fails after earlier records were applied, import restores the pre-import graph state instead of leaving partial entities or token mappings behind.

**Import rejects malformed entity wire invariants.** Snapshot import now rejects zero/negative entity IDs, negative versions, non-canonical label token lists, reserved `tkg_` property keys, unsorted/duplicate property keys, unknown property type tags, negative base-entity IDs, and property records that exceed the destination graph's validation limits before constructing entities.

**Badger reads reject semantic wire corruption.** Current and history entity reads now validate persisted `NodeWire`/`RelWire` before constructing `types.Node` or `types.Relationship`, so token-0, out-of-range token, malformed property, or invalid ID fields return read errors instead of panicking or truncating.

**Numeric wire validation preserves tag fidelity.** Checked property wire accepts `float32` NaN and infinities for scalar and slice values while still rejecting finite `float64` payloads that would overflow or round when narrowed to `float32`. Numeric payloads reconstruct to the tagged type even when MsgPack presents a compatible decoded shape such as `float32` for `float64`, `[]int64` for `[]int`, or `uint` for signed/unsigned integer tags.

**Badger deletes keep disk I/O out of the index write lock.** Cache-miss node and relationship deletes prefetch the old row before taking `idxMu.Lock`, then re-read the current cached row under the lock so index cleanup uses the row that is actually deleted.

**Badger sync writes include metadata and split relationship keys.** With `SyncWrites` enabled, property, temporal, high-frequency, and vector index create/drop operations flush their persisted definition metadata before returning, after releasing `idxMu`. The Badger split relationship helper paths used by Tiered routing and repair flush their entity/out and incoming-index writeOps too.

**Checked wire encoders reject nil entities.** Internal `NodeToWireChecked` and `RelToWireChecked` now return conversion errors for nil node/relationship payloads instead of dereferencing before the checked error path.

**Import rejects incomplete and unknown-tag streams.** Snapshot import now treats missing required header/registry records and unknown record tags as `ErrCorruptExport`; a stream with an accepted format version cannot silently skip records or report an empty no-op success.

**Import enforces stream counts and uniqueness.** Current node/relationship record counts must match the export header, and duplicate singleton, current, or history-version records inside one stream are rejected with `ErrCorruptExport`.

**Import staging caps are non-negative.** `ImportOptions.MaxStagedBytes == 0` remains unlimited; negative caps return `ErrImportSizeLimit` before a staging file is created.

**Import decode errors use the corrupt-export sentinel.** Malformed MsgPack record bodies now wrap `ErrCorruptExport` across header, registry, current entity, and history records.

**Property size limits are recursive.** `MaxPropertyValueSize` applies to strings nested inside supported property containers, not only top-level string values.

**Property values use an exact data-plane allowlist.** The property layer accepts only the concrete scalar, slice, and map types that hashing, deep-copy, and MsgPack reconstruction support, plus explicitly registered custom struct types. Named aliases and unsupported slices are rejected before hashing.

**Entity property construction is defensive.** `NewPropertySlice`, `PropertySlice.Set`, `Node.SetProperty`, and `Relationship.SetProperty` deep-copy accepted reference values before storing them. `Node.SetProperties` and `Relationship.SetProperties` validate direct `PropertySlice` input, sort it by key, collapse duplicate keys with last-value-wins semantics, and deep-copy values before installing them. Registered custom values are validated again after `DeepCopyValue`; a bad copy result or copy panic is rejected instead of becoming entity state.

**Entity nil receivers fail closed.** Nil `*types.Node` and `*types.Relationship` accessors return zero values, no-error mutators no-op, and error-returning mutators return `ErrNilNode`/`ErrNilRelationship`. Nil `*types.PropertySlice` pointer mutators return `ErrNilPropertySlice`; nil `*types.TemporalMetadata` helpers and nil integrity `DeepCopy` calls also return zero/nil without panicking.

**Property CAS enforces final shape.** `g.Nodes().CompareAndSetProperty` and `g.Rels().CompareAndSetProperty` recheck `MaxPropertiesPerEntity` after a matched add/delete/set mutation and before writing history, so CAS cannot create an entity that ordinary update paths would reject. CAS comparison uses exact-type property equality with NaN matching and does not return or copy stored reference values.

**Scalar property queries match property equality.** `g.Nodes().ByLabelAndProperty` canonicalizes `float32`/`float64` signed zero and NaN payloads on both fallback scans and property-index lookups, preserving concrete type separation while matching the compare-and-set equality contract.

**Registry resolution enforces graph name limits.** `Nodes.HasLabel` and `Rels.HasType` fail closed for empty, whitespace-only, and overlong names. `IO.Import` and graph construction from persisted registries apply the same configured name limit before accepting registry mappings. (The `g.Resolve().LabelToken`/`RelTypeToken`/`LookupLabel`/`LookupRelType` helpers were removed in v4 — they leaked the internal `uint16` token representation.)

**Registry metadata persists with committed tokens.** Successful graph writes, batch execution, index creation, and snapshot import that create label or relationship-type tokens save the current registries to persistent Badger/Tiered stores before returning. Rollback paths persist the restored snapshots, so reopened durable rows keep their token mappings even if the process did not reach `Graph.Close()`. In batch execution, a post-write registry checkpoint failure is reported as a batch error without rolling the returned entity pointers back to their pre-commit skeleton state. Transaction create/import methods record any committed row for rollback even when that trailing checkpoint returns an error, and `Commit` retries the registry checkpoint before making the transaction irreversible.

**Internal registries are zero-value safe.** Zero-value label and relationship-type registries lazily install reserved token 0, report length 0, export the reserved-name slice, import persisted names, and allocate token 1 first.

**Registered custom properties persist with type fidelity.** Custom struct values registered through `RegisterPropertyStructType` are written as registered type names plus MsgPack payloads and reconstructed through the registry. The property layer preserves value-vs-pointer shape across `DeepCopyValue`, the wire path rejects custom values whose MsgPack round-trip changes `HashBytes`, and untyped nil registration now returns `ErrUnsupportedValueType` instead of reporting success without registering a type.

**Store relationship replacements preserve indexed fields.** Store-level relationship replacement, history replacement, delete tombstones, and cascade tombstones now reject type/endpoint changes with `ErrInvalidStoreMutation`, and history writes reject snapshots stored under the wrong entity ID.

**Store node replacements preserve label indexes.** `ReplaceNode` and `ReplaceNodeWithHistory` reject label-token changes with `ErrInvalidStoreMutation`; use the label-token mutation APIs for label changes. Badger history replacement cleans secondary indexes from the stored current row, not from a caller-supplied previous snapshot.

**Store label-token helpers validate exact deltas.** `AddNodeLabelToken`, `RemoveNodeLabelToken`, and their history variants reject unchanged or differently changed node payloads before touching label indexes, history, caches, or vector indexes.

**Delete-with-history requires complete relationship tombstones.** `DeleteNodeWithHistory` rejects missing, duplicate, or unrelated relationship tombstones before deleting connected relationships or writing tombstone history.

**Node delete tombstones use the fully locked current row.** Graph-level node deletion now re-reads the node after locking the node and all connected relationships, so a node update that lands between the first adjacency scan and the final delete is captured in the delete tombstone history.

**Store-level node deletion fails closed on connected nodes.** `DeleteNode` and `DeleteNodesBatch` remove only unconnected node rows. If any target node still participates in a relationship, MemoryStore, BadgerStore, and TieredStore return `ErrInvalidStoreMutation` and leave all target nodes and relationships intact; connected deletes are handled by `DeleteNodeCascade` or `DeleteNodeWithHistory`.

**Adjacency reads fail closed for missing nodes.** Outgoing/incoming relationship reads now distinguish missing explicit node IDs from existing isolated nodes. Missing IDs return `ErrNodeNotFound`; existing nodes with no matching relationships still return empty results. `g.Temporal().NeighborsAt` preserves historical traversal for deleted target nodes by validating the target at the query time before treating missing current adjacency as empty current candidates.

**Store mutations reject non-positive entity IDs and reserved tokens.** Store-level node and relationship put/replace/delete/cascade/archive/restore/batch mutations, plus Badger split and repair helpers used by TieredStore routing and repair, now return `ErrInvalidStoreMutation` for zero/negative node IDs, zero/negative relationship IDs, zero node-label tokens, zero relationship types, or zero/negative relationship endpoints before any row, index, delete, archive, restore, scan, or batch mutation is applied. Repair incoming-index deletes and orphan relationship index purges keep in-memory and pending/persisted index state in sync.

**Graph mutations reject non-positive target IDs before entity work.** Standalone, transaction, and batch update/delete paths, node-label mutations, property compare-and-set, close-version calls, and admin archive/restore now validate zero/negative target IDs before lookup, locking, snapshot, capability checks, or queueing, returning `ErrInvalidStoreMutation` instead of a misleading not-found result.

**Relationship creates reject non-positive endpoints before endpoint work.** Pointer-form, ID-form, import, transaction, and batch relationship create paths now validate zero/negative endpoint node IDs before endpoint locks, duplicate probes, constraints, or relationship-type token allocation.

**Store history replacements reject nil payloads.** `ReplaceNodeWithHistory` and `ReplaceRelWithHistory` now return `ErrInvalidStoreMutation` for nil current/history payloads before reading IDs or marshaling rows.

**Provenance shadow inputs are type-checked.** `tkg_author_id` and `tkg_authorized_by` must be strings, `tkg_signature` must be `[]byte`, and invalid non-nil values now error instead of being stripped as zero-value metadata.

**Tiered high-frequency indexes follow shard topology.** HFI definitions are tracked and persisted inside `tiered.Store`, inherited by rotated hot shards, applied to lazily opened archives, restored after restart, and cleared with the store.

**Tiered temporal index kinds stay uniform across shards.** Retrying a partial temporal or high-frequency index create is allowed for the same kind, but HFI retries must use the same bucket size and `tiered.Store` returns `ErrTemporalIndexExists` before mutating shards when any shard already has the other temporal index kind for that label. If a later shard fails during create or drop, earlier shard-local changes are rolled back before the error is returned.

**Tiered delete-with-history restores pre-node relationship deletes on failure.** If `DeleteNodeWithHistory` deletes one or more connected relationships and then a later relationship delete or node tombstone write fails before the node row is removed, the store restores those relationships and their pre-call history.

**Tiered cascade deletes restore prior relationship deletes on failure.** Plain `DeleteNodeCascade` now snapshots connected relationships before mutation and restores already-deleted relationship rows if a later relationship delete or node row delete fails while the node still exists.

**Tiered cascade deletes purge orphan adjacency.** Plain `DeleteNodeCascade` now tolerates stale adjacency entries whose relationship row is already missing and removes those shard-local index entries while deleting the node, matching the history-aware cascade path.

**Tiered archive/restore ignores stale adjacency-only entries.** `ArchiveNode` and `RestoreNode` now move live connected relationships and purge stale source- and destination-shard adjacency entries whose relationship row is already missing, so a reference node is not stuck behind an orphan index entry.

**Delete batches coalesce duplicate IDs.** Store backends now normalize duplicate node/relationship IDs before batch delete validation, so a duplicated target is deleted once without corrupting counters or indexes.

**Tiered node batch deletes roll back prior shard deletes.** `DeleteNodesBatch` preflights connected-node guards across all shard buckets, then restores earlier accepted node deletes if a later shard bucket fails before the store-level vector index is updated.

**Tiered relationship batch deletes roll back prior shard deletes.** If `DeleteRelationshipsBatch` has already deleted relationships from one tiered shard and a later same-shard or cross-shard delete fails, the earlier relationship deletes are restored before the error is returned.

**Batch partial failures return an error.** `BatchBuilder.Execute` still returns detailed per-operation results, but any failed queued operation now also makes the error return wrap `ErrBatchFailed`. A normal `if err != nil` check can no longer miss partial failure.

**Batch builders are internally serialized and one-shot.** Concurrent queue calls are protected by the builder, and once `Execute` begins, later queue calls or repeat `Execute` calls return `ErrBatchDone` instead of racing or replaying prior operations.

**Batch event flushes preserve async priority.** `BatchBuilder.Execute` buffers mutation events during replay, releases graph and builder locks, then calls `PublishBatch` once so async event delivery sees the whole batch before applying priority order.

**Event buses work as zero values.** `events.EventBus` lazily initializes subscriptions, and `events.AsyncEventBus` lazily starts default dispatcher/queues on first use. Constructors remain convenience/configuration helpers, not mandatory setup.

**Event buses ignore nil subscriptions and nil receivers.** Sync and async `Subscribe(nil)` return no-op unsubscribe functions and do not install nil handlers; the async zero value is not started by a nil subscription. Nil `*events.EventBus` and `*events.AsyncEventBus` receivers treat `Subscribe`, `Publish`, `PublishBatch`, and `Close` as no-ops where applicable.

**Async event buses close without trapping blocked publishers.** `AsyncEventBus.Close` marks the bus closing, closes the stop signal so `BackpressureBlock` publishers waiting on full queues can return, waits for active publishers, then drains accepted events before the dispatcher exits. Post-close `Publish`, `PublishBatch`, and `Subscribe` calls are no-ops.

**Async event buses serialize dispatch for priority correctness.** `AsyncEventBusConfig.Workers` is accepted for compatibility, but dispatch is capped at one dispatcher so `PublishBatch` priority order cannot be broken during close or drain.

**Async event buses normalize invalid backpressure.** `events.NewAsyncEventBus` treats an unknown `BackpressureStrategy` as `BackpressureBlock`, preserving delivery instead of silently dropping every publish.

**Transaction rollback restores registries.** `GraphTx.Rollback` now restores label and relationship-type registries to their `BeginTx` snapshots, so rolled-back transactions do not leak newly created tokens.

**Post-close APIs stop at the lifecycle gate.** Read/query/index/registry/IO/admin/stats/hash, tx, and batch queue paths now re-check `ErrGraphClosed` under the graph lock before touching shared state, so `Close()` drains them before closing stores or persisting registries. No-error state mutators such as event-bus setters and ID generators no-op after close, and no-error list surfaces such as `g.Index().Providers()` return empty once the closed flag is visible.

**Provider listing fails closed after waiting for teardown.** `g.Index().Providers()` re-checks the closed flag under `g.mu.RLock`, so a caller that started before `Close()` but waited behind the lifecycle lock cannot return stale provider names.

**Temporal Allen helpers reject nil entity pointers.** `g.Temporal().NodeInterval(nil)` and nil-node `RelateNodes` calls now return `ErrNilNode`; `RelInterval(nil)` and nil-rel `RelateRels` calls return `ErrNilRelationship` instead of panicking.

**Temporal vector search no longer hides history read failures.** Candidate resolution now skips only expected temporal misses; store/history errors propagate from both filtered and over-fetch vector-search paths.

**Current vector search no longer hides corrupt candidates.** Badger and Tiered `SearchNearestNodes` now skip only concurrent-delete misses; unreadable candidate rows return an error instead of disappearing from the result set.

**Store vector search honors temporal filters and pagination.** Direct MemoryStore, BadgerStore, and TieredStore `SearchNearestNodes` calls now apply `ValidAt` / `ValidStart`+`ValidEnd` before the k-cut and honor `QueryOpts.After` and `Limit` over distance order. Graph-level `g.Index().SearchNearest` still paginates after temporal/history resolution, but it strips those cursor opts from the backend call so pagination is applied once.

**Vector search treats `k` as a limit, not an allocation budget.** `VectorIndex.SearchNearest` caps heap capacity by indexed entry count, and the graph-level temporal over-fetch fallback caps its resolved-candidate buffer by the bounded probe ceiling. Tiny indexes and external backends without filtered search no longer allocate or panic proportional to huge caller `k` values.

**ID-only queries honor temporal filters.** Store-level `AllNodeIDs` and `AllRelIDs` now apply temporal `QueryOpts` before cursor pagination. They still avoid entity deserialization for non-temporal scans; temporal scans read metadata because IDs alone cannot prove validity.

**Badger temporal pages verify cold-cache candidates before limiting.** BadgerStore `NodesByLabel`, `AllNodes`, `NodesByLabelAndProperty`, `RelationshipsByType`, and `AllRelationships` now consume `Limit` only after cache-miss rows have been fetched and checked against `ValidAt` / interval filters. A cold-cache expired early ID can no longer hide a later valid entity.

**Tiered corrupt delete-with-history clears vector candidates.** When a shard completes corrupt-node cleanup but returns the corruption error, `DeleteNodeWithHistory` now still purges the TieredStore-level vector entry before returning.

**Snapshot-style scans exclude standalone mutations.** `g.IO().Export`, `g.Temporal().Snapshot`, and `g.Admin().VerifyShard` now hold the graph write lock for their multi-read scans, so standalone mutations cannot interleave mid-export, mid-snapshot, or mid-verification.

**Temporal diff callbacks do not hold the graph lock.** `g.Temporal().DiffCallback` keeps graph read-lock windows around ID collection and entity resolution, then invokes `DiffHandlers` outside the lock so handlers can safely call graph read APIs.

**Tiered node replacement preserves shard-class invariants.** `ReplaceNode` and `ReplaceNodeWithHistory` now reject primary-label changes that would move a node between reference and event classes, instead of leaving the live row on the wrong shard.

**Badger node updates report corrupt current rows.** `ReplaceNode`, label-token writes, and their history variants now return old-state read errors when the node still exists, rather than silently overwriting corrupted durable data.

**Archive/restore migrates cross-shard relationship placement.** `ArchiveNode` moves a reference node to `refArchive` without dropping or rejecting relationships to live reference/event nodes. The relationship entity/out leg and incoming leg move to the shards implied by the endpoints' new locations; `RestoreNode` reverses the placement. Post-archive `AddRelationship` with one archived endpoint now writes the same split placement.

**Archive/restore preflights destination collisions.** `ArchiveNode` and `RestoreNode` reject duplicate destination node or live relationship placement before writing the temporary destination node, and purge orphan destination adjacency-only entries during that preflight. Failed moves roll back without leaving extra nodes behind.

**Tiered rollback failures surface with primary errors.** Cross-shard archive/restore moves and tiered batch creates now attempt to undo every completed step on failure. If rollback itself fails, the returned error includes the primary failure and the rollback failure instead of silently dropping the rollback error.

**Cascade deletes purge orphan relationship indexes.** Memory and Badger cascade-delete paths now remove stale relationship IDs from type and adjacency indexes even when the relationship entity is already missing. Badger orphan cleanup checks the relationship row itself instead of trusting index-derived state, and scans persisted type/out/in keys directly so ignored stale disk keys are removed too.

**Tiered relationship routing ignores stale type-index owners.** Tiered relationship ID routing and repair shard probes now verify that a candidate Badger shard has the relationship entity row, not only index-derived relationship membership. A stale type-index row in an earlier-probed shard no longer hides the live relationship row in a later shard, and orphan incoming repair purges the local stale type/out/in index set for that missing relationship.

**Badger restart rebuild uses entity rows for liveness.** Node rows rebuild `nodeIDs` and label indexes; relationship rows rebuild `relIDs`, type indexes, and entity-shard outgoing indexes. Stale label/type/outgoing keys without entity rows are ignored instead of becoming live IDs, while incoming-only relationship keys remain visible for tiered cross-shard repair.

**Badger fixed-width scans require exact lengths.** Restart rebuild, history ID scans, pending history overlays, per-entity history reads, and history truncation ignore overlong keys with valid prefixes instead of truncating them into live entity or history state.

**Vector index creation rejects wrong-dimension backfill rows.** `CreateVectorIndex` now fails with `ErrDimensionMismatch` and removes its placeholder index if an existing vector property has the wrong length, instead of returning success over a partial index.

**Graph index creation applies to future labels.** `g.Index().CreateProperty`, `CreateTemporal`, `CreateHighFrequency`, and `CreateVector` now create the label token when needed, so a successful create before the first matching node is still an active index for future writes.

**High-frequency index buckets are validated.** `g.Index().CreateHighFrequency` and Store-level `CreateHighFrequencyIndex` reject bucket sizes that are not positive whole milliseconds with `ErrInvalidTemporalIndexConfig`; a created HFI always has a time bucket width that matches the millisecond `Instant` precision.

**High-frequency indexes cover current and future nodes.** MemoryStore and BadgerStore backfill matching current nodes during `CreateHighFrequencyIndex` and maintain buckets on later node writes, replacements, label changes, history writes, and deletes. Badger creation fails closed on corrupt existing node rows so TieredStore can roll back earlier shard creates.

**Internal no-error index helpers tolerate nil and zero values.** Property indexes lazily initialize their value maps on first `Add`; zero-value high-frequency indexes lazily initialize buckets on first `Add`; zero-value LRU caches lazily initialize as minimum-capacity caches; nil property, temporal, high-frequency, and vector cleanup/read helpers no-op or return zero/nil values. Nil vector writes/searches fail closed with `ErrInvalidVectorIndexConfig`.

**Temporal constraints fail closed.** `g.Constraints().Add` and `Set` reject unknown temporal constraint kinds with wrapped `ErrTemporalConstraint` and `ErrInvalidTemporalConstraint` before changing the configured set. Relationship writes still fail closed if an invalid set reaches enforcement.

**ConstraintSet iteration rejects nil callbacks.** `temporal.ConstraintSet.ForEach(nil)` returns `ErrInvalidTemporalConstraint` consistently, including empty sets.

**Tiered reference labels are validated.** `tiered.New` rejects empty or whitespace-only `Config.RefLabels`; the ontology helper ignores empty names when used directly. Nil `*ontology.OntologyMapping` receivers behave like an empty mapping: all labels classify as event, registry swaps return nil, and `RefLabels` returns nil.

**Store index APIs reject token-0 labels.** Direct Store-level property, temporal, high-frequency, and vector index operations now reject label token 0 with `ErrInvalidStoreMutation`; persisted Badger/Tiered index definitions with token 0 are rejected on open.

**Index property targets reject shadow keys.** Property/vector index creation, drop, search, `Nodes.ByLabelAndProperty`, and temporal stored-property queries reject reserved `tkg_` property keys with `types.ErrReservedPrefix` because shadow properties are graph-resolved metadata, not stored `PropertySlice` keys.

**Badger index metadata fails closed.** Badger now rejects corrupt MsgPack property, temporal, high-frequency, and vector index-definition records on open instead of silently dropping persisted index metadata.

**Graph index target operations fail on invalid targets.** `g.Index().DeleteProperty`, `DeleteTemporal`, `DeleteHighFrequency`, `DeleteVector`, and `SearchNearest` validate labels and return the matching not-found sentinel for unknown labels instead of reporting a successful no-op or empty result. Property/vector target operations also enforce property-key length limits.

**Vector index configuration is explicit.** `CreateVectorIndex` rejects non-positive dimensions and unsupported distance metrics with `ErrInvalidVectorIndexConfig`; Badger and Tiered reopen paths reject invalid persisted vector-index definitions.

**Tiered vector metadata rejects conflicting duplicates.** Reopen deduplicates identical repeated vector definitions and rejects conflicting duplicate `(label, property)` records with `ErrVectorIndexExists` instead of letting the last persisted record win.

**Vector Store errors are public contract errors.** `pkg/graph/store` now owns the vector-index sentinels (`ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`, `ErrInvalidVectorIndexConfig`), with aliases kept on `graph`, `pkg/graph/index`, and concrete backends.

**Vector index maintenance rejects wrong-dimension writes.** Once a vector index exists, node create/update/label/history paths validate matching vector properties before mutating store state. A wrong-length vector returns `ErrDimensionMismatch` and leaves the node row unchanged.

**Persistent stores retain index definitions.** Badger persists property, temporal, high-frequency, and vector index definitions and rebuilds entries on open. Tiered persists store-level temporal/HFI tracking plus vector definitions. Dropping an index or clearing a store removes those definitions, so restart cannot lose active indexes or resurrect removed ones.

**Index metadata excludes unfinished builds.** Badger property, temporal, high-frequency, and vector definitions, plus Tiered vector definitions, are persisted only after a phased create finishes its backfill. Concurrent metadata writes cannot durably publish an in-flight placeholder.

**Index queries ignore unfinished builds.** Badger property, temporal, and high-frequency reads fall back to full scans while a phased index create is backfilling. Badger and Tiered vector searches report `ErrVectorIndexNotFound` for in-flight vector placeholders until finalization, so callers never get partial index results.

**Tiered vector definition writes are rollback-safe.** Tiered vector index create/drop snapshots the raw definition file before persisting, restores or removes it on write failure, and fsyncs the metadata directory after deleting the last definition.

**Tiered catalog writes are rollback-safe.** `ShardCatalog.Save` snapshots the raw JSON file before writing and restores or removes it on write failure, so in-memory catalog rollback does not leave a changed catalog on disk.

**Tiered catalog loads validate topology.** `shard_catalog.json` rejects duplicate names, multiple hot event shards, invalid kind/tier combinations, unsafe paths, negative stats, and invalid event windows before shard handles are opened.

**Tiered registry writes are rollback-safe.** The flat `registry.msgpack` save helper snapshots the prior raw file and restores or removes it if the atomic write reports an error.

**Tiered registry loads validate persisted names.** `registry.msgpack` rejects malformed label or relationship-type slices before startup/load code or deprecated single-registry save paths can preserve corrupt metadata.

**Tiered rotation requires inherited indexes.** Hot-shard rotation now aborts before catalog/topology changes if tracked temporal or high-frequency indexes cannot be installed on the new shard.

**Tracked temporal index application is all-or-error.** When TieredStore applies stored temporal/HFI tracking, a later conflict rolls back indexes created earlier in that same shard apply call and in earlier shards during persisted tracking load, while preserving pre-existing shard indexes.

**Phased index create cleanup is placeholder-scoped.** Badger property, temporal, high-frequency, and vector index builds, plus Tiered vector index builds, only remove the placeholder created by that call on failure and refuse to finalize if a concurrent drop or recreate replaced it.

**Tiered repair scans incoming index entries directly.** `RunRepair` Phase 1 now snapshots each shard's `in/` index entries instead of deriving them from live node IDs, so orphaned incoming entries whose endpoint node is already missing are also removed.

**Tiered catalog updates are save-or-rollback.** Lazy archive creation, catalog rebuild, and immutable-shard verification caching now return catalog-save errors and restore in-memory catalog state instead of keeping process-local metadata that restart cannot recover.

**Tiered catalog rebuild counts cold shards.** `RebuildCatalog` now opens closed cold event shards and recomputes their counts from durable data instead of trusting stale catalog counts.

**Tiered shard listing keeps closed-shard counts.** `ListShards` now reports catalog counts for closed cold event shards and closed archives instead of showing `0/0` only because the backing store is idle.

**Tiered clear wipes historical shards too.** `tiered.Store.Clear` now clears closed cold event shards and restarted warm shards, then resets catalog verification/stat caches so post-clear admin reads cannot reuse stale metadata.

**Tiered warm/cold shards remain mutable owners.** Warm/cold event tiers are not new-write targets, but their Badger handles are writable when open because existing event entities continue to update/delete on their owner shard after rotation and restart.

**Tiered idle-close errors surface later.** Cold-shard idle eviction now records and logs `BadgerStore.Close` failures, blocks later lazy checkout with that error, and joins it into `Store.Close`.

**Tiered close synchronizes event-shard handles.** `tiered.Store.Close` now takes each event shard's mutex before closing or clearing its `BadgerStore` pointer, matching `closeIdleShards` and avoiding unsynchronized close-vs-idle-close access.

**Tiered post-close routing fails closed.** Checked node/relationship routing, create-time rotation, valid index create/drop/search calls, direct read/count/bulk/history APIs, empty batch/history write edges, and public admin/metadata operations now return `ErrStoreClosed` once `tiered.Store.Close` has started, so direct Store callers cannot route into closed reference state, nil event shard handles, stale store-level index maps, stale reference-shard read data, or post-close catalog/registry mutation.

**Badger post-close APIs fail closed.** `badger.Store` now returns `ErrStoreClosed` for public operations after `Close`, including cache-hit reads, counters, empty batch/query helpers, history scans, index create/drop/search, metadata save/load, `Flush`, and split relationship helper writes. No-error diagnostic/routing helpers return zero or nil after close rather than exposing stale in-memory state.

**Memory post-close APIs fail closed.** `memory.Store` now marks itself closed on `Close`; public reads, writes, history, index, count, iteration, and empty-fast-path calls return `ErrStoreClosed` after shutdown, while exported test-only tampering helpers become inert.

**MemoryStore zero value is usable.** Direct callers can use `var ms memory.Store` as an empty in-memory backend. Reads return empty/not-found results, and node/relationship writes, index creation, and history writes lazily initialize internal maps before mutation.

**Persistent Store zero values fail closed.** Zero-value `badger.Store` and `tiered.Store` values are not operational backends, but lifecycle and checked read/write entry points return `ErrStoreClosed` instead of panicking on nil internal handles.

**Store lifecycle nil receivers fail closed.** Nil `*memory.Store`, `*badger.Store`, and `*tiered.Store` receivers return `store.ErrNilStore` from `Close` and `Clear` instead of panicking. `graph.ErrNilStore` aliases the same sentinel used for typed nil `Config.Store` rejection.

**Store iteration rejects nil callbacks.** Memory, Badger, and Tiered `ForEach*ID` iterators return `ErrInvalidStoreMutation` for nil callbacks on open stores instead of panicking; after close, `ErrStoreClosed` remains the first lifecycle error.

**Store iteration callbacks can re-enter stores.** Memory, Badger, and Tiered `ForEach*ID` iterators invoke callbacks outside backend locks and shard checkouts, so callback code can safely call Store methods. Badger history iteration pages IDs, masks pending truncation deletes before async flush, and stops at the start high-water mark to keep that boundary bounded.

**Store registry APIs reject nil registries.** Badger and Tiered registry save/load methods return `ErrInvalidStoreMutation` for nil label or relationship-type registries on open stores, while closed stores still return `ErrStoreClosed` first. `tiered.Store.SetLabelRegistry(nil)` is a no-op and cannot clear routing state.

**Tiered migration owns registry loading.** `tiered.MigrateFromBadger(src, dst)` requires an empty destination, loads label and relationship-type registries from the source Badger store, preflights migrated entity tokens and relationship endpoints against the source data, saves registries to the destination only after a successful copy, and rolls back inserted destination entities plus the destination registry file on failure. Nil stores, non-empty destinations, or missing source registry metadata for non-empty data return `ErrInvalidStoreMutation`. Closed source or destination stores return `ErrStoreClosed`.

**Vector search close races fail closed.** Memory, Badger, and Tiered vector searches re-check Store close state after vector index scans and filtered callbacks, so a close triggered during candidate filtering cannot return a successful post-close result.

**Tiered weekly shards use ISO week boundaries.** A 1-week `ShardWindow` starts on the ISO Monday for the ID timestamp's ISO week-year, including week-one years where January 1 belongs to the previous ISO week.

**Tiered sub-day shards use fixed-duration boundaries.** Accepted minute/hour `ShardWindow` values route by the enclosing duration bucket, and `tiered.New` rejects fractional-millisecond windows and idle timeouts so shard comparisons match snowflake timestamp precision.

**Tiered event creates honor caller-supplied timestamps.** Graph-generated current event IDs still land in the hot shard, while backfilled/imported event node IDs are created on the shard that their snowflake timestamp will later resolve to.

**Tiered duplicate checks include closed cold shards.** Caller-supplied node or relationship IDs already stored in an idle-closed cold event shard are rejected before a create can write the same ID into another shard. Relationship duplicate checks resolve the actual owner shard, not only the relationship ID timestamp, because a current-ID relationship can still be owned by an old start-node shard. Core-generated fresh IDs use an internal generated-ID fast path, including batch execution, so normal creates do not open unrelated cold shards.

**Tiered vector depth filters cover event shards.** `SearchNearestNodes` now applies `DepthHot`/`DepthWarm` to event-shard tiers before vector top-k selection, not only to `refArchive`. Cold/warm event vectors can no longer occupy the small-k heap for depth-limited searches that asked to skip them.

**Query depth fails closed.** MemoryStore, BadgerStore, and TieredStore now reject unknown `QueryOpts.Depth` values with `ErrInvalidShardDepth` on query methods that accept `QueryOpts`. `DepthAll` (`0`), `DepthHot`, and `DepthWarm` are the only accepted depth values; valid non-default depth selectors are equivalent to all data on single-shard stores.

**Graph queries validate depth before empty results.** Graph-level node, relationship, property, and vector queries reject invalid `QueryOpts.Depth` before an unregistered label/type/index can return a normal empty result.

**Interval QueryOpts require valid active ranges.** `ValidStart` and `ValidEnd` form an interval filter only when both values are greater than zero. Non-positive interval pairs are treated as no interval filter at both Store and graph API boundaries; active intervals with `ValidStart >= ValidEnd` return `ErrInvalidTimeRange`.

**Negative query limits fail closed.** `QueryOpts.Limit` accepts `0` for unbounded queries or a positive maximum. Negative limits return `ErrInvalidQueryLimit` instead of behaving like `0`.

**Negative query cursors fail closed.** `QueryOpts.After` accepts `0` for the first page or a non-negative entity cursor. Negative cursors return `ErrInvalidQueryCursor` instead of widening the query to the first page.

**History cursor scans validate raw pagination.** `AllNodeHistoryIDsFrom` and `AllRelHistoryIDsFrom` use the same non-negative cursor and limit contract: `limit == 0` returns all remaining IDs, negative limits return `ErrInvalidQueryLimit`, and negative cursors return `ErrInvalidQueryCursor`.

**History truncation rejects negative retention.** `TruncateNodeHistory` and `TruncateRelHistory` keep `keepVersions == 0` as the explicit clear-all request. Negative retention returns `ErrInvalidStoreMutation` and leaves history untouched.

**History reads reject non-positive target IDs.** Direct Store `Get*Version`, `Get*History`, and `Truncate*History`, plus graph `g.Nodes().History` / `g.Rels().History`, now return `ErrInvalidStoreMutation` for zero/negative IDs before empty-history, version-not-found, or no-op truncation behavior.

**Graph query names fail closed.** Label/type query, temporal label query, and count APIs now reject empty, whitespace-only, or overlong names before treating them as unregistered. Relationship adjacency helpers keep empty `typeName` as the documented "all types" selector, but reject whitespace-only or overlong non-empty type filters.

**Mutation names fail closed.** Node label mutations and relationship type creation reject empty, whitespace-only, or overlong names before property validation, entity lookup, registry lookup, or transaction rollback snapshots can return unrelated errors.

**Vector search targets fail before zero-k shortcuts.** `g.Index().SearchNearest` validates malformed label/property targets and unknown labels before returning an empty result for `k <= 0`.

**Transaction updates validate before snapshots.** `GraphTx.UpdateNode` and `UpdateRelationship` reject malformed update keys and values before rollback snapshot lookup, so invalid input is not hidden by missing-entity errors. Batch update queues reuse the same validation and accept provenance shadow keys consistently with standalone updates.

**Transaction reads have node/relationship parity.** `GraphTx.GetRelationship` mirrors `GetNode` so transaction callbacks can read relationships without calling standalone APIs that wait behind the transaction write lock. Successful tx-scoped node and relationship reads increment the same read counters as standalone reads, and rollback restores those counters.

**Batch empty updates are no-op reads.** Queued empty node/relationship updates still check that the entity exists at execute time, but successful empty updates no longer increment `BatchResult.Updated` or publish update events.

**Batch creates count in graph stats.** Successful batch node and relationship creates increment `g.Stats().Get().NodesAdded` and `RelsAdded` in the same units reported by `BatchResult.Created`.

**Temporal closes count as updates.** Successful `g.Nodes().CloseVersion` and `g.Rels().CloseVersion` increment the graph update counters because they persist entity state and publish update events; rejected repeat closes leave counters unchanged.

**Rolled-back transactions do not leak stats.** `GraphTx.Rollback` restores the operation-counter snapshot captured at `BeginTx`, so rolled-back writes are not visible through `g.Stats().Get()`.

**Transaction snapshots distinguish nodes and relationships.** Rollback tracks node and relationship snapshots separately even when caller-supplied imports use the same underlying snowflake value for a `NodeID` and `RelID`, so both entities revert independently.

**Rejected label adds do not register labels.** `g.Nodes().AddLabel` checks `MaxLabelsPerNode` before creating a token for a new label, so failed label additions cannot leave unreachable names in the registry.

**Failed node label writes do not keep new labels.** Node create/import/add-label paths restore newly allocated label tokens if the final Store write fails, multi-label allocation failures roll back partial suffixes, and the rollback guard releases its mutex even when a custom Store panics.

**Batch node queueing is registry-clean.** `BatchBuilder.AddNode` validates inputs and returns a node skeleton without registering unseen labels. `Execute` allocates the real label tokens, retokenizes the returned node pointer in place, and rolls those tokens back if node batch persistence fails or panics.

**Failed relationship creates do not keep new rel types.** Direct, import, and batch relationship creates run endpoint and temporal constraint checks before allocating a token for a new relationship type; if the final store write fails after allocation, the new token is rolled back. Failed writes cannot leave unreachable type names in the registry.

**Failed index creates do not keep new labels.** Graph-level index create APIs still register labels for successful future-label indexes, but restore the label registry if the backend create fails.

**Tiered bulk reads keep sorted ordering.** `tiered.Store.GetNodesByIDs` and `GetRelationshipsByIDs` now sort results by ascending ID like the memory and Badger backends. Reversed-input regression tests pin the backend and graph API behavior.

**Bulk ID reads fail loud on missing explicit IDs.** `g.Nodes().GetByIDs`, `g.Rels().GetByIDs`, and the Store-level `Get*ByIDs` methods now return the not-found sentinel when any requested ID is absent instead of reporting success with a partial result.

**Tiered creates reject cross-shard duplicate IDs.** `PutNode`, `PutRelationship`, and their batch variants reject caller-supplied duplicate IDs on the reference/archive shard and the timestamp-routed event owner; relationship creates also check existing owner placement before writing. Batch preflight catches these duplicates before writing any entity.

**Archive-close safety uses pinned-snapshot identity.** `RunRepair` resolves endpoints from its already-pinned shard snapshot, and history reads/truncation carry a stable archive-placement flag from the checked router. A concurrent `Close` that nils `refArchive` while waiting for active archive checkouts can no longer make repair or history code forget that a pinned store is the archive.

**Restored reference history stays visible at hot/warm depth.** Depth-limited history iterators now gate by current owner first: old archive history left behind by archive/restore no longer hides a restored ref-shard node or relationship from `DepthHot`/`DepthWarm`.

**Vector index creation is failure-atomic.** Badger and Tiered vector-index creation now remove their placeholder on scan/backfill failure instead of leaving an empty live index behind. Vector backfill also uses mutation tracking so concurrent node updates cannot be overwritten by stale scan results.

### What's new in 3.1.20

**Vector index stale after `UpdateNode` fixed.** `ReplaceNodeWithHistory` — called by every `UpdateNode` — never updated vector indexes. The old entry was never removed; the new one was never inserted. After any node update, `SearchNearestNodes` returned pre-update distances. Fixed in all three store backends.

**Batch operations now maintain temporal and vector indexes.** `PutNodesBatch` and `DeleteNodesBatch` silently skipped temporal and vector index maintenance. Batch-inserted nodes were invisible to temporal queries and vector searches; batch-deleted nodes remained as phantom candidates. Fixed in `MemoryStore`, `BadgerStore`, and `TieredStore` (six locations total). `TieredStore.ReplaceNodeWithHistory` was also missing the TieredStore-level vector map update.

**Dead code `shardForRelID` removed.** The unchecked variant was never called (all callers use `shardForRelIDChecked`). It contained a checkout-without-pin bug.

### What's new in 3.1.18

**Vector index stale after `UpdateNode` fixed.** `ReplaceNodeWithHistory` — called by every `UpdateNode` — never updated vector indexes. The old entry was never removed; the new one was never inserted. After any node update, `SearchNearestNodes` returned distances based on the pre-update vector. Fixed in all three store backends.

**Batch operations now maintain temporal and vector indexes.** `PutNodesBatch` and `DeleteNodesBatch` silently skipped temporal and vector index maintenance. Batch-inserted nodes were invisible to temporal queries and vector searches; batch-deleted nodes remained as phantom candidates in both. Fixed in `MemoryStore`, `BadgerStore`, and `TieredStore` (six locations total). `TieredStore.ReplaceNodeWithHistory` also fixed — it delegated to a shard but did not update the TieredStore-level vector index map.

**Dead code `shardForRelID` removed.** The unchecked variant was never called (all callers use `shardForRelIDChecked`). It contained a checkout-without-pin bug in its cold-shard probe loop. Deleted.

### What's new in 3.1.17

**Vector index phantom results after node deletion fixed.** `DeleteNodeCascade` and `DeleteNodeWithHistory` — the two paths used by the public `DeleteNode` API — failed to remove the deleted node from in-memory vector indexes. After deletion the node remained a k-NN candidate. Five locations fixed across all three backends: `MemoryStore.DeleteNodeCascade`, `MemoryStore.DeleteNodeWithHistory`, `BadgerStore.cascadeDeleteInner` (normal and corruption paths), `TieredStore.DeleteNodeCascade`, and `TieredStore.DeleteNodeWithHistory`.

### What's new in 3.1.16

**`BadgerStore.Clear` flush race closed.** A flush goroutine that snapshotted pending writes under `idxMu.RLock` but had not yet submitted its `WriteBatch` could race past `DropAll()` and resurrect pre-Clear entities after a restart. `Clear` now acquires `flushMu` first (same ordering as `flush()`), serialising both paths end-to-end.

**`sync.Map` field-replacement race fixed.** `bs.labelCounts = sync.Map{}` raced concurrent `NodeCountByLabel` calls that read the field without holding `idxMu`. Fixed with `Range+Delete`.

**All secondary indexes cleared on `Clear`.** `temporalIndexes`, `hfIndexes`, `vectorIndexes`, and (for TieredStore) `tempIdxLabels` are now reset. Previously they were left populated, causing "already exists" errors and stale vector candidates on a logically empty store.

### What's new in 3.1.15

**`SearchNearestNodes` now fully honours `QueryOpts`.** Previously temporal filters (`ValidAt`, `ValidStart`/`ValidEnd`), cursor pagination (`After`/`Limit`), and depth gating (`Depth`) were silently ignored. Temporal eligibility filtering now happens **before** the k-cut (via `FilteredVectorSearchCapability`) so near-but-ineligible candidates cannot crowd out farther-but-eligible candidates from the top-k. Cursor pagination is applied over the distance-ranked result order. TieredStore's depth filter excludes archive-resident reference nodes and out-of-depth event-shard nodes before heap selection, including when a temporal filter is present. **k <= 0** now returns `nil, nil` instead of panicking.

### What's new in 3.1.14

**`ImportGraph` panic safety.** `wireToNode`/`wireToRel` panic on token 0 (reserved). `ImportGraph` reads from an arbitrary `io.Reader`; a corrupt or malicious export stream became a process crash. `validateNodeWire`/`validateRelWire` guard all four record types (node, nodeHist, rel, relHist) before construction, returning the typed `ErrCorruptExport` sentinel on malformed IDs, token-0 or out-of-range tokens, invalid versions, non-canonical labels, and malformed property slices.

**`RunRepair` error propagation.** Phase 2's `GetRelationship` and `shardForNodeID` errors were all silently `continue`d — conflating `ErrRelNotFound` (legitimate TOCTOU skip) with I/O failures, routing failures, and closed-shard errors. Repair returned "succeeded" while needed `in/` repairs were missed. Now `errors.Is(err, ErrRelNotFound)` is the only legitimate skip; all operational errors propagate.

### What's new in 3.1.13

**`DeepCopier` interface enforced at registration.** `RegisterPropertyStructType` now returns `error` and validates both `HashableValue` (prevents runtime panic in hash computation) and the new `DeepCopier` interface (prevents store-boundary violations where nested mutable state in a registered struct survives `PutNode`/`GetNode` round-trips outside locks and index maintenance). Registration checks the exact form passed — value form with pointer-receiver methods is rejected; the correct idiom for pointer-receiver types is `RegisterPropertyStructType((*T)(nil))`. Untyped nil is rejected because it carries no type to register.

### What's new in 3.1.12

**Admin-path event-shard pinning.** `ListShards`, `RebuildCatalog`, `Clear`, and four index admin methods (`CreateTemporalIndex`, `DropTemporalIndex`, `CreateHighFrequencyIndex`, `DropHighFrequencyIndex`) now pin event shards via `checkoutStore`/`checkinStore` before touching their BadgerStores. Pre-fix, a concurrent `Close` could free a shard's DB while an admin call was still reading or writing it. `findRelInAnyShardStore` now consults the caller's pre-pinned snapshot instead of re-resolving, closing a `Close`-race window in `RunRepair`.

**`ArchiveNode` / `RestoreNode` serialised under `g.mu.Lock()`.** Both admin methods now take the full write lock, the same exclusion class as a transaction. Pre-fix, a concurrent `AddRelationship` could slip between `ArchiveNode`'s adjacency pre-scan and its cascade, creating a cross-shard rel the cascade then partially destroyed.

**`PutRelationship` cross-shard archive guard.** This release added a guard for relationships crossing the archive boundary. Superseded by later releases that migrate relationship placement across `refArchive` and live shards.

### What's new in 3.1.11

**refArchive parity in indexed and bulk reads.** `NodesByLabel`, `NodesByLabelAndProperty`, `NodeCountByLabel`, `RelationshipsByType`, `RelCountByType`, `AllNodes`, `AllRelationships`, `AllNodeIDs`, `AllRelIDs` now include archived entities at `DepthAll`. Pre-fix, archived nodes stayed `GetNode`-addressable but vanished from indexed/bulk reads. `DepthHot`/`DepthWarm` continue to exclude archive (caller explicitly asked for hotter tiers).

**Close-race protection on archive paths.** `shardForNodeIDChecked` / `shardForRelIDChecked` / `forEachHistoryShard` / `findRelInAnyShardStore` / `ArchiveNode` / `RestoreNode` now pin the archive via `checkoutArchive`, mirroring the `activeReqs` discipline used for cold event shards.

**`ArchiveNode` cross-shard relationship handling.** This release rejected relationships crossing the archive boundary to avoid data loss. Superseded by later releases that move relationship placement during archive/restore.

### What's new in 3.1.10

**History-aware indexed candidate planning.** `NodesByLabel`, `NodesByLabelAndProperty`, `RelationshipsByType`, `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` now derive candidates from the appropriate index (label / property / type / adjacency) and merge them with history IDs. Previously they fell back to `Store.ForEachNodeID` over every entity. Performance fix for any temporal query with a narrow predicate.

**`AllNodes` / `AllRelationships` history-aware temporal opts.** Closes a hole in v3.1.7's history-aware sweep: temporal `QueryOpts` on these generic entry points now resolve through the union of current + history IDs.

**Batch hardening.** `BatchBuilder.AddRelationship` now enforces `AllowSelfLoops`; batch metadata is stamped at execute time (not queue time); rels with failed-create endpoints are skipped with diagnostic errors; endpoint integrity hashes captured under endpoint lock; failed relationship creates restore queue-time `TxFrom`, endpoint hashes, and type tokens. See `CHANGELOG.md` `[3.1.10]` for the full list.

**Batch update queues snapshot inputs.** `BatchBuilder.UpdateNode` and `UpdateRelationship` deep-copy queued update maps, including nested property values, so caller mutations after queueing cannot change what `Execute` applies.

### What's new in 3.1.9

**`IndexProvider` extension point.** Out-of-tree indexes plug into the graph through `index.IndexProvider` (`Name()`, `OnEvent(ev) error`, `Close()`) and may implement `index.Initializable` for bulk-load through a read-only `GraphReader`. Providers register via `g.Index().RegisterProvider`, receive lifecycle events through the attached sync or async event bus, and own their persistence + query routing. Nil and typed nil providers are rejected before `Name()` is called, and empty or whitespace-only provider names are rejected before registration or unregistration lookup. `Graph.Close()` and `g.Index().UnregisterProvider` wait for in-flight `Init` before calling provider `Close()`. `OnEvent` errors are logged with provider and event context but do not veto already-committed graph mutations. First consumer is tkgd's spatial R-tree.

**`HashableValue` interface.** Custom property struct types can now participate in node/relationship integrity hashing. Register the type via `types.RegisterPropertyStructType`; implement `HashableValue.HashBytes() []byte` for a deterministic binary representation. Treat `HashBytes` like a wire format — once shipped, the encoding is locked because every existing hash chain depends on it.

### What's new in 3.1.8

**Typed entity IDs.** Public Graph API and all internal plumbing now use typed wrappers `types.NodeID` / `types.RelID` / `types.EntityID` instead of raw `snowflake.ID`. The compiler now catches NodeID/RelID/EntityID mixups that previously passed silently. Migration for downstream callers is mostly mechanical: `n.InternalID().SnowflakeID()` → `n.ID()` at typed Graph callsites. See `CHANGELOG.md` `[3.1.8]` → "Migration notes for downstream consumers" for the full upgrade guide. `InternalID()` retained as a deprecated alias for source compatibility.

**TieredStore cross-shard hardening.** Seven distinct correctness fixes around cold-shard rel reachability, history fan-out, checkout pinning, refArchive race protection, and a primary-label-class invariant. Restores parity with `MemoryStore` and `BadgerStore` for tombstones after deleting reference nodes and cross-shard relationships. New shared store-contract test suite runs the same behavioural guarantees against all three Store implementations.

### Behavior change in 3.1.7

`NodesByLabel(label, opts)`, `NodesByLabelAndProperty(label, key, value, opts)`, and `RelationshipsByType(typeName, opts)` now scan history when called with a temporal `QueryOpts` (`ValidAt` and/or `ValidStart`/`ValidEnd`). Previously these generic entry points routed temporal queries through store-side pushdown that consults only current indexes, so a node that had a label at the requested time but no longer carries it (or a relationship that has since been deleted) was silently missed. Callers using temporal opts will now see different — and correct — results. Non-temporal calls retain the original fast pushdown. See `CHANGELOG.md` `[3.1.7]` for details.

## Snowflake Configuration

Both generator sets (nodes and relationships) are initialized with explicit parameters:

```text
+---------------------------------------------------------------+
|  1 bit  |       48 bits        |   5 bits   |     10 bits     |
|  zero   |     time (usec)      |   node ID  |    sequence     |
+---------------------------------------------------------------+
```

| Parameter | Value |
|-----------|-------|
| Epoch | `2026-01-01 00:00:00 UTC` |
| Precision | Microseconds (`snowflake.WithMicroseconds()`) |
| Node bits | 5 (max `SnowflakeNodeID` is 15 since it maps to `id*2` and `id*2+1`) |
| Step bits | 10 (1024 unique IDs per microsecond) |

Each concurrent graph instance **must** use a different `Config.SnowflakeNodeID` (0-15). Generators are stateless — no counter persistence, no crash recovery.



## Build & Test

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector enabled
make test-integration  # integration tests (long-running)
make bench-graph-baseline    # repeatable graph API benchmark baseline
make bench-graph-production-small  # production-shaped graph benchmark suite
make bench-graph-production-large  # large stress graph benchmark suite
make cover          # coverage report -> coverage.html
make check          # pre-commit: vet + build + test
make ci             # full pipeline: fmt-check + vet + lint + build + test-race + security + vulncheck
make fmt            # format code
make lint           # golangci-lint (errcheck, govet, staticcheck, revive, ...)
make security       # gosec static analysis
make vulncheck      # govulncheck for known CVEs
```

Linting via `make lint` (golangci-lint) is part of CI.

Run a single test:

```bash
go test -run TestFoo ./pkg/types/
```

## Tutorials

Progressive tutorials in `tutorials/`, each a standalone `main.go`:

| Tutorial | Topic |
|----------|-------|
| `001_basic_graph` | Create nodes, relationships, and query the graph |
| `002_temporal` | Temporal metadata (ValidFrom/ValidTo, CreatedAt, UpdatedAt) |
| `003_badger_persistence` | On-disk `badger.Store`, close/reopen, registry persistence |
| `004_full_features` | Update operations, version history, hash chain integrity |
| `005_performance` | Benchmark `memory.Store` vs `badger.Store` (throughput, memory, storage) |

Run any tutorial: `go run ./tutorials/001_basic_graph/`

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
