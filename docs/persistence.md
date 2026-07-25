# Persistence Documentation

## Persistence (Badger)

Configure with `Config.BadgerDir` (on-disk) or `Config.BadgerInMemory: true` (testing):

```go
g, err := graph.New(graph.Config{
    SnowflakeNodeID: 1,
    BadgerDir:       "/path/to/data",
})
// ... use graph ...
g.Close() // saves registries + closes DB
```

Data is serialized using msgpack. Keys use fixed-width binary encoding with single-byte prefix tags for correct sort order. Registries are restored on startup and are saved both when new label/relationship-type tokens are committed and again during `Close()`. Rollback paths persist restored registry snapshots, so durable entity rows are not left without token mappings when a process restarts after successful writes but before graph-level close.

On-disk format versioning: every persisted `NodeWire`/`RelWire` row carries a per-row `FormatVersion` (`fv`; absent on pre-versioning rows = version 1), and the badger backend verifies/stamps a store-level `wire_format_version` meta marker at open. A directory (or row) written by a newer release fails closed with `store.ErrWireFormatVersionUnsupported` instead of misdecoding fields this binary does not know about. Backward compatible with every existing directory: absent marker is stamped on the first read-write open, legacy rows keep decoding.

**Wire format v2 — patchable transaction-time slot (current).** In v2 the transaction-time tail (`tf`/`tt` — TxFrom/TxTo) is a FIXED-WIDTH, always-present TRAILING slot: the last two map entries, each a full-width int64 (the 9-byte `0xd3` form msgpack already emits for present int64 values), emitted after every other field. This lets the ingest pipeline (ADR-0006 §4.5) pre-encode a whole row on the producer thread with a ZERO tail and have the single applier patch in its stamped TxFrom/TxTo with a bounded in-place memory write (`storeutil.PatchWireTemporalTail`, marker-validated and fail-closed with `ErrCorruptWire` on a truncated or non-v2 buffer) instead of a second msgpack pass. The equivalence `Patch(PreEncode(E, 0), T) == Encode(E, T)` holds byte-for-byte by construction — the tail is the only difference — and because TxFrom/TxTo are not hashed, a patched buffer still passes `Verify*Chain` and replicates verbatim. DECODE IS VERSION-TRANSPARENT: msgpack map keys are self-describing, so both v1 (omitempty mid-map tail) and v2 (fixed tail) rows decode with no read-side version branch; mixed v1+v2 rows coexist in one directory. Opening an old (v1-marked or unmarked) directory with v2 code reopens it read-write and restamps the marker to 2; subsequent writes are v2. Apply-side CONSUMPTION of the pre-encoded buffer is now WIRED (ADR-0006 §4.5 Scenario B): the optional `store.PreEncodedPutCapability` (`PutNodesBatchPreEncoded`, native memory/badger only) lets the ingest applier hand the store the producer-thread-encoded row with its tail patched, skipping the second msgpack pass. The applier uses a pre-encoded buffer ONLY when it is provably byte-identical to the store's own encode — for a genesis create the sole field that can diverge between prepare and apply is the label tokens (a probe token re-stamped to a different real token, §4.4), so when the finalized tokens differ from the pre-encoded ones (or the patch fails, or the store declines the capability) the applier falls back to encode-at-flush, which is byte-identical by construction. Provenance is by the typed in-process buffer the applier carries, NEVER by sniffing stored bytes: `HasWireTemporalTail` is a debug/validation helper, never a "this stored row is patchable" decision (v1/foreign marker bytes can coincide). The change-log put body stays the UNTOKENIZED encode-at-flush form on both paths, so cross-backend change-feed parity is byte-identical.

**Threat model — the transaction-time tail is deliberately OUTSIDE the content hash.** TxFrom/TxTo have never been part of `ComputeNode/RelHash` (the hash keys on labels/type + properties + id + version, not transaction time), so a patched buffer replicates verbatim and passes `Verify*Chain` unchanged — that is exactly what makes the patch sound. The corollary is that the tail's integrity rests on TRANSPORT and STORAGE integrity (the msgpack framing, the Badger WriteBatch, the change-log co-commit), NOT on the entity hash: a bit-flip in the persisted tail is not detected by `Verify*Chain` (it never was — this is unchanged from v1, where TxFrom was also unhashed). `PatchWireTemporalTail` marker-validates the fixed slot before writing (fail-closed with `ErrCorruptWire`) so a MIS-FED in-process buffer is never silently corrupted, but it does not and cannot cryptographically bind the stamped time to the row's content.

Write-pressure bound: `badger.Config.MaxPendingWrites` (default 100,000 ops; negative disables; moot under `SyncWrites`) bounds the async write buffer. It is a backend-level knob with no `graph.Config` mirror — reach it by constructing the `badger.Store` yourself and passing it as `Config.Store` (the same is true of `badger.Config.FlushInterval` below). Dirty cache entries are never evicted by design, so without the bound a sustained burst faster than `FlushInterval` grows memory without limit; at the bound the writing call flushes synchronously (backpressure), and a failing backpressure flush surfaces its error to the writer with the ops requeued. `Store.PendingWriteCount()` exposes the pressure signal.

Sync writes: set `Config.SyncWrites: true` to eliminate the 100ms async flush window — each write is flushed to disk synchronously (Badger `WithSyncWrites(true)` + immediate `flush()` after every store call). This removes the in-memory buffer vulnerability at the cost of higher write latency. `FlushInterval` is forced to 0 and the background flush goroutine is not started when `SyncWrites` is true. Badger index definition mutations (`Create*Index` / `Drop*Index` for property, temporal, high-frequency, and vector indexes) use the same synchronous flush path after releasing `idxMu`; split relationship helper writeOps used by Tiered routing and repair do too.

Current and history entity reads validate semantic `NodeWire` / `RelWire`
invariants after MsgPack decode and before constructing `types.Node` or
`types.Relationship`. Corrupt-but-decodable rows with invalid IDs, token-0,
out-of-range tokens, non-canonical label lists, reserved property keys, or
unknown property tags return read errors instead of panicking or truncating.
Checked property-wire validation preserves valid `float32` NaN and infinity
values for scalar and slice properties, but finite `float64` values that would
overflow or round when reconstructed as `float32` remain corrupt wire. Float
payloads tagged as `float64` or `[]float64` reconstruct as float64 values even
when a decoded element arrives as `float32`, and `[]int` tags reconstruct
decoded `[]int64` payloads as `[]int`. Integer tags reconstruct decoded `uint`
values that pass the same checked range validation.
The checked encode helpers reject nil node/relationship payloads before reading
IDs, tokens, properties, temporal metadata, or integrity metadata.

Store write paths validate relationship indexed identity before replacements
or tombstones are accepted. `ReplaceRelationship`, `ReplaceRelWithHistory`,
`DeleteRelWithHistory`, and relationship tombstones passed to
`DeleteNodeWithHistory` must preserve the existing relationship type token and
endpoints; mismatches return `ErrInvalidStoreMutation` before current rows,
history, or indexes are changed.

Store write paths also reject zero entity identities. Current-row put/replace
and batch writes return `ErrInvalidStoreMutation` for zero node IDs, zero
relationship IDs, or zero relationship endpoints before endpoint lookup,
routing, row mutation, index mutation, or batch partial writes. Badger split
relationship helper writes validate the same relationship ID/type/endpoint
index tuple before writing or deleting entity/out or incoming-index rows, and
repair-only incoming-index deletion helpers reject zero end-node or relationship
IDs before scanning. Repair incoming-index deletes update the process-local
`inIdx` and pending/persisted write state together.

Atomic history replacement paths validate both payloads before dereferencing
or marshaling them. `ReplaceNodeWithHistory` and `ReplaceRelWithHistory`
return `ErrInvalidStoreMutation` for nil current rows or nil previous snapshots
instead of panicking.

Badger cache-miss deletes prefetch old entity rows before taking `idxMu.Lock`.
`DeleteNode` and `DeleteRelationship` then re-read the current cached row under
the write lock and use that row's labels, type token, and endpoints for index
cleanup. The normal cache-miss Badger read therefore happens outside the global
index write lock, while direct Store-level ID reuse in the prefetch window
cannot make cleanup use stale indexed metadata.

Persisted index definitions are validated before they become in-memory state on
open. Badger property, temporal, high-frequency, and vector definitions must decode as MsgPack
and reject label token 0 with `ErrInvalidStoreMutation`; Badger property/vector
and Tiered vector definitions also reject reserved `tkg_` property keys with
`types.ErrReservedPrefix`. Token 0 is an unset sentinel, and shadow keys are
graph-resolved metadata, not valid stored index targets.

After `badger.Store.Close`, public operations fail closed with
`ErrStoreClosed` instead of returning cache hits, O(1) counter values, empty
fast-path success, vector search results, history scans, registry data, or
split relationship helper mutations from closed in-memory state. No-error
diagnostics and routing helpers return zero or nil after close; the private
flush primitive still uses the Badger closed-DB sentinel to guard against
`WriteBatch.Flush()` deadlocks.

`memory.Store` has no external resources to tear down, but `Close` still marks
the lifecycle boundary. Public reads, writes, history, index, count, iteration,
and empty-fast-path operations return `ErrStoreClosed` after shutdown instead
of serving or mutating process-local maps. Exported test-only tampering helpers
return no data or no-op after close.

Store `ForEach*ID` APIs treat a nil callback as invalid input, not a panic:
MemoryStore, BadgerStore, and TieredStore return `ErrInvalidStoreMutation` on
open stores, while closed stores still return `ErrStoreClosed` before input
validation.

Badger history ID scans merge the async write buffer over persisted history
keys. Pending history writes add IDs immediately, and pending history delete
writeOps from `TruncateNodeHistory` / `TruncateRelHistory` mask persisted keys
until the write buffer flushes.

Badger and Tiered registry persistence APIs treat nil registry pointers as
invalid caller input. On open stores, nil label or relationship-type registries
return `ErrInvalidStoreMutation`; after close, `ErrStoreClosed` remains the
first error. `tiered.Store.SetLabelRegistry(nil)` is a no-op and does not clear
ontology routing state.

Successful graph writes, batch execution, index creation, and snapshot import
that create label or relationship-type tokens save the current registries to
persistent stores before returning. Batch execution treats a registry
checkpoint failure after the Store write as a committed-row error: the batch
reports the failure, but returned entity pointers remain in the finalized state
that matches the written rows. Transaction create/import methods add committed
rows to the rollback log before returning the checkpoint error, so a later
rollback removes those rows and restores the registry snapshot. Commit retries
the registry checkpoint before publishing events or releasing the transaction;
if the checkpoint still fails, the transaction remains open for retry or
rollback.

`tiered.MigrateFromBadger(src, dst)` requires an empty destination, loads label
and relationship-type registries from the source BadgerStore, wires destination
routing from that source label registry, preflights migrated entity tokens and
relationship endpoints against the source data, and saves both registries to the
destination TieredStore after a successful copy. Failures roll back inserted
destination entities, restore the previous ontology registry, and restore or
remove the destination registry file to match its pre-migration state. Nil
stores, non-empty destinations, and missing source registry metadata for
non-empty data return `ErrInvalidStoreMutation`. Closed source or destination
stores return `ErrStoreClosed`, including the empty-source case.

IO import/export treats stream endpoints as caller input. Nil and typed nil
readers return `ErrNilReader` before import staging files are created; nil and
typed nil writers return `ErrNilWriter` before export attempts any stream
write. This keeps malformed caller input out of the snapshot pipeline instead
of relying on panics from `io.ReadFull` or `Writer.Write`.

Vector search methods in all in-tree stores re-check close state after
in-memory vector index scans and filter callbacks before returning empty or
raw-ID results. Direct Store-level `SearchNearestNodes` applies temporal/depth
eligibility before heap selection and `QueryOpts.After`/`Limit` over the
distance-ranked result slice; graph-level search passes only backend-relevant
opts to stores and paginates after current/history resolution.

Badger temporal bulk reads do not let cold-cache candidates consume `Limit`
before exact filtering. Index snapshots plus cache peeks may drop cached rows
that are already known to be temporally invalid, but cache misses are fetched
and checked before they count toward the page.

Badger property, temporal, high-frequency, and vector index-definition metadata and Tiered
vector index-definition metadata skip phased-create placeholders until backfill
finalization completes. Placeholders remain visible in memory for concurrent
write maintenance, but they are not durable definitions and are not written by
unrelated concurrent metadata changes. Read paths treat the same placeholders
as not query-ready: Badger property/temporal/high-frequency queries use complete
scan paths while a create is building, and Badger/Tiered vector search returns
`ErrVectorIndexNotFound` until vector create finalization clears the build
marker.

Tiered vector-index definition create/drop snapshots the raw
`vector_indexes.msgpack` file before persisting changes. If the persisted
definition write reports an error, the old raw file is restored or a newly
created file is removed. Removing the last vector definition fsyncs the metadata
directory so a crash cannot resurrect a dropped index definition.

Tiered catalog saves apply the same raw-file rollback rule to the catalog JSON:
`ShardCatalog.Save` snapshots the existing file before writing and restores or
removes it if the atomic write reports an error.

Tiered registry saves apply the same raw-file rollback rule to
`registry.msgpack`: the helper snapshots the existing file before writing and
restores or removes it if the atomic write reports an error.

Tiered registry loads validate both persisted halves. Malformed label or
relationship-type slices are rejected before startup/load code or deprecated
single-registry save paths can preserve corrupt metadata.

## Tiered Persistence (`tiered.Store`)

For workloads with distinct reference data (Case, User) and high-volume events (Signal, Alert):

```go
import (
    "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
    "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

ts, err := tiered.New(tiered.Config{
    DataDir:     "/path/to/data",
    RefLabels:   []string{"Case", "Organization", "User"},
    ShardWindow: 7 * 24 * time.Hour, // weekly event shards
    ColdAfter:   30 * 24 * time.Hour, // demote warm→cold after 30 days
})
g, err := graph.New(graph.Config{
    SnowflakeNodeID: 1,
    Store:           ts,
})
```

Directory layout: `data/meta/` (catalog + registry + store-level index definitions), `data/reference/` (ref shard), `data/events/<window>/` (event shards), `data/archive/` (archived reference entities). Hot shard receives graph-generated current event creates; caller-supplied/backfilled event node IDs are created on the shard their snowflake timestamp resolves to. Public Store create-time duplicate checks use cold-shard-aware ID routing, so a node or relationship ID already persisted in an idle-closed cold event shard cannot be reused in another shard; relationship duplicate checks resolve actual ownership, not only the relationship ID timestamp, because a current-ID relationship can still live on an old start-node shard. Graph-generated single and batch creates use an internal generated-ID fast path and do not open unrelated cold shards. On window expiry, `RotateHotShard()` demotes hot→warm and creates a new hot shard. A 1-week `ShardWindow` uses ISO week starts: Monday for the timestamp's ISO week-year, with ISO week 1 anchored by January 4; accepted sub-day windows use fixed-duration starts that contain the timestamp. `ShardWindow` must be at least 1 minute and a whole millisecond. Warm/cold tiers are not new-write targets, but open Badger handles stay writable because existing event entities continue to update/delete on their owner shard after rotation and restart. Warm shards are recovered from catalog on restart. Cold shards are lazy-opened on first access and auto-closed after idle timeout; `ColdAfter`/`IdleTimeout` reject negative durations, and positive `IdleTimeout` must be a whole millisecond before the idle-close goroutine starts. Idle-close failures are recorded, logged, block later lazy checkout, and are joined into `Store.Close`. Explicit `Store.Close` and idle-close serialize on the event shard mutex before closing or clearing a shard handle. Archive/restore moves live relationship placement, purges orphan destination adjacency during preflight, and purges stale source-shard adjacency-only entries during final source-node removal. Badger restart rebuilds live node and relationship membership from entity rows instead of stale label/type/outgoing keys; incoming-only relationship keys remain visible for cross-shard repair. Badger orphan cleanup, tiered relationship routing, and repair shard probes verify the relationship entity row rather than trusting index-derived membership; repair purges local stale type/out/in keys and counters together for orphaned relationship IDs. Badger persists high-frequency index definitions and rebuilds their buckets on open. Tiered temporal and high-frequency index create/drop serialize against rotation, persist store-level tracking metadata, and keep one temporal index kind per label across all shards; HFI bucket sizes are positive whole milliseconds to match persisted `Instant` precision and same-kind HFI retries must use the same bucket; cross-kind or different-bucket leftovers return `ErrTemporalIndexExists` before other shards are mutated, later-shard failures roll back earlier shard-local changes, tracked-index application to a single newly opened shard removes earlier same-call creations if a later tracked definition fails, and persisted tracking replay rolls back earlier shard creations if a later shard rejects the metadata. Once close starts, checked routing, create-time rotation, valid TieredStore index create/drop/search calls, direct read/count/bulk/history APIs, empty batch/history write edges, and public admin/metadata operations return `ErrStoreClosed`. `SetLabelRegistry(nil)` is ignored so a direct caller cannot erase reference-label routing.


## Tiered background-error recovery

Idle/transient cold-shard close failures set a sticky background error and the tiered store fails closed on every subsequent path. `tiered.Store.RecoverBackgroundError()` re-probes the persistence path with an atomic catalog save once the operator has fixed the underlying condition; on success the gate clears in place (no close/re-open). It clears the lifecycle gate only — run `VerifyShard`/`RunRepair` before trusting affected shards for critical reads.

## Change-log / op-log (opt-in, `ChangeLog`)

With `graph.Config.ChangeLog` (or `badger.Config.ChangeLog` / `memory.WithChangeLog()`) enabled, every committed mutation appends a durable, ordered record to the change-log: the topology-agnostic foundation for horizontal scaling (change-data-capture, audit trail, point-in-time recovery today; read-replica streaming next). Off by default with zero write-path overhead.

**On-disk layout.** Records live under the `0x09/<8B big-endian LSN>` keyspace, value = `tag(1 byte) ‖ msgpack(body)`. A big-endian LSN makes a prefix scan yield ascending commit order. A `meta/last_lsn` watermark records the highest committed LSN.

**Atomicity.** The badger backend appends each record (and the `last_lsn` watermark) in the SAME `WriteBatch` as the entity rows and counters, so a record and the mutation it describes commit atomically — there is no committed-but-unlogged window, and on restart the LSN allocator reseeds from `last_lsn` (crash-consistent with the maximum `0x09` key), so LSNs stay strictly monotonic and are never reissued. With the async write buffer, only durably-flushed records are visible to the feed; backpressure counts buffered records so a hot-key workload cannot grow the (uncoalesced) log buffer without bound.

**Record tags** (`store.ChangeTag`, 13 in total — a consumer switching on the tag must cover all of them): `NodePut`/`RelPut` (1/2 — new current state), `NodeDelete`/`RelDelete` (3/4 — a `WithHistory` flag distinguishes a hard cascade from a tombstone-appending delete; node deletes carry the cascaded relationship IDs), `NodeHistoryVersion`/`RelHistoryVersion` (5/6 — explicit-version writes from import, bitemporal-cascade correction, migration, and transaction-rollback restore), `NodeHistoryTruncate`/`RelHistoryTruncate` (7/8), `Meta` (9 — RESERVED, no door emits it yet; see "Deferred" under Backups), `Clear` (10), `ForeignIncoming`/`ForeignIncomingDelete` (11/12 — the ADR-0010 Model-A cross-machine incoming half-edge stub and its cascade removal; a replica MUST route both by the END node's slot, since the rel's own slot is foreign, and both are idempotent), and `RangePurge` (13 — the ADR-0008 R3 retention range purge: ONE logical record naming a PREDICATE, body `RangePurgeBody{label token, boundary, mode}`, which the replica RE-EXECUTES against its own state rather than applying N per-entity deletes). Bodies reuse the `NodeWire`/`RelWire` format (so no on-disk wire-format bump) and are read back through `SafeUnmarshal` — a corrupt `0x09` value fails closed with `ErrCorruptWire`, never a process crash.

**Reading.** `g.Replication().ChangeFeed(afterLSN, limit)` returns committed records in ascending LSN order; `ForEachChange(afterLSN, fn)` streams them OOM-safe with the callback invoked outside store locks; `LastCommittedLSN()` is the watermark a session reads after a write for read-your-writes routing. A backend opened without a change-log returns an empty feed (`ChangeLogEnabled()` reports `false`). All three in-tree backends — memory, badger, and (since ADR-0005 §2) tiered — implement the capability.

**Replication model.** The change-log ALONE does not converge a replica from empty: a replica bootstraps from a full export snapshot (which includes the token registry) and then tails the feed from the snapshot's LSN. The memory backend implements the same capability (in-RAM, non-durable) for testing; all backends share one set of record-body builders, so their feeds are byte-identical for property-free entities and decode-equivalent for property-bearing ones (badger tokenizes property keys on the wire while memory keeps key strings).

**Tiered change-log (ADR-0005 §2).** The tiered backend has no single `WriteBatch` — a mutation commits in exactly one shard. It still provides a coherent store-global feed: a store-owned `atomic.Uint64` LSN allocator is injected into every shard (`badger.Config.ChangeLogSeqSource`, nil elsewhere = self-owned counter, so standalone badger is byte-for-byte unchanged), so each shard co-commits its own records + `LastLSNKey` in its own batch while LSNs form ONE total order. Reseed at open reads a single monotonic `changelog_lsn_watermark` key on the always-present reference shard (written after every log-bearing flush via `badger.Config.OnChangeLogFlush`), never opening a cold shard; an unreadable watermark fences the change-log with a sticky error and refuses to hand out LSNs (`RecoverChangeLog` clears it in place). The feed runs a flush-before-read durability barrier (`tiered.Store.Flush()` over every open shard — also satisfying the export `storeFlusher`), captures `W = LastCommittedLSN` right after the barrier, and runs a W-bounded paged k-way merge over every catalog shard (one shard checked out at a time; cold-shard log segments paged read-only) emitting only `LSN <= W`; records allocated during the drain are deferred to the next poll. The barrier and W-bound together eliminate the cross-shard flush-reordering silent-loss (a lower LSN buffered on a slow shard is never skipped by a tail that already passed a higher durable LSN). This unlocks `ExportSince`/`Watermark`/`ImportMerge` and read-replica bootstrap+tail on tiered primaries.

## Backups (one-call ergonomics)

`g.IO().BackupTo` / `g.IO().BackupDeltaTo` / `graph.RestoreInto` wrap `Export` / `ExportSince` / `Import` / `ImportMerge` / `HeaderOf` with filesystem orchestration — deterministic filenames and chain validation — so a caller does not have to invent a naming scheme or a manifest. **No new wire format**: every backup file is byte-for-byte what `Export`/`ExportSince` already produce; a file written by `BackupTo` is a valid `Import` source on its own, with or without `RestoreInto`.

**Naming is derived from the stream's own header cursor, never wall time.** `(*io.API).BackupTo(dir)` writes a full export to `dir/backup-<LSN>-full.tkg`, where `<LSN>` is `DeltaHeader.To.LSN` zero-padded to 20 digits (the full width of a `uint64`, so a directory listing sorts in chain order) — the exact cursor `HeaderOf` would decode back out of the file, not a value computed separately and possibly stale by the time the file is written. `(*io.API).BackupDeltaTo(dir, since)` is the `ExportSince` counterpart: `dir/backup-<sinceLSN>-to-<toLSN>-delta.tkg`. Both fsync the file before returning.

**Refuses to overwrite.** If the deterministic target filename already exists in `dir`, both methods return `ErrBackupExists` and leave `dir` untouched — a backup is staged to a temp file in the same directory first and only renamed into place once no collision is found. On a change-log-less backend `BackupTo` still succeeds with the zero `Cursor` (there is no change-log point to name it by), so every such backup in a directory shares the name `backup-00000000000000000000-full.tkg` — only the first call to a given `dir` succeeds, matching the "no silent overwrite" contract exactly.

**Empty deltas write no file.** If there is nothing committed after `since` to reproduce (zero change records beyond the header/registry — e.g. calling `BackupDeltaTo` again right after a successful backup, before any further mutation), NO file is written and the call returns `since` UNCHANGED: a caller polling "is there anything new since my last backup" gets a side-effect-free no-op instead of an empty file to track and prune.

**Declines exactly like `ExportSince`.** `BackupDeltaTo` on a backend with no active change-log returns the same wrapped `store.ErrCapabilityNotSupported` `ExportSince` returns; the caller's fallback is the same — take a full backup with `BackupTo` instead.

**Restoring a chain.** `graph.RestoreInto(cfg, dir)` opens a graph from `cfg`, then discovers exactly one full backup (`backup-<lsn>-full.tkg`) and zero or more delta backups (`backup-<since>-to-<to>-delta.tkg`) in `dir`. Before replaying anything it reads every file's header (`HeaderOf` — cheap, does not consume entity/change records) and validates the WHOLE chain is gapless: the full backup's cursor must equal the first delta's `From`, and each subsequent delta's `From` must equal the previous file's `To` — the same pairwise invariant `ExportSince`/`ImportMerge` already enforce one delta at a time, checked here across the whole set so a missing or foreign file is reported by name up front instead of surfacing as an `ImportMerge` failure partway through an already-mutated graph. A cursor-epoch mismatch (a delta from an unrelated graph lineage) fails closed with a wrapped `ErrCursorUnknown`; an LSN gap (a missing delta, or deltas that don't chain) fails closed with a wrapped `ErrDeltaBaseMismatch` — both name the offending file. Zero or more than one full backup in `dir` is also an error (nothing to guess from). On success, `RestoreInto` has replayed `Import` (the full backup) then `ImportMerge` (each delta, oldest first) and returns the opened graph, ready for use; on any failure it closes what it opened before returning the error.

**Deferred.** `MetaSet` does not yet emit a record (the `ChangeMeta` tag is reserved); and LSN-watermark-driven log GC (pruning records below the minimum replica-acked LSN) lands with read replicas — on tiered, a whole retention-dropped event shard drops its log segment with it, so consumers must checkpoint before a shard is retention-dropped (the same discipline any log-truncation policy requires).

## Persistent property index (opt-in, `PropertyIndexOnDisk`)

With `badger.Config.PropertyIndexOnDisk` (or the mirrored `graph.Config.PropertyIndexOnDisk`) enabled, entries created by `CreatePropertyIndex` are kept out of RAM: instead of populating the in-memory `PropertyIndex.Entries`/`numBuckets` maps, each indexed `(node, value)` pair becomes one row under a new `0x0A` keyspace. Off by default — a `CreatePropertyIndex`'d property still lives entirely in RAM unless this flag is set. Follows the same shape as `LabelIndexOnDisk`/`AdjacencyIndexOnDisk`: persisted keyspace + prefix/range iteration + a pending-write overlay that resolves set-vs-delete PER KEY over the persisted keyspace (lesson 57), not a running aggregate.

**On-disk layout.** `0x0A/<2B propertyKeyToken>/<domain-tagged value bytes>/<8B nodeID>`. The value payload is domain-tagged:

- **Numeric** (any int/uint/float width): a fixed 18-byte payload — `domainTag(1) | sortKey(8) | subtypeTag(1) | rawBits(8)`. `sortKey` is the order-preserving IEEE-754 sign-flip encoding of the value's float64 magnitude (lesson 25's bit-pattern case, applied to key bytes instead of the hash/equality contract), so every numeric width shares ONE ordered domain — a byte-range scan spans `int`, `uint`, and `float` values uniformly, mirroring the in-memory ordered view's own cross-subtype range semantics. The `subtypeTag`+`rawBits` trailer disambiguates EXACT equality across types that share a magnitude: `int64(5)`, `uint64(5)`, and `float64(5.0)` sort to the same position but remain distinct stored values (matching the RAM index's type-prefixed `Entries`-map equality).
- **Raw** (string / bool / `TemporalValue`): `domainTag(1) | rawBytes(var)` — the canonical `types.IndexablePropertyValueKey` string's bytes, verbatim. None of these types support range scans, so byte order doesn't need to encode magnitude, and the value key's own `"s:"`/`"b:"`/`"tv:"` prefix already prevents cross-type collisions. Length is bounded by the property's own `ValidationLimits.MaxPropertyValueSize` check at write time (properties are validated before they ever reach index maintenance) — the on-disk codec enforces no additional limit.

**Key design point.** The on-disk key deliberately omits the label token (unlike the RAM `PropertyIndexKey{LabelToken, PropertyKey}`), so a property key indexed by definitions on two DIFFERENT labels shares ONE physical row. This is safe because every reader (equality via `NodesByLabelAndProperty`, range via `ForEachNodeByLabelPropertyRange`) already re-fetches the candidate node and rechecks `HasLabelTokenRaw` before trusting a match — the same over-select-then-recheck contract those readers use against the RAM-mode indexed path. `DropPropertyIndex` reference-counts by PropertyKey (not the full label+key pair) before physically purging rows, so dropping one label's definition never corrupts a sibling definition on another label that indexes the same property key.

**Write-path maintenance.** Every node-mutation door maintains the on-disk keyspace: `PutNode`, `DeleteNode`, `ReplaceNode`, `ReplaceNodeWithHistory`, the four label-token doors (`AddNodeLabelToken{,WithHistory}`/`RemoveNodeLabelToken{,WithHistory}`), `PutNodesBatch`, `DeleteNodesBatch`, and the cascade-delete corruption-path brute-force purge (mirroring `PurgeNodeFromAllPropertyIndexes`'s RAM-mode sweep when node data is unavailable). Every site merges the property-index writeOps into the SAME `appendOps` call as the entity row and its other secondary-index writes — a property-index entry and the row it describes always land in one `WriteBatch`, never a committed-but-unindexed window.

**Enable on an existing directory.** Unlike the label/adjacency keyspaces (written transactionally since the format's inception, so their on-disk mode needs no migration), the `0x0A` keyspace is NEW. A directory that already has property-index DEFINITIONS (from a prior `CreatePropertyIndex` call, entries kept in RAM) is backfilled from current node state exactly once, the first time `PropertyIndexOnDisk` is turned on — guarded by a `wire_format_version`-style meta marker (`property_index_on_disk_built`) so a later open with the flag still on skips the rescan and trusts the keyspace the ongoing write-path maintenance already kept in sync.

**Requires a property-key registry.** The on-disk key uses a compact 2-byte property-key TOKEN, resolved through the store's property-key registry (`badger.Config.PropertyKeyRegistry`, always wired when opened via `pkg/graph`). `CreatePropertyIndex` fails closed with `ErrInvalidStoreMutation` in disk mode without one, rather than silently indexing nothing.

**Range cardinality trade-off.** `NodeRangeCardinality` (the O(1) exact-count accelerator backing `count(p) WHERE p.k > x`) always declines (`exact=false`) in disk mode instead of reimplementing the RAM ordered view's per-value bucket-size sum — the persisted keyspace does not maintain those bucket sizes. This is a pure availability difference, never a wrong answer: callers already handle decline by falling back to `ForEachNodeByLabelPropertyRange` plus an exact count.

## Persistent temporal-index rebuild accelerator (opt-in, `TemporalIndexOnDisk`)

With `badger.Config.TemporalIndexOnDisk` (or the mirrored `graph.Config.TemporalIndexOnDisk` / `tiered.Config.TemporalIndexOnDisk`) enabled, the maxTo-augmented temporal interval index (`g.Index().CreateTemporal`) keeps a compact raw-entry log on disk so a store reopen no longer has to re-derive every entry from a full node fetch+decode. This is a DIFFERENT trade-off shape than `LabelIndexOnDisk`/`AdjacencyIndexOnDisk`/`PropertyIndexOnDisk` above: those move an index OFF the RAM heap and answer LIVE reads from the persisted keyspace instead. The temporal interval index's stabbing (`QueryAt`) and interval (`QueryOverlap`) queries walk an implicit balanced BST augmented with per-subtree max-upper-bound (`subMax`) — a structure that has no on-disk analogue — so it always stays fully resident in RAM at runtime; the flag has zero effect on live query results within a session. It exists solely to make the REBUILD-AT-OPEN step cheap.

**The cost this eliminates.** `loadIndexesScan`'s first pass (an unconditional `KeyNode` prefix iteration) already decodes every node row once to rebuild `nodeIDs`/`nodeHashes`/`labelIdx`/property-key counts — that decode is unavoidable baseline overhead shared by every store. Historically, rebuilding a label's `TemporalIndex` was a SEPARATE, ADDITIONAL pass on top of that baseline: for every node carrying a temporal-indexed label, a second Badger point-get plus full msgpack decode of the entire row, purely to extract two `int64` fields (`from`, `to`). `TemporalIndexOnDisk` replaces that second pass with a compact prefix stream over the new keyspace.

**On-disk layout.** `0x0B/<2B labelToken>/<8B order-preserving-encoded from>/<8B nodeID>`, value = `<8B to>`. The `from` component is encoded with the standard sign-bit-flip trick (mirroring the numeric domain's IEEE-754 sign-flip in the property index above, applied to a signed `int64` instead of a float), so a plain prefix iteration over one label's sub-keyspace visits entries in EXACTLY the `(From ASC, ID ASC)` order `TemporalIndex.Entries` requires — `loadIndexesScan` streams straight from iteration order into `TemporalIndex.AddKnownAbsent`, no separate sort pass.

**Write-path maintenance.** Maintained ALONGSIDE — never instead of — the existing RAM maintenance (`indexpkg.AddNodeToTemporalIndexes`/`RemoveNodeFromTemporalIndexes`) at every node-mutation door that already calls those: `PutNode`, `DeleteNode`, `ReplaceNode`, `ReplaceNodeWithHistory`, the four label-token doors, `PutNodesBatch`, `DeleteNodesBatch`, and the cascade-delete corruption-path brute-force purge. `CreateTemporalIndex`'s own three-phase backfill writes the disk rows directly (no reliance on the rebuild-on-enable marker); `DropTemporalIndex` purges the whole `labelToken` prefix — safe unconditionally because (unlike the property-index keyspace, whose rows are shared across labels indexing the same property key) a `TemporalIndex` definition is exclusively keyed by `labelToken`: only one definition can ever exist per label.

**Enable on an existing directory.** The `0x0B` keyspace is new (like `0x0A`), so an existing directory with temporal-index definitions predating the flag is backfilled from current node state exactly once, on the first open with the flag set — guarded by the `temporal_index_on_disk_built` meta marker (mirroring `property_index_on_disk_built`), committed in the SAME `WriteBatch` as the backfill rows so a crash mid-backfill leaves either nothing or everything, never a half-built keyspace a later open trusts as complete. The marker is set unconditionally on the first open with the flag on (even an empty backfill), so any temporal index created afterward — in that session or a later one — stays self-sufficient via the live write-path maintenance above.

**Measured** (100k entities, one temporal-indexed label, ~6 properties each; Apple Silicon, in-tree): ~1.8x faster open (e.g. 795ms → 452ms). The ratio is driven specifically by eliminating the redundant SECOND full-row decode pass described above, not by avoiding node decode entirely (the first, unconditional pass still pays that cost) — a corpus with multiple temporal-indexed labels covering overlapping entities would see a larger win, since each additional label used to mean one more redundant per-entity decode pass. See `TestTemporalIndexOnDisk_OpenTimeMeasurement_100k` in `pkg/graph/store/badger`.

## Encryption at rest (opt-in, `EncryptionKey`)

With `badger.Config.EncryptionKey` (or the mirrored `graph.Config.EncryptionKey` / `tiered.Config.EncryptionKey`, which applies per shard) set, Badger encrypts SSTables, the value log, the WAL, and its own key registry with AES. Off by default, in which case the on-disk format is byte-for-byte the pre-encryption one.

**Key length is validated at `New`.** The key must be 0 (disabled), 16, 24, or 32 bytes — AES-128/192/256; any other length fails closed with `badger.ErrInvalidEncryptionKeyLength`. `EncryptionKeyRotation` is Badger's `EncryptionKeyRotationDuration` (how often a new internal data key is generated for the encrypted value log); zero keeps Badger's stock 10-day default, and the field is ignored when no key is set.

**Encryption REQUIRES both caches.** Badger panics — not returns an error — at `Open` when encryption or compression is on with `BlockCacheSize == 0`, and on the first encrypted SSTable flush with `IndexCacheSize == 0` (its stock `IndexCacheSize` default is 0, unlike `BlockCacheSize`'s 256MB). `New` therefore fails closed with `badger.ErrEncryptionRequiresBlockCache` / `badger.ErrEncryptionRequiresIndexCache` rather than letting either panic escape. Set both positive whenever `EncryptionKey` is set.

**Wrong key / plaintext directory fails at open, before any row is read.** Reopening an encrypted directory with the wrong key, or opening an existing PLAINTEXT directory with a non-empty key, fails `Open` with an error wrapping `badgerv4.ErrEncryptionKeyMismatch` (`errors.Is`-able) — Badger detects both by decrypting a sanity marker in its `KEYREGISTRY` file.

## History delta encoding (opt-in, `HistoryDeltaEncoding`)

With `badger.Config.HistoryDeltaEncoding` (or the mirrored `graph.Config.HistoryDeltaEncoding` / `tiered.Config.HistoryDeltaEncoding`) enabled, version-history rows (`0x07`/`0x08`) are stored as anchor+delta instead of full snapshots (ADR-0009 / B6): a version `V` with `V % HistoryAnchorInterval == 0` is a full ANCHOR and the rest are DELTAS carrying only the properties that changed vs that interval anchor, eliding large unchanged values a full snapshot would re-serialize every version. The CURRENT row (`0x01`/`0x02`) is always full. Off by default while the path soaks; a no-op on the memory backend, which keeps full snapshots as the differential oracle.

**No migration, and no wire-format bump.** The framing is self-describing — an anchor (and any legacy pre-flag row) is the raw full marshal, a delta is 1-byte `'D'`-tagged — so reads always accept BOTH forms and the flag can be toggled on an existing store. A downgraded delta-unaware binary opens the store but fails closed with `ErrCorruptWire` on a `'D'`-tagged row rather than misreading it.

**The anchor interval is baked into the on-disk layout.** `badger.Config.HistoryAnchorInterval` (mirrored on `graph.Config`; the tiered config carries only the on/off flag, so its shards use the default) overrides the spacing (0 = the default 16; validated at `New` as 0 or in `[2, 4096]`; moot when `HistoryDeltaEncoding` is off). A larger interval stores more deltas per anchor (less storage, more reconstruction reads); a smaller one the reverse. Because a delta reconstructed against the wrong anchor would be a SILENT misread, the interval is pinned by a persisted marker verified at open: reopening a delta store at a different interval fails closed with `store.ErrHistoryAnchorIntervalMismatch`. To change it on an existing delta store, rewrite history — there is no inline migration.
