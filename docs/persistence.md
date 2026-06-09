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
