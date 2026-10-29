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

On-disk format versioning and the integrity-hash format: see "Format versions and markers" and "Integrity hashes" below; upgrading across releases: "Upgrading a store".

**Wire format v2 — patchable transaction-time slot.** Layout: "Row wire format" below (`fv` 2). The ingest pipeline (since 4.14.0) pre-encodes a row on the producer thread with a zero tail, and the single applier patches the stamped TxFrom/TxTo in place (`storeutil.PatchWireTemporalTail`, marker-validated, fail-closed with `ErrCorruptWire` on a truncated or non-v2 buffer) instead of a second msgpack pass; `Patch(PreEncode(E, 0), T) == Encode(E, T)` holds byte-for-byte. Decode is version-transparent (msgpack keys are self-describing): v1 and v2 rows coexist, and a writable open of a v1-marked or unmarked directory restamps the marker to 2. The optional `store.PreEncodedPutCapability` (`PutNodesBatchPreEncoded`; native memory/badger, sharded routes each row to its slot) receives the patched buffer only when it is provably byte-identical to the store's own encode: the finalized label tokens equal the pre-encoded ones (a probe token re-stamped to another real token diverges) and the patch succeeded; otherwise the applier encodes at flush. Provenance is the typed in-process buffer, never sniffing stored bytes (`HasWireTemporalTail` is a debug helper). The change-log put body stays the untokenized encode-at-flush form on both paths, so feeds are byte-identical across backends.

**Threat model — the transaction-time tail is outside the content hash.** TxFrom/TxTo were never part of `ComputeNode/RelHash` (labels/type + properties + id + version), so a patched buffer replicates verbatim and passes `Verify*Chain`. The tail's integrity therefore rests on transport and storage integrity (msgpack framing, the Badger WriteBatch, the change-log co-commit), not on the entity hash: a bit-flip in a persisted tail is not detected by `Verify*Chain`. `PatchWireTemporalTail` marker-validates the slot before writing, so a mis-fed in-process buffer is never silently corrupted, but it cannot bind the stamped time to the row's content.

Write-pressure bound: `badger.Config.MaxPendingWrites` (default 100,000 ops; negative disables; under `SyncWrites` it bounds only the writes a `DurableCommit` group holds, which then flush in steps) bounds the async write buffer; the buffer flushes earlier at half of one badger transaction (`MaxBatchSize` bytes or `MaxBatchCount` entries, ~5 MB at the 64 MB memtable), so a backpressure flush commits as one transaction instead of a staged one that writes each entry three times. It is a backend-level knob with no `graph.Config` mirror — reach it by constructing the `badger.Store` yourself and passing it as `Config.Store` (the same is true of `badger.Config.FlushInterval` below). Dirty cache entries are never evicted by design, so without the bound a sustained burst faster than `FlushInterval` grows memory without limit; at the bound the writing call flushes synchronously (backpressure), and a failing backpressure flush surfaces its error to the writer with the ops requeued. `Store.PendingWriteCount()` exposes the pressure signal.

Sync writes: set `Config.SyncWrites: true` to eliminate the 100ms async flush window — each write is flushed to disk synchronously (Badger `WithSyncWrites(true)` + immediate `flush()` after every store call). This removes the in-memory buffer vulnerability at the cost of higher write latency. `FlushInterval` is forced to 0 and the background flush goroutine is not started when `SyncWrites` is true. Badger index definition mutations (`Create*Index` / `Drop*Index` for property, temporal, high-frequency, and vector indexes) use the same synchronous flush path after releasing `idxMu`; split relationship helper writeOps used by Tiered routing and repair do too.

### Durability and crash semantics (what an acknowledged write survives)

The badger backend acknowledges a write when it is applied in memory, not when it is on disk. What survives a failure depends on the mode:

| Mode | Process crash (kill -9, panic) | OS crash / power loss |
|---|---|---|
| Default (`SyncWrites=false`, `FlushInterval`=100ms, `MaxPendingWrites`=100,000) | Loses every acknowledged write still in the async buffer: normally up to ~100 ms of writes, up to `MaxPendingWrites` ops under a burst. Flushed batches survive (they are in Badger's log, in the OS page cache). | Additionally may lose recently flushed batches: Badger does not fsync without `SyncWrites`. A staged flush (above one badger transaction) whose staging the power loss tore is discarded at the next open, logged, like any lost unsynced flush. |
| `SyncWrites=true` | Nothing acknowledged is lost: each Store call is flushed and fsynced before it returns — except inside a `DurableCommit` group (`GraphTx` mutations, `Batch.Execute`, `Rollback`), whose writes are held and fsynced together when the group's door returns. | Same. |
| `graph.Config.DurableCommit=true` (default off) | A `GraphTx` commit (`Tx().Run*`), `Batch.Execute` or strong-ingest group is on disk when its door returns: one flush of the whole pending buffer plus a WAL fsync per open shard that took writes; a failed flush returns `ErrCommitNotDurable` with the group committed in memory and still pending. Standalone mutations and concurrent-mode ingest keep the async window. | Narrower: Badger's `Sync` covers only the active memtable WAL and the current value-log file, so rows of a group whose flush crossed a memtable or value-log switch can be unsynced after the return. Power-loss durability needs `DurableCommit` + `SyncWrites` on a badger, sharded or tiered store (`tiered.Config.SyncWrites`): a group then costs one Badger-synced WriteBatch per touched shard at its commit, not one sync per mutation (backlog 52), measured 13.5 ms per 530-op group against 12.5 ms for `DurableCommit` alone (backlog 15, `tasks/evidence/aisoc-commit-cost/52-bench.txt`). |

Atomicity across a crash is per flush, not per logical operation group:

- A flush is all-or-nothing (FLUSHPART, `docs/badger.md` rule 1): data, counters, change-log records and the LSN watermark commit as one badger transaction when they fit one (`MaxBatchSize` = 15% of `MemTableSize`, `MaxBatchCount` entries: about 10 MB / 104,855 at defaults); a larger flush is staged under `0x0E`, committed by the marker `flush_stage` (which raises the store format marker to 9), applied, unmarked and cleaned. `New` re-applies a marked flush before reading a row and deletes unmarked staging; a read-only open with a marker fails with an error matching badger's `ErrTruncateNeeded` (the tiered recovery probe then opens writable). A staged flush whose apply or unmark fails after its marker fails the store closed (`ErrStoreClosed` at every door, also matching the cause) until the next open applies it; it is never requeued, writes still buffered are lost at `Close` (whose final flush is refused, writing no open image), and in tiered the shard's error fails the whole store closed. The marker records whether its flush ran under `SyncWrites`: without, a staging that lost entries (a power loss) is discarded at open, logged; with, it can only be corruption, so the open refuses with `ErrInvalidStoreMutation` naming `badger.DiscardStagedFlush(cfg)`, which drops the flush from a closed store directory (its writes are lost; an applied part stays). Either discard also drops `node_count` and `rel_count`, so the next open counts the live rows. Before v4.75.0 a flush was one `WriteBatch`, which badger split above those limits: a crash kept a random part of it.
- A write the badger store could not persist is refused at its door (`store.ErrEntryTooLarge`, before any mutation): a row or change-log record above `ValueLogFileSize` (in memory, 1 MB), or an on-disk property index key above 65,000 bytes, each less what a staged flush adds (9 key bytes, 1 value byte). Before v4.75.0 such an entry was acknowledged, then badger refused it at every flush, and nothing written after it reached disk.
- A process that dies inside `Clear` (`Admin().Reset()`) of a badger store reopens the old store or an empty one: `Clear` deletes the entity counters, drops the data, then deletes the remaining metadata (CLEARMETA). Before, the metadata went first, and a crash left rows whose label and property-key tokens new labels then reused. A tiered or sharded `Clear` writes a pending marker first and clears the shard holding the registries last; an open that finds the marker finishes the `Clear`, so a crash at any stage reopens empty (CLEARSHARDS).
- Damage on disk is reported, not returned as data (VERIFY, v4.75.2): badger verifies every value-log value and table block it reads, a failed check is `ErrCorruptWire` or a refused open, and badger's errors and warnings reach `slog` when `Config.Logger` is nil. A writable open cuts the newest value-log file at its first bad entry (badger, `value.go:603-617`); rho warns when that cut written data. Before v4.75.2 none of this was checked or logged. badger reports a table block that fails its check on a point read or an iterator as a missing key; since a clean open stopped reading the rows (v4.84.0), a read that misses the row of a node or relationship the store lists, with no write to it in between, is `ErrCorruptWire`, never "not found" (`badgerstore_row_missing.go`).
- A multi-entity `g.Tx()` is **not** crash-atomic in any mode: its rows are flushed by the ordinary async flush or per call under `SyncWrites` (at the commit inside a `DurableCommit` group), so a crash before `Commit` can persist part of it (see `docs/architecture.md`, "Transaction isolation — what v4 actually guarantees"). `DurableCommit` adds durability on return, not atomicity: before the return a crash can keep any subset, and a flush above about 15 % of `MemTableSize` is split by Badger (all-or-nothing on disk is v5; "Durable-on-return commit" below).
- The strong ingest mode (`g.Ingest()`, `Sync`) coalesces a group's flushes (`store.GroupCommitCapability`; the group is flushed before it is acknowledged), the closest v4 has to an atomic multi-entity write; crash atomicity is still not promised (batch size limits and background flushes can split it).
- `tiered.Store` cross-shard relationship writes are two shard commits; a crash between them leaves residue that `RunRepair` resolves.
- The commit clock survives a crash (backlog 44): every flush that writes rows also writes the newest transaction stamp of every row the store has written (`0x0F/tx_commit_floor`, first in the same `WriteBatch`; tiered keeps its event and archive shards' floor on the reference shard, written ahead of each shard's batch and fsynced there before a `DurableFlush` fsyncs the rows it covers), and `graph.New` raises the clock to it (`store.CommitFloorCapability`). After a crash, `NowTx()` and new stamps stay above every stored row, also when a write burst ran the clock ahead of the wall; only a `NowTx()` / `AdvanceClock` reservation that wrote no row before the crash can be lost (the Close-time watermark covers those).

Durable meta writes (`MetaSetDurable`, the property-key registry save) sync badger's write-ahead log before they
return (METASYNC): a process crash never loses them, and a power loss does not either unless badger rotated its
memtable between the write and the sync, because `db.Sync` syncs only the active memtable's WAL; the write is then on
disk once the retired memtable's flush to L0 completes, seconds later. `SyncWrites=true` closes that gap. A power-loss
test of it needs a rig (LazyFS or dm-flakey) that rho does not have.

Choose `DurableCommit=true` + `SyncWrites=true` (table above) for power-loss durability of commit groups, `SyncWrites=true` alone when every acknowledged standalone write must never be lost; the default trades that window for throughput. The memory store is not durable at all (sealed relationship segments of a `SegmentDir` excepted).

### Durable-on-return commit (`DurableCommit`)

`graph.Config.DurableCommit` (off by default; backlog 11) makes `GraphTx.Commit`, `Tx().Run*`, `Batch.Execute`
and the strong ingest applier call `store.DurableFlushCapability.DurableFlush` once per group, after the group is
applied and the graph locks are released (readers and the next writer do not wait for the fsync). Badger's
`DurableFlush` holds `flushMu` across the normal flush (pending buffer, counters and change-log records in one
WriteBatch) and a `db.Sync()` of the WAL; the sync runs only when a WriteBatch reached the WAL without an fsync since
the last one (the `unsynced` flag every flush sets), never under `SyncWrites`. Tiered folds it over the reference
shard, the open archive and the open event shards (a closed cold shard has nothing pending and is not opened);
sharded over every slot. A store without stable storage (memory, `BadgerInMemory`, in-memory tiered/sharded) fails
`New` with `ErrCapabilityNotSupported`.

- After success a crash keeps the whole group; before the return it can keep any subset (a background flush may
  have written part of it, and Badger splits a buffer above its transaction limit, about 15 % of `MemTableSize`,
  ≈ 9.6 MB at the 64 MB default, in arbitrary order). A consumer that writes its cut record last and treats rows
  above the last durable cut as unfinished is safe while that flush fits one Badger transaction.
- A failed flush returns `ErrCommitNotDurable`: the group is committed in memory and requeued, the next successful
  flush persists it. `RunWithLSN` still returns the LSN and `Batch.Execute` its result (with a `durable-commit`
  `BatchError`), so a caller never re-applies a committed group. Standalone mutations and concurrent-mode ingest
  `Submit` keep the async flush.
- With `SyncWrites` too, the per-mutation sync is held inside the group (`store.MutationFlushHoldCapability`,
  badger and tiered): each `GraphTx` mutation, the whole `Batch.Execute` window and a `Rollback` run under the
  exclusive graph lock with the flush held, and the group's one `DurableFlush` commits the held writes as one
  Badger-synced WriteBatch (backlog 52: one sync per group instead of one per mutation). A held buffer that reaches
  `MaxPendingWrites` flushes synced in steps; `Rollback` ends with one durable flush. Side effect: each `GraphTx`
  mutation takes `c.mu.Lock` instead of the shared lock, so standalone writers, readers and concurrent ingest lanes
  wait for it (in-memory work only). On tiered one shared hold covers every shard, one opened during the hold
  included; sharded keeps per-mutation syncs.
- `DurableFlush` re-checks the closed state under `flushMu`; `Close` sets `closing` before its final flush, so a
  commit racing `Graph.Close` gets `ErrCommitNotDurable` wrapping `ErrStoreClosed` while `Close`'s own flush writes
  the group.

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

Store-boundary invariants (positive IDs, immutable indexed identity, nil payloads and callbacks, closed and
zero-value stores, index-create placeholders, delete lock scope, vector and temporal paging) are owned by
[`docs/architecture.md`](architecture.md#design-invariants); `tiered.MigrateFromBadger` by `docs/api.md`
"Migration". Storage-side details not stated there:

- Store read methods that take caller IDs (`GetNode`, `GetRelationship`, `Get*ByIDs`, adjacency reads,
  `Get*Version`, `Get*History`, `Truncate*History`) reject zero or negative IDs with `ErrInvalidStoreMutation`
  before not-found or empty handling; adjacency `typeToken == 0` means "all relationship types". Repair-only
  helpers (`PurgeOrphanRelationshipIndexes`, `DeleteIncomingByRelID`, `ScanAndDeleteIncoming`) reject them before
  scanning. History snapshots carry the same entity ID as their storage key. Delete batch methods coalesce
  duplicate IDs before validation.
- Tiered `DeleteNodeCascade`, `DeleteNodesBatch` and `DeleteRelationshipsBatch` restore earlier accepted deletes
  when a later bucket or cross-shard split delete fails while the node row still exists.
- Non-nil `ForEach*ID` callbacks run outside backend locks and tiered shard checkouts, so they may call Store
  methods. Badger history iterators page IDs and stop at the start high-water mark, so IDs a callback creates do
  not extend the active iterator.
- Badger fixed-width key scans are exact-length only: restart rebuild, history ID scans, pending history overlays,
  per-entity history reads and truncation ignore overlong keys with a valid prefix.
- Badger history ID scans merge the async write buffer over persisted history keys. Pending history writes add IDs immediately, and pending history delete writeOps from `TruncateNodeHistory` / `TruncateRelHistory` mask persisted keys until the write buffer flushes. A read of one entity's history finds its buffered rows by the entity's key prefix, as the on-disk property lookup finds its values' buffered entries, not by walking the buffer: its cost does not grow with the writes buffered beside it (HISTPEND, v4.89.4: one read beside 64,000 unrelated buffered writes took 86x its time beside 1,000, now 0.5-1x, `TestHistPendHistoryReadIgnoresUnrelatedPending`). A read of a whole history keyspace (the exact erasure closure) still walks the buffer.
- Persisted index definitions are validated before they become in-memory state on open. Badger property, temporal, high-frequency, and vector definitions must decode as MsgPack and reject label token 0 with `ErrInvalidStoreMutation`; Badger property/vector and Tiered vector definitions also reject reserved `tkg_` property keys with `types.ErrReservedPrefix`. Token 0 is an unset sentinel, and shadow keys are graph-resolved metadata, not valid stored index targets.
- Successful graph writes, batch execution, index creation, and snapshot import that create label or relationship-type tokens save the current registries to persistent stores before returning. Batch execution treats a registry checkpoint failure after the Store write as a committed-row error: the batch reports the failure, but returned entity pointers remain in the finalized state that matches the written rows. Transaction create/import methods add committed rows to the rollback log before returning the checkpoint error, so a later rollback removes those rows and restores the registry snapshot. Commit retries the registry checkpoint before publishing events or releasing the transaction; if the checkpoint still fails, the transaction remains open for retry or rollback.

Tiered metadata files are all-or-error: vector-index definitions, temporal / high-frequency tracking, the catalog
JSON (`ShardCatalog.Save`) and `registry.msgpack` each snapshot the raw file before writing and restore or remove
it when the atomic write reports an error, so in-memory rollback cannot diverge from the file. Removing the last
vector definition fsyncs the metadata directory so a crash cannot resurrect it. `registry.msgpack` loads reject
malformed label or relationship-type slices; catalog loads reject duplicate shard names, several hot event shards,
invalid kind/tier combinations, unsafe relative paths, negative stats and zero or inverted event windows before
reopening shard handles.

## Format versions and markers

Purpose: an older binary must refuse data it cannot read, instead of misdecoding it or writing around a feature it does not know. Two levels carry the version:

- **Per row**: every `NodeWire` / `RelWire` has `FormatVersion` (`fv`; absent on pre-versioning rows = 1). A row is written at the highest `fv` its features need (`emittedRowFormatVersion`); a decoder that reads less fails closed on that row (`ErrWireFormatVersionUnsupported`) in a checked decode, an import, a replica apply or `loadIndexes`. It never skips the key.
- **Per store**: the badger meta key `wire_format_version` (big-endian uint16, also per shard of a tiered or sharded store) is verified at `New` before any row is decoded and before any write. A marker above `MaxReadableStoreFormatVersion` fails the open; an absent one is stamped by a writable open; a lower one is raised; an unparsable one is corruption.

| Marker | Written by | Meaning | Rows at `fv` |
|---|---|---|---|
| absent / 2 | v4.6.0 to v4.51.1 | row format v2 (fixed-width `tf`/`tt` tail) | 2; 3 for a retraction tombstone (v4.49.2) |
| 3 | v4.51.2 on | ex-adjacency index kept with every relationship delete (backlog 50) | as 2 |
| 4 | v4.54.0 on | integrity hashes stored as 32 raw bytes (backlog 61) | 4 |
| 5 | v4.52.2 on, only with an Elements definition | an Elements property index is persisted (backlog 63) | as 3 |
| 6 | v4.54.0 on, only after a first excise | the store holds a void row (backlog 64) | 5 for the void row |
| 7 | v4.56.0 on, only for an `UntimedUnbounded` or `UntimedWholeAxis` store | the store's untimed valid-start mode is `UntimedUnbounded` or (round 11, key byte 3) `UntimedWholeAxis`, or a conversion to `UntimedUnbounded` started (backlog 73; an earlier v4.56.0 or later reader refuses byte 3 at open, `ErrCorruptWire`); raised on the shard holding the MetaKV key (badger; tiered reference shard; sharded anchor) in one transaction with the key `untimed_valid_start` (`MetaSetDurableRaising`) | unchanged (an untimed start is an ordinary `ValidFrom`; an `UntimedWholeAxis` row stores literal instants, the open end `math.MaxInt64`) |
| 8 | read since v4.62.0; written by no release yet | the store holds a delete tombstone that kept its believed `ValidTo` (`DeleteKeptValidTo`, key `dk`; backlogs 4, 7, 25): a close at or after the delete, or the open end of a row starting at or after it | 6 for that tombstone |
| 9 | v4.75.0 on, only after a first staged flush | the store has (or had) a flush larger than one badger transaction, staged under `0x0E` with the marker `flush_stage` (FLUSHPART); raised in the marker's transaction | unchanged |
| 10 | v4.76.0 on, only after a first open-ended excise | the store holds an open-ended void row (`ExciseNodeValidity` / `ExciseRelValidity` with `b = 0`, OPENEXCISE) | 7 for that void row |
| 11 | v4.77.0 on, only with `ValueDictionaries` configured and a first value coded | the store holds value-dictionary entries and rows coded with them (DICT, "Value dictionaries" below); raised with the first entry | 8 for a coded row |
| 12 | v4.91.0 on, only with `PropertyIndexOnDiskLabels` and a first listed definition built | the store holds a per-definition on-disk property index marker (PROPDISKLABEL, "Persistent property index" below); raised with the first marker, by its create or the open that rebuilds it | unchanged |
| 13 | v4.92.0 on, only with an on-disk property index (`PropertyIndexOnDisk`, `PropertyIndexOnDiskLabels`, `RelPropertyIndexOnDiskTypes`) and a first definition built | the store holds `0x0C` entries and their built markers (RELDISK, "Persistent relationship property index" below), which an older release would neither maintain nor clear; raised with the first marker, by its create or the open that builds it | unchanged |
| 14 | v4.94.0 on, only with `PropertyIndexValidEnd` listing an on-disk definition | the store's `0x0A` entries of a listed definition carry valid-end bounds that every write must reset (VALIDENDPRUNE, "Valid-end bounds" below), which an older release would not do; raised with the definition's valid-end marker, by its create or the writable open that lists it | unchanged |
| 15 | v4.96.0 on, only with a composite node index of a label kept on disk (`PropertyIndexOnDisk`, `PropertyIndexOnDiskLabels`) built | the store holds composite entries in `0x0C` (kind 4) and their built markers (COMPDISK, "Composite node indexes on disk" below), which an older release would neither maintain nor clear; raised with the first marker, by its create or the writable open that builds it | unchanged |

One tracker serves all markers: `bs.formatMarker`, raised by `raiseFormatMarkerPendingLocked` under `wbMu` in the same section that appends the row or definition needing it (`appendOpsRaising`, `appendOpsLoggedRoutedRaising`) or writes the meta entry needing it (`MetaSetDurableRaising`, marker 7), or in the batch of the open that writes the entry needing it (marker 12 and 14, `planPropertyIndexDisk`; markers 13 and 15, `commitEntityIndexOpen`). The marker never lags its cause, a later raise for a lower feature never lowers a higher pending marker, and it is never lowered (dropping an Elements index or never again excising keeps the store closed to older binaries; so does an emptied unbounded store later opened under `UntimedFromTxFrom`). A store with none of the features keeps its marker (3 or 4), a `UntimedFromTxFrom` store included; `Clear` resets the tracker and re-stamps the writer's base marker. Bump protocol: encoders (lesson 39), decode path and marker logic move together. Row layout: "Row wire format" below; the retraction key `rx`, the void key `vd` and the kept-valid-to key `dk` are keys an older reader must not skip, which is why they raise `fv`.

## Value dictionaries

Purpose: a string value repeated across many rows of one property (an action word, an ip, a sid, a folder) is
stored once per store (DICT, v4.77.0). `Config.ValueDictionaries` (badger and tiered; `store.ValueDictionary`) names
each dictionary, its cap and its members, a (label or relationship type, property) pair each; the graph installs its
label and type registries to resolve the names. Only member string values of 4 bytes or more are coded; a value past
the cap stays inline and is counted (`ValueDictionaryStats`).

Storage, per badger store (per tiered shard): meta `vdict_names` (msgpack `[]string`, a name per id) and
`vdict/` + the token (4 bytes big-endian) holding uvarint(name id) + the value. A row stores the property as
`dt` + the token in place of `v` (`fv` 8). A new entry, a grown names list and the marker raise to 11 are appended
to the pending batch under the dictionary's lock before the encode returns the token, so they precede every row
using it: no flush, single or staged, commits the row without its entry. Decode resolves tokens without a lock
from an append-only table loaded on the first coded row; a token that does not resolve is `ErrCorruptWire`. Coded
rows decode without the configuration. `Clear` stores the in-memory entries again (a token handed out before the
wipe may reach a row after it). The change log, exports and replica rows carry the plain value; reads, filters and
property indexes see it.

## Integrity hashes

Purpose: a row's hashes (`h`, `ph`, and on a relationship `fnh`, `tnh`) chain its versions; they cost bytes, and a development store may not want them.

Storage. A hash is stored as its 32 raw bytes (msgpack bin 8, 34 bytes) instead of 64 hex characters (66 bytes); both forms decode to the same lowercase 64-hex string at the boundary, so `types.NodeIntegrity` / `RelIntegrity` are unchanged. One conversion (`pkg/internal/hash32`) serves the rows, the compact frozen rows in RAM (`types/compact.go`) and the column segments. A string that is not 64 lowercase hex characters stays a string. Binary rows are `fv` 4 and raise the marker to 4 (a higher marker stays). Exports and the change feed carry `fv` 4, so an importing or replicating release needs v4.52.2 or later.

Switch. `Config.DisableIntegrityHashes` (default false = on) computes and stores no hash, resident or on disk; author, signature and authorization fields are unaffected. With it set, `Hash().VerifyNodeChain` / `VerifyRelChain` and `Tier().VerifyShard` return `ErrIntegrityDisabled` (never "valid"), `Config.RelSegments` is refused, and an import or replica apply verifies and then drops the hashes it carries. An on-graph refuses an off-graph's export (`ErrCorruptExport`).

Persisted setting. MetaKV key `integrity_hashes`, written durably (`MetaSetDurable` on badger, the tiered reference shard and the sharded anchor) as the last step of a successful `New`, kept across `Admin().Reset()`. A store with rows keeps its setting by refusal: `New` under the other setting fails with `ErrIntegrityModeMismatch` and writes nothing. Without the key the rows decide (a hashed row makes an on-store, an unhashed row an off-store); a store without rows takes the configured setting. To change it, export and import into a new store opened with the wanted setting. A custom backend without MetaKV follows the configuration.

Cost, a relationship row of four properties (measured 2026-10 with 20,000 relationships, badger): 366.9 B raw / 251.2 B Snappy as hex, 270.9 / 151.8 B binary, 158.9 / 43.5 B with hashes off. Errors: `docs/errors.md`.

## Upgrading a store

A store is opened by a release that reads its marker (the table above). Opening writable under a newer release may raise the marker; there is no way back except a copy made before the upgrade.

| Marker | Read by | Refused by (`ErrWireFormatVersionUnsupported` at `New`, nothing written) |
|---|---|---|
| absent / 2 | every release | none |
| 3 | v4.51.2 and later | v4.51.1 and earlier |
| 4 | v4.52.2 and later | v4.52.1 and earlier |
| 5 | v4.52.2 and later | v4.52.1 and earlier |
| 6 | v4.54.0 and later | v4.53.0 and earlier |
| 7 | v4.56.0 and later (mode byte 3, UntimedWholeAxis: v4.65.0 and later) | v4.55.3 and earlier (a release that ignores `untimed_valid_start` would write `TxFrom`-start creates into an `UntimedUnbounded` store) |
| 8 | v4.62.0 and later | v4.61.x and earlier (a release that skips `dk` would read a kept close as the delete's end) |
| 9 | v4.75.0 and later | v4.74.x and earlier (a release that ignores `flush_stage` would lose a pending staged flush and keep writing; a newer release would then re-apply the stale staged values) |
| 10 | v4.76.0 and later | v4.75.x and earlier (an open void row reads as malformed) |
| 11 | v4.77.0 and later | v4.76.x and earlier (a release that skips `dt` would read a coded property as nil) |

Rows and feeds behave alike: an `fv` above what a release reads fails that row closed in an export, a change feed or a replica (fv 3 retraction: v4.49.2 and later; fv 4 binary hashes: v4.52.2 and later; fv 5 void row: v4.54.0 and later; fv 6 kept-valid-to tombstone: read since v4.62.0; written by no release yet). Upgrade readers, replicas and importers before the writer.

First writable open on v4.54.0 (one-time work, no action needed):

- the open image is format 2, so a store closed by an older release is rebuilt from its rows once, and a clean `Close` writes the new image (cost: the full open of the "Clean reopen" section);
- with `TemporalIndexOnDisk`, the 0x0B keyspace is rebuilt under the marker `temporal_index_on_disk_built_v2` (envelopes of untimed rows now start at their `TxFrom`, see api.md "Valid-time start (Config.UntimedValidStart)"); the old marker is deleted, so an older release rebuilds it again after a downgrade; a read-only open rebuilds in RAM and writes nothing;
- with `ColumnsOnDisk`, persisted column blobs of the old format are not used and the columns are rebuilt on first read;
- a writable open raises the marker to 4 (binary hashes); hex rows already on disk stay readable and are rewritten as they change;
- v4.52.1 or earlier cannot open the store afterwards. Roll back only to v4.52.2 or v4.53.0 (they read markers 4 and 5, not 6), and only before the first excise.

First writable open on v4.89.0: the open image is format 4 (`TKGOI\x04`, slot, kind and since in the header); a format-3 image is read as a `Close` image, so the upgrade rebuilds nothing. A downgrade to v4.88.x rebuilds once (it compares the full magic); slot-1 chunks (`tkg_open_image_d<i>`) stay on disk until v4.89.0 writes that slot again — no marker change, no refusal.

First writable open on v4.58.0: the open image is format 3 (`TKGOI\x03`), so every badger directory (each sharded slot, each tiered shard as it opens) is rebuilt from its rows once and a clean `Close` writes the new image; a read-only open rebuilds in RAM on every open until a writable `Close`; downgrade to v4.57.1 rebuilds once more (it compares the full magic) — no marker change, no refusal.

Consumers: untimed rows start at their `TxFrom`, an update setting only `tkg_valid_to` closes the version, and the excise doors exist; see api.md and the 4.54.0 CHANGELOG entry.

Mode byte 3 (round 11, `UntimedWholeAxis`): the store's rows are on the literal valid axis (`types.LiteralValidAxis`: a stored 0 is the instant 0, an open end `math.MaxInt64`). An open end is stored as `math.MaxInt64` under the wire key `vt` (12 bytes), which a legacy open row omits, so each open row (current rows, open history rows) is 12 bytes larger than on a legacy store; an untimed start is `math.MinInt64`, as an `UntimedUnbounded` untimed first version already stores. No row carries a flag or a format version for it; the store reads the axis from the mode key when it opens (badger: before its open-time index rebuild; tiered and sharded: from the reference / anchor shard, handed to every shard), and a release that does not know byte 3 refuses the store at open (`ErrCorruptWire`). Its exports carry export format version 4, which a release before it refuses (`ErrIncompatibleExport`).

Marker 7 (v4.56.0, `Config.UntimedValidStart`): a store created under `UntimedUnbounded`, or converted with `Admin().SetUntimedValidStart(ctx, UntimedUnbounded)` (raised when the conversion starts), is refused by v4.55.3 and earlier. Since v4.57.0 a NEW store under the default (`UntimedDefault`) is an `UntimedUnbounded` store and carries marker 7 from its first open: set `UntimedFromTxFrom` explicitly for a new store that v4.55.3 or earlier must still open. A `UntimedFromTxFrom` store, every store with rows written before the option and every store created under v4.56.x defaults, keeps its marker and stays readable by v4.54.0 and later. Convert only once every reader, replica and importer runs v4.56.0 or later; there is no way back short of a copy made before the conversion.

## Row wire format

Source: `pkg/graph/internal/storeutil/wire.go`.

```go
type NodeWire struct {
    FormatVersion uint8          `msgpack:"fv,omitempty"` // per-row format version; absent = 1
    ID            int64          `msgpack:"id"`
    PrimaryLabel  int            `msgpack:"pl"`
    ExtraLabels   []int          `msgpack:"el,omitempty"`
    Properties    []PropertyWire `msgpack:"p,omitempty"`
    Version       int            `msgpack:"v"`
    HasTemporal   bool           `msgpack:"ht,omitempty"`
    ValidFrom     int64          `msgpack:"vf,omitempty"`
    ValidTo       int64          `msgpack:"vt,omitempty"`
    TxFrom        int64          `msgpack:"tf,omitempty"` // written as the fixed trailing slot (fv 2)
    TxTo          int64          `msgpack:"tt,omitempty"` // written as the fixed trailing slot
    CreatedAt     int64          `msgpack:"ca,omitempty"`
    UpdatedAt     int64          `msgpack:"ua,omitempty"`
    DeletedAt     int64          `msgpack:"da,omitempty"`
    Retracted          bool   `msgpack:"rx,omitempty"` // retraction tombstone; only with da != 0
    Void               bool   `msgpack:"vd,omitempty"` // excise void piece
    DeleteKeptValidTo  bool   `msgpack:"dk,omitempty"` // delete tombstone that kept its ValidTo; only with da != 0
    CreatedBy          string `msgpack:"cb,omitempty"`
    UpdatedBy          string `msgpack:"ub,omitempty"`
    BaseEntityID       int64  `msgpack:"be,omitempty"`
    Hash               string `msgpack:"h,omitempty"`  // 32 raw bytes on the wire at fv >= 4
    PrevHash           string `msgpack:"ph,omitempty"`
    AuthorID           string `msgpack:"aid,omitempty"`
    Signature          []byte `msgpack:"sig,omitempty"`
    AuthorizedBy       string `msgpack:"aby,omitempty"`
    AuthorizationLevel uint8  `msgpack:"al,omitempty"`
}

type RelWire struct {
    FormatVersion uint8          `msgpack:"fv,omitempty"`
    ID            int64          `msgpack:"id"`
    RelType       int            `msgpack:"rt"`
    StartID       int64          `msgpack:"s"`
    EndID         int64          `msgpack:"e"`
    Properties    []PropertyWire `msgpack:"p,omitempty"`
    Version       int            `msgpack:"v"`
    // temporal, rx/vd/dk, provenance and integrity fields as NodeWire, plus:
    FromNodeHash  string         `msgpack:"fnh,omitempty"` // start-node hash at write time
    ToNodeHash    string         `msgpack:"tnh,omitempty"` // end-node hash at write time
}

type PropertyWire struct {
    Key           string `msgpack:"k,omitempty"`  // empty when KeyToken carries the key
    KeyToken      uint16 `msgpack:"kt,omitempty"` // property-key token; 0 = raw Key on the wire
    Value         any    `msgpack:"v"`
    Type          byte   `msgpack:"t"`            // property type tag (exact Go type on decode)
    Nil           bool   `msgpack:"n,omitempty"`  // typed nil slice/map marker
    CustomType    string `msgpack:"ct,omitempty"` // registered custom struct type
    CustomPointer bool   `msgpack:"cp,omitempty"` // original form was *T
}
```

All three types have hand-written `EncodeMsgpack` implementations (`wire_encode.go`, no reflective omitempty
checks): a new field goes into the custom encoder as well as the struct tag (lesson 39). Nested property values
whose Go kind msgpack would lose (inside `[]any` / `map[string]any`) are marker-led arrays:
`["\x00tkg.tv", kind, iso]` for a `TemporalValue`, `["\x00tkg.k", tag, payload]` / `["\x00tkg.k", tag]` for a typed
scalar/container or typed nil, `["\x00tkg.c", type, pointer, msgpack]` for a registered struct
(`wire_nested_temporal.go`); they are optional keys, so no `fv` bump.

Row format versions (`fv`; the levels and the store marker: "Format versions and markers" above):

| `fv` | Constant | Meaning |
|------|----------|---------|
| 2 | `CurrentWireFormatVersion` | `tf`/`tt` is a fixed-width, always-present trailing slot: the last two map entries, each a full int64 (`0xd3` + 8B), written even without temporal metadata (`wire_temporal_tail.go`). v1 (absent `fv`, omitempty mid-map tail) and v2 decode alike. |
| 3 | `RetractedWireFormatVersion` | the row carries `rx` (retraction tombstone) |
| 4 | `BinaryHashWireFormatVersion` | `h`, `ph`, `fnh`, `tnh` as 32 raw bytes (msgpack bin, 34B) instead of 64 hex characters (66B); decoders read both |
| 5 | `VoidWireFormatVersion` | the row carries `vd` (an excise void piece) |
| 6 | `KeptValidToWireFormatVersion` | the row carries `dk`; read since v4.62.0, written by no release yet |
| 7 | `OpenVoidWireFormatVersion` | the row carries `vd` with an open end (an excise of `[a, open)`, v4.76.0); 5 and below refuse an open void row |
| 8 | `ValueDictWireFormatVersion` | a property carries `dt` (a value-dictionary token, v4.77.0) in place of `v`; a delta-history `Meta` carries 8 when its row does |

A row's `fv` is the max over its features; every decoder (checked, selection-scope partial and its scanner,
delta-history `Meta`, import, change feed, replica apply) fails closed above 8. Checked validation
(`ValidateNodeWire` / `ValidateRelWire`) refuses `rx` or `dk` without `da`, `dk` on a close before `da`, and `vd`
on a row that cannot be a void piece (`checkVoidWire`). Retraction tombstones written by v4.49.1 (`fv=2` plus `rx`)
still decode as retractions, but a release reading `fv <= 2` skips the key and answers a plain delete; upgrade every
reader of such data (`tasks/evidence/retraction-fv3/05-old-binary-fails-closed.txt`).

Integer widths: `pl`, `el` elements, `rt` and `v` use `EncodeInt` (smallest form: 0–127 1B, 128–255 2B, 256–65535
3B, 65536+ 5B, so tokens have uint32 room without migration); `kt` uses `EncodeUint16` (always 3B); IDs (`id`, `s`,
`e`, `be`) and every instant (`vf`, `vt`, `tf`, `tt`, `ca`, `ua`, `da`) use `EncodeInt64`, always 9B.

## Key layout

Every key starts with a 1-byte prefix (`storeutil/keys.go`, `property_index_key.go`, `temporal_index_key.go`). IDs
are big-endian int64 (8B) and tokens big-endian uint16 (2B), so byte order is sort order and recent IDs cluster in
the LSM. Every prefix from `0x01` to `0x0F` is in use.

| Prefix | Key | Value | Size | Purpose |
|--------|-----|-------|------|---------|
| `0x01` `KeyNode` | `<8B nodeID>` | `msgpack(NodeWire)` | 9B | current node row |
| `0x02` `KeyRel` | `<8B relID>` | `msgpack(RelWire)` | 9B | current relationship row |
| `0x03` `KeyLabel` | `<2B labelToken>/<8B nodeID>` | empty | 11B | label index |
| `0x04` `KeyRelType` | `<2B relTypeToken>/<8B relID>` | empty | 11B | type index |
| `0x05` `KeyOut` | `<8B start>/<2B type>/<8B end>/<8B relID>` | empty | 27B | outgoing adjacency |
| `0x06` `KeyIn` | `<8B end>/<2B type>/<8B start>/<8B relID>` | empty | 27B | incoming adjacency |
| `0x07` `KeyHistNode` | `<8B nodeID>/<8B version>` | history row | 17B | node version history |
| `0x08` `KeyHistRel` | `<8B relID>/<8B version>` | history row | 17B | relationship version history |
| `0x09` `KeyChangeLog` | `<8B LSN>` | `tag(1B) ‖ msgpack(body)` | 9B | change-log record (`ChangeLog`) |
| `0x0A` `KeyPropertyIndex` | `<2B propKeyToken>/<payload>/<8B nodeID>` | empty, or an 8B big-endian valid-end bound (`PropertyIndexValidEnd`) | var | property index (`PropertyIndexOnDisk`) |
| `0x0B` `KeyTemporalIndex` | `<2B labelToken>/<8B from>/<8B nodeID>` | 8B big-endian TO | 19B | temporal-index rebuild rows (`TemporalIndexOnDisk`) |
| `0x0C` `KeyEntityIndex` | `<1B kind>/<2B scope token>/<2B propKeyToken>/<payload>/<8B ID>[/<8B txFrom>]` | empty | var | relationship property index and node membership entries (`RelPropertyIndexOnDiskTypes`, on-disk node indexes; RELDISK), composite node index entries (kind 4, COMPDISK) |
| `0x0D` `KeyExAdj` | `<8B nodeID>/<1B dir>/<8B relID>` | empty | 18B | ex-adjacency (`ExAdjacencyCapability`, backlog 50) |
| `0x0E` `KeyFlushStage` | `<8B staging sequence>/<the entry's own key>` | `op(1B) ‖ value` | var | a staged flush, between its start and its clean-up (FLUSHPART); no reader iterates it |
| `0x0F` `KeyMeta` | `<name>` | per key (below) | var | metadata |

Prefix scans: all outgoing from X `0x05/<X>/`, of type T `0x05/<X>/<T>/`, all incoming to Y `0x06/<Y>/`; an
entity's history in version order `0x07/<id>/` (`HistNodePrefix`); the change-log in commit order `0x09`; a node's
ex-adjacency per direction `0x0D/<node>/<dir>/` (the relationships that named the node in a row its live adjacency
no longer holds). Fixed-width scans check the exact key length.

Value encodings:

- History (`0x07`/`0x08`): a full row, or with `HistoryDeltaEncoding` a delta led by the 1-byte `'D'` (`0x44`) tag
  (`wire_history_delta.go`); reads accept both ("History delta encoding").
- Property index (`0x0A`): `payload` is domain-tagged. Numeric `0x01 | sortKey(8B) | subtypeTag(1B) | rawBits(8B)`
  (`sortKey` the order-preserving sign-flip of the float64 magnitude, one ordered domain for every numeric width);
  raw (string / bool / `TemporalValue`) `0x02 | canonical value-key bytes`. The key omits the label, so definitions
  on two labels over one property key share rows ("Persistent property index").
- Temporal index (`0x0B`): `from` is sign-bit flipped, so a prefix scan yields (From ASC, ID ASC).
- Entity index (`0x0C`, `entity_index_key.go`): kind 1 a relationship's current entry (no `txFrom`), kind 2 a
  relationship's member entry, kind 3 a node's member entry, kind 4 a composite node index's current entry (no
  `txFrom`); the scope token is the type or label, so a definition owns its prefix. Numeric payload as `0x0A`; raw
  `0x02 | value-key bytes with 0x00 written 0x00 0xFF | 0x00`, so an equality walk stops at the first key longer
  than its value's prefix ID and tail ("Persistent relationship property index"). A kind-4 head names every key:
  `<2B label>/<2B first key token>/<1B key count>/<2B per other key>`, so two composites of a label never share a
  prefix; its payload is the tuple's value key as is, whose parts are length-prefixed, so no tuple's key begins
  another's and the walk needs no terminator ("Composite node indexes on disk").

On disk vs RAM: label and adjacency keys (`0x03`–`0x06`) are always written; `LabelIndexOnDisk` /
`AdjacencyIndexOnDisk` answer reads from them instead of RAM maps (both also on `tiered.Config`, passed to every shard;
label since v4.79.0). With the label index on disk, doc-values columns decline and their callers read per node. `PropertyIndexOnDisk` moves property-index
entries into `0x0A`; `TemporalIndexOnDisk` keeps `0x0B` only as a rebuild accelerator (the index stays resident);
each is backfilled from current rows once, under its build marker. `RelPropertyIndexOnDiskTypes` moves the listed types' relationship property indexes into `0x0C`.
Composite node indexes of a label kept on disk move their entries into `0x0C` (COMPDISK). Every other index (vector,
relationship property of an unlisted type, relationship-type temporal, high-frequency, composite of a label in RAM)
persists only its definition; its contents come
from the open image after a clean `Close`, else from the rows ("Clean reopen"). RAM sidecars (history presence,
history stamps, label / rel-type / property tx-membership) have no keys; the property tx-membership sidecars travel
in the open image ("History rows: versions, lives and RAM sidecars").

Metadata `0x0F/<name>` (`MetaKey(name)`):

| Name | Value |
|------|-------|
| `label_tokens`, `reltype_tokens`, `property_keys` | `msgpack([]string)`, index = token value |
| `wire_format_version` | 2B big-endian store format marker |
| `history_anchor_interval` | pinned delta anchor interval |
| `last_lsn` | 8B big-endian change-log watermark (with `ChangeLog`) |
| `flush_stage` | 8B staging sequence + 8B entry count (distinct keys) + 1B flags (1 = written under `SyncWrites`) of a committed staged flush; present only until it is applied |
| `node_count`, `rel_count` | counters, in the same WriteBatch as the rows |
| `tx_commit_floor` | 8B big-endian newest `TxFrom`/`TxTo`/`DeletedAt` of every row written, first entry of each flush that carries rows; tiered keeps its other shards' floor in the reference shard's `tx_commit_floor_shards` (backlog 44) |
| `prop_indexes`, `rel_prop_indexes`, `temporal_index_defs`, `reltype_temporal_index_defs`, `high_frequency_index_defs`, `vector_index_defs`, `composite_index_defs` | index definitions |
| `property_index_on_disk_built`, `temporal_index_on_disk_built_v2` | one-shot build markers (`temporal_index_on_disk_built` is the pre-v4.54.0 marker, deleted by the rebuild) |
| `tkg_open_image`, `tkg_open_image_c<i>` | open image header and 512 KiB chunks |

Plus the graph layer's `MetaKVCapability` entries under the same encoding (e.g. `schema_version`, `graph_epoch`,
`tx_instant_floor`, `unique_constraints`, `compacted_through_tx`, `id_slot_lease`, `rel_exadj_built`,
`untimed_valid_start`, `integrity_hashes`) and persisted column chunks (`ColumnsOnDisk`). There is no
`next_node_id` / `next_rel_id` key: IDs are stateless snowflakes. The tiered store keeps label and type names in
`data/meta/registry.msgpack` instead.

## Open sequence

1. Badger `New`: verify or stamp `wire_format_version` and `history_anchor_interval` before any row is decoded;
   load `property_keys` before the index rebuild (tokenized rows need it); learn the valid axis from the persisted
   untimed mode; install indexes from a trusted open image or rebuild them from the rows; with `ChangeLog`, reseed
   the LSN allocator from `last_lsn`.
2. `graph.New`: validate the config, build the snowflake generators (no counter load, no max-key scan), load the
   label and rel-type registries from the store, raise the commit clock to `tx_commit_floor`.
3. Decoded `NodeWire` / `RelWire` rows are complete entities; nothing is injected.

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

Directory layout: `data/meta/` (catalog + registry + store-level index definitions), `data/reference/` (ref shard), `data/events/<window>/` (event shards), `data/archive/` (archived reference entities), `data/index/` (the owner index of event-label unique keys, made at the first such `CreateUnique`; v4.81.0). The owner index's claims are not synced one by one: each flush that makes event-shard rows durable (`SyncWrites`, `DurableFlush`) syncs the claims written so far first, one more fsync per such flush; a claim whose row a crash lost is ignored by readers, who check the claimant's row. Hot shard receives graph-generated current event creates; caller-supplied/backfilled event node IDs are created on the shard their snowflake timestamp resolves to. Public Store create-time duplicate checks use cold-shard-aware ID routing, so a node or relationship ID already persisted in an idle-closed cold event shard cannot be reused in another shard; relationship duplicate checks resolve actual ownership, not only the relationship ID timestamp, because a current-ID relationship can still live on an old start-node shard. Graph-generated single and batch creates use an internal generated-ID fast path and do not open unrelated cold shards. On window expiry, `RotateHotShard()` demotes hot→warm and creates a new hot shard. A 1-week `ShardWindow` uses ISO week starts: Monday for the timestamp's ISO week-year, with ISO week 1 anchored by January 4; daily and weekly windows start at local midnight in `Config.ShardWindowLocation`'s zone, or the process's zone (`time.Local`, so `TZ` decides) when it is nil (v4.82.0; a reopen under another zone keeps the stored windows and only later days follow it) and end at the next window's local midnight (calendar days, so a window holding a daylight-saving change is 23 or 25 hours long per day it spans; DSTWIN, v4.78.5); where the clocks jump forward at midnight (America/Santiago, Havana, Asuncion) a day starts at the jump (v4.81.1); accepted sub-day windows use fixed-duration starts that contain the timestamp. `ShardWindow` must be at least 1 minute and a whole millisecond. Warm/cold tiers are not new-write targets, but open Badger handles stay writable because existing event entities continue to update/delete on their owner shard after rotation and restart. Warm shards are recovered from catalog on restart. Cold shards are lazy-opened on first access (an open of the store, of a graph on it, and a `Flush` open none: OPENCOLD, v4.82.0) and auto-closed after idle timeout; `ColdAfter`/`IdleTimeout` reject negative durations, and positive `IdleTimeout` must be a whole millisecond before the idle-close goroutine starts. Idle-close failures are recorded, logged, block later lazy checkout, and are joined into `Store.Close`. Explicit `Store.Close` and idle-close serialize on the event shard mutex before closing or clearing a shard handle. Archive/restore moves live relationship placement, purges orphan destination adjacency during preflight, and purges stale source-shard adjacency-only entries during final source-node removal. Badger restart rebuilds live node and relationship membership from entity rows instead of stale label/type/outgoing keys; incoming-only relationship keys remain visible for cross-shard repair. Badger orphan cleanup, tiered relationship routing, and repair shard probes verify the relationship entity row rather than trusting index-derived membership; repair purges local stale type/out/in keys and counters together for orphaned relationship IDs. Badger persists high-frequency index definitions and rebuilds their buckets on open. Tiered temporal and high-frequency index create/drop serialize against rotation, persist store-level tracking metadata, and keep one temporal index kind per label across all shards; HFI bucket sizes are positive whole milliseconds to match persisted `Instant` precision and same-kind HFI retries must use the same bucket; cross-kind or different-bucket leftovers return `ErrTemporalIndexExists` before other shards are mutated, later-shard failures roll back earlier shard-local changes, tracked-index application to a single newly opened shard removes earlier same-call creations if a later tracked definition fails, and persisted tracking replay rolls back earlier shard creations if a later shard rejects the metadata. Once close starts, checked routing, create-time rotation, valid TieredStore index create/drop/search calls, direct read/count/bulk/history APIs, empty batch/history write edges, and public admin/metadata operations return `ErrStoreClosed`. `SetLabelRegistry(nil)` is ignored so a direct caller cannot erase reference-label routing.


Where a new version of a long-lived node lands on tiered (backlog 21): on its OWNER shard, never the hot one — the reference shard if it holds the ID, else the archive, else the event shard whose window contains the ID's snowflake MINT instant (`tieredstore_routing.go`). A cold owner is lazy-opened for the write and closed on checkin unless `MaxOpenColdShards` retains it; a `RefLabels` label lives on the always-open reference shard and cannot change class on update. Nodes rewritten at every cut (an overview) should therefore be reference-class. A relationship retention purge visits every shard (cold ones opened for the call and closed again) and then drops the legs a cross-shard relationship left on its endpoints' shards.

## Tiered background-error recovery

Idle/transient cold-shard close failures set a sticky background error and the tiered store fails closed on every subsequent path. `tiered.Store.RecoverBackgroundError()` re-probes the persistence path with an atomic catalog save once the operator has fixed the underlying condition; on success the gate clears in place (no close/re-open). It clears the lifecycle gate only — run `VerifyShard`/`RunRepair` before trusting affected shards for critical reads.

## Change-log / op-log (opt-in, `ChangeLog`)

With `graph.Config.ChangeLog` (or `badger.Config.ChangeLog` / `memory.WithChangeLog()`) enabled, every committed mutation appends a durable, ordered record to the change-log: the topology-agnostic foundation for horizontal scaling (change-data-capture, audit trail, point-in-time recovery today; read-replica streaming next). Off by default with zero write-path overhead.

**On-disk layout.** Records live under the `0x09/<8B big-endian LSN>` keyspace, value = `tag(1 byte) ‖ msgpack(body)`. A big-endian LSN makes a prefix scan yield ascending commit order. A `meta/last_lsn` watermark records the highest committed LSN.

**Atomicity.** The badger backend commits each record (and the `last_lsn` watermark) in the same flush as the entity rows and counters, one badger transaction or one staged flush (see Durability), so a record and the mutation it describes commit atomically — there is no committed-but-unlogged window, and on restart the LSN allocator reseeds from `last_lsn` (crash-consistent with the maximum `0x09` key), so LSNs stay strictly monotonic and are never reissued. With the async write buffer, only durably-flushed records are visible to the feed; backpressure counts buffered records so a hot-key workload cannot grow the (uncoalesced) log buffer without bound.

**Record tags** (`store.ChangeTag`, 14 in total — a consumer switching on the tag must cover all of them): `NodePut`/`RelPut` (1/2 — new current state), `NodeDelete`/`RelDelete` (3/4 — a `WithHistory` flag distinguishes a hard cascade from a tombstone-appending delete; node deletes carry the cascaded relationship IDs), `NodeHistoryVersion`/`RelHistoryVersion` (5/6 — explicit-version writes from import, bitemporal-cascade correction, migration, and transaction-rollback restore), `NodeHistoryTruncate`/`RelHistoryTruncate` (7/8), `Meta` (9 — body `MetaBody{Key, Value}`; emitted only for the untimed valid-start mode key `untimed_valid_start` when a store is set to or converted to `UntimedUnbounded`, backlog 73; a replica applies and persists it and fails closed on any other key; a replica before v4.56.0 stops at it with `unknown change tag 9` — upgrade replicas first), `Clear` (10), `ForeignIncoming`/`ForeignIncomingDelete` (11/12 — the cross-machine incoming half-edge stub (since 4.18.0) and its cascade removal; a replica MUST route both by the END node's slot, since the rel's own slot is foreign, and both are idempotent), and `RangePurge` (13 — the retention range purge (since 4.18.0): ONE logical record naming a PREDICATE, body `RangePurgeBody{label token, boundary, mode}`, which the replica RE-EXECUTES against its own state rather than applying N per-entity deletes), and `RelRangePurge` (14 — the relationship retention purge, backlog 21: body `RelRangePurgeBody{type token, boundary, mode}`, re-executed the same way; a separate tag so a replica that predates it stops at the unknown tag instead of purging nodes of the label sharing the token — upgrade replicas first). Bodies reuse the `NodeWire`/`RelWire` format (so no on-disk wire-format bump) and are read back through `SafeUnmarshal` — a corrupt `0x09` value fails closed with `ErrCorruptWire`, never a process crash.

**Placement and scope.** The record is emitted in-backend (badger, memory, tiered, sharded), never by a `Store` decorator: crash safety needs the record in the data batch, and a decorator would lose native-store trust (architecture.md "Change-log (op-log) is IN-BACKEND and co-committed, never a Store decorator"). Inside a `GraphTx` the records are buffered (`store.TxChangeLogScope`) and get their LSNs at `Commit`, so a rolled-back or uncommitted transaction emits no record and burns no LSN; an unscoped door (standalone doors, the concurrent ingest session, a store without the scope) appends its record eagerly, one per store mutation.

**Rollback with the log enabled.** A rolled-back `GraphTx` emits nothing (its records are scoped and minted at commit). `Batch.Execute` and `IO().Import` emit eagerly, so with the log enabled their token registries are append-only across rollback: a durable record may already reference a new token, and de-allocating it would let the number be reused on a replica. Only allocation or persist failures, which fire before any record, still de-allocate.

**Reading.** `g.Replication().ChangeFeed(afterLSN, limit)` returns committed records in ascending LSN order; `ForEachChange(afterLSN, fn)` streams them OOM-safe with the callback invoked outside store locks; `LastCommittedLSN()` is the watermark a session reads after a write for read-your-writes routing. Only flushed records are in the feed (`store.ChangeFeedCapability`: "a record buffered but not yet flushed is not surfaced"): on badger with `SyncWrites=false` an acknowledged write reaches the feed and `LastCommittedLSN` at the next async flush (`FlushInterval`, default 100 ms), so a replica polling right after the write sees nothing yet and gets it on a later poll; `SyncWrites=true` makes each write visible on return, and tiered flushes before every read. A backend opened without a change-log returns an empty feed (`ChangeLogEnabled()` reports `false`). All four in-tree backends — memory, badger, (since 4.12.0) tiered, and sharded (one store-global LSN allocator over its slots) — implement the capability.

**Replication model.** The change-log ALONE does not converge a replica from empty: a replica bootstraps from a full export snapshot (which includes the token registry) and then tails the feed from the snapshot's LSN. The memory backend implements the same capability (in-RAM, non-durable) for testing; all backends share one set of record-body builders, so their feeds are byte-identical for property-free entities and decode-equivalent for property-bearing ones (badger tokenizes property keys on the wire while memory keeps key strings).

**Tiered change-log (since 4.12.0).** The tiered backend has no single `WriteBatch` — a mutation commits in exactly one shard. It still provides a coherent store-global feed: a store-owned `atomic.Uint64` LSN allocator is injected into every shard (`badger.Config.ChangeLogSeqSource`, nil elsewhere = self-owned counter, so standalone badger is byte-for-byte unchanged), so each shard co-commits its own records + `LastLSNKey` in its own batch while LSNs form ONE total order. Reseed at open reads a single monotonic `changelog_lsn_watermark` key on the always-present reference shard (written after every log-bearing flush via `badger.Config.OnChangeLogFlush`), never opening a cold shard; an unreadable watermark fences the change-log with a sticky error and refuses to hand out LSNs (`RecoverChangeLog` clears it in place). The feed runs a flush-before-read durability barrier (`tiered.Store.Flush()` over every open shard — also satisfying the export `storeFlusher`), captures `W = LastCommittedLSN` right after the barrier, and runs a W-bounded paged k-way merge over every catalog shard (one shard checked out at a time; cold-shard log segments paged read-only) emitting only `LSN <= W`; records allocated during the drain are deferred to the next poll. The barrier and W-bound together eliminate the cross-shard flush-reordering silent-loss (a lower LSN buffered on a slow shard is never skipped by a tail that already passed a higher durable LSN). This unlocks `ExportSince`/`Watermark`/`ImportMerge` and read-replica bootstrap+tail on tiered primaries.

## Backups (one-call ergonomics)

`g.IO().BackupTo` / `g.IO().BackupDeltaTo` / `graph.RestoreInto` wrap `Export` / `ExportSince` / `Import` / `ImportMerge` / `HeaderOf` with filesystem orchestration — deterministic filenames and chain validation — so a caller does not have to invent a naming scheme or a manifest. **No new wire format**: every backup file is byte-for-byte what `Export`/`ExportSince` already produce; a file written by `BackupTo` is a valid `Import` source on its own, with or without `RestoreInto`.

**Naming is derived from the stream's own header cursor, never wall time.** `(*io.API).BackupTo(dir)` writes a full export to `dir/backup-<LSN>-full.tkg`, where `<LSN>` is `DeltaHeader.To.LSN` zero-padded to 20 digits (the full width of a `uint64`, so a directory listing sorts in chain order) — the exact cursor `HeaderOf` would decode back out of the file, not a value computed separately and possibly stale by the time the file is written. `(*io.API).BackupDeltaTo(dir, since)` is the `ExportSince` counterpart: `dir/backup-<sinceLSN>-to-<toLSN>-delta.tkg`. Both fsync the file before returning.

**Refuses to overwrite.** If the deterministic target filename already exists in `dir`, both methods return `ErrBackupExists` and leave `dir` untouched — a backup is staged to a temp file in the same directory first and only renamed into place once no collision is found. On a change-log-less backend `BackupTo` still succeeds with the zero `Cursor` (there is no change-log point to name it by), so every such backup in a directory shares the name `backup-00000000000000000000-full.tkg` — only the first call to a given `dir` succeeds, matching the "no silent overwrite" contract exactly.

**Empty deltas write no file.** If there is nothing committed after `since` to reproduce (zero change records beyond the header/registry — e.g. calling `BackupDeltaTo` again right after a successful backup, before any further mutation), NO file is written and the call returns `since` UNCHANGED: a caller polling "is there anything new since my last backup" gets a side-effect-free no-op instead of an empty file to track and prune.

**Declines exactly like `ExportSince`.** `BackupDeltaTo` on a backend with no active change-log returns the same wrapped `store.ErrCapabilityNotSupported` `ExportSince` returns; the caller's fallback is the same — take a full backup with `BackupTo` instead.

**Restoring a chain.** `graph.RestoreInto(cfg, dir)` opens a graph from `cfg`, then discovers exactly one full backup (`backup-<lsn>-full.tkg`) and zero or more delta backups (`backup-<since>-to-<to>-delta.tkg`) in `dir`. Before replaying anything it reads every file's header (`HeaderOf` — cheap, does not consume entity/change records) and validates the WHOLE chain is gapless: the full backup's cursor must equal the first delta's `From`, and each subsequent delta's `From` must equal the previous file's `To` — the same pairwise invariant `ExportSince`/`ImportMerge` already enforce one delta at a time, checked here across the whole set so a missing or foreign file is reported by name up front instead of surfacing as an `ImportMerge` failure partway through an already-mutated graph. A cursor-epoch mismatch (a delta from an unrelated graph lineage) fails closed with a wrapped `ErrCursorUnknown`; an LSN gap (a missing delta, or deltas that don't chain) fails closed with a wrapped `ErrDeltaBaseMismatch` — both name the offending file. Zero or more than one full backup in `dir` is also an error (nothing to guess from). On success, `RestoreInto` has replayed `Import` (the full backup) then `ImportMerge` (each delta, oldest first) and returns the opened graph, ready for use; on any failure it closes what it opened before returning the error.

**Deferred.** A generic `MetaSet` emits no record (only the untimed valid-start mode key does, as `ChangeMeta`); and LSN-watermark-driven log GC (pruning records below the minimum replica-acked LSN) lands with read replicas — on tiered, a whole retention-dropped event shard drops its log segment with it, so consumers must checkpoint before a shard is retention-dropped (the same discipline any log-truncation policy requires).

## Clean reopen: keys plus the open image (backlog 54)

A badger open used to decode every current row to rebuild the RAM indexes (liveness sets, label / type /
adjacency maps, planner statistics, and the property, relationship-property, composite, temporal,
relationship-type temporal, high-frequency and vector index contents), so it grew with the store: 14.3 s for
519 K nodes / 436 K relationships in the ai-soc shape, 24.8 s on ai-soc's own store. A writable
`Close` that flushed cleanly ends with the **open image**: the index contents above and the planner statistics,
chunked (512 KiB each) under one of two key sets, `0x0F tkg_open_image_c<i>` (slot 0) or `tkg_open_image_d<i>`
(slot 1), and a header `0x0F tkg_open_image` written last in its own commit: magic `TKGOI\x04`, chunk count,
length, slot, kind (`Close` or periodic), the image's *since* version (the store's newest badger version when its
encode ran) and CRC-32C. An image goes to the slot the current header does not name, deleting that slot's chunks
past its own count, so a crash while it is written leaves the previous header naming the previous chunks.
`Close` holds the flush lock from the image's drained check until the store is marked closed, so a write racing
`Close` cannot commit between the encode and the header. The next open trusts the image whole only if `Close`
wrote it and its header is still the store's newest badger version (`db.MaxVersion()`); a missing chunk, a length
or checksum mismatch or a decode error sends the open to the full rebuild. Otherwise (writes of a session that
crashed, a write by an older release that knows nothing of the image, a raw badger write, a periodic image) the
open is a **delta open** from the image's since version (OPENIMAGE, v4.88.0). Every commit after it carries a higher badger
version, so one iterator over the versions above it (`IteratorOptions.SinceTs`, all versions, deletes included)
names every entity whose row, history, label, type or adjacency keys changed since: the touched entities. The delta open walks the
entity, label, type and adjacency keys as an open from an image without ID sets does, decodes only the touched
entities' rows, keeps the image's index entries of live untouched entities and adds the touched live ones from
their rows. Planner statistics are deferred to their first read and the property membership sidecars build at
their first lookup. An index defined or redefined after the image installs under its current definition: the
image's entries are the rows' values (a vector, a time), and an index the image lacks builds from its rows. A
purged entity whose tombstones a compaction dropped is named by no newer key, but it is not live, so the
liveness filter drops its entries. A crash reopen after one record decodes 1 row at 1 M entities, where the
full rebuild decoded 1.54 M (`TestCrashReopenCostNotLinear`). The full rebuild, and a delta open for its touched
entities, also repair the keys: a crash inside a flush badger split into several transactions can leave label,
type or adjacency keys that disagree with the rows, which a rebuild skips but a trusted image would believe, so a
writable open deletes the keys it skips and writes the ones a row needs and the keyspace lacks (a failed repair
suppresses the session's image; a read-only open repairs nothing and writes none). A trusted open then:

- takes the live node and relationship ID sets from the image (EXISTMAP, v4.84.0: trusted only when their sizes
  equal the persisted `node_count` / `rel_count`, else it walks the entity keys) and, with them, the rel-type
  index (an ID set per type) and the label counts (OPENWALK, v4.87.0); it builds ordinals and the label and
  adjacency maps from the index **keys** alone (no value read). With `LabelIndexOnDisk` it walks no label key (an
  index that needs a label's members reads that label's keys), with `AdjacencyIndexOnDisk` no adjacency key, so
  with both a clean open walks no key per entity. An image without these sections walks their keys; an older
  release fails on the image's sections and rebuilds once from the rows;
- installs each defined index from the image (temporal envelopes restricted to the label's or type's current
  members, as a rebuild would keep them); an index the image lacks (still being created at `Close`, a
  pending `TemporalIndexOnDisk` backfill) is rebuilt from its rows as before;
- restores the planner statistics the closing process held: presence and type-class counters, and per
  accumulator the HyperLogLog registers, the ordered family, the typed min/max and the dirty flag. They are
  therefore not a full rebuild's: the NDV sketch keeps values deleted in earlier sessions (NDV never decreases,
  `docs/query-planners.md`), and a dirty min/max is rescanned by the next `PropertyStats` read, which then
  returns the exact values a rebuild finds. An image without statistics (closed with `DisablePlannerStats`, or
  while they were still deferred) defers them: the first statistics read builds them from the current rows, as a
  full open would, holding the flush and index write locks — readers and writers wait for that one scan — and a
  failed scan returns its error and stays deferred;
- restores the property membership sidecars (the pinned property lookups and `RelsEffectivePage` /
  `NodesEffectivePage`) the closing process had built, so their first lookup scans nothing (backlog 74). They
  are derived from the current and history rows like the indexes, so the same trust rule covers them; one not
  built at `Close` is built at its first lookup, in the one pass that builds every unbuilt declared sidecar of
  its kind. v4.56.0 and older do not know the section and rebuild from the rows (a downgrade costs one full open);
- fills node integrity hashes on first use.

Measured on the ai-soc shape: `tasks/evidence/open-cost/03-open-time.txt`. Read-only opens read the image and
never write one. A store whose rows were damaged on disk after a clean close (no commit) opens on the fast path
and reports the damage when the row is read, where the full rebuild skipped and counted it
(`IndexRebuildStats`). A raw badger commit that only deletes keys can be dropped by a bottom-level compaction,
which leaves the header the newest version again and the image trusted; rho-tkg's own deletes commit together
with their counters, so only raw writers can hit this.

**Periodic images (`OpenImageInterval`, OPENIMAGE step 2, v4.89.0).** A store that never closes cleanly reopens
from its last clean `Close`, so a crash reopen decodes every row written since, without bound in a long run.
`badger.Config.OpenImageInterval` (mirrored by `graph.Config`, `tiered.Config` for every shard it opens and
`sharded.Config` for every slot; zero, the default, writes images at `Close` only; ignored in-memory and read-only)
writes an image every interval while the store runs: it takes the flush lock and the index read lock, commits the
write buffer, encodes the RAM indexes at since = the newest badger version, releases the locks and writes the
image beside the previous one (two slots, above). Writes wait for the flush and the encode, and a read that queues behind a waiting write (the index lock is a
`sync.RWMutex`) waits with it. A crash
reopen then decodes only the rows written since the last image; a crash while one is written keeps the previous
one (`TestCheckpointCrashWhileWritingKeepsPrevious`), and an interval with no commit since the last image writes
nothing. A periodic image is never trusted whole: the next open is a delta open from its since version, also when
nothing was written after it. Measured at 1 M nodes / 1 M relationships with a property and a temporal index
(`tasks/evidence/open-cost/10-checkpoint.txt`): a 16 MB image (8 B per entity), writers wait 31-33 ms for its
encode, the chunk and header writes take 19 ms outside the locks, and a crash reopen after 100 writes decodes 300
rows in 2.7 s, against 4.0 M rows in 18.0 s for the full rebuild. Pick the interval from the crash reopen you can
accept: the decode work after a crash is the writes of one interval.

## Persistent property index (opt-in, `PropertyIndexOnDisk`)

With `badger.Config.PropertyIndexOnDisk` (or the mirrored `graph.Config.PropertyIndexOnDisk`) enabled, entries created by `CreatePropertyIndex` are kept out of RAM: instead of populating the in-memory `PropertyIndex.Entries`/`numBuckets` maps, each indexed `(node, value)` pair becomes one row under a new `0x0A` keyspace. Off by default — a `CreatePropertyIndex`'d property still lives entirely in RAM unless this flag is set. Follows the same shape as `LabelIndexOnDisk`/`AdjacencyIndexOnDisk`: persisted keyspace + prefix/range iteration + a pending-write overlay that resolves set-vs-delete PER KEY over the persisted keyspace (lesson 57), not a running aggregate.

**On-disk layout.** `0x0A/<2B propertyKeyToken>/<domain-tagged value bytes>/<8B nodeID>`. The value payload is domain-tagged:

- **Numeric** (any int/uint/float width): a fixed 18-byte payload — `domainTag(1) | sortKey(8) | subtypeTag(1) | rawBits(8)`. `sortKey` is the order-preserving IEEE-754 sign-flip encoding of the value's float64 magnitude (lesson 25's bit-pattern case, applied to key bytes instead of the hash/equality contract), so every numeric width shares ONE ordered domain — a byte-range scan spans `int`, `uint`, and `float` values uniformly, mirroring the in-memory ordered view's own cross-subtype range semantics. The `subtypeTag`+`rawBits` trailer disambiguates EXACT equality across types that share a magnitude: `int64(5)`, `uint64(5)`, and `float64(5.0)` sort to the same position but remain distinct stored values (matching the RAM index's type-prefixed `Entries`-map equality).
- **Raw** (string / bool / `TemporalValue`): `domainTag(1) | rawBytes(var)` — the canonical `types.IndexablePropertyValueKey` string's bytes, verbatim. None of these types support range scans, so byte order doesn't need to encode magnitude, and the value key's own `"s:"`/`"b:"`/`"tv:"` prefix already prevents cross-type collisions. Length is bounded by the property's own `ValidationLimits.MaxPropertyValueSize` check at write time (properties are validated before they ever reach index maintenance) — the on-disk codec enforces no additional limit. The payload has no terminator, so a value's key prefix is shared by every longer value that extends it (`"s:/a"` by `"s:/a/b"`): the STARTS WITH scan relies on that, and an equality lookup walks from `prefix||minNodeID` to `prefix||maxNodeID` only (PROPSTR, v4.88.1), bounds of the node IDs the entries carry: the node rows' smallest and largest IDs at the first lookup, widened by every entry a write builds before it commits and never narrowed, kept in memory (PROPBOUNDS, v4.89.3: a seek from each end of the node keyspace per lookup skipped every deleted and rewritten row there). A longer value is visited only if its next eight bytes fall between the bounds (`TestPropStrLookupIgnoresLongerStrings`: 2 keys visited beside 1,000 and 10,000 longer strings, before 1,001 and 10,001). The lookup reads every version and deleted entry of its range and keeps each key's newest version, so the walk ends at its bound: without `AllVersions` badger skipped deleted entries and older versions inside `Seek` until the second live key after the start, wherever it was (PROPSEEK, v4.89.1, `docs/badger.md` rule 12).

**Key design point.** The on-disk key deliberately omits the label token (unlike the RAM `PropertyIndexKey{LabelToken, PropertyKey}`), so a property key indexed by definitions on two DIFFERENT labels shares ONE physical row. This is safe because every reader (equality via `NodesByLabelAndProperty`, range via `ForEachNodeByLabelPropertyRange`) already re-fetches the candidate node and rechecks `HasLabelTokenRaw` before trusting a match — the same over-select-then-recheck contract those readers use against the RAM-mode indexed path. `DropPropertyIndex` reference-counts by PropertyKey (not the full label+key pair) before physically purging rows, so dropping one label's definition never corrupts a sibling definition on another label that indexes the same property key.

**Write-path maintenance.** Every node-mutation door maintains the on-disk keyspace: `PutNode`, `DeleteNode`, `ReplaceNode`, `ReplaceNodeWithHistory`, the four label-token doors (`AddNodeLabelToken{,WithHistory}`/`RemoveNodeLabelToken{,WithHistory}`), `PutNodesBatch`, `DeleteNodesBatch`, and the cascade-delete corruption-path brute-force purge (mirroring `PurgeNodeFromAllPropertyIndexes`'s RAM-mode sweep when node data is unavailable). Every site merges the property-index writeOps into the SAME `appendOps` call as the entity row and its other secondary-index writes — a property-index entry and the row it describes always land in one `WriteBatch`, never a committed-but-unindexed window. An update (the replace and label-token doors, with or without history) writes only the entries whose value changed: an entry it would delete and set again stays as stored, since each write added a version that every lookup of the value visits until compaction drops it (PROPREWRITE, v4.89.2, `TestPropRewriteUnchangedValueWritesNoEntry`). While a property index is being created the update writes it anyway: the build's backfill skips a node the write path touched (`TestPropRewriteDuringIndexBuildKeepsEntry`).

**Enable on an existing directory.** Unlike the label/adjacency keyspaces (written transactionally since the format's inception, so their on-disk mode needs no migration), the `0x0A` keyspace is NEW. A directory that already has property-index DEFINITIONS (from a prior `CreatePropertyIndex` call, entries kept in RAM) is backfilled from current node state exactly once, the first time `PropertyIndexOnDisk` is turned on — guarded by a `wire_format_version`-style meta marker (`property_index_on_disk_built`) so a later open with the flag still on skips the rescan and trusts the keyspace the ongoing write-path maintenance already kept in sync. A writable open without the flag clears the marker (its writes do not maintain `0x0A`), so the next open with it rebuilds the keyspace and deletes the rows the rebuild does not write again; a read-only open with the flag over an unbuilt keyspace keeps that open's entries in RAM (since v4.90.2).

**Per label (`PropertyIndexOnDiskLabels`, since v4.91.0).** `badger.Config`, `tiered.Config` and `graph.Config` take `PropertyIndexOnDiskLabels []string`: the property indexes of the listed labels keep their entries in `0x0A`, every other label's stay in RAM (ignored with `PropertyIndexOnDisk`, which lists every label). A definition's place is fixed when the open loads it or `CreatePropertyIndex` makes it; the label is named by the graph's registry, at open by `Config.LabelNames` (tiered passes the registry from its `meta/registry.msgpack`) or the registry the store persisted. Rows stay keyed by property key, not label, and every reader rechecks label and value, so a listed and an unlisted label may index the same key. Each listed definition has a built marker (`property_index_on_disk_def_built/` + label token + key), written by its create or by the open that rebuilds it and deleted by a writable open that keeps it in RAM or by its drop: a label that joins the list is rebuilt at the next writable open, together with every on-disk definition of its property keys, and the keys' stale rows are dropped in the same batch; a read-only open keeps an unbuilt definition in RAM. The first marker raises the store format marker to 12, so a release that would neither maintain nor clear it refuses the store. Every door that declines under `PropertyIndexOnDisk` (range counts, equality counts, bounds, value statistics, the ordered ID walks; `RangeCounts` and `Elements` at create) declines for a listed label's index only; `StatesPropertyBounds` still reports only the store-wide flag. A label cannot join while one of its values would make a key above badger's 65,000 bytes: that open fails (`Key with size ... exceeded 65000 limit`) and the store reopens unchanged without the label.

**Valid-end bounds (`PropertyIndexValidEnd`, since v4.94.0, VALIDENDPRUNE).** `badger.Config`, `tiered.Config` and `graph.Config` take `PropertyIndexValidEnd []store.PropertyIndexValidEnd` (`{Label, KeyPrefix}`): an on-disk definition of a listed label on a key starting with the prefix keeps, in the value of each of its nodes' `0x0A` entries, an upper bound on the node's effective valid end (8 bytes big-endian; empty: unknown). A current `ByLabelAndProperty` / `ByLabelAndPropertyValues` with `QueryOpts.ValidEndAfter` leaves out the entries bounded below the bound without reading their rows, and reads the rest. Entries are keyed by property key, not label, so a bound is the node's, whichever definition wrote it. A bound never lies below the node's end at the current state: every write of a node's current row writes its listed entries unknown in the row's batch, and so does a write of history rows alone (read from the node's current row); only a read sets a bound, the end it resolved from the node's timeline, and only if no row of the node was buffered since the lookup read the store's write sequence (4,096 stripes by node ID; a probe read before `Clear` is skipped, and one read before the store was opened again is skipped once its node is written on the new open). A refine writes index entries only: no row, stamp, change-log record or event, and a `TxPin` / `TxAt` read never sees it. A read-only open refines nothing. Each listed definition has a valid-end marker (`property_index_valid_end_built/` + label token + key), set by its create or by the writable open that lists it, which first writes unknown every bounded entry of the key (unless the open rebuilds the key), and deleted by a writable open that does not list it (its writes keep no bound); a read-only open trusts only a marked definition. The first marker raises the store format marker to 14: once a store was opened with the listing, an older release refuses it, and there is no rollback; a store never opened with the listing keeps its format. Counts since open: `g.Index().PropertyIndexValidEndStats()` (entries pruned, unknown entries read, bounds refined, refines skipped); whether a definition prunes: `g.Index().PropertyIndexValidEndListed(label, key)` (since v4.95.0). Tiered prunes on the reference shard only, so a listed label must be in `RefLabels`; other shards' nodes are read.

**Requires a property-key registry.** The on-disk key uses a compact 2-byte property-key TOKEN, resolved through the store's property-key registry (`badger.Config.PropertyKeyRegistry`, always wired when opened via `pkg/graph`). `CreatePropertyIndex` fails closed with `ErrInvalidStoreMutation` in disk mode without one, rather than silently indexing nothing.

**Range cardinality trade-off.** `NodeRangeCardinality` (the O(1) exact-count accelerator backing `count(p) WHERE p.k > x`) always declines (`exact=false`) in disk mode instead of reimplementing the RAM ordered view's per-value bucket-size sum — the persisted keyspace does not maintain those bucket sizes. This is a pure availability difference, never a wrong answer: callers already handle decline by falling back to `ForEachNodeByLabelPropertyRange` plus an exact count.

## Persistent relationship property index (`RelPropertyIndexOnDiskTypes`, since v4.92.0)

Purpose (RELDISK, SCALE goal 2: RAM does not grow with data). A relationship property index and the membership
sidecar behind its pinned reads (`TxPin`, or `TxAt` with a valid time) hold every value a relationship ever carried
in RAM, about 25 B per posting when a value has 20 or more rows and about 100 B when every value is distinct.
`badger.Config`, `tiered.Config` and `graph.Config` take `RelPropertyIndexOnDiskTypes []string`: the listed types'
property indexes keep their entries in the `0x0C` keyspace (key layout above), written in the batch of the row, and
no membership sidecar.

- **Current entries** (kind 1): one per relationship and indexed value, deleted when the value changes or the
  relationship goes; equality lookups (`ByTypeAndProperty`, its multi-value form) read them, then the rows.
- **Member entries** (kind 2): one per value a row of the relationship carried and per transaction time of such a
  row, history rows included; never deleted by an update. A reader folds them per relationship to the lowest
  transaction time (0, unknown, lowest of all): the membership sidecar's answer (`ForEachRelPropertyTxMember` and
  its window, page, range and any-value doors), read from disk. An update that keeps a value writes no entry again
  unless its row lowers the relationship's bound.
- **Node member entries** (kind 3): every node definition kept on disk (`PropertyIndexOnDisk` or a label in
  `PropertyIndexOnDiskLabels`) answers its pinned reads from member entries too, so neither index kind builds a
  membership sidecar in RAM. Its `0x0A` entries keep the current lookups.
- **Places and markers.** The type is named at open by `badger.Config.RelTypeNames` (tiered passes its registry
  file) or the registry the store persisted. Each definition kept in `0x0C` has a built marker
  (`entity_index_built/` + kind + scope token + key), set by its create or the open that builds it, deleted by a
  writable open that keeps it in RAM or by its drop: a type that joins the list is built at the next writable open,
  with its stale entries deleted in the same `WriteBatch` and the markers last; a read-only open keeps an unbuilt
  definition in RAM. The first marker raises the format marker to 13: a store with any on-disk property index
  built by v4.92.0 is refused by v4.91.x.
- **Removal.** A drop deletes the definition's entries; a retention purge and an exact erasure delete every
  entry of the entities they remove, history included (a member entry holds a value verbatim); `Clear` drops the
  keyspace.
- **Refused or declined.** `RangeCounts` and `Elements` on a listed type (`ErrCapabilityNotSupported`, also an
  Elements definition loaded at open). A member key is 12 bytes longer than the `0x0A` key, so the largest value an
  on-disk index accepts shrinks by 12 bytes; every write door refuses a longer one with `ErrEntryTooLarge` before
  any mutation.
- **Cost.** A paged member lookup (`...After`) walks its whole value, then pages; a history truncate leaves member
  entries, which only widen a candidate set the reader filters by the rows.

## Composite node indexes on disk (since v4.96.0, COMPDISK)

Purpose (SCALE goal 2: RAM does not grow with data). A composite node index (`g.Index().CreateComposite`) held
every node's tuple in RAM, about 8 B a posting: on ai-soc's BA store DerivedPage's (ov_namespace, ov_rel) held
253 MB (287 tuples, 30.9 M postings) and grows with valid time. A composite of a label whose property indexes are
kept on disk (`PropertyIndexOnDisk`, or the label in `PropertyIndexOnDiskLabels`) keeps its entries in `0x0C`
instead; no new option.

- **Current entries only** (kind 4): one per node and current tuple, written in the batch of the row and deleted
  when the tuple changes or the node goes; an update that keeps the tuple writes no entry. The RAM index holds its
  declared keys only (`PropertyIndexStats`: `OnDisk`, 0 bytes).
- **Reads.** A current `ByLabelAndProperties` and the ID page behind the composite page doors seek to the tuple's
  first entry above the cursor (`QueryOpts.After`) and read up to the page's limit, the write buffer merged; the
  rows are read per page as with the RAM index. A pinned or valid-time read keeps its path: the current matches
  (read from these entries) with the single-key member sets of its keys, or else the label's history, each
  candidate rechecked against its rows, so it needs no member entries.
- **Places and markers.** As for the single-key definitions ("Persistent relationship property index"): each
  composite kept on disk has a built marker (`entity_index_built/` + kind 4 + label token + the encoded key tuple),
  set by its create or the writable open that builds it, deleted by a writable open that keeps it in RAM or by its
  drop. A label that joins the list is built at the next writable open from current rows, its stale entries
  deleted in the same `WriteBatch`; a read-only open, or an open without a property-key registry, keeps an
  unmarked definition in RAM. A create reads the label's rows without the index lock and then merges an entry for
  each node not written meanwhile (a write during the scan writes its own entry).
- **Format.** The first marker raises the format marker to 15: once a store built a composite on disk, v4.95.x and
  older refuse it, and there is no rollback; a store with no composite of a listed label keeps its format.
- **Removal.** A drop deletes its entries and marker, a retention purge and an exact erasure the removed node's
  entries, `Clear` the keyspace.
- **Cost.** The build at an open scans the label's rows once, inside `New`. An entry key is the head (8 to 12 B),
  the tuple key and the ID: a few dozen bytes of disk per node where RAM held about 8 B.

## Persistent temporal-index rebuild accelerator (opt-in, `TemporalIndexOnDisk`)

With `badger.Config.TemporalIndexOnDisk` (or the mirrored `graph.Config.TemporalIndexOnDisk` / `tiered.Config.TemporalIndexOnDisk`) enabled, the maxTo-augmented temporal interval index (`g.Index().CreateTemporal`) keeps a compact raw-entry log on disk so a store reopen no longer has to re-derive every entry from a full node fetch+decode. This is a DIFFERENT trade-off shape than `LabelIndexOnDisk`/`AdjacencyIndexOnDisk`/`PropertyIndexOnDisk` above: those move an index OFF the RAM heap and answer LIVE reads from the persisted keyspace instead. The temporal interval index's stabbing (`QueryAt`) and interval (`QueryOverlap`) queries walk an implicit balanced BST augmented with per-subtree max-upper-bound (`subMax`) — a structure that has no on-disk analogue — so it always stays fully resident in RAM at runtime; the flag has zero effect on live query results within a session. It exists solely to make the REBUILD-AT-OPEN step cheap.

**The cost this eliminates.** `loadIndexesScan`'s first pass (an unconditional `KeyNode` prefix iteration) already decodes every node row once to rebuild `nodeIDs`/`labelIdx`/property-key counts — that decode is unavoidable baseline overhead shared by every store. Historically, rebuilding a label's `TemporalIndex` was a SEPARATE, ADDITIONAL pass on top of that baseline: for every node carrying a temporal-indexed label, a second Badger point-get plus full msgpack decode of the entire row, purely to extract two `int64` fields (`from`, `to`). `TemporalIndexOnDisk` replaces that second pass with a compact prefix stream over the new keyspace.

**On-disk layout.** `0x0B/<2B labelToken>/<8B order-preserving-encoded from>/<8B nodeID>`, value = `<8B to>`. The `from` component is encoded with the standard sign-bit-flip trick (mirroring the numeric domain's IEEE-754 sign-flip in the property index above, applied to a signed `int64` instead of a float), so a plain prefix iteration over one label's sub-keyspace visits entries in EXACTLY the `(From ASC, ID ASC)` order `TemporalIndex.Entries` requires — `loadIndexesScan` streams straight from iteration order into `TemporalIndex.AddKnownAbsent`, no separate sort pass.

**Write-path maintenance.** Maintained ALONGSIDE — never instead of — the existing RAM maintenance (`indexpkg.AddNodeToTemporalIndexes`/`RemoveNodeFromTemporalIndexes`) at every node-mutation door that already calls those: `PutNode`, `DeleteNode`, `ReplaceNode`, `ReplaceNodeWithHistory`, the four label-token doors, `PutNodesBatch`, `DeleteNodesBatch`, and the cascade-delete corruption-path brute-force purge. `CreateTemporalIndex`'s own three-phase backfill writes the disk rows directly (no reliance on the rebuild-on-enable marker); `DropTemporalIndex` purges the whole `labelToken` prefix — safe unconditionally because (unlike the property-index keyspace, whose rows are shared across labels indexing the same property key) a `TemporalIndex` definition is exclusively keyed by `labelToken`: only one definition can ever exist per label. A create that fails or crashes before it publishes purges the label's rows, and an open purges the rows of every label with no definition, so a later create plus a crash reopen cannot load two rows for one node. How the create stays sound against concurrent writes: architecture.md, "Temporal envelope indexes".

**Enable on an existing directory.** The `0x0B` keyspace is new (like `0x0A`), so an existing directory with temporal-index definitions predating the flag is backfilled from current node state exactly once, on the first open with the flag set — guarded by the `temporal_index_on_disk_built_v2` meta marker (`storeutil.TemporalIndexOnDiskBuiltKey`) (mirroring `property_index_on_disk_built`), committed in the SAME `WriteBatch` as the backfill rows so a crash mid-backfill leaves either nothing or everything, never a half-built keyspace a later open trusts as complete. The marker is set unconditionally on the first open with the flag on (even an empty backfill), so any temporal index created afterward — in that session or a later one — stays self-sufficient via the live write-path maintenance above.

**Measured** (100k entities, one temporal-indexed label, ~6 properties each; Apple Silicon, in-tree): ~1.8x faster open (e.g. 795ms → 452ms). The ratio is driven specifically by eliminating the redundant SECOND full-row decode pass described above, not by avoiding node decode entirely (the first, unconditional pass still pays that cost) — a corpus with multiple temporal-indexed labels covering overlapping entities would see a larger win, since each additional label used to mean one more redundant per-entity decode pass. See `TestTemporalIndexOnDisk_OpenTimeMeasurement_100k` in `pkg/graph/store/badger`.

## Encryption at rest (opt-in, `EncryptionKey`)

With `badger.Config.EncryptionKey` (or the mirrored `graph.Config.EncryptionKey` / `tiered.Config.EncryptionKey`, which applies per shard) set, Badger encrypts SSTables, the value log, the WAL, and its own key registry with AES. Off by default, in which case the on-disk format is byte-for-byte the pre-encryption one.

**Key length is validated at `New`.** The key must be 0 (disabled), 16, 24, or 32 bytes — AES-128/192/256; any other length fails closed with `badger.ErrInvalidEncryptionKeyLength`. `EncryptionKeyRotation` is Badger's `EncryptionKeyRotationDuration` (how often a new internal data key is generated for the encrypted value log); zero keeps Badger's stock 10-day default, and the field is ignored when no key is set.

**Encryption REQUIRES both caches.** Badger panics — not returns an error — at `Open` when encryption or compression is on with `BlockCacheSize == 0`, and on the first encrypted SSTable flush with `IndexCacheSize == 0` (its stock `IndexCacheSize` default is 0, unlike `BlockCacheSize`'s 256MB). `New` therefore fails closed with `badger.ErrEncryptionRequiresBlockCache` / `badger.ErrEncryptionRequiresIndexCache` rather than letting either panic escape. Set both positive whenever `EncryptionKey` is set.

**Wrong key / plaintext directory fails at open, before any row is read.** Reopening an encrypted directory with the wrong key, or opening an existing PLAINTEXT directory with a non-empty key, fails `Open` with an error wrapping `badgerv4.ErrEncryptionKeyMismatch` (`errors.Is`-able) — Badger detects both by decrypting a sanity marker in its `KEYREGISTRY` file.

## History delta encoding (opt-in, `HistoryDeltaEncoding`)

With `badger.Config.HistoryDeltaEncoding` (or the mirrored `graph.Config.HistoryDeltaEncoding` / `tiered.Config.HistoryDeltaEncoding`) enabled, version-history rows (`0x07`/`0x08`) are stored as anchor+delta instead of full snapshots (B6, since 4.17.0): a version `V` with `V % HistoryAnchorInterval == 0` is a full ANCHOR and the rest are DELTAS carrying only the properties that changed vs that interval anchor, eliding large unchanged values a full snapshot would re-serialize every version. The CURRENT row (`0x01`/`0x02`) is always full. Off by default while the path soaks; a no-op on the memory backend, which keeps full snapshots as the differential oracle.

**No migration, and no wire-format bump.** The framing is self-describing — an anchor (and any legacy pre-flag row) is the raw full marshal, a delta is 1-byte `'D'`-tagged — so reads always accept BOTH forms and the flag can be toggled on an existing store. A downgraded delta-unaware binary opens the store but fails closed with `ErrCorruptWire` on a `'D'`-tagged row rather than misreading it.

**The anchor interval is baked into the on-disk layout.** `badger.Config.HistoryAnchorInterval` (mirrored on `graph.Config`; the tiered config carries only the on/off flag, so its shards use the default) overrides the spacing (0 = the default 16; validated at `New` as 0 or in `[2, 4096]`; moot when `HistoryDeltaEncoding` is off). A larger interval stores more deltas per anchor (less storage, more reconstruction reads); a smaller one the reverse. Because a delta reconstructed against the wrong anchor would be a SILENT misread, the interval is pinned by a persisted marker verified at open: reopening a delta store at a different interval fails closed with `store.ErrHistoryAnchorIntervalMismatch`. To change it on an existing delta store, rewrite history — there is no inline migration.

## History rows: versions, lives and RAM sidecars (no format change)

Every row a write appends takes the version one above the highest stored for the entity (current row and history; `internal/core/version_alloc.go`), so history keys `0x07`/`0x08` `/<id>/<version>` are unique and ascend in write order. A re-import of a deleted ID continues that numbering and links its `PrevHash` to the top row; chains written by v4.43–v4.47 whose earlier life a re-import overwrote stay as stored (no migration). The read-side doors `HasHistory`, `LatestStamps` and the pinned property lookups answer from RAM sidecars (badger: history presence set, history-stamps fold, per-index property tx-membership; memory: its maps): built lazily on first use after open from the history keys and values, maintained wherever a history key enters the write buffer, dropped by `Clear`. The property tx-membership sidecars are built by one scan per kind (rel, node) for every declared index and travel in the open image (backlog 74); the others are never persisted — a reopen pays one build per sidecar instead of a migration.

## Column segments (opt-in, `RelSegments` / `SegmentDir`, memory store)

`graph.Config.RelSegments` declares bulk relationship types (ADR-0011): the memory store seals their rows into immutable column segments once the unsealed rows exceed `SegmentMemoryBudget` (default 256 MiB), and every read door answers from the union of unsealed rows and segments with identical answers. Without `SegmentDir` the segments stay in RAM. With it, each sealed segment is a self-contained file in that directory (`internal/segdir`): `LOCK` (flock of the one open store), `MANIFEST` (magic, version, CRC32C-checked JSON body: the truth), `seg-t<token>-<seq>.tkgs` files read memory-mapped, and `*.tmp` writes in progress. A seal writes the segment `.tmp`, fsyncs, renames, fsyncs the directory, then commits the manifest the same way; a segment exists iff the durable manifest lists it, and the next open removes `.tmp` and unlisted files. A declaration on badger, tiered or sharded fails `New` with `ErrCapabilityNotSupported` (badger is ADR-0011 step S7).
