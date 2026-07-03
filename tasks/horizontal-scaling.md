# Horizontal Scaling Plan — rho-tkg (base layer)

Status: Phase 0 and the rho-tkg half of Phase 1 **SHIPPED** in `CHANGELOG.md`
`[4.10.0]`–`[4.11.0]` (durable change-log, log-shipped read replicas, gapless
bootstrap, lazy registry refetch, failover lease, plus node-level delta
export/merge; see "Implementation status" below). The remaining Phase-1 work is
sigma-side (Bolt routing + read-your-writes watermark + promotion orchestration);
Phase 2 (partitioning) is deferred, out of scope. This is the "beyond one box"
companion to sigma-tkgd's `docs/design-enterprise-scale.md`, which shipped the four
single-node ceilings (on-disk label/adjacency indexes, streaming `ForEach*` scans,
byte-budgeted caches, columnar frames) and explicitly deferred sharding as "a
distributed-systems project (partitioning, cross-shard traversal, distributed
transactions)". This document is that deferred project, scoped base-layer-first.

Consumer context: sigma-tkgd embeds rho-tkg via `graph.New(graph.Config{Store: ...})`
and drives it through Cypher (`pkg/cypher`), Tyla warded-Datalog reasoning (`pkg/tyla`),
a Bolt/Neo4j-wire server (`pkg/bolt`), and an HTTP server (`pkg/server`).

## Decision (confirmed with the maintainer, 2026-06-23)

**Target axis: read throughput + HA. The graph fits on one node. STOP after Phase 1
(read replicas). Do NOT partition.** Rationale: deep variable-length / `shortestPath`
Cypher and Tyla's whole-EDB fixpoint chase are catastrophic under partitioning (one RPC
fan per BFS frontier per hop); read replicas need none of the hardest blockers.

Phase 2 (partitioning) is documented below for completeness but is OUT OF SCOPE until a
decision gate is re-opened by proven data-size or write-throughput pressure.

## The sequence

```
Phase 0  (base layer only, ships standalone as CDC/audit/PITR)  ──► everyone
Phase 1  (single-writer primary + log-shipped read replicas)    ──► the target; STOP here
Phase 2  (partitioning B/C/D, forked on a decision gate)         ──► DEFERRED, out of scope
```

Phase 0 and the rho-tkg half of Phase 1 are entirely base-layer work. sigma is touched
only in Phase 1, only for Bolt routing and the read-your-writes watermark.

---

## Implementation status (shipped in `[4.10.0]`–`[4.11.0]`)

**Phase 0 — DONE & merged** (commit `0bb83f9`): durable ordered change-log + `g.Replication().ChangeFeed/ForEachChange/LastCommittedLSN`.

**Phase 1 — base-layer apply engine DONE** (this increment): a graph opens as a
log-shipped read replica and converges byte-exact via bootstrap + tail.
- `graph.Config.ReadOnlyReplica` + `ErrReadOnlyReplica` + core `checkWritable()` gate on every user mutation door (reads / `IO().Import` / apply stay open).
- `g.Replication().ApplyChange`/`ApplyChanges` (verbatim foreign-ID-door apply, reusing the import trust pipeline; idempotent) + `AppliedLSN`/`SetAppliedLSN` watermark (`meta/replica_applied_lsn`).
- `ChangeNodePut`/`ChangeRelPut` reshaped to `NodePutBody`/`RelPutBody` with the `WithHistory` bit (replica reproduces history depth exactly); put-record wire built untokenized → byte-identical cross-backend feeds + no propkey-registry dependency.
- Tests: end-to-end byte-exact convergence (all record kinds incl. cascade delete + deleted-entity history), idempotent double-apply, read-only gate parity.

**Phase 1 — base layer COMPLETE** (this increment): the deferred items above are done.
- Gapless handoff: export header bumped to v2 with `SnapshotLSN`, read via `LastCommittedLSN()` UNDER the export's `c.mu.Lock` (gapless); import records it as the replica's initial applied watermark (flush-before-watermark). Importers accept v1 OR v2.
- Lazy token-registry refetch: `g.Replication().RegistrySnapshot()` (`store.RegistrySnapshot{Labels,RelTypes,PropKeys,CapturedAtLSN}`, LSN-before-names capture) + `store.ReplicationSource` (`Config.ReplicationSource` / `g.SetReplicationSource`; a primary's `g.Replication()` satisfies it in-process) + the apply-path hook (`validate*TokensWithRefetch`) that refetches on an unknown label/rel-type token, guards `CapturedAtLSN >= rec.LSN`, append-only-extends via the new registry `AppendNames(prefix,suffix)` grow primitive (all 3 registries; persist-then-rollback on failure), and re-validates. Property keys are NOT synced (untokenized in records → tokenized locally).
- Failover: `store.IDSlotLeaseRecord` + `g.Replication().IDSlotLease()`/`SetIDSlotLease()` (`meta/id_slot_lease`, `SafeUnmarshal` read, slot 0-15 validated, last-writer-wins hint); promotion = Close+New with the leased `SnowflakeNodeID`. The orchestrator owns slot assignment + the reopen trigger. (`UseLeasedSlot` auto-read-at-New was intentionally NOT built — it would require reordering `New` to construct the store before the immutable generators; the orchestrator reads the lease and passes the slot explicitly.)
- Tests: AppendNames per-registry (parity, prefix-divergence, capacity, malformed); v1/v2/too-new version range; SnapshotLSN watermark round-trip + no-changelog; refetch happy-path (post-snapshot label + rel-type resolve, byte-exact converge) + no-source fail-closed + RegistrySnapshot-no-changelog; lease round-trip + range-reject + promotion-by-reopen (slot bits + no collision).

**Phase 1 — remaining = sigma only (the network/orchestration half):**
- Bolt ROUTE per-role lists (write→primary, read→replicas); the transport that carries `RegistrySnapshot`/`ApplyChange` over the wire (a `store.ReplicationSource` impl + a tail loop).
- Session-scoped read-your-writes LSN watermark (consumer-side; compare a session bookmark against a replica's `AppliedLSN()`).
- Promotion orchestration: decide the new slot, write the lease (serialized), trigger Close+New. rho-tkg exposes all the primitives; sigma coordinates.

---

## Readiness map (file:line-grounded; the "why" behind the shape)

### 5 hard blockers

| # | Blocker | Phase 1 (replicas) | Phase 2 (partitioning) |
|---|---|---|---|
| B1 | 16-instance ID ceiling. `SnowflakeNodeID` 0-15; nodes=`id*2`/rels=`id*2+1` fill the 5-bit node field; microsecond mode hard-caps node+step at 15 bits (no spare). IDs are 8-byte BE values HASHED into `tkg_hash`/`tkg_prev_hash` and rel hashes bind BOTH endpoint IDs. The snowflake TIME field is triple-load-bearing (tiered shard router + 256-mutex lock-shard selector + `tkg_created_at` fallback). | **Avoided** (one writer; existing generator) | Must repurpose the 5-bit node field as a partition id (keeps int64/parity/hash anchor); hard cap 16 partitions |
| B2 | Cross-entity atomicity is LOCAL only. `Replace*WithHistory` / `DeleteNodeWithHistory([]RelTombstone)` commit current+history+adjacency+counters in ONE Badger `WriteBatch`. No 2PC, no durable redo/undo; tx rollback is in-RAM-snapshot only. | **Avoided** (one atomic door; replicas re-verify) | Needs saga (eventual) or Raft+2PC (strong; durable prepared-state does not exist today) |
| B3 | In-process locking. `locks.Manager` = fixed 256 `sync.Mutex` keyed on the low 8 bits of the snowflake TIMESTAMP (not entity ID); `c.txMu` serializes all tx; `c.mu` RWMutex per call. | **Avoided** (single lock domain) | Needs distributed lock or single-owner-per-partition routing |
| B4 | No global commit sequence (LSN) and no change-data-capture seam. Ordering = per-entity `uint32` Version + per-process wall-clock `TxFrom` (1ms floor). No ordered op-log; no observer/hook anywhere in `pkg/graph/store`. | **Solved in Phase 0** | Reused (LSN becomes the replication/Raft log) |
| B5 | Process-local token registries. label/reltype/property-key string→uint16 maps; TOKENS (not strings) are persisted in every `NodeWire`/`RelWire`. Independent per-partition allocation maps the same token to different strings. | **Avoided** (replicas replay the snapshot; never allocate) | Mandatory cluster-wide registry as a hard prerequisite |

### Key enablers
- **E1 — single clean Store seam.** `Config.Store storepkg.MandatoryStore` (core.go:221); non-nil used verbatim. A new backend needs zero changes to `core.New`, the 14 sub-APIs, or sigma. sigma already injects `tiered` this way.
- **E2 — optional capabilities by type-assertion at `New`** (core.go:904-920). New capabilities (ChangeFeed, …) add purely additively.
- **E3 — the wire format is already a replication-log entry.** `NodeWire`/`RelWire` carry `FormatVersion` + ID + tokens + props + Version + full `TemporalMetadata` + integrity hashes. `CurrentWireFormatVersion` + `ErrWireFormatVersionUnsupported` = working version negotiation. Export framing (`writeExportRecord`/`readExportRecord`, export.go:357/387; amplification-safe `readImportStageRecord`) = ready stream frame format.
- **E4 — the import trust pipeline IS the replica apply path.** `SafeUnmarshal → WireTo*Checked → token-in-registry validate → content-hash recompute → full-chain verify → rollback on mismatch`. Export/Import + `tiered.MigrateFromBadger` = working full-graph SEED/bootstrap primitive.
- **E5 — `MetaKVCapability`** = durable side channel for applied-LSN watermark / schema markers. `HistoryVersionPageCapability` + `AllNodeHistoryIDsFrom` = cursor-resumable catch-up. `QueryOpts` keyset pagination (After + sorted-by-ID) = free k-way merge key.
- **E6 — untrusted-external-store → defensive deep-copy** (`storeRowsTrust`, queries.go:242) already makes a remote store safe by default.
- **E7 — node-first adjacency** in both engines (`Outgoing`/`OutgoingForNodes` → `map[NodeID][]rel`) = clean per-shard RPC unit + frontier-batching primitive.
- **E8 — Bolt `ROUTE`** already speaks the Neo4j routing-table protocol with WRITE/READ/ROUTE roles + `AdvertisedAddress` (returns one server today; a cluster populates real per-role lists with NO protocol change). `QueryMeta.Mutates` is the write/read classifier.

### Sigma constraints that shaped the recommendation
- S1 — transparent at the Store seam, BUT `pkg/cypher` binds a concrete `*graph.Graph` through every operator (no interface inside cypher) → routing must live BEHIND the Store, never require interface-izing the sub-APIs.
- S3 — deep var-length / `shortestPath` is the most network-hostile path (BFS frontier+visited in one heap, crosses shards every hop). Read replicas keep it fully local.
- S4 — whole-graph materializers (`buildAdjCSR`, GRAIL `buildReachIndex`, `asOfSnapshot`, Tyla `BuildEDB`) are single-process; opt-in/cache-amortized → run fine on a full read-replica.
- S5 — Tyla chase runs purely on an in-RAM `FactSet`, never touches the graph during iteration; a replica holds the whole graph so the chase is unaffected.
- S6 — Bolt sessions are sticky-by-connection with in-RAM result streams; `RUN` is immediate autocommit; rollback-after-write fails. Read-your-writes is currently free and breaks across nodes → needs a session-scoped LSN watermark (Phase 1).

---

## Phase 0 — Durable Ordered Op-Log + Cluster LSN (IMPLEMENTATION-READY)

Scope: rho-tkg base layer only. Topology-agnostic foundation. Purely additive; breaks no
existing contract; default-off (zero overhead). Backends: **badger (durable) + memory
(in-RAM, test parity)**. Tiered deferred (§Phase-0 deferred).

Standalone value: **change-data-capture / audit trail / point-in-time recovery on one
node.** NOT replication yet — replication (base snapshot + tail the feed + apply path) is
Phase 1.

> Validated by a 3-lens adversarial review against the actual code (2026-06-23). The
> architecture below (in-backend, single-WriteBatch atomicity, LSN under `wbMu`) was
> verified correct; the taxonomy/scope/contract corrections from that review are folded in.

### A. The load-bearing decision: IN-BACKEND, not a generic decorator

A `ChangeLogStore` decorator over `MandatoryStore` is WRONG for two code-grounded reasons:
1. **Atomicity.** Crash-safety requires the log record in the SAME Badger `WriteBatch` as
   the rows + counters (badgerstore_flush.go:111-193, the "counters in same batch" rule).
   A decorator sits above the Store interface and cannot reach the inner `WriteBatch`; it
   could only do a second separate write, reintroducing the committed-but-unlogged window.
2. **Trust/perf.** `core.New` discovers native backends by reflection (`isExactNativeStore`
   core.go:284-291, matches only `*memory`/`*badger`/`*tiered`) and sets `storeRowsTrust`.
   A decorator wrapping badger is untrusted → the frozen-pointer zero-copy scan path is
   disabled and every scan row is defensively deep-copied (queries.go:242).

**Decision: badger.Store and memory.Store implement the op-log + `ChangeFeedCapability`
directly, in-backend.** Optional capability, type-asserted at `core.New` like every other.

### B. Replication model: SNAPSHOT + OP-LOG (the op-log alone does NOT converge a replica)

Token `GetOrCreate` happens in the CORE layer (resolution.go, validation.go); the badger
store only ever sees already-tokenized rows and cannot name a token or co-commit a
per-token registry record. So a per-token `ChangeRegistry` tag is architecturally
impossible in-backend. Mirror export/import:

- A replica bootstraps from a **full snapshot** (export: registry `RegistryFileData` +
  entities + history), THEN tails the op-log from the snapshot's LSN.
- Tokens in the feed are resolvable from each row's own `NodeWire`/`RelWire` bytes.
- **Document plainly:** the op-log alone is insufficient to converge a replica; a base
  snapshot is required. Incremental registry shipping, if ever needed, must be emitted
  from the CORE layer (not in-backend).

### C. New keyspace + meta marker (storeutil)

Add to `pkg/graph/internal/storeutil/keys.go` (next to `KeyHistRel`=0x08, `KeyMeta`=0x0F;
0x09 confirmed free — keys.go uses 0x01-0x08 + 0x0F only, and `loadIndexes` iterates
per-prefix so a new prefix cannot break rebuild):

```go
KeyChangeLog byte = 0x09 // + 8B BE LSN = 9B ; value = tag(1B) || msgpack(body)

func ChangeLogKey(lsn uint64) []byte                // k[0]=KeyChangeLog; BE uint64 lsn
func ChangeLogLSNFromKey(k []byte) (uint64, bool)   // inverse, for scan/seed
```

8-byte LSN (uint64): at 1M writes/s lasts ~580k years. Meta marker (sibling of
`WireFormatVersionKey`): `var LastLSNKey = MetaKey("last_lsn")` — value = 8B BE max
committed LSN.

### D. Record taxonomy (COMPLETE — covers every durable store-mutation door)

On-disk value at `ChangeLogKey(lsn)`: `tag(1B) || msgpack(body)` (KV length implicit).
Bodies reuse existing `NodeWire`/`RelWire` (so `FormatVersion` negotiation is inherited;
no version bump).

| Tag | Emitted by (badger doors) | Body | Replica apply op |
|---|---|---|---|
| `NodePut` / `RelPut` | `PutNode`,`ReplaceNode`,`Replace*WithHistory`, `Add/RemoveNodeLabelToken{,WithHistory}` / rel equivalents | wire (new current) | upsert current row |
| `NodeDelete` (sub-kind **hard-cascade** vs **with-history**) | `DeleteNode`,`DeleteNodeCascade`,`DeleteNodeWithHistory` | `{id, optTombstone, relTombstones}` | cascade = hard-delete rows; with-history = append tombstones |
| `RelDelete` (same sub-kinds) | `DeleteRelationship`,`DeleteRelWithHistory` | `{id, optTombstone}` | as above |
| `NodeHistoryVersion` / `RelHistoryVersion` | `PutNodeVersion`/`PutRelVersion` (import.go, bitemporal cascade `temporal_cascade.go`, migration, tx-rollback restore) | `{id, version, wire}` | write history row at explicit version |
| `HistoryTruncate` / `HistoryTrim` | `Truncate*History`,`Trim*HistoryFrom` | `{id, keepVersions/minVersion}` | mirror truncation |
| `Meta` | `MetaSet` (schema/migration markers) | `{key, value}` | mirror meta |
| `Clear` | `Clear`/`DropAll` | — | replica clears + resets log state |

Notes:
- `DeleteNodeCascade` is a HARD delete (no tombstone/history, node_batch.go:17-37);
  `DeleteNodeWithHistory` DOES write tombstones. Two distinct apply ops → the delete
  sub-kind is mandatory.
- Pure index/meta-def doors (`CreatePropertyIndex`/`Drop*`/index-def persistence,
  orphan-index purge) call `appendOps` but represent NO logical entity change → they MUST
  NOT emit a record (verified correct in the plan: `appendOps` stays a thin wrapper).

### E. Badger backend changes

Struct fields (badgerstore.go, near pending/wbMu ~313-323):

```go
logSeq     atomic.Uint64   // last assigned LSN; seeded at open from LastLSNKey (empty range => 0)
logEnabled bool            // cfg.ChangeLog; gates all log work (zero overhead when off)
pendingLog []logRecord     // append-only (NOT coalesced), guarded by wbMu
```

`cfg.ChangeLog bool` added to `badger.Config` (+ `graph.Config.ChangeLog` passthrough);
default false = today's write path byte-for-byte.

**Per-door accumulator (NOT per-appendOps).** `DeleteNodeCascade`/`DeleteNodeWithHistory`
issue MANY `appendOps` under one `idxMu.Lock` (per-rel `deleteRelByInfo`, orphan purge,
node-delete, tombstone history). So assemble ONE logical record at the door boundary,
after all sub-`appendOps`, appended under the still-held `idxMu.Lock`. LSN assigned under
`wbMu` (atomic with the append) so order is total + gap-free. Current-state doors
(`PutNode`, `ReplaceNode`, `PutRelationship`) and `PutNodesBatch` (single `appendOps`) can
build the record at their existing single call site.

LSN-order == effective-order is verified for current-state doors: LSN assigned in the
`wbMu` section while the writer still holds `idxMu.Lock`; `idxMu` serializes the
`cache.Put`+enqueue unit. NB the history-version doors (`PutNodeVersion`/`PutRelVersion`)
call `appendOps` WITHOUT `idxMu` held — their ordering rests on the core entity locks +
the `wbMu` LSN assignment; account for this in the accumulator.

**flush() integration (badgerstore_flush.go:111) — with ALL mechanical fixes:**

1. In the Step-1 snapshot (under `idxMu.RLock`+`wbMu`), also grab+reset `pendingLog` —
   but place this swap AFTER `persistRegistryIfGrew()` succeeds (it early-returns on
   failure, flush.go:137-140), OR `requeueLog` in that failure path too.
2. Change the empty-check to `if len(ops)==0 && len(logs)==0 { return nil }` and still
   create/flush the batch when only logs are present (else log-only flushes vanish).
3. After entity ops + counters, before `wb.Flush()`: `sort` logs by LSN (robust to
   requeue interleave), `SetEntry(ChangeLogKey(lsn), tag||body)` each, then
   `SetEntry(LastLSNKey, maxLSN)` — all in the ONE WriteBatch.
4. Add `requeueLog(logs)` to EVERY error/early-return-after-snapshot branch:
   the `SetEntry` error branch, the `dbClosed`/`ErrDBClosed` branch (flush.go:177-181),
   AND the final `wb.Flush()` error branch (flush.go:182-185).

Data + log records + `LastLSNKey` + counters commit atomically → no committed-but-unlogged
and no logged-but-uncommitted window.

**Async backpressure:** include `len(pendingLog)` in `pendingLen()`/`flushIfNeeded` when
`logEnabled` (a hot-key workload coalesces `pending`→~1 but appends an uncoalesced record
per write, which would otherwise grow unbounded between flush ticks).

**SyncWrites:** unchanged structurally (flush per mutation). NB `DeleteNodeCascade` flushes
directly only under `syncWrites` (node_batch.go:33-36); in async mode its records sit in
`pendingLog` until the next tick — consistent with the existing data path.

**Restart seed (loadIndexes, ~badgerstore.go:900 where counters seed):** read `LastLSNKey`;
if absent, scan the max `0x09` key; **empty range => seed 0** (not error). `LastLSNKey`
commits in the same batch as the records, so marker and max-`0x09`-key are crash-consistent;
`logSeq` is monotonic via `logSeq.Add`, never rewound (mirrors `reconcilePersistedCounter`).

**Clear() (badgerstore.go:1232):** `DropAll` wipes `0x09` + `LastLSNKey`; ALSO reset
`pendingLog` under `wbMu`; keep `logSeq` monotonic across `Clear`.

**Read methods (decode through `SafeUnmarshal` — mandatory rule / lesson 47):**

```go
func (bs *Store) ChangeFeed(afterLSN uint64, limit int) ([]storecontract.ChangeRecord, error)
func (bs *Store) ForEachChange(afterLSN uint64, fn func(storecontract.ChangeRecord) bool) error
func (bs *Store) LastCommittedLSN() (uint64, error)
```

- All call `checkOpen()` + respect the `dbClosed` guard.
- Iterate `0x09/` seeking `ChangeLogKey(afterLSN+1)` ascending; decode `tag||msgpack`
  via `storeutil.SafeUnmarshal` → `ErrCorruptWire` on a corrupt value (never panic).
- Only DURABLY-COMMITTED records are visible (buffered `pendingLog` is not surfaced;
  consumer resumes from `LastCommittedLSN`). Callbacks invoked outside locks/txn.
- `ChangeFeed` slice form enforces a `limit` cap (OOM-safety; prefer `ForEachChange`).

### F. The capability (pkg/graph/store/capabilities.go)

Additive optional capability (documented like `TransactionTimeQueryCapability`); NOT
embedded in `Store` (genuinely optional). `ChangeRecord` + `ChangeTag` live in this package.

```go
// ChangeFeedCapability is OPTIONAL. Backends that maintain a durable ordered op-log
// expose it so a primary can stream committed mutations and a replica resume from a
// checkpoint. Records are returned in ascending LSN order; only DURABLY-COMMITTED
// records are visible. The feed is MUTATION-LEVEL: a rolled-back transaction appears as
// its forward ops followed by its compensating ops (it still converges a replica).
// Callbacks are invoked outside backend locks (same contract as ForEachNodeID).
type ChangeFeedCapability interface {
    ChangeFeed(afterLSN uint64, limit int) ([]ChangeRecord, error)
    ForEachChange(afterLSN uint64, fn func(ChangeRecord) bool) error
    LastCommittedLSN() (uint64, error)
}

type ChangeRecord struct {
    LSN     uint64
    Tag     ChangeTag
    Payload []byte // tag-specific msgpack body
}
```

### G. Transaction / CDC contract (mutation-level, honest)

The tx engine applies mutations as individual store calls DURING the tx (not buffered);
`Commit` does no store writes (tx.go:416-451); `Rollback` UNDOES by issuing MORE store
mutations (Truncate/PutNodeVersion/restore, tx.go:453-543,756-795). So the feed is
mutation-level: a rolled-back tx emits forward ops then compensating ops. This is honest
for CDC and STILL converges a replica (forward-then-undo ends at the restored state).
Tx-aware buffering (suppress rolled-back records) is a Phase-1 refinement if exactly-
committed CDC is later required.

### H. Core wiring + returnable watermark

- Discovery (core.go ~904-920, next to `txTimeQuery`/`depthHistory`): `if cf, ok :=
  store.(storepkg.ChangeFeedCapability); ok { c.changeFeed = cf }`.
- **Tiered promotion guard (REQUIRED):** `tiered.Store` embeds badger shards, so
  `store.(ChangeFeedCapability)` could succeed via method promotion and expose ONE shard's
  LSN as the global feed. Force tiered to nil via the `embedsNativeCapability`/
  `directMethods` pattern (as `nativeTransactionTimeQuery`, core.go:578).
- Accessor: a new nil-safe `g.Replication()` sub-API (mirrors the 14 accessors) exposing
  `ChangeFeed`/`ForEachChange`/`LastCommittedLSN`; returns wrapped
  `ErrCapabilityNotSupported` when `c.changeFeed` is nil. The ONE public-surface addition.
- Watermark: `LastAssignedLSN() uint64` = GLOBAL high-water-mark (not a tx-private set;
  sufficient for read-your-writes routing in Phase 1). Document `LastAssignedLSN >=
  LastCommittedLSN` until flush. For tx/batch: `CommitWithInfo() (CommitInfo, error)` with
  `Commit()` as a wrapper (verify `BatchAPI.Execute`'s real signature first).

### I. Memory backend

`memory.Store` implements `ChangeFeedCapability` with an in-RAM ordered slice + `logSeq`
atomic, appended in the existing mutation critical section (`ms.mu.Lock` covers the whole
mutation, memorystore_history.go:23). Not durable (acceptable — memory is never the
persistence of record); gives full test parity. Gate behind a `memory.New` option
(thread `graph.Config.ChangeLog` through core.go:899 — small constructor change), default off.

### J. Phase-0 deferred (state in CHANGELOG)

- **Tiered backend.** A correct tiered op-log needs a global LSN allocated ABOVE the shards
  and stamped into each shard's WriteBatch + cross-shard seed reconciliation — a tiered-
  level coordinator. Since the Phase-1 primary is a single badger store, tiered ChangeFeed
  is deferred. `tiered.Store` simply does not implement the capability → `g.Replication()`
  returns `ErrCapabilityNotSupported` (correct, honest).
- **Log GC.** Phase 0 keeps the log unbounded (or a simple time/size retention cap as a
  follow-up). LSN-watermark-driven GC (prune `< min-acked-LSN`) is Phase 1 (needs replica
  acks). Document the unbounded-growth caveat.
- **Network frame / apply codec.** `writeExportRecord`/`readExportRecord` framing is reused
  for the stream, but the apply path (decode + `SafeUnmarshal` + chain-verify + apply) is
  built in Phase 1; corrupt-frame APPLY tests live in Phase 1 (the corrupt-`0x09`-VALUE
  read test stays in Phase 0).

### K. Test plan (repo's 17 rules + Phase-0 specifics)

1. Every new public method direct test (badger + memory): `ChangeFeed`, `ForEachChange`,
   `LastCommittedLSN`, `CommitWithInfo`, `LastAssignedLSN`, `g.Replication()` accessors.
2. Node/Rel parity: every node-side tag has a rel-side test, INCLUDING the rel-side
   cascade-with-tombstones exact-set feed test.
3. Sentinel `errors.Is`: `ErrCapabilityNotSupported` from `g.Replication()` on a store
   without the capability (tiered, memory-with-log-off); corrupt `0x09` value →
   `ErrCorruptWire`.
4. Two-phase (rule 15): create X at LSN n, mutate to Y at LSN n+k; assert the feed has
   BOTH in order and replaying [0..n] reconstructs X, [0..n+k] reconstructs Y. ALSO a
   version-write-door two-phase: reconstruct a multi-version chain after an import, assert
   HISTORICAL not current state.
5. Adversarial exact-set (rule 16): multi-entity, multi-mutation; feed is EXACTLY the
   ordered LSN→record set (no missing/phantom/dup), including a cascade producing ONE
   `NodeDelete` record (not N) and a label-add producing a `NodePut`.
6. LSN monotonic + gap-free under concurrency (`make test-race`): N goroutines; LSNs
   1..M no gaps/dups; per-entity record order == mutation order.
7. Crash/restart seed: write K records, reopen, assert `logSeq` resumes at K+1; empty DB
   seeds 0.
8. Atomicity / requeue: simulate a flush error (SetEntry, dbClosed, wb.Flush branches);
   assert NONE of {row, counters, log records, LastLSNKey} landed and records re-flush
   intact + in order next cycle.
9. Log-only flush regression: a flush cycle with `pendingLog` non-empty and zero entity
   ops still writes the records + `LastLSNKey`.
10. SyncWrites vs async (+ `MaxPendingWrites` backpressure incl. `pendingLog`): identical
    feed content.
11. Tx committed-vs-rolled-back feed content (highest-risk interaction); import-into-a-
    log-enabled primary convergence.
12. Zero-overhead-when-off: `ChangeLog=false` writes no `0x09` keys, no extra allocs.
13. `make cover` ≥ 80% per new file; every tag branch exercised.

### L. PR breakdown (small independently-valuable releases)

- PR1 (S): storeutil keys (`0x09` + `ChangeLogKey`/`LastLSNKey`) + `ChangeRecord`/
  `ChangeTag` taxonomy + `ChangeFeedCapability` interface + codec tests. No behavior change.
- PR2 (M): badger in-backend op-log — fields, per-door accumulator, `flush()` integration
  with ALL mechanical fixes, restart seed, read methods with `SafeUnmarshal`. **Standalone-
  valuable release (CDC/audit/PITR).**
- PR3 (S): memory parity + cross-backend feed test.
- PR4 (S): core discovery wiring + tiered promotion guard + `g.Replication()` + watermark
  accessors.
- PR5 (S): docs (CHANGELOG `[Unreleased]`, docs/architecture.md op-log section,
  docs/persistence.md, CLAUDE.md key-layout `0x09` row) + lessons.md entry (the
  snapshot+op-log contract; the decorator-vs-in-backend decision).

Effort total: M. Risk: low (additive, default-off, single backend for the core).

---

## Phase 1 — single-writer primary + read replicas (the target)

Goal: linear read-throughput scaling + HA, no hash-chain/atomicity/in-process-heap impact.
Effort L, risk med. (Detailed plan to be drilled when Phase 0 lands.)

Base-layer work (rho-tkg):
- Replica apply path = the import trust pipeline (E4) made incremental: each record runs
  `SafeUnmarshal → WireTo*Checked → token-validate → hash-recompute → chain-verify`, then
  applies via the SAME atomic `Replace*WithHistory`/`Delete*WithHistory` door. Fails closed.
- Bootstrap = full export snapshot (registry + entities + history), then tail from the
  snapshot LSN (the snapshot+op-log model from Phase 0 §B).
- `ErrReadOnlyReplica` gating mutation doors on a replica.
- Per-replica applied-LSN watermark in `MetaKV`; ID-slot CAS lease in `MetaKV` for
  failover (keeps the 16-cap, parity, hash anchors untouched).
- Remaining op-log tags' apply ops (history-version, truncate, meta, clear); LSN-watermark
  log GC.

Consumer work (sigma — minimal):
- Bolt `ROUTE` (E8): real per-role lists (WRITE=primary, READ=replicas, ROUTE=all); route
  by `QueryMeta.Mutates`. No protocol change.
- Read-your-writes: Bolt session records max-written LSN; reads carry it as a SESSION-
  SCOPED routing token (NOT overloaded onto `QueryOpts.TxAt` — preserve bitemporal
  `NodeAtTx`); router dispatches to a replica with `applied-LSN >= required`, else primary.
  Router evicts replicas past a staleness bound (fail-safe).

Killer feature partitioning cannot match: each replica is a COMPLETE in-process graph, so
deep `-[:R*..k]->`, `shortestPath`, the CSR/GRAIL materializers, and the Tyla chase run
with zero cross-node hops.

---

## Phase 2 — partitioning (DEFERRED, out of scope)

Documented only so the boundary is explicit. Re-open ONLY if data size or cross-partition
write throughput is PROVEN to exceed one node. Decision gate:

1. Is the pressure read-throughput, data-size, write-throughput, or HA?
2. If data/write: are genuinely cross-partition edges RARE (co-locatable) or COMMON?
3. Do multi-partition Cypher CREATE/MERGE/DELETE need strict atomicity, or is
   eventual-consistency-with-anti-entropy acceptable?

| Variant | Scales | Consistency | Disqualifier risk |
|---|---|---|---|
| B — hash partitioning + saga + affinity + anti-entropy | data size, partition-local writes | eventual for cross-partition edges | endpoint-hash TOCTOU on the saga path silently breaks `VerifyRelChain` unless a cross-partition advisory lock spans fetch→bind |
| C — per-partition Raft + 2PC coordinator | data size + writes + HA | strong cross-partition atomicity | heaviest; blocking-protocol availability cliff; the Raft log IS the durable prepared-state (closes B2 + the TOCTOU) |
| D — compute/storage disaggregation | compute independently | per-storage-layer | same TOCTOU as B; route deep traversal to a full read-replica |

Each partition would itself be a Phase-1 primary+replica group (the two compose).
ID scheme: repurpose the 5-bit node field as a home-partition id (keeps int64/parity/hash
anchor) → hard cap 16 partitions. Other ID schemes break the hash format or tiered
time-routing.

### Explicitly deferred regardless
- Write-throughput scaling (single primary + global `c.txMu` + 16-cap generator is a hard
  ceiling; needs Phase 2).
- Data size beyond one node (every replica holds the whole graph, N-fold footprint).
- >16 partitions; transparent rebalancing of existing data (moving an entity changes its
  partition bits → new ID → broken hash chain; rebalance is an offline re-mint).
- Large-graph Tyla chase (the fixpoint needs the full EDB in one heap; Phase 2 distributes
  the EDB BUILD, not the chase).
- Cross-replica audit-log verification (per-process random HMAC keys, S7).
- Per-node process-local limit reconciliation behind a balancer (S7) — deployment concern.

---

## Method note

This plan was produced by: (1) a 7-area file:line-grounded read of rho-tkg + sigma-tkgd;
(2) 4 independent architecture designs adversarially scored on 5 lenses (temporal/integrity
correctness, sigma compatibility, base-layer incrementality, ops/failure complexity,
scales-the-right-thing); (3) a synthesized base-layer-first roadmap; (4) a 3-lens
adversarial review of the Phase-0 detail against the actual code, whose corrections are
folded into §Phase-0 above.
