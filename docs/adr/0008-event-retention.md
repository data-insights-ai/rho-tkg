# ADR-0008 — Event retention at scale: first-class entity purge routed by snowflake time

Status: DRAFT — owner review requested
Date: 2026-07-15
Depends on: ADR-0001 (history compaction — the trim/stub/watermark precedent this
extends from versions to whole entities), ADR-0005 (change-log — the replication
record family a purge re-uses), ADR-0007 (slot-sharded store — range purge is
per-shard-parallel there)

## 1. Problem

Cybersecurity and observability workloads ingest massive event volumes (log
entries: event→machine, event→user edges) and must continuously REMOVE events
that are no longer worth keeping — million-plus entity purges, on a rolling
window. Today the only removal doors are:

- **Point delete** (`DeleteNode`/`DeleteNodeCascade`) — writes TOMBSTONE history
  rows for the node AND every connected rel (`node_delete.go`), because "history
  is never erased." At retention scale this DOUBLES the write volume to remove
  data, and leaves the tombstones themselves to be reaped later.
- **History compaction** (ADR-0001) — trims OLDEST version-history rows of
  entities that are still live. Events rarely update, so they have no history to
  trim; compaction removes nothing that matters for log retention.

Neither is entity RETENTION: hard-purge whole aged-out entities + their indexes +
their history, cheaply, at range scale. That is the gap this ADR fills.

## 2. Decision

Add a distinct first-class operation — **retention purge** — that hard-removes
whole entities below a policy boundary WITHOUT writing tombstones. Because it
deliberately breaks the append-only "history is never erased" ethos, it is
constrained on three axes (mirroring how ADR-0001 constrained compaction):

1. **Policy-gated.** Never implicit. A caller supplies an explicit
   `RetentionPolicy{Label, Before}` to an admin door (`g.Admin().PurgeExpired…`);
   a store-global config flag (`Config.AllowRetentionPurge`) must be on. No
   background sweeper in v1 — the operator drives it (a cron/orchestrator calls
   the door), so purge timing is never a surprise.
2. **Watermarked.** A per-label retention watermark
   (`retention_watermark/<labelToken>` in MetaKV) records the transaction/creation
   time below which entities of that label have been purged. A temporal read
   pinned BEFORE a label's watermark fails with a new sentinel
   `ErrRetentionExpired` ("gone per retention policy") — NEVER silently absent,
   exactly as `ErrHistoryCompacted` guards compacted knowledge. Point doors check
   the queried entity's label watermark; scan doors fail the whole scan when the
   pin precedes any relevant label watermark.
3. **Logged.** A purge emits ONE logical range-purge change-log record (§2.2),
   not N per-entity deletes — so replicas converge without million-scale fan-out.

### 2.1 What "expired" means (v1 vs v2)

| Axis | v1 (this ADR) | v2 (follow-up) |
|---|---|---|
| **age** — `snowflakeTime(id) < Before` | YES — the cheap path. Snowflake IDs are time-ordered, so `0x01/<8B id>` is time-CLUSTERED: "all events of label L older than T" is a CONTIGUOUS key range. Purge is a sequential scan-and-delete over a prefix, not N point lookups. | — |
| **world-time expiry** — `ValidTo < T` | NO — `ValidTo` is not clustered by ID, so it cannot be a range. | YES — driven by the temporal interval index (`0x0B` keyspace, ADR persistence) which already orders by validity. The record format (§2.2) is designed so both fit. |

v1 covers the dominant log-retention need (drop events older than the window).
The `RetentionPolicy` struct carries a `Mode {ByAge, ByValidTo}` field from day
one so the door signature and the record format do not change when v2 lands.

### 2.2 Replication: one logical record, deterministically re-executed

Per-entity change-log records at million scale are fan-out madness. Instead a
purge emits ONE `ChangeRangePurge{Label, Mode, Before, PolicyHash}` record. The
replica RE-EXECUTES it deterministically: because replicas apply LSN-ordered,
their pre-purge state for label L below `Before` is BYTE-IDENTICAL to the
primary's (LSN total order ⇒ identical local current state), so re-running the
same range predicate removes exactly the same entities. The replica may execute
it as a range purge even if its shard topology differs (ADR-0007: a 16-lane
primary → 2-shard replica) — the record names a PREDICATE, not physical rows.

This is the same "retention ops flow through the log as re-executable predicates"
mechanism the deferred old-12c work anticipated; it is built HERE for range purge
first, and the ADR-0001 history-compaction record can adopt the same family later
(lifting `ErrCompactionChangeLogEnabled`).

### 2.3 Adjacency across the purge boundary

Events have edges. Purging an event must clean BOTH adjacency directions:

- **Both endpoints in-range** (event→event, or the purged event's own rels): the
  edge rows and their adjacency entries fall in the same purge and are removed
  with the range — cheap.
- **Edge into a SURVIVING reference entity** (event→machine, event→user): the
  survivor's incoming-adjacency entry (`0x06/<survivorID>/…`) must be cleaned or
  it becomes a phantom (the exact `AdjacencyOrphan` class ADR-0007's
  `VerifyConsistency` detects). This reverse-index cleanup is the expensive part.
  v1 is **correct-synchronous**: as each event is purged, its edges into
  survivors are removed from the survivor's inIdx in the same chunk batch. If
  measurement shows reverse-index cleanup dominates, v2 adds a LAZY repair sweep
  (mark survivors dirty, reconcile inIdx in the background) as the escape hatch —
  but v1 must not leave orphans.

On sharded (ADR-0007) a survivor and the purged event may be on different shards;
the cleanup respects the split-write ordering in reverse (remove the survivor's
inIdx entry on the survivor's shard). On tiered the survivor is a reference-shard
entity; the same reverse cleanup applies.

## 3. Invariants (each gets a test or a stated reason)

1. **No silent absence.** A temporal read pinned before a label's retention
   watermark returns `ErrRetentionExpired`, never an incomplete result. (Two-door
   test: point door per-label; scan door whole-scan — rule 15/16.)
2. **Idempotent + resumable.** A purge interrupted mid-range re-runs to the same
   end state. The watermark advances ONLY after its range is fully clean, and the
   purge below a watermark is a no-op — so a crash mid-purge is closed by re-run
   (the ADR-0001 crash model).
3. **No orphaned adjacency.** After a purge, `VerifyConsistency` (ADR-0007) /
   the tiered/badger equivalents report zero `AdjacencyOrphans` and zero
   `RelEndpointOrphans` for surviving entities. (Adversarial: an event with edges
   into a surviving machine — the survivor's inIdx must be clean afterward.)
4. **Replica byte-exact convergence.** A replica re-executing the
   `ChangeRangePurge` record reaches the same current state as the primary,
   including onto a different shard count. (The topology-independence crown, the
   ADR-0007 S3 pattern.)
5. **Locks bounded.** A range purge over millions of entities NEVER holds
   `c.mu.Lock` for the whole range — it chunks (entity-locked per chunk via
   `LockMany`, one `WriteBatch` + one log-record segment per chunk, chunk
   watermarks). A concurrent reader sees either pre- or post-chunk state, never a
   half-purged entity.
6. **History goes too.** A purged entity's version-history rows (`0x07`/`0x08`)
   AND index entries (label/adjacency/property/temporal) are all removed — a purge
   is not a soft-delete; the entity is gone below the watermark. (No stale index
   entry survives — the ADR-0007 "index cleanup on corruption" discipline.)

## 4. Staged plan

- **R0 — ADR acceptance** (this document).
- **R1 — watermark + sentinel + read-door plumbing (fail-closed FIRST).**
  `ErrRetentionExpired` sentinel (canonical in `internal/core`, re-exported from
  `pkg/graph`); per-label `retention_watermark` MetaKV; point/scan temporal read
  doors consult it. No purge yet — the guard lands before the thing it guards, so
  a half-built purge can never read as complete. Gate: two-door watermark tests.
- **R2 — single-store age-based range purge (badger + memory).** Chunked,
  idempotent, entity-locked per chunk; removes entity rows + history + all index
  entries + survivor inIdx cleanup; advances the watermark last. `RetentionPolicy
  {Label, Mode: ByAge, Before}`. Gate: oracle arm (a probe below the watermark →
  `ErrRetentionExpired`, never wrong data); adversarial adjacency (edges into
  survivors).
- **R3 — change-log range-purge record + replica re-execution.** The
  `ChangeRangePurge` record family; replica apply re-executes the predicate;
  cross-backend byte-parity of the record; the byte-exact convergence crown
  (incl. differing topology). Gate: ADR-0005 battery extended + convergence crown.
- **R4 — tiered + sharded mapping.** Tiered: a purge whose range covers a whole
  cold event-shard maps to the O(1) `Archive`/shard-drop primitive
  (`g.Tier().Archive` exists) instead of a row scan; a partial-shard range falls
  back to R2's range purge. Sharded (ADR-0007): the range purge runs per-shard in
  parallel (each shard's key range independent), the survivor inIdx cleanup
  respects cross-shard ordering. Gate: tiered shard-drop maps to the SAME logical
  record a single-store range purge emits (a differing-topology replica executes
  it as a range purge).
- **R5 — (optional, later) `ValidTo`-driven expiry (v2).** `Mode: ByValidTo`
  via the temporal interval index; the record format already carries `Mode`.
  Release: its own version after R1–R4 land (owner decision — v1 ships first).

## 5. Interaction with existing subsystems

- **ADR-0001 history compaction.** Compaction trims VERSIONS of live entities and
  writes a stub anchor; retention purges WHOLE entities and writes nothing (they
  are gone below the watermark). Two watermarks coexist: `CompactedThroughTx`
  (per-entity stub boundary) and `retention_watermark/<label>` (per-label purge
  boundary). A read failing either returns the matching sentinel. The
  `ChangeRangePurge` record family is designed so ADR-0001's currently-blocked
  change-log-enabled compaction (`ErrCompactionChangeLogEnabled`) can adopt the
  same re-executable-record mechanism later.
- **ADR-0007 sharded store.** Range purge is the workload the sharded store is
  built for (TB/day ingest). Purge is embarrassingly parallel per shard; the S3
  change-log feed carries the `ChangeRangePurge` record in the one-total-order
  merge. `VerifyConsistency` is the post-purge audit door.
- **Unique constraints (ADR-0002).** A `UniqueForever` value owned by a purged
  entity is released when the entity is purged (the durable owner registry entry
  is reaped in the purge chunk) — else a value would be barred forever by a
  ghost. `UniqueCurrent` needs no action (it consults current state only).

## 6. Risks

1. **Reverse-index cleanup cost** (§2.3) — cleaning survivors' inIdx is the
   expensive, non-range part of an otherwise-sequential purge. Mitigation: measure
   in R2; the lazy-repair sweep is the designed v2 escape hatch, but v1 is
   correct-synchronous (no orphans).
2. **A too-aggressive watermark hides live data.** The watermark is the
   fail-closed boundary; a bug that advances it past un-purged data would surface
   as `ErrRetentionExpired` on live reads (loud), not silent loss — the R1
   fail-closed-first ordering is deliberate so this class is always loud.
3. **Purge vs concurrent ingest of an in-range ID.** Snowflake time is
   monotonic-ish but a backfilled `tkg_tx_from` (§4.1 `AllowTxBackfill`) could
   mint an entity whose age is below a just-advanced watermark. Mitigation: the
   purge predicate is on the SNOWFLAKE-ID time (immutable, mint-ordered), not the
   backfilled TxFrom; backfilled facts below the watermark are rejected at write
   (`ErrRetentionExpired` on create) rather than silently purged later.
4. **Scope.** A full retention subsystem. Stages are sized so each is
   independently green and shippable; R1 (fail-closed plumbing) ships value —
   correct read semantics — before any destructive code exists.
