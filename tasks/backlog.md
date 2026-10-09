# rho-tkg backlog

**Single todo/roadmap file.** Done work is **dropped** from here — `CHANGELOG.md`
is the source of truth for what shipped, why, and measured numbers. Keep only
genuinely open items (and short pointers to closed epics / reopen criteria).

This file tracks only rho-tkg work. External orchestration and product-layer
RPCs that already have local primitives here are out of scope.

**Severity legend:** CRITICAL = crash / data loss / replica divergence / silent
corruption. HIGH = silent wrong answer or reachable correctness bug. MEDIUM =
concurrency edge / perf cliff / contract inconsistency. LOW = smell / doc drift.
TEST-GAP = real behavior unverified (may hide a bug). FEATURE = plausible
capability not yet built. DO-NOT-BUILD = decided against; reopen criteria only.

**Remaining open work:** no CRITICAL items; HIGH: items 12, 14, 18, 19 (item 3 closed 2026-09-24). Open:

0. **Column segments on NVMe (ADR-0011, accepted 2026-09-24)** — FEATURE: steps S0–S7 in `docs/adr/0011-column-segments.md` §6, each with a failing test first and a gate measured at the three synthday sizes; integrity block size configurable (`IntegrityBlockRows`, default 64). Goal: resident memory independent of the day size for declared bulk relationship types (~744 B/rel today). **Progress:** S0 done (baseline harness `bench/segment_baseline_test.go`; memory 718–723 B/rel, badger lean does not reproduce 191 — measures 301), S1 done (codec `pkg/graph/internal/segment`; 22–25 B/HOP on disk; decode of full rows below the memory store's zero-copy scan rate, columns alone 24–28 M rows/s), S2 done on the rho-tkg side (memory store seals declared types into in-RAM segments on a background sealer; `Config.RelSegments`; segments share store-level endpoint and string dictionaries; P6 HOP resident 24.0 / 24.8 / 25.7 B/HOP beyond the memtable, gate ≤ 60 met; legacy 70–71 B/HOP over it because of `support`), S5 done on the rho-tkg side (`ScanRelSegments`, `ScanRelColumns` from segment columns) with the sigma branch `seg/s5-columns` built and tested, not released. **Open from S2/S5:** (a) the ai-soc half of S2's gate — xcheck AGREE on the three synthday days and on BA on Flux with `engine.Open` declaring HOP (ai-soc work, after P6); (b) the per-segment adjacency directory (a node code and two CSR offsets per endpoint per segment, 0.22 → 0.68 B/HOP from 1 to 5 segments) — S4's merge bounds it; (c) a rho-tkg tag carrying S2/S5, then the sigma release that reads it (sigma's `go.mod` still requires v4.38.1); (d) the rel property / temporal indexes and the lazily built belief-watermark and rel-type tx-membership sidecars keep one entry per row when a caller creates or triggers them — check at the ai-soc gate whether ai-soc's queries build them; (e) the row doors on sealed rows stay 3–20× below the zero-copy row store: consumers of bulk types read `ScanRelSegments`. **S3 done on the rho-tkg side (2026-09-25):** `Config.SegmentDir` — self-contained segment files, manifest, crash recovery, mmap reads; Go heap beyond the memtable 0.3–0.7 B/HOP at the three sizes (gate ≤ 1 B/HOP growth met), write median 7.86 s against S2 7.87 s (5 alternating runs each, pinned; gate met). **Open from S3:** (f) the BA day on Flux (masked twin in git) — ai-soc passes `SegmentDir`; (g) per-segment node hashes and dictionaries make files 27–28 B/HOP at 12.6 M (4–5 segments) — S4's merge; (h) Q8 (page cache under cgroup `MemoryMax`) — measure on Flux; (i) a reopen verifies every integrity group (~0.46 M rows/s): fine for a day, a cost for a year of segments. Next: S4.
1. **Import-under-a-scope** (improvement-not-bug, deferred locking)
2. **BACKLOG 22** — six TEST-GAP research items (from the retired `.harden/` ledger)
3. **Temporal adjacency scan cost** (v4.35.0 follow-up) — measure before building anything (see below)
4. **Temporal semantics review 2026-09-24** — traced findings, each needs a failing test before a fix (see below)
5. **P7 row compaction — follow-ups** (MEDIUM, perf; P7 itself landed 2026-09-25, CHANGELOG Unreleased: 608 → 388 B per P6 HOP row, 531 → 401 B per node). Remaining measured terms per P6 HOP row: struct 80, compact metadata 96, properties 128, `rels` 22, `typeIdx` 21, adjacency ~35. Options, none built: (a) property keys as tokens in a frozen row's own representation (−32 B at 4 properties; every property accessor needs a second path, and a process-wide key table must be bounded); (b) compact form for updated / history versions (today only first versions compact; matters for update-heavy workloads — measure one first); (c) the badger entity cache could cache `CompactFrozenCopy` rows too (its budget uses `ApproxHeapBytes`, which already knows the compact size); (d) store-side interning of repeated string value boxes (legacy schema: ~16 B per `actor`; ai-soc P6 already shares boxes, so no consumer needs it now). (The 3.15 M `ByType` gap seen on a loaded host did not reproduce on a quiet one: median 9.07 → 9.56 M rows/s, CHANGELOG.) "Lazy adjacency" was not built: compact sets save always, and a lazy build would come back on the first adjacency read (sigma's hop scans).

6. **Belief endings at a caller instant** (FEATURE, requested by sigma-tkgd's realtime ingest design 2026-10-09): `DeleteWithTx` / `UpdateWithTx` behind `AllowTxBackfill`, so a replayed record can end or supersede belief at its own transaction instant instead of the write clock. Handover with API, semantics, file-level changes and the red tests to write first: `tasks/handover-tx-backfill-delete-update-20261009.md`.

7. **Caller-instant delete of a row with a scheduled close** (FEATURE, follows item 6): `DeleteWithTx(id, t)` refuses when the recorded `ValidTo >= t`, because one tombstone row cannot end belief at `t` and keep the close that pins before `t` believed (decision 2026-10-09, handover §6.11). Lifting it needs a tombstone that keeps the believed `ValidTo` beside the deletion instant (no clamp for a caller instant), with the normalizer (`txtime.go:468-479`) and the valid-time history reads agreeing. Write the two-phase red test first.

8. **Pinned property lookups scale with the history, not the matches** (MEDIUM, perf, found by sigma-tkgd's realtime ingest 2026-10-09): `ByTypeAndProperty`, `ByLabelAndProperty` / `ByLabelAndProperties` and the named `*PropertyAt` / `*PropertyDuring` doors fold every ID with a history row into the candidate set, across all types, and load each chain. At 100 k rels with 20 % revised and 5 % deleted a 200-match pinned lookup takes 20.7 ms against 0.10 ms (memory) and 199 ms against 5.7 ms (badger); sigma saw 1.2-1.5 s against 11 ms at 1 M. Recommended: a K1-style append-only value ever-member sidecar, resolver stays the authority. Handover with cause (file:line), options, tests first and the benchmark target: `tasks/handover-pinned-index-churn-20261009.md`.

9. **WITHDRAWN 2026-10-09 (René, relayed by the ai-soc session: "rho-tkg already has unique snowflake ids")** — property-keyed unique constraint on relationships (ai-soc request 4). ai-soc uses the snowflake ID as the global ID and maps its source record id to it in its own index. Possible cheap follow-up, not requested: a relationship door that accepts a caller-supplied snowflake ID "if absent", mirroring `Nodes().AddByIDIfAbsent` (today `Rels().AddByIDIfAbsent` dedups on the `(start, end, type)` triple, not on a caller ID).

10. **Tiered store: composite indexes and relationship temporal indexes** (FEATURE, ai-soc request 3; DECIDED YES 2026-10-09, René, relayed by the ai-soc session; ai-soc keeps the tiered store for weeks of retention). Today tiered declines both: no `RelTypeTemporalIndexCapability` (`core/store_capabilities.go:80-85`, `store/tiered/temporal_index_listing.go:24-31`) and no `CompositeIndexCapability` (`store/tiered/index_introspection.go:8-17`); the comment at `pkg/graph/index/api.go:267-269` wrongly lists sharded as declining (sharded implements rel temporal indexes, `store/sharded/reltype_temporal_index.go:16-30`). Design points: per-shard indexes fanned out by the tiered store (as sharded does) vs one store-level index; rotation hot→warm→cold and cold demotion must move or rebuild the entries (TEST-GAP 22b adjacent); the rel temporal index keeps one entry per row (backlog S2/S5 note (d)) so the budget on a week of raw edges must be measured first. Red tests: two-phase over rotation, cold checkout, repair; parity with badger's answers on the same data. Scheduling: René.

11. **Durable-on-return commit** (FEATURE, ai-soc request 9; DECIDED YES 2026-10-09, René, relayed by the ai-soc session): `GraphTx.Commit` and `Batch.Execute` flush the badger pending buffer before returning, one WriteBatch per call, so a crash can lose only whole uncommitted groups and `SyncWrites` is not needed per mutation. Today neither flushes (`core/tx.go`, `core/batch_execute.go`); the pending buffer is a map flushed in map order (`store/badger/badgerstore_flush.go:166-176`) and badger splits an oversized WriteBatch into several internal transactions, so without `SyncWrites` a crash can persist an arbitrary subset of a group. Design points: opt-in (`Config` flag or a commit option) vs default; the tiered and sharded stores flush every shard the group touched; a group above badger's transaction size limit is still split — document or refuse; an all-or-nothing on-disk guarantee stays v5 (PLAN §5.2). Red tests: crash-shaped (SIGKILL or a closed-without-flush store) after Commit returns shows the whole group; after a crash before Commit either the whole group or none is on disk is NOT promised and the test says so. Scheduling: René.

12. **HIGH: unique constraint bypass via SetNodeVersionInterval props (standalone, GraphTx, Batch, Session)** — found in review 2026-10-09. `cascadeNodeVersionInterval` (`core/temporal_cascade.go:81`) appends rows from a props PATCH and never consults `enforceUniqueForNode` (`core/unique_constraints.go:561`), so a cascade can give a node a value another current node already holds under `CreateUnique` / `CreateUniqueForever`, through every door that forwards to it: `Temporal().SetNodeVersionInterval`, `GraphTx.SetNodeVersionInterval` (`core/tx_mutations.go:293`), `BatchBuilder.SetNodeVersionInterval` (`core/batch_queue.go:384`, applied at `core/batch_execute.go:564`) and `Session.SetNodeVersionInterval` (`core/ingest_interval_doors.go`, strong and concurrent apply). Decide first whether a unique value is judged on the current row only (then a cascade that does not change the current row's value is fine) or on every valid-time slice it writes. Red test first: two-phase, on all four doors and all four backends — `CreateUnique(label, key)`, node A holds `v`, node B holds `w`; a cascade on B whose props patch sets `key=v` must be refused with `ErrUniqueViolation` (errors.Is) and leave B's history and `NodeAtTx` unchanged; counterpart: a patch to a free value succeeds; `UniqueForever`: a patch to a value owned forever by another node is refused. Until fixed, `CreateUnique`'s doc comment states that `SetNodeVersionInterval` is not checked. Scheduling: next.

13. **Ingest applier attributes group errors by numeric entity id, not by kind** (MEDIUM, found 2026-10-09 writing `TestSessionSetVersionInterval_FailedGroupShape`): the strong-async applier keys group-error attribution by the numeric id, so a missing node id and a missing rel id with the same number attribute errors to the wrong group. Minted snowflake ids never collide across the two generators, but caller-supplied ids (`AddByID`, `Import`) can (the rollback-snapshot rule in AGENTS.md "Data Model" already keys by kind + id for the same reason). Red test first: two groups, a node and a rel with the same numeric id, each missing; each group gets its own sentinel. Fix: key by (kind, id).

14. **A correction on a deleted entity copies the tombstone stamps** (HIGH, pre-existing, found in the
    review of the one-tick fix 2026-10-09; the reviewer confirmed it on memory, badger, sharded and
    tiered). `correctionTemporal` (`core/temporal_cascade.go:486-496`) starts every correction piece
    from the template's whole `TemporalMetadata`, `TxTo` and `DeletedAt` included (node `:516`, rel
    `:546`), and the resumption rows (`:185`/`:195` node, `:654`/`:664` rel) keep the source row's
    `TxTo`/`DeletedAt`. On a hard-deleted entity the template is the tombstone, so `Add` → `Delete` →
    `SetNodeVersionInterval` appends a row with `TxTo = TxFrom - 1` and a `DeletedAt`:
    `NodeAsOf(now)` reports the entity absent while `NodeAtTx(t, now)` returns the correction (two
    doors disagree). Red test first, all four backends, node and rel: every appended row has
    `TxTo == 0 || TxTo >= TxFrom`, and `NodeAsOf` / `NodeAtTx` (and rel mirrors) agree after the
    cascade. **Decision needed (lesson 46):** a cascade on a deleted entity either undeletes (the
    appended rows are a new, live belief) or is refused with a sentinel error. Not implemented.
    `TestCascadeTemplate_DeletedEntityUsesNewestHistoryRow` pins today's behaviour (the cascade
    succeeds, gap content from the newest history row); revise it with the decision.
    **Second trigger (W5 oracle, 2026-10-09, confirmed by review):** a live, updated-and-closed entity: `SetNodeVersionInterval` writes rows with `TxTo < TxFrom`, the current row then carries a `TxTo`, and the next Update keeps it. No decision needed for this trigger: an appended row's `TxTo` must be 0.

16. **Range folds under an interval: predicate-anywhere or value at the resolved version?** (MEDIUM, contract inconsistency, found reviewing item C scan-temporal-opts 2026-10-09). Decide whether all range folds (ordered included) pass the range into the resolver's predicate (predicate-anywhere) like `ByLabelAndProperty`. Today, under `ValidStart`+`ValidEnd`, `Nodes().ForEachByLabelPropertyRange` / `Rels().ForEachByTypePropertyRange` and the ordered / prefix siblings (`forEachNodeInRangeTemporal`, `forEachNodeValueOrderedTemporal` and rel mirrors) test the range on the ONE version `ByLabel` / `ByType` resolve (the most recent overlapping one), while `ByLabelAndProperty` / `ByTypeAndProperty` pass the property into `findNodeVersionForOpts`'s predicate and match a value held anywhere in the interval (`graph_property_query.go:87-95`). A value in range only during an earlier part of the interval is found by the equality door and missed by the range doors. Point opts (`ValidAt`, `TxAt`, `TxPin`) agree. If predicate-anywhere is chosen: the selecting predicate must be EXACT (an over-selecting one could pick a version fn then rejects while an earlier one matched), and the ordered doors sort on the value of the matching version. Red test first: rule-16 shape (value in range on the earlier version only, interval spanning both) across all range doors.

15. **Durable commit: power-loss gap on a memtable switch** (MEDIUM, follows item 11, found by the item-11 review 2026-10-09): `Config.DurableCommit`'s `DurableFlush` ends with badger `db.Sync()`, which fsyncs only the active memtable's WAL and the current value-log file (badger v4.9.2 `db.go:705-709`). A flush that fills the memtable switches to a new one in `ensureRoomForWrite` (`db.go:1015-1040`) without syncing the retired WAL, and a finished value-log file is synced only under `SyncWrites` (`memtable.go:408`), so rows of a group that crossed a switch can be unsynced after `DurableFlush` returned: process-crash safe, not power-loss safe (documented on `Config.DurableCommit`, `store.DurableFlushCapability`, docs/architecture.md "Durable-on-return commit"). Options: (a) sync the retired memtable WAL (and the finished vlog file) on switch when a durable flush is in progress — needs a badger hook or a fork; (b) measure `SyncWrites` cost against `DurableCommit` on a group workload and recommend it for strict durability. Red test first: a group larger than one memtable, a power-loss-shaped check (fsync accounting or a dm-flakey/`LD_PRELOAD` drop of unsynced pages). Scheduling: René.

**DECIDED NO 2026-10-09 (René, relayed by the ai-soc session):** an index-provider "failure aborts the mutation" option (ai-soc request 5; ai-soc rebuilds from the change feed) and one instant per commit group (ai-soc request 7; the cut-record pin is enough). Reopen only with a consumer case the change feed or the cut-record pin cannot cover.

18. **Cascade row versions collide with later Updates** (HIGH, pre-existing at v4.43.0, reproduced by the W5 oracle on all four backends, confirmed by review 2026-10-09): a bounded `SetVersionInterval` gives its appended row `maxVersion+1` (`core/temporal_cascade.go:119-128`) while the current row stays current with a lower version. (a) A later plain Update changes `NodeAsOf` at an EARLIER pin (v1 → v2), because the as-of resolver selects the newest VERSION with TxFrom ≤ pin, and after the Update a higher-version row exists whose TxFrom lies at or below that pin's reading of the chain (lesson 62's "newest is by version" trap, now in the other direction). (b) The next Update or CloseVersion uses `current.Version()+1` (`core/node_update.go:175-176`), colliding with the cascade row's version, so the chain holds two rows with one version; a later Delete then changes the TxAt answer at an earlier pin (v1 → absent). Red tests first (two-phase, all four backends, node and rel): as-of and TxAt answers at a pin taken before the later Update/Delete are unchanged after it. Fix direction: one version allocator for every appended row (cascade included) and a resolver that does not equate "newest version" with "newest belief" when versions interleave with cascades. The W5 oracle skips these fingerprints (`txbTangled`); remove the skip with the fix.

19. **GraphTx rollback drops the higher-version cascade row** (HIGH, pre-existing at v4.43.0, memory and badger; sharded and tiered keep it; confirmed by review 2026-10-09): `snapshotCurrentNodeLocked` / `snapshotCurrentRelLocked` take the trim path with `historyTrimFrom = current.Version()` (`core/tx.go:310-321` node, `:357-369` rel), and Rollback then trims every history row at or above that version, including a cascade row with a higher version. Rollback after `tx.UpdateNode`/`tx.DeleteNode`/`tx.UpdateRelationship`/`tx.DeleteRelationship` drops history 2 → 1; a REFUSED `tx.UpdateNodeWithTx`/`UpdateRelationshipWithTx` does the same because it refuses after the snapshot (a refused `DeleteRelationshipWithTx` does not). Red tests first (memory and badger, node and rel): history count and the pinned answers before and after the rollback are equal. Fix: use the full-copy snapshot path when any history row has a version above `current.Version()`; run `check*CallerUpdate` before the snapshot in the GraphTx `Update*WithTx` twins. The W5 oracle skips this (`txbNoTxRollback`); remove the skip with the fix.

**v5:** the next engine generation is planned on branch `v5` (`docs/v5/PLAN.md`, `RESEARCH-REVIEW.md`, `DISCUSSION.md` on branch `v5`, revised 2026-10-09 after `tasks/review-v5-plan-vs-code-20261009.md`; branch created 2026-10-09 from main `32568c4`). v4 keeps taking consumer features (decision René 2026-10-09, the earlier fixes-only rule is deleted). ADR-0011 S2+ waited for owner decision D6 in that plan. **DECIDED 2026-09-25 (René): S2 and S5 ship on v4 as v4.39.0**, because the AI-SOC cross-check needs them now (ADR-0011 §6 gate met: 24.0 / 24.8 / 25.7 B/HOP at 790 K / 3.15 M / 12.6 M); v5 carries the segments forward from this code.

---

## Open — temporal semantics review 2026-09-24

Found by a code trace during the v1.3-handoff review. The three findings that were
reproduced (delete after close, correction template, endpoint masking) are fixed on
branch `fix/v4-temporal-semantics`. The items below are TRACED, NOT YET REPRODUCED:
write the failing two-phase test first; drop the item if the test passes.

- **(HIGH?) `NodesDuring` / `RelsDuring` open end.** `end == 0` is resolved to now + 1
  (`c.resolveOpenEndInstant`, `temporal.go`; now = `c.readNow()` since 2026-09-24, so a
  version stamped ahead of the wall is no longer missed), so an entity valid only in the
  future is missed; `NodesRelating` keeps the open end as +inf.
- **(HIGH?) Future transaction time from `validInstantAfter`.** Update/CloseVersion/Delete of a
  row whose explicit ValidFrom is in the future stamps `TxFrom/TxTo = ValidFrom + 1` without
  advancing the floor, contradicting "every committed entity has TxFrom <= NowTx()"
  (`txtime.go` ~L159) and SPEC.md ~L439.
- **(HIGH?) Re-import of a deleted ID.** `Nodes().Import(id, …)` checks only the current row;
  the re-imported entity restarts at version 0 and its first Update writes history version 0,
  overwriting the previous life's version 0 (`memorystore_history.go` ~L888).
- **(MEDIUM?) As-of vs point resolver order.** `SelectAsOf` picks the newest candidate by
  version; the point resolver breaks overlaps by (TxFrom, version). After a cascade whose rows
  carry a lower TxFrom than a later-version Update, `NodeAsOf` and `NodeAtTx` can pick
  different rows (`asof_select_test.go` ~L119-129 shows the inversion shape).
- **(MEDIUM?) `NodeMatchesValidTime` on a current row with unset ValidFrom** answers "valid since
  mint", so a consumer post-filtering current rows accepts today's properties for times
  before the last update, where `NodeAt` returns the older version.
- **CLOSED 2026-09-24 — TxAt-only doors "nondeterministic"** (seg/s2 backlog "item 4", later item 5, found
  by the S2 oracle). Not map order: the implicit valid-time "now" was the wall clock, below
  the stamps of versions written while the transaction clock ran ahead. Fixed on
  `fix/txat-determinism` (CHANGELOG Unreleased); seg/s2 dropped `segKnownNondeterministic`
  and its item after merging v4.38.1: the S2 oracle compares both doors again.
- **(LOW) Snowflake ID horizon.** 48-bit microseconds from 2026-01-01 end on 2034-12-02
  (arithmetic). Needs a plan before v4 data outlives it; v5 drops clock bits from IDs.
- **(KNOWN LIMITATION) Future-scheduled close, then delete.** The delete clamps the scheduled
  `ValidTo` in place, so a read pinned before the delete sees the row open-ended. Fixing it
  needs either a separate tombstone version (store delete contract and chain shape change) or
  readers using `min(ValidTo, DeletedAt)` everywhere (column scans, segments, valid-time
  indexes). v5 replaces in-place tombstones with lifecycle closes.

---

## Open — import-under-a-scope

Carried over from the retired `tasks/todo.md` (`4f71fc3`).

- **Import-under-a-scope** (the proper fix beyond the import stopgap): wrap
  `IO().Import` in a `TxChangeLogScope` so a FAILED import emits NOTHING, not just
  "no poison". Import emits change-log records IN-BACKEND eagerly, outside any
  scope, so `importRollback.restoreRegistries` is still gated on
  `changeLogEnabled` — the append-only stopgap — rather than de-allocating tokens
  exactly. Needs import to also take `c.txMu` (it takes only `c.mu.Lock` today, so
  a between-mutations open tx scope would collide with import's `BeginLogScope`);
  a locking change, deferred. The current stopgap is correct — see
  `tasks/lessons.md` 55.

  **Measured 2026-08-05** (`BenchmarkImport_ChangeLogCost`, 5,000 nodes + 5,000
  rels, variance <1%):

  | | time | allocs |
  |---|---|---|
  | ChangeLog off | 216ms | 1.4M |
  | ChangeLog on | 621ms | 7.0M |

  The STOPGAP itself costs nothing: `restoreRegistries` is reached only from
  `rollback()`, i.e. only on a FAILED import, and its change-log branch is an early
  `return nil` — cheaper, not dearer. Nothing on the success path touches it.

  The eager emission the stopgap works around costs 2.9x, so the prize is real —
  but a SECOND BLOCKER, not previously recorded here, is that the fix may not
  collect it. A scope BUFFERS records in memory (badger: `bs.scopeLog`, an
  unbounded `[][]byte` released only at commit). A transaction is small; an import
  is the whole graph, and bootstrap-importing into a change-log-enabled store is
  exactly the case that would hold every record at once. So the item trades a 2.9x
  time cost for a memory cost proportional to the entire import, and wants a
  spill/chunk story before the locking change is even worth planning.

---

## Open — BACKLOG 22: adversarial / soak research park

Rescued 2026-08-05 when the June 2026 `.harden/coverage.md` ledger was deleted.
**TEST-GAP / research**, not known defects. The wire-decode and import-amplification
bugs that ledger found shipped as v4.9.2 / v4.9.3. These six were the "attack next"
remainder and were never numbered into BACKLOG 6–21. Partial later coverage is
noted per item.

Prioritize only when a consumer needs the confidence or when touching the named
subsystem.

### 22a. [TEST-GAP] Tiered crash-fault injection between cross-shard writes

Process-kill between cross-shard split-writes (E→R / R→E), mid-flush, mid-cascade.
Happy-path and a few residue paths exist (`tieredstore_write_rel_crash_residue_test.go`,
`tiered_registry_crash_test.go`); no systematic fault-injection matrix that kills at
each ordering step and asserts reopen + `RunRepair` / rollback leave a consistent
neighborhood.

### 22b. [TEST-GAP] Clock-skew vs hot→warm rotation and cold demotion

Rotation / cold demotion / `ShardWindow` edges under non-monotonic or skewed wall
clock (tests must not use sub-millisecond windows). Atomic catalog rotation tests
exist; deliberate clock jumps across window boundaries do not.

### 22c. [TEST-GAP] Fuzz tiered metadata decoders independently

Tiered catalog / registry / temporal-index / vector-index definition files
(`registry_file.go`, `temporal_index_file.go`, `vector_index_file.go`, catalog
JSON) are not fuzzed. Badger meta goes through `SafeUnmarshal` but is not
independently fuzzed. Goal: native Go fuzz + committed `testdata/fuzz/` crashers,
same discipline as `FuzzWireTo*Checked` / `FuzzImport`.

### 22d. [TEST-GAP] Property / vector index queries under adversarial values

NaN / ±Inf / huge-dim / mixed-type values on **query** paths (not only write
validation). Partial coverage exists (dim-mismatch sentinels, some type-class
handling, vector tests); not an exhaustive battery or fuzz on the query door.

### 22e. [TEST-GAP] Long-running concurrency soak under the race detector

Mixed standalone + tx + batch + concurrent ingest for minutes under `-race`,
beyond targeted race tests. Opt-in / manual (or a long CI job) — not `make test`
short mode.

### 22f. [TEST-GAP] Resource-exhaustion cliffs on huge-but-valid inputs

Max labels / properties / containers / blobs at `ValidationLimits` ceilings —
latency/alloc cliffs. Valid inputs must not OOM or hang; fail closed with clear
sentinels where a limit exists.

---

## Open — temporal adjacency scan cost (v4.35.0 follow-up)

`Rels().ForEachAdjacentRelAt` / `ForEachAdjacentEndpointAt` under a valid-time
filter now resolve relationship VERSIONS (`forEachAdjacentRelVersionLocked`,
CHANGELOG 4.35.0). Two costs were accepted for correctness and are UNMEASURED:

- the badger inline-stamp decode-skip (OPT15) is bypassed under a filter, so
  every adjacent row is decoded and rows whose live version fails the filter
  pay one chain read;
- the deleted-rel fold (`forEachRelAdjacencyCandidateID`) is O(deleted rels)
  per call — the same as `OutgoingRelsAt`, but a consumer's hop expansion
  calls the scan door once per source node, so a graph with many deleted
  edges pays it per node per hop.

**Next action:** measure first, on the consumer's temporal hop benchmarks
(sigma-tkgd `stream_spine` / count-reach fast paths) with a graph that has a
deleted-edge population, before building anything. If the fold shows, the
mechanism is a per-node deleted-adjacency index (deleted rel ids keyed by
endpoint) so the candidate set becomes O(degree + deleted-at-this-node). If
the decode shows, re-admit the stamp as a candidate PRUNER only: a stamp that
passes yields the live row, a stamp that fails still resolves the chain.

---

## Not tracked here (cross-team)

Consumer builds these; rho-tkg already exposes the local primitives:

- START→END foreign-stub-delete fan-out (BACKLOG 2 Inc 4c)
- Consumer-gated constraint dry-run (HP2.5)

When consumer pins a shape that needs a **new** rho-tkg primitive, it re-enters
**Open** as a concrete item.

---

## Closed (pointers only — detail in CHANGELOG)

| Epic | Where it landed |
|------|-----------------|
| BACKLOG 1 — Retention purge (ex-ADR-0008 R2–R5) | CHANGELOG (4.18–4.24 era) |
| BACKLOG 2 — Cross-machine Model A (ADR-0010 §3.3) | CHANGELOG |
| BACKLOG 3 — Columnar / streaming whole-node fetch | CHANGELOG |
| BACKLOG 4 — Review adaptations (4b–4e; **4a DO NOT BUILD**) | CHANGELOG |
| BACKLOG 5 — Rel ordering-soundness (`RelRangeCardinality`, type-class) | CHANGELOG |
| BACKLOG 6–21 — full-library hardening (~196 findings) | closed 2026-07-18…22, `[4.24.0]` |
| Last HIGH (10b cascade/resumption) + 10c perf follow-up | `[4.24.0]` / next-day fix |
| Consumer-gated ask batch (5 items, 2026-07-29) | `[4.25.0]` same day |
| CI bench-gate (blocking) | 2026-07-29, `bench.yml` |
| Item 3 (HIGH) — entity wire widened nested values (small ints, typed slices, typed nil, custom structs); DECIDED 2026-09-24: kind envelope, no write-time normalization | CHANGELOG `[4.37.0]` Fixed |
| HIGH — badger reads dropped or replaced rows when a flush + eviction landed mid-read (scans, `NodesAsOf`/`RelsAsOf`, point-read cache fills); flush epoch + `scanSnapshot` + `LoadCleanAt`, 2026-09-24 | CHANGELOG `[Unreleased]` Fixed |
| HIGH — one-tick `[t, t+1)` rows skipped as the eclipse sentinel (ex-KNOWN LIMITATION "1 ms pieces look eclipsed"; CloseVersion at vf+1 lost, delete at vf+1 hid history, width-1 cascade piece invisible); skip removed 2026-10-09 | CHANGELOG `[Unreleased]` Fixed "One-tick valid intervals are ordinary spans" |

Recover closed investigation prose via `git log --all -- tasks/backlog.md` if needed.

### DO NOT BUILD / reopen criteria (keep here so they are not re-filed)

- **Per-version temporal-envelope prune (ex-BACKLOG 4a):** owner-decided net-negative
  for the confirmed workload. Detail in CHANGELOG.
- **Wire `fv` bump** (widen temporal tail to full envelope + eclipsed-row flag) **and
  inverted-suffix history keyspace:** DECIDED NOT BUILT 2026-07-29 after no-format-change
  paths shipped (tail-peek TX; selection-skeleton + zero-alloc token scanner VT).
  **Reopen only if** consumer re-runs depth oracles against current HEAD **and** residual
  depth-linearity still breaks a concrete consumer latency budget — then the prepared
  v3 design is the next step.
- **Zone maps before I/O (columnar phase-2 ZM):** withdrawn 2026-08-04; envelope prune +
  in-RAM `BlockCanMatch` cover the need. Reopen only if a consumer needs a block-level
  pre-I/O prune that those cannot express without breaking full-membership snapshots
  (see CHANGELOG `[4.27.0]`).
