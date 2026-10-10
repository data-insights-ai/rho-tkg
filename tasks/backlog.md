# rho-tkg backlog

**Single todo/roadmap file.** Done work is **dropped** from here — `CHANGELOG.md`
is the source of truth for what shipped, why, and measured numbers. Keep only
genuinely open items (and short pointers to closed epics / reopen criteria).
Item numbers are stable (code comments cite them); a closed item keeps its number in the Closed table.

This file tracks only rho-tkg work. External orchestration and product-layer
RPCs that already have local primitives here are out of scope.

**Severity legend:** CRITICAL = crash / data loss / replica divergence / silent
corruption. HIGH = silent wrong answer or reachable correctness bug. MEDIUM =
concurrency edge / perf cliff / contract inconsistency. LOW = smell / doc drift.
TEST-GAP = real behavior unverified (may hide a bug). FEATURE = plausible
capability not yet built. DO-NOT-BUILD = decided against; reopen criteria only.

**Remaining open work (verified against the code at v4.49.0, 2026-10-10):** no reproduced CRITICAL or HIGH
item; two traced, unreproduced HIGH? findings in item 4. Next step: item 43 (retraction), then 34, 33 + 42 (+ 41),
35, 28, 21. Open items, one line each (full text below):

| # | Type / severity | Item | Status |
|---|---|---|---|
| 0 | FEATURE | Column segments on NVMe (ADR-0011) | S0-S3, S5 done (S2/S5 in v4.39.0, S3 + P7 in v4.40.0); S4 merge, S6, S7 and sub-points (a), (b), (d)-(i) open |
| 1 | MEDIUM (improvement, deferred locking) | Import-under-a-scope | open; `IO().Import` still takes only `c.mu` (no `c.txMu`), stopgap in place |
| 2 | TEST-GAP | Old BACKLOG 22: six TEST-GAP research items (22a-22f) | open; prioritise only when a consumer needs the confidence |
| 3 | MEDIUM (perf, unmeasured) | Temporal adjacency scan cost (v4.35.0 follow-up) | open; measure first |
| 4 | HIGH? / MEDIUM? (traced, not reproduced) | Temporal semantics review 2026-09-24: traced findings | open; each needs a failing two-phase test first; code unchanged at the cited sites (`resolveOpenEndInstant`, `validInstantAfter` in `core/temporal.go`) |
| 5 | MEDIUM (perf) | P7 row compaction follow-ups | open; options (a)-(d) unbuilt (P7 itself shipped in v4.40.0) |
| 7 | FEATURE (limitation) | Caller-instant delete of a row with a scheduled close | open; `DeleteWithTx` refuses `ValidTo >= t` with `ErrTxOrder` (R8, v4.44.0) |
| 10 | FEATURE | Tiered store: remaining work after composite + rel temporal indexes | indexes shipped in v4.45.0; (a) sequential-scan rebuild and (b) rel property indexes / node `TemporalCandidateCapability` on tiered open |
| 13 | MEDIUM | Ingest applier attributes group errors by numeric entity id, not by kind | open; `groupApplyError` still keys `idToGroup` by `types.EntityID` (`core/ingest.go`) |
| 15 | MEDIUM | Durable commit: power-loss gap on a memtable switch | open; documented on `Config.DurableCommit` (v4.44.0); scheduling: René |
| 16 | MEDIUM (contract) | Range folds under an interval: predicate-anywhere or value at the resolved version? | open; decision pending; behaviour documented in the v4.44.0 CHANGELOG |
| 20 | LOW (TEST-GAP, perf) | Effective-read cost: RelAtTx benchmark targets unmeasured | fixes 1a (v4.44.1), 1b (v4.46.0), 1c (v4.47.0), 2a (v4.46.0) shipped; only the RelAtTx benchmarks remain |
| 21 | FEATURE | Purge relationships by age, gate retention reads per type | open; no `PurgeExpiredRels` in the tree; sigma: not blocking until `@overview` starts |
| 22 | MEDIUM (not reproduced) | Badger rel temporal index create races concurrent writes | open; `CreateRelTemporalIndex` still uses the snapshot / unlocked build / install shape |
| 23 | LOW (perf, unmeasured) | `changeFeedPage` prefetches past its page | open; `PrefetchValues = true`, no `opts.Prefix` (`badgerstore_changelog.go:648-659`) |
| 25 | MEDIUM | A future close followed by a delete changes the answer at an earlier pin | open; `stampDeleteTombstone` still clamps (`core/temporal.go:133-137`) |
| 26 | LOW (known limit) | Old chains with collided versions keep answering in the old version order | open; schedule only if a consumer holds such chains |
| 28 | FEATURE (perf) | State form of the as-of column doors and counts | open; design first (sigma) |
| 31 | LOW | Bulk and point as-of disagree with the cascade oracle on version gaps | open; fix when a consumer holds such chains |
| 33 | MEDIUM (perf) | Badger effective-timeline scan costs 13-17 µs per relationship | open; restart pending (baseline `tasks/evidence/badger-read-cost/`) |
| 34 | FEATURE | `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx` | open; agent stopped before committing anything; restart (sigma replay need) |
| 35 | FEATURE | Broader effective scans | open; signatures confirmed by sigma 2026-10-10 (HANDOVER.md §11) |
| 37 | LOW | Tiered error-rollback restore leaves a window with empty history | open; `restoreRelHistorySnapshot` still truncates then puts (`tieredstore_write_history.go:899`) |
| 39 | LOW (not tested) | Per-entity compaction stubs survive `Admin().Reset` | open; `reapCompactionForReset` still leaves the stubs (`core/compaction.go:285`) |
| 40 | LOW (cosmetic) | A cascade resumption may end at an earlier life's own bound | open |
| 41 | MEDIUM (RAM) | Merge the history stamps sidecar into the presence set | open; after 42 |
| 42 | FEATURE (perf) | A current-row stamps capability | open; restart with 33 |
| 43 | FEATURE — NEXT STEP | Retraction: a way to say "this was never true" | open, not built (the agent was stopped before committing anything); spec below |
| 44 | MEDIUM (traced, not reproduced) | Commit-clock floor persisted only at Close | open; filed 2026-10-10 from `tasks/review-v5-plan-vs-code-20261009.md` §6.5 |
| 45 | MEDIUM (API) | Internal sentinels reach callers but have no exported alias; `ErrMixedNumericColumn` is outside the errors inventory test | open; found by the doc review of `docs/errors.md` 2026-10-10 |
| 46 | LOW (stale comments) | Code comments that contradict the code or cite ADRs no longer in `docs/adr/` | open; found by the doc reviews 2026-10-10 |
| 47 | LOW (unverified claims) | `NodesByLabelAt` vs K1; a RAM budget for the property sidecars | open; each needs a real check before it becomes work |

---

## Open

### 0. Column segments on NVMe (ADR-0011)

*FEATURE* — S0-S3, S5 done (S2/S5 in v4.39.0, S3 + P7 in v4.40.0); S4 merge, S6, S7 and sub-points (a), (b), (d)-(i) open.

**Column segments on NVMe (ADR-0011, accepted 2026-09-24)** — FEATURE: steps S0–S7 in `docs/adr/0011-column-segments.md` §6, each with a failing test first and a gate measured at the three synthday sizes; integrity block size configurable (`IntegrityBlockRows`, default 64). Goal: resident memory independent of the day size for declared bulk relationship types (~744 B/rel today). **Progress:** S0 done (baseline harness `bench/segment_baseline_test.go`; memory 718–723 B/rel, badger lean does not reproduce 191 — measures 301), S1 done (codec `pkg/graph/internal/segment`; 22–25 B/HOP on disk; decode of full rows below the memory store's zero-copy scan rate, columns alone 24–28 M rows/s), S2 done on the rho-tkg side (memory store seals declared types into in-RAM segments on a background sealer; `Config.RelSegments`; segments share store-level endpoint and string dictionaries; P6 HOP resident 24.0 / 24.8 / 25.7 B/HOP beyond the memtable, gate ≤ 60 met; legacy 70–71 B/HOP over it because of `support`), S5 done on the rho-tkg side (`ScanRelSegments`, `ScanRelColumns` from segment columns) with the sigma branch `seg/s5-columns` built and tested, not released. **Open from S2/S5:** (a) the ai-soc half of S2's gate — xcheck AGREE on the three synthday days and on BA on Flux with `engine.Open` declaring HOP (ai-soc work, after P6); (b) the per-segment adjacency directory (a node code and two CSR offsets per endpoint per segment, 0.22 → 0.68 B/HOP from 1 to 5 segments) — S4's merge bounds it; (c) DONE: v4.39.0 carries S2/S5 and sigma-tkgd pins the latest tag; (d) the rel property / temporal indexes and the lazily built belief-watermark and rel-type tx-membership sidecars keep one entry per row when a caller creates or triggers them — check at the ai-soc gate whether ai-soc's queries build them; (e) the row doors on sealed rows stay 3–20× below the zero-copy row store: consumers of bulk types read `ScanRelSegments`. **S3 done on the rho-tkg side (2026-09-25):** `Config.SegmentDir` — self-contained segment files, manifest, crash recovery, mmap reads; Go heap beyond the memtable 0.3–0.7 B/HOP at the three sizes (gate ≤ 1 B/HOP growth met), write median 7.86 s against S2 7.87 s (5 alternating runs each, pinned; gate met). **Open from S3:** (f) the BA day on Flux (masked twin in git) — ai-soc passes `SegmentDir`; (g) per-segment node hashes and dictionaries make files 27–28 B/HOP at 12.6 M (4–5 segments) — S4's merge; (h) Q8 (page cache under cgroup `MemoryMax`) — measure on Flux; (i) a reopen verifies every integrity group (~0.46 M rows/s): fine for a day, a cost for a year of segments. Next: S4.

### 1. Import-under-a-scope

*MEDIUM (improvement, deferred locking)* — open; `IO().Import` still takes only `c.mu` (no `c.txMu`), stopgap in place.

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

### 2. Old BACKLOG 22: six TEST-GAP research items (22a-22f)

*TEST-GAP* — open; prioritise only when a consumer needs the confidence.

Rescued 2026-08-05 when the June 2026 `.harden/coverage.md` ledger was deleted.
**TEST-GAP / research**, not known defects. The wire-decode and import-amplification
bugs that ledger found shipped as v4.9.2 / v4.9.3. These six were the "attack next"
remainder and were never numbered into BACKLOG 6–21. Partial later coverage is
noted per item.

Prioritize only when a consumer needs the confidence or when touching the named
subsystem.

#### 22a. [TEST-GAP] Tiered crash-fault injection between cross-shard writes

Process-kill between cross-shard split-writes (E→R / R→E), mid-flush, mid-cascade.
Happy-path and a few residue paths exist (`tieredstore_write_rel_crash_residue_test.go`,
`tiered_registry_crash_test.go`); no systematic fault-injection matrix that kills at
each ordering step and asserts reopen + `RunRepair` / rollback leave a consistent
neighborhood.

#### 22b. [TEST-GAP] Clock-skew vs hot→warm rotation and cold demotion

Rotation / cold demotion / `ShardWindow` edges under non-monotonic or skewed wall
clock (tests must not use sub-millisecond windows). Atomic catalog rotation tests
exist; deliberate clock jumps across window boundaries do not.

#### 22c. [TEST-GAP] Fuzz tiered metadata decoders independently

Tiered catalog / registry / temporal-index / vector-index definition files
(`registry_file.go`, `temporal_index_file.go`, `vector_index_file.go`, catalog
JSON) are not fuzzed. Badger meta goes through `SafeUnmarshal` but is not
independently fuzzed. Goal: native Go fuzz + committed `testdata/fuzz/` crashers,
same discipline as `FuzzWireTo*Checked` / `FuzzImport`.

#### 22d. [TEST-GAP] Property / vector index queries under adversarial values

NaN / ±Inf / huge-dim / mixed-type values on **query** paths (not only write
validation). Partial coverage exists (dim-mismatch sentinels, some type-class
handling, vector tests); not an exhaustive battery or fuzz on the query door.

#### 22e. [TEST-GAP] Long-running concurrency soak under the race detector

Mixed standalone + tx + batch + concurrent ingest for minutes under `-race`,
beyond targeted race tests. Opt-in / manual (or a long CI job) — not `make test`
short mode.

#### 22f. [TEST-GAP] Resource-exhaustion cliffs on huge-but-valid inputs

Max labels / properties / containers / blobs at `ValidationLimits` ceilings —
latency/alloc cliffs. Valid inputs must not OOM or hang; fail closed with clear
sentinels where a limit exists.

### 3. Temporal adjacency scan cost (v4.35.0 follow-up)

*MEDIUM (perf, unmeasured)* — open; measure first.

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

### 4. Temporal semantics review 2026-09-24: traced findings

*HIGH? / MEDIUM? (traced, not reproduced)* — open; each needs a failing two-phase test first; code unchanged at the cited sites (`resolveOpenEndInstant`, `validInstantAfter` in `core/temporal.go`).

Found by a code trace during the v1.3-handoff review. The three reproduced findings (delete after close,
correction template, endpoint masking) shipped in v4.38.0 (merge `4473705` of `fix/v4-temporal-semantics`).
The items below are TRACED, NOT YET REPRODUCED: write the failing two-phase test first; drop the item if the
test passes.

- **(HIGH?) `NodesDuring` / `RelsDuring` open end.** `end == 0` is resolved to now + 1
  (`c.resolveOpenEndInstant`, `temporal.go`; now = `c.readNow()` since 2026-09-24, so a
  version stamped ahead of the wall is no longer missed), so an entity valid only in the
  future is missed; `NodesRelating` keeps the open end as +inf.
- **(HIGH?) Future transaction time from `validInstantAfter`.** Update/CloseVersion/Delete of a
  row whose explicit ValidFrom is in the future stamps `TxFrom/TxTo = ValidFrom + 1` without
  advancing the floor, contradicting "every committed entity has TxFrom <= NowTx()"
  (`txtime.go` ~L159) and SPEC.md ~L439.
- **(MEDIUM?) As-of vs point resolver order.** `SelectAsOf` picks the newest candidate by
  version; the point resolver breaks overlaps by (TxFrom, version). After a cascade whose rows
  carry a lower TxFrom than a later-version Update, `NodeAsOf` and `NodeAtTx` can pick
  different rows (`asof_select_test.go` ~L119-129 shows the inversion shape). Since v4.46.0 the
  record doors answer "the newest row recorded by the pin" by design and the one version allocator
  (item 18) orders versions by write; re-check whether any divergence remains beyond that documented split.
- **(MEDIUM?) `NodeMatchesValidTime` on a current row with unset ValidFrom** answers "valid since
  mint", so a consumer post-filtering current rows accepts today's properties for times
  before the last update, where `NodeAt` returns the older version.
- **(LOW) Snowflake ID horizon.** 48-bit microseconds from 2026-01-01 end on 2034-12-02
  (arithmetic). Needs a plan before v4 data outlives it; v5 drops clock bits from IDs.
- **(KNOWN LIMITATION) Future-scheduled close, then delete** — tracked as item 25 (and item 7).

### 5. P7 row compaction follow-ups

*MEDIUM (perf)* — open; options (a)-(d) unbuilt (P7 itself shipped in v4.40.0).

**P7 row compaction — follow-ups** (MEDIUM, perf; P7 itself shipped in v4.40.0: 608 → 388 B per P6 HOP row, 531 → 401 B per node). Remaining measured terms per P6 HOP row: struct 80, compact metadata 96, properties 128, `rels` 22, `typeIdx` 21, adjacency ~35. Options, none built: (a) property keys as tokens in a frozen row's own representation (−32 B at 4 properties; every property accessor needs a second path, and a process-wide key table must be bounded); (b) compact form for updated / history versions (today only first versions compact; matters for update-heavy workloads — measure one first); (c) the badger entity cache could cache `CompactFrozenCopy` rows too (its budget uses `ApproxHeapBytes`, which already knows the compact size); (d) store-side interning of repeated string value boxes (legacy schema: ~16 B per `actor`; ai-soc P6 already shares boxes, so no consumer needs it now). (The 3.15 M `ByType` gap seen on a loaded host did not reproduce on a quiet one: median 9.07 → 9.56 M rows/s, CHANGELOG.) "Lazy adjacency" was not built: compact sets save always, and a lazy build would come back on the first adjacency read (sigma's hop scans).

### 7. Caller-instant delete of a row with a scheduled close

*FEATURE (limitation)* — open; `DeleteWithTx` refuses `ValidTo >= t` with `ErrTxOrder` (R8, v4.44.0).

**Caller-instant delete of a row with a scheduled close** (FEATURE, follows item 6): `DeleteWithTx(id, t)` refuses when the recorded `ValidTo >= t`, because one tombstone row cannot end belief at `t` and keep the close that pins before `t` believed (decision 2026-10-09, handover §6.11; the refusal shipped in v4.44.0, `core/tx_order.go`). Lifting it needs a tombstone that keeps the believed `ValidTo` beside the deletion instant (no clamp for a caller instant), with the normalizer (`normalizeTemporalVisibleAtTxTime`, `core/txtime.go:495`) and the valid-time history reads agreeing. Write the two-phase red test first.

### 10. Tiered store: remaining work after composite + rel temporal indexes

*FEATURE* — indexes shipped in v4.45.0; (a) sequential-scan rebuild and (b) rel property indexes / node `TemporalCandidateCapability` on tiered open.

Composite and relationship temporal indexes on tiered shipped in v4.45.0 (ai-soc request 3; per-shard fan-out anchored by the reference shard; rel temporal 156 B/relationship, composite 310 B/node; the rel temporal index lives on reference + hot + warm shards only, cold demotion frees it, `PromoteColdShardsAtOpen` rebuilds it; ≈ 5.8–9.2 GB per indexed type at `ColdAfter` = 1 day). Remaining: (a) a sequential-scan rebuild — today one point read plus one history prefix scan per relationship (0.018–0.021 M rels/s), 29–54 min per day shard on reopen or promotion; (b) relationship property indexes (`CreateRelPropertyIndex` declines, `store/tiered/tieredstore_write.go`) and the node-side `TemporalCandidateCapability` are still declined on tiered.

### 13. Ingest applier attributes group errors by numeric entity id, not by kind

*MEDIUM* — open; `groupApplyError` still keys `idToGroup` by `types.EntityID` (`core/ingest.go`).

**Ingest applier attributes group errors by numeric entity id, not by kind** (MEDIUM, found 2026-10-09 writing `TestSessionSetVersionInterval_FailedGroupShape`): the strong-async applier keys group-error attribution by the numeric id, so a missing node id and a missing rel id with the same number attribute errors to the wrong group. Minted snowflake ids never collide across the two generators, but caller-supplied ids (`AddByID`, `Import`) can (the rollback-snapshot rule in AGENTS.md "Data Model" already keys by kind + id for the same reason). Red test first: two groups, a node and a rel with the same numeric id, each missing; each group gets its own sentinel. Fix: key by (kind, id).

### 15. Durable commit: power-loss gap on a memtable switch

*MEDIUM* — open; documented on `Config.DurableCommit` (v4.44.0); scheduling: René.

**Durable commit: power-loss gap on a memtable switch** (MEDIUM, follows item 11, found by the item-11 review 2026-10-09): `Config.DurableCommit`'s `DurableFlush` ends with badger `db.Sync()`, which fsyncs only the active memtable's WAL and the current value-log file (badger v4.9.2 `db.go:705-709`). A flush that fills the memtable switches to a new one in `ensureRoomForWrite` (`db.go:1015-1040`) without syncing the retired WAL, and a finished value-log file is synced only under `SyncWrites` (`memtable.go:408`), so rows of a group that crossed a switch can be unsynced after `DurableFlush` returned: process-crash safe, not power-loss safe (documented on `Config.DurableCommit`, `store.DurableFlushCapability`, docs/architecture.md "Durable-on-return commit"). Options: (a) sync the retired memtable WAL (and the finished vlog file) on switch when a durable flush is in progress — needs a badger hook or a fork; (b) measure `SyncWrites` cost against `DurableCommit` on a group workload and recommend it for strict durability. Red test first: a group larger than one memtable, a power-loss-shaped check (fsync accounting or a dm-flakey/`LD_PRELOAD` drop of unsynced pages). Scheduling: René.

### 16. Range folds under an interval: predicate-anywhere or value at the resolved version?

*MEDIUM (contract)* — open; decision pending; behaviour documented in the v4.44.0 CHANGELOG.

**Range folds under an interval: predicate-anywhere or value at the resolved version?** (MEDIUM, contract inconsistency, found reviewing item C scan-temporal-opts 2026-10-09). Decide whether all range folds (ordered included) pass the range into the resolver's predicate (predicate-anywhere) like `ByLabelAndProperty`. Today, under `ValidStart`+`ValidEnd`, `Nodes().ForEachByLabelPropertyRange` / `Rels().ForEachByTypePropertyRange` and the ordered / prefix siblings (`forEachNodeInRangeTemporal`, `forEachNodeValueOrderedTemporal` and rel mirrors) test the range on the ONE version `ByLabel` / `ByType` resolve (the most recent overlapping one), while `ByLabelAndProperty` / `ByTypeAndProperty` pass the property into `findNodeVersionForOpts`'s predicate and match a value held anywhere in the interval (`graph_property_query.go:87-95`). A value in range only during an earlier part of the interval is found by the equality door and missed by the range doors. Point opts (`ValidAt`, `TxAt`, `TxPin`) agree. If predicate-anywhere is chosen: the selecting predicate must be EXACT (an over-selecting one could pick a version fn then rejects while an earlier one matched), and the ordered doors sort on the value of the matching version. Red test first: rule-16 shape (value in range on the earlier version only, interval spanning both) across all range doors.

### 20. Effective-read cost: RelAtTx benchmark targets unmeasured

*LOW (TEST-GAP, perf)* — fixes 1a (v4.44.1), 1b (v4.46.0), 1c (v4.47.0), 2a (v4.46.0) shipped; only the RelAtTx benchmarks remain.

Everything the handover `tasks/handover-effective-read-cost-20261009.md` asked for shipped (Closed table) except its `BenchmarkRelAtTx/{plain,cascaded}/{hot,cold}` rows and their targets (< 1 µs plain hot row, cold row + 1 µs, ≤ 6 µs cascaded via the skeleton path; 2.0 / 9.5 / 18.5 µs at v4.43.0): no such benchmark exists in the tree. Add it (ReportAllocs, bench-gate) and measure on v4.49.0 before deciding whether any work remains.

### 21. Purge relationships by age, gate retention reads per type

*FEATURE* — open; no `PurgeExpiredRels` in the tree; sigma: not blocking until `@overview` starts.

**Purge relationships by age, and gate retention reads per type** (FEATURE, requested by sigma-tkgd's temporal overview design 2026-10-09; first filed as item 10 in c8de314 and dropped by my backlog edit in f952015, restored here under a new number because 10 is now the tiered indexes): `Admin().PurgeExpiredRels` with a watermark per relationship type, a retention scan gate that a purge of the raw event type does not trip for the long-lived `Overview` nodes, and the tiered landing place of a long-lived node's new versions (answered from the code). Handover with verified citations, API, backend and change-feed semantics, and the red tests to write first: `tasks/handover-overview-retention-20261009.md`.

### 22. Badger rel temporal index create races concurrent writes

*MEDIUM (not reproduced)* — open; `CreateRelTemporalIndex` still uses the snapshot / unlocked build / install shape.

**Badger rel temporal index create races concurrent writes** (MEDIUM, pre-existing, NOT reproduced — read from code, review of backlog 10, 2026-10-09). `CreateRelTemporalIndex` (`store/badger/badgerstore_reltype_temporal_index.go`, the create body) snapshots the type's relationship IDs under `idxMu`, builds the envelope index unlocked and installs it; a relationship created or updated in the gap is not folded in, because `ExtendRelInTemporalIndexes` returns early while no index exists — lesson 74's lazy-build rule. A missed new relationship is merely uncovered (kept); a missed update of a covered one can leave its envelope short of a row, i.e. an unsound prune. On tiered this now runs against the hot shard while it ingests. Red test first with a write-generation guard (lesson 63): write between snapshot and install, then prune at the written row's interval; fix with the 3-phase mutated-set install `CreateTemporalIndex` uses. The header comment was corrected (it claimed the scan held `idxMu`).

### 23. `changeFeedPage` prefetches past its page

*LOW (perf, unmeasured)* — open; `PrefetchValues = true`, no `opts.Prefix` (`badgerstore_changelog.go:648-659`).

**`changeFeedPage` prefetches past its page** (LOW, perf, found by the review of the history-prefix fix 2026-10-09; not measured): `store/badger/badgerstore_changelog.go:654-659` iterates with `PrefetchValues = true`, no `opts.Prefix`, `Seek(start)` then `ValidForPrefix(ChangeLogPrefix())`: a small-limit poll near the tail prefetches up to 100 values it never uses and may read past the change-log keyspace. Fix: `opts.Prefix = prefix` before `NewIterator`, and `PrefetchValues = limit == 0 || limit > 100`. Measure with a polling benchmark first (replica watchers poll it).

### 25. A future close followed by a delete changes the answer at an earlier pin

*MEDIUM* — open; `stampDeleteTombstone` still clamps (`core/temporal.go:133-137`).

**A future close followed by a delete changes the answer at an earlier pin** (MEDIUM, pre-existing on main, found by the review of the cascade-correctness branch 2026-10-09): `CloseVersion(far-1)` → pin → `Delete`: `AsOf(pin)` changes from `[1000,far-1)` to `[1000,inf)` and `AtTx(far, pin)` from absent to present, because `stampDeleteTombstone` clamps the close to the delete instant (`core/temporal.go:136`) and the as-of rewind (`normalizeTemporalVisibleAtTxTime`, `core/txtime.go:495`) reopens it to 0 (the same "known limitation: future-scheduled close, then delete" of the 2026-09-24 review, now that the life-end cap exists the delete could keep a finite ValidTo). Red test first: two-phase on all four backends, node and rel; fix: the tombstone keeps the believed ValidTo beside the deletion instant (see item 7). (Seen again on a re-imported ID's earlier life, item L review 2026-10-09.)

### 26. Old chains with collided versions keep answering in the old version order

*LOW (known limit)* — open; schedule only if a consumer holds such chains.

**Old chains with collided versions keep answering in the old version order** (LOW, known limit, documented in the v4.46.0 CHANGELOG): chains written before the version allocator (backlog 18b) that already hold two rows with one version cannot be repaired by the read rule: e.g. old S2 followed by two old Updates answers the patch row at the pin after the first Update. A one-off repair tool (walk chains, renumber) is possible without a format change; schedule only if a consumer holds such chains.

### 28. State form of the as-of column doors and counts

*FEATURE (perf)* — open; design first (sigma).

**State form of the as-of column doors and counts** (FEATURE, requested by sigma-tkgd 2026-10-09 after it moved every pinned read to the state doors): pinned COUNT and aggregate queries lost the as-of column path because `DocValuesSnapshotAsOf` / the as-of column set / `ScanNodeColumns` as-of are record doors. Since v4.46.0 `ScanNodeColumns`/`ScanRelColumns` answer `ValidAt + TxAt` exactly (the ByLabel/ByType fold, item C), but at row-fold cost. Wanted: a cached state column set keyed by (label, valid-at, pin) (or a segment-based cut) so `CountByLabelAt`/column scans with `ValidAt + TxAt` regain the as-of column speed. Design first (cache key explosion over valid-at; reuse the effective-timeline cut logic of item J). **Measured by sigma-tkgd on v4.46.0 (2026-10-09), pinned count over 1 M nodes with every tenth updated after the pin, 3 runs:** memory store state label scan 1.81 s, state `ScanNodeColumns` 1.74 s, unpinned columns 0.61 s; badger in-memory 14.2 s (80 M allocs) and 13.0 s (74 M allocs) for the state doors vs 1.41 s unpinned columns: the v4.46 state column door is exact but only 4–8 % faster than the label scan (it folds rows) and about 9× slower than unpinned columns on badger. Target: a state column path within ~2× of the unpinned columns on badger (rows whose current row answers the state need no history read; only entities with history after the pin need the cut).

### 31. Bulk and point as-of disagree with the cascade oracle on version gaps

*LOW* — open; fix when a consumer holds such chains.

**Bulk and point as-of disagree with the cascade oracle on version gaps** (LOW, pre-existing, found by the review of the bulk as-of change 2026-10-09): on a chain with a version gap above the current row (direct `Store.TruncateNodeHistory`, or a re-import over a compacted old life stored by v4.43–v4.47 (since item 38 a re-import starts above the chain's top)) both `NodeAsOf` doors miss a row above a gap past current+1 (56 cases against `cascadeAsofWant` in the reviewer's scratch test, identical on main and the branch). The allocator (item G) never creates such a gap; the density assumption is documented in `core/version_alloc.go` and AGENTS.md. Fix when a consumer holds such chains: either close the gap at truncate/re-import time (renumber under the entity lock) or make the doors scan for the max version above current. Red test first (the reviewer's five gap chains, node and rel, with/without reopen, 17 pins).

### 33. Badger effective-timeline scan costs 13-17 µs per relationship

*MEDIUM (perf)* — open; restart pending (baseline `tasks/evidence/badger-read-cost/`).

**Badger effective-timeline scan costs 13-17 µs per relationship** (MEDIUM, perf, from the effective-timeline numbers of v4.47.0: ≈ 3 s for 200 K rels vs 1.3 µs/rel on memory): the pinned candidate gather decodes every current row and each rel's row is then read again. Reuse the rows the gather decoded, or gather candidate IDs only (key scan) and read once. Measure with `BenchmarkForEachRelEffectiveByType` before and after.

### 34. `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx`

*FEATURE* — open; agent stopped before committing anything; restart (sigma replay need).

**`SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx`: a caller instant for interval rewrites** (FEATURE, requested by sigma-tkgd 2026-10-09 from the burst-growth replay): a replayed burst growth `[vs,ve) → [vs,ve')` cannot be stamped at the record's own transaction instant because the append-only cascade (`Temporal().SetXVersionInterval`, `GraphTx`, `BatchBuilder`, ingest `Session`) stamps `TxFrom = now`. Mirror `DeleteWithTx`/`UpdateWithTx` (handover tx-backfill §6): gate `AllowTxBackfill`, `ErrInvalidTxFrom` for non-positive or future instants, `ErrTxOrder` against the entity's own chain (t must exceed the newest `TxFrom` and every appended piece takes the same caller instant), one instant for every appended row of the cascade, `notePastDatedWrite` for the as-of cache, whole-unit pre-flight in Batch/ingest, replica reproduces the stamps. Red tests first on all four backends, node and rel, every door (the W5 oracle gets the new door in its generator).

### 35. Broader effective scans

*FEATURE* — open; signatures confirmed by sigma 2026-10-10 (HANDOVER.md §11).

**Broader effective scans** (FEATURE, requested by sigma-tkgd 2026-10-09, exact call patterns from its builtins and push-down; N/M = nodes/rels at the pin, K up to 500 k rels from a push-down, S up to 100 k seed nodes, E entities per session cut): after `ForEachRelEffectiveByType` / `ForEachNodeEffectiveByLabel` (item J), all with the same contract as the per-entity door (one entity's segments contiguous and ascending, entities deleted before the pin included, fn false stops): (1) `ForEachNodeEffective(pin, fn func(NodeSegment) bool)` and `ForEachRelEffective(pin, fn func(RelSegment) bool)`, label- and type-free, one call each per evaluation (builtins node/2, edge/3, prop/3 today: a TxPin scan that misses entities deleted before the pin plus one timeline call per row, twice when prop is used); (2) `ForEachRelEffectiveAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin, fn func(RelSegment) bool)` batched for the src_/dst_ push-down (today `Outgoing/IncomingForNodesAtPin`, which misses rels deleted before the pin, plus K timeline calls), direction fixed per call, deleted-before-pin rels included, each rel once even if both endpoints are seeds (a single-node form would mean up to 100 k calls); (3) `NodesEffectiveByIDs(ids []types.NodeID, pin, fn)` and `RelsEffectiveByIDs(ids []types.RelID, pin, fn)`, unknown IDs skipped, one call per @source or per cut and kind; (4) optional, low priority: `ForEachNodeEffectiveByLabels(labels []string, pin, fn)`, each node once even with several matching labels. Red tests first: exact-set per pin on the oracle generators (live, closed, deleted-before-pin, deleted-after-pin, created-after-pin; rels with both endpoints seeded; duplicate IDs in a by-IDs batch), memory/badger/tiered/sharded.

### 37. Tiered error-rollback restore leaves a window with empty history

*LOW* — open; `restoreRelHistorySnapshot` still truncates then puts (`tieredstore_write_history.go:899`).

**Tiered error-rollback restore leaves a window with empty history** (LOW, pre-existing, found by the review of the point-door race fix 2026-10-09): `rollbackDeletedRelationships` → `restoreRelHistorySnapshot` (`store/tiered/tieredstore_write_history.go:899`) runs `TruncateRelHistory(0)` and then `PutRelVersion`; on that error-rollback path a concurrent reader can see an empty history between the two calls. Fix by restoring in one shard call or under a lock readers respect (the publish-history-first rule of AGENTS.md "Version History"). Red test first: a hook between the truncate and the put, plus a pinned `RelAtTx`/`History` read there, on the tiered store.

### 39. Per-entity compaction stubs survive `Admin().Reset`

*LOW (not tested)* — open; `reapCompactionForReset` still leaves the stubs (`core/compaction.go:285`).

**Per-entity compaction stubs survive `Admin().Reset`** (LOW, found by the review of item L 2026-10-09; not tested): `reapCompactionForReset` deliberately leaves per-entity stubs (it assumes IDs are never reused); on memory an `Import` of an old ID after `Admin().Reset` can still restart at version 0 under a leftover stub, because Reset zeroes the watermark that gates the stub probe (`stubLifeStart`). Red test first: compact → Reset → Import(old ID) → `Verify*Chain` on memory/badger/tiered, node and rel. Fix: Reset reaps the stubs (it already wipes the rows), or the stub probe does not depend on the watermark.

### 40. A cascade resumption may end at an earlier life's own bound

*LOW (cosmetic)* — open.

**A cascade resumption may end at an earlier life's own bound** (LOW, cosmetic, item L review 2026-10-09): `temporal_cascade.go:334,352` (`nodeResumptionEnd` / `relResumptionEnd`) considers bounds of rows before the last tombstone, which adds an extra split row; reads are unchanged (life oracle). Fix only if row count matters: consider only rows after the last tombstone. Red first: a row-count assertion on a cascade after a re-import.

### 41. Merge the history stamps sidecar into the presence set

*MEDIUM (RAM)* — open; after 42.

**Merge the history stamps sidecar into the presence set** (MEDIUM, RAM, from the review of `LatestStamps` 2026-10-10): one map with `{from, to, top}` under one protocol saves about 30 B/ID and one of the two lazy builds (66 B/ID today vs the 16 B target; 66 MB at 1 M IDs with history). Red first: the existing differentials (HasHistory, LatestStamps) stay green on the merged structure; a memory-per-ID benchmark gate.

### 42. A current-row stamps capability

*FEATURE (perf)* — open; restart with 33.

**A current-row stamps capability** (FEATURE, perf, from the review of `LatestStamps` 2026-10-10): `LatestStamps` on a badger entity-cache miss costs 6.5–7.1 µs and 25 allocs (reads and decodes the whole current row), sharded/tiered allocate 8–12 per call (core treats sharded as an untrusted store: no Lend, a validated copy). Add an optional capability that reads only the current row's temporal block (badger: temporal-block read on a cache miss; sharded/tiered: slot routing without copying). Measure `BenchmarkLatestStampsRotating` before and after.

### 43. Retraction: a way to say "this was never true"

*FEATURE — NEXT STEP* — open, not built (the agent was stopped before committing anything); spec below.

**Retraction: a way to say "this was never true"** (FEATURE, NEXT STEP, requested by ai-soc via sigma-tkgd and confirmed by René 2026-10-10 — this overrides the earlier relayed "no door needed"; ai-soc retracts wrong records and ends the edges of an unfinished commit group in recovery, `ai-soc graphmgr/recovery.go:254`; sigma's effective read must see the same): `Delete` keeps its VALIDITY-END meaning (the tombstone caps the life at the delete instant D, `core/chain_resolver.go` `lifeEnds`; past valid time stays readable at later pins). Missing is the transaction-time fact "belief in this row ends at instant T; at pins >= T the entity is ABSENT FOR EVERY VALID TIME; at pins < T nothing changes". No existing way: a cascade patches properties of a state and cannot remove state, `CloseVersion` needs `validFrom < validTo`, a cascade on a deleted entity is refused, and the retraction rule inside `SelectAsOf` (lessons 60/62) is only reachable together with the valid-time cap at D. Proposed API (names open): `Nodes()/Rels().Retract(ctx, id)` and `RetractWithTx(ctx, id, t)` (gate `AllowTxBackfill`, `ErrInvalidTxFrom`, `ErrTxOrder` against the whole chain like `DeleteWithTx`), twins on `GraphTx`, `BatchBuilder` (whole-unit pre-flight, group atomicity: a failed group retracts nothing) and ingest `Session`. Semantics to pin with red tests on every door: point/state doors (`NodeAtTx`/`RelAtTx`, `NodeAt`), record doors (`NodeAsOf`/`RelAsOf`/`NodesAsOf`/`RelsAsOf`, `TxPin` scans), generic `ByLabel`/`ByType` with `ValidAt`/`TxAt`/`TxPin`, `Temporal().NodesAtTx`/`RelsAtTx`, `NodeEffectiveTimeline`/`RelEffectiveTimeline` (empty at pins >= T) and the scan forms (entity excluded at pins >= T, present with its segments at pins < T), counts (`CountByLabelAt`), `HasHistory` (true) and `History`/`LatestStamps` (tombstone visible; `deleted` true; `txTo` includes T). Open design points (decide with evidence first, defaults in brackets): marker storage [CORRECTED after reading the code 2026-10-10: a reserved stored property is NOT possible — `PropertySlice.Set` rejects every `tkg_` key and the shadow keys are virtual read-only (`pkg/types/shadow.go`). Default now: an additive optional `Retracted bool` field on `types.TemporalMetadata` with an `omitempty` msgpack tag on NodeWire/RelWire (+ the partial temporal decoder `internal/storeutil/wire_temporal_meta.go`, delta history `Meta`, export/import record format, replica apply); the temporal block is NOT part of the content hash (integrity.computeNodeHash), so the hash is unchanged; the compact frozen form (`types/compact.go`) requires DeletedAt == 0 so a retraction tombstone never takes it (verify `compact_fields_test.go` still enumerates the field). Old binaries skip the unknown wire key and degrade a retraction to a Delete (validity end): mixed-version replicas diverge in answers, so the CHANGELOG needs an 'upgrade replicas first' migration block like DeleteWithTx. Decide the wire format-version rule from docs/SPEC.md §9.1a (no `fv` bump if old readers can safely ignore the field; they cannot ignore its MEANING, so state it). Expose through an accessor (`Temporal().Retracted`/`types.TemporalMetadata.Retracted`) and decide whether a 22nd shadow key `tkg_retracted` is worth changing the shadow registry test]; the life-end cap [retraction caps its life at -infinity: one more branch in `lifeEnds`, the supersession rule and the timeline sweep]; interplay with a later re-import [the new life is visible, versions continue (item L)] and with a retraction of an already-deleted entity [refuse with the not-found/`ErrEntityDeleted` family or allow upgrading the tombstone: decide]; interplay with backlog 25 (close then delete); `UniqueCurrent` frees the value, `UniqueForever` keeps the claim (as Delete); compaction and retention purge must keep the marker with the tombstone; additive only (minor release, `docs/stability.md`: no existing answer changes because no existing row carries a marker). Red tests first, all four backends, node AND rel, two-phase pins before/at/after T, plus the oracle generators (`tx_backfill_oracle_test.go`, `effective_timeline_oracle_test.go`) extended with the op and a brute-force belief definition written separately from the resolver.

### 44. Commit-clock floor persisted only at Close

*MEDIUM (traced, not reproduced)* — open; filed 2026-10-10 from `tasks/review-v5-plan-vs-code-20261009.md` §6.5.

A crash after a burst whose monotonic floor outran the wall reopens with `NowTx` below committed stamps (lesson 71's reopen case on the crash path): `persistInstantFloor` is called only from `Close` (`core/core.go`, `core/instant_floor.go:129`), and `seedInstantFloor` reseeds only from that watermark. Red test first: a crash-shaped child (exit without `Close`) after writes stamped ahead of the wall, reopen, assert `NowTx()` and a fresh write's `TxFrom` exceed every stored `TxFrom`. Fix candidates: persist the floor with the durable commit / flush, or reseed from the newest stored stamp at open.

---

### 45. Internal sentinels reach callers but have no exported alias

*MEDIUM (API)* — found by the doc review of `docs/errors.md` (group C, 2026-10-10).

`ErrNilCallback` (`pkg/graph/internal/grapherr/errors.go:13`), `ErrCommitClockExhausted` and `ErrForeignStampImplausible`
(`pkg/graph/internal/core/core.go`) can be returned to callers, but no public package re-exports them, so `errors.Is` cannot
name them; `pkg/graph/temporal/api.go:474` even tells callers to expect `ErrNilCallback`. `graph.ErrMixedNumericColumn`
(`pkg/graph/column_scan.go`) lives outside the `errors.go` inventory that `errors_doc_test.go` / `errors_identity_test.go` pin.
Red tests first: for each sentinel, a public-layer test that `errors.Is(err, graph.ErrX)` holds for a real failing call (nil callback on a
Tx/Temporal door, clock exhaustion by `AdvanceClock` to the limit, an implausible foreign stamp on replica apply, a mixed numeric column
scan); the inventory test fails when an exported error is missing from `errors.go` or `docs/errors.md`. Fix: export aliases in
`pkg/graph/errors.go` (additive), move `ErrMixedNumericColumn` into the inventory, add rows to `docs/errors.md`.

### 46. Code comments that contradict the code or cite ADRs that no longer exist

*LOW (stale comments)* — found by the doc reviews (groups C, D, 2026-10-10).

`pkg/types/node.go:30` says "88 bytes" (the struct is 96 B, `pkg/types/layout_test.go`); `pkg/graph/internal/core/ingest_lanes.go:79-80`
says the lane range is [0,127] (it is 0-15); `pkg/graph/store/tiered/tieredstore_property_stats.go:19` and `bench/ingest_pipeline_test.go`
cite ADR-0005 / ADR-0006, which are no longer in `docs/adr/` (recover with `git log --all -- docs/adr/`, or rewrite the comment to state the
rule itself); the header of `pkg/graph/internal/core/effective_timeline.go` (~lines 10-20) describes resolving each piece with the point resolver, but the code (`effectivePieces`, ~line 115) uses one max-heap sweep. Comment-only change; the docs-consistency tests must stay green.

### 47. Claims that need a real check before they become work

*LOW (unverified)* — carried from HANDOVER.md §7 and left unfiled by the doc review of the tasks files (group E).

(a) "`NodesByLabelAt` still folds all history instead of using the K1 sidecar": `NodesByLabelAt` is a valid-time door and K1 is
transaction-time membership; verify whether the named door can use K1 at all (a two-phase test with a churned label, memory/badger)
before filing a fix. (b) "A RAM budget for the property sidecars": v4.48.0 measured 25-30 B per posting (97-117 B near-unique) and defined
no budget; reopen only if a consumer reports memory pressure; then design a per-store budget that drops a sidecar and falls back to the fold.

## v5

v5 is Markus Nissl's work on branch `origin/v5`, a separate Go module (`github.com/data-insights-ai/rho-tkg/v5` in
`v5/`); the plan (`docs/v5/PLAN.md`, `RESEARCH-REVIEW.md`, `DISCUSSION.md`, revised 2026-10-09 after
`tasks/review-v5-plan-vs-code-20261009.md`) lives on branch `v5`, not on main (our plan commit `b1193dc`, tag
`v5-plan-20261009`). Do not push to `v5` (HANDOVER.md §4). What the v4 changes mean for the importer:
`tasks/v4-changes-for-v5-importer-20261010.md`. v4 keeps taking consumer features (decision René 2026-10-09, the
earlier fixes-only rule is deleted). **DECIDED 2026-09-25 (René): ADR-0011 S2 and S5 shipped on v4 as v4.39.0**
(ADR-0011 §6 gate met: 24.0 / 24.8 / 25.7 B/HOP at 790 K / 3.15 M / 12.6 M); v5 carries the segments forward.

---

## Not tracked here (cross-team)

Consumer builds these; rho-tkg already exposes the local primitives:

- START→END foreign-stub-delete fan-out (BACKLOG 2 Inc 4c)
- Consumer-gated constraint dry-run (HP2.5)

When consumer pins a shape that needs a **new** rho-tkg primitive, it re-enters
**Open** as a concrete item.

---

## Closed (pointers only — detail in CHANGELOG)

| Epic / item | Where it landed |
|------|-----------------|
| BACKLOG 1 — Retention purge (ex-ADR-0008 R2–R5) | CHANGELOG (4.18–4.24 era) |
| BACKLOG 2 — Cross-machine Model A (ADR-0010 §3.3) | CHANGELOG |
| BACKLOG 3 — Columnar / streaming whole-node fetch | CHANGELOG |
| BACKLOG 4 — Review adaptations (4b–4e; **4a DO NOT BUILD**) | CHANGELOG |
| BACKLOG 5 — Rel ordering-soundness (`RelRangeCardinality`, type-class) | CHANGELOG |
| BACKLOG 6–21 (old numbering) — full-library hardening (~196 findings) | closed 2026-07-18…22, `[4.24.0]` |
| Last HIGH (10b cascade/resumption) + 10c perf follow-up | `[4.24.0]` / next-day fix |
| Consumer-gated ask batch (5 items, 2026-07-29) | `[4.25.0]` same day |
| CI bench-gate (blocking) | 2026-07-29, `bench.yml` |
| Item 3 (HIGH) — entity wire widened nested values; DECIDED 2026-09-24: kind envelope, no write-time normalization | `[4.37.0]` Fixed |
| HIGH — badger reads dropped or replaced rows when a flush + eviction landed mid-read (flush epoch, `scanSnapshot`, `LoadCleanAt`) | `[4.37.1]` Fixed |
| Review 2026-09-24: delete after close, correction template, endpoint masking (HIGH) | `[4.38.0]` (merge `4473705`) |
| Review 2026-09-24: TxAt-only doors "nondeterministic" (implicit valid-time now = wall clock; now `readNow()`) | `[4.38.1]` |
| Item 6 (FEATURE) — `DeleteWithTx` / `UpdateWithTx` on every write door, `ErrTxOrder` | `[4.44.0]` Added; handover `tasks/handover-tx-backfill-delete-update-20261009.md` |
| Item 11 (FEATURE) — durable-on-return commit `Config.DurableCommit` | `[4.44.0]` Added (power-loss gap: item 15) |
| HIGH — one-tick `[t, t+1)` rows skipped as the eclipse sentinel; skip removed | `[4.44.0]` Fixed "One-tick valid intervals are ordinary spans" |
| Review-v5 §6.2 — column and range scans ignored `TxAt`/`TxPin` | `[4.44.0]` Fixed "Column and range scans answer temporal options" (interval semantics: item 16) |
| Item 20 fix 1a — badger history iterators bounded to the entity prefix | `[4.44.1]` Fixed |
| Item 10 (FEATURE) — tiered composite + relationship temporal indexes, hot+warm bound | `[4.45.0]` Added (remaining work: item 10 above) |
| Item 12 (HIGH) — unique constraint bypass via `SetNodeVersionInterval` props on all four doors | `[4.46.0]` Fixed "unique constraints are enforced on `SetNodeVersionInterval` props patches" |
| Item 14 (HIGH) — a correction on a deleted entity copied the tombstone stamps; refused with `ErrEntityDeleted` | `[4.46.0]` Changed "A valid-time correction on a hard-deleted entity is refused", Fixed "Rows a write appends carry no retraction" |
| Item 18 (HIGH) — cascade row versions collided with later Updates; as-of answers at a pin changed after a later write | `[4.46.0]` Changed "The as-of doors answer the newest row recorded by the pin", Fixed "One version allocator", "The chain resolver reads chains in version order" |
| Item 19 (HIGH) — GraphTx rollback dropped the cascade rows above the current version | `[4.46.0]` Fixed "GraphTx rollback keeps the cascade rows above the current version" |
| Item 20 fix 1b — `Nodes()/Rels().HasHistory` | `[4.46.0]` Added; evidence `tasks/evidence/has-history/` |
| Item 20 fix 2a (HIGH) — a delete after a bounded cascade left the entity readable | `[4.46.0]` Fixed "A delete after a bounded cascade ends the entity in every door" |
| Item 24 (HIGH) — history compaction broke the hash chain of a cascaded entity | `[4.46.0]` Fixed "History compaction keeps every row the kept chain's hash links point to" |
| Item 20 fix 1c — `Node/RelEffectiveTimeline` | `[4.47.0]` Added; evidence `tasks/evidence/effective-timeline/` |
| Item 27 (FEATURE) — scan forms `ForEachRelEffectiveByType` / `ForEachNodeEffectiveByLabel` | `[4.47.0]` Added |
| Item 32 (HIGH) — pinned doors missed an entity while a write moved its row to history | `[4.47.0]` Fixed; evidence `tasks/evidence/point-door-race/` |
| Item 36 (FEATURE) — `types.NodeID.MintInstant()` / `types.RelID.MintInstant()` | `[4.47.0]` Added; evidence `tasks/evidence/mint-instant/` |
| Read-time supersession rule (a replaced row ends where its replacer starts) and life ordering of re-imported IDs | `[4.47.0]` Fixed "A replacing write … ends every older belief", "A re-imported ID's rows order after the earlier life's" (code `chain_supersession.go`, `chainWriteOrder`) |
| Item 8 (MEDIUM, perf) — pinned property lookups cost the history; property membership sidecar | `[4.48.0]` Added "Temporal property lookups cost the value's ever-members"; evidence `tasks/evidence/pinned-property-index/` |
| Item 38 + review 2026-09-24 "(HIGH?) Re-import of a deleted ID" (HIGH, data loss) | `[4.48.0]` Changed "A backfilled re-import of a deleted ID must be recorded after the ID's chain", Fixed "a re-import of a deleted ID no longer overwrites the earlier life's history"; evidence `tasks/evidence/reimport-life/` |
| Item 29 (MEDIUM) — a failed node write left its `UniqueForever` value owned (crash consistency between claim and row: v5 PLAN §5.2) | `[4.49.0]` Fixed; evidence `tasks/evidence/unique-claims/` |
| Item 30 (FEATURE, perf) — `Nodes()/Rels().LatestStamps(id)` | `[4.49.0]` Added; evidence `tasks/evidence/latest-stamps/` |

Recover closed investigation prose via `git log --all -- tasks/backlog.md` if needed.

### DO NOT BUILD / reopen criteria (keep here so they are not re-filed)

- **Item 9 — property-keyed unique constraint on relationships (ai-soc request 4): WITHDRAWN 2026-10-09** (René,
  relayed by ai-soc: "rho-tkg already has unique snowflake ids"). ai-soc uses the snowflake ID as the global ID.
  Possible cheap follow-up, not requested: a relationship door that accepts a caller-supplied snowflake ID "if
  absent", mirroring `Nodes().AddByIDIfAbsent` (today `Rels().AddByIDIfAbsent` dedups on `(start, end, type)`).
- **DECIDED NO 2026-10-09 (René, relayed by ai-soc):** an index-provider "failure aborts the mutation" option
  (ai-soc request 5; ai-soc rebuilds from the change feed) and one instant per commit group (ai-soc request 7; the
  cut-record pin is enough). Reopen only with a consumer case the change feed or the cut-record pin cannot cover.
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
