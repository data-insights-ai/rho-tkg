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

**Remaining open work (verified against the code at v4.51.2, 2026-10-12):** no reproduced CRITICAL item. In progress in the v4.51.3 batch: 52-55 (ai-soc, sigma) and the post-release review findings F1-F6 (`tasks/todo.md`). On hold: 41, 28, 21. Open items, one line each (full text below):

| # | Type / severity | Item | Status |
|---|---|---|---|
| 0 | FEATURE | Column segments on NVMe (ADR-0011) | S0-S3, S5 done (S2/S5 in v4.39.0, S3 + P7 in v4.40.0); S4 merge, S6, S7 and sub-points (a), (b), (d)-(i) open |
| 1 | MEDIUM (improvement, deferred locking) | Import-under-a-scope | open; `IO().Import` still takes only `c.mu` (no `c.txMu`), stopgap in place |
| 2 | TEST-GAP | Old BACKLOG 22: six TEST-GAP research items (22a-22f) | open; prioritise only when a consumer needs the confidence |
| 3 | MEDIUM (perf, unmeasured) | Temporal adjacency scan cost (v4.35.0 follow-up) | open; measure first |
| 4 | HIGH? / MEDIUM? (traced, not reproduced) | Temporal semantics review 2026-09-24: traced findings | open; each needs a failing two-phase test first; code unchanged at the cited sites (`resolveOpenEndInstant`, `validInstantAfter` in `core/temporal.go`) |
| 5 | MEDIUM (perf) | P7 row compaction follow-ups | open; options (a)-(d) unbuilt (P7 itself shipped in v4.40.0) |
| 7 | FEATURE (limitation) | Caller-instant delete of a row with a scheduled close | open; `DeleteWithTx` refuses `ValidTo >= t` with `ErrTxOrder` (R8, v4.44.0) |
| 10 | FEATURE | Tiered store: remaining work after composite + rel temporal indexes | indexes shipped in v4.45.0; (a) sequential-scan rebuild done (Unreleased, ≈ 4× vs the per-rel build); (b) rel property indexes / node `TemporalCandidateCapability` on tiered open |
| 13 | MEDIUM | Ingest applier attributes group errors by numeric entity id, not by kind | open; `groupApplyError` still keys `idToGroup` by `types.EntityID` (`core/ingest.go`) |
| 16 | MEDIUM (contract) | Range folds under an interval: predicate-anywhere or value at the resolved version? | open; decision pending; behaviour documented in the v4.44.0 CHANGELOG |
| 20 | LOW (TEST-GAP, perf) | Effective-read cost: RelAtTx benchmark targets unmeasured | fixes 1a (v4.44.1), 1b (v4.46.0), 1c (v4.47.0), 2a (v4.46.0) shipped; only the RelAtTx benchmarks remain |
| 22 | MEDIUM (reproduced) | Badger rel temporal index create races concurrent writes | done (Unreleased): 3-phase mutated-set install |
| 23 | LOW (perf, unmeasured) | `changeFeedPage` prefetches past its page | open; `PrefetchValues = true`, no `opts.Prefix` (`badgerstore_changelog.go:648-659`) |
| 25 | MEDIUM | A future close followed by a delete changes the answer at an earlier pin | open; `stampDeleteTombstone` still clamps (`core/temporal.go:133-137`) |
| 26 | LOW (known limit) | Old chains with collided versions keep answering in the old version order | open; schedule only if a consumer holds such chains |
| 31 | LOW | Bulk and point as-of disagree with the cascade oracle on version gaps | open; fix when a consumer holds such chains |
| 37 | LOW | Tiered error-rollback restore leaves a window with empty history | open; `restoreRelHistorySnapshot` still truncates then puts (`tieredstore_write_history.go:899`) |
| 39 | LOW (not tested) | Per-entity compaction stubs survive `Admin().Reset` | open; `reapCompactionForReset` still leaves the stubs (`core/compaction.go:285`) |
| 40 | LOW (cosmetic) | A cascade resumption may end at an earlier life's own bound | open |
| 45 | MEDIUM (API) | Internal sentinels reach callers but have no exported alias; `ErrMixedNumericColumn` is outside the errors inventory test | open; found by the doc review of `docs/errors.md` 2026-10-10 |
| 46 | LOW (stale comments) | Code comments that contradict the code or cite ADRs no longer in `docs/adr/` | open; found by the doc reviews 2026-10-10 |
| 47 | LOW (unverified claims) | `NodesByLabelAt` vs K1; a RAM budget for the property sidecars | open; each needs a real check before it becomes work |
| 56 | MEDIUM | Node temporal envelope holes: `PutNodeVersion`, `CreateTemporalIndex` publish-before-fold window, label add on a live node, memory re-create | done (Unreleased Fixed); all cases red, all fixed |
| 57 | FEATURE (sigma R1) | Batched point read at (validAt, txAt): `NodesAtTxByIDs` / `RelsAtTxByIDs` | done (Unreleased Added); badger -29% per 100 IDs, memory parity |
| 58 | FEATURE (sigma R2) | Tiered: tx-membership sidecars (label/type, property value) and `*From` valid-end acceleration | DONE (Unreleased) |
| 59 | PERF (sigma R3) | Tiered event-shard routing by mint instant: binary search instead of a linear window scan | done (Unreleased Changed); 700 shards 3301 -> 33 ns/op, evidence `tasks/evidence/shard-routing/` |
| 60 | LOW (store-level only: graph Archive/Restore hold `c.mu.Lock`) | Tiered ref-then-archive readers outside moveMu straddle an ArchiveNode/RestoreNode | DONE (Unreleased): 16 scan doors and the point reads (GetNode, GetRelationship, Lend*, *ByIDs, adjacency, degree, current stamps) and NodePropertyStats/NDV hold moveMu's read side across ref+archive (route+read); ArchiveNode/RestoreNode use the unlocked route. Not a bug: DocValues folds (NodeMutationEpoch, Gate 2), history-ID scans (dedup), history point reads/walks (rows stay put, walks fall back to every shard; guard cases, no lock). Not covered: write doors route without moveMu (fenced by the graph's c.mu.Lock) |
| 61 | FEATURE (René, ai-soc) | Integrity-hash switch (default on) and one binary hash path | step 1 done (Unreleased Added + Deprecated: switch, binary hashes readable); step 2 (writer binary, marker 4) committed separately for the patch after; evidence `tasks/evidence/integrity-switch/` |
| 62 | SEMANTICS (sigma observation) | One valid-from source for untimed rows: TxFrom (today creates use the mint instant, updates TxFrom) | phase A done (Unreleased → v4.52.2): third answer fixed (row doors), Deprecated markers + notes; phase B (switch to TxFrom) in v4.52.3 |
| 63 | FEATURE (sigma R6) | Membership by rel ID: list properties are not indexed (propertyslice.go:353); recommended: one indexed support node per (Derived, relID), no rho change; fallback `PropertyIndexOptions{Elements: true}` | done (Unreleased Added): `PropertyIndexOptions{Elements: true}` on memory, badger, sharded, tiered ref labels; evidence `tasks/evidence/property-elements/` |
| 64 | FEATURE (sigma R7) | Valid-time excise [a,b): new door `Temporal().ExciseNodeValidity` with void pieces (no existing composition: CloseVersion only on the open current row) | shape confirmed by sigma; build after 62 (both touch the resolver) |
| 65 | SEMANTICS (traced) | `Update` with only `tkg_valid_to` copies the end onto the new row unchecked (node_update.go:224-233, relationship_update.go:~219; UpdateInPlace too); tests use it as "expire" (docvalues_test.go:120, retention_purge_validto_test.go:63). Chosen: consolidate with CloseVersion — such an update closes the version as CloseVersion does (rejects t <= valid-from); deprecation ritual together with 62 | phase A done (Unreleased → v4.52.2): Deprecated notes + CHANGELOG; phase B (close, shared helper with CloseVersion) in v4.52.3 |
| 66 | FEATURE (sigma round 7, low: Tyla lazy graph sources, not needed by sigma's current release) | Property-index range walks and exact range counts at a transaction pin (`TxAt` / `TxPin`), like `ForEachByLabelPropertyRangeOrdered` and `RangeCardinality` at current state | open, not built in round 7: the pinned doors fold every label member today (Stage B, O(N log N)); the tx-membership sidecar (backlog 8) is keyed by equality value, so an index answer at a pin needs an ordered view of (value, ID, TxFrom/TxTo) per index (memory and write cost per index write: opt-in per index, as `RangeCounts`). Design first |
| 67 | FEATURE (sigma round 7, low) | Per label: no current node carries an explicit `tkg_valid_from` / `tkg_valid_to` (rows stated eternal without reading them) | open, not built in round 7. Checked: `PropertyTypeClassCounts` does not answer it (it counts property values; valid-from / valid-to live in `TemporalMetadata`, and the counts are current-state, not per pin). Needs a per-label counter of rows with an explicit range (a write cost on every node write: opt-in per label, or lazy from the label scan once per label epoch) and a rule for a pin (history rows). Design first |
| 52 | HIGH (ai-soc) | Durable commit under SyncWrites costs one fsync per mutation | in progress, v4.51.3 |
| 53 | HIGH (ai-soc) | Relationship planner-stat memos hold one RAM entry per relationship | in progress, v4.51.3 |
| 54 | HIGH (ai-soc) | Badger open walks every current row (open linear in history) | in progress, v4.51.3 |
| 55 | MEDIUM (ai-soc, sigma) | Effective scans read one relationship per Badger transaction | in progress, v4.51.3 |

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

**Caller-instant delete of a row with a scheduled close** (FEATURE, follows item 6): `DeleteWithTx(id, t)` refuses when the recorded `ValidTo >= t`, because one tombstone row cannot end belief at `t` and keep the close that pins before `t` believed (decision 2026-10-09, tx-backfill handover §6.11 (removed, in git history); the refusal shipped in v4.44.0, `core/tx_order.go`). Lifting it needs a tombstone that keeps the believed `ValidTo` beside the deletion instant (no clamp for a caller instant), with the normalizer (`normalizeTemporalVisibleAtTxTime`, `core/txtime.go:495`) and the valid-time history reads agreeing. Write the two-phase red test first.

### 10. Tiered store: remaining work after composite + rel temporal indexes

*FEATURE* — indexes shipped in v4.45.0; (a) sequential-scan rebuild DONE (Unreleased); (b) rel property indexes / node `TemporalCandidateCapability` on tiered open.

Composite and relationship temporal indexes on tiered shipped in v4.45.0 (ai-soc request 3; per-shard fan-out anchored by the reference shard; rel temporal 156 B/relationship, composite 310 B/node; the rel temporal index lives on reference + hot + warm shards only, cold demotion frees it, `PromoteColdShardsAtOpen` rebuilds it; ≈ 5.8–9.2 GB per indexed type at `ColdAfter` = 1 day). (a) DONE (Unreleased): the build was one point read plus one history prefix scan per relationship (0.018–0.021 M rels/s in the field, 29–54 min per day shard on a crash reopen or a cold-to-warm promotion); it is now one read transaction with two forward walks (rows, history) in ID order — 200,000 relationships with history, reopened store: 78,919 → 304,166 rels/s (3.9×, host load ≈ 15; 3.9–4.5× over three runs), `tasks/evidence/rel-temporal-rebuild/03-bench.txt`; field shard times not re-measured; backlog 22 (create races writes) fixed alongside. Remaining: (b) relationship property indexes (`CreateRelPropertyIndex` declines, `store/tiered/tieredstore_write.go`) and the node-side `TemporalCandidateCapability` are still declined on tiered.

### 13. Ingest applier attributes group errors by numeric entity id, not by kind

*MEDIUM* — open; `groupApplyError` still keys `idToGroup` by `types.EntityID` (`core/ingest.go`).

**Ingest applier attributes group errors by numeric entity id, not by kind** (MEDIUM, found 2026-10-09 writing `TestSessionSetVersionInterval_FailedGroupShape`): the strong-async applier keys group-error attribution by the numeric id, so a missing node id and a missing rel id with the same number attribute errors to the wrong group. Minted snowflake ids never collide across the two generators, but caller-supplied ids (`AddByID`, `Import`) can (the rollback-snapshot rule in AGENTS.md "Data Model" already keys by kind + id for the same reason). Red test first: two groups, a node and a rel with the same numeric id, each missing; each group gets its own sentinel. Fix: key by (kind, id).

### 16. Range folds under an interval: predicate-anywhere or value at the resolved version?

*MEDIUM (contract)* — open; decision pending; behaviour documented in the v4.44.0 CHANGELOG.

**Range folds under an interval: predicate-anywhere or value at the resolved version?** (MEDIUM, contract inconsistency, found reviewing item C scan-temporal-opts 2026-10-09). Decide whether all range folds (ordered included) pass the range into the resolver's predicate (predicate-anywhere) like `ByLabelAndProperty`. Today, under `ValidStart`+`ValidEnd`, `Nodes().ForEachByLabelPropertyRange` / `Rels().ForEachByTypePropertyRange` and the ordered / prefix siblings (`forEachNodeInRangeTemporal`, `forEachNodeValueOrderedTemporal` and rel mirrors) test the range on the ONE version `ByLabel` / `ByType` resolve (the most recent overlapping one), while `ByLabelAndProperty` / `ByTypeAndProperty` pass the property into `findNodeVersionForOpts`'s predicate and match a value held anywhere in the interval (`graph_property_query.go:87-95`). A value in range only during an earlier part of the interval is found by the equality door and missed by the range doors. Point opts (`ValidAt`, `TxAt`, `TxPin`) agree. If predicate-anywhere is chosen: the selecting predicate must be EXACT (an over-selecting one could pick a version fn then rejects while an earlier one matched), and the ordered doors sort on the value of the matching version. Red test first: rule-16 shape (value in range on the earlier version only, interval spanning both) across all range doors.

### 20. Effective-read cost: RelAtTx benchmark targets unmeasured

*LOW (TEST-GAP, perf)* — open; only the RelAtTx benchmarks remain (everything else in the sigma request shipped, see Closed).

Missing: `BenchmarkRelAtTx/{plain,cascaded}/{hot,cold}` with targets < 1 µs plain hot row, cold row + 1 µs, ≤ 6 µs cascaded via the skeleton path (2.0 / 9.5 / 18.5 µs at v4.43.0, sigma-tkgd stream "effective"). No such benchmark exists in the tree. Add it (ReportAllocs, bench-gate; profile first: 0.15 µs of the 2.0 µs is the row read) and measure on the current release before deciding whether any work remains. Original spec: `git log -- tasks/handover-effective-read-cost-20261009.md`.

### 22. Badger rel temporal index create races concurrent writes

*MEDIUM (reproduced 2026-10-14)* — DONE (Unreleased): `CreateRelTemporalIndex` installs a Building index (writes fold in and record their IDs), builds unlocked, re-reads the recorded IDs' rows, merges and publishes; red tests `TestCreateRelTemporalIndex_WriteBetweenSnapshotAndInstall` and `_UpdateMidBuildHistoryFlushedLate` (`tasks/evidence/rel-temporal-rebuild/06-red-22.txt`).

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

### 31. Bulk and point as-of disagree with the cascade oracle on version gaps

*LOW* — open; fix when a consumer holds such chains.

**Bulk and point as-of disagree with the cascade oracle on version gaps** (LOW, pre-existing, found by the review of the bulk as-of change 2026-10-09): on a chain with a version gap above the current row (direct `Store.TruncateNodeHistory`, or a re-import over a compacted old life stored by v4.43–v4.47 (since item 38 a re-import starts above the chain's top)) both `NodeAsOf` doors miss a row above a gap past current+1 (56 cases against `cascadeAsofWant` in the reviewer's scratch test, identical on main and the branch). The allocator (item G) never creates such a gap; the density assumption is documented in `core/version_alloc.go` and AGENTS.md. Fix when a consumer holds such chains: either close the gap at truncate/re-import time (renumber under the entity lock) or make the doors scan for the max version above current. Red test first (the reviewer's five gap chains, node and rel, with/without reopen, 17 pins).


### 37. Tiered error-rollback restore leaves a window with empty history

*LOW* — open; `restoreRelHistorySnapshot` still truncates then puts (`tieredstore_write_history.go:899`).

**Tiered error-rollback restore leaves a window with empty history** (LOW, pre-existing, found by the review of the point-door race fix 2026-10-09): `rollbackDeletedRelationships` → `restoreRelHistorySnapshot` (`store/tiered/tieredstore_write_history.go:899`) runs `TruncateRelHistory(0)` and then `PutRelVersion`; on that error-rollback path a concurrent reader can see an empty history between the two calls. Fix by restoring in one shard call or under a lock readers respect (the publish-history-first rule of AGENTS.md "Version History"). Red test first: a hook between the truncate and the put, plus a pinned `RelAtTx`/`History` read there, on the tiered store.

### 39. Per-entity compaction stubs survive `Admin().Reset`

*LOW (not tested)* — open; `reapCompactionForReset` still leaves the stubs (`core/compaction.go:285`).

**Per-entity compaction stubs survive `Admin().Reset`** (LOW, found by the review of item L 2026-10-09; not tested): `reapCompactionForReset` deliberately leaves per-entity stubs (it assumes IDs are never reused); on memory an `Import` of an old ID after `Admin().Reset` can still restart at version 0 under a leftover stub, because Reset zeroes the watermark that gates the stub probe (`stubLifeStart`). Red test first: compact → Reset → Import(old ID) → `Verify*Chain` on memory/badger/tiered, node and rel. Fix: Reset reaps the stubs (it already wipes the rows), or the stub probe does not depend on the watermark.

### 40. A cascade resumption may end at an earlier life's own bound

*LOW (cosmetic)* — open.

**A cascade resumption may end at an earlier life's own bound** (LOW, cosmetic, item L review 2026-10-09): `temporal_cascade.go:334,352` (`nodeResumptionEnd` / `relResumptionEnd`) considers bounds of rows before the last tombstone, which adds an extra split row; reads are unchanged (life oracle). Fix only if row count matters: consider only rows after the last tombstone. Red first: a row-count assertion on a cascade after a re-import.

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
rule itself); the header of `pkg/graph/internal/core/effective_timeline.go` (~lines 10-20) describes resolving each piece with the point resolver, but the code (`effectivePieces`, ~line 115) uses one max-heap sweep. Also stale lesson citations: `pkg/graph/store/badger/badgerstore_property_stats.go:38` cites lesson 62 for the write-generation guard (it is lesson 63); `pkg/graph/store/store.go:30` and the `B33`/`B7`/`B36` citations in `docs/architecture.md` use an old B-numbering that no longer exists in `tasks/lessons.md`. Comment-only change; the docs-consistency tests must stay green.

### 47. Claims that need a real check before they become work

*LOW (unverified)* — carried from the 2026-10-10 handover and left unfiled by the doc review of the tasks files (group E).

(a) "`NodesByLabelAt` still folds all history instead of using the K1 sidecar": `NodesByLabelAt` is a valid-time door and K1 is
transaction-time membership; verify whether the named door can use K1 at all (a two-phase test with a churned label, memory/badger)
before filing a fix. (b) "A RAM budget for the property sidecars": v4.48.0 measured 25-30 B per posting (97-117 B near-unique) and defined
no budget; reopen only if a consumer reports memory pressure; then design a per-store budget that drops a sidecar and falls back to the fold.

### 56. Node temporal envelope holes — DONE (Unreleased)

*MEDIUM* — every case went red (`tasks/evidence/node-temporal-envelope/01-red.txt`) and is fixed (`02-green.txt`, CHANGELOG [Unreleased] Fixed). (a) `PutNodeVersion` never extended the node temporal index — badger red, memory red: fixed (a covered or Building index takes the row). (b) Badger `CreateTemporalIndex` published before its history fold — red: fixed (stays Building until the fold, late writers' history folded at the publish); memory has no such window but skipped IDs written during its scan — red: fixed (refolds their live rows). (c) Label added back to a live node — badger red, memory red: fixed (add-label doors fold history into an uncovered index). (d) Memory re-create of a deleted node with history — red: fixed.

### 57. Batched point read at (validAt, txAt) — DONE (Unreleased)

*FEATURE* — sigma-tkgd R1 (2026-10-14, design sigma-tkgd `tasks/history-via-rho-design.md`): sigma reads up to ~100 old rows per cut at one pin; `NodesEffectiveByIDs`/`RelsEffectiveByIDs` return whole timelines. Add `temporal.API.NodesAtTxByIDs(ids, validAt, txAt) ([]*types.Node, error)` and `RelsAtTxByIDs`: results aligned to ids, nil where `NodeAtTx` would return not-found or no row valid at validAt; call-level errors as `NodeAtTx` (pin checks, retention, compaction). Additive. **Done:** both doors (CHANGELOG [Unreleased] Added); each ID runs the per-ID gates and body, one read lock per 256 IDs, one store read snapshot per call; a pin is required (`txAt <= 0` → `ErrInvalidTimeRange`, as the effective by-IDs doors). Red `tasks/evidence/batched-attx/01-red.txt`, green `02-green.txt`, bench `03-bench.txt` (100 IDs, half from history: badger 293 → 207 µs, memory 48 → 47 µs). `GetNodesByIDs` is not used: it fails the whole batch on one missing ID and badger answers it with one read per ID anyway; the read snapshot is the batch win.

### 58. Tiered: tx-membership sidecars and valid-end acceleration — DONE (Unreleased)

*FEATURE* — sigma-tkgd R2: done (CHANGELOG [Unreleased] Added, `store/tiered/tieredstore_tx_members.go`, evidence `tasks/evidence/tiered-capabilities/`). Every shard's badger answer unioned; refine is a no-op on tiered; the node property form declines when the label has members off the reference shard, the rel property form always declines.

### 59. Tiered event-shard routing by mint instant

*PERF* — sigma-tkgd R3: `timestampToEventShardEntry` (`store/tiered/tieredstore_routing.go:59`) scans every event shard per ID. Binary search over the shard windows (sorted, non-overlapping) under the same lock; same fallback.

## v5

v5 is Markus Nissl's work on branch `origin/v5`, a separate Go module (`v5/`); the plan (`docs/v5/PLAN.md`, `RESEARCH-REVIEW.md`, `DISCUSSION.md`) lives on branch `v5`, not on main (our plan commit `b1193dc`, tag `v5-plan-20261009`). Do not push to `v5`; Markus merges v5 into main (René 2026-10-12), we do not; open questions for him are the rho-tkg GitHub issue #11. Maturity, merge analysis and the v4 to v5 door mapping: `docs/v5-analysis.md`; the Fable review of the branch: `docs/v5/HANDOVER.md` on branch `v5`. The older notes `tasks/review-v5-plan-vs-code-20261009.md` and `tasks/v4-changes-for-v5-importer-20261010.md` were removed from main (recover with `git log -- <path>`). v4 keeps taking consumer features (René 2026-10-09). **DECIDED 2026-09-25 (René): ADR-0011 S2 and S5 shipped on v4 as v4.39.0** (ADR-0011 §6 gate met: 24.0 / 24.8 / 25.7 B/HOP at 790 K / 3.15 M / 12.6 M); v5 carries the segments forward.

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
| Item 6 (FEATURE) — `DeleteWithTx` / `UpdateWithTx` on every write door, `ErrTxOrder` | `[4.44.0]` Added; evidence `tasks/evidence/tx-backfill-delete-update/` |
| Item 34 (FEATURE) — `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx` on every door | `[4.50.1]` Added; evidence `tasks/evidence/interval-withtx/` |
| Item 49 (HIGH, tiered) — re-import onto another shard made earlier lives unreachable | `[4.51.2]` Fixed; evidence `tasks/evidence/reimport-shards/` |
| Item 50 (MEDIUM, sigma) — pinned adjacency doors missed a re-imported rel's earlier life; AtNodes O(H) | `[4.51.2]` Added/Changed/Fixed (ex-adjacency index, store marker 3); evidence `tasks/evidence/effective-scans/50-*` |
| Item 51 (HIGH, sigma step 6) — effective scans bounded by valid end | `[4.51.2]` Added; evidence `tasks/evidence/effective-validfrom/` |
| Item 52 (HIGH, ai-soc) — durable commit under SyncWrites synced per mutation | `[4.51.3]` Changed; evidence `tasks/evidence/aisoc-commit-cost/52-*` |
| Item 53 (HIGH, ai-soc) — rel planner-stat memos one RAM entry per rel | `[4.51.3]` Fixed; evidence `tasks/evidence/planner-stats-memo/` |
| Item 54 (HIGH, ai-soc) — badger open linear in history | `[4.51.3]` Changed (open image incl. planner stats, stale-key repair); evidence `tasks/evidence/open-cost/` |
| Item 55 (MEDIUM, ai-soc + sigma) — effective scans one badger txn per rel | `[4.51.3]` Changed; evidence `tasks/evidence/effective-scans/55-*` |
| Item 21 (FEATURE, sigma) — purge relationships by age, per-type retention gate | `[4.51.5]` Added/Fixed; evidence `tasks/evidence/rel-retention/` |
| Item 28 (FEATURE perf, sigma) — state form of the as-of column doors and counts | `[4.51.5]` Added/Changed (ordinals 0 on temporal scans); evidence `tasks/evidence/state-columns/` |
| Item 41 (MEDIUM RAM) — history presence / stamps sidecars | `[4.51.5]` Changed (open-addressing columnar tables); evidence `tasks/evidence/badger-read-cost/41-0*` |
| Item 44 (MEDIUM) — commit-clock floor persisted only at Close: a crash reopened `NowTx` below stored stamps | `[4.51.4]` Fixed (commit floor persisted with the rows, `store.CommitFloorCapability`); evidence `tasks/evidence/clock-floor-crash/` |
| Item 15 (MEDIUM) — durable commit power-loss gap on a memtable switch | `[4.51.4]` docs: power loss needs `DurableCommit` + `SyncWrites` (13.5 ms vs 12.5 ms per 530-op group, `tasks/evidence/aisoc-commit-cost/52-bench.txt`) |
| Item 48 (FEATURE, sigma-tkgd) — `ColumnData.Other`: column-scan cells held outside the column (node and rel batches); typed slots aligned with `Null` | `[4.51.1]` Added; evidence `tasks/evidence/column-other/` |
| Item 35 (FEATURE, sigma-tkgd) — broader effective scans: `ForEachNodeEffective` / `ForEachRelEffective`, `ForEachRelEffectiveAtNodes`, `NodesEffectiveByIDs` / `RelsEffectiveByIDs`, `ForEachNodeEffectiveByLabels` | `[4.51.1]` Added; evidence `tasks/evidence/effective-scans/` |
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
| Item 43 (FEATURE, ai-soc) — retraction: `Retract` / `RetractWithTx` on every write door, "this was never true" | `[4.49.1]` Added "Retraction: a way to say this was never true"; evidence `tasks/evidence/retraction/` |
| Item 33 (MEDIUM, perf) — badger effective scans read each candidate row once (`store.MemberIDsCapability`) | `[4.51.1]` Changed; evidence `tasks/evidence/badger-read-cost/` |
| Item 42 (FEATURE, perf) — `store.CurrentStampsCapability`, the current-row half of `LatestStamps` | `[4.51.1]` Added; evidence `tasks/evidence/badger-read-cost/` |

Recover closed investigation prose via `git log --all -- tasks/backlog.md` if needed.

### DO NOT BUILD / reopen criteria (keep here so they are not re-filed)

- **Distinct start and end counts per relationship type maintained with the writes (sigma-tkgd round 5 request 5):
  NOT BUILT 2026-10-11.** `RelTypeDegreeStats` is derived and cached per type; since round 5 request 9 the memory
  store keys it on a per-type stamp as badger does, so the cold harness's writes to another type no longer force
  the recount that motivated the request. Maintaining exact distinct counts under writes to the type itself needs a
  per-node degree counter per type (O(distinct endpoints) RAM, about 40-50 B per endpoint in a Go map) and two map
  updates per relationship write of the type, a new write and memory cost the round's design rule only allows as
  opt-in or lazy; sigma-tkgd reverted its use of the degree statistics for the one-hop estimate pending a cost-model
  calibration (C4s). Reopen when sigma-tkgd re-adopts the degree statistics and measures the recount after writes
  to the read type itself on its cold rows; then build it opt-in per type (built lazily on the first
  `RelTypeDegreeStats` of a type that opted in, maintained at the relationship write seams).
  Re-evaluated 2026-10-14 (round 6): half met. sigma-tkgd re-adopted the statistic (`a04176971`, C4u: the weighed
  expand's fanout from `RelTypeDegreeStats`) and states the recount after a write to the type itself, but has not
  measured it on a cold row; its cold harness writes another type, which round 5 request 9 already keeps from
  forcing a recount. Still not built; the reopen criterion is the measurement.
  BUILT 2026-10-14 (round 7 request 2): measured in rho-tkg (`BenchmarkRelTypeDegreeStatsRecount`, three types of
  n relationships): the recount after a write to the read type itself is 0.7 ms at n = 10,000 and 12-16 ms at
  n = 100,000 (memory and badger) against 2-6 us for a write plus a cached read; a statement that writes a type
  and plans a hop over it pays that per statement. Criterion met; built opt-in per type
  (`Stats().TrackRelTypeDegrees`), see CHANGELOG [Unreleased]. Kept here as the record of the decision.
- **Item 9 — property-keyed unique constraint on relationships (ai-soc request 4): WITHDRAWN 2026-10-09** (René,
  relayed by ai-soc: "rho-tkg already has unique snowflake ids"). ai-soc uses the snowflake ID as the global ID.
  Possible cheap follow-up, not requested: a relationship door that accepts a caller-supplied snowflake ID "if
  absent", mirroring `Nodes().AddByIDIfAbsent` (today `Rels().AddByIDIfAbsent` dedups on `(start, end, type)`).
- **DECIDED NO 2026-10-09 (René, relayed by ai-soc):** an index-provider "failure aborts the mutation" option
  (ai-soc request 5; ai-soc rebuilds from the change feed) and one instant per commit group (ai-soc request 7; the
  cut-record pin is enough). Reopen only with a consumer case the change feed or the cut-record pin cannot cover.
- **Async ingest outcome ambiguity (R4 of the temporal-replay plan, ai-soc):** NOT BUILT, conditional. R0-R3 and R5 shipped
  (R3/R5 in v4.35.1, `store.GroupCommitCapability`; `Config.DurableCommit` in v4.44.0); the ingest `AppliedSeq` counts failed
  attempts and async failures are consumed on first wait (evicted after 8,192), so a process-local token is no durable checkpoint.
  Reopen only if ai-soc moves off synchronous sessions. Benchmark and evidence: `bench/temporalreplay/`,
  `tasks/evidence/temporal-index/` (the plan file `tasks/plan-temporal-index-ingestion.md` was removed, in git history).
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
