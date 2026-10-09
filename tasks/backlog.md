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

**Remaining open work:** no CRITICAL or HIGH items (item 3 closed 2026-09-24; 12, 14, 18, 19, 24 and the 2a half of 20 closed 2026-10-09). Open:

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

10. **Tiered store: composite indexes and relationship temporal indexes** (FEATURE, ai-soc request 3; DECIDED YES 2026-10-09). BUILT on branch `worktree-agent-a143172e71e31fafe`: per-shard fan-out anchored by the reference shard; measured rel temporal 156 B/relationship, composite 310 B/node, rebuild 0.018–0.021 M rels/s (CHANGELOG Unreleased). Bound (default): the rel temporal index lives on reference + hot + warm shards only (cold demotion frees it, `PromoteColdShardsAtOpen` rebuilds it), ≈ 5.8–9.2 GB per indexed type at `ColdAfter` = 1 day. Remaining: (a) a sequential-scan rebuild — today one point read plus one history prefix scan per relationship, 29–54 min per day shard on reopen or promotion; (b) relationship property indexes and the node-side `TemporalCandidateCapability` are still declined on tiered.

11. **Durable-on-return commit** (FEATURE, ai-soc request 9; DECIDED YES 2026-10-09, René, relayed by the ai-soc session): `GraphTx.Commit` and `Batch.Execute` flush the badger pending buffer before returning, one WriteBatch per call, so a crash can lose only whole uncommitted groups and `SyncWrites` is not needed per mutation. Today neither flushes (`core/tx.go`, `core/batch_execute.go`); the pending buffer is a map flushed in map order (`store/badger/badgerstore_flush.go:166-176`) and badger splits an oversized WriteBatch into several internal transactions, so without `SyncWrites` a crash can persist an arbitrary subset of a group. Design points: opt-in (`Config` flag or a commit option) vs default; the tiered and sharded stores flush every shard the group touched; a group above badger's transaction size limit is still split — document or refuse; an all-or-nothing on-disk guarantee stays v5 (PLAN §5.2). Red tests: crash-shaped (SIGKILL or a closed-without-flush store) after Commit returns shows the whole group; after a crash before Commit either the whole group or none is on disk is NOT promised and the test says so. Scheduling: René.

13. **Ingest applier attributes group errors by numeric entity id, not by kind** (MEDIUM, found 2026-10-09 writing `TestSessionSetVersionInterval_FailedGroupShape`): the strong-async applier keys group-error attribution by the numeric id, so a missing node id and a missing rel id with the same number attribute errors to the wrong group. Minted snowflake ids never collide across the two generators, but caller-supplied ids (`AddByID`, `Import`) can (the rollback-snapshot rule in AGENTS.md "Data Model" already keys by kind + id for the same reason). Red test first: two groups, a node and a rel with the same numeric id, each missing; each group gets its own sentinel. Fix: key by (kind, id).

16. **Range folds under an interval: predicate-anywhere or value at the resolved version?** (MEDIUM, contract inconsistency, found reviewing item C scan-temporal-opts 2026-10-09). Decide whether all range folds (ordered included) pass the range into the resolver's predicate (predicate-anywhere) like `ByLabelAndProperty`. Today, under `ValidStart`+`ValidEnd`, `Nodes().ForEachByLabelPropertyRange` / `Rels().ForEachByTypePropertyRange` and the ordered / prefix siblings (`forEachNodeInRangeTemporal`, `forEachNodeValueOrderedTemporal` and rel mirrors) test the range on the ONE version `ByLabel` / `ByType` resolve (the most recent overlapping one), while `ByLabelAndProperty` / `ByTypeAndProperty` pass the property into `findNodeVersionForOpts`'s predicate and match a value held anywhere in the interval (`graph_property_query.go:87-95`). A value in range only during an earlier part of the interval is found by the equality door and missed by the range doors. Point opts (`ValidAt`, `TxAt`, `TxPin`) agree. If predicate-anywhere is chosen: the selecting predicate must be EXACT (an over-selecting one could pick a version fn then rejects while an earlier one matched), and the ordered doors sort on the value of the matching version. Red test first: rule-16 shape (value in range on the earlier version only, interval spanning both) across all range doors.

15. **Durable commit: power-loss gap on a memtable switch** (MEDIUM, follows item 11, found by the item-11 review 2026-10-09): `Config.DurableCommit`'s `DurableFlush` ends with badger `db.Sync()`, which fsyncs only the active memtable's WAL and the current value-log file (badger v4.9.2 `db.go:705-709`). A flush that fills the memtable switches to a new one in `ensureRoomForWrite` (`db.go:1015-1040`) without syncing the retired WAL, and a finished value-log file is synced only under `SyncWrites` (`memtable.go:408`), so rows of a group that crossed a switch can be unsynced after `DurableFlush` returned: process-crash safe, not power-loss safe (documented on `Config.DurableCommit`, `store.DurableFlushCapability`, docs/architecture.md "Durable-on-return commit"). Options: (a) sync the retired memtable WAL (and the finished vlog file) on switch when a durable flush is in progress — needs a badger hook or a fork; (b) measure `SyncWrites` cost against `DurableCommit` on a group workload and recommend it for strict durability. Red test first: a group larger than one memtable, a power-loss-shaped check (fsync accounting or a dm-flakey/`LD_PRELOAD` drop of unsynced pages). Scheduling: René.

**DECIDED NO 2026-10-09 (René, relayed by the ai-soc session):** an index-provider "failure aborts the mutation" option (ai-soc request 5; ai-soc rebuilds from the change feed) and one instant per commit group (ai-soc request 7; the cut-record pin is enough). Reopen only with a consumer case the change feed or the cut-record pin cannot cover.

20. **Delete after a bounded cascade leaves the entity readable; badger History reads cost a value prefetch** (HIGH, found by sigma-tkgd 2026-10-09, reproduced on memory, badger, tiered): same root as item 18 for the as-of half. Handover with causes (file:line), fixes 1a/1b/1c and 2a, acceptance tests and benchmark targets: `tasks/handover-effective-read-cost-20261009.md`. The one-tick claim is already fixed on main (`30cfea0`). **Fix 2a closed 2026-10-09** (wave 3 item G; CHANGELOG [Unreleased] Fixed "A delete after a bounded cascade ends the entity in every door"); the history-cost half (1a-1c) stays here.

21. **Purge relationships by age, and gate retention reads per type** (FEATURE, requested by sigma-tkgd's temporal overview design 2026-10-09; first filed as item 10 in c8de314 and dropped by my backlog edit in f952015, restored here under a new number because 10 is now the tiered indexes): `Admin().PurgeExpiredRels` with a watermark per relationship type, a retention scan gate that a purge of the raw event type does not trip for the long-lived `Overview` nodes, and the tiered landing place of a long-lived node's new versions (answered from the code). Handover with verified citations, API, backend and change-feed semantics, and the red tests to write first: `tasks/handover-overview-retention-20261009.md`.

22. **Badger rel temporal index create races concurrent writes** (MEDIUM, pre-existing, NOT reproduced — read from code, review of backlog 10, 2026-10-09). `CreateRelTemporalIndex` (`store/badger/badgerstore_reltype_temporal_index.go`, the create body) snapshots the type's relationship IDs under `idxMu`, builds the envelope index unlocked and installs it; a relationship created or updated in the gap is not folded in, because `ExtendRelInTemporalIndexes` returns early while no index exists — lesson 74's lazy-build rule. A missed new relationship is merely uncovered (kept); a missed update of a covered one can leave its envelope short of a row, i.e. an unsound prune. On tiered this now runs against the hot shard while it ingests. Red test first with a write-generation guard (lesson 63): write between snapshot and install, then prune at the written row's interval; fix with the 3-phase mutated-set install `CreateTemporalIndex` uses. The header comment was corrected (it claimed the scan held `idxMu`).

23. **`changeFeedPage` prefetches past its page** (LOW, perf, found by the review of the history-prefix fix 2026-10-09; not measured): `store/badger/badgerstore_changelog.go:654-659` iterates with `PrefetchValues = true`, no `opts.Prefix`, `Seek(start)` then `ValidForPrefix(ChangeLogPrefix())`: a small-limit poll near the tail prefetches up to 100 values it never uses and may read past the change-log keyspace. Fix: `opts.Prefix = prefix` before `NewIterator`, and `PrefetchValues = limit == 0 || limit > 100`. Measure with a polling benchmark first (replica watchers poll it).

25. **A future close followed by a delete changes the answer at an earlier pin** (MEDIUM, pre-existing on main, found by the review of the cascade-correctness branch 2026-10-09): `CloseVersion(far-1)` → pin → `Delete`: `AsOf(pin)` changes from `[1000,far-1)` to `[1000,inf)` and `AtTx(far, pin)` from absent to present, because `stampDeleteTombstone` clamps the close to the delete instant (`core/temporal.go:136`) and the as-of rewind at `core/txtime.go:501` reopens it to 0 (the same "known limitation: future-scheduled close, then delete" of the 2026-09-24 review, now that the life-end cap exists the delete could keep a finite ValidTo). Red test first: two-phase on all four backends, node and rel; fix: the tombstone keeps the believed ValidTo beside the deletion instant (see item 7).

26. **Old chains with collided versions keep answering in the old version order** (LOW, known limit, documented in the v4.46.0 CHANGELOG): chains written before the version allocator (backlog 18b) that already hold two rows with one version cannot be repaired by the read rule: e.g. old S2 followed by two old Updates answers the patch row at the pin after the first Update. A one-off repair tool (walk chains, renumber) is possible without a format change; schedule only if a consumer holds such chains.

27. **Scan forms of the effective timeline** (FEATURE, requested by sigma-tkgd 2026-10-09, after `NodeEffectiveTimeline` / `RelEffectiveTimeline`, handover effective-read-cost fix 1c): `ForEachRelEffectiveByType(type, pin, fn func(RelSegment) bool) error` and the node twin by label; same contract, entity order free, segments of one entity contiguous and ascending.

28. **State form of the as-of column doors and counts** (FEATURE, requested by sigma-tkgd 2026-10-09 after it moved every pinned read to the state doors): pinned COUNT and aggregate queries lost the as-of column path because `DocValuesSnapshotAsOf` / the as-of column set / `ScanNodeColumns` as-of are record doors. Since v4.46.0 `ScanNodeColumns`/`ScanRelColumns` answer `ValidAt + TxAt` exactly (the ByLabel/ByType fold, item C), but at row-fold cost. Wanted: a cached state column set keyed by (label, valid-at, pin) (or a segment-based cut) so `CountByLabelAt`/column scans with `ValidAt + TxAt` regain the as-of column speed. Design first (cache key explosion over valid-at; reuse the effective-timeline cut logic of item J); measure sigma's pinned count at 1 M before building.

29. **Update door: UniqueForever claim kept after a failed write** (MEDIUM, same fault family as the cascade fix of item I, found by its author 2026-10-09; being confirmed by the review): `checkAndClaimForever` (core/unique_forever.go) and `enforceUniqueForNodeHeld` keep the claims when a later claim in the same call fails, and a failed update store write is never answered with a withdrawal ("correctable via ReleaseOwnership"). Mirror the cascade fix (`claimForever` / `withdrawForeverClaims`). Red tests first on all four backends (injected store failure after the claim); crash consistency between the persisted claim and the row write needs an atomic MetaKV+store commit (v5 §5.2).

30. **`LatestStamps(id)`: newest stamps of an entity in O(1)** (FEATURE, requested by sigma-tkgd from ai-soc's real-data profile 2026-10-09: the cut check "written after the pin" (ErrCutAhead) and late-belief detection read a full History per entity per cut, 8.3 ms at 10,000 versions, 88 % of the CPU of ai-soc's BA run): `Nodes()/Rels().LatestStamps(id) (txFrom, txTo types.Instant, deleted bool, err error)` = max TxFrom and max TxTo over ALL rows of the entity (head, history, appended cascade rows, tombstone) plus deleted, O(1) or O(log history) on every backend. The head row is not enough (appended cascade rows are stamped after the head's TxFrom while the head stays; ended entities have no head). Design notes before the red tests: TxFrom is not co-monotonic with version (`validInstantAfter`, tx_order.go), so "the top-version row" does not carry the max; the stamps live in row values, not keys, so the HasHistory presence set (IDs and, with the pending bulk as-of work, top version) needs a small per-ID stamp sidecar (max TxFrom, max TxTo, deleted: ~16 B/ID) maintained at the single place every history/current row passes (the `noteHistoryKey` point for history rows plus the current-row put/replace/delete doors), built lazily with the same write-generation guard (lessons 63/74), invalidated to "unknown" on trim/compaction/purge/rollback; memory store computes from the maps; tiered/sharded route like History. Agree with the effective-timeline (item J) cut logic so one release carries both if it fits; else directly after. Red tests: differential vs a full History scan over the oracle generators on four backends (create/update/cascade/close/delete/DeleteWithTx/compaction/purge/rollback/reopen/Clear), mutants for each maintenance point.

31. **Bulk and point as-of disagree with the cascade oracle on version gaps** (LOW, pre-existing, found by the review of the bulk as-of change 2026-10-09): on a chain with a version gap above the current row (direct `Store.TruncateNodeHistory`, or a re-import over a compacted old life) both `NodeAsOf` doors miss a row above a gap past current+1 (56 cases against `cascadeAsofWant` in the reviewer's scratch test, identical on main and the branch). The allocator (item G) never creates such a gap; the density assumption is documented in `core/version_alloc.go` and AGENTS.md. Fix when a consumer holds such chains: either close the gap at truncate/re-import time (renumber under the entity lock) or make the doors scan for the max version above current. Red test first (the reviewer's five gap chains, node and rel, with/without reopen, 17 pins).

32. **Point as-of doors miss an entity while a concurrent write moves its row to history** (HIGH?, pre-existing on main, found by the effective-timeline pointwise test 2026-10-09; evidence `tasks/evidence/effective-timeline/finding-point-door-race-*` once merged): `NodeAtTx`/`RelAtTx` at a FIXED pin return ErrNoVersionValidAt/not-found for an entity whose Update, CloseVersion or Delete is moving its current row into history at that moment (40 misses in 30 runs on badger and sharded): the reader sees neither the old current row nor the new history row. The effective-timeline door holds the entity lock for this reason (mutant 9). Fix at the shared seam rather than by locking every point read blindly: either a read-side retry on a flush/epoch change (lesson 64 pattern: snapshot the overlay BEFORE the badger view), or the entity lock after an audit of callers that already hold entity locks. Red test first: writer loop (Update/Close/Delete) racing a fixed-pin NodeAtTx/RelAtTx on all four backends under `-race`, zero misses allowed.

33. **Badger effective-timeline scan costs 13-17 µs per relationship** (MEDIUM, perf, from the review-pending item J numbers: ≈ 3 s for 200 K rels vs 1.3 µs/rel on memory): the pinned candidate gather decodes every current row and each rel's row is then read again. Reuse the rows the gather decoded, or gather candidate IDs only (key scan) and read once. Measure with `BenchmarkForEachRelEffectiveByType` before and after.

34. **`SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx`: a caller instant for interval rewrites** (FEATURE, requested by sigma-tkgd 2026-10-09 from the burst-growth replay): a replayed burst growth `[vs,ve) → [vs,ve')` cannot be stamped at the record's own transaction instant because the append-only cascade (`Temporal().SetXVersionInterval`, `GraphTx`, `BatchBuilder`, ingest `Session`) stamps `TxFrom = now`. Mirror `DeleteWithTx`/`UpdateWithTx` (handover tx-backfill §6): gate `AllowTxBackfill`, `ErrInvalidTxFrom` for non-positive or future instants, `ErrTxOrder` against the entity's own chain (t must exceed the newest `TxFrom` and every appended piece takes the same caller instant), one instant for every appended row of the cascade, `notePastDatedWrite` for the as-of cache, whole-unit pre-flight in Batch/ingest, replica reproduces the stamps. Red tests first on all four backends, node and rel, every door (the W5 oracle gets the new door in its generator).

35. **Broader effective scans** (FEATURE, requested by sigma-tkgd 2026-10-09, exact call patterns from its builtins and push-down; N/M = nodes/rels at the pin, K up to 500 k rels from a push-down, S up to 100 k seed nodes, E entities per session cut): after `ForEachRelEffectiveByType` / `ForEachNodeEffectiveByLabel` (item J), all with the same contract as the per-entity door (one entity's segments contiguous and ascending, entities deleted before the pin included, fn false stops): (1) `ForEachNodeEffective(pin, fn func(NodeSegment) bool)` and `ForEachRelEffective(pin, fn func(RelSegment) bool)`, label- and type-free, one call each per evaluation (builtins node/2, edge/3, prop/3 today: a TxPin scan that misses entities deleted before the pin plus one timeline call per row, twice when prop is used); (2) `ForEachRelEffectiveAtNodes(nodeIDs []types.NodeID, dir Direction, relTypes []string, pin, fn func(RelSegment) bool)` batched for the src_/dst_ push-down (today `Outgoing/IncomingForNodesAtPin`, which misses rels deleted before the pin, plus K timeline calls), direction fixed per call, deleted-before-pin rels included, each rel once even if both endpoints are seeds (a single-node form would mean up to 100 k calls); (3) `NodesEffectiveByIDs(ids []types.NodeID, pin, fn)` and `RelsEffectiveByIDs(ids []types.RelID, pin, fn)`, unknown IDs skipped, one call per @source or per cut and kind; (4) optional, low priority: `ForEachNodeEffectiveByLabels(labels []string, pin, fn)`, each node once even with several matching labels. Red tests first: exact-set per pin on the oracle generators (live, closed, deleted-before-pin, deleted-after-pin, created-after-pin; rels with both endpoints seeded; duplicate IDs in a by-IDs batch), memory/badger/tiered/sharded.

36. **Public derivation of the derived valid-from (mint instant) of an ID** (FEATURE, small, requested by sigma-tkgd 2026-10-09; prerequisite for its adoption of the "0 = unset, the start is the mint instant" rule on unpinned reads, column scans and Get rows, which do not carry the derived start): one public function that is the SAME function the resolver uses (`storeutil.EntityValidFrom` without metadata / `SnowflakeInstant`, backed by `snowflake.Layout.CreatedAt`, in `pkg/graph/internal/`, unreachable from `pkg/types`): e.g. `types.NodeID.MintInstant()` / `RelID.MintInstant()` if the layout can live in `pkg/types` without leaking the dependency (AGENTS.md rule 12), else `g.Temporal().DerivedValidFrom(...)`. Decide the home by what the package graph allows; red tests: equals the resolver's derived start for IDs from every generator (node/rel, all SnowflakeNodeID 0-15, tiered and sharded IDs), monotone in ID order, zero/negative IDs refused.

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
| Item 14 (HIGH) — a correction on a deleted entity copied the tombstone stamps; appended rows kept a superseded row's `TxTo`. DECIDED 2026-10-09: refuse with `ErrEntityDeleted` | CHANGELOG `[Unreleased]` Changed "A valid-time correction on a hard-deleted entity is refused", Fixed "Rows a write appends carry no retraction"; tests `TestCascadeOnDeletedEntityRefused`, `TestCascadeAppendedRowsCarryNoRetraction` |
| Item 18 (HIGH) — cascade row versions collided with later Updates; as-of answers at a pin changed after a later write | CHANGELOG `[Unreleased]` Changed "The as-of doors answer the newest row recorded by the pin", Fixed "One version allocator", "The chain resolver reads chains in version order"; test `TestCascadeVersionsUniqueAndPastStable`; W5 skip `txbTangled` removed |
| Item 19 (HIGH) — GraphTx rollback dropped the cascade rows above the current version | CHANGELOG `[Unreleased]` Fixed "GraphTx rollback keeps the cascade rows above the current version"; tests `TestTxRollbackKeepsCascadeRows`, `TestTxRefusedUpdateWithTxTakesNoSnapshot`; W5 skip `txbNoTxRollback` removed |
| Item 20, fix 2a (HIGH) — a delete after a bounded cascade left the entity readable (as-of and valid time) | CHANGELOG `[Unreleased]` Fixed "A delete after a bounded cascade ends the entity in every door"; tests `TestAsOfBoundedCascadeThenDelete`, `TestValidTimeAfterDeleteBoundedCascade`, `TestOneTickCascadeRows` (handover acceptance 1, 2, 4) |
| Item 24 (HIGH) — history compaction broke the hash chain (and export/import) of a cascaded entity | CHANGELOG `[Unreleased]` Fixed "History compaction keeps every row the kept chain's hash links point to"; test `TestCompactionKeepsPrevHashAnchors` |
| Item 12 (HIGH) — unique constraint bypass via `SetNodeVersionInterval` props on all four doors; enforced in the cascade kernel (open-ended patch: both scopes; bounded: `UniqueForever` only), 2026-10-09 | CHANGELOG `[Unreleased]` Fixed "unique constraints are enforced on `SetNodeVersionInterval` props patches" |

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
