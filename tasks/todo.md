# todo — open work only (rho-tkg v4 main; state: HANDOVER.md). Shipped = `docs/CHANGELOG.md`; the old ledger (done rows, releases, review log, v5 WP history, release gates) = `git show aeb7f8e0:tasks/todo.md`.

## Open requests

| # | Request (user's words) | Acceptance | State |
|---|---|---|---|
| 92 | sigma 2026-10-21 FYI: "ai-soc's reopen profile shows ReadHistory, which reads through your readUnderRLock, at 526 s against 57 s for fix5. ai-soc is analysing that on their side and may come to you." | rho acts only on ai-soc's profile (rho version + callee under `fn`); `readUnderRLock` (locks.go:80) unchanged since v4.66.0 | waiting for ai-soc |
| 102 | ai-soc 2026-10-21: "a rollback, or TruncateRelHistory of many rels, that is O(n + pending)" (one failed write-back rollback > 2 h on Flux) | `TestPendingOverlayCostIndependentOfPendingSize` + `TestRollbackManyCreatedRelsLinearInPending` green → patch | deferred (René 2026-10-21: "focus on the critical path and write these items for the next step"); Next steps D1 |
| 108 | René 2026-10-22: "test again with a fresh agent and modify the md files if necessary" | cold-start test 2 (fresh agent, new questions, not test 1's 12); its gaps fixed in the owning file | open (consolidation part done by row 110) |
| 111 | René 2026-10-22: "why 15? are all files really necessary and valid and up2date? reanalyse" | per-file verdict table (needed by whom, overlap, facts vs HEAD with file:line) + resulting `git ls-files '*.md'` + docs tests green | open |
| 112 | René 2026-10-22: "and please clean up todo.md.. there should be just information what is next" | this file holds no done row; HANDOVER points here for next steps and decisions; `scripts/check-ledger.sh` exit 0; docs tests green | open (built 2026-10-22, uncommitted) |

## Next steps

Critical path (lesson 79 (10): build now only what blocks ai-soc on Flux or sigma's O1-3):

1. Rows 112, 111, then 108 (md work above), each with its acceptance check, then commit.
2. ai-soc reruns its full Flux run on v4.70.0 (row 106 fix) and sigma wires O1-3 onto `LowestAnsweredPins` (P1) and
   `RelEffectiveTimelinesByProperties` (tiered: driver = first indexed key, device before estate; segments whole; DepthAll
   only; ai-soc declares (type, device) rel indexes). Trigger to act: a consumer report against v4.70.0.
3. Row 92: wait for ai-soc's profile. Trigger: ai-soc sends rho version + callee under `fn`.
4. Watch origin (HANDOVER Appendix A) at session start; on a Markus release: merge origin/main, check his code against the
   rules (one valid start, Void rows, Elements, format markers, head cache, version allocation), fix his CHANGELOG heading.

Deferred (not critical path; build when the trigger fires):

- D1. Row 102 (badger `rangePending` walks all of pending + flushing per per-entity overlay read). Resume on local branch
  `row102-pending-ranged` (base 6ac9ea5f; 8fa7a967 = the two red tests, f4b27842 = WIP `badgerstore_write_buffer.go`,
  call sites half converted); rebase onto main first → patch. Trigger: ai-soc raises priority or the critical path is empty.
- D2. Store-wide badger cache budget on tiered (sigma FYI 2026-10-21, ai-soc BA RESHEAP 0.53 → 1.13 GB in 24 h; 0.18 GB
  ristretto under `Session.checkEntities → latestStamps`): each open shard owns its own block/index cache
  (`badgerstore_options.go:182`), so cache RAM = open shards × per-shard size. Next: one budget or a documented per-shard
  size. Trigger: ai-soc memory pressure.
- D3. Row 105 option (b): rel composite tx-membership sidecar (2,500-3,500 LOC, 4 backends). Trigger: only if option (c)
  (`RelEffectiveTimelinesByProperties`) measures too slow on BA.
- D4. Tombstone writer (`DeleteKeptValidTo`, fixes backlog 4 (ii), 7, 25; sigma P24 waits on it): branch
  `tombstone-writer-v4.63` (`37a1ff56`), rebuild on the v4.66 axis code (`stampDeleteTombstone`, `EndValidity`), ~½ day,
  a minor; heads-up to sigma + ai-soc before its tag. Trigger: decision 3.
- D5. Backlog 80-82 and the review nits not taken (v4.69.2: 4, git 82558ea9; v4.70.0: 3, git 3c370aab). Trigger: a consumer hits one.
- D6. Stale code-comment cites of todo sections that are gone (`tasks/todo.md "R3 sizing"`: tiered/badger adjacency-disk
  tests; `tasks/todo.md, Tier D`: `core/node_delete.go:353,378`): repoint on the next touch (sizing in git f94060df).

## Backlog (open)

Numbers are stable (code cites "backlog N"); a number missing here is closed (`docs/CHANGELOG.md`, or the old ledger above for
"decided not to build"). `scripts/check-ledger.sh` flags an open request citing a number missing here. Full prose:
`git show 6e20157c:tasks/backlog.md`. Scope: essential patches and ai-soc items are built, the rest waits for a consumer.

| # | Type | Problem (in code) | Next step |
|---|---|---|---|
| 0 | FEATURE | Column segments on NVMe (ADR-0011, `docs/architecture.md`; `pkg/graph/internal/segment`, `Config.RelSegments`): S0-S3, S5 done | S4 merge ((b) per-segment adjacency directory, (g) B/HOP at 12.6 M), S6, S7; sub-points (a) ai-soc S2 gate half, (d) per-row index entries, (e) bulk consumers read `ScanRelSegments`, (f) BA day on Flux, (h) page cache under `MemoryMax`, (i) reopen integrity cost |
| 1 | MEDIUM | A failed `IO().Import` should emit no change-log record; stopgap `importRollback.restoreRegistries` is correct (lesson 55); import takes `c.mu` only | needs `c.txMu` in import + a spill story for scope buffers; prize: change log costs 2.9x on import |
| 2 | TEST-GAP | Research items 22a-f: tiered crash matrix, clock skew vs rotation, fuzz tiered metadata decoders, NaN/Inf/huge-dim query inputs, race soak, `ValidationLimits` ceilings | when a consumer needs the confidence or the subsystem is touched |
| 3 | MEDIUM (perf) | `ForEachAdjacentRelAt` / `ForEachAdjacentEndpointAt` bypass badger's decode-skip and fold deleted rels O(deleted) per call | measure on sigma's temporal hop benchmarks with deleted edges first |
| 4 | HIGH? / MEDIUM? | `core/temporal.go`: (i) `NodesDuring`/`RelsDuring` `end == 0` misses future-only entities; (ii) delete of a future-ValidFrom row (reproduced `TestDeleteAheadOfClock4`, `tasks/evidence/4/`; fix = D4); (iii) `SelectAsOf` vs point resolver after a cascade; (iv) `NodeMatchesValidTime` on unset ValidFrom; (v) snowflake horizon 2034-12-02 | a failing two-phase test per finding first; drop a finding whose test passes |
| 5 | MEDIUM (perf) | P7 row compaction follow-ups (388 B per HOP row left) | (a) property key tokens, (b) compact history versions, (c) badger cache holds frozen rows, (d) string interning; measure first |
| 7 | FEATURE | `DeleteWithTx(id, t)` refuses a recorded `ValidTo >= t` (`core/tx_order.go`) | with D4 and 25; two-phase red test first |
| 10 | FEATURE | Tiered lacks node-side `TemporalCandidateCapability` | on request |
| 13 | MEDIUM | Async ingest attributes group errors by numeric id (`groupApplyError`, `core/ingest.go`): node and rel with one number land in the wrong group | red: two groups, node + rel with one id; key by (kind, id) |
| 16 | MEDIUM (contract) | Under `ValidStart`+`ValidEnd`, range folds test the resolved version, `ByLabelAndProperty` matches anywhere in the interval | decision pending; if predicate-anywhere: red across all range doors |
| 20 | LOW (perf) | `BenchmarkRelAtTx/{plain,cascaded}/{hot,cold}` missing (targets < 1 / +1 / ≤ 6 µs) | add with ReportAllocs + bench gate, measure first |
| 23 | LOW (perf) | `changeFeedPage` prefetches unused values, may read past the keyspace (`badgerstore_changelog.go:648-659`) | polling benchmark; `opts.Prefix`, conditional `PrefetchValues` |
| 25 | MEDIUM | `CloseVersion(far-1)` → pin → `Delete`: as-of rewind reopens `[1000,inf)` (`core/temporal.go:149`, `storeutil/asof_select.go:172`) | with D4 and 7 |
| 26 | LOW | Pre-allocator chains with two rows of one version keep the old order | renumber tool only if a consumer holds such chains |
| 31 | LOW | `NodeAsOf` misses a row above a version gap past current + 1 (`core/version_alloc.go`) | if a consumer holds such chains; red: five gap chains, node + rel, 17 pins |
| 37 | LOW | Tiered error-rollback restore: reader sees empty history between truncate and put (`tieredstore_write_history.go:913`) | red with a hook between truncate and put; restore in one shard call |
| 39 | LOW | Compaction stubs survive `Admin().Reset`; Import of an old ID can restart at version 0 (`core/compaction.go:285`) | red: compact → Reset → Import → `Verify*Chain`; Reset reaps stubs |
| 40 | LOW | Cascade resumption considers bounds before the last tombstone: one extra split row (`temporal_cascade.go:425,444`) | only if row count matters |
| 45 | MEDIUM (API) | `ErrNilCallback`, `ErrCommitClockExhausted`, `ErrForeignStampImplausible` without exported alias; `ErrMixedNumericColumn` outside the errors inventory test | red per sentinel via `errors.Is`; aliases + `docs/errors.md` rows |
| 46 | LOW | Stale comments: ADR-0006 cites, `badgerstore_property_stats.go:38` lesson 62 (is 63), old B-numbers, `effective_timeline.go` header | fix on the next touch |
| 47 | LOW | (a) `NodesByLabelAt` "folds history instead of K1" unverified; (b) RAM budget for property sidecars | (a) two-phase test first; (b) on consumer memory pressure |
| 71 | FEATURE | Hull of current rows in O(1)/O(log n) (`ValidHullByLabel` is one pass per call) | when a consumer measures the pass |
| 76 | LOW (perf) | Number equality lookups: one store call per width key (up to 12) | per-(label, key) set of stored widths |
| 78 | FEATURE | `EndValidity` lacks `*WithTx`, `BatchBuilder` and ingest `Session` twins (`tombstoneSpec.keepPast`) | on request |
| 80 | MEDIUM | Whole-axis entities patched while closed keep a stale current row (`cascadeNewCurrent`, `core/temporal_cascade.go:798`) | only if a consumer holds such rows |
| 81 | LOW | Sharded rel row with local start, remote end: no in-key repair at open | no requester |
| 82 | LOW | Tiered ex-adjacency of a reference node at `DepthAll` opens every cold shard | no requester |

## Open decisions (René; my default)

1. Review findings not taken under the scope rule (tests review #5-#10, code review #4-#7, docs #9-#10, old rows 77-82).
   Default: untaken until a consumer hits one.
2. v5 restart (row 61) and the first v5 tag. Default: paused; `v5.0.0-alpha.1` at S2 exit once restarted.
3. Tombstone writer rebuild (D4). Default: parked until sigma or ai-soc needs P24.
4. Retention purge vs retained pins (row 104): a purged entity answers not-found at pins it answered before. (a) no rho
   change, sigma treats not-found for a seen id as purged; (b) `ErrRetentionExpired` via a per-entity purge tombstone
   (behaviour change, deprecation ritual). Default: (a).
5. `UntimedWholeAxis` default for new stores: announced in CHANGELOG `[4.65.0]` Deprecated for "the next minor", held for
   five minors. Default: stays held; the next minor either flips it or rewords the notice.
6. Backlog 16 (range folds: resolved version vs predicate-anywhere). Default: keep today's documented behaviour.

## v5 (paused, row 61; restart needs René)

Lives on local branch `v5-takeover` (not pushed; origin/v5 = Markus `91426024` + WP-01..WP-38 merged except these). Open WPs:

- WP-16: per-partition allocation + `Graph.Owner(ctx, id)` on `OwnerOf`; red `TestOwnerIsCreatorAcrossTwoPartitions`.
- WP-18: Markus F2 only, WAL write-EIO fail-stop accepted by the classifier (raftlog + checker), then ENOSPC/fsync leaves.
- WP-22: one byte framing `v5/internal/wire` (Cursor, BoundedWriter, Seal) replacing the v5.0-path codec copies; golden bytes unchanged.
- WP-28: carry `Recorded` evidence through the dist op log and range-move export (both refuse it today); conformance v4 adapter → v4.57.1.
- WP-33: Scan streams index pages instead of materialising candidates (65,536-row cap → ErrInvalidRequest on big labels).
