# todo — v4 main ledger (state 2026-10-12, main at v4.51.1)

The live TODO is `HANDOVER.md` §11; open work with full specs is `tasks/backlog.md`; what shipped is `CHANGELOG.md`.
This file keeps the user requests with their state, one line per release, and the open queue. The detailed wave
ledgers of 2026-10-09/10 (per-step red tests, merges, reviews) are in git history: `git log -p -- tasks/todo.md`.

## User requests

| # | Request (user's words) | State | Proof |
|---|---|---|---|
| 1 | "for the next step: rho-tkg lacks is a way to say 'this was never true'.. also add it to the handover.md.. request from ai-soc" (2026-10-10) | done — v4.49.1 (de06846), hardened v4.49.2 (3b78b92) | CHANGELOG `[4.49.1]`, `[4.49.2]`; `tasks/evidence/retraction/` |
| 2 | "continue" — restart the two stopped agents (backlog 34; backlog 33 + 42 + 41) | done for 34 (v4.50.1), 33 + 42 (v4.51.1); 41 open with measurements | baseline `tasks/evidence/badger-read-cost/01-baseline-latest-stamps-rotating.txt` |
| 3 | "stop all tasks and write a comprehensive handover.md" | done | `HANDOVER.md` |
| 4 | "review all md files with the current code base and keep the md files up2date" | in progress (doc review groups) | this file, `tasks/backlog.md` |
| 5 | "lets go in the v4 main" — review §6/§9 fixes, backlog 10, 11 | done | v4.44.0, v4.44.1, v4.45.0 below |
| 6 | Queue promised to sigma-tkgd / ai-soc (2026-10-09): cascade correctness, HasHistory, effective timeline, re-import, pinned lookups, LatestStamps | done | v4.46.0–v4.49.0 below |
| 7 | "reanalyse the full code and the new architecture change" (v5 plan) | done | `tasks/review-v5-plan-vs-code-20261009.md` |
| 8 | "analyse if all these changes are good and how to do it properly" / "tests always first … break the code tests" / "analyse if these changes break any existing code" (`DeleteWithTx` / `UpdateWithTx`) | done | handover `tasks/handover-tx-backfill-delete-update-20261009.md` §6, `tasks/evidence/tx-backfill-delete-update/`, v4.44.0 |
| 9 | "Monitor github, Markus is working on the v5" | stopped at the handover; re-arm if still wanted | `HANDOVER.md` Appendix C |
| 10 | Standing: "Always clean solutions to new items.. no hacks or shortcuts .. always write a no-happy-path test before the code"; no attribution lines in commits | standing | `tasks/lessons.md` 77, 78 |
| 11 | "Start a new fable review for the V5 branch and Analyse what has been done (quality) and what is open to do to merge the V5 branch with Main .. write the result into a handover.MD into the V5 branch" (2026-10-12) | done — Fable review of origin/v5 (3ede096d) in worktree branch `v5-handover`; output `docs/v5/HANDOVER.md` (root HANDOVER.md stays the v4 handover, avoids a merge conflict); commit without attribution, pushed fast-forward to origin/v5 | `git show origin/v5:docs/v5/HANDOVER.md` (eaa339c3) |
| 12 | "Delete the old and work ok item 34 on the v4" (2026-10-11) | done | v4.50.1 (09e58d3) |
| 13 | "analyse the v5 work if we can also merge with our v4 work and how fjnsihed it is" / "Report should go in main docs" / "Add the mapping and add a section in hand over.MD" / "push all to main" | done | `docs/v5-analysis.md` §5; `HANDOVER.md` §15 |
| 14 | "can we use the v5 already?" / ask the sigma and aisoc agents | done — no (both consumers) | `docs/v5-analysis.md` §4 |
| 15 | "Why were the other items not started? Parallel Opus subagents?" + "Monitoring sonnet agent" + "Review with opus high" + "Use a fable Agent at the end" | done | v4.51.1 (dbd74cf), items 35, 48, 33, 42 |
| 16 | sigma-tkgd 2026-10-12 (consumer, relayed): valid-end bounded effective scans (backlog 51) + cheap AtNodes gather (backlog 50) for its step 6, flat restore | in progress — René 2026-10-12 "Go" (49 + 50 + 51 for v4.51.2) | `tasks/backlog.md` 50, 51 |
| 17 | ai-soc 2026-10-12 (consumer, relayed; René to ai-soc: "We have to hurry"): durable 530-op commit ≤ 0.1 s with DurableCommit+SyncWrites kept (backlog 52); ai-soc adopts v4.51.2 when tagged | measured, filed; build waits for René's "fix 52" | `tasks/evidence/aisoc-commit-cost/01-measure.txt`, `tasks/backlog.md` 52 |
| 18 | ai-soc 2026-10-12 (consumer): RSS 71 GiB at 7.67 M of 39.93 M rows, ≈57 % rel planner-stat memos (backlog 53); asks if DisablePlannerStats is safe | answered (safe; cost: stat APIs fail closed, sigma columnsExact → row feed); 53 build waits for René | `tasks/backlog.md` 53 |

## Releases (one line each; all gates `make ci-docker` exit 0, consumers build + vet)

| Tag | Commit | Content (backlog items) |
|---|---|---|
| v4.44.0 | f7cd0ba | `DeleteWithTx` / `UpdateWithTx` / `ErrTxOrder` on every door (6), one-tick spans, column + range scans answer temporal opts, ingest `Set*VersionInterval`, `Config.DurableCommit` (11), go 1.26.9 |
| v4.44.1 | 91d7c8c | badger History iterators bounded to the entity prefix (20 fix 1a) |
| v4.45.0 | b30f66f | tiered composite + rel temporal indexes, hot+warm bound (10) |
| v4.46.0 | fac763a | cascade correctness (14, 18, 19, 24, 20 fix 2a), pin-stable as-of rule, `HasHistory` (20 fix 1b), unique on cascade patches (12), bulk as-of presence |
| v4.47.0 | b3eb884 | `Node/RelEffectiveTimeline` + scan forms (20 fix 1c, 27), `MintInstant` (36), point-door race fix (32), read-time supersession rule, life ordering |
| v4.48.0 | bd787bd | pinned property lookups via the tx-membership sidecar (8), re-import continues versions across lives (38) |
| v4.49.0 | aedc56d | `Nodes/Rels().LatestStamps` (30), every `UniqueForever`-claiming door withdraws claims on failed writes (29) |
| v4.49.1 | de06846 | retraction `Retract` / `RetractWithTx` on every write door (43) |
| v4.49.2 | 3b78b92 | retraction rows at fv=3 (old binaries fail closed), batch rel delete queue order |
| v4.50.1 | 09e58d3 | `Set{Node,Rel}VersionIntervalWithTx` on every door (34) |
| v4.51.1 | dbd74cf | broader effective scans (35), `ColumnData.Other` (48), current-row stamps + ID-only effective gathers on badger (33, 42); on top of Markus's v4.51.0 |

## Open queue (mirrors `HANDOVER.md` §11; specs in `tasks/backlog.md`)

- [ ] v4.51.2 batch (René 2026-10-12 "Go" on 49/50/51; three Opus build agents in worktrees, one item each; one Opus-high review each; final Fable review; one `make ci-docker`; PATCH tag v4.51.2):
  - [x] 49 merged 650a616d (review Opus high: merge after fixes; fix round 76ea1660: generated batch skips note, one-time backfill, node-door tests, single-walk probe). 49 in code? `pkg/graph/store/tiered/tieredstore_read_history_rel.go:98-103` (history read goes to the live row's shard only). Red test `TestReimportAcrossShardsKeepsEarlierLife` (memory/badger/sharded/tiered); break cases: move A-shard→B-shard and back, node re-import, two re-imports, pin inside each life for History/RelEffectiveTimeline/RelAsOf/ForEachRelEffective/RelsEffectiveByIDs. Proof: red output then green under `tasks/evidence/reimport-shards/`.
  - [x] 50 merged 9909cc82 (review Opus high: merge after fixes; fix round redesigned to a store-kept ex-adjacency index, AtNodes 0.8 ms vs by-IDs 0.8 ms badger at 100 K; badger store marker 3 = v4.51.1 and older refuse to open). 50 in code? `pkg/graph/internal/core/temporal.go:789-793` (candidates = current ∪ deleted only); `effective_scans.go:236` (AtNodes folds every rel with history). Red tests `TestPinnedAdjacencyFindsReimportedEarlierLife` (every adjacency door, 4 backends) + one-seed cost gate `BenchmarkEffectiveAtNodesOneSeed` (100 K rels with history: AtNodes within a constant factor of the by-IDs door); break cases: re-import with other src, other dst, same endpoints, re-import twice, deleted-never-reimported, reopen on badger (persisted re-import set). Proof: red output, bench before/after under `tasks/evidence/effective-scans/`.
  - 51 in code? `pkg/graph/internal/core/effective_timeline.go:639`, `effective_scans.go:414` (enumerate by type/label over all history). Red test `TestEffectiveByTypeValidFromCostFollowsResult` (sigma's acceptance: 1k vs 10k retired, constant matches, entities read constant on memory and badger) + semantics table; break cases: validFrom exactly at a valid end (half-open), untimed entity included, later correction moving valid end below validFrom (pin before vs after), retracted, deleted, tiered/sharded parity, validFrom = 0 equals the unbounded door. Proof: red output, entities-read counts under `tasks/evidence/effective-validfrom/`. **Built on the agent branch (not merged):** `ForEachRelEffectiveByTypeFrom` / `ForEachNodeEffectiveByLabelsFrom` + RAM valid-end index (memory, badger; tiered/sharded unaccelerated). RED 1,005 / 10,005 entities read (`01-red.txt`) → GREEN 5 with 1,000 or 10,000 deleted on memory and badger (`02-green.txt`; closed-only retirement read once, then 5); 14 mutants, 13 killed + 1 equivalent (`06-mutants.txt`); short suite, race on touched packages, lint-docker, security-docker exit 0. Waits for the Opus-high review.
- [x] 43 retraction door — released v4.49.1/v4.49.2.
- [x] 34 `Set{Node,Rel}VersionIntervalWithTx` on every door — released v4.50.1 (09e58d3, gate scratchpad ci-docker-v4501.txt exit 0, coverage 87.4%), sigma-tkgd and ai-soc notified 2026-10-11.
- [x] 33 + 42 released v4.51.1 (c62d995; review Opus high: merge after F1 test + F2 sharded nativeStoreTypes, both done). 41 left open with measurements (merge saves only where both doors run).
- [x] 48 `Other` marker on `ColumnData` — released v4.51.1 (1357c2f + review fixes 5890157 optional RelColumnOtherSource, 3aaaf74 integer widths agree); review Opus high: merge after fix. In code? `store/capabilities.go:1268` has Null only; rel `SegmentColumnValues.Other` at `store/rel_segments.go:259`. Red: per backend a list/map/kind-mismatch/missing cell vs Row(k) class; break-the-code: Other never set, set for missing cell, wrong column index, stale after update. Proof: `tasks/evidence/column-other/`.
- [x] 35 broader effective scans — released v4.51.1 (AtNodes O(H) per call documented; cheap gather with backlog 50).
- [ ] 28 state column fast path (design first) — waits for sigma's design input; not started.
- [ ] 21 retention `PurgeExpiredRels` + per-type gate (sigma, not blocking).
- [ ] Residual backlog: 0–5, 7, 10 (remaining), 13, 15, 16, 20 (RelAtTx benchmarks), 22, 23, 25, 26, 31, 37, 39, 40, 44.
- [ ] (René) stale `wip/badger-scan-flush-evict` branch deleted 2026-10-11; named stability exceptions vs deprecation ritual.
- [x] Reviews of 33/42/41, 35, 48: one round each, Opus effort high (René 2026-10-12: "Review with opus high"); Sonnet watchdog runs over the three build agents.
- [x] Final Fable review of the v4.51.1 batch (first prepared as v4.50.2; Markus released v4.51.0 meanwhile): tag, 3 LOW (2 doc fixes applied, nil-cell Other carried) (René 2026-10-12: "Use a fable Agent at the end").
- [ ] 4.51.2 candidate: a committed test for a patched badger column snapshot vs Other (merge review, LOW).
- [ ] 4.51.2 candidate (Fable final review 2026-10-12, LOW, not reproduced): nil-valued properties are Other on the row drivers but absent on badger's columnar snapshot (index/docvalues.go:588 vs column_batch_build.go); red test first before filing.
- [ ] Next release v4.51.2 (v4.51.1 released 2026-10-12 on top of Markus's v4.51.0; v4.50.1 released 2026-10-11; PATCH only; v4.50.0 was released by Markus 2026-10-10, René 2026-10-10: no minor versions until asked) when 43 / 34 / 33 are merged; gate, tag, notify sigma-tkgd and ai-soc.
