# todo — v4 main ledger (state 2026-10-10, main at v4.49.0)

The live TODO is `HANDOVER.md` §11; open work with full specs is `tasks/backlog.md`; what shipped is `CHANGELOG.md`.
This file keeps the user requests with their state, one line per release, and the open queue. The detailed wave
ledgers of 2026-10-09/10 (per-step red tests, merges, reviews) are in git history: `git log -p -- tasks/todo.md`.

## User requests

| # | Request (user's words) | State | Proof |
|---|---|---|---|
| 1 | "for the next step: rho-tkg lacks is a way to say 'this was never true'.. also add it to the handover.md.. request from ai-soc" (2026-10-10) | open — NEXT STEP, not built | spec `tasks/backlog.md` item 43; `HANDOVER.md` §11 item 0 |
| 2 | "continue" — restart the two stopped agents (backlog 34; backlog 33 + 42 + 41) | open | baseline `tasks/evidence/badger-read-cost/01-baseline-latest-stamps-rotating.txt` |
| 3 | "stop all tasks and write a comprehensive handover.md" | done | `HANDOVER.md` |
| 4 | "review all md files with the current code base and keep the md files up2date" | in progress (doc review groups) | this file, `tasks/backlog.md` |
| 5 | "lets go in the v4 main" — review §6/§9 fixes, backlog 10, 11 | done | v4.44.0, v4.44.1, v4.45.0 below |
| 6 | Queue promised to sigma-tkgd / ai-soc (2026-10-09): cascade correctness, HasHistory, effective timeline, re-import, pinned lookups, LatestStamps | done | v4.46.0–v4.49.0 below |
| 7 | "reanalyse the full code and the new architecture change" (v5 plan) | done | `tasks/review-v5-plan-vs-code-20261009.md` |
| 8 | "analyse if all these changes are good and how to do it properly" / "tests always first … break the code tests" / "analyse if these changes break any existing code" (`DeleteWithTx` / `UpdateWithTx`) | done | handover `tasks/handover-tx-backfill-delete-update-20261009.md` §6, `tasks/evidence/tx-backfill-delete-update/`, v4.44.0 |
| 9 | "Monitor github, Markus is working on the v5" | stopped at the handover; re-arm if still wanted | `HANDOVER.md` Appendix C |
| 10 | Standing: "Always clean solutions to new items.. no hacks or shortcuts .. always write a no-happy-path test before the code"; no attribution lines in commits | standing | `tasks/lessons.md` 77, 78 |

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

## Open queue (mirrors `HANDOVER.md` §11; specs in `tasks/backlog.md`)

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
