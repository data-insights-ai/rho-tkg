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

- [ ] 43 retraction door (`Retract` / `RetractWithTx` + GraphTx, Batch, Session twins) — NEXT STEP.
- [ ] 34 `Set{Node,Rel}VersionIntervalWithTx` on every door — restart.
- [ ] 33 + 42, then 41 — badger read cost — restart.
- [ ] 35 broader effective scans (signatures confirmed by sigma 2026-10-10).
- [ ] 28 state column fast path (design first) (waits for sigma).
- [ ] 21 retention `PurgeExpiredRels` + per-type gate (sigma, not blocking).
- [ ] Residual backlog: 0–5, 7, 10 (remaining), 13, 15, 16, 20 (RelAtTx benchmarks), 22, 23, 25, 26, 31, 37, 39, 40, 44.
- [ ] (René) delete the stale `wip/badger-scan-flush-evict` branch; named stability exceptions vs deprecation ritual.
- [ ] Next release v4.49.1 (PATCH only, René 2026-10-10: no minor versions until asked) when 43 / 34 / 33 are merged; gate, tag, notify sigma-tkgd and ai-soc.
