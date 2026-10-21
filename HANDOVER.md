# HANDOVER — rho-tkg v4 line (2026-10-21: latest tag v4.69.1, row 100 race fix; v4.69.0 row 99 IsReferenceLabel)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). This file owns state and next steps only. Rules: `tasks/lessons.md` (digest at the top). Requests, ledger, releases,
decisions: `tasks/todo.md` (the ONE plan). Open items: `tasks/backlog.md` (open table). What shipped: `CHANGELOG.md`. Read in
this order: this file, lessons digest, todo (user requests, releases table), backlog open table, `CHANGELOG.md` top.

## 1. Next steps (focus René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC")

Critical path first (René 2026-10-21: "for now we should focus on the critical path and write these items for the next step then"):
only items that block ai-soc on Flux or sigma get built now; the rest is listed under "Next step (deferred)" below.

1. **Waiting for ai-soc** (row 92): reopen `ReadHistory` 526 s vs 57 s; rho's `readUnderRLock` is unchanged since v4.66.0; rho acts
   on ai-soc's profile only.
2. **Open work:** the open table of `tasks/backlog.md` (80-82 from the v4.66/v4.67 reviews; nothing with a requester).
3. **Watch origin:** `WATCH_LOG_DIR=<scratchpad> scripts/watch-origin.sh` in the background (Appendix A). When Markus releases on
   main: merge origin/main, check his code against our rules (one valid start, Void rows, Elements, format markers, head cache,
   version allocation), fix his CHANGELOG heading if he left entries under [Unreleased].

### Next step (deferred, not critical path)

- Row 102: badger `rangePending` walks the whole pending + flushing buffer per per-entity overlay read (history, disk-mode
  adjacency) → rollback of n rels costs n × pending (ai-soc Flux: one failed write-back rollback > 2 h; normal pushes 0-0.64 %).
  Resume on local branch `row102-pending-ranged` (red tests 8fa7a967 + WIP) → patch.
- Backlog 80-82 (review findings, no requester).

## 2. State

- **Scope:** just essential patches, except ai-soc items (standing go, René 2026-10-21); lesson 79 (10).
- **Tombstone writer** (`DeleteKeptValidTo`, fixes backlog 4/25/7): branch `tombstone-writer-v4.63` (`37a1ff56`), parked ("not
  now", René via sigma 2026-10-19); needs a rebuild on the v4.66 axis code (Markus's axis-aware `stampDeleteTombstone`,
  `EndValidity`), ~half a day, a later minor. v4.64.0 was never cut. sigma's P24 (marker-aware ValidTo reads) is parked with it.
- **v5 PAUSED** (René 2026-10-16, row 61): local branch `v5-takeover` (not pushed; origin/v5 untouched). Do not resume without René.
  Reason: real ai-soc import showed ~92 records/op, 11x v4 disk, ~200 writes/s. Gap analysis: `docs/v5-gap-to-main.md`. Real-store
  harness + scale notes: local branches `v5-realstore-harness` (`af75a982`, `tasks/evidence/v5-scale/notes.md`) and
  `wp-37-import-throughput` (`392f88e2`); not pushed. v5 façade waits for Markus; no posts on GitHub issue #11 (René tells Markus).
- **Consumers** (answer new requests the same day: ledger row first, then lesson 79):
  - ai-soc: on rho v4.67.0+ with tiered as the main store (`AdjacencyIndexOnDisk`, `RelsByMint`, `SyncWrites`, `DepthWarm` on
    windowed reads); declares `CreateRelTemporal` on its 29 estate-indexed rel types and sets `UntimedValidStart:
    UntimedUnbounded` for new stores. (PINnn = ai-soc's numbered dependency pin; BA/BO = one customer data day; `/bars`,
    `/tkg/asof` = its HTTP read doors.)
  - sigma-tkgd: v0.11.46 (2026-10-21, CI green) on rho v4.69.1: `g.Admin().IsReferenceLabel` at writer, read doors and tkgd startup; TestFoldReadDepthHot race-free (rows 99, 100 accepted)
    Derived/Session lookups); adopts v4.69.1 when sent, moving its reference-label check into session/writer open (F0b).
  - Notify after gate + tag + push: `sigma-tkgd-c3` and `ai-soc-main-e7` (`ListAgents`, main session only; socket names change
    per session). If neither resolves, put the release note here and tell René in one line.
- **Housekeeping:** session scratchpad emptied 2026-10-20 (row 76); deletions stay René's call (`rm -rf` is denied to agents).
  Docs check: `go test -count=1 -run 'Docs|Consistency' ./...`. Consumer build check: Appendix B (sigma-tkgd, ai-soc,
  agent-bookkeeping).

## 3. René's open decisions (my default)

1. Review findings not taken under the scope rule: tests review #5-#10, code review #4-#7, docs #9-#10 rest (todo rows 77-82).
   Default: stay untaken until a consumer hits one.
2. v5 restart (row 61) and the first v5 tag (todo decision 4). Default: paused; alpha at S2 exit once restarted.
3. Tombstone writer rebuild (above). Default: parked until sigma or ai-soc needs P24.

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main/engine`,
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
