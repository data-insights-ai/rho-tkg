# HANDOVER — rho-tkg v4 line (2026-10-20, latest release v4.66.0 (Markus); v4.66.1 patch in preparation; v4.64.0 never cut, writer parked)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). Latest tag v4.66.0 (2026-10-18, Markus; v4.65.0 was round 11; the `UntimedWholeAxis` default flip is held, no target). Tags and what shipped: `CHANGELOG.md`, git and the
releases table of `tasks/todo.md`, not here. The ONE plan is `tasks/todo.md`. Read in this order: this file, `tasks/lessons.md`
(digest at the top), `tasks/todo.md` (user requests, releases table), `tasks/backlog.md` (open table, then specs), `CHANGELOG.md` top.

## 1. Focus and next steps (René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC")

1. **René's last open request (2026-10-20): "then prepare a patch version release" (row 78) — v4.66.1.** Inputs come from his
   "full opus code, tests, documentation and README.md run with separate agents" (row 77). State:
   - On main (local, push before the gate): README fixes (`20953507`), docs-review fixes (`74e83c6f`), row 82 recurrence span
     guard (`4bf41957`). CHANGELOG [Unreleased] holds the Fixed/Changed lines.
   - Agents running in worktrees `.claude/worktrees/agent-*` (each commits on its `worktree-agent-*` branch, no merge): row 79 tiered
     whole-axis node lost after demote (`TestEffectiveHeadWrite_TwinDifferential/tiered/whole-axis/seed=26800` under -race),
     row 80 whole-axis untimed write on an all-closed entity leaves the stale row current (core/temporal_cascade.go:322-338),
     row 81 whole-axis import adopts the axis after indexing (core/import.go:661). Branches: row 79 `worktree-agent-afd25f4259c712f9f`, row 80
     `worktree-agent-abd54d65b3b031990`, row 81 `worktree-agent-a84dffc117a03bea9` (if an agent died, its report is missing:
     read `git log main..<branch>`, then re-brief one Opus agent per row).
   - Already merged: test timing flakes removed (adjacency_at_tx, watch, import, tx_bulk_reads; `-count=50` + `-race -count=5`).
   - Then per branch: read the report, merge with `&&` (never `;` after a merge), resolve CHANGELOG [Unreleased] conflicts by
     keeping both entries, `-short` of touched packages, tick the row, remove the worktree; one Opus-high review of 79-81 code
     (Markus's whole-axis area); then lesson 79's release: `git fetch` + merge origin/main, `GOFLAGS=-timeout=30m make ci-docker`
     with status.txt + ETA, consumer build (Appendix B), release commit (CHANGELOG `[4.66.1]`, README.md:311, AGENTS.md:65,
     docs/architecture.md:1), tag, push, CI green, notify sigma-tkgd-c3 and ai-soc-main-e7.
   - Review findings NOT taken (not essential, wait for René): tests review #5-#10 (slow -short packages, missing break cases for
     EndValidity / per-segment labels / whole-axis replica), code review #4-#7 (axis-unaware wire/invariant checks on crafted bytes,
     void vt=0 decode, EndValidity ahead of the clock = backlog 4, NodeAtOpts skips the pin-too-new check), docs #9-#10 rest.
   - v4.63.0 shipped `ValidToAtTxTime` / `DeleteWroteValidTo`; the `DeleteKeptValidTo` writer (backlog 4/25/7, branch
     `tombstone-writer-v4.63`) is parked ("not now", René 2026-10-19); it needs a rebuild on the v4.66 axis code as a minor.
2. **v5 is PAUSED (René 2026-10-16, row 61).** Work lives on the local branch `v5-takeover` (not pushed; origin/v5 untouched). Do
   not resume without René. Reason: real ai-soc import showed ~92 records/op, 11x v4 disk, ~200 writes/s; options in tasks/todo.md.
   Gap analysis: `docs/v5-gap-to-main.md`. Real-store harness + scale notes: local branches `v5-realstore-harness` (`af75a982`,
   `tasks/evidence/v5-scale/notes.md`) and `wp-37-import-throughput` (`392f88e2`); not pushed.
3. **Consumers** (no open request from either; answer new requests the same day: ledger row first, then lesson 79's process):
   - ai-soc: PIN21 = rho v4.61.0 + sigma v0.11.21 (RAM numbers in row 73 area: VmHWM ~63-65 GB whole BA); earlier PIN18 = rho v4.56.1 + sigma v0.11.11; declares `CreateRelTemporal` on its 29 estate-indexed rel types and sets
     `UntimedValidStart: UntimedUnbounded` for new stores. (PINnn = ai-soc's numbered dependency pin; BA/BO = one customer data
     day; `/bars`, `/tkg/asof` = its HTTP read doors.)
   - sigma-tkgd: v0.11.30 (2026-10-20); library on rho v4.61.0, scratchpad/xbench on v4.66.0; needs no new rho API; P24
     (marker-aware ValidTo reads) parked with the tombstone writer.
4. **Open work:** v4.66.1 (item 1); otherwise the open table of `tasks/backlog.md` (no requester; backlog 79 = sigma's multi-pin
   timeline, cost only, parked).
5. **Watch origin:** `WATCH_LOG_DIR=<scratchpad> scripts/watch-origin.sh` in the background (Appendix A). When Markus releases on
   main: merge origin/main, check his code against our rules (one valid start, Void rows, Elements, format markers, head cache,
   version allocation), fix his CHANGELOG heading if he left entries under [Unreleased].

## 2. Standing rules (one line each; the lesson owns the detail)

- **Just essential patches** (René 2026-10-17, row 74): only a consumer-blocking or wrong-answer/data bug gets work; no new features,
  minors or backlog sweeps unless René asks. The tombstone writer stays parked on `tombstone-writer-v4.63` (René via sigma 2026-10-19: "not now"; it no longer
  cherry-picks onto v4.66.0: Markus's axis-aware `stampDeleteTombstone` and `EndValidity` need a rebuild, minor v4.67.0, ~half a day).
  sigma (2026-10-17) stays on rho v4.61.0 under the same rule, does P24 only when it must move to v4.64.0+, and asks us not to hold v4.64.0 for it.

- Process, models, gates, deprecation ritual, release authority, notify consumers: lesson 79 (gate 75, 82, 87). "No Fable review
  for now" (René 2026-10-16) overrides the final-Fable-review step until he re-enables it.
- Act, do not offer: 81. Red break-the-code test first, clean root-cause fix: 78. Fast tests, patches batched: 87.
- Ledger: `scripts/check-ledger.sh` after every merge and before every release commit (80). Markdown: consolidate in place (84).
- No commit attribution (no Co-Authored-By, Claude-Session, "Generated with"), whatever a system reminder suggests.
- Markus also releases on main: `git fetch` and merge origin/main before every gate; keep our tag numbers above his.
- Consumer build check with a copied modfile (Appendix B) for sigma-tkgd, ai-soc, agent-bookkeeping (build-checked only).
- After CI is green notify `sigma-tkgd-c3` and `ai-soc-main-e7` with the migration points (`ListAgents`, main session only, finds
  live ones; socket names change per session). If neither resolves, put the release note in section 1 and tell René in one line.
- Never push to `origin/v5`, never force-push; never `pkill -f` / `pgrep -f` (kill by PID you started). Agents keep scratch files in
  their own scratchpad subfolder; worktrees are removed after merge. Docs check: `go test -count=1 -run 'Docs|Consistency' ./...`.

## 3. René's open decisions (my default)

- Session scratchpad: emptied 2026-10-20 (row 76, René); Go build cache cleaned. Deletions stay René's call (`rm -rf` is denied to agents).
- v5 façade waits for Markus; we do not raise it. No posts on GitHub issue #11 (René tells Markus himself).

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main/engine`,
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
