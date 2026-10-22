# HANDOVER — rho-tkg v4 line (2026-10-22: latest tag v4.70.0, rows 104-105 sigma O1-3 doors; v4.69.2 row 106; v4.69.1 row 100; v4.69.0 row 99)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). This file = state, consumers, glossary, user-only actions. File roles: README.md § Documentation.

Next steps and René's open decisions: `tasks/todo.md`.

## State

- **Scope:** just essential patches, except ai-soc items (standing go, René 2026-10-21); lesson 79 (10).
- **Tombstone writer** parked (René via sigma 2026-10-19: "not now"); v4.64.0 was never cut; resume plan: `tasks/todo.md` D4.
- **v5 PAUSED** (René 2026-10-16, row 61): local branch `v5-takeover` (not pushed; origin/v5 untouched). Do not resume without René.
  Reason (real ai-soc slice, 14.3 k ops): ~73 records/op, 5.6 KB/op on disk, 157-176 ops/s; projected for 5 M ops ~28 GB (11x v4 disk), 8-9 h. The
  91.8 records/op is a synthetic in-memory probe; options A/B/C are in no committed file: `git show af75a982:tasks/evidence/v5-scale/notes.md`.
  Gap analysis (file removed 2026-10-22): `git show 7c0a4b74:docs/v5-gap-to-main.md`. Real-store harness: local branches `v5-realstore-harness`
  (`af75a982`) and `wp-37-import-throughput` (`392f88e2`); not pushed. Local branch `v5` (`b1193dc0`) = stale, behind origin/v5 (`91426024`) by 1,440 commits, 1 ahead (its extra review file is in main at `0622da63`); deletable (René, user-only action 5). Since the takeover (row 51) `pkg/graph` + `v5compat` are ours on `v5-takeover` (open WPs in todo; WP history `git show aeb7f8e0:tasks/todo.md`); no posts on GitHub issue #11 (René tells Markus).
- **Consumers** (answer new requests the same day: ledger row first, then lesson 79):
  - ai-soc: root module `ai-soc-main` pins rho v4.70.0 (`engine/` still v4.41.0), tiered as the main store (`AdjacencyIndexOnDisk`, `RelsByMint`, `SyncWrites`, `DepthWarm` on
    windowed reads); declares `CreateRelTemporal` on its 29 estate-indexed rel types and sets `UntimedValidStart:
    UntimedUnbounded` for new stores. (PINnn = ai-soc's numbered dependency pin; BA/BO = one customer data day; `/bars`,
    `/tkg/asof` = its HTTP read doors.)
  - sigma-tkgd: v0.11.55 (2026-10-22) pins rho v4.70.0; O1-3 consumer work in `tasks/todo.md` Next steps.
  - Notify after gate + tag + push: `sigma-tkgd-c3` and `ai-soc-main-e7` (`ListAgents`, main session only; socket names change
    per session). If neither resolves, put the release note here and tell René in one line.
- **Unpushed:** local main is ahead of origin/main by the md commits 6e20157c, aeb7f8e0 (+ the row-112 commit); pushing is René's or the next release's call.
- **Housekeeping:** session scratchpad emptied 2026-10-20 (row 76); deletions stay René's call (`rm -rf` is denied to agents).
  Docs check: `go test -count=1 -run 'Docs|Consistency' ./...`. Consumer build check: Appendix B (sigma-tkgd, ai-soc,
  agent-bookkeeping).

## Glossary

BA / BO = one customer's data day each (ai-soc estates); Flux = the machine where ai-soc runs full runs (paths /srv/fast/rene/...);
PINnn = ai-soc's numbered dependency pin; R-numbers (R19, R20, R32 ...) and O1-3, P1, F0b = sigma's own work items (O1-3 = sigma's
resident-state work: counting state leaves RAM; P1 its first step); a "cut" = one committed ingest step (a tx pin); rows NN = the
user-request rows (open ones in tasks/todo.md, all in `git show aeb7f8e0:tasks/todo.md`); backlog NN = the items of tasks/todo.md "Backlog (open)" (closed ones: docs/CHANGELOG.md);
lesson NN = `### NN.` in docs/CONTRIBUTING.md.

## User-only actions (René, 2026-10-22 cleanup; agents' `rm` is denied)

1. `! rm -rf /dev/shm/sigma-*` (sigma test dirs, ~11 GB) and the merged worktrees in `~/Work/2026/datainsights/sigma-tkgd` (`git worktree list`).
2. `! rm -rf /tmp/claude-1000/-home-renework2023-Work-2026-datainsights-rho-tkg/aa649ecc-3385-4943-a9bf-f630b51c9f5c/scratchpad/r3` (rho 22 M bench seed, 3.3 GB).
3. `! rm ~/Work/2026/datainsights/rho-tkg/warm` (stray 10-byte file, untracked).
4. gh-dashboard scratchpad (667 MB, path in that session). The cleanup message to tts-tests-b6 expired and monitoring-a9's is
   held (different permission mode); tell them directly if wanted.
5. `! git branch -D v5 worktree-agent-a0d52d5298e44faee` (full name; `v5` = stale copy of origin/v5 `04c7fd4f`, worktree-agent: its test and evidence are `tasks/evidence/4/` on main, backlog item 4). Both content-complete elsewhere.

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time. `origin/codex/*` (5 branches) and `origin/round10` are Markus's; their md is not ours.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main` (and its `engine/`),
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
