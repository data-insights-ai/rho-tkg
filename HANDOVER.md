# HANDOVER — rho-tkg v4 line (2026-10-22: latest tag v4.70.0, rows 104-105 sigma O1-3 doors; v4.69.2 row 106; v4.69.1 row 100; v4.69.0 row 99)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). Ownership: this file = state, next steps, René's open decisions (§3), user-only actions; `docs/CONTRIBUTING.md` =
the rules (digest at the top, lessons as `### N.`); `tasks/todo.md` = the ONE plan (user requests, releases table, v5 work
packages and decisions, "Backlog (open)" with the stable backlog numbers); `docs/CHANGELOG.md` = what shipped. Read in this
order: this file, the `docs/CONTRIBUTING.md` rules digest + lessons, `tasks/todo.md` (user requests, Backlog (open),
releases table), `docs/CHANGELOG.md` top. No other md files (README.md § Documentation lists the set).

## 1. Next steps (focus René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC")

Critical path first (René 2026-10-21: "for now we should focus on the critical path and write these items for the next step then"):
only items that block ai-soc on Flux or sigma get built now; the rest is listed under "Next step (deferred)" below.

1. **Shipped v4.69.2 = row 106** (ai-soc Flux blocker: tiered as-of pages); ai-soc pins it and reruns the full run.
2. **Shipped v4.70.0 = rows 104-105** (sigma O1-3 doors); open from it: René decision §3 item 4 (purge vs retained pins).
   sigma uses LowestAnsweredPins (P1); RelEffectiveTimelinesByProperties comes with its paging layer: on tiered the driver is
   the first indexed key (device before estate), segments whole, DepthAll only; ai-soc declares (type, device) rel indexes.
   Answered without code: row 107 (ai-soc /bars: sigma ListDerived as is, fixed pin = consistent, first TxFrom = VersionBefore(id, 1)).
   Row 100's red test `pkg/graph/store/tiered/idle_timeout_race_test.go` (`TestSetIdleTimeoutForTest_NoRace`) is untracked: v4.69.1
   shipped the fix without it; run it with `-race`, commit it.
3. **Waiting for ai-soc** (row 92): reopen `ReadHistory` 526 s vs 57 s; rho's `readUnderRLock` is unchanged since v4.66.0; rho acts
   on ai-soc's profile only.
4. **Open work:** `tasks/todo.md` "Backlog (open)" (80-82 from the v4.66/v4.67 reviews; nothing with a requester).
5. **Watch origin:** start `WATCH_LOG_DIR=<scratchpad> scripts/watch-origin.sh` in the background at session start (Appendix A). When Markus releases on
   main: merge origin/main, check his code against our rules (one valid start, Void rows, Elements, format markers, head cache,
   version allocation), fix his CHANGELOG heading if he left entries under [Unreleased].

### Next step (deferred, not critical path)

- Row 102: badger `rangePending` walks the whole pending + flushing buffer per per-entity overlay read (history, disk-mode
  adjacency) → rollback of n rels costs n × pending (ai-soc Flux: one failed write-back rollback > 2 h; normal pushes 0-0.64 %).
  Resume on local branch `row102-pending-ranged`: 8fa7a967 = the two red tests; f4b27842 = WIP (new badgerstore_write_buffer.go
  prefix-ordered buffer, call sites in badgerstore*.go half converted; the agent stopped mid "refactor of all production sites") → patch.
  Base 6ac9ea5f, main is 46 commits ahead: rebase onto main first.
- Backlog 80-82 (review findings, no requester); review nits not taken: v4.69.2 (4) and v4.70.0 (3), in git 82558ea9 / 3c370aab.
- Row 105 option (b): a real rel composite tx-membership sidecar (2,500-3,500 LOC, 4 backends), only if (c) measures too slow on BA.
- sigma FYI 2026-10-21 (ai-soc BA RESHEAP, rho-attributed heap 0.53 → 1.13 GB in 24 h): 0.18 GB ristretto Calloc under
  Session.checkEntities → latestStamps. In code: each opened tiered shard owns its own badger block/index cache (badgerCfg
  passes BlockCacheSize; 0 = Badger's default, badgerstore_options.go:182), so cache RAM = open shards × per-shard size,
  bounded by MaxOpenColdShards but never declared as one budget. Next step: one store-wide cache budget or a documented
  per-shard size (ai-soc sets BlockCacheSize/IndexCacheSize). Not critical path.

## 2. State

- **Scope:** just essential patches, except ai-soc items (standing go, René 2026-10-21); lesson 79 (10).
- **Tombstone writer** (`DeleteKeptValidTo`, fixes backlog 4/25/7): branch `tombstone-writer-v4.63` (`37a1ff56`), parked ("not
  now", René via sigma 2026-10-19); needs a rebuild on the v4.66 axis code (Markus's axis-aware `stampDeleteTombstone`,
  `EndValidity`), ~half a day, a later minor. v4.64.0 was never cut. sigma's P24 (marker-aware ValidTo reads) is parked with it.
- **v5 PAUSED** (René 2026-10-16, row 61): local branch `v5-takeover` (not pushed; origin/v5 untouched). Do not resume without René.
  Reason (real ai-soc slice, 14.3 k ops): ~73 records/op, 5.6 KB/op on disk, 157-176 ops/s; projected for 5 M ops ~28 GB (11x v4 disk), 8-9 h. The
  91.8 records/op is a synthetic in-memory probe; options A/B/C are in no committed file: `git show af75a982:tasks/evidence/v5-scale/notes.md`.
  Gap analysis (file removed 2026-10-22): `git show 7c0a4b74:docs/v5-gap-to-main.md`. Real-store harness: local branches `v5-realstore-harness`
  (`af75a982`) and `wp-37-import-throughput` (`392f88e2`); not pushed. Local branch `v5` (`b1193dc0`) = stale copy of origin/v5 `04c7fd4f` (docs/v5/* identical;
  its extra review file is in main at `0622da63`), deletable (René, user-only action 5). Since the takeover (row 51) `pkg/graph` + `v5compat` are ours on `v5-takeover` (WP table in todo); no posts on GitHub issue #11 (René tells Markus).
- **Consumers** (answer new requests the same day: ledger row first, then lesson 79):
  - ai-soc: root module `ai-soc-main` pins rho v4.70.0 (`engine/` still v4.41.0), tiered as the main store (`AdjacencyIndexOnDisk`, `RelsByMint`, `SyncWrites`, `DepthWarm` on
    windowed reads); declares `CreateRelTemporal` on its 29 estate-indexed rel types and sets `UntimedValidStart:
    UntimedUnbounded` for new stores. (PINnn = ai-soc's numbered dependency pin; BA/BO = one customer data day; `/bars`,
    `/tkg/asof` = its HTTP read doors.)
  - sigma-tkgd: v0.11.55 (2026-10-22) pins rho v4.70.0; O1-3 state in §1 item 2.
  - Notify after gate + tag + push: `sigma-tkgd-c3` and `ai-soc-main-e7` (`ListAgents`, main session only; socket names change
    per session). If neither resolves, put the release note here and tell René in one line.
- **Housekeeping:** session scratchpad emptied 2026-10-20 (row 76); deletions stay René's call (`rm -rf` is denied to agents).
  Docs check: `go test -count=1 -run 'Docs|Consistency' ./...`. Consumer build check: Appendix B (sigma-tkgd, ai-soc,
  agent-bookkeeping).

## Glossary

BA / BO = one customer's data day each (ai-soc estates); Flux = the machine where ai-soc runs full runs (paths /srv/fast/rene/...);
PINnn = ai-soc's numbered dependency pin; R-numbers (R19, R20, R32 ...) and O1-3, P1, F0b = sigma's own work items (O1-3 = sigma's
resident-state work: counting state leaves RAM; P1 its first step); a "cut" = one committed ingest step (a tx pin); rows NN = the
user-request rows of tasks/todo.md; backlog NN = the items of tasks/todo.md "Backlog (open)" (closed ones: docs/CHANGELOG.md);
lesson NN = `### NN.` in docs/CONTRIBUTING.md.

## User-only actions (René, 2026-10-22 cleanup; agents' `rm` is denied)

1. `! rm -rf /dev/shm/sigma-*` (sigma test dirs, ~11 GB) and the merged worktrees in `~/Work/2026/datainsights/sigma-tkgd` (`git worktree list`).
2. `! rm -rf /tmp/claude-1000/-home-renework2023-Work-2026-datainsights-rho-tkg/aa649ecc-3385-4943-a9bf-f630b51c9f5c/scratchpad/r3` (rho 22 M bench seed, 3.3 GB).
3. `! rm ~/Work/2026/datainsights/rho-tkg/warm` (stray 10-byte file, untracked).
4. gh-dashboard scratchpad (667 MB, path in that session). The cleanup message to tts-tests-b6 expired and monitoring-a9's is
   held (different permission mode); tell them directly if wanted.
5. `! git branch -D v5 worktree-agent-a0d52d5298e44faee` (full name; `v5` = stale copy of origin/v5 `04c7fd4f`, worktree-agent: its test and evidence are `tasks/evidence/4/` on main, backlog item 4). Both content-complete elsewhere.
6. Run the row-110 md move command (the main session posts it).

## 3. René's open decisions (my default)

1. Review findings not taken under the scope rule: tests review #5-#10, code review #4-#7, docs #9-#10 rest (todo rows 77-82).
   Default: stay untaken until a consumer hits one.
2. v5 restart (row 61) and the first v5 tag (todo decision 4). Default: paused; alpha at S2 exit once restarted.
3. Tombstone writer rebuild (above). Default: parked until sigma or ai-soc needs P24.
4. Retention purge vs retained pins (todo row 104): a purged entity answers not-found at pins it answered before (no tombstone).
   (a) no rho change, sigma treats not-found for an id it saw before as purged; (b) rho refuses those pins with
   ErrRetentionExpired (per-entity purge tombstone, behaviour change via the deprecation ritual). Default: (a).
5. `UntimedWholeAxis` as the default for new stores: CHANGELOG `[4.65.0]` Deprecated announced it for "the next minor"; held
   since (row 75b), five minors shipped without it. Default: stays held; the next minor either flips it or rewords the notice.

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time. `origin/codex/*` (5 branches) and `origin/round10` are Markus's; their md is not ours.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main` (and its `engine/`),
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
