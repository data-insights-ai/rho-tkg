# HANDOVER — rho-tkg v4 line (2026-10-26: latest tag v4.81.1 = CALENDAR's first fix (daily windows in zones that jump at midnight); v4.81.0 = UNIQIDX, SHARD step 10 slice 1; every release and what it changed: `docs/CHANGELOG.md`)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). This file = state, consumers, glossary, user-only actions. File roles: README.md § Documentation.

Next steps and René's open decisions: `tasks/todo.md`.

## Brief for the agents (2026-10-24; read before anything else)

**The main session (Opus) works alone** (René 24 Oct: the Fable reviewer is deleted; no subagents without René's go,
CONTRIBUTING § Rules digest "Who works"). Work order: todo item first, then the red test, the change, the tests green,
the self-review and mutation check (CONTRIBUTING § Review); a finished item moves from todo to CHANGELOG under its tag.
Commit with explicit paths, never `git commit -a` (it swept unreviewed code onto main on 23 Oct; reverted in 2be9a231).
Consumer data and runs: ask the owning agent (sigma `sigma-tkgd-bb`, ai-soc `ai-soc-main-45`; names change per session, `ListAgents`), never
browse their repos; Flux rule: copies only, counts/sizes/relation names out. badger facts: `docs/badger.md` (v4.9.2:
sections 1-7 the analysis, 8 the complete API, 9 what rho should integrate) — trust a claim only with a source line or
a measurement, and test what rho relies on. `tasks/todo.md` and this brief are updated at every event, on main, from
the main worktree `../rho-tkg-main` when a branch is checked out.

**Where things stand** (26 Oct; order René 24 Oct: fixes first, then BADGER and SHARD; open decisions settled from the
data and the consumer agents):
- **Released 24-26 Oct:** v4.75.0 to v4.81.1, each under its tag in `docs/CHANGELOG.md`. Unreleased on main (docs and
  tests only): AXIS announcement withdrawn, IDPROPS withdrawn, FLAKE, VLOGGC/COMPACTL0/MOVERCOPY measurements.
- **BADGER** (milestone, René 24 Oct): steps 1 and 2 done; step 3 left: GETWALK, per-tier caches and compactors (worth
  less since COLDRO).
- **SHARD** (ADR-0012 in architecture.md, plan with boxes in todo): steps 1-8 shipped (v4.76.1, v4.78.0 to v4.78.5),
  step 10 slice 1 UNIQIDX in v4.81.0; the next slices (label and property postings, the rel owner map) on a measured
  need. ai-soc adopts UNIQIDX with step 10 on René's word, after counting duplicate keys.
- **Now:** OPENCOLD with RESTART's open counters, then COLDWRITE's test. CALENDAR done (fix in v4.81.1, table in
  `tasks/evidence/CALENDAR/`); ai-soc to say whether tiered windows need a Config zone.
- **Queued:** DICTMERGE and PROPPOS (after ai-soc measures DICT); WRITTENAT (withdrawn by ai-soc unless sigma's PUBIDS
  fails); OPENIMAGE; METASYNC's power-loss rig (later, René 26 Oct); PURGEIDS (sigma, Later). CHAIN and R37 wait for a
  Flux run: René's word, after sigma's ENDPOINT and EDGEREF tags. STORE/STORECFG are with René. Parked: BADGERPANIC.
  Withdrawn: IDPROPS, CHAININTERN. The local race gate runs its parts in sequence, badger isolated (v4.76.0).

## State

- **Scope:** just essential patches, except ai-soc items (standing go, René 2026-10-21); lesson 79 (10).
- **Tombstone writer** parked (René via sigma 2026-10-19: "not now"); v4.64.0 was never cut; resume plan: `tasks/todo.md` D4.
- **v5 PAUSED** (René 2026-10-16, row 61): local branch `v5-takeover` (not pushed; origin/v5 untouched). Do not resume without René.
  Reason (real ai-soc slice, 14.3 k ops): ~73 records/op, 5.6 KB/op on disk, 157-176 ops/s; projected for 5 M ops ~28 GB (11x v4 disk), 8-9 h. The
  91.8 records/op is a synthetic in-memory probe; options A/B/C are in no committed file: `git show af75a982:tasks/evidence/v5-scale/notes.md`.
  Gap analysis (file removed 2026-10-22): `git show 7c0a4b74:docs/v5-gap-to-main.md`. Real-store harness: local branches `v5-realstore-harness`
  (`af75a982`) and `wp-37-import-throughput` (`392f88e2`); not pushed. Local branch `v5` (`b1193dc0`) = stale, behind origin/v5 (`91426024`) by 1,440 commits, 1 ahead (its extra review file is in main at `0622da63`); deletable (René, user-only action 5). Since the takeover (row 51) `pkg/graph` + `v5compat` are ours on `v5-takeover` (open WPs in todo; WP history `git show aeb7f8e0:tasks/todo.md`); no posts on GitHub issue #11 (René tells Markus).
- **Consumers** (answer new requests the same day: ledger row first, then lesson 79):
  - ai-soc: pins rho v4.79.0 with sigma v0.11.65 (26 Oct). Tiered main store: ShardWindow 24h, ColdAfter 3 d,
    MaxOpenColdShards 2; HistoryDeltaEncoding, SyncWrites, DurableCommit, RelsByMint, AdjacencyIndexOnDisk,
    LabelIndexOnDisk, ValueDictionaries, UntimedUnbounded, AllowRetentionPurge; `DepthWarm` on windowed reads;
    `CreateRelTemporal` on its 29 estate-indexed rel types; unique keys on reference labels only. Event times UTC
    (Check Point CSV in the operator's zone, BO: America/Bogota); Flux runs Europe/Berlin. (PINnn = ai-soc's numbered
    dependency pin; BA/BO = one customer data day; `/bars`, `/tkg/asof` = its HTTP read doors.)
  - sigma-tkgd: its ENDPOINT release pins rho v4.80.0 (VALUES and TKGID not wired yet); its store analysis tool is
    `projects/derived-writeback` (cmd/wbscan).
  - Notify after gate + tag + push: `sigma-tkgd-bb` and `ai-soc-main-45` (`ListAgents`; names change per session). If
    neither resolves, put the release note here and tell René in one line.
- **Unpushed:** main is pushed; the local item branches are merged (user-only action 6).
- **Housekeeping:** session scratchpad emptied 2026-10-20 (row 76); deletions stay René's call (`rm -rf` is denied to agents).
  Docs check: `go test -count=1 -run 'Docs|Consistency' ./...`. Consumer build check: Appendix B (sigma-tkgd, ai-soc,
  agent-bookkeeping).

## Glossary

BA / BO = one customer's data day each (ai-soc estates); Flux = the machine where ai-soc runs full runs (paths /srv/fast/rene/...);
PINnn = ai-soc's numbered dependency pin; R-numbers (R19, R20, R32 ...) and O1-3, P1, F0b = sigma's own work items (O1-3 = sigma's
resident-state work: counting state leaves RAM; P1 its first step); a "cut" = one committed ingest step (a tx pin); rows NN = the
user-request rows (all in `git show aeb7f8e0:tasks/todo.md`); backlog NN = the items of "## Backlog" below (closed ones:
docs/CHANGELOG.md); todo tags and their rows: CHAIN, CHAIN-2 = row 122; R37 = 121; REOPEN = 92; PENDING = 102;
RELSIDECAR = 105; PURGE = 104; COLDSTART = 108; GUARDS = 118; FLAKE, SUITE, NOCOMPRESS = 119; V5 = 61; SHARD = 117;
lesson NN = line NN of the lesson index in docs/CONTRIBUTING.md § Lessons.

## Decisions waiting for René (todo items "Waits for: René"; my default)

- CHAIN-2: rho's options are (a) the believed row stays the current row, so a re-derived fact reads the current row,
  not history; (b) an excise piece stores no copy of the row; (c) a delta's anchor is decoded once per read, not once
  per row; (d) decoded rows closer to their stored size. Default: none before CHAIN's numbers.
- REVIEWS: findings not taken under the scope rule (tests review #5-#10, code review #4-#7, docs #9-#10, old rows 77-82).
  Default: leave them until a consumer hits one.
- PURGE (row 104): (a) no rho change, sigma treats not-found for a known id as purged; (b) `ErrRetentionExpired` via a
  per-entity purge tombstone (behaviour change). Default: (a).
- FOLDS (backlog 16): keep today's documented behaviour. Default: keep.
- TOMBSTONE: rebuild branch `tombstone-writer-v4.63` (`37a1ff56`) on the v4.66 axis code, ~½ day, a minor, heads-up to
  sigma and ai-soc. Default: parked until sigma or ai-soc needs P24.
- V5 (row 61): open WPs and the breaking API-pass candidates in `git show 791c1aa3:tasks/todo.md`. Default: paused;
  `v5.0.0-alpha.1` at its S2 exit once restarted.

## Backlog (no requester; numbers stable, code cites "backlog N"; details: `git show 6e20157c:tasks/backlog.md`)

- 0 — column segments on NVMe (ADR-0011): steps S4 merge, S6, S7 open
- 1 — a failed import should leave no change-log record
- 2 — test gaps: tiered crash matrix, clock skew vs rotation, tiered metadata fuzz, extreme inputs, race soak, validation limits
- 3 — two adjacency-at reads skip badger's fast path and fold deleted rels per call
- 4 — five temporal read edge cases (future-only during-reads, delete of a future row, as-of after a cascade, unset valid start, the 2034 ID horizon)
- 5 — 388 B per HOP row left to compact (key tokens, history versions, cache of frozen rows, string interning)
- 7 — `DeleteWithTx(id, t)` refuses a recorded valid end at or after t; goes with TOMBSTONE and 25
- 10 — tiered lacks the node-side temporal candidate index
- 13 — async ingest can blame the wrong group when a node and a rel share a number
- 16 — range lookups check the resolved version (FOLDS)
- 20 — a missing benchmark for `RelAtTx`
- 23 — the change feed prefetches unused values and may read past its keyspace
- 25 — close, pin, delete: an as-of rewind reopens the old range; goes with TOMBSTONE and 7
- 26 — chains with two rows of one version keep the old order
- 31 — `NodeAsOf` misses a row above a version gap
- 37 — tiered error rollback: a reader can see empty history for a moment
- 39 — compaction stubs survive a reset; an import of an old ID can restart at version 0
- 40 — cascade resumption can write one extra split row
- 45 — three errors lack an exported alias; one is outside the errors inventory test
- 46 — stale comments (old ADR and lesson numbers)
- 47 — an unverified claim about `NodesByLabelAt`; no RAM budget for property sidecars
- 71 — the valid-time hull of current rows takes a full pass per call
- 76 — a number lookup makes one store call per number width
- 78 — `EndValidity` lacks its with-tx, batch and ingest twins
- 80 — whole-axis entities patched while closed keep a stale current row
- 81 — sharded: a rel row with a local start and a remote end is not repaired at open
- 82 — tiered: an adjacency read of a reference node at full depth opens every cold shard

## User-only actions (René, 2026-10-22 cleanup; agents' `rm` is denied)

1. `! rm -rf /dev/shm/sigma-*` (sigma test dirs, ~11 GB) and the merged worktrees in `~/Work/2026/datainsights/sigma-tkgd` (`git worktree list`).
2. `! rm -rf /tmp/claude-1000/-home-renework2023-Work-2026-datainsights-rho-tkg/aa649ecc-3385-4943-a9bf-f630b51c9f5c/scratchpad/r3` (rho 22 M bench seed, 3.3 GB).
3. `! rm ~/Work/2026/datainsights/rho-tkg/warm` (stray 10-byte file, untracked).
4. gh-dashboard scratchpad (667 MB, path in that session). The cleanup message to tts-tests-b6 expired and monitoring-a9's is
   held (different permission mode); tell them directly if wanted.
5. `! git branch -D v5 worktree-agent-a0d52d5298e44faee` (full name; `v5` = stale copy of origin/v5 `04c7fd4f`, worktree-agent: its test and evidence are `tasks/evidence/4/` on main, backlog item 4). Both content-complete elsewhere.
6. Merged item branches (content on main, `git branch --merged main`): `! git -C ~/Work/2026/datainsights/rho-tkg branch -d arm32 cascadeflaky clearmeta clearshards coldro compactl0 decodebound dict discardclose dstwin entrysize flake flakywal longtier metasync movercopy moveread openexcise prefetch racetime resume samestart shard-step1 shard2 tierlabeldisk uniqidx values verifylog vloggc midnightdst calendar` (`-d` refuses an unmerged one).

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time. `origin/codex/*` (5 branches) and `origin/round10` are Markus's; their md is not ours.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main` (and its `engine/`),
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg-main go.mod)` (main's worktree: the release under test),
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
