# HANDOVER — rho-tkg v4 line (2026-11-04: latest tag v4.104.0; every release and what it changed: `docs/CHANGELOG.md`)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). This file holds state only: releases, branches, consumers, glossary, the numbered backlog, user-only actions.
Rules (read every session): `docs/CONTRIBUTING.md` § Rules digest. Next steps and René's open decisions with my
defaults: `tasks/todo.md`. File roles: README.md § Documentation.

## State

- **State of 4 Nov, read this first** (next steps: `tasks/todo.md`). Latest tag v4.104.0 (round 4: VALUETHRESH,
  LENDCOST; a minor). FREEZE round 4 (ai-soc, 4 Nov): rho v4.104.0 + sigma v0.11.162 are the pair for ai-soc's pin
  (ai-soc ea30bfda), its three local sets and the next Flux run; a later tag belongs to the next run, never a re-pin.
  Main keeps going untagged. Measuring is ai-soc's combined run on main's tip, not per item (René via ai-soc); ai-soc
  names its quiet windows: then no ci-docker, probe, bench or long test here.
  - **On main, unreleased:** RELHIST P2 (a patch: the plural timeline doors read and fill the head cache; a plain
    Update, CloseVersion or ReplaceVersion keeps the entry valid). No branch open.
  - **FIXED-MEMORY:** step 0 measured (`tasks/evidence/FIXED-MEMORY/step0.txt`); step 1 (existence files on the
    reference shard and archive) is next, on a branch, red test first. Path (b) waits for René.
  - **Reviews:** René (3 Nov, "yes, always"): one read-only Fable review per finished branch, before its merge, given
    the code and the diff, not my conclusions.
- **Release gates, latest first** (what each release changed: `docs/CHANGELOG.md` under its tag; the gate notes of
  v4.85.0 to v4.100.1: `git show 7fd75c40:HANDOVER.md`):
  - **v4.104.0** (round 4, a minor: VALUETHRESH adds `ValueThreshold` to the configs; LENDCOST fixes v4.103.0's tiered
    lend regression): tagged on main, which is 7e4399c4 + VLOGCLOSED (the first ci-docker run panicked in VALUETHRESH's
    GC pass on a DB a crash test closed under the store; the pass now returns) + the release commit. ai-soc's round-4
    gate on 7e4399c4 + sigma c2ce628f3 green (2-day budget arm: LendByIDs 94.5 s, user CPU 4,302 s, 1,753 bars equal;
    60-min 4 arms 260 bars equal; masked cycles pass); consumers build and vet clean; ci-docker green on the second
    run; scale probe level with v4.103.0 (149 B/entity insert, one outlier of 12; 170 B reopen); tag CI green.
  - **v4.103.0** (round 3, a minor: TIERLEND and HEADLEND add exported names; HISTDECODE, HISTTRIM, CASCADEMETA
    fixes): released from branch `release-v4.103.0` (0156a78c + the release commit); ai-soc's round-3 gate on 0156a78c
    + sigma 9e3ff88e8 green; consumers build and vet clean; ci-docker green; scale probe level with v4.102.0.
  - **v4.102.0** (round 2, a minor: RHOMEMIDS, BATCHIDX and UNIQUELOOK add exported names, UNIQUELOOK format marker
    16; BATCHADD, RHOPOST, BATCHFAIL fixes): released from branch `release-v4.102.0` (98ea7d36 + the release commit +
    a tests-only lint fix); ai-soc's 4-arm gate on 98ea7d36 + sigma d8be171c8 green; ci-docker green (first run red
    only on lint in three round-2 tests); consumers build and vet clean; scale probe level with v4.101.0.
  - **v4.101.0** (COLDEDGE, a minor): ci-docker green on a6245ca0 at load 30-45; one tests-only fix after it
    (`934b2fba`, a load flake); consumers build and vet clean; scale probe in `tasks/evidence/SCALE/existence_maps.txt`.
  - **v4.75.0 to v4.100.1** (24 Oct to 3 Nov): each gated as the checklist says (ci-docker or ci-fast, consumers,
    scale probe, tag CI); exceptions worth knowing: v4.98.0's tag CI is red on `TestValueDictEntryHeap`, so pin
    v4.98.1; v4.99.0 and v4.98.4 were cut from older tags and merged back.
- **Checklist facts learned at releases:** the ai-soc engine builds only with GOTOOLCHAIN=go1.27.3 (its parent module
  requires it); merging a release branch back can move Unreleased CHANGELOG entries under the tag's heading, so check
  the heading after every such merge; `scripts/patchgate` is a package: `go run ./scripts/patchgate <base>`.
- **Branches:** every merged branch is deleted (René 27 Oct: "delete if everything is merged"). Item work starts a
  branch from main in the main checkout or a worktree removed when the item ends. The 7 unmerged branches of 27 Oct
  live as parents of one commit on `archive/unmerged-2026-10-27` (6554d8ad, pushed; tree = main's, merge strategy
  ours; René 27 Oct), whose message gives each tip, base and status. Restore one with `git branch <name> <tip>`:
  row102-pending-ranged f4b27842, tombstone-writer-v4.63 37a1ff56, v5-takeover 3bd4d13a, v5-realstore-harness
  af75a982, wp-37-import-throughput 392f88e2, worktree-agent-a34d277df6499077d d2cc529f,
  worktree-agent-a6a166b726378f33b d479f1e1 (the last two: uncommitted 22 Oct agent-worktree changes, not reviewed).
- **v5 PAUSED** (René 2026-10-16, row 61): `v5-takeover` (tip 3bd4d13a, in `archive/unmerged-2026-10-27`; origin/v5
  untouched). Do not resume without René. Reason (real ai-soc slice, 14.3 k ops): ~73 records/op, 5.6 KB/op on disk,
  157-176 ops/s; projected for 5 M ops ~28 GB (11x v4 disk), 8-9 h. Options A/B/C: `git show
  af75a982:tasks/evidence/v5-scale/notes.md`; gap analysis: `git show 7c0a4b74:docs/v5-gap-to-main.md`; the old
  `v5` plan docs: origin/v5 at `04c7fd4f`. No posts on GitHub issue #11 (René tells Markus).
- **Consumers and their agents** (ask the agent for runs and data, never browse their repos; answer a request the same
  day with its todo item first; agent names change per session, `ListAgents`; if neither resolves at a release, put
  the release note here and tell René in one line):
  - ai-soc (4 Nov: pinned at ea30bfda on rho v4.104.0 + sigma v0.11.162; its v5 data layout work is on its branch
    `v5-new-datalayout`). Tiered main store: ShardWindow 24h, ColdAfter 3 d, MaxOpenColdShards 2;
    HistoryDeltaEncoding, SyncWrites, DurableCommit, RelsByMint, AdjacencyIndexOnDisk, LabelIndexOnDisk,
    ValueDictionaries, UntimedUnbounded, AllowRetentionPurge; DerivedPage's property index on disk
    (PropertyIndexOnDiskLabels); `DepthWarm` on windowed reads; `CreateRelTemporal` on its 29 estate-indexed rel
    types; unique keys on reference labels only; Derived (and its composite index) on the reference shard; its
    /metrics exports `aisoc_store_shard_access_total{tier}` from ShardAccessStats. Event times UTC (Check Point CSV in
    the operator's zone, BO: America/Bogota); Flux runs Europe/Berlin.
  - sigma-tkgd (4 Nov: v0.11.162 on rho v4.104.0; plans `ValueThreshold` 32 KiB on tiered for its 64 KiB DerivedPage
    blocks); its test confirms rho's retention-purge contract (null at or above the watermark, `ErrRetentionExpired`
    below); its store analysis tool is `projects/derived-writeback` (cmd/wbscan).
  - agent-bookkeeping (`~/Work/2026/bds421/sigma/agent-bookkeeping`): build check only.
- **Housekeeping:** deletions are René's call (`rm` is denied to agents; user-only actions below; finished scratch goes
  on the RHO_DONE list in `~/cleanup.sh`). Consumer build check: Appendix B.

## Glossary

BA / BO = one customer's data day each (ai-soc estates); Flux = the machine where ai-soc runs full runs (paths /srv/fast/rene/...);
PINnn = ai-soc's numbered dependency pin; `/bars`, `/tkg/asof` = ai-soc's HTTP read doors; R-numbers (R19, R20, R32 ...) and O1-3, P1, F0b = sigma's own work items (O1-3 = sigma's
resident-state work: counting state leaves RAM; P1 its first step); a "cut" = one committed ingest step (a tx pin); rows NN = the
user-request rows (all in `git show aeb7f8e0:tasks/todo.md`); backlog NN = the items of "## Backlog" below (closed ones:
docs/CHANGELOG.md); todo tags and their rows: CHAIN, CHAIN-2 = row 122; R37 = 121; REOPEN = 92; PENDING = 102;
RELSIDECAR = 105; PURGE = 104; COLDSTART = 108; GUARDS = 118; FLAKE, SUITE, NOCOMPRESS = 119; V5 = 61; SHARD = 117;
lesson NN = line NN of the lesson index in docs/CONTRIBUTING.md § Lessons.

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

René's word 26 Oct: the merged item branches are deleted (39, flushpart with -D) and row102-pending-ranged,
tombstone-writer-v4.63, v5-realstore-harness and wp-37-import-throughput were pushed as backups; since 27 Oct all
unmerged branches are in `archive/unmerged-2026-10-27` (see Branches).

1. `! rm -rf /dev/shm/sigma-*` (sigma test dirs: 33 dirs, /dev/shm 4.5 GB on 4 Nov; ask sigma first, some may be a
   running test's) and the merged worktrees in `~/Work/2026/datainsights/sigma-tkgd` (`git worktree list`).
2. `! rm ~/Work/2026/datainsights/rho-tkg/warm` (stray 10-byte file, untracked; agents leave it alone).
3. gh-dashboard scratchpad (667 MB, path in that session); tell that session directly if wanted.

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time. `origin/codex/*` (5 branches) and `origin/round10` are Markus's; their md is not ours.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main` (and its `engine/`),
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)` (the release under test),
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`
(the ai-soc `engine/` with `GOTOOLCHAIN=go1.27.3` in front of both commands).
