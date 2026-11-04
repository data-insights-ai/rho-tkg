# HANDOVER — rho-tkg v4 line (releases: `git tag`; what each changed: `docs/CHANGELOG.md`)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). This file holds state only: releases, branches, consumers, glossary, the numbered backlog, user-only actions.
Rules (read every session): `docs/CONTRIBUTING.md` § Rules digest. Next steps and René's open decisions with my
defaults: `tasks/todo.md`. File roles: README.md § Documentation.

## State

- **State of 4 Nov, read this first** (next steps: `tasks/todo.md`). Round 5 (RELHIST P2, a patch) is released.
  René (4 Nov, via ai-soc): "I want all current versions in the release", which lifted the round-4 freeze;
  sigma tags on v4.104.1, ai-soc runs its gate (60-min 4 arms + masked cycles), pins and releases ai-soc v5.1.2 on
  that pair. Measuring is ai-soc's combined run on main's tip, not per item (René via ai-soc); ai-soc names its quiet
  windows: then no minor or major gate, probe, bench or long test here.
  - **Released in v4.104.2 (4 Nov):** COSTCOUNT and the release cycle on Go's facilities (`make release-patch|minor|
    major`, `scripts/release`; the patch gate 12 s warm, 662 s cold in a fresh checkout). The pre-commit hook is gone
    (René, 4 Nov). Parked, unmerged: branch `consolidate` (test folding, René stopped it 4 Nov).
  - **FIXED-MEMORY:** step 0 measured (`tasks/evidence/FIXED-MEMORY/step0.txt`); step 1 (existence files on the
    reference shard and archive) is next, on a branch, red test first. Path (b) waits for René.
  - **Reviews:** René (3 Nov, "yes, always"): one read-only Fable review per finished branch, before its merge, given
    the code and the diff, not my conclusions.
- **Checklist facts learned at releases:** the ai-soc engine builds only with GOTOOLCHAIN=go1.27.3 (its parent module
  requires it); merging a release branch back can move Unreleased CHANGELOG entries under the tag's heading, so check
  the heading after every such merge; the release tool is `go run ./scripts/release` (`make kind`, `gate-*`, `release-*`).
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
  on the RHO_DONE list in `~/cleanup.sh`, run before and after every release). Consumer build check (diagnosis only): `git show 904fd063:HANDOVER.md`, Appendix B.

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
2. gh-dashboard scratchpad (667 MB, path in that session); tell that session directly if wanted.

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time. `origin/codex/*` (5 branches) and `origin/round10` are Markus's; their md is not ours.
