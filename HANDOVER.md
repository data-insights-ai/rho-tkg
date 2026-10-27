# HANDOVER — rho-tkg v4 line (2026-10-27: latest tag v4.89.0; every release and what it changed: `docs/CHANGELOG.md`)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). This file holds state only: releases, branches, consumers, glossary, the numbered backlog, user-only actions.
Rules (read every session): `docs/CONTRIBUTING.md` § Rules digest. Next steps and René's open decisions with my
defaults: `tasks/todo.md`. File roles: README.md § Documentation.

## State

- **Released 24-27 Oct:** v4.75.0 to v4.89.0 (v4.83.0 R37, v4.84.0 EXISTMAP, v4.84.1 TXSNAP, v4.85.0 ROWPROBE, v4.86.0 TIERSTATS, v4.86.1 ASYNCSTAGE, v4.87.0 OPENWALK, v4.87.1 EVENTCLOCK + PENDSEEK, v4.87.2 IDLESTAMP, v4.88.0 OPENIMAGE, v4.88.1 PROPSTR, v4.89.0 OPENIMAGE step 2), each under its tag in `docs/CHANGELOG.md`. Nothing unreleased on main.
- **v4.85.0** (ROWPROBE) and **v4.86.0** (TIERSTATS): CI green, consumers build and vet clean, sigma and ai-soc
  notified; scale probes as v4.84.0. v4.85.0's ci-docker passed every step but the ledger (a todo line ending in
  "Done"); the todo-only fix reran docs tests and the ledger, not the whole gate. Since GATETIER (main, after
  v4.86.0) ci-docker also runs `make test-long` (~1 min).
- **v4.86.1** (ASYNCSTAGE, a patch): ci-fast green plus every other package's short tests, consumers build and vet
  clean; async ingest 1.55-2.0x (`tasks/evidence/WRITERATE/results.txt`).
- **v4.87.0** (OPENWALK, a minor: the open image gains two sections, an older release rebuilds once): tag CI
  green; ci-docker green on the second run (the first hit VERIFY V2's multi-block fixture, fixed with the release), consumers build
  and vet clean; scale probe in `tasks/evidence/SCALE/existence_maps.txt` (reopen with 1 M rels 1.25 s to 0.13 s).
- **v4.87.1** (EVENTCLOCK + PENDSEEK, a patch for ai-soc's Flux run k10262212 and kit A/B): tag CI and ci-fast green, consumers
  build and vet clean; a mutation without an event publisher takes no event timestamp, hot and warm checkouts stamp
  no `lastAccess`, a disk property equality lookup seeks its buffered entries (64 k buffered: 5.2 ms to 3.5 us).
- **v4.87.2** (IDLESTAMP, a patch: v4.87.1 stamped `lastAccess` at a demotion and turned sigma's TestFoldReadDepthHot
  red): tag CI and ci-fast green, consumers build and vet clean; sigma's three tests checked on it (two pass, the third is sigma's).
- **v4.88.0** (OPENIMAGE step 1, a minor: a crash open applies the older open image): tag CI green; ci-docker green on the second
  run (the first failed six TestIndexRebuildStats tests in test-race, fixed with the release; its ledger step then
  flagged a todo line ending in "Done", fixed in the release commit), consumers build and vet clean; a crash reopen
  after one record decodes 1 row at 1 M entities (the full rebuild 1.54 M); 25 delta-open mutations caught.
- **v4.88.1** (PROPSTR, a patch for ai-soc's BO run on v4.87.2): tag CI and ci-fast green, consumers build and vet clean; an
  on-disk string equality lookup beside 10,000 longer strings visits 2 keys, before 10,001. The first cut added
  `IDSet.Bounds`, which the patch gate refuses as a door (exported names in internal packages count, CONTRIBUTING
  Rules digest); the bounds now come from two seeks on the node keyspace.
- **v4.89.0** (OPENIMAGE step 2, a minor: `Config.OpenImageInterval` writes a periodic open image, header format 4):
  tag CI green; ci-docker green on the third run (the first failed fmt-check on `badgerstore.go`, the second two unconvert
  findings, both fixed on the branch),
  consumers build and vet clean; at 1 M nodes / 1 M rels a 16 MB image, 31-33 ms encode under the locks, crash
  reopen 2.7 s (300 rows) against 18.0 s (4.0 M rows) for the full rebuild; 15 checkpoint and 4 pass-through
  mutations caught.
- **Branches:** asyncstage (merged into v4.86.1), openwalk (merged into v4.87.0), eventclock and pendseek (merged into v4.87.1), idlestamp (merged into v4.87.2), openimage (merged into v4.88.0; origin/openimage is the copy before its rebase onto main, not force-pushed), propstr (merged into v4.88.1, local only), checkpoint (merged into v4.89.0, local only), propseek (PROPSEEK, af31a6da, local, unmerged: next v4.89.1). The merged item branches are deleted and the kept ones pushed
  (René, 26 Oct); user-only action 5 stays. main is pushed.
- **v5 PAUSED** (René 2026-10-16, row 61): local branch `v5-takeover` (not pushed; origin/v5 untouched). Do not resume without René.
  Reason (real ai-soc slice, 14.3 k ops): ~73 records/op, 5.6 KB/op on disk, 157-176 ops/s; projected for 5 M ops ~28 GB (11x v4 disk), 8-9 h. The
  91.8 records/op is a synthetic in-memory probe; options A/B/C are in no committed file: `git show af75a982:tasks/evidence/v5-scale/notes.md`.
  Gap analysis (file removed 2026-10-22): `git show 7c0a4b74:docs/v5-gap-to-main.md`. Real-store harness: local branches `v5-realstore-harness`
  (`af75a982`) and `wp-37-import-throughput` (`392f88e2`); not pushed. Local branch `v5` (`b1193dc0`) = stale, behind origin/v5 (`91426024`) by 1,440 commits, 1 ahead (its extra review file is in main at `0622da63`); deletable (René, user-only action 5). Since the takeover (row 51) `pkg/graph` + `v5compat` are ours on `v5-takeover` (open WPs in todo; WP history `git show aeb7f8e0:tasks/todo.md`); no posts on GitHub issue #11 (René tells Markus).
- **Consumers and their agents** (ask the agent for runs and data, never browse their repos; answer a request the same
  day with its todo item first; agent names change per session, `ListAgents`; if neither resolves at a release, put
  the release note here and tell René in one line):
  - ai-soc, agent `ai-soc-main-45`: pins rho v4.83.0 (26 Oct; v4.84.0 and v4.84.1 notified: pins v4.84.1 at its next bump, PIN) with sigma v0.11.65. Tiered
    main store: ShardWindow 24h, ColdAfter 3 d, MaxOpenColdShards 2; HistoryDeltaEncoding, SyncWrites, DurableCommit,
    RelsByMint, AdjacencyIndexOnDisk, LabelIndexOnDisk (PropertyIndexOnDisk switched on 26 Oct, P4BSTORE 77d7fb7c,
    and off again 27 Oct until PROPSTR: string lookups walk every longer string), ValueDictionaries, UntimedUnbounded, AllowRetentionPurge;
    `DepthWarm` on windowed reads; `CreateRelTemporal` on its 29 estate-indexed rel types; unique keys on reference
    labels only; Derived (and its composite index) on the reference shard. Pins rho v4.88.0 with sigma v0.11.94 (27 Oct);
    its /metrics exports `aisoc_store_shard_access_total{tier}` from ShardAccessStats (kit BA slice: warm, cold and
    cold opens 0). Event times UTC (Check Point CSV in the
    operator's zone, BO: America/Bogota); Flux runs Europe/Berlin.
  - sigma-tkgd, agent `sigma-tkgd-bb`: bumps to rho v4.85.0 and merges ROWPROBE on the door (26 Oct); PAGEREF/PAGESTATS
    next, on TIERSTATS. v0.11.80 (26 Oct: KEYDICT; v0.11.79 pins rho v4.84.1 in every module, full
    gate under race passed, no test adapted for EXISTMAP's ErrCorruptWire) (VALUES and TKGID not wired yet); its
    test confirms rho's retention-purge contract (null at or above the watermark, `ErrRetentionExpired` below); its
    store analysis tool is `projects/derived-writeback` (cmd/wbscan).
  - agent-bookkeeping (`~/Work/2026/bds421/sigma/agent-bookkeeping`): build check only.
- **Housekeeping:** deletions are René's call (`rm` is denied to agents; user-only actions below). Consumer build
  check: Appendix B.

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
tombstone-writer-v4.63, v5-realstore-harness and wp-37-import-throughput are pushed to origin as backups.

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
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg-main go.mod)` (main's worktree: the release under test),
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
