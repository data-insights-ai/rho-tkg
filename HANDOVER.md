# HANDOVER — rho-tkg v4 line (2026-10-23: latest tag v4.74.0 = Admin().NodeStoredSizes / ForEachNodeStoredSize + winners read in place (row 121); v4.73.0 the listing page door + tiered read snapshot; v4.72.2 a history version read decodes once; v4.72.1 a cold node read decodes its properties once; v4.72.0 memory history temporal-meta read + timeline resolver on skeletons (ai-soc R37); v4.71.0 tiered history temporal-meta read + timelines lend the current row (sigma R37) + tag-only CI + test cleanup; v4.70.3 tiered MaxOpenColdShards counter fix + sharded invalid-ID fix + architecture/CONTRIBUTING rewrite; v4.70.2 gosec fix of the gate tool + `make ci-fast` as the patch gate; v4.70.1 md consolidation; v4.70.0 rows 104-105 sigma O1-3 doors; v4.69.2 row 106; v4.69.1 row 100; v4.69.0 row 99)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). This file = state, consumers, glossary, user-only actions. File roles: README.md § Documentation.

Next steps and René's open decisions: `tasks/todo.md`.

## Brief for the agents (2026-10-23 evening; read before anything else)

**Both agents.** The main session (Opus) does all the work itself, analysis and code; ONE persistent Fable agent reviews
and monitors; no other subagents without René's go (CONTRIBUTING § Rules digest "Who works"). Work order: todo item
first, then the red test, the change, the tests green, the Fable review; a finished item moves from todo to CHANGELOG
under its tag. Commit with explicit paths, never `git commit -a` (it swept unreviewed code onto main on 23 Oct; reverted
in 2be9a231). Consumer data and runs: ask the owning agent (sigma "Handover and README review", ai-soc `ai-soc-main-45`),
never browse their repos; Flux rule: copies only, counts/sizes/relation names out. badger facts: `docs/badger.md`
(read from v4.9.2's source, measured, Fable-reviewed; rules 1-11) — check a badger claim there or in the source, never
from memory.

**Opus (builder), where things stand:**
- **FLUSHPART (Now, a patch).** A flush above badger's transaction limit (about 10 MB / 104,855 entries) is several
  badger transactions; rho's flush adds its ops in map order, then counters, then change-log records, and calls it
  atomic (docs/badger.md rule 1). Tests written, uncommitted: `pkg/graph/store/badger/flush_split_test.go`
  (`TestFlushAboveBatchLimitIsAllOrNothingAfterCrash`, `...WithRemovalsReopensAfterCrash`; 8 MB memtable so the limit
  is ~13,100 entries; crash = `FailNextFlushForTest` on the flush and on Close's final flush). They are NOT red yet: all
  20,000 nodes and records survive. Cause not found; next check the durable state right after the failing `Flush`
  (`DBForTest`), whether the requeued ops are written by another path before Close, and the write buffer's self-flush
  (`MaxPendingWrites` 100,000, `badgerstore_flush.go:111`). Planned fix (not yet reviewed): commit a flush that fits in
  one badger `Txn` as today; on `ErrTxnTooBig`, stage it under a private key prefix, commit a small marker, apply, drop
  the marker, delete the staging keys; at open, marker present = re-apply, absent = discard staging. Have Fable review
  the design before building.
- **SHARD (Next).** Design ADR-0012 (architecture.md); plan with boxes in todo. Step 1 (refuse a missing shard
  directory, one `shardDir` seam) is on local branch `shard-step1` (5da2cdfa, not pushed): code + red tests; still to
  change: pre-create `reference/` and `archive/` only for a new store (René's choice (b)). Adopted review points for
  later steps: the mover checks a flock on the shard DIRECTORY (the `LOCK` file is only a pid file, not copied); a
  read-only handle is closed before a writable open of the same directory; `DiskBytes` counts blocks; a shard with
  `.mem` files gets one writable open before a move or a read-only open.
- **Later items:** GETWALK, PREFETCH, ITERPREFIX, VLOGGC (badger rules 2-4, 9); RESUME (sigma's red test is in its
  message; one resumption row too many per rewritten step); CHAIN waits for René's "run" (sigma's
  `projects/derived-writeback` scan on the Flux copy `/srv/fast/rene/rho-analysis-k10230918/store`).

**Fable (reviewer and monitor):** review every step's diff and test output against `docs/badger.md`, ADR-0012 and the
SHARD plan; write no code or tests, run none; answer only what is wrong or missing. Already reviewed: ADR-0012 (three
volumes, mover, global index), docs/badger.md (seven corrections confirmed; the Seek cost settled by profile in
`tasks/evidence/BADGER/results.txt`). Next for review: the FLUSHPART design, then its diff; SHARD step 1's diff.

## State

- **Scope:** just essential patches, except ai-soc items (standing go, René 2026-10-21); lesson 79 (10).
- **Tombstone writer** parked (René via sigma 2026-10-19: "not now"); v4.64.0 was never cut; resume plan: `tasks/todo.md` D4.
- **v5 PAUSED** (René 2026-10-16, row 61): local branch `v5-takeover` (not pushed; origin/v5 untouched). Do not resume without René.
  Reason (real ai-soc slice, 14.3 k ops): ~73 records/op, 5.6 KB/op on disk, 157-176 ops/s; projected for 5 M ops ~28 GB (11x v4 disk), 8-9 h. The
  91.8 records/op is a synthetic in-memory probe; options A/B/C are in no committed file: `git show af75a982:tasks/evidence/v5-scale/notes.md`.
  Gap analysis (file removed 2026-10-22): `git show 7c0a4b74:docs/v5-gap-to-main.md`. Real-store harness: local branches `v5-realstore-harness`
  (`af75a982`) and `wp-37-import-throughput` (`392f88e2`); not pushed. Local branch `v5` (`b1193dc0`) = stale, behind origin/v5 (`91426024`) by 1,440 commits, 1 ahead (its extra review file is in main at `0622da63`); deletable (René, user-only action 5). Since the takeover (row 51) `pkg/graph` + `v5compat` are ours on `v5-takeover` (open WPs in todo; WP history `git show aeb7f8e0:tasks/todo.md`); no posts on GitHub issue #11 (René tells Markus).
- **Consumers** (answer new requests the same day: ledger row first, then lesson 79):
  - ai-soc: root module `ai-soc-main` measured on rho v4.73.0 (`engine/` still v4.41.0), tiered as the main store (`AdjacencyIndexOnDisk`, `RelsByMint`, `SyncWrites`, `DepthWarm` on
    windowed reads); declares `CreateRelTemporal` on its 29 estate-indexed rel types and sets `UntimedValidStart:
    UntimedUnbounded` for new stores. (PINnn = ai-soc's numbered dependency pin; BA/BO = one customer data day; `/bars`,
    `/tkg/asof` = its HTTP read doors.)
  - sigma-tkgd: main adopted rho v4.73.0 (ListDerivedRows on the page door); its store analysis tool is
    `projects/derived-writeback` (cmd/wbscan).
  - Notify after gate + tag + push: sigma (session "Handover and README review") and `ai-soc-main-45` (`ListAgents`;
    names change per session). If neither resolves, put the release note here and tell René in one line.
- **Unpushed:** main is pushed; only branch `shard-step1` (SHARD step 1 work) is local.
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
- AXIS: `UntimedWholeAxis` as the default for new stores, announced in CHANGELOG `[4.65.0]` for "the next minor" and held
  for five minors. Default: keep holding; the next minor makes it or rewords the notice.
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

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time. `origin/codex/*` (5 branches) and `origin/round10` are Markus's; their md is not ours.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main` (and its `engine/`),
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
