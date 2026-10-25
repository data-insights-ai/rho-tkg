# HANDOVER — rho-tkg v4 line (2026-10-25: latest tag v4.78.2 = MOVEREAD (reads of a moving shard go on from a read-only handle on its source) + TRUNCPIN (tiered history truncates hold the cold shards they write); v4.78.1 = DISCARDCLOSE (a scan or rel-type build snapshot ends without Discard after Close) + DROPALL (Clear never calls DropAll; an empty value names a failed value-log read); v4.78.0 = the volume minor, SHARD steps 2-8 (warm and cold volumes, `MoveShard`, the idle sweep's moves, `MoveShardOffline`, `ShardInfo` volume, bytes and cold opens; catalog `__format__` entry: a one-way door once a shard leaves hot); v4.77.2 = ARM32 (32-bit builds again) + LONGTIER (the badger long tier passes again; tests only); v4.77.1 = RESUME (a valid-time write ending where the row valid there starts appends no resumption copy); v4.77.0 = DICT (value dictionaries: a listed property's string values stored once per store or shard, a token in each row; row format 8, store marker 11: a one-way door); v4.76.1 = SHARD step 1 (a cataloged shard whose directory is missing fails with fs.ErrNotExist instead of opening empty; reference/ and archive/ made only for a new store); v4.76.0 = OPENEXCISE (an excise accepts an open end; row format 7, store marker 10: a one-way door); v4.75.5 = SAMESTART (a same-start chain closed narrower no longer exposes the older row); v4.75.4 = CLEARSHARDS (a crash inside a tiered or sharded Clear reopens empty: pending marker, registry shard last); v4.75.3 = CLEARMETA (a crash inside Clear reopens the old store or an empty one); v4.75.2 = VERIFY + BADGERLOG (value and block checksums on, badger panics on rho's reads become ErrCorruptWire, badger's log reaches slog by default, a value-log cut at open is reported); v4.75.1 = DECODEBOUND (the row decoder rejects msgpack extension types: an 80-byte row took 13 s); v4.75.0 = FLUSHPART (a flush commits as one badger transaction or staged, format marker 9) + KEYSIZE/VALUESIZE (store.ErrEntryTooLarge at every badger write door); v4.74.0 = Admin().NodeStoredSizes / ForEachNodeStoredSize + winners read in place (row 121); v4.73.0 the listing page door + tiered read snapshot; v4.72.2 a history version read decodes once; v4.72.1 a cold node read decodes its properties once; v4.72.0 memory history temporal-meta read + timeline resolver on skeletons (ai-soc R37); v4.71.0 tiered history temporal-meta read + timelines lend the current row (sigma R37) + tag-only CI + test cleanup; v4.70.3 tiered MaxOpenColdShards counter fix + sharded invalid-ID fix + architecture/CONTRIBUTING rewrite; v4.70.2 gosec fix of the gate tool + `make ci-fast` as the patch gate; v4.70.1 md consolidation; v4.70.0 rows 104-105 sigma O1-3 doors; v4.69.2 row 106; v4.69.1 row 100; v4.69.0 row 99)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). This file = state, consumers, glossary, user-only actions. File roles: README.md § Documentation.

Next steps and René's open decisions: `tasks/todo.md`.

## Brief for the agents (2026-10-24; read before anything else)

**The main session (Opus) works alone** (René 24 Oct: the Fable reviewer is deleted; no subagents without René's go,
CONTRIBUTING § Rules digest "Who works"). Work order: todo item first, then the red test, the change, the tests green,
the self-review and mutation check (CONTRIBUTING § Review); a finished item moves from todo to CHANGELOG under its tag.
Commit with explicit paths, never `git commit -a` (it swept unreviewed code onto main on 23 Oct; reverted in 2be9a231).
Consumer data and runs: ask the owning agent (sigma "Handover and README review", ai-soc `ai-soc-main-45`), never
browse their repos; Flux rule: copies only, counts/sizes/relation names out. badger facts: `docs/badger.md` (v4.9.2:
sections 1-7 the analysis, 8 the complete API, 9 what rho should integrate) — trust a claim only with a source line or
a measurement, and test what rho relies on. `tasks/todo.md` and this brief are updated at every event, on main, from
the main worktree `../rho-tkg-main` when a branch is checked out.

**Where things stand** (order, René 24 Oct, DICT moved up 25 Oct: SHARD step 1, DICT, BADGER step 3 / SHARD steps
2-10, then DROPALL, DISCARDCLOSE, METASYNC):
- **v4.75.2 (VERIFY + BADGERLOG)** shipped; ai-soc's multi-MB value-log rows are now checksummed.
- **v4.75.3 (CLEARMETA)**, **v4.75.4 (CLEARSHARDS)**, **v4.75.5 (SAMESTART)** and **v4.76.0 (OPENEXCISE)** shipped.
  **v4.76.1 (SHARD step 1)**, **v4.77.0 (DICT)** and **v4.77.1 (RESUME)** and **v4.77.2 (ARM32, LONGTIER)**, **v4.78.0 (SHARD steps
  2-8, the volume minor)**, **v4.78.1 (DISCARDCLOSE, DROPALL)** and **v4.78.2 (MOVEREAD, TRUNCPIN)** shipped; ai-soc measures DICT on a rebuilt slice before it pins (a one-way door). Next:
  BADGER step 3, SHARD step 10 (René, with ai-soc's counts); METASYNC's rig is René's call. Open decisions are settled from the data and the consumer agents (René 24 Oct).
- **SHARD**: design ADR-0012 (architecture.md), plan with boxes in todo; step 1 shipped in v4.76.1 (option (b)), steps 2-8 in v4.78.0.
  Points for later steps: the mover checks a flock on the shard DIRECTORY (the `LOCK` file is only a pid file);
  a read-only handle is closed before a writable open of the same directory; `DiskBytes` counts blocks; a shard with
  `.mem` files gets one writable open before a move or a read-only open.
- **Queued:** DICTMERGE and PROPPOS (after DICT's measurement); IDPROPS (after ai-soc's on-disk product; CHAININTERN withdrawn: 0.31 % of the heap); WRITTENAT, VALUES, TKGID, TIERLABELDISK; GETWALK,
  PREFETCH, ITERPREFIX, VLOGGC, OPENIMAGE; CHAIN waits for René's word (a Flux run); STORE is with René. Parked:
  BADGERPANIC (badger as it is). The local race gate runs its parts in sequence, badger isolated (v4.76.0).

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
