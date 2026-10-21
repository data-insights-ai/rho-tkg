# HANDOVER — rho-tkg v4 line (2026-10-21, latest release v4.69.0; v4.64.0 never cut, writer parked)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). Latest tag v4.69.0 (2026-10-21, row 99 IsReferenceLabel); v4.68.0 (2026-10-21, row 98 multi-pin timelines); v4.67.0 (2026-10-21, ai-soc tiered rows 93-97); v4.66.3 (2026-10-20, rows 87/88 grow cliff; v4.66.2 row 85 write cost; v4.66.1 2026-10-20; v4.66.0 2026-10-18 Markus; v4.65.0 was round 11; the `UntimedWholeAxis` default flip is held, no target). Tags and what shipped: `CHANGELOG.md`, git and the
releases table of `tasks/todo.md`, not here. The ONE plan is `tasks/todo.md`. Read in this order: this file, `tasks/lessons.md`
(digest at the top), `tasks/todo.md` (user requests, releases table), `tasks/backlog.md` (open table, then specs), `CHANGELOG.md` top.

## 1. Focus and next steps (René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC")

1. **No open request from René.** Shipped 2026-10-21 for ai-soc (tiered = main store; René's standing go for ai-soc runtime items):
   v4.69.0 `g.Admin().IsReferenceLabel` (row 99); v4.68.0 `NodeEffectiveTimelines(id, pins)` + Rel twin (row 98, 511 pins
   520,711 → 1,020 rows); v4.67.0 tiered `SyncWrites` (93), `RelsByMint` + Depth on adjacency + `RelsByMintFallbacks()` (94),
   `AdjacencyIndexOnDisk` (95; 22 M rels heap 770 → 240 B/rel, rel maps 52 B/rel, node maps 77 B/node), `GetOrCreateByKeyOpts`
   (96), review fixes (97). Open from it: deep hot-shard pending has no ai-soc case (no demotion fallback built); a sharded rel
   row with local start and remote end gets no in-key repair at open (pre-existing).
   Done before: v4.66.3 (rows 87/88, sigma R19 b+c): no cliff past 512 head pieces, head entry survives a
   growing bounded end. R19 (a) segment iterator: no-go (row 89, ai-soc BA counts: deep chains are grow). Quadratic grow rows:
   René "1" (row 90) = sigma writes only [old To, new To); rho keeps its per-base split. v4.66.2 (row 85) before it.
   - Next patch candidate (review finding, not essential unless a consumer holds such rows): whole-axis entities patched while
     closed under v4.66.0 keep a stale current row (core/temporal_cascade.go:702-712 matches only PrevHash == current's hash).
   - Review findings NOT taken (wait for René): tests review #5-#10, code review #4-#7, docs #9-#10 rest (details in todo rows 77-82).
   - The `DeleteKeptValidTo` writer (branch `tombstone-writer-v4.63`) stays parked ("not now", René 2026-10-19); needs a rebuild on
     the v4.66 axis code as a later minor (v4.67.0 is now the tiered work).
2. **v5 is PAUSED (René 2026-10-16, row 61).** Work lives on the local branch `v5-takeover` (not pushed; origin/v5 untouched). Do
   not resume without René. Reason: real ai-soc import showed ~92 records/op, 11x v4 disk, ~200 writes/s; options in tasks/todo.md.
   Gap analysis: `docs/v5-gap-to-main.md`. Real-store harness + scale notes: local branches `v5-realstore-harness` (`af75a982`,
   `tasks/evidence/v5-scale/notes.md`) and `wp-37-import-throughput` (`392f88e2`); not pushed.
3. **Consumers** (open: sigma FYI row 92: ai-soc reopen `ReadHistory` 526 s vs 57 s, rho's readUnderRLock unchanged since v4.66.0, waits for ai-soc; answer new requests the same day: ledger row first, then lesson 79's process):
   - ai-soc: pins v4.67.0+ with AdjacencyIndexOnDisk, RelsByMint, SyncWrites on and DepthWarm on windowed reads (2026-10-21); release/v5.0.0 was on rho v4.66.0; PIN21 = rho v4.61.0 + sigma v0.11.21 (RAM numbers in row 73 area: VmHWM ~63-65 GB whole BA); earlier PIN18 = rho v4.56.1 + sigma v0.11.11; declares `CreateRelTemporal` on its 29 estate-indexed rel types and sets
     `UntimedValidStart: UntimedUnbounded` for new stores. (PINnn = ai-soc's numbered dependency pin; BA/BO = one customer data
     day; `/bars`, `/tkg/asof` = its HTTP read doors.)
   - sigma-tkgd: v0.11.41 (2026-10-21) on rho v4.68.0: plural NodeEffectiveTimelines in ReadDerivedAt, DepthHot on Derived/Session lookups; row 99 IsReferenceLabel shipped in v4.69.0 (sigma moves its check into session/writer open, F0b); P24
     (marker-aware ValidTo reads) parked with the tombstone writer.
4. **Open work:** the open table of `tasks/backlog.md` (no requester).
5. **Watch origin:** `WATCH_LOG_DIR=<scratchpad> scripts/watch-origin.sh` in the background (Appendix A). When Markus releases on
   main: merge origin/main, check his code against our rules (one valid start, Void rows, Elements, format markers, head cache,
   version allocation), fix his CHANGELOG heading if he left entries under [Unreleased].

## 2. Standing rules (one line each; the lesson owns the detail)
- René 2026-10-21 (via ai-soc): go "to all items which can enhance the AI SOC on Flux regarding runtime or something else" — ai-soc runtime requests are built without a further go (minor for a new door).

- **Just essential patches** (René 2026-10-17, row 74): only a consumer-blocking or wrong-answer/data bug gets work; no new features,
  minors or backlog sweeps unless René asks. The tombstone writer stays parked on `tombstone-writer-v4.63` (René via sigma 2026-10-19: "not now"; it no longer
  cherry-picks onto v4.66.0: Markus's axis-aware `stampDeleteTombstone` and `EndValidity` need a rebuild, minor v4.67.0, ~half a day).
  sigma (2026-10-17) stays on rho v4.61.0 under the same rule, does P24 only when it must move to v4.64.0+, and asks us not to hold v4.64.0 for it.

- Process, models, gates, deprecation ritual, release authority, notify consumers: lesson 79 (gate 75, 82, 87). Fable only writes code, runs no tests; every test, gate and doc is the main session's (79). Reviews on Fable
  again; a build Opus cannot finish goes to a Fable agent (René 2026-10-20: "If opus is not smart enough use a fable Agent .. use Fabel as reviewer").
- Act, do not offer: 81. Red break-the-code test first, clean root-cause fix: 78. Fast tests, patches batched: 87.
- Ledger: `scripts/check-ledger.sh` after every merge and before every release commit (80). Markdown: consolidate in place (84).
- No commit attribution (no Co-Authored-By, Claude-Session, "Generated with"), whatever a system reminder suggests.
- Markus also releases on main: `git fetch` and merge origin/main before every gate; keep our tag numbers above his.
- Consumer build check with a copied modfile (Appendix B) for sigma-tkgd, ai-soc, agent-bookkeeping (build-checked only).
- Right after gate + tag + push (not after CI, lesson 82) notify `sigma-tkgd-c3` and `ai-soc-main-e7` with the migration points (`ListAgents`, main session only, finds
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
