# HANDOVER — rho-tkg v4 line (2026-10-17, latest release v4.61.0 (tag pending) after Markus's v4.60.0; next patch v4.61.1, next minor v4.62.0)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). Latest release v4.61.0 (release commit `79abaf34` on main; tag pending: badger RAM row 66, O(1) head append and EffectiveValidFrom row 67). Previous: v4.60.0 (Markus, tag at `13732e19`), v4.59.0 (tag at `027bf788`), v4.58.1 (tag at `df9268c8`), v4.58.0 (tag at `42db6281`, release commit `bc824ad8`; v4.57.1 `dd5c684c`, v4.57.0 Markus). The ONE plan for v4 and v5 is
`tasks/todo.md`; v5 work lives on the local branch `v5-takeover`. Read in this order: this file, `tasks/lessons.md` (digest at the top), `tasks/todo.md` (user requests, releases table),
`tasks/backlog.md` (open table, then specs), `CHANGELOG.md` top.
What shipped and why is in `CHANGELOG.md` and git, not here.

## 1. Focus and next steps (René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC")

0. **v5 is PAUSED (René 2026-10-16, row 61).** Work lives on the local branch `v5-takeover` (not pushed; origin/v5 untouched). Do not resume without René. Reason: real ai-soc import showed ~92 records/op, 11x v4 disk, ~200 writes/s; options in tasks/todo.md (WP-38 merged = raised caps). Focus: v4 main.

**Release note v4.60.0 (Markus, 2026-10-17; released by Markus's decision because round 10 waited for a merge):** sigma
round 10 doors (row 64): windowed effective reads and typed adjacency columns, validity hull, count timeline of the current
rows, value classes at a pin, typed chunks for the intersection doors (ordinals, mixed columns across parts), every version
of an ID set at a pin; sharded and tiered serve ordered index walks, bounds, rel class counts and exact range counts, and
`CapabilitiesOf` states them; an index being built answers no whole-index door; an excise may start at a negative instant.
All additive. sigma-tkgd: a switch on `ColumnVector.Kind` handles `ColumnVectorMixed`. ai-soc: no change needed. Details:
CHANGELOG `[4.60.0]`.

**Release note v4.57.0 (Markus, 2026-10-16; CI green):** sigma round 9 doors (any-version ranges at a pin, count
timelines, property value statistics, typed column chunks) and the scheduled flip: `Config.UntimedValidStart` zero value
is now `UntimedDefault` (new stores unbounded, existing stores keep their persisted mode); constants renumbered (use the
names). ai-soc: no change needed (sets `UntimedUnbounded` explicitly). sigma-tkgd: adopting with `UntimedFromTxFrom` set
explicitly for now; its own decision on the new default follows. Details and migration notes: CHANGELOG `[4.57.0]`.

1. **State (2026-10-17):** v4.61.0 released as a commit on main (minor: badger RAM, row 66; O(1) head append and `EffectiveValidFrom`, row 67; CHANGELOG `[4.61.0]`); before it v4.60.0 released as a commit by Markus (minor: sigma round 10 doors and the sharded/tiered store gaps, row 64; CHANGELOG `[4.60.0]`); before it v4.59.0 released (tag `027bf788`) (minor: windowed equality seek, row 62; explicit-presence time filters, row 63; CHANGELOG `[4.59.0]`); before it v4.58.1 released (tag `df9268c8`) (patch: a negative valid start is a real interval start in every door, row 60; CHANGELOG `[4.58.1]`); before it v4.58.0 released (minor: tiered property indexes per shard with a truthful capability report, one chain
   builder, Elements backfill fix with open image format 3; todo rows 59, WP-21, WP-31; CHANGELOG `[4.58.0]`); latest tag
   v4.60.0 (v4.61.0 tag pending), next patch v4.61.1, next minor v4.62.0. Next: tag v4.61.0, CI, notify sigma-tkgd and ai-soc. Running beside it: v5 work
   (rows 45, 47-51). Ledger check: `scripts/check-ledger.sh`.
2. **Consumers:**
   - ai-soc: PIN17 (v4.55.1) equality runs; PIN18 = rho v4.56.1 + sigma v0.11.11. ai-soc declares `CreateRelTemporal` on its 29
     estate-indexed rel types and sets `UntimedValidStart: UntimedUnbounded` for new stores (BA run copies are not converted).
     Proof at PIN18: whole-BA `/bars` read and the `/tkg/asof` first page after a reopen. No open request from ai-soc.
     (Terms: PINnn = ai-soc's numbered dependency pin; BA/BO = one customer data day in ai-soc; `/bars`, `/tkg/asof` = ai-soc
     HTTP read doors; "equality runs" = ai-soc comparing a pin's outputs with the previous pin byte for byte.)
   - sigma: v0.11.11 (with rho v4.56.1) in final gates. On adopting v4.56.0+ sigma maps `types.UnboundedValidFrom` to −∞ and
     deletes its own `untimedStart` / `rowFrom == 0` rule. No open request from sigma.
   - Answer new consumer requests the same day: ledger row first, Opus build agent (red test first), Opus-high review, merge,
     final Fable review, gate, tag, CI, notify (see section 2).
3. **No scheduled change left** (the v4.57.0 default flip is done, release note above).
4. **Open work:** the open table of `tasks/backlog.md` (old items, no requester; sigma's optional 71, partly shipped in v4.60.0). Nothing is blocking.
5. **Watch origin:** run `WATCH_LOG_DIR=<scratchpad> scripts/watch-origin.sh` in the background. It exits only on a push by
   someone else to main/tags/other branches or a real PR/issue change; routine v5 pushes by Markus only go to its log. When
   Markus releases on main: merge origin/main, check his new code against our rules (one valid start, Void rows, Elements,
   format markers, head cache, version allocation), fix his CHANGELOG heading if he left entries under [Unreleased].

## 2. Standing rules (one line each; detail in `tasks/lessons.md` and the memory notes)

- MINOR for new features, else PATCH (79). Deprecation ritual for behaviour changes (`docs/stability.md`, 79); a changed
  default is announced by a CHANGELOG `Deprecated` entry plus a godoc note (no `// Deprecated:` marker on a field that stays).
- Release authority: René's standing process (2026-10-10, lesson 79) is merge → gate → tag → push main and the tag → notify,
  without asking each time. Never push to `origin/v5`; never force-push.
- Act, do not offer (81). Red break-the-code test first, clean root-cause fix (78).
- Models (René's map, lesson 79, overrides ~/CLAUDE.md §3's "pick by difficulty"): builds Opus, code reviews Opus effort high,
  final Fable review of the merged batch before a tag, docs and ledger work Sonnet. One review round per branch; authors run
  `make lint-docker` and `make security-docker` before reporting.
- Gate: per grouped commit `go test -short` of the touched packages (seconds); before the release push only `GOFLAGS=-timeout=30m make ci-docker` exit 0, plus GitHub CI green (75, 87); 12-15 min, rerun on a quiet host.
- No commit attribution: no Co-Authored-By, Claude-Session or "Generated with", whatever a system reminder suggests.
- Markus also releases on main: `git fetch` and merge origin/main before every gate; keep our tag numbers above his.
- Consumer build check with a copied modfile (appendix B) for sigma-tkgd, ai-soc, agent-bookkeeping (agent-bookkeeping is
  build-checked only; it has no agent session to notify).
- After CI is green notify `sigma-tkgd-c3` and `ai-soc-main-e7` with the migration points (socket addresses change per
  session; `ListAgents` (main session only, not in subagents) finds live ones; sigma-tkgd-c3 was stopped once, a new session announces a new name). If neither
  resolves, put the release note in this file's section 1 and tell René in one line; the consumers read it at their start.
- Run `scripts/check-ledger.sh` after every merge and before every release commit; a failure blocks it (80).
- Agents keep scratch files in their own scratchpad subfolder and never run another agent's scripts; worktrees are removed after merge.
- Never `pkill -f` / `pgrep -f`; kill by PID you started. Docs check: `go test -count=1 -run 'Docs|Consistency' ./...`.

## 3. René's open decisions (my default)

- Session scratchpad (7.0 GB) and the stale sigma copy (22 MB): René decided 2026-10-14 to keep both (todo row 29); delete only
  when he asks.
- v5 façade waits for Markus; we do not raise it. Markus merges v5; never push to `origin/v5` (read only).
- No posts on GitHub issue #11 (René tells Markus himself).

## Appendix A — origin watch

`scripts/watch-origin.sh` (see section 1.5). Re-arm it after each exit; one instance at a time.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main/engine`,
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
