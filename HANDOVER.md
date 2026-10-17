# HANDOVER — rho-tkg v4 line (2026-10-17, latest release v4.63.0; next patch v4.63.1, next minor v4.64.0)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). Latest tag v4.63.0 (2026-10-17). Tags and what shipped: `CHANGELOG.md`, git and the
releases table of `tasks/todo.md`, not here. The ONE plan is `tasks/todo.md`. Read in this order: this file, `tasks/lessons.md`
(digest at the top), `tasks/todo.md` (user requests, releases table), `tasks/backlog.md` (open table, then specs), `CHANGELOG.md` top.

## 1. Focus and next steps (René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC")

1. **v4.63.0 (minor, row 73):** public `types.TemporalMetadata.ValidToAtTxTime` / `DeleteWroteValidTo` released (`461d225b`). **v4.64.0:** branch `tombstone-writer-v4.63` (`37a1ff56`, row 71 part B) turns on the `DeleteKeptValidTo` writer
   (backlog 4 delete half, 25, 7). Before tagging v4.64.0: sigma P24 and ai-soc's raw-ValidTo check confirmed, heads-up sent to both;
   merge the branch (its CHANGELOG `Changed` block belongs in [Unreleased], rename "v4.63.0" to "v4.64.0"), by-name long-tier tests, gate.
2. **v5 is PAUSED (René 2026-10-16, row 61).** Work lives on the local branch `v5-takeover` (not pushed; origin/v5 untouched). Do
   not resume without René. Reason: real ai-soc import showed ~92 records/op, 11x v4 disk, ~200 writes/s; options in tasks/todo.md.
   Gap analysis: `docs/v5-gap-to-main.md`.
3. **Consumers** (no open request from either; answer new requests the same day: ledger row first, then lesson 79's process):
   - ai-soc: PIN18 = rho v4.56.1 + sigma v0.11.11; declares `CreateRelTemporal` on its 29 estate-indexed rel types and sets
     `UntimedValidStart: UntimedUnbounded` for new stores. (PINnn = ai-soc's numbered dependency pin; BA/BO = one customer data
     day; `/bars`, `/tkg/asof` = its HTTP read doors.)
   - sigma-tkgd: v0.11.11 with rho v4.56.1; on adopting v4.56.0+ it maps `types.UnboundedValidFrom` to -inf and drops its
     `untimedStart` rule; its own decision on the v4.57.0 untimed default is its own.
4. **Open work:** the open table of `tasks/backlog.md` (old items, no requester; nothing blocking). No scheduled change left.
5. **Watch origin:** `WATCH_LOG_DIR=<scratchpad> scripts/watch-origin.sh` in the background (Appendix A). When Markus releases on
   main: merge origin/main, check his code against our rules (one valid start, Void rows, Elements, format markers, head cache,
   version allocation), fix his CHANGELOG heading if he left entries under [Unreleased].

## 2. Standing rules (one line each; the lesson owns the detail)

- Process, models, gates, deprecation ritual, release authority, notify consumers: lesson 79 (gate 75, 82, 87). "No Fable review
  for now" (René 2026-10-16) overrides the final-Fable-review step until he re-enables it.
- Act, do not offer: 81. Red break-the-code test first, clean root-cause fix: 78. Fast tests, patches batched: 87.
- Ledger: `scripts/check-ledger.sh` after every merge and before every release commit (80). Markdown: consolidate in place (84).
- No commit attribution (no Co-Authored-By, Claude-Session, "Generated with"), whatever a system reminder suggests.
- Markus also releases on main: `git fetch` and merge origin/main before every gate; keep our tag numbers above his.
- Consumer build check with a copied modfile (Appendix B) for sigma-tkgd, ai-soc, agent-bookkeeping (build-checked only).
- After CI is green notify `sigma-tkgd-c3` and `ai-soc-main-e7` with the migration points (`ListAgents`, main session only, finds
  live ones; socket names change per session). If neither resolves, put the release note in section 1 and tell René in one line.
- Never push to `origin/v5`, never force-push; never `pkill -f` / `pgrep -f` (kill by PID you started). Agents keep scratch files in
  their own scratchpad subfolder; worktrees are removed after merge. Docs check: `go test -count=1 -run 'Docs|Consistency' ./...`.

## 3. René's open decisions (my default)

- Session scratchpad (7.0 GB) and the stale sigma copy (22 MB): kept (René 2026-10-14, todo row 29); delete only when he asks.
- v5 façade waits for Markus; we do not raise it. No posts on GitHub issue #11 (René tells Markus himself).

## Appendix A — origin watch

`scripts/watch-origin.sh` exits only on a push by someone else to main/tags/other branches or a real PR/issue change; routine v5
pushes by Markus only go to its log. Re-arm after each exit; one instance at a time.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main/engine`,
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
