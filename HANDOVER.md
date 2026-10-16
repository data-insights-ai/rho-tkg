# HANDOVER — rho-tkg v4 line (2026-10-16, main = tag v4.56.1)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). Latest tag `v4.56.1`; main = tag. Read in this order: this file, `tasks/lessons.md` (digest at the top),
`tasks/todo.md` (user requests, releases table), `tasks/backlog.md` (open table, then specs), `CHANGELOG.md` top.
What shipped and why is in `CHANGELOG.md` and git, not here.

## 1. Focus and next steps (René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC")

1. **State (2026-10-16):** main = v4.56.1, GitHub CI green, nothing in progress, no agents running, ledger check passes
   (`scripts/check-ledger.sh`). Last user request rows 41-44 in `tasks/todo.md` are done.
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
3. **Next scheduled change:** v4.57.0 flips the default of `Config.UntimedValidStart` for NEW stores to `UntimedUnbounded`
   (announced as Deprecated in v4.56.0). Only do it together with the next minor feature or when a consumer asks.
4. **Open work:** the open table of `tasks/backlog.md` (old items, no requester; sigma's optional 71). Nothing is blocking.
5. **Watch origin:** run `WATCH_LOG_DIR=<scratchpad> scripts/watch-origin.sh` in the background. It exits only on a push by
   someone else to main/tags/other branches or a real PR/issue change; routine v5 pushes by Markus only go to its log. When
   Markus releases on main: merge origin/main, check his new code against our rules (one valid start, Void rows, Elements,
   format markers, head cache, version allocation), fix his CHANGELOG heading if he left entries under [Unreleased].

## 2. Standing rules (one line each; detail in `tasks/lessons.md` and the memory notes)

- MINOR for new features, else PATCH (79). Deprecation ritual for behaviour changes (`docs/stability.md`, 79); a changed
  default is announced by a CHANGELOG `Deprecated` entry plus a godoc note (no `// Deprecated:` marker on a field that stays).
- Release authority: René's standing process (2026-10-10, lesson 79) is merge → gate → tag → push main and the tag → notify,
  without asking each time. The v4 release workflow does not push to `origin/v5`; never force-push. Markus explicitly authorized the final v5 takeover push below.
- Act, do not offer (81). Red break-the-code test first, clean root-cause fix (78).
- Models (René's map, lesson 79, overrides ~/CLAUDE.md §3's "pick by difficulty"): builds Opus, code reviews Opus effort high,
  final Fable review of the merged batch before a tag, docs and ledger work Sonnet. One review round per branch; authors run
  `make lint-docker` and `make security-docker` before reporting.
- Gate: `GOFLAGS=-timeout=30m make ci-docker` exit 0, plus GitHub CI green (75); 12-15 min, rerun on a quiet host.
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
- Markus requested a v5 handover to René on 2026-10-16, with one final push to `origin/v5`. Continue from section 7; this grants no main merge/tag/release authority for v5.
- No posts on GitHub issue #11 (René tells Markus himself).

## 7. v5 pointer

**René takeover, 2026-10-16:** Markus asked to finish only the bounded fixes already
in flight, commit them, and make one final push to `origin/v5`. All three Sol
agents are stopped; no owned native jobs remain. Pull `v5` and read
[`tasks/evidence/v2-integrated-checkpoint-20261016/handover-79b1cff9/README.md`](tasks/evidence/v2-integrated-checkpoint-20261016/handover-79b1cff9/README.md)
for exact source pins, portable evidence, reproductions and remaining failures.
All fixes are integrated commits, not patches awaiting application. V2 remains
unaccepted at 42 accepted planning points; its full A–F exits remain unchanged.
The current P01 regression, directory-sync reopen and WAL short-write pass;
state retained-feed recovery and WAL EIO classification still fail. Two earlier
feed/AS9 failures also remain open; full current gates and the cost matrix have
not run. Continue with those concrete failures before expanding campaigns.
Latest main metadata through `97efa1c0` is merged at `e76b8b9b`; it changed no
v5 implementation source. The tested implementation pin is `79b1cff9`.

v5 is Markus Nissl's separate Go module `.../rho-tkg/v5` in `v5/` on branch `origin/v5`.
**Consumers cannot switch yet:** the public surface remains value packages; graph construction,
transactions, reads and changes remain internal. Internal certified current/historical reads and
`tools/v5-migrate`'s qualified narrow preflight exist; neither is a public graph API or accepted
consumer migration. Rho owns database/storage invariants; sigma owns reasoning and its adapter;
embedding consumers supply transport/process orchestration.

The original `docs/v5/PLAN.md` governs full V0–V7; `tasks/backlog.md` records current phase status.
V0/V1 are accepted and V2 remains open. Exact finite source/test receipts live under
`tasks/evidence/`; old test counts, estimates, pins and earlier merge observations remain historical.
`docs/v5/HANDOVER.md` is external review, reconciled against PLAN, not permission to reduce those
gates or push/merge main. `docs/v5-analysis.md` §5 keeps the 45-family consumer mapping; immutable
sigma/ai-soc inventories and current v4 source deltas are in `docs/v5/reference/consumer-*.json`.
The current integrated v4 source baseline is `0fa37786`/v4.56.1, unqualified
for the current v5 reader/importer. Main metadata through `97efa1c0` is also merged. The persisted
`Config.UntimedValidStart` policy, explicit conversion, store marker 7, replicated
`ChangeMeta` and export `uu` header must be carried into V4/V6 mapping and
reader/importer qualification; older receipts cover none of these additions.
This changes the v4 row/store format, implicit valid-from rule, void pieces,
indexes and read APIs, including sorted equality/composite postings with keyset
page push-down. Existing consumer manifests still describe older source;
refresh their source deltas and prove the reader/importer against the new formats
before migration acceptance. No earlier qualification covers this baseline.
Final source selection precedes any new ten-target reader gate.
Historical v4.51.7 at `0eb0a0ac` is qualified by
`tools/v5-migrate/evidence/main4517-0eb0a0ac.json` (absolute Go1.26.9, 96.2% total,
both preflight functions 100%); that receipt does not cover newer sources.
v4.51.6 was source-only; public batched consumer/selector/routing proof is separate.
Historical v4.51.5 nonempty retained-state cases passed at `da5d32e6` (96.2%,
both preflight functions 100%); new retention/refusal and consumer/destination-import
qualification remain pending. Historical `9e805ddc`/v4.51.4 and earlier receipts
are immutable; exact upstream CI/source equivalence and this mapping slice do
not establish consumer adoption or V6 acceptance.
**Markus integrates v5; this handover authorizes no main push/tag/release** (decision 5).
The v4 release workflow remains separate; this explicitly authorized v5 takeover keeps shared
CHANGELOG/backlog/lessons/AGENTS/Makefile/CI changes when reconciling main.

## Appendix A — origin watch

`scripts/watch-origin.sh` (see section 1.5). Re-arm it after each exit; one instance at a time.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main/engine`,
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.
