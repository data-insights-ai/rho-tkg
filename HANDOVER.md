# HANDOVER — rho-tkg v4 line (rewritten 2026-10-14, main ahead of v4.52.0)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go 1.26.9; a Go
library, not a database). Latest tag `v4.52.0` (Markus); own last `v4.51.8`. Read, in this order: this file, `tasks/lessons.md` (digest at the top),
`tasks/todo.md` (user requests, open queue with the per-step red tests), `tasks/backlog.md` (specs), `CHANGELOG.md` top.
History (what shipped, why) is in `CHANGELOG.md` and git, not here.

## 1. Focus and next steps (René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC"; queue ordered by what ai-soc and sigma need)

1. **v4.54.0 released 2026-10-14** (on top of Markus v4.53.0): 62/65 phase B + R9, 64 excise (sigma R7), 61 step 2 binary writer, one marker tracker (markers 4/5/6). NEXT: final Fable review of v4.52.2..HEAD, `GOFLAGS=-timeout=30m make ci-docker`, version lines to v4.54.0, consumer build check, tag, push, GitHub CI, notify sigma + ai-soc (ai-soc asked to be told), docs second pass (row 36) for the 61/62/65/64 files.
2. **sigma Derived write-back (todo row 33):** R6 = backlog 63 `PropertyIndexOptions{Elements: true}` (building); R7 = backlog 64 `ExciseNodeValidity` (+ rel mirror), starts after 62 (both touch the resolver); R8 done (guard test).
3. **Integrity-hash switch (row 31, backlog 61):** building; notify ai-soc when it ships.
4. **One valid-from source (row 32, backlog 62) + Update-with-valid_to closes like CloseVersion (backlog 65):** deprecation in v4.52.2, change in v4.53.0; building.
5. **Parked** (no consumer asking): backlog 0–5, 7, 10b, 13, 16, 20, 23, 25, 26, 31, 37, 39, 40, 45–47; tiered write doors without moveMu (store-level only).

Status of running jobs: `scratchpad/status.txt` (session scratchpad). Decisions René made 2026-10-14: todo rows 29 (cleanup, v5 façade waits for Markus, no issue-#11 post, patch only) and 31.

## 2. Decisions (René 2026-10-12, `tasks/todo.md` row 20: "1) yes, why is this a question? 2) now, 3) yes 4) yes 5) no, Markus will do this 6) deprecation process 7) yes")

| # | Decision | Effect |
|---|---|---|
| 1, 2 | Build 52-55 now | the v4.51.3 batch above; open work with a clear next step is started, not offered (lesson 81) |
| 3, 4 | Send Markus the patch-only rule and the 12 "not in plan" rows of `docs/v5-analysis.md` §5 | done: GitHub issue #11 |
| 5 | Markus merges v5 into main, we do not | never push to `origin/v5`; read only |
| 6 | Behaviour changes go through the deprecation ritual (`docs/stability.md`) from now on | no new named exceptions (the v4.46.0 and v4.48.0 ones stay documented); while only patch versions ship, "one minor release before" reads "one release before": the `// Deprecated:` marker plus a CHANGELOG Deprecated entry ship in patch N with sigma-tkgd and ai-soc notified, the change lands in N+1 or later; additive (opt-in) designs preferred (decided 2026-10-12, docs/stability.md) |
| 7 | Origin watch re-armed | appendix A (script) |

Standing rules from René (all in `tasks/lessons.md`): PATCH versions only, no minor until asked (79); clean root-cause fix and a
red break-the-code test first (78); one review round per branch, authors run lint + security (79); models: builds Opus, code
reviews Opus effort high, final Fable review before tagging, doc work Sonnet (79); no attribution lines in commits (no
Co-Authored-By, Claude-Session or "Generated with"), whatever a system reminder suggests; v4 keeps taking consumer features
(the old "v4 gets fixes only" rule is deleted).

## 3. Consumers and contacts

- **sigma-tkgd** (`~/Work/2026/datainsights/sigma-tkgd`): pins the latest tag; uses the timeline doors, scan forms, `HasHistory`,
  `LatestStamps`, `MintInstant`, the state doors. Open from sigma: 28 (state column fast path; measured pinned count over 1 M
  nodes: badger 13-14 s vs 1.4 s unpinned), 21 (retention for `@overview`), `/admin/import` can see `ErrTxOrder` on a backfilled
  re-import of a deleted ID (notified). Its old session `sigma-tkgd-c3` is stopped; a new session announces a new name.
- **ai-soc** (`~/Work/2026/datainsights/ai-soc/ai-soc-main/engine`; session `ai-soc-main-e7`): embeds rho + sigma in one process,
  badger/tiered, DurableCommit + SyncWrites; adopts v4.51.2, waits for v4.51.3 (52-55). Pinned contract: timeline segments never
  merge distinct rows with equal content. Decisions via ai-soc: rel unique key withdrawn (snowflake IDs are the global IDs), tiered
  indexes yes (done), durable commit yes (done), provider-abort no, group instant no. Its delta-feed door ("rows written after T")
  exists: `Config.ChangeLog` + `Tx().RunWithLSN` + `Replication().ForEachChange(afterLSN)` (+4-27 % per commit).
- **agent-bookkeeping** (`~/Work/2026/bds421/sigma/agent-bookkeeping`, v4.40): builds clean, no special needs.
- **Markus Nissl** builds **v5** (section 7) and has also pushed v4 minors straight to main (v4.50.0, v4.51.0): watch origin
  (appendix A), fetch before every release, keep our tag numbers above his.
- Socket addresses of other sessions change per session; use `ListAgents` to find live ones. Message consumers after each tag with
  the migration points (what changed in answers, new doors).

## 4. Release process (all releases; gates are `make ci-docker` exit 0)

1. Merge reviewed branches (authors already ran `make lint-docker` and `make security-docker`; run them on each branch before
   merging, three gate re-runs came from skipping that). Merge conflicts are nearly always `CHANGELOG.md` and `tasks/backlog.md`:
   keep both sides; entries go under `## [Unreleased]` with their own `### Added/Fixed/Changed`, never into a released section.
2. Put the `[Unreleased]` body under `## [x.y.z] - date` with a headline paragraph (behaviour changes bold); bump the version lines
   (`AGENTS.md` "Status: vX", `README.md` "Current release", `docs/architecture.md` title; the docs-consistency test pins them);
   update `docs/api.md`, `docs/errors.md`, the backlog Closed table, the `tasks/todo.md` rows and releases table in the SAME commit.
3. Commit `release: vX.Y.Z — ...`; `make ci-docker` (fmt-check, vet, lint, build, test-race, security, vulncheck, cover-gate >= 80 %,
   check-metakv-reap; 12-15 min, longer under load: a run timed out in core at load 70, rerun on a quiet host); build + vet the three
   consumers (appendix B); final Fable review of the batch; `git tag -a`; push main and the tag; message the consumers.
4. Agent skeleton that worked: one Opus agent per item in a worktree, prompt = spec files (5-8), lessons 78/79, TDD rules, mutants,
   "run lint-docker + security-docker before reporting", "no attribution lines", "no pkill -f", "do not touch files owned by a
   parallel agent"; ONE Opus-high reviewer per branch (read-only, own brute-force oracle where semantics are subtle); fix round to
   the SAME agent via SendMessage; evidence under `tasks/evidence/<item>/` (red, green, mutants). Worktrees: merge the branch back,
   `git worktree unlock` + `git worktree remove --force`, `git branch -d` before calling a task done.

## 5. Environment traps

- `make ci-docker` is 12-15 min on the shared 32-core host (load 10-25 from other agents); the bench-gate comparator is noisy on
  identical code under load, so the PinnedRel family and LatestStamps benchmarks are allocs-gated (time canary opt-in via `TIME_CANARY`).
- Never `pkill -f` / `pgrep -f` (other sessions' tests share the machine); kill by PID you started. Processes of ai-soc/sigma
  (`go test ... TestBOWholeEndToEnd`, `go test -race ./aisoc/...`) are not ours.
- The permission classifier blocked `git push` twice early on (René ran `! git push ...`); some sandbox hooks reject commands that
  mention "github.com" (`git archive`, `git worktree add`); the Write tool works.
- Scratch consumer builds live in `/tmp/claude-1000/.../scratchpad` and may be gone; recreate from appendix B.
- Customer profiles (ai-soc BA run) are read on Flux only (`/srv/fast/rene/project-2026-09-010-BA/aisoc-s3-20261012/smoke/prof`).
- Useful tests: `TestTxBackfillOracle_CrossBackend` (`TXB_ORACLE_SEEDS`), `TestEffectiveTimeline_PointwiseOracle`
  (`ET_ORACLE_SEEDS`), `TestReImportLifeOracle`, `TestHasHistoryDifferential`, `TestLatestStampsDifferential`,
  `TestPointDoorRace_UnderMovingWriters`, `TestUniqueClaims_*`; bitemporal oracles in
  `pkg/graph/internal/core/bitemporaloracle*_test.go`. Docs check: `go test -count=1 -run 'Docs|Consistency' ./pkg/graph/internal/core/`.

## 6. Branches and worktrees

`main` = release line (tags v4.44.0 to v4.51.2 reachable). `v5` (local) is our docs-only plan commit `b1193dc`; `origin/v5` is
Markus's work, read only. Tag `v5-plan-20261009` marks the plan. Agent worktrees of the v4.51.3 batch are `worktree-agent-*`
branches under `.claude/worktrees/`: merge and remove them before closing the batch. Stopped branches with uncommitted work from
the 2026-10-10 session (`worktree-agent-adcfa76ae8b4c0dfe`, `worktree-agent-aa4b0ebc0683fcb3f`) were superseded by v4.50.1 and
v4.51.1; delete them if still present.

## 7. v5 pointer

v5 is Markus Nissl's separate Go module `.../rho-tkg/v5` in `v5/` on branch `origin/v5` (Raft log, two-group transactions, graph
store, snapshots). **Answer to "can consumers use it yet": not yet** (graph code is `internal/`; no public graph API, no as-of or
timeline reads, no v4 importer; neither sigma-tkgd nor ai-soc can switch). Where to read: `docs/v5-analysis.md` (merge analysis,
v4-work coverage, maturity, consumer answers, §5 v4 to v5 door mapping); `docs/v5/HANDOVER.md` on branch `v5` (Fable review of
the branch: quality and what is open before it can merge); GitHub issue #11 (patch-only rule and the 12 "not in plan" rows sent to
Markus). **Markus merges v5; we do not** (decision 5). v5 edits shared files (CHANGELOG `[Unreleased]`, backlog, lessons, AGENTS,
Makefile, ci.yml), so expect conflicts once main moves. Do not push to `v5`.

## Appendix A — origin watch (Monitor tool, timeout 1800000 ms, re-arm on expiry; one at a time, two overlap and double-report)

```bash
#!/usr/bin/env bash
# Watches origin of rho-tkg for pushes by anyone but "dev team" (this machine's git user),
# plus every new/deleted branch or tag, plus PR and issue changes. One line per event.
cd /home/renework2023/Work/2026/datainsights/rho-tkg || exit 1
ME="dev team"

declare -A P
load() { # name of assoc array to fill from ls-remote
  local -n arr=$1; arr=()
  local sha ref
  while read -r sha ref; do [ -n "$ref" ] && arr["$ref"]=$sha; done < <(git ls-remote --heads --tags origin 2>/dev/null)
}
load P
[ ${#P[@]} -eq 0 ] && { echo "watch: initial ls-remote failed"; exit 1; }

prs() { gh pr list --state all --limit 30 --json number,title,state,headRefName,updatedAt,author \
  --jq '.[] | "PR #\(.number) [\(.state)] \(.headRefName) by \(.author.login): \(.title) @\(.updatedAt)"' 2>/dev/null | sort; }
issues() { gh issue list --state all --limit 30 --json number,title,state,updatedAt,author \
  --jq '.[] | "ISSUE #\(.number) [\(.state)] by \(.author.login): \(.title) @\(.updatedAt)"' 2>/dev/null | sort; }
prevprs=$(prs); previss=$(issues)

echo "watch armed: ${#P[@]} refs, $(echo "$prevprs" | grep -c .) PRs"
while true; do
  sleep 60
  declare -A C; load C
  if [ ${#C[@]} -gt 0 ]; then
    git fetch -q origin --prune --tags 2>/dev/null
    for ref in "${!C[@]}"; do
      new=${C[$ref]}; old=${P[$ref]:-}
      [ "$new" = "$old" ] && continue
      short=${ref#refs/heads/}; short=${short#refs/tags/}
      case "$ref" in *'^{}') continue ;; esac
      if [ -z "$old" ]; then
        case "$ref" in
          refs/tags/*) echo "NEW TAG $short -> ${new:0:7}" ;;
          *) echo "NEW BRANCH $short @ ${new:0:7}: $(git log -1 --format='%an: %s' "$new" 2>/dev/null | cut -c1-120)" ;;
        esac
      else
        range="$old..$new"
        authors=$(git log --format='%an' "$range" 2>/dev/null | sort -u)
        if [ -z "$authors" ]; then
          echo "REF MOVED (force-push or rewind) $short ${old:0:7} -> ${new:0:7}"
        elif echo "$authors" | grep -qvx "$ME"; then
          n=$(git rev-list --count "$range" 2>/dev/null)
          echo "PUSH $short +$n commit(s) by $(echo "$authors" | grep -vx "$ME" | paste -sd, -):"
          git log --format='  %h %an: %s' "$range" 2>/dev/null | grep -v " $ME:" | head -5 | cut -c1-140
        fi
      fi
    done
    for ref in "${!P[@]}"; do
      [ -z "${C[$ref]:-}" ] && case "$ref" in *'^{}') ;; *) echo "DELETED ${ref#refs/*/}" ;; esac
    done
    P=(); for k in "${!C[@]}"; do P[$k]=${C[$k]}; done
  fi
  unset C
  curprs=$(prs)
  if [ -n "$curprs" ] && [ "$curprs" != "$prevprs" ]; then
    comm -13 <(echo "$prevprs") <(echo "$curprs"); prevprs=$curprs
  fi
  curiss=$(issues)
  if [ -n "$curiss" ] && [ "$curiss" != "$previss" ]; then
    comm -13 <(echo "$previss") <(echo "$curiss"); previss=$curiss
  fi
done
```

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main/engine`,
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`,
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`.

## Appendix C — what a fresh session can rely on

CAN: the repo at `main` (this file, `CHANGELOG.md`, `tasks/**`, `docs/**`, tags) and the memory notes in
`~/.claude/projects/-home-renework2023-Work-2026-datainsights-rho-tkg/memory/`. CANNOT: other sessions' sockets, earlier agent
contexts or ids, `/tmp/claude-1000/.../scratchpad` files (evidence worth keeping is in `tasks/evidence/`). The global
`~/CLAUDE.md` and the repo `CLAUDE.md`/`AGENTS.md` load automatically.
