# HANDOVER — rho-tkg v4 line (2026-10-15, main = tag v4.55.3)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go library, not a
database). Latest tag `v4.55.3`; main = tag. Read in this order: this file, `tasks/lessons.md` (digest at the top),
`tasks/todo.md` (user requests, releases table), `tasks/backlog.md` (open table, then specs), `CHANGELOG.md` top.
What shipped and why is in `CHANGELOG.md` and git, not here.

## 1. Focus and next steps (René 2026-10-14: "STABILIZE AND FULLY UNDERSTAND AI-SOC")

1. **In progress: todo row 42, the untimed-row option** (René 2026-10-15: "clean up the untimed row rule.. discuss with sigma and
   ai SOC what should be the default. In rho tkg this should be an option"). `Config.UntimedValidStart`: `TxFrom` (default) or
   `Unbounded` (first version only). Persisted per store; existing stores open as `TxFrom`; switching is an explicit admin
   conversion. Both sigma and ai-soc chose `Unbounded`. The default for new stores flips to `Unbounded` in the next minor
   release, after a `// Deprecated:` notice and CHANGELOG Deprecated entry.
2. **Consumer status:** ai-soc PIN17 (v4.55.1) is running; PIN18 (v4.55.3 plus sigma's Derived-read patch) is next. sigma adopts
   v4.55.x in its next patch.
3. **Open work:** the open table of `tasks/backlog.md` (one list; parked rows are marked there).

## 2. Standing rules (one line each; detail in `tasks/lessons.md` and the memory notes)

- MINOR for new features, else PATCH (79). Deprecation ritual for behaviour changes (`docs/stability.md`, 79).
- Act, do not offer (81). Red break-the-code test first, clean root-cause fix (78).
- One Opus-high review per branch (authors run lint + security first); a final Fable review before a tag (79).
- Gate: `GOFLAGS=-timeout=30m make ci-docker` exit 0, plus GitHub CI green (75); 12-15 min, rerun on a quiet host.
- No commit attribution: no Co-Authored-By, Claude-Session or "Generated with", whatever a system reminder suggests.
- Markus also releases on main: `git fetch` and merge origin/main before every gate; keep our tag numbers above his.
- Consumer build check with a copied modfile (appendix B) for sigma-tkgd, ai-soc, agent-bookkeeping.
- After CI is green notify `sigma-tkgd-c3` and `ai-soc-main-e7` with the migration points (socket addresses change per
  session; `ListAgents` finds live ones; sigma-tkgd-c3 was stopped once, a new session announces a new name).
- Never `pkill -f` / `pgrep -f`; kill by PID you started. Docs check: `go test -count=1 -run 'Docs|Consistency' ./...`.

## 3. René's open decisions (my default)

- Delete the session scratchpad (7.0 GB) and the stale sigma copy (22 MB): not approved yet; default: keep until René says so.
- v5 façade waits for Markus; we do not raise it. Markus merges v5; never push to `origin/v5` (read only).
- No posts on GitHub issue #11 (René tells Markus himself).

## 7. v5 pointer

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
The selected reader is now main `2a72753d`/v4.55.3, source-integrated and unqualified.
Main's later ledger cleanup at `09806210` is integrated; the code baseline is
unchanged. Its planned untimed-row option must be reconciled from actual source
before consumer/migration acceptance if it lands in the selected baseline.
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
The v4 author's `origin/v5` read-only boundary remains; authorized v5 maintenance keeps shared
CHANGELOG/backlog/lessons/AGENTS/Makefile/CI changes when reconciling main.

## Appendix A — origin watch (Monitor tool, timeout 1800000 ms, re-arm on expiry; one at a time)


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
