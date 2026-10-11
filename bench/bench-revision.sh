#!/usr/bin/env bash
# Run the current benchmark harness against one immutable production revision.
# Usage: bench-revision.sh REF OUTFILE FILTER COUNT [BENCHTIME]
set -euo pipefail
ref="${1:?revision}"; out="${2:?output}"; filter="${3:?filter}"
count="${4:?count}"; benchtime="${5:-0.3s}"
root="$(git rev-parse --show-toplevel)"
harness="$root/bench"
out="$(cd "$(dirname "$out")" && pwd)/$(basename "$out")"
scratch="$(mktemp -d)"
worktree="$scratch/revision"
cleanup() {
  git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT
revision="$(git -C "$root" rev-parse "$ref^{commit}")"
git -C "$root" worktree add --detach "$worktree" "$revision" >&2
# Only the test harness is replaced; production source and module dependencies
# retain the requested revision. Removing the old harness also prevents stale
# benchmark files from silently adding different rows to the baseline.
rm -rf "$worktree/bench"
cp -R "$harness" "$worktree/bench"
if git -C "$worktree" diff --name-only | awk '$0 !~ /^bench\// { bad=1 } END {exit bad}'; then :
else echo 'benchmark overlay changed production files' >&2; exit 1; fi
started="$(date +%s)"
echo "bench-revision: revision=$revision count=$count benchtime=$benchtime filter=$filter" >&2
(
  cd "$worktree"
  go test -bench="$filter" -benchmem -benchtime="$benchtime" -count="$count" -run '^$' ./bench
) | tee -a "$out"
echo "bench-revision: revision=$revision duration=$(( $(date +%s) - started ))s" >&2
