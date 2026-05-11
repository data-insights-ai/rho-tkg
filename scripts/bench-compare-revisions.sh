#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
refs=("$@")
if [[ ${#refs[@]} -eq 0 ]]; then
  refs=(HEAD 4ee8c9e d0706de)
fi

bench_count="${BENCH_COUNT:-1}"
bench_time="${BENCH_TIME:-1s}"
bench_pattern="${BENCH_PATTERN:-Benchmark(AddNode|AddRelationship|AddNodeLabel|RemoveNodeLabel)}"
out_dir="${BENCH_OUT:-/tmp/tkg-bench-compare-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$out_dir"

worktrees=()
cleanup() {
  for wt in "${worktrees[@]}"; do
    git -C "$repo_root" worktree remove --force "$wt" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

sanitize_ref() {
  printf '%s' "$1" | tr '/:@^~ ' '______'
}

bench_pkg_for() {
  local wt="$1"
  if [[ -f "$wt/pkg/graph/internal/core/bench_ingest_test.go" ]]; then
    printf './pkg/graph/internal/core'
    return
  fi
  if [[ -f "$wt/pkg/graph/bench_ingest_test.go" ]]; then
    printf './pkg/graph'
    return
  fi
  return 1
}

summary="$out_dir/summary.tsv"
printf 'ref\tbenchmark\tns/op\tB/op\tallocs/op\n' > "$summary"

for ref in "${refs[@]}"; do
  safe_ref="$(sanitize_ref "$ref")"
  wt="$out_dir/worktree-$safe_ref"
  raw="$out_dir/$safe_ref.txt"
  worktrees+=("$wt")

  git -C "$repo_root" worktree add --detach "$wt" "$ref" >/dev/null
  pkg="$(bench_pkg_for "$wt")"

  {
    echo "# ref: $ref"
    echo "# package: $pkg"
    echo "# pattern: $bench_pattern"
    echo "# count: $bench_count"
    echo "# benchtime: $bench_time"
    (
      cd "$wt"
      go test -run '^$' -bench "$bench_pattern" -benchmem -benchtime "$bench_time" -count "$bench_count" "$pkg"
    )
  } | tee "$raw"

  rows="$out_dir/$safe_ref.rows.tsv"
  awk -v ref="$ref" '
    /^Benchmark/ {
      # go test bench fields:
      # name iterations ns ns/op bytes B/op allocs allocs/op
      printf "%s\t%s\t%s\t%s\t%s\n", ref, $1, $3, $5, $7
    }
  ' "$raw" > "$rows"
  if [[ ! -s "$rows" ]]; then
    echo "error: no benchmark rows matched for $ref in $pkg" >&2
    exit 1
  fi
  cat "$rows" >> "$summary"
done

echo
echo "Raw outputs: $out_dir"
echo "Summary: $summary"
column -t -s $'\t' "$summary" || cat "$summary"
