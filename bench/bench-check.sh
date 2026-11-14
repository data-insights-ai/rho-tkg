#!/usr/bin/env bash
# bench/bench-check.sh — LOCAL regression gate for the bench/ suite.
#
# Runs a fresh benchmark pass and compares it against
# bench/local-baseline.txt (captured by `make bench-baseline`) via
# bench-compare.sh — the shared comparator the CI gate also uses — failing
# (exit 1) on any scenario whose time regressed by more than
# REGRESSION_THRESHOLD_PCT (default 15) percent.
#
# Baselines are per-machine (see .gitignore — bench/local-baseline.txt is
# never committed): never compare a baseline captured on one machine
# against a run on another; hardware/load noise dwarfs real regressions.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
baseline="$repo_root/bench/local-baseline.txt"
current="${BENCH_CURRENT:-$repo_root/bench/local-current.txt}"
csv_report="${BENCH_REPORT_CSV:-$repo_root/bench/local-benchstat.csv}"

if [[ ! -f "$baseline" ]]; then
  echo "bench-check: $baseline not found — run 'make bench-baseline' first" >&2
  exit 1
fi

echo "bench-check: running a fresh benchmark pass -> $current" >&2
go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$' ./bench | tee "$current"

exec "$repo_root/bench/bench-compare.sh" "$baseline" "$current" "$csv_report"
