#!/usr/bin/env bash
# bench/bench-check.sh — regression gate for the bench/ suite.
#
# Compares bench/local-baseline.txt (captured by `make bench-baseline`)
# against a FRESH benchmark run and fails (exit 1) if any scenario's time
# regressed by more than REGRESSION_THRESHOLD_PCT (default 15) percent.
#
# Baselines are per-machine (see .gitignore — bench/local-baseline.txt is
# never committed): never compare a baseline captured on one machine
# against a run on another; hardware/load noise dwarfs real regressions.
#
# Why raw CSV instead of benchstat's own "+N.NN% / ~" delta column: with
# -count=1 (this suite's default, for a fast local/CI signal) benchstat
# cannot compute a p-value, so its human-readable delta column ALWAYS
# prints "~" ("not significant") regardless of the actual magnitude of
# change — a real 100x regression would be silently masked. `benchstat
# -format csv` instead prints the raw per-file sec/op values unconditionally;
# this script computes the percentage itself from those two raw numbers,
# so it works correctly at any sample count, including n=1.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
baseline="$repo_root/bench/local-baseline.txt"
current="${BENCH_CURRENT:-$repo_root/bench/local-current.txt}"
csv_report="${BENCH_REPORT_CSV:-$repo_root/bench/local-benchstat.csv}"
threshold="${REGRESSION_THRESHOLD_PCT:-15}"

if [[ ! -f "$baseline" ]]; then
  echo "bench-check: $baseline not found — run 'make bench-baseline' first" >&2
  exit 1
fi

if ! command -v benchstat >/dev/null 2>&1; then
  echo "bench-check: benchstat not found on PATH, installing golang.org/x/perf/cmd/benchstat@latest" >&2
  go install golang.org/x/perf/cmd/benchstat@latest
fi

benchstat_bin="$(command -v benchstat || true)"
if [[ -z "$benchstat_bin" ]]; then
  gopath_bin="$(go env GOPATH)/bin"
  benchstat_bin="$gopath_bin/benchstat"
fi
if [[ ! -x "$benchstat_bin" ]]; then
  echo "bench-check: benchstat install did not produce an executable at $benchstat_bin (check PATH / GOPATH/bin)" >&2
  exit 1
fi

echo "bench-check: running a fresh benchmark pass -> $current" >&2
go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$' ./bench | tee "$current"

echo "bench-check: comparing $baseline (old) vs $current (new)" >&2
# benchstat's text-format summary is still printed for humans (stdout is
# usually a CI step log or a terminal); the CSV is the machine-readable
# artifact the threshold check below actually parses.
"$benchstat_bin" "$baseline" "$current" || true
"$benchstat_bin" -format csv "$baseline" "$current" 2>/dev/null > "$csv_report"

# Scan only the sec/op table (the first metric block in benchstat's CSV
# output; a later B/op or allocs/op header line ends it) and flag any row
# — including the trailing "geomean" aggregate row — whose current sec/op
# exceeds its baseline sec/op by more than $threshold percent. A benchmark
# name column is always non-empty for a data row; the per-table file-name
# line and the metric header line both have an empty first column, so
# `$1 != ""` alone excludes them without needing to track line order.
awk -v thr="$threshold" '
  BEGIN { FS = "," }
  /^,sec\/op,/ { in_block = 1; next }
  /^,B\/op,/ || /^,allocs\/op,/ { in_block = 0; next }
  in_block && $1 != "" {
    name = $1
    base = $2 + 0
    cur  = $4 + 0
    if (base > 0) {
      pct = (cur - base) / base * 100
      if (pct > thr) {
        printf "bench-check: REGRESSION %s: baseline=%ss current=%ss (+%.2f%% > %s%%)\n", name, $2, $4, pct, thr
        bad = 1
      }
    }
  }
  END { if (bad) { exit 1 } }
' "$csv_report"

echo "bench-check: no scenario regressed time by more than ${threshold}% — ok"
