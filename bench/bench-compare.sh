#!/usr/bin/env bash
# bench/bench-compare.sh — the ONE benchmark-regression comparator.
#
# Usage: bench-compare.sh <old.txt> <new.txt> [csv-report-path]
#
# Compares two `go test -bench` output files with benchstat and fails
# (exit 1) if any scenario's time regressed by more than
# REGRESSION_THRESHOLD_PCT (default 15) percent. Both the local gate
# (bench-check.sh: per-machine baseline vs fresh run) and the CI gate
# (.github/workflows/bench.yml: merge-base vs HEAD on the SAME runner)
# delegate here so the threshold logic exists exactly once.
#
# Why raw CSV instead of benchstat's own "+N.NN% / ~" delta column: with
# -count=1 benchstat cannot compute a p-value, so its human-readable delta
# column ALWAYS prints "~" ("not significant") regardless of the actual
# magnitude of change — a real 100x regression would be silently masked.
# `benchstat -format csv` instead prints the raw per-file values
# unconditionally (medians when -count > 1); this script computes the
# percentage itself from those two numbers, so it works at any sample
# count, including n=1.
set -euo pipefail

old="${1:?usage: bench-compare.sh <old.txt> <new.txt> [csv-report]}"
new="${2:?usage: bench-compare.sh <old.txt> <new.txt> [csv-report]}"
csv_report="${3:-$(mktemp)}"
threshold="${REGRESSION_THRESHOLD_PCT:-15}"

if ! command -v benchstat >/dev/null 2>&1; then
  echo "bench-compare: benchstat not found on PATH, installing golang.org/x/perf/cmd/benchstat@latest" >&2
  go install golang.org/x/perf/cmd/benchstat@latest
fi
benchstat_bin="$(command -v benchstat || true)"
if [[ -z "$benchstat_bin" ]]; then
  benchstat_bin="$(go env GOPATH)/bin/benchstat"
fi
if [[ ! -x "$benchstat_bin" ]]; then
  echo "bench-compare: benchstat install did not produce an executable at $benchstat_bin (check PATH / GOPATH/bin)" >&2
  exit 1
fi

echo "bench-compare: comparing $old (old) vs $new (new), threshold ${threshold}%" >&2
# benchstat's text-format summary is still printed for humans; the CSV is the
# machine-readable artifact the threshold check below actually parses.
"$benchstat_bin" "$old" "$new" || true
"$benchstat_bin" -format csv "$old" "$new" 2>/dev/null > "$csv_report"

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
        printf "bench-compare: REGRESSION %s: baseline=%ss current=%ss (+%.2f%% > %s%%)\n", name, $2, $4, pct, thr
        bad = 1
      }
    }
  }
  END { if (bad) { exit 1 } }
' "$csv_report"

echo "bench-compare: no scenario regressed time by more than ${threshold}% — ok"
