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

# bench-gate.awk (see its header): a TIME gate on the sec/op table (the first
# metric block in benchstat's CSV output; the next block's header line ends it,
# whatever its unit — B/op, allocs/op or a custom b.ReportMetric unit such as
# build-ms) and an ALLOCS gate (ALLOCS_THRESHOLD_PCT, default 10) for one
# benchmark family (ALLOCS_GATE_FAMILY, default PinnedRelPropertyLookup) whose
# rows outside the TIME_CANARY regex are gated on allocs/op only. A time-gated
# row fails when its current sec/op exceeds the baseline by more than
# $threshold percent; so does the geomean of the time-gated rows.
LC_ALL=C awk -v thr="$threshold" -v allocs_thr="${ALLOCS_THRESHOLD_PCT:-10}" \
  -v family="${ALLOCS_GATE_FAMILY:-}" -v canary="${TIME_CANARY:-}" \
  -f "$(dirname "$0")/bench-gate.awk" "$csv_report"

echo "bench-compare: no time-gated scenario regressed by more than ${threshold}% and no allocs-gated one by more than ${ALLOCS_THRESHOLD_PCT:-10}% allocs/op — ok"
