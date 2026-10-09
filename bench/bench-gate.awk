# bench-gate.awk — the regression check of bench-compare.sh over a benchstat
# -format csv report (see bench-compare.sh). Usage:
#   awk -v thr=<percent> [-v allocs_thr=<percent>] [-v family=<regex>]
#       [-v canary=<regex>] -f bench-gate.awk report.csv
# Run it with LC_ALL=C: under a comma-decimal locale awk reads 1.429e-05 as 1.
# Exit 1 when a gated row regressed.
#
# Two gates:
#  - TIME: the sec/op block (the first block; it ends at the next header line,
#    whatever its unit — B/op, allocs/op or a custom b.ReportMetric unit such
#    as build-ms). A row fails when its current sec/op exceeds the baseline by
#    more than thr percent. Every row is time-gated EXCEPT rows of the
#    allocs-gated family that do not match the canary regex. The aggregate
#    "geomean" row is recomputed over the time-gated rows only (benchstat's own
#    geomean also covers the excluded rows).
#  - ALLOCS: rows of the family (regex on the benchstat row name) fail when
#    their current allocs/op exceed the baseline by more than allocs_thr
#    percent.
# Defaults: family = the PinnedRelPropertyLookup benchmarks (backlog 8), whose
# time rows swing tens of percent between identical runs on a loaded runner
# while allocs/op does not move, and whose real regression (the lookup falling
# back to the history fold) multiplies allocs; canary = their 8 time-gated rows
# (memory and badger x {1type/sigma, 5types/unrelated-x10} x matches200);
# allocs_thr = 10. family=none disables the allocs gate and time-gates every
# row.
BEGIN {
  FS = ","
  if (family == "") family = "^PinnedRelPropertyLookup/"
  if (canary == "") canary = "^PinnedRelPropertyLookup/(memory|badger)/[0-9]+/(1type/sigma|5types/unrelated-x10)/matches200/"
  if (allocs_thr == "") allocs_thr = 10
  in_family_mode = (family != "none")
  logsum = 0; n = 0
}
/^,sec\/op,/ { block = "time"; next }
/^,allocs\/op,/ { block = "allocs"; next }
/^,/ { block = ""; next }
block == "time" && $1 != "" && $1 != "geomean" {
  name = $1
  if (in_family_mode && name ~ family && name !~ canary) next
  base = $2 + 0
  cur  = $4 + 0
  if (base > 0 && cur > 0) {
    logsum += log(cur / base); n++
    pct = (cur - base) / base * 100
    if (pct > thr) {
      printf "bench-compare: REGRESSION %s: baseline=%ss current=%ss (+%.2f%% > %s%%)\n", name, $2, $4, pct, thr
      bad = 1
    }
  }
  next
}
block == "allocs" && in_family_mode && $1 != "" && $1 != "geomean" && $1 ~ family {
  base = $2 + 0
  cur  = $4 + 0
  if (base > 0) {
    pct = (cur - base) / base * 100
    if (pct > allocs_thr) {
      printf "bench-compare: ALLOCS REGRESSION %s: baseline=%s current=%s allocs/op (+%.2f%% > %s%%)\n", $1, $2, $4, pct, allocs_thr
      bad = 1
    }
  }
}
END {
  if (n > 0) {
    pct = (exp(logsum / n) - 1) * 100
    if (pct > thr) {
      printf "bench-compare: REGRESSION geomean (time-gated rows): +%.2f%% > %s%%\n", pct, thr
      bad = 1
    }
  }
  if (bad) { exit 1 }
}
