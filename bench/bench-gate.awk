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
#    more than thr percent. Rows of the allocs-gated family are NOT time-gated
#    unless they match the opt-in canary. The aggregate "geomean" row is
#    recomputed over the time-gated rows only (benchstat's own geomean also
#    covers the family's rows).
#  - ALLOCS: rows of the family (regex on the benchstat row name) fail when
#    their current allocs/op exceed the baseline by more than allocs_thr
#    percent, or, on a 0 allocs/op baseline (no percent exists), as soon as
#    they allocate.
# Defaults: family = the PinnedRelPropertyLookup benchmarks (backlog 8):
# allocs-gated; their time rows are reported, not gated — on a shared host
# identical code swung +42..+178 % in time while allocs/op did not move, and
# the regression they exist to catch (the lookup falling back to the history
# fold) multiplies allocs (1,423 -> 106,555). allocs_thr = 10. canary is empty
# (no family row time-gated); canary=canary opts in the documented 8-row time
# canary below (memory and badger x {1type/sigma, 5types/unrelated-x10} x
# matches200) for quiet runners, any other value is used as the regex.
# The LatestStamps benchmarks (backlog 30) belong to the family too: their
# door is 0 allocs/op, and a door falling back to the History fold allocates
# per history row (230,388 allocs/op at 10,000 versions).
# family=none disables the allocs gate and time-gates every row.
BEGIN {
  FS = ","
  documented_canary = "^PinnedRelPropertyLookup/(memory|badger)/[0-9]+/(1type/sigma|5types/unrelated-x10)/matches200/"
  if (family == "") family = "^(PinnedRelPropertyLookup|LatestStamps)/"
  if (canary == "canary") canary = documented_canary
  if (allocs_thr == "") allocs_thr = 10
  in_family_mode = (family != "none")
  logsum = 0; n = 0
}
/^,sec\/op,/ { block = "time"; next }
/^,allocs\/op,/ { block = "allocs"; next }
/^,/ { block = ""; next }
block == "time" && $1 != "" && $1 != "geomean" {
  name = $1
  if (in_family_mode && name ~ family && (canary == "" || name !~ canary)) next
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
  } else if ($2 != "" && cur > 0) {
    printf "bench-compare: ALLOCS REGRESSION %s: baseline=0 current=%s allocs/op (a zero-alloc row allocates)\n", $1, $4
    bad = 1
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
