# bench-gate.awk — the time-regression check of bench-compare.sh over a
# benchstat -format csv report (see bench-compare.sh). Usage:
#   awk -v thr=<percent> -f bench-gate.awk report.csv
# Exit 1 when a sec/op row regressed by more than thr percent. Run it with
# LC_ALL=C: under a comma-decimal locale awk reads 1.429e-05 as 1.
  BEGIN { FS = "," }
  # The sec/op block ends at the next header line (every header starts with
  # ","): the file-name line of the next block, whatever unit it holds —
  # a custom b.ReportMetric unit such as build-ms as well as B/op.
  /^,sec\/op,/ { in_block = 1; next }
  /^,/ { in_block = 0; next }
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
