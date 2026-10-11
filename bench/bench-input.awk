# Validate the complete Go benchmark sample sets before comparing metrics.
FNR == 1 { side++ }
/^[[:space:]]*--- FAIL:|^FAIL([[:space:]]|$)|^panic:|^fatal error:/ {
  print "bench-compare: failed benchmark input: " FILENAME > "/dev/stderr"
  bad = 1
}
/^Benchmark[^[:space:]]+[[:space:]]+[0-9]+[[:space:]]/ {
  if ($2 + 0 < 1) bad = 1
  ns = bytes = allocs = 0
  for (i = 4; i <= NF; i += 2) {
    if ($(i-1) !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/) {
      print "bench-compare: invalid metric: " $1 > "/dev/stderr"
      bad = 1
    }
    if ($i == "ns/op") ns = 1
    if ($i == "B/op") bytes = 1
    if ($i == "allocs/op") allocs = 1
  }
  if (!ns || !bytes || !allocs) {
    print "bench-compare: incomplete metrics: " $1 > "/dev/stderr"
    bad = 1
  }
  if (side == 1) old[$1]++
  else if (side == 2) new[$1]++
  else bad = 1
  rows[side]++
}
END {
  if (side != 2 || rows[1] == 0 || rows[2] == 0) {
    print "bench-compare: both inputs need nonempty benchmark results" > "/dev/stderr"
    bad = 1
  }
  for (name in old) if (old[name] != new[name]) {
    print "bench-compare: missing or unequal samples: " name > "/dev/stderr"
    bad = 1
  }
  for (name in new) if (!(name in old)) {
    print "bench-compare: unmatched benchmark: " name > "/dev/stderr"
    bad = 1
  }
  if (bad) exit 1
}
