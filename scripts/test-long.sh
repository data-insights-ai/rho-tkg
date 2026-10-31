#!/usr/bin/env bash
# The release gate's long tier (GATETIER): every test that skips under -short with a plain `if testing.Short()`
# (no race exception), so it runs in no other gate, runs here without -short and without race, at the size its
# bounds were measured at. The list is read from the code on every run: a new plain skip joins it by itself.
# LONG_TIER=list prints "package<TAB>test" per test instead of running them.
set -u
cd "$(dirname "$0")/.." || exit 2
list=$(find pkg -name '*_test.go' -print0 | xargs -0 awk '
  FNR == 1 { fn = "" }
  /^func / { fn = $2; sub(/\(.*/, "", fn) }
  pending { if (fn ~ /^Test/ && ($0 ~ /Skip/ || $0 ~ /^[ \t]*return$/)) { d = FILENAME; sub(/\/[^\/]*$/, "", d); print d "\t" fn } pending = 0 }
  /^[ \t]*if testing\.Short\(\) \{[ \t]*$/ { pending = 1 }
' | sort -u)
if [ -z "$list" ]; then
  echo "test-long: no test skips under a plain testing.Short()"
  exit 0
fi
if [ "${LONG_TIER:-}" = "list" ]; then
  printf '%s\n' "$list"
  exit 0
fi
fail=0
for pkg in $(printf '%s\n' "$list" | cut -f1 | sort -u); do
  re=$(printf '%s\n' "$list" | awk -F'\t' -v p="$pkg" '$1 == p { print $2 }' | paste -sd'|')
  echo "test-long: ./$pkg: $re"
  scripts/test-named.sh "./$pkg/" "$re" -- -timeout 30m || fail=1
done
exit $fail
