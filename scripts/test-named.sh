#!/usr/bin/env bash
# Runs named tests and fails when one did not run: go test exits 0 on a -run regex matching nothing.
# A test skipped under -short is reported as skipped: it belongs to the long tier (the major gate).
# No -count=1: Go's test cache answers an unchanged package.
# Usage: test-named.sh <pkg> <name|name...> -- [go test flags]
set -u
pkg=$1
names=$2
shift 3
out=$(go test -v -run "^(${names})\$" "$@" "$pkg" 2>&1)
status=$?
if [ "$status" -ne 0 ]; then
  printf '%s\n' "$out"
  exit "$status"
fi
skipped=()
for name in ${names//|/ }; do
  if printf '%s\n' "$out" | grep -Eq -- "--- SKIP: ${name}( |\$)"; then
    skipped+=("$name")
  elif ! printf '%s\n' "$out" | grep -Eq -- "--- PASS: ${name}( |\$)"; then
    echo "test-named: $pkg: $name did not run"
    exit 1
  fi
done
if [ "${#skipped[@]}" -gt 0 ]; then
  echo "test-named: $pkg: skipped under -short (long tier): ${skipped[*]}"
fi
echo "ok $pkg $names"
