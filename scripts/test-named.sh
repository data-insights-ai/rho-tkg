#!/usr/bin/env bash
# Runs named tests and fails when one did not run: go test exits 0 on a -run regex matching nothing.
# A test skipped under -short reruns without -short and -race, so a gate never counts its skip as a proof.
# Usage: test-named.sh <pkg> <name|name...> -- [go test flags]
set -u
pkg=$1
names=$2
shift 3
out=$(go test -v -count=1 -run "^(${names})\$" "$@" "$pkg" 2>&1)
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
  rest=()
  short=0
  for flag in "$@"; do
    case "$flag" in
      -short) short=1 ;;
      -race) ;;
      *) rest+=("$flag") ;;
    esac
  done
  if [ "$short" -eq 1 ]; then
    list=$(IFS='|'; printf '%s' "${skipped[*]}")
    echo "test-named: $pkg: rerun without -short: $list"
    exec "$0" "$pkg" "$list" -- ${rest[@]+"${rest[@]}"}
  fi
  echo "test-named: $pkg: skipped: ${skipped[*]}"
fi
echo "ok $pkg $names"
