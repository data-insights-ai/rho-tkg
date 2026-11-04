#!/usr/bin/env bash
# measure.sh: the measurement level (René 4 Nov). Runs every test behind the
# `measure` build tag, one test per go test call so no other test shares its
# process, and lists which passed. Run by hand on a quiet machine; never part
# of a gate.
set -u
cd "$(dirname "$0")/.."
fail=0
for f in $(grep -rl --include='*_test.go' '^//go:build measure' pkg); do
  dir=./$(dirname "$f")
  for t in $(grep -oE '^func (Test[A-Za-z0-9_]+)' "$f" | cut -c6-); do
    if go test -tags measure -count=1 -run "^${t}\$" "$dir" >/dev/null 2>&1; then
      echo "ok   $dir $t"
    else
      echo "FAIL $dir $t"; fail=1
    fi
  done
done
exit $fail
