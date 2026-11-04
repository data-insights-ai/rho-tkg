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
    if out=$(go test -tags measure -count=1 -v -run "^${t}\$" "$dir" 2>&1); then
      echo "ok   $dir $t"; echo "$out" | grep -E "_test.go:[0-9]+:" | sed "s/^/       /"
    else
      echo "FAIL $dir $t"; echo "$out" | grep -E "_test.go:[0-9]+:|FAIL|panic" | sed "s/^/       /"; fail=1
    fi
  done
done
exit $fail
