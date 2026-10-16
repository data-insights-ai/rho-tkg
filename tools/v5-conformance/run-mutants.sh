#!/usr/bin/env bash
# Runs the suite once per mutant (internal/mutant) and requires every run to
# fail. Prints the failing subtests per mutant. Exit 1 if any mutant survives.
set -u
export GOFLAGS=-mod=mod GOPROXY=off GOWORK=off
cd "$(dirname "$0")"
names=$(sed -n 's/^\t"\([a-z-]*\)", *\/\/.*/\1/p' internal/mutant/mutant.go)
survivors=0
for m in $names; do
  out=$(CONFORMANCE_MUTANT=$m go test -count=1 -run TestV4 . 2>&1)
  if [ $? -eq 0 ]; then
    echo "SURVIVED  $m"
    survivors=$((survivors+1))
  else
    fails=$(printf '%s\n' "$out" | sed -n 's/^ *--- FAIL: TestV4\/\([a-z]*\/[A-Za-z]*\/[A-Za-z]*\) .*/\1/p' | tr '\n' ' ')
    echo "killed    $m  <- $fails"
  fi
done
echo "survivors: $survivors"
[ "$survivors" -eq 0 ]
