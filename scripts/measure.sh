#!/usr/bin/env bash
# The measurement level (//go:build measure): heap and allocation bounds, on a quiet host, by hand.
# Each file's tests run in their own process, so one file's heap does not move another's bound.
set -euo pipefail
for f in $(grep -rl '^//go:build measure' --include='*_test.go' pkg); do
  tests=$(grep -oE '^func Test\w+' "$f" | cut -c6- | paste -sd'|')
  go test -tags measure -count=1 -v -run "^($tests)\$" "./${f%/*}"
done
