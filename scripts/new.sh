#!/usr/bin/env bash
# make new PKG=./pkg/... RUN='TestA|TestB': the new code's few tests, without -short, on Go's test cache.
# Fails when RUN matches no test in a package or a test skips. Full log: .git/make-new.out
set -uo pipefail
pkg=${PKG:-} run=${RUN:-}
if [[ -z $pkg || -z $run ]]; then echo "usage: make new PKG=./pkg/... RUN='TestA|TestB'"; exit 2; fi
out=.git/make-new.out
# shellcheck disable=SC2086 # PKG may name several packages
go test -timeout 10m -v -run "$run" $pkg >"$out" 2>&1; rc=$?
grep -E '^(--- |ok|FAIL|panic:)|_test\.go:[0-9]+:' "$out"
(( rc == 0 )) || exit $rc
! grep -q 'no tests to run' "$out" || { echo "make new: RUN='$run' matches no test in a package"; exit 1; }
! grep -q '^--- SKIP' "$out" || { echo "make new: a test skipped (above)"; exit 1; }
