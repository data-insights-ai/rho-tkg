#!/usr/bin/env bash
# This machine's baseline for bench/bench-check.sh (bench/local-baseline.txt, gitignored).
set -euo pipefail
cd "$(dirname "$0")/.."
go test -bench=. -benchmem -benchtime=0.3s -count=1 -run '^$' ./bench | tee bench/local-baseline.txt
