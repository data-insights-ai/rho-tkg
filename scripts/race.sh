#!/usr/bin/env bash
# -race over the short tier, under the host-wide race lock (one -race suite at a time, shared with sigma).
# The release starts it detached after a patch push (log .git/race.out); cron runs it every 6 h.
set -euo pipefail
cd "$(dirname "$0")/.."
export GOFLAGS=-trimpath
flock "${RACE_LOCK:-$HOME/.cache/race-gate.lock}" go test -race -short -timeout 30m ./...
