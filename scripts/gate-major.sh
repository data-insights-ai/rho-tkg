#!/usr/bin/env bash
# The major gate (go run ./scripts/release major runs it), comprehensive and cold: make build and lint,
# then -race -count=1 with the coverage floor, gosec, the fuzz harnesses and govulncheck.
# Every step runs; a red run lists every red step.
set -uo pipefail
cd "$(dirname "$0")/.."
export GOFLAGS=-trimpath
COVER_MIN=${COVER_MIN:-80}
GOSEC_VERSION=${GOSEC_VERSION:-v2.29.0}
GOVULNCHECK_VERSION=${GOVULNCHECK_VERSION:-v1.7.0}
docker_go() {
  docker run --rm -v "$PWD":/src -w /src -v rho-tkg-gocache:/go -v rho-tkg-buildcache:/root/.cache/go-build \
    "golang:$(awk '/^go /{print $2}' go.mod)" sh -c "$1"
}
red=()
step() { echo "== $1"; "${@:2}" || red+=("$1"); }
cover() {
  flock "${RACE_LOCK:-$HOME/.cache/race-gate.lock}" go test -race -count=1 -shuffle=on -timeout 90m \
    -coverpkg=./pkg/... -coverprofile=coverage.out ./... &&
  go tool cover -func=coverage.out | awk -v min="$COVER_MIN" \
    '/^total:/ { sub("%","",$3); print "coverage " $3 "% (floor " min "%)"; exit $3+0 < min }'
}
step build make build
step lint make lint
step test-major cover
step gosec docker_go "go install github.com/securego/gosec/v2/cmd/gosec@$GOSEC_VERSION && gosec -quiet ./..."
step fuzz scripts/fuzz.sh
step vuln docker_go "go install golang.org/x/vuln/cmd/govulncheck@$GOVULNCHECK_VERSION && govulncheck ./..."
(( ${#red[@]} == 0 )) || { echo "gate-major red: ${red[*]}"; exit 1; }
