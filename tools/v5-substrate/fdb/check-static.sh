#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
repo=$(cd ../../.. && pwd)
tag="rho-v5-fdb-static-$(date +%s)-$$"
out="$PWD/artifacts/$tag"
mkdir -p "$out"
containers=()
cleanup() {
  for c in ${containers[*]-}; do docker rm -f "$c" >/dev/null 2>&1 || true; done
  docker image rm "$tag" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker build -t "$tag" . >"$out/build.log" 2>&1
shasum -a 256 *.go go.mod go.sum Dockerfile "$repo/.golangci.yml" >"$out/source-sha256.txt"
for tool in lint security vulncheck; do
  name="$tag-$tool"; containers+=("$name")
  docker run --name "$name" --memory 4g \
    --mount "type=volume,source=rho-v5-fdb-$tool-go,target=/go" \
    --mount "type=volume,source=rho-v5-fdb-$tool-cache,target=/root/.cache/go-build" \
    --mount "type=bind,source=$PWD,target=/harness" \
    --mount "type=bind,source=$repo/.golangci.yml,target=/tmp/rho-golangci.yml,readonly" \
    "$tag" timeout --signal=KILL 600s /bin/bash -c '
      case "$1" in
        lint) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 && golangci-lint run --config /tmp/rho-golangci.yml ./...;;
        security) go install github.com/securego/gosec/v2/cmd/gosec@v2.29.0 && gosec -quiet ./...;;
        vulncheck) go install golang.org/x/vuln/cmd/govulncheck@v1.7.0 && govulncheck ./...;;
      esac' -- "$tool" >"$out/$tool.txt" 2>&1
done
printf '%s\n' "$out"
