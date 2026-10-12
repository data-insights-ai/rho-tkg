#!/bin/bash
# shellcheck disable=SC2016
# Container command variables deliberately expand inside their owning container.
set -euo pipefail
exec 3>&1 # Preserve the original output channel through redirected failures.
cd "$(dirname "$0")"
mode=${1:-test}
profile=${2:-small}
case "$mode" in smoke|test|graph-fault|lifecycle-trace|static) ;; *) echo 'usage: run-local.sh [smoke|test|graph-fault|lifecycle-trace|static] [small|large]' >&2; exit 2;; esac
runtime='foundationdb/foundationdb@sha256:c61744624e479d370a0120176e518bb569a2a18ee9abceaf7022c4367b54f1db'
tag="rho-v5-fdb-$(date +%s)-$$"
out="$PWD/artifacts/$tag"
mkdir -p "$out"
settings=$(python3 -B launcher.py profile "$profile" "${FDB_ADMISSION_MIB:-4096}" "${FDB_ADMISSION_CPUS:-2}" "$out/profile.json")
read -r server_mib resident_mib cache_mib client_mib <<<"$settings"
containers=()
volumes=()
images=()
phase_deadline=$((SECONDS+600))
bounded_docker() {
  local limit="$1"; shift
  local remaining=$((phase_deadline-SECONDS))
  if [ "$remaining" -le 0 ]; then echo 'phase deadline expired' >&2; return 124; fi
  if [ "$limit" -gt "$remaining" ]; then limit="$remaining"; fi
  python3 -B launcher.py bounded "$limit" "$out/commands.jsonl" docker "$@"
}
docker() { bounded_docker 45 "$@"; }
cleanup() {
  local result=$? cleanup_result=0
  trap - EXIT INT TERM
  set +e
  phase_deadline=$((SECONDS+180))
  for c in ${containers[@]+"${containers[@]}"}; do
    docker logs "$c" >"$out/$c.log" 2>&1
    docker inspect "$c" >"$out/$c-final-inspect.json" 2>&1
    docker rm -f "$c" >>"$out/cleanup.txt" 2>&1 || cleanup_result=1
  done
  for v in ${volumes[@]+"${volumes[@]}"}; do docker volume rm "$v" >>"$out/cleanup.txt" 2>&1 || cleanup_result=1; done
  if [ "${network_created:-0}" = 1 ]; then docker network rm "$tag" >>"$out/cleanup.txt" 2>&1 || cleanup_result=1; fi
  for i in ${images[@]+"${images[@]}"}; do docker image rm "$i" >>"$out/cleanup.txt" 2>&1 || cleanup_result=1; done
  python3 -B launcher.py receipt "$out" "$result" "$cleanup_result" "$mode" || cleanup_result=1
  if [ "$result" = 0 ] && [ "$cleanup_result" != 0 ]; then result=1; fi
  printf '%s\n' "$out" >&3
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
capture_status() {
  local container="$1" destination="$2"
  for attempt in 1 2 3 4 5 6; do
    if docker exec "$container" fdbcli --timeout 5 --exec 'status json' >"$destination.attempt$attempt" 2>"$destination.attempt$attempt.stderr"; then
      if python3 -c 'import json,sys;json.load(open(sys.argv[1]))' "$destination.attempt$attempt"; then cp "$destination.attempt$attempt" "$destination"; return 0; fi
    fi
    sleep 1
  done
  return 1
}
wait_healthy() {
  local container="$1" destination="$2" ready=0
  for attempt in {1..90}; do
    if docker exec "$container" fdbcli --timeout 3 --exec 'status json' >"$destination.attempt$attempt" 2>"$destination.attempt$attempt.stderr" &&
      python3 -B launcher.py healthy "$destination.attempt$attempt" 2>"$destination.validation$attempt"; then
      cp "$destination.attempt$attempt" "$destination"; ready=1; break
    fi
    sleep 1
  done
  if [ "$ready" != 1 ]; then echo 'healthy fully replicated triple cluster not established' >&2; return 1; fi
}
inspect_resources() {
  local name="$1" role="$2" destination="$3"
  if ! docker inspect "$name" >"$destination"; then return 1; fi
  python3 -B launcher.py resources "$out/profile.json" "$destination" "$role"
}
capture_client() {
  local name="$1" prefix="$2"
  # Stopped container cgroups are gone; each ordinary client captures before exit.
  inspect_resources "$name" client "$out/$prefix-inspect.json"
}
stop_client() {
  local name="$1" prefix="$2"
  # Killing a timed-out Docker CLI does not terminate its container. An
  # uncertain stop is terminal for this launcher: never admit another client.
  if ! docker inspect "$name" >"$out/$prefix-stop-inspect.json"; then
    echo 'client terminal state unavailable; refusing another client' >&2; exit 1
  fi
  if ! python3 -B launcher.py stopped "$out/$prefix-stop-inspect.json"; then
    docker exec "$name" cat /sys/fs/cgroup/memory.peak >"$out/$prefix-forced-memory-peak.txt" || true
    if ! docker kill --signal KILL "$name" >"$out/$prefix-forced-kill.txt" ||
      ! docker inspect "$name" >"$out/$prefix-stop-inspect.json" ||
      ! python3 -B launcher.py stopped "$out/$prefix-stop-inspect.json"; then
      echo 'client did not stop; refusing another client' >&2; exit 1
    fi
  fi
}
source_image="$tag-source"; client_image="$tag-client-image"
images+=("$source_image")
shasum -a 256 ./*.go ./*.sh ./*.py go.mod go.sum Dockerfile .dockerignore >"$out/source-sha256.txt"
git rev-parse HEAD >"$out/source-head.txt"
git status --porcelain -- . >"$out/source-status.txt"
uname -a >"$out/host.txt"
docker version >"$out/docker-version.txt"
docker info --format '{{json .}}' >"$out/docker-info.json"
bounded_docker 180 pull "$runtime" >"$out/pull-runtime.log" 2>&1
# This target executes no RUN instruction. Compilation/header setup occur in
# one 1 GiB/2 CPU container, before any live cluster process exists.
bounded_docker 300 build --target source -t "$source_image" . >"$out/build-source.log" 2>&1
build_name="$tag-prebuild"; containers+=("$build_name")
bounded_docker 600 run --name "$build_name" --memory 1024m --cpus 2 \
  --mount "type=bind,source=$out,target=/evidence" "$source_image" /bin/bash -c '
    timeout --signal=KILL 180s bash prepare-client.sh && timeout --signal=KILL 180s go mod download && timeout --signal=KILL 180s go build -o /usr/local/bin/fdb-spike .
    result=$?
    cat /sys/fs/cgroup/memory.peak > /evidence/build-memory-peak.txt
    cat /sys/fs/cgroup/memory.stat > /evidence/build-memory-stat.txt
    go version > /evidence/build-go-version.txt
    exit "$result"' >"$out/prebuild.log" 2>&1
inspect_resources "$build_name" build "$out/prebuild-inspect.json"
images+=("$client_image")
docker commit "$build_name" "$client_image" >"$out/client-image-id.txt"
docker image inspect "$client_image" "$runtime" >"$out/images.json"
if [ "$mode" = static ]; then
  phase_deadline=$((SECONDS+1200))
  for tool in lint security vulncheck; do
    name="$tag-$tool"; containers+=("$name")
    bounded_docker 400 run --name "$name" --memory 1024m --cpus 2 \
      --mount "type=bind,source=$out,target=/evidence" \
      --mount "type=volume,source=rho-v5-fdb-$tool-go,target=/go" \
      --mount "type=volume,source=rho-v5-fdb-$tool-cache,target=/root/.cache/go-build" \
      --mount "type=bind,source=$(cd ../../.. && pwd)/.golangci.yml,target=/tmp/rho-golangci.yml,readonly" \
      "$client_image" /bin/bash -c '
        case "$1" in
          lint) timeout --signal=KILL 180s go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2 && timeout --signal=KILL 180s golangci-lint run --config /tmp/rho-golangci.yml ./...;;
          security) timeout --signal=KILL 180s go install github.com/securego/gosec/v2/cmd/gosec@v2.29.0 && timeout --signal=KILL 180s gosec -quiet ./...;;
          vulncheck) timeout --signal=KILL 180s go install golang.org/x/vuln/cmd/govulncheck@v1.7.0 && timeout --signal=KILL 180s govulncheck ./...;;
        esac
        result=$?; cat /sys/fs/cgroup/memory.peak > "/evidence/$1-memory-peak.txt"
        du -s -B1 /go /root/.cache/go-build > "/evidence/$1-cache-bytes.txt"
        cat /sys/fs/cgroup/memory.stat > "/evidence/$1-memory-stat.txt"; exit "$result"' -- "$tool" >"$out/$tool.txt" 2>&1
    inspect_resources "$name" build "$out/$tool-inspect.json"
  done
  exit 0
fi
phase_deadline=$((SECONDS+900))
docker network create --internal "$tag" >"$out/network-id.txt"
network_created=1
docker network inspect "$tag" >"$out/network.json"
subnet=$(docker network inspect --format '{{(index .IPAM.Config 0).Subnet}}' "$tag")
ips=()
for i in 0 1 2 3; do ips+=("$(python3 -c 'import ipaddress,sys;print(ipaddress.ip_network(sys.argv[1])[10+int(sys.argv[2])])' "$subnet" "$i")"); done
connection="rho_spike:rhov5$(date +%s)$$@${ips[0]}:4500,${ips[1]}:4500,${ips[2]}:4500"
printf '%s\n' "$connection" >"$out/fdb.cluster"
for i in 0 1 2 3; do
  c="$tag-zone$i"; v="$tag-data$i"
  volumes+=("$v"); containers+=("$c")
  docker volume create "$v" >/dev/null
  docker run -d --name "$c" --hostname "zone$i" --network "$tag" --ip "${ips[$i]}" \
    --memory "${server_mib}m" --cpus 0.375 --mount "type=volume,source=$v,target=/var/fdb/data" \
    --env "FDB_CLUSTER_FILE_CONTENTS=$connection" --env "FDB_RESIDENT_MIB=$resident_mib" --env "FDB_CACHE_MIB=$cache_mib" \
    --entrypoint /bin/bash "$runtime" -c 'printf "%s\n" "$FDB_CLUSTER_FILE_CONTENTS" > /var/fdb/fdb.cluster; exec fdbserver -C /var/fdb/fdb.cluster -p auto:4500 -l 0.0.0.0:4500 --datadir /var/fdb/data --logdir /var/fdb/logs --locality-zoneid="$HOSTNAME" --locality-machineid="$HOSTNAME" --memory "${FDB_RESIDENT_MIB}MiB" --storage-memory "${FDB_CACHE_MIB}MiB" --cache-memory "${FDB_CACHE_MIB}MiB" --maxlogssize 20MiB' >/dev/null
  inspect_resources "$c" server "$out/zone$i-inspect.json"
done
servers=("$tag-zone0" "$tag-zone1" "$tag-zone2" "$tag-zone3")
docker exec "${servers[0]}" fdbcli --timeout 30 --exec 'configure new triple ssd' >"$out/configure.txt" 2>&1
wait_healthy "${servers[0]}" "$out/status.json"
client_index=0
client() {
  client_index=$((client_index+1))
  local command="$1" name="$tag-client$client_index" result=0
  containers+=("$name")
  bounded_docker 55 run --name "$name" --network "$tag" --memory "${client_mib}m" --cpus 0.5 \
    --env "FDB_CONNECTION_STRING=$connection" --mount "type=bind,source=$out,target=/evidence" \
    "$client_image" /bin/bash -c 'timeout --signal=KILL 45s fdb-spike "$1"; result=$?; cat /sys/fs/cgroup/memory.peak > "/evidence/$1-$2-memory-peak.txt"; cat /sys/fs/cgroup/memory.current > "/evidence/$1-$2-memory-current.txt"; cat /sys/fs/cgroup/memory.stat > "/evidence/$1-$2-memory-stat.txt"; exit "$result"' -- "$command" "$client_index" >"$out/$command-$client_index.json" || result=$?
  stop_client "$name" "$command-$client_index"
  if ! capture_client "$name" "$command-$client_index"; then result=1; fi
  return "$result"
}
client smoke
if [ "$mode" = test ] || [ "$mode" = graph-fault ] || [ "$mode" = lifecycle-trace ]; then
  name="$tag-tests"; containers+=("$name")
  bounded_docker 310 run --name "$name" --network "$tag" --memory "${client_mib}m" --cpus 0.5 \
    --env "FDB_CONNECTION_STRING=$connection" --env FDB_ZONE_IPS="${ips[*]}" \
    --mount "type=bind,source=$out,target=/evidence" "$client_image" /bin/bash -c '
      timeout --signal=KILL 120s go vet ./... && timeout --signal=KILL 180s go test -timeout 170s -count=1 -race -coverprofile=/evidence/coverage.out -v ./...
      result=$?; cat /sys/fs/cgroup/memory.peak > /evidence/test-client-memory-peak.txt
      cat /sys/fs/cgroup/memory.stat > /evidence/test-client-memory-stat.txt; exit "$result"' >"$out/tests.txt" 2>&1
  capture_client "$name" tests
fi
if [ "$mode" != smoke ] && [ "$mode" != lifecycle-trace ]; then
  prefix=fault
  if [ "$mode" = graph-fault ]; then prefix=graph-fault; fi
  client "$prefix-seed"
  for phase in before after; do
    name="$tag-unknown-$phase"; containers+=("$name")
    docker run -d --name "$name" --network "$tag" --memory "${client_mib}m" --cpus 0.5 \
      --env "FDB_CONNECTION_STRING=$connection" --mount "type=bind,source=$out,target=/evidence" \
      "$client_image" fdb-spike "$prefix-$phase-child" >/dev/null
    reached=0
    for attempt in {1..30}; do
      docker logs "$name" >"$out/$phase-child-marker.json" 2>&1
      if python3 -B launcher.py marker "$out/$phase-child-marker.json" "$phase" 2>/dev/null; then reached=1; break; fi
      sleep 1
    done
    if [ "$reached" != 1 ]; then echo 'exact child native-commit seam not reached' >&2; exit 1; fi
    docker exec "$name" cat /sys/fs/cgroup/memory.peak >"$out/$phase-child-memory-peak.txt"
    docker exec "$name" cat /sys/fs/cgroup/memory.stat >"$out/$phase-child-memory-stat.txt"
    docker kill --signal KILL "$name" >"$out/$phase-child-kill.txt"
    docker inspect "$name" >"$out/$phase-child-killed.json"
    python3 -B launcher.py killed "$out/$phase-child-killed.json"
    capture_client "$name" "$phase-child"
    client "$prefix-recover-$phase"
  done
  client "$prefix-verify"
  if [ "$mode" = graph-fault ]; then client graph-fault-prepare-progress; fi
  wait_healthy "${servers[0]}" "$out/status-before-zone-loss.json"
  for i in 0 1 2 3; do docker exec "${servers[$i]}" /bin/bash -c 'cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.stat' >"$out/zone$i-pre-fault-memory.txt"; done
  docker kill --signal KILL "${servers[0]}" >"$out/zone-kill.txt"
  docker inspect "${servers[0]}" >"$out/zone-killed.json"
  python3 -B launcher.py killed "$out/zone-killed.json"
  progressed=0
  for attempt in {1..6}; do
    if client "$prefix-progress"; then progressed=1; break; fi
    sleep 2
  done
  if [ "$progressed" != 1 ]; then echo 'one-zone-loss graph/scalar progress not established' >&2; exit 1; fi
  capture_status "${servers[1]}" "$out/status-with-zone-loss.json"
  client "$prefix-verify"
  docker start "${servers[0]}" >"$out/zone-restart.txt"
  wait_healthy "${servers[1]}" "$out/status-before-all-stop.json"
  for i in 0 1 2 3; do docker exec "${servers[$i]}" /bin/bash -c 'cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.stat' >"$out/zone$i-pre-reopen-memory.txt"; done
  for c in "${servers[@]}"; do
    docker kill --signal KILL "$c" >>"$out/all-server-kill.txt"
    docker inspect "$c" >"$out/$c-all-stopped.json"
    python3 -B launcher.py killed "$out/$c-all-stopped.json"
  done
  # Every exact server is stopped before ANY same-volume server starts.
  for c in "${servers[@]}"; do docker start "$c" >>"$out/all-server-restart.txt"; done
  wait_healthy "${servers[1]}" "$out/status-reopened.json"
  client "$prefix-progress"
  client "$prefix-verify"
  if [ "$mode" = graph-fault ]; then client graph-fault-terminal; fi
fi
if [ "$mode" = lifecycle-trace ]; then client lifecycle-trace; fi
wait_healthy "${servers[0]}" "$out/status-after.json"
for i in 0 1 2 3; do
  docker exec "${servers[$i]}" /bin/bash -c 'date -u +%FT%T.%NZ; cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.stat' >"$out/zone$i-cgroup-final.txt"
  docker exec "${servers[$i]}" du -s -B1 /var/fdb/data /var/fdb/logs >"$out/zone$i-disk.txt"
  inspect_resources "${servers[$i]}" server "$out/zone$i-final-inspect.json"
done
docker stats --no-stream --format '{{json .}}' "${servers[@]}" >"$out/server-stats.jsonl"
name="$tag-native"; containers+=("$name")
docker run --name "$name" --network none --memory "${client_mib}m" --cpus 0.5 "$client_image" /bin/bash -c 'go version; fdbcli --version; sha256sum /usr/local/lib/libfdb_c.so /usr/local/include/foundationdb/*; ldd /usr/local/lib/libfdb_c.so' >"$out/native.txt"
capture_client "$name" native
if [ "$mode" = test ] || [ "$mode" = graph-fault ] || [ "$mode" = lifecycle-trace ]; then
  name="$tag-cover"; containers+=("$name")
  docker run --name "$name" --network none --memory "${client_mib}m" --cpus 0.5 --mount "type=bind,source=$out,target=/evidence" "$client_image" go tool cover -func=/evidence/coverage.out >"$out/coverage-functions.txt"
  capture_client "$name" cover
fi
