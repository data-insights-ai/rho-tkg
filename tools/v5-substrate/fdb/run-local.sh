#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
mode=${1:-test}
case "$mode" in smoke|test) ;; *) echo 'usage: run-local.sh [smoke|test]' >&2; exit 2;; esac
runtime='foundationdb/foundationdb@sha256:c61744624e479d370a0120176e518bb569a2a18ee9abceaf7022c4367b54f1db'
tag="rho-v5-fdb-$(date +%s)-$$"
out="$PWD/artifacts/$tag"
mkdir -p "$out"
containers=()
volumes=()
cleanup() {
  for c in ${containers[*]-}; do docker logs "$c" >"$out/$c.log" 2>&1 || true; docker rm -f "$c" >/dev/null 2>&1 || true; done
  for v in ${volumes[*]-}; do docker volume rm "$v" >/dev/null 2>&1 || true; done
  docker network rm "$tag" >/dev/null 2>&1 || true
  if [ -n "${client_image-}" ]; then docker image rm "$client_image" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
capture_status() {
  local container="$1" destination="$2"
  for attempt in 1 2 3 4 5 6; do
    if docker exec "$container" fdbcli --timeout 10 --exec 'status json' >"$destination.attempt$attempt" 2>"$destination.attempt$attempt.stderr"; then
      if python3 -c 'import json,sys;json.load(open(sys.argv[1]))' "$destination.attempt$attempt"; then cp "$destination.attempt$attempt" "$destination"; return 0; fi
    fi
    sleep 1
  done
  return 1
}
client_image="$tag-client"
shasum -a 256 *.go go.mod go.sum Dockerfile run-local.sh .dockerignore >"$out/source-sha256.txt"
docker build -t "$client_image" . >"$out/build.log" 2>&1
docker image inspect "$client_image" "$runtime" >"$out/images.json"
docker version >"$out/docker-version.txt"
docker network create --internal "$tag" >"$out/network-id.txt"
docker network inspect "$tag" >"$out/network.json"
subnet=$(docker network inspect --format '{{(index .IPAM.Config 0).Subnet}}' "$tag")
ips=()
for i in 0 1 2 3; do
  ips+=("$(python3 -c 'import ipaddress,sys;print(ipaddress.ip_network(sys.argv[1])[10+int(sys.argv[2])])' "$subnet" "$i")")
done
connection="rho_spike:rhov5$(date +%s)$$@${ips[0]}:4500,${ips[1]}:4500,${ips[2]}:4500"
printf '%s\n' "$connection" >"$out/fdb.cluster"
for i in 0 1 2 3; do
  c="$tag-zone$i"; v="$tag-data$i"
  volumes+=("$v"); containers+=("$c")
  docker volume create "$v" >/dev/null
  docker run -d --name "$c" --hostname "zone$i" --network "$tag" --ip "${ips[$i]}" \
    --memory 2g --mount "type=volume,source=$v,target=/var/fdb/data" \
    --env "FDB_CLUSTER_FILE_CONTENTS=$connection" --entrypoint /bin/bash "$runtime" \
    -c 'printf "%s\n" "$FDB_CLUSTER_FILE_CONTENTS" > /var/fdb/fdb.cluster; exec fdbserver -C /var/fdb/fdb.cluster -p auto:4500 -l 0.0.0.0:4500 --datadir /var/fdb/data --logdir /var/fdb/logs --locality-zoneid="$HOSTNAME" --locality-machineid="$HOSTNAME" --memory 1536MiB --storage-memory 128MiB --cache-memory 128MiB --maxlogssize 20MiB' >/dev/null
done
docker exec "${containers[0]}" fdbcli --timeout 30 --exec 'configure new triple ssd' >"$out/configure.txt" 2>&1
ready=0
for i in {1..90}; do
  if ! docker exec "${containers[0]}" fdbcli --timeout 5 --exec 'status json' >"$out/status.json" 2>"$out/status.stderr"; then sleep 1; continue; fi
  if python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));sys.exit(not (d.get("client",{}).get("database_status",{}).get("healthy",False) and d.get("cluster",{}).get("full_replication",False) and d.get("cluster",{}).get("data",{}).get("state",{}).get("name")=="healthy"))' "$out/status.json"; then ready=1; break; fi
  sleep 1
done
if [ "$ready" != 1 ]; then echo 'cluster not available' >&2; exit 1; fi
python3 - "$out/status.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]));c=d['cluster']['configuration']
assert c['redundancy_mode']=='triple',c
assert len(d['cluster']['processes'])==4,d['cluster']['processes']
assert len(d['client']['coordinators']['coordinators'])==3,d['client']['coordinators']
assert len(d['cluster']['machines'])==4,d['cluster']['machines']
assert d['cluster']['recovery_state']['name']=='fully_recovered',d['cluster']['recovery_state']
assert {p['locality']['zoneid'] for p in d['cluster']['machines'].values()}=={'zone0','zone1','zone2','zone3'},d['cluster']['machines']
assert d['cluster']['data']['state']['min_replicas_remaining']>=3,d['cluster']['data']['state']
PY
docker inspect "${containers[@]:0:4}" >"$out/servers.json"
containers+=("$tag-smoke")
docker run --name "$tag-smoke" --network "$tag" --env "FDB_CONNECTION_STRING=$connection" \
  --mount "type=bind,source=$out,target=/evidence" "$client_image" /bin/bash -c 'timeout --signal=KILL 30s fdb-spike smoke; result=$?; cat /sys/fs/cgroup/memory.peak > /evidence/smoke-client-memory-peak.txt; exit "$result"'  >"$out/smoke.json"
if [ "$mode" = test ]; then
  containers+=("$tag-client")
  docker run --name "$tag-client" --network "$tag" --memory 2g \
    --env "FDB_CONNECTION_STRING=$connection" --env FDB_ZONE_IPS="${ips[*]}" \
    --mount "type=bind,source=$out,target=/evidence" "$client_image" timeout --signal=KILL 300s \
    /bin/bash -c 'go vet ./... || exit $?; timeout --signal=KILL 240s go test -timeout 180s -count=1 -race -coverprofile=/evidence/coverage.out -v ./...; result=$?; cat /sys/fs/cgroup/memory.peak > /evidence/client-cgroup-memory-peak.txt; cat /sys/fs/cgroup/memory.current > /evidence/client-cgroup-memory-current.txt; cat /sys/fs/cgroup/memory.stat > /evidence/client-cgroup-memory-stat.txt; exit "$result"' >"$out/tests.txt" 2>&1
fi
if [ "$mode" = test ]; then
  fault_index=0
  fault_client() {
    fault_index=$((fault_index+1))
    local name="$tag-fault$fault_index"
    containers+=("$name")
    docker run --name "$name" --network "$tag" --memory 512m \
      --env "FDB_CONNECTION_STRING=$connection" --mount "type=bind,source=$out,target=/evidence" \
      "$client_image" /bin/bash -c 'timeout --signal=KILL 45s fdb-spike "$1"; result=$?; cat /sys/fs/cgroup/memory.peak > "/evidence/$1-client-memory-peak.txt"; exit "$result"' -- "$1" >"$out/$1-$fault_index.json"
  }
  fault_client fault-seed
  for phase in before after; do
    name="$tag-unknown-$phase"
    containers+=("$name")
    docker run -d --name "$name" --network "$tag" --memory 512m \
      --env "FDB_CONNECTION_STRING=$connection" --mount "type=bind,source=$out,target=/evidence" \
      "$client_image" fdb-spike "fault-$phase-child" >/dev/null
    marker=0
    for i in {1..30}; do
      docker logs "$name" >"$out/$phase-child-marker.json" 2>&1
      if python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));sys.exit("ready" not in d)' "$out/$phase-child-marker.json" 2>/dev/null; then marker=1; break; fi
      sleep 1
    done
    if [ "$marker" != 1 ]; then echo 'child fault seam not reached' >&2; exit 1; fi
    docker exec "$name" cat /sys/fs/cgroup/memory.peak >"$out/$phase-child-memory-peak.txt"
    docker kill --signal KILL "$name" >"$out/$phase-child-kill.txt"
    test "$(docker inspect --format '{{.State.ExitCode}}' "$name")" = 137
    fault_client "fault-recover-$phase"
  done
  fault_client fault-verify
  capture_status "${containers[0]}" "$out/status-before-zone-loss.json"
  python3 - "$out/status-before-zone-loss.json" <<'PY_READY'
import json,sys
d=json.load(open(sys.argv[1]));c=d['cluster']
assert d['client']['database_status']['healthy'] and c['full_replication']
assert c['data']['state']['healthy'] and c['data']['state']['min_replicas_remaining']>=3
assert {m['locality']['zoneid'] for m in c['machines'].values()}=={'zone0','zone1','zone2','zone3'}
PY_READY
  for c in "${containers[@]:0:4}"; do printf "%s " "$c"; docker exec "$c" cat /sys/fs/cgroup/memory.peak; done >"$out/server-pre-fault-cgroup-peaks.txt"
  docker kill --signal KILL "${containers[0]}" >"$out/zone-kill.txt"
  test "$(docker inspect --format '{{.State.ExitCode}}' "${containers[0]}")" = 137
  # Progress may be temporarily unavailable during native role recovery. Do not
  # turn an application UNKNOWN into an abort; repeat the SAME payload/digest.
  progressed=0
  for i in {1..6}; do
    if fault_client fault-progress; then progressed=1; break; fi
    sleep 2
  done
  if [ "$progressed" != 1 ]; then echo 'one-zone-loss progress not established' >&2; exit 1; fi
  capture_status "${containers[1]}" "$out/status-with-zone-loss.json"
  for c in "${containers[@]:1:3}"; do printf "%s " "$c"; docker exec "$c" cat /sys/fs/cgroup/memory.peak; done >"$out/server-under-zone-loss-cgroup-peaks.txt"
  docker start "${containers[0]}" >"$out/zone-restart.txt"
  # Kill servers sequentially; all four are down before any server restart.
  for c in "${containers[@]:0:4}"; do docker kill --signal KILL "$c"; done >"$out/all-server-kill.txt"
  for c in "${containers[@]:0:4}"; do docker start "$c"; done >"$out/all-server-restart.txt"
  recovered=0
  for i in {1..60}; do
    if ! docker exec "${containers[1]}" fdbcli --timeout 5 --exec 'status json' >"$out/status-reopened.json" 2>"$out/status-reopened.stderr"; then sleep 1; continue; fi
    if python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));sys.exit(not (d.get("client",{}).get("database_status",{}).get("healthy",False) and d.get("cluster",{}).get("full_replication",False) and d.get("cluster",{}).get("data",{}).get("state",{}).get("name")=="healthy"))' "$out/status-reopened.json"; then recovered=1; break; fi
    sleep 1
  done
  if [ "$recovered" != 1 ]; then echo 'server reopen unavailable' >&2; exit 1; fi
  fault_client fault-progress
fi
capture_status "${containers[0]}" "$out/status-after.json"
for i in 0 1 2 3; do docker exec "${containers[$i]}" /bin/bash -c 'date -u +%FT%T.%NZ; cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.stat' >"$out/zone$i-cgroup-final.txt" & done
wait
docker stats --no-stream --format '{{json .}}' "${containers[@]:0:4}" >"$out/server-stats.jsonl"
for c in "${containers[@]:0:4}"; do docker exec "$c" du -s -B1 /var/fdb/data /var/fdb/logs; docker exec "$c" cat /sys/fs/cgroup/memory.peak; done >"$out/disk-by-zone.txt"
docker run --rm --network none "$client_image" /bin/bash -c 'go version; fdbcli --version; sha256sum /usr/local/lib/libfdb_c.so /usr/local/include/foundationdb/*; ldd /usr/local/lib/libfdb_c.so' >"$out/native.txt"
if [ "$mode" = test ]; then docker run --rm --network none --mount "type=bind,source=$out,target=/evidence" "$client_image" go tool cover -func=/evidence/coverage.out >"$out/coverage-functions.txt"; fi
printf '%s\n' "$out"
