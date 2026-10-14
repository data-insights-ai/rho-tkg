#!/bin/bash
# Sourced only by run-local.sh after its owned four-zone setup. Reuses that
# launcher's resources, bounded Docker ledger, descendant fencing and cleanup.
# This is actual local process/domain isolation, never a simulated fault.
[ "${mode:-}" = platform-faults ] || { echo 'explicit platform-faults owner required' >&2; return 2; }
phase_deadline=$((SECONDS+1800))
platform_client() {
  local command="$1" destination="$2" result=0
  client_index=$((client_index+1))
  platform_client_name="$tag-platform-client$client_index"
  containers+=("$platform_client_name")
  bounded_docker 30 run --name "$platform_client_name" --network "$tag" \
    --memory "${client_mib}m" --cpus 0.5 --env "FDB_CONNECTION_STRING=$connection" \
    --env "FDB_PLATFORM_CASE=$platform_case" --env "FDB_PLATFORM_COORDINATOR=$platform_coord" \
    --env "FDB_PLATFORM_GRAPH=$platform_graph" --env "FDB_PLATFORM_ARTIFACT=/evidence/$platform_key" \
    --mount "type=bind,source=$out,target=/evidence" "$client_image" /bin/bash -c '
      timeout --signal=KILL 20s fdb-spike "$1"; result=$?
      cat /sys/fs/cgroup/memory.peak > "/evidence/$2/$3-memory-peak.txt"
      cat /sys/fs/cgroup/memory.stat > "/evidence/$2/$3-memory-stat.txt"
      exit "$result"' -- "$command" "$platform_key" "$client_index" >"$destination" 2>"$destination.stderr" || result=$?
  stop_client "$platform_client_name" "$platform_key-client$client_index"
  capture_client "$platform_client_name" "$platform_key-client$client_index" || result=1
  return "$result"
}
for platform_case in transaction log storage one-zone two-zones network commit-issued; do
  for platform_coord in A B; do
    platform_key="platform-$platform_case-$platform_coord"
    platform_graph="platform-v1-$tag-$platform_case-$platform_coord"
    platform_dir="$out/$platform_key"
    mkdir "$platform_dir"
    platform_client platform-fault-seed "$platform_dir/seed.json"
    wait_healthy "${servers[0]}" "$platform_dir/healthy-before.json"
    docker inspect "${servers[@]}" >"$platform_dir/owned.before.json"
    for c in "${servers[@]}"; do docker top "$c" -eo pid,args >"$platform_dir/$c.top"; done
    python3 -B platform_fault_controller.py select "$platform_dir/healthy-before.json" \
      "$platform_dir/owned.before.json" "$tag" "$platform_dir" "$platform_case" "$platform_coord" \
      "$platform_dir/plan.json" "$out/commands.jsonl" >"$platform_dir/targets.txt"
    if [ "$platform_case" = commit-issued ]; then
      name="$tag-platform-unknown-$platform_coord"; containers+=("$name")
      docker run -d --name "$name" --network "$tag" --memory "${client_mib}m" --cpus 0.5 \
        --env "FDB_CONNECTION_STRING=$connection" --env "FDB_PLATFORM_CASE=$platform_case" \
        --env "FDB_PLATFORM_COORDINATOR=$platform_coord" --env "FDB_PLATFORM_GRAPH=$platform_graph" \
        --env "FDB_PLATFORM_ARTIFACT=/evidence/$platform_key" \
        --mount "type=bind,source=$out,target=/evidence" "$client_image" fdb-spike platform-fault-commit-child >/dev/null
      reached=0
      for attempt in {1..20}; do
        docker logs "$name" >"$platform_dir/commit-marker.json" 2>"$platform_dir/commit-marker.stderr"
        if python3 -B platform_fault_controller.py marker "$platform_dir/commit-marker.json" "$platform_dir/seed.json" 2>"$platform_dir/marker-validation.txt"; then reached=1; break; fi
        sleep 1
      done
      [ "$reached" = 1 ] || { echo 'actual native Commit-issued seam not reached' >&2; return 1; }
      docker inspect "$name" >"$platform_dir/caller.before.json"
      docker exec "$name" /bin/bash -c 'cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.stat' >"$platform_dir/caller-live-memory.txt"
      docker kill --signal KILL "$name" >"$platform_dir/caller-kill.txt"
      docker inspect "$name" >"$platform_dir/caller.stopped.json"
      python3 -B launcher.py killed "$platform_dir/caller.stopped.json"
      docker logs "$name" >"$platform_dir/caller-final.stdout" 2>"$platform_dir/caller-final.stderr"
      capture_client "$name" "$platform_key-caller"
    else
      while read -r c ip; do
        docker inspect "$c" >"$platform_dir/$c.before.json"
        docker exec "$c" /bin/bash -c 'cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.stat' >"$platform_dir/$c-fault-memory.txt"
        if [ "$platform_case" = network ]; then
          docker network disconnect "$tag" "$c" >"$platform_dir/$c-disconnect.txt"
          docker inspect "$c" >"$platform_dir/$c.detached.json"
        else
          docker kill --signal KILL "$c" >"$platform_dir/$c-kill.txt"
          docker inspect "$c" >"$platform_dir/$c.stopped.json"
          python3 -B launcher.py killed "$platform_dir/$c.stopped.json"
        fi
      done <"$platform_dir/targets.txt"
      progressed=0
      for attempt in {1..8}; do
        platform_client platform-fault-submit "$platform_dir/submit-$attempt.json"
        classification=$(python3 -B platform_fault_controller.py submission "$platform_dir/submit-$attempt.json" \
          "$platform_client_name" "$platform_dir/submissions.jsonl")
        if [ "$platform_case" = two-zones ] || [ "$platform_case" = network ]; then break; fi
        if [ "$classification" = committed ]; then progressed=1; break; fi
        # A definite invalid/storage/config cause never authorizes a blind retry.
        python3 -B platform_fault_controller.py retry "$platform_dir/submit-$attempt.json"
      done
      if [ "$platform_case" != two-zones ] && [ "$platform_case" != network ] && [ "$progressed" != 1 ]; then
        echo 'actual one-domain/role-loss progress missing' >&2; return 1
      fi
      while read -r c ip; do
        if [ "$platform_case" = network ]; then
          docker network connect --ip "$ip" "$tag" "$c" >"$platform_dir/$c-connect.txt"
        else
          docker start "$c" >"$platform_dir/$c-restart.txt"
        fi
      done <"$platform_dir/targets.txt"
    fi
    wait_healthy "${servers[0]}" "$platform_dir/healthy-after.json"
    while read -r c ip; do docker inspect "$c" >"$platform_dir/$c.after.json"; done <"$platform_dir/targets.txt"
    # Lookup and SAME-wire retry occur only after actual heal; SDK retries keep
    # their original identity. A missing pair never fabricates ABORT authority.
    platform_client platform-fault-recover "$platform_dir/recovery.jsonl"
    platform_client platform-fault-terminal "$platform_dir/terminal.json"
    printf '{"Terminal":"%s"}\n' "$platform_client_name" >"$platform_dir/clients.json"
    python3 -B platform_fault_controller.py finalize "$platform_dir" "$out/commands.jsonl"
  done
done
