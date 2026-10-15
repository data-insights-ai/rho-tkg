#!/bin/bash
# Sourced only by the existing owned four-zone launcher. No SDK driver, native
# reducer, worker, resource profile or decision is replaced by this schedule.
[ "${mode:-}" = composed-platform-faults ] || { echo 'explicit composed fault owner required' >&2; return 2; }

composed_before_reap() {
  local kind="$1" phase="$2" directory="$3" c ip
  [ "$kind" = "$composition_kind" ] && [ "$phase" = "$composition_phase" ] || return 0
  wait_healthy "${servers[0]}" "$directory/composition-healthy-before.json"
  docker inspect "${servers[@]}" >"$directory/composition-owned.before.json"
  for c in "${servers[@]}"; do docker top "$c" -eo pid,args >"$directory/$c.top"; done
  python3 -B composed_fault_controller.py select "$directory" "$tag" "$composition_kind" \
    "$composition_seam" "$composition_fault" "$composition_coord" >"$directory/composition-targets.txt"
  while read -r c ip; do
    docker inspect "$c" >"$directory/$c.composition-before.json"
    docker exec "$c" /bin/bash -c 'cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.stat' >"$directory/$c.composition-memory.txt"
    if [ "$composition_fault" = network ]; then
      docker network disconnect "$tag" "$c" >"$directory/$c.composition-disconnect.txt"
      docker inspect "$c" >"$directory/$c.composition-detached.json"
    else
      docker kill --signal KILL "$c" >"$directory/$c.composition-kill.txt"
      docker inspect "$c" >"$directory/$c.composition-stopped.json"
      python3 -B launcher.py killed "$directory/$c.composition-stopped.json"
    fi
  done <"$directory/composition-targets.txt"
}

composed_after_reap() {
  local kind="$1" phase="$2" directory="$3" c ip
  [ "$kind" = "$composition_kind" ] && [ "$phase" = "$composition_phase" ] || return 0
  # All selected domains stay lost until the SAME SDK caller is actually reaped.
  while read -r c ip; do
    if [ "$composition_fault" = network ]; then
      docker network connect --ip "$ip" "$tag" "$c" >"$directory/$c.composition-connect.txt"
    else
      docker start "$c" >"$directory/$c.composition-restart.txt"
    fi
  done <"$directory/composition-targets.txt"
  wait_healthy "${servers[0]}" "$directory/composition-healthy-after.json"
  while read -r c ip; do docker inspect "$c" >"$directory/$c.composition-after.json"; done <"$directory/composition-targets.txt"
}

run_composed_faults() {
  local composition_end=$((SECONDS+36000)) case_end
  python3 -B composed_fault_controller.py admit "$out"
  for composition_fault in transaction log storage one-zone two-zones network; do
    for composition_seam in chunk source-eof manifest-issued owner gc; do
      for composition_coord in A B; do
        case_end=$((SECONDS+600))
        if [ "$case_end" -gt "$composition_end" ]; then case_end="$composition_end"; fi
        phase_deadline="$case_end"
        if [ "$SECONDS" -ge "$phase_deadline" ]; then echo 'fixed composed fault budget exhausted' >&2; return 1; fi
        if [ "$composition_seam" = owner ] || [ "$composition_seam" = gc ]; then
          composition_kind=source; composition_phase="$composition_seam"
          history_coord="$composition_coord"
          history_key="composed-source-$composition_fault-$composition_seam-$composition_coord"
          history_graph="source-history-fault-v1-$tag-$composition_fault-$composition_seam-$composition_coord"
          history_dir="$out/$history_key"; mkdir "$history_dir"
          python3 -B composed_fault_controller.py begin "$history_dir" "$composition_kind" "$composition_seam" "$composition_fault" "$composition_coord" "$out/commands.jsonl"
          wait_healthy "${servers[0]}" "$history_dir/healthy-before.json"
          history_ledger_start=$(python3 -B history_retirement_controller.py ledger-start "$out/commands.jsonl")
          history_client history-fault-seed "$history_dir/seed.json"
          history_child owner history-fault-owner-child
          history_client history-fault-recover-owner "$history_dir/recovery.json"
          history_recovery_client="$history_client_name"
          history_child gc history-fault-gc-child
          history_client history-fault-terminal "$history_dir/terminal.json"
          printf '{"LedgerStart":%s,"Recovery":"%s","Terminal":"%s"}\n' "$history_ledger_start" "$history_recovery_client" "$history_client_name" >"$history_dir/clients.json"
          wait_healthy "${servers[0]}" "$history_dir/healthy-after.json"
          python3 -B composed_fault_controller.py finalize "$history_dir" "$out/commands.jsonl"
        else
          composition_kind=column; composition_phase=publication
          publication_case="$composition_seam"; publication_coord="$composition_coord"
          publication_key="composed-column-$composition_fault-$composition_seam-$composition_coord"
          publication_graph="column-publication-v1-$tag-$composition_fault-$composition_seam-$composition_coord"
          publication_dir="$out/$publication_key"; mkdir "$publication_dir"
          python3 -B composed_fault_controller.py begin "$publication_dir" "$composition_kind" "$composition_seam" "$composition_fault" "$composition_coord" "$out/commands.jsonl"
          wait_healthy "${servers[0]}" "$publication_dir/healthy-before.json"
          publication_ledger_start=$(python3 -B column_publication_controller.py ledger-start "$out/commands.jsonl")
          publication_client column-fault-seed "$publication_dir/seed.json"
          publication_child publication column-fault-publication-child
          publication_client column-fault-recover "$publication_dir/recovery.json"
          publication_recovery_client="$publication_client_name"
          publication_child gc column-fault-gc-child
          publication_client column-fault-terminal "$publication_dir/terminal.json"
          printf '{"LedgerStart":%s,"Recovery":"%s","Terminal":"%s"}\n' "$publication_ledger_start" "$publication_recovery_client" "$publication_client_name" >"$publication_dir/clients.json"
          wait_healthy "${servers[0]}" "$publication_dir/healthy-after.json"
          python3 -B composed_fault_controller.py finalize "$publication_dir" "$out/commands.jsonl"
        fi
      done
    done
  done
  phase_deadline="$composition_end"
  python3 -B composed_fault_controller.py complete "$out"
}
