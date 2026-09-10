#!/usr/bin/env bash
# R1 sweep driver for the temporal-replay benchmark (tasks/plan-temporal-index-ingestion.md).
#
# Two passes per configuration on the same durable workload:
#   timing   — plain run, wall/latency/RSS/digest into <out>/<cfg>.json
#   counting — the same run under strace -c for EXACT durability syscalls
#              (msync is Badger v4's WAL sync under WithSyncWrites; fsync covers
#              registry/manifest/vlog) into <out>/<cfg>.strace
# Every run is bounded by `timeout` and an address-space ulimit (the watchdog).
set -euo pipefail

OUT=${1:?usage: run.sh <out-dir> [rows] [facts]}
ROWS=${2:-3000}
FACTS=${3:-750}
MODES=${MODES:-"tx strong concurrent"}
PRODUCERS=${PRODUCERS:-"1 4"}
BATCHES=${BATCHES:-"1 16 128 1024"}
TIMEOUT=${TIMEOUT:-900}
MEM_KB=${MEM_KB:-16777216} # 16 GiB address-space ceiling per run

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
mkdir -p "$OUT"
cd "$ROOT"
go test -c -o "$OUT/replay.test" ./bench/temporalreplay/
{
  echo "revision: $(git rev-parse HEAD)"
  echo "dirty: $(git status --short | wc -l) paths"
  echo "go: $(go version)"
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "host cpus: $(nproc)  mem: $(free -g | awk '/Mem:/{print $2}') GiB"
  echo "disk: $(df -h "$OUT" | tail -1)"
  echo "rows=$ROWS facts=$FACTS modes=$MODES producers=$PRODUCERS batches=$BATCHES"
} > "$OUT/env.txt"

run_one() { # mode producers batch pass
  local m=$1 p=$2 b=$3 pass=$4
  local cfg="${m}-p${p}-b${b}"
  local dir="$OUT/db-$cfg-$pass"
  local env=(TKG_REPLAY_MODE="$m" TKG_REPLAY_ROWS="$ROWS" TKG_REPLAY_FACTS="$FACTS"
             TKG_REPLAY_PRODUCERS="$p" TKG_REPLAY_BATCH="$b" TKG_REPLAY_DIR="$dir")
  if [[ $pass == timing ]]; then
    ( ulimit -v "$MEM_KB"; env "${env[@]}" TKG_REPLAY_OUT="$OUT/$cfg.json" \
        timeout "$TIMEOUT" "$OUT/replay.test" -test.run '^TestReplayRun$' -test.count=1 \
        > "$OUT/$cfg.timing.log" 2>&1 ) || echo "FAILED timing $cfg" >> "$OUT/failures.txt"
  else
    ( ulimit -v "$MEM_KB"; env "${env[@]}" TKG_REPLAY_OUT="$OUT/$cfg.counting.json" \
        timeout "$TIMEOUT" strace -f -c -e trace=fsync,fdatasync,msync,sync_file_range,pwrite64,write \
        -o "$OUT/$cfg.strace" "$OUT/replay.test" -test.run '^TestReplayRun$' -test.count=1 \
        > "$OUT/$cfg.counting.log" 2>&1 ) || echo "FAILED counting $cfg" >> "$OUT/failures.txt"
  fi
  rm -rf "$dir"
}

for m in $MODES; do
  for p in $PRODUCERS; do
    for b in $BATCHES; do
      run_one "$m" "$p" "$b" timing
      run_one "$m" "$p" "$b" counting
      echo "done $m p$p b$b $(date -u +%H:%M:%S)"
    done
  done
done
echo "sweep complete $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUT/env.txt"
