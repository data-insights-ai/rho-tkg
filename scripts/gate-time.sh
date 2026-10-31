#!/usr/bin/env bash
# Runs a gate and appends what it cost to gate-times.log in the git common dir, one line per run, kept out of git:
# start (UTC), gate, seconds, exit status, commit. Usage: gate-time.sh <gate> <command...>
set -u
gate=$1
shift
start=$(date +%s)
"$@"
status=$?
log="$(git rev-parse --git-common-dir)/gate-times.log"
printf '%s %s %ss exit=%s %s\n' "$(date -u -d "@$start" +%FT%TZ)" "$gate" "$(($(date +%s) - start))" "$status" \
  "$(git rev-parse --short HEAD)" >>"$log"
exit "$status"
