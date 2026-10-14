#!/usr/bin/env bash
# Tests scripts/test-race.sh against a fake `go` (no toolchain needed): the
# shard plan, that every shard runs exactly its packages / top-level tests, that
# the shards together are exhaustive and disjoint, and the refusals.
# Run: bash scripts/test-race-test.sh
set -euo pipefail

script=$(cd "$(dirname "$0")" && pwd)/test-race.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin"
mod=example.com/m
core=$mod/pkg/graph/internal/core

# The fake go: package inventory, compiled core test inventory, and a log of
# every `go test` run (one line each). FAKE_GO_FAIL=<substring> fails the run
# whose arguments contain it.
cat > "$work/bin/go" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [[ "\$1" == list ]]; then
  if [[ "\${FAKE_GO_PROFILE-root}" == v5 ]]; then
    case "\${@: -1}" in
      ./...) printf '%s\n' $mod/internal/a $mod/internal/graphapply $mod/internal/z ;;
      ./internal/graphapply) echo $mod/internal/graphapply ;;
      *) exit 1 ;;
    esac
    exit 0
  fi
  case "\${@: -1}" in
    ./...) printf '%s\n' $mod/pkg/a $mod/pkg/graph $core $mod/pkg/graph/store/memory $mod/pkg/z ;;
    ./pkg/graph/internal/core) echo $core ;;
    ./pkg/graph) echo $mod/pkg/graph ;;
    ./pkg/graph/store/memory) echo $mod/pkg/graph/store/memory ;;
    *) echo "fake go: unknown list \$*" >&2; exit 1 ;;
  esac
  exit 0
fi
if [[ " \$* " == *' -list '* ]]; then
  printf '%s\n' TestA TestB TestC TestD TestE TestF FuzzG ExampleH TestI
  if [[ "\${FAKE_GO_PROFILE-root}" == v5 ]]; then
  echo "ok  	$mod/internal/graphapply	0.1s"
  else
  echo "ok  	$core	0.1s"
  fi
  exit 0
fi
echo "\$*" >> "\$FAKE_GO_LOG"
if [[ -n "\${FAKE_GO_FAIL-}" && "\$*" == *"\$FAKE_GO_FAIL"* ]]; then exit 1; fi
EOF
chmod +x "$work/bin/go"
export PATH="$work/bin:$PATH" FAKE_GO_LOG="$work/log"

failures=0
fail() { printf 'FAIL: %s\n' "$*" >&2; failures=$((failures + 1)); }
run() { # run <RACE_SHARD> <args...>: stdout+stderr to $work/out, log reset
  : > "$FAKE_GO_LOG"
  local shard=$1; shift
  RACE_SHARD=$shard bash "$script" "$@" > "$work/out" 2>&1
}
log() { cat "$FAKE_GO_LOG"; }

# 1. The plan names every shard once, in a fixed order.
run list 4 || fail "list exited nonzero: $(cat "$work/out")"
want='["other","graph","memory","core-1","core-2","core-3","core-4"]'
[[ "$(cat "$work/out")" == "$want" ]] || fail "list = $(cat "$work/out"), want $want"

# 2. other: every package except the split and the isolated ones, one run.
run other 4 || fail "other exited nonzero: $(cat "$work/out")"
[[ "$(log)" == "test -short -race -count=1 $mod/pkg/a $mod/pkg/z" ]] || fail "other ran: $(log)"

# 3. an isolated package runs alone.
run graph 4 || fail "graph exited nonzero"
[[ "$(log)" == "test -short -race -count=1 $mod/pkg/graph" ]] || fail "graph ran: $(log)"
run memory 4 || fail "memory exited nonzero"
[[ "$(log)" == "test -short -race -count=1 $mod/pkg/graph/store/memory" ]] || fail "memory ran: $(log)"

# 4. core shards: one run each, only core, and together every compiled name
#    exactly once (exhaustive, disjoint).
all=()
for n in 1 2 3 4; do
  run "core-$n" 4 || fail "core-$n exited nonzero"
  [[ $(log | wc -l) -eq 1 ]] || fail "core-$n ran $(log | wc -l) times"
  line=$(log)
  [[ "$line" == "test -short -race -count=1 -run ^("*")\$ $core" ]] || fail "core-$n ran: $line"
  pattern=${line#*-run ^(}
  pattern=${pattern%)\$ *}
  IFS='|' read -r -a names <<< "$pattern"
  all+=("${names[@]}")
done
got=$(printf '%s\n' "${all[@]}" | LC_ALL=C sort)
want=$(printf '%s\n' ExampleH FuzzG TestA TestB TestC TestD TestE TestF TestI | LC_ALL=C sort)
[[ "$got" == "$want" ]] || fail "core shards cover ${got//$'\n'/ }, want ${want//$'\n'/ }"

# 5. no selector: every part in sequence (local/v5 behaviour), 7 runs.
run '' 4 || fail "full sequence exited nonzero"
[[ $(log | wc -l) -eq 7 ]] || fail "full sequence ran $(log | wc -l) parts: $(log)"
grep -q 'all root packages and core shards passed' "$work/out" || fail "full sequence lacks its completion marker"

# 6. one shard: plain go test ./..., the selector is refused.
run '' 1 || fail "unsharded exited nonzero"
[[ "$(log)" == "test -short -race -count=1 ./..." ]] || fail "unsharded ran: $(log)"
run other 1 && fail "RACE_SHARD with 1 shard accepted"

# 7. refusals: unknown or out-of-range selectors run nothing.
for bad in core-0 core-5 core-x core- foo 'other ' graph/x list2; do
  if run "$bad" 4; then fail "RACE_SHARD='$bad' accepted"; fi
  [[ ! -s "$FAKE_GO_LOG" ]] || fail "RACE_SHARD='$bad' ran: $(log)"
done

# 8. a failing part fails the run and prints no completion marker.
if FAKE_GO_FAIL=pkg/graph/store/memory run '' 4; then fail "a failing memory run passed the sequence"; fi
grep -q 'passed' "$work/out" && fail "a failed sequence printed its completion marker"
if FAKE_GO_FAIL=TestB run core-4 4; then fail "a failing core shard passed"; fi # sorted round-robin: TestB is core-4

# 9. v5 has no isolated packages. Its four graphapply shards still retain
# every parent and its subtests, including on Bash 3.2 with set -u.
export FAKE_GO_PROFILE=v5
run list 4 v5 || fail "v5 list exited nonzero"
[[ "$(cat "$work/out")" == '["other","graphapply-1","graphapply-2","graphapply-3","graphapply-4"]' ]] || fail "v5 plan: $(cat "$work/out")"
run '' 4 v5 || fail "v5 full sequence exited nonzero: $(cat "$work/out")"
[[ $(log | wc -l) -eq 5 ]] || fail "v5 sequence ran: $(log)"
grep -q 'all v5 packages and graphapply shards passed' "$work/out" || fail "v5 completion marker missing"
run graphapply-4 4 v5 || fail "v5 graphapply-4 exited nonzero"
[[ "$(log)" == "test -short -race -count=1 -run ^(TestB|TestF)\$ $mod/internal/graphapply" ]] || fail "v5 parent shard: $(log)"
run core-1 4 v5 && fail "v5 accepted a root selector"
[[ ! -s "$FAKE_GO_LOG" ]] || fail "v5 invalid selector ran tests"

if (( failures > 0 )); then
  printf '%d failure(s)\n' "$failures" >&2
  exit 1
fi
echo 'test-race-test: ok'
