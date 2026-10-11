#!/usr/bin/env bash
# Shared root race gate. CI splits only the large core package; each top-level
# test retains all its subtests and Go's existing per-invocation timeout.
set -euo pipefail

fail() {
  printf 'root race gate: %s\n' "$*" >&2
  exit 1
}

if (( $# > 1 )); then
  fail 'expected one core-shard count (1 or 4)'
fi
shards=${1-1}
flags=(-short -race -count=1)
case "$shards" in
  1) exec go test "${flags[@]}" ./... ;;
  4) ;;
  *) fail 'core-shard count must be 1 or 4' ;;
esac

# Race-aware package discovery preserves the current platform/build constraints.
# Inherited GOFLAGS, GOMEMLIMIT and CPU settings are never rewritten here.
core=$(go list -race ./pkg/graph/internal/core)
[[ -n "$core" && "$core" != *$'\n'* ]] || fail 'expected one core package'
packages=$(go list -race ./...)
[[ -n "$packages" ]] || fail 'empty root package inventory'
other=()
core_count=0
while IFS= read -r package; do
  [[ -n "$package" ]] || fail 'empty package name'
  if [[ "$package" == "$core" ]]; then
    core_count=$((core_count + 1))
  else
    other+=("$package")
  fi
done <<< "$packages"
(( core_count == 1 )) || fail 'core package must appear exactly once'
duplicates=$(printf '%s\n' "$packages" | LC_ALL=C sort | uniq -d)
[[ -z "$duplicates" ]] || fail 'duplicate root packages'

# Go supplies the compiled inventory, rather than source-name heuristics. Fuzz
# targets run their seed corpora under -run; examples run through the same door.
# Listing errors are fatal. A missing/foreign footer cannot look like a partial
# successful inventory. The core package currently has no TestMain side effects.
listed=$(go test "${flags[@]}" -list '^(Test|Fuzz|Example)' "$core")
names=()
footers=0
while IFS= read -r line; do
  case "$line" in
    '') ;;
    Test*|Fuzz*|Example*)
      [[ "$line" != *' '* && "$line" != *$'\t'* && "$line" != *$'\r'* ]] || fail 'invalid compiled test name'
      names+=("$line")
      ;;
    ok[[:space:]]*)
      read -r status package duration <<< "$line"
      [[ "$status" == ok && "$package" == "$core" && -n "$duration" ]] || fail 'unexpected inventory footer'
      footers=$((footers + 1))
      ;;
    *) fail "unexpected inventory output: $line" ;;
  esac
done <<< "$listed"
(( footers == 1 && ${#names[@]} >= shards )) || fail 'incomplete or too-small core inventory'
sorted=$(printf '%s\n' "${names[@]}" | LC_ALL=C sort)
duplicates=$(printf '%s\n' "$sorted" | uniq -d)
[[ -z "$duplicates" ]] || fail 'duplicate compiled test names'

# Round-robin sorted names distribute families without splitting any parent.
# Quote RE2 metacharacters defensively, including in future compiled names.
regex_quote() {
  local text=$1 meta
  for meta in "\\" '.' '^' '$' '|' '?' '*' '+' '(' ')' '[' ']' '{' '}'; do
    text=${text//"$meta"/\\"$meta"}
  done
  printf '%s' "$text"
}
patterns=('' '' '' '')
counts=(0 0 0 0)
assigned=()
index=0
while IFS= read -r name; do
  bucket=$((index % shards))
  quoted=$(regex_quote "$name")
  if [[ -n "${patterns[$bucket]}" ]]; then
    patterns[bucket]+='|'
  fi
  patterns[bucket]+="$quoted"
  counts[bucket]=$((counts[bucket] + 1))
  assigned+=("$name")
  index=$((index + 1))
done <<< "$sorted"
assigned_sorted=$(printf '%s\n' "${assigned[@]}" | LC_ALL=C sort)
[[ "$assigned_sorted" == "$sorted" && ${#assigned[@]} == "${#names[@]}" ]] || fail 'core assignment is not exhaustive and disjoint'
for ((bucket=0; bucket<shards; bucket++)); do
  (( counts[bucket] > 0 )) || fail 'empty core shard'
done

printf 'root race gate: %s other packages, %s core names, %s exhaustive shards\n' "${#other[@]}" "${#names[@]}" "$shards"
# Fail fast and return nonzero on any package/shard failure; a partially run gate
# never prints its completion marker. No retry or altered timeout is involved.
if (( ${#other[@]} > 0 )); then
  go test "${flags[@]}" "${other[@]}"
fi
for ((bucket=0; bucket<shards; bucket++)); do
  pattern="^(${patterns[$bucket]})$"
  printf 'root race gate: core shard %s/%s, %s names, -run %s\n' "$((bucket + 1))" "$shards" "${counts[$bucket]}" "$pattern"
  go test "${flags[@]}" -run "$pattern" "$core"
done
printf 'root race gate: all root packages and core shards passed\n'
