#!/usr/bin/env bash
# Shared root/v5 race gate. CI splits only the selected large package; each
# top-level test retains all subtests and Go's existing per-invocation timeout.
set -euo pipefail

gate="root race gate"
fail() {
  printf '%s: %s\n' "$gate" "$*" >&2
  exit 1
}

if (( $# > 2 )); then
  fail 'expected a shard count (1 or 4) and optional root/v5 profile'
fi
case "${2-root}" in
  root) split_path=./pkg/graph/internal/core; split_name=core ;;
  v5) gate="v5 race gate"; split_path=./internal/graphapply; split_name=graphapply ;;
  *) fail 'profile must be root or v5' ;;
esac
shards=${1-1}
flags=(-short -race -count=1)
case "$shards" in
  1) exec go test "${flags[@]}" ./... ;;
  4) ;;
  *) fail "$split_name-shard count must be 1 or 4" ;;
esac

# Race-aware package discovery preserves the current platform/build constraints.
# Inherited GOFLAGS, GOMEMLIMIT and CPU settings are never rewritten here.
split_package=$(go list -race "$split_path")
[[ -n "$split_package" && "$split_package" != *$'\n'* ]] || fail "expected one $split_name package"
packages=$(go list -race ./...)
[[ -n "$packages" ]] || fail "empty ${2-root} package inventory"
other=()
split_count=0
while IFS= read -r package; do
  [[ -n "$package" ]] || fail 'empty package name'
  if [[ "$package" == "$split_package" ]]; then
    split_count=$((split_count + 1))
  else
    other+=("$package")
  fi
done <<< "$packages"
(( split_count == 1 )) || fail "$split_name package must appear exactly once"
duplicates=$(printf '%s\n' "$packages" | LC_ALL=C sort | uniq -d)
[[ -z "$duplicates" ]] || fail "duplicate ${2-root} packages"

# Go supplies the compiled inventory, rather than source-name heuristics. Fuzz
# targets run their seed corpora under -run; examples run through the same door.
# Listing errors are fatal. A missing/foreign footer cannot look like a partial
# successful inventory. Neither selected package has TestMain side effects.
# Process-test parents re-exec the compiled binary with their own child selector;
# the parent shard selector never restricts the compiled child entry points.
listed=$(go test "${flags[@]}" -list '^(Test|Fuzz|Example)' "$split_package")
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
      [[ "$status" == ok && "$package" == "$split_package" && -n "$duration" ]] || fail 'unexpected inventory footer'
      footers=$((footers + 1))
      ;;
    *) fail "unexpected inventory output: $line" ;;
  esac
done <<< "$listed"
(( footers == 1 && ${#names[@]} >= shards )) || fail "incomplete or too-small $split_name inventory"
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
[[ "$assigned_sorted" == "$sorted" && ${#assigned[@]} == "${#names[@]}" ]] || fail "$split_name assignment is not exhaustive and disjoint"
for ((bucket=0; bucket<shards; bucket++)); do
  (( counts[bucket] > 0 )) || fail "empty $split_name shard"
done

printf '%s: %s other packages, %s %s names, %s exhaustive shards\n' "$gate" "${#other[@]}" "${#names[@]}" "$split_name" "$shards"
# Fail fast and return nonzero on any package/shard failure; a partially run gate
# never prints its completion marker. No retry or altered timeout is involved.
if (( ${#other[@]} > 0 )); then
  go test "${flags[@]}" "${other[@]}"
fi
for ((bucket=0; bucket<shards; bucket++)); do
  pattern="^(${patterns[$bucket]})$"
  printf '%s: %s shard %s/%s, %s names, -run %s\n' "$gate" "$split_name" "$((bucket + 1))" "$shards" "${counts[$bucket]}" "$pattern"
  go test "${flags[@]}" -run "$pattern" "$split_package"
done
printf '%s: all %s packages and %s shards passed\n' "$gate" "${2-root}" "$split_name"
