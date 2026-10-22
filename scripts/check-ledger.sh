#!/usr/bin/env bash
# Fails when tasks/todo.md or HANDOVER.md lag behind the repo (lesson 80).
# Run before every release commit and after every merge.
set -u
cd "$(dirname "$0")/.." || exit 2
fail=0
latest=$(git tag --sort=-creatordate | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
grep -q "${latest}" HANDOVER.md || { echo "ledger: HANDOVER.md does not name ${latest}"; fail=1; }
# docs/CHANGELOG.md's newest heading (first "## [x.y.z]" after [Unreleased]) is the latest tag, or, in a release
# commit before its tag (lesson 82 step 4), an untagged version above it that HANDOVER already names.
top=$(grep -oE '^## \[[0-9]+\.[0-9]+\.[0-9]+\]' docs/CHANGELOG.md | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
if [ "v${top}" != "${latest}" ]; then
  newest=$(printf '%s\n%s\n' "${latest#v}" "${top}" | sort -V | tail -1)
  if git rev-parse -q --verify "refs/tags/v${top}" >/dev/null || [ "${newest}" != "${top}" ]; then
    echo "ledger: docs/CHANGELOG.md top release heading [${top}] is neither ${latest} nor a pending newer release"; fail=1
  elif ! grep -q "v${top}" HANDOVER.md; then
    echo "ledger: pending release v${top}: HANDOVER.md does not name it"; fail=1
  fi
fi
# todo.md holds next steps only: sections Next, Waiting, Decisions, Parked backlog (one "- N — problem" line per item).
for sec in 'Next' 'Waiting' 'Decisions' 'Parked backlog'; do
  grep -q "^## ${sec}" tasks/todo.md || { echo "ledger: tasks/todo.md lacks '## ${sec}'"; fail=1; }
done
# Every line outside the parked backlog is a next step, a trigger or a decision.
steps=$(awk '/^## /{f=($0 !~ /^## Parked backlog/)} f' tasks/todo.md)
# A done item does not belong in todo.md (shipped = docs/CHANGELOG.md).
echo "$steps" | grep -iE '✅|^([0-9]+\.|-) +(\([0-9]+\) +)?(done|shipped|answered)\b|\b(done|shipped|answered)[.)]*$' | cut -c1-90 | sed 's/^/ledger: done item left in todo.md: /' | grep . && fail=1
# A step citing a backlog item that is closed, i.e. whose number is not a line of "## Parked backlog".
open_items=$(awk '/^## /{f=($0 ~ /^## Parked backlog/);next} f' tasks/todo.md | grep -oE '^- [0-9]+[a-z]? ' | grep -oE '[0-9]+[a-z]?' | sort -u)
[ -n "$open_items" ] || { echo "ledger: found no parked backlog items in tasks/todo.md (section or '- N — ' format changed?)"; fail=1; }
while IFS= read -r row; do
  [ -n "$row" ] || continue
  for n in $(echo "$row" | grep -oiE 'backlog [0-9]+[a-z]?(( \([a-z]+\))?(, | and |-)[0-9]+[a-z]?)*' | grep -oE '[0-9]+[a-z]?'); do
    if ! echo "$open_items" | grep -qx "$n"; then echo "ledger: todo cites closed backlog $n (not in Parked backlog): ${row:0:80}"; fail=1; fi
  done
done <<< "$steps"
exit $fail
