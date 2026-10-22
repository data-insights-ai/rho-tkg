#!/usr/bin/env bash
# Fails when tasks/todo.md or HANDOVER.md lag behind the repo (lesson 80).
# Run before every release commit and after every merge.
set -u
cd "$(dirname "$0")/.." || exit 2
fail=0
latest=$(git tag --sort=-creatordate | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
grep -q "${latest}" HANDOVER.md || { echo "ledger: HANDOVER.md does not name ${latest}"; fail=1; }
# docs/CHANGELOG.md's newest released heading (first "## [x.y.z]" after [Unreleased]) must be the latest tag.
top=$(grep -oE '^## \[[0-9]+\.[0-9]+\.[0-9]+\]' docs/CHANGELOG.md | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
[ "v${top}" = "${latest}" ] || { echo "ledger: docs/CHANGELOG.md top release heading [${top}] is not ${latest}"; fail=1; }
# todo.md holds open work only: its open-request rows.
grep -q '^## Open requests' tasks/todo.md || { echo "ledger: tasks/todo.md lacks '## Open requests'"; fail=1; }
requests=$(awk '/^## Open requests/{f=1;next} /^## /{f=0} f' tasks/todo.md | grep -E '^\| [0-9]+[a-z]? \|')
# A done row does not belong in todo.md (shipped = docs/CHANGELOG.md).
echo "$requests" | grep -E '✅|\| *(done|shipped|answered)\b' | cut -c1-90 | sed 's/^/ledger: done row left in todo.md: /' | grep . && fail=1
# An open request citing a backlog item that is closed, i.e. whose number is not a row of "## Backlog (open)".
open_items=$(awk '/^## Backlog \(open\)/{f=1;next} /^## /{f=0} f' tasks/todo.md | grep -oE '^\| [0-9]+[a-z]? \|' | grep -oE '[0-9]+[a-z]?' | sort -u)
[ -n "$open_items" ] || { echo "ledger: found no open backlog items in tasks/todo.md (section or table format changed?)"; fail=1; }
while IFS= read -r row; do
  [ -n "$row" ] || continue
  for n in $(echo "$row" | grep -oiE 'backlog [0-9]+[a-z]?' | awk '{print $2}'); do
    if ! echo "$open_items" | grep -qx "$n"; then echo "ledger: open request cites closed backlog $n (not in Backlog (open)): ${row:0:80}"; fail=1; fi
  done
done <<< "$requests"
exit $fail
