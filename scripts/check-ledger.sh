#!/usr/bin/env bash
# Fails when tasks/todo.md or HANDOVER.md lag behind the repo (lesson 80).
# Run before every release commit and after every merge.
set -u
cd "$(dirname "$0")/.." || exit 2
fail=0
latest=$(git tag --sort=-creatordate | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
grep -q "^| ${latest} |" tasks/todo.md || { echo "ledger: releases table lacks ${latest}"; fail=1; }
grep -q "${latest}" HANDOVER.md || { echo "ledger: HANDOVER.md does not name ${latest}"; fail=1; }
head -1 tasks/todo.md | grep -q "${latest}" || { echo "ledger: todo.md header does not name ${latest}"; fail=1; }
# The user-request rows only (other todo tables also start with "| N |").
requests=$(awk '/^## User requests/{f=1;next} /^## /{f=0} f' tasks/todo.md | grep -E '^\| [0-9]+[a-z]? \|')
# A user-request row still "open"/"building"/"in progress" citing a backlog item that is closed, i.e. whose
# number is not a row of todo's "## Backlog (open)" table (backlog.md moved there in row 110).
open_items=$(awk '/^## Backlog \(open\)/{f=1;next} /^## /{f=0} f' tasks/todo.md | grep -oE '^\| [0-9]+[a-z]? \|' | grep -oE '[0-9]+[a-z]?' | sort -u)
[ -n "$open_items" ] || { echo "ledger: found no open backlog items in tasks/todo.md (section or table format changed?)"; fail=1; }
while IFS= read -r row; do
  [ -n "$row" ] || continue
  for n in $(echo "$row" | grep -oiE 'backlog [0-9]+[a-z]?' | awk '{print $2}'); do
    if ! echo "$open_items" | grep -qx "$n"; then echo "ledger: row still open but backlog $n is closed (not in Backlog (open)): ${row:0:80}"; fail=1; fi
  done
done < <(echo "$requests" | grep -iE '\| *(open|building|in progress)')
# A request row "in progress" for which no worktree or agent branch exists is stale: list them for the reader.
echo "$requests" | grep -iE '\| *(in progress|building)' | cut -c1-90 | sed 's/^/ledger: still in progress (check): /'
exit $fail
