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
# A user-request row still "open"/"building"/"in progress" whose backlog item is closed.
# Closed rows read "| Item 73 (…" or "| BACKLOG 12 — …"; take the first number of each.
closed=$(awk '/^## Closed/{f=1} f' tasks/backlog.md | grep -oiE '^\| *(item|backlog) +[0-9]+[a-z]?' | grep -oE '[0-9]+[a-z]?$' | sort -u)
[ -n "$closed" ] || { echo "ledger: found no closed backlog items (table format changed?)"; fail=1; }
while IFS= read -r row; do
  for n in $(echo "$row" | grep -oE 'backlog [0-9]+[a-z]?' | awk '{print $2}'); do
    if echo "$closed" | grep -qx "$n"; then echo "ledger: row still open but backlog $n is closed: ${row:0:80}"; fail=1; fi
  done
done < <(grep -E '^\| [0-9]+[a-z]? \|' tasks/todo.md | grep -iE '\| *(open|building|in progress)')
# A request row "in progress" for which no worktree or agent branch exists is stale: list them for the reader.
grep -E '^\| [0-9]+[a-z]? \|' tasks/todo.md | grep -iE '\| *(in progress|building)' | cut -c1-90 | sed 's/^/ledger: still in progress (check): /'
exit $fail
