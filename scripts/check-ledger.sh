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
closed=$(awk '/^## Closed/{f=1} f' tasks/backlog.md | grep -oE '^\| *[0-9]+[a-z]? *\|' | tr -dc '0-9a-z\n' | sort -u)
while IFS= read -r row; do
  for n in $(echo "$row" | grep -oE 'backlog [0-9]+[a-z]?' | awk '{print $2}'); do
    if echo "$closed" | grep -qx "$n"; then echo "ledger: row still open but backlog $n is closed: ${row:0:80}"; fail=1; fi
  done
done < <(grep -E '^\| [0-9]+[a-z]? \|' tasks/todo.md | grep -iE '\| *(open|building|in progress)')
exit $fail
