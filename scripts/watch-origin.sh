#!/usr/bin/env bash
set -o pipefail
# Exits (one notification) only when someone other than "dev team" pushes to rho-tkg origin,
# or opens/updates a PR or issue. Silent otherwise; no timeout.
LOG=${WATCH_LOG_DIR:-/tmp}
cd /home/renework2023/Work/2026/datainsights/rho-tkg || exit 1
snap() { git ls-remote --heads --tags origin 2>/dev/null | sort; }
prs() { gh pr list --state all --limit 30 --json number,updatedAt,author --jq '.[]|select(.author.login!="renebd421")|"\(.number) \(.updatedAt)"' 2>/dev/null | sort; }
iss() { gh issue list --state all --limit 30 --json number,updatedAt,author,comments --jq '.[]|select((((.comments|last)|.author.login) // .author.login) != "renebd421")|"\(.number) \(.updatedAt)"' 2>/dev/null | sort; }
prev=$(snap); pp=$(prs); pi=$(iss)
[ -z "$prev" ] && { echo "watch: initial ls-remote failed"; exit 1; }
while true; do
  sleep 60
  cur=$(snap); [ -z "$cur" ] && continue
  if [ "$cur" != "$prev" ]; then
    git fetch -q origin --prune --tags 2>/dev/null
    out=""
    while read -r sha ref; do
      old=$(echo "$prev" | awk -v r="$ref" '$2==r{print $1}')
      [ "$sha" = "$old" ] && continue
      case "$ref" in *'^{}') continue;; esac
      range=$([ -n "$old" ] && echo "$old..$sha" || echo "-1 $sha")
      a=$(git log --format='%an|%h %s' $range 2>/dev/null | grep -v '^dev team|' | head -5)
      # routine v5 work is logged, not reported; only main/tags/other branches wake us
      if [ "$ref" = "refs/heads/v5" ]; then [ -n "$a" ] && echo "$(date -Is) $a" >> "$LOG/v5-pushes.log"; continue; fi
      [ -n "$a" ] && out+="${ref#refs/*/}: $(echo "$a" | cut -d'|' -f1 | sort -u | tr '\n' ' ')— $(echo "$a" | cut -d'|' -f2 | head -3 | tr '\n' ';')"$'\n'
    done <<< "$cur"
    prev=$cur
    [ -n "$out" ] && { echo "PUSH by others:"; echo "$out"; exit 0; }
  fi
  # A failed gh call (network) returns nothing: skip this poll instead of reporting every item as changed.
  np=$(prs) || continue; ni=$(iss) || continue
  if [ "$np" != "$pp" ] || [ "$ni" != "$pi" ]; then
    d=$(comm -13 <( (echo "$pp"; echo "$pi") | sort) <( (echo "$np"; echo "$ni") | sort))
    # ignore issue changes made by us only if last comment author is renebd421 (checked cheaply: report anyway, they are rare)
    pp=$np; pi=$ni
    [ -n "$d" ] && { echo "PR/issue change: $d"; exit 0; }
  fi
done
