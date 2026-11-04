#!/usr/bin/env bash
# Patch gate (make ci-fast): every step at once, one PASS/FAIL line each with
# its seconds, red if any step is red. The tests are patchgate's plan: only the
# tests that reach the code changed since the last release tag, by name, under
# -race, one step per package.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 2

base=$(git tag --sort=-creatordate | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1)
log=$(git rev-parse --git-dir)/ci-fast # step logs of the last run, overwritten
mkdir -p "$log"

plan=$(go run ./scripts/patchgate "$base" 2>"$log/plan.err") || {
	cat "$log/plan.err" >&2
	echo "FAIL plan ($base..work tree)"
	exit 1
}

pids=() names=()
step() { # step <name> <command...>
	local name=$1 out
	out=$log/${1//\//_}.out
	shift
	(
		start=$SECONDS
		if "$@" >"$out" 2>&1; then
			echo "PASS $name $((SECONDS - start)) s"
		else
			echo "FAIL $name $((SECONDS - start)) s"
			tail -40 "$out"
			exit 1
		fi
	) &
	pids+=($!) names+=("$name")
}

step fmt make --no-print-directory fmt-check
step vet go vet ./...
step build go build ./...
step lint make --no-print-directory lint-docker
step docs go test -count=1 -run 'Docs|Consistency|MD|Errors' ./...
while read -r kind pkg run; do
	[ "$kind" = race ] || continue
	names_re=${run#^(}
	step "race:${pkg#./}" scripts/test-named.sh "$pkg" "${names_re%)\$}" -- -short -race
done <<<"$plan"

red=0
for p in "${pids[@]}"; do
	wait "$p" || red=1
done
[ "$red" = 0 ] && scripts/check-ledger.sh >"$log/ledger.out" 2>&1 || {
	[ "$red" = 0 ] && { echo "FAIL ledger"; cat "$log/ledger.out"; }
	echo "ci-fast: red ($base..work tree, load $(cut -d' ' -f1 /proc/loadavg))"
	exit 1
}
echo "ci-fast: ok ($base..work tree, load $(cut -d' ' -f1 /proc/loadavg))"
