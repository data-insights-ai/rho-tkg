#!/usr/bin/env bash
# release.sh [--dry-run] — rho's one release process (`make release`), René 4 Nov:
#   1 preconditions: on main, clean (nothing untracked), equal to origin/main;
#   2 kind from the diff since the last tag (scripts/patchgate -kind): a patch,
#     or a minor (a new exported name, internal packages included; a store
#     format marker; a badger minor bump);
#   3 docs/CHANGELOG.md's newest heading is the next version (written by hand,
#     from the finished todo items, in the commit before the release);
#   4 the gate: `make ci-fast` for a patch, `make ci-docker` for a minor;
#   5 `git tag -a` (message: that CHANGELOG section), `git push --atomic`.
# The tag is the version: the script edits no file. One PASS/FAIL line per step
# with its seconds; it stops at the first red. --dry-run: everything but the
# preconditions' refusal, the tag and the push.
set -uo pipefail

dry=
for a in "$@"; do
	case $a in
	--dry-run) dry=1 ;;
	*)
		echo "usage: scripts/release.sh [--dry-run]" >&2
		exit 2
		;;
	esac
done
cd "$(git rev-parse --show-toplevel)" || exit 2
log=$(git rev-parse --git-dir)/release.log # the last step's output, overwritten

t_all=$SECONDS
step() { # step <name> <command...>
	local name=$1 t=$SECONDS
	shift
	if "$@" >"$log" 2>&1; then
		printf 'PASS %-13s %4s s\n' "$name" $((SECONDS - t))
	else
		printf 'FAIL %-13s %4s s\n' "$name" $((SECONDS - t))
		tail -n 40 "$log"
		echo "release.sh: red at $name, $((SECONDS - t_all)) s"
		exit 1
	fi
}

tag=$(git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*') || exit 2
IFS=. read -r X Y Z <<<"${tag#v}"

pre() {
	local st=0
	[ "$(git branch --show-current)" = main ] || { echo "not on main"; st=1; }
	[ -z "$(git status --porcelain)" ] || { echo "tree not clean"; st=1; }
	git fetch -q origin main && [ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] ||
		{ echo "HEAD differs from origin/main"; st=1; }
	return $st
}
if [ -n "$dry" ]; then
	pre || echo "release.sh: preconditions not met (dry-run goes on)"
else
	step preconditions pre
fi

kind=$(go run ./scripts/patchgate -kind "$tag") || { echo "FAIL kind"; exit 1; }
if [ "$(head -1 <<<"$kind")" = minor ]; then
	next=v$X.$((Y + 1)).0 gate=ci-docker
else
	next=v$X.$Y.$((Z + 1)) gate=ci-fast
fi
echo "release.sh: $tag -> $next ($kind)" | head -6

# The newest "## [x.y.z]" section of docs/CHANGELOG.md, without its heading.
section() { awk '/^## \[[0-9]/{ if (seen++) exit; next } seen' docs/CHANGELOG.md; }
changelog() {
	local top
	top=$(grep -oE '^## \[[0-9]+\.[0-9]+\.[0-9]+\]' docs/CHANGELOG.md | head -1 | tr -d '#[] ')
	[ "v$top" = "$next" ] || { echo "docs/CHANGELOG.md's newest heading is [$top]; write [${next#v}] first"; return 1; }
	[ -n "$(section | tr -d '[:space:]')" ] || { echo "docs/CHANGELOG.md [${next#v}] is empty"; return 1; }
}
step changelog changelog
step "$gate" make --no-print-directory "$gate"

if [ -n "$dry" ]; then
	echo "release.sh: dry-run, $next not tagged, $((SECONDS - t_all)) s"
	exit 0
fi
step tag git tag -a "$next" -m "$next" -m "$(section)"
step push git push --atomic origin main "refs/tags/$next"
echo "release.sh: $next, $gate, $((SECONDS - t_all)) s"
