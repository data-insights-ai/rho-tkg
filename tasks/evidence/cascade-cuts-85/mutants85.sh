#!/bin/bash
# Row 85 mutants of the prepared correction chain: each must fail
# TestCascadeCorrection85_{Oracle,EmptyAndSingle,Shapes}.
cd /home/renework2023/Work/2026/datainsights/rho-tkg/.claude/worktrees/agent-a4246d7a9c854d460 || exit 1
F=pkg/graph/internal/core/temporal_cascade.go
ORIG=/tmp/claude-1000/-home-renework2023-Work-2026-datainsights-rho-tkg/aa649ecc-3385-4943-a9bf-f630b51c9f5c/scratchpad/tc.go.orig
run() {
  name="$1"; from="$2"; to="$3"
  cp "$ORIG" "$F"
  python3 /tmp/claude-1000/-home-renework2023-Work-2026-datainsights-rho-tkg/aa649ecc-3385-4943-a9bf-f630b51c9f5c/scratchpad/subst.py "$F" "$from" "$to" || { echo "NOT APPLIED: $name"; return; }
  if go test -short -run 'TestCascadeCorrection85_(Oracle|EmptyAndSingle|Shapes)' -count=1 ./pkg/graph/internal/core/ >/dev/null 2>&1; then
    echo "SURVIVED: $name"
  else
    echo "killed:   $name"
  fi
}
run "life-end caps (read doors' caps)" 'sweepIntervals(c, k, preChain, nil)' 'sweepIntervals(c, k, preChain, chainLifeEnds(preChain))'
run "scan: void winner kept" '	if isVoidRow(r) {
		return zero, false' '	if false && isVoidRow(r) {
		return zero, false'
run "sweep: void winner kept" '		if !isVoidRow(r) {
			rows[j], oks[j] = r, true' '		if true {
			rows[j], oks[j] = r, true'
run "sweep: end inclusive" 'h[0].e <= t {' 'h[0].e < t {'
run "sweep: start exclusive" 'ivs[next].s <= t {' 'ivs[next].s < t {'
run "scan: equal beliefs keep the later" 'return newerBelief(p.arr, x, y) > 0' 'return newerBelief(p.arr, x, y) >= 0'
run "scan: monotonic lowest position" 'return x.pos > y.pos // the monotonic arm' 'return x.pos < y.pos // the monotonic arm'
run "sweep: no belief rank" '	if p.cascade {
		rankBeliefs(p.arr, ivs)
	}' ''
run "sorted input (lesson 73)" '	p := &correctionChain[T]{c: c, k: k, chain: preChain}' '	p := &correctionChain[T]{c: c, k: k, chain: preChain}
	if len(preChain) > 0 {
		s := slices.Clone(preChain)
		k.sortChain(c, s)
		preChain = s
	}'
run "gap pieces merged with template" 'segs[n-1].base == base && (!splitGaps || segs[n-1].gap == gap)' 'segs[n-1].base == base'
cp "$ORIG" "$F"
