package core

import (
	"fmt"
	"testing"
)

// TestAnchorSafeTrim_Table pins the compaction keep rule of backlog 24 on
// hand-built chains (hist ascending by version; hash "hN", PrevHash names the
// row a version links to). Each case names the faulty planner it catches.
func TestAnchorSafeTrim_Table(t *testing.T) {
	t.Parallel()
	h := func(v uint32, prev string) versionInfo {
		return versionInfo{version: v, hash: fmt.Sprintf("h%d", v), prevHash: prev}
	}
	cur := func(v uint32, prev string) *versionInfo {
		x := h(v, prev)
		return &x
	}
	tests := []struct {
		name    string
		hist    []versionInfo
		current *versionInfo
		trim    int
		want    int
	}{
		{
			// Catches a planner that keeps more than needed on a plain chain.
			name:    "plain update chain: the policy's trim stands",
			hist:    []versionInfo{h(0, ""), h(1, "h0"), h(2, "h1"), h(3, "h2"), h(4, "h3")},
			current: cur(5, "h4"),
			trim:    4, want: 4,
		},
		{
			// Bounded cascade after two updates: piece v3 and resumption v4 link
			// to their base v2, the current v5 to v4. Catches a version count
			// that trims v2 (the chain stops verifying) and a planner that gives
			// up (0) although v0 and v1 can go.
			name:    "cascade rows link to an older base: keep the base, trim below it",
			hist:    []versionInfo{h(0, ""), h(1, "h0"), h(2, "h1"), h(3, "h2"), h(4, "h2")},
			current: cur(5, "h4"),
			trim:    4, want: 2,
		},
		{
			name:    "every row is a base of a kept row: nothing can go",
			hist:    []versionInfo{h(0, ""), h(1, "h0"), h(2, "h0"), h(3, "h1")},
			current: cur(4, "h2"),
			trim:    3, want: 0,
		},
		{
			// A chain that did not verify before compaction (a dangling
			// PrevHash, e.g. rows without integrity data or a re-import next to
			// an old stub). Catches a planner that then never trims again: the
			// policy's trim stands, as before backlog 24.
			name:    "unverifiable before: the policy's trim stands",
			hist:    []versionInfo{h(0, ""), h(1, "h0"), h(2, "x")},
			current: cur(3, "h2"),
			trim:    2, want: 2,
		},
		{
			name:    "no trim planned",
			hist:    []versionInfo{h(0, ""), h(1, "h0")},
			current: cur(2, "h1"),
			trim:    0, want: 0,
		},
		{
			// Deleted entity: no current row; the tombstone links to v1.
			name: "no current row",
			hist: []versionInfo{h(0, ""), h(1, "h0"), h(2, "h1")},
			trim: 1, want: 1,
		},
	}
	for _, tc := range tests {
		if got := anchorSafeTrim(tc.hist, tc.current, tc.trim); got != tc.want {
			t.Errorf("%s: anchorSafeTrim = %d; want %d", tc.name, got, tc.want)
		}
	}
}
