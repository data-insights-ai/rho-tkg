package core

import (
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// labelTokenDiff must report the COUNT of added/removed tokens, not just one
// arbitrary token — the apply path relies on the counts to reject a multi-token
// diff (a record a correct contiguous feed never produces) instead of silently
// applying one token and diverging the label index.
func TestLabelTokenDiff_Counts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		localExtra, nExtra   []uint16 // both share primary token 10
		wantAddedN, wantRemN int
	}{
		{"no change", nil, nil, 0, 0},
		{"single add", nil, []uint16{20}, 1, 0},
		{"single remove", []uint16{20}, nil, 0, 1},
		{"add and remove", []uint16{20}, []uint16{30}, 1, 1},
		{"two added", nil, []uint16{20, 30}, 2, 0},
		{"two removed", []uint16{20, 30}, nil, 0, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			local := types.NewNode(types.NodeID(1), 10, tc.localExtra)
			n := types.NewNode(types.NodeID(1), 10, tc.nExtra)
			_, _, addedN, removedN := labelTokenDiff(local, n)
			if addedN != tc.wantAddedN || removedN != tc.wantRemN {
				t.Fatalf("labelTokenDiff = (addedN=%d, removedN=%d), want (%d, %d)", addedN, removedN, tc.wantAddedN, tc.wantRemN)
			}
		})
	}
}

// applyNodeLabelChangeLocked must FAIL CLOSED on a label diff that is not exactly
// one token: two added, two removed, or one added and one removed. A label-token
// door mutates one token, so a multi-token diff cannot come from a correct feed;
// applying one token silently would leave the label index diverged from the
// stored row. The guard runs before any store write, so no partial mutation
// occurs. The diff counts themselves are pinned by TestLabelTokenDiff_Counts.
func TestApplyNodeLabelChange_FailsClosedOnMultiTokenDiff(t *testing.T) {
	t.Parallel()
	c := newTxTimeGraph(t)
	diffs := []struct {
		name               string
		localExtra, nExtra []uint16 // both share primary token 10
	}{
		{"two added", nil, []uint16{20, 30}},
		{"two removed", []uint16{20, 30}, nil},
		{"add and remove", []uint16{20}, []uint16{30}},
	}
	for i, tc := range diffs {
		id := types.NodeID(i + 1)
		local := types.NewNode(id, 10, tc.localExtra)
		n := types.NewNode(id, 10, tc.nExtra)
		added, removed, addedN, removedN := labelTokenDiff(local, n)
		err := c.applyNodeLabelChangeLocked(local, n, added, removed, addedN, removedN, false)
		if err == nil {
			t.Fatalf("%s: applyNodeLabelChangeLocked accepted a multi-token label diff — must fail closed", tc.name)
		}
		if !strings.Contains(err.Error(), "single-token mutation") {
			t.Fatalf("%s: error = %q, want it to name the single-token-mutation contract", tc.name, err)
		}
	}
}
