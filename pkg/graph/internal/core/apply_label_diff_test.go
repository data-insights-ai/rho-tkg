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
// one token. A label-token door mutates one token, so a multi-token diff cannot
// come from a correct feed; applying one token silently would leave the label
// index diverged from the stored row. The guard runs before any store write, so
// no partial mutation occurs.
func TestApplyNodeLabelChange_FailsClosedOnMultiTokenDiff(t *testing.T) {
	c := newTxTimeGraph(t)

	local := types.NewNode(types.NodeID(1), 10, nil)          // tokens {10}
	n := types.NewNode(types.NodeID(1), 10, []uint16{20, 30}) // tokens {10,20,30}
	added, removed, addedN, removedN := labelTokenDiff(local, n)
	if addedN != 2 || removedN != 0 {
		t.Fatalf("setup: diff = (addedN=%d, removedN=%d), want (2, 0)", addedN, removedN)
	}

	err := c.applyNodeLabelChangeLocked(local, n, added, removed, addedN, removedN, false)
	if err == nil {
		t.Fatal("applyNodeLabelChangeLocked silently accepted a 2-token label diff — must fail closed")
	}
	if !strings.Contains(err.Error(), "single-token mutation") {
		t.Fatalf("error = %q, want it to name the single-token-mutation contract", err)
	}

	// Symmetric add+remove must also fail closed (the case the old guard caught).
	localAR := types.NewNode(types.NodeID(2), 10, []uint16{20})
	nAR := types.NewNode(types.NodeID(2), 10, []uint16{30})
	a, r, aN, rN := labelTokenDiff(localAR, nAR)
	if aN != 1 || rN != 1 {
		t.Fatalf("setup add+remove: (addedN=%d, removedN=%d), want (1, 1)", aN, rN)
	}
	if err := c.applyNodeLabelChangeLocked(localAR, nAR, a, r, aN, rN, false); err == nil {
		t.Fatal("applyNodeLabelChangeLocked accepted a simultaneous add+remove — must fail closed")
	}
}
