package core

import (
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// SYSTEM PROPERTY: labelTokenDiff must report the COUNT of removed tokens, not a
// single arbitrary token. The 2-removed count is the mirror of the 2-added count;
// the apply guard relies on removedN to reject a multi-remove diff a correct
// contiguous feed never emits.
func TestLabelTokenDiff_TwoRemovedCount(t *testing.T) {
	local := types.NewNode(types.NodeID(3), 10, []uint16{20, 30}) // tokens {10,20,30}
	n := types.NewNode(types.NodeID(3), 10, nil)                  // tokens {10}
	added, removed, addedN, removedN := labelTokenDiff(local, n)
	if addedN != 0 || removedN != 2 {
		t.Fatalf("labelTokenDiff = (added=%d, removed=%d, addedN=%d, removedN=%d), want addedN=0, removedN=2",
			added, removed, addedN, removedN)
	}
}

// SYSTEM PROPERTY: applyNodeLabelChangeLocked must FAIL CLOSED on a diff that
// removes TWO tokens — the symmetric half of the 2-added bug the change fixed.
// A label-token door mutates exactly one token, so a 2-removed diff cannot come
// from a correct feed; applying one remove silently would diverge the label index
// from the stored row. The guard runs before any store write, so no partial
// mutation occurs (we do NOT assert via GetNode — the row is never persisted).
func TestApplyNodeLabelChange_FailsClosedOn2Removed(t *testing.T) {
	c := newTxTimeGraph(t)

	local := types.NewNode(types.NodeID(3), 10, []uint16{20, 30}) // tokens {10,20,30}
	n := types.NewNode(types.NodeID(3), 10, nil)                  // tokens {10}
	added, removed, addedN, removedN := labelTokenDiff(local, n)
	if addedN != 0 || removedN != 2 {
		t.Fatalf("setup: diff = (addedN=%d, removedN=%d), want (0, 2)", addedN, removedN)
	}

	err := c.applyNodeLabelChangeLocked(local, n, added, removed, addedN, removedN, false)
	if err == nil {
		t.Fatal("applyNodeLabelChangeLocked silently accepted a 2-token remove diff — must fail closed (would apply only one remove and diverge the label index)")
	}
	if !strings.Contains(err.Error(), "single-token mutation") {
		t.Fatalf("error = %q, want it to name the single-token-mutation contract", err)
	}
}
