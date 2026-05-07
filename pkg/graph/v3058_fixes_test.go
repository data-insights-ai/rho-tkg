package graph

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Note: the temporalIndex lazy-sort regression tests (Fix G) now live
// alongside the implementation in pkg/graph/internal/index/temporal_index_test.go.
// This file retains the unrelated Node-label tests originally bundled here.

// --- Fix H: RemoveLabelTokenRaw nil extraLabels ---

// TestRemoveLabelTokenRaw_ExtraLabelsNilAfterLastRemoval verifies that after all
// extra labels are removed, ExtraLabelTokens() returns nil (not an empty slice).
func TestRemoveLabelTokenRaw_ExtraLabelsNilAfterLastRemoval(t *testing.T) {
	t.Parallel()

	const primary uint16 = 1
	const extra1 uint16 = 2
	const extra2 uint16 = 3

	t.Run("remove_only_extra", func(t *testing.T) {
		t.Parallel()
		n := types.NewNode(types.NodeID(snowflake.ID(1)), primary, []uint16{extra1})
		// Sanity: one extra label present.
		if extras := n.ExtraLabelTokens(); len(extras) != 1 {
			t.Fatalf("pre-condition: ExtraLabelTokens = %v, want [2]", extras)
		}
		ok := n.RemoveLabelTokenRaw(extra1)
		if !ok {
			t.Fatal("RemoveLabelTokenRaw returned false, want true")
		}
		// After removal, ExtraLabelTokens must return nil.
		if extras := n.ExtraLabelTokens(); extras != nil {
			t.Errorf("ExtraLabelTokens = %v (len=%d), want nil", extras, len(extras))
		}
	})

	t.Run("promote_only_extra_to_primary", func(t *testing.T) {
		t.Parallel()
		// Node with primary=1, extra=2. Remove primary → extra2 becomes primary.
		n := types.NewNode(types.NodeID(snowflake.ID(2)), primary, []uint16{extra1})
		ok := n.RemoveLabelTokenRaw(primary)
		if !ok {
			t.Fatal("RemoveLabelTokenRaw(primary) returned false, want true")
		}
		// primary is now extra1; extraLabels must be nil.
		if uint16(n.PrimaryLabelToken()) != extra1 {
			t.Errorf("primary = %d, want %d", n.PrimaryLabelToken(), extra1)
		}
		if extras := n.ExtraLabelTokens(); extras != nil {
			t.Errorf("ExtraLabelTokens = %v, want nil after promotion", extras)
		}
	})

	t.Run("remove_one_of_two_extras_leaves_non_nil", func(t *testing.T) {
		t.Parallel()
		n := types.NewNode(types.NodeID(snowflake.ID(3)), primary, []uint16{extra1, extra2})
		ok := n.RemoveLabelTokenRaw(extra1)
		if !ok {
			t.Fatal("RemoveLabelTokenRaw returned false")
		}
		// One extra remains — slice must NOT be nil.
		extras := n.ExtraLabelTokens()
		if extras == nil {
			t.Fatal("ExtraLabelTokens = nil, want non-nil (one extra remains)")
		}
		if len(extras) != 1 || uint16(extras[0]) != extra2 {
			t.Errorf("ExtraLabelTokens = %v, want [%d]", extras, extra2)
		}
	})
}
