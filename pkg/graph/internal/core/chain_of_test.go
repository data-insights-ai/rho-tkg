package core

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// inlineNodeChain is the "history + current if non-nil" builder the doors
// carried inline before WP-21 (temporal.go, temporal_queries.go,
// effective_timeline.go, migration_bitemporal.go, range_any_version.go).
func inlineNodeChain(history []*types.Node, current *types.Node) []*types.Node {
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}
	return chain
}

func chainOfNodeRow(version uint32, txFrom, deletedAt types.Instant) *types.Node {
	n := types.NewNode(1, 1, nil)
	n.SetVersion(version)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: txFrom, TxFrom: txFrom, DeletedAt: deletedAt})
	return n
}

// TestChainOfMatchesInlineBuilders: chainOf is the inline builder (same rows,
// same order, never reordered, current only when non-nil), and
// preCorrectionChain is chainOf in version order.
func TestChainOfMatchesInlineBuilders(t *testing.T) {
	t.Parallel()
	v0, v1, v2 := chainOfNodeRow(0, 100, 0), chainOfNodeRow(1, 200, 0), chainOfNodeRow(2, 300, 0)
	dead := chainOfNodeRow(1, 200, 300)
	reborn := chainOfNodeRow(0, 400, 0) // a re-import before backlog 38: version 0 again
	cases := []struct {
		name    string
		history []*types.Node
		current *types.Node
	}{
		{"empty history, nil current", nil, nil},
		{"empty non-nil history, nil current", []*types.Node{}, nil},
		{"history only", []*types.Node{v0, v1}, nil},
		{"deleted entity: nil current", []*types.Node{v0, dead}, nil},
		{"current only", nil, v0},
		{"history and current", []*types.Node{v0, v1}, v2},
		{"version order reversed in", []*types.Node{v2, v0}, v1},
		{"duplicate version", []*types.Node{v0, v0}, v0},
		{"re-imported life shares a version", []*types.Node{v0, dead}, reborn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			histCopy := slices.Clone(tc.history)
			got := chainOf(tc.history, tc.current)
			want := inlineNodeChain(tc.history, tc.current)
			if !slices.Equal(got, want) {
				t.Fatalf("chainOf = %s, inline = %s", chainNodeVersions(got), chainNodeVersions(want))
			}
			if !slices.Equal(tc.history, histCopy) {
				t.Fatalf("chainOf changed its history argument")
			}
			if len(got) > 0 && len(tc.history) > 0 && &got[0] == &tc.history[0] {
				t.Fatalf("chainOf aliases its history argument")
			}
			if tc.current == nil {
				return
			}
			if got, want := preCorrectionChain(tc.history, tc.current), versionOrdered(want); !slices.Equal(got, want) {
				t.Fatalf("preCorrectionChain = %s, want %s", chainNodeVersions(got), chainNodeVersions(want))
			}
		})
	}
	// The relationship instantiation is the same code; one case pins it compiles and keeps nil out.
	r := types.NewRelationship(1, 1, 2, 3)
	if got := chainOf([]*types.Relationship{r}, nil); len(got) != 1 || got[0] != r {
		t.Fatalf("chainOf(rel history, nil) = %v", got)
	}
}
