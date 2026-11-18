package core

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func chainOfNodeRow(version uint32, txFrom, deletedAt types.Instant) *types.Node {
	n := types.NewNode(1, 1, nil)
	n.SetVersion(version)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: txFrom, TxFrom: txFrom, DeletedAt: deletedAt})
	return n
}

// TestChainOfMatchesInlineBuilders: chainOf returns what the doors' former
// inline builders did — a fresh slice of the history rows as stored (never
// reordered, the argument neither changed nor aliased), then the current row
// only when non-nil; preCorrectionChain is that chain in write order (by life,
// then version). The door answers on these shapes are pinned by
// TestChainDoorsCharacterization.
func TestChainOfMatchesInlineBuilders(t *testing.T) {
	t.Parallel()
	v0, v1, v2 := chainOfNodeRow(0, 100, 0), chainOfNodeRow(1, 200, 0), chainOfNodeRow(2, 300, 0)
	dead := chainOfNodeRow(1, 200, 300)
	reborn := chainOfNodeRow(0, 400, 0) // a re-import stored before the re-import allocator: version 0 again
	cases := []struct {
		name    string
		history []*types.Node
		current *types.Node
		want    []*types.Node // chainOf
		wantPre []*types.Node // preCorrectionChain (current non-nil only)
	}{
		{"empty history, nil current", nil, nil, []*types.Node{}, nil},
		{"empty non-nil history, nil current", []*types.Node{}, nil, []*types.Node{}, nil},
		{"history only", []*types.Node{v0, v1}, nil, []*types.Node{v0, v1}, nil},
		{"deleted entity: nil current", []*types.Node{v0, dead}, nil, []*types.Node{v0, dead}, nil},
		{"current only", nil, v0, []*types.Node{v0}, []*types.Node{v0}},
		{"history and current", []*types.Node{v0, v1}, v2, []*types.Node{v0, v1, v2}, []*types.Node{v0, v1, v2}},
		{"version order reversed in", []*types.Node{v2, v0}, v1, []*types.Node{v2, v0, v1}, []*types.Node{v0, v1, v2}},
		{"duplicate version", []*types.Node{v0, v0}, v0, []*types.Node{v0, v0, v0}, []*types.Node{v0, v0, v0}},
		{"re-imported life shares a version", []*types.Node{v0, dead}, reborn, []*types.Node{v0, dead, reborn}, []*types.Node{v0, dead, reborn}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			histCopy := slices.Clone(tc.history)
			got := chainOf(tc.history, tc.current)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("chainOf = %s, want %s", chainNodeVersions(got), chainNodeVersions(tc.want))
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
			if got := preCorrectionChain(tc.history, tc.current); !slices.Equal(got, tc.wantPre) {
				t.Fatalf("preCorrectionChain = %s, want %s", chainNodeVersions(got), chainNodeVersions(tc.wantPre))
			}
		})
	}
	// The relationship instantiation is the same code; one case pins it compiles and keeps nil out.
	r := types.NewRelationship(1, 1, 2, 3)
	if got := chainOf([]*types.Relationship{r}, nil); len(got) != 1 || got[0] != r {
		t.Fatalf("chainOf(rel history, nil) = %v", got)
	}
}
