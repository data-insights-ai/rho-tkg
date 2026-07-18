package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestIterByLabel_EqualsByLabel proves the streaming whole-node door (BACKLOG 3)
// yields EXACTLY the same node set (and order) as the materializing ByLabel, over a
// large label that exercises the bulk-scan substrate, on both backends.
func TestIterByLabel_EqualsByLabel(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		const n = 3000 // > any internal bulk/parallel threshold
		for i := 0; i < n; i++ {
			label := "Ev"
			if i%4 == 0 {
				label = "Other" // interleave a second label
			}
			if _, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"seq": int64(i)}); err != nil {
				t.Fatalf("add %d: %v", i, err)
			}
		}

		want, err := g.Nodes().ByLabel("Ev", store.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabel: %v", err)
		}

		// Callback form.
		var got []types.NodeID
		if err := g.Nodes().ForEachByLabel("Ev", store.QueryOpts{}, func(nd *types.Node) bool {
			got = append(got, nd.ID())
			return true
		}); err != nil {
			t.Fatalf("ForEachByLabel: %v", err)
		}
		assertSameIDs(t, "ForEachByLabel", want, got)

		// iter.Seq2 form.
		var got2 []types.NodeID
		for nd, err := range g.Nodes().IterByLabel(ctx, "Ev", store.QueryOpts{}) {
			if err != nil {
				t.Fatalf("IterByLabel yield err: %v", err)
			}
			got2 = append(got2, nd.ID())
		}
		assertSameIDs(t, "IterByLabel", want, got2)

		// Early stop via iter.Seq2 break.
		seen := 0
		for range g.Nodes().IterByLabel(ctx, "Ev", store.QueryOpts{}) {
			seen++
			if seen == 5 {
				break
			}
		}
		if seen != 5 {
			t.Fatalf("early-stop saw %d, want 5", seen)
		}
	})
}

func assertSameIDs(t *testing.T, who string, want []*types.Node, got []types.NodeID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d nodes, want %d", who, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i].ID() {
			t.Fatalf("%s: order/set mismatch at %d", who, i)
		}
	}
}
