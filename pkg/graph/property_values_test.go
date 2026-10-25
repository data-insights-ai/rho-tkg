package graph_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nodeIDList is the IDs of nodes, in order.
func nodeIDList(nodes []*types.Node) []types.NodeID {
	out := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		out[i] = n.ID()
	}
	return out
}

// TestByLabelAndPropertyValuesAllBackends pins VALUES (sigma, 24 Oct): one
// lookup by many values of one property answers the union of the
// single-value lookups (ByLabelAndProperty per value), each node once, in ID
// order, paged over the union by opts.After / opts.Limit, with or without a
// property index, under a valid-time filter too, on every backend. Equal
// values (1, 1.0, int8(1)) and repeats count once; no values and values no
// node has answer nothing.
func TestByLabelAndPropertyValuesAllBackends(t *testing.T) {
	t.Parallel()
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		vals := []any{int64(1), 1.0, "a", "b", int64(2), true}
		for i := 0; i < 36; i++ {
			props := map[string]any{"v": vals[i%len(vals)], "tkg_valid_from": types.Instant(1000 + i)}
			if _, err := g.Nodes().Add(ctx, []string{"Item"}, props); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"Other"}, map[string]any{"v": "a"}); err != nil {
				t.Fatal(err)
			}
		}
		for _, indexed := range []bool{false, true} {
			if indexed {
				if err := g.Index().CreateProperty("Item", "v"); err != nil {
					t.Fatal(err)
				}
			}
			for _, opts := range []storepkg.QueryOpts{{}, {ValidAt: 1020, ValidAtSet: true}} {
				for _, values := range [][]any{
					{int64(1), "a"},
					{1.0, int8(1), "a", "a", "zz"},
					{"b", true, int64(2)},
					{"zz"},
					{},
				} {
					name := fmt.Sprintf("indexed=%v validAt=%d values=%v", indexed, opts.ValidAt, values)
					var want []types.NodeID
					for _, v := range values {
						got, err := g.Nodes().ByLabelAndProperty("Item", "v", v, opts)
						if err != nil {
							t.Fatalf("%s: ByLabelAndProperty(%v): %v", name, v, err)
						}
						want = append(want, nodeIDList(got)...)
					}
					slices.Sort(want)
					want = slices.Compact(want)
					got, err := g.Nodes().ByLabelAndPropertyValues("Item", "v", values, opts)
					if err != nil {
						t.Fatalf("%s: ByLabelAndPropertyValues: %v", name, err)
					}
					if ids := nodeIDList(got); !slices.Equal(ids, want) {
						t.Fatalf("%s: %d nodes %v, want the union %d %v", name, len(ids), ids, len(want), want)
					}
					var paged []types.NodeID
					page := opts
					page.Limit = 4
					for {
						got, err := g.Nodes().ByLabelAndPropertyValues("Item", "v", values, page)
						if err != nil {
							t.Fatalf("%s: page: %v", name, err)
						}
						if len(got) == 0 {
							break
						}
						paged = append(paged, nodeIDList(got)...)
						page.After = types.EntityID(got[len(got)-1].ID())
					}
					if !slices.Equal(paged, want) {
						t.Fatalf("%s: pages %v, want %v", name, paged, want)
					}
				}
			}
		}
		if _, err := g.Nodes().ByLabelAndPropertyValues("Item", "v", []any{struct{ X int }{1}}, storepkg.QueryOpts{}); err == nil {
			t.Fatalf("a value that is not a property value was accepted")
		}
		if got, err := g.Nodes().ByLabelAndPropertyValues("Nope", "v", []any{"a"}, storepkg.QueryOpts{}); err != nil || len(got) != 0 {
			t.Fatalf("unknown label: %d nodes (%v), want none", len(got), err)
		}
	})
}
