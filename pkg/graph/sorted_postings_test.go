package graph_test

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 72 at the public doors on every backend (tiered and sharded push the
// page into each shard and cut the merge): Nodes().ByLabelAndProperties and
// ByLabelAndProperty walked in keyset pages of random size equal the label
// scan filtered, sorted and cut. On tiered the primary label routes: "Ref"
// to the reference shard, "P" (with "Ref" as an extra label) to the hot event
// shard, so the pages merge two shards.
func TestSortedPostingsPagesAllBackends(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		rng := rand.New(rand.NewSource(11)) // #nosec G404 -- deterministic test input
		if err := g.Index().CreateProperty("Ref", "a"); err != nil {
			t.Fatal(err)
		}
		if err := g.Index().CreateComposite("Ref", []string{"a", "b"}); err != nil {
			t.Fatal(err)
		}
		props := func() map[string]any {
			return map[string]any{"a": int64(rng.Intn(3)), "b": []string{"x", "y"}[rng.Intn(2)]}
		}
		var ids []types.NodeID
		for i := 0; i < 160; i++ {
			labels := []string{"Ref"}
			if rng.Intn(2) == 0 {
				labels = []string{"P", "Ref"}
			}
			n, err := g.Nodes().Add(ctx, labels, props())
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, n.ID())
		}
		for op := 0; op < 200; op++ {
			i := rng.Intn(len(ids))
			if ids[i] == 0 {
				continue
			}
			if rng.Intn(5) == 0 {
				if err := g.Nodes().Delete(ctx, ids[i]); err != nil {
					t.Fatal(err)
				}
				ids[i] = 0
				continue
			}
			if _, err := g.Nodes().Update(ctx, ids[i], props()); err != nil {
				t.Fatal(err)
			}
		}
		all, err := g.Nodes().ByLabel("Ref", storepkg.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		for a := int64(0); a < 3; a++ {
			for _, bv := range []string{"x", "y", ""} {
				values := map[string]any{"a": a, "b": bv}
				if bv == "" {
					values = map[string]any{"a": a}
				}
				var want []types.NodeID
				for _, n := range all {
					ok := true
					for k, v := range values {
						got, _ := n.GetProperty(k)
						ok = ok && got == v
					}
					if ok {
						want = append(want, n.ID())
					}
				}
				slices.Sort(want)
				if len(want) == 0 {
					t.Fatalf("%s: no node holds %v: generator too thin", b.name, values)
				}
				door := func(o storepkg.QueryOpts) []*types.Node {
					var out []*types.Node
					var err error
					if len(values) == 1 {
						out, err = g.Nodes().ByLabelAndProperty("Ref", "a", a, o)
					} else {
						out, err = g.Nodes().ByLabelAndProperties("Ref", values, o)
					}
					if err != nil {
						t.Fatal(err)
					}
					return out
				}
				var walked []types.NodeID
				var o storepkg.QueryOpts
				for {
					o.Limit = 1 + rng.Intn(7)
					page := door(o)
					if len(page) == 0 {
						break
					}
					for _, n := range page {
						walked = append(walked, n.ID())
					}
					o.After = types.EntityID(page[len(page)-1].ID())
				}
				if !slices.Equal(walked, want) {
					t.Fatalf("%s %v: walk %v, want %v", b.name, values, walked, want)
				}
				if got := door(storepkg.QueryOpts{}); fmt.Sprint(nodeIDsOf(got)) != fmt.Sprint(want) {
					t.Fatalf("%s %v: unpaged %v, want %v", b.name, values, nodeIDsOf(got), want)
				}
			}
		}
	})
}
