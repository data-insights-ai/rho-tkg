package graph_test

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// numericValue is a property value as the range doors compare it, ok=false
// for a non-numeric value (a string never matches a numeric range).
func numericValue(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, !math.IsNaN(x)
	}
	return 0, false
}

func inRange(v, lo, hi float64, inclLo, inclHi bool) bool {
	return ((inclLo && v >= lo) || (!inclLo && v > lo)) && ((inclHi && v <= hi) || (!inclHi && v < hi))
}

// Nodes().RangeCardinality and Rels().RangeCardinality equal a count by
// iterating every member, on every backend that answers (memory, badger,
// sharded — sharded relationships since round 4); tiered answers neither
// (exact=false, count 0). The data spans several chunks of the ordered view
// (about 3,000 distinct values), mixes integers, fractions, strings, NaN and
// missing values, and is checked again after updates and deletes moved and
// drained values.
func TestRangeCardinalityMatchesIteration(t *testing.T) {
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(0x4242)) //nolint:gosec // deterministic test
			if err := g.Index().CreateProperty("P", "v"); err != nil && b.name != "tiered" {
				t.Fatal(err)
			}
			relIndexed := g.Index().CreateRelProperty("R", "v") == nil
			value := func() any {
				switch rng.Intn(10) {
				case 0:
					return "s"
				case 1:
					return math.NaN()
				case 2:
					return float64(rng.Intn(3000)) + 0.5
				default:
					return int64(rng.Intn(3000) - 500)
				}
			}
			var nodes []*types.Node
			var rels []types.RelID
			for i := range 4000 {
				props := map[string]any{}
				if i%17 != 0 {
					props["v"] = value()
				}
				n, err := g.Nodes().Add(ctx, []string{"P"}, props)
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n)
				if i > 0 && i%4 == 0 {
					r, err := g.Rels().Add(ctx, "R", nodes[i-1], n, map[string]any{"v": value()})
					if err != nil {
						t.Fatal(err)
					}
					rels = append(rels, r.InternalID())
				}
			}
			check := func(step string) {
				t.Helper()
				var nodeVals, relVals []float64
				if err := g.Nodes().ForEachByLabel("P", graphpkg.QueryOpts{}, func(n *types.Node) bool {
					if v, ok := n.GetProperty("v"); ok {
						if f, ok := numericValue(v); ok {
							nodeVals = append(nodeVals, f)
						}
					}
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if err := g.Rels().ForEachByType("R", graphpkg.QueryOpts{}, func(r *types.Relationship) bool {
					if v, ok := r.GetProperty("v"); ok {
						if f, ok := numericValue(v); ok {
							relVals = append(relVals, f)
						}
					}
					return true
				}); err != nil {
					t.Fatal(err)
				}
				bounds := []float64{math.Inf(-1), math.Inf(1), -600, 0, 0.5, 100, 1499.5, 2999, 3500}
				for range 30 {
					bounds = append(bounds, float64(rng.Intn(3200)-550), float64(rng.Intn(3000))+0.5)
				}
				for range 200 {
					lo, hi := bounds[rng.Intn(len(bounds))], bounds[rng.Intn(len(bounds))]
					il, ih := rng.Intn(2) == 0, rng.Intn(2) == 0
					for _, side := range []struct {
						name    string
						vals    []float64
						count   func() (int64, bool, error)
						answers bool
					}{
						{"nodes", nodeVals, func() (int64, bool, error) {
							return g.Nodes().RangeCardinality("P", "v", lo, hi, il, ih, graphpkg.QueryOpts{})
						}, b.name != "tiered"},
						{"rels", relVals, func() (int64, bool, error) {
							return g.Rels().RangeCardinality("R", "v", lo, hi, il, ih, graphpkg.QueryOpts{})
						}, relIndexed && b.name != "tiered"},
					} {
						got, exact, err := side.count()
						if err != nil {
							t.Fatal(err)
						}
						if !side.answers {
							if exact || got != 0 {
								t.Fatalf("%s %s: a backend without the count answered %d, exact=%v", step, side.name, got, exact)
							}
							continue
						}
						if !exact {
							t.Fatalf("%s %s: [%v,%v] declined", step, side.name, lo, hi)
						}
						var want int64
						for _, v := range side.vals {
							if inRange(v, lo, hi, il, ih) {
								want++
							}
						}
						if got != want {
							t.Fatalf("%s %s: [%v,%v] incl(%v,%v) = %d, iteration counts %d", step, side.name, lo, hi, il, ih, got, want)
						}
					}
				}
			}
			check("loaded")
			for i, n := range nodes {
				switch i % 5 {
				case 0:
					if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
						t.Fatal(err)
					}
				case 1:
					if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": value()}); err != nil {
						t.Fatal(err)
					}
				}
			}
			for i, id := range rels {
				if i%3 == 0 {
					if _, err := g.Rels().Update(ctx, id, map[string]any{"v": value()}); err != nil && !isGone(err) {
						t.Fatal(err)
					}
				}
			}
			check("after updates and deletes")
		})
	}
}

func isGone(err error) bool {
	return err != nil && (errors.Is(err, graphpkg.ErrRelNotFound) || errors.Is(err, graphpkg.ErrEntityDeleted))
}
