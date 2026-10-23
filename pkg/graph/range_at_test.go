package graph_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// rangeAtFold is the temporal range door's definition: ByLabel's (ByType's)
// versions under opts whose value, as a float64 (a float32 widened), is not
// outside [min, max] (NaN kept: the door over-selects, fn re-checks).
func rangeAtFold(v any, min, max float64) bool {
	var f float64
	switch x := v.(type) {
	case int64:
		f = float64(x)
	case int32:
		f = float64(x)
	case float32:
		f = float64(x)
	case float64:
		f = x
	default:
		return false
	}
	return !(f < min || f > max)
}

func rangeAtCount(v any, min, max float64, inclMin, inclMax bool) bool {
	if !rangeAtFold(v, min, max) {
		return false
	}
	var f float64
	switch x := v.(type) {
	case int64:
		f = float64(x)
	case int32:
		f = float64(x)
	case float32:
		f = float64(x)
	case float64:
		f = x
	}
	if math.IsNaN(f) || (f == min && !inclMin) || (f == max && !inclMax) {
		return false
	}
	return true
}

// TestRangeAtFromSidecar (sigma-tkgd round 8 request 2): under every temporal
// coordinate — all valid time as recorded at a pin (ValidStart 1, ValidEnd
// MaxInt64, TxAt), TxPin, ValidAt with and without TxAt, a valid-time window —
// Nodes().ForEachByLabelPropertyRange / Rels().ForEachByTypePropertyRange
// return exactly ByLabel's / ByType's versions whose value is in range (the
// fold), in ID order, through random creates, value updates with and without
// a new valid-from, deletes and label changes; RangeCardinalityAt at the
// coordinate counts them exactly on memory and badger (served from the
// declared index's membership sidecar) and declines on tiered and sharded.
func TestRangeAtFromSidecar(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		label := "Ref" // tiered keeps property indexes on its reference labels
		if err := g.Index().CreateProperty(label, "v"); err != nil {
			t.Fatal(err)
		}
		relIndexed := g.Index().CreateRelProperty("R", "w") == nil
		served := b.name == "memory" || b.name == "badger"
		rng := rand.New(rand.NewPCG(8, 2))
		values := []any{int64(5), int64(10), 10.0, 12.5, float32(0.1), 0.1, int32(-3), "s", math.NaN(), int64(40), math.Inf(1)}
		pick := func() any { return values[rng.IntN(len(values))] }

		var nodes []*types.Node
		var rels []*types.Relationship
		var pins []types.Instant
		pin := func(n *types.Node) {
			from, _, _ := n.TxStamps()
			pins = append(pins, from)
		}
		hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 30 {
			props := map[string]any{"v": pick(), "tkg_valid_from": types.Instant(1000 + 100*i)}
			n, err := g.Nodes().Add(ctx, []string{label}, props)
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
			pin(n)
			r, err := g.Rels().Add(ctx, "R", hub, n, map[string]any{"w": pick(), "tkg_valid_from": types.Instant(1000 + 100*i)})
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r)
		}
		// Phase 1: value updates with explicit, rising valid-from instants
		// (a version stamped "now" would refuse a later explicit one).
		for step := range 90 {
			i := rng.IntN(len(nodes))
			if step%3 == 2 {
				if _, err := g.Rels().Update(ctx, rels[i].ID(), map[string]any{"w": pick(), "tkg_valid_from": types.Instant(5000 + 10*step)}); err != nil {
					t.Fatal(err)
				}
				continue
			}
			n, err := g.Nodes().Update(ctx, nodes[i].ID(), map[string]any{"v": pick(), "tkg_valid_from": types.Instant(5000 + 10*step)})
			if err != nil {
				t.Fatal(err)
			}
			pin(n)
		}
		verify := func(stage string) {
			t.Helper()
			end, err := g.Nodes().Add(ctx, []string{"Other"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			pin(end)

			var coords []storepkg.QueryOpts
			for _, p := range []types.Instant{pins[5], pins[len(pins)/2], pins[len(pins)-1]} {
				coords = append(coords,
					storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: p},
					storepkg.QueryOpts{TxPin: p},
					storepkg.QueryOpts{ValidAt: 2000, TxAt: p})
			}
			coords = append(coords, storepkg.QueryOpts{ValidAt: 6000}, storepkg.QueryOpts{ValidStart: 1500, ValidEnd: 5200},
				storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pins[len(pins)-1], Limit: 3, After: types.EntityID(nodes[4].ID())})
			ranges := [][2]float64{{0, 50}, {math.Inf(-1), 10}, {10, 10}, {0.1, 0.1}, {0.1000000001, 1}, {12.5, math.Inf(1)}, {70, 80}, {-9, -8}, {50, 0}}

			for ci, opts := range coords {
				wantNodes, err := g.Nodes().ByLabel(label, storepkg.QueryOpts{ValidAt: opts.ValidAt, ValidStart: opts.ValidStart, ValidEnd: opts.ValidEnd, TxAt: opts.TxAt, TxPin: opts.TxPin, After: opts.After})
				if err != nil {
					t.Fatal(err)
				}
				wantRels, err := g.Rels().ByType("R", storepkg.QueryOpts{ValidAt: opts.ValidAt, ValidStart: opts.ValidStart, ValidEnd: opts.ValidEnd, TxAt: opts.TxAt, TxPin: opts.TxPin, After: opts.After})
				if err != nil {
					t.Fatal(err)
				}
				for _, rg := range ranges {
					name := fmt.Sprintf("%s coord %d %+v range %v", stage, ci, opts, rg)
					var want, got []string
					for _, n := range wantNodes {
						if v, ok := n.GetProperty("v"); ok && rangeAtFold(v, rg[0], rg[1]) && (opts.Limit == 0 || len(want) < opts.Limit) {
							want = append(want, fmt.Sprintf("%d=%v@%d", n.ID(), v, n.Version()))
						}
					}
					if err := g.Nodes().ForEachByLabelPropertyRange(label, "v", rg[0], rg[1], true, true, opts, func(n *types.Node) bool {
						v, _ := n.GetProperty("v")
						got = append(got, fmt.Sprintf("%d=%v@%d", n.ID(), v, n.Version()))
						return true
					}); err != nil {
						t.Fatalf("%s: %v", name, err)
					}
					if !slices.Equal(got, want) {
						t.Fatalf("%s nodes:\n got  %v\n want %v", name, got, want)
					}
					var wantR, gotR []string
					for _, r := range wantRels {
						if v, ok := r.GetProperty("w"); ok && rangeAtFold(v, rg[0], rg[1]) && (opts.Limit == 0 || len(wantR) < opts.Limit) {
							wantR = append(wantR, fmt.Sprintf("%d=%v@%d", r.ID(), v, r.Version()))
						}
					}
					if err := g.Rels().ForEachByTypePropertyRange("R", "w", rg[0], rg[1], true, true, opts, func(r *types.Relationship) bool {
						v, _ := r.GetProperty("w")
						gotR = append(gotR, fmt.Sprintf("%d=%v@%d", r.ID(), v, r.Version()))
						return true
					}); err != nil {
						t.Fatalf("%s rels: %v", name, err)
					}
					if !slices.Equal(gotR, wantR) {
						t.Fatalf("%s rels:\n got  %v\n want %v", name, gotR, wantR)
					}
					if opts.Limit != 0 || opts.After != 0 {
						continue
					}
					for _, incl := range [][2]bool{{true, true}, {false, true}, {true, false}} {
						var wantC, wantRC int64
						for _, n := range wantNodes {
							if v, ok := n.GetProperty("v"); ok && rangeAtCount(v, rg[0], rg[1], incl[0], incl[1]) {
								wantC++
							}
						}
						for _, r := range wantRels {
							if v, ok := r.GetProperty("w"); ok && rangeAtCount(v, rg[0], rg[1], incl[0], incl[1]) {
								wantRC++
							}
						}
						c, exact, err := g.Nodes().RangeCardinalityAt(label, "v", rg[0], rg[1], incl[0], incl[1], opts)
						if err != nil {
							t.Fatal(err)
						}
						rc, rexact, err := g.Rels().RangeCardinalityAt("R", "w", rg[0], rg[1], incl[0], incl[1], opts)
						if err != nil {
							t.Fatal(err)
						}
						if !served {
							if exact || rexact {
								t.Fatalf("%s %s: counts exact on a store without the sidecar range", name, b.name)
							}
							continue
						}
						if !exact || c != wantC {
							t.Fatalf("%s incl %v: node count %d exact=%v, want %d", name, incl, c, exact, wantC)
						}
						if relIndexed && (!rexact || rc != wantRC) {
							t.Fatalf("%s incl %v: rel count %d exact=%v, want %d", name, incl, rc, rexact, wantRC)
						}
					}
				}
			}

		}
		verify("values")
		// Phase 2a: values no row held before, after the numeric views opened.
		for step := range 20 {
			i := rng.IntN(len(nodes))
			nv := []any{77.25, int64(-8), float32(71.5)}[step%3]
			n, err := g.Nodes().Update(ctx, nodes[i].ID(), map[string]any{"v": nv, "tkg_valid_from": types.Instant(9000 + 10*step)})
			if err != nil {
				t.Fatal(err)
			}
			pin(n)
			if _, err := g.Rels().Update(ctx, rels[i].ID(), map[string]any{"w": nv, "tkg_valid_from": types.Instant(9000 + 10*step)}); err != nil {
				t.Fatal(err)
			}
		}
		verify("new values")
		// Phase 2: label changes and deletes.
		deleted := map[int]bool{}
		for step := range 40 {
			i := rng.IntN(len(nodes))
			if deleted[i] {
				continue
			}
			switch step % 2 {
			case 0:
				if b.tiered {
					continue // tiered refuses label changes across its shard classes
				}
				if err := g.Nodes().AddLabel(ctx, nodes[i].ID(), "Keep"); err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes().RemoveLabel(ctx, nodes[i].ID(), label); err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes().AddLabel(ctx, nodes[i].ID(), label); err != nil {
					t.Fatal(err)
				}
			case 1:
				if step%4 == 1 {
					if err := g.Nodes().Delete(ctx, nodes[i].ID()); err != nil {
						t.Fatal(err)
					}
					deleted[i] = true
				}
			}
		}
		verify("labels and deletes")
		allAt := storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pins[len(pins)-1]}
		// RangeCardinality keeps declining at a coordinate; RangeCardinalityAt
		// declines without one.
		if c, exact, err := g.Nodes().RangeCardinality(label, "v", 0, 50, true, true, allAt); err != nil || exact || c != 0 {
			t.Fatalf("RangeCardinality at a coordinate: %d exact=%v err=%v", c, exact, err)
		}
		if c, exact, err := g.Rels().RangeCardinalityAt("R", "w", 0, 50, true, true, storepkg.QueryOpts{}); err != nil || exact || c != 0 {
			t.Fatalf("RangeCardinalityAt without a coordinate: %d exact=%v err=%v", c, exact, err)
		}
		if _, _, err := g.Nodes().RangeCardinalityAt("", "v", 0, 1, true, true, allAt); err == nil {
			t.Fatal("empty label: no error")
		}
		// Without a declared index the count declines and the door folds.
		if c, exact, err := g.Nodes().RangeCardinalityAt(label, "nope", 0, 1, true, true, allAt); err != nil || exact || c != 0 {
			t.Fatalf("no index: %d exact=%v err=%v", c, exact, err)
		}
		if c, exact, err := g.Nodes().RangeCardinalityAt(label, "v", math.NaN(), 1, true, true, allAt); err != nil || exact || c != 0 {
			t.Fatalf("NaN bound: %d exact=%v err=%v", c, exact, err)
		}
	})
}
