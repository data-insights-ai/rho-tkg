package graph_test

import (
	"context"
	"math"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// cypherEqual is the oracle of the equality counts: numbers by value with a
// float32 widened, no crossing between kinds, nil and NaN never equal.
func cypherEqual(a, b any) bool {
	num := func(v any) (float64, bool) {
		switch x := v.(type) {
		case int:
			return float64(x), true
		case int64:
			return float64(x), true
		case int32:
			return float64(x), true
		case uint8:
			return float64(x), true
		case float32:
			return float64(x), true
		case float64:
			return x, true
		}
		return 0, false
	}
	if a == nil || b == nil {
		return false
	}
	fa, na := num(a)
	fb, nb := num(b)
	if na || nb {
		return na && nb && fa == fb
	}
	switch a.(type) {
	case string, bool:
		return a == b
	}
	return false
}

// TestCountByPropertyValue (sigma-tkgd round 7 request 4): on memory, badger
// and sharded the equality counts equal a brute-force count of the current
// rows Cypher equality keeps, through writes, updates and deletes; they
// decline (exact=false, no error) without an index, for an unknown label or
// type, under temporal opts, for a list value and on tiered.
func TestCountByPropertyValue(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		if err := g.Index().CreateProperty("Ref", "v"); err != nil {
			t.Fatal(err)
		}
		if err := g.Index().CreateRelProperty("R", "v"); err != nil && !b.tiered {
			t.Fatal(err)
		}
		values := []any{int64(3), 3.0, int(3), float32(3), uint8(3), "3", true, int64(4), 2.5, "a", false, math.NaN(), int32(-1), float32(0.1), 0.1}
		var nodes []*types.Node
		for i := range 45 {
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"v": values[i%len(values)]})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		var rels []*types.Relationship
		for i := range 30 {
			r, err := g.Rels().Add(ctx, "R", nodes[i], nodes[i+1], map[string]any{"v": values[(i*2)%len(values)]})
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r)
		}
		if _, err := g.Nodes().Update(ctx, nodes[1].ID(), map[string]any{"v": int64(3)}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, rels[2].ID(), map[string]any{"v": "3"}); err != nil {
			t.Fatal(err)
		}
		if err := g.Rels().Delete(ctx, rels[0].ID()); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().Delete(ctx, nodes[44].ID()); err != nil {
			t.Fatal(err)
		}

		queries := []any{int64(3), 3.0, float32(3), "3", true, false, int64(4), 2.5, "a", "zz", int64(99), nil, math.NaN(), int32(-1), 0.1, float32(0.1)}
		for _, q := range queries {
			var wantNodes, wantRels int64
			if err := g.Nodes().ForEachByLabel("Ref", storepkg.QueryOpts{}, func(n *types.Node) bool {
				if v, ok := n.GetProperty("v"); ok && cypherEqual(q, v) {
					wantNodes++
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if err := g.Rels().ForEachByType("R", storepkg.QueryOpts{}, func(r *types.Relationship) bool {
				if v, ok := r.GetProperty("v"); ok && cypherEqual(q, v) {
					wantRels++
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			gotNodes, exactNodes, err := g.Nodes().CountByLabelPropertyValue("Ref", "v", q, storepkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			gotRels, exactRels, err := g.Rels().CountByTypePropertyValue("R", "v", q, storepkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			if b.tiered {
				if exactNodes || exactRels {
					t.Fatalf("tiered %T %v: exact %v / %v, want declined", q, q, exactNodes, exactRels)
				}
				continue
			}
			if !exactNodes || gotNodes != wantNodes {
				t.Errorf("nodes %T %v: %d exact=%v, want %d", q, q, gotNodes, exactNodes, wantNodes)
			}
			if !exactRels || gotRels != wantRels {
				t.Errorf("rels %T %v: %d exact=%v, want %d", q, q, gotRels, exactRels, wantRels)
			}
		}

		declined := func(name string, count int64, exact bool, err error) {
			t.Helper()
			if err != nil || exact || count != 0 {
				t.Errorf("%s: %d exact=%v err=%v, want declined", name, count, exact, err)
			}
		}
		c, e, err := g.Nodes().CountByLabelPropertyValue("Ref", "w", int64(3), storepkg.QueryOpts{})
		declined("no index", c, e, err)
		c, e, err = g.Nodes().CountByLabelPropertyValue("Nope", "v", int64(3), storepkg.QueryOpts{})
		declined("unknown label", c, e, err)
		c, e, err = g.Rels().CountByTypePropertyValue("NOPE", "v", int64(3), storepkg.QueryOpts{})
		declined("unknown type", c, e, err)
		c, e, err = g.Nodes().CountByLabelPropertyValue("Ref", "v", int64(3), storepkg.QueryOpts{ValidAt: types.Instant(1)})
		declined("ValidAt", c, e, err)
		c, e, err = g.Rels().CountByTypePropertyValue("R", "v", int64(3), storepkg.QueryOpts{TxPin: types.Instant(1)})
		declined("TxPin", c, e, err)
		c, e, err = g.Nodes().CountByLabelPropertyValue("Ref", "v", []any{int64(3)}, storepkg.QueryOpts{})
		declined("list", c, e, err)
		c, e, err = g.Nodes().CountByLabelPropertyValue("Ref", "v", int64(1<<53+1), storepkg.QueryOpts{})
		declined("past 2^53", c, e, err)

		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := g.Nodes().CountByLabelPropertyValue("Ref", "v", int64(3), storepkg.QueryOpts{}); err == nil {
			t.Error("closed graph: no error")
		}
		if _, _, err := g.Rels().CountByTypePropertyValue("R", "v", int64(3), storepkg.QueryOpts{}); err == nil {
			t.Error("closed graph: no rel error")
		}
	})
}

// TestCountByPropertyValueDeclinesOnDiskIndex: badger with
// PropertyIndexOnDisk keeps no counted node value sets, so the node count
// declines (the relationship index stays in RAM and answers).
func TestCountByPropertyValueDeclinesOnDiskIndex(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{BadgerInMemory: true, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.Index().CreateProperty("Ref", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Add(context.Background(), []string{"Ref"}, map[string]any{"v": int64(3)}); err != nil {
		t.Fatal(err)
	}
	if c, exact, err := g.Nodes().CountByLabelPropertyValue("Ref", "v", int64(3), storepkg.QueryOpts{}); err != nil || exact || c != 0 {
		t.Fatalf("disk index: %d exact=%v err=%v, want declined", c, exact, err)
	}
}
