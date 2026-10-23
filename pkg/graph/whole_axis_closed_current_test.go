package graph_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// An untimed update or label change of an UntimedWholeAxis entity whose rows
// are all closed (no own-open row: a tkg_valid_to close or an excise of the
// middle) patches every segment; the current slot must follow the newest
// belief, so Get, the update's return value and the current indexes agree
// with NodeAt (todo row 80). The open-tail shape is the guard.
func TestWholeAxisClosedCurrentFollowsNewestBelief(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	shapes := map[string]func(t *testing.T, g *graphpkg.Graph, id types.NodeID){
		"validto": func(t *testing.T, g *graphpkg.Graph, id types.NodeID) {
			if _, err := g.Nodes().Update(ctx, id, map[string]any{types.ShadowValidTo: types.Instant(2000)}); err != nil {
				t.Fatal(err)
			}
		},
		"twoclosed": func(t *testing.T, g *graphpkg.Graph, id types.NodeID) { // [1000,1500) and [2000,3000)
			if _, err := g.Nodes().Update(ctx, id, map[string]any{types.ShadowValidTo: types.Instant(3000)}); err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal().ExciseNodeValidity(ctx, id, 1500, 2000); err != nil {
				t.Fatal(err)
			}
		},
		"opentail": func(t *testing.T, g *graphpkg.Graph, id types.NodeID) {
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"j": int64(2), types.ShadowValidFrom: types.Instant(2000)}); err != nil {
				t.Fatal(err)
			}
		},
	}
	for _, b := range allStoreBackendsWith(r11WholeAxis) {
		if b.tiered {
			continue // tiered demote is row 79's scope
		}
		for name, shape := range shapes {
			t.Run(b.name+"/"+name, func(t *testing.T) {
				g := b.open(t)
				if err := g.Constraints().CreateUnique(ctx, "A", "k"); err != nil {
					t.Fatal(err)
				}
				n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(1000)})
				if err != nil {
					t.Fatal(err)
				}
				id := n.ID()
				shape(t, g, id)
				ret, err := g.Nodes().Update(ctx, id, map[string]any{"k": int64(5)})
				if err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes().AddLabel(ctx, id, "B"); err != nil {
					t.Fatal(err)
				}
				if v, _ := ret.Properties().Get("k"); v != int64(5) {
					t.Errorf("Update returned k=%v, want 5", v)
				}
				cur, err := g.Nodes().Get(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				ls := g.Nodes().Labels(cur)
				slices.Sort(ls)
				if v, _ := cur.Properties().Get("k"); v != int64(5) || strings.Join(ls, ",") != "A,B" {
					t.Errorf("Get: k=%v labels %v, want k=5 [A B]", v, ls)
				}
				at, err := g.Temporal().NodeAt(id, 1200)
				if err != nil {
					t.Fatal(err)
				}
				if v, _ := at.Properties().Get("k"); v != int64(5) {
					t.Errorf("NodeAt(1200) k=%v, want 5", v)
				}
				if ns, err := g.Nodes().ByLabel("B", storepkg.QueryOpts{}); err != nil || len(ns) != 1 {
					t.Errorf("ByLabel(B) = %d, %v; want 1", len(ns), err)
				}
				if c, err := g.Nodes().CountByLabel("B"); err != nil || c != 1 {
					t.Errorf("CountByLabel(B) = %d, %v; want 1", c, err)
				}
				if ns, err := g.Nodes().ByLabelAndProperty("A", "k", int64(5), storepkg.QueryOpts{}); err != nil || len(ns) != 1 {
					t.Errorf("ByLabelAndProperty(A,k,5) = %d, %v; want 1", len(ns), err)
				}
				if ns, err := g.Nodes().ByLabelAndProperty("A", "k", int64(1), storepkg.QueryOpts{}); err != nil || len(ns) != 0 {
					t.Errorf("ByLabelAndProperty(A,k,1) = %d, %v; want 0", len(ns), err)
				}
				// UniqueCurrent: k=1 is free now, k=5 is taken.
				if _, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1)}); err != nil {
					t.Errorf("Add k=1 after the update: %v, want nil", err)
				}
				if _, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(5)}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
					t.Errorf("Add k=5 after the update: %v, want ErrUniqueViolation", err)
				}
			})
		}
	}
}

// The relationship twin: an untimed update of a closed relationship moves
// the current slot to the patched row; the open tail is the guard.
func TestWholeAxisClosedCurrentFollowsNewestBeliefRel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11WholeAxis) {
		if b.tiered {
			continue
		}
		for _, closed := range []bool{true, false} {
			name := b.name + "/opentail"
			if closed {
				name = b.name + "/validto"
			}
			t.Run(name, func(t *testing.T) {
				g := b.open(t)
				from, err := g.Nodes().Add(ctx, []string{"N"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				to, err := g.Nodes().Add(ctx, []string{"N"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(ctx, "R", from, to, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(1000)})
				if err != nil {
					t.Fatal(err)
				}
				second := map[string]any{"j": int64(2), types.ShadowValidFrom: types.Instant(2000)}
				if closed {
					second = map[string]any{types.ShadowValidTo: types.Instant(2000)}
				}
				if _, err := g.Rels().Update(ctx, r.ID(), second); err != nil {
					t.Fatal(err)
				}
				ret, err := g.Rels().Update(ctx, r.ID(), map[string]any{"k": int64(5)})
				if err != nil {
					t.Fatal(err)
				}
				if v, _ := ret.Properties().Get("k"); v != int64(5) {
					t.Errorf("Update returned k=%v, want 5", v)
				}
				cur, err := g.Rels().Get(ctx, r.ID())
				if err != nil {
					t.Fatal(err)
				}
				if v, _ := cur.Properties().Get("k"); v != int64(5) {
					t.Errorf("Get k=%v, want 5", v)
				}
				if rs, err := g.Rels().ByTypeAndProperty("R", "k", int64(5), storepkg.QueryOpts{}); err != nil || len(rs) != 1 {
					t.Errorf("ByTypeAndProperty(R,k,5) = %d, %v; want 1", len(rs), err)
				}
			})
		}
	}
}
