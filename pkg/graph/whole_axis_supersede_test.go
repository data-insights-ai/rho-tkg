package graph_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// An untimed update of an UntimedWholeAxis entity supersedes the version it
// replaces in transaction time: History shows the replaced row as
// tx[T, T') with T' the new row's TxFrom, as Update writes on the legacy axis
// (sigma-tkgd report 2026-10-18: the replaced row came back as tx[T, _)).
// The stamp changes no read: pins before the update answer the old values,
// pins after it the new ones, at every valid instant. Nodes and
// relationships, every backend.

// r12NodeRow returns the row of version v in rows (r12RelRow: relationships).
func r12NodeRow(t *testing.T, rows []*types.Node, v uint32) *types.Node {
	t.Helper()
	for _, r := range rows {
		if r.Version() == v {
			return r
		}
	}
	t.Fatalf("no history row of version %d", v)
	return nil
}

func r12RelRow(t *testing.T, rows []*types.Relationship, v uint32) *types.Relationship {
	t.Helper()
	for _, r := range rows {
		if r.Version() == v {
			return r
		}
	}
	t.Fatalf("no history row of version %d", v)
	return nil
}

func TestWholeAxisUpdateStampsSupersededTxTo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11WholeAxis) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)

			// A pure whole-axis node and relationship.
			n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"a": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			m, err := g.Nodes().Add(ctx, []string{"A"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "E", n, m, map[string]any{"a": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			before := r11NowAfter(t, g)
			n2, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"b": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			r2, err := g.Rels().Update(ctx, r.ID(), map[string]any{"b": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			after := r11NowAfter(t, g)

			nh, err := g.Nodes().History(n.ID())
			if err != nil {
				t.Fatal(err)
			}
			old := r12NodeRow(t, nh, n.Version()).Temporal()
			if old.TxTo == 0 || old.TxTo != n2.Temporal().TxFrom {
				t.Errorf("node: replaced row tx[%d, %d), want TxTo = the new row's TxFrom %d", old.TxFrom, old.TxTo, n2.Temporal().TxFrom)
			}
			rh, err := g.Rels().History(r.ID())
			if err != nil {
				t.Fatal(err)
			}
			rold := r12RelRow(t, rh, r.Version()).Temporal()
			if rold.TxTo == 0 || rold.TxTo != r2.Temporal().TxFrom {
				t.Errorf("rel: replaced row tx[%d, %d), want TxTo = the new row's TxFrom %d", rold.TxFrom, rold.TxTo, r2.Temporal().TxFrom)
			}
			for _, v := range []types.Instant{types.UnboundedValidFrom + 1, -5, 0, before, r11Far} {
				r11Want(t, fmt.Sprintf("node b AT %d pin before", v), r11KAt(t, g, n.ID(), "b", v, before), "unset")
				r11Want(t, fmt.Sprintf("node b AT %d pin after", v), r11KAt(t, g, n.ID(), "b", v, after), "2")
				r11Want(t, fmt.Sprintf("node a AT %d pin after", v), r11KAt(t, g, n.ID(), "a", v, after), "1")
				r11Want(t, fmt.Sprintf("rel b AT %d pin before", v), r11RelKAt(t, g, r.ID(), "b", v, before), "unset")
				r11Want(t, fmt.Sprintf("rel b AT %d pin after", v), r11RelKAt(t, g, r.ID(), "b", v, after), "2")
			}

			// A segmented node and relationship (k=1 on [100,200), k=2 on
			// [200, open)): an update changing every segment supersedes the
			// current row.
			for _, rel := range []bool{false, true} {
				var curV uint32
				var nodeID types.NodeID
				var relID types.RelID
				if rel {
					relID = r11SegmentedRel(t, g)
					cur, err := g.Rels().Get(ctx, relID)
					if err != nil {
						t.Fatal(err)
					}
					curV = cur.Version()
				} else {
					nodeID = r11Segmented(t, g)
					cur, err := g.Nodes().Get(ctx, nodeID)
					if err != nil {
						t.Fatal(err)
					}
					curV = cur.Version()
				}
				pin := r11NowAfter(t, g)
				var newTx, oldTo types.Instant
				if rel {
					up, err := g.Rels().Update(ctx, relID, map[string]any{"j": int64(5)})
					if err != nil {
						t.Fatal(err)
					}
					newTx = up.Temporal().TxFrom
					h, err := g.Rels().History(relID)
					if err != nil {
						t.Fatal(err)
					}
					oldTo = r12RelRow(t, h, curV).Temporal().TxTo
				} else {
					up, err := g.Nodes().Update(ctx, nodeID, map[string]any{"j": int64(5)})
					if err != nil {
						t.Fatal(err)
					}
					newTx = up.Temporal().TxFrom
					h, err := g.Nodes().History(nodeID)
					if err != nil {
						t.Fatal(err)
					}
					oldTo = r12NodeRow(t, h, curV).Temporal().TxTo
				}
				if oldTo != newTx {
					t.Errorf("segmented rel=%v: replaced current row TxTo %d, want %d", rel, oldTo, newTx)
				}
				pinAfter := r11NowAfter(t, g)
				for _, c := range []struct {
					v       types.Instant
					k, j    string
					jBefore string
				}{{50, "none", "none", "none"}, {150, "1", "5", "unset"}, {250, "2", "5", "unset"}, {r11Far, "2", "5", "unset"}} {
					at := func(key string, p types.Instant) string {
						if rel {
							return r11RelKAt(t, g, relID, key, c.v, p)
						}
						return r11KAt(t, g, nodeID, key, c.v, p)
					}
					r11Want(t, fmt.Sprintf("rel=%v k AT %d after", rel, c.v), at("k", pinAfter), c.k)
					r11Want(t, fmt.Sprintf("rel=%v j AT %d after", rel, c.v), at("j", pinAfter), c.j)
					r11Want(t, fmt.Sprintf("rel=%v k AT %d before", rel, c.v), at("k", pin), c.k)
					r11Want(t, fmt.Sprintf("rel=%v j AT %d before", rel, c.v), at("j", pin), c.jBefore)
				}
			}
		})
	}
}

// TestWholeAxisUpdatePartialKeepsReads: an update that changes some segments
// only (the others already hold the value) leaves the unchanged segments'
// rows answering, at every valid instant, after the write.
func TestWholeAxisUpdatePartialKeepsReads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11WholeAxis) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			id := r11Segmented(t, g)
			// k=9 on [300, 400) splits the open segment: k=2 on [200,300) and
			// [400, open) are two rows (the second a resumption).
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, id, 300, 400, map[string]any{"k": int64(9)}); err != nil {
				t.Fatal(err)
			}
			pin := r11NowAfter(t, g)
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			after := r11NowAfter(t, g)
			for _, c := range []struct {
				v             types.Instant
				before, after string
			}{{50, "none", "none"}, {150, "1", "2"}, {250, "2", "2"}, {350, "9", "2"}, {450, "2", "2"}, {r11Far, "2", "2"}} {
				r11Want(t, fmt.Sprintf("k AT %d before", c.v), r11KAt(t, g, id, "k", c.v, pin), c.before)
				r11Want(t, fmt.Sprintf("k AT %d after", c.v), r11KAt(t, g, id, "k", c.v, after), c.after)
			}

			// A void span split by a newer correction: [150,160) and
			// [170,180) stay void after an update that changes every valid
			// segment.
			n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal().ExciseNodeValidity(ctx, n.ID(), 150, 180); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), 160, 170, map[string]any{"k": int64(9)}); err != nil {
				t.Fatal(err)
			}
			pin = r11NowAfter(t, g)
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(3)}); err != nil {
				t.Fatal(err)
			}
			after = r11NowAfter(t, g)
			for _, c := range []struct {
				v             types.Instant
				before, after string
			}{{50, "none", "none"}, {120, "1", "3"}, {155, "none", "none"}, {165, "9", "3"}, {175, "none", "none"}, {200, "1", "3"}, {r11Far, "1", "3"}} {
				r11Want(t, fmt.Sprintf("void: k AT %d before", c.v), r11KAt(t, g, n.ID(), "k", c.v, pin), c.before)
				r11Want(t, fmt.Sprintf("void: k AT %d after", c.v), r11KAt(t, g, n.ID(), "k", c.v, after), c.after)
			}

			// An older belief hidden under a newer one and a void span: k=7
			// on [165,175) (older), k=2 from 100 (newer), [150,180) excised,
			// k=9 on [155,160). The update must not bring k=7 back at 170.
			h, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(50)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, h.ID(), 165, 175, map[string]any{"k": int64(7)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, h.ID(), 100, types.InfiniteValidTo, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			if err := g.Temporal().ExciseNodeValidity(ctx, h.ID(), 150, 180); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, h.ID(), 155, 160, map[string]any{"k": int64(9)}); err != nil {
				t.Fatal(err)
			}
			pin = r11NowAfter(t, g)
			if _, err := g.Nodes().Update(ctx, h.ID(), map[string]any{"k": int64(3)}); err != nil {
				t.Fatal(err)
			}
			after = r11NowAfter(t, g)
			for _, c := range []struct {
				v             types.Instant
				before, after string
			}{{40, "none", "none"}, {70, "1", "3"}, {120, "2", "3"}, {152, "none", "none"}, {157, "9", "3"}, {170, "none", "none"}, {178, "none", "none"}, {200, "2", "3"}} {
				r11Want(t, fmt.Sprintf("hidden: k AT %d before", c.v), r11KAt(t, g, h.ID(), "k", c.v, pin), c.before)
				r11Want(t, fmt.Sprintf("hidden: k AT %d after", c.v), r11KAt(t, g, h.ID(), "k", c.v, after), c.after)
			}
		})
	}
}
