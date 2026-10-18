package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// An untimed label change of an UntimedWholeAxis node with explicit
// valid-time segments applies to every segment and keeps the boundaries, as
// the untimed property update does (SQL:2011, no FOR PORTION OF). Reads at a
// valid instant in any segment see the new label set at pins after the
// change and the old one at pins before it; the current label index follows
// the current row. Doors: Nodes().AddLabel / RemoveLabel and the GraphTx
// AddNodeLabel / RemoveNodeLabel (with rollback). Every backend.

// wlSegmented: labels [A], k=1 on [1000,2000), k=2 on [2000, open) (a timed
// update).
func wlSegmented(t *testing.T, g *graphpkg.Graph) types.NodeID {
	t.Helper()
	ctx := context.Background()
	n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(1000)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(2), types.ShadowValidFrom: types.Instant(2000)}); err != nil {
		t.Fatal(err)
	}
	return n.ID()
}

// wlLabelsAt is the sorted label set of node id at valid v believed at pin
// (0: now), "none" when no version is valid there.
func wlLabelsAt(t *testing.T, g *graphpkg.Graph, id types.NodeID, v, pin types.Instant) string {
	t.Helper()
	var n *types.Node
	var err error
	if pin == 0 {
		n, err = g.Temporal().NodeAt(id, v)
	} else {
		n, err = g.Temporal().NodeAtTx(id, v, pin)
	}
	if errors.Is(err, graphpkg.ErrNoVersionValidAt) || errors.Is(err, graphpkg.ErrNodeNotFound) {
		return "none"
	}
	if err != nil {
		t.Fatalf("node %v at %d pin %d: %v", id, v, pin, err)
	}
	ls := g.Nodes().Labels(n)
	slices.Sort(ls)
	return strings.Join(ls, ",")
}

// wlByLabel reports whether id is in ByLabel(label, opts).
func wlByLabel(t *testing.T, g *graphpkg.Graph, label string, opts storepkg.QueryOpts, id types.NodeID) bool {
	t.Helper()
	ns, err := g.Nodes().ByLabel(label, opts)
	if err != nil {
		t.Fatalf("ByLabel %s %+v: %v", label, opts, err)
	}
	return slices.ContainsFunc(ns, func(n *types.Node) bool { return n.ID() == id })
}

func TestWholeAxisLabelChangePerSegment(t *testing.T) {
	ctx := context.Background()
	for _, door := range []string{"api", "tx"} {
		for _, b := range allStoreBackendsWith(r11WholeAxis) {
			t.Run(door+"/"+b.name, func(t *testing.T) {
				g := b.open(t)
				addLabel := func(id types.NodeID, l string) error { return g.Nodes().AddLabel(ctx, id, l) }
				removeLabel := func(id types.NodeID, l string) error { return g.Nodes().RemoveLabel(ctx, id, l) }
				if door == "tx" {
					addLabel = func(id types.NodeID, l string) error {
						return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.AddNodeLabel(id, l) })
					}
					removeLabel = func(id types.NodeID, l string) error {
						return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.RemoveNodeLabel(id, l) })
					}
				}
				id := wlSegmented(t, g)
				other, err := g.Nodes().Add(ctx, []string{"A"}, nil) // a whole-axis bystander
				if err != nil {
					t.Fatal(err)
				}
				p0 := r11NowAfter(t, g)
				if err := addLabel(id, "B"); err != nil {
					t.Fatalf("AddLabel B: %v", err)
				}
				p1 := r11NowAfter(t, g)

				for _, c := range []struct {
					v                types.Instant
					before, after, k string
				}{{500, "none", "none", "none"}, {1500, "A", "A,B", "1"}, {2500, "A", "A,B", "2"}, {r11Far, "A", "A,B", "2"}} {
					r11Want(t, fmt.Sprintf("labels AT %d pin p0", c.v), wlLabelsAt(t, g, id, c.v, p0), c.before)
					r11Want(t, fmt.Sprintf("labels AT %d pin p1", c.v), wlLabelsAt(t, g, id, c.v, p1), c.after)
					r11Want(t, fmt.Sprintf("labels AT %d now", c.v), wlLabelsAt(t, g, id, c.v, 0), c.after)
					r11Want(t, fmt.Sprintf("k AT %d now (boundaries kept)", c.v), r11KAt(t, g, id, "k", c.v, 0), c.k)
				}
				// Label scans at valid instants in each segment, both pins.
				for _, v := range []types.Instant{1500, 2500} {
					if !wlByLabel(t, g, "B", storepkg.QueryOpts{ValidAt: v}, id) {
						t.Errorf("ByLabel B AT %d: missing", v)
					}
					if wlByLabel(t, g, "B", storepkg.QueryOpts{ValidAt: v, TxAt: p0}, id) {
						t.Errorf("ByLabel B AT %d TxAt p0: present before the change", v)
					}
					if !wlByLabel(t, g, "B", storepkg.QueryOpts{ValidAt: v, TxAt: p1}, id) {
						t.Errorf("ByLabel B AT %d TxAt p1: missing", v)
					}
					if !wlByLabel(t, g, "A", storepkg.QueryOpts{ValidAt: v}, id) {
						t.Errorf("ByLabel A AT %d: missing", v)
					}
				}
				if wlByLabel(t, g, "B", storepkg.QueryOpts{ValidAt: 500}, id) {
					t.Errorf("ByLabel B AT 500: present where no version is valid")
				}
				if !wlByLabel(t, g, "B", storepkg.QueryOpts{TxPin: p1}, id) || wlByLabel(t, g, "B", storepkg.QueryOpts{TxPin: p0}, id) {
					t.Errorf("ByLabel B TxPin: want present at p1 only")
				}
				// The current label index follows the current row.
				if !wlByLabel(t, g, "B", storepkg.QueryOpts{}, id) || wlByLabel(t, g, "B", storepkg.QueryOpts{}, other.ID()) {
					t.Errorf("current ByLabel B: want the node only")
				}
				if c, err := g.Nodes().CountByLabel("B"); err != nil || c != 1 {
					t.Errorf("CountByLabel B = %d, %v; want 1", c, err)
				}
				cur, err := g.Nodes().Get(ctx, id)
				if err != nil || !g.Nodes().HasLabel(cur, "B") {
					t.Errorf("Get after AddLabel: %v, %v; want label B", cur, err)
				}

				// Idempotent: every segment holds B, nothing is written.
				h1, err := g.Nodes().History(id)
				if err != nil {
					t.Fatal(err)
				}
				if err := addLabel(id, "B"); err != nil {
					t.Fatalf("AddLabel B again: %v", err)
				}
				h2, err := g.Nodes().History(id)
				if err != nil {
					t.Fatal(err)
				}
				if len(h1) != len(h2) {
					t.Errorf("AddLabel of a label every segment holds wrote %d rows", len(h2)-len(h1))
				}

				// RemoveLabel per segment.
				p2 := r11NowAfter(t, g)
				if err := removeLabel(id, "A"); err != nil {
					t.Fatalf("RemoveLabel A: %v", err)
				}
				p3 := r11NowAfter(t, g)
				for _, v := range []types.Instant{1500, 2500} {
					r11Want(t, fmt.Sprintf("labels AT %d pin p2", v), wlLabelsAt(t, g, id, v, p2), "A,B")
					r11Want(t, fmt.Sprintf("labels AT %d pin p3", v), wlLabelsAt(t, g, id, v, p3), "B")
					if wlByLabel(t, g, "A", storepkg.QueryOpts{ValidAt: v}, id) {
						t.Errorf("ByLabel A AT %d after RemoveLabel: present", v)
					}
					if !wlByLabel(t, g, "A", storepkg.QueryOpts{ValidAt: v, TxAt: p2}, id) {
						t.Errorf("ByLabel A AT %d TxAt p2: missing", v)
					}
				}
				if wlByLabel(t, g, "A", storepkg.QueryOpts{}, id) || !wlByLabel(t, g, "A", storepkg.QueryOpts{}, other.ID()) {
					t.Errorf("current ByLabel A after RemoveLabel: want the bystander only")
				}
				// Refusals per segment.
				if err := removeLabel(id, "B"); !errors.Is(err, graphpkg.ErrLastLabel) {
					t.Errorf("RemoveLabel of every segment's last label: %v, want ErrLastLabel", err)
				}
				if err := removeLabel(id, "A"); !errors.Is(err, graphpkg.ErrLabelNotFound) {
					t.Errorf("RemoveLabel of a label no segment carries: %v, want ErrLabelNotFound", err)
				}
				if err := removeLabel(id, "NeverSeen"); !errors.Is(err, graphpkg.ErrLabelNotFound) {
					t.Errorf("RemoveLabel of an unknown label: %v, want ErrLabelNotFound", err)
				}
				r11Want(t, "after the refusals: labels AT 1500", wlLabelsAt(t, g, id, 1500, 0), "B")
			})
		}
	}
}

// TestWholeAxisLabelChangeRollback: a GraphTx whose per-segment label change
// rolls back restores the current row, its label index and every pin's
// answer; a label change of a closed whole-axis node applies to its segment.
func TestWholeAxisLabelChangeRollback(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11WholeAxis) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			id := wlSegmented(t, g)
			cur, err := g.Nodes().Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			pin := r11NowAfter(t, g)
			errBoom := errors.New("boom")
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				if err := tx.AddNodeLabel(id, "C"); err != nil {
					return err
				}
				return errBoom
			}); !errors.Is(err, errBoom) {
				t.Fatalf("tx: %v, want boom", err)
			}
			for _, v := range []types.Instant{1500, 2500} {
				r11Want(t, fmt.Sprintf("after rollback: labels AT %d", v), wlLabelsAt(t, g, id, v, 0), "A")
				r11Want(t, fmt.Sprintf("after rollback: labels AT %d pin", v), wlLabelsAt(t, g, id, v, pin), "A")
				if wlByLabel(t, g, "C", storepkg.QueryOpts{ValidAt: v}, id) {
					t.Errorf("after rollback: ByLabel C AT %d has the node", v)
				}
			}
			if wlByLabel(t, g, "C", storepkg.QueryOpts{}, id) || !wlByLabel(t, g, "A", storepkg.QueryOpts{}, id) {
				t.Errorf("after rollback: current label index changed")
			}
			after, err := g.Nodes().Get(ctx, id)
			if err != nil || after.Version() != cur.Version() {
				t.Errorf("after rollback: current version %v, want %d (%v)", after, cur.Version(), err)
			}
			h, err := g.Nodes().History(id)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range h {
				if r.Version() > cur.Version() {
					t.Errorf("after rollback: history keeps version %d above the current %d", r.Version(), cur.Version())
				}
			}
		})
	}
}
