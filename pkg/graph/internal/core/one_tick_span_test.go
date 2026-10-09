package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// One-tick valid intervals are ordinary spans.
//
// A row with ValidTo == ValidFrom+1 was once the cascade's "eclipse" sentinel
// (a pre-994df82 in-place cascade marked fully-covered rows that way). No writer
// has produced the sentinel since the append-only cascade (2026-06-12), but the
// resolvers kept skipping every such row. A caller-written [t, t+1) interval, a
// CloseVersion at vf+1, a delete landing at vf+1 and a width-1 cascade piece all
// produce exactly that shape, and the temporal doors silently dropped them while
// the store's own predicates (Rels().ByType with no temporal opts) still saw them.
//
// Every test below runs over memory, badger (in-memory), sharded and tiered
// (txbBackends), node AND relationship (rule 2), named AND generic doors
// (rule 17). Each states the faulty implementation it catches.

// otsBase is a valid-time origin one hour in the past: far enough below the
// wall that no clock floor or future-valid rule interferes, and on the hot
// shard's week for tiered.
func otsBase() types.Instant { return txbWall() - 3_600_000 }

func otsNodeV(t *testing.T, n *types.Node, key string) any {
	t.Helper()
	v, _ := n.GetProperty(key)
	return v
}

func otsRelV(t *testing.T, r *types.Relationship, key string) any {
	t.Helper()
	v, _ := r.GetProperty(key)
	return v
}

// otsEndpoints adds a "Ref" start and an "Ev" end node valid from long before
// base (cross-shard on tiered), so the effective (endpoint-masked) rel doors
// do not mask the edge for an endpoint reason.
func otsEndpoints(t *testing.T, g *Core, base types.Instant) (*types.Node, *types.Node) {
	t.Helper()
	ctx := context.Background()
	s, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": base - 100_000})
	if err != nil {
		t.Fatalf("Add(Ref): %v", err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"tkg_valid_from": base - 100_000})
	if err != nil {
		t.Fatalf("Add(Ev): %v", err)
	}
	return s, e
}

func wantNoVersion(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Errorf("%s: err = %v, want ErrNoVersionValidAt", what, err)
	}
}

// TestOneTickSpanVisible_NodeDoors catches a resolver that treats a
// caller-written [t, t+1) node as the eclipse sentinel and skips it: every
// named and generic valid-time door must return it at t, and none may return it
// at t+1 (half-open) or t-1. The width-2 control [t, t+2) catches a "fix" that
// merely moves the sentinel to another width, or that widens a one-tick span to
// cover t+1.
func TestOneTickSpanVisible_NodeDoors(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			b := otsBase()
			one, err := g.Nodes.Add(ctx, []string{"Span"}, map[string]any{"tkg_valid_from": b, "tkg_valid_to": b + 1, "k": "one"})
			if err != nil {
				t.Fatalf("Add one: %v", err)
			}
			two, err := g.Nodes.Add(ctx, []string{"Span"}, map[string]any{"tkg_valid_from": b, "tkg_valid_to": b + 2, "k": "two"})
			if err != nil {
				t.Fatalf("Add two: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			both := []types.NodeID{one.ID(), two.ID()}
			onlyTwo := []types.NodeID{two.ID()}

			// The store door already sees the one-tick row (no temporal opts).
			all, err := g.Nodes.ByLabel("Span", storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("ByLabel{}: %v", err)
			}
			assertNodeSet(t, "ByLabel{} (store door)", all, both)

			// Point at t: every door returns both.
			n, err := g.Temporal.NodeAt(one.ID(), b)
			if err != nil {
				t.Errorf("NodeAt(one, t): %v (one-tick span skipped)", err)
			} else if otsNodeV(t, n, "k") != "one" {
				t.Errorf("NodeAt(one, t).k = %v", otsNodeV(t, n, "k"))
			}
			n, err = g.Temporal.NodeAtTx(one.ID(), b, pin)
			if err != nil {
				t.Errorf("NodeAtTx(one, t, now): %v", err)
			} else if otsNodeV(t, n, "k") != "one" {
				t.Errorf("NodeAtTx(one, t, now).k = %v", otsNodeV(t, n, "k"))
			}
			got, err := g.Temporal.NodesAt(b)
			if err != nil {
				t.Fatalf("NodesAt: %v", err)
			}
			assertNodeSet(t, "NodesAt(t)", got, both)
			got, err = g.Temporal.NodesByLabelAt("Span", b)
			if err != nil {
				t.Fatalf("NodesByLabelAt: %v", err)
			}
			assertNodeSet(t, "NodesByLabelAt(t)", got, both)
			got, err = g.Nodes.ByLabel("Span", storepkg.QueryOpts{ValidAt: b})
			if err != nil {
				t.Fatalf("ByLabel{ValidAt}: %v", err)
			}
			assertNodeSet(t, "ByLabel{ValidAt: t}", got, both)
			got, err = g.Nodes.ByLabel("Span", storepkg.QueryOpts{ValidAt: b, TxAt: pin})
			if err != nil {
				t.Fatalf("ByLabel{ValidAt,TxAt}: %v", err)
			}
			assertNodeSet(t, "ByLabel{ValidAt: t, TxAt: now}", got, both)
			got, err = g.Temporal.NodesAtTx(b, pin)
			if err != nil {
				t.Fatalf("NodesAtTx: %v", err)
			}
			assertNodeSet(t, "NodesAtTx(t, now)", got, both)
			snap, err := g.Temporal.Snapshot(b)
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			assertNodeSet(t, "Snapshot(t).Nodes", snap.Nodes, both)

			// During [t, t+2000): both; [t+1, t+2000): only the width-2 row;
			// [t-1000, t): neither (the spans start at t).
			got, err = g.Temporal.NodesDuring(b, b+2000)
			if err != nil {
				t.Fatalf("NodesDuring: %v", err)
			}
			assertNodeSet(t, "NodesDuring[t, t+2000)", got, both)
			got, err = g.Nodes.ByLabel("Span", storepkg.QueryOpts{ValidStart: b, ValidEnd: b + 2000})
			if err != nil {
				t.Fatalf("ByLabel{During}: %v", err)
			}
			assertNodeSet(t, "ByLabel{ValidStart: t, ValidEnd: t+2000}", got, both)
			got, err = g.Temporal.NodesDuringTx(b, b+2000, pin)
			if err != nil {
				t.Fatalf("NodesDuringTx: %v", err)
			}
			assertNodeSet(t, "NodesDuringTx[t, t+2000)", got, both)
			got, err = g.Temporal.NodesDuring(b+1, b+2000)
			if err != nil {
				t.Fatalf("NodesDuring(t+1): %v", err)
			}
			assertNodeSet(t, "NodesDuring[t+1, t+2000)", got, onlyTwo)
			got, err = g.Temporal.NodesDuring(b-1000, b)
			if err != nil {
				t.Fatalf("NodesDuring(before): %v", err)
			}
			assertNodeSet(t, "NodesDuring[t-1000, t)", got, nil)

			// Relating with every Allen relation: any existing version relates.
			got, err = g.Temporal.NodesRelating(b, b+2000, types.AllRelations())
			if err != nil {
				t.Fatalf("NodesRelating: %v", err)
			}
			assertNodeSet(t, "NodesRelating(all)", got, both)
			// {Equals [t, t+1)} is the one-tick row alone.
			got, err = g.Temporal.NodesRelating(b, b+1, types.Equals.Set())
			if err != nil {
				t.Fatalf("NodesRelating(Equals): %v", err)
			}
			assertNodeSet(t, "NodesRelating(Equals [t, t+1))", got, []types.NodeID{one.ID()})

			// Boundary t+1: half-open — the one-tick row must NOT match.
			_, err = g.Temporal.NodeAt(one.ID(), b+1)
			wantNoVersion(t, "NodeAt(one, t+1)", err)
			got, err = g.Temporal.NodesAt(b + 1)
			if err != nil {
				t.Fatalf("NodesAt(t+1): %v", err)
			}
			assertNodeSet(t, "NodesAt(t+1)", got, onlyTwo)
			got, err = g.Nodes.ByLabel("Span", storepkg.QueryOpts{ValidAt: b + 1})
			if err != nil {
				t.Fatalf("ByLabel{ValidAt: t+1}: %v", err)
			}
			assertNodeSet(t, "ByLabel{ValidAt: t+1}", got, onlyTwo)
			// t+2 and t-1: nothing.
			got, err = g.Temporal.NodesAt(b + 2)
			if err != nil {
				t.Fatalf("NodesAt(t+2): %v", err)
			}
			assertNodeSet(t, "NodesAt(t+2)", got, nil)
			_, err = g.Temporal.NodeAt(one.ID(), b-1)
			wantNoVersion(t, "NodeAt(one, t-1)", err)
		})
	}
}

// TestOneTickSpanVisible_RelDoors is the relationship mirror (rule 2) of
// TestOneTickSpanVisible_NodeDoors: it catches RelAt, RelsDuring,
// ByType{ValidAt}, RelsRelating and the effective (endpoint-masked) doors
// skipping a [t, t+1) edge while Rels().ByType(T, QueryOpts{}) finds it —
// the two-doors disagreement the consumer reported.
func TestOneTickSpanVisible_RelDoors(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			b := otsBase()
			s, e := otsEndpoints(t, g, b)
			one, err := g.Rels.AddByID(ctx, "SPAN", s.ID(), e.ID(), map[string]any{"tkg_valid_from": b, "tkg_valid_to": b + 1, "k": "one"})
			if err != nil {
				t.Fatalf("AddByID one: %v", err)
			}
			two, err := g.Rels.AddByID(ctx, "SPAN", s.ID(), e.ID(), map[string]any{"tkg_valid_from": b, "tkg_valid_to": b + 2, "k": "two"})
			if err != nil {
				t.Fatalf("AddByID two: %v", err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			both := []types.RelID{one.ID(), two.ID()}
			onlyTwo := []types.RelID{two.ID()}

			all, err := g.Rels.ByType("SPAN", storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("ByType{}: %v", err)
			}
			assertRelSet(t, "ByType{} (store door)", all, both)

			r, err := g.Temporal.RelAt(one.ID(), b)
			if err != nil {
				t.Errorf("RelAt(one, t): %v (one-tick span skipped)", err)
			} else if otsRelV(t, r, "k") != "one" {
				t.Errorf("RelAt(one, t).k = %v", otsRelV(t, r, "k"))
			}
			r, err = g.Temporal.RelAtTx(one.ID(), b, pin)
			if err != nil {
				t.Errorf("RelAtTx(one, t, now): %v", err)
			} else if otsRelV(t, r, "k") != "one" {
				t.Errorf("RelAtTx(one, t, now).k = %v", otsRelV(t, r, "k"))
			}
			got, err := g.Temporal.RelsAt(b)
			if err != nil {
				t.Fatalf("RelsAt: %v", err)
			}
			assertRelSet(t, "RelsAt(t)", got, both)
			got, err = g.Temporal.RelsByTypeAt("SPAN", b)
			if err != nil {
				t.Fatalf("RelsByTypeAt: %v", err)
			}
			assertRelSet(t, "RelsByTypeAt(t)", got, both)
			got, err = g.Rels.ByType("SPAN", storepkg.QueryOpts{ValidAt: b})
			if err != nil {
				t.Fatalf("ByType{ValidAt}: %v", err)
			}
			assertRelSet(t, "ByType{ValidAt: t}", got, both)
			got, err = g.Rels.ByType("SPAN", storepkg.QueryOpts{ValidAt: b, TxAt: pin})
			if err != nil {
				t.Fatalf("ByType{ValidAt,TxAt}: %v", err)
			}
			assertRelSet(t, "ByType{ValidAt: t, TxAt: now}", got, both)
			got, err = g.Temporal.RelsAtTx(b, pin)
			if err != nil {
				t.Fatalf("RelsAtTx: %v", err)
			}
			assertRelSet(t, "RelsAtTx(t, now)", got, both)
			got, err = g.Temporal.OutgoingRelsAt(s.ID(), b)
			if err != nil {
				t.Fatalf("OutgoingRelsAt: %v", err)
			}
			assertRelSet(t, "OutgoingRelsAt(start, t)", got, both)
			got, err = g.Temporal.IncomingRelsAt(e.ID(), b)
			if err != nil {
				t.Fatalf("IncomingRelsAt: %v", err)
			}
			assertRelSet(t, "IncomingRelsAt(end, t)", got, both)
			snap, err := g.Temporal.Snapshot(b)
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			assertRelSet(t, "Snapshot(t).Relationships", snap.Relationships, both)

			got, err = g.Temporal.RelsDuring(b, b+2000)
			if err != nil {
				t.Fatalf("RelsDuring: %v", err)
			}
			assertRelSet(t, "RelsDuring[t, t+2000)", got, both)
			got, err = g.Rels.ByType("SPAN", storepkg.QueryOpts{ValidStart: b, ValidEnd: b + 2000})
			if err != nil {
				t.Fatalf("ByType{During}: %v", err)
			}
			assertRelSet(t, "ByType{ValidStart: t, ValidEnd: t+2000}", got, both)
			got, err = g.Temporal.RelsDuringTx(b, b+2000, pin)
			if err != nil {
				t.Fatalf("RelsDuringTx: %v", err)
			}
			assertRelSet(t, "RelsDuringTx[t, t+2000)", got, both)
			got, err = g.Temporal.RelsDuring(b+1, b+2000)
			if err != nil {
				t.Fatalf("RelsDuring(t+1): %v", err)
			}
			assertRelSet(t, "RelsDuring[t+1, t+2000)", got, onlyTwo)
			got, err = g.Temporal.RelsDuring(b-1000, b)
			if err != nil {
				t.Fatalf("RelsDuring(before): %v", err)
			}
			assertRelSet(t, "RelsDuring[t-1000, t)", got, nil)

			got, err = g.Temporal.RelsRelating(b, b+2000, types.AllRelations())
			if err != nil {
				t.Fatalf("RelsRelating: %v", err)
			}
			assertRelSet(t, "RelsRelating(all)", got, both)
			got, err = g.Temporal.RelsRelating(b, b+1, types.Equals.Set())
			if err != nil {
				t.Fatalf("RelsRelating(Equals): %v", err)
			}
			assertRelSet(t, "RelsRelating(Equals [t, t+1))", got, []types.RelID{one.ID()})

			_, err = g.Temporal.RelAt(one.ID(), b+1)
			wantNoVersion(t, "RelAt(one, t+1)", err)
			got, err = g.Temporal.RelsAt(b + 1)
			if err != nil {
				t.Fatalf("RelsAt(t+1): %v", err)
			}
			assertRelSet(t, "RelsAt(t+1)", got, onlyTwo)
			got, err = g.Rels.ByType("SPAN", storepkg.QueryOpts{ValidAt: b + 1})
			if err != nil {
				t.Fatalf("ByType{ValidAt: t+1}: %v", err)
			}
			assertRelSet(t, "ByType{ValidAt: t+1}", got, onlyTwo)
			got, err = g.Temporal.RelsAt(b + 2)
			if err != nil {
				t.Fatalf("RelsAt(t+2): %v", err)
			}
			assertRelSet(t, "RelsAt(t+2)", got, nil)
			_, err = g.Temporal.RelAt(one.ID(), b-1)
			wantNoVersion(t, "RelAt(one, t-1)", err)
		})
	}
}

// TestOneTickSpanVisible_CloseVersionAtVfPlusOne catches the lost close: a
// CloseVersion(id, vf+1) writes a closed row [vf, vf+1) that the resolver
// skipped as eclipsed, so the superseded OPEN row kept answering every later
// instant (NodeAt(vf+500) found the entity the caller had just closed). Two
// phases (rule 15): a pin before the close still sees the open belief. The
// width-2 control (close at vf+2) must behave the same way one tick later.
func TestOneTickSpanVisible_CloseVersionAtVfPlusOne(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		for _, width := range []types.Instant{1, 2} {
			name := be.name + "/width1"
			if width == 2 {
				name = be.name + "/width2"
			}
			t.Run(name+"/node", func(t *testing.T) {
				g := be.open(t, false)
				ctx := context.Background()
				vf := otsBase()
				n, err := g.Nodes.Add(ctx, []string{"Span"}, map[string]any{"tkg_valid_from": vf, "k": "x"})
				if err != nil {
					t.Fatalf("Add: %v", err)
				}
				// Phase 1: open from vf.
				if _, err := g.Temporal.NodeAt(n.ID(), vf+500); err != nil {
					t.Fatalf("pre-close NodeAt(vf+500): %v", err)
				}
				if err := g.Nodes.CloseVersion(ctx, n.ID(), vf+width); err != nil {
					t.Fatalf("CloseVersion(vf+%d): %v", width, err)
				}
				// Phase 2.
				got, err := g.Temporal.NodeAt(n.ID(), vf)
				if err != nil {
					t.Fatalf("NodeAt(vf): %v", err)
				}
				if tm := got.Temporal(); tm == nil || tm.ValidTo != vf+width {
					t.Errorf("NodeAt(vf) answered the pre-close row %+v, want ValidTo = vf+%d", got.Temporal(), width)
				}
				_, err = g.Temporal.NodeAt(n.ID(), vf+width)
				wantNoVersion(t, "NodeAt(vf+width) after close", err)
				_, err = g.Temporal.NodeAt(n.ID(), vf+500)
				wantNoVersion(t, "NodeAt(vf+500) after close (old open row still answering)", err)
				ns, err := g.Temporal.NodesAt(vf + 500)
				if err != nil {
					t.Fatalf("NodesAt: %v", err)
				}
				assertNodeSet(t, "NodesAt(vf+500) after close", ns, nil)
				ns, err = g.Nodes.ByLabel("Span", storepkg.QueryOpts{ValidAt: vf + 500})
				if err != nil {
					t.Fatalf("ByLabel: %v", err)
				}
				assertNodeSet(t, "ByLabel{ValidAt: vf+500} after close", ns, nil)
				ns, err = g.Nodes.ByLabel("Span", storepkg.QueryOpts{ValidAt: vf})
				if err != nil {
					t.Fatalf("ByLabel: %v", err)
				}
				assertNodeSet(t, "ByLabel{ValidAt: vf} after close", ns, []types.NodeID{n.ID()})
				// A pin just before the close row was recorded sees the open belief.
				closeTx := otsNewestNodeTx(t, g, n.ID())
				old, err := g.Temporal.NodeAtTx(n.ID(), vf+500, closeTx-1)
				if err != nil {
					t.Fatalf("NodeAtTx(vf+500, pin before close): %v", err)
				}
				if tm := old.Temporal(); tm != nil && tm.ValidTo != 0 {
					t.Fatalf("pin before close answered a closed row %+v", tm)
				}
			})
			t.Run(name+"/rel", func(t *testing.T) {
				g := be.open(t, false)
				ctx := context.Background()
				vf := otsBase()
				s, e := otsEndpoints(t, g, vf)
				r, err := g.Rels.AddByID(ctx, "SPAN", s.ID(), e.ID(), map[string]any{"tkg_valid_from": vf, "k": "x"})
				if err != nil {
					t.Fatalf("AddByID: %v", err)
				}
				if _, err := g.Temporal.RelAt(r.ID(), vf+500); err != nil {
					t.Fatalf("pre-close RelAt(vf+500): %v", err)
				}
				if err := g.Rels.CloseVersion(ctx, r.ID(), vf+width); err != nil {
					t.Fatalf("CloseVersion(vf+%d): %v", width, err)
				}
				got, err := g.Temporal.RelAt(r.ID(), vf)
				if err != nil {
					t.Fatalf("RelAt(vf): %v", err)
				}
				if tm := got.Temporal(); tm == nil || tm.ValidTo != vf+width {
					t.Errorf("RelAt(vf) answered the pre-close row %+v, want ValidTo = vf+%d", got.Temporal(), width)
				}
				_, err = g.Temporal.RelAt(r.ID(), vf+width)
				wantNoVersion(t, "RelAt(vf+width) after close", err)
				_, err = g.Temporal.RelAt(r.ID(), vf+500)
				wantNoVersion(t, "RelAt(vf+500) after close (old open row still answering)", err)
				rs, err := g.Temporal.RelsAt(vf + 500)
				if err != nil {
					t.Fatalf("RelsAt: %v", err)
				}
				assertRelSet(t, "RelsAt(vf+500) after close", rs, nil)
				rs, err = g.Rels.ByType("SPAN", storepkg.QueryOpts{ValidAt: vf + 500})
				if err != nil {
					t.Fatalf("ByType: %v", err)
				}
				assertRelSet(t, "ByType{ValidAt: vf+500} after close", rs, nil)
				rs, err = g.Rels.ByType("SPAN", storepkg.QueryOpts{ValidAt: vf})
				if err != nil {
					t.Fatalf("ByType: %v", err)
				}
				assertRelSet(t, "ByType{ValidAt: vf} after close", rs, []types.RelID{r.ID()})
				closeTx := otsNewestRelTx(t, g, r.ID())
				old, err := g.Temporal.RelAtTx(r.ID(), vf+500, closeTx-1)
				if err != nil {
					t.Fatalf("RelAtTx(vf+500, pin before close): %v", err)
				}
				if tm := old.Temporal(); tm != nil && tm.ValidTo != 0 {
					t.Fatalf("pin before close answered a closed row %+v", tm)
				}
			})
		}
	}
}

// otsNewestNodeTx returns the highest TxFrom in the node's chain.
func otsNewestNodeTx(t *testing.T, g *Core, id types.NodeID) types.Instant {
	t.Helper()
	var hi types.Instant
	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if cur, err := g.Nodes.Get(context.Background(), id); err == nil {
		hist = append(hist, cur)
	}
	for _, h := range hist {
		if tm := h.Temporal(); tm != nil && tm.TxFrom > hi {
			hi = tm.TxFrom
		}
	}
	return hi
}

// otsNewestRelTx mirrors otsNewestNodeTx.
func otsNewestRelTx(t *testing.T, g *Core, id types.RelID) types.Instant {
	t.Helper()
	var hi types.Instant
	for _, r := range txbChain(t, g, id) {
		if tm := r.Temporal(); tm != nil && tm.TxFrom > hi {
			hi = tm.TxFrom
		}
	}
	return hi
}

// TestOneTickSpanVisible_DeleteAtVfPlusOne catches a hard delete whose instant
// lands at vf+1 hiding the deleted entity's whole history: the tombstone clamps
// ValidTo to vf+1, the row becomes [vf, vf+1), and the resolver skipped it as
// eclipsed, so NodeAt(vf) reported "no version" for an entity that existed then
// (B32: history stays queryable after deletion). Two phases: a pin at vf (the
// delete not yet recorded) sees the open belief at vf+500.
func TestOneTickSpanVisible_DeleteAtVfPlusOne(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name+"/node", func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			vf := otsBase()
			n, err := g.Nodes.AddWithTx(ctx, []string{"Span"}, map[string]any{"tkg_valid_from": vf, "k": "x"}, vf)
			if err != nil {
				t.Fatalf("AddWithTx: %v", err)
			}
			if err := g.Nodes.DeleteWithTx(ctx, n.ID(), vf+1); err != nil {
				t.Fatalf("DeleteWithTx(vf+1): %v", err)
			}
			if got, err := g.Temporal.NodeAt(n.ID(), vf); err != nil {
				t.Errorf("NodeAt(vf) after delete at vf+1: %v (history row hidden)", err)
			} else if otsNodeV(t, got, "k") != "x" {
				t.Errorf("NodeAt(vf).k = %v", otsNodeV(t, got, "k"))
			}
			_, err = g.Temporal.NodeAt(n.ID(), vf+1)
			wantNoVersion(t, "NodeAt(vf+1) after delete", err)
			ns, err := g.Temporal.NodesAt(vf)
			if err != nil {
				t.Fatalf("NodesAt: %v", err)
			}
			assertNodeSet(t, "NodesAt(vf) after delete", ns, []types.NodeID{n.ID()})
			ns, err = g.Nodes.ByLabel("Span", storepkg.QueryOpts{ValidAt: vf})
			if err != nil {
				t.Fatalf("ByLabel: %v", err)
			}
			assertNodeSet(t, "ByLabel{ValidAt: vf} after delete", ns, []types.NodeID{n.ID()})
			ns, err = g.Temporal.NodesDuring(vf, vf+2000)
			if err != nil {
				t.Fatalf("NodesDuring: %v", err)
			}
			assertNodeSet(t, "NodesDuring[vf, vf+2000) after delete", ns, []types.NodeID{n.ID()})
			if _, err := g.Temporal.NodeAtTx(n.ID(), vf+500, vf); err != nil {
				t.Errorf("NodeAtTx(vf+500, pin vf): %v (pre-delete belief lost)", err)
			}
		})
		t.Run(be.name+"/rel", func(t *testing.T) {
			g := be.open(t, true)
			ctx := context.Background()
			vf := otsBase()
			s, e := otsEndpoints(t, g, vf)
			r, err := g.Rels.AddWithTx(ctx, "SPAN", s, e, map[string]any{"tkg_valid_from": vf, "k": "x"}, vf)
			if err != nil {
				t.Fatalf("AddWithTx: %v", err)
			}
			if err := g.Rels.DeleteWithTx(ctx, r.ID(), vf+1); err != nil {
				t.Fatalf("DeleteWithTx(vf+1): %v", err)
			}
			if got, err := g.Temporal.RelAt(r.ID(), vf); err != nil {
				t.Errorf("RelAt(vf) after delete at vf+1: %v (history row hidden)", err)
			} else if otsRelV(t, got, "k") != "x" {
				t.Errorf("RelAt(vf).k = %v", otsRelV(t, got, "k"))
			}
			_, err = g.Temporal.RelAt(r.ID(), vf+1)
			wantNoVersion(t, "RelAt(vf+1) after delete", err)
			rs, err := g.Temporal.RelsAt(vf)
			if err != nil {
				t.Fatalf("RelsAt: %v", err)
			}
			assertRelSet(t, "RelsAt(vf) after delete", rs, []types.RelID{r.ID()})
			rs, err = g.Rels.ByType("SPAN", storepkg.QueryOpts{ValidAt: vf})
			if err != nil {
				t.Fatalf("ByType: %v", err)
			}
			assertRelSet(t, "ByType{ValidAt: vf} after delete", rs, []types.RelID{r.ID()})
			rs, err = g.Temporal.RelsDuring(vf, vf+2000)
			if err != nil {
				t.Fatalf("RelsDuring: %v", err)
			}
			assertRelSet(t, "RelsDuring[vf, vf+2000) after delete", rs, []types.RelID{r.ID()})
			if _, err := g.Temporal.RelAtTx(r.ID(), vf+500, vf); err != nil {
				t.Errorf("RelAtTx(vf+500, pin vf): %v (pre-delete belief lost)", err)
			}
		})
	}
}

// TestOneTickSpanVisible_CascadeWidthOnePiece catches a width-1 cascade piece
// being invisible: SetNodeVersionInterval(id, t, t+1, patch) appends a [t, t+1)
// correction row that the resolver skipped, so NodeAt(t) answered the
// uncorrected base. The piece must answer at t, the base at t-1 and t+1, and
// the generic door must agree. Two phases (rule 15): before the correction t
// reads the base; after it, a pin older than the correction still reads the
// base (lesson 46 — the correction is a new belief, never a rewrite) while a
// pin at the correction reads the piece.
//
// The "boundary" case catches the same skip on a piece the CORRECTION-BASE
// split produces: a pre-existing own boundary at t+1 cuts a wide patch
// [t, t+2000) into [t, t+1) and [t+1, t+2000); the 1-wide first piece was lost,
// so the patched key vanished at t.
func TestOneTickSpanVisible_CascadeWidthOnePiece(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name+"/node", func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			b := otsBase()
			n, err := g.Nodes.Add(ctx, []string{"Span"}, map[string]any{"tkg_valid_from": b - 10_000, "v": int64(1)})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			// Phase 1: base value at t.
			if got, err := g.Temporal.NodeAt(n.ID(), b); err != nil || otsNodeV(t, got, "v") != int64(1) {
				t.Fatalf("pre-correction NodeAt(t) = %v, %v", got, err)
			}
			pre, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), b, b+1, map[string]any{"v": int64(2)}); err != nil {
				t.Fatalf("SetNodeVersionInterval: %v", err)
			}
			post, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			for _, c := range []struct {
				at   types.Instant
				want int64
			}{{b - 1, 1}, {b, 2}, {b + 1, 1}, {b + 500, 1}} {
				got, err := g.Temporal.NodeAt(n.ID(), c.at)
				if err != nil {
					t.Fatalf("NodeAt(t%+d): %v", c.at-b, err)
				}
				if v := otsNodeV(t, got, "v"); v != c.want {
					t.Errorf("NodeAt(t%+d).v = %v, want %d", c.at-b, v, c.want)
				}
				ns, err := g.Nodes.ByLabel("Span", storepkg.QueryOpts{ValidAt: c.at})
				if err != nil || len(ns) != 1 {
					t.Fatalf("ByLabel{ValidAt: t%+d} = %d rows, %v", c.at-b, len(ns), err)
				}
				if v := otsNodeV(t, ns[0], "v"); v != c.want {
					t.Errorf("ByLabel{ValidAt: t%+d}.v = %v, want %d", c.at-b, v, c.want)
				}
			}
			ns, err := g.Temporal.NodesDuring(b, b+1)
			if err != nil || len(ns) != 1 || otsNodeV(t, ns[0], "v") != int64(2) {
				t.Errorf("NodesDuring[t, t+1) = %v, %v; want the piece (v=2)", ns, err)
			}
			// Old pin: the uncorrected belief. New pin: the piece.
			got, err := g.Temporal.NodeAtTx(n.ID(), b, pre)
			if err != nil || otsNodeV(t, got, "v") != int64(1) {
				t.Errorf("NodeAtTx(t, pin before correction) = %v, %v; want v=1", got, err)
			}
			got, err = g.Temporal.NodeAtTx(n.ID(), b, post)
			if err != nil || otsNodeV(t, got, "v") != int64(2) {
				t.Errorf("NodeAtTx(t, pin after correction) = %v, %v; want v=2", got, err)
			}
		})
		t.Run(be.name+"/rel", func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			b := otsBase()
			s, e := otsEndpoints(t, g, b)
			r, err := g.Rels.AddByID(ctx, "SPAN", s.ID(), e.ID(), map[string]any{"tkg_valid_from": b - 10_000, "v": int64(1)})
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			if got, err := g.Temporal.RelAt(r.ID(), b); err != nil || otsRelV(t, got, "v") != int64(1) {
				t.Fatalf("pre-correction RelAt(t) = %v, %v", got, err)
			}
			pre, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), b, b+1, map[string]any{"v": int64(2)}); err != nil {
				t.Fatalf("SetRelVersionInterval: %v", err)
			}
			post, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			for _, c := range []struct {
				at   types.Instant
				want int64
			}{{b - 1, 1}, {b, 2}, {b + 1, 1}, {b + 500, 1}} {
				got, err := g.Temporal.RelAt(r.ID(), c.at)
				if err != nil {
					t.Fatalf("RelAt(t%+d): %v", c.at-b, err)
				}
				if v := otsRelV(t, got, "v"); v != c.want {
					t.Errorf("RelAt(t%+d).v = %v, want %d", c.at-b, v, c.want)
				}
				rs, err := g.Rels.ByType("SPAN", storepkg.QueryOpts{ValidAt: c.at})
				if err != nil || len(rs) != 1 {
					t.Fatalf("ByType{ValidAt: t%+d} = %d rows, %v", c.at-b, len(rs), err)
				}
				if v := otsRelV(t, rs[0], "v"); v != c.want {
					t.Errorf("ByType{ValidAt: t%+d}.v = %v, want %d", c.at-b, v, c.want)
				}
			}
			rs, err := g.Temporal.RelsDuring(b, b+1)
			if err != nil || len(rs) != 1 || otsRelV(t, rs[0], "v") != int64(2) {
				t.Errorf("RelsDuring[t, t+1) = %v, %v; want the piece (v=2)", rs, err)
			}
			got, err := g.Temporal.RelAtTx(r.ID(), b, pre)
			if err != nil || otsRelV(t, got, "v") != int64(1) {
				t.Errorf("RelAtTx(t, pin before correction) = %v, %v; want v=1", got, err)
			}
			got, err = g.Temporal.RelAtTx(r.ID(), b, post)
			if err != nil || otsRelV(t, got, "v") != int64(2) {
				t.Errorf("RelAtTx(t, pin after correction) = %v, %v; want v=2", got, err)
			}
		})
		t.Run(be.name+"/node/boundary_split", func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			b := otsBase()
			n, err := g.Nodes.Add(ctx, []string{"Span"}, map[string]any{"tkg_valid_from": b - 10_000, "v": int64(1)})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			// An own boundary at t+1: v=3 from t+1 on.
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), b+1, 0, map[string]any{"v": int64(3)}); err != nil {
				t.Fatalf("SetNodeVersionInterval(t+1, open): %v", err)
			}
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), b, b+2000, map[string]any{"w": "x"}); err != nil {
				t.Fatalf("SetNodeVersionInterval(t, t+2000): %v", err)
			}
			for _, c := range []struct {
				at    types.Instant
				wantV int64
				wantW any
			}{{b - 1, 1, nil}, {b, 1, "x"}, {b + 1, 3, "x"}, {b + 2000, 3, nil}} {
				got, err := g.Temporal.NodeAt(n.ID(), c.at)
				if err != nil {
					t.Fatalf("NodeAt(t%+d): %v", c.at-b, err)
				}
				if v, w := otsNodeV(t, got, "v"), otsNodeV(t, got, "w"); v != c.wantV || w != c.wantW {
					t.Errorf("NodeAt(t%+d) = v:%v w:%v, want v:%d w:%v", c.at-b, v, w, c.wantV, c.wantW)
				}
			}
		})
		t.Run(be.name+"/rel/boundary_split", func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			b := otsBase()
			s, e := otsEndpoints(t, g, b)
			r, err := g.Rels.AddByID(ctx, "SPAN", s.ID(), e.ID(), map[string]any{"tkg_valid_from": b - 10_000, "v": int64(1)})
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), b+1, 0, map[string]any{"v": int64(3)}); err != nil {
				t.Fatalf("SetRelVersionInterval(t+1, open): %v", err)
			}
			if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), b, b+2000, map[string]any{"w": "x"}); err != nil {
				t.Fatalf("SetRelVersionInterval(t, t+2000): %v", err)
			}
			for _, c := range []struct {
				at    types.Instant
				wantV int64
				wantW any
			}{{b - 1, 1, nil}, {b, 1, "x"}, {b + 1, 3, "x"}, {b + 2000, 3, nil}} {
				got, err := g.Temporal.RelAt(r.ID(), c.at)
				if err != nil {
					t.Fatalf("RelAt(t%+d): %v", c.at-b, err)
				}
				if v, w := otsRelV(t, got, "v"), otsRelV(t, got, "w"); v != c.wantV || w != c.wantW {
					t.Errorf("RelAt(t%+d) = v:%v w:%v, want v:%d w:%v", c.at-b, v, w, c.wantV, c.wantW)
				}
			}
		})
	}
}

// TestCascadeTemplate_DeletedEntityUsesNewestHistoryRow pins the gap-piece
// template once the eclipse filter is gone: on a hard-deleted entity (no
// current row) the template is the NEWEST history row. A correction before the
// entity's first valid-from is a gap piece whose content is the template plus
// the patch, so it must carry v=2 (the last update), not v=1 — catching a
// template taken from history[0], or a cascade that refuses a history-only
// entity.
func TestCascadeTemplate_DeletedEntityUsesNewestHistoryRow(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name+"/node", func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			b := otsBase()
			n, err := g.Nodes.Add(ctx, []string{"Span"}, map[string]any{"tkg_valid_from": b - 10_000, "v": int64(1)})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(2)}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), b-20_000, b-15_000, map[string]any{"w": "x"}); err != nil {
				t.Fatalf("SetNodeVersionInterval on a deleted node: %v", err)
			}
			got, err := g.Temporal.NodeAt(n.ID(), b-17_000)
			if err != nil {
				t.Fatalf("NodeAt(gap piece): %v", err)
			}
			if v, w := otsNodeV(t, got, "v"), otsNodeV(t, got, "w"); v != int64(2) || w != "x" {
				t.Fatalf("gap piece = v:%v w:%v, want v:2 w:x (template = newest history row)", v, w)
			}
			_, err = g.Temporal.NodeAt(n.ID(), b-15_000)
			wantNoVersion(t, "NodeAt(gap piece end)", err)
		})
		t.Run(be.name+"/rel", func(t *testing.T) {
			g := be.open(t, false)
			ctx := context.Background()
			b := otsBase()
			s, e := otsEndpoints(t, g, b-30_000)
			r, err := g.Rels.AddByID(ctx, "SPAN", s.ID(), e.ID(), map[string]any{"tkg_valid_from": b - 10_000, "v": int64(1)})
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(2)}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), b-20_000, b-15_000, map[string]any{"w": "x"}); err != nil {
				t.Fatalf("SetRelVersionInterval on a deleted rel: %v", err)
			}
			got, err := g.Temporal.RelAt(r.ID(), b-17_000)
			if err != nil {
				t.Fatalf("RelAt(gap piece): %v", err)
			}
			if v, w := otsRelV(t, got, "v"), otsRelV(t, got, "w"); v != int64(2) || w != "x" {
				t.Fatalf("gap piece = v:%v w:%v, want v:2 w:x (template = newest history row)", v, w)
			}
			_, err = g.Temporal.RelAt(r.ID(), b-15_000)
			wantNoVersion(t, "RelAt(gap piece end)", err)
		})
	}
}
