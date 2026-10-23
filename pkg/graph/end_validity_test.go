package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	graphtemporal "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EndValidity (sigma-tkgd 2026-10-18): the delete that ends validity at its
// instant T and keeps the valid past, on every valid axis — the tombstone the
// legacy axis's Delete writes (DeletedAt = TxTo = T, ValidTo = T where open,
// not Retracted). Doors: Nodes().EndValidity (cascading to the node's
// relationships), Rels().EndValidity, GraphTx EndNodeValidity /
// EndRelValidity. Both axes, every backend.

type evMode struct {
	name string
	mode graphpkg.UntimedValidStart
}

var evModes = []evMode{
	{"legacy-txfrom", graphpkg.UntimedFromTxFrom},
	{"legacy-unbounded", graphpkg.UntimedUnbounded},
	{"whole-axis", graphpkg.UntimedWholeAxis},
}

func evNodeIDs(ns []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, 0, len(ns))
	for _, n := range ns {
		ids = append(ids, n.ID())
	}
	slices.Sort(ids)
	return ids
}

func evRelIDs(rs []*types.Relationship) []types.RelID {
	ids := make([]types.RelID, 0, len(rs))
	for _, r := range rs {
		ids = append(ids, r.ID())
	}
	slices.Sort(ids)
	return ids
}

func evWantNodes(t *testing.T, what string, got []*types.Node, want ...types.NodeID) {
	t.Helper()
	slices.Sort(want)
	if g := evNodeIDs(got); !slices.Equal(g, want) {
		t.Errorf("%s: %v, want %v", what, g, want)
	}
}

func evWantRels(t *testing.T, what string, got []*types.Relationship, want ...types.RelID) {
	t.Helper()
	slices.Sort(want)
	if g := evRelIDs(got); !slices.Equal(g, want) {
		t.Errorf("%s: %v, want %v", what, g, want)
	}
}

// evTombstone checks the newest history row of a deleted entity: the legacy
// Delete tombstone (not Retracted, ValidTo == DeletedAt == TxTo).
func evTombstone(t *testing.T, what string, tm *types.TemporalMetadata, retracted bool) types.Instant {
	t.Helper()
	if tm == nil || tm.DeletedAt == 0 {
		t.Fatalf("%s: no tombstone (%+v)", what, tm)
	}
	if tm.Retracted != retracted {
		t.Errorf("%s: Retracted = %v, want %v", what, tm.Retracted, retracted)
	}
	if !retracted && (tm.ValidTo != tm.DeletedAt || tm.TxTo != tm.DeletedAt || !tm.DeleteWroteValidTo()) {
		t.Errorf("%s: tombstone ValidTo %d DeletedAt %d TxTo %d, want ValidTo == DeletedAt == TxTo", what, tm.ValidTo, tm.DeletedAt, tm.TxTo)
	}
	return tm.DeletedAt
}

func evSorted(ids []types.NodeID) []types.NodeID {
	slices.Sort(ids)
	return ids
}

func newestNodeRow(t *testing.T, g *graphpkg.Graph, id types.NodeID) *types.Node {
	t.Helper()
	h, err := g.Nodes().History(id)
	if err != nil || len(h) == 0 {
		t.Fatalf("history of %v: %d rows, %v", id, len(h), err)
	}
	return slices.MaxFunc(h, func(a, b *types.Node) int { return int(a.Version()) - int(b.Version()) })
}

func newestRelRow(t *testing.T, g *graphpkg.Graph, id types.RelID) *types.Relationship {
	t.Helper()
	h, err := g.Rels().History(id)
	if err != nil || len(h) == 0 {
		t.Fatalf("history of %v: %d rows, %v", id, len(h), err)
	}
	return slices.MaxFunc(h, func(a, b *types.Relationship) int { return int(a.Version()) - int(b.Version()) })
}

func TestEndValidityKeepsThePast(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, m := range evModes {
		for _, door := range []string{"api", "tx"} {
			for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.UntimedValidStart = m.mode }) {
				t.Run(m.name+"/"+door+"/"+b.name, func(t *testing.T) {
					g := b.open(t)
					// n (untimed, k=1), keep (untimed, k=1, never deleted),
					// timed (k=1 from valid 100); r: n -> keep, s: keep -> timed.
					n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"k": int64(1)})
					if err != nil {
						t.Fatal(err)
					}
					keep, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"k": int64(1)})
					if err != nil {
						t.Fatal(err)
					}
					timed, err := g.Nodes().Add(ctx, []string{"Q"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(100)})
					if err != nil {
						t.Fatal(err)
					}
					r, err := g.Rels().Add(ctx, "E", n, keep, map[string]any{"k": int64(1)})
					if err != nil {
						t.Fatal(err)
					}
					s, err := g.Rels().Add(ctx, "F", keep, timed, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(100)})
					if err != nil {
						t.Fatal(err)
					}
					vBefore := r11NowAfter(t, g) // a valid instant (and pin) before the deletes
					endNode := func(id types.NodeID) error { return g.Nodes().EndValidity(ctx, id) }
					endRel := func(id types.RelID) error { return g.Rels().EndValidity(ctx, id) }
					if door == "tx" {
						endNode = func(id types.NodeID) error {
							return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.EndNodeValidity(id) })
						}
						endRel = func(id types.RelID) error {
							return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.EndRelValidity(id) })
						}
					}
					if err := endNode(n.ID()); err != nil {
						t.Fatalf("end node: %v", err)
					}
					if err := endRel(s.ID()); err != nil {
						t.Fatalf("end rel: %v", err)
					}
					if err := endNode(timed.ID()); err != nil {
						t.Fatalf("end timed node: %v", err)
					}
					after := r11NowAfter(t, g)

					// Tombstones: the legacy Delete tombstone, cascaded to r.
					tn := evTombstone(t, "node", newestNodeRow(t, g, n.ID()).Temporal(), false)
					tr := evTombstone(t, "cascaded rel", newestRelRow(t, g, r.ID()).Temporal(), false)
					evTombstone(t, "rel", newestRelRow(t, g, s.ID()).Temporal(), false)
					evTombstone(t, "timed node", newestNodeRow(t, g, timed.ID()).Temporal(), false)
					if tn != tr {
						t.Errorf("cascade instants: node %d, rel %d", tn, tr)
					}

					// Current state: gone.
					if _, err := g.Nodes().Get(ctx, n.ID()); !errors.Is(err, graphpkg.ErrNodeNotFound) {
						t.Errorf("Get after EndValidity: %v, want ErrNodeNotFound", err)
					}
					if _, err := g.Rels().Get(ctx, r.ID()); !errors.Is(err, graphpkg.ErrRelNotFound) {
						t.Errorf("Get cascaded rel: %v, want ErrRelNotFound", err)
					}
					ps, err := g.Nodes().ByLabel("P", storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "ByLabel P now", ps, keep.ID())
					pk, err := g.Nodes().ByLabelAndProperty("P", "k", int64(1), storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "ByLabelAndProperty now", pk, keep.ID())
					if c, err := g.Nodes().CountByLabel("P"); err != nil || c != 1 {
						t.Errorf("CountByLabel P = %d, %v; want 1", c, err)
					}
					if c, err := g.Nodes().Count(); err != nil || c != 1 {
						t.Errorf("Count = %d, %v; want 1", c, err)
					}
					if c, err := g.Rels().Count(); err != nil || c != 0 {
						t.Errorf("rel Count = %d, %v; want 0", c, err)
					}
					es, err := g.Rels().ByType("E", storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					evWantRels(t, "ByType E now", es)

					// The valid past at the current belief: kept.
					for _, c := range []struct {
						v    types.Instant
						want string
					}{{vBefore, "1"}, {tn - 1, "1"}, {tn, "none"}, {r11Far, "none"}} {
						r11Want(t, fmt.Sprintf("node AT %d now", c.v), r11KAt(t, g, n.ID(), "k", c.v, 0), c.want)
						r11Want(t, fmt.Sprintf("node AT %d pin after", c.v), r11KAt(t, g, n.ID(), "k", c.v, after), c.want)
						r11Want(t, fmt.Sprintf("cascaded rel AT %d now", c.v), r11RelKAt(t, g, r.ID(), "k", c.v, 0), c.want)
					}
					r11Want(t, "timed node AT 150", r11KAt(t, g, timed.ID(), "k", 150, 0), "1")
					r11Want(t, "timed node AT 50", r11KAt(t, g, timed.ID(), "k", 50, 0), "none")
					r11Want(t, "timed node AT far", r11KAt(t, g, timed.ID(), "k", r11Far, 0), "none")
					r11Want(t, "rel s AT 150", r11RelKAt(t, g, s.ID(), "k", 150, 0), "1")
					r11Want(t, "rel s AT far", r11RelKAt(t, g, s.ID(), "k", r11Far, 0), "none")
					// Pinned before the delete: open, as before it.
					r11Want(t, "node AT far pin before", r11KAt(t, g, n.ID(), "k", r11Far, vBefore), "1")
					r11Want(t, "rel s AT far pin before", r11RelKAt(t, g, s.ID(), "k", r11Far, vBefore), "1")

					// Generic and interval doors at a valid instant before T.
					pv, err := g.Nodes().ByLabel("P", storepkg.QueryOpts{ValidAt: vBefore})
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "ByLabel P AT before", pv, n.ID(), keep.ID())
					pf, err := g.Nodes().ByLabel("P", storepkg.QueryOpts{ValidAt: r11Far})
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "ByLabel P AT far", pf, keep.ID())
					pkv, err := g.Nodes().ByLabelAndProperty("P", "k", int64(1), storepkg.QueryOpts{ValidAt: vBefore})
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "ByLabelAndProperty AT before", pkv, n.ID(), keep.ID())
					ev, err := g.Rels().ByType("E", storepkg.QueryOpts{ValidAt: vBefore})
					if err != nil {
						t.Fatal(err)
					}
					evWantRels(t, "ByType E AT before", ev, r.ID())
					dur, err := g.Temporal().NodesDuring(vBefore, vBefore+1)
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "NodesDuring before", dur, n.ID(), keep.ID(), timed.ID())
					durFar, err := g.Temporal().NodesDuring(r11Far, r11Far+1)
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "NodesDuring far", durFar, keep.ID())
					rdur, err := g.Temporal().RelsDuring(vBefore, vBefore+1)
					if err != nil {
						t.Fatal(err)
					}
					evWantRels(t, "RelsDuring before", rdur, r.ID(), s.ID())

					// The effective timeline at the current pin ends at T.
					segs, err := g.Temporal().NodeEffectiveTimeline(n.ID(), after)
					if err != nil {
						t.Fatal(err)
					}
					if len(segs) != 1 || segs[0].ValidTo != tn {
						t.Errorf("effective timeline after EndValidity: %+v, want one segment ending at %d", segs, tn)
					}
					rsegs, err := g.Temporal().RelEffectiveTimeline(r.ID(), after)
					if err != nil {
						t.Fatal(err)
					}
					if len(rsegs) != 1 || rsegs[0].ValidTo != tr {
						t.Errorf("rel effective timeline after EndValidity: %+v, want one segment ending at %d", rsegs, tr)
					}

					// Record doors: absent at a pin after T, present before.
					asofAfter, err := g.Temporal().NodesAsOf(after)
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "NodesAsOf after", asofAfter, keep.ID())
					asofBefore, err := g.Temporal().NodesAsOf(vBefore)
					if err != nil {
						t.Fatal(err)
					}
					evWantNodes(t, "NodesAsOf before", asofBefore, n.ID(), keep.ID(), timed.ID())
					// The effective scan by label and the effective adjacency.
					var scanned []types.NodeID
					if err := g.Temporal().ForEachNodeEffectiveByLabel("P", after, func(sg graphtemporal.NodeSegment) bool {
						if sg.Node.ID() == n.ID() && sg.ValidTo != tn {
							t.Errorf("effective scan: n's segment ends at %d, want %d", sg.ValidTo, tn)
						}
						scanned = append(scanned, sg.Node.ID())
						return true
					}); err != nil {
						t.Fatal(err)
					}
					slices.Sort(scanned)
					if want := []types.NodeID{n.ID(), keep.ID()}; !slices.Equal(slices.Compact(scanned), evSorted(want)) {
						t.Errorf("ForEachNodeEffectiveByLabel P: %v, want %v", scanned, want)
					}
					out, err := g.Temporal().OutgoingRelsAt(n.ID(), vBefore)
					if err != nil {
						t.Fatal(err)
					}
					evWantRels(t, "OutgoingRelsAt before", out, r.ID())
					out, err = g.Temporal().OutgoingRelsAt(keep.ID(), r11Far)
					if err != nil && !errors.Is(err, graphpkg.ErrNoVersionValidAt) {
						t.Fatal(err)
					}
					evWantRels(t, "OutgoingRelsAt keep far", out)

					// Errors as Delete.
					if err := g.Nodes().EndValidity(ctx, n.ID()); !errors.Is(err, graphpkg.ErrNodeNotFound) {
						t.Errorf("EndValidity of a deleted node: %v, want ErrNodeNotFound", err)
					}
					if err := g.Rels().EndValidity(ctx, r.ID()); !errors.Is(err, graphpkg.ErrRelNotFound) {
						t.Errorf("EndValidity of a deleted rel: %v, want ErrRelNotFound", err)
					}
				})
			}
		}
	}
}

// TestEndValidityLegacyIsDelete: on a legacy-axis graph EndValidity writes
// Delete's rows (same fields apart from the instants), and on a whole-axis
// graph Delete stays a retraction while EndValidity is not.
func TestEndValidityLegacyIsDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, m := range evModes {
		for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.UntimedValidStart = m.mode }) {
			t.Run(m.name+"/"+b.name, func(t *testing.T) {
				g := b.open(t)
				mk := func() (types.NodeID, types.RelID) {
					a, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(100)})
					if err != nil {
						t.Fatal(err)
					}
					z, err := g.Nodes().Add(ctx, []string{"P"}, nil)
					if err != nil {
						t.Fatal(err)
					}
					r, err := g.Rels().Add(ctx, "E", a, z, nil)
					if err != nil {
						t.Fatal(err)
					}
					return a.ID(), r.ID()
				}
				dn, dr := mk()
				en, er := mk()
				if err := g.Nodes().Delete(ctx, dn); err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes().EndValidity(ctx, en); err != nil {
					t.Fatal(err)
				}
				del := newestNodeRow(t, g, dn).Temporal()
				end := newestNodeRow(t, g, en).Temporal()
				delR := newestRelRow(t, g, dr).Temporal()
				endR := newestRelRow(t, g, er).Temporal()
				evTombstone(t, "EndValidity node", end, false)
				evTombstone(t, "EndValidity rel", endR, false)
				wholeAxis := m.mode == graphpkg.UntimedWholeAxis
				evTombstone(t, "Delete node", del, wholeAxis)
				evTombstone(t, "Delete rel", delR, wholeAxis)
				if !wholeAxis {
					norm := func(tm types.TemporalMetadata) types.TemporalMetadata {
						tm.TxFrom, tm.TxTo, tm.DeletedAt, tm.ValidTo, tm.CreatedAt, tm.UpdatedAt = 0, 0, 0, 0, 0, 0
						if tm.ValidFrom > 100 { // UntimedFromTxFrom: no claim is stored as 0 for both
							tm.ValidFrom = 0
						}
						return tm
					}
					if a, b := norm(*del), norm(*end); fmt.Sprintf("%+v", a) != fmt.Sprintf("%+v", b) {
						t.Errorf("legacy axis: Delete tombstone %+v, EndValidity tombstone %+v", a, b)
					}
					if a, b := norm(*delR), norm(*endR); fmt.Sprintf("%+v", a) != fmt.Sprintf("%+v", b) {
						t.Errorf("legacy axis: Delete rel tombstone %+v, EndValidity rel tombstone %+v", a, b)
					}
				}
				r11Want(t, "EndValidity: AT 150 now", r11KAt(t, g, en, "k", 150, 0), "1")
				want := "1"
				if wholeAxis {
					want = "none" // Delete is a retraction there
				}
				r11Want(t, "Delete: AT 150 now", r11KAt(t, g, dn, "k", 150, 0), want)
			})
		}
	}
}
