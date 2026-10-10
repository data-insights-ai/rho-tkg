package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 43 (retraction) through the pkg/graph facade: every public
// retraction door on every backend. The read semantics (absent at every valid
// time at pins >= T, unchanged before T) are the door matrix of
// internal/core/retraction_doors_test.go; these tests pin the contract around
// it: the marker in History, HasHistory, LatestStamps, the refusals, the
// caller-instant gates, rollback, units, constraints, lifecycles, replicas.

type pubRetractDoor struct {
	name   string
	withTx bool
	node   func(id types.NodeID, at types.Instant) error
	rel    func(id types.RelID, at types.Instant) error
}

func pubBatch(g *graphpkg.Graph, q func(bb *graphpkg.BatchBuilder) error) error {
	res, err := g.Batch().Run(q)
	if err != nil && res != nil && len(res.Errors) > 0 {
		return res.Errors[0].Err
	}
	return err
}

func pubSession(g *graphpkg.Graph, concurrent bool, q func(s *ingest.Session) error) error {
	s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, Concurrent: concurrent})
	if err != nil {
		return err
	}
	qErr := q(s)
	_, subErr := s.Submit()
	if cErr := s.Close(); cErr != nil {
		return cErr
	}
	if qErr != nil {
		return qErr
	}
	return subErr
}

func pubRetractDoors(g *graphpkg.Graph) []pubRetractDoor {
	ctx := context.Background()
	out := []pubRetractDoor{
		{"Nodes/Rels.Retract", false,
			func(id types.NodeID, _ types.Instant) error { return g.Nodes().Retract(ctx, id) },
			func(id types.RelID, _ types.Instant) error { return g.Rels().Retract(ctx, id) }},
		{"Nodes/Rels.RetractWithTx", true,
			func(id types.NodeID, at types.Instant) error { return g.Nodes().RetractWithTx(ctx, id, at) },
			func(id types.RelID, at types.Instant) error { return g.Rels().RetractWithTx(ctx, id, at) }},
		{"GraphTx.Retract", false,
			func(id types.NodeID, _ types.Instant) error {
				return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.RetractNode(id) })
			},
			func(id types.RelID, _ types.Instant) error {
				return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.RetractRelationship(id) })
			}},
		{"GraphTx.RetractWithTx", true,
			func(id types.NodeID, at types.Instant) error {
				return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.RetractNodeWithTx(id, at) })
			},
			func(id types.RelID, at types.Instant) error {
				return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.RetractRelationshipWithTx(id, at) })
			}},
		{"BatchBuilder.Retract", false,
			func(id types.NodeID, _ types.Instant) error {
				return pubBatch(g, func(bb *graphpkg.BatchBuilder) error { return bb.RetractNode(id) })
			},
			func(id types.RelID, _ types.Instant) error {
				return pubBatch(g, func(bb *graphpkg.BatchBuilder) error { return bb.RetractRelationship(id) })
			}},
		{"BatchBuilder.RetractWithTx", true,
			func(id types.NodeID, at types.Instant) error {
				return pubBatch(g, func(bb *graphpkg.BatchBuilder) error { return bb.RetractNodeWithTx(id, at) })
			},
			func(id types.RelID, at types.Instant) error {
				return pubBatch(g, func(bb *graphpkg.BatchBuilder) error { return bb.RetractRelationshipWithTx(id, at) })
			}},
	}
	for _, concurrent := range []bool{false, true} {
		mode := fmt.Sprintf("Session(concurrent=%v)", concurrent)
		out = append(out,
			pubRetractDoor{mode + ".Retract", false,
				func(id types.NodeID, _ types.Instant) error {
					return pubSession(g, concurrent, func(s *ingest.Session) error { return s.RetractNode(id) })
				},
				func(id types.RelID, _ types.Instant) error {
					return pubSession(g, concurrent, func(s *ingest.Session) error { return s.RetractRelationship(id) })
				}},
			pubRetractDoor{mode + ".RetractWithTx", true,
				func(id types.NodeID, at types.Instant) error {
					return pubSession(g, concurrent, func(s *ingest.Session) error { return s.RetractNodeWithTx(id, at) })
				},
				func(id types.RelID, at types.Instant) error {
					return pubSession(g, concurrent, func(s *ingest.Session) error { return s.RetractRelationshipWithTx(id, at) })
				}},
		)
	}
	return out
}

// pubFixture adds a "Ref" node, an "Ev" node and a LINK between them (cross
// shard on tiered), each valid from an hour ago and updated once, so every
// entity has a history row.
type pubFixture struct {
	ref, ev types.NodeID
	rel     types.RelID
	vf      types.Instant
}

func newPubFixture(t *testing.T, g *graphpkg.Graph) pubFixture {
	t.Helper()
	ctx := context.Background()
	f := pubFixture{vf: types.Instant(time.Now().Add(-time.Hour).UnixMilli())}
	ref, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"k": "ref", "tkg_valid_from": f.vf})
	if err != nil {
		t.Fatalf("Add Ref: %v", err)
	}
	ev, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"k": "ev", "tkg_valid_from": f.vf})
	if err != nil {
		t.Fatalf("Add Ev: %v", err)
	}
	r, err := g.Rels().AddByID(ctx, "LINK", ref.ID(), ev.ID(), map[string]any{"w": int64(1), "tkg_valid_from": f.vf})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	if _, err := g.Nodes().Update(ctx, ev.ID(), map[string]any{"k": "ev2"}); err != nil {
		t.Fatalf("Update Ev: %v", err)
	}
	if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("Update LINK: %v", err)
	}
	f.ref, f.ev, f.rel = ref.ID(), ev.ID(), r.ID()
	return f
}

func newestNode(t *testing.T, g *graphpkg.Graph, id types.NodeID) *types.Node {
	t.Helper()
	h, err := g.Nodes().History(id)
	if err != nil || len(h) == 0 {
		t.Fatalf("Nodes().History(%v) = %d rows, %v", id, len(h), err)
	}
	best := h[0]
	for _, r := range h {
		if r.Version() > best.Version() {
			best = r
		}
	}
	return best
}

func newestRel(t *testing.T, g *graphpkg.Graph, id types.RelID) *types.Relationship {
	t.Helper()
	h, err := g.Rels().History(id)
	if err != nil || len(h) == 0 {
		t.Fatalf("Rels().History(%v) = %d rows, %v", id, len(h), err)
	}
	best := h[0]
	for _, r := range h {
		if r.Version() > best.Version() {
			best = r
		}
	}
	return best
}

func nowTx(t *testing.T, g *graphpkg.Graph) types.Instant {
	t.Helper()
	at, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	return at
}

// TestRetract_HistoryMarksTombstoneAndStampsReport: after a retraction History
// returns the tombstone with Temporal().Retracted == true — on the node AND on
// every relationship the node retraction cascades, all at ONE instant (the
// caller's t for the WithTx doors) — HasHistory stays true and LatestStamps
// reports deleted with txTo = T and the TxFrom it reported before. A plain
// Delete's tombstone carries false (the control). Catches: a door that writes
// a plain Delete, a cascade that marks the node but not its relationships or
// stamps them at another instant, a door that ignores or moves t, a
// LatestStamps / HasHistory that loses a retracted entity.
func TestRetract_HistoryMarksTombstoneAndStampsReport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			for _, d := range pubRetractDoors(g) {
				// Node retraction cascading to its relationship.
				f := newPubFixture(t, g)
				nFrom, _, _, err := g.Nodes().LatestStamps(f.ev)
				if err != nil {
					t.Fatalf("LatestStamps: %v", err)
				}
				at := nowTx(t, g)
				if err := d.node(f.ev, at); err != nil {
					t.Fatalf("%s node: %v", d.name, err)
				}
				nt, rt := newestNode(t, g, f.ev).Temporal(), newestRel(t, g, f.rel).Temporal()
				if !nt.Retracted || !rt.Retracted {
					t.Fatalf("%s: node marker %v, cascaded relationship marker %v; want both true", d.name, nt.Retracted, rt.Retracted)
				}
				if nt.DeletedAt != rt.DeletedAt || nt.TxTo != nt.DeletedAt || rt.TxTo != rt.DeletedAt {
					t.Fatalf("%s: node tombstone %+v and cascaded tombstone %+v are not one instant", d.name, *nt, *rt)
				}
				if d.withTx && nt.DeletedAt != at {
					t.Fatalf("%s: tombstone instant %d, want the caller's %d", d.name, nt.DeletedAt, at)
				}
				if has, err := g.Nodes().HasHistory(f.ev); err != nil || !has {
					t.Fatalf("%s: HasHistory = %v, %v; want true", d.name, has, err)
				}
				if has, err := g.Rels().HasHistory(f.rel); err != nil || !has {
					t.Fatalf("%s: Rels().HasHistory = %v, %v; want true", d.name, has, err)
				}
				from, to, deleted, err := g.Nodes().LatestStamps(f.ev)
				if err != nil || !deleted || to != nt.DeletedAt || from != nFrom {
					t.Fatalf("%s: LatestStamps = (%d, %d, %v, %v); want (%d, %d, true, nil)", d.name, from, to, deleted, err, nFrom, nt.DeletedAt)
				}
				if _, to, deleted, err := g.Rels().LatestStamps(f.rel); err != nil || !deleted || to != rt.DeletedAt {
					t.Fatalf("%s: Rels().LatestStamps = (_, %d, %v, %v); want txTo %d, deleted", d.name, to, deleted, err, rt.DeletedAt)
				}

				// Relationship retraction: the endpoints stay.
				f2 := newPubFixture(t, g)
				at = nowTx(t, g)
				if err := d.rel(f2.rel, at); err != nil {
					t.Fatalf("%s rel: %v", d.name, err)
				}
				rt2 := newestRel(t, g, f2.rel).Temporal()
				if !rt2.Retracted || (d.withTx && rt2.DeletedAt != at) {
					t.Fatalf("%s: relationship tombstone %+v; want the marker (at the caller's %d for WithTx)", d.name, *rt2, at)
				}
				if _, err := g.Nodes().Get(ctx, f2.ev); err != nil {
					t.Fatalf("%s: a relationship retraction removed its endpoint: %v", d.name, err)
				}
				if _, to, deleted, err := g.Rels().LatestStamps(f2.rel); err != nil || !deleted || to != rt2.DeletedAt {
					t.Fatalf("%s: Rels().LatestStamps = (_, %d, %v, %v); want txTo %d, deleted", d.name, to, deleted, err, rt2.DeletedAt)
				}
			}
			// Control: a plain Delete does not mark.
			f := newPubFixture(t, g)
			if err := g.Nodes().Delete(ctx, f.ev); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if newestNode(t, g, f.ev).Temporal().Retracted || newestRel(t, g, f.rel).Temporal().Retracted {
				t.Fatal("a plain Delete wrote a retraction marker")
			}
		})
	}
}

// TestRetract_RefusesUnknownDeletedAndRetracted: an ID that never existed is
// ErrNodeNotFound / ErrRelNotFound (and NOT ErrEntityDeleted); an ID already
// deleted or already retracted is refused with an error matching BOTH
// ErrEntityDeleted and the not-found sentinel (decision: no upgrade of a
// Delete's tombstone into a retraction — fail closed, additive later), and
// the refusal writes nothing. Catches: a second tombstone, a silent upgrade
// of a delete, a door that loses the classification.
func TestRetract_RefusesUnknownDeletedAndRetracted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			for _, d := range pubRetractDoors(g) {
				// Unknown IDs.
				err := d.node(g.Nodes().NextID(), nowTx(t, g))
				if !errors.Is(err, graphpkg.ErrNodeNotFound) || errors.Is(err, graphpkg.ErrEntityDeleted) {
					t.Fatalf("%s unknown node: err = %v; want ErrNodeNotFound, not ErrEntityDeleted", d.name, err)
				}
				err = d.rel(g.Rels().NextID(), nowTx(t, g))
				if !errors.Is(err, graphpkg.ErrRelNotFound) || errors.Is(err, graphpkg.ErrEntityDeleted) {
					t.Fatalf("%s unknown relationship: err = %v; want ErrRelNotFound, not ErrEntityDeleted", d.name, err)
				}

				for _, prior := range []string{"deleted", "retracted"} {
					f := newPubFixture(t, g)
					var err error
					if prior == "deleted" {
						err = g.Nodes().Delete(ctx, f.ev)
					} else {
						err = g.Nodes().Retract(ctx, f.ev)
					}
					if err != nil {
						t.Fatalf("prior %s: %v", prior, err)
					}
					nh, _ := g.Nodes().History(f.ev)
					rh, _ := g.Rels().History(f.rel)
					err = d.node(f.ev, nowTx(t, g))
					if !errors.Is(err, graphpkg.ErrEntityDeleted) || !errors.Is(err, graphpkg.ErrNodeNotFound) {
						t.Fatalf("%s on a %s node: err = %v; want ErrEntityDeleted and ErrNodeNotFound", d.name, prior, err)
					}
					err = d.rel(f.rel, nowTx(t, g))
					if !errors.Is(err, graphpkg.ErrEntityDeleted) || !errors.Is(err, graphpkg.ErrRelNotFound) {
						t.Fatalf("%s on a %s relationship: err = %v; want ErrEntityDeleted and ErrRelNotFound", d.name, prior, err)
					}
					nh2, _ := g.Nodes().History(f.ev)
					rh2, _ := g.Rels().History(f.rel)
					if len(nh2) != len(nh) || len(rh2) != len(rh) {
						t.Fatalf("%s on a %s entity wrote: node history %d -> %d, relationship %d -> %d", d.name, prior, len(nh), len(nh2), len(rh), len(rh2))
					}
					if prior == "deleted" && (newestNode(t, g, f.ev).Temporal().Retracted || newestRel(t, g, f.rel).Temporal().Retracted) {
						t.Fatalf("%s upgraded a Delete's tombstone into a retraction", d.name)
					}
				}
			}
		})
	}
}

// TestRetractWithTx_GatesAndOrder: the caller-instant contract of
// DeleteWithTx on every WithTx retraction door. Gate off: ErrTxBackfillDisabled
// for a valid t (the value check wins for an invalid one); t = 0, -1, or in
// the future: ErrInvalidTxFrom with the gate on and off; t at or below a
// recorded stamp of the chain, or below the version start: ErrTxOrder (which
// wraps ErrInvalidTxFrom); a recorded close at or after t: ErrTxOrder; a node
// retraction whose cascaded relationship carries a TxFrom at or above t:
// ErrTxOrder for the whole cascade. Every refusal writes nothing. The
// counterpart (t one past every stamp) is stamped verbatim. Catches: WithTx
// without AllowTxBackfill, a missing upper bound, an order check that skips
// the history or the cascade, a refusal that writes.
func TestRetractWithTx_GatesAndOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, gate := range []bool{true, false} {
		for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = gate }) {
			t.Run(fmt.Sprintf("%s/gate=%v", b.name, gate), func(t *testing.T) {
				g := b.open(t)
				for _, d := range pubRetractDoors(g) {
					if !d.withTx {
						continue
					}
					f := newPubFixture(t, g)
					unchanged := func(what string) {
						t.Helper()
						if _, err := g.Nodes().Get(ctx, f.ev); err != nil {
							t.Fatalf("%s %s: the node is gone: %v", d.name, what, err)
						}
						if _, err := g.Rels().Get(ctx, f.rel); err != nil {
							t.Fatalf("%s %s: the relationship is gone: %v", d.name, what, err)
						}
					}
					future := types.Instant(time.Now().Add(time.Hour).UnixMilli())
					for _, bad := range []types.Instant{0, -1, future} {
						if err := d.node(f.ev, bad); !errors.Is(err, graphpkg.ErrInvalidTxFrom) || errors.Is(err, graphpkg.ErrTxOrder) {
							t.Fatalf("%s node t=%d: err = %v; want ErrInvalidTxFrom", d.name, bad, err)
						}
						if err := d.rel(f.rel, bad); !errors.Is(err, graphpkg.ErrInvalidTxFrom) || errors.Is(err, graphpkg.ErrTxOrder) {
							t.Fatalf("%s rel t=%d: err = %v; want ErrInvalidTxFrom", d.name, bad, err)
						}
						unchanged(fmt.Sprintf("t=%d", bad))
					}
					if !gate {
						at := nowTx(t, g)
						if err := d.node(f.ev, at); !errors.Is(err, graphpkg.ErrTxBackfillDisabled) {
							t.Fatalf("%s node, gate off: err = %v; want ErrTxBackfillDisabled", d.name, err)
						}
						if err := d.rel(f.rel, at); !errors.Is(err, graphpkg.ErrTxBackfillDisabled) {
							t.Fatalf("%s rel, gate off: err = %v; want ErrTxBackfillDisabled", d.name, err)
						}
						unchanged("gate off")
						continue
					}
					// The binding stamps of the chain.
					evTxFrom, _, _, _ := g.Nodes().LatestStamps(f.ev)
					relTxFrom, _, _, _ := g.Rels().LatestStamps(f.rel)
					for _, at := range []types.Instant{evTxFrom, evTxFrom - 1, f.vf - 1} {
						if err := d.node(f.ev, at); !errors.Is(err, graphpkg.ErrTxOrder) || !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
							t.Fatalf("%s node t=%d (chain TxFrom %d): err = %v; want ErrTxOrder", d.name, at, evTxFrom, err)
						}
					}
					for _, at := range []types.Instant{relTxFrom, relTxFrom - 1} {
						if err := d.rel(f.rel, at); !errors.Is(err, graphpkg.ErrTxOrder) {
							t.Fatalf("%s rel t=%d: err = %v; want ErrTxOrder", d.name, at, err)
						}
					}
					unchanged("order")

					// A cascaded relationship recorded after t refuses the whole
					// node retraction: the node's own stamps allow t.
					g2 := b.open(t)
					n, err := g2.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"tkg_valid_from": f.vf})
					if err != nil {
						t.Fatal(err)
					}
					between := nowTx(t, g2)
					ref, err := g2.Nodes().Add(ctx, []string{"Ref"}, nil)
					if err != nil {
						t.Fatal(err)
					}
					r, err := g2.Rels().AddByID(ctx, "LINK", ref.ID(), n.ID(), nil)
					if err != nil {
						t.Fatal(err)
					}
					var door2 pubRetractDoor
					for _, dd := range pubRetractDoors(g2) {
						if dd.name == d.name {
							door2 = dd
						}
					}
					if err := door2.node(n.ID(), between); !errors.Is(err, graphpkg.ErrTxOrder) {
						t.Fatalf("%s: cascaded relationship recorded after t: err = %v; want ErrTxOrder", d.name, err)
					}
					if _, err := g2.Rels().Get(ctx, r.ID()); err != nil {
						t.Fatalf("%s: refused cascade removed the relationship: %v", d.name, err)
					}
					if _, err := g2.Nodes().Get(ctx, n.ID()); err != nil {
						t.Fatalf("%s: refused cascade removed the node: %v", d.name, err)
					}

					// A recorded close at or after t refuses.
					c, err := g2.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"tkg_valid_from": f.vf})
					if err != nil {
						t.Fatal(err)
					}
					closeAt := nowTx(t, g2) + 60_000
					if err := g2.Nodes().CloseVersion(ctx, c.ID(), closeAt); err != nil {
						t.Fatalf("CloseVersion: %v", err)
					}
					if err := door2.node(c.ID(), nowTx(t, g2)); !errors.Is(err, graphpkg.ErrTxOrder) {
						t.Fatalf("%s: recorded close after t: err = %v; want ErrTxOrder", d.name, err)
					}

					// Counterpart: one past every stamp is accepted verbatim.
					at := nowTx(t, g)
					if err := d.node(f.ev, at); err != nil {
						t.Fatalf("%s node at %d: %v", d.name, at, err)
					}
					if tm := newestNode(t, g, f.ev).Temporal(); tm.DeletedAt != at || tm.TxTo != at || !tm.Retracted {
						t.Fatalf("%s: tombstone %+v; want TxTo = DeletedAt = %d, retracted", d.name, *tm, at)
					}
				}
			})
		}
	}
}

// TestRetract_CurrentStateDoorsAbsent: the current-state doors lose a
// retracted node and its relationships exactly as after a Delete (Get, the
// plain scans and counters, adjacency); the bystanders stay.
func TestRetract_CurrentStateDoorsAbsent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		f := newPubFixture(t, g)
		keep := newPubFixture(t, g)
		if err := g.Nodes().Retract(ctx, f.ev); err != nil {
			t.Fatalf("Retract: %v", err)
		}
		if _, err := g.Nodes().Get(ctx, f.ev); !errors.Is(err, graphpkg.ErrNodeNotFound) {
			t.Fatalf("Get(retracted) err = %v; want ErrNodeNotFound", err)
		}
		if _, err := g.Rels().Get(ctx, f.rel); !errors.Is(err, graphpkg.ErrRelNotFound) {
			t.Fatalf("Rels().Get(cascaded) err = %v; want ErrRelNotFound", err)
		}
		ns, err := g.Nodes().ByLabel("Ev", graphpkg.QueryOpts{})
		if err != nil || len(ns) != 1 || ns[0].ID() != keep.ev {
			t.Fatalf("ByLabel(Ev) = %v, %v; want only the bystander", ns, err)
		}
		if n, err := g.Nodes().CountByLabel("Ev"); err != nil || n != 1 {
			t.Fatalf("CountByLabel(Ev) = %d, %v; want 1", n, err)
		}
		rs, err := g.Rels().Outgoing(f.ref, "")
		if err != nil || len(rs) != 0 {
			t.Fatalf("Outgoing(ref) = %v, %v; want none", rs, err)
		}
		if n, err := g.Rels().CountByType("LINK"); err != nil || n != 1 {
			t.Fatalf("CountByType(LINK) = %d, %v; want 1", n, err)
		}
	})
}
