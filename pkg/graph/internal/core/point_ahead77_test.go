package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 77 (found by row 67): a plain superseding write of a version that
// starts in the future (Update, the label and CAS doors, CloseVersion) used to
// stamp the version's start + 1 as TxFrom, ahead of the transaction clock: a
// head pin did not reach the row, every later write on the entity was stamped
// below it, and the unpinned point door (NodeAt / RelAt) answered an older
// version than the effective timeline at the head. The start is now the row's
// ValidFrom and TxFrom the clock. Memory and badger, node and relationship: after
// every step no row is stamped ahead of the clock, every superseded row ends
// at or after its start, and at every segment start of the head timeline the
// unpinned point door and the at-tx door at the newest TxFrom answer the
// segment's version.
func TestPointDoorAheadOfClock77(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range r10Backends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				g, err := New(be.cfg())
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = g.Close() }()
				e := newAppendEntity(t, g, rel)
				future := g.peekNow() + 1_000_000
				update := func(w int64) func() error {
					return func() error {
						p := map[string]any{"w": w}
						if rel {
							_, err := g.Rels.Update(ctx, e.rid, p)
							return err
						}
						_, err := g.Nodes.Update(ctx, e.nid, p)
						return err
					}
				}
				cas := func() error {
					if rel {
						_, err := g.Rels.CompareAndSetProperty(ctx, e.rid, "w", int64(98), int64(97))
						return err
					}
					_, err := g.Nodes.CompareAndSetProperty(ctx, e.nid, "w", int64(98), int64(97))
					return err
				}
				label := func() error {
					if rel {
						return update(96)()
					}
					return g.Nodes.AddLabel(ctx, e.nid, "B")
				}
				closeAt := func(end types.Instant) func() error {
					return func() error {
						if rel {
							return g.Rels.CloseVersion(ctx, e.rid, end)
						}
						return g.Nodes.CloseVersion(ctx, e.nid, end)
					}
				}
				steps := []struct {
					name string
					fn   func() error
				}{
					{"Update at the clock", update(1)},
					{"append 2000", func() error { return e.set(g, 2000, 0, 2) }},
					{"append in the future", func() error { return e.set(g, future, 0, 3) }},
					{"Update of a future version", update(98)},
					{"CAS of a future version", cas},
					{"label / Update of a future version", label},
					{"gap correction [10,50)", func() error { return e.set(g, 10, 50, 4) }},
					{"append above", func() error { return e.set(g, future+10, 0, 5) }},
					{"Update after the append", update(99)},
					{"CloseVersion in the future", closeAt(future + 100)},
				}
				for _, s := range steps {
					if err := s.fn(); err != nil {
						t.Fatalf("%s: %v", s.name, err)
					}
					stampsAtClock77(t, g, e, s.name)
					pointAgrees77(t, g, e, s.name)
				}
			})
		}
	}
}

// stampsAtClock77: no row of the entity carries a TxFrom ahead of the commit
// clock, and no superseded row's TxTo precedes its TxFrom.
func stampsAtClock77(t *testing.T, g *Core, e appendEntity, when string) {
	t.Helper()
	var tms []*types.TemporalMetadata
	if e.rel {
		h, err := g.getRelHistory(e.rid)
		if err != nil {
			t.Fatalf("%s: history: %v", when, err)
		}
		cur, err := g.getCurrentRelationship(e.rid)
		if err != nil {
			t.Fatalf("%s: current: %v", when, err)
		}
		for _, r := range chainOf(h, cur) {
			tms = append(tms, r.Temporal())
		}
	} else {
		h, err := g.getNodeHistory(e.nid)
		if err != nil {
			t.Fatalf("%s: history: %v", when, err)
		}
		cur, err := g.getCurrentNode(e.nid)
		if err != nil {
			t.Fatalf("%s: current: %v", when, err)
		}
		for _, n := range chainOf(h, cur) {
			tms = append(tms, n.Temporal())
		}
	}
	clock := g.peekNow()
	for i, tm := range tms {
		if tm.TxFrom > clock {
			t.Errorf("%s: row %d TxFrom %d is ahead of the clock %d", when, i, tm.TxFrom, clock)
		}
		if tm.TxTo != 0 && tm.TxTo < tm.TxFrom {
			t.Errorf("%s: row %d TxTo %d before TxFrom %d", when, i, tm.TxTo, tm.TxFrom)
		}
	}
}

// pointAgrees77 checks, at every segment start of the effective timeline at
// the head, that NodeAt / RelAt (unpinned) and NodeAtTx / RelAtTx at the
// newest TxFrom answer the segment's version.
func pointAgrees77(t *testing.T, g *Core, e appendEntity, when string) {
	t.Helper()
	head := g.peekNow()
	if e.rel {
		newest, _, _, _ := g.Rels.LatestStamps(e.rid)
		segs, err := g.Temporal.RelEffectiveTimeline(e.rid, head)
		if err != nil {
			t.Fatalf("%s: timeline: %v", when, err)
		}
		for _, s := range segs {
			at, err := g.Temporal.RelAt(e.rid, s.ValidFrom)
			if err != nil || at.Version() != s.Rel.Version() {
				t.Errorf("%s: RelAt(%d) = %v %v, head timeline version %d", when, s.ValidFrom, ver(at), err, s.Rel.Version())
			}
			tx, err := g.Temporal.RelAtTx(e.rid, s.ValidFrom, newest)
			if err != nil || tx.Version() != s.Rel.Version() {
				t.Errorf("%s: RelAtTx(%d, %d) = %v %v, head timeline version %d", when, s.ValidFrom, newest, ver(tx), err, s.Rel.Version())
			}
		}
		return
	}
	newest, _, _, _ := g.Nodes.LatestStamps(e.nid)
	segs, err := g.Temporal.NodeEffectiveTimeline(e.nid, head)
	if err != nil {
		t.Fatalf("%s: timeline: %v", when, err)
	}
	for _, s := range segs {
		at, err := g.Temporal.NodeAt(e.nid, s.ValidFrom)
		if err != nil || at.Version() != s.Node.Version() {
			t.Errorf("%s: NodeAt(%d) = %v %v, head timeline version %d", when, s.ValidFrom, ver(at), err, s.Node.Version())
		}
		tx, err := g.Temporal.NodeAtTx(e.nid, s.ValidFrom, newest)
		if err != nil || tx.Version() != s.Node.Version() {
			t.Errorf("%s: NodeAtTx(%d, %d) = %v %v, head timeline version %d", when, s.ValidFrom, newest, ver(tx), err, s.Node.Version())
		}
	}
}

// ver is v's version, nil for a nil row.
func ver[T interface {
	comparable
	Version() uint32
}](v T) any {
	var zero T
	if v == zero {
		return nil
	}
	return v.Version()
}
