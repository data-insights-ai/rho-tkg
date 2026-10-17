package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 67 (sigma-tkgd 2026-10-17): an open correction that starts at or
// after the current row's start, on an entity whose current row is the
// chain's newest belief and top version, is planned from the current row
// alone (chainFromCurrent, effective_head_write.go) — no history read, so its
// cost does not grow with the chain. Everything else (a bounded or backdated
// correction, an excise, a current row that is not the newest belief) is
// planned as before.

// appendEntity is one node or relationship of a test graph with the doors an
// append test drives.
type appendEntity struct {
	rel  bool
	nid  types.NodeID
	rid  types.RelID
	base types.Instant // the genesis' valid-from
}

func newAppendEntity(tb testing.TB, g *Core, rel bool) appendEntity {
	tb.Helper()
	ctx := context.Background()
	e := appendEntity{rel: rel, base: 1000}
	p := map[string]any{"w": int64(0), "tkg_valid_from": int64(e.base)}
	if !rel {
		n, err := g.Nodes.Add(ctx, []string{"A"}, p)
		if err != nil {
			tb.Fatalf("Add: %v", err)
		}
		e.nid = n.ID()
		return e
	}
	s, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"w": int64(0)})
	if err != nil {
		tb.Fatalf("Add: %v", err)
	}
	d, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"w": int64(1)})
	if err != nil {
		tb.Fatalf("Add: %v", err)
	}
	r, err := g.Rels.Add(ctx, "R", s, d, p)
	if err != nil {
		tb.Fatalf("Rels.Add: %v", err)
	}
	e.rid = r.ID()
	return e
}

// set runs SetVersionInterval [from, to) on the entity.
func (e appendEntity) set(g *Core, from, to types.Instant, i int) error {
	ctx := context.Background()
	p := map[string]any{"w": int64(i)}
	if e.rel {
		_, err := g.Temporal.SetRelVersionInterval(ctx, e.rid, from, to, p)
		return err
	}
	_, err := g.Temporal.SetNodeVersionInterval(ctx, e.nid, from, to, p)
	return err
}

// grow appends open versions until the chain holds versions rows; the
// version written k-th starts at base + 10k.
func (e appendEntity) grow(tb testing.TB, g *Core, from, versions int) {
	tb.Helper()
	for i := from; i < versions; i++ {
		if err := e.set(g, e.base+types.Instant(10*i), 0, i); err != nil {
			tb.Fatalf("append %d: %v", i, err)
		}
	}
}

// appendCost is the mean heap allocations and time of k open appends on a
// chain of versions rows.
func appendCost(tb testing.TB, be r10Backend, rel bool, versions, k int) (allocs float64, ns float64) {
	tb.Helper()
	g, err := New(be.cfg())
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	e := newAppendEntity(tb, g, rel)
	e.grow(tb, g, 1, versions)
	var dur time.Duration
	m := mallocs(func() {
		start := time.Now()
		e.grow(tb, g, versions, versions+k)
		dur = time.Since(start)
	})
	return float64(m) / float64(k), float64(dur.Nanoseconds()) / float64(k)
}

// TestCascadeAppend67_AllocsFlat: the allocations of an open append at the
// head do not grow with the chain (1, 100 and 1,000 versions; memory and
// badger; nodes and relationships). Before backlog 67 every append read the
// whole history (88 % of a changing write's allocations at 1,000 versions).
func TestCascadeAppend67_AllocsFlat(t *testing.T) {
	const k = 20
	for _, be := range r10Backends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				a1, n1 := appendCost(t, be, rel, 1, k)
				a100, n100 := appendCost(t, be, rel, 100, k)
				a1000, n1000 := appendCost(t, be, rel, 1000, k)
				t.Logf("allocs/append %.1f / %.1f / %.1f, ns/append %.0f / %.0f / %.0f at 1 / 100 / 1,000 versions",
					a1, a100, a1000, n1, n100, n1000)
				// Flat: the 1,000-version chain costs what the shorter ones do
				// within background noise (badger's own goroutines allocate a
				// few objects per append under -race); an O(V) path costs
				// thousands (27,464 per append at 1,000 versions before 67).
				base := max(a1, a100)
				if a1000 > base*1.5+8 {
					t.Fatalf("allocs per append grow with the chain: %.1f / %.1f / %.1f at 1 / 100 / 1,000 versions", a1, a100, a1000)
				}
			})
		}
	}
}

// TestCascadeAppend67_Twin: on twin graphs, graph A plans appends from the
// current row (and corrections from the head entry), graph B reads the whole
// history (headWriteOff). Both run open appends, backdated open and bounded
// corrections that cut earlier versions, an excise, an Update and a close,
// caller-instant appends (accepted and refused) and a GraphTx append rolled
// back; after every step the rows must be byte-identical and A's timelines
// equal B's fold at the head and at earlier pins, and the point, as-of and
// at-tx doors must agree with A's timeline (cross-door agreement) on every
// backend.
func TestCascadeAppend67_Twin(t *testing.T) {
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			tw := newHeadTwin(t, be, 0x67)
			ctx := context.Background()
			nid, rid := tw.nodes[1], tw.rels[0] // open, valid from 100 and 50
			var p map[string]any                // one patch per step, for both twins
			pointDoor := true                   // the unpinned point door is checked too
			run := func(name string, fn func(g *Core, rel bool) error) {
				t.Helper()
				for _, rel := range []bool{false, true} {
					tw.last = fmt.Sprintf("%s rel=%v", name, rel)
					p = tw.props()
					_ = tw.both(func(g *Core) error { return fn(g, rel) })
					tw.pins = append(tw.pins, tw.a.peekNow())
					tw.check(tw.last)
					appendCrossDoor(t, tw.a, nid, rid, tw.pins, tw.last, pointDoor)
				}
			}
			set := func(from, to types.Instant) func(g *Core, rel bool) error {
				return func(g *Core, rel bool) error {
					if rel {
						_, err := g.Temporal.SetRelVersionInterval(ctx, rid, from, to, p)
						return err
					}
					_, err := g.Temporal.SetNodeVersionInterval(ctx, nid, from, to, p)
					return err
				}
			}
			for i := range 8 {
				run(fmt.Sprintf("append %d", 200+10*i), set(types.Instant(200+10*i), 0))
			}
			run("append at the current start", set(270, 0))
			run("backdated open", set(120, 0))
			for i := range 4 {
				run(fmt.Sprintf("append %d", 300+10*i), set(types.Instant(300+10*i), 0))
			}
			run("backdated bounded [150,170)", set(150, 170))
			run("append 400", set(400, 0))
			run("bounded at the head [410,420)", set(410, 420))
			run("append 430", set(430, 0))
			run("excise [440,450)", func(g *Core, rel bool) error {
				if rel {
					return g.Temporal.ExciseRelValidity(ctx, rid, 440, 450)
				}
				return g.Temporal.ExciseNodeValidity(ctx, nid, 440, 450)
			})
			run("append 460", set(460, 0))
			run("Update", func(g *Core, rel bool) error {
				if rel {
					_, err := g.Rels.Update(ctx, rid, p)
					return err
				}
				_, err := g.Nodes.Update(ctx, nid, p)
				return err
			})
			head := func(g *Core, rel bool) types.Instant {
				if rel {
					r, _ := g.Rels.Get(ctx, rid)
					return r.EffectiveValidFrom()
				}
				n, _ := g.Nodes.Get(ctx, nid)
				return n.EffectiveValidFrom()
			}
			run("append after the Update", func(g *Core, rel bool) error { return set(head(g, rel)+5, 0)(g, rel) })
			run("append before the Update's start", func(g *Core, rel bool) error { return set(head(g, rel)-1, 0)(g, rel) })
			withTx := func(refused bool) func(g *Core, rel bool) error {
				return func(g *Core, rel bool) error {
					f, to := latest(g, rel, nid, rid)
					at := max(f, to) + 1
					if refused {
						at = max(f, to) // at the newest stamp: ErrTxOrder
					}
					from := head(g, rel) + 7
					if rel {
						_, err := g.Temporal.SetRelVersionIntervalWithTx(ctx, rid, from, 0, p, at)
						return err
					}
					_, err := g.Temporal.SetNodeVersionIntervalWithTx(ctx, nid, from, 0, p, at)
					return err
				}
			}
			run("append at a caller instant", withTx(false))
			run("append at a refused caller instant", withTx(true))
			run("close", func(g *Core, rel bool) error {
				end := map[string]any{"tkg_valid_to": int64(head(g, rel) + 50)}
				if rel {
					_, err := g.Rels.Update(ctx, rid, end)
					return err
				}
				_, err := g.Nodes.Update(ctx, nid, end)
				return err
			})
			run("append after the close", func(g *Core, rel bool) error { return set(head(g, rel)+60, 0)(g, rel) })
			run("GraphTx append, rolled back", func(g *Core, rel bool) error {
				tx, err := g.BeginTx()
				if err != nil {
					return err
				}
				from := head(g, rel) + 3
				if rel {
					_, err = tx.SetRelVersionInterval(rid, from, 0, map[string]any{"w": int64(7)})
				} else {
					_, err = tx.SetNodeVersionInterval(nid, from, 0, map[string]any{"w": int64(7)})
				}
				if err != nil {
					_ = tx.Rollback()
					return err
				}
				return tx.Rollback()
			})
			run("GraphTx append, committed", func(g *Core, rel bool) error {
				tx, err := g.BeginTx()
				if err != nil {
					return err
				}
				from := head(g, rel) + 4
				if rel {
					_, err = tx.SetRelVersionInterval(rid, from, 0, map[string]any{"w": int64(8)})
				} else {
					_, err = tx.SetNodeVersionInterval(nid, from, 0, map[string]any{"w": int64(8)})
				}
				if err != nil {
					_ = tx.Rollback()
					return err
				}
				return tx.Commit()
			})
			// A current row stamped ahead of the clock (an Update of a version
			// that starts in the future: validInstantAfter), then a gap
			// correction before the genesis: its row lies above the current
			// row's version with an older TxFrom, so only the version probe
			// sees it; the append after it must not plan from the current row.
			// The unpinned point door (NodeAt / RelAt) is not compared from
			// here on: with a row stamped ahead of the clock it answers an
			// older version than the head timeline on main too (the rows are
			// graph B's, planned from the history; not backlog 67).
			future := tw.a.peekNow() + 1_000_000
			pointDoor = false
			run("append in the future", set(future, 0))
			run("Update of a future version", func(g *Core, rel bool) error {
				if rel {
					_, err := g.Rels.Update(ctx, rid, p)
					return err
				}
				_, err := g.Nodes.Update(ctx, nid, p)
				return err
			})
			run("gap correction [10,50)", set(10, 50))
			run("append above a row the current row does not see", set(future+10, 0))
			if n := tw.a.appendWrites.Load(); n < 20 {
				t.Fatalf("graph A planned %d appends from the current row, want at least 20", n)
			}
			if n := tw.b.appendWrites.Load(); n != 0 {
				t.Fatalf("graph B planned %d appends from the current row", n)
			}
		})
	}
}

func latest(g *Core, rel bool, nid types.NodeID, rid types.RelID) (types.Instant, types.Instant) {
	if rel {
		f, to, _, _ := g.Rels.LatestStamps(rid)
		return f, to
	}
	f, to, _, _ := g.Nodes.LatestStamps(nid)
	return f, to
}

// appendCrossDoor checks that the at-tx door (NodesAtTxByIDs /
// RelsAtTxByIDs) and, with pointDoor, the unpinned point door (NodeAt /
// RelAt, at the head) answer, at every segment
// start of the effective timeline at the head and the last three pins, the
// segment's row.
func appendCrossDoor(t *testing.T, g *Core, nid types.NodeID, rid types.RelID, pins []types.Instant, when string, pointDoor bool) {
	t.Helper()
	head := g.peekNow()
	pins = pins[max(0, len(pins)-3):]
	for _, pin := range append([]types.Instant{head}, pins...) {
		segs, err := g.Temporal.NodeEffectiveTimeline(nid, pin)
		if err != nil {
			t.Fatalf("%s: node timeline at %d: %v", when, pin, err)
		}
		for _, s := range segs {
			got, err := g.Temporal.NodesAtTxByIDs([]types.NodeID{nid}, s.ValidFrom, pin)
			if err != nil || len(got) != 1 || got[0] == nil || got[0].Version() != s.Node.Version() {
				t.Fatalf("%s: node at-tx (%d, %d): %v %v, timeline version %d", when, s.ValidFrom, pin, got, err, s.Node.Version())
			}
			if pin == head && pointDoor {
				at, err := g.Temporal.NodeAt(nid, s.ValidFrom)
				if err != nil || at.Version() != s.Node.Version() {
					t.Fatalf("%s: NodeAt(%d): %v %v, timeline version %d", when, s.ValidFrom, at, err, s.Node.Version())
				}
			}
		}
		rsegs, err := g.Temporal.RelEffectiveTimeline(rid, pin)
		if err != nil {
			t.Fatalf("%s: rel timeline at %d: %v", when, pin, err)
		}
		for _, s := range rsegs {
			got, err := g.Temporal.RelsAtTxByIDs([]types.RelID{rid}, s.ValidFrom, pin)
			if err != nil || len(got) != 1 || got[0] == nil || got[0].Version() != s.Rel.Version() {
				t.Fatalf("%s: rel at-tx (%d, %d): %v %v, timeline version %d", when, s.ValidFrom, pin, got, err, s.Rel.Version())
			}
			if pin == head && pointDoor {
				at, err := g.Temporal.RelAt(rid, s.ValidFrom)
				if err != nil || at.Version() != s.Rel.Version() {
					t.Fatalf("%s: RelAt(%d): %v %v, timeline version %d", when, s.ValidFrom, at, err, s.Rel.Version())
				}
			}
		}
	}
}

// TestCurrentRowEffectiveValidFrom67: the current row's EffectiveValidFrom
// is the start of the timeline's last segment, for a row written by Update
// (stored ValidFrom 0: no caller claim, kept), by a cascade (explicit), by a
// create under both untimed modes, and by UpdateWithTx; nodes and
// relationships, on every backend.
func TestCurrentRowEffectiveValidFrom67(t *testing.T) {
	ctx := context.Background()
	for _, be := range r10AllBackends(t) {
		for _, mode := range []UntimedValidStart{UntimedFromTxFrom, UntimedUnbounded} {
			t.Run(fmt.Sprintf("%s/mode=%d", be.name, mode), func(t *testing.T) {
				cfg := be.cfg()
				cfg.UntimedValidStart, cfg.AllowTxBackfill = mode, true
				g, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = g.Close() })
				s, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"w": int64(0)})
				if err != nil {
					t.Fatal(err)
				}
				d, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"w": int64(1)})
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels.Add(ctx, "R", s, d, map[string]any{"w": int64(0)})
				if err != nil {
					t.Fatal(err)
				}
				nid, rid := s.ID(), r.ID()
				check := func(when string, wantStored0 bool) {
					t.Helper()
					n, err := g.Nodes.Get(ctx, nid)
					if err != nil {
						t.Fatal(err)
					}
					segs, err := g.Temporal.NodeEffectiveTimeline(nid, g.peekNow())
					if err != nil || len(segs) == 0 {
						t.Fatalf("%s: node timeline: %v %d", when, err, len(segs))
					}
					if got, want := n.EffectiveValidFrom(), segs[len(segs)-1].ValidFrom; got != want {
						t.Fatalf("%s: node EffectiveValidFrom %d, timeline's last segment starts at %d", when, got, want)
					}
					if vf, _, _ := n.ValidRange(); wantStored0 && vf != 0 {
						t.Fatalf("%s: node stored ValidFrom %d, want 0 (no caller claim)", when, vf)
					}
					rr, err := g.Rels.Get(ctx, rid)
					if err != nil {
						t.Fatal(err)
					}
					rsegs, err := g.Temporal.RelEffectiveTimeline(rid, g.peekNow())
					if err != nil || len(rsegs) == 0 {
						t.Fatalf("%s: rel timeline: %v %d", when, err, len(rsegs))
					}
					if got, want := rr.EffectiveValidFrom(), rsegs[len(rsegs)-1].ValidFrom; got != want {
						t.Fatalf("%s: rel EffectiveValidFrom %d, timeline's last segment starts at %d", when, got, want)
					}
					if vf, _, _ := rr.ValidRange(); wantStored0 && vf != 0 {
						t.Fatalf("%s: rel stored ValidFrom %d, want 0 (no caller claim)", when, vf)
					}
				}
				check("create", mode == UntimedFromTxFrom)
				for i := range 3 {
					if _, err := g.Nodes.Update(ctx, nid, map[string]any{"w": int64(10 + i)}); err != nil {
						t.Fatal(err)
					}
					if _, err := g.Rels.Update(ctx, rid, map[string]any{"w": int64(10 + i)}); err != nil {
						t.Fatal(err)
					}
					check(fmt.Sprintf("Update %d", i), true)
				}
				n, _ := g.Nodes.Get(ctx, nid)
				rr, _ := g.Rels.Get(ctx, rid)
				if _, err := g.Temporal.SetNodeVersionInterval(ctx, nid, n.EffectiveValidFrom()+3, 0, map[string]any{"w": int64(20)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Temporal.SetRelVersionInterval(ctx, rid, rr.EffectiveValidFrom()+3, 0, map[string]any{"w": int64(20)}); err != nil {
					t.Fatal(err)
				}
				check("append", false)
				nf, nt, _, _ := g.Nodes.LatestStamps(nid)
				rf, rt, _, _ := g.Rels.LatestStamps(rid)
				if _, err := g.Nodes.UpdateWithTx(ctx, nid, map[string]any{"w": int64(30)}, max(nf, nt)+1); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.UpdateWithTx(ctx, rid, map[string]any{"w": int64(30)}, max(rf, rt)+1); err != nil {
					t.Fatal(err)
				}
				check("UpdateWithTx", true)
				var nilNode *types.Node
				var nilRel *types.Relationship
				if nilNode.EffectiveValidFrom() != 0 || nilRel.EffectiveValidFrom() != 0 {
					t.Fatal("a nil row's EffectiveValidFrom is not 0")
				}
			})
		}
	}
}
