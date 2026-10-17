package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11, zero is an instant: on the literal valid axis (UntimedWholeAxis)
// an entity valid from the instant 0 is an ordinary entity. Its start goes
// through the one start rule (the axis's Start), and an open append at its
// head is planned from the current row alone (backlog 67, O(1)), writing
// the rows the history read writes.

// TestRound11ZeroStartRule: the axis's start of a row stored valid from 0 is
// 0 on the literal axis (node and relationship, the metadata rule and the
// frozen-row read), equal to the start of its timeline's segment, also after
// a close; the legacy axis reads the same stored 0 as "no claim" (its TxFrom).
func TestRound11ZeroStartRule(t *testing.T) {
	ctx := context.Background()
	for _, be := range r10WholeAxisBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			a := g.axis()
			if !a.Literal() {
				t.Fatalf("axis %+v, want the literal axis", a)
			}
			s, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"tkg_valid_from": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			d, err := g.Nodes.Add(ctx, []string{"A"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels.Add(ctx, "R", s, d, map[string]any{"tkg_valid_from": types.ExplicitInstant(0)})
			if err != nil {
				t.Fatal(err)
			}
			check := func(when string) {
				t.Helper()
				n, err := g.Nodes.Get(ctx, s.ID())
				if err != nil {
					t.Fatal(err)
				}
				segs, err := g.Temporal.NodeEffectiveTimeline(s.ID(), g.peekNow())
				if err != nil || len(segs) == 0 {
					t.Fatalf("%s: node timeline: %v %d", when, err, len(segs))
				}
				if got := a.NodeStart(n); got != 0 || segs[0].ValidFrom != 0 {
					t.Errorf("%s: node starts at %d on the axis, timeline at %d; want 0", when, got, segs[0].ValidFrom)
				}
				if got := a.Start(n.Temporal(), s.ID().MintInstant()); got != 0 {
					t.Errorf("%s: metadata start %d, want 0", when, got)
				}
				if tx, _, _ := n.TxStamps(); types.LegacyValidAxis.NodeStart(n) != tx {
					t.Errorf("%s: the legacy axis reads the stored 0 as no claim (TxFrom %d), got %d", when, tx, types.LegacyValidAxis.NodeStart(n))
				}
				rr, err := g.Rels.Get(ctx, r.ID())
				if err != nil {
					t.Fatal(err)
				}
				rsegs, err := g.Temporal.RelEffectiveTimeline(r.ID(), g.peekNow())
				if err != nil || len(rsegs) == 0 {
					t.Fatalf("%s: rel timeline: %v %d", when, err, len(rsegs))
				}
				if got := a.RelStart(rr); got != 0 || rsegs[0].ValidFrom != 0 {
					t.Errorf("%s: rel starts at %d on the axis, timeline at %d; want 0", when, got, rsegs[0].ValidFrom)
				}
			}
			check("valid from 0")
			if _, err := g.Nodes.Update(ctx, s.ID(), map[string]any{"tkg_valid_to": int64(100)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"tkg_valid_to": int64(100)}); err != nil {
				t.Fatal(err)
			}
			check("closed at 100")
		})
	}
}

// TestRound11ZeroAppendTwin: on twin graphs on the literal axis (A plans
// appends from the current row, B reads the whole history), an entity valid
// from the instant 0 takes open appends at the head, an append at its
// current start, backdated corrections that end at or cross 0, an Update, a
// caller-instant append and a close. After every step the rows are
// byte-identical and A's timelines equal B's fold, on every backend; A
// planned the appends from the current row.
func TestRound11ZeroAppendTwin(t *testing.T) {
	for _, be := range r10WholeAxisBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			tw := newHeadTwin(t, be, 0x11)
			ctx := context.Background()
			open := tw.a.axis().OpenEnd()
			nid := tw.a.nextNodeID()
			tw.last = "create valid from 0"
			if err := tw.both(func(g *Core) error {
				_, err := g.Nodes.Import(ctx, nid, []string{"H"}, map[string]any{"w": int64(1), "tkg_valid_from": int64(0)})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			tw.nodes = append(tw.nodes, nid)
			rid := tw.a.nextRelID()
			tw.last = "rel valid from 0"
			if err := tw.both(func(g *Core) error {
				return tw.importRel(g, rid, 0, map[string]any{"w": int64(1), "tkg_valid_from": int64(0)})
			}); err != nil {
				t.Fatal(err)
			}
			tw.rels = append(tw.rels, rid)
			before := tw.a.appendWrites.Load()
			var p map[string]any
			run := func(name string, fn func(g *Core, rel bool) error) {
				t.Helper()
				for _, rel := range []bool{false, true} {
					tw.last = fmt.Sprintf("%s rel=%v", name, rel)
					p = tw.props()
					if err := tw.both(func(g *Core) error { return fn(g, rel) }); err != nil {
						t.Fatalf("%s: %v", tw.last, err)
					}
					tw.pins = append(tw.pins, tw.a.peekNow())
					tw.check(tw.last)
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
			head := func(g *Core, rel bool) types.Instant {
				if rel {
					r, _ := g.Rels.Get(ctx, rid)
					return g.axis().RelStart(r)
				}
				n, _ := g.Nodes.Get(ctx, nid)
				return g.axis().NodeStart(n)
			}
			run("append 5 onto version 0", set(5, open))
			for i := range 4 {
				run(fmt.Sprintf("append %d", 20+10*i), set(types.Instant(20+10*i), open))
			}
			run("append at the current start", set(50, open))
			run("backdated ending at 0", set(-10, 0))
			run("backdated crossing 0", set(-10, 10))
			run("backdated open from 0", set(0, open))
			run("append 70", set(70, open))
			run("Update", func(g *Core, rel bool) error {
				if rel {
					_, err := g.Rels.Update(ctx, rid, p)
					return err
				}
				_, err := g.Nodes.Update(ctx, nid, p)
				return err
			})
			run("append after the Update", func(g *Core, rel bool) error { return set(head(g, rel)+5, open)(g, rel) })
			run("append at a caller instant", func(g *Core, rel bool) error {
				f, to := latest(g, rel, nid, rid)
				from := head(g, rel) + 7
				if rel {
					_, err := g.Temporal.SetRelVersionIntervalWithTx(ctx, rid, from, open, p, max(f, to)+1)
					return err
				}
				_, err := g.Temporal.SetNodeVersionIntervalWithTx(ctx, nid, from, open, p, max(f, to)+1)
				return err
			})
			run("close", func(g *Core, rel bool) error {
				end := map[string]any{"tkg_valid_to": int64(head(g, rel) + 50)}
				if rel {
					_, err := g.Rels.Update(ctx, rid, end)
					return err
				}
				_, err := g.Nodes.Update(ctx, nid, end)
				return err
			})
			if n := tw.a.appendWrites.Load() - before; n < 10 {
				t.Fatalf("graph A planned %d appends on the entities valid from 0 from the current row, want at least 10", n)
			}
			if n := tw.b.appendWrites.Load(); n != 0 {
				t.Fatalf("graph B planned %d appends from the current row", n)
			}
			for _, g := range []*Core{tw.a, tw.b} {
				first, err := g.getNodeVersion(nid, 0)
				if err != nil {
					t.Fatal(err)
				}
				if vf, _, _ := first.ValidRange(); vf != 0 {
					t.Fatalf("version 0 starts at %d, want 0", vf)
				}
			}
		})
	}
}

// TestRound11ZeroAppendAllocsFlat: an open append at the head of an entity
// valid from the instant 0 on the literal axis costs what an append on an
// entity valid from 1000 costs, flat in the chain (1 and 1,000 versions;
// memory and badger; nodes and relationships).
func TestRound11ZeroAppendAllocsFlat(t *testing.T) {
	if testing.Short() {
		t.Skip("1,000-version chains")
	}
	const k = 20
	cost := func(be r10Backend, rel bool, base types.Instant, versions int) float64 {
		g, err := New(be.cfg())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = g.Close() }()
		e := newZeroAxisAppendEntity(t, g, rel, base)
		e.growOpen(t, g, 1, versions)
		m := mallocs(func() { e.growOpen(t, g, versions, versions+k) })
		return float64(m) / float64(k)
	}
	for _, be := range r10Backends() {
		be := r10Backend{be.name, func() Config { c := be.cfg(); c.UntimedValidStart = UntimedWholeAxis; return c }}
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				start := time.Now()
				p1, p1000 := cost(be, rel, 1000, 1), cost(be, rel, 1000, 1000)
				z1, z1000 := cost(be, rel, 0, 1), cost(be, rel, 0, 1000)
				t.Logf("allocs/append from 1000: %.1f / %.1f, from 0: %.1f / %.1f at 1 / 1,000 versions (%v)", p1, p1000, z1, z1000, time.Since(start))
				if z1000 > max(z1, p1, p1000)*1.5+8 {
					t.Fatalf("an append on an entity valid from 0 grows with the chain: %.1f / %.1f (from 1000: %.1f / %.1f)", z1, z1000, p1, p1000)
				}
			})
		}
	}
}

// newZeroAxisAppendEntity is newAppendEntity on a literal-axis graph, valid
// from base (0 included).
func newZeroAxisAppendEntity(tb testing.TB, g *Core, rel bool, base types.Instant) appendEntity {
	tb.Helper()
	ctx := context.Background()
	e := appendEntity{rel: rel, base: base}
	p := map[string]any{"w": int64(0), "tkg_valid_from": int64(base)}
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

// growOpen is grow with the open end of the graph's valid axis.
func (e appendEntity) growOpen(tb testing.TB, g *Core, from, versions int) {
	tb.Helper()
	for i := from; i < versions; i++ {
		if err := e.set(g, e.base+types.Instant(10*i), g.axis().OpenEnd(), i); err != nil {
			tb.Fatalf("append %d: %v", i, err)
		}
	}
}
