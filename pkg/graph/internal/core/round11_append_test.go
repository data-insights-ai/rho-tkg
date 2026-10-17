package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11 on v4.61.0 (sigma-tkgd, 2026-10-17): the write-side presence of the
// instant 0 (types.ExplicitInstant(0), item 2) goes through v4.61.0's one start
// rule (TemporalMetadata / Node / Relationship.EffectiveValidFrom) and its
// O(1) append at the head (backlog 67). An explicit valid-from 0 is a real
// start, an absent claim keeps its meaning, and an append planned from the
// current row writes the rows the history read writes.

// TestRound11EffectiveValidFromAtZero: a row valid from the explicit 0 starts
// at 0 by EffectiveValidFrom (node and relationship, the metadata rule and the
// frozen-row read), equal to the start of its timeline's segment; a plain 0
// keeps its untimed start (TxFrom, or -inf under UntimedUnbounded); after a
// close at 100 the start is still 0. On every backend, both untimed modes.
func TestRound11EffectiveValidFromAtZero(t *testing.T) {
	ctx := context.Background()
	for _, be := range r10AllBackends(t) {
		for _, mode := range []UntimedValidStart{UntimedFromTxFrom, UntimedUnbounded} {
			t.Run(fmt.Sprintf("%s/mode=%d", be.name, mode), func(t *testing.T) {
				cfg := be.cfg()
				cfg.UntimedValidStart = mode
				g, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = g.Close() })
				zero := func() map[string]any { return map[string]any{"tkg_valid_from": types.ExplicitInstant(0)} }
				s, err := g.Nodes.Add(ctx, []string{"A"}, zero())
				if err != nil {
					t.Fatal(err)
				}
				d, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"tkg_valid_from": int64(0)})
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels.Add(ctx, "R", s, d, zero())
				if err != nil {
					t.Fatal(err)
				}
				pr, err := g.Rels.Add(ctx, "R", d, s, map[string]any{"tkg_valid_from": int64(0)})
				if err != nil {
					t.Fatal(err)
				}
				check := func(when string, nid types.NodeID, rid types.RelID, want func(tx types.Instant) types.Instant) {
					t.Helper()
					n, err := g.Nodes.Get(ctx, nid)
					if err != nil {
						t.Fatal(err)
					}
					tx, _, _ := n.TxStamps()
					segs, err := g.Temporal.NodeEffectiveTimeline(nid, g.peekNow())
					if err != nil || len(segs) == 0 {
						t.Fatalf("%s: node timeline: %v %d", when, err, len(segs))
					}
					if got := n.EffectiveValidFrom(); got != want(tx) || got != segs[0].ValidFrom {
						t.Errorf("%s: node EffectiveValidFrom %d, want %d (timeline starts at %d)", when, got, want(tx), segs[0].ValidFrom)
					}
					if got := n.Temporal().EffectiveValidFrom(nid.MintInstant()); got != want(tx) {
						t.Errorf("%s: node TemporalMetadata.EffectiveValidFrom %d, want %d", when, got, want(tx))
					}
					rr, err := g.Rels.Get(ctx, rid)
					if err != nil {
						t.Fatal(err)
					}
					rtx, _, _ := rr.TxStamps()
					rsegs, err := g.Temporal.RelEffectiveTimeline(rid, g.peekNow())
					if err != nil || len(rsegs) == 0 {
						t.Fatalf("%s: rel timeline: %v %d", when, err, len(rsegs))
					}
					if got := rr.EffectiveValidFrom(); got != want(rtx) || got != rsegs[0].ValidFrom {
						t.Errorf("%s: rel EffectiveValidFrom %d, want %d (timeline starts at %d)", when, got, want(rtx), rsegs[0].ValidFrom)
					}
				}
				atZero := func(types.Instant) types.Instant { return 0 }
				untimed := func(tx types.Instant) types.Instant {
					if mode == UntimedUnbounded {
						return types.UnboundedValidFrom
					}
					return tx
				}
				check("explicit 0", s.ID(), r.ID(), atZero)
				check("plain 0", d.ID(), pr.ID(), untimed)
				if _, err := g.Nodes.Update(ctx, s.ID(), map[string]any{"tkg_valid_to": int64(100)}); err != nil {
					t.Fatal(err)
				}
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"tkg_valid_to": int64(100)}); err != nil {
					t.Fatal(err)
				}
				check("explicit 0 closed at 100", s.ID(), r.ID(), atZero)
			})
		}
	}
}

// TestRound11AppendTwinAtZero: on twin graphs (A plans appends from the
// current row, B reads the whole history), an entity valid from the explicit
// 0 takes open appends at the head (the first one from version 0), an append
// at its current start, a backdated one, an Update, a caller-instant append
// and a close; corrections that cross 0 are refused alike (ErrValidEndAtZero)
// and write nothing. After every step the rows are byte-identical and A's
// timelines equal B's fold, on every backend; A planned the appends from the
// current row.
func TestRound11AppendTwinAtZero(t *testing.T) {
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			tw := newHeadTwin(t, be, 0x11)
			ctx := context.Background()
			nid := tw.a.nextNodeID()
			tw.last = "create valid from the explicit 0"
			if err := tw.both(func(g *Core) error {
				_, err := g.Nodes.Import(ctx, nid, []string{"H"}, map[string]any{"w": int64(1), "tkg_valid_from": types.ExplicitInstant(0)})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			tw.nodes = append(tw.nodes, nid)
			rid := tw.a.nextRelID()
			tw.last = "rel valid from the explicit 0"
			if err := tw.both(func(g *Core) error {
				return tw.importRel(g, rid, 0, map[string]any{"w": int64(1), "tkg_valid_from": types.ExplicitInstant(0)})
			}); err != nil {
				t.Fatal(err)
			}
			tw.rels = append(tw.rels, rid)
			before := tw.a.appendWrites.Load()
			var p map[string]any
			run := func(name string, wantErr error, fn func(g *Core, rel bool) error) {
				t.Helper()
				for _, rel := range []bool{false, true} {
					tw.last = fmt.Sprintf("%s rel=%v", name, rel)
					p = tw.props()
					err := tw.both(func(g *Core) error { return fn(g, rel) })
					if wantErr == nil && err != nil {
						t.Fatalf("%s: %v", tw.last, err)
					}
					if wantErr != nil && !errors.Is(err, wantErr) {
						t.Fatalf("%s: %v, want %v", tw.last, err, wantErr)
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
					return r.EffectiveValidFrom()
				}
				n, _ := g.Nodes.Get(ctx, nid)
				return n.EffectiveValidFrom()
			}
			run("append 5 onto version 0", nil, set(5, 0))
			for i := range 4 {
				run(fmt.Sprintf("append %d", 20+10*i), nil, set(types.Instant(20+10*i), 0))
			}
			run("append at the current start", nil, set(50, 0))
			run("backdated open crossing 0", ErrValidEndAtZero, set(-10, 0))
			run("backdated bounded crossing 0", ErrValidEndAtZero, set(-10, 10))
			run("backdated open from 1", nil, set(1, 0))
			run("append 70", nil, set(70, 0))
			run("Update", nil, func(g *Core, rel bool) error {
				if rel {
					_, err := g.Rels.Update(ctx, rid, p)
					return err
				}
				_, err := g.Nodes.Update(ctx, nid, p)
				return err
			})
			run("append after the Update", nil, func(g *Core, rel bool) error { return set(head(g, rel)+5, 0)(g, rel) })
			run("append at a caller instant", nil, func(g *Core, rel bool) error {
				f, to := latest(g, rel, nid, rid)
				from := head(g, rel) + 7
				if rel {
					_, err := g.Temporal.SetRelVersionIntervalWithTx(ctx, rid, from, 0, p, max(f, to)+1)
					return err
				}
				_, err := g.Temporal.SetNodeVersionIntervalWithTx(ctx, nid, from, 0, p, max(f, to)+1)
				return err
			})
			run("close", nil, func(g *Core, rel bool) error {
				end := map[string]any{"tkg_valid_to": int64(head(g, rel) + 50)}
				if rel {
					_, err := g.Rels.Update(ctx, rid, end)
					return err
				}
				_, err := g.Nodes.Update(ctx, nid, end)
				return err
			})
			if n := tw.a.appendWrites.Load() - before; n < 12 {
				t.Fatalf("graph A planned %d appends on the explicit-0 entities from the current row, want at least 12", n)
			}
			if n := tw.b.appendWrites.Load(); n != 0 {
				t.Fatalf("graph B planned %d appends from the current row", n)
			}
			for _, g := range []*Core{tw.a, tw.b} {
				first, err := g.getNodeVersion(nid, 0)
				if err != nil {
					t.Fatal(err)
				}
				if !first.StartsAtZero() {
					t.Fatalf("version 0 lost the claim")
				}
			}
		})
	}
}

// TestRound11AppendAllocsFlatAtZero: an open append at the head of an entity
// valid from the explicit 0 costs what an append on a plain entity costs, flat
// in the chain (1 and 1,000 versions; memory and badger; nodes and
// relationships): the claim adds no history read.
func TestRound11AppendAllocsFlatAtZero(t *testing.T) {
	if testing.Short() {
		t.Skip("1,000-version chains")
	}
	const k = 20
	cost := func(be r10Backend, rel, zero bool, versions int) float64 {
		g, err := New(be.cfg())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = g.Close() }()
		e := newAppendEntity(t, g, rel)
		if zero {
			e = newZeroAppendEntity(t, g, rel)
		}
		e.grow(t, g, 1, versions)
		var dur time.Duration
		m := mallocs(func() {
			start := time.Now()
			e.grow(t, g, versions, versions+k)
			dur = time.Since(start)
		})
		_ = dur
		return float64(m) / float64(k)
	}
	for _, be := range r10Backends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				p1, p1000 := cost(be, rel, false, 1), cost(be, rel, false, 1000)
				z1, z1000 := cost(be, rel, true, 1), cost(be, rel, true, 1000)
				t.Logf("allocs/append plain %.1f / %.1f, explicit 0 %.1f / %.1f at 1 / 1,000 versions", p1, p1000, z1, z1000)
				if z1000 > max(z1, p1, p1000)*1.5+8 {
					t.Fatalf("an append on an explicit-0 entity grows with the chain: %.1f / %.1f (plain %.1f / %.1f)", z1, z1000, p1, p1000)
				}
			})
		}
	}
}

// newZeroAppendEntity is newAppendEntity valid from the explicit 0, its
// appends from base 1000 as there.
func newZeroAppendEntity(tb testing.TB, g *Core, rel bool) appendEntity {
	tb.Helper()
	ctx := context.Background()
	e := appendEntity{rel: rel, base: 1000}
	p := map[string]any{"w": int64(0), "tkg_valid_from": types.ExplicitInstant(0)}
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
