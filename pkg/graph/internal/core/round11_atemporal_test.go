package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11, UntimedAtemporal: the untimed update is the cascade kernel over
// every valid segment (cascadeExisting). On a pure atemporal entity it is
// planned from the current row (backlog 67, O(1)); on twin graphs the rows
// are byte-identical to the ones the history read builds (graph B,
// headWriteOff), for a pure entity, a timed one and a relationship, through
// the plain, caller-instant, GraphTx and batch doors, on every backend.

func atemporalBackends(tb testing.TB) []r10Backend {
	var out []r10Backend
	for _, be := range r10AllBackends(tb) {
		cfg := be.cfg
		out = append(out, r10Backend{be.name, func() Config {
			c := cfg()
			c.UntimedValidStart = UntimedAtemporal
			return c
		}})
	}
	return out
}

func TestRound11AtemporalTwin(t *testing.T) {
	ctx := context.Background()
	for _, be := range atemporalBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			tw := newHeadTwin(t, be, 0xa7)
			pure, timed, rid := tw.nodes[0], tw.nodes[1], tw.rels[0] // untimed (-inf), from 100, from 50
			var p map[string]any
			run := func(name string, fn func(g *Core) error) {
				t.Helper()
				tw.last = name
				p = tw.props()
				if err := tw.both(fn); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				tw.pins = append(tw.pins, tw.a.peekNow())
				tw.check(name)
			}
			before := tw.a.appendWrites.Load()
			for i := range 4 {
				run(fmt.Sprintf("untimed update %d of the pure node", i), func(g *Core) error {
					_, err := g.Nodes.Update(ctx, pure, p)
					return err
				})
			}
			if n := tw.a.appendWrites.Load() - before; n != 4 {
				t.Fatalf("graph A planned %d of 4 untimed updates of a pure atemporal node from its current row", n)
			}
			run("untimed update of the timed node", func(g *Core) error {
				_, err := g.Nodes.Update(ctx, timed, p)
				return err
			})
			run("untimed update of a relationship", func(g *Core) error {
				_, err := g.Rels.Update(ctx, rid, p)
				return err
			})
			run("timed split of the pure node", func(g *Core) error {
				_, err := g.Temporal.SetNodeVersionInterval(ctx, pure, 300, 400, p)
				return err
			})
			run("untimed update after the split", func(g *Core) error {
				_, err := g.Nodes.Update(ctx, pure, p)
				return err
			})
			run("untimed update at a caller instant", func(g *Core) error {
				f, to := latest(g, false, pure, rid)
				_, err := g.Nodes.UpdateWithTx(ctx, pure, p, max(f, to)+1)
				return err
			})
			run("GraphTx untimed update", func(g *Core) error {
				tx, err := g.BeginTx()
				if err != nil {
					return err
				}
				if _, err := tx.UpdateNode(timed, p); err != nil {
					_ = tx.Rollback()
					return err
				}
				return tx.Commit()
			})
			run("GraphTx untimed update rolled back", func(g *Core) error {
				tx, err := g.BeginTx()
				if err != nil {
					return err
				}
				if _, err := tx.UpdateNode(timed, map[string]any{"w": int64(-1)}); err != nil {
					_ = tx.Rollback()
					return err
				}
				return tx.Rollback()
			})
			// After the rollback the node answers as before it.
			n, err := tw.a.Nodes.Get(ctx, timed)
			if err != nil {
				t.Fatal(err)
			}
			if w, _ := n.GetProperty("w"); w == int64(-1) {
				t.Fatalf("a rolled-back untimed update is visible")
			}
		})
	}
}

// TestRound11AtemporalAllocsFlat: an untimed update of a pure atemporal node
// costs the same at 1 and 1,000 versions (memory and badger).
func TestRound11AtemporalAllocsFlat(t *testing.T) {
	if testing.Short() {
		t.Skip("1,000-version chains")
	}
	ctx := context.Background()
	const k = 20
	cost := func(be r10Backend, versions int) float64 {
		cfg := be.cfg()
		cfg.UntimedValidStart = UntimedAtemporal
		g, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = g.Close() }()
		n, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"w": int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < versions; i++ {
			if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"w": int64(i)}); err != nil {
				t.Fatal(err)
			}
		}
		m := mallocs(func() {
			for i := range k {
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"w": int64(versions + i)}); err != nil {
					t.Fatal(err)
				}
			}
		})
		cur, err := g.Nodes.Get(ctx, n.ID())
		if err != nil {
			t.Fatal(err)
		}
		if cur.EffectiveValidFrom() != types.UnboundedValidFrom {
			t.Fatalf("the current version starts at %d, want -inf", cur.EffectiveValidFrom())
		}
		return float64(m) / k
	}
	for _, be := range r10Backends() {
		t.Run(be.name, func(t *testing.T) {
			a1, a1000 := cost(be, 1), cost(be, 1000)
			t.Logf("allocs per untimed update: %.1f at 1 version, %.1f at 1,000", a1, a1000)
			if a1000 > a1*1.5+8 {
				t.Fatalf("an untimed atemporal update grows with the chain: %.1f / %.1f", a1, a1000)
			}
		})
	}
}

// The caller-instant pre-flight of a batch under UntimedAtemporal decides a
// whole-axis node from its current row (nodeHeadSegmentRows): no history read
// beyond what every mode's pre-flight does. Measured against the same batch
// under UntimedDefault at 1,000 versions (that pre-flight's own order rule
// reads the chain; the final review's per-segment check doubled it).
func TestRound11AtemporalPreflightWholeAxisNoHistoryRead(t *testing.T) {
	if testing.Short() {
		t.Skip("1,000-version chains")
	}
	ctx := context.Background()
	const k, versions = 20, 1000
	cost := func(be r10Backend, mode UntimedValidStart) float64 {
		cfg := be.cfg()
		cfg.UntimedValidStart = mode
		cfg.AllowTxBackfill = true
		g, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = g.Close() }()
		n, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"w": int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < versions; i++ {
			if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"w": int64(i)}); err != nil {
				t.Fatal(err)
			}
		}
		m := mallocs(func() {
			for i := range k {
				b, err := NewBatchBuilder(g)
				if err != nil {
					t.Fatal(err)
				}
				if err := b.UpdateNodeWithTx(n.ID(), map[string]any{"w": int64(versions + i)}, g.now()); err != nil {
					t.Fatal(err)
				}
				if _, err := b.Execute(); err != nil {
					t.Fatal(err)
				}
			}
		})
		return float64(m) / k
	}
	for _, be := range r10Backends() {
		t.Run(be.name, func(t *testing.T) {
			atemporal, def := cost(be, UntimedAtemporal), cost(be, UntimedDefault)
			t.Logf("allocs per caller-instant batch update at %d versions: %.1f atemporal, %.1f default", versions, atemporal, def)
			if atemporal > def*0.75 {
				t.Fatalf("the atemporal pre-flight reads the chain of a whole-axis node: %.1f / %.1f", atemporal, def)
			}
		})
	}
}
