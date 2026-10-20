package core

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 85, consumer shape (ai-soc 2026-10-20, TestDerivedWriteCostFlatInVersionChain;
// sigma's Derived write-back, overview_graph.go reconcile / derivedIn /
// voidUncovered). Per cut, per Derived entity, sigma writes through a GraphTx:
//
//   - fold row: ONE bounded two-tick version [t_k, t_k+2) after every row of
//     the chain; the current row stays the closed genesis, the chain is
//     monotonic and every version is its own piece of the head timeline;
//   - Flux row: an open append [t_k, +inf) (planned from the current row,
//     backlog 67), then a bounded void correction [t_{k-1}, t_k) of the
//     stretch the new version no longer covers (one piece plus the
//     resumption that takes the current slot): the chain is non-monotonic;
//
// and reads the entity's effective timeline before (believed) and after
// (derivedIn) the writes. The head entry answered the bounded writes until
// its caps (512 pieces / 1,024 bounds) dropped it; past them every write read
// and copied the whole history (13 x the allocations at 640 versions vs 40 on
// ai-soc's test). The entry now keeps a suffix and a write leaves it behind,
// so a cut's writes cost the same at any chain length.

// cutShape is one consumer shape: cut runs cut k on the entity.
type cutShape struct {
	name   string
	closed bool // the genesis is a closed row (fold row) or open (Flux row)
	cut    func(tb testing.TB, g *Core, e appendEntity, k int)
}

// cutShapes are the two shapes, written through a GraphTx as sigma does.
var cutShapes = []cutShape{
	{"fold-row", true, func(tb testing.TB, g *Core, e appendEntity, k int) {
		s := e.base + types.Instant(1000*k)
		inTx(tb, g, func(tx *GraphTx) error { return e.setTx(tx, s, s+2, k) })
	}},
	{"flux-row", false, func(tb testing.TB, g *Core, e appendEntity, k int) {
		s := e.base + types.Instant(1000*k)
		inTx(tb, g, func(tx *GraphTx) error {
			if err := e.setTx(tx, s, g.axis().OpenEnd(), k); err != nil {
				return err
			}
			return e.setTxProps(tx, s-1000, s, map[string]any{"void": true})
		})
	}},
}

func inTx(tb testing.TB, g *Core, fn func(tx *GraphTx) error) {
	tb.Helper()
	tx, err := g.BeginTx()
	if err != nil {
		tb.Fatal(err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		tb.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

// setTx runs SetVersionInterval [from, to) on the entity in tx.
func (e appendEntity) setTx(tx *GraphTx, from, to types.Instant, i int) error {
	return e.setTxProps(tx, from, to, map[string]any{"w": int64(i)})
}

func (e appendEntity) setTxProps(tx *GraphTx, from, to types.Instant, p map[string]any) error {
	if e.rel {
		_, err := tx.SetRelVersionInterval(e.rid, from, to, p)
		return err
	}
	_, err := tx.SetNodeVersionInterval(e.nid, from, to, p)
	return err
}

// timeline reads the entity's effective timeline at the committed pin, as
// sigma's believed / derivedIn do; it returns the segment count.
func (e appendEntity) timeline(tb testing.TB, g *Core) int {
	tb.Helper()
	pin, err := g.Temporal.CommittedTx()
	if err != nil {
		tb.Fatal(err)
	}
	if e.rel {
		segs, err := g.Temporal.RelEffectiveTimeline(e.rid, pin)
		if err != nil {
			tb.Fatal(err)
		}
		return len(segs)
	}
	segs, err := g.Temporal.NodeEffectiveTimeline(e.nid, pin)
	if err != nil {
		tb.Fatal(err)
	}
	return len(segs)
}

// newCutEntity is an entity with the shape's genesis at base.
func newCutEntity(tb testing.TB, g *Core, rel, closed bool) appendEntity {
	tb.Helper()
	ctx := context.Background()
	e := appendEntity{rel: rel, base: 1_000_000}
	p := map[string]any{"w": int64(0), "tkg_valid_from": int64(e.base)}
	if closed {
		p["tkg_valid_to"] = int64(e.base + 2)
	}
	if !rel {
		n, err := g.Nodes.Add(ctx, []string{"A"}, p)
		if err != nil {
			tb.Fatal(err)
		}
		e.nid = n.ID()
		return e
	}
	s, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"w": int64(0)})
	if err != nil {
		tb.Fatal(err)
	}
	d, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"w": int64(1)})
	if err != nil {
		tb.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "R", s, d, p)
	if err != nil {
		tb.Fatal(err)
	}
	e.rid = r.ID()
	return e
}

// cutCost is the cheapest of three batches of k cuts of shape sh on an
// entity that has seen `cuts` cuts: heap allocations and nanoseconds per
// cut. With reads, each cut also reads the timeline before and after its
// writes, as sigma does.
func cutCost(tb testing.TB, be r10Backend, sh cutShape, rel, reads bool, cuts, k int) (allocs, ns float64) {
	tb.Helper()
	g, err := New(be.cfg())
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	e := newCutEntity(tb, g, rel, sh.closed)
	run := func(k int) {
		for i := 1; i <= k; i++ {
			if reads {
				e.timeline(tb, g)
			}
			sh.cut(tb, g, e, cuts+i)
			if reads {
				e.timeline(tb, g)
			}
		}
		cuts += k
	}
	run(cuts)
	allocs, ns = math.Inf(1), math.Inf(1)
	for range 5 { // min of five: badger's goroutines allocate in bursts (row 84)
		var dur time.Duration
		m := mallocs(func() {
			start := time.Now()
			run(k)
			dur = time.Since(start)
		})
		allocs = min(allocs, float64(m)/float64(k))
		ns = min(ns, float64(dur.Nanoseconds())/float64(k))
	}
	return allocs, ns
}

// TestCascadeHead85_ConsumerShapeFlat: the allocations of one cut do not
// grow with the chain (48 vs 640 cuts: past the entry's former caps of 512
// pieces and 1,024 bounds), for both shapes, nodes and relationships, memory
// and badger, writes alone (the bound) and with sigma's reads around them
// (logged: both timelines have one segment per version, so a read's answer
// grows with the chain). Before the fix the write path read and copied the
// whole history past the caps: x10 to x14.
func TestCascadeHead85_ConsumerShapeFlat(t *testing.T) {
	const small, large, k = 48, 640, 3
	for _, be := range r10Backends() {
		for _, sh := range cutShapes {
			for _, rel := range []bool{false, true} {
				for _, reads := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/rel=%v/reads=%v", be.name, sh.name, rel, reads), func(t *testing.T) {
						a, na := cutCost(t, be, sh, rel, reads, small, k)
						b, nb := cutCost(t, be, sh, rel, reads, large, k)
						ratio := b / a
						t.Logf("allocs per cut %.0f / %.0f, µs per cut %.0f / %.0f at %d / %d cuts (x%.1f allocs)", a, b, na/1e3, nb/1e3, small, large, ratio)
						if reads {
							return // the read's answer has one segment per version
						}
						// Flat within noise: badger's own goroutines add a few
						// objects per cut; an O(V) path adds thousands.
						if b > a*1.5+16 {
							t.Errorf("a cut's allocations grow with the chain: %.0f at %d cuts, %.0f at %d (x%.1f)", a, small, b, large, ratio)
						}
					})
				}
			}
		}
	}
}
