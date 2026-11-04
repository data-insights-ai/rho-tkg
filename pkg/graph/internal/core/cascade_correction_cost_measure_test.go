//go:build measure

package core

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Measurement level (MEMGATE, `make measure`): row 85's cost is resolver work
// on a chain already in memory (cuts × V log V), which no read, row or store
// count sees, and bytes per write count the whole process. The prepared
// chain's answers are pinned in the suite (TestCascadeCorrection85_Oracle).

// update runs Update on the entity.
func (e appendEntity) update(g *Core, p map[string]any) error {
	ctx := context.Background()
	if e.rel {
		_, err := g.Rels.Update(ctx, e.rid, p)
		return err
	}
	_, err := g.Nodes.Update(ctx, e.nid, p)
	return err
}

// cutsCost is the cheapest of three batches of k whole-axis untimed updates
// that change nothing (every row already holds z = 1) on an entity whose chain
// holds versions rows: bytes allocated and time per write. Such an update cuts
// [UnboundedValidFrom, +inf) at every bound of the chain and appends no row,
// so the chain stays the same size across the batches.
func cutsCost(tb testing.TB, be r10Backend, rel bool, versions, k int) (bytes, ns float64) {
	tb.Helper()
	cfg := be.cfg()
	cfg.UntimedValidStart = UntimedWholeAxis
	g, err := New(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	e := newAppendEntity(tb, g, rel)
	if err := e.update(g, map[string]any{"z": int64(1)}); err != nil {
		tb.Fatalf("update z: %v", err)
	}
	rows := 2
	for i := 1; rows < versions; i++ {
		s := e.base + types.Instant(10*i)
		if err := e.set(g, s, s+5, i); err != nil {
			tb.Fatalf("bounded append %d: %v", i, err)
		}
		rows += 2 // the piece and the resumption
	}
	before := e.rows(tb, g)
	bytes, ns = math.Inf(1), math.Inf(1)
	for range 3 {
		var a, b runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&a)
		start := time.Now()
		for range k {
			if err := e.update(g, map[string]any{"z": int64(1)}); err != nil {
				tb.Fatalf("no-op update: %v", err)
			}
		}
		dur := time.Since(start)
		runtime.ReadMemStats(&b)
		bytes = min(bytes, float64(b.TotalAlloc-a.TotalAlloc)/float64(k))
		ns = min(ns, float64(dur.Nanoseconds())/float64(k))
	}
	if after := e.rows(tb, g); after != before {
		tb.Fatalf("a no-op update appended rows: %d -> %d", before, after)
	}
	return bytes, ns
}

// TestCascadeCorrection85_CostFlat: a write's cost grows with the chain as
// V log V (its history read and one sweep), not as cuts × V log V. From 100
// to 1,000 rows (the cuts grow with the chain here) bytes per write grow
// about 10 × a log factor (time is logged); the per-cut resolver grew 30 to 60 × (memory:
// 120 µs to 6.3 ms per write before row 85).
func TestCascadeCorrection85_CostFlat(t *testing.T) {
	const k = 5
	for _, be := range r10Backends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				b100, n100 := cutsCost(t, be, rel, 100, k)
				b1000, n1000 := cutsCost(t, be, rel, 1000, k)
				t.Logf("per write: %.0f / %.0f bytes, %.0f / %.0f µs at 100 / 1,000 rows (x%.1f bytes, x%.1f time)",
					b100, b1000, n100/1e3, n1000/1e3, b1000/b100, n1000/n100)
				// 10 × log2(1000)/log2(100) = 15 for V log V; 20 leaves room;
				// cuts × V log V measured 29 to 62. Bytes decide: time is logged
				// only, since a shared runner stretched it to x34.9 on a
				// healthy write (v4.78.4 CI, CASCADEFLAKY) and its bound would
				// overlap the regression's.
				if b1000 > 20*b100 {
					t.Errorf("bytes per write grow with cuts × chain: %.0f -> %.0f (x%.1f)", b100, b1000, b1000/b100)
				}
			})
		}
	}
}
