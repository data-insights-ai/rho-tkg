package core

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// sigma R10 (backlog 68): the head-pin effective timeline of an entity whose
// versions keep superseding one validity costs O(segments), not O(history
// versions), on every backend that offers HistoryEpochCapability — also
// right after a correction (sigma's Derived writer reads, then writes, every
// cut).

// r10WholeAxisBackends is r10AllBackends under UntimedWholeAxis (round 11:
// the literal valid axis), named "<backend>/whole-axis".
func r10WholeAxisBackends(tb testing.TB) []r10Backend {
	var out []r10Backend
	for _, be := range r10AllBackends(tb) {
		cfg := be.cfg
		out = append(out, r10Backend{be.name + "/whole-axis", func() Config {
			c := cfg()
			c.UntimedValidStart = UntimedWholeAxis
			return c
		}})
	}
	return out
}

func r10AllBackends(tb testing.TB) []r10Backend {
	return []r10Backend{
		{"memory", func() Config { return Config{} }},
		{"badger", func() Config { return Config{BadgerInMemory: true} }},
		{"sharded", func() Config {
			st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				tb.Fatalf("sharded.New: %v", err)
			}
			return Config{Store: st}
		}},
		{"tiered", func() Config {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"},
				ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
			if err != nil {
				tb.Fatalf("tiered.New: %v", err)
			}
			tb.Cleanup(func() { _ = ts.Close() })
			return Config{Store: ts}
		}},
	}
}

type (
	nodeIDArg struct{ id types.NodeID }
	relIDArg  struct{ id types.RelID }
)

// mallocs counts the heap allocations fn makes.
func mallocs(fn func()) uint64 {
	var a, b runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&a)
	fn()
	runtime.ReadMemStats(&b)
	return b.Mallocs - a.Mallocs
}

func TestR10HeadTimelineCostIndependentOfHistory(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	ctx := context.Background()
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			small := r10Node(t, g, 2)
			big := r10Node(t, g, 300)
			bigRel := r10Rel(t, g, 300)
			// Warm: the first head read of an entity may fold its history.
			_ = headReadsCost(t, g, nodeIDArg{small})
			_ = headReadsCost(t, g, nodeIDArg{big})
			_ = headReadsCost(t, g, relIDArg{bigRel})
			base := headReadsCost(t, g, nodeIDArg{small})
			// The gate counts (deterministic, whatever the load): every read
			// is answered from the head entry, and a 300-version entity reads
			// no more history rows than a 2-version one. Allocations count
			// the whole process and are logged only (MEMGATE).
			check := func(what string, id any) {
				t.Helper()
				c := headReadsCost(t, g, id)
				t.Logf("%s: %d/%d head hits, %d history rows (2 versions: %d); %d allocs per read (2 versions: %d; process-wide, log only)",
					what, c.hits, headCostReads, c.rows, base.rows, c.allocs, base.allocs)
				if c.hits != headCostReads || c.rows > base.rows {
					t.Errorf("%s: %d of %d reads from the head entry, %d history rows read (2 versions: %d): the read folds the history",
						what, c.hits, headCostReads, c.rows, base.rows)
				}
			}
			if base.hits != headCostReads {
				t.Fatalf("2 versions: %d of %d reads from the head entry", base.hits, headCostReads)
			}
			check("node, 300 versions", nodeIDArg{big})
			check("rel, 300 versions", relIDArg{bigRel})
			// sigma's cut: write one more version of the same validity in a
			// GraphTx, then read. The read after the write stays cheap.
			for i := range 3 {
				tx, err := g.BeginTx()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.SetNodeVersionInterval(big, 1000, 2000, map[string]any{"ov_c1": int64(-i)}); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.SetRelVersionInterval(bigRel, 1000, 2000, map[string]any{"ov_c1": int64(-i)}); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				check(fmt.Sprintf("node after correction %d", i), nodeIDArg{big})
				check(fmt.Sprintf("rel after correction %d", i), relIDArg{bigRel})
				if _, err := g.Temporal.SetNodeVersionInterval(ctx, big, 1000, 2000, map[string]any{"ov_c2": int64(i)}); err != nil {
					t.Fatal(err)
				}
				check(fmt.Sprintf("node after standalone correction %d", i), nodeIDArg{big})
			}
			segs, err := g.Temporal.NodeEffectiveTimeline(big, g.peekNow())
			if err != nil || len(segs) != 1 || fmt.Sprint(segs[0].Node.PropertiesMap()["ov_c2"]) != "2" {
				t.Fatalf("head timeline: %v %v", segs, err)
			}
		})
	}
}

// headCostReads is the head reads headReadsCost samples.
const headCostReads = 10

// headReadCost is what headCostReads head reads of one entity cost: the reads
// answered from the head entry and the full history rows read
// (Core.historyRead), both counts, and the allocations per read (log only:
// the whole process).
type headReadCost struct{ hits, rows, allocs uint64 }

// headReadsCost reads id (nodeIDArg or relIDArg) headCostReads times.
func headReadsCost(t *testing.T, g *Core, id any) headReadCost {
	t.Helper()
	var c headReadCost
	g.historyRead = func(n int) { c.rows += uint64(n) } // #nosec G115 -- a length, never negative
	defer func() { g.historyRead = nil }()
	hits := g.headHits.Load()
	c.allocs = nodeAllocsOnce(t, g, id)
	c.hits = g.headHits.Load() - hits
	return c
}

// nodeAllocsOnce is one sample of the allocations per head read of id
// (nodeIDArg or relIDArg), over headCostReads reads.
func nodeAllocsOnce(t *testing.T, g *Core, id any) uint64 {
	t.Helper()
	return mallocs(func() {
		for range headCostReads {
			var err error
			switch x := id.(type) {
			case nodeIDArg:
				_, err = g.Temporal.NodeEffectiveTimeline(x.id, g.peekNow())
			case relIDArg:
				_, err = g.Temporal.RelEffectiveTimeline(x.id, g.peekNow())
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}) / headCostReads
}
