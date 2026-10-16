package core

import (
	"context"
	"fmt"
	"math"
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
			// The minimum of five samples: mallocs counts the whole process,
			// so another goroutine's allocations (a badger flush, a parallel
			// test) can land in one sample (minAllocsPerRun in the badger
			// column-scan alloc tests).
			nodeAllocs := func(id any) uint64 {
				best := uint64(math.MaxUint64)
				for range 5 {
					best = min(best, nodeAllocsOnce(t, g, id))
				}
				return best
			}
			// Warm: the first head read of an entity may fold its history.
			_ = nodeAllocs(nodeIDArg{small})
			_ = nodeAllocs(nodeIDArg{big})
			_ = nodeAllocs(relIDArg{bigRel})
			base := nodeAllocs(nodeIDArg{small})
			limit := base + 30
			if got := nodeAllocs(nodeIDArg{big}); got > limit {
				t.Errorf("node, 300 versions: %d allocs per head read, 2 versions: %d (limit %d): the read folds the history", got, base, limit)
			}
			if got := nodeAllocs(relIDArg{bigRel}); got > limit {
				t.Errorf("rel, 300 versions: %d allocs per head read (limit %d)", got, limit)
			}
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
				if got := nodeAllocs(nodeIDArg{big}); got > limit {
					t.Errorf("node after correction %d: %d allocs per head read (limit %d)", i, got, limit)
				}
				if got := nodeAllocs(relIDArg{bigRel}); got > limit {
					t.Errorf("rel after correction %d: %d allocs per head read (limit %d)", i, got, limit)
				}
				if _, err := g.Temporal.SetNodeVersionInterval(ctx, big, 1000, 2000, map[string]any{"ov_c2": int64(i)}); err != nil {
					t.Fatal(err)
				}
				if got := nodeAllocs(nodeIDArg{big}); got > limit {
					t.Errorf("node after standalone correction %d: %d allocs per head read (limit %d)", i, got, limit)
				}
			}
			segs, err := g.Temporal.NodeEffectiveTimeline(big, g.peekNow())
			if err != nil || len(segs) != 1 || fmt.Sprint(segs[0].Node.PropertiesMap()["ov_c2"]) != "2" {
				t.Fatalf("head timeline: %v %v", segs, err)
			}
		})
	}
}

// nodeAllocsOnce is one sample of the allocations per head read of id
// (nodeIDArg or relIDArg), over ten reads.
func nodeAllocsOnce(t *testing.T, g *Core, id any) uint64 {
	t.Helper()
	return mallocs(func() {
		for range 10 {
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
	}) / 10
}
