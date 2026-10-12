package bench

import (
	"fmt"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkLatestStamps measures g.Rels().LatestStamps on one relationship
// with 0 ("plain"), 100 and 1,000 history versions (updates; every tenth
// step a bounded correction plus a correction inside it, whose pieces are
// appended above the current row), memory and badger (backlog 30). On these
// two backends the door is 0 allocs/op on a built sidecar and a cached current
// row (tiered and sharded allocate per call: routing, and sharded copies the
// current row); the regression
// it exists to catch — the door falling back to the History fold — allocates
// per history row (2,638 allocs/op at 100 versions, 230,388 at 10,000 on
// badger), so the family is allocs-gated (bench-gate.awk). 1,000 keeps the
// fixture to seconds in CI; pkg/graph's BenchmarkLatestStamps measures 10,000
// against the History scan. A warm-up call builds badger's sidecar outside
// the timed loop.
func BenchmarkLatestStamps(b *testing.B) {
	for _, bc := range backendCases {
		for _, versions := range []int{0, 100, 1_000} {
			name := fmt.Sprintf("%s/versions=%d", bc.name, versions)
			if versions == 0 {
				name = bc.name + "/plain"
			}
			b.Run(name, func(b *testing.B) {
				g := newBenchGraph(b, bc)
				id := latestStampsFixture(b, g, versions)
				if _, _, _, err := g.Rels().LatestStamps(id); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					if _, _, deleted, err := g.Rels().LatestStamps(id); err != nil || deleted {
						b.Fatalf("LatestStamps: deleted=%v, %v", deleted, err)
					}
				}
			})
		}
	}
	// Rotating IDs over an entity cache far smaller than the IDs (backlog 42):
	// badger reads nearly every current row from disk, tiered routes to the
	// owner shard, sharded asks the owner slot. These rows gate the current-row
	// half: a door that decodes the whole row on a cache miss (v4.49.0: 25
	// allocs/op on badger, 38 on tiered, 34 on sharded at the same miss rate in
	// pkg/graph's BenchmarkLatestStampsRotating) fails the allocs gate.
	for _, backend := range []string{"badger", "tiered", "sharded"} {
		b.Run(backend+"/rotating-cache-miss", func(b *testing.B) {
			g, flush := latestStampsRotatingGraph(b, backend)
			ids := latestStampsRotatingFixture(b, g, 2_000)
			if err := flush(); err != nil {
				b.Fatal(err)
			}
			if _, _, _, err := g.Rels().LatestStamps(ids[0]); err != nil { // builds the sidecars
				b.Fatal(err)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if _, _, deleted, err := g.Rels().LatestStamps(ids[(i*7919)%len(ids)]); err != nil || deleted {
					b.Fatalf("LatestStamps: deleted=%v, %v", deleted, err)
				}
				i++
			}
		})
	}
}

// latestStampsRotatingCache is the per-cache entry capacity of the rotating
// rows: far below the fixture's 2,000 relationships, so a rotating read misses.
const latestStampsRotatingCache = 64

// latestStampsRotatingGraph opens backend (badger, tiered or sharded) on a
// temporary directory with a small entity cache and returns the graph and the
// store's flush.
func latestStampsRotatingGraph(b *testing.B, backend string) (*graph.Graph, func() error) {
	b.Helper()
	var st storepkg.Store
	var flush func() error
	switch backend {
	case "badger":
		s, err := badger.New(badger.Config{Dir: b.TempDir(), CacheCapacity: latestStampsRotatingCache})
		if err != nil {
			b.Fatal(err)
		}
		st, flush = s, s.Flush
	case "tiered":
		s, err := tiered.New(tiered.Config{DataDir: b.TempDir(), ShardWindow: 7 * 24 * time.Hour, CacheCapacity: latestStampsRotatingCache})
		if err != nil {
			b.Fatal(err)
		}
		st, flush = s, s.Flush
	default:
		s, err := sharded.New(sharded.Config{Dir: b.TempDir(), BaseSlot: 0, SlotCount: 2, CacheCapacity: latestStampsRotatingCache})
		if err != nil {
			b.Fatal(err)
		}
		st, flush = s, s.Flush
	}
	g, err := graph.New(graph.Config{Store: st})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	return g, flush
}

// latestStampsRotatingFixture adds rels relationships over 200 nodes, every
// hundredth with ten history rows, and returns their IDs.
func latestStampsRotatingFixture(b *testing.B, g *graph.Graph, rels int) []types.RelID {
	b.Helper()
	ctx := benchCtx()
	nodes := addLabeledNodes(b, g, "Host", 200)
	ids := make([]types.RelID, 0, rels)
	for i := 0; i < rels; i++ {
		r, err := g.Rels().AddByID(ctx, "SEEN", nodes[i%len(nodes)], nodes[(i*7+1)%len(nodes)], map[string]any{"w": int64(i)})
		if err != nil {
			b.Fatal(err)
		}
		ids = append(ids, r.ID())
		if i%100 == 0 {
			for v := 0; v < 10; v++ {
				if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(-v)}); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	return ids
}

// latestStampsFixture builds one relationship with at least versions history
// rows and returns its ID.
func latestStampsFixture(b *testing.B, g *graph.Graph, versions int) types.RelID {
	b.Helper()
	ctx := benchCtx()
	nodes := addLabeledNodes(b, g, "Host", 2)
	r, err := g.Rels().AddByID(ctx, "SEEN", nodes[0], nodes[1], map[string]any{"w": int64(0)})
	if err != nil {
		b.Fatal(err)
	}
	rows := 0
	for v := 1; rows < versions; v++ {
		if v%10 != 0 {
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(v)}); err != nil {
				b.Fatal(err)
			}
			rows++
			continue
		}
		now, err := g.Temporal().NowTx()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := g.Temporal().SetRelVersionInterval(ctx, r.ID(), now+10, now+20, map[string]any{"w": int64(-v)}); err != nil {
			b.Fatal(err)
		}
		if _, err := g.Temporal().SetRelVersionInterval(ctx, r.ID(), now+11, now+15, map[string]any{"w": int64(-v)}); err != nil {
			b.Fatal(err)
		}
		rows += 4
	}
	return r.ID()
}
