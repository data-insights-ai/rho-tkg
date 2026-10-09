package bench

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkLatestStamps measures g.Rels().LatestStamps on one relationship
// with 0 ("plain"), 100 and 1,000 history versions (updates; every tenth
// step a bounded correction plus a correction inside it, whose pieces are
// appended above the current row), memory and badger (backlog 30). The door
// is 0 allocs/op on a built sidecar and a cached current row; the regression
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
