package bench

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// historyPresenceFixtureRels is the relationship count of the history
// presence fixture; every 100th relationship (1 %) is updated once, so it
// holds one history row while its neighbours stay plain.
const (
	historyPresenceFixtureRels = 10_000
	historyPresenceStride      = 100
)

// historyPresenceFixture builds the fixture and returns the plain and the
// updated relationship IDs.
func historyPresenceFixture(b *testing.B, g *graph.Graph) (plain, updated []types.RelID) {
	b.Helper()
	ctx := benchCtx()
	nodes := addLabeledNodes(b, g, "Host", 1000)
	for i := 0; i < historyPresenceFixtureRels; i++ {
		r, err := g.Rels().AddByID(ctx, "CONNECTS", nodes[i%len(nodes)], nodes[(i*7+1)%len(nodes)], map[string]any{"seq": i})
		if err != nil {
			b.Fatalf("add rel %d: %v", i, err)
		}
		if i%historyPresenceStride == 0 {
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"seq": -i - 1}); err != nil {
				b.Fatalf("update rel %d: %v", i, err)
			}
			updated = append(updated, r.ID())
			continue
		}
		plain = append(plain, r.ID())
	}
	return plain, updated
}

// BenchmarkRelHistoryPlain measures g.Rels().History for a relationship
// without history in a graph where 1 % of the relationships hold history:
// the per-entity cost an effective-state reader paid before HasHistory
// (handover-effective-read-cost-20261009 §1, backlog 20).
func BenchmarkRelHistoryPlain(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			plain, _ := historyPresenceFixture(b, g)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				h, err := g.Rels().History(plain[i%len(plain)])
				if err != nil || len(h) != 0 {
					b.Fatalf("plain rel: %d rows, %v", len(h), err)
				}
				i++
			}
		})
	}
}

// BenchmarkRelHasHistory measures g.Rels().HasHistory on the same fixture,
// for a plain relationship (miss) and an updated one (hit). The first call
// builds badger's RAM set outside the timed loop.
func BenchmarkRelHasHistory(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			plain, updated := historyPresenceFixture(b, g)
			for _, tc := range []struct {
				name string
				ids  []types.RelID
				want bool
			}{{"miss", plain, false}, {"hit", updated, true}} {
				b.Run(tc.name, func(b *testing.B) {
					if has, err := g.Rels().HasHistory(tc.ids[0]); err != nil || has != tc.want {
						b.Fatalf("warm-up: %v, %v", has, err)
					}
					b.ReportAllocs()
					i := 0
					for b.Loop() {
						has, err := g.Rels().HasHistory(tc.ids[i%len(tc.ids)])
						if err != nil || has != tc.want {
							b.Fatalf("HasHistory = %v, %v; want %v", has, err, tc.want)
						}
						i++
					}
				})
			}
		})
	}
}
