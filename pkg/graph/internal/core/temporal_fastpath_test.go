package core

import (
	"context"
	"strconv"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// deepHistoryNode builds a node with `versions` tiled versions via repeated
// Update with increasing valid-from, returning the id and the latest valid-from.
func deepHistoryNode(b *testing.B, g *Core, versions int) (types.NodeID, types.Instant) {
	b.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(0)})
	if err != nil {
		b.Fatalf("Add: %v", err)
	}
	vf := types.Instant(1000)
	for i := 1; i < versions; i++ {
		vf += 1000
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_valid_from": vf, "x": int64(i)}); err != nil {
			b.Fatalf("Update %d: %v", i, err)
		}
	}
	return n.ID(), vf
}

// BenchmarkNodeAt_DeepHistory measures NodeAt across history depths, for both
// a query at the latest valid time and a query at an early historical valid
// time. BACKLOG 10b removed the prior current-row-alone short-circuit
// (nodeCurrentAnswersAt) — it assumed the open current row could never be
// outranked by a bounded history row on belief, which a bounded cascade can
// violate without replacing current (see temporal_queries.go's
// nodeAtLockedTx doc comment) — so both queries below now resolve through
// the same full-chain path; this benchmark tracks how that path scales with
// history depth rather than contrasting a fast vs. slow path.
func BenchmarkNodeAt_DeepHistory(b *testing.B) {
	for _, depth := range []int{8, 64, 256} {
		bs, err := badger.New(badger.Config{InMemory: true, FlushInterval: 1 << 62})
		if err != nil {
			b.Fatalf("badger.New: %v", err)
		}
		g, err := New(Config{Store: bs})
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		id, latestVF := deepHistoryNode(b, g, depth)

		b.Run("latest/depth="+strconv.Itoa(depth), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := g.Temporal.NodeAt(id, latestVF+1); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("historical/depth="+strconv.Itoa(depth), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := g.Temporal.NodeAt(id, 1500); err != nil {
					b.Fatal(err)
				}
			}
		})
		_ = g.Close()
	}
}
