package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkNodesAtTxByIDs — 100 IDs at one (validAt, txAt), half updated
// after the pin (answered from history), half answered by the current row:
// the batch door against a loop of NodeAtTx (backlog 57).
func BenchmarkNodesAtTxByIDs(b *testing.B) {
	for _, be := range []struct {
		name string
		cfg  Config
	}{{"memory", Config{}}, {"badger", Config{BadgerInMemory: true}}} {
		b.Run(be.name, func(b *testing.B) {
			g, err := New(be.cfg)
			if err != nil {
				b.Fatalf("New: %v", err)
			}
			defer func() { _ = g.Close() }()
			ctx := context.Background()
			ids := make([]types.NodeID, 100)
			for i := range ids {
				n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"tkg_valid_from": types.Instant(1000), "i": int64(i)})
				if err != nil {
					b.Fatalf("Add: %v", err)
				}
				ids[i] = n.ID()
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				b.Fatalf("NowTx: %v", err)
			}
			for _, id := range ids[:50] {
				if _, err := g.Nodes.Update(ctx, id, map[string]any{"u": true}); err != nil {
					b.Fatalf("Update: %v", err)
				}
			}
			b.Run("loop", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					for _, id := range ids {
						if _, err := g.Temporal.NodeAtTx(id, pin, pin); err != nil {
							b.Fatalf("NodeAtTx: %v", err)
						}
					}
				}
			})
			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := g.Temporal.NodesAtTxByIDs(ids, pin, pin); err != nil {
						b.Fatalf("NodesAtTxByIDs: %v", err)
					}
				}
			})
		})
	}
}
