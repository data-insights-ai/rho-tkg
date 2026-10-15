package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkLabelTimeStats (round 8 request 3): 100,000 untimed nodes of one
// label. "eternal" reads the stats with no history row in the store (served
// from the cache; LastEnded O(1)); "history" the same after 1,000 of them were
// updated (LastEnded one stamp lookup per ever-member); "write+read" updates
// one member (always the same, so the members with history stay 1,000), then
// reads (the explicit count and LastEnded recounted after the write).
func BenchmarkLabelTimeStats(b *testing.B) {
	const n = 100_000
	ctx := context.Background()
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		g, err := graphpkg.New(backend.cfg)
		if err != nil {
			b.Fatal(err)
		}
		var ids []types.NodeID
		for lo := 0; lo < n; lo += 8192 {
			props := make([]map[string]any, min(8192, n-lo))
			for i := range props {
				props[i] = map[string]any{"v": int64(lo + i)}
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				nodes, err := tx.AddNodes([]string{"P"}, props)
				for _, nd := range nodes {
					ids = append(ids, nd.ID())
				}
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
		read := func(b *testing.B, wantEternal bool) {
			st, _, err := g.Stats().LabelTimeStats("P")
			if err != nil || st.Members != n || st.Eternal() != wantEternal {
				b.Fatal(st, err)
			}
		}
		b.Run(fmt.Sprintf("%s/eternal", backend.name), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				read(b, true)
			}
		})
		for i := range 1000 {
			if _, err := g.Nodes().Update(ctx, ids[i*97], map[string]any{"v": int64(-1)}); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(fmt.Sprintf("%s/history", backend.name), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				read(b, false)
			}
		})
		b.Run(fmt.Sprintf("%s/write+read", backend.name), func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				i++
				if _, err := g.Nodes().Update(ctx, ids[0], map[string]any{"v": int64(i)}); err != nil {
					b.Fatal(err)
				}
				read(b, false)
			}
		})
		_ = g.Close()
	}
}
