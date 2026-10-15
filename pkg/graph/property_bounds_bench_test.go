package graph_test

import (
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkPropertyBounds (round 8 request 1): the numeric bounds of an
// indexed key over 300,000 nodes. "bounds" is Stats().PropertyBounds alone;
// "write+bounds" and "write+stats" first move the maximum (one Update of the
// node holding it, so the old maximum leaves), then read the bounds through
// PropertyBounds or through PropertyStats' exact Min/Max (which rescans the
// label after a write removed an extreme value).
func BenchmarkPropertyBounds(b *testing.B) {
	const n = 300_000
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
		if err := g.Index().CreateProperty("P", "v"); err != nil {
			b.Fatal(err)
		}
		var top types.NodeID
		for lo := 0; lo < n; lo += 8192 {
			props := make([]map[string]any, min(8192, n-lo))
			for i := range props {
				props[i] = map[string]any{"v": float64(lo + i)}
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				nodes, err := tx.AddNodes([]string{"P"}, props)
				if err == nil && lo+len(props) == n {
					top = nodes[len(nodes)-1].ID()
				}
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
		for _, mode := range []string{"bounds", "write+bounds", "write+stats"} {
			b.Run(fmt.Sprintf("%s/%s", backend.name, mode), func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					want := float64(n - 1)
					if mode != "bounds" {
						i++
						want = float64(n + i%2)
						if _, err := g.Nodes().Update(ctx, top, map[string]any{"v": want}); err != nil {
							b.Fatal(err)
						}
					}
					var got float64
					if mode == "write+stats" {
						st, err := g.Stats().PropertyStats("P", "v")
						if err != nil {
							b.Fatal(err)
						}
						got = st.Max.(float64)
					} else {
						bd, ok, err := g.Stats().PropertyBounds("P", "v", storepkg.QueryOpts{})
						if err != nil || !ok {
							b.Fatal(ok, err)
						}
						got = bd.Max
					}
					if got != want {
						b.Fatalf("max %v, want %v", got, want)
					}
				}
			})
		}
		_ = g.Close()
	}
}
