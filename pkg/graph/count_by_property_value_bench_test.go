package graph_test

import (
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// BenchmarkCountByPropertyValue (round 7 request 4): the count of a label's
// nodes whose indexed value equals 7 (3,000 of 300,000), from the index's set
// sizes ("count") against the lookup a caller had before: the int64 and the
// float64 lookups' rows counted ("lookup").
func BenchmarkCountByPropertyValue(b *testing.B) {
	const n = 300_000
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
		for lo := 0; lo < n; lo += 8192 {
			props := make([]map[string]any, min(8192, n-lo))
			for i := range props {
				props[i] = map[string]any{"v": int64((lo + i) % 100)}
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { _, err := tx.AddNodes([]string{"P"}, props); return err }); err != nil {
				b.Fatal(err)
			}
		}
		for _, mode := range []string{"lookup", "count"} {
			b.Run(fmt.Sprintf("%s/%s", backend.name, mode), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					var got int64
					if mode == "count" {
						c, exact, err := g.Nodes().CountByLabelPropertyValue("P", "v", int64(7), storepkg.QueryOpts{})
						if err != nil || !exact {
							b.Fatal(exact, err)
						}
						got = c
					} else {
						for _, q := range []any{int64(7), 7.0} {
							rows, err := g.Nodes().ByLabelAndProperty("P", "v", q, storepkg.QueryOpts{})
							if err != nil {
								b.Fatal(err)
							}
							got += int64(len(rows))
						}
					}
					if got != n/100 {
						b.Fatalf("count %d", got)
					}
				}
			})
		}
		_ = g.Close()
	}
}
