package graph_test

import (
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkColumnChunks (sigma-tkgd round 9, W10's typed chunk scan): one
// pass over a 100,000-member label's column snapshot of two integer columns
// and one string column of 1,000 values. "rowat" reads each position boxed
// (RowAt) and type-switches each value, as a consumer of the row door does;
// "chunk" reads typed chunks of 1,024 rows into one reused chunk and reads
// each value from its vector; "chunk-only" reads the chunks alone (the
// door's own cost). ns/row-col is per row and column.
func BenchmarkColumnChunks(b *testing.B) {
	const n = 100_000
	keys := []string{"a", "b", "s"}
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
		for lo := 0; lo < n; lo += 4096 {
			props := make([]map[string]any, min(4096, n-lo))
			for i := range props {
				k := lo + i
				props[i] = map[string]any{"a": int64(k), "b": int64(k % 97), "s": fmt.Sprintf("v%d", k%1000)}
			}
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
				_, err := tx.AddNodes([]string{"W"}, props)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
		snap, _, ok, err := g.Nodes().DocValuesSnapshot("W", keys)
		if err != nil || !ok {
			b.Fatal(ok, err)
		}
		cr := snap.(types.NodeColumnChunkReader)
		if cr.Len() != n {
			b.Fatalf("%d rows", cr.Len())
		}
		for _, mode := range []string{"rowat", "chunk", "chunk-only"} {
			b.Run(backend.name+"/"+mode, func(b *testing.B) {
				b.ReportAllocs()
				var chunk types.ColumnChunk
				vals, present := make([]any, len(keys)), make([]bool, len(keys))
				for b.Loop() {
					var sum int64
					var bytes int
					switch mode {
					case "rowat":
						for i := range n {
							_ = cr.RowAt(i, vals, present)
							for c := range keys {
								if !present[c] {
									continue
								}
								switch v := vals[c].(type) {
								case int64:
									sum += v
								case string:
									bytes += len(v)
								}
							}
						}
					case "chunk":
						for from := 0; from < n; from += 1024 {
							cr.ReadChunk(from, min(from+1024, n), &chunk)
							a, bb, s := chunk.Cols[0], chunk.Cols[1], chunk.Cols[2]
							for i := range chunk.IDs {
								if a.Present[i] {
									sum += a.Ints[i]
								}
								if bb.Present[i] {
									sum += bb.Ints[i]
								}
								if s.Present[i] {
									bytes += len(s.Dict[s.Codes[i]])
								}
							}
						}
					}
					if mode == "chunk-only" {
						for from := 0; from < n; from += 1024 {
							cr.ReadChunk(from, min(from+1024, n), &chunk)
						}
						sum, bytes = int64(len(chunk.IDs)), 1
					}
					if sum == 0 || bytes == 0 {
						b.Fatal("nothing read")
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n*len(keys)), "ns/row-col")
			})
		}
		_ = g.Close()
	}
}
