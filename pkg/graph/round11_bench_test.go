package graph_test

import (
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// BenchmarkRound11PointSeek (round 11 item 1): one indexed equality seek of
// the number 1000 (ten forms, three matches in three widths) and of a string
// with the same matches, through the plain door and the pinned named door, on
// every backend over 1000 rows. Before round 11 the numeric seek asked the
// store once per form.
func BenchmarkRound11PointSeek(b *testing.B) {
	for _, sb := range allStoreBackends() {
		t := &testing.T{} // the helpers' t: its cleanups never run, so the graph is closed below
		g := sb.open(t)
		r11SeekWorld(t, g, 1000)
		now, err := g.Temporal().NowTx()
		if err != nil {
			b.Fatal(err)
		}
		for _, c := range []struct {
			name string
			seek func() (int, error)
		}{
			{"num", func() (int, error) {
				rows, err := g.Nodes().ByLabelAndProperty("Ev", "k", int64(1000), storepkg.QueryOpts{})
				return len(rows), err
			}},
			{"str", func() (int, error) {
				rows, err := g.Nodes().ByLabelAndProperty("Ev", "s", "v0", storepkg.QueryOpts{})
				return len(rows), err
			}},
			{"num-at", func() (int, error) {
				rows, err := g.Temporal().NodesByLabelPropertyAt("Ev", "k", int64(1000), now)
				return len(rows), err
			}},
		} {
			b.Run(sb.name+"/"+c.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if n, err := c.seek(); err != nil || n != 3 {
						b.Fatalf("%d rows, %v", n, err)
					}
				}
			})
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
