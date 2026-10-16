package graph_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// round9Bench is the round 9 benchmarks' graph: n nodes of label "Idx" with
// an indexed property v over 10,000 values, two versions each (the second
// value n above the first), 1,000 of them timed (explicit valid-from), the
// rest untimed; pin after the writes.
func round9Bench(b *testing.B, cfg graphpkg.Config, n int) (*graphpkg.Graph, types.Instant) {
	b.Helper()
	ctx := context.Background()
	g, err := graphpkg.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	if err := g.Index().CreateProperty("Idx", "v"); err != nil {
		b.Fatal(err)
	}
	var pin types.Instant
	for lo := 0; lo < n; lo += 4096 {
		props := make([]map[string]any, min(4096, n-lo))
		for i := range props {
			props[i] = map[string]any{"v": float64((lo + i) % 10_000)}
			if lo+i < 1000 {
				props[i]["tkg_valid_from"] = types.Instant(1000 + lo + i)
			}
		}
		var nodes []*types.Node
		if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			var err error
			nodes, err = tx.AddNodes([]string{"Idx"}, props)
			return err
		}); err != nil {
			b.Fatal(err)
		}
		for i, nd := range nodes {
			up := map[string]any{"v": float64(n + (lo+i)%10_000)}
			if lo+i < 1000 {
				up["tkg_valid_from"] = types.Instant(5000 + lo + i)
			}
			r, err := g.Nodes().Update(ctx, nd.ID(), up)
			if err != nil {
				b.Fatal(err)
			}
			pin, _, _ = r.TxStamps()
		}
	}
	return g, pin
}

// round9TimelinePins counts the pins timeline-first asked, across runs.
var round9TimelinePins int

var round9Backends = []struct {
	name string
	cfg  graphpkg.Config
}{
	{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
	{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
}

// BenchmarkRound9Doors (sigma-tkgd round 9) at 100,000 nodes with two
// versions each, one CPU:
//   - range-at / range-any / range-any-count: R9-1's target, a range of 500
//     entities on their earlier version (the any-version doors) against
//     RangeCardinalityAt over 500 resolved versions on the same data;
//   - timeline-first / timeline-cached: R9-2, the count timeline at a new
//     pin (a build per call; run it with a fixed -benchtime Nx) and a
//     cached read;
//   - values-cached / values-after-write / values-pin: R9-3, the current
//     statistics cached, the first read after a write that shrinks a value,
//     and at a pin (cached after its first build).
func BenchmarkRound9Doors(b *testing.B) {
	const n = 100_000
	ctx := context.Background()
	for _, backend := range round9Backends {
		g, pin := round9Bench(b, backend.cfg, n)
		at := storepkg.QueryOpts{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pin}
		run := func(name string, fn func(b *testing.B)) {
			b.Run(backend.name+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				fn(b)
			})
		}
		run("range-at", func(b *testing.B) {
			for b.Loop() {
				c, exact, err := g.Nodes().RangeCardinalityAt("Idx", "v", n+1000, n+1049, true, true, at)
				if err != nil || !exact || c != 500 {
					b.Fatal(c, exact, err)
				}
			}
		})
		run("range-any", func(b *testing.B) {
			for b.Loop() {
				got := 0
				if err := g.Nodes().ForEachByLabelPropertyRangeAnyVersion("Idx", "v", 1000, 1049, true, true, pin, func(*types.Node) bool {
					got++
					return true
				}); err != nil || got != 500 {
					b.Fatal(got, err)
				}
			}
		})
		run("range-any-count", func(b *testing.B) {
			for b.Loop() {
				c, exact, err := g.Nodes().RangeCardinalityAnyVersion("Idx", "v", 1000, 1049, true, true, pin)
				if err != nil || !exact || c.Entities != 500 {
					b.Fatal(c, exact, err)
				}
			}
		})
		run("timeline-first", func(b *testing.B) {
			for b.Loop() {
				// A pin no call asked before (the writes below it are
				// those of pin for a bounded run: -benchtime Nx), so
				// every call builds.
				round9TimelinePins++
				p := pin - types.Instant(round9TimelinePins)
				if _, exact, err := g.Nodes().CountTimelineByLabel("Idx", p, storepkg.CountTimelineOpts{UntimedFromMinusInf: true}); err != nil || !exact {
					b.Fatal(exact, err)
				}
			}
		})
		run("timeline-cached", func(b *testing.B) {
			for b.Loop() {
				segs, exact, err := g.Nodes().CountTimelineByLabel("Idx", pin, storepkg.CountTimelineOpts{UntimedFromMinusInf: true})
				if err != nil || !exact || len(segs) == 0 {
					b.Fatal(exact, err)
				}
			}
		})
		run("values-cached", func(b *testing.B) {
			for b.Loop() {
				s, exact, err := g.Stats().PropertyValueStats("Idx", "v", 0)
				if err != nil || !exact || s.Distinct != 10_000 {
					b.Fatal(s, exact, err)
				}
			}
		})
		nodes, err := g.Nodes().ByLabel("Idx", storepkg.QueryOpts{Limit: 64})
		if err != nil {
			b.Fatal(err)
		}
		run("values-after-write", func(b *testing.B) {
			i := 0
			for b.Loop() {
				b.StopTimer()
				nd := nodes[i%len(nodes)]
				i++
				if _, err := g.Nodes().Update(ctx, nd.ID(), map[string]any{"v": float64(n + 10_000 + i)}); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if _, exact, err := g.Stats().PropertyValueStats("Idx", "v", 0); err != nil || !exact {
					b.Fatal(exact, err)
				}
			}
		})
		run("values-pin", func(b *testing.B) {
			for b.Loop() {
				s, exact, err := g.Stats().PropertyValueStats("Idx", "v", pin)
				if err != nil || !exact || s.Rows != 2*n {
					b.Fatal(s, exact, err)
				}
			}
		})
		_ = g.Close()
	}
}

// BenchmarkIndexedUpdateWritePath: an Update of an indexed property on
// memory and badger (R9-3's write cost: the index drops its cached value
// statistics, one pointer store). Runs on the base too (no round 9 API).
func BenchmarkIndexedUpdateWritePath(b *testing.B) {
	ctx := context.Background()
	for _, backend := range round9Backends {
		b.Run(backend.name, func(b *testing.B) {
			g, err := graphpkg.New(backend.cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			if err := g.Index().CreateProperty("P", "v"); err != nil {
				b.Fatal(err)
			}
			nd, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(0)})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			i := int64(0)
			for b.Loop() {
				i++
				if _, err := g.Nodes().Update(ctx, nd.ID(), map[string]any{"v": i % 1000}); err != nil {
					b.Fatal(fmt.Sprint(err))
				}
			}
		})
	}
}
