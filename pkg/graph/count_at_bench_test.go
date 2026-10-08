package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// countAtGraph: 20,000 Person and 20,000 Other nodes, a KNOWS from each
// person to its other; then 2,000 people and 1,000 relationships updated and
// 1,000 people deleted with their relationships. t0 is a pin before the
// changes, t1 an instant after them.
func countAtGraph(b *testing.B, cfg graphpkg.Config) (*graphpkg.Graph, types.Instant, types.Instant) {
	b.Helper()
	g, err := graphpkg.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	people := make([]types.NodeID, 0, 20000)
	rels := make([]types.RelID, 0, 20000)
	for i := range 20000 {
		p, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"i": int64(i), "name": "x"})
		if err != nil {
			b.Fatal(err)
		}
		o, err := g.Nodes().Add(ctx, []string{"Other"}, map[string]any{"i": int64(i)})
		if err != nil {
			b.Fatal(err)
		}
		r, err := g.Rels().AddByID(ctx, "KNOWS", p.ID(), o.ID(), map[string]any{"w": int64(i)})
		if err != nil {
			b.Fatal(err)
		}
		people = append(people, p.ID())
		rels = append(rels, r.ID())
	}
	t0, err := g.Temporal().NowTx()
	if err != nil {
		b.Fatal(err)
	}
	for i := range 2000 {
		if _, err := g.Nodes().Update(ctx, people[i*10], map[string]any{"i": int64(-i)}); err != nil {
			b.Fatal(err)
		}
	}
	for i := range 1000 {
		if _, err := g.Rels().Update(ctx, rels[i*20+1], map[string]any{"w": int64(-i)}); err != nil {
			b.Fatal(err)
		}
	}
	for i := range 1000 {
		if err := g.Rels().Delete(ctx, rels[i*10+5]); err != nil {
			b.Fatal(err)
		}
		if err := g.Nodes().Delete(ctx, people[i*10+5]); err != nil {
			b.Fatal(err)
		}
	}
	time.Sleep(2 * time.Millisecond)
	t1 := types.InstantFromTime(time.Now())
	if now, err := g.Temporal().NowTx(); err == nil && now > t1 {
		t1 = now
	}
	return g, t0, t1
}

// BenchmarkCountAt: the exact count of a label's nodes (a type's
// relationships) at a read coordinate — len(ByLabel) / len(ByType) against
// CountByLabelAt / CountByTypeAt, memory and badger; and the count at a pin
// whose as-of column set DocValuesSnapshotAsOf cached.
func BenchmarkCountAt(b *testing.B) {
	for _, be := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{}},
		{"badger", graphpkg.Config{BadgerInMemory: true}},
	} {
		g, t0, t1 := countAtGraph(b, be.cfg)
		for _, q := range []struct {
			name string
			opts graphpkg.QueryOpts
		}{
			{"validAt", graphpkg.QueryOpts{ValidAt: t1}},
			{"window", graphpkg.QueryOpts{ValidStart: t0, ValidEnd: t1}},
			{"txPin", graphpkg.QueryOpts{TxPin: t0}},
		} {
			b.Run(be.name+"/nodes/"+q.name+"/ByLabel", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					ns, err := g.Nodes().ByLabel("Person", q.opts)
					if err != nil || len(ns) < 18000 {
						b.Fatal(len(ns), err)
					}
				}
			})
			b.Run(be.name+"/nodes/"+q.name+"/CountByLabelAt", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					n, err := g.Nodes().CountByLabelAt("Person", q.opts)
					if err != nil || n < 18000 {
						b.Fatal(n, err)
					}
				}
			})
			b.Run(be.name+"/rels/"+q.name+"/ByType", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rs, err := g.Rels().ByType("KNOWS", q.opts)
					if err != nil || len(rs) < 18000 {
						b.Fatal(len(rs), err)
					}
				}
			})
			b.Run(be.name+"/rels/"+q.name+"/CountByTypeAt", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					n, err := g.Rels().CountByTypeAt("KNOWS", q.opts)
					if err != nil || n < 18000 {
						b.Fatal(n, err)
					}
				}
			})
		}
		if _, _, ok, err := g.Nodes().DocValuesSnapshotAsOf("Person", nil, t0); err != nil || !ok {
			b.Fatal(ok, err)
		}
		b.Run(be.name+"/nodes/txPinCached/CountByLabelAt", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n, err := g.Nodes().CountByLabelAt("Person", graphpkg.QueryOpts{TxPin: t0})
				if err != nil || n != 20000 {
					b.Fatal(n, err)
				}
			}
		})
		_ = g.Close()
	}
}
