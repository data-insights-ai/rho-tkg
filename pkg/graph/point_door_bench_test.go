package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// pointDoorGraph: 5,000 Person nodes, each with a KNOWS to an Other node;
// every person and relationship updated once after the pin t0. t1 is a pin
// after the updates. Hot reads (pin t1, valid t1) are answered by the current
// row; cold reads (pin t0, valid t0) resolve the history row (backlog 32
// benchmarks: the point doors before and after the history-first move order).
func pointDoorGraph(b *testing.B, cfg graphpkg.Config) (*graphpkg.Graph, []types.NodeID, []types.RelID, types.Instant, types.Instant) {
	b.Helper()
	g, err := graphpkg.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	const n = 5000
	people := make([]types.NodeID, 0, n)
	rels := make([]types.RelID, 0, n)
	for i := range n {
		p, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"i": int64(i)})
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
	for i := range n {
		if _, err := g.Nodes().Update(ctx, people[i], map[string]any{"i": int64(-i - 1)}); err != nil {
			b.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, rels[i], map[string]any{"w": int64(-i - 1)}); err != nil {
			b.Fatal(err)
		}
	}
	t1, err := g.Temporal().NowTx()
	if err != nil {
		b.Fatal(err)
	}
	return g, people, rels, t0, t1
}

func BenchmarkPointDoors(b *testing.B) {
	for _, be := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{}},
		{"badger", graphpkg.Config{BadgerInMemory: true}},
	} {
		g, people, rels, t0, t1 := pointDoorGraph(b, be.cfg)
		for _, temp := range []struct {
			name         string
			validAt, pin types.Instant
			version      uint32
		}{
			{"hot", t1, t1, 1},
			{"cold", t0, t0, 0},
		} {
			b.Run(be.name+"/NodeAtTx/"+temp.name, func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					n, err := g.Temporal().NodeAtTx(people[i%len(people)], temp.validAt, temp.pin)
					if err != nil || n.Version() != temp.version {
						b.Fatal(n.Version(), n.Temporal(), temp.validAt, temp.pin, err)
					}
					i++
				}
			})
			b.Run(be.name+"/RelAtTx/"+temp.name, func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					r, err := g.Temporal().RelAtTx(rels[i%len(rels)], temp.validAt, temp.pin)
					if err != nil || r.Version() != temp.version {
						b.Fatal(r.Version(), r.Temporal(), temp.validAt, temp.pin, err)
					}
					i++
				}
			})
			b.Run(be.name+"/NodeAsOf/"+temp.name, func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					n, err := g.Temporal().NodeAsOf(people[i%len(people)], temp.pin)
					if err != nil || n.Version() != temp.version {
						b.Fatal(n.Version(), n.Temporal(), temp.validAt, temp.pin, err)
					}
					i++
				}
			})
		}
	}
}

// BenchmarkMoveWrites measures the with-history write doors the fix reorders
// (the badger Update moves the current row into history).
func BenchmarkMoveWrites(b *testing.B) {
	for _, be := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{}},
		{"badger", graphpkg.Config{BadgerInMemory: true}},
	} {
		g, people, rels, _, _ := pointDoorGraph(b, be.cfg)
		ctx := context.Background()
		b.Run(be.name+"/Nodes.Update", func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if _, err := g.Nodes().Update(ctx, people[i%len(people)], map[string]any{"i": int64(i)}); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
		b.Run(be.name+"/Rels.Update", func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if _, err := g.Rels().Update(ctx, rels[i%len(rels)], map[string]any{"w": int64(i)}); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}
