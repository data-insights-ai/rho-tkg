package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func degreeStats(t *testing.T, g *graphpkg.Graph, typeName string) storepkg.RelTypeDegreeStats {
	t.Helper()
	st, err := g.Stats().RelTypeDegreeStats(typeName)
	if err != nil {
		t.Fatalf("RelTypeDegreeStats(%q): %v", typeName, err)
	}
	if !st.Exact {
		t.Fatalf("RelTypeDegreeStats(%q) not exact with no concurrent writer", typeName)
	}
	return st
}

// A hub with 5 outgoing T, a node with 3 incoming T, a self-loop, and an
// unrelated type U whose relationships must not count. Two-phase: after the
// hub loses relationships and a cascade removes the in-hub, the cached
// answer must not survive (each step's stats are exact for that state).
func TestRelTypeDegreeStats(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		labels := []string{"A"}
		if b.tiered {
			labels = []string{"Ref"}
		}
		add := func() *types.Node {
			n, err := g.Nodes().Add(ctx, labels, nil)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
		rel := func(typ string, s, e *types.Node) *types.Relationship {
			r, err := g.Rels().Add(ctx, typ, s, e, nil)
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
		hub, sink := add(), add()
		var spokes []*types.Node
		var hubRels []*types.Relationship
		for i := 0; i < 5; i++ {
			s := add()
			spokes = append(spokes, s)
			hubRels = append(hubRels, rel("T", hub, s))
		}
		for i := 0; i < 3; i++ {
			rel("T", spokes[i], sink)
		}
		var event *types.Node // tiered: a cross-shard edge into the hot event shard
		if b.tiered {
			ev, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			event = ev
			rel("T", sink, ev)
		}
		loopNode := add()
		rel("T", loopNode, loopNode)
		for i := 0; i < 9; i++ {
			rel("U", sink, hub) // U: in-degree 9 at hub, out 9 at sink — must not leak into T
		}

		extra := int64(0)
		if b.tiered {
			extra = 1
		}
		st := degreeStats(t, g, "T")
		want := storepkg.RelTypeDegreeStats{Rels: 9 + extra, Starts: 5 + extra, Ends: 7 + extra, MaxOut: 5, MaxIn: 3, MaxOutNode: hub.ID(), MaxInNode: sink.ID(), Exact: true}
		if st != want {
			t.Fatalf("T stats = %+v, want %+v", st, want)
		}
		if again := degreeStats(t, g, "T"); again != st {
			t.Fatalf("repeat read = %+v, want %+v", again, st)
		}
		u := degreeStats(t, g, "U")
		if u.Rels != 9 || u.MaxOut != 9 || u.MaxIn != 9 || u.MaxOutNode != sink.ID() || u.MaxInNode != hub.ID() || u.Starts != 1 || u.Ends != 1 {
			t.Fatalf("U stats = %+v", u)
		}
		all := degreeStats(t, g, "")
		if all.Rels != 18+extra || all.MaxIn != 9 || all.MaxOut != 9+extra {
			t.Fatalf("all-type stats = %+v", all)
		}

		// The hub loses three relationships: MaxOut drops to the next value.
		for _, r := range hubRels[:3] {
			if err := g.Rels().Delete(ctx, r.ID()); err != nil {
				t.Fatal(err)
			}
		}
		st = degreeStats(t, g, "T")
		if st.MaxOut != 2 || st.Rels != 6+extra {
			t.Fatalf("after deletes: %+v, want MaxOut 2 at the hub, Rels %d", st, 6+extra)
		}
		if st.MaxOutNode != hub.ID() {
			t.Fatalf("MaxOutNode after deletes = %d, want the hub %d", st.MaxOutNode, hub.ID())
		}
		// A property write moves no degree but must not break the cache either.
		if err := g.Rels().SetProperty(ctx, hubRels[3].ID(), "w", int64(1)); err != nil {
			t.Fatal(err)
		}
		if again := degreeStats(t, g, "T"); again.MaxOut != 2 || again.Rels != st.Rels {
			t.Fatalf("after a property write: %+v", again)
		}
		// A cascade removes the sink and its 3 incoming T (and the tiered edge).
		if err := g.Nodes().Delete(ctx, sink.ID()); err != nil {
			t.Fatal(err)
		}
		st = degreeStats(t, g, "T")
		if st.MaxIn != 1 || st.Rels != 3 {
			t.Fatalf("after the cascade: %+v, want MaxIn 1, Rels 3", st)
		}
		if st.MaxInNode == sink.ID() || (event != nil && st.MaxInNode == event.ID()) {
			t.Fatal("a deleted node is still reported as the in-hub")
		}
		if u := degreeStats(t, g, "U"); u != (storepkg.RelTypeDegreeStats{Exact: true}) {
			t.Fatalf("U after its endpoint's cascade = %+v, want zero", u)
		}

		if z := degreeStats(t, g, "NeverSeen"); z != (storepkg.RelTypeDegreeStats{Exact: true}) {
			t.Fatalf("unregistered type = %+v", z)
		}
		if _, err := g.Stats().RelTypeDegreeStats("   "); err == nil {
			t.Fatal("a whitespace-only type name must be rejected")
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Stats().RelTypeDegreeStats("T"); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("after Close: %v, want ErrGraphClosed", err)
		}
	})
}

// BenchmarkRelTypeDegreeStats: the first read after a write counts the type
// (sigma-tkgd's 36,001 KNOWS shape); later reads are served from the cache.
func BenchmarkRelTypeDegreeStats(b *testing.B) {
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
		ctx := context.Background()
		var nodes []*types.Node
		for i := 0; i < 20000; i++ {
			n, err := g.Nodes().Add(ctx, []string{"Person"}, nil)
			if err != nil {
				b.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		if _, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
			for i := 0; i < 36001; i++ {
				if _, err := bb.AddRelationship("KNOWS", nodes[(i*7919)%len(nodes)], nodes[(i*104729+1)%len(nodes)], nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
		b.Run(backend.name+"/count", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				// A write to another type keeps KNOWS's badger stripe but moves
				// the memory store's store-wide epoch; a KNOWS write moves both.
				if _, err := g.Rels().Add(ctx, "KNOWS", nodes[i%len(nodes)], nodes[(i+1)%len(nodes)], nil); err != nil {
					b.Fatal(err)
				}
				if _, err := g.Stats().RelTypeDegreeStats("KNOWS"); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(backend.name+"/cached", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := g.Stats().RelTypeDegreeStats("KNOWS"); err != nil {
					b.Fatal(err)
				}
			}
		})
		_ = g.Close()
	}
}
