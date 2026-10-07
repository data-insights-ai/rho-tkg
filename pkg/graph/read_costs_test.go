package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkReadCosts measures the read doors a query planner prices, per
// backend, on a graph of one label (four properties per node) and one type
// with 1.8 relationships per node, at 4,000 nodes (every row fits badger's
// default 10,000-entry caches: held) and 20,000 nodes (most rows decode): a
// by-ID lend of one node again and again and of every node in turn, a whole
// label scan and a whole type scan per row, and the outgoing adjacency of
// every node per relationship. Each sub-benchmark reports ns/row. These are
// the numbers store.ReadCosts states per backend (run with -cpu 1).
func BenchmarkReadCosts(b *testing.B) {
	for _, size := range []int{4000, 20000} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) { benchmarkReadCosts(b, size, size*9/5) })
	}
}

func benchmarkReadCosts(b *testing.B, nodes, rels int) {
	backends := []struct {
		name string
		cfg  func(b *testing.B) graphpkg.Config
	}{
		{"memory", func(*testing.B) graphpkg.Config { return graphpkg.Config{SnowflakeNodeID: 0} }},
		{"badger", func(*testing.B) graphpkg.Config { return graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true} }},
		{"tiered", func(b *testing.B) graphpkg.Config {
			ts, err := tiered.New(tiered.Config{InMemory: true, ShardWindow: 7 * 24 * time.Hour})
			if err != nil {
				b.Fatal(err)
			}
			return graphpkg.Config{SnowflakeNodeID: 0, Store: ts}
		}},
		{"sharded", func(b *testing.B) graphpkg.Config {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				b.Fatal(err)
			}
			return graphpkg.Config{SnowflakeNodeID: 0, Store: st}
		}},
	}
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			g, err := graphpkg.New(backend.cfg(b))
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			ctx := context.Background()
			ids := make([]types.NodeID, nodes)
			batch, err := g.Batch().New()
			if err != nil {
				b.Fatal(err)
			}
			created := make([]*types.Node, nodes)
			for i := range nodes {
				n, err := batch.AddNode([]string{"Person"}, map[string]any{"name": "a name", "age": int64(i % 90), "city": "Linz", "score": float64(i)})
				if err != nil {
					b.Fatal(err)
				}
				created[i] = n
			}
			for i := range rels {
				if _, err := batch.AddRelationship("KNOWS", created[i%nodes], created[(i*7+1)%nodes], nil); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			for i, n := range created {
				ids[i] = n.ID()
			}
			time.Sleep(300 * time.Millisecond) // let a badger flush drain (lesson 72)

			perRow := func(b *testing.B, rows int) {
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(rows), "ns/row")
			}
			b.Run("lend-held", func(b *testing.B) {
				for b.Loop() {
					if _, err := g.Nodes().Lend(ctx, ids[7]); err != nil {
						b.Fatal(err)
					}
				}
				perRow(b, 1)
			})
			b.Run("lend-every", func(b *testing.B) {
				i := 0
				for b.Loop() {
					if _, err := g.Nodes().Lend(ctx, ids[i]); err != nil {
						b.Fatal(err)
					}
					i = (i + 7919) % nodes
				}
				perRow(b, 1)
			})
			b.Run("label-scan", func(b *testing.B) {
				for b.Loop() {
					if err := g.Nodes().ForEachByLabel("Person", storepkg.QueryOpts{NoSort: true}, func(*types.Node) bool { return true }); err != nil {
						b.Fatal(err)
					}
				}
				perRow(b, nodes)
			})
			b.Run("type-scan", func(b *testing.B) {
				for b.Loop() {
					if err := g.Rels().ForEachByType("KNOWS", storepkg.QueryOpts{NoSort: true}, func(*types.Relationship) bool { return true }); err != nil {
						b.Fatal(err)
					}
				}
				perRow(b, rels)
			})
			b.Run("adjacency", func(b *testing.B) {
				for b.Loop() {
					for _, id := range ids {
						if err := g.Rels().ForEachOutgoing(id, "KNOWS", func(*types.Relationship) bool { return true }); err != nil {
							b.Fatal(err)
						}
					}
				}
				perRow(b, rels)
			})
		})
	}
}

// interfaceOnlyStore hides every optional capability of the store it wraps:
// only the store.Store interface's methods are promoted.
type interfaceOnlyStore struct{ storepkg.Store }

// Every in-tree backend states its read costs with its kind and how many rows
// it holds: memory all, badger its cache capacity (-1 resident, 0 when a byte
// budget governs), tiered and sharded per shard. Decoded never costs less
// than held, memory less than badger reading from disk. A store that states
// none answers ok=false; a closed graph is an error.
func TestReadCosts(t *testing.T) {
	want := map[string]struct {
		backend string
		held    int
	}{
		"memory": {"memory", -1}, "badger": {"badger", 8}, "tiered": {"tiered", 10000}, "sharded": {"sharded", 10000},
	}
	var mem, bad storepkg.ReadCosts
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		c, ok, err := g.Stats().ReadCosts()
		if err != nil || !ok {
			t.Fatalf("ReadCosts: %v %v", ok, err)
		}
		if c.Backend != want[b.name].backend || c.HeldRows != want[b.name].held {
			t.Fatalf("%+v, want backend %q and %d held rows", c, want[b.name].backend, want[b.name].held)
		}
		for _, pair := range [][2]float64{{c.LendHeld, c.LendDecoded}, {c.ScanRowHeld, c.ScanRowDecoded}, {c.AdjacencyRelHeld, c.AdjacencyRelDecoded}} {
			if pair[0] <= 0 || pair[1] < pair[0] {
				t.Fatalf("%+v: a held cost above its decoded one, or none", c)
			}
		}
		switch b.name {
		case "memory":
			mem = c
		case "badger":
			bad = c
		}
	})
	if mem.LendDecoded >= bad.LendDecoded || mem.ScanRowDecoded >= bad.ScanRowDecoded {
		t.Fatalf("memory %+v not below badger %+v", mem, bad)
	}
	for _, tc := range []struct {
		name string
		cfg  badger.Config
		held int
	}{
		{"resident", badger.Config{InMemory: true, ResidentCache: true}, -1},
		{"byte budget", badger.Config{InMemory: true, CacheBudgetBytes: 1 << 20}, 0},
		{"default", badger.Config{InMemory: true}, badger.DefaultCacheCapacity},
	} {
		st, err := badger.New(tc.cfg)
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: st})
		if err != nil {
			t.Fatal(err)
		}
		if c, _, _ := g.Stats().ReadCosts(); c.HeldRows != tc.held {
			t.Fatalf("badger %s: %d held rows, want %d", tc.name, c.HeldRows, tc.held)
		}
		_ = g.Close()
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: interfaceOnlyStore{memory.New()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := g.Stats().ReadCosts(); ok || err != nil {
		t.Fatalf("a store stating none: ok=%v err=%v", ok, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.Stats().ReadCosts(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed graph: %v", err)
	}
}
