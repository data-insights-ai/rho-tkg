package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// windowReachGraph builds sigma-tkgd's OPT21bWindowReach shape (task record
// C4s): a layered DAG of 12 layers of 250 nodes (layer 0 :Start, the rest
// :Mid, each with an "id"), three :R edges per node to the next layer, all
// valid from early, one isolated :Iso target; then the cold harness's writes:
// one :Mid node and one :ColdTick node updated, one :COLD_TICK relationship
// updated, so a few nodes and one relationship have history.
func windowReachGraph(b *testing.B, cfg graphpkg.Config) (*graphpkg.Graph, [][]types.NodeID) {
	b.Helper()
	g, err := graphpkg.New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	const layers, width, fanout = 12, 250, 3
	batch, err := g.Batch().New()
	if err != nil {
		b.Fatal(err)
	}
	nodes := make([][]*types.Node, layers)
	for l := range layers {
		nodes[l] = make([]*types.Node, width)
		for i := range width {
			label := "Mid"
			if l == 0 {
				label = "Start"
			}
			if nodes[l][i], err = batch.AddNode([]string{label}, map[string]any{"id": int64(l*width + i), "tkg_valid_from": int64(100)}); err != nil {
				b.Fatal(err)
			}
		}
	}
	for l := 0; l+1 < layers; l++ {
		for i := range width {
			for k := 1; k <= fanout; k++ {
				if _, err := batch.AddRelationship("R", nodes[l][i], nodes[l+1][(i*7+k)%width], map[string]any{"tkg_valid_from": int64(1000)}); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	iso, err := batch.AddNode([]string{"Iso"}, map[string]any{"id": int64(999999), "tkg_valid_from": int64(100)})
	if err != nil {
		b.Fatal(err)
	}
	_ = iso
	if _, err := batch.Execute(); err != nil {
		b.Fatal(err)
	}
	tickA, err := g.Nodes().Add(ctx, []string{"ColdTick"}, map[string]any{"t": int64(0)})
	if err != nil {
		b.Fatal(err)
	}
	tickB, err := g.Nodes().Add(ctx, []string{"ColdTick"}, map[string]any{"t": int64(0)})
	if err != nil {
		b.Fatal(err)
	}
	tickRel, err := g.Rels().Add(ctx, "COLD_TICK", tickA, tickB, map[string]any{"t": int64(0)})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := g.Nodes().Update(ctx, tickA.ID(), map[string]any{"t": int64(1)}); err != nil {
		b.Fatal(err)
	}
	if _, err := g.Rels().Update(ctx, tickRel.ID(), map[string]any{"t": int64(1)}); err != nil {
		b.Fatal(err)
	}
	if _, err := g.Nodes().Update(ctx, nodes[5][17].ID(), map[string]any{"t": int64(1)}); err != nil {
		b.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // drain the badger write buffer
	ids := make([][]types.NodeID, layers)
	for l := range layers {
		ids[l] = make([]types.NodeID, width)
		for i, n := range nodes[l] {
			ids[l][i] = n.ID()
		}
	}
	return g, ids
}

func windowReachBackends() []struct {
	name string
	cfg  graphpkg.Config
} {
	return []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	}
}

// BenchmarkWindowAdjacencyReach: a breadth-first search from every :Start
// node over :R through ForEachAdjacentEndpointAt, without a filter
// (static), in the window [1500, 6000) and at the instant 3000 (round 5
// request 6: the window's search was 11 times the static one on a graph where
// only an unrelated relationship has history).
func BenchmarkWindowAdjacencyReach(b *testing.B) {
	for _, backend := range windowReachBackends() {
		b.Run(backend.name, func(b *testing.B) {
			g, ids := windowReachGraph(b, backend.cfg)
			for _, arm := range []struct {
				name string
				opts storepkg.QueryOpts
			}{
				{"static", storepkg.QueryOpts{}},
				{"window", storepkg.QueryOpts{ValidStart: 1500, ValidEnd: 6000}},
				{"at", storepkg.QueryOpts{ValidAt: 3000}},
			} {
				b.Run(arm.name, func(b *testing.B) {
					for b.Loop() {
						seen := make(map[types.NodeID]struct{}, 3000)
						frontier := append([]types.NodeID(nil), ids[0]...)
						for len(frontier) > 0 {
							var next []types.NodeID
							for _, id := range frontier {
								if err := g.Rels().ForEachAdjacentEndpointAt(id, "R", false, arm.opts, func(_ types.RelID, other types.NodeID) bool {
									if _, ok := seen[other]; !ok {
										seen[other] = struct{}{}
										next = append(next, other)
									}
									return true
								}); err != nil {
									b.Fatal(err)
								}
							}
							frontier = next
						}
						if len(seen) != 11*250 {
							b.Fatalf("reached %d", len(seen))
						}
					}
				})
			}
		})
	}
}

// BenchmarkWindowPropertySeek: the coordinate seeks of OPT21bWindowReach,
// (:Start {id: 0}) and (:Iso {id: 999999}) through ByLabelAndProperty in the
// window [1500, 6000) and without one (round 5 request 8: the window's seek
// read the history of every node with history, of any label).
func BenchmarkWindowPropertySeek(b *testing.B) {
	for _, backend := range windowReachBackends() {
		b.Run(backend.name, func(b *testing.B) {
			g, _ := windowReachGraph(b, backend.cfg)
			for _, arm := range []struct {
				name string
				opts storepkg.QueryOpts
			}{
				{"static", storepkg.QueryOpts{}},
				{"window", storepkg.QueryOpts{ValidStart: 1500, ValidEnd: 6000}},
				{"at", storepkg.QueryOpts{ValidAt: 3000}},
			} {
				b.Run(arm.name, func(b *testing.B) {
					for b.Loop() {
						start, err := g.Nodes().ByLabelAndProperty("Start", "id", int64(0), arm.opts)
						if err != nil || len(start) != 1 {
							b.Fatalf("start %d %v", len(start), err)
						}
						iso, err := g.Nodes().ByLabelAndProperty("Iso", "id", int64(999999), arm.opts)
						if err != nil || len(iso) != 1 {
							b.Fatalf("iso %d %v", len(iso), err)
						}
					}
				})
			}
		})
	}
}
