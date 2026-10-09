package graph_test

import (
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// storeBackend names one in-tree backend a graph test runs against.
type storeBackend struct {
	name string
	// tiered is true for the tiered store, whose label "Ref" routes to the
	// reference shard and every other label to the hot event shard, so a
	// relationship between a "Ref" node and another node is cross-shard.
	tiered bool
	open   func(t *testing.T) *graphpkg.Graph
}

// allStoreBackends lists memory, badger, tiered and sharded. Each open builds
// a fresh graph closed at test cleanup.
func allStoreBackends() []storeBackend { return allStoreBackendsWith(nil) }

// allStoreBackendsWith is allStoreBackends with mut applied to every Config
// (nil = unchanged) before the graph is opened.
func allStoreBackendsWith(mut func(*graphpkg.Config)) []storeBackend {
	newGraph := func(t *testing.T, cfg graphpkg.Config) *graphpkg.Graph {
		t.Helper()
		if mut != nil {
			mut(&cfg)
		}
		g, err := graphpkg.New(cfg)
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}
	return []storeBackend{
		{name: "memory", open: func(t *testing.T) *graphpkg.Graph {
			return newGraph(t, graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}, SnowflakeNodeID: 0})
		}},
		{name: "badger", open: func(t *testing.T) *graphpkg.Graph {
			return newGraph(t, graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}, SnowflakeNodeID: 0, BadgerInMemory: true, CacheCapacity: 8})
		}},
		{name: "tiered", tiered: true, open: func(t *testing.T) *graphpkg.Graph {
			ts, err := tiered.New(tiered.Config{
				InMemory:      true,
				RefLabels:     []string{"Ref"},
				ShardWindow:   7 * 24 * time.Hour,
				FlushInterval: 1<<63 - 1,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			return newGraph(t, graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}, SnowflakeNodeID: 0, Store: ts})
		}},
		{name: "sharded", open: func(t *testing.T) *graphpkg.Graph {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return newGraph(t, graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}, SnowflakeNodeID: 0, Store: st})
		}},
	}
}

// forAllStoreBackends runs fn once per in-tree backend as a subtest.
func forAllStoreBackends(t *testing.T, fn func(t *testing.T, b storeBackend, g *graphpkg.Graph)) {
	t.Helper()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			fn(t, b, b.open(t))
		})
	}
}
