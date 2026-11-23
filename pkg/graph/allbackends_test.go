package graph_test

import (
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/core"
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

// noAutoFlush is the FlushInterval of every store allStoreBackends opens: only
// an explicit flush or a full buffer drains the write buffer, so the versions
// and buffered rows a read counts do not depend on when the 100 ms background
// flush fires (on a loaded host under -race it split the writes a test expects
// in one batch: TestReadBudgetPublicScans, TestReadBudgetPublicTimelines,
// TestReadBudgetPublicAtNodes and TestReadBudgetPublicByIDs counted more
// candidates).
const noAutoFlush = time.Duration(1<<63 - 1)

// allStoreBackends lists memory, badger, tiered and sharded. Each open builds
// a fresh graph closed at test cleanup. No store flushes in the background
// (noAutoFlush).
func allStoreBackends() []storeBackend { return allStoreBackendsWith(nil) }

// allStoreBackendsWith is allStoreBackends with mut applied to every Config
// (nil = unchanged) before the graph is opened.
func allStoreBackendsWith(mut func(*graphpkg.Config)) []storeBackend {
	newGraph := func(t *testing.T, cfg graphpkg.Config) *graphpkg.Graph {
		t.Helper()
		if mut != nil {
			mut(&cfg)
		}
		core.SetBadgerFlushIntervalForTest(&cfg, noAutoFlush)
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
				FlushInterval: noAutoFlush,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			return newGraph(t, graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}, SnowflakeNodeID: 0, Store: ts})
		}},
		{name: "sharded", open: func(t *testing.T) *graphpkg.Graph {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, FlushInterval: noAutoFlush})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return newGraph(t, graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}, SnowflakeNodeID: 0, Store: st})
		}},
	}
}

// txFromCfg pins a backend to UntimedFromTxFrom, for a test that asserts the
// TxFrom start of an untimed create: since v4.57.0 a new store
// under the default is UntimedUnbounded (TestUntimedDefaultFlip).
func txFromCfg(c *graphpkg.Config) { c.UntimedValidStart = graphpkg.UntimedFromTxFrom }

// forAllStoreBackendsTxFrom is forAllStoreBackends over UntimedFromTxFrom
// stores (txFromCfg).
func forAllStoreBackendsTxFrom(t *testing.T, fn func(t *testing.T, b storeBackend, g *graphpkg.Graph)) {
	t.Helper()
	for _, b := range allStoreBackendsWith(txFromCfg) {
		t.Run(b.name, func(t *testing.T) {
			fn(t, b, b.open(t))
		})
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
