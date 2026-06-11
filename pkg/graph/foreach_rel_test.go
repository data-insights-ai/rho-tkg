package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Streaming rel-scan probes. Every probe runs against BOTH in-tree
// stores — memory and badger — through the same core routing: parallel
// implementations of one contract get both arms visited.

func forEachRelBackends(t *testing.T, run func(t *testing.T, g *graph.Graph)) {
	t.Helper()
	backends := []struct {
		name string
		cfg  graph.Config
	}{
		{name: "memory", cfg: graph.Config{SnowflakeNodeID: 1}},
		{name: "badger", cfg: graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, CacheCapacity: 8}},
	}
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			g, err := graph.New(b.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			run(t, g)
		})
	}
}

// seedRelGraph creates hub→spoke KNOWS rels plus spoke→hub LIKES rels:
// hub has n outgoing KNOWS and n incoming LIKES; every spoke has 1 of each.
func seedRelGraph(t *testing.T, g *graph.Graph, n int) (hub *types.Node, spokes []*types.Node) {
	t.Helper()
	ctx := context.Background()
	hub, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"hub": true})
	if err != nil {
		t.Fatalf("Add hub: %v", err)
	}
	for i := 0; i < n; i++ {
		s, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"idx": int64(i)})
		if err != nil {
			t.Fatalf("Add spoke %d: %v", i, err)
		}
		if _, err := g.Rels().Add(ctx, "KNOWS", hub, s, map[string]any{"idx": int64(i)}); err != nil {
			t.Fatalf("Add KNOWS %d: %v", i, err)
		}
		if _, err := g.Rels().Add(ctx, "LIKES", s, hub, nil); err != nil {
			t.Fatalf("Add LIKES %d: %v", i, err)
		}
		spokes = append(spokes, s)
	}
	return hub, spokes
}

// TestForEachRelByType_MatchesByType pins that the streaming scan emits
// exactly the rows the materializing ByType returns, in the same order —
// past the badger entity-cache capacity, where the no-fill reads must still
// decode every row correctly.
func TestForEachRelByType_MatchesByType(t *testing.T) {
	forEachRelBackends(t, func(t *testing.T, g *graph.Graph) {
		seedRelGraph(t, g, 40) // 5x the badger arm's cache capacity

		want, err := g.Rels().ByType("KNOWS", graph.QueryOpts{})
		if err != nil {
			t.Fatalf("ByType: %v", err)
		}
		if len(want) != 40 {
			t.Fatalf("ByType returned %d rows, want 40", len(want))
		}

		var got []*types.Relationship
		if err := g.Rels().ForEachByType("KNOWS", graph.QueryOpts{}, func(r *types.Relationship) bool {
			got = append(got, r)
			return true
		}); err != nil {
			t.Fatalf("ForEachByType: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("streamed %d rows, ByType returned %d", len(got), len(want))
		}
		for i := range want {
			if got[i].ID() != want[i].ID() {
				t.Fatalf("row %d: streamed ID %d, ByType ID %d", i, got[i].ID(), want[i].ID())
			}
			gv, _ := got[i].GetProperty("idx")
			wv, _ := want[i].GetProperty("idx")
			if gv != wv {
				t.Fatalf("row %d: streamed idx %v, ByType idx %v", i, gv, wv)
			}
		}
	})
}

// TestForEachRelByType_EarlyStopAndLimit pins fn=false termination and the
// Limit opt — both must stop the scan without error.
func TestForEachRelByType_EarlyStopAndLimit(t *testing.T) {
	forEachRelBackends(t, func(t *testing.T, g *graph.Graph) {
		seedRelGraph(t, g, 20)

		seen := 0
		if err := g.Rels().ForEachByType("KNOWS", graph.QueryOpts{}, func(*types.Relationship) bool {
			seen++
			return seen < 5
		}); err != nil {
			t.Fatalf("early stop: %v", err)
		}
		if seen != 5 {
			t.Fatalf("early stop saw %d rows, want 5", seen)
		}

		seen = 0
		if err := g.Rels().ForEachByType("KNOWS", graph.QueryOpts{Limit: 7}, func(*types.Relationship) bool {
			seen++
			return true
		}); err != nil {
			t.Fatalf("limit: %v", err)
		}
		if seen != 7 {
			t.Fatalf("Limit=7 saw %d rows", seen)
		}
	})
}

// TestForEachRelByType_NilCallbackAndUnknownType pins the error and empty
// edges: nil fn is rejected; an unregistered type streams nothing.
func TestForEachRelByType_NilCallbackAndUnknownType(t *testing.T) {
	forEachRelBackends(t, func(t *testing.T, g *graph.Graph) {
		seedRelGraph(t, g, 3)

		if err := g.Rels().ForEachByType("KNOWS", graph.QueryOpts{}, nil); err == nil {
			t.Fatal("nil callback accepted")
		}
		calls := 0
		if err := g.Rels().ForEachByType("NO_SUCH_TYPE", graph.QueryOpts{}, func(*types.Relationship) bool {
			calls++
			return true
		}); err != nil {
			t.Fatalf("unknown type: %v", err)
		}
		if calls != 0 {
			t.Fatalf("unknown type streamed %d rows", calls)
		}
	})
}

// TestForEachRelByType_CallbackReentry pins the relaxed-isolation promise:
// fn may call back into the graph (the materializing path under c.mu could
// not allow this — RWMutex readers deadlock behind queued writers).
func TestForEachRelByType_CallbackReentry(t *testing.T) {
	forEachRelBackends(t, func(t *testing.T, g *graph.Graph) {
		seedRelGraph(t, g, 10)

		var reErr error
		if err := g.Rels().ForEachByType("KNOWS", graph.QueryOpts{}, func(r *types.Relationship) bool {
			if _, err := g.Rels().Get(context.Background(), r.ID()); err != nil {
				reErr = fmt.Errorf("re-entrant Get(%d): %w", r.ID(), err)
				return false
			}
			return true
		}); err != nil {
			t.Fatalf("ForEachByType: %v", err)
		}
		if reErr != nil {
			t.Fatal(reErr)
		}
	})
}

// TestForEachRelByType_TemporalFallback pins that a temporal filter routes
// through the history-aware materializing path and still streams rows.
func TestForEachRelByType_TemporalFallback(t *testing.T) {
	forEachRelBackends(t, func(t *testing.T, g *graph.Graph) {
		seedRelGraph(t, g, 6)

		rels, err := g.Rels().ByType("KNOWS", graph.QueryOpts{})
		if err != nil || len(rels) == 0 {
			t.Fatalf("seed read: %v (%d)", err, len(rels))
		}
		at := rels[0].Temporal().TxFrom
		want, err := g.Rels().ByType("KNOWS", graph.QueryOpts{ValidAt: at})
		if err != nil {
			t.Fatalf("ByType temporal: %v", err)
		}
		var got int
		if err := g.Rels().ForEachByType("KNOWS", graph.QueryOpts{ValidAt: at}, func(*types.Relationship) bool {
			got++
			return true
		}); err != nil {
			t.Fatalf("ForEachByType temporal: %v", err)
		}
		if got != len(want) {
			t.Fatalf("temporal stream %d rows, ByType %d", got, len(want))
		}
	})
}

// TestForEachAdjacent_MatchesMaterializing pins both directions, filtered
// and unfiltered, against the materializing Outgoing/Incoming siblings on a
// hub node — same rows, same order.
func TestForEachAdjacent_MatchesMaterializing(t *testing.T) {
	forEachRelBackends(t, func(t *testing.T, g *graph.Graph) {
		hub, _ := seedRelGraph(t, g, 25)

		cases := []struct {
			name     string
			typeName string
			incoming bool
			wantLen  int
		}{
			{name: "outgoing all", typeName: "", wantLen: 25},
			{name: "outgoing typed", typeName: "KNOWS", wantLen: 25},
			{name: "outgoing wrong type", typeName: "LIKES", wantLen: 0},
			{name: "incoming all", incoming: true, typeName: "", wantLen: 25},
			{name: "incoming typed", incoming: true, typeName: "LIKES", wantLen: 25},
			{name: "incoming wrong type", incoming: true, typeName: "KNOWS", wantLen: 0},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var want []*types.Relationship
				var err error
				if tc.incoming {
					want, err = g.Rels().Incoming(hub.ID(), tc.typeName)
				} else {
					want, err = g.Rels().Outgoing(hub.ID(), tc.typeName)
				}
				if err != nil {
					t.Fatalf("materializing: %v", err)
				}
				if len(want) != tc.wantLen {
					t.Fatalf("materializing returned %d rows, want %d", len(want), tc.wantLen)
				}

				var got []*types.Relationship
				collect := func(r *types.Relationship) bool {
					got = append(got, r)
					return true
				}
				if tc.incoming {
					err = g.Rels().ForEachIncoming(hub.ID(), tc.typeName, collect)
				} else {
					err = g.Rels().ForEachOutgoing(hub.ID(), tc.typeName, collect)
				}
				if err != nil {
					t.Fatalf("streaming: %v", err)
				}
				if len(got) != len(want) {
					t.Fatalf("streamed %d rows, materializing %d", len(got), len(want))
				}
				for i := range want {
					if got[i].ID() != want[i].ID() {
						t.Fatalf("row %d: streamed ID %d, materializing ID %d", i, got[i].ID(), want[i].ID())
					}
				}
			})
		}
	})
}

// TestForEachAdjacent_MissingNode pins ErrNodeNotFound parity with the
// materializing siblings — including with an unregistered type name, where
// the node-exists check must still fire.
func TestForEachAdjacent_MissingNode(t *testing.T) {
	forEachRelBackends(t, func(t *testing.T, g *graph.Graph) {
		seedRelGraph(t, g, 2)
		missing := types.NodeID(999999999)

		noCall := func(*types.Relationship) bool { t.Fatal("fn called for missing node"); return false }
		if err := g.Rels().ForEachOutgoing(missing, "", noCall); !errors.Is(err, graph.ErrNodeNotFound) {
			t.Fatalf("outgoing missing node: %v, want ErrNodeNotFound", err)
		}
		if err := g.Rels().ForEachIncoming(missing, "KNOWS", noCall); !errors.Is(err, graph.ErrNodeNotFound) {
			t.Fatalf("incoming missing node typed: %v, want ErrNodeNotFound", err)
		}
	})
}

// TestForEachAdjacent_UnregisteredTypeAndEarlyStop pins the unregistered
// type edge (existing node: no error, zero rows) and fn=false termination.
func TestForEachAdjacent_UnregisteredTypeAndEarlyStop(t *testing.T) {
	forEachRelBackends(t, func(t *testing.T, g *graph.Graph) {
		hub, _ := seedRelGraph(t, g, 10)

		calls := 0
		if err := g.Rels().ForEachOutgoing(hub.ID(), "NO_SUCH_TYPE", func(*types.Relationship) bool {
			calls++
			return true
		}); err != nil {
			t.Fatalf("unregistered type: %v", err)
		}
		if calls != 0 {
			t.Fatalf("unregistered type streamed %d rows", calls)
		}

		seen := 0
		if err := g.Rels().ForEachOutgoing(hub.ID(), "", func(*types.Relationship) bool {
			seen++
			return seen < 4
		}); err != nil {
			t.Fatalf("early stop: %v", err)
		}
		if seen != 4 {
			t.Fatalf("early stop saw %d rows, want 4", seen)
		}

		if err := g.Rels().ForEachIncoming(hub.ID(), "", nil); err == nil {
			t.Fatal("nil callback accepted")
		}
	})
}
