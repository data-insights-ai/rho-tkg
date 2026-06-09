package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredEventShardTokenizedReload reproduces the production failure: event
// shards hold nodes whose property keys are tokenized against the single
// canonical registry (persisted only on the reference shard). On reload, each
// event shard's loadIndexes must be able to resolve those tokens — otherwise the
// tokenized rows are silently dropped and the node counter no longer matches the
// live rows, which crash-loops the store.
//
// Before the fix: the event shard decoded against its own (empty) meta registry
// → 3,858-of-7,591-style row loss → fatal "node counter does not match N live
// rows" on open. After the fix: the canonical registry is injected into every
// shard at open, so all rows decode and counts match.
func TestTieredEventShardTokenizedReload(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	open := func() *graphpkg.Graph {
		t.Helper()
		ts, err := tieredpkg.New(tieredpkg.Config{DataDir: dir, RefLabels: []string{"Ref"}})
		if err != nil {
			t.Fatalf("tiered.New: %v", err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: ts})
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		return g
	}

	const n = 200
	g := open()
	ids := make([]types.NodeID, 0, n)
	for i := 0; i < n; i++ {
		// Label "Event" is NOT a RefLabel → routed to the event shard. Distinct
		// property keys get tokenized via the shared registry (persisted only on
		// the reference shard).
		node, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{
			"source_ip":    "10.0.0.1",
			"source_user":  "alice",
			"technique_id": "T1059",
			"severity":     int64(i),
		})
		if err != nil {
			t.Fatalf("add event node %d: %v", i, err)
		}
		ids = append(ids, node.ID())
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen — must NOT crash, and every event node must survive with properties.
	g2 := open()
	defer func() { _ = g2.Close() }()

	cnt, err := g2.Stats().NodeCount()
	if err != nil {
		t.Fatalf("NodeCount after reload: %v", err)
	}
	if cnt != n {
		t.Fatalf("NodeCount after reload = %d, want %d (tokenized event rows were dropped)", cnt, n)
	}
	// Tokenized property keys must resolve back to strings.
	got, err := g2.Nodes().Get(ctx, ids[0])
	if err != nil {
		t.Fatalf("Get(%v) after reload: %v", ids[0], err)
	}
	if v, ok := got.GetProperty("source_ip"); !ok || v != "10.0.0.1" {
		t.Errorf("source_ip after reload = (%v, %v), want (10.0.0.1, true)", v, ok)
	}
	if v, ok := got.GetProperty("technique_id"); !ok || v != "T1059" {
		t.Errorf("technique_id after reload = (%v, %v), want (T1059, true)", v, ok)
	}
}
