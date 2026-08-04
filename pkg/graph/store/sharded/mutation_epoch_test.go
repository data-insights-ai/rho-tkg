package sharded_test

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// TestShardedMutationEpochAdvancesOnWrite pins the correctness fix: on a
// multi-lane (sharded) store the node/rel mutation epoch — which a consumer keys
// a read cache on to invalidate after writes — must ADVANCE on every write.
// Before the fix it was a constant 0 (sharded declined the capability), so an
// epoch-keyed cache never invalidated and served stale reads after writes.
func TestShardedMutationEpochAdvancesOnWrite(t *testing.T) {
	t.Parallel()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	e0 := g.Nodes().NodeMutationEpoch()

	a, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add a: %v", err)
	}
	e1 := g.Nodes().NodeMutationEpoch()
	if e1 <= e0 {
		t.Fatalf("node epoch did not advance on write: %d -> %d (constant epoch = stale-cache bug)", e0, e1)
	}

	// A write to a node whose slot may differ still advances the folded epoch.
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"}); err != nil {
		t.Fatalf("Add b: %v", err)
	}
	if g.Nodes().NodeMutationEpoch() <= e1 {
		t.Fatalf("node epoch did not advance on second write: still %d", e1)
	}

	// Rel epoch likewise advances on a rel write.
	r0 := g.Rels().RelMutationEpoch()
	// A self-loop may or may not be rejected depending on config; either outcome is
	// fine here because the point is the epoch API, not the write.
	_, _ = g.Rels().Add(ctx, "KNOWS", a, a, nil)
	// Use a valid rel between two live nodes.
	b, _ := g.Nodes().Add(ctx, []string{"Person"}, nil)
	if _, err := g.Rels().Add(ctx, "KNOWS", a, b, nil); err != nil {
		t.Fatalf("Add rel: %v", err)
	}
	if g.Rels().RelMutationEpoch() <= r0 {
		t.Fatalf("rel epoch did not advance on rel write: %d -> %d", r0, g.Rels().RelMutationEpoch())
	}
}
