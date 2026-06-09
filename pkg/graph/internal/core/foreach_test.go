package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ─── Graph-level: temporal queries still work ─────────────────────────────────

func TestGraph_ForEachBasedTemporalQueries(t *testing.T) {
	t.Parallel()

	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           memory.New(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Add a node.
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nodeID := n.ID()
	// vStart <= t (inclusive) means querying at the same millisecond
	// as the node's snowflake-derived ValidFrom still matches — no
	// wall-clock sleep needed (R5-F10).
	now := types.Instant(time.Now().UnixMilli())

	// GetNodesValidAt should find Alice.
	nodes, err := g.Temporal.NodesAt(now)
	if err != nil {
		t.Fatalf("GetNodesValidAt: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if nodes[0].ID() != nodeID {
		t.Error("wrong node ID")
	}

	// Add a relationship.
	n2, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_, err = g.Rels.Add(context.Background(), "KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	now2 := types.Instant(time.Now().UnixMilli())

	// GetRelationshipsValidAt should find the rel.
	rels, err := g.Temporal.RelsAt(now2)
	if err != nil {
		t.Fatalf("GetRelationshipsValidAt: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("got %d rels, want 1", len(rels))
	}

	// GetNodesValidDuring should find both nodes.
	nodesDuring, err := g.Temporal.NodesDuring(now, now2+1000)
	if err != nil {
		t.Fatalf("GetNodesValidDuring: %v", err)
	}
	if len(nodesDuring) != 2 {
		t.Fatalf("got %d nodes during, want 2", len(nodesDuring))
	}

	// GetRelationshipsValidDuring should find the rel.
	relsDuring, err := g.Temporal.RelsDuring(now, now2+1000)
	if err != nil {
		t.Fatalf("GetRelationshipsValidDuring: %v", err)
	}
	if len(relsDuring) != 1 {
		t.Fatalf("got %d rels during, want 1", len(relsDuring))
	}

	// Snapshot should work.
	snap, err := g.Temporal.Snapshot(now2)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.NodeCount != 2 {
		t.Fatalf("snapshot has %d nodes, want 2", snap.NodeCount)
	}
	if snap.RelCount != 1 {
		t.Fatalf("snapshot has %d rels, want 1", snap.RelCount)
	}
}
