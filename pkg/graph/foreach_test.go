package graph

import (
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	n, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nodeID := n.ID()
	time.Sleep(2 * time.Millisecond)
	now := types.Instant(time.Now().UnixMilli())

	// GetNodesValidAt should find Alice.
	nodes, err := g.GetNodesValidAt(now)
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
	n2, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_, err = g.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	now2 := types.Instant(time.Now().UnixMilli())

	// GetRelationshipsValidAt should find the rel.
	rels, err := g.GetRelationshipsValidAt(now2)
	if err != nil {
		t.Fatalf("GetRelationshipsValidAt: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("got %d rels, want 1", len(rels))
	}

	// GetNodesValidDuring should find both nodes.
	nodesDuring, err := g.GetNodesValidDuring(now, now2+1000)
	if err != nil {
		t.Fatalf("GetNodesValidDuring: %v", err)
	}
	if len(nodesDuring) != 2 {
		t.Fatalf("got %d nodes during, want 2", len(nodesDuring))
	}

	// GetRelationshipsValidDuring should find the rel.
	relsDuring, err := g.GetRelationshipsValidDuring(now, now2+1000)
	if err != nil {
		t.Fatalf("GetRelationshipsValidDuring: %v", err)
	}
	if len(relsDuring) != 1 {
		t.Fatalf("got %d rels during, want 1", len(relsDuring))
	}

	// Snapshot should work.
	snap, err := g.Snapshot(now2)
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
