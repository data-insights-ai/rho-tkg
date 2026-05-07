// Internal-package tests for tiered.Store (Graph-integration) implementations
// of RemoveNodeLabelToken, CreateVectorIndex, DropVectorIndex, SearchNearestNodes.
package graph

import (
	"errors"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// --- tiered.Store: RemoveNodeLabelToken, CreateVectorIndex, DropVectorIndex, SearchNearestNodes ---

func TestTieredStore_RemoveNodeLabelToken(t *testing.T) {
	g, _ := newTestTieredGraph(t)

	n, err := g.AddNode([]string{"User", "Admin"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	if err := g.RemoveNodeLabel(id, "Admin"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	updated, _ := g.GetNode(id)
	if g.NodeHasLabel(updated, "Admin") {
		t.Error("label 'Admin' still present after removal from tiered.Store")
	}
}

func TestTieredStore_VectorIndex_CreateAndSearch(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	label := "User"
	key := "embedding"

	n1, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0, 0}})
	n2, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0, 1, 0}})

	if err := g.CreateVectorIndex(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{1, 0, 0}, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results, got none")
	}
	if results[0].ID() != n1.ID() {
		t.Errorf("expected n1 as closest, got something else (n2=%v)", n2.ID())
	}
}

func TestTieredStore_VectorIndex_AlreadyExists(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	g.AddNode([]string{"User"}, nil)
	g.CreateVectorIndex("User", "v", 2, storepkg.DistanceCosine)
	err := g.CreateVectorIndex("User", "v", 2, storepkg.DistanceCosine)
	if !errors.Is(err, ErrVectorIndexExists) {
		t.Errorf("expected ErrVectorIndexExists, got %v", err)
	}
}

func TestTieredStore_VectorIndex_Drop(t *testing.T) {
	g, _ := newTestTieredGraph(t)
	g.AddNode([]string{"User"}, nil)
	g.CreateVectorIndex("User", "v", 2, storepkg.DistanceCosine)
	if err := g.DropVectorIndex("User", "v"); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}
	_, err := g.SearchNearestNodes("User", "v", []float32{1, 0}, 1, storepkg.QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound after drop, got %v", err)
	}
}
