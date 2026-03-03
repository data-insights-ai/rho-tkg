package graph_test

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/graph"
)

func TestVectorIndex_CreateAndSearch_Cosine(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "Item"
	key := "embedding"

	n1, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0, 0}})
	n2, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0, 1, 0}})
	n3, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0.1, 0}})

	if err := g.CreateVectorIndex(label, key, 3, graph.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Query closest to [1, 0, 0]: n1 and n3 should rank before n2.
	query := []float32{1, 0, 0}
	results, err := g.SearchNearestNodes(label, key, query, 3, graph.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	// First result should be n1 (exact match) or n3 (very close).
	firstID := results[0].InternalID().SnowflakeID()
	if firstID != n1.InternalID().SnowflakeID() && firstID != n3.InternalID().SnowflakeID() {
		t.Errorf("first result should be n1 or n3 (closest to [1,0,0]), got other")
	}
	// n2 should be last (orthogonal).
	lastID := results[len(results)-1].InternalID().SnowflakeID()
	if lastID != n2.InternalID().SnowflakeID() {
		t.Errorf("last result should be n2 (most distant), got other")
	}
	_ = n1
	_ = n2
	_ = n3
}

func TestVectorIndex_CreateAndSearch_Euclidean(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "Vec"
	key := "v"

	n1, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0, 0}})
	n2, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 1}})
	n3, _ := g.AddNode([]string{label}, map[string]any{key: []float32{10, 10}})

	if err := g.CreateVectorIndex(label, key, 2, graph.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{0.1, 0.1}, 2, graph.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	// Closest to [0.1, 0.1]: n1=[0,0] d≈0.14, n2=[1,1] d≈1.27, n3 much farther.
	if results[0].InternalID().SnowflakeID() != n1.InternalID().SnowflakeID() {
		t.Error("first result should be n1 (closest to origin)")
	}
	_ = n2
	_ = n3
}

func TestVectorIndex_AutoMaintained_OnAdd(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "E"
	key := "v"

	// Register the label by adding a seed node without a vector, then create the index.
	seed, _ := g.AddNode([]string{label}, nil) // registers label; no vector property
	_ = seed

	if err := g.CreateVectorIndex(label, key, 2, graph.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Add a node with a vector AFTER the index was created — should be auto-indexed.
	n, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})

	results, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 1, graph.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].InternalID().SnowflakeID() != n.InternalID().SnowflakeID() {
		t.Error("node added after index creation should be searchable")
	}
}

func TestVectorIndex_AutoMaintained_OnDelete(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "E"
	key := "v"

	n, _ := g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})
	id := n.InternalID().SnowflakeID()

	if err := g.CreateVectorIndex(label, key, 2, graph.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	if err := g.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 10, graph.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	for _, r := range results {
		if r.InternalID().SnowflakeID() == id {
			t.Error("deleted node should not appear in search results")
		}
	}
}

func TestVectorIndex_DimensionMismatch(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "X"
	key := "v"
	_, _ = g.AddNode([]string{label}, nil) // register label

	if err := g.CreateVectorIndex(label, key, 3, graph.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	_, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 1, graph.QueryOpts{})
	if !errors.Is(err, graph.ErrDimensionMismatch) {
		t.Errorf("expected ErrDimensionMismatch for wrong query dims, got %v", err)
	}
}

func TestVectorIndex_IndexAlreadyExists(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "X"
	key := "v"
	_, _ = g.AddNode([]string{label}, nil)

	_ = g.CreateVectorIndex(label, key, 2, graph.DistanceCosine)
	err := g.CreateVectorIndex(label, key, 2, graph.DistanceCosine)
	if !errors.Is(err, graph.ErrVectorIndexExists) {
		t.Errorf("expected ErrVectorIndexExists, got %v", err)
	}
}

func TestVectorIndex_IndexNotFound(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "X"
	_, _ = g.AddNode([]string{label}, nil)

	err := g.DropVectorIndex(label, "nonexistent")
	if !errors.Is(err, graph.ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound, got %v", err)
	}
}

func TestVectorIndex_LabelNotRegistered_ReturnsNil(t *testing.T) {
	g, _ := graph.New(graph.Config{})

	// Label "Ghost" not registered — CreateVectorIndex should return nil (no-op).
	err := g.CreateVectorIndex("Ghost", "v", 2, graph.DistanceCosine)
	if err != nil {
		t.Errorf("expected nil for unregistered label, got %v", err)
	}
}

func TestVectorIndex_SearchEmpty_ReturnsNil(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "Empty"
	key := "v"
	_, _ = g.AddNode([]string{label}, nil) // register label, no vector property

	if err := g.CreateVectorIndex(label, key, 2, graph.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 5, graph.QueryOpts{})
	if err != nil {
		t.Fatalf("expected nil error for empty index, got %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty index, got %v", results)
	}
}

func TestVectorIndex_SearchNotFound_ReturnsError(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "X"
	_, _ = g.AddNode([]string{label}, nil)

	_, err := g.SearchNearestNodes(label, "missing", []float32{1, 0}, 1, graph.QueryOpts{})
	if !errors.Is(err, graph.ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound when no index, got %v", err)
	}
}

func TestVectorIndex_DropAndRecreate(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "D"
	key := "v"
	_, _ = g.AddNode([]string{label}, map[string]any{key: []float32{1, 0}})

	if err := g.CreateVectorIndex(label, key, 2, graph.DistanceCosine); err != nil {
		t.Fatalf("first CreateVectorIndex: %v", err)
	}

	if err := g.DropVectorIndex(label, key); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}

	// Should be able to create again after drop.
	if err := g.CreateVectorIndex(label, key, 2, graph.DistanceCosine); err != nil {
		t.Fatalf("second CreateVectorIndex after drop: %v", err)
	}
}

func TestVectorIndex_AutoMaintained_OnUpdate(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	label := "U"
	key := "v"

	n, _ := g.AddNode([]string{label}, map[string]any{key: []float32{0, 1}})
	id := n.InternalID().SnowflakeID()

	if err := g.CreateVectorIndex(label, key, 2, graph.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Update node vector.
	if _, err := g.UpdateNode(id, map[string]any{key: []float32{1, 0}}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// Query closest to [1, 0]: updated node should be closest.
	results, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 1, graph.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].InternalID().SnowflakeID() != id {
		t.Error("updated node should reflect new vector in index")
	}
}

func TestVectorIndex_toFloat32Slice_MixedAny(t *testing.T) {
	// Test that []any containing float64 values is accepted by the index.
	g, _ := graph.New(graph.Config{})
	label := "M"
	key := "v"

	// Store as []any of float64 (what JSON unmarshaling produces).
	mixedVec := []any{float64(1), float64(0)}
	n, err := g.AddNode([]string{label}, map[string]any{key: mixedVec})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := g.CreateVectorIndex(label, key, 2, graph.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.SearchNearestNodes(label, key, []float32{1, 0}, 1, graph.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].InternalID().SnowflakeID() != n.InternalID().SnowflakeID() {
		t.Error("node with []any float64 vector should be indexed")
	}
}
