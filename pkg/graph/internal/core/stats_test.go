package core

import (
	"testing"
)

func TestGraphStats_InitialState(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	s := g.Stats()
	if s.NodesAdded != 0 || s.NodesRead != 0 || s.NodesUpdated != 0 || s.NodesDeleted != 0 {
		t.Errorf("initial node counters non-zero: %+v", s)
	}
	if s.RelsAdded != 0 || s.RelsRead != 0 || s.RelsUpdated != 0 || s.RelsDeleted != 0 {
		t.Errorf("initial rel counters non-zero: %+v", s)
	}
	if s.NodeCacheHits != 0 || s.NodeCacheMisses != 0 || s.RelCacheHits != 0 || s.RelCacheMisses != 0 {
		t.Errorf("initial cache counters non-zero: %+v", s)
	}
}

func TestGraphStats_NodeCounters(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	n, err := g.AddNode([]string{"X"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	s := g.Stats()
	if s.NodesAdded != 1 {
		t.Errorf("NodesAdded = %d, want 1", s.NodesAdded)
	}

	if _, err := g.GetNode(id); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	s = g.Stats()
	if s.NodesRead != 1 {
		t.Errorf("NodesRead = %d, want 1", s.NodesRead)
	}

	if _, err := g.UpdateNode(id, map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	s = g.Stats()
	if s.NodesUpdated != 1 {
		t.Errorf("NodesUpdated = %d, want 1", s.NodesUpdated)
	}

	if err := g.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	s = g.Stats()
	if s.NodesDeleted != 1 {
		t.Errorf("NodesDeleted = %d, want 1", s.NodesDeleted)
	}
}

func TestGraphStats_RelCounters(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	a, err := g.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.AddNode([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	s := g.Stats()
	if s.RelsAdded != 1 {
		t.Errorf("RelsAdded = %d, want 1", s.RelsAdded)
	}

	if _, err := g.GetRelationship(rid); err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	s = g.Stats()
	if s.RelsRead != 1 {
		t.Errorf("RelsRead = %d, want 1", s.RelsRead)
	}

	if _, err := g.UpdateRelationship(rid, map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	s = g.Stats()
	if s.RelsUpdated != 1 {
		t.Errorf("RelsUpdated = %d, want 1", s.RelsUpdated)
	}

	if err := g.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	s = g.Stats()
	if s.RelsDeleted != 1 {
		t.Errorf("RelsDeleted = %d, want 1", s.RelsDeleted)
	}
}

// TestGraphStats_EmptyUpdate_NoUpdateIncrement verifies that an empty updates map
// causes UpdateNode to delegate to GetNode (incrementing NodesRead) without
// incrementing NodesUpdated.
func TestGraphStats_EmptyUpdate_NoUpdateIncrement(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	n, err := g.AddNode([]string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	before := g.Stats()
	// Empty updates → returns GetNode result; increments Read but not Updated.
	if _, err := g.UpdateNode(id, map[string]any{}); err != nil {
		t.Fatalf("UpdateNode (empty): %v", err)
	}
	after := g.Stats()

	if after.NodesUpdated != before.NodesUpdated {
		t.Errorf("NodesUpdated changed on empty update: %d -> %d", before.NodesUpdated, after.NodesUpdated)
	}
	if after.NodesRead != before.NodesRead+1 {
		t.Errorf("NodesRead should increment on empty UpdateNode (calls GetNode): %d -> %d", before.NodesRead, after.NodesRead)
	}
}

func TestGraphStats_CacheMetrics_MemoryStore_Zero(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{}) // MemoryStore — no cache instrumentation
	n, err := g.AddNode([]string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	if _, err := g.GetNode(id); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if _, err := g.GetNode(id); err != nil {
		t.Fatalf("GetNode (2nd): %v", err)
	}

	s := g.Stats()
	if s.NodeCacheHits != 0 || s.NodeCacheMisses != 0 {
		t.Errorf("MemoryStore node cache metrics should be 0: hits=%d misses=%d", s.NodeCacheHits, s.NodeCacheMisses)
	}
	if s.RelCacheHits != 0 || s.RelCacheMisses != 0 {
		t.Errorf("MemoryStore rel cache metrics should be 0: hits=%d misses=%d", s.RelCacheHits, s.RelCacheMisses)
	}
}

// TestGraphStats_CacheMetrics_BadgerStore verifies that badger.Store populates
// NodeCacheHits and NodeCacheMisses in Stats(). AddNode calls PutNode which
// inserts the node into the LRU cache, so subsequent GetNode calls are cache hits.
func TestGraphStats_CacheMetrics_BadgerStore(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New(BadgerInMemory): %v", err)
	}
	defer func() {
		if err := g.Close(); err != nil {
			t.Logf("Close: %v", err)
		}
	}()

	n, err := g.AddNode([]string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// PutNode inserted the node into the LRU cache, so both GetNode calls are hits.
	if _, err := g.GetNode(id); err != nil {
		t.Fatalf("GetNode (1st): %v", err)
	}
	if _, err := g.GetNode(id); err != nil {
		t.Fatalf("GetNode (2nd): %v", err)
	}

	s := g.Stats()
	if s.NodeCacheHits == 0 {
		t.Errorf("expected NodeCacheHits > 0 with badger.Store, got %d", s.NodeCacheHits)
	}
	// Total cache activity must be positive.
	if s.NodeCacheHits+s.NodeCacheMisses == 0 {
		t.Error("expected some node cache activity, got zero for both hits and misses")
	}
}

func TestGraphStats_UpdateNodeInPlace_CountsAsUpdate(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	n, err := g.AddNode([]string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	before := g.Stats().NodesUpdated
	if _, err := g.UpdateNodeInPlace(id, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("UpdateNodeInPlace: %v", err)
	}
	after := g.Stats().NodesUpdated
	if after != before+1 {
		t.Errorf("NodesUpdated after UpdateNodeInPlace: got %d, want %d", after, before+1)
	}
}

func TestGraphStats_UpdateRelInPlace_CountsAsUpdate(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	a, err := g.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.AddNode([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := r.ID()

	before := g.Stats().RelsUpdated
	if _, err := g.UpdateRelInPlace(id, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}
	after := g.Stats().RelsUpdated
	if after != before+1 {
		t.Errorf("RelsUpdated after UpdateRelInPlace: got %d, want %d", after, before+1)
	}
}
