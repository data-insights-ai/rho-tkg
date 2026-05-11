package core

import (
	"errors"
	"testing"
)

func TestGraphStats_InitialState(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	s := g.Stats.Get()
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
	n, err := g.Nodes.Add([]string{"X"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	s := g.Stats.Get()
	if s.NodesAdded != 1 {
		t.Errorf("NodesAdded = %d, want 1", s.NodesAdded)
	}

	if _, err := g.Nodes.Get(id); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	s = g.Stats.Get()
	if s.NodesRead != 1 {
		t.Errorf("NodesRead = %d, want 1", s.NodesRead)
	}

	if _, err := g.Nodes.Update(id, map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	s = g.Stats.Get()
	if s.NodesUpdated != 1 {
		t.Errorf("NodesUpdated = %d, want 1", s.NodesUpdated)
	}

	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	s = g.Stats.Get()
	if s.NodesDeleted != 1 {
		t.Errorf("NodesDeleted = %d, want 1", s.NodesDeleted)
	}
}

func TestGraphStats_RelCounters(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	s := g.Stats.Get()
	if s.RelsAdded != 1 {
		t.Errorf("RelsAdded = %d, want 1", s.RelsAdded)
	}

	if _, err := g.Rels.Get(rid); err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	s = g.Stats.Get()
	if s.RelsRead != 1 {
		t.Errorf("RelsRead = %d, want 1", s.RelsRead)
	}

	if _, err := g.Rels.Update(rid, map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	s = g.Stats.Get()
	if s.RelsUpdated != 1 {
		t.Errorf("RelsUpdated = %d, want 1", s.RelsUpdated)
	}

	if err := g.Rels.Delete(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	s = g.Stats.Get()
	if s.RelsDeleted != 1 {
		t.Errorf("RelsDeleted = %d, want 1", s.RelsDeleted)
	}
}

func TestGraphStats_BatchCreateCounters(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	a, err := b.AddNode([]string{"BatchStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	bn, err := b.AddNode([]string{"BatchStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	if _, err := b.AddRelationship("BATCH_STATS_REL", a, bn, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	result, err := b.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Created != 3 {
		t.Fatalf("Created = %d, want 3", result.Created)
	}

	s := g.Stats.Get()
	if s.NodesAdded != 2 {
		t.Fatalf("NodesAdded = %d, want 2", s.NodesAdded)
	}
	if s.RelsAdded != 1 {
		t.Fatalf("RelsAdded = %d, want 1", s.RelsAdded)
	}
}

func TestGraphStats_CloseVersionCounters(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add([]string{"CloseStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"CloseStats"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add("CLOSE_STATS_REL", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	before := g.Stats.Get()
	if err := g.Nodes.CloseVersion(a.ID(), 100); err != nil {
		t.Fatalf("CloseVersion node: %v", err)
	}
	afterNode := g.Stats.Get()
	if afterNode.NodesUpdated != before.NodesUpdated+1 {
		t.Fatalf("NodesUpdated after node CloseVersion = %d, want %d", afterNode.NodesUpdated, before.NodesUpdated+1)
	}

	if err := g.Rels.CloseVersion(r.ID(), 100); err != nil {
		t.Fatalf("CloseVersion rel: %v", err)
	}
	afterRel := g.Stats.Get()
	if afterRel.RelsUpdated != before.RelsUpdated+1 {
		t.Fatalf("RelsUpdated after rel CloseVersion = %d, want %d", afterRel.RelsUpdated, before.RelsUpdated+1)
	}

	if err := g.Nodes.CloseVersion(a.ID(), 200); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second node CloseVersion = %v, want ErrAlreadyClosed", err)
	}
	if err := g.Rels.CloseVersion(r.ID(), 200); !errors.Is(err, ErrAlreadyClosed) {
		t.Fatalf("second rel CloseVersion = %v, want ErrAlreadyClosed", err)
	}
	afterRejected := g.Stats.Get()
	if afterRejected.NodesUpdated != afterNode.NodesUpdated {
		t.Fatalf("NodesUpdated changed after rejected CloseVersion: %d -> %d", afterNode.NodesUpdated, afterRejected.NodesUpdated)
	}
	if afterRejected.RelsUpdated != afterRel.RelsUpdated {
		t.Fatalf("RelsUpdated changed after rejected CloseVersion: %d -> %d", afterRel.RelsUpdated, afterRejected.RelsUpdated)
	}
}

// TestGraphStats_EmptyUpdate_NoUpdateIncrement verifies that an empty updates map
// causes UpdateNode to delegate to GetNode (incrementing NodesRead) without
// incrementing NodesUpdated.
func TestGraphStats_EmptyUpdate_NoUpdateIncrement(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	n, err := g.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	before := g.Stats.Get()
	// Empty updates → returns GetNode result; increments Read but not Updated.
	if _, err := g.Nodes.Update(id, map[string]any{}); err != nil {
		t.Fatalf("UpdateNode (empty): %v", err)
	}
	after := g.Stats.Get()

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
	n, err := g.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	if _, err := g.Nodes.Get(id); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if _, err := g.Nodes.Get(id); err != nil {
		t.Fatalf("GetNode (2nd): %v", err)
	}

	s := g.Stats.Get()
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

	n, err := g.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// PutNode inserted the node into the LRU cache, so both GetNode calls are hits.
	if _, err := g.Nodes.Get(id); err != nil {
		t.Fatalf("GetNode (1st): %v", err)
	}
	if _, err := g.Nodes.Get(id); err != nil {
		t.Fatalf("GetNode (2nd): %v", err)
	}

	s := g.Stats.Get()
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
	n, err := g.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	before := g.Stats.Get().NodesUpdated
	if _, err := g.Nodes.UpdateInPlace(id, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("UpdateNodeInPlace: %v", err)
	}
	after := g.Stats.Get().NodesUpdated
	if after != before+1 {
		t.Errorf("NodesUpdated after UpdateNodeInPlace: got %d, want %d", after, before+1)
	}
}

func TestGraphStats_UpdateRelInPlace_CountsAsUpdate(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	id := r.ID()

	before := g.Stats.Get().RelsUpdated
	if _, err := g.Rels.UpdateInPlace(id, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}
	after := g.Stats.Get().RelsUpdated
	if after != before+1 {
		t.Errorf("RelsUpdated after UpdateRelInPlace: got %d, want %d", after, before+1)
	}
}
