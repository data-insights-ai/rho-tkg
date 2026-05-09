package core

import (
	"sync"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestGraphAddRelationshipByID_TemporalProps(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"X"}, nil)
	nB, _ := g.Nodes.Add([]string{"X"}, nil)

	r, err := g.Rels.AddByID("REL", nA.ID(), nB.ID(), map[string]any{
		"tkg_valid_from": int64(1000),
		"tkg_created_at": int64(2000),
	})
	if err != nil {
		t.Fatalf("AddRelationshipByID() returned error: %v", err)
	}
	tm := r.Temporal()
	if tm == nil {
		t.Fatal("Temporal() is nil")
	}
	if int64(tm.ValidFrom) != 1000 {
		t.Errorf("ValidFrom = %d, want 1000", tm.ValidFrom)
	}
	if int64(tm.CreatedAt) != 2000 {
		t.Errorf("CreatedAt = %d, want 2000", tm.CreatedAt)
	}
}

// ---- AddRelationshipByIDIfAbsent ----

func TestGraphAddRelationshipByIDIfAbsent_DifferentTypes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"Person"}, nil)
	nB, _ := g.Nodes.Add([]string{"Person"}, nil)
	aID := nA.ID()
	bID := nB.ID()

	// Same endpoints, different types — both should create.
	_, created1, err := g.Rels.AddByIDIfAbsent("KNOWS", aID, bID, nil)
	if err != nil {
		t.Fatalf("KNOWS: %v", err)
	}
	if !created1 {
		t.Error("KNOWS: created should be true")
	}

	_, created2, err := g.Rels.AddByIDIfAbsent("LIKES", aID, bID, nil)
	if err != nil {
		t.Fatalf("LIKES: %v", err)
	}
	if !created2 {
		t.Error("LIKES: created should be true")
	}

	// Both should exist.
	all, err := g.Rels.Outgoing(aID, "")
	if err != nil {
		t.Fatalf("OutgoingRelationships(all): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("OutgoingRelationships(all): got %d, want 2", len(all))
	}
}

func TestGraphUpdateNodeUpdatedAt(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	updated, _ := g.Nodes.Update(id, map[string]any{"name": "Alice"})
	tm := updated.Temporal()
	if tm == nil {
		t.Fatal("temporal should be set after update")
	}
	if tm.UpdatedAt == 0 {
		t.Fatal("UpdatedAt should be non-zero after update")
	}
}

func TestGraphUpdateNodeConcurrentDifferentNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})

	const count = 20
	ids := make([]types.NodeID, count)
	for i := range count {
		n, _ := g.Nodes.Add([]string{"X"}, map[string]any{"v": 0})
		ids[i] = n.ID()
	}

	var wg sync.WaitGroup
	wg.Add(count)
	for i := range count {
		go func(idx int) {
			defer wg.Done()
			g.Nodes.Update(ids[idx], map[string]any{"v": idx + 1})
		}(i)
	}
	wg.Wait()

	for i, id := range ids {
		got, err := g.Nodes.Get(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		v, _ := got.GetProperty("v")
		if v != i+1 {
			t.Errorf("node %d: v = %v, want %d", id, v, i+1)
		}
	}
}

func TestGraphUpdateRelUpdatedAt(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"X"}, nil)
	nB, _ := g.Nodes.Add([]string{"X"}, nil)
	r, _ := g.Rels.Add("KNOWS", nA, nB, nil)
	id := r.ID()

	updated, _ := g.Rels.Update(id, map[string]any{"x": 1})
	tm := updated.Temporal()
	if tm == nil {
		t.Fatal("temporal should be set after update")
	}
	if tm.UpdatedAt == 0 {
		t.Fatal("UpdatedAt should be non-zero after update")
	}
}

func TestGraphVerifyNodeChain_DeletedEntity(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	// Update to create version history.
	_, err = g.Nodes.Update(nodeID, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}

	// Delete the node — tombstone saved to history.
	if err := g.Nodes.Delete(nodeID); err != nil {
		t.Fatal(err)
	}

	// Verify the hash chain of the deleted node — must succeed.
	valid, err := g.Hash.VerifyNodeChain(nodeID)
	if err != nil {
		t.Fatalf("VerifyNodeChain on deleted node: %v", err)
	}
	if !valid {
		t.Fatal("hash chain verification should succeed for deleted entity with history")
	}
}

func TestGraphVerifyRelChain_DeletedEntity(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	nA, _ := g.Nodes.Add([]string{"X"}, nil)
	nB, _ := g.Nodes.Add([]string{"X"}, nil)
	r, _ := g.Rels.Add("KNOWS", nA, nB, map[string]any{"w": int64(1)})
	relID := r.ID()

	// Update to create version history.
	_, err = g.Rels.Update(relID, map[string]any{"w": int64(2)})
	if err != nil {
		t.Fatal(err)
	}

	// Delete the relationship — tombstone saved to history.
	if err := g.Rels.Delete(relID); err != nil {
		t.Fatal(err)
	}

	// Verify the hash chain of the deleted relationship — must succeed.
	valid, err := g.Hash.VerifyRelChain(relID)
	if err != nil {
		t.Fatalf("VerifyRelChain on deleted rel: %v", err)
	}
	if !valid {
		t.Fatal("hash chain verification should succeed for deleted relationship with history")
	}
}

func TestGraphBadgerGetNodesValidAt_DeletedNode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	clk := useTestClock(t, g)

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	id := n.ID()

	validTime := nowMs() // wall-clock: before any test-clock-stamped Delete (R5-F10)

	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Query at pre-deletion time — both nodes should appear.
	nodes, err := g.Temporal.NodesAt(validTime)
	if err != nil {
		t.Fatalf("GetNodesValidAt: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes at pre-deletion time, got %d", len(nodes))
	}

	// Query strictly after Delete (test-clock-stamped ValidTo).
	nodes, err = g.Temporal.NodesAt(clk.PeekInstant())
	if err != nil {
		t.Fatalf("GetNodesValidAt now: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node at current time, got %d", len(nodes))
	}
}

func TestGraphBadgerSnapshot_IncludesDeletedNodes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	useTestClock(t, g)

	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	g.Rels.Add("KNOWS", a, b, nil)

	snapshotTime := nowMs() // wall-clock: before test-clock-stamped Delete (R5-F10)

	if err := g.Nodes.Delete(a.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Snapshot at pre-deletion time — both nodes and the rel.
	snap, err := g.Temporal.Snapshot(snapshotTime)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.NodeCount != 2 {
		t.Fatalf("expected 2 nodes, got %d", snap.NodeCount)
	}
	if snap.RelCount != 1 {
		t.Fatalf("expected 1 rel, got %d", snap.RelCount)
	}
}
