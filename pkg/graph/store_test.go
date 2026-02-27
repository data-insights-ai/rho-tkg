package graph

import (
	"errors"
	"sync"
	"testing"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// ─── MemoryStore: Node operations ───────────────────────────────────────────

func TestMemoryStorePutGetNode(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, nil)

	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode() returned error: %v", err)
	}

	got, err := ms.GetNode(snowflake.ID(1))
	if err != nil {
		t.Fatalf("GetNode() returned error: %v", err)
	}
	if got != n {
		t.Fatal("GetNode() returned different pointer than PutNode()")
	}
}

func TestMemoryStorePutDuplicateNodeReturnsError(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, nil)

	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	err := ms.PutNode(n)
	if err == nil {
		t.Fatal("PutNode duplicate should return error")
	}
	if !errors.Is(err, ErrNodeExists) {
		t.Errorf("errors.Is(err, ErrNodeExists) = false; err = %v", err)
	}
}

func TestMemoryStoreGetNonexistentNode(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	_, err := ms.GetNode(snowflake.ID(999))
	if err == nil {
		t.Fatal("GetNode(nonexistent) should return error")
	}
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestMemoryStoreDeleteNode(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, []uint16{20})
	ms.PutNode(n)

	if err := ms.DeleteNode(snowflake.ID(1)); err != nil {
		t.Fatalf("DeleteNode() returned error: %v", err)
	}

	_, err := ms.GetNode(snowflake.ID(1))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}

	// Label index must be cleaned up.
	if nodes := ms.NodesByLabel(10); len(nodes) != 0 {
		t.Errorf("NodesByLabel(10) after delete: got %d nodes, want 0", len(nodes))
	}
	if nodes := ms.NodesByLabel(20); len(nodes) != 0 {
		t.Errorf("NodesByLabel(20) after delete: got %d nodes, want 0", len(nodes))
	}
}

func TestMemoryStoreDeleteNonexistentNode(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	err := ms.DeleteNode(snowflake.ID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("DeleteNode(nonexistent): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

// ─── MemoryStore: Relationship operations ───────────────────────────────────

func TestMemoryStorePutGetRelationship(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship() returned error: %v", err)
	}

	got, err := ms.GetRelationship(snowflake.ID(100))
	if err != nil {
		t.Fatalf("GetRelationship() returned error: %v", err)
	}
	if got != r {
		t.Fatal("GetRelationship() returned different pointer")
	}
}

func TestMemoryStorePutRelMissingStartNode(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	err := ms.PutRelationship(r)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("PutRelationship(missing start): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestMemoryStorePutRelMissingEndNode(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	ms.PutNode(nA)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	err := ms.PutRelationship(r)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("PutRelationship(missing end): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestMemoryStorePutDuplicateRelReturnsError(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	ms.PutRelationship(r)

	err := ms.PutRelationship(r)
	if !errors.Is(err, ErrRelExists) {
		t.Errorf("PutRelationship duplicate: errors.Is(err, ErrRelExists) = false; err = %v", err)
	}
}

func TestMemoryStoreDeleteRelationship(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	ms.PutRelationship(r)

	if err := ms.DeleteRelationship(snowflake.ID(100)); err != nil {
		t.Fatalf("DeleteRelationship() returned error: %v", err)
	}

	_, err := ms.GetRelationship(snowflake.ID(100))
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship after delete: errors.Is(err, ErrRelNotFound) = false; err = %v", err)
	}
}

func TestMemoryStoreDeleteNonexistentRel(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	err := ms.DeleteRelationship(snowflake.ID(999))
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("DeleteRelationship(nonexistent): errors.Is(err, ErrRelNotFound) = false; err = %v", err)
	}
}

// ─── MemoryStore: Index queries ─────────────────────────────────────────────

func TestMemoryStoreNodesByLabel(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n1 := types.NewNode(snowflake.ID(1), 10, []uint16{20})
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	n3 := types.NewNode(snowflake.ID(3), 30, nil)
	ms.PutNode(n1)
	ms.PutNode(n2)
	ms.PutNode(n3)

	// Label 10: n1 + n2.
	got := ms.NodesByLabel(10)
	if len(got) != 2 {
		t.Fatalf("NodesByLabel(10) = %d nodes, want 2", len(got))
	}

	// Label 20: only n1 (extra label).
	got = ms.NodesByLabel(20)
	if len(got) != 1 {
		t.Fatalf("NodesByLabel(20) = %d nodes, want 1", len(got))
	}

	// Label 99: none.
	got = ms.NodesByLabel(99)
	if len(got) != 0 {
		t.Fatalf("NodesByLabel(99) = %d nodes, want 0", len(got))
	}
}

func TestMemoryStoreRelsByType(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r1 := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	r2 := types.NewRelationship(snowflake.ID(101), 5, snowflake.ID(10), snowflake.ID(20))
	r3 := types.NewRelationship(snowflake.ID(102), 7, snowflake.ID(10), snowflake.ID(20))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)
	ms.PutRelationship(r3)

	// Type 5: r1 + r2.
	got := ms.RelationshipsByType(5)
	if len(got) != 2 {
		t.Fatalf("RelationshipsByType(5) = %d rels, want 2", len(got))
	}

	// Type 7: r3.
	got = ms.RelationshipsByType(7)
	if len(got) != 1 {
		t.Fatalf("RelationshipsByType(7) = %d rels, want 1", len(got))
	}

	// Type 99: none.
	got = ms.RelationshipsByType(99)
	if len(got) != 0 {
		t.Fatalf("RelationshipsByType(99) = %d rels, want 0", len(got))
	}
}

// ─── MemoryStore: Adjacency queries ─────────────────────────────────────────

func TestMemoryStoreOutgoingRelationships(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	nC := types.NewNode(snowflake.ID(30), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	r1 := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	r2 := types.NewRelationship(snowflake.ID(101), 7, snowflake.ID(10), snowflake.ID(30))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	// All outgoing from node 10.
	got := ms.OutgoingRelationships(snowflake.ID(10), 0)
	if len(got) != 2 {
		t.Fatalf("OutgoingRelationships(10, 0) = %d rels, want 2", len(got))
	}

	// Filtered by type 5.
	got = ms.OutgoingRelationships(snowflake.ID(10), 5)
	if len(got) != 1 {
		t.Fatalf("OutgoingRelationships(10, 5) = %d rels, want 1", len(got))
	}

	// Node with no outgoing.
	got = ms.OutgoingRelationships(snowflake.ID(20), 0)
	if len(got) != 0 {
		t.Fatalf("OutgoingRelationships(20, 0) = %d rels, want 0", len(got))
	}
}

func TestMemoryStoreIncomingRelationships(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	nC := types.NewNode(snowflake.ID(30), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	r1 := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	r2 := types.NewRelationship(snowflake.ID(101), 7, snowflake.ID(30), snowflake.ID(20))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	// All incoming to node 20.
	got := ms.IncomingRelationships(snowflake.ID(20), 0)
	if len(got) != 2 {
		t.Fatalf("IncomingRelationships(20, 0) = %d rels, want 2", len(got))
	}

	// Filtered by type 5.
	got = ms.IncomingRelationships(snowflake.ID(20), 5)
	if len(got) != 1 {
		t.Fatalf("IncomingRelationships(20, 5) = %d rels, want 1", len(got))
	}

	// Node with no incoming.
	got = ms.IncomingRelationships(snowflake.ID(10), 0)
	if len(got) != 0 {
		t.Fatalf("IncomingRelationships(10, 0) = %d rels, want 0", len(got))
	}
}

func TestMemoryStoreOutgoingTypeZeroReturnsAll(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r1 := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	r2 := types.NewRelationship(snowflake.ID(101), 7, snowflake.ID(10), snowflake.ID(20))
	r3 := types.NewRelationship(snowflake.ID(102), 9, snowflake.ID(10), snowflake.ID(20))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)
	ms.PutRelationship(r3)

	got := ms.OutgoingRelationships(snowflake.ID(10), 0)
	if len(got) != 3 {
		t.Fatalf("OutgoingRelationships(10, 0) = %d rels, want 3 (all types)", len(got))
	}
}

// ─── MemoryStore: Counts ────────────────────────────────────────────────────

func TestMemoryStoreNodeCount(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	if ms.NodeCount() != 0 {
		t.Fatalf("empty NodeCount() = %d, want 0", ms.NodeCount())
	}

	ms.PutNode(types.NewNode(snowflake.ID(1), 1, nil))
	ms.PutNode(types.NewNode(snowflake.ID(2), 1, nil))
	if ms.NodeCount() != 2 {
		t.Fatalf("NodeCount() = %d, want 2", ms.NodeCount())
	}
}

func TestMemoryStoreRelCount(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	if ms.RelationshipCount() != 0 {
		t.Fatalf("empty RelationshipCount() = %d, want 0", ms.RelationshipCount())
	}

	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20)))
	if ms.RelationshipCount() != 1 {
		t.Fatalf("RelationshipCount() = %d, want 1", ms.RelationshipCount())
	}
}

// ─── MemoryStore: Adjacency cleanup on delete ───────────────────────────────

func TestMemoryStoreDeleteRelAdjacencyCleanup(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	ms.PutRelationship(r)

	// Delete the relationship.
	ms.DeleteRelationship(snowflake.ID(100))

	// Adjacency must be empty.
	if out := ms.OutgoingRelationships(snowflake.ID(10), 0); len(out) != 0 {
		t.Errorf("OutgoingRelationships after delete: got %d, want 0", len(out))
	}
	if in := ms.IncomingRelationships(snowflake.ID(20), 0); len(in) != 0 {
		t.Errorf("IncomingRelationships after delete: got %d, want 0", len(in))
	}

	// Type index must be empty.
	if rels := ms.RelationshipsByType(5); len(rels) != 0 {
		t.Errorf("RelationshipsByType after delete: got %d, want 0", len(rels))
	}
}

// ─── MemoryStore: Concurrent access ─────────────────────────────────────────

func TestMemoryStoreConcurrentAccess(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	const goroutines = 20

	// Pre-create nodes for relationships.
	for i := range goroutines * 2 {
		ms.PutNode(types.NewNode(snowflake.ID(int64(i+1)), 1, nil))
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			relID := snowflake.ID(int64(1000 + idx))
			startID := snowflake.ID(int64(idx*2 + 1))
			endID := snowflake.ID(int64(idx*2 + 2))

			r := types.NewRelationship(relID, 5, startID, endID)
			ms.PutRelationship(r)

			ms.GetRelationship(relID)
			ms.OutgoingRelationships(startID, 0)
			ms.IncomingRelationships(endID, 0)
			ms.RelationshipCount()
			ms.NodeCount()
		}(i)
	}

	wg.Wait()

	if ms.RelationshipCount() != goroutines {
		t.Errorf("RelationshipCount() = %d, want %d", ms.RelationshipCount(), goroutines)
	}
}
