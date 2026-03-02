package graph

import (
	"errors"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
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
	if got.InternalID() != n.InternalID() {
		t.Fatal("GetNode() returned node with different ID")
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
	nodes, err2 := ms.NodesByLabel(10, QueryOpts{})
	if err2 != nil {
		t.Fatal(err2)
	}
	if len(nodes) != 0 {
		t.Errorf("NodesByLabel(10) after delete: got %d nodes, want 0", len(nodes))
	}
	nodes, err2 = ms.NodesByLabel(20, QueryOpts{})
	if err2 != nil {
		t.Fatal(err2)
	}
	if len(nodes) != 0 {
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
	if got.InternalID() != r.InternalID() {
		t.Fatal("GetRelationship() returned relationship with different ID")
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
	got, err := ms.NodesByLabel(10, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("NodesByLabel(10) = %d nodes, want 2", len(got))
	}

	// Label 20: only n1 (extra label).
	got, err = ms.NodesByLabel(20, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("NodesByLabel(20) = %d nodes, want 1", len(got))
	}

	// Label 99: none.
	got, err = ms.NodesByLabel(99, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := ms.RelationshipsByType(5, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("RelationshipsByType(5) = %d rels, want 2", len(got))
	}

	// Type 7: r3.
	got, err = ms.RelationshipsByType(7, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("RelationshipsByType(7) = %d rels, want 1", len(got))
	}

	// Type 99: none.
	got, err = ms.RelationshipsByType(99, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := ms.OutgoingRelationships(snowflake.ID(10), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("OutgoingRelationships(10, 0) = %d rels, want 2", len(got))
	}

	// Filtered by type 5.
	got, err = ms.OutgoingRelationships(snowflake.ID(10), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("OutgoingRelationships(10, 5) = %d rels, want 1", len(got))
	}

	// Node with no outgoing.
	got, err = ms.OutgoingRelationships(snowflake.ID(20), 0)
	if err != nil {
		t.Fatal(err)
	}
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
	got, err := ms.IncomingRelationships(snowflake.ID(20), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("IncomingRelationships(20, 0) = %d rels, want 2", len(got))
	}

	// Filtered by type 5.
	got, err = ms.IncomingRelationships(snowflake.ID(20), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("IncomingRelationships(20, 5) = %d rels, want 1", len(got))
	}

	// Node with no incoming.
	got, err = ms.IncomingRelationships(snowflake.ID(10), 0)
	if err != nil {
		t.Fatal(err)
	}
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

	got, err := ms.OutgoingRelationships(snowflake.ID(10), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("OutgoingRelationships(10, 0) = %d rels, want 3 (all types)", len(got))
	}
}

// ─── MemoryStore: Counts ────────────────────────────────────────────────────

func TestMemoryStoreNodeCount(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	cnt, err := ms.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("empty NodeCount() = %d, want 0", cnt)
	}

	ms.PutNode(types.NewNode(snowflake.ID(1), 1, nil))
	ms.PutNode(types.NewNode(snowflake.ID(2), 1, nil))
	cnt, err = ms.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Fatalf("NodeCount() = %d, want 2", cnt)
	}
}

func TestMemoryStoreRelCount(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	cnt, err := ms.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("empty RelationshipCount() = %d, want 0", cnt)
	}

	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20)))
	cnt, err = ms.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatalf("RelationshipCount() = %d, want 1", cnt)
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
	out, err := ms.OutgoingRelationships(snowflake.ID(10), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Errorf("OutgoingRelationships after delete: got %d, want 0", len(out))
	}
	in, err := ms.IncomingRelationships(snowflake.ID(20), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(in) != 0 {
		t.Errorf("IncomingRelationships after delete: got %d, want 0", len(in))
	}

	// Type index must be empty.
	rels, err := ms.RelationshipsByType(5, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 0 {
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
			_, _ = ms.OutgoingRelationships(startID, 0)
			_, _ = ms.IncomingRelationships(endID, 0)
			_, _ = ms.RelationshipCount()
			_, _ = ms.NodeCount()
		}(i)
	}

	wg.Wait()

	cnt, err := ms.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if cnt != goroutines {
		t.Errorf("RelationshipCount() = %d, want %d", cnt, goroutines)
	}
}

// ─── MemoryStore: Deterministic sort order ──────────────────────────────────

func TestMemoryStoreNodesByLabelSortedByID(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	// Insert in non-sequential order to ensure sort is effective.
	n3 := types.NewNode(snowflake.ID(30), 1, nil)
	n1 := types.NewNode(snowflake.ID(10), 1, nil)
	n2 := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(n3)
	ms.PutNode(n1)
	ms.PutNode(n2)

	result, err := ms.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("NodesByLabel(1) = %d nodes, want 3", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev := result[i-1].InternalID().SnowflakeID()
		curr := result[i].InternalID().SnowflakeID()
		if prev >= curr {
			t.Errorf("NodesByLabel not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

func TestMemoryStoreRelsByTypeSortedByID(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(1), 1, nil)
	nB := types.NewNode(snowflake.ID(2), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	// Insert in reverse order.
	r3 := types.NewRelationship(snowflake.ID(300), 5, snowflake.ID(1), snowflake.ID(2))
	r1 := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(1), snowflake.ID(2))
	r2 := types.NewRelationship(snowflake.ID(200), 5, snowflake.ID(1), snowflake.ID(2))
	ms.PutRelationship(r3)
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	result, err := ms.RelationshipsByType(5, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("RelationshipsByType(5) = %d rels, want 3", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev := result[i-1].InternalID().SnowflakeID()
		curr := result[i].InternalID().SnowflakeID()
		if prev >= curr {
			t.Errorf("RelationshipsByType not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

func TestMemoryStoreOutgoingRelsSortedByID(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(1), 1, nil)
	nB := types.NewNode(snowflake.ID(2), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	// Insert in reverse order.
	r3 := types.NewRelationship(snowflake.ID(300), 5, snowflake.ID(1), snowflake.ID(2))
	r1 := types.NewRelationship(snowflake.ID(100), 7, snowflake.ID(1), snowflake.ID(2))
	r2 := types.NewRelationship(snowflake.ID(200), 5, snowflake.ID(1), snowflake.ID(2))
	ms.PutRelationship(r3)
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	result, err := ms.OutgoingRelationships(snowflake.ID(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("OutgoingRelationships = %d rels, want 3", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev := result[i-1].InternalID().SnowflakeID()
		curr := result[i].InternalID().SnowflakeID()
		if prev >= curr {
			t.Errorf("OutgoingRelationships not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

func TestMemoryStoreIncomingRelsSortedByID(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(1), 1, nil)
	nB := types.NewNode(snowflake.ID(2), 1, nil)
	nC := types.NewNode(snowflake.ID(3), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)
	ms.PutNode(nC)

	// All point to nA, inserted in reverse order.
	r3 := types.NewRelationship(snowflake.ID(300), 5, snowflake.ID(3), snowflake.ID(1))
	r1 := types.NewRelationship(snowflake.ID(100), 7, snowflake.ID(2), snowflake.ID(1))
	r2 := types.NewRelationship(snowflake.ID(200), 5, snowflake.ID(3), snowflake.ID(1))
	ms.PutRelationship(r3)
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	result, err := ms.IncomingRelationships(snowflake.ID(1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("IncomingRelationships = %d rels, want 3", len(result))
	}
	for i := 1; i < len(result); i++ {
		prev := result[i-1].InternalID().SnowflakeID()
		curr := result[i].InternalID().SnowflakeID()
		if prev >= curr {
			t.Errorf("IncomingRelationships not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── MemoryStore: DeleteNodeCascade ──────────────────────────────────────────

func TestMemoryStoreDeleteNodeCascade(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	ms.PutRelationship(r)

	// Cascade delete A — relationship should be removed, B should survive.
	if err := ms.DeleteNodeCascade(snowflake.ID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// Node A gone.
	if _, err := ms.GetNode(snowflake.ID(10)); !errors.Is(err, ErrNodeNotFound) {
		t.Error("node A should be deleted")
	}

	// Relationship gone.
	if _, err := ms.GetRelationship(snowflake.ID(100)); !errors.Is(err, ErrRelNotFound) {
		t.Error("relationship should be cascade-deleted")
	}

	// Node B survives.
	if _, err := ms.GetNode(snowflake.ID(20)); err != nil {
		t.Errorf("node B should still exist: %v", err)
	}

	// Adjacency cleaned.
	out, _ := ms.OutgoingRelationships(snowflake.ID(10), 0)
	if len(out) != 0 {
		t.Errorf("outgoing should be empty, got %d", len(out))
	}
	in, _ := ms.IncomingRelationships(snowflake.ID(20), 0)
	if len(in) != 0 {
		t.Errorf("incoming should be empty, got %d", len(in))
	}
}

func TestMemoryStoreDeleteNodeCascadeSelfLoop(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	ms.PutNode(nA)

	// Self-loop: A → A.
	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(10))
	ms.PutRelationship(r)

	if err := ms.DeleteNodeCascade(snowflake.ID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade self-loop: %v", err)
	}

	nc, _ := ms.NodeCount()
	rc, _ := ms.RelationshipCount()
	if nc != 0 {
		t.Errorf("NodeCount = %d, want 0", nc)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount = %d, want 0", rc)
	}
}

func TestMemoryStoreDeleteNodeCascadeNotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	err := ms.DeleteNodeCascade(snowflake.ID(999))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

// ─── MemoryStore: Deterministic sort order ──────────────────────────────────

func TestMemoryStoreNodesByLabelDeterministic(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	for i := range 20 {
		ms.PutNode(types.NewNode(snowflake.ID(int64(i+1)), 1, nil))
	}

	// Call multiple times — must return the same order every time.
	first, err := ms.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		got, err := ms.NodesByLabel(1, QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(first) {
			t.Fatalf("length mismatch: %d vs %d", len(got), len(first))
		}
		for i := range first {
			if first[i].InternalID().SnowflakeID() != got[i].InternalID().SnowflakeID() {
				t.Fatalf("non-deterministic order at index %d: %d vs %d",
					i, first[i].InternalID().SnowflakeID(), got[i].InternalID().SnowflakeID())
			}
		}
	}
}

// ─── MemoryStore: ReplaceNode / ReplaceRelationship ─────────────────────────

func TestMemoryStoreReplaceNode(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("name", "Alice")
	ms.PutNode(n)

	// Retrieve, modify, replace.
	updated, _ := ms.GetNode(snowflake.ID(1))
	_ = updated.SetProperty("name", "Bob")

	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode() returned error: %v", err)
	}

	got, err := ms.GetNode(snowflake.ID(1))
	if err != nil {
		t.Fatalf("GetNode after replace: %v", err)
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Bob" {
		t.Fatalf("property after replace = %v, want Bob", v)
	}
}

func TestMemoryStoreReplaceNodeNotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(999), 10, nil)

	err := ms.ReplaceNode(n)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("ReplaceNode(nonexistent): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestMemoryStoreReplaceNodeCacheIsolation(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("name", "Alice")
	ms.PutNode(n)

	// Replace with a new value.
	updated, _ := ms.GetNode(snowflake.ID(1))
	_ = updated.SetProperty("name", "Bob")
	ms.ReplaceNode(updated)

	// Mutate the replaced node AFTER the call — must not affect store.
	_ = updated.SetProperty("name", "MUTATED")

	got, _ := ms.GetNode(snowflake.ID(1))
	v, _ := got.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("ReplaceNode did not deep copy: got %v, want Bob", v)
	}
}

func TestMemoryStoreReplaceRelationship(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	_ = r.SetProperty("weight", 1.0)
	ms.PutRelationship(r)

	// Retrieve, modify, replace.
	updated, _ := ms.GetRelationship(snowflake.ID(100))
	_ = updated.SetProperty("weight", 2.0)

	if err := ms.ReplaceRelationship(updated); err != nil {
		t.Fatalf("ReplaceRelationship() returned error: %v", err)
	}

	got, err := ms.GetRelationship(snowflake.ID(100))
	if err != nil {
		t.Fatalf("GetRelationship after replace: %v", err)
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != 2.0 {
		t.Fatalf("property after replace = %v, want 2.0", v)
	}
}

func TestMemoryStoreReplaceRelNotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	r := types.NewRelationship(snowflake.ID(999), 5, snowflake.ID(10), snowflake.ID(20))

	err := ms.ReplaceRelationship(r)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("ReplaceRelationship(nonexistent): errors.Is(err, ErrRelNotFound) = false; err = %v", err)
	}
}

func TestMemoryStoreReplaceRelCacheIsolation(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	_ = r.SetProperty("weight", 1.0)
	ms.PutRelationship(r)

	// Replace with new value.
	updated, _ := ms.GetRelationship(snowflake.ID(100))
	_ = updated.SetProperty("weight", 2.0)
	ms.ReplaceRelationship(updated)

	// Mutate after call — must not affect store.
	_ = updated.SetProperty("weight", 999.0)

	got, _ := ms.GetRelationship(snowflake.ID(100))
	v, _ := got.GetProperty("weight")
	if v != 2.0 {
		t.Fatalf("ReplaceRelationship did not deep copy: got %v, want 2.0", v)
	}
}

// ─── MemoryStore: Cache isolation ───────────────────────────────────────────

func TestMemoryStorePutNodeCacheIsolation(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("name", "Alice")
	ms.PutNode(n)

	// Mutate the original after Put.
	_ = n.SetProperty("name", "MUTATED")

	// GetNode must return the original value, not the mutation.
	got, err := ms.GetNode(snowflake.ID(1))
	if err != nil {
		t.Fatal(err)
	}
	v, _ := got.GetProperty("name")
	if v != "Alice" {
		t.Fatalf("PutNode did not copy: got %v, want Alice", v)
	}
}

func TestMemoryStoreGetNodeReturnsCopy(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("name", "Alice")
	ms.PutNode(n)

	// Get twice, mutate first result.
	first, _ := ms.GetNode(snowflake.ID(1))
	_ = first.SetProperty("name", "MUTATED")

	// Second Get must be unaffected.
	second, _ := ms.GetNode(snowflake.ID(1))
	v, _ := second.GetProperty("name")
	if v != "Alice" {
		t.Fatalf("GetNode returned shared pointer: got %v, want Alice", v)
	}
}

func TestMemoryStorePutRelCacheIsolation(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	_ = r.SetProperty("weight", 1.0)
	ms.PutRelationship(r)

	// Mutate original after Put.
	_ = r.SetProperty("weight", 999.0)

	got, err := ms.GetRelationship(snowflake.ID(100))
	if err != nil {
		t.Fatal(err)
	}
	v, _ := got.GetProperty("weight")
	if v != 1.0 {
		t.Fatalf("PutRelationship did not copy: got %v, want 1.0", v)
	}
}

func TestMemoryStoreGetRelReturnsCopy(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	_ = r.SetProperty("weight", 1.0)
	ms.PutRelationship(r)

	first, _ := ms.GetRelationship(snowflake.ID(100))
	_ = first.SetProperty("weight", 999.0)

	second, _ := ms.GetRelationship(snowflake.ID(100))
	v, _ := second.GetProperty("weight")
	if v != 1.0 {
		t.Fatalf("GetRelationship returned shared pointer: got %v, want 1.0", v)
	}
}

// ─── MemoryStore: Node version history ──────────────────────────────────────

func TestMemoryStorePutGetNodeVersion(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("name", "Alice")

	if err := ms.PutNodeVersion(snowflake.ID(1), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	got, err := ms.GetNodeVersion(snowflake.ID(1), 0)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if got.InternalID() != n.InternalID() {
		t.Fatal("version snapshot has wrong ID")
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("property mismatch: got %v", v)
	}

	// Cache isolation: mutate returned copy, re-read should be unaffected.
	_ = got.SetProperty("name", "mutated")
	got2, _ := ms.GetNodeVersion(snowflake.ID(1), 0)
	v2, _ := got2.GetProperty("name")
	if v2 != "Alice" {
		t.Fatalf("GetNodeVersion returned shared pointer: got %v, want Alice", v2)
	}
}

func TestMemoryStoreGetNodeVersionNotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	_, err := ms.GetNodeVersion(snowflake.ID(1), 0)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestMemoryStoreGetNodeHistory(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	id := snowflake.ID(1)

	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		if err := ms.PutNodeVersion(id, ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	history, err := ms.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

func TestMemoryStoreGetNodeHistoryEmpty(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	history, err := ms.GetNodeHistory(snowflake.ID(999))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d entries", len(history))
	}
}

func TestMemoryStoreGetNodeHistoryAscending(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	id := snowflake.ID(1)

	// Store out of order: 2, 0, 1.
	for _, ver := range []uint32{2, 0, 1} {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		if err := ms.PutNodeVersion(id, ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	history, err := ms.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("history not ascending: version[%d]=%d >= version[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestMemoryStoreTruncateNodeHistory(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	id := snowflake.ID(1)

	for ver := uint32(0); ver < 5; ver++ {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		ms.PutNodeVersion(id, ver, n)
	}

	if err := ms.TruncateNodeHistory(id, 2); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	history, _ := ms.GetNodeHistory(id)
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	// Should keep versions 3 and 4 (most recent).
	if history[0].Version() != 3 {
		t.Errorf("history[0].Version() = %d, want 3", history[0].Version())
	}
	if history[1].Version() != 4 {
		t.Errorf("history[1].Version() = %d, want 4", history[1].Version())
	}
}

func TestMemoryStoreTruncateNodeHistoryAll(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	id := snowflake.ID(1)

	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		ms.PutNodeVersion(id, ver, n)
	}

	if err := ms.TruncateNodeHistory(id, 0); err != nil {
		t.Fatalf("TruncateNodeHistory(0): %v", err)
	}

	history, _ := ms.GetNodeHistory(id)
	if len(history) != 0 {
		t.Fatalf("expected empty history after truncate all, got %d", len(history))
	}
}

func TestMemoryStoreDeleteNodePreservesHistory(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	n := types.NewNode(snowflake.ID(1), 10, nil)
	ms.PutNode(n)

	// Store some history.
	for ver := uint32(0); ver < 3; ver++ {
		snap := types.NewNode(snowflake.ID(1), 10, nil)
		snap.SetVersion(ver)
		ms.PutNodeVersion(snowflake.ID(1), ver, snap)
	}

	if err := ms.DeleteNodeCascade(snowflake.ID(1)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// History is preserved after cascade delete — temporal queries need it.
	history, _ := ms.GetNodeHistory(snowflake.ID(1))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved history entries after cascade delete, got %d", len(history))
	}
}

// ─── MemoryStore: Relationship version history ──────────────────────────────

func TestMemoryStorePutGetRelVersion(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	_ = r.SetProperty("weight", 1.5)

	if err := ms.PutRelVersion(snowflake.ID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	got, err := ms.GetRelVersion(snowflake.ID(100), 0)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if got.InternalID() != r.InternalID() {
		t.Fatal("version snapshot has wrong ID")
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != 1.5 {
		t.Fatalf("property mismatch: got %v", v)
	}

	// Cache isolation.
	_ = got.SetProperty("weight", 999.0)
	got2, _ := ms.GetRelVersion(snowflake.ID(100), 0)
	v2, _ := got2.GetProperty("weight")
	if v2 != 1.5 {
		t.Fatalf("GetRelVersion returned shared pointer: got %v, want 1.5", v2)
	}
}

func TestMemoryStoreGetRelVersionNotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	_, err := ms.GetRelVersion(snowflake.ID(100), 0)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestMemoryStoreGetRelHistory(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	id := snowflake.ID(100)

	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(id, 5, snowflake.ID(10), snowflake.ID(20))
		r.SetVersion(ver)
		ms.PutRelVersion(id, ver, r)
	}

	history, err := ms.GetRelHistory(id)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

func TestMemoryStoreGetRelHistoryEmpty(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	history, err := ms.GetRelHistory(snowflake.ID(999))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d entries", len(history))
	}
}

func TestMemoryStoreGetRelHistoryAscending(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	id := snowflake.ID(100)

	// Store out of order: 2, 0, 1.
	for _, ver := range []uint32{2, 0, 1} {
		r := types.NewRelationship(id, 5, snowflake.ID(10), snowflake.ID(20))
		r.SetVersion(ver)
		ms.PutRelVersion(id, ver, r)
	}

	history, err := ms.GetRelHistory(id)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("history not ascending: version[%d]=%d >= version[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestMemoryStoreTruncateRelHistory(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	id := snowflake.ID(100)

	for ver := uint32(0); ver < 5; ver++ {
		r := types.NewRelationship(id, 5, snowflake.ID(10), snowflake.ID(20))
		r.SetVersion(ver)
		ms.PutRelVersion(id, ver, r)
	}

	if err := ms.TruncateRelHistory(id, 2); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}

	history, _ := ms.GetRelHistory(id)
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	if history[0].Version() != 3 {
		t.Errorf("history[0].Version() = %d, want 3", history[0].Version())
	}
	if history[1].Version() != 4 {
		t.Errorf("history[1].Version() = %d, want 4", history[1].Version())
	}
}

func TestMemoryStoreTruncateRelHistoryAll(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	id := snowflake.ID(100)

	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(id, 5, snowflake.ID(10), snowflake.ID(20))
		r.SetVersion(ver)
		ms.PutRelVersion(id, ver, r)
	}

	if err := ms.TruncateRelHistory(id, 0); err != nil {
		t.Fatalf("TruncateRelHistory(0): %v", err)
	}

	history, _ := ms.GetRelHistory(id)
	if len(history) != 0 {
		t.Fatalf("expected empty history after truncate all, got %d", len(history))
	}
}

func TestMemoryStoreDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	ms.PutRelationship(r)

	for ver := uint32(0); ver < 3; ver++ {
		snap := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
		snap.SetVersion(ver)
		ms.PutRelVersion(snowflake.ID(100), ver, snap)
	}

	if err := ms.DeleteRelationship(snowflake.ID(100)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// History is preserved after delete — temporal queries need it.
	history, _ := ms.GetRelHistory(snowflake.ID(100))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved history entries after delete, got %d", len(history))
	}
}

func TestMemoryStoreDeleteNodeCascadePreservesRelHistory(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	r := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
	ms.PutRelationship(r)

	for ver := uint32(0); ver < 3; ver++ {
		snap := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20))
		snap.SetVersion(ver)
		ms.PutRelVersion(snowflake.ID(100), ver, snap)
	}

	if err := ms.DeleteNodeCascade(snowflake.ID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// History is preserved after cascade delete — temporal queries need it.
	history, _ := ms.GetRelHistory(snowflake.ID(100))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved rel history after cascade, got %d", len(history))
	}

	// Node has no version history entries (only rel versions were stored).
	nHistory, _ := ms.GetNodeHistory(snowflake.ID(10))
	if len(nHistory) != 0 {
		t.Fatalf("expected 0 node history (none created), got %d", len(nHistory))
	}
}

// ─── MemoryStore: Bulk queries — AllNodes ───────────────────────────────────

func TestMemoryStoreAllNodesEmpty(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	got, err := ms.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllNodes() on empty store = %v, want nil", got)
	}
}

func TestMemoryStoreAllNodes(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	ms.PutNode(types.NewNode(snowflake.ID(1), 10, nil))
	ms.PutNode(types.NewNode(snowflake.ID(2), 20, nil))
	ms.PutNode(types.NewNode(snowflake.ID(3), 10, nil))

	got, err := ms.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllNodes() = %d nodes, want 3", len(got))
	}
}

func TestMemoryStoreAllNodesSorted(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	// Insert in reverse order to ensure sort is effective.
	ms.PutNode(types.NewNode(snowflake.ID(30), 1, nil))
	ms.PutNode(types.NewNode(snowflake.ID(10), 1, nil))
	ms.PutNode(types.NewNode(snowflake.ID(20), 1, nil))

	got, err := ms.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllNodes() = %d nodes, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := got[i-1].InternalID().SnowflakeID()
		curr := got[i].InternalID().SnowflakeID()
		if prev >= curr {
			t.Errorf("AllNodes not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── MemoryStore: Bulk queries — AllRelationships ───────────────────────────

func TestMemoryStoreAllRelsEmpty(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	got, err := ms.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllRelationships() on empty store = %v, want nil", got)
	}
}

func TestMemoryStoreAllRels(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20)))
	ms.PutRelationship(types.NewRelationship(snowflake.ID(101), 7, snowflake.ID(10), snowflake.ID(20)))
	ms.PutRelationship(types.NewRelationship(snowflake.ID(102), 5, snowflake.ID(20), snowflake.ID(10)))

	got, err := ms.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllRelationships() = %d rels, want 3", len(got))
	}
}

func TestMemoryStoreAllRelsSorted(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(1), 1, nil)
	nB := types.NewNode(snowflake.ID(2), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	// Insert in reverse order.
	ms.PutRelationship(types.NewRelationship(snowflake.ID(300), 5, snowflake.ID(1), snowflake.ID(2)))
	ms.PutRelationship(types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(1), snowflake.ID(2)))
	ms.PutRelationship(types.NewRelationship(snowflake.ID(200), 5, snowflake.ID(1), snowflake.ID(2)))

	got, err := ms.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllRelationships() = %d rels, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := got[i-1].InternalID().SnowflakeID()
		curr := got[i].InternalID().SnowflakeID()
		if prev >= curr {
			t.Errorf("AllRelationships not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── MemoryStore: Bulk queries — GetNodesByIDs ──────────────────────────────

func TestMemoryStoreGetNodesByIDsEmpty(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	got, err := ms.GetNodesByIDs(nil)
	if err != nil {
		t.Fatalf("GetNodesByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetNodesByIDs(nil) = %v, want nil", got)
	}

	got, err = ms.GetNodesByIDs([]snowflake.ID{})
	if err != nil {
		t.Fatalf("GetNodesByIDs([]) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetNodesByIDs([]) = %v, want nil", got)
	}
}

func TestMemoryStoreGetNodesByIDs(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	ms.PutNode(types.NewNode(snowflake.ID(1), 10, nil))
	ms.PutNode(types.NewNode(snowflake.ID(2), 20, nil))
	ms.PutNode(types.NewNode(snowflake.ID(3), 10, nil))

	// Request 2 existing + 1 missing → should return 2, skip missing.
	got, err := ms.GetNodesByIDs([]snowflake.ID{snowflake.ID(1), snowflake.ID(999), snowflake.ID(3)})
	if err != nil {
		t.Fatalf("GetNodesByIDs() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetNodesByIDs() = %d nodes, want 2", len(got))
	}
}

func TestMemoryStoreGetNodesByIDsSorted(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	ms.PutNode(types.NewNode(snowflake.ID(30), 1, nil))
	ms.PutNode(types.NewNode(snowflake.ID(10), 1, nil))
	ms.PutNode(types.NewNode(snowflake.ID(20), 1, nil))

	// Request in reverse order — results must still be sorted ascending.
	got, err := ms.GetNodesByIDs([]snowflake.ID{snowflake.ID(30), snowflake.ID(10), snowflake.ID(20)})
	if err != nil {
		t.Fatalf("GetNodesByIDs() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetNodesByIDs() = %d nodes, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := got[i-1].InternalID().SnowflakeID()
		curr := got[i].InternalID().SnowflakeID()
		if prev >= curr {
			t.Errorf("GetNodesByIDs not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── MemoryStore: Bulk queries — GetRelationshipsByIDs ──────────────────────

func TestMemoryStoreGetRelsByIDsEmpty(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	got, err := ms.GetRelationshipsByIDs(nil)
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) = %v, want nil", got)
	}

	got, err = ms.GetRelationshipsByIDs([]snowflake.ID{})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs([]) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetRelationshipsByIDs([]) = %v, want nil", got)
	}
}

func TestMemoryStoreGetRelsByIDs(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(10), 1, nil)
	nB := types.NewNode(snowflake.ID(20), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(10), snowflake.ID(20)))
	ms.PutRelationship(types.NewRelationship(snowflake.ID(101), 7, snowflake.ID(10), snowflake.ID(20)))
	ms.PutRelationship(types.NewRelationship(snowflake.ID(102), 5, snowflake.ID(20), snowflake.ID(10)))

	// Request 2 existing + 1 missing → should return 2, skip missing.
	got, err := ms.GetRelationshipsByIDs([]snowflake.ID{snowflake.ID(100), snowflake.ID(999), snowflake.ID(102)})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetRelationshipsByIDs() = %d rels, want 2", len(got))
	}
}

func TestMemoryStoreGetRelsByIDsSorted(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	nA := types.NewNode(snowflake.ID(1), 1, nil)
	nB := types.NewNode(snowflake.ID(2), 1, nil)
	ms.PutNode(nA)
	ms.PutNode(nB)

	ms.PutRelationship(types.NewRelationship(snowflake.ID(300), 5, snowflake.ID(1), snowflake.ID(2)))
	ms.PutRelationship(types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(1), snowflake.ID(2)))
	ms.PutRelationship(types.NewRelationship(snowflake.ID(200), 5, snowflake.ID(1), snowflake.ID(2)))

	// Request in reverse order — results must still be sorted ascending.
	got, err := ms.GetRelationshipsByIDs([]snowflake.ID{snowflake.ID(300), snowflake.ID(100), snowflake.ID(200)})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetRelationshipsByIDs() = %d rels, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		prev := got[i-1].InternalID().SnowflakeID()
		curr := got[i].InternalID().SnowflakeID()
		if prev >= curr {
			t.Errorf("GetRelationshipsByIDs not sorted: result[%d].ID=%d >= result[%d].ID=%d", i-1, prev, i, curr)
		}
	}
}

// ─── MemoryStore: Batch operations ──────────────────────────────────────────

func TestMemoryStorePutNodesBatchEmpty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	if err := ms.PutNodesBatch(nil); err != nil {
		t.Fatalf("PutNodesBatch(nil) returned error: %v", err)
	}
	if err := ms.PutNodesBatch([]*types.Node{}); err != nil {
		t.Fatalf("PutNodesBatch([]) returned error: %v", err)
	}
}

func TestMemoryStorePutNodesBatch(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	nodes := []*types.Node{
		types.NewNode(snowflake.ID(1), 10, nil),
		types.NewNode(snowflake.ID(2), 10, []uint16{20}),
		types.NewNode(snowflake.ID(3), 20, nil),
	}

	if err := ms.PutNodesBatch(nodes); err != nil {
		t.Fatalf("PutNodesBatch returned error: %v", err)
	}

	count, _ := ms.NodeCount()
	if count != 3 {
		t.Fatalf("NodeCount = %d, want 3", count)
	}

	for _, n := range nodes {
		got, err := ms.GetNode(n.InternalID().SnowflakeID())
		if err != nil {
			t.Fatalf("GetNode(%d) returned error: %v", n.InternalID().SnowflakeID(), err)
		}
		if got.PrimaryLabelToken().Value() != n.PrimaryLabelToken().Value() {
			t.Errorf("node %d: primary label mismatch", n.InternalID().SnowflakeID())
		}
	}

	// Verify label index.
	byLabel, _ := ms.NodesByLabel(10, QueryOpts{})
	if len(byLabel) != 2 {
		t.Fatalf("NodesByLabel(10) = %d nodes, want 2", len(byLabel))
	}
}

func TestMemoryStorePutNodesBatchDuplicate(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Pre-existing node.
	existing := types.NewNode(snowflake.ID(1), 10, nil)
	if err := ms.PutNode(existing); err != nil {
		t.Fatal(err)
	}

	nodes := []*types.Node{
		types.NewNode(snowflake.ID(2), 10, nil),
		types.NewNode(snowflake.ID(1), 10, nil), // duplicate
	}

	err := ms.PutNodesBatch(nodes)
	if !errors.Is(err, ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}

	// Zero mutations.
	count, _ := ms.NodeCount()
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1 (zero mutations)", count)
	}
}

func TestMemoryStorePutNodesBatchInternalDuplicate(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	nodes := []*types.Node{
		types.NewNode(snowflake.ID(1), 10, nil),
		types.NewNode(snowflake.ID(1), 20, nil), // same ID
	}

	err := ms.PutNodesBatch(nodes)
	if err == nil {
		t.Fatal("expected error for internal duplicate, got nil")
	}

	count, _ := ms.NodeCount()
	if count != 0 {
		t.Fatalf("NodeCount = %d, want 0 (zero mutations)", count)
	}
}

func TestMemoryStorePutRelsBatchEmpty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	if err := ms.PutRelationshipsBatch(nil); err != nil {
		t.Fatalf("PutRelationshipsBatch(nil) returned error: %v", err)
	}
}

func TestMemoryStorePutRelsBatch(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Create endpoints.
	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	n3 := types.NewNode(snowflake.ID(3), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	_ = ms.PutNode(n3)

	rels := []*types.Relationship{
		types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(1), snowflake.ID(2)),
		types.NewRelationship(snowflake.ID(101), 5, snowflake.ID(2), snowflake.ID(3)),
		types.NewRelationship(snowflake.ID(102), 6, snowflake.ID(1), snowflake.ID(3)),
	}

	if err := ms.PutRelationshipsBatch(rels); err != nil {
		t.Fatalf("PutRelationshipsBatch returned error: %v", err)
	}

	count, _ := ms.RelationshipCount()
	if count != 3 {
		t.Fatalf("RelationshipCount = %d, want 3", count)
	}

	// Verify adjacency.
	outgoing, _ := ms.OutgoingRelationships(snowflake.ID(1), 0)
	if len(outgoing) != 2 {
		t.Fatalf("OutgoingRelationships(1, 0) = %d, want 2", len(outgoing))
	}
}

func TestMemoryStorePutRelsBatchDuplicate(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	// Pre-existing relationship.
	existing := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(1), snowflake.ID(2))
	_ = ms.PutRelationship(existing)

	rels := []*types.Relationship{
		types.NewRelationship(snowflake.ID(101), 5, snowflake.ID(1), snowflake.ID(2)),
		types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(1), snowflake.ID(2)), // duplicate
	}

	err := ms.PutRelationshipsBatch(rels)
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("expected ErrRelExists, got %v", err)
	}

	// Zero mutations.
	count, _ := ms.RelationshipCount()
	if count != 1 {
		t.Fatalf("RelationshipCount = %d, want 1 (zero mutations)", count)
	}
}

func TestMemoryStoreDeleteNodesBatchEmpty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	if err := ms.DeleteNodesBatch(nil); err != nil {
		t.Fatalf("DeleteNodesBatch(nil) returned error: %v", err)
	}
}

func TestMemoryStoreDeleteNodesBatch(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	n3 := types.NewNode(snowflake.ID(3), 20, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	_ = ms.PutNode(n3)

	if err := ms.DeleteNodesBatch([]snowflake.ID{snowflake.ID(1), snowflake.ID(3)}); err != nil {
		t.Fatalf("DeleteNodesBatch returned error: %v", err)
	}

	count, _ := ms.NodeCount()
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1", count)
	}

	// Verify label index cleaned up.
	byLabel, _ := ms.NodesByLabel(20, QueryOpts{})
	if len(byLabel) != 0 {
		t.Fatalf("NodesByLabel(20) = %d nodes, want 0 after delete", len(byLabel))
	}
}

func TestMemoryStoreDeleteNodesBatchMissing(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	err := ms.DeleteNodesBatch([]snowflake.ID{snowflake.ID(1), snowflake.ID(999)})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	// Zero mutations.
	count, _ := ms.NodeCount()
	if count != 2 {
		t.Fatalf("NodeCount = %d, want 2 (zero mutations)", count)
	}
}

func TestMemoryStoreDeleteRelsBatchEmpty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	if err := ms.DeleteRelationshipsBatch(nil); err != nil {
		t.Fatalf("DeleteRelationshipsBatch(nil) returned error: %v", err)
	}
}

func TestMemoryStoreDeleteRelsBatch(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	r1 := types.NewRelationship(snowflake.ID(100), 5, snowflake.ID(1), snowflake.ID(2))
	r2 := types.NewRelationship(snowflake.ID(101), 5, snowflake.ID(2), snowflake.ID(1))
	_ = ms.PutRelationship(r1)
	_ = ms.PutRelationship(r2)

	// Add version history so we can verify cleanup.
	_ = ms.PutRelVersion(snowflake.ID(100), 0, r1)

	if err := ms.DeleteRelationshipsBatch([]snowflake.ID{snowflake.ID(100), snowflake.ID(101)}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch returned error: %v", err)
	}

	count, _ := ms.RelationshipCount()
	if count != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", count)
	}

	// History is preserved after delete — temporal queries need it.
	history, _ := ms.GetRelHistory(snowflake.ID(100))
	if len(history) != 1 {
		t.Fatalf("GetRelHistory(100) = %d entries, want 1 (preserved after delete)", len(history))
	}
}

// ─── MemoryStore: ReplaceNodeWithHistory ────────────────────────────────────

func TestMemoryStoreReplaceNodeWithHistory(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(snowflake.ID(1), 10, nil)
	_ = n.SetProperty("name", "Alice")
	n.SetVersion(0)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	// Prepare updated state.
	updated, _ := ms.GetNode(snowflake.ID(1))
	prevState := updated.DeepCopy()
	prevVersion := updated.Version()
	_ = updated.SetProperty("name", "Bob")
	updated.SetVersion(1)

	if err := ms.ReplaceNodeWithHistory(updated, prevVersion, prevState); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}

	// Verify current state updated.
	current, _ := ms.GetNode(snowflake.ID(1))
	props := current.PropertiesMap()
	if props["name"] != "Bob" {
		t.Fatalf("got name=%v, want Bob", props["name"])
	}
	if current.Version() != 1 {
		t.Fatalf("version = %d, want 1", current.Version())
	}

	// Verify history entry exists.
	hist, _ := ms.GetNodeVersion(snowflake.ID(1), 0)
	histProps := hist.PropertiesMap()
	if histProps["name"] != "Alice" {
		t.Fatalf("history name=%v, want Alice", histProps["name"])
	}
}

func TestMemoryStoreReplaceNodeWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(snowflake.ID(999), 10, nil)
	err := ms.ReplaceNodeWithHistory(n, 0, n)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("want ErrNodeNotFound, got %v", err)
	}
}

// ─── MemoryStore: ReplaceRelWithHistory ─────────────────────────────────────

func TestMemoryStoreReplaceRelWithHistory(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Create endpoints.
	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	r := types.NewRelationship(snowflake.ID(100), 1, snowflake.ID(1), snowflake.ID(2))
	_ = r.SetProperty("weight", int64(5))
	r.SetVersion(0)
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Prepare updated state.
	updated, _ := ms.GetRelationship(snowflake.ID(100))
	prevState := updated.DeepCopy()
	prevVersion := updated.Version()
	_ = updated.SetProperty("weight", int64(10))
	updated.SetVersion(1)

	if err := ms.ReplaceRelWithHistory(updated, prevVersion, prevState); err != nil {
		t.Fatalf("ReplaceRelWithHistory: %v", err)
	}

	// Verify current state updated.
	current, _ := ms.GetRelationship(snowflake.ID(100))
	props := current.PropertiesMap()
	if props["weight"] != int64(10) {
		t.Fatalf("got weight=%v, want 10", props["weight"])
	}

	// Verify history entry exists.
	hist, _ := ms.GetRelVersion(snowflake.ID(100), 0)
	histProps := hist.PropertiesMap()
	if histProps["weight"] != int64(5) {
		t.Fatalf("history weight=%v, want 5", histProps["weight"])
	}
}

func TestMemoryStoreReplaceRelWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	r := types.NewRelationship(snowflake.ID(999), 1, snowflake.ID(1), snowflake.ID(2))
	err := ms.ReplaceRelWithHistory(r, 0, r)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("want ErrRelNotFound, got %v", err)
	}
}

// ─── MemoryStore: AllNodeIDs / AllRelIDs ────────────────────────────────────

func TestMemoryStoreAllNodeIDs_Empty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	ids, err := ms.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if ids != nil {
		t.Fatalf("expected nil, got %v", ids)
	}
}

func TestMemoryStoreAllNodeIDs_ReturnsSorted(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Insert 5 nodes.
	for _, id := range []snowflake.ID{50, 30, 10, 40, 20} {
		n := types.NewNode(id, 1, nil)
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := ms.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 {
		t.Fatalf("got %d IDs, want 5", len(ids))
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("IDs not sorted at index %d: %d <= %d", i, ids[i], ids[i-1])
		}
	}
}

func TestMemoryStoreAllNodeIDs_Pagination(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	for _, id := range []snowflake.ID{10, 20, 30, 40, 50} {
		n := types.NewNode(id, 1, nil)
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	// Page 1: Limit=2.
	ids, _ := ms.AllNodeIDs(QueryOpts{Limit: 2})
	if len(ids) != 2 {
		t.Fatalf("page1 len=%d, want 2", len(ids))
	}

	// Page 2: After last ID from page 1.
	ids2, _ := ms.AllNodeIDs(QueryOpts{Limit: 2, After: ids[1]})
	if len(ids2) != 2 {
		t.Fatalf("page2 len=%d, want 2", len(ids2))
	}
	if ids2[0] <= ids[1] {
		t.Fatal("page2 first ID should be > page1 last ID")
	}

	// Page 3: remaining.
	ids3, _ := ms.AllNodeIDs(QueryOpts{Limit: 2, After: ids2[1]})
	if len(ids3) != 1 {
		t.Fatalf("page3 len=%d, want 1", len(ids3))
	}
}

func TestMemoryStoreAllRelIDs_Empty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	ids, err := ms.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if ids != nil {
		t.Fatalf("expected nil, got %v", ids)
	}
}

func TestMemoryStoreAllRelIDs_ReturnsSorted(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Need nodes for rel endpoints.
	n1 := types.NewNode(snowflake.ID(1), 1, nil)
	n2 := types.NewNode(snowflake.ID(2), 1, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	for _, id := range []snowflake.ID{50, 30, 10, 40, 20} {
		r := types.NewRelationship(id, 1, snowflake.ID(1), snowflake.ID(2))
		if err := ms.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := ms.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 {
		t.Fatalf("got %d IDs, want 5", len(ids))
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("IDs not sorted at index %d", i)
		}
	}
}

func TestMemoryStoreAllRelIDs_Pagination(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n1 := types.NewNode(snowflake.ID(1), 1, nil)
	n2 := types.NewNode(snowflake.ID(2), 1, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)

	for _, id := range []snowflake.ID{10, 20, 30, 40, 50} {
		r := types.NewRelationship(id, 1, snowflake.ID(1), snowflake.ID(2))
		_ = ms.PutRelationship(r)
	}

	// Page 1: Limit=2.
	ids, _ := ms.AllRelIDs(QueryOpts{Limit: 2})
	if len(ids) != 2 {
		t.Fatalf("page1 len=%d, want 2", len(ids))
	}

	// Page 2.
	ids2, _ := ms.AllRelIDs(QueryOpts{Limit: 2, After: ids[1]})
	if len(ids2) != 2 {
		t.Fatalf("page2 len=%d, want 2", len(ids2))
	}
}

// ─── Property index purge tests ─────────────────────────────────────────────

func TestPurgeNodeFromAllPropertyIndexes_Empty(t *testing.T) {
	t.Parallel()
	indexes := make(map[propertyIndexKey]*propertyIndex)
	// Should not panic.
	purgeNodeFromAllPropertyIndexes(indexes, snowflake.ID(42))
}

func TestPurgeNodeFromAllPropertyIndexes_RemovesFromAll(t *testing.T) {
	t.Parallel()
	indexes := make(map[propertyIndexKey]*propertyIndex)

	idx1 := newPropertyIndex()
	idx1.add(snowflake.ID(1), "Alice")
	idx1.add(snowflake.ID(2), "Bob")
	indexes[propertyIndexKey{labelToken: 1, propertyKey: "name"}] = idx1

	idx2 := newPropertyIndex()
	idx2.add(snowflake.ID(1), 30)
	idx2.add(snowflake.ID(3), 25)
	indexes[propertyIndexKey{labelToken: 1, propertyKey: "age"}] = idx2

	purgeNodeFromAllPropertyIndexes(indexes, snowflake.ID(1))

	// ID 1 should be gone from both indexes.
	if s := idx1.lookup("Alice"); s != nil {
		if _, ok := s[snowflake.ID(1)]; ok {
			t.Error("ID 1 should be removed from name index")
		}
	}
	if s := idx2.lookup(30); s != nil {
		if _, ok := s[snowflake.ID(1)]; ok {
			t.Error("ID 1 should be removed from age index")
		}
	}

	// Other IDs should remain.
	if s := idx1.lookup("Bob"); len(s) != 1 {
		t.Errorf("Bob should still have 1 entry, got %d", len(s))
	}
	if s := idx2.lookup(25); len(s) != 1 {
		t.Errorf("age 25 should still have 1 entry, got %d", len(s))
	}
}

func TestPurgeNodeFromAllPropertyIndexes_OtherNodesUnaffected(t *testing.T) {
	t.Parallel()
	indexes := make(map[propertyIndexKey]*propertyIndex)

	idx := newPropertyIndex()
	idx.add(snowflake.ID(10), "val")
	idx.add(snowflake.ID(20), "val")
	idx.add(snowflake.ID(30), "other")
	indexes[propertyIndexKey{labelToken: 1, propertyKey: "key"}] = idx

	// Purge ID 10.
	purgeNodeFromAllPropertyIndexes(indexes, snowflake.ID(10))

	// ID 20 and 30 should be unaffected.
	s := idx.lookup("val")
	if _, ok := s[snowflake.ID(20)]; !ok {
		t.Error("ID 20 should still be in index")
	}
	s2 := idx.lookup("other")
	if _, ok := s2[snowflake.ID(30)]; !ok {
		t.Error("ID 30 should still be in index")
	}

	// Empty value sets should be cleaned up.
	if _, ok := s[snowflake.ID(10)]; ok {
		t.Error("ID 10 should be purged")
	}
}
