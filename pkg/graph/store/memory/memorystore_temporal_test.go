package memory

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// putTestNodeWithTemporal creates and stores a node with explicit temporal metadata.
// labelToken is the primary label token. ValidFrom/ValidTo are explicit.
func putTestNodeWithTemporal(t *testing.T, ms *Store, id snowflake.ID, labelToken uint16, validFrom, validTo types.Instant) {
	t.Helper()
	n := types.NewNode(types.NodeID(id), labelToken, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
}

// putTestRelWithTemporal creates and stores a relationship with explicit temporal metadata.
func putTestRelWithTemporal(t *testing.T, ms *Store, id snowflake.ID, typeToken uint16, startID, endID snowflake.ID, validFrom, validTo types.Instant) {
	t.Helper()
	r := types.NewRelationship(types.RelID(id), typeToken, types.NodeID(startID), types.NodeID(endID))
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo})
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
}

func TestMemoryStore_NodesByLabel_ValidAt(t *testing.T) {
	t.Parallel()
	ms := New()

	// 3 nodes with label 1: valid [100,300), valid [200,400), valid [500,-)
	putTestNodeWithTemporal(t, ms, snowflake.ID(10), 1, 100, 300)
	putTestNodeWithTemporal(t, ms, snowflake.ID(20), 1, 200, 400)
	putTestNodeWithTemporal(t, ms, snowflake.ID(30), 1, 500, 0)

	// At t=250: node 10 (valid) and node 20 (valid). Node 30 not yet valid.
	nodes, err := ms.NodesByLabel(1, QueryOpts{ValidAt: 250})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	if nodes[0].ID() != 10 || nodes[1].ID() != 20 {
		t.Errorf("unexpected node IDs: %d, %d", nodes[0].ID(), nodes[1].ID())
	}
}

func TestMemoryStore_NodesByLabel_ValidAt_Pagination(t *testing.T) {
	t.Parallel()
	ms := New()

	// 5 nodes with label 1, all valid at t=500
	for i := snowflake.ID(10); i <= 50; i += 10 {
		putTestNodeWithTemporal(t, ms, i, 1, 100, 0)
	}
	// 1 node with label 1 that expires before t=500
	putTestNodeWithTemporal(t, ms, snowflake.ID(60), 1, 100, 300)

	// ValidAt=500, Limit=2 → should return first 2 of the 5 valid nodes.
	nodes, err := ms.NodesByLabel(1, QueryOpts{ValidAt: 500, Limit: 2})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}

	// Page 2: After the last node of page 1.
	nodes2, err := ms.NodesByLabel(1, QueryOpts{ValidAt: 500, Limit: 2, After: types.EntityID(nodes[1].ID())})
	if err != nil {
		t.Fatalf("NodesByLabel page 2: %v", err)
	}
	if len(nodes2) != 2 {
		t.Fatalf("page 2: got %d nodes, want 2", len(nodes2))
	}
}

func TestMemoryStore_AllNodes_ValidAt(t *testing.T) {
	t.Parallel()
	ms := New()

	putTestNodeWithTemporal(t, ms, snowflake.ID(10), 1, 100, 200)
	putTestNodeWithTemporal(t, ms, snowflake.ID(20), 2, 100, 0)

	// At t=150: both valid.
	nodes, err := ms.AllNodes(QueryOpts{ValidAt: 150})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes at t=150, want 2", len(nodes))
	}

	// At t=250: only node 20 (node 10 expired at 200).
	nodes, err = ms.AllNodes(QueryOpts{ValidAt: 250})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes at t=250, want 1", len(nodes))
	}
	if nodes[0].ID() != 20 {
		t.Errorf("expected node 20, got %d", nodes[0].ID())
	}
}

func TestMemoryStore_AllNodeIDs_ValidAt(t *testing.T) {
	t.Parallel()
	ms := New()

	putTestNodeWithTemporal(t, ms, snowflake.ID(10), 1, 100, 200)
	putTestNodeWithTemporal(t, ms, snowflake.ID(20), 2, 100, 0)

	ids, err := ms.AllNodeIDs(QueryOpts{ValidAt: 250})
	if err != nil {
		t.Fatalf("AllNodeIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != 20 {
		t.Fatalf("AllNodeIDs ValidAt=250 = %v, want [20]", ids)
	}
}

func TestMemoryStore_AllNodeReads_ValidAtPaginationSkipsExpiredRows(t *testing.T) {
	t.Parallel()
	ms := New()

	putTestNodeWithTemporal(t, ms, snowflake.ID(10), 1, 100, 200)
	putTestNodeWithTemporal(t, ms, snowflake.ID(20), 1, 100, 0)
	putTestNodeWithTemporal(t, ms, snowflake.ID(30), 1, 100, 300)
	putTestNodeWithTemporal(t, ms, snowflake.ID(40), 1, 100, 0)
	putTestNodeWithTemporal(t, ms, snowflake.ID(50), 1, 100, 0)

	ids, err := ms.AllNodeIDs(QueryOpts{ValidAt: 500, Limit: 2})
	if err != nil {
		t.Fatalf("AllNodeIDs page 1: %v", err)
	}
	if len(ids) != 2 || ids[0] != 20 || ids[1] != 40 {
		t.Fatalf("AllNodeIDs page 1 = %v, want [20 40]", ids)
	}

	ids, err = ms.AllNodeIDs(QueryOpts{ValidAt: 500, Limit: 2, After: types.EntityID(ids[1])})
	if err != nil {
		t.Fatalf("AllNodeIDs page 2: %v", err)
	}
	if len(ids) != 1 || ids[0] != 50 {
		t.Fatalf("AllNodeIDs page 2 = %v, want [50]", ids)
	}

	nodes, err := ms.AllNodes(QueryOpts{ValidAt: 500, Limit: 2})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(nodes) != 2 || nodes[0].ID() != 20 || nodes[1].ID() != 40 {
		t.Fatalf("AllNodes = %v, want IDs [20 40]", nodes)
	}
}

func TestMemoryStore_RelationshipsByType_ValidAt(t *testing.T) {
	t.Parallel()
	ms := New()

	// Create endpoints.
	putTestNodeWithTemporal(t, ms, snowflake.ID(1), 1, 100, 0)
	putTestNodeWithTemporal(t, ms, snowflake.ID(2), 1, 100, 0)

	// 2 rels of type 1: valid [100,300), valid [100,-)
	putTestRelWithTemporal(t, ms, snowflake.ID(100), 1, 1, 2, 100, 300)
	putTestRelWithTemporal(t, ms, snowflake.ID(200), 1, 1, 2, 100, 0)

	// At t=350: only rel 200 (rel 100 expired at 300).
	rels, err := ms.RelationshipsByType(1, QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("got %d rels, want 1", len(rels))
	}
	if rels[0].ID() != 200 {
		t.Errorf("expected rel 200, got %d", rels[0].ID())
	}
}

func TestMemoryStore_NodesByLabelAndProperty_ValidAt(t *testing.T) {
	t.Parallel()
	ms := New()

	// 2 nodes with label 1 and property "color"="red".
	n1 := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	n1.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 300})
	n1.SetProperties(mustPropertySlice(t, map[string]any{"color": "red"}))
	if err := ms.PutNode(n1); err != nil {
		t.Fatal(err)
	}

	n2 := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	n2.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 0})
	n2.SetProperties(mustPropertySlice(t, map[string]any{"color": "red"}))
	if err := ms.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Fallback path (no property index).
	nodes, err := ms.NodesByLabelAndProperty(1, "color", "red", QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("fallback: got %d nodes, want 1", len(nodes))
	}
	if nodes[0].ID() != 20 {
		t.Errorf("fallback: expected node 20, got %d", nodes[0].ID())
	}

	// Create property index and test indexed path.
	if err := ms.CreatePropertyIndex(1, "color"); err != nil {
		t.Fatal(err)
	}

	nodes, err = ms.NodesByLabelAndProperty(1, "color", "red", QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty indexed: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("indexed: got %d nodes, want 1", len(nodes))
	}
	if nodes[0].ID() != 20 {
		t.Errorf("indexed: expected node 20, got %d", nodes[0].ID())
	}
}

func TestMemoryStore_AllRelationships_ValidAt(t *testing.T) {
	t.Parallel()
	ms := New()

	putTestNodeWithTemporal(t, ms, snowflake.ID(1), 1, 100, 0)
	putTestNodeWithTemporal(t, ms, snowflake.ID(2), 1, 100, 0)

	putTestRelWithTemporal(t, ms, snowflake.ID(100), 1, 1, 2, 100, 200)
	putTestRelWithTemporal(t, ms, snowflake.ID(200), 1, 1, 2, 300, 0)

	// At t=150: only rel 100.
	rels, err := ms.AllRelationships(QueryOpts{ValidAt: 150})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 1 || rels[0].ID() != 100 {
		t.Fatalf("t=150: got %d rels, want 1 (rel 100)", len(rels))
	}

	// At t=350: only rel 200.
	rels, err = ms.AllRelationships(QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 1 || rels[0].ID() != 200 {
		t.Fatalf("t=350: got %d rels, want 1 (rel 200)", len(rels))
	}
}

func TestMemoryStore_AllRelIDs_ValidAt(t *testing.T) {
	t.Parallel()
	ms := New()

	putTestNodeWithTemporal(t, ms, snowflake.ID(1), 1, 100, 0)
	putTestNodeWithTemporal(t, ms, snowflake.ID(2), 1, 100, 0)

	putTestRelWithTemporal(t, ms, snowflake.ID(100), 1, 1, 2, 100, 200)
	putTestRelWithTemporal(t, ms, snowflake.ID(200), 1, 1, 2, 300, 0)

	ids, err := ms.AllRelIDs(QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatalf("AllRelIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != 200 {
		t.Fatalf("AllRelIDs ValidAt=350 = %v, want [200]", ids)
	}
}

func TestMemoryStore_AllRelationshipReads_ValidAtPaginationSkipsExpiredRows(t *testing.T) {
	t.Parallel()
	ms := New()

	putTestNodeWithTemporal(t, ms, snowflake.ID(1), 1, 100, 0)
	putTestNodeWithTemporal(t, ms, snowflake.ID(2), 1, 100, 0)
	putTestRelWithTemporal(t, ms, snowflake.ID(100), 1, 1, 2, 100, 200)
	putTestRelWithTemporal(t, ms, snowflake.ID(200), 1, 1, 2, 100, 0)
	putTestRelWithTemporal(t, ms, snowflake.ID(300), 1, 1, 2, 100, 300)
	putTestRelWithTemporal(t, ms, snowflake.ID(400), 1, 1, 2, 100, 0)
	putTestRelWithTemporal(t, ms, snowflake.ID(500), 1, 1, 2, 100, 0)

	ids, err := ms.AllRelIDs(QueryOpts{ValidAt: 500, Limit: 2})
	if err != nil {
		t.Fatalf("AllRelIDs page 1: %v", err)
	}
	if len(ids) != 2 || ids[0] != 200 || ids[1] != 400 {
		t.Fatalf("AllRelIDs page 1 = %v, want [200 400]", ids)
	}

	ids, err = ms.AllRelIDs(QueryOpts{ValidAt: 500, Limit: 2, After: types.EntityID(ids[1])})
	if err != nil {
		t.Fatalf("AllRelIDs page 2: %v", err)
	}
	if len(ids) != 1 || ids[0] != 500 {
		t.Fatalf("AllRelIDs page 2 = %v, want [500]", ids)
	}

	rels, err := ms.AllRelationships(QueryOpts{ValidAt: 500, Limit: 2})
	if err != nil {
		t.Fatalf("AllRelationships: %v", err)
	}
	if len(rels) != 2 || rels[0].ID() != 200 || rels[1].ID() != 400 {
		t.Fatalf("AllRelationships = %v, want IDs [200 400]", rels)
	}
}

func TestMemoryStore_NodesByLabel_IntervalFilter(t *testing.T) {
	t.Parallel()
	ms := New()

	// node [100,300), node [400,600), node [700,-)
	putTestNodeWithTemporal(t, ms, snowflake.ID(10), 1, 100, 300)
	putTestNodeWithTemporal(t, ms, snowflake.ID(20), 1, 400, 600)
	putTestNodeWithTemporal(t, ms, snowflake.ID(30), 1, 700, 0)

	// Query interval [250,500) — overlaps with node 10 [100,300) and node 20 [400,600).
	nodes, err := ms.NodesByLabel(1, QueryOpts{ValidStart: 250, ValidEnd: 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("interval [250,500): got %d nodes, want 2", len(nodes))
	}
}

func TestMemoryStore_NoFilter_BackwardCompatible(t *testing.T) {
	t.Parallel()
	ms := New()

	putTestNodeWithTemporal(t, ms, snowflake.ID(10), 1, 100, 300)
	putTestNodeWithTemporal(t, ms, snowflake.ID(20), 1, 400, 600)

	// No filter — should return all nodes.
	nodes, err := ms.NodesByLabel(1, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("no filter: got %d nodes, want 2", len(nodes))
	}
}
