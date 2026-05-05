package graph

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func putBadgerNodeTemporal(t *testing.T, bs *BadgerStore, id snowflake.ID, labelToken uint16, validFrom, validTo types.Instant) {
	t.Helper()
	n := types.NewNode(types.NodeID(id), labelToken, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
}

func putBadgerRelTemporal(t *testing.T, bs *BadgerStore, id snowflake.ID, typeToken uint16, startID, endID snowflake.ID, validFrom, validTo types.Instant) {
	t.Helper()
	r := types.NewRelationship(types.RelID(id), typeToken, types.NodeID(startID), types.NodeID(endID))
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo})
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
}

func TestBadgerStore_NodesByLabel_ValidAt(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putBadgerNodeTemporal(t, bs, snowflake.ID(10), 1, 100, 300)
	putBadgerNodeTemporal(t, bs, snowflake.ID(20), 1, 200, 400)
	putBadgerNodeTemporal(t, bs, snowflake.ID(30), 1, 500, 0)

	nodes, err := bs.NodesByLabel(1, QueryOpts{ValidAt: 250})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
}

func TestBadgerStore_NodesByLabel_ValidAt_Pagination(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	for i := snowflake.ID(10); i <= 50; i += 10 {
		putBadgerNodeTemporal(t, bs, i, 1, 100, 0)
	}
	putBadgerNodeTemporal(t, bs, snowflake.ID(60), 1, 100, 300)

	nodes, err := bs.NodesByLabel(1, QueryOpts{ValidAt: 500, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("page 1: got %d nodes, want 2", len(nodes))
	}

	nodes2, err := bs.NodesByLabel(1, QueryOpts{ValidAt: 500, Limit: 2, After: types.EntityID(nodes[1].ID())})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes2) != 2 {
		t.Fatalf("page 2: got %d nodes, want 2", len(nodes2))
	}
}

func TestBadgerStore_AllNodes_ValidAt(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putBadgerNodeTemporal(t, bs, snowflake.ID(10), 1, 100, 200)
	putBadgerNodeTemporal(t, bs, snowflake.ID(20), 2, 100, 0)

	nodes, err := bs.AllNodes(QueryOpts{ValidAt: 250})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes at t=250, want 1", len(nodes))
	}
	if nodes[0].ID() != 20 {
		t.Errorf("expected node 20, got %d", nodes[0].ID())
	}
}

func TestBadgerStore_RelationshipsByType_ValidAt(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putBadgerNodeTemporal(t, bs, snowflake.ID(1), 1, 100, 0)
	putBadgerNodeTemporal(t, bs, snowflake.ID(2), 1, 100, 0)

	putBadgerRelTemporal(t, bs, snowflake.ID(100), 1, 1, 2, 100, 300)
	putBadgerRelTemporal(t, bs, snowflake.ID(200), 1, 1, 2, 100, 0)

	rels, err := bs.RelationshipsByType(1, QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 1 {
		t.Fatalf("got %d rels, want 1", len(rels))
	}
	if rels[0].ID() != 200 {
		t.Errorf("expected rel 200, got %d", rels[0].ID())
	}
}

func TestBadgerStore_AllRelationships_ValidAt(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putBadgerNodeTemporal(t, bs, snowflake.ID(1), 1, 100, 0)
	putBadgerNodeTemporal(t, bs, snowflake.ID(2), 1, 100, 0)

	putBadgerRelTemporal(t, bs, snowflake.ID(100), 1, 1, 2, 100, 200)
	putBadgerRelTemporal(t, bs, snowflake.ID(200), 1, 1, 2, 300, 0)

	rels, err := bs.AllRelationships(QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 1 || rels[0].ID() != 200 {
		t.Fatalf("got %d rels, want 1 (rel 200)", len(rels))
	}
}

func TestBadgerStore_NodesByLabelAndProperty_ValidAt(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n1 := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	n1.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 300})
	n1.SetProperties(mustPropertySlice(t, map[string]any{"color": "red"}))
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}

	n2 := types.NewNode(types.NodeID(snowflake.ID(20)), 1, nil)
	n2.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 0})
	n2.SetProperties(mustPropertySlice(t, map[string]any{"color": "red"}))
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Fallback path (no property index).
	nodes, err := bs.NodesByLabelAndProperty(1, "color", "red", QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].ID() != 20 {
		t.Fatalf("fallback: got %d nodes, want 1 (node 20)", len(nodes))
	}

	// Create property index and test indexed path.
	if err := bs.CreatePropertyIndex(1, "color"); err != nil {
		t.Fatal(err)
	}

	nodes, err = bs.NodesByLabelAndProperty(1, "color", "red", QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].ID() != 20 {
		t.Fatalf("indexed: got %d nodes, want 1 (node 20)", len(nodes))
	}
}

func TestBadgerStore_NodesByLabel_IntervalFilter(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putBadgerNodeTemporal(t, bs, snowflake.ID(10), 1, 100, 300)
	putBadgerNodeTemporal(t, bs, snowflake.ID(20), 1, 400, 600)
	putBadgerNodeTemporal(t, bs, snowflake.ID(30), 1, 700, 0)

	nodes, err := bs.NodesByLabel(1, QueryOpts{ValidStart: 250, ValidEnd: 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("interval [250,500): got %d nodes, want 2", len(nodes))
	}
}
