package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ─── Batch operations ─────────────────────────────────────────────────────────

func TestBadgerStorePutNodesBatchEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.PutNodesBatch(nil); err != nil {
		t.Fatalf("PutNodesBatch(nil) returned error: %v", err)
	}
	if err := bs.PutNodesBatch([]*types.Node{}); err != nil {
		t.Fatalf("PutNodesBatch([]) returned error: %v", err)
	}
}

func TestBadgerStorePutNodesBatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(2)), 10, []uint16{20}),
		types.NewNode(types.NodeID(snowflake.ID(3)), 20, nil),
	}

	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatalf("PutNodesBatch returned error: %v", err)
	}

	count, _ := bs.NodeCount()
	if count != 3 {
		t.Fatalf("NodeCount = %d, want 3", count)
	}

	for _, n := range nodes {
		got, err := bs.GetNode(n.ID())
		if err != nil {
			t.Fatalf("GetNode(%d) returned error: %v", n.ID(), err)
		}
		if got.PrimaryLabelToken().Value() != n.PrimaryLabelToken().Value() {
			t.Errorf("node %d: primary label mismatch", n.ID())
		}
	}

	// Verify label index.
	byLabel, _ := bs.NodesByLabel(10, QueryOpts{})
	if len(byLabel) != 2 {
		t.Fatalf("NodesByLabel(10) = %d nodes, want 2", len(byLabel))
	}
}

func TestBadgerStorePutNodesBatchDuplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil), // duplicate
	}

	err := bs.PutNodesBatch(nodes)
	if !errors.Is(err, ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}

	count, _ := bs.NodeCount()
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1 (zero mutations)", count)
	}
}

func TestBadgerStorePutNodesBatchInternalDuplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil),
		types.NewNode(types.NodeID(snowflake.ID(1)), 20, nil),
	}

	err := bs.PutNodesBatch(nodes)
	if err == nil {
		t.Fatal("expected error for internal duplicate, got nil")
	}

	count, _ := bs.NodeCount()
	if count != 0 {
		t.Fatalf("NodeCount = %d, want 0 (zero mutations)", count)
	}
}

func TestBadgerStorePutRelsBatchEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.PutRelationshipsBatch(nil); err != nil {
		t.Fatalf("PutRelationshipsBatch(nil) returned error: %v", err)
	}
}

func TestBadgerStorePutRelsBatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)

	rels := []*types.Relationship{
		types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))),
		types.NewRelationship(types.RelID(snowflake.ID(101)), 5, types.NodeID(snowflake.ID(2)), types.NodeID(snowflake.ID(3))),
		types.NewRelationship(types.RelID(snowflake.ID(102)), 6, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(3))),
	}

	if err := bs.PutRelationshipsBatch(rels); err != nil {
		t.Fatalf("PutRelationshipsBatch returned error: %v", err)
	}

	count, _ := bs.RelationshipCount()
	if count != 3 {
		t.Fatalf("RelationshipCount = %d, want 3", count)
	}

	outgoing, _ := bs.OutgoingRelationships(types.NodeID(1), 0)
	if len(outgoing) != 2 {
		t.Fatalf("OutgoingRelationships(1, 0) = %d, want 2", len(outgoing))
	}
}

func TestBadgerStorePutRelsBatchDuplicate(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	putTestRel(t, bs, 100, 5, 1, 2)

	rels := []*types.Relationship{
		types.NewRelationship(types.RelID(snowflake.ID(101)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))),
		types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2))), // duplicate
	}

	err := bs.PutRelationshipsBatch(rels)
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("expected ErrRelExists, got %v", err)
	}

	count, _ := bs.RelationshipCount()
	if count != 1 {
		t.Fatalf("RelationshipCount = %d, want 1 (zero mutations)", count)
	}
}

func TestBadgerStoreDeleteNodesBatchEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.DeleteNodesBatch(nil); err != nil {
		t.Fatalf("DeleteNodesBatch(nil) returned error: %v", err)
	}
}

func TestBadgerStoreDeleteNodesBatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 20, nil)

	if err := bs.DeleteNodesBatch([]types.NodeID{types.NodeID(1), types.NodeID(3)}); err != nil {
		t.Fatalf("DeleteNodesBatch returned error: %v", err)
	}

	count, _ := bs.NodeCount()
	if count != 1 {
		t.Fatalf("NodeCount = %d, want 1", count)
	}

	byLabel, _ := bs.NodesByLabel(20, QueryOpts{})
	if len(byLabel) != 0 {
		t.Fatalf("NodesByLabel(20) = %d nodes, want 0 after delete", len(byLabel))
	}
}

func TestBadgerStoreDeleteNodesBatchMissing(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	err := bs.DeleteNodesBatch([]types.NodeID{types.NodeID(1), types.NodeID(999)})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	count, _ := bs.NodeCount()
	if count != 2 {
		t.Fatalf("NodeCount = %d, want 2 (zero mutations)", count)
	}
}

func TestBadgerStoreDeleteRelsBatchEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if err := bs.DeleteRelationshipsBatch(nil); err != nil {
		t.Fatalf("DeleteRelationshipsBatch(nil) returned error: %v", err)
	}
}

func TestBadgerStoreDeleteRelsBatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	putTestRel(t, bs, 100, 5, 1, 2)
	putTestRel(t, bs, 101, 5, 2, 1)

	if err := bs.DeleteRelationshipsBatch([]types.RelID{types.RelID(100), types.RelID(101)}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch returned error: %v", err)
	}

	count, _ := bs.RelationshipCount()
	if count != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", count)
	}
}
