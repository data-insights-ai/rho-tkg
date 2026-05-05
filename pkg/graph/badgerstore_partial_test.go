package graph

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// newTestBadgerStoreInMemory creates an in-memory BadgerStore for testing.
func newTestBadgerStoreInMemory(t *testing.T) *BadgerStore {
	t.Helper()
	bs, err := NewBadgerStore(BadgerStoreConfig{
		InMemory:      true,
		FlushInterval: 1<<63 - 1, // effectively disable periodic flush
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

func TestPutRelEntityAndOut_CreatesEntityButNotInIdx(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	// Create two nodes.
	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Create a relationship using partial write.
	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, types.NodeID(n1.ID()), types.NodeID(n2.ID()))
	if err := bs.putRelEntityAndOut(r); err != nil {
		t.Fatal(err)
	}

	// Entity should exist.
	if !bs.hasRelID(relID) {
		t.Error("hasRelID should be true after putRelEntityAndOut")
	}

	// GetRelationship should work.
	got, err := bs.GetRelationship(types.RelID(relID))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.ID().SnowflakeID() != relID {
		t.Error("relationship ID mismatch")
	}

	// outIdx should contain the rel.
	outIDs := bs.outgoingRelIDs(n1.ID().SnowflakeID())
	if len(outIDs) != 1 || outIDs[0] != relID {
		t.Errorf("outgoingRelIDs = %v, want [%d]", outIDs, relID)
	}

	// inIdx should NOT contain the rel (partial write skips in/).
	inIDs := bs.incomingRelIDs(n2.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("incomingRelIDs should be empty after putRelEntityAndOut, got %v", inIDs)
	}
}

func TestPutRelIncoming_CreatesInIdxOnly(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	// Create one node (the endpoint).
	gen := newTestGen(t, 0)
	endNode := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(endNode); err != nil {
		t.Fatal(err)
	}

	startID := snowflake.ID(999999)
	relID := snowflake.ID(888888)
	var relType uint16 = 1

	if err := bs.putRelIncoming(endNode.ID().SnowflakeID(), startID, relType, relID); err != nil {
		t.Fatal(err)
	}

	// inIdx should contain the rel.
	inIDs := bs.incomingRelIDs(endNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 || inIDs[0] != relID {
		t.Errorf("incomingRelIDs = %v, want [%d]", inIDs, relID)
	}

	// Rel entity should NOT exist (only the in/ index was written).
	if bs.hasRelID(relID) {
		t.Error("hasRelID should be false — putRelIncoming doesn't store entity")
	}
}

func TestDeleteRelEntityAndOut_RemovesEntityButNotInIdx(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Use full PutRelationship first, then partial delete.
	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, types.NodeID(n1.ID()), types.NodeID(n2.ID()))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	info, err := bs.deleteRelEntityAndOut(relID)
	if err != nil {
		t.Fatalf("deleteRelEntityAndOut: %v", err)
	}

	// Entity should be gone.
	if bs.hasRelID(relID) {
		t.Error("hasRelID should be false after deleteRelEntityAndOut")
	}

	// outIdx should be empty.
	outIDs := bs.outgoingRelIDs(n1.ID().SnowflakeID())
	if len(outIDs) != 0 {
		t.Errorf("outgoingRelIDs should be empty, got %v", outIDs)
	}

	// inIdx should STILL contain the rel (partial delete doesn't touch in/).
	inIDs := bs.incomingRelIDs(n2.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 || inIDs[0] != relID {
		t.Errorf("incomingRelIDs should still contain rel, got %v", inIDs)
	}

	// Verify info is populated correctly.
	if info.id != relID {
		t.Errorf("info.id = %d, want %d", info.id, relID)
	}
	if info.startID != n1.ID().SnowflakeID() {
		t.Error("info.startID mismatch")
	}
	if info.endID != n2.ID().SnowflakeID() {
		t.Error("info.endID mismatch")
	}
}

func TestDeleteRelIncoming_RemovesInIdxOnly(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Full PutRelationship.
	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, types.NodeID(n1.ID()), types.NodeID(n2.ID()))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	info := relDeleteInfo{
		id:      relID,
		relType: r.TypeToken().Value(),
		startID: n1.ID().SnowflakeID(),
		endID:   n2.ID().SnowflakeID(),
	}
	if err := bs.deleteRelIncoming(info); err != nil {
		t.Fatal(err)
	}

	// inIdx should be empty.
	inIDs := bs.incomingRelIDs(n2.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("incomingRelIDs should be empty after deleteRelIncoming, got %v", inIDs)
	}

	// Entity should STILL exist.
	if !bs.hasRelID(relID) {
		t.Error("hasRelID should be true — deleteRelIncoming doesn't remove entity")
	}

	// outIdx should still have the rel.
	outIDs := bs.outgoingRelIDs(n1.ID().SnowflakeID())
	if len(outIDs) != 1 || outIDs[0] != relID {
		t.Errorf("outgoingRelIDs should still contain rel, got %v", outIDs)
	}
}

func TestHasNodeID_HasRelID(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	nodeID := gen.Generate()
	n := types.NewNode(types.NodeID(nodeID), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}

	if !bs.hasNodeID(nodeID) {
		t.Error("hasNodeID should be true")
	}
	if bs.hasNodeID(snowflake.ID(999)) {
		t.Error("hasNodeID should be false for unknown ID")
	}
	if bs.hasRelID(snowflake.ID(999)) {
		t.Error("hasRelID should be false for unknown ID")
	}
}

func TestOutgoingRelIDs_Sorted(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)
	var relIDs []snowflake.ID
	for i := 0; i < 5; i++ {
		relID := relGen.Generate()
		relIDs = append(relIDs, relID)
		r := types.NewRelationship(types.RelID(relID), 1, types.NodeID(n1.ID()), types.NodeID(n2.ID()))
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}

	outIDs := bs.outgoingRelIDs(n1.ID().SnowflakeID())
	if len(outIDs) != 5 {
		t.Fatalf("outgoingRelIDs count = %d, want 5", len(outIDs))
	}
	for i := 1; i < len(outIDs); i++ {
		if outIDs[i] <= outIDs[i-1] {
			t.Errorf("outgoingRelIDs not sorted: [%d]=%d >= [%d]=%d", i-1, outIDs[i-1], i, outIDs[i])
		}
	}
}

func TestIncomingRelIDs_TypeFilter(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)

	// Type 1 rel.
	r1 := types.NewRelationship(types.RelID(relGen.Generate()), 1, types.NodeID(n1.ID()), types.NodeID(n2.ID()))
	if err := bs.PutRelationship(r1); err != nil {
		t.Fatal(err)
	}

	// Type 2 rel.
	r2 := types.NewRelationship(types.RelID(relGen.Generate()), 2, types.NodeID(n1.ID()), types.NodeID(n2.ID()))
	if err := bs.PutRelationship(r2); err != nil {
		t.Fatal(err)
	}

	// All types.
	all := bs.incomingRelIDs(n2.ID().SnowflakeID(), 0)
	if len(all) != 2 {
		t.Fatalf("incomingRelIDs(0) = %d, want 2", len(all))
	}

	// Filter type 1.
	type1 := bs.incomingRelIDs(n2.ID().SnowflakeID(), 1)
	if len(type1) != 1 {
		t.Fatalf("incomingRelIDs(1) = %d, want 1", len(type1))
	}

	// Filter type 2.
	type2 := bs.incomingRelIDs(n2.ID().SnowflakeID(), 2)
	if len(type2) != 1 {
		t.Fatalf("incomingRelIDs(2) = %d, want 1", len(type2))
	}

	// Filter unknown type.
	type99 := bs.incomingRelIDs(n2.ID().SnowflakeID(), 99)
	if len(type99) != 0 {
		t.Errorf("incomingRelIDs(99) = %d, want 0", len(type99))
	}
}

// newTestGen creates a snowflake generator for testing.
func newTestGen(t *testing.T, nodeID int64) *snowflake.Node {
	t.Helper()
	gen, err := snowflake.NewNode(nodeID,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return gen
}
