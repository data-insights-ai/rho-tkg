package tiered

import (
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestTieredStore_Repair_OrphanedIncoming(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create a ref node (Case) and an event node (Signal).
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	// Manually create an orphaned in/ entry on refShard pointing to a non-existent rel.
	fakeRelID := relGen.Generate()
	if err := ts.RefShardForTest().PutRelIncoming(
		refNode.ID().SnowflakeID(),
		evtNode.ID().SnowflakeID(),
		relTok,
		fakeRelID,
	); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	// Verify the orphaned in/ entry exists.
	inIDs := ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 {
		t.Fatalf("expected 1 incoming rel, got %d", len(inIDs))
	}

	// Run repair.
	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 1 {
		t.Errorf("OrphanedInEntries = %d, want 1", result.OrphanedInEntries)
	}

	// Verify the orphaned in/ entry was removed.
	inIDs = ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("expected 0 incoming rels after repair, got %d", len(inIDs))
	}
}

func TestTieredStore_Repair_MissingIncoming(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create a ref node and an event node.
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	// Create a cross-shard relationship (E→R) but ONLY the entity+out side.
	// This simulates a partial write failure where the in/ write didn't happen.
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), relTypeTok,
		evtNode.ID(),
		refNode.ID())

	// Write only entity+out to the event shard (hotShard).
	ts.MuForTest().RLock()
	hotStore := ts.HotShardForTest().Store()
	ts.MuForTest().RUnlock()
	if err := hotStore.PutRelEntityAndOut(r); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}

	// Verify the in/ entry is missing on refShard.
	inIDs := ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Fatalf("expected 0 incoming rels before repair, got %d", len(inIDs))
	}

	// Run repair.
	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.MissingInEntries != 1 {
		t.Errorf("MissingInEntries = %d, want 1", result.MissingInEntries)
	}

	// Verify the in/ entry was created.
	inIDs = ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 {
		t.Errorf("expected 1 incoming rel after repair, got %d", len(inIDs))
	}
}
