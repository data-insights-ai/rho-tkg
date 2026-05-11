package memory

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestMemoryStoreNilLifecycleReturnsNilStore(t *testing.T) {
	t.Parallel()
	var ms *Store
	if err := ms.Close(); !errors.Is(err, ErrNilStore) {
		t.Fatalf("Close nil store = %v, want ErrNilStore", err)
	}
	if err := ms.Clear(); !errors.Is(err, ErrNilStore) {
		t.Fatalf("Clear nil store = %v, want ErrNilStore", err)
	}
}

func TestMemoryStoreZeroValueSupportsBasicCRUD(t *testing.T) {
	t.Parallel()
	var ms Store

	if _, err := ms.GetNode(types.NodeID(1)); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("zero-value GetNode before writes = %v, want ErrNodeNotFound", err)
	}
	if count, err := ms.NodeCount(); err != nil || count != 0 {
		t.Fatalf("zero-value NodeCount = (%d, %v), want (0, nil)", count, err)
	}

	n1 := types.NewNode(types.NodeID(1), 1, nil)
	if err := n1.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(2), 1, nil)
	if err := n2.SetProperty("name", "Bob"); err != nil {
		t.Fatalf("SetProperty n2: %v", err)
	}
	if err := ms.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatalf("zero-value CreatePropertyIndex: %v", err)
	}
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("zero-value PutNode n1: %v", err)
	}
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("zero-value PutNode n2: %v", err)
	}

	rel := types.NewRelationship(types.RelID(100), 1, n1.ID(), n2.ID())
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatalf("zero-value PutRelationship: %v", err)
	}
	if err := ms.PutNodeVersion(n1.ID(), n1.Version(), n1.DeepCopy()); err != nil {
		t.Fatalf("zero-value PutNodeVersion: %v", err)
	}

	nodes, err := ms.NodesByLabelAndProperty(1, "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("zero-value NodesByLabelAndProperty: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != n1.ID() {
		t.Fatalf("zero-value indexed query = %v, want node %d", nodes, n1.ID())
	}
	rels, err := ms.OutgoingRelationships(n1.ID(), 1)
	if err != nil {
		t.Fatalf("zero-value OutgoingRelationships: %v", err)
	}
	if len(rels) != 1 || rels[0].ID() != rel.ID() {
		t.Fatalf("zero-value outgoing = %v, want rel %d", rels, rel.ID())
	}
	history, err := ms.GetNodeHistory(n1.ID())
	if err != nil {
		t.Fatalf("zero-value GetNodeHistory: %v", err)
	}
	if len(history) != 1 || history[0].ID() != n1.ID() {
		t.Fatalf("zero-value history = %v, want node %d", history, n1.ID())
	}
}

func TestMemoryStorePublicAPIsReturnStoreClosedAfterClose(t *testing.T) {
	t.Parallel()
	ms := New()

	n1 := types.NewNode(types.NodeID(1), 1, nil)
	if err := n1.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty name: %v", err)
	}
	if err := n1.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty vec: %v", err)
	}
	n2 := types.NewNode(types.NodeID(2), 2, nil)
	if err := n2.SetProperty("name", "Bob"); err != nil {
		t.Fatalf("SetProperty name: %v", err)
	}
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	rel := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ms.PutNodeVersion(types.NodeID(1), 0, n1.DeepCopy()); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	if err := ms.PutRelVersion(types.RelID(100), 0, rel.DeepCopy()); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if err := ms.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ms.CreateHighFrequencyIndex(2, time.Second); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ms.CreateVectorIndex(1, "vec", 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	updatedN1 := types.NewNode(types.NodeID(1), 1, []uint16{3})
	if err := updatedN1.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty updated name: %v", err)
	}
	if got := ms.GetNodeHistoryEntry(types.NodeID(1), 0); got != nil {
		t.Fatalf("GetNodeHistoryEntry after close = %v, want nil", got)
	}
	tamperedNode := types.NewNode(types.NodeID(1), 9, nil)
	if err := tamperedNode.SetProperty("name", "Mallory"); err != nil {
		t.Fatalf("SetProperty tampered name: %v", err)
	}
	ms.SetNodeHistoryEntryForTest(types.NodeID(1), 0, tamperedNode)
	ms.SetNodeForTest(types.NodeID(1), tamperedNode)
	ms.mu.RLock()
	currentName, _ := ms.nodes[types.NodeID(1)].GetProperty("name")
	historyName, _ := ms.nodeHistory[types.NodeID(1)][0].GetProperty("name")
	ms.mu.RUnlock()
	if currentName != "Alice" || historyName != "Alice" {
		t.Fatalf("test export helpers mutated closed store: current=%v history=%v", currentName, historyName)
	}
	replacementRel := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	closedChecks := []struct {
		name string
		run  func() error
	}{
		{name: "PutNode", run: func() error { return ms.PutNode(types.NewNode(types.NodeID(3), 1, nil)) }},
		{name: "GetNode", run: func() error { _, err := ms.GetNode(types.NodeID(1)); return err }},
		{name: "ReplaceNode", run: func() error { return ms.ReplaceNode(updatedN1) }},
		{name: "DeleteNode", run: func() error { return ms.DeleteNode(types.NodeID(1)) }},
		{name: "DeleteNodeCascade", run: func() error { return ms.DeleteNodeCascade(types.NodeID(1)) }},
		{name: "RemoveNodeLabelToken", run: func() error { return ms.RemoveNodeLabelToken(types.NodeID(1), 1, updatedN1) }},
		{name: "AddNodeLabelToken", run: func() error { return ms.AddNodeLabelToken(types.NodeID(1), 3, updatedN1) }},
		{name: "PutNodesBatch empty", run: func() error { return ms.PutNodesBatch(nil) }},
		{name: "DeleteNodesBatch empty", run: func() error { return ms.DeleteNodesBatch(nil) }},
		{name: "PutRelationship", run: func() error {
			return ms.PutRelationship(types.NewRelationship(types.RelID(101), 1, types.NodeID(1), types.NodeID(2)))
		}},
		{name: "GetRelationship", run: func() error { _, err := ms.GetRelationship(types.RelID(100)); return err }},
		{name: "ReplaceRelationship", run: func() error { return ms.ReplaceRelationship(replacementRel) }},
		{name: "DeleteRelationship", run: func() error { return ms.DeleteRelationship(types.RelID(100)) }},
		{name: "PutRelationshipsBatch empty", run: func() error { return ms.PutRelationshipsBatch(nil) }},
		{name: "DeleteRelationshipsBatch empty", run: func() error { return ms.DeleteRelationshipsBatch(nil) }},
		{name: "NodeCount", run: func() error { _, err := ms.NodeCount(); return err }},
		{name: "RelationshipCount", run: func() error { _, err := ms.RelationshipCount(); return err }},
		{name: "NodeCountByLabel", run: func() error { _, err := ms.NodeCountByLabel(1); return err }},
		{name: "RelCountByType", run: func() error { _, err := ms.RelCountByType(1); return err }},
		{name: "NodesByLabel", run: func() error { _, err := ms.NodesByLabel(1, QueryOpts{}); return err }},
		{name: "AllNodes", run: func() error { _, err := ms.AllNodes(QueryOpts{}); return err }},
		{name: "GetNodesByIDs empty", run: func() error { _, err := ms.GetNodesByIDs(nil); return err }},
		{name: "NodesByLabelAndProperty", run: func() error { _, err := ms.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{}); return err }},
		{name: "AllNodeIDs", run: func() error { _, err := ms.AllNodeIDs(QueryOpts{}); return err }},
		{name: "ForEachNodeID", run: func() error {
			return ms.ForEachNodeID(func(types.NodeID) bool { t.Fatal("ForEachNodeID callback ran after close"); return false })
		}},
		{name: "RelationshipsByType", run: func() error { _, err := ms.RelationshipsByType(1, QueryOpts{}); return err }},
		{name: "OutgoingRelationships", run: func() error { _, err := ms.OutgoingRelationships(types.NodeID(1), 0); return err }},
		{name: "OutgoingRelationshipsForNodes empty", run: func() error { _, err := ms.OutgoingRelationshipsForNodes(nil, 0); return err }},
		{name: "IncomingRelationships", run: func() error { _, err := ms.IncomingRelationships(types.NodeID(2), 0); return err }},
		{name: "IncomingRelationshipsForNodes empty", run: func() error { _, err := ms.IncomingRelationshipsForNodes(nil, 0); return err }},
		{name: "AllRelationships", run: func() error { _, err := ms.AllRelationships(QueryOpts{}); return err }},
		{name: "GetRelationshipsByIDs empty", run: func() error { _, err := ms.GetRelationshipsByIDs(nil); return err }},
		{name: "AllRelIDs", run: func() error { _, err := ms.AllRelIDs(QueryOpts{}); return err }},
		{name: "ForEachRelID", run: func() error {
			return ms.ForEachRelID(func(types.RelID) bool { t.Fatal("ForEachRelID callback ran after close"); return false })
		}},
		{name: "RemoveNodeLabelTokenWithHistory", run: func() error { return ms.RemoveNodeLabelTokenWithHistory(types.NodeID(1), 1, updatedN1, 0, n1) }},
		{name: "AddNodeLabelTokenWithHistory", run: func() error { return ms.AddNodeLabelTokenWithHistory(types.NodeID(1), 3, updatedN1, 0, n1) }},
		{name: "ReplaceNodeWithHistory", run: func() error { return ms.ReplaceNodeWithHistory(updatedN1, 0, n1) }},
		{name: "DeleteNodeWithHistory", run: func() error { return ms.DeleteNodeWithHistory(types.NodeID(1), 0, n1, nil) }},
		{name: "PutNodeVersion", run: func() error { return ms.PutNodeVersion(types.NodeID(1), 1, n1) }},
		{name: "GetNodeVersion", run: func() error { _, err := ms.GetNodeVersion(types.NodeID(1), 0); return err }},
		{name: "GetNodeHistory", run: func() error { _, err := ms.GetNodeHistory(types.NodeID(1)); return err }},
		{name: "TruncateNodeHistory", run: func() error { return ms.TruncateNodeHistory(types.NodeID(1), 1) }},
		{name: "ForEachNodeHistoryID", run: func() error {
			return ms.ForEachNodeHistoryID(func(types.NodeID) bool { t.Fatal("ForEachNodeHistoryID callback ran after close"); return false })
		}},
		{name: "AllNodeHistoryIDs", run: func() error { _, err := ms.AllNodeHistoryIDs(); return err }},
		{name: "AllNodeHistoryIDsFrom", run: func() error { _, err := ms.AllNodeHistoryIDsFrom(types.NodeID(0), 0); return err }},
		{name: "ReplaceRelWithHistory", run: func() error { return ms.ReplaceRelWithHistory(replacementRel, 0, rel) }},
		{name: "DeleteRelWithHistory", run: func() error { return ms.DeleteRelWithHistory(types.RelID(100), 0, rel) }},
		{name: "PutRelVersion", run: func() error { return ms.PutRelVersion(types.RelID(100), 1, rel) }},
		{name: "GetRelVersion", run: func() error { _, err := ms.GetRelVersion(types.RelID(100), 0); return err }},
		{name: "GetRelHistory", run: func() error { _, err := ms.GetRelHistory(types.RelID(100)); return err }},
		{name: "TruncateRelHistory", run: func() error { return ms.TruncateRelHistory(types.RelID(100), 1) }},
		{name: "ForEachRelHistoryID", run: func() error {
			return ms.ForEachRelHistoryID(func(types.RelID) bool { t.Fatal("ForEachRelHistoryID callback ran after close"); return false })
		}},
		{name: "AllRelHistoryIDs", run: func() error { _, err := ms.AllRelHistoryIDs(); return err }},
		{name: "AllRelHistoryIDsFrom", run: func() error { _, err := ms.AllRelHistoryIDsFrom(types.RelID(0), 0); return err }},
		{name: "CreatePropertyIndex", run: func() error { return ms.CreatePropertyIndex(1, "other") }},
		{name: "DropPropertyIndex", run: func() error { return ms.DropPropertyIndex(1, "name") }},
		{name: "CreateTemporalIndex", run: func() error { return ms.CreateTemporalIndex(2) }},
		{name: "DropTemporalIndex", run: func() error { return ms.DropTemporalIndex(1) }},
		{name: "CreateHighFrequencyIndex", run: func() error { return ms.CreateHighFrequencyIndex(3, time.Second) }},
		{name: "DropHighFrequencyIndex", run: func() error { return ms.DropHighFrequencyIndex(2) }},
		{name: "CreateVectorIndex", run: func() error { return ms.CreateVectorIndex(1, "other_vec", 2, storepkg.DistanceCosine) }},
		{name: "DropVectorIndex", run: func() error { return ms.DropVectorIndex(1, "vec") }},
		{name: "SearchNearestNodes", run: func() error { _, err := ms.SearchNearestNodes(1, "vec", []float32{1, 0}, 1, QueryOpts{}); return err }},
		{name: "SearchNearestFiltered", run: func() error { _, err := ms.SearchNearestFiltered(1, "vec", []float32{1, 0}, 1, nil); return err }},
		{name: "Clear", run: func() error { return ms.Clear() }},
	}

	for _, check := range closedChecks {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("%s error = %v, want ErrStoreClosed", check.name, err)
		}
	}
}

func TestMemoryStoreSearchNearestFilteredReturnsClosedWhenFilterClosesStore(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(1), 1, nil)
	if err := n.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty vec: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ms.CreateVectorIndex(1, "vec", 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	ids, err := ms.SearchNearestFiltered(1, "vec", []float32{1, 0}, 1, func(snowflake.ID) bool {
		if err := ms.Close(); err != nil {
			t.Fatalf("Close from filter: %v", err)
		}
		return true
	})
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("SearchNearestFiltered error = %v, want ErrStoreClosed", err)
	}
	if ids != nil {
		t.Fatalf("SearchNearestFiltered ids = %v, want nil", ids)
	}
}
