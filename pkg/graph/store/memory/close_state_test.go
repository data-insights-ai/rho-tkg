package memory

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

func TestMemoryStoreZeroValueConcurrentFirstUse(t *testing.T) {
	t.Parallel()
	var ms Store

	const goroutines = 32
	start := make(chan struct{})
	errs := make(chan error, goroutines*6)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			<-start

			if _, err := ms.GetNode(types.NodeID(id + 1)); !errors.Is(err, ErrNodeNotFound) {
				errs <- fmt.Errorf("GetNode = %v, want ErrNodeNotFound", err)
			}
			if _, err := ms.GetRelationship(types.RelID(id + 1)); !errors.Is(err, ErrRelNotFound) {
				errs <- fmt.Errorf("GetRelationship = %v, want ErrRelNotFound", err)
			}
			if count, err := ms.NodeCount(); err != nil || count != 0 {
				errs <- fmt.Errorf("NodeCount = (%d, %v), want (0, nil)", count, err)
			}
			if count, err := ms.RelationshipCount(); err != nil || count != 0 {
				errs <- fmt.Errorf("RelationshipCount = (%d, %v), want (0, nil)", count, err)
			}
			if count, err := ms.NodeCountByLabel(1); err != nil || count != 0 {
				errs <- fmt.Errorf("NodeCountByLabel = (%d, %v), want (0, nil)", count, err)
			}
			if count, err := ms.RelCountByType(1); err != nil || count != 0 {
				errs <- fmt.Errorf("RelCountByType = (%d, %v), want (0, nil)", count, err)
			}
		}(i)
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestMemoryStoreIndexAPIsCheckLifecycleBeforeValidation(t *testing.T) {
	t.Parallel()

	checks := func(ms *Store) []struct {
		name string
		run  func() error
	} {
		return []struct {
			name string
			run  func() error
		}{
			{name: "CreatePropertyIndex", run: func() error { return ms.CreatePropertyIndex(0, "") }},
			{name: "DropPropertyIndex", run: func() error { return ms.DropPropertyIndex(0, "") }},
			{name: "CreateTemporalIndex", run: func() error { return ms.CreateTemporalIndex(0) }},
			{name: "DropTemporalIndex", run: func() error { return ms.DropTemporalIndex(0) }},
			{name: "CreateHighFrequencyIndex", run: func() error { return ms.CreateHighFrequencyIndex(0, 0) }},
			{name: "DropHighFrequencyIndex", run: func() error { return ms.DropHighFrequencyIndex(0) }},
			{name: "CreateVectorIndex", run: func() error { return ms.CreateVectorIndex(0, "", 0, storepkg.DistanceMetric(99)) }},
			{name: "DropVectorIndex", run: func() error { return ms.DropVectorIndex(0, "") }},
			{name: "SearchNearestNodes", run: func() error {
				_, err := ms.SearchNearestNodes(0, "", nil, -1, QueryOpts{Limit: -1})
				return err
			}},
			{name: "SearchNearestFiltered", run: func() error {
				_, err := ms.SearchNearestFiltered(0, "", nil, -1, nil)
				return err
			}},
			{name: "NodesByLabelAndProperty", run: func() error {
				_, err := ms.NodesByLabelAndProperty(0, "", nil, QueryOpts{Limit: -1})
				return err
			}},
		}
	}

	var nilStore *Store
	for _, check := range checks(nilStore) {
		if err := check.run(); !errors.Is(err, ErrNilStore) {
			t.Fatalf("nil %s error = %v, want ErrNilStore", check.name, err)
		}
	}

	ms := New()
	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, check := range checks(ms) {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("closed %s error = %v, want ErrStoreClosed", check.name, err)
		}
	}
}

func TestMemoryStoreMutationAPIsCheckLifecycleBeforeValidation(t *testing.T) {
	t.Parallel()

	checks := func(ms *Store) []struct {
		name string
		run  func() error
	} {
		return []struct {
			name string
			run  func() error
		}{
			{name: "PutNode", run: func() error { return ms.PutNode(nil) }},
			{name: "ReplaceNode", run: func() error { return ms.ReplaceNode(nil) }},
			{name: "DeleteNode", run: func() error { return ms.DeleteNode(0) }},
			{name: "DeleteNodeCascade", run: func() error { return ms.DeleteNodeCascade(0) }},
			{name: "RemoveNodeLabelToken", run: func() error { return ms.RemoveNodeLabelToken(0, 0, nil) }},
			{name: "AddNodeLabelToken", run: func() error { return ms.AddNodeLabelToken(0, 0, nil) }},
			{name: "PutNodesBatch", run: func() error { return ms.PutNodesBatch([]*types.Node{nil}) }},
			{name: "DeleteNodesBatch", run: func() error { return ms.DeleteNodesBatch([]types.NodeID{0}) }},
			{name: "PutRelationship", run: func() error { return ms.PutRelationship(nil) }},
			{name: "PutRelationshipGeneratedIDWithEndpointHashes", run: func() error {
				_, _, err := ms.PutRelationshipGeneratedIDWithEndpointHashes(nil, generatedcreate.FreshGraphID)
				return err
			}},
			{name: "ReplaceRelationship", run: func() error { return ms.ReplaceRelationship(nil) }},
			{name: "DeleteRelationship", run: func() error { return ms.DeleteRelationship(0) }},
			{name: "PutRelationshipsBatch", run: func() error { return ms.PutRelationshipsBatch([]*types.Relationship{nil}) }},
			{name: "DeleteRelationshipsBatch", run: func() error { return ms.DeleteRelationshipsBatch([]types.RelID{0}) }},
			{name: "RemoveNodeLabelTokenWithHistory", run: func() error {
				return ms.RemoveNodeLabelTokenWithHistory(0, 0, nil, 0, nil)
			}},
			{name: "AddNodeLabelTokenWithHistory", run: func() error {
				return ms.AddNodeLabelTokenWithHistory(0, 0, nil, 0, nil)
			}},
			{name: "ReplaceNodeWithHistory", run: func() error { return ms.ReplaceNodeWithHistory(nil, 0, nil) }},
			{name: "DeleteNodeWithHistory", run: func() error { return ms.DeleteNodeWithHistory(0, 0, nil, nil) }},
			{name: "PutNodeVersion", run: func() error { return ms.PutNodeVersion(0, 0, nil) }},
			{name: "TruncateNodeHistory", run: func() error { return ms.TruncateNodeHistory(0, -1) }},
			{name: "TrimNodeHistoryFrom", run: func() error { return ms.TrimNodeHistoryFrom(0, 0) }},
			{name: "ReplaceRelWithHistory", run: func() error { return ms.ReplaceRelWithHistory(nil, 0, nil) }},
			{name: "DeleteRelWithHistory", run: func() error { return ms.DeleteRelWithHistory(0, 0, nil) }},
			{name: "PutRelVersion", run: func() error { return ms.PutRelVersion(0, 0, nil) }},
			{name: "TruncateRelHistory", run: func() error { return ms.TruncateRelHistory(0, -1) }},
			{name: "TrimRelHistoryFrom", run: func() error { return ms.TrimRelHistoryFrom(0, 0) }},
		}
	}

	var nilStore *Store
	for _, check := range checks(nilStore) {
		if err := check.run(); !errors.Is(err, ErrNilStore) {
			t.Fatalf("nil %s error = %v, want ErrNilStore", check.name, err)
		}
	}

	ms := New()
	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, check := range checks(ms) {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("closed %s error = %v, want ErrStoreClosed", check.name, err)
		}
	}
}

func TestMemoryStoreReadAPIsCheckLifecycleBeforeValidation(t *testing.T) {
	t.Parallel()

	checks := func(ms *Store) []struct {
		name string
		run  func() error
	} {
		return []struct {
			name string
			run  func() error
		}{
			{name: "GetNode", run: func() error { _, err := ms.GetNode(0); return err }},
			{name: "NodeIntegrityHash", run: func() error { _, err := ms.NodeIntegrityHash(0); return err }},
			{name: "EndpointIntegrityHashes", run: func() error {
				_, _, err := ms.EndpointIntegrityHashes(0, 0)
				return err
			}},
			{name: "GetRelationship", run: func() error { _, err := ms.GetRelationship(0); return err }},
			{name: "OutgoingRelationships", run: func() error { _, err := ms.OutgoingRelationships(0, 0); return err }},
			{name: "OutgoingRelationshipsForNodes", run: func() error {
				_, err := ms.OutgoingRelationshipsForNodes([]types.NodeID{0}, 0)
				return err
			}},
			{name: "IncomingRelationships", run: func() error { _, err := ms.IncomingRelationships(0, 0); return err }},
			{name: "IncomingRelationshipsForNodes", run: func() error {
				_, err := ms.IncomingRelationshipsForNodes([]types.NodeID{0}, 0)
				return err
			}},
			{name: "NodesByLabel", run: func() error { _, err := ms.NodesByLabel(0, QueryOpts{Limit: -1}); return err }},
			{name: "RelationshipsByType", run: func() error { _, err := ms.RelationshipsByType(0, QueryOpts{Limit: -1}); return err }},
			{name: "NodeCount", run: func() error { _, err := ms.NodeCount(); return err }},
			{name: "RelationshipCount", run: func() error { _, err := ms.RelationshipCount(); return err }},
			{name: "NodeCountByLabel", run: func() error { _, err := ms.NodeCountByLabel(0); return err }},
			{name: "RelCountByType", run: func() error { _, err := ms.RelCountByType(0); return err }},
			{name: "AllNodeIDs", run: func() error { _, err := ms.AllNodeIDs(QueryOpts{Limit: -1}); return err }},
			{name: "AllRelIDs", run: func() error { _, err := ms.AllRelIDs(QueryOpts{Limit: -1}); return err }},
			{name: "ForEachNodeID", run: func() error { return ms.ForEachNodeID(nil) }},
			{name: "ForEachRelID", run: func() error { return ms.ForEachRelID(nil) }},
			{name: "ForEachNodeHistoryID", run: func() error { return ms.ForEachNodeHistoryID(nil) }},
			{name: "ForEachRelHistoryID", run: func() error { return ms.ForEachRelHistoryID(nil) }},
			{name: "AllNodeHistoryIDs", run: func() error { _, err := ms.AllNodeHistoryIDs(); return err }},
			{name: "AllRelHistoryIDs", run: func() error { _, err := ms.AllRelHistoryIDs(); return err }},
			{name: "AllNodeHistoryIDsFrom", run: func() error { _, err := ms.AllNodeHistoryIDsFrom(0, -1); return err }},
			{name: "AllRelHistoryIDsFrom", run: func() error { _, err := ms.AllRelHistoryIDsFrom(0, -1); return err }},
			{name: "AllNodes", run: func() error { _, err := ms.AllNodes(QueryOpts{Limit: -1}); return err }},
			{name: "AllRelationships", run: func() error { _, err := ms.AllRelationships(QueryOpts{Limit: -1}); return err }},
			{name: "GetNodesByIDs", run: func() error { _, err := ms.GetNodesByIDs([]types.NodeID{0}); return err }},
			{name: "GetRelationshipsByIDs", run: func() error { _, err := ms.GetRelationshipsByIDs([]types.RelID{0}); return err }},
			{name: "GetNodeVersion", run: func() error { _, err := ms.GetNodeVersion(0, 0); return err }},
			{name: "GetNodeHistory", run: func() error { _, err := ms.GetNodeHistory(0); return err }},
			{name: "GetRelVersion", run: func() error { _, err := ms.GetRelVersion(0, 0); return err }},
			{name: "GetRelHistory", run: func() error { _, err := ms.GetRelHistory(0); return err }},
			{name: "NodeAsOf", run: func() error { _, err := ms.NodeAsOf(0, 0); return err }},
			{name: "RelAsOf", run: func() error { _, err := ms.RelAsOf(0, 0); return err }},
			{name: "NodesAsOf", run: func() error { _, err := ms.NodesAsOf(0); return err }},
			{name: "RelsAsOf", run: func() error { _, err := ms.RelsAsOf(0); return err }},
		}
	}

	var nilStore *Store
	for _, check := range checks(nilStore) {
		if err := check.run(); !errors.Is(err, ErrNilStore) {
			t.Fatalf("nil %s error = %v, want ErrNilStore", check.name, err)
		}
	}

	ms := New()
	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, check := range checks(ms) {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("closed %s error = %v, want ErrStoreClosed", check.name, err)
		}
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
