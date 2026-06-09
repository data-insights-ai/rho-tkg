package badger

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestBadgerStoreNilLifecycleReturnsNilStore(t *testing.T) {
	t.Parallel()
	var bs *Store
	if err := bs.Close(); !errors.Is(err, ErrNilStore) {
		t.Fatalf("Close nil store = %v, want ErrNilStore", err)
	}
	if err := bs.Clear(); !errors.Is(err, ErrNilStore) {
		t.Fatalf("Clear nil store = %v, want ErrNilStore", err)
	}
}

func TestBadgerStoreNilNoErrorHelpersReturnZero(t *testing.T) {
	t.Parallel()
	var bs *Store
	if bs.HasNodeID(snowflake.ID(1)) {
		t.Fatal("nil HasNodeID returned true")
	}
	if bs.HasRelID(snowflake.ID(1)) {
		t.Fatal("nil HasRelID returned true")
	}
	if ids := bs.IncomingRelIDs(snowflake.ID(1), 0); ids != nil {
		t.Fatalf("nil IncomingRelIDs = %v, want nil", ids)
	}
	if entries := bs.IncomingIndexEntries(); entries != nil {
		t.Fatalf("nil IncomingIndexEntries = %v, want nil", entries)
	}
	if ids := bs.OutgoingRelIDs(snowflake.ID(1)); ids != nil {
		t.Fatalf("nil OutgoingRelIDs = %v, want nil", ids)
	}
	if hits := bs.NodeCacheHits(); hits != 0 {
		t.Fatalf("nil NodeCacheHits = %d, want 0", hits)
	}
	if misses := bs.NodeCacheMisses(); misses != 0 {
		t.Fatalf("nil NodeCacheMisses = %d, want 0", misses)
	}
	if hits := bs.RelCacheHits(); hits != 0 {
		t.Fatalf("nil RelCacheHits = %d, want 0", hits)
	}
	if misses := bs.RelCacheMisses(); misses != 0 {
		t.Fatalf("nil RelCacheMisses = %d, want 0", misses)
	}
	if stats := bs.IndexRebuildStats(); stats != (IndexRebuildStats{}) {
		t.Fatalf("nil IndexRebuildStats = %+v, want zero", stats)
	}
}

func TestBadgerStoreZeroValueLifecycleReturnsStoreClosed(t *testing.T) {
	t.Parallel()
	var bs Store
	if err := bs.Close(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Close zero-value store = %v, want ErrStoreClosed", err)
	}
	if err := bs.Clear(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Clear zero-value store = %v, want ErrStoreClosed", err)
	}
	if _, err := bs.GetNode(types.NodeID(1)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("GetNode zero-value store = %v, want ErrStoreClosed", err)
	}
}

func TestBadgerStoreClosingStateFailsPublicOperations(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	bs.closing.Store(true)

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "PutNode", run: func() error { return bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)) }},
		{name: "GetNode", run: func() error { _, err := bs.GetNode(types.NodeID(1)); return err }},
		{name: "Flush", run: func() error { return bs.Flush() }},
	}
	for _, check := range checks {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("%s while closing = %v, want ErrStoreClosed", check.name, err)
		}
	}
}

func TestBadgerStoreIndexAPIsCheckLifecycleBeforeValidation(t *testing.T) {
	t.Parallel()

	checks := func(bs *Store) []struct {
		name string
		run  func() error
	} {
		return []struct {
			name string
			run  func() error
		}{
			{name: "CreatePropertyIndex", run: func() error { return bs.CreatePropertyIndex(0, "") }},
			{name: "DropPropertyIndex", run: func() error { return bs.DropPropertyIndex(0, "") }},
			{name: "CreateTemporalIndex", run: func() error { return bs.CreateTemporalIndex(0) }},
			{name: "DropTemporalIndex", run: func() error { return bs.DropTemporalIndex(0) }},
			{name: "CreateHighFrequencyIndex", run: func() error { return bs.CreateHighFrequencyIndex(0, 0) }},
			{name: "DropHighFrequencyIndex", run: func() error { return bs.DropHighFrequencyIndex(0) }},
			{name: "TemporalIndexState", run: func() error {
				_, _, _, err := bs.TemporalIndexState(0)
				return err
			}},
			{name: "CreateVectorIndex", run: func() error { return bs.CreateVectorIndex(0, "", 0, DistanceMetric(99)) }},
			{name: "DropVectorIndex", run: func() error { return bs.DropVectorIndex(0, "") }},
			{name: "SearchNearestNodes", run: func() error {
				_, err := bs.SearchNearestNodes(0, "", nil, -1, QueryOpts{Limit: -1})
				return err
			}},
			{name: "SearchNearestFiltered", run: func() error {
				_, err := bs.SearchNearestFiltered(0, "", nil, -1, nil)
				return err
			}},
			{name: "NodesByLabelAndProperty", run: func() error {
				_, err := bs.NodesByLabelAndProperty(0, "", nil, QueryOpts{Limit: -1})
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

	bs := newTestBadgerStore(t)
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, check := range checks(bs) {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("closed %s error = %v, want ErrStoreClosed", check.name, err)
		}
	}
}

func TestBadgerStoreMutationAPIsCheckLifecycleBeforeValidation(t *testing.T) {
	t.Parallel()

	checks := func(bs *Store) []struct {
		name string
		run  func() error
	} {
		return []struct {
			name string
			run  func() error
		}{
			{name: "PutNode", run: func() error { return bs.PutNode(nil) }},
			{name: "ReplaceNode", run: func() error { return bs.ReplaceNode(nil) }},
			{name: "DeleteNode", run: func() error { return bs.DeleteNode(0) }},
			{name: "DeleteNodeCascade", run: func() error { return bs.DeleteNodeCascade(0) }},
			{name: "RemoveNodeLabelToken", run: func() error { return bs.RemoveNodeLabelToken(0, 0, nil) }},
			{name: "AddNodeLabelToken", run: func() error { return bs.AddNodeLabelToken(0, 0, nil) }},
			{name: "PutRelationship", run: func() error { return bs.PutRelationship(nil) }},
			{name: "ReplaceRelationship", run: func() error { return bs.ReplaceRelationship(nil) }},
			{name: "DeleteRelationship", run: func() error { return bs.DeleteRelationship(0) }},
			{name: "PutRelEntityAndOut", run: func() error { return bs.PutRelEntityAndOut(nil) }},
			{name: "PutRelIncoming", run: func() error { return bs.PutRelIncoming(0, 0, 0, 0) }},
			{name: "DeleteRelEntityAndOut", run: func() error {
				_, err := bs.DeleteRelEntityAndOut(0)
				return err
			}},
			{name: "DeleteRelIncoming", run: func() error { return bs.DeleteRelIncoming(RelDeleteInfo{}) }},
			{name: "DeleteIncomingByRelID", run: func() error { return bs.DeleteIncomingByRelID(0, 0) }},
			{name: "ScanAndDeleteIncoming", run: func() error { return bs.ScanAndDeleteIncoming(0, 0) }},
			{name: "PurgeOrphanRelationshipIndexes", run: func() error { return bs.PurgeOrphanRelationshipIndexes(0) }},
			{name: "RemoveNodeLabelTokenWithHistory", run: func() error {
				return bs.RemoveNodeLabelTokenWithHistory(0, 0, nil, 0, nil)
			}},
			{name: "AddNodeLabelTokenWithHistory", run: func() error {
				return bs.AddNodeLabelTokenWithHistory(0, 0, nil, 0, nil)
			}},
			{name: "ReplaceNodeWithHistory", run: func() error { return bs.ReplaceNodeWithHistory(nil, 0, nil) }},
			{name: "DeleteNodeWithHistory", run: func() error { return bs.DeleteNodeWithHistory(0, 0, nil, nil) }},
			{name: "PutNodeVersion", run: func() error { return bs.PutNodeVersion(0, 0, nil) }},
			{name: "TrimNodeHistoryFrom", run: func() error { return bs.TrimNodeHistoryFrom(0, 0) }},
			{name: "ReplaceRelWithHistory", run: func() error { return bs.ReplaceRelWithHistory(nil, 0, nil) }},
			{name: "DeleteRelWithHistory", run: func() error { return bs.DeleteRelWithHistory(0, 0, nil) }},
			{name: "PutRelVersion", run: func() error { return bs.PutRelVersion(0, 0, nil) }},
			{name: "TrimRelHistoryFrom", run: func() error { return bs.TrimRelHistoryFrom(0, 0) }},
		}
	}

	var nilStore *Store
	for _, check := range checks(nilStore) {
		if err := check.run(); !errors.Is(err, ErrNilStore) {
			t.Fatalf("nil %s error = %v, want ErrNilStore", check.name, err)
		}
	}

	bs := newTestBadgerStore(t)
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, check := range checks(bs) {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("closed %s error = %v, want ErrStoreClosed", check.name, err)
		}
	}
}

func TestBadgerStorePublicAPIsReturnStoreClosedAfterClose(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

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
	if err := bs.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	rel := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := bs.PutNodeVersion(types.NodeID(1), 0, n1.DeepCopy()); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	if err := bs.PutRelVersion(types.RelID(100), 0, rel.DeepCopy()); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if err := bs.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	if err := bs.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := bs.CreateHighFrequencyIndex(2, time.Second); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := bs.CreateVectorIndex(1, "vec", 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	labels := registrypkg.NewLabelRegistry()
	labels.GetOrCreate("Person")
	relTypes := registrypkg.NewRelTypeRegistry()
	relTypes.GetOrCreate("KNOWS")
	if err := bs.SaveRegistries(labels, relTypes); err != nil {
		t.Fatalf("SaveRegistries: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	updatedN1 := types.NewNode(types.NodeID(1), 1, []uint16{3})
	if err := updatedN1.SetProperty("name", "Alice"); err != nil {
		t.Fatalf("SetProperty updated name: %v", err)
	}
	replacementRel := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	closedChecks := []struct {
		name string
		run  func() error
	}{
		{name: "PutNode", run: func() error { return bs.PutNode(types.NewNode(types.NodeID(3), 1, nil)) }},
		{name: "GetNode", run: func() error { _, err := bs.GetNode(types.NodeID(1)); return err }},
		{name: "ReplaceNode", run: func() error { return bs.ReplaceNode(updatedN1) }},
		{name: "DeleteNode", run: func() error { return bs.DeleteNode(types.NodeID(1)) }},
		{name: "DeleteNodeCascade", run: func() error { return bs.DeleteNodeCascade(types.NodeID(1)) }},
		{name: "RemoveNodeLabelToken", run: func() error { return bs.RemoveNodeLabelToken(types.NodeID(1), 1, updatedN1) }},
		{name: "AddNodeLabelToken", run: func() error { return bs.AddNodeLabelToken(types.NodeID(1), 3, updatedN1) }},
		{name: "PutNodesBatch empty", run: func() error { return bs.PutNodesBatch(nil) }},
		{name: "DeleteNodesBatch empty", run: func() error { return bs.DeleteNodesBatch(nil) }},
		{name: "PutRelationship", run: func() error {
			return bs.PutRelationship(types.NewRelationship(types.RelID(101), 1, types.NodeID(1), types.NodeID(2)))
		}},
		{name: "GetRelationship", run: func() error { _, err := bs.GetRelationship(types.RelID(100)); return err }},
		{name: "ReplaceRelationship", run: func() error { return bs.ReplaceRelationship(replacementRel) }},
		{name: "DeleteRelationship", run: func() error { return bs.DeleteRelationship(types.RelID(100)) }},
		{name: "PutRelationshipsBatch empty", run: func() error { return bs.PutRelationshipsBatch(nil) }},
		{name: "DeleteRelationshipsBatch empty", run: func() error { return bs.DeleteRelationshipsBatch(nil) }},
		{name: "NodeCount", run: func() error { _, err := bs.NodeCount(); return err }},
		{name: "RelationshipCount", run: func() error { _, err := bs.RelationshipCount(); return err }},
		{name: "NodeCountByLabel", run: func() error { _, err := bs.NodeCountByLabel(1); return err }},
		{name: "RelCountByType", run: func() error { _, err := bs.RelCountByType(1); return err }},
		{name: "NodesByLabel", run: func() error { _, err := bs.NodesByLabel(1, QueryOpts{}); return err }},
		{name: "AllNodes", run: func() error { _, err := bs.AllNodes(QueryOpts{}); return err }},
		{name: "GetNodesByIDs empty", run: func() error { _, err := bs.GetNodesByIDs(nil); return err }},
		{name: "NodesByLabelAndProperty", run: func() error { _, err := bs.NodesByLabelAndProperty(1, "name", "Alice", QueryOpts{}); return err }},
		{name: "AllNodeIDs", run: func() error { _, err := bs.AllNodeIDs(QueryOpts{}); return err }},
		{name: "ForEachNodeID", run: func() error {
			return bs.ForEachNodeID(func(types.NodeID) bool { t.Fatal("ForEachNodeID callback ran after close"); return false })
		}},
		{name: "RelationshipsByType", run: func() error { _, err := bs.RelationshipsByType(1, QueryOpts{}); return err }},
		{name: "OutgoingRelationships", run: func() error { _, err := bs.OutgoingRelationships(types.NodeID(1), 0); return err }},
		{name: "OutgoingRelationshipsForNodes empty", run: func() error { _, err := bs.OutgoingRelationshipsForNodes(nil, 0); return err }},
		{name: "IncomingRelationships", run: func() error { _, err := bs.IncomingRelationships(types.NodeID(2), 0); return err }},
		{name: "IncomingRelationshipsForNodes empty", run: func() error { _, err := bs.IncomingRelationshipsForNodes(nil, 0); return err }},
		{name: "AllRelationships", run: func() error { _, err := bs.AllRelationships(QueryOpts{}); return err }},
		{name: "GetRelationshipsByIDs empty", run: func() error { _, err := bs.GetRelationshipsByIDs(nil); return err }},
		{name: "AllRelIDs", run: func() error { _, err := bs.AllRelIDs(QueryOpts{}); return err }},
		{name: "ForEachRelID", run: func() error {
			return bs.ForEachRelID(func(types.RelID) bool { t.Fatal("ForEachRelID callback ran after close"); return false })
		}},
		{name: "RemoveNodeLabelTokenWithHistory", run: func() error { return bs.RemoveNodeLabelTokenWithHistory(types.NodeID(1), 1, updatedN1, 0, n1) }},
		{name: "AddNodeLabelTokenWithHistory", run: func() error { return bs.AddNodeLabelTokenWithHistory(types.NodeID(1), 3, updatedN1, 0, n1) }},
		{name: "ReplaceNodeWithHistory", run: func() error { return bs.ReplaceNodeWithHistory(updatedN1, 0, n1) }},
		{name: "DeleteNodeWithHistory", run: func() error { return bs.DeleteNodeWithHistory(types.NodeID(1), 0, n1, nil) }},
		{name: "PutNodeVersion", run: func() error { return bs.PutNodeVersion(types.NodeID(1), 1, n1) }},
		{name: "GetNodeVersion", run: func() error { _, err := bs.GetNodeVersion(types.NodeID(1), 0); return err }},
		{name: "GetNodeHistory", run: func() error { _, err := bs.GetNodeHistory(types.NodeID(1)); return err }},
		{name: "TruncateNodeHistory", run: func() error { return bs.TruncateNodeHistory(types.NodeID(1), 1) }},
		{name: "ForEachNodeHistoryID", run: func() error {
			return bs.ForEachNodeHistoryID(func(types.NodeID) bool { t.Fatal("ForEachNodeHistoryID callback ran after close"); return false })
		}},
		{name: "AllNodeHistoryIDs", run: func() error { _, err := bs.AllNodeHistoryIDs(); return err }},
		{name: "AllNodeHistoryIDsFrom", run: func() error { _, err := bs.AllNodeHistoryIDsFrom(types.NodeID(0), 0); return err }},
		{name: "MaxNodeHistoryID", run: func() error { _, err := bs.MaxNodeHistoryID(); return err }},
		{name: "ReplaceRelWithHistory", run: func() error { return bs.ReplaceRelWithHistory(replacementRel, 0, rel) }},
		{name: "DeleteRelWithHistory", run: func() error { return bs.DeleteRelWithHistory(types.RelID(100), 0, rel) }},
		{name: "PutRelVersion", run: func() error { return bs.PutRelVersion(types.RelID(100), 1, rel) }},
		{name: "GetRelVersion", run: func() error { _, err := bs.GetRelVersion(types.RelID(100), 0); return err }},
		{name: "GetRelHistory", run: func() error { _, err := bs.GetRelHistory(types.RelID(100)); return err }},
		{name: "TruncateRelHistory", run: func() error { return bs.TruncateRelHistory(types.RelID(100), 1) }},
		{name: "ForEachRelHistoryID", run: func() error {
			return bs.ForEachRelHistoryID(func(types.RelID) bool { t.Fatal("ForEachRelHistoryID callback ran after close"); return false })
		}},
		{name: "AllRelHistoryIDs", run: func() error { _, err := bs.AllRelHistoryIDs(); return err }},
		{name: "AllRelHistoryIDsFrom", run: func() error { _, err := bs.AllRelHistoryIDsFrom(types.RelID(0), 0); return err }},
		{name: "MaxRelHistoryID", run: func() error { _, err := bs.MaxRelHistoryID(); return err }},
		{name: "CreatePropertyIndex", run: func() error { return bs.CreatePropertyIndex(1, "other") }},
		{name: "DropPropertyIndex", run: func() error { return bs.DropPropertyIndex(1, "name") }},
		{name: "CreateTemporalIndex", run: func() error { return bs.CreateTemporalIndex(2) }},
		{name: "DropTemporalIndex", run: func() error { return bs.DropTemporalIndex(1) }},
		{name: "CreateHighFrequencyIndex", run: func() error { return bs.CreateHighFrequencyIndex(3, time.Second) }},
		{name: "DropHighFrequencyIndex", run: func() error { return bs.DropHighFrequencyIndex(2) }},
		{name: "CreateVectorIndex", run: func() error { return bs.CreateVectorIndex(1, "other_vec", 2, DistanceCosine) }},
		{name: "DropVectorIndex", run: func() error { return bs.DropVectorIndex(1, "vec") }},
		{name: "SearchNearestNodes", run: func() error { _, err := bs.SearchNearestNodes(1, "vec", []float32{1, 0}, 1, QueryOpts{}); return err }},
		{name: "SearchNearestFiltered", run: func() error { _, err := bs.SearchNearestFiltered(1, "vec", []float32{1, 0}, 1, nil); return err }},
		{name: "PutRelEntityAndOut", run: func() error {
			return bs.PutRelEntityAndOut(types.NewRelationship(types.RelID(101), 1, types.NodeID(1), types.NodeID(2)))
		}},
		{name: "PutRelIncoming", run: func() error { return bs.PutRelIncoming(snowflake.ID(2), snowflake.ID(1), 1, snowflake.ID(100)) }},
		{name: "DeleteRelEntityAndOut", run: func() error { _, err := bs.DeleteRelEntityAndOut(snowflake.ID(100)); return err }},
		{name: "DeleteRelIncoming", run: func() error {
			return bs.DeleteRelIncoming(RelDeleteInfo{ID: snowflake.ID(100), RelType: 1, StartID: snowflake.ID(1), EndID: snowflake.ID(2)})
		}},
		{name: "DeleteIncomingByRelID", run: func() error { return bs.DeleteIncomingByRelID(snowflake.ID(2), snowflake.ID(100)) }},
		{name: "ScanAndDeleteIncoming", run: func() error { return bs.ScanAndDeleteIncoming(snowflake.ID(2), snowflake.ID(100)) }},
		{name: "PurgeOrphanRelationshipIndexes", run: func() error { return bs.PurgeOrphanRelationshipIndexes(types.RelID(100)) }},
		{name: "SaveRegistries", run: func() error { return bs.SaveRegistries(labels, relTypes) }},
		{name: "SaveLabelRegistry", run: func() error { return bs.SaveLabelRegistry(labels) }},
		{name: "LoadLabelRegistry", run: func() error { _, err := bs.LoadLabelRegistry(registrypkg.NewLabelRegistry()); return err }},
		{name: "SaveRelTypeRegistry", run: func() error { return bs.SaveRelTypeRegistry(relTypes) }},
		{name: "LoadRelTypeRegistry", run: func() error { _, err := bs.LoadRelTypeRegistry(registrypkg.NewRelTypeRegistry()); return err }},
		{name: "Flush", run: func() error { return bs.Flush() }},
		{name: "Clear", run: func() error { return bs.Clear() }},
	}

	for _, check := range closedChecks {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("%s error = %v, want ErrStoreClosed", check.name, err)
		}
	}

	if bs.HasNodeID(snowflake.ID(1)) {
		t.Fatal("HasNodeID returned true after close")
	}
	if bs.HasRelID(snowflake.ID(100)) {
		t.Fatal("HasRelID returned true after close")
	}
	if ids := bs.IncomingRelIDs(snowflake.ID(2), 0); ids != nil {
		t.Fatalf("IncomingRelIDs after close = %v, want nil", ids)
	}
	if entries := bs.IncomingIndexEntries(); entries != nil {
		t.Fatalf("IncomingIndexEntries after close = %v, want nil", entries)
	}
	if ids := bs.OutgoingRelIDs(snowflake.ID(1)); ids != nil {
		t.Fatalf("OutgoingRelIDs after close = %v, want nil", ids)
	}
	if hits := bs.NodeCacheHits(); hits != 0 {
		t.Fatalf("NodeCacheHits after close = %d, want 0", hits)
	}
	if misses := bs.NodeCacheMisses(); misses != 0 {
		t.Fatalf("NodeCacheMisses after close = %d, want 0", misses)
	}
	if hits := bs.RelCacheHits(); hits != 0 {
		t.Fatalf("RelCacheHits after close = %d, want 0", hits)
	}
	if misses := bs.RelCacheMisses(); misses != 0 {
		t.Fatalf("RelCacheMisses after close = %d, want 0", misses)
	}
	if stats := bs.IndexRebuildStats(); stats != (IndexRebuildStats{}) {
		t.Fatalf("IndexRebuildStats after close = %+v, want zero", stats)
	}
}

func TestBadgerStoreSearchNearestFilteredReturnsClosedWhenFilterClosesStore(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(1), 1, nil)
	if err := n.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty vec: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.CreateVectorIndex(1, "vec", 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	ids, err := bs.SearchNearestFiltered(1, "vec", []float32{1, 0}, 1, func(snowflake.ID) bool {
		if err := bs.Close(); err != nil {
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
