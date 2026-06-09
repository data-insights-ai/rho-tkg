package tiered

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTieredStoreRelationshipBulkQueriesIncludeArchiveOnlyInDepthAll(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	archivedStart := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	archivedEnd := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	liveStart := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	liveEnd := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{archivedStart, archivedEnd, liveStart, liveEnd} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	archivedRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, archivedStart.ID(), archivedEnd.ID())
	liveRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, liveStart.ID(), liveEnd.ID())
	for _, r := range []*types.Relationship{archivedRel, liveRel} {
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship(%d): %v", r.ID(), err)
		}
	}
	if err := ts.ArchiveNode(archivedStart.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	count, err := ts.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount: %v", err)
	}
	if count != 2 {
		t.Fatalf("RelationshipCount = %d, want 2", count)
	}
	typeCount, err := ts.RelCountByType(1)
	if err != nil {
		t.Fatalf("RelCountByType: %v", err)
	}
	if typeCount != 2 {
		t.Fatalf("RelCountByType = %d, want 2", typeCount)
	}

	allRels, err := ts.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelationships DepthAll: %v", err)
	}
	requireTieredRelationshipIDs(t, allRels, archivedRel.ID(), liveRel.ID())

	hotRels, err := ts.AllRelationships(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatalf("AllRelationships DepthHot: %v", err)
	}
	requireTieredRelationshipIDs(t, hotRels, liveRel.ID())

	allIDs, err := ts.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatalf("AllRelIDs DepthAll: %v", err)
	}
	requireTieredRelIDs(t, allIDs, archivedRel.ID(), liveRel.ID())

	hotIDs, err := ts.AllRelIDs(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatalf("AllRelIDs DepthHot: %v", err)
	}
	requireTieredRelIDs(t, hotIDs, liveRel.ID())
}

func TestTieredStoreRelationshipCountSentinels(t *testing.T) {
	ts, _, _ := setupBatchDelete(t)

	if _, err := ts.RelCountByType(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("RelCountByType(0) = %v, want ErrInvalidStoreMutation", err)
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ts.RelationshipCount(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("RelationshipCount closed = %v, want ErrStoreClosed", err)
	}
	if _, err := ts.RelCountByType(1); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("RelCountByType closed = %v, want ErrStoreClosed", err)
	}
}

func requireTieredRelationshipIDs(t *testing.T, rels []*types.Relationship, want ...types.RelID) {
	t.Helper()
	ids := make([]types.RelID, 0, len(rels))
	for _, r := range rels {
		ids = append(ids, r.ID())
	}
	requireTieredRelIDs(t, ids, want...)
}

func requireTieredRelIDs(t *testing.T, got []types.RelID, want ...types.RelID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("relationship IDs = %v, want %v", got, want)
	}
	seen := make(map[types.RelID]struct{}, len(got))
	for _, id := range got {
		seen[id] = struct{}{}
	}
	for _, id := range want {
		if _, ok := seen[id]; !ok {
			t.Fatalf("relationship IDs = %v, missing %d", got, id)
		}
	}
}
