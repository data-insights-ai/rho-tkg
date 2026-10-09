package badger

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A retention purge erases a relationship's history with its row, so nothing
// of it is left for an envelope to cover: both purge doors remove its rel-type
// temporal entry. A merely deleted relationship keeps its entry (append-only;
// its history stays).
func TestRelTypeTemporalIndex_RetentionPurgeRemovesEnvelope(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	const relType, label = uint16(6), uint16(1)
	for _, id := range []types.NodeID{1, 2, 3} {
		if err := bs.PutNode(types.NewNode(id, label, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}
	put := func(id types.RelID, start, end types.NodeID) {
		t.Helper()
		r := types.NewRelationship(id, relType, start, end)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	put(101, 1, 2) // purged by info
	put(102, 1, 3) // purged as adjacent to node 3
	put(103, 2, 1) // deleted only
	if err := bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: 101, TypeToken: relType, StartID: 1, EndID: 2}); err != nil {
		t.Fatalf("PurgeRelationshipByInfo: %v", err)
	}
	if n, err := bs.PurgeAdjacentRelsForNode(3); err != nil || n != 1 {
		t.Fatalf("PurgeAdjacentRelsForNode = %d, %v; want 1 removed", n, err)
	}
	if err := bs.DeleteRelationship(103); err != nil {
		t.Fatal(err)
	}
	covered := func(id types.RelID) bool {
		bs.idxMu.RLock()
		defer bs.idxMu.RUnlock()
		_, _, ok := bs.relTypeTemporalIndexes[relType].EnvelopeOf(id.SnowflakeID())
		return ok
	}
	for _, id := range []types.RelID{101, 102} {
		if covered(id) {
			t.Errorf("retention-purged relationship %d still has an envelope entry", id)
		}
	}
	if !covered(103) {
		t.Error("deleted relationship 103 lost its envelope entry")
	}
}
