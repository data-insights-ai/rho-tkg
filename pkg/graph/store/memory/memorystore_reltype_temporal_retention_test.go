package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A retention purge erases a relationship's history with its row, so its
// rel-type temporal entry goes too; a merely deleted relationship keeps its
// entry (append-only; its history stays).
func TestRelTypeTemporalIndex_RetentionPurgeRemovesEnvelope(t *testing.T) {
	ms := New()
	const relType, aged, kept = uint16(6), uint16(1), uint16(2)
	nodes := map[types.NodeID]uint16{1: aged, 2: kept, 3: kept}
	for id, label := range nodes {
		if err := ms.PutNode(types.NewNode(id, label, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}
	put := func(id types.RelID, start, end types.NodeID) {
		t.Helper()
		r := types.NewRelationship(id, relType, start, end)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
		if err := ms.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	put(101, 1, 2) // purged with node 1
	put(103, 2, 3) // deleted only
	if err := ms.DeleteRelationship(103); err != nil {
		t.Fatal(err)
	}
	if res, err := ms.PurgeNodesByLabelBefore(aged, types.Instant(1)<<60, 10); err != nil || res.NodesPurged != 1 {
		t.Fatalf("PurgeNodesByLabelBefore = %+v, %v; want one node purged", res, err)
	}
	covered := func(id types.RelID) bool {
		ms.mu.RLock()
		defer ms.mu.RUnlock()
		_, _, ok := ms.relTypeTemporalIndexes[relType].EnvelopeOf(id.SnowflakeID())
		return ok
	}
	if covered(101) {
		t.Error("retention-purged relationship 101 still has an envelope entry")
	}
	if !covered(103) {
		t.Error("deleted relationship 103 lost its envelope entry")
	}
}
