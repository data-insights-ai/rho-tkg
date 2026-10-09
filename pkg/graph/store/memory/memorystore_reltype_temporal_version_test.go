package memory

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PutRelVersion joins a LIVE relationship's past version into its rel-type
// temporal envelope (an imported past version stays findable), and leaves a
// relationship without a current row uncovered (never pruned) instead of
// vouching for one version while its other rows stay outside.
func TestRelTypeTemporalIndex_PutRelVersionExtendsLiveOnly(t *testing.T) {
	ms := New()
	const relType, label = uint16(6), uint16(1)
	for _, id := range []types.NodeID{1, 2} {
		if err := ms.PutNode(types.NewNode(id, label, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.CreateRelTemporalIndex(relType); err != nil {
		t.Fatal(err)
	}
	live := types.NewRelationship(101, relType, 1, 2)
	live.SetTemporal(&types.TemporalMetadata{ValidFrom: 3000})
	live.SetVersion(2)
	if err := ms.PutRelationship(live); err != nil {
		t.Fatal(err)
	}
	past := types.NewRelationship(101, relType, 1, 2)
	past.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	past.SetVersion(1)
	if err := ms.PutRelVersion(101, 1, past); err != nil {
		t.Fatal(err)
	}
	orphan := types.NewRelationship(102, relType, 1, 2) // history only, no current row
	orphan.SetTemporal(&types.TemporalMetadata{ValidFrom: 8000, ValidTo: 9000})
	orphan.SetVersion(3)
	if err := ms.PutRelVersion(102, 3, orphan); err != nil {
		t.Fatal(err)
	}
	ids := []types.RelID{101, 102}
	kept, ok := ms.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 1500})
	if !ok || !slices.Equal(kept, ids) {
		t.Errorf("at 1500 kept %v (ok=%v), want both: 101's past version and the uncovered 102", kept, ok)
	}
	kept, _ = ms.PruneRelTypeTemporalCandidates(relType, ids, QueryOpts{ValidAt: 500})
	if !slices.Equal(kept, []types.RelID{102}) {
		t.Errorf("at 500 kept %v, want [102] (101's envelope starts at 1000, 102 is not covered)", kept)
	}
	ms.mu.RLock()
	_, _, covered := ms.relTypeTemporalIndexes[relType].EnvelopeOf(102)
	ms.mu.RUnlock()
	if covered {
		t.Error("a relationship without a current row got an envelope from one history version")
	}
}
