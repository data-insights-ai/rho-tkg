package memory

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A deleted relationship keeps its rel-type temporal envelope (append-only),
// so a version later written for it through PutRelVersion (a cascade's
// bounded correction, an import of its history) must join that envelope —
// a covered id whose envelope misses one of its rows would be pruned wrongly.
func TestRelTypeTemporalIndex_PutRelVersionExtendsDeletedCovered(t *testing.T) {
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
	r := types.NewRelationship(101, relType, 1, 2)
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ms.DeleteRelationship(101); err != nil {
		t.Fatal(err)
	}
	late := types.NewRelationship(101, relType, 1, 2)
	late.SetTemporal(&types.TemporalMetadata{ValidFrom: 7000, ValidTo: 8000})
	late.SetVersion(5)
	if err := ms.PutRelVersion(101, 5, late); err != nil {
		t.Fatal(err)
	}
	if kept, _ := ms.PruneRelTypeTemporalCandidates(relType, []types.RelID{101}, QueryOpts{ValidAt: 7500}); !slices.Equal(kept, []types.RelID{101}) {
		t.Errorf("at 7500 kept %v, want [101]: version 5 is a row of the covered relationship", kept)
	}
}
