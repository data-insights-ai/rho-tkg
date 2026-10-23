package memory

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryPropertyNDVDoesNotRescan (round 5 request 1): after a delete
// removed the maximum, NodePropertyNDV / RelPropertyNDV return the NDV and
// Count the stats doors return and leave the accumulators dirty (no
// rescan). Faulty implementation caught: NDV served through the stats door.
func TestMemoryPropertyNDVDoesNotRescan(t *testing.T) {
	t.Parallel()
	ms := New()
	defer ms.Close()
	put := func(id int64, v int64) {
		n := types.NewNode(types.NodeID(id), 1, nil)
		if err := n.SetProperty("v", v); err != nil {
			t.Fatal(err)
		}
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	put(501, 1)
	put(502, 5)
	put(503, 100)
	putRel := func(id int64, w int64) {
		r := types.NewRelationship(types.RelID(id), 7, 501, 502)
		if err := r.SetProperty("w", w); err != nil {
			t.Fatal(err)
		}
		if err := ms.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	putRel(601, 3)
	putRel(603, 300)
	if err := ms.DeleteNode(503); err != nil {
		t.Fatal(err)
	}
	if err := ms.DeleteRelationship(603); err != nil {
		t.Fatal(err)
	}
	ndv, err := ms.NodePropertyNDV(1, "v")
	if err != nil {
		t.Fatal(err)
	}
	relNDV, err := ms.RelPropertyNDV(7, "w")
	if err != nil {
		t.Fatal(err)
	}
	if !ms.propertyStats[1]["v"].Dirty() || !ms.relPropertyStats[7]["w"].Dirty() {
		t.Fatal("the NDV doors rescanned (the accumulators are no longer dirty)")
	}
	stats, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatal(err)
	}
	if ndv != (storecontract.PropertyNDV{NDV: stats.NDV, Count: stats.Count}) || ndv.Count != 2 || stats.Max != int64(5) {
		t.Fatalf("NodePropertyNDV = %+v, NodePropertyStats = %+v", ndv, stats)
	}
	relStats, err := ms.RelPropertyStats(7, "w")
	if err != nil {
		t.Fatal(err)
	}
	if relNDV != (storecontract.PropertyNDV{NDV: relStats.NDV, Count: relStats.Count}) || relNDV.Count != 1 {
		t.Fatalf("RelPropertyNDV = %+v, RelPropertyStats = %+v", relNDV, relStats)
	}
	if got, err := ms.NodePropertyNDV(9, "v"); err != nil || got != (storecontract.PropertyNDV{}) {
		t.Fatalf("missing pair = %+v, %v", got, err)
	}
	if got, err := ms.RelPropertyNDV(9, "w"); err != nil || got != (storecontract.PropertyNDV{}) {
		t.Fatalf("missing rel pair = %+v, %v", got, err)
	}
	if _, err := ms.NodePropertyNDV(0, "v"); err == nil {
		t.Fatal("label token 0 accepted")
	}
	if _, err := ms.RelPropertyNDV(0, "w"); err == nil {
		t.Fatal("type token 0 accepted")
	}
	if _, err := ms.NodePropertyNDV(1, "tkg_valid_from"); !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("shadow key: %v", err)
	}
	if _, err := ms.RelPropertyNDV(7, "tkg_valid_from"); !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("shadow key: %v", err)
	}
	var nilStore *Store
	if _, err := nilStore.NodePropertyNDV(1, "v"); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store: %v", err)
	}
	if _, err := nilStore.RelPropertyNDV(1, "v"); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store: %v", err)
	}
	disabled := New(WithoutPlannerStats())
	defer disabled.Close()
	if _, err := disabled.NodePropertyNDV(1, "v"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("disabled: %v", err)
	}
	if _, err := disabled.RelPropertyNDV(1, "v"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("disabled: %v", err)
	}
}
