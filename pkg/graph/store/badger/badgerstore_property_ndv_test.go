package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBadgerPropertyNDVDoesNotRescan (round 5 request 1): after a delete
// removed the maximum, NodePropertyNDV / RelPropertyNDV return the NDV and
// Count NodePropertyStats / RelPropertyStats return, without the rescan
// (rescanTestHook never fires, and the accumulator stays dirty for the next
// NodePropertyStats call). Faulty implementation caught: NDV served through
// the stats door (which rescans).
func TestBadgerPropertyNDVDoesNotRescan(t *testing.T) {
	bs := newTestBadgerStore(t)
	rescans := 0
	bs.rescanTestHook = func(int) { rescans++ }
	put := func(id int64, v int64) {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
		if err := n.SetProperty("v", v); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	put(501, 1)
	put(502, 5)
	put(503, 100)
	putRel := func(id int64, w int64) {
		r := types.NewRelationship(types.RelID(snowflake.ID(id)), 7, types.NodeID(snowflake.ID(501)), types.NodeID(snowflake.ID(502)))
		if err := r.SetProperty("w", w); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	putRel(601, 3)
	putRel(603, 300)
	if err := bs.DeleteNode(types.NodeID(snowflake.ID(503))); err != nil {
		t.Fatal(err)
	}
	if err := bs.DeleteRelationship(types.RelID(snowflake.ID(603))); err != nil {
		t.Fatal(err)
	}

	ndv, err := bs.NodePropertyNDV(1, "v")
	if err != nil {
		t.Fatal(err)
	}
	relNDV, err := bs.RelPropertyNDV(7, "w")
	if err != nil {
		t.Fatal(err)
	}
	if rescans != 0 {
		t.Fatalf("PropertyNDV rescanned %d times", rescans)
	}
	stats, err := bs.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatal(err)
	}
	if rescans != 1 {
		t.Fatalf("NodePropertyStats after the NDV call rescanned %d times, want 1 (the NDV call must leave it dirty)", rescans)
	}
	if ndv != (storecontract.PropertyNDV{NDV: stats.NDV, Count: stats.Count}) || ndv.Count != 2 || ndv.NDV < 2 {
		t.Fatalf("NodePropertyNDV = %+v, NodePropertyStats = %+v", ndv, stats)
	}
	relStats, err := bs.RelPropertyStats(7, "w")
	if err != nil {
		t.Fatal(err)
	}
	if relNDV != (storecontract.PropertyNDV{NDV: relStats.NDV, Count: relStats.Count}) || relNDV.Count != 1 {
		t.Fatalf("RelPropertyNDV = %+v, RelPropertyStats = %+v", relNDV, relStats)
	}
	sketch, count, err := bs.NodePropertyNDVSketch(1, "v")
	if err != nil || count != 2 || sketch.Estimate() != stats.NDV {
		t.Fatalf("NodePropertyNDVSketch = (%v, %d, %v)", sketch, count, err)
	}
	if got, err := bs.NodePropertyNDV(1, "missing"); err != nil || got != (storecontract.PropertyNDV{}) {
		t.Fatalf("missing pair = %+v, %v", got, err)
	}
	if sk, c, err := bs.NodePropertyNDVSketch(1, "missing"); err != nil || sk != nil || c != 0 {
		t.Fatalf("missing pair sketch = %v, %d, %v", sk, c, err)
	}
	if _, err := bs.NodePropertyNDV(0, "v"); err == nil {
		t.Fatal("label token 0 accepted")
	}
	if _, err := bs.RelPropertyNDV(0, "w"); err == nil {
		t.Fatal("type token 0 accepted")
	}
	if _, err := bs.RelPropertyNDV(7, "tkg_valid_from"); !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("shadow key: %v", err)
	}
	if _, err := bs.NodePropertyNDV(1, "tkg_valid_from"); !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("shadow key: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.NodePropertyNDV(1, "v"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed NodePropertyNDV: %v", err)
	}
	if _, err := bs.RelPropertyNDV(7, "w"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed RelPropertyNDV: %v", err)
	}
	if _, _, err := bs.NodePropertyNDVSketch(1, "v"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed NodePropertyNDVSketch: %v", err)
	}
}

// TestBadgerPropertyNDVDisabledPlannerStats: a store opened without planner
// statistics refuses the NDV doors as it refuses the stats doors.
func TestBadgerPropertyNDVDisabledPlannerStats(t *testing.T) {
	bs, err := New(Config{InMemory: true, DisablePlannerStats: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if _, err := bs.NodePropertyNDV(1, "v"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodePropertyNDV: %v", err)
	}
	if _, err := bs.RelPropertyNDV(1, "v"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("RelPropertyNDV: %v", err)
	}
	if _, _, err := bs.NodePropertyNDVSketch(1, "v"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodePropertyNDVSketch: %v", err)
	}
}
