package memory

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryStoreDisablePlannerStats proves the WithoutPlannerStats option
// (1) makes every planner-stat capability decline with ErrCapabilityNotSupported,
// (2) skips the per-write counter maintenance, and (3) leaves ordinary
// node reads/writes fully correct — the flag is a pure planner-stats opt-out,
// never a data-path change.
func TestMemoryStoreDisablePlannerStats(t *testing.T) {
	t.Parallel()
	ms := New(WithoutPlannerStats())
	t.Cleanup(func() { _ = ms.Close() })

	n := types.NewNode(types.NodeID(snowflake.ID(101)), 1, []uint16{2})
	if err := n.SetProperty("id", int64(1)); err != nil {
		t.Fatalf("SetProperty n.id: %v", err)
	}
	if err := n.SetProperty("score", 3.5); err != nil {
		t.Fatalf("SetProperty n.score: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode n: %v", err)
	}

	// Data path is unaffected: the node round-trips verbatim.
	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if v, ok := got.GetProperty("id"); !ok || v != int64(1) {
		t.Fatalf("round-trip id = (%v, %v), want (1, true)", v, ok)
	}

	// Every planner-stat capability declines, at every arity.
	if _, err := ms.NodeCountByLabelAndPropertyKey(1, "id"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodeCountByLabelAndPropertyKey err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := ms.NodePropertyStats(1, "id"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodePropertyStats err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, _, _, _, err := ms.NodePropertyStatsSketch(1, "id"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodePropertyStatsSketch err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := ms.NodePropertyTypeClassCounts(1, "score"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodePropertyTypeClassCounts err = %v, want ErrCapabilityNotSupported", err)
	}

	// The maintenance sweep never ran: the internal counter maps stay empty.
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.propertyKeyCounts) != 0 {
		t.Fatalf("propertyKeyCounts populated despite disabled stats: %v", ms.propertyKeyCounts)
	}
	if len(ms.propertyStats) != 0 {
		t.Fatalf("propertyStats populated despite disabled stats: %v", ms.propertyStats)
	}
	if len(ms.propertyTypeClassCounts) != 0 {
		t.Fatalf("propertyTypeClassCounts populated despite disabled stats: %v", ms.propertyTypeClassCounts)
	}
}

// TestMemoryStoreDefaultPlannerStatsEnabled is the control: without the option,
// the same writes DO populate the counters and the capabilities answer.
func TestMemoryStoreDefaultPlannerStatsEnabled(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n := types.NewNode(types.NodeID(snowflake.ID(201)), 1, []uint16{2})
	if err := n.SetProperty("id", int64(1)); err != nil {
		t.Fatalf("SetProperty n.id: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode n: %v", err)
	}

	if got, err := ms.NodeCountByLabelAndPropertyKey(2, "id"); err != nil || got != 1 {
		t.Fatalf("default NodeCountByLabelAndPropertyKey = (%d, %v), want (1, nil)", got, err)
	}
}
