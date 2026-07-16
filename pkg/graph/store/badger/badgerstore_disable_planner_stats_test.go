package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBadgerStoreDisablePlannerStats mirrors the memory-store proof: with
// Config.DisablePlannerStats set, every planner-stat capability declines with
// ErrCapabilityNotSupported, the per-write maintenance sweep never runs, and
// ordinary node reads/writes stay byte-correct.
func TestBadgerStoreDisablePlannerStats(t *testing.T) {
	bs, err := New(Config{
		InMemory:            true,
		FlushInterval:       1<<63 - 1,
		DisablePlannerStats: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })

	n := types.NewNode(types.NodeID(snowflake.ID(101)), 1, []uint16{2})
	if err := n.SetProperty("id", int64(1)); err != nil {
		t.Fatalf("SetProperty n.id: %v", err)
	}
	if err := n.SetProperty("score", 3.5); err != nil {
		t.Fatalf("SetProperty n.score: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode n: %v", err)
	}

	// Data path is unaffected: the node round-trips verbatim.
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if v, ok := got.GetProperty("id"); !ok || v != int64(1) {
		t.Fatalf("round-trip id = (%v, %v), want (1, true)", v, ok)
	}

	// Every planner-stat capability declines.
	if _, err := bs.NodeCountByLabelAndPropertyKey(1, "id"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodeCountByLabelAndPropertyKey err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := bs.NodePropertyStats(1, "id"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodePropertyStats err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, _, _, _, err := bs.NodePropertyStatsSketch(1, "id"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodePropertyStatsSketch err = %v, want ErrCapabilityNotSupported", err)
	}
	if _, err := bs.NodePropertyTypeClassCounts(1, "score"); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatalf("NodePropertyTypeClassCounts err = %v, want ErrCapabilityNotSupported", err)
	}

	// The maintenance sweep never ran: every internal counter store is empty.
	var keyCount, typeClassCount int
	bs.propertyKeyCounts.Range(func(_, _ any) bool { keyCount++; return true })
	bs.propertyTypeClassCounts.Range(func(_, _ any) bool { typeClassCount++; return true })
	if keyCount != 0 {
		t.Fatalf("propertyKeyCounts populated despite disabled stats: %d entries", keyCount)
	}
	if typeClassCount != 0 {
		t.Fatalf("propertyTypeClassCounts populated despite disabled stats: %d entries", typeClassCount)
	}
	bs.idxMu.RLock()
	statsLen := len(bs.propertyStats)
	bs.idxMu.RUnlock()
	if statsLen != 0 {
		t.Fatalf("propertyStats populated despite disabled stats: %d entries", statsLen)
	}
}

// TestBadgerStoreDefaultPlannerStatsEnabled is the control.
func TestBadgerStoreDefaultPlannerStatsEnabled(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	n := types.NewNode(types.NodeID(snowflake.ID(201)), 1, []uint16{2})
	if err := n.SetProperty("id", int64(1)); err != nil {
		t.Fatalf("SetProperty n.id: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode n: %v", err)
	}

	if got, err := bs.NodeCountByLabelAndPropertyKey(2, "id"); err != nil || got != 1 {
		t.Fatalf("default NodeCountByLabelAndPropertyKey = (%d, %v), want (1, nil)", got, err)
	}
}
