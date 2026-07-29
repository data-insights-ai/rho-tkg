package badger

import (
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Deterministic commit-window guards for the TemporalMetaHistoryCapability
// scan (historyTemporalMetaByPrefix) — the selection-skeleton substrate of the
// core historical-pin fast path. Same system property as
// TestFlushingCommitWindow_GetNodeHistory_NoDropAcrossFlush: a concurrent
// flush() that commits parked rows and clears `flushing` inside the reader's
// window must not make a version vanish from the temporal-meta enumeration —
// the overlay is captured BEFORE the badger View (lesson 64 ordering), and
// historyScanTestHook fires exactly in between so the race lands on demand.

func TestFlushingCommitWindow_NodeHistoryTemporalMeta_NoDropAcrossFlush(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	cur := types.NewNode(types.NodeID(1), 10, nil)
	cur.SetVersion(1)
	if err := bs.PutNode(cur); err != nil {
		t.Fatalf("PutNode(current): %v", err)
	}
	v0 := types.NewNode(types.NodeID(1), 10, nil)
	v0.SetVersion(0)
	v0.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200, TxFrom: 300, TxTo: 400})
	if err := bs.PutNodeVersion(types.NodeID(1), 0, v0); err != nil {
		t.Fatalf("PutNodeVersion(v0): %v", err)
	}

	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	metas, err := bs.NodeHistoryTemporalMeta(types.NodeID(1))
	if err != nil {
		t.Fatalf("NodeHistoryTemporalMeta: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("NodeHistoryTemporalMeta dropped a version across the commit window: got %d, want 1 (v0)", len(metas))
	}
	if metas[0].Version != 0 || metas[0].Temporal == nil || metas[0].Temporal.TxFrom != 300 || metas[0].Temporal.ValidFrom != 100 {
		t.Fatalf("NodeHistoryTemporalMeta returned wrong skeleton: %+v", metas[0])
	}
}

func TestFlushingCommitWindow_RelHistoryTemporalMeta_NoDropAcrossFlush(t *testing.T) {
	bs := newFlushParkStore(t, nil)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	cur := types.NewRelationship(types.RelID(7), 5, types.NodeID(1), types.NodeID(2))
	cur.SetVersion(1)
	if err := bs.PutRelationship(cur); err != nil {
		t.Fatalf("PutRelationship(current): %v", err)
	}
	v0 := types.NewRelationship(types.RelID(7), 5, types.NodeID(1), types.NodeID(2))
	v0.SetVersion(0)
	v0.SetTemporal(&types.TemporalMetadata{ValidFrom: 11, TxFrom: 22, TxTo: 33})
	if err := bs.PutRelVersion(types.RelID(7), 0, v0); err != nil {
		t.Fatalf("PutRelVersion(v0): %v", err)
	}

	parkPendingIntoFlushing(t, bs)

	var once sync.Once
	bs.historyScanTestHook = func() {
		once.Do(func() { commitFlushingToBadger(t, bs) })
	}
	defer func() { bs.historyScanTestHook = nil }()

	metas, err := bs.RelHistoryTemporalMeta(types.RelID(7))
	if err != nil {
		t.Fatalf("RelHistoryTemporalMeta: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("RelHistoryTemporalMeta dropped a version across the commit window: got %d, want 1 (v0)", len(metas))
	}
	if metas[0].Version != 0 || metas[0].Temporal == nil || metas[0].Temporal.TxFrom != 22 || metas[0].Temporal.ValidFrom != 11 {
		t.Fatalf("RelHistoryTemporalMeta returned wrong skeleton: %+v", metas[0])
	}
}
