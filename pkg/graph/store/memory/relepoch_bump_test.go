package memory

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 17a: DeleteRelWithHistory, DeleteNodeCascade, and DeleteNodeWithHistory
// are all adjacency-mutating delete paths — the store's own doc comment on
// bumpRelEpoch says it is "called by every relationship-mutation path" — yet
// none of the three called it, despite bumpNodeEpoch being called correctly.
// A concurrent X5 expand-aggregation column reader's Gate-2 staleness re-check
// (RelMutationEpoch) would then pass even though adjacency had changed
// mid-scan, serving a torn aggregate as valid.

func TestDeleteRelWithHistory_BumpsRelMutationEpoch(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	nA := types.NewNode(types.NodeID(10), 1, nil)
	nB := types.NewNode(types.NodeID(20), 2, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}
	r := types.NewRelationship(types.RelID(100), 1, nA.ID(), nB.ID())
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	before := ms.RelMutationEpoch()

	now := types.Instant(time.Now().UnixMilli())
	tombR := r.DeepCopy()
	tm := tombR.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		tombR.SetTemporal(tm)
	}
	tm.DeletedAt, tm.ValidTo, tm.TxFrom, tm.TxTo = now, now, now, now

	if err := ms.DeleteRelWithHistory(r.ID(), r.Version(), tombR); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}

	if after := ms.RelMutationEpoch(); after == before {
		t.Fatalf("RelMutationEpoch unchanged (%d) after DeleteRelWithHistory — BACKLOG 17a regression: a concurrent X5 adjacency reader would see this delete as a no-op", after)
	}
}

func TestDeleteNodeCascade_BumpsRelMutationEpoch(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	nA := types.NewNode(types.NodeID(10), 1, nil)
	nB := types.NewNode(types.NodeID(20), 2, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}
	r := types.NewRelationship(types.RelID(100), 1, nA.ID(), nB.ID())
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	before := ms.RelMutationEpoch()

	if err := ms.DeleteNodeCascade(nA.ID()); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	if after := ms.RelMutationEpoch(); after == before {
		t.Fatalf("RelMutationEpoch unchanged (%d) after DeleteNodeCascade — BACKLOG 17a regression: cascade removed relationship 100 but a concurrent X5 adjacency reader would see this as a no-op", after)
	}
}

func TestDeleteNodeWithHistory_BumpsRelMutationEpoch(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	n := types.NewNode(types.NodeID(10), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	before := ms.RelMutationEpoch()

	now := types.Instant(time.Now().UnixMilli())
	tombN := n.DeepCopy()
	tmN := tombN.Temporal()
	if tmN == nil {
		tmN = &types.TemporalMetadata{}
		tombN.SetTemporal(tmN)
	}
	tmN.DeletedAt, tmN.ValidTo, tmN.TxFrom, tmN.TxTo = now, now, now, now

	// No connected relationships — still an adjacency-relevant delete door
	// (the store's own doc comment says bumpRelEpoch is called by EVERY
	// relationship-mutation path; a spurious bump on a rel-free node is safe
	// per the documented "coarse counter, spurious bump is harmless" design).
	if err := ms.DeleteNodeWithHistory(n.ID(), n.Version(), tombN, nil); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	if after := ms.RelMutationEpoch(); after == before {
		t.Fatalf("RelMutationEpoch unchanged (%d) after DeleteNodeWithHistory — BACKLOG 17a regression", after)
	}
}

// TestDeleteNodeWithHistory_BumpsRelMutationEpochWithConnectedRel covers the
// case that actually matters most: a node WITH a connected relationship,
// whose tombstoned removal is the real adjacency change the X5 staleness gate
// must catch.
func TestDeleteNodeWithHistory_BumpsRelMutationEpochWithConnectedRel(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	nA := types.NewNode(types.NodeID(10), 1, nil)
	nB := types.NewNode(types.NodeID(20), 2, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}
	r := types.NewRelationship(types.RelID(100), 1, nA.ID(), nB.ID())
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	before := ms.RelMutationEpoch()

	now := types.Instant(time.Now().UnixMilli())
	tombN := nA.DeepCopy()
	tmN := tombN.Temporal()
	if tmN == nil {
		tmN = &types.TemporalMetadata{}
		tombN.SetTemporal(tmN)
	}
	tmN.DeletedAt, tmN.ValidTo, tmN.TxFrom, tmN.TxTo = now, now, now, now

	tombR := r.DeepCopy()
	tmR := tombR.Temporal()
	if tmR == nil {
		tmR = &types.TemporalMetadata{}
		tombR.SetTemporal(tmR)
	}
	tmR.DeletedAt, tmR.ValidTo, tmR.TxFrom, tmR.TxTo = now, now, now, now

	relTombs := []RelTombstone{{ID: r.ID(), PrevVersion: r.Version(), Tombstone: tombR}}
	if err := ms.DeleteNodeWithHistory(nA.ID(), nA.Version(), tombN, relTombs); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	if after := ms.RelMutationEpoch(); after == before {
		t.Fatalf("RelMutationEpoch unchanged (%d) after DeleteNodeWithHistory removed connected rel 100 — BACKLOG 17a regression", after)
	}
}
