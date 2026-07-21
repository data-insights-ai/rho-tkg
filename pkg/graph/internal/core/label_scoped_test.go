package core

import (
	"context"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// These tests exercise the BACKLOG 11f Batch D label-door wiring —
// addNodeLabelTokenWithHistory / removeNodeLabelTokenWithHistory — the same
// way generated_create_scoped_test.go exercises putGeneratedNode/
// putGeneratedRelationship for Batch A. Nothing in GraphTx constructs a
// token-carrying context yet, so this is the only way to observe the
// routing end-to-end today.

func TestAddNodeLabelTokenWithHistory_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	ctx := withScopeToken(context.Background(), token)

	if err := c.addNodeLabelTokenWithHistory(ctx, n.ID(), 20, updated, prev.Version(), prev); err != nil {
		t.Fatalf("addNodeLabelTokenWithHistory: %v", err)
	}

	// Load-bearing: invisible via the eager feed proves the record was
	// routed into the scope buffer, not the plain eager door.
	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — addNodeLabelTokenWithHistory did not route through the scoped door", len(recs))
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records after commit, want 1", len(recs))
	}
	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(20) {
		t.Fatal("label mutation must have landed")
	}
}

func TestAddNodeLabelTokenWithHistory_NoTokenRoutesThroughPlainDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	if err := c.addNodeLabelTokenWithHistory(context.Background(), n.ID(), 20, updated, prev.Version(), prev); err != nil {
		t.Fatalf("addNodeLabelTokenWithHistory: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}

func TestRemoveNodeLabelTokenWithHistory_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, []uint16{20})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	ctx := withScopeToken(context.Background(), token)

	if err := c.removeNodeLabelTokenWithHistory(ctx, n.ID(), 20, updated, prev.Version(), prev); err != nil {
		t.Fatalf("removeNodeLabelTokenWithHistory: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — removeNodeLabelTokenWithHistory did not route through the scoped door", len(recs))
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records after commit, want 1", len(recs))
	}
	got, err := ms.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.HasLabelTokenRaw(20) {
		t.Fatal("label removal must have landed")
	}
}

func TestRemoveNodeLabelTokenWithHistory_NoTokenRoutesThroughPlainDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, []uint16{20})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.RemoveLabelTokenRaw(20)
	updated.SetVersion(prev.Version() + 1)

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	if err := c.removeNodeLabelTokenWithHistory(context.Background(), n.ID(), 20, updated, prev.Version(), prev); err != nil {
		t.Fatalf("removeNodeLabelTokenWithHistory: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}
