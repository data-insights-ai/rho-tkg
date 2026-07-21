package core

import (
	"context"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// These tests exercise the BACKLOG 11f Batch D import-door wiring —
// putImportedNode / putImportedRelationship — the same way
// generated_create_scoped_test.go exercises putGeneratedNode/
// putGeneratedRelationship for Batch A. Nothing in GraphTx constructs a
// token-carrying context yet, so this is the only way to observe the
// routing end-to-end today.

func TestPutImportedNode_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token == 0 {
		t.Fatal("BeginScopedLog token = 0, want nonzero")
	}
	ctx := withScopeToken(context.Background(), token)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := c.putImportedNode(ctx, n); err != nil {
		t.Fatalf("putImportedNode: %v", err)
	}

	// Load-bearing: invisible via the eager feed proves the record was
	// routed into the scope buffer (PutNodeScoped), not the plain eager door.
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — putImportedNode did not route through PutNodeScoped", len(recs))
	}

	if _, err := ms.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records after commit, want 1", len(recs))
	}
	if got, err := ms.GetNode(n.ID()); err != nil || got == nil {
		t.Fatalf("GetNode: %v, %v", got, err)
	}
}

func TestPutImportedNode_NoTokenRoutesThroughPlainDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := c.putImportedNode(context.Background(), n); err != nil {
		t.Fatalf("putImportedNode: %v", err)
	}

	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}

func TestPutImportedRelationship_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	if err := ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	before, err := ms.LastCommittedLSN() // the 2 node-create records, eager
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	ctx := withScopeToken(context.Background(), token)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := c.putImportedRelationship(ctx, r); err != nil {
		t.Fatalf("putImportedRelationship: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — putImportedRelationship did not route through PutRelationshipScoped", len(recs))
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
}

func TestPutImportedRelationship_NoTokenRoutesThroughPlainDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	if err := ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ms.PutNode(types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := c.putImportedRelationship(context.Background(), r); err != nil {
		t.Fatalf("putImportedRelationship: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}
