package core

import (
	"context"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// These tests exercise the BACKLOG 11f Batch A call sites directly (bare
// *Core with generatedCreate == nil, exactly like
// TestPutGeneratedRelationshipUsesGeneratedCreateCapability above) — nothing
// in GraphTx constructs a token-carrying context yet, so this is the only way
// to observe the routing end-to-end today. A store.ScopedPutCapability
// implementation is used as the observable: a record that lands in the
// store's scope buffer (invisible via ChangeFeed until CommitScopedLog) can
// only have arrived there via the *Scoped door, never the plain one.

func TestPutGeneratedNode_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
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
	if err := c.putGeneratedNode(ctx, n); err != nil {
		t.Fatalf("putGeneratedNode: %v", err)
	}

	// Load-bearing: invisible via the eager feed proves the record was routed
	// into the scope buffer (PutNodeScoped), not the plain eager door.
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — putGeneratedNode did not route through PutNodeScoped", len(recs))
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
}

func TestPutGeneratedNode_NoTokenRoutesThroughPlainDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms}

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := c.putGeneratedNode(context.Background(), n); err != nil {
		t.Fatalf("putGeneratedNode: %v", err)
	}

	// No scope was ever opened, so a plain (non-scoped) call must be
	// immediately visible — regression guard: an accidental unconditional
	// scope check must not swallow the record.
	recs, err := ms.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}

func TestPutGeneratedNode_TokenIgnoredWhenGeneratedCreateSet(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	rec := &generatedCreateRelationshipRecorder{}
	c := &Core{store: ms, generatedCreate: rec}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	ctx := withScopeToken(context.Background(), token)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	// generatedCreate.PutNodeGeneratedID panics on this recorder (see
	// generated_create_wrapper_test.go) — reaching it (rather than the store)
	// proves the c.generatedCreate != nil branch takes priority over scoped
	// routing, exactly as documented on putGeneratedNode.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from generatedCreateRelationshipRecorder.PutNodeGeneratedID (proves the generatedCreate branch ran, not the scoped store branch)")
		}
	}()
	_ = c.putGeneratedNode(ctx, n)
}

func TestPutGeneratedRelationship_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
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
	if err := c.putGeneratedRelationship(ctx, r); err != nil {
		t.Fatalf("putGeneratedRelationship: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — putGeneratedRelationship did not route through PutRelationshipScoped", len(recs))
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

// createRelWithTypeRollback's relPersistEndpointHashWrite branch (via
// putRelationshipEndpointHashWrite) routes through the generatedcreate scoped
// endpoint-hash capability when ctx carries a token.
func TestPutRelationshipEndpointHashWrite_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms, endpointHashWrite: ms}

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
	if _, _, err := c.putRelationshipEndpointHashWrite(ctx, r); err != nil {
		t.Fatalf("putRelationshipEndpointHashWrite: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — putRelationshipEndpointHashWrite did not route through the scoped capability", len(recs))
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

func TestPutRelationshipEndpointHashWrite_NoTokenRoutesThroughPlainDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c := &Core{store: ms, endpointHashWrite: ms}

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
	if _, _, err := c.putRelationshipEndpointHashWrite(context.Background(), r); err != nil {
		t.Fatalf("putRelationshipEndpointHashWrite: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}

// Compile-time reminder that memory.Store is expected to satisfy the
// endpoint-hash capabilities used above (keeps this test file honest if a
// future refactor narrows what it implements).
var (
	_ generatedcreate.RelationshipEndpointHashCapability       = (*memory.Store)(nil)
	_ generatedcreate.RelationshipEndpointHashScopedCapability = (*memory.Store)(nil)
)
