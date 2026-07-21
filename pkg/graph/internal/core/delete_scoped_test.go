package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// These tests exercise the BACKLOG 11f Batch C call sites directly
// (deleteNodeInternal's Phase B closure via deleteNodeLocked /
// deleteRelationshipInternal on a fully wired *Core) — nothing in GraphTx
// constructs a token-carrying context yet, so this is the only way to
// observe the routing end-to-end today. Mirrors update_scoped_test.go's
// shape for Batch B's doors.

func TestDeleteNodeInternal_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	n, err := c.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token == 0 {
		t.Fatal("BeginScopedLog token = 0, want nonzero")
	}
	ctx := withScopeToken(context.Background(), token)

	if _, err := c.deleteNodeInternal(ctx, n.InternalID()); err != nil {
		t.Fatalf("deleteNodeInternal: %v", err)
	}

	// Load-bearing: invisible via the eager feed proves the record was
	// routed into the scope buffer (DeleteNodeWithHistoryScoped), not the
	// plain unscoped door.
	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — deleteNodeInternal did not route through DeleteNodeWithHistoryScoped", len(recs))
	}

	// The entity delete itself must land regardless of log scoping.
	if _, err := c.Nodes.Get(context.Background(), n.InternalID()); err == nil {
		t.Fatal("Nodes.Get after delete succeeded, want ErrNodeNotFound")
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

func TestDeleteNodeInternal_NoTokenRoutesThroughPlainDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	n, err := c.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	// No scope was ever opened, so a plain (non-scoped) call must be
	// immediately visible — regression guard: an accidental unconditional
	// scope check must not swallow the record.
	if _, err := c.deleteNodeInternal(context.Background(), n.InternalID()); err != nil {
		t.Fatalf("deleteNodeInternal: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}

func TestDeleteRelationshipInternal_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	n1, err := c.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Nodes.Add n1: %v", err)
	}
	n2, err := c.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Nodes.Add n2: %v", err)
	}
	r, err := c.Rels.Add(context.Background(), "KNOWS", n1, n2, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("Rels.Add: %v", err)
	}

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := ms.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	ctx := withScopeToken(context.Background(), token)

	if err := c.deleteRelationshipInternal(ctx, r.InternalID()); err != nil {
		t.Fatalf("deleteRelationshipInternal: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — deleteRelationshipInternal did not route through DeleteRelWithHistoryScoped", len(recs))
	}

	if _, err := c.Rels.Get(context.Background(), r.InternalID()); err == nil {
		t.Fatal("Rels.Get after delete succeeded, want ErrRelNotFound")
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

func TestDeleteRelationshipInternal_NoTokenRoutesThroughPlainDoor(t *testing.T) {
	ms := memory.New(memory.WithChangeLog())
	c, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	n1, err := c.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Nodes.Add n1: %v", err)
	}
	n2, err := c.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Nodes.Add n2: %v", err)
	}
	r, err := c.Rels.Add(context.Background(), "KNOWS", n1, n2, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("Rels.Add: %v", err)
	}

	before, err := ms.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	if err := c.deleteRelationshipInternal(context.Background(), r.InternalID()); err != nil {
		t.Fatalf("deleteRelationshipInternal: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}
