package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// These tests exercise the BACKLOG 11f Batch B call sites directly
// (updateNodePreparedInternal / updateRelationshipPreparedInternal on a fully
// wired *Core) — nothing in GraphTx constructs a token-carrying context yet,
// so this is the only way to observe the routing end-to-end today. Mirrors
// generated_create_scoped_test.go's shape for Batch A's doors 1-3.

func TestUpdateNodePreparedInternal_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
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

	if _, _, err := c.updateNodePreparedInternal(ctx, n.InternalID(), updateProvenance{}, updateTemporal{}, map[string]any{"name": "Bob"}); err != nil {
		t.Fatalf("updateNodePreparedInternal: %v", err)
	}

	// Load-bearing: invisible via the eager feed proves the record was
	// routed into the scope buffer (ReplaceNodeWithHistoryScoped), not the
	// plain unscoped door.
	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — updateNodePreparedInternal did not route through ReplaceNodeWithHistoryScoped", len(recs))
	}

	// The entity write itself must land regardless of log scoping.
	got, err := c.Nodes.Get(context.Background(), n.InternalID())
	if err != nil {
		t.Fatalf("Nodes.Get: %v", err)
	}
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("got name=%v, want Bob", got.PropertiesMap()["name"])
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

func TestUpdateNodePreparedInternal_NoTokenRoutesThroughPlainDoor(t *testing.T) {
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
	if _, _, err := c.updateNodePreparedInternal(context.Background(), n.InternalID(), updateProvenance{}, updateTemporal{}, map[string]any{"name": "Bob"}); err != nil {
		t.Fatalf("updateNodePreparedInternal: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}

func TestUpdateRelationshipPreparedInternal_ScopedTokenRoutesThroughScopedDoor(t *testing.T) {
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

	if _, _, err := c.updateRelationshipPreparedInternal(ctx, r.InternalID(), updateProvenance{}, updateTemporal{}, map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("updateRelationshipPreparedInternal: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — updateRelationshipPreparedInternal did not route through ReplaceRelWithHistoryScoped", len(recs))
	}

	got, err := c.Rels.Get(context.Background(), r.InternalID())
	if err != nil {
		t.Fatalf("Rels.Get: %v", err)
	}
	if got.PropertiesMap()["weight"] != int64(2) {
		t.Fatalf("got weight=%v, want 2", got.PropertiesMap()["weight"])
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

func TestUpdateRelationshipPreparedInternal_NoTokenRoutesThroughPlainDoor(t *testing.T) {
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

	if _, _, err := c.updateRelationshipPreparedInternal(context.Background(), r.InternalID(), updateProvenance{}, updateTemporal{}, map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("updateRelationshipPreparedInternal: %v", err)
	}

	recs, err := ms.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("feed has %d records, want 1 (eager, no scope opened)", len(recs))
	}
}
