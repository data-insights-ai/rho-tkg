package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// These tests exercise the BACKLOG 11f Batch E cascade-door wiring end to
// end through the real cascadeNodeVersionInterval / cascadeRelVersionInterval
// entry points (via TempOps.SetNodeVersionInterval/SetRelVersionInterval,
// which forward ctx unchanged — the same function GraphTx.
// SetNodeVersionInterval/SetRelVersionInterval call) — not just the
// putNodeVersionScopedAware/replaceNodeScopedAware helpers in isolation.
// This proves the wiring added in temporal_cascade.go actually routes
// without needing to touch (or re-verify) the delicate BACKLOG 10b cascade
// algorithm itself.

func newTxTimeGraphWithChangeLog(t *testing.T) *Core {
	t.Helper()
	// Config.ChangeLog only threads through the DEFAULT badger construction
	// path (core.go) — the default in-memory store needs the change-log
	// enabled directly via memory.WithChangeLog(), injected as Config.Store.
	g, err := New(Config{AllowReset: true, Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestSetNodeVersionInterval_ScopedTokenRoutesThroughScopedDoors(t *testing.T) {
	c := newTxTimeGraphWithChangeLog(t)

	n, err := c.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"x":              int64(1),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	feed, ok := c.store.(storepkg.ChangeFeedCapability)
	if !ok {
		t.Fatal("store does not implement ChangeFeedCapability")
	}
	scoped, ok := c.store.(storepkg.ScopedTxChangeLog)
	if !ok {
		t.Fatal("store does not implement ScopedTxChangeLog")
	}

	before, err := feed.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := scoped.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	if token == 0 {
		t.Fatal("BeginScopedLog token = 0, want nonzero (log enabled)")
	}
	ctx := withScopeToken(context.Background(), token)

	if _, err := c.Temporal.SetNodeVersionInterval(ctx, n.ID(), 2000, 0, map[string]any{
		"x": int64(2),
	}); err != nil {
		t.Fatalf("SetNodeVersionInterval: %v", err)
	}

	// Load-bearing: invisible via the eager feed proves the cascade's
	// PutNodeVersion/ReplaceNode calls routed into the scope buffer, not the
	// plain eager doors — even though the cascade's data mutations (the new
	// history row + the new current row) have already landed.
	recs, err := feed.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — cascade did not route through the scoped doors", len(recs))
	}

	got, err := c.Temporal.NodeAt(n.ID(), 2500)
	if err != nil {
		t.Fatalf("NodeAt 2500: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(2) {
		t.Fatalf("at VT=2500 x = %v, want 2 — cascade mutation must land even though its log records are scoped", v)
	}

	if _, err := scoped.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = feed.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("feed has 0 records after commit, want at least 1")
	}
}

func TestSetNodeVersionInterval_NoTokenRoutesThroughPlainDoors(t *testing.T) {
	c := newTxTimeGraphWithChangeLog(t)

	n, err := c.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"x":              int64(1),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	feed, ok := c.store.(storepkg.ChangeFeedCapability)
	if !ok {
		t.Fatal("store does not implement ChangeFeedCapability")
	}
	before, err := feed.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	// No scope opened — context.Background() carries no token, so
	// scopeTokenFrom returns (0, false) and every cascade store call takes
	// the plain eager door, exactly as before Batch E.
	if _, err := c.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2000, 0, map[string]any{
		"x": int64(2),
	}); err != nil {
		t.Fatalf("SetNodeVersionInterval: %v", err)
	}

	recs, err := feed.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("feed has 0 records, want eager records (no scope opened)")
	}
}

func TestSetRelVersionInterval_ScopedTokenRoutesThroughScopedDoors(t *testing.T) {
	c := newTxTimeGraphWithChangeLog(t)

	n1, err := c.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add n1: %v", err)
	}
	n2, err := c.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add n2: %v", err)
	}
	r, err := c.Rels.Add(context.Background(), "REL", n1, n2, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"x":              int64(1),
	})
	if err != nil {
		t.Fatalf("Add rel: %v", err)
	}

	feed, ok := c.store.(storepkg.ChangeFeedCapability)
	if !ok {
		t.Fatal("store does not implement ChangeFeedCapability")
	}
	scoped, ok := c.store.(storepkg.ScopedTxChangeLog)
	if !ok {
		t.Fatal("store does not implement ScopedTxChangeLog")
	}

	before, err := feed.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	token, err := scoped.BeginScopedLog()
	if err != nil {
		t.Fatalf("BeginScopedLog: %v", err)
	}
	ctx := withScopeToken(context.Background(), token)

	if _, err := c.Temporal.SetRelVersionInterval(ctx, r.ID(), 2000, 0, map[string]any{
		"x": int64(2),
	}); err != nil {
		t.Fatalf("SetRelVersionInterval: %v", err)
	}

	recs, err := feed.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("record visible before CommitScopedLog (%d records) — rel cascade did not route through the scoped doors", len(recs))
	}

	got, err := c.Temporal.RelAt(r.ID(), 2500)
	if err != nil {
		t.Fatalf("RelAt 2500: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(2) {
		t.Fatalf("at VT=2500 x = %v, want 2 — cascade mutation must land even though its log records are scoped", v)
	}

	if _, err := scoped.CommitScopedLog(token); err != nil {
		t.Fatalf("CommitScopedLog: %v", err)
	}
	recs, err = feed.ChangeFeed(before, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("feed has 0 records after commit, want at least 1")
	}
}
