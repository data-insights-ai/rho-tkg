package core

import (
	"context"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAsOfColumnCache_SurvivesForwardIngest is the core proof of the X5-temporal
// fix: an as-of column at a fixed past txAt is CACHED, and — unlike the current-state
// column cache — the cache SURVIVES write-active forward ingest (a new version has
// TxFrom = now > txAt, so it cannot change the past belief). Only a history rewrite
// invalidates it. Observed via pointer identity of the returned *LabelDocValues:
// same pointer == cache hit (no rebuild).
func TestAsOfColumnCache_SurvivesForwardIngest(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	var last *types.Node
	for i := 0; i < 3; i++ {
		n, err := g.Nodes.Add(ctx, []string{"Metric"}, map[string]any{"score": int64(i + 1)})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		last = n
	}
	t0 := txFromStamp(t, last.Temporal()) // all three believed to exist at t0

	col1, err := g.buildAsOfColumns("Metric", []string{"score"}, t0)
	if err != nil {
		t.Fatalf("build 1: %v", err)
	}
	if col1.Len() != 3 {
		t.Fatalf("as-of members at t0 = %d, want 3", col1.Len())
	}

	// Second build at the same txAt — cache HIT (identical pointer, no rebuild).
	col2, _ := g.buildAsOfColumns("Metric", []string{"score"}, t0)
	if col2 != col1 {
		t.Fatal("second build at same txAt was NOT a cache hit (different pointer)")
	}

	// Forward ingest: advance the clock and create MORE Metric nodes.
	if _, err := g.Temporal.AdvanceClock(t0 + 1_000_000); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := g.Nodes.Add(ctx, []string{"Metric"}, map[string]any{"score": int64(100 + i)}); err != nil {
			t.Fatalf("forward add %d: %v", i, err)
		}
	}

	// Build again at the SAME past txAt: the cache survived the ingest (same pointer),
	// and the past belief is unchanged (still exactly the original 3 members).
	col3, _ := g.buildAsOfColumns("Metric", []string{"score"}, t0)
	if col3 != col1 {
		t.Fatal("as-of cache did NOT survive forward ingest (rebuilt) — the whole point of the fix")
	}
	if col3.Len() != 3 {
		t.Fatalf("as-of belief at t0 after ingest = %d members, want 3 (forward writes must not leak into a past belief)", col3.Len())
	}
}

// TestAsOfColumnCache_HistoryRewriteInvalidates proves the cache is discarded when
// history is rewritten (here via the retention-purge watermark advance choke), so a
// stale past belief is never served after a rewrite.
func TestAsOfColumnCache_HistoryRewriteInvalidates(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Metric"}, map[string]any{"score": int64(1)})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	t0 := txFromStamp(t, n.Temporal())

	col1, err := g.buildAsOfColumns("Metric", []string{"score"}, t0)
	if err != nil {
		t.Fatalf("build 1: %v", err)
	}
	if col2, _ := g.buildAsOfColumns("Metric", []string{"score"}, t0); col2 != col1 {
		t.Fatal("expected cache hit before rewrite")
	}

	// A history-rewrite choke fires (every choke — compaction / retention purge /
	// truncate / backfill / past-dated apply — routes through this same bump).
	g.asOfColumns.bump()

	col3, err := g.buildAsOfColumns("Metric", []string{"score"}, t0)
	if err != nil {
		t.Fatalf("build after rewrite: %v", err)
	}
	if col3 == col1 {
		t.Fatal("as-of cache was NOT invalidated after a history-rewrite bump (stale belief risk)")
	}
}

// TestAsOfColumnCache_TxRollbackInvalidates is the BACKLOG 14a regression:
// GraphTx.Rollback rewrites history via direct store calls (ReplaceNode,
// DeleteNodeCascade, ...), bypassing the higher-level mutation doors that
// normally bump this cache — Rollback was the one history-rewrite site with
// no bump call. Under this graph's documented relaxed per-entity isolation
// (BeginTx does NOT hold c.mu for the tx's whole lifetime), a concurrent
// reader can observe and cache an open tx's in-flight, not-yet-committed
// state; without the bump, rolling that state back leaves the cache stale
// forever at that (label, txAt) pin.
func TestAsOfColumnCache_TxRollbackInvalidates(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	// A committed, pre-existing Metric node — present both before and after
	// the tx below, so the as-of member COUNT (not just pointer identity,
	// which an empty-result fast path could satisfy either way) distinguishes
	// a stale cache from a correctly-rebuilt one.
	existing, err := g.Nodes.Add(ctx, []string{"Metric"}, map[string]any{"score": int64(0)})
	if err != nil {
		t.Fatalf("add existing: %v", err)
	}
	t0 := txFromStamp(t, existing.Temporal())
	if _, err := g.Temporal.AdvanceClock(t0 + 1); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	txNode, err := tx.AddNode([]string{"Metric"}, map[string]any{"score": int64(1)})
	if err != nil {
		t.Fatalf("tx AddNode: %v", err)
	}
	tTx := txFromStamp(t, txNode.Temporal())

	// A concurrent reader observes the tx's in-flight (not-yet-committed) row
	// and caches an as-of column reflecting BOTH members — permitted under
	// this graph's relaxed per-entity isolation (the tx does not hold c.mu
	// for its lifetime).
	col1, err := g.buildAsOfColumns("Metric", []string{"score"}, tTx)
	if err != nil {
		t.Fatalf("build 1 (mid-tx): %v", err)
	}
	if col1.Len() != 2 {
		t.Fatalf("mid-tx as-of members at tTx = %d, want 2 (existing + in-flight tx node)", col1.Len())
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Post-rollback, the same (label, txAt) pin must be rebuilt (cache
	// invalidated) and reflect only the ONE node that actually ever existed —
	// the rolled-back tx node must not linger in a stale cached column.
	col2, err := g.buildAsOfColumns("Metric", []string{"score"}, tTx)
	if err != nil {
		t.Fatalf("build 2 (post-rollback): %v", err)
	}
	if col2 == col1 {
		t.Fatal("as-of cache was NOT invalidated by Rollback (stale pointer) — BACKLOG 14a regression")
	}
	if col2.Len() != 1 {
		t.Fatalf("post-rollback as-of members at tTx = %d, want 1 (rolled-back node must not appear — stale cache, BACKLOG 14a regression)", col2.Len())
	}
}

// TestAsOfColumnCache_RealChokesBumpEpoch verifies the history-rewrite choke points
// actually increment the epoch, so buildAsOfColumns's cache is discarded after each.
// (The invalidation MECHANISM is proven above; this proves the WIRING is present.)
func TestAsOfColumnCache_RealChokesBumpEpoch(t *testing.T) {
	ctx := context.Background()

	t.Run("retention-watermark", func(t *testing.T) {
		g, _ := New(Config{})
		defer g.Close()
		n, _ := g.Nodes.Add(ctx, []string{"Metric"}, nil)
		tok, _ := g.labels.Lookup("Metric")
		before := g.asOfColumns.currentEpoch()
		if err := g.advanceRetentionWatermark(tok, txFromStamp(t, n.Temporal())+1); err != nil {
			t.Fatalf("advanceRetentionWatermark: %v", err)
		}
		if g.asOfColumns.currentEpoch() == before {
			t.Fatal("advanceRetentionWatermark did not bump the as-of epoch")
		}
	})

	t.Run("compaction-watermark", func(t *testing.T) {
		g, _ := New(Config{})
		defer g.Close()
		before := g.asOfColumns.currentEpoch()
		if err := g.advanceCompactionWatermark(types.Instant(1 << 40)); err != nil {
			t.Fatalf("advanceCompactionWatermark: %v", err)
		}
		if g.asOfColumns.currentEpoch() == before {
			t.Fatal("advanceCompactionWatermark did not bump the as-of epoch")
		}
	})

	t.Run("backfill", func(t *testing.T) {
		g, _ := New(Config{AllowTxBackfill: true})
		defer g.Close()
		before := g.asOfColumns.currentEpoch()
		// A backfilled create stamps a past tkg_tx_from — the primary-side past-dated write.
		if _, err := g.Nodes.Add(ctx, []string{"Metric"}, map[string]any{"tkg_tx_from": types.Instant(1)}); err != nil {
			t.Fatalf("backfill add: %v", err)
		}
		if g.asOfColumns.currentEpoch() == before {
			t.Fatal("backfill did not bump the as-of epoch")
		}
	})

	t.Run("tx-rollback", func(t *testing.T) {
		g, _ := New(Config{})
		defer g.Close()
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if _, err := tx.AddNode([]string{"Metric"}, nil); err != nil {
			t.Fatalf("tx AddNode: %v", err)
		}
		before := g.asOfColumns.currentEpoch()
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		if g.asOfColumns.currentEpoch() == before {
			t.Fatal("GraphTx.Rollback did not bump the as-of epoch (BACKLOG 14a)")
		}
	})
}

// TestAsOfColumnCache_NoteAppliedTx_PastDatedBumps proves the replica rule directly:
// a forward apply (TxFrom >= max seen) leaves the epoch unchanged (cache stays warm);
// only an out-of-order (past-dated) apply bumps the epoch.
func TestAsOfColumnCache_NoteAppliedTx_PastDatedBumps(t *testing.T) {
	a := newAsOfColumnCache()
	base := a.currentEpoch()

	// Forward applies (monotonically increasing TxFrom) must NOT bump.
	for _, tf := range []types.Instant{100, 200, 200, 300} {
		a.noteAppliedTx(tf)
	}
	if a.currentEpoch() != base {
		t.Fatalf("forward applies bumped the epoch (%d -> %d) — the cache would go cold under normal replication", base, a.currentEpoch())
	}

	// An out-of-order (past-dated) apply MUST bump.
	a.noteAppliedTx(150) // < max seen (300)
	if a.currentEpoch() != base+1 {
		t.Fatalf("past-dated apply did not bump the epoch (%d -> %d)", base, a.currentEpoch())
	}

	// Non-positive TxFrom is ignored (no bump).
	after := a.currentEpoch()
	a.noteAppliedTx(0)
	if a.currentEpoch() != after {
		t.Fatal("zero TxFrom bumped the epoch")
	}
}

func benchAsOfSetup(b *testing.B, n int) (*Core, types.Instant) {
	b.Helper()
	g, err := New(Config{})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	var last *types.Node
	for i := 0; i < n; i++ {
		nd, err := g.Nodes.Add(ctx, []string{"Metric"}, map[string]any{"score": int64(i)})
		if err != nil {
			b.Fatalf("add: %v", err)
		}
		last = nd
	}
	tm := last.Temporal()
	if tm == nil || tm.TxFrom <= 0 {
		b.Fatalf("no TxFrom on last node")
	}
	return g, tm.TxFrom
}

func sumAsOf(b *testing.B, g *Core, txAt types.Instant) {
	var sum int64
	_, ok, err := g.Nodes.ForEachDocValuesAsOf("Metric", []string{"score"}, txAt, func(_ types.NodeID, vals []any, present []bool) bool {
		if present[0] {
			sum += vals[0].(int64)
		}
		return true
	})
	if err != nil || !ok {
		b.Fatalf("ForEachDocValuesAsOf: ok=%v err=%v", ok, err)
	}
}

// BenchmarkForEachDocValuesAsOfCached measures repeated same-txAt aggregation with the
// column cache warm (the realistic dashboard "AS OF SYSTEM TIME $t" pattern).
func BenchmarkForEachDocValuesAsOfCached(b *testing.B) {
	g, txAt := benchAsOfSetup(b, 20_000)
	defer g.Close()
	sumAsOf(b, g, txAt) // warm the cache
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sumAsOf(b, g, txAt)
	}
}

// BenchmarkForEachDocValuesAsOfUncached bumps the epoch before each call, forcing the
// full materialize-and-build every time — the pre-fix behavior (throwaway columns).
func BenchmarkForEachDocValuesAsOfUncached(b *testing.B) {
	g, txAt := benchAsOfSetup(b, 20_000)
	defer g.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.asOfColumns.bump()
		sumAsOf(b, g, txAt)
	}
}

// TestAsOfColumnCache_EvictionAndEpochGuard covers put's FIFO eviction (memory bound)
// and its epoch-race guard (a rewrite during the build discards the column).
func TestAsOfColumnCache_EvictionAndEpochGuard(t *testing.T) {
	a := newAsOfColumnCache()
	epoch := a.currentEpoch()
	emptyCol := func() *indexpkg.LabelDocValues {
		return indexpkg.BuildLabelDocValues(epoch, nil, nil, func(types.NodeID, string) (any, bool) { return nil, false })
	}

	// Fill past the cap → oldest entries evicted, size stays bounded.
	for i := 0; i < asOfCacheCap+10; i++ {
		a.put(asOfCacheKey{label: 1, txAt: int64(i)}, emptyCol(), epoch)
	}
	a.mu.Lock()
	size := len(a.cols)
	a.mu.Unlock()
	if size != asOfCacheCap {
		t.Fatalf("cache size = %d after overflow, want %d (FIFO eviction)", size, asOfCacheCap)
	}
	// The oldest key (txAt 0) was evicted; the newest is present.
	if _, ok := a.get(asOfCacheKey{label: 1, txAt: 0}, nil, epoch); ok {
		t.Fatal("oldest entry survived eviction")
	}
	if _, ok := a.get(asOfCacheKey{label: 1, txAt: int64(asOfCacheCap + 9)}, nil, epoch); !ok {
		t.Fatal("newest entry missing")
	}

	// Epoch-race guard: a put stamped with a now-stale epoch is dropped.
	stale := a.currentEpoch()
	a.bump()
	a.put(asOfCacheKey{label: 2, txAt: 1}, emptyCol(), stale)
	if _, ok := a.get(asOfCacheKey{label: 2, txAt: 1}, nil, a.currentEpoch()); ok {
		t.Fatal("a column built under a stale epoch was cached (torn-belief risk)")
	}
}

// TestNoteAppliedTxFrom_NilSafe covers the nil-metadata guard.
func TestNoteAppliedTxFrom_NilSafe(t *testing.T) {
	g, _ := New(Config{})
	defer g.Close()
	before := g.asOfColumns.currentEpoch()
	g.noteAppliedTxFrom(nil) // must not panic or bump
	if g.asOfColumns.currentEpoch() != before {
		t.Fatal("nil metadata bumped the epoch")
	}
}
