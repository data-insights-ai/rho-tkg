package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestApplyChangeRecord_ChangeClearReapsCoreStateLikeReset pins BACKLOG 12a: a
// replica applying a ChangeClear record must end up in the SAME state as a
// primary calling Admin.Reset() — not merely with an empty Store. Reset()
// clears several Core-level in-memory/derived-state fields that a bare
// store.Clear() cannot reach (the as-of DocValues cache, named as-of tags,
// unique-constraint definitions, UniqueForever ownership claims, the
// compaction watermark, the retention watermark, and op counters). Before this
// fix, applyChangeRecordLocked's ChangeClear case called only store.Clear(),
// leaving all of that Core-level state stale on the replica.
func TestApplyChangeRecord_ChangeClearReapsCoreStateLikeReset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 1. Op counters: exercise at least one.
	a, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"email": "a@example.com"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}

	// 2. Unique constraint (in-memory + durable definition).
	if err := g.Constraints.CreateUnique(ctx, "Person", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}
	if !g.hasUniqueConstraints.Load() {
		t.Fatal("hasUniqueConstraints not set after CreateUnique — test setup broken")
	}

	// 3. UniqueForever owner claim.
	if err := g.Constraints.CreateUniqueForever(ctx, "Person", "ssn"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"ssn": "123-45-6789"}); err != nil {
		t.Fatalf("AddNode with forever-constrained value: %v", err)
	}
	if len(g.uniqueOwners) == 0 {
		t.Fatal("uniqueOwners empty after CreateUniqueForever + claim — test setup broken")
	}

	// 4. Named as-of tag.
	if err := g.Temporal.TagAsOf("before-clear", types.Instant(1000)); err != nil {
		t.Fatalf("TagAsOf: %v", err)
	}
	if tags, err := g.Temporal.AsOfTags(); err != nil || len(tags) == 0 {
		t.Fatalf("AsOfTags after TagAsOf = (%v, %v), want a non-empty map — test setup broken", tags, err)
	}

	// 5. Compaction / retention watermarks (simulated directly — exercising a
	// real compaction/purge run is out of scope for this reap-logic test).
	g.compactedThroughTx.Store(12345)
	g.retentionMaxWatermark.Store(6789)

	before, _ := g.Stats.Get()
	if before.NodesAdded == 0 {
		t.Fatal("NodesAdded counter is 0 before Clear — test setup broken")
	}

	_ = a // silence unused if node identity isn't needed further

	// Apply a synthetic ChangeClear, exactly as a replica tailing a primary's
	// Admin.Reset() would.
	if err := g.Repl.ApplyChange(storepkg.ChangeRecord{LSN: 1, Tag: storepkg.ChangeClear}); err != nil {
		t.Fatalf("ApplyChange(ChangeClear): %v", err)
	}

	if got, _ := g.Stats.Get(); got.NodesAdded != 0 {
		t.Fatalf("NodesAdded after ChangeClear apply = %d, want 0 (op counters not reaped)", got.NodesAdded)
	}
	if g.hasUniqueConstraints.Load() {
		t.Fatal("hasUniqueConstraints still true after ChangeClear apply — unique constraints not reaped")
	}
	if len(g.uniqueConstraints) != 0 {
		t.Fatalf("uniqueConstraints map after ChangeClear apply = %v, want empty", g.uniqueConstraints)
	}
	if len(g.uniqueOwners) != 0 {
		t.Fatalf("uniqueOwners after ChangeClear apply = %v, want empty (UniqueForever claims not reaped)", g.uniqueOwners)
	}
	if tags, err := g.Temporal.AsOfTags(); err != nil || len(tags) != 0 {
		t.Fatalf("AsOfTags after ChangeClear apply = (%v, %v), want (empty map, nil) — as-of tags not reaped", tags, err)
	}
	if got := g.compactedThroughTx.Load(); got != 0 {
		t.Fatalf("compactedThroughTx after ChangeClear apply = %d, want 0 — compaction watermark not reaped", got)
	}
	if got := g.retentionMaxWatermark.Load(); got != 0 {
		t.Fatalf("retentionMaxWatermark after ChangeClear apply = %d, want 0 — retention watermark not reaped", got)
	}

	// The entities themselves must also be gone (the pre-existing property this
	// test extends).
	if count, err := g.Nodes.Count(); err != nil || count != 0 {
		t.Fatalf("node count after ChangeClear apply = (%d, %v), want (0, nil)", count, err)
	}
}
