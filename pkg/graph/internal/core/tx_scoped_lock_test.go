package core

import (
	"context"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// These tests exercise the BACKLOG 11f lock-relaxation flip itself:
// GraphTx.lockActiveCoreWrite uses a shared c.mu.RLock (instead of the
// legacy exclusive c.mu.Lock + SetLogDivert) for any store implementing the
// full storepkg.ScopedTxCapability (memory and badger, as of Batch F).
// Stores that do NOT implement it (tiered, sharded) correctly keep using the
// legacy exclusive mechanism — the flip must never silently apply to a
// store with only partial Scoped support. See tx.go's usesSharedLock /
// lockActiveCoreWrite doc comments for the full design.

func newTxTestGraphWithChangeLog(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{AllowReset: true, Store: memory.New(memory.WithChangeLog())})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

// ─── The store-capability decision itself ───────────────────────────────────

func TestGraphTx_UsesSharedLock_MemoryWithChangeLog(t *testing.T) {
	t.Parallel()
	g := newTxTestGraphWithChangeLog(t)
	if g.scopedChangeLog == nil {
		t.Fatal("Core.scopedChangeLog is nil for a memory store with WithChangeLog() — memory must implement the full storepkg.ScopedTxCapability")
	}
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if tx.scopeToken == 0 {
		t.Fatal("BeginTx did not open a nonzero scope token for a change-log-enabled ScopedTxCapability store")
	}
	if !tx.usesSharedLock() {
		t.Fatal("usesSharedLock() = false, want true for a store implementing the full ScopedTxCapability")
	}
}

func TestGraphTx_UsesSharedLock_NoChangeLog(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t) // no WithChangeLog — log disabled entirely
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	// scopedChangeLog may or may not be set (memory always implements the
	// interface), but BeginScopedLog returns token 0 when the log is off —
	// either way usesSharedLock must be true (nothing to divert).
	if !tx.usesSharedLock() {
		t.Fatal("usesSharedLock() = false, want true when the change-log is disabled")
	}
}

// TestGraphTx_UsesSharedLock_PartialCapabilityStoreFallsBack is the defense-
// in-depth proof for ScopedTxCapability's central safety invariant: a store
// that supports the legacy TxChangeLogScope mechanism but NOT the full
// BACKLOG 11f Scoped contract (tiered, today) must NEVER be granted the fast
// path — see ScopedTxCapability's doc comment on why a partial grant would
// leak a rolled-back record into the durable feed.
func TestGraphTx_UsesSharedLock_PartialCapabilityStoreFallsBack(t *testing.T) {
	t.Parallel()
	ts, err := tiered.New(tiered.Config{
		InMemory:    true,
		RefLabels:   []string{"Ref"},
		ChangeLog:   true,
		ShardWindow: 7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })

	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	if g.scopedChangeLog != nil {
		t.Fatal("Core.scopedChangeLog must be nil for tiered — it does not implement the full storepkg.ScopedTxCapability")
	}
	if g.txLogScope == nil {
		t.Fatal("Core.txLogScope must be non-nil for a change-log-enabled tiered store — the legacy mechanism must still be available")
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if tx.scopeToken != 0 {
		t.Fatal("scopeToken must stay 0 when the store has no scopedChangeLog")
	}
	if tx.usesSharedLock() {
		t.Fatal("usesSharedLock() = true, want false — a partial-capability store (tiered) must keep using the legacy exclusive-lock mechanism")
	}
}

// ─── The actual concurrency claim: a shared-lock tx mutation does not block
// a concurrent standalone write for its duration ───────────────────────────

func TestGraphTx_ScopedChangeLog_ConcurrentStandaloneWriteDoesNotBlock(t *testing.T) {
	t.Parallel()
	g := newTxTestGraphWithChangeLog(t)
	ctx := context.Background()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	// Manually hold the mutation lock a real tx mutation method would hold
	// for its call duration — this is the DETERMINISTIC way to observe lock
	// behavior directly (no timing-based race with a real mutation, which
	// completes too fast to reliably observe). tx.go's own methods are
	// exactly this bracket around one *Internal call.
	if err := tx.lockActiveCoreWrite(); err != nil {
		t.Fatalf("lockActiveCoreWrite: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := g.Nodes.Add(ctx, []string{"Standalone"}, nil)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			tx.unlockActiveCoreWrite()
			t.Fatalf("concurrent standalone AddNode: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		tx.unlockActiveCoreWrite()
		t.Fatal("concurrent standalone AddNode blocked while tx held its shared-lock mutation window — BACKLOG 11f lock relaxation regressed")
	}

	tx.unlockActiveCoreWrite()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx = nil // committed — defer must not double-Rollback
}

// TestGraphTx_LegacyMechanism_ConcurrentStandaloneWriteBlocks is the
// regression guard on the OLD behavior for a partial-capability store: the
// exclusive lock must still genuinely exclude a concurrent standalone write
// for the duration of one tx mutation call.
func TestGraphTx_LegacyMechanism_ConcurrentStandaloneWriteBlocks(t *testing.T) {
	t.Parallel()
	ts, err := tiered.New(tiered.Config{
		InMemory:    true,
		RefLabels:   []string{"Ref"},
		ChangeLog:   true,
		ShardWindow: 7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })

	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	ctx := context.Background()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	if err := tx.lockActiveCoreWrite(); err != nil {
		t.Fatalf("lockActiveCoreWrite: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
		done <- err
	}()

	select {
	case <-done:
		tx.unlockActiveCoreWrite()
		t.Fatal("concurrent standalone AddNode did NOT block against the legacy exclusive lock — expected it to wait for unlockActiveCoreWrite")
	case <-time.After(150 * time.Millisecond):
		// Expected: still blocked. Release and confirm it then proceeds.
	}

	tx.unlockActiveCoreWrite()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("standalone AddNode after unlock: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("standalone AddNode never completed after unlockActiveCoreWrite")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tx = nil
}

// ─── Rollback under the new mechanism still discards everything ────────────

func TestGraphTx_ScopedChangeLog_RollbackDiscardsEverything(t *testing.T) {
	t.Parallel()
	g := newTxTestGraphWithChangeLog(t)
	ctx := context.Background()

	// Seed state OUTSIDE the tx so the tx has pre-existing rows to mutate,
	// delete, and relabel (adversarial: exercise every reverse-mutation door
	// rollback_scoped.go wires, not just creates).
	n1, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("seed Add n1: %v", err)
	}
	n2, err := g.Nodes.Add(ctx, []string{"A", "B"}, map[string]any{"x": int64(2)})
	if err != nil {
		t.Fatalf("seed Add n2: %v", err)
	}
	n3, err := g.Nodes.Add(ctx, []string{"A"}, nil)
	if err != nil {
		t.Fatalf("seed Add n3: %v", err)
	}
	seedRel, err := g.Rels.Add(ctx, "REL", n1, n2, nil)
	if err != nil {
		t.Fatalf("seed Add rel: %v", err)
	}

	before, err := g.Repl.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	// Create.
	if _, err := tx.AddNode([]string{"New"}, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNode: %v", err)
	}
	// Update.
	if _, err := tx.UpdateNode(n1.ID(), map[string]any{"x": int64(99)}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.UpdateNode: %v", err)
	}
	// Label add/remove.
	if err := tx.AddNodeLabel(n3.ID(), "Extra"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNodeLabel: %v", err)
	}
	if err := tx.RemoveNodeLabel(n2.ID(), "B"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.RemoveNodeLabel: %v", err)
	}
	// Delete a relationship and a node (cascade).
	if err := tx.DeleteRelationship(seedRel.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.DeleteRelationship: %v", err)
	}
	if err := tx.DeleteNode(n1.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.DeleteNode: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// The feed must show ZERO new records past the pre-tx watermark — the
	// entire point of routing every forward AND reverse mutation through the
	// same scope token.
	count := 0
	if err := g.Repl.ForEachChange(before, func(_ storepkg.ChangeRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if count != 0 {
		t.Fatalf("feed has %d records after rollback, want 0 — a rolled-back tx must emit NOTHING", count)
	}

	// Original state fully restored.
	got1, err := g.Nodes.Get(ctx, n1.ID())
	if err != nil {
		t.Fatalf("Get n1 after rollback: %v", err)
	}
	if v, _ := got1.GetProperty("x"); v != int64(1) {
		t.Fatalf("n1.x after rollback = %v, want 1 (update must be undone)", v)
	}
	got2, err := g.Nodes.Get(ctx, n2.ID())
	if err != nil {
		t.Fatalf("Get n2 after rollback: %v", err)
	}
	if !g.Nodes.HasLabel(got2, "B") {
		t.Fatal("n2 must still have label B after rollback")
	}
	got3, err := g.Nodes.Get(ctx, n3.ID())
	if err != nil {
		t.Fatalf("Get n3 after rollback: %v", err)
	}
	if g.Nodes.HasLabel(got3, "Extra") {
		t.Fatal("n3 must NOT have label Extra after rollback")
	}
	if _, err := g.Rels.Get(ctx, seedRel.ID()); err != nil {
		t.Fatalf("seed relationship must still exist after rollback: %v", err)
	}
	cnt, err := g.Nodes.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if cnt != 3 {
		t.Fatalf("node count after rollback = %d, want 3 (the tx's create must be undone)", cnt)
	}
}

// ─── Commit under the new mechanism emits everything, once ─────────────────

func TestGraphTx_ScopedChangeLog_CommitEmitsEverything(t *testing.T) {
	t.Parallel()
	g := newTxTestGraphWithChangeLog(t)
	ctx := context.Background()

	n1, err := g.Nodes.Add(ctx, []string{"A"}, nil)
	if err != nil {
		t.Fatalf("seed Add n1: %v", err)
	}

	before, err := g.Repl.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.AddNode([]string{"New"}, nil); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNode: %v", err)
	}
	if _, err := tx.UpdateNode(n1.ID(), map[string]any{"y": int64(1)}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.UpdateNode: %v", err)
	}

	// Not visible before commit.
	count := 0
	if err := g.Repl.ForEachChange(before, func(_ storepkg.ChangeRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("ForEachChange (pre-commit): %v", err)
	}
	if count != 0 {
		t.Fatalf("feed has %d records before Commit, want 0", count)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if tx.CommittedLSN() == 0 {
		t.Fatal("CommittedLSN() = 0 after a committing tx with real mutations")
	}

	count = 0
	if err := g.Repl.ForEachChange(before, func(_ storepkg.ChangeRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("ForEachChange (post-commit): %v", err)
	}
	if count != 2 {
		t.Fatalf("feed has %d records after Commit, want 2 (one create, one update)", count)
	}
}
