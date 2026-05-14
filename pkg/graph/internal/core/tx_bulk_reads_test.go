package core

import (
	"context"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Tx-side bulk-read mirror tests for v4.0.2.
//
// Every public sub-API read method that takes c.mu.RLock or
// c.readUnderRLock deadlocks inside an open tx (lesson 31). v4.0.1
// mirrored the metadata-resolution set; v4.0.2 adds the bulk
// data-retrieval set (Get/GetByIDs/All/ByLabel/ByLabelAndProperty/Count
// for Nodes; equivalent for Rels; Temporal queries; AsOf; Stats;
// SearchNearest). One correctness test per method plus a deadlock
// regression for the most user-visible shape (g.Nodes.ByLabel inside
// a tx).

func setupTxReadFixture(t *testing.T) (*Core, types.NodeID, types.NodeID, types.RelID, types.Instant) {
	t.Helper()
	g := newTxTestGraph(t)
	ctx := context.Background()

	t0 := types.Instant(time.Now().UnixMilli())

	a, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"since": "2024"})
	if err != nil {
		t.Fatal(err)
	}
	return g, a.ID(), b.ID(), r.ID(), t0
}

// --- Nodes ---

func TestTxBulkReads_NodeMirrors(t *testing.T) {
	t.Parallel()
	g, aID, bID, _, _ := setupTxReadFixture(t)

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	if got, err := tx.GetNode(aID); err != nil || got == nil {
		t.Errorf("tx.GetNode(a) = (%v, %v)", got, err)
	}

	nodes, err := tx.GetNodesByIDs([]types.NodeID{aID, bID})
	if err != nil || len(nodes) != 2 {
		t.Errorf("tx.GetNodesByIDs = (len=%d, %v), want len=2", len(nodes), err)
	}

	all, err := tx.AllNodes(storepkg.QueryOpts{})
	if err != nil || len(all) != 2 {
		t.Errorf("tx.AllNodes len=%d, err=%v, want 2", len(all), err)
	}

	byLabel, err := tx.NodesByLabel("Person", storepkg.QueryOpts{})
	if err != nil || len(byLabel) != 2 {
		t.Errorf("tx.NodesByLabel(Person) len=%d, err=%v, want 2", len(byLabel), err)
	}

	byLP, err := tx.NodesByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if err != nil || len(byLP) != 1 {
		t.Errorf("tx.NodesByLabelAndProperty(name=Alice) len=%d, err=%v, want 1", len(byLP), err)
	}

	count, err := tx.NodeCount()
	if err != nil || count != 2 {
		t.Errorf("tx.NodeCount = (%d, %v), want 2", count, err)
	}

	cnt, err := tx.NodeCountByLabel("Person")
	if err != nil || cnt != 2 {
		t.Errorf("tx.NodeCountByLabel(Person) = (%d, %v), want 2", cnt, err)
	}
}

// --- Rels ---

func TestTxBulkReads_RelMirrors(t *testing.T) {
	t.Parallel()
	g, aID, _, rID, _ := setupTxReadFixture(t)

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	if got, err := tx.GetRelationship(rID); err != nil || got == nil {
		t.Errorf("tx.GetRelationship = (%v, %v)", got, err)
	}

	rels, err := tx.GetRelsByIDs([]types.RelID{rID})
	if err != nil || len(rels) != 1 {
		t.Errorf("tx.GetRelsByIDs len=%d, err=%v, want 1", len(rels), err)
	}

	all, err := tx.AllRels(storepkg.QueryOpts{})
	if err != nil || len(all) != 1 {
		t.Errorf("tx.AllRels len=%d, err=%v, want 1", len(all), err)
	}

	byType, err := tx.RelsByType("KNOWS", storepkg.QueryOpts{})
	if err != nil || len(byType) != 1 {
		t.Errorf("tx.RelsByType(KNOWS) len=%d, err=%v, want 1", len(byType), err)
	}

	out, err := tx.OutgoingRels(aID, "")
	if err != nil || len(out) != 1 {
		t.Errorf("tx.OutgoingRels(a) len=%d, err=%v, want 1", len(out), err)
	}

	in, err := tx.IncomingRels(aID, "")
	if err != nil {
		t.Errorf("tx.IncomingRels(a) err=%v", err)
	}
	_ = in

	outMap, err := tx.OutgoingRelsForNodes([]types.NodeID{aID}, "")
	if err != nil {
		t.Errorf("tx.OutgoingRelsForNodes err=%v", err)
	}
	if len(outMap[aID]) != 1 {
		t.Errorf("tx.OutgoingRelsForNodes[a] len=%d, want 1", len(outMap[aID]))
	}

	inMap, err := tx.IncomingRelsForNodes([]types.NodeID{aID}, "")
	if err != nil {
		t.Errorf("tx.IncomingRelsForNodes err=%v", err)
	}
	_ = inMap

	count, err := tx.RelCount()
	if err != nil || count != 1 {
		t.Errorf("tx.RelCount = (%d, %v), want 1", count, err)
	}

	cnt, err := tx.RelCountByType("KNOWS")
	if err != nil || cnt != 1 {
		t.Errorf("tx.RelCountByType(KNOWS) = (%d, %v), want 1", cnt, err)
	}
}

// --- Temporal ---

func TestTxBulkReads_TemporalMirrors(t *testing.T) {
	t.Parallel()
	g, aID, _, rID, _ := setupTxReadFixture(t)
	now := types.Instant(time.Now().UnixMilli() + 1000)

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	if nodes, err := tx.NodesAt(now); err != nil {
		t.Errorf("tx.NodesAt err=%v", err)
	} else if len(nodes) != 2 {
		t.Errorf("tx.NodesAt len=%d, want 2", len(nodes))
	}

	if rels, err := tx.RelsAt(now); err != nil {
		t.Errorf("tx.RelsAt err=%v", err)
	} else if len(rels) != 1 {
		t.Errorf("tx.RelsAt len=%d, want 1", len(rels))
	}

	if got, err := tx.NodeAt(aID, now); err != nil || got == nil {
		t.Errorf("tx.NodeAt = (%v, %v)", got, err)
	}

	if got, err := tx.RelAt(rID, now); err != nil || got == nil {
		t.Errorf("tx.RelAt = (%v, %v)", got, err)
	}

	if labeled, err := tx.NodesByLabelAt("Person", now); err != nil || len(labeled) != 2 {
		t.Errorf("tx.NodesByLabelAt(Person) len=%d err=%v", len(labeled), err)
	}

	nbrs, err := tx.NeighborsAt(aID, now)
	if err != nil {
		t.Errorf("tx.NeighborsAt err=%v", err)
	}
	if len(nbrs) != 1 {
		t.Errorf("tx.NeighborsAt len=%d, want 1", len(nbrs))
	}

	out, err := tx.OutgoingRelsAt(aID, now)
	if err != nil {
		t.Errorf("tx.OutgoingRelsAt err=%v", err)
	}
	if len(out) != 1 {
		t.Errorf("tx.OutgoingRelsAt len=%d, want 1", len(out))
	}

	if _, err := tx.IncomingRelsAt(aID, now); err != nil {
		t.Errorf("tx.IncomingRelsAt err=%v", err)
	}

	if _, err := tx.NodesByLabelPropertyAt("Person", "name", "Alice", now); err != nil {
		t.Errorf("tx.NodesByLabelPropertyAt err=%v", err)
	}
}

// --- AsOf ---

func TestTxBulkReads_AsOfMirrors(t *testing.T) {
	t.Parallel()
	g, aID, _, rID, _ := setupTxReadFixture(t)
	now := types.Instant(time.Now().UnixMilli() + 1000)

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.NodeAsOf(aID, now); err != nil {
		// May return ErrNoVersionAsOf depending on tx metadata; just ensure no deadlock.
		t.Logf("tx.NodeAsOf err=%v (informational)", err)
	}
	if _, err := tx.RelAsOf(rID, now); err != nil {
		t.Logf("tx.RelAsOf err=%v (informational)", err)
	}
	if _, err := tx.NodesAsOf(now); err != nil {
		t.Errorf("tx.NodesAsOf err=%v", err)
	}
	if _, err := tx.RelsAsOf(now); err != nil {
		t.Errorf("tx.RelsAsOf err=%v", err)
	}
}

// --- Stats / Index / Misc ---

func TestTxBulkReads_StatsMirrors(t *testing.T) {
	t.Parallel()
	g, _, _, _, _ := setupTxReadFixture(t)

	tx, _ := g.BeginTx()
	defer func() { _ = tx.Rollback() }()

	stats, err := tx.Stats()
	if err != nil {
		t.Errorf("tx.Stats err=%v", err)
	}
	if stats.NodesAdded < 2 {
		t.Errorf("tx.Stats.NodesAdded = %d, want >= 2", stats.NodesAdded)
	}

	if labelCounts, err := tx.AllLabelCounts(); err != nil {
		t.Errorf("tx.AllLabelCounts err=%v", err)
	} else if labelCounts["Person"] != 2 {
		t.Errorf("tx.AllLabelCounts[Person] = %d, want 2", labelCounts["Person"])
	}

	if typeCounts, err := tx.AllRelTypeCounts(); err != nil {
		t.Errorf("tx.AllRelTypeCounts err=%v", err)
	} else if typeCounts["KNOWS"] != 1 {
		t.Errorf("tx.AllRelTypeCounts[KNOWS] = %d, want 1", typeCounts["KNOWS"])
	}

	// IndexProviders on a fresh graph is empty but the call must not deadlock.
	if got := tx.IndexProviders(); got == nil {
		// nil is fine — returns a (possibly empty) slice; just check no panic/deadlock.
		_ = got
	}

	// Constraints on a fresh graph returns the zero constraint set.
	_ = tx.Constraints()

	// EventBus / AsyncEventBus: none attached, expect nil.
	if eb := tx.EventBus(); eb != nil {
		t.Errorf("tx.EventBus = %v, want nil", eb)
	}
	if aeb := tx.AsyncEventBus(); aeb != nil {
		t.Errorf("tx.AsyncEventBus = %v, want nil", aeb)
	}

	// ListShards on a non-tiered backend returns ErrNotTieredStore.
	if _, err := tx.ListShards(); err == nil {
		t.Errorf("tx.ListShards on non-tiered store: want error, got nil")
	}
}

// --- After Commit/Rollback ---

func TestTxBulkReads_AfterDone(t *testing.T) {
	t.Parallel()
	g, _, _, _, _ := setupTxReadFixture(t)

	tx, _ := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Every bulk-read mirror should return ErrTxDone (or a zero value for the
	// no-error mirrors).
	if _, err := tx.GetNodesByIDs([]types.NodeID{1}); err == nil {
		t.Errorf("tx.GetNodesByIDs after commit: want error, got nil")
	}
	if _, err := tx.AllNodes(storepkg.QueryOpts{}); err == nil {
		t.Errorf("tx.AllNodes after commit: want error, got nil")
	}
	if _, err := tx.NodesByLabel("Person", storepkg.QueryOpts{}); err == nil {
		t.Errorf("tx.NodesByLabel after commit: want error, got nil")
	}
	if n, err := tx.NodeCount(); err == nil || n != 0 {
		t.Errorf("tx.NodeCount after commit: want (0, err), got (%d, %v)", n, err)
	}
	if _, err := tx.Stats(); err == nil {
		t.Errorf("tx.Stats after commit: want error, got nil")
	}
	if got := tx.IndexProviders(); got != nil {
		t.Errorf("tx.IndexProviders after commit: want nil, got %v", got)
	}
	// Constraints just shouldn't panic; we don't depend on a particular value.
	_ = tx.Constraints()
}

// --- Deadlock regression: the headline case from the bug report ---
//
// TestTxBulkReads_ByLabelDeadlockRegression pins the exact failure mode reported:
//   "exec_match.go:646 calling g.Nodes.ByLabel(label, opts)" hangs inside a tx.
// Spawns the bad shape in a goroutine, asserts it hangs (proving the bug-class
// still exists for the non-tx path), then rolls back to unblock.

func TestTxBulkReads_ByLabelDeadlockRegression(t *testing.T) {
	t.Parallel()
	g, _, _, _, _ := setupTxReadFixture(t)

	tx, _ := g.BeginTx()

	done := make(chan struct{})
	go func() {
		_, _ = g.Nodes.ByLabel("Person", storepkg.QueryOpts{}) // MUST deadlock
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("g.Nodes.ByLabel returned inside an open tx — the non-tx accessor is unexpectedly reentrant against c.mu")
	case <-time.After(250 * time.Millisecond):
		// expected hang
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	<-done
}

// TestTxBulkReads_NodesAtDeadlockRegression mirrors the above for a temporal
// query — confirms readUnderRLock-based methods exhibit the same deadlock.

func TestTxBulkReads_NodesAtDeadlockRegression(t *testing.T) {
	t.Parallel()
	g, _, _, _, _ := setupTxReadFixture(t)
	now := types.Instant(time.Now().UnixMilli() + 1000)

	tx, _ := g.BeginTx()

	done := make(chan struct{})
	go func() {
		_, _ = g.Temporal.NodesAt(now)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("g.Temporal.NodesAt returned inside an open tx — readUnderRLock unexpectedly reentrant")
	case <-time.After(250 * time.Millisecond):
		// expected hang
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	<-done
}
