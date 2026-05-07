package core

import (
	"errors"
	"sync"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// newDiffGraph returns a Graph for diff tests.
func newDiffGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// setNodeTemporal directly sets ValidFrom/ValidTo on a stored node.
// This gives tests precise control over temporal visibility without relying
// on snowflake timestamp resolution.
func setNodeTemporal(t *testing.T, g *Core, id types.NodeID, validFrom, validTo types.Instant) {
	t.Helper()
	n, err := g.store.GetNode(id)
	if err != nil {
		t.Fatalf("setNodeTemporal GetNode: %v", err)
	}
	tm := n.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		n.SetTemporal(tm)
	}
	tm.ValidFrom = validFrom
	tm.ValidTo = validTo
	if err := g.store.ReplaceNode(n); err != nil {
		t.Fatalf("setNodeTemporal ReplaceNode: %v", err)
	}
}

// setRelTemporal directly sets ValidFrom/ValidTo on a stored relationship.
func setRelTemporal(t *testing.T, g *Core, id types.RelID, validFrom, validTo types.Instant) {
	t.Helper()
	r, err := g.store.GetRelationship(id)
	if err != nil {
		t.Fatalf("setRelTemporal GetRelationship: %v", err)
	}
	tm := r.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		r.SetTemporal(tm)
	}
	tm.ValidFrom = validFrom
	tm.ValidTo = validTo
	if err := g.store.ReplaceRelationship(r); err != nil {
		t.Fatalf("setRelTemporal ReplaceRelationship: %v", err)
	}
}

// TestDiffSnapshots_InvalidRange verifies that invalid time ranges return ErrInvalidTimeRange.
func TestDiffSnapshots_InvalidRange(t *testing.T) {
	g := newDiffGraph(t)

	now := types.Instant(time.Now().UnixMilli())
	cases := []struct {
		name   string
		t1, t2 types.Instant
	}{
		{"t1==t2", now, now},
		{"t1>t2", now + 1, now},
		{"t1==0", 0, now},
		{"t2==0", now, 0},
		{"both_zero", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := g.DiffSnapshots(tc.t1, tc.t2)
			if !errors.Is(err, ErrInvalidTimeRange) {
				t.Fatalf("expected ErrInvalidTimeRange, got %v", err)
			}
		})
	}
}

// TestDiffSnapshots_EmptyGraph verifies that diffing an empty graph returns
// an empty (non-nil) diff without panicking.
func TestDiffSnapshots_EmptyGraph(t *testing.T) {
	g := newDiffGraph(t)
	t1 := types.Instant(time.Now().UnixMilli()) - 200
	t2 := t1 + 100

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots empty graph: %v", err)
	}
	if diff == nil {
		t.Fatal("DiffSnapshots returned nil diff")
	}
	if len(diff.NodesCreated)+len(diff.NodesUpdated)+len(diff.NodesDeleted) != 0 {
		t.Fatalf("expected empty node diff, got: created=%d updated=%d deleted=%d",
			len(diff.NodesCreated), len(diff.NodesUpdated), len(diff.NodesDeleted))
	}
	if len(diff.RelsCreated)+len(diff.RelsUpdated)+len(diff.RelsDeleted) != 0 {
		t.Fatalf("expected empty rel diff")
	}
}

// TestDiffSnapshots_Created verifies that a node added between t1 and t2 appears
// in NodesCreated.
func TestDiffSnapshots_Created(t *testing.T) {
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	n, err := g.AddNode([]string{"Thing"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()

	// ValidFrom between t1 and t2 → not visible at t1, visible at t2.
	setNodeTemporal(t, g, nid, base+150, 0)

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.NodesCreated) != 1 {
		t.Fatalf("expected 1 created node, got %d (deleted=%d, updated=%d)",
			len(diff.NodesCreated), len(diff.NodesDeleted), len(diff.NodesUpdated))
	}
	if diff.NodesCreated[0].ID() != nid {
		t.Fatal("created node ID mismatch")
	}
}

// TestDiffSnapshots_Deleted verifies that a node that expires between t1 and t2
// appears in NodesDeleted.
func TestDiffSnapshots_Deleted(t *testing.T) {
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	n, err := g.AddNode([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()

	// ValidFrom before t1, ValidTo between t1 and t2 → visible at t1, gone at t2.
	setNodeTemporal(t, g, nid, base+50, base+200)

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.NodesDeleted) != 1 {
		t.Fatalf("expected 1 deleted node, got %d (created=%d, updated=%d)",
			len(diff.NodesDeleted), len(diff.NodesCreated), len(diff.NodesUpdated))
	}
	if diff.NodesDeleted[0].ID() != nid {
		t.Fatal("deleted node ID mismatch")
	}
}

// TestDiffSnapshots_Updated verifies that a node whose properties changed between
// t1 and t2 appears in NodesUpdated.
func TestDiffSnapshots_Updated(t *testing.T) {
	g := newDiffGraph(t)

	n, err := g.AddNode([]string{"User"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()

	// t1 captures state BEFORE the update.
	t1 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	// Update the node (its hash changes).
	if _, err := g.UpdateNode(nid, map[string]any{"name": "Alice Updated"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	t2 := types.Instant(time.Now().UnixMilli()) + 10

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.NodesUpdated) != 1 {
		t.Fatalf("expected 1 updated node, got %d (created=%d, deleted=%d)",
			len(diff.NodesUpdated), len(diff.NodesCreated), len(diff.NodesDeleted))
	}
	upd := diff.NodesUpdated[0]
	beforeName, _ := upd.Before.GetProperty("name")
	afterName, _ := upd.After.GetProperty("name")
	if beforeName != "Alice" {
		t.Fatalf("Before.name: expected Alice, got %v", beforeName)
	}
	if afterName != "Alice Updated" {
		t.Fatalf("After.name: expected Alice Updated, got %v", afterName)
	}
}

// TestDiffSnapshots_Unchanged verifies that a node not changed between t1 and t2
// does not appear in any diff list.
func TestDiffSnapshots_Unchanged(t *testing.T) {
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	n, err := g.AddNode([]string{"Static"}, map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Explicitly set ValidFrom before t1 so the node is visible at both t1 and t2.
	// No updates → hash identical at both snapshots → should not appear in any list.
	setNodeTemporal(t, g, n.ID(), base+50, 0)

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.NodesCreated)+len(diff.NodesUpdated)+len(diff.NodesDeleted) != 0 {
		t.Fatalf("expected empty diff for unchanged node, got: created=%d updated=%d deleted=%d",
			len(diff.NodesCreated), len(diff.NodesUpdated), len(diff.NodesDeleted))
	}
}

// TestDiffSnapshots_RelCreated verifies that a relationship added between t1 and t2
// appears in RelsCreated.
func TestDiffSnapshots_RelCreated(t *testing.T) {
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)

	// Both nodes valid at t1.
	setNodeTemporal(t, g, a.ID(), base+50, 0)
	setNodeTemporal(t, g, b.ID(), base+50, 0)

	r, err := g.AddRelationship("LINKS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Rel ValidFrom between t1 and t2 → not visible at t1, visible at t2.
	setRelTemporal(t, g, rid, base+150, 0)

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.RelsCreated) != 1 {
		t.Fatalf("expected 1 created rel, got %d (deleted=%d, updated=%d)",
			len(diff.RelsCreated), len(diff.RelsDeleted), len(diff.RelsUpdated))
	}
	if diff.RelsCreated[0].ID() != rid {
		t.Fatal("created rel ID mismatch")
	}
}

// TestDiffSnapshots_RelDeleted verifies that a relationship expiring between t1
// and t2 appears in RelsDeleted.
func TestDiffSnapshots_RelDeleted(t *testing.T) {
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)

	setNodeTemporal(t, g, a.ID(), base+50, 0)
	setNodeTemporal(t, g, b.ID(), base+50, 0)

	r, err := g.AddRelationship("LINKS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Rel ValidFrom before t1, ValidTo between t1 and t2.
	setRelTemporal(t, g, rid, base+50, base+200)

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.RelsDeleted) != 1 {
		t.Fatalf("expected 1 deleted rel, got %d (created=%d, updated=%d)",
			len(diff.RelsDeleted), len(diff.RelsCreated), len(diff.RelsUpdated))
	}
	if diff.RelsDeleted[0].ID() != rid {
		t.Fatal("deleted rel ID mismatch")
	}
}

// TestDiffSnapshots_RelUpdated verifies that a relationship whose properties changed
// between t1 and t2 appears in RelsUpdated.
func TestDiffSnapshots_RelUpdated(t *testing.T) {
	g := newDiffGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, err := g.AddRelationship("LINKS", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	t1 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	if _, err := g.UpdateRelationship(rid, map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	t2 := types.Instant(time.Now().UnixMilli()) + 10

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.RelsUpdated) != 1 {
		t.Fatalf("expected 1 updated rel, got %d (created=%d, deleted=%d)",
			len(diff.RelsUpdated), len(diff.RelsCreated), len(diff.RelsDeleted))
	}
	upd := diff.RelsUpdated[0]
	beforeW, _ := upd.Before.GetProperty("w")
	afterW, _ := upd.After.GetProperty("w")
	if beforeW != int64(1) {
		t.Fatalf("Before.w: expected 1, got %v", beforeW)
	}
	if afterW != int64(2) {
		t.Fatalf("After.w: expected 2, got %v", afterW)
	}
}

// TestNodeHash_NilIntegrity verifies that nodeHash returns the empty string
// when integrity is unset, so DiffSnapshotsCallback treats two such versions
// as identical (no spurious Updated entry).
func TestNodeHash_NilIntegrity(t *testing.T) {
	g := newDiffGraph(t)

	n, _ := g.AddNode([]string{"Shared"}, nil)
	n.SetIntegrity(nil) // exercise nodeHash nil branch

	if got := nodeHash(n); got != "" {
		t.Fatalf("nodeHash(nil-integrity) = %q, want empty", got)
	}
}

// TestRelHash_NilIntegrity verifies the nil-integrity path for relationships.
func TestRelHash_NilIntegrity(t *testing.T) {
	g := newDiffGraph(t)
	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("E", a, b, nil)
	r.SetIntegrity(nil) // exercise relHash nil branch

	if got := relHash(r); got != "" {
		t.Fatalf("relHash(nil-integrity) = %q, want empty", got)
	}
}

// TestDiffSnapshots_MixedScenario verifies a mix of created, updated, deleted,
// and unchanged entities each appear in the correct diff category.
func TestDiffSnapshots_MixedScenario(t *testing.T) {
	g := newDiffGraph(t)

	// "unchanged" — created before the diff window, not modified.
	_, err := g.AddNode([]string{"Unchanged"}, map[string]any{"v": "same"})
	if err != nil {
		t.Fatalf("AddNode unchanged: %v", err)
	}

	// "updated" — exists at t1, properties change before t2.
	toUpdate, err := g.AddNode([]string{"Updated"}, map[string]any{"v": "before"})
	if err != nil {
		t.Fatalf("AddNode toUpdate: %v", err)
	}

	t1 := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	// Perform the update between t1 and t2.
	if _, err := g.UpdateNode(toUpdate.ID(), map[string]any{"v": "after"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// "created" — added after t1.
	created, err := g.AddNode([]string{"Created"}, nil)
	if err != nil {
		t.Fatalf("AddNode created: %v", err)
	}

	t2 := types.Instant(time.Now().UnixMilli()) + 10

	diff, err := g.DiffSnapshots(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}

	if len(diff.NodesCreated) != 1 {
		t.Fatalf("expected 1 created, got %d", len(diff.NodesCreated))
	}
	if diff.NodesCreated[0].ID() != created.ID() {
		t.Fatal("wrong node in Created list")
	}
	if len(diff.NodesUpdated) != 1 {
		t.Fatalf("expected 1 updated, got %d", len(diff.NodesUpdated))
	}
	if diff.NodesUpdated[0].Before.ID() != toUpdate.ID() {
		t.Fatal("wrong node in Updated list")
	}
	if len(diff.NodesDeleted) != 0 {
		t.Fatalf("expected 0 deleted, got %d", len(diff.NodesDeleted))
	}
}

// --- Fix C: DiffSnapshots does not block BeginTx ---

func TestDiffSnapshots_DoesNotBlockWrites(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Seed a node with explicit ValidFrom so temporal queries have real work to do.
	n, err := g.AddNode([]string{"Thing"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	setNodeTemporal(t, g, n.ID(), 1000, 0)

	t1 := types.Instant(500)
	t2 := types.Instant(2000)

	const goroutines = 8
	var wg sync.WaitGroup

	// Launch goroutines that call DiffSnapshots concurrently.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = g.DiffSnapshots(t1, t2)
		}()
	}

	// Launch goroutines that call BeginTx (requires g.mu.Lock) concurrently.
	// With the old code (g.mu.RLock held by DiffSnapshots), these would all
	// queue behind DiffSnapshots. With the fix, they run freely.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := g.BeginTx()
			_, _ = tx.AddNode([]string{"Concurrent"}, nil)
			_ = tx.Commit()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed — no deadlock.
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent DiffSnapshots + BeginTx goroutines did not complete within 10s")
	}
}
