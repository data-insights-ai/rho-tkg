package core

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func newTxTimeGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func assertTxTimeVisibleBeforeDelete(t *testing.T, tm *types.TemporalMetadata) {
	t.Helper()
	if tm == nil {
		t.Fatal("as-of entity has nil temporal metadata")
	}
	if tm.TxTo != 0 {
		t.Fatalf("as-of TxTo = %d, want 0 before delete transaction", tm.TxTo)
	}
	if tm.ValidTo != 0 {
		t.Fatalf("as-of ValidTo = %d, want 0 before delete transaction", tm.ValidTo)
	}
	if tm.DeletedAt != 0 {
		t.Fatalf("as-of DeletedAt = %d, want 0 before delete transaction", tm.DeletedAt)
	}
}

func TestTxFromSetOnAdd(t *testing.T) {
	// TxFrom should be set on new node and rel. The test asserts
	// "TxFrom is captured between two clock samples bracketing Add" —
	// works equally well with the injected test clock (which is what
	// c.now() consults).
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)
	before := clk.PeekInstant()

	n, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	after := clk.PeekInstant()

	tm := n.Temporal()
	if tm == nil {
		t.Fatal("node has no temporal metadata")
	}
	if tm.TxFrom < before || tm.TxFrom > after {
		t.Errorf("TxFrom %d not in range [%d, %d]", tm.TxFrom, before, after)
	}
	if tm.TxTo != 0 {
		t.Errorf("TxTo should be 0 on new node, got %d", tm.TxTo)
	}

	// Same for relationship
	n2, _ := g.Nodes.Add([]string{"B"}, nil)
	before2 := clk.PeekInstant()
	r, err := g.Rels.Add("REL", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	after2 := clk.PeekInstant()

	rtm := r.Temporal()
	if rtm == nil {
		t.Fatal("rel has no temporal metadata")
	}
	if rtm.TxFrom < before2 || rtm.TxFrom > after2 {
		t.Errorf("rel TxFrom %d not in range [%d, %d]", rtm.TxFrom, before2, after2)
	}
}

func TestTxToSetOnUpdate(t *testing.T) {
	// Old version should have TxTo set; new version has a new TxFrom
	g := newTxTimeGraph(t)
	useTestClock(t, g)

	n, _ := g.Nodes.Add([]string{"A"}, nil)
	nid := n.ID()
	origTxFrom := n.Temporal().TxFrom

	// Test clock advances 1ms per c.now() call — Update gets a strictly
	// greater TxFrom without a wall-clock sleep (R5-F10).

	updated, err := g.Nodes.Update(nid, map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// New version: TxFrom should be after original TxFrom, TxTo should be 0
	updTm := updated.Temporal()
	if updTm.TxFrom <= origTxFrom {
		t.Errorf("new version TxFrom %d should be > original %d", updTm.TxFrom, origTxFrom)
	}
	if updTm.TxTo != 0 {
		t.Errorf("new version TxTo should be 0, got %d", updTm.TxTo)
	}

	// History (version 0): old version should have TxTo set
	hist, err := g.Nodes.History(nid)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) == 0 {
		t.Fatal("no history found")
	}
	oldVer := hist[0]
	oldTm := oldVer.Temporal()
	if oldTm == nil || oldTm.TxTo == 0 {
		t.Error("old version TxTo should be set (non-zero)")
	}
	if oldTm.TxTo > updTm.TxFrom {
		t.Errorf("old TxTo %d should be <= new TxFrom %d", oldTm.TxTo, updTm.TxFrom)
	}
}

func TestTxToSetOnDelete(t *testing.T) {
	// Deleted node's last history version should have TxTo set
	g := newTxTimeGraph(t)
	useTestClock(t, g)

	n, _ := g.Nodes.Add([]string{"A"}, nil)
	nid := n.ID()
	origTxFrom := n.Temporal().TxFrom

	if err := g.Nodes.Delete(nid); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// The tombstone history entry should have TxFrom set, TxTo non-zero
	hist, err := g.Nodes.History(nid)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) == 0 {
		t.Fatal("no history for deleted node")
	}
	tomb := hist[len(hist)-1]
	tm := tomb.Temporal()
	if tm == nil {
		t.Fatal("tombstone has no temporal")
	}
	if tm.TxFrom == 0 {
		t.Error("tombstone TxFrom should be set")
	}
	if tm.TxFrom != origTxFrom {
		t.Fatalf("tombstone TxFrom = %d, want original live TxFrom %d", tm.TxFrom, origTxFrom)
	}
	if tm.TxTo == 0 {
		t.Error("tombstone TxTo should be set")
	}
	if tm.TxTo <= tm.TxFrom {
		t.Fatalf("tombstone TxTo %d should be after TxFrom %d", tm.TxTo, tm.TxFrom)
	}
}

func TestGetNodeAsOf_BeforeCreate(t *testing.T) {
	g := newTxTimeGraph(t)
	useTestClock(t, g)

	before := types.Instant(time.Now().UnixMilli() - 1000) // 1 second in past
	n, _ := g.Nodes.Add([]string{"A"}, nil)
	nid := n.ID()

	_, err := g.Temporal.NodeAsOf(nid, before)
	if !errors.Is(err, ErrNoVersionAsOf) {
		t.Errorf("expected ErrNoVersionAsOf, got %v", err)
	}
}

func TestGetNodeAsOf_CurrentVersion(t *testing.T) {
	g := newTxTimeGraph(t)
	useTestClock(t, g)

	n, _ := g.Nodes.Add([]string{"A"}, nil)
	nid := n.ID()

	after := types.Instant(time.Now().UnixMilli() + 1000) // 1 second in future

	got, err := g.Temporal.NodeAsOf(nid, after)
	if err != nil {
		t.Fatalf("GetNodeAsOf: %v", err)
	}
	if got.ID() != nid {
		t.Error("wrong node returned")
	}
}

func TestGetNodeAsOf_HistoricalVersion(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, _ := g.Nodes.Add([]string{"A"}, nil)
	nid := n.ID()

	// Record the TxFrom of the original version
	origTxFrom := n.Temporal().TxFrom

	// Widen the gap between origTxFrom and newTxFrom so the test's
	// midpoint lands strictly between them (R5-F10).
	clk.Advance(2 * time.Millisecond)

	// Update the node
	updated, _ := g.Nodes.Update(nid, map[string]any{"x": 1})
	newTxFrom := updated.Temporal().TxFrom

	// Query at a time between original TxFrom and newTxFrom
	mid := origTxFrom + (newTxFrom-origTxFrom)/2
	if mid == origTxFrom {
		mid = origTxFrom + 1
	}

	got, err := g.Temporal.NodeAsOf(nid, mid)
	if err != nil {
		t.Fatalf("GetNodeAsOf at mid: %v", err)
	}
	// Should return version 0 (original), not version 1 (updated)
	if got.Version() != 0 {
		t.Errorf("expected version 0, got %d", got.Version())
	}
}

func TestGetNodesAsOf_FiltersCorrectly(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	// Create first node — TxFrom comes from the test clock.
	n1, _ := g.Nodes.Add([]string{"A"}, nil)
	_ = n1

	// Capture midTime AFTER n1's c.now(). Then advance the clock by
	// 1ms so n2's TxFrom is strictly greater than midTime. Without
	// the explicit Advance, the next c.now() would return PeekInstant()
	// itself and n2.TxFrom == midTime, breaking the "n2 NOT returned"
	// assertion.
	midTime := clk.PeekInstant()
	clk.Advance(1 * time.Millisecond)

	// Create second node after midTime
	n2, _ := g.Nodes.Add([]string{"A"}, nil)
	_ = n2

	// GetNodesAsOf(midTime) should return only n1 (n2 didn't exist yet)
	got, err := g.Temporal.NodesAsOf(midTime)
	if err != nil {
		t.Fatalf("GetNodesAsOf: %v", err)
	}

	found1, found2 := false, false
	for _, node := range got {
		id := node.ID()
		if id == n1.ID() {
			found1 = true
		}
		if id == n2.ID() {
			found2 = true
		}
	}
	if !found1 {
		t.Error("n1 should be returned (existed at midTime)")
	}
	if found2 {
		t.Error("n2 should NOT be returned (created after midTime)")
	}
}

func TestNodeAsOfDeletedEntityBeforeDelete(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add([]string{"A"}, map[string]any{"name": "live"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()
	asOf := n.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Nodes.Delete(nid); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	got, err := g.Temporal.NodeAsOf(nid, asOf)
	if err != nil {
		t.Fatalf("NodeAsOf before delete: %v", err)
	}
	if got.ID() != nid {
		t.Fatalf("NodeAsOf returned %d, want %d", got.ID(), nid)
	}
	assertTxTimeVisibleBeforeDelete(t, got.Temporal())
	name, _ := got.GetProperty("name")
	if name != "live" {
		t.Fatalf("NodeAsOf property name = %v, want live", name)
	}

	_, err = g.Temporal.NodeAsOf(nid, clk.PeekInstant())
	if !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("NodeAsOf after delete = %v, want ErrNoVersionAsOf", err)
	}
}

func TestNodesAsOfDeletedEntityBeforeDelete(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add([]string{"A"}, map[string]any{"name": "live"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()
	asOf := n.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Nodes.Delete(nid); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	nodes, err := g.Temporal.NodesAsOf(asOf)
	if err != nil {
		t.Fatalf("NodesAsOf before delete: %v", err)
	}
	found := false
	for _, got := range nodes {
		if got.ID() == nid {
			found = true
			assertTxTimeVisibleBeforeDelete(t, got.Temporal())
			name, _ := got.GetProperty("name")
			if name != "live" {
				t.Fatalf("NodesAsOf node name = %v, want live", name)
			}
		}
	}
	if !found {
		t.Fatalf("NodesAsOf before delete did not include node %d", nid)
	}

	nodes, err = g.Temporal.NodesAsOf(clk.PeekInstant())
	if err != nil {
		t.Fatalf("NodesAsOf after delete: %v", err)
	}
	for _, got := range nodes {
		if got.ID() == nid {
			t.Fatalf("NodesAsOf after delete included node %d", nid)
		}
	}
}

func TestRelAsOfDeletedEntityBeforeDelete(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n1, _ := g.Nodes.Add([]string{"A"}, nil)
	n2, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("REL", n1, n2, map[string]any{"state": "live"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()
	asOf := r.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Rels.Delete(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	got, err := g.Temporal.RelAsOf(rid, asOf)
	if err != nil {
		t.Fatalf("RelAsOf before delete: %v", err)
	}
	if got.ID() != rid {
		t.Fatalf("RelAsOf returned %d, want %d", got.ID(), rid)
	}
	assertTxTimeVisibleBeforeDelete(t, got.Temporal())
	state, _ := got.GetProperty("state")
	if state != "live" {
		t.Fatalf("RelAsOf property state = %v, want live", state)
	}

	_, err = g.Temporal.RelAsOf(rid, clk.PeekInstant())
	if !errors.Is(err, ErrNoVersionAsOf) {
		t.Fatalf("RelAsOf after delete = %v, want ErrNoVersionAsOf", err)
	}
}

func TestRelsAsOfCascadeDeletedRelationshipBeforeDelete(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n1, _ := g.Nodes.Add([]string{"A"}, nil)
	n2, _ := g.Nodes.Add([]string{"B"}, nil)
	r, err := g.Rels.Add("REL", n1, n2, map[string]any{"state": "live"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()
	asOf := r.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Nodes.Delete(n1.ID()); err != nil {
		t.Fatalf("DeleteNode cascade: %v", err)
	}

	rels, err := g.Temporal.RelsAsOf(asOf)
	if err != nil {
		t.Fatalf("RelsAsOf before cascade delete: %v", err)
	}
	found := false
	for _, got := range rels {
		if got.ID() == rid {
			found = true
			assertTxTimeVisibleBeforeDelete(t, got.Temporal())
			state, _ := got.GetProperty("state")
			if state != "live" {
				t.Fatalf("RelsAsOf relationship state = %v, want live", state)
			}
		}
	}
	if !found {
		t.Fatalf("RelsAsOf before cascade delete did not include relationship %d", rid)
	}

	rels, err = g.Temporal.RelsAsOf(clk.PeekInstant())
	if err != nil {
		t.Fatalf("RelsAsOf after cascade delete: %v", err)
	}
	for _, got := range rels {
		if got.ID() == rid {
			t.Fatalf("RelsAsOf after cascade delete included relationship %d", rid)
		}
	}
}

func TestGetRelAsOf(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n1, _ := g.Nodes.Add([]string{"A"}, nil)
	n2, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("REL", n1, n2, nil)
	rid := r.ID()

	before := r.Temporal().TxFrom - 1

	_, err := g.Temporal.RelAsOf(rid, before)
	if !errors.Is(err, ErrNoVersionAsOf) {
		t.Errorf("expected ErrNoVersionAsOf before creation, got %v", err)
	}

	// "after" must lie strictly after the rel's TxFrom on the test
	// clock. PeekInstant() returns the next instant the clock will
	// hand out (≥ TxFrom + 1ms here since c.now() advanced past
	// TxFrom on the Add path).
	after := clk.PeekInstant() + 1000
	got, err := g.Temporal.RelAsOf(rid, after)
	if err != nil {
		t.Fatalf("GetRelAsOf after: %v", err)
	}
	if got.ID() != rid {
		t.Error("wrong rel returned")
	}
}
