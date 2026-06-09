package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
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
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	before2 := clk.PeekInstant()
	r, err := g.Rels.Add(context.Background(), "REL", n, n2, nil)
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

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	nid := n.ID()
	origTxFrom := n.Temporal().TxFrom

	// Test clock advances 1ms per c.now() call — Update gets a strictly
	// greater TxFrom without a wall-clock sleep (R5-F10).

	updated, err := g.Nodes.Update(context.Background(), nid, map[string]any{"x": 1})
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

func TestTxTimeMonotonicWhenClockRepeats(t *testing.T) {
	g := newTxTimeGraph(t)
	fixed := time.Now().Add(time.Second)
	g.SetClockForTest(t, func() time.Time { return fixed })

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"state": "initial"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"state": "updated"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if updated.Temporal().TxFrom <= n.Temporal().TxFrom {
		t.Fatalf("updated node TxFrom = %d, want > original %d", updated.Temporal().TxFrom, n.Temporal().TxFrom)
	}

	before, err := g.Temporal.NodeAsOf(n.ID(), n.Temporal().TxFrom)
	if err != nil {
		t.Fatalf("NodeAsOf original TxFrom: %v", err)
	}
	if state, ok := before.GetProperty("state"); !ok || state != "initial" {
		t.Fatalf("NodeAsOf original state = %v, %v; want initial", state, ok)
	}
	after, err := g.Temporal.NodeAsOf(n.ID(), updated.Temporal().TxFrom)
	if err != nil {
		t.Fatalf("NodeAsOf updated TxFrom: %v", err)
	}
	if state, ok := after.GetProperty("state"); !ok || state != "updated" {
		t.Fatalf("NodeAsOf updated state = %v, %v; want updated", state, ok)
	}
}

func TestRelTxTimeMonotonicWhenClockRepeats(t *testing.T) {
	g := newTxTimeGraph(t)
	fixed := time.Now().Add(time.Second)
	g.SetClockForTest(t, func() time.Time { return fixed })

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "REL", a, b, map[string]any{"state": "initial"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	updated, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"state": "updated"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	if updated.Temporal().TxFrom <= r.Temporal().TxFrom {
		t.Fatalf("updated rel TxFrom = %d, want > original %d", updated.Temporal().TxFrom, r.Temporal().TxFrom)
	}

	before, err := g.Temporal.RelAsOf(r.ID(), r.Temporal().TxFrom)
	if err != nil {
		t.Fatalf("RelAsOf original TxFrom: %v", err)
	}
	if state, ok := before.GetProperty("state"); !ok || state != "initial" {
		t.Fatalf("RelAsOf original state = %v, %v; want initial", state, ok)
	}
	after, err := g.Temporal.RelAsOf(r.ID(), updated.Temporal().TxFrom)
	if err != nil {
		t.Fatalf("RelAsOf updated TxFrom: %v", err)
	}
	if state, ok := after.GetProperty("state"); !ok || state != "updated" {
		t.Fatalf("RelAsOf updated state = %v, %v; want updated", state, ok)
	}
}

func TestTxTimeMandatoryFallback_NodeAndRelVersions(t *testing.T) {
	g := newMandatoryOnlyGraph(t)
	if g.txTimeQuery != nil {
		t.Fatal("mandatory-only graph unexpectedly enabled transaction-time query capability")
	}
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"state": "initial"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	origNodeTx := n.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)
	updatedNode, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"state": "updated"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	oldNode, err := g.Temporal.NodeAsOf(n.ID(), origNodeTx)
	if err != nil {
		t.Fatalf("NodeAsOf original through fallback: %v", err)
	}
	if state, ok := oldNode.GetProperty("state"); !ok || state != "initial" {
		t.Fatalf("fallback original node state = %v, %v; want initial", state, ok)
	}
	currentNode, err := g.Temporal.NodeAsOf(n.ID(), updatedNode.Temporal().TxFrom)
	if err != nil {
		t.Fatalf("NodeAsOf current through fallback: %v", err)
	}
	if state, ok := currentNode.GetProperty("state"); !ok || state != "updated" {
		t.Fatalf("fallback current node state = %v, %v; want updated", state, ok)
	}

	other, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode other: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "REL", updatedNode, other, map[string]any{"state": "initial"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	origRelTx := r.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)
	updatedRel, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"state": "updated"})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	oldRel, err := g.Temporal.RelAsOf(r.ID(), origRelTx)
	if err != nil {
		t.Fatalf("RelAsOf original through fallback: %v", err)
	}
	if state, ok := oldRel.GetProperty("state"); !ok || state != "initial" {
		t.Fatalf("fallback original rel state = %v, %v; want initial", state, ok)
	}
	currentRel, err := g.Temporal.RelAsOf(r.ID(), updatedRel.Temporal().TxFrom)
	if err != nil {
		t.Fatalf("RelAsOf current through fallback: %v", err)
	}
	if state, ok := currentRel.GetProperty("state"); !ok || state != "updated" {
		t.Fatalf("fallback current rel state = %v, %v; want updated", state, ok)
	}
}

func TestTxTimeMandatoryFallback_BulkUsesHistoryIDsAndSkipsDeleted(t *testing.T) {
	g := newMandatoryOnlyGraph(t)
	if g.txTimeQuery != nil {
		t.Fatal("mandatory-only graph unexpectedly enabled transaction-time query capability")
	}
	clk := useTestClock(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"name": "deleted later"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "REL", a, b, map[string]any{"state": "deleted later"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	asOf := r.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Nodes.Delete(context.Background(), a.ID()); err != nil {
		t.Fatalf("DeleteNode cascade: %v", err)
	}

	nodes, err := g.Temporal.NodesAsOf(asOf)
	if err != nil {
		t.Fatalf("NodesAsOf fallback before delete: %v", err)
	}
	foundNode := false
	for _, got := range nodes {
		if got.ID() == a.ID() {
			foundNode = true
			assertTxTimeVisibleBeforeDelete(t, got.Temporal())
		}
	}
	if !foundNode {
		t.Fatalf("NodesAsOf fallback before delete did not include history-only node %d", a.ID())
	}

	rels, err := g.Temporal.RelsAsOf(asOf)
	if err != nil {
		t.Fatalf("RelsAsOf fallback before delete: %v", err)
	}
	foundRel := false
	for _, got := range rels {
		if got.ID() == r.ID() {
			foundRel = true
			assertTxTimeVisibleBeforeDelete(t, got.Temporal())
		}
	}
	if !foundRel {
		t.Fatalf("RelsAsOf fallback before delete did not include history-only relationship %d", r.ID())
	}

	afterDelete := clk.PeekInstant()
	nodes, err = g.Temporal.NodesAsOf(afterDelete)
	if err != nil {
		t.Fatalf("NodesAsOf fallback after delete: %v", err)
	}
	for _, got := range nodes {
		if got.ID() == a.ID() {
			t.Fatalf("NodesAsOf fallback after delete included node %d", a.ID())
		}
	}
	rels, err = g.Temporal.RelsAsOf(afterDelete)
	if err != nil {
		t.Fatalf("RelsAsOf fallback after delete: %v", err)
	}
	for _, got := range rels {
		if got.ID() == r.ID() {
			t.Fatalf("RelsAsOf fallback after delete included relationship %d", r.ID())
		}
	}
}

func TestTxToSetOnDelete(t *testing.T) {
	// Deleted node's last history version should have TxTo set
	g := newTxTimeGraph(t)
	useTestClock(t, g)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	nid := n.ID()
	origTxFrom := n.Temporal().TxFrom

	if err := g.Nodes.Delete(context.Background(), nid); err != nil {
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
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	nid := n.ID()

	_, err := g.Temporal.NodeAsOf(nid, before)
	if !errors.Is(err, ErrNoVersionAsOf) {
		t.Errorf("expected ErrNoVersionAsOf, got %v", err)
	}
}

func TestGetNodeAsOf_CurrentVersion(t *testing.T) {
	g := newTxTimeGraph(t)
	useTestClock(t, g)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
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

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	nid := n.ID()

	// Record the TxFrom of the original version
	origTxFrom := n.Temporal().TxFrom

	// Widen the gap between origTxFrom and newTxFrom so the test's
	// midpoint lands strictly between them (R5-F10).
	clk.Advance(2 * time.Millisecond)

	// Update the node
	updated, _ := g.Nodes.Update(context.Background(), nid, map[string]any{"x": 1})
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
	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	_ = n1

	// Capture midTime AFTER n1's c.now(). Then advance the clock by
	// 1ms so n2's TxFrom is strictly greater than midTime. Without
	// the explicit Advance, the next c.now() would return PeekInstant()
	// itself and n2.TxFrom == midTime, breaking the "n2 NOT returned"
	// assertion.
	midTime := clk.PeekInstant()
	clk.Advance(1 * time.Millisecond)

	// Create second node after midTime
	n2, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
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

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"name": "live"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()
	asOf := n.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Nodes.Delete(context.Background(), nid); err != nil {
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

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"name": "live"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()
	asOf := n.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Nodes.Delete(context.Background(), nid); err != nil {
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

func TestNodeAsOfBeforeCloseVersionHidesCloseState(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"name": "live"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	beforeCloseTx := clk.PeekInstant() - 1
	closeTx := clk.PeekInstant()
	closeValidTo := g.nodeValidFrom(n) + 2000
	if err := g.Nodes.CloseVersion(context.Background(), n.ID(), closeValidTo); err != nil {
		t.Fatalf("CloseVersion: %v", err)
	}

	before, err := g.Temporal.NodeAsOf(n.ID(), beforeCloseTx)
	if err != nil {
		t.Fatalf("NodeAsOf before close: %v", err)
	}
	assertTxTimeVisibleBeforeDelete(t, before.Temporal())
	if got, _ := before.GetProperty("name"); got != "live" {
		t.Fatalf("NodeAsOf before close property = %v, want live", got)
	}

	after, err := g.Temporal.NodeAsOf(n.ID(), closeTx)
	if err != nil {
		t.Fatalf("NodeAsOf at close: %v", err)
	}
	if tm := after.Temporal(); tm == nil || tm.ValidTo != closeValidTo || tm.TxFrom != closeTx {
		t.Fatalf("NodeAsOf at close temporal = %+v, want ValidTo=%d TxFrom=%d", tm, closeValidTo, closeTx)
	}

	nodes, err := g.Temporal.NodesAsOf(beforeCloseTx)
	if err != nil {
		t.Fatalf("NodesAsOf before close: %v", err)
	}
	for _, got := range nodes {
		if got.ID() == n.ID() {
			assertTxTimeVisibleBeforeDelete(t, got.Temporal())
			return
		}
	}
	t.Fatalf("NodesAsOf before close did not include node %d", n.ID())
}

func TestNodesAsOfReturnsSortedByID(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	for i := 0; i < 64; i++ {
		if _, err := g.Nodes.Add(context.Background(), []string{"SortedNode"}, map[string]any{"i": int64(i)}); err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
	}
	nodes, err := g.Temporal.NodesAsOf(clk.PeekInstant())
	if err != nil {
		t.Fatalf("NodesAsOf: %v", err)
	}
	if len(nodes) != 64 {
		t.Fatalf("len(NodesAsOf) = %d, want 64", len(nodes))
	}
	for i := 1; i < len(nodes); i++ {
		if nodes[i-1].ID() > nodes[i].ID() {
			t.Fatalf("NodesAsOf order[%d:%d] = %d > %d", i-1, i, nodes[i-1].ID(), nodes[i].ID())
		}
	}
}

func TestRelAsOfDeletedEntityBeforeDelete(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "REL", n1, n2, map[string]any{"state": "live"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()
	asOf := r.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Rels.Delete(context.Background(), rid); err != nil {
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

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "REL", n1, n2, map[string]any{"state": "live"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()
	asOf := r.Temporal().TxFrom
	clk.Advance(2 * time.Millisecond)

	if err := g.Nodes.Delete(context.Background(), n1.ID()); err != nil {
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

func TestRelAsOfBeforeCloseVersionHidesCloseState(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "REL", n1, n2, map[string]any{"state": "live"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	beforeCloseTx := clk.PeekInstant() - 1
	closeTx := clk.PeekInstant()
	closeValidTo := g.relValidFrom(r) + 2000
	if err := g.Rels.CloseVersion(context.Background(), r.ID(), closeValidTo); err != nil {
		t.Fatalf("CloseVersion: %v", err)
	}

	before, err := g.Temporal.RelAsOf(r.ID(), beforeCloseTx)
	if err != nil {
		t.Fatalf("RelAsOf before close: %v", err)
	}
	assertTxTimeVisibleBeforeDelete(t, before.Temporal())
	if got, _ := before.GetProperty("state"); got != "live" {
		t.Fatalf("RelAsOf before close property = %v, want live", got)
	}

	after, err := g.Temporal.RelAsOf(r.ID(), closeTx)
	if err != nil {
		t.Fatalf("RelAsOf at close: %v", err)
	}
	if tm := after.Temporal(); tm == nil || tm.ValidTo != closeValidTo || tm.TxFrom != closeTx {
		t.Fatalf("RelAsOf at close temporal = %+v, want ValidTo=%d TxFrom=%d", tm, closeValidTo, closeTx)
	}

	rels, err := g.Temporal.RelsAsOf(beforeCloseTx)
	if err != nil {
		t.Fatalf("RelsAsOf before close: %v", err)
	}
	for _, got := range rels {
		if got.ID() == r.ID() {
			assertTxTimeVisibleBeforeDelete(t, got.Temporal())
			return
		}
	}
	t.Fatalf("RelsAsOf before close did not include relationship %d", r.ID())
}

func TestRelsAsOfReturnsSortedByID(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"SortedEndpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"SortedEndpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	for i := 0; i < 64; i++ {
		if _, err := g.Rels.Add(context.Background(), "SORTED_REL", a, b, map[string]any{"i": int64(i)}); err != nil {
			t.Fatalf("AddRelationship %d: %v", i, err)
		}
	}
	rels, err := g.Temporal.RelsAsOf(clk.PeekInstant())
	if err != nil {
		t.Fatalf("RelsAsOf: %v", err)
	}
	if len(rels) != 64 {
		t.Fatalf("len(RelsAsOf) = %d, want 64", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i-1].ID() > rels[i].ID() {
			t.Fatalf("RelsAsOf order[%d:%d] = %d > %d", i-1, i, rels[i-1].ID(), rels[i].ID())
		}
	}
}

func TestGetRelAsOf(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	n2, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, _ := g.Rels.Add(context.Background(), "REL", n1, n2, nil)
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

func TestTxTimeQueriesAfterCloseReturnGraphClosed(t *testing.T) {
	g := newTxTimeGraph(t)
	useTestClock(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "REL", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := g.Temporal.NodeAsOf(a.ID(), a.Temporal().TxFrom); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("NodeAsOf after close = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Temporal.RelAsOf(r.ID(), r.Temporal().TxFrom); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("RelAsOf after close = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Temporal.NodesAsOf(a.Temporal().TxFrom); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("NodesAsOf after close = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Temporal.RelsAsOf(r.Temporal().TxFrom); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("RelsAsOf after close = %v, want ErrGraphClosed", err)
	}
}

type txTimeQueryFaultStore struct {
	storepkg.MandatoryStore
	err            error
	node           *types.Node
	rel            *types.Relationship
	nodes          []*types.Node
	rels           []*types.Relationship
	nodeAsOfCalls  atomic.Int64
	relAsOfCalls   atomic.Int64
	nodesAsOfCalls atomic.Int64
	relsAsOfCalls  atomic.Int64
}

func (s *txTimeQueryFaultStore) NodeAsOf(types.NodeID, types.Instant) (*types.Node, error) {
	s.nodeAsOfCalls.Add(1)
	return s.node, s.err
}

func (s *txTimeQueryFaultStore) RelAsOf(types.RelID, types.Instant) (*types.Relationship, error) {
	s.relAsOfCalls.Add(1)
	return s.rel, s.err
}

func (s *txTimeQueryFaultStore) NodesAsOf(types.Instant) ([]*types.Node, error) {
	s.nodesAsOfCalls.Add(1)
	return s.nodes, s.err
}

func (s *txTimeQueryFaultStore) RelsAsOf(types.Instant) ([]*types.Relationship, error) {
	s.relsAsOfCalls.Add(1)
	return s.rels, s.err
}

func TestTxTimeQueryCopyPolicy(t *testing.T) {
	t.Parallel()
	native, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New native memory: %v", err)
	}
	t.Cleanup(func() { _ = native.Close() })
	if native.txTimeQuery == nil {
		t.Fatal("native memory transaction-time query capability was not enabled")
	}
	if native.txTimeQueryCopy {
		t.Fatal("native memory transaction-time query rows should not be defensively copied twice")
	}

	external, err := New(Config{Store: &txTimeQueryFaultStore{MandatoryStore: memory.New()}})
	if err != nil {
		t.Fatalf("New external capability: %v", err)
	}
	t.Cleanup(func() { _ = external.Close() })
	if external.txTimeQuery == nil {
		t.Fatal("direct external transaction-time query capability was not enabled")
	}
	if !external.txTimeQueryCopy {
		t.Fatal("direct external transaction-time query rows must be copied before graph normalization")
	}
}

func TestTemporalAsOfRejectsInvalidIDsBeforeTxTimeCapability(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic transaction-time lookup fault")
	store := &txTimeQueryFaultStore{
		MandatoryStore: memory.New(),
		err:            injected,
	}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.txTimeQuery == nil {
		t.Fatal("direct transaction-time query capability was not enabled")
	}

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "zero node",
			run: func() error {
				_, err := g.Temporal.NodeAsOf(0, 1)
				return err
			},
		},
		{
			name: "negative node",
			run: func() error {
				_, err := g.Temporal.NodeAsOf(types.NodeID(-1), 1)
				return err
			},
		},
		{
			name: "zero relationship",
			run: func() error {
				_, err := g.Temporal.RelAsOf(0, 1)
				return err
			},
		},
		{
			name: "negative relationship",
			run: func() error {
				_, err := g.Temporal.RelAsOf(types.RelID(-1), 1)
				return err
			},
		},
	}
	for _, tc := range tests {
		err := tc.run()
		if !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("%s error = %v, want ErrInvalidStoreMutation", tc.name, err)
		}
		if errors.Is(err, injected) {
			t.Fatalf("%s reached transaction-time capability before validation", tc.name)
		}
	}
	if got := store.nodeAsOfCalls.Load(); got != 0 {
		t.Fatalf("NodeAsOf capability calls = %d, want 0", got)
	}
	if got := store.relAsOfCalls.Load(); got != 0 {
		t.Fatalf("RelAsOf capability calls = %d, want 0", got)
	}
}

func TestTemporalAsOfMapsNilCapabilityMissToNoVersion(t *testing.T) {
	t.Parallel()
	store := &txTimeQueryFaultStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.txTimeQuery == nil {
		t.Fatal("direct transaction-time query capability was not enabled")
	}

	if n, err := g.Temporal.NodeAsOf(types.NodeID(1), 1); !errors.Is(err, ErrNoVersionAsOf) || n != nil {
		t.Fatalf("NodeAsOf nil capability miss = (%v, %v), want nil, ErrNoVersionAsOf", n, err)
	}
	if r, err := g.Temporal.RelAsOf(types.RelID(1), 1); !errors.Is(err, ErrNoVersionAsOf) || r != nil {
		t.Fatalf("RelAsOf nil capability miss = (%v, %v), want nil, ErrNoVersionAsOf", r, err)
	}
	if got := store.nodeAsOfCalls.Load(); got != 1 {
		t.Fatalf("NodeAsOf capability calls = %d, want 1", got)
	}
	if got := store.relAsOfCalls.Load(); got != 1 {
		t.Fatalf("RelAsOf capability calls = %d, want 1", got)
	}
}

func TestTemporalAsOfRejectsMismatchedCapabilityRows(t *testing.T) {
	t.Parallel()
	store := &txTimeQueryFaultStore{
		MandatoryStore: memory.New(),
		node:           types.NewNode(types.NodeID(2), 1, nil),
		rel:            types.NewRelationship(types.RelID(2), 1, types.NodeID(1), types.NodeID(2)),
	}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.txTimeQuery == nil {
		t.Fatal("direct transaction-time query capability was not enabled")
	}

	if n, err := g.Temporal.NodeAsOf(types.NodeID(1), 1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || n != nil {
		t.Fatalf("NodeAsOf mismatched capability row = (%v, %v), want nil, ErrInvalidStoreMutation", n, err)
	}
	if r, err := g.Temporal.RelAsOf(types.RelID(1), 1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || r != nil {
		t.Fatalf("RelAsOf mismatched capability row = (%v, %v), want nil, ErrInvalidStoreMutation", r, err)
	}
}

func TestTemporalBulkAsOfRejectsInvalidCapabilityRows(t *testing.T) {
	t.Parallel()
	store := &txTimeQueryFaultStore{
		MandatoryStore: memory.New(),
		nodes: []*types.Node{
			types.NewNode(types.NodeID(1), 1, nil),
			nil,
		},
		rels: []*types.Relationship{
			types.NewRelationship(types.RelID(1), 1, types.NodeID(1), types.NodeID(2)),
			nil,
		},
	}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.txTimeQuery == nil {
		t.Fatal("direct transaction-time query capability was not enabled")
	}

	if nodes, err := g.Temporal.NodesAsOf(1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || nodes != nil {
		t.Fatalf("NodesAsOf invalid capability row = (%v, %v), want nil, ErrInvalidStoreMutation", nodes, err)
	}
	if rels, err := g.Temporal.RelsAsOf(1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) || rels != nil {
		t.Fatalf("RelsAsOf invalid capability row = (%v, %v), want nil, ErrInvalidStoreMutation", rels, err)
	}
}

func TestTemporalAsOfCopiesCapabilityRowsBeforeVisibilityNormalization(t *testing.T) {
	t.Parallel()
	node := types.NewNode(types.NodeID(1), 1, nil)
	node.SetTemporal(&types.TemporalMetadata{TxFrom: 1, TxTo: 20, ValidTo: 30, DeletedAt: 30})
	rel := types.NewRelationship(types.RelID(1), 1, types.NodeID(1), types.NodeID(2))
	rel.SetTemporal(&types.TemporalMetadata{TxFrom: 1, TxTo: 20, ValidTo: 30, DeletedAt: 30})
	bulkNode := types.NewNode(types.NodeID(2), 1, nil)
	bulkNode.SetTemporal(&types.TemporalMetadata{TxFrom: 1, TxTo: 20, ValidTo: 30, DeletedAt: 30})
	bulkRel := types.NewRelationship(types.RelID(2), 1, types.NodeID(1), types.NodeID(2))
	bulkRel.SetTemporal(&types.TemporalMetadata{TxFrom: 1, TxTo: 20, ValidTo: 30, DeletedAt: 30})

	store := &txTimeQueryFaultStore{
		MandatoryStore: memory.New(),
		node:           node,
		rel:            rel,
		nodes:          []*types.Node{bulkNode},
		rels:           []*types.Relationship{bulkRel},
	}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	gotNode, err := g.Temporal.NodeAsOf(node.ID(), 10)
	if err != nil {
		t.Fatalf("NodeAsOf: %v", err)
	}
	if gotNode.Temporal().TxTo != 0 || gotNode.Temporal().DeletedAt != 0 || gotNode.Temporal().ValidTo != 0 {
		t.Fatalf("visible node temporal = %+v, want future close/delete hidden", gotNode.Temporal())
	}
	if tm := node.Temporal(); tm.TxTo != 20 || tm.DeletedAt != 30 || tm.ValidTo != 30 {
		t.Fatalf("source node temporal mutated to %+v", tm)
	}

	gotRel, err := g.Temporal.RelAsOf(rel.ID(), 10)
	if err != nil {
		t.Fatalf("RelAsOf: %v", err)
	}
	if gotRel.Temporal().TxTo != 0 || gotRel.Temporal().DeletedAt != 0 || gotRel.Temporal().ValidTo != 0 {
		t.Fatalf("visible rel temporal = %+v, want future close/delete hidden", gotRel.Temporal())
	}
	if tm := rel.Temporal(); tm.TxTo != 20 || tm.DeletedAt != 30 || tm.ValidTo != 30 {
		t.Fatalf("source rel temporal mutated to %+v", tm)
	}

	gotNodes, err := g.Temporal.NodesAsOf(10)
	if err != nil {
		t.Fatalf("NodesAsOf: %v", err)
	}
	if len(gotNodes) != 1 || gotNodes[0].Temporal().TxTo != 0 || gotNodes[0].Temporal().DeletedAt != 0 || gotNodes[0].Temporal().ValidTo != 0 {
		t.Fatalf("visible bulk nodes = %+v, want one normalized node", gotNodes)
	}
	if tm := bulkNode.Temporal(); tm.TxTo != 20 || tm.DeletedAt != 30 || tm.ValidTo != 30 {
		t.Fatalf("source bulk node temporal mutated to %+v", tm)
	}

	gotRels, err := g.Temporal.RelsAsOf(10)
	if err != nil {
		t.Fatalf("RelsAsOf: %v", err)
	}
	if len(gotRels) != 1 || gotRels[0].Temporal().TxTo != 0 || gotRels[0].Temporal().DeletedAt != 0 || gotRels[0].Temporal().ValidTo != 0 {
		t.Fatalf("visible bulk rels = %+v, want one normalized relationship", gotRels)
	}
	if tm := bulkRel.Temporal(); tm.TxTo != 20 || tm.DeletedAt != 30 || tm.ValidTo != 30 {
		t.Fatalf("source bulk rel temporal mutated to %+v", tm)
	}
}

func TestTxTimeQueryCapabilityBulkErrorsPropagate(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic transaction-time bulk fault")
	g, err := New(Config{Store: &txTimeQueryFaultStore{
		MandatoryStore: memory.New(),
		err:            injected,
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if g.txTimeQuery == nil {
		t.Fatal("direct transaction-time query capability was not enabled")
	}

	if _, err := g.Temporal.NodesAsOf(1); !errors.Is(err, injected) {
		t.Fatalf("NodesAsOf error = %v, want injected fault", err)
	}
	if _, err := g.Temporal.RelsAsOf(1); !errors.Is(err, injected) {
		t.Fatalf("RelsAsOf error = %v, want injected fault", err)
	}
}

func TestNormalizeTemporalVisibleAtTxTime(t *testing.T) {
	normalizeTemporalVisibleAtTxTime(nil, 10)

	if got := nodeVisibleAtTxTime(nil, 10); got != nil {
		t.Fatalf("nodeVisibleAtTxTime(nil) = %v, want nil", got)
	}
	if got := relVisibleAtTxTime(nil, 10); got != nil {
		t.Fatalf("relVisibleAtTxTime(nil) = %v, want nil", got)
	}

	t.Run("future TxTo hidden", func(t *testing.T) {
		tm := &types.TemporalMetadata{TxTo: 20}
		normalizeTemporalVisibleAtTxTime(tm, 10)
		if tm.TxTo != 0 {
			t.Fatalf("TxTo = %d, want 0", tm.TxTo)
		}
	})

	t.Run("past TxTo kept", func(t *testing.T) {
		tm := &types.TemporalMetadata{TxTo: 10}
		normalizeTemporalVisibleAtTxTime(tm, 10)
		if tm.TxTo != 10 {
			t.Fatalf("TxTo = %d, want 10", tm.TxTo)
		}
	})

	t.Run("future delete hidden with matching valid to", func(t *testing.T) {
		tm := &types.TemporalMetadata{ValidTo: 20, DeletedAt: 20}
		normalizeTemporalVisibleAtTxTime(tm, 10)
		if tm.DeletedAt != 0 || tm.ValidTo != 0 {
			t.Fatalf("DeletedAt=%d ValidTo=%d, want both hidden", tm.DeletedAt, tm.ValidTo)
		}
	})

	t.Run("future delete hidden without changing independent valid to", func(t *testing.T) {
		tm := &types.TemporalMetadata{ValidTo: 15, DeletedAt: 20}
		normalizeTemporalVisibleAtTxTime(tm, 10)
		if tm.DeletedAt != 0 || tm.ValidTo != 15 {
			t.Fatalf("DeletedAt=%d ValidTo=%d, want DeletedAt hidden and ValidTo kept", tm.DeletedAt, tm.ValidTo)
		}
	})

	t.Run("past delete kept", func(t *testing.T) {
		tm := &types.TemporalMetadata{ValidTo: 10, DeletedAt: 10}
		normalizeTemporalVisibleAtTxTime(tm, 10)
		if tm.DeletedAt != 10 || tm.ValidTo != 10 {
			t.Fatalf("DeletedAt=%d ValidTo=%d, want both kept", tm.DeletedAt, tm.ValidTo)
		}
	})
}
