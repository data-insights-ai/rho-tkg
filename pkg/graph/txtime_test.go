package graph

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

func newTxTimeGraph(t *testing.T) *Graph {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestTxFromSetOnAdd(t *testing.T) {
	// TxFrom should be set on new node and rel
	g := newTxTimeGraph(t)
	before := types.Instant(time.Now().UnixMilli())

	n, err := g.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	after := types.Instant(time.Now().UnixMilli())

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
	n2, _ := g.AddNode([]string{"B"}, nil)
	before2 := types.Instant(time.Now().UnixMilli())
	r, err := g.AddRelationship("REL", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	after2 := types.Instant(time.Now().UnixMilli())

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

	n, _ := g.AddNode([]string{"A"}, nil)
	nid := n.InternalID().SnowflakeID()
	origTxFrom := n.Temporal().TxFrom

	time.Sleep(2 * time.Millisecond) // ensure measurable time difference

	updated, err := g.UpdateNode(nid, map[string]any{"x": 1})
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
	hist, err := g.GetNodeHistory(nid)
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

	n, _ := g.AddNode([]string{"A"}, nil)
	nid := n.InternalID().SnowflakeID()

	if err := g.DeleteNode(nid); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// The tombstone history entry should have TxFrom set, TxTo non-zero
	hist, err := g.GetNodeHistory(nid)
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
}

func TestGetNodeAsOf_BeforeCreate(t *testing.T) {
	g := newTxTimeGraph(t)

	before := types.Instant(time.Now().UnixMilli() - 1000) // 1 second in past
	n, _ := g.AddNode([]string{"A"}, nil)
	nid := n.InternalID().SnowflakeID()

	_, err := g.GetNodeAsOf(nid, before)
	if !errors.Is(err, ErrNoVersionAsOf) {
		t.Errorf("expected ErrNoVersionAsOf, got %v", err)
	}
}

func TestGetNodeAsOf_CurrentVersion(t *testing.T) {
	g := newTxTimeGraph(t)

	n, _ := g.AddNode([]string{"A"}, nil)
	nid := n.InternalID().SnowflakeID()

	after := types.Instant(time.Now().UnixMilli() + 1000) // 1 second in future

	got, err := g.GetNodeAsOf(nid, after)
	if err != nil {
		t.Fatalf("GetNodeAsOf: %v", err)
	}
	if got.InternalID().SnowflakeID() != nid {
		t.Error("wrong node returned")
	}
}

func TestGetNodeAsOf_HistoricalVersion(t *testing.T) {
	g := newTxTimeGraph(t)

	n, _ := g.AddNode([]string{"A"}, nil)
	nid := n.InternalID().SnowflakeID()

	// Record the TxFrom of the original version
	origTxFrom := n.Temporal().TxFrom

	time.Sleep(2 * time.Millisecond)

	// Update the node
	updated, _ := g.UpdateNode(nid, map[string]any{"x": 1})
	newTxFrom := updated.Temporal().TxFrom

	// Query at a time between original TxFrom and newTxFrom
	mid := origTxFrom + (newTxFrom-origTxFrom)/2
	if mid == origTxFrom {
		mid = origTxFrom + 1
	}

	got, err := g.GetNodeAsOf(nid, mid)
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

	// Create first node
	n1, _ := g.AddNode([]string{"A"}, nil)
	_ = n1

	// Record time
	midTime := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	// Create second node after midTime
	n2, _ := g.AddNode([]string{"A"}, nil)
	_ = n2

	// GetNodesAsOf(midTime) should return only n1 (n2 didn't exist yet)
	got, err := g.GetNodesAsOf(midTime)
	if err != nil {
		t.Fatalf("GetNodesAsOf: %v", err)
	}

	found1, found2 := false, false
	for _, node := range got {
		id := node.InternalID().SnowflakeID()
		if id == n1.InternalID().SnowflakeID() {
			found1 = true
		}
		if id == n2.InternalID().SnowflakeID() {
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

func TestGetRelAsOf(t *testing.T) {
	g := newTxTimeGraph(t)

	n1, _ := g.AddNode([]string{"A"}, nil)
	n2, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("REL", n1, n2, nil)
	rid := r.InternalID().SnowflakeID()

	before := r.Temporal().TxFrom - 1

	_, err := g.GetRelAsOf(rid, before)
	if !errors.Is(err, ErrNoVersionAsOf) {
		t.Errorf("expected ErrNoVersionAsOf before creation, got %v", err)
	}

	after := types.Instant(time.Now().UnixMilli() + 1000)
	got, err := g.GetRelAsOf(rid, after)
	if err != nil {
		t.Fatalf("GetRelAsOf after: %v", err)
	}
	if got.InternalID().SnowflakeID() != rid {
		t.Error("wrong rel returned")
	}
}
