package graph

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestGraphTx_PropertyConvenienceMethods(t *testing.T) {
	g := newTestGraph(t)

	n, err := g.AddNode([]string{"A"}, map[string]any{"old": "value"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	b, err := g.AddNode([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.AddRelationship("REL", n, b, map[string]any{"old_weight": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	tx := g.BeginTx()
	if err := tx.SetNodeProperty(n.InternalID().SnowflakeID(), "new", "set"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("SetNodeProperty: %v", err)
	}
	if err := tx.DeleteNodeProperty(n.InternalID().SnowflakeID(), "old"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("DeleteNodeProperty: %v", err)
	}
	if err := tx.SetRelationshipProperty(r.InternalID().SnowflakeID(), "weight", int64(2)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("SetRelationshipProperty: %v", err)
	}
	if err := tx.DeleteRelationshipProperty(r.InternalID().SnowflakeID(), "old_weight"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("DeleteRelationshipProperty: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotNode, err := g.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if v, ok := gotNode.GetProperty("new"); !ok || v != "set" {
		t.Fatalf("node new property = %v (ok=%v), want set", v, ok)
	}
	if _, ok := gotNode.GetProperty("old"); ok {
		t.Fatal("node old property should be deleted")
	}

	gotRel, err := g.GetRelationship(r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if v, ok := gotRel.GetProperty("weight"); !ok || v != int64(2) {
		t.Fatalf("rel weight = %v (ok=%v), want 2", v, ok)
	}
	if _, ok := gotRel.GetProperty("old_weight"); ok {
		t.Fatal("rel old_weight property should be deleted")
	}
}

func TestBadgerStore_AddNodeLabelToken(t *testing.T) {
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 100, 1, nil)
	id := n.InternalID().SnowflakeID()
	updated := n.DeepCopy()
	if !updated.AddLabelTokenRaw(2) {
		t.Fatal("AddLabelTokenRaw returned false")
	}

	if err := bs.AddNodeLabelToken(id, 2, updated); err != nil {
		t.Fatalf("AddNodeLabelToken: %v", err)
	}

	nodes, err := bs.NodesByLabel(2, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatal("node missing from added label index")
	}
	got, err := bs.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasLabelTokenRaw(2) {
		t.Fatal("persisted node missing added label token")
	}
}

func TestBadgerStore_AddNodeLabelTokenWithHistory(t *testing.T) {
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 101, 1, nil)
	id := n.InternalID().SnowflakeID()
	prevVersion := n.Version()
	prevState := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(2)
	updated.SetVersion(prevVersion + 1)

	if err := bs.AddNodeLabelTokenWithHistory(id, 2, updated, prevVersion, prevState); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory: %v", err)
	}

	hist, err := bs.GetNodeVersion(id, prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist.Version() != prevVersion {
		t.Fatalf("history version = %d, want %d", hist.Version(), prevVersion)
	}
	nodes, err := bs.NodesByLabel(2, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatal("node missing from added label index")
	}
}

func TestTieredStore_AddNodeLabelToken(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	n, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()
	tok, err := g.GetOrCreateLabel("User")
	if err != nil {
		t.Fatalf("GetOrCreateLabel: %v", err)
	}
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(tok)

	if err := ts.AddNodeLabelToken(id, tok, updated); err != nil {
		t.Fatalf("AddNodeLabelToken: %v", err)
	}

	nodes, err := g.NodesByLabel("User", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatal("node missing from TieredStore added label index")
	}
}

func TestTieredStore_AddNodeLabelTokenWithHistory(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	n, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()
	tok, err := g.GetOrCreateLabel("User")
	if err != nil {
		t.Fatalf("GetOrCreateLabel: %v", err)
	}
	prevVersion := n.Version()
	prevState := n.DeepCopy()
	updated := n.DeepCopy()
	updated.AddLabelTokenRaw(tok)
	updated.SetVersion(prevVersion + 1)

	if err := ts.AddNodeLabelTokenWithHistory(id, tok, updated, prevVersion, prevState); err != nil {
		t.Fatalf("AddNodeLabelTokenWithHistory: %v", err)
	}

	hist, err := g.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 1 {
		t.Fatalf("history entries = %d, want 1", len(hist))
	}
	nodes, err := g.NodesByLabel("User", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatal("node missing from TieredStore added label index")
	}
}

func TestBadgerStore_AddNodeLabelToken_NotFound(t *testing.T) {
	bs := newTestBadgerStore(t)

	updated := types.NewNode(snowflake.ID(999), 1, []uint16{2})
	err := bs.AddNodeLabelToken(snowflake.ID(999), 2, updated)
	if err == nil {
		t.Fatal("expected error for missing node")
	}
}
